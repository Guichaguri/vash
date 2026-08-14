//! Executing Redis commands.
//!
//! A **codec, not an executor** (M10 phase 2b). Every command that touches
//! storage is translated into a `vash_core::Command`, handed to
//! [`crate::dispatch::execute`] — the same function that runs VCP and memcached
//! — and rendered back into this dialect. This module opens no transaction and
//! calls no `Store` method; `state.store` does not appear in it.
//!
//! That is what makes the counting and the error classification single-sourced.
//! Requests are counted where they execute, so a Redis command cannot be missed
//! by an instrumentation call somebody forgot to add; and `StoreError` is
//! classified once, in `dispatch::to_failed`, rather than here as well by a
//! second judgement that had to agree with the first forever.
//!
//! Two things stay local, and the line between them is the useful one:
//! **commands about the conversation** — `HELLO`, `AUTH`, `QUIT`, and the
//! version they negotiate — never reach the boundary, because they are
//! properties of this socket rather than operations on the cache. Everything
//! else does.
//!
//! Several Redis commands collapse onto one domain operation on the way through:
//! `EXISTS`, `TYPE` and `TTL` are all deadline queries that differ only in how
//! the answer is worded, and `PERSIST` is an `EXPIRE` with a guard. The
//! translation is in [`translate`] and the wording in [`render`].
//!
//! **The arithmetic commands are atomic (M10).** `INCR`, `INCRBY`, `DECR`,
//! `DECRBY`, `INCRBYFLOAT`, `INCREX` and `APPEND` are single storage primitives
//! evaluated inside the shard writer's transaction, so two clients incrementing
//! one counter cannot lose an update — the guarantee Redis gets from being
//! single-threaded, obtained here from the single writer thread that already
//! serialises every write to a shard. They used to be a read followed by a
//! write issued from here, and were not atomic; `vash_core::arith` is where the
//! arithmetic moved to.
//!
//! **The conditional writes are atomic too (M10 phase 2).** `SET … KEEPTTL`,
//! `SET … GET`, `EXPIRE`/`EXPIREAT` with `NX`/`XX`/`GT`/`LT` and `PERSIST` each
//! used to read something here and then write against what they had read.
//! They are now single storage primitives: the guard is evaluated, and the
//! displaced value captured, inside the transaction that does the writing.
//! `KEEPTTL` in particular no longer reads a deadline at all — it travels as
//! `TtlChange::Keep` and is settled against the record being replaced, off the
//! lookup an overwrite performs anyway.
//!
//! **`MSETEX` with `NX`/`XX` is atomic within a shard and no further**, because
//! its guard has to see every key at once and a batch spanning shards is several
//! transactions. That is plan §16's standing non-goal rather than an open seam
//! here; `docs/protocol.md` states it.

use tracing::{error, warn};
use vash_core::{CoreError, Key, Reply, SetMode, Stored};
use vash_proto::resp::command::{Condition, ExpireCondition, Expiry, IncrEx, Number};
use vash_proto::resp::{Command, ErrorReply, Outcome, ProtocolError, Version, encode};

use crate::auth::ConnAuth;
use crate::dispatch::Closing;
use crate::metrics::ErrorClass;
use crate::state::ServerState;

/// Executes every complete RESP command in `block`, appending the replies.
///
/// The block was already measured by the caller to end on a command boundary,
/// so parsing here cannot run short; re-parsing rather than passing the parsed
/// form across is what keeps the borrowed key and value slices from crossing a
/// task boundary, exactly as the memcached path does.
///
/// `version` is the connection's negotiated RESP dialect. It is `&mut` because
/// a `HELLO` in the middle of a pipelined block changes how everything after it
/// is rendered.
pub fn execute_block(
    state: &ServerState,
    conn: &mut ConnAuth,
    block: &[u8],
    version: &mut Version,
    out: &mut Vec<u8>,
) -> Closing {
    let mut rest = block;
    while !rest.is_empty() {
        match vash_proto::resp::parse(rest) {
            Ok(Outcome::Command(parsed)) => {
                let consumed = parsed.consumed;
                if execute(state, conn, &parsed.command, version, out) == Closing::Yes {
                    return Closing::Yes;
                }
                rest = &rest[consumed..];
            }
            Err(ProtocolError::Recoverable { reply, consumed }) => {
                state.metrics.other();
                state.metrics.error(ErrorClass::Client);
                encode::error_reply(out, &reply);
                rest = &rest[consumed..];
            }
            // Unreachable: the caller only includes whole commands. Stopping
            // rather than looping keeps a logic slip from spinning a core.
            Ok(Outcome::Incomplete) | Err(ProtocolError::Fatal(_)) => {
                error!("a RESP block did not end on a command boundary");
                break;
            }
        }
    }
    Closing::No
}

/// Whether a command can be answered without any chance of writing.
///
/// Note what is missing: every arithmetic command rewrites its key, and so does
/// `APPEND`, despite both reading first.
pub fn inline_safe(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::Get { .. }
            | Command::MGet { .. }
            | Command::Exists { .. }
            | Command::Type { .. }
            | Command::Ttl { .. }
            | Command::Ping { .. }
            | Command::Hello { .. }
            | Command::Auth(_)
            | Command::Quit
            // Reads a few atomics and the store's environment statistics, none
            // of which walks anything.
            | Command::Info { .. }
    )
    // `SCAN` is missing and must stay missing: it is bounded by
    // `listing_max_scan` records, not by anything a runtime worker should be
    // blocked for. `Command::ListKeys` — what it becomes — answers the same.
}

fn execute(
    state: &ServerState,
    conn: &mut ConnAuth,
    command: &Command<'_>,
    version: &mut Version,
    out: &mut Vec<u8>,
) -> Closing {
    if let Command::Quit = command {
        state.metrics.other();
        encode::ok(out);
        return Closing::Yes;
    }

    // Refused before the command is looked at. Redis is *more* restrictive than
    // VCP here — a bare `HELLO` is refused, where VCP must allow it because
    // first-byte detection needs the connection to open with one.
    if let Some(failure) = refusal(state, conn, command) {
        state.metrics.auth_refused();
        state.metrics.other();
        state.metrics.error(failure.class);
        encode::error(out, failure.code, failure.message);
        return Closing::No;
    }

    match run(state, conn, command, version, out) {
        Ok(()) => {}
        Err(failure) => {
            // Only what the shared executor has not already counted; see
            // [`Failure::counted`].
            if !failure.counted {
                state.metrics.other();
                state.metrics.error(failure.class);
            }
            encode::error(out, failure.code, failure.message);
        }
    }
    Closing::No
}

/// Whether a Redis command must be refused for want of authentication.
///
/// The pre-auth set is `AUTH`, `HELLO … AUTH`, and `QUIT`. A bare `HELLO` gets
/// Redis's own long message, which exists precisely to tell a client how to
/// authenticate and negotiate at once.
fn refusal(state: &ServerState, conn: &ConnAuth, command: &Command<'_>) -> Option<Failure> {
    if conn.is_authenticated() || !state.auth.required() {
        return None;
    }

    match command {
        Command::Auth(_) | Command::Quit => None,
        Command::Hello { auth: Some(_), .. } => None,
        Command::Hello { auth: None, .. } => Some(Failure {
            code: "NOAUTH",
            message: HELLO_UNAUTHENTICATED,
            class: ErrorClass::Client,
            counted: false,
        }),
        _ => Some(Failure {
            code: "NOAUTH",
            message: "Authentication required.",
            class: ErrorClass::Client,
            counted: false,
        }),
    }
}

/// Redis's own wording, long as it is: client libraries surface it verbatim and
/// it is the only place the combined form is explained.
const HELLO_UNAUTHENTICATED: &str = "HELLO must be called with the client already \
     authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to \
     authenticate the client and select the RESP protocol version at the same time";

/// One message for a bad name and a bad secret alike, as Redis does, so the
/// error does not confirm which names exist.
const WRONGPASS: &str = "invalid username-password pair or user is disabled.";

/// Verifies a Redis credential and records the outcome.
fn authenticate(
    state: &ServerState,
    conn: &mut ConnAuth,
    credential: &vash_proto::resp::command::Credential<'_>,
) -> Answered {
    let table = state.auth.current();

    // Redis distinguishes this from a wrong password, and it matters: it is the
    // difference between "your credential is wrong" and "there is nothing here
    // to authenticate against". A client must never have the two confused.
    // Redis 7's wording, verified against 7.4.10.
    if !table.configured() {
        return Err(Failure::client(
            "AUTH <password> called without any password configured for the default user. \
             Are you sure your configuration is correct?",
        ));
    }

    let name = credential
        .name
        .unwrap_or(crate::auth::DEFAULT_NAME.as_bytes());

    match table.verify(name, credential.secret) {
        Some(identity) => {
            conn.succeed(identity);
            state.metrics.auth_ok();
            Ok(())
        }
        None => {
            conn.fail();
            warn!(
                name = %String::from_utf8_lossy(name),
                failures = conn.failures(),
                "authentication failed"
            );
            state.metrics.auth_failed();
            Err(Failure {
                code: "WRONGPASS",
                message: WRONGPASS,
                class: ErrorClass::Client,
                counted: false,
            })
        }
    }
}

/// A command that could not be answered.
///
/// Carries the wire error and how it should be counted, so every failure path
/// reports both and neither can be forgotten.
#[derive(Debug)]
struct Failure {
    code: &'static str,
    message: &'static str,
    class: ErrorClass,
    /// Already counted by [`crate::dispatch::execute`].
    ///
    /// Failures raised *before* a command reaches the shared executor — a key
    /// this store cannot hold, an expiry it cannot represent — are counted by
    /// this module, because nothing else has seen them. Everything from the
    /// executor was counted there, and counting it again would double it.
    counted: bool,
}

impl Failure {
    /// A failure raised here, before the command reached the executor.
    fn client(message: &'static str) -> Self {
        Self {
            code: "ERR",
            message,
            class: ErrorClass::Client,
            counted: false,
        }
    }

    /// A client error the executor already counted.
    fn executed(message: &'static str) -> Self {
        Self {
            counted: true,
            ..Self::client(message)
        }
    }

    /// A logic slip in this module — a command translated into a reply it cannot
    /// render. Never anything a client can provoke.
    fn internal() -> Self {
        Self {
            code: "ERR",
            message: "internal error",
            class: ErrorClass::Internal,
            counted: false,
        }
    }
}

impl From<&ErrorReply<'_>> for Failure {
    fn from(reply: &ErrorReply<'_>) -> Self {
        match reply {
            ErrorReply::Err(message) => Failure::client(message),
            ErrorReply::Coded(code, message) => Failure {
                code,
                message,
                class: ErrorClass::Client,
                counted: false,
            },
            // The parser produces the rest; they never reach the executor.
            _ => Failure::client("syntax error"),
        }
    }
}

/// Validation raised on this side of the boundary, which is only ever about a
/// key: everything else a client can get wrong is caught by the parser above or
/// by the store below.
impl From<CoreError> for Failure {
    fn from(err: CoreError) -> Self {
        match err {
            CoreError::EmptyKey | CoreError::KeyTooLong { .. } => Failure::client(INVALID_KEY),
            other => {
                error!(error = %other, "unexpected validation failure translating a command");
                Failure::internal()
            }
        }
    }
}

const NOT_AN_INTEGER: &str = "value is not an integer or out of range";
const NOT_A_FLOAT: &str = "value is not a valid float";
const OVERFLOW: &str = "increment or decrement would overflow";
const NOT_FINITE: &str = "increment would produce NaN or Infinity";
/// Redis accepts any binary key of any length; this store has LMDB's ceiling
/// and rejects the empty key. Both are documented divergences.
const INVALID_KEY: &str = "invalid key";
/// Redis names the command in this one, so there is a constant per command that
/// can produce it rather than a formatted string.
const INVALID_EXPIRE_SET: &str = "invalid expire time in 'set' command";
const INVALID_EXPIRE_MSETEX: &str = "invalid expire time in 'msetex' command";
const INVALID_EXPIRE_INCREX: &str = "invalid expire time in 'increx' command";

type Answered = Result<(), Failure>;

fn run(
    state: &ServerState,
    conn: &mut ConnAuth,
    command: &Command<'_>,
    version: &mut Version,
    out: &mut Vec<u8>,
) -> Answered {
    match command {
        // Answered here too, though the caller normally intercepts it: the
        // close is its business, not this function's.
        Command::Quit => {
            state.metrics.other();
            encode::ok(out);
            Ok(())
        }

        Command::Auth(credential) => {
            state.metrics.other();
            authenticate(state, conn, credential)?;
            encode::ok(out);
            Ok(())
        }

        Command::Hello {
            version: requested,
            auth,
        } => {
            state.metrics.other();

            // Three steps, in this order, because Redis does it in this order
            // and the order is observable twice over (verified against 7.4.10):
            //
            //   1. Validate the version. `HELLO 9 AUTH <good credential>`
            //      answers `NOPROTO` and leaves the connection *unauthenticated*
            //      — the credential is never looked at.
            //   2. Authenticate. A bad credential answers `WRONGPASS` and must
            //      leave the connection exactly as it was, rather than
            //      switching it to RESP3 and then refusing.
            //   3. Only now apply the version.
            let negotiated = match requested {
                None => *version,
                Some(2) => Version::Resp2,
                Some(3) => Version::Resp3,
                Some(_) => {
                    return Err(
                        (&ErrorReply::Coded("NOPROTO", "unsupported protocol version")).into(),
                    );
                }
            };

            if let Some(credential) = auth {
                authenticate(state, conn, credential)?;
            }

            *version = negotiated;
            encode::hello(out, *version);
            Ok(())
        }

        // `SCAN` reaches storage like everything below, but not through
        // `translate`: its cursor is a token that has to be resolved against
        // the server's table before there is a `ListRequest` to translate into,
        // and the next token has to be issued from the page that comes back.
        // The boundary is still `dispatch::execute` — only the pure-function
        // helper is bypassed.
        Command::Scan(scan) => run_scan(state, scan, *version, out),

        // The one command that touches storage without building a `Reply`: a
        // `GET` hit is written straight out of the store into the buffer, so
        // the value is copied once rather than twice. The boundary it steps
        // around is the `Reply`, not the counting — `execute_get_into` counts
        // exactly what `dispatch::execute` counts for a `GET`.
        Command::Get { key } => {
            let hit = crate::dispatch::execute_get_into(
                state,
                key_of(key)?,
                crate::metrics::CommandKind::Get,
                crate::metrics::Dialect::Resp,
                &mut |value| encode::bulk(out, value.data),
            )
            .map_err(resp_error)?;
            if !hit {
                encode::null(out, *version);
            }
            Ok(())
        }

        // Everything past this point touches storage, so it goes through the
        // shared boundary: translated into a `vash_core::Command`, executed by
        // `dispatch::execute` — which is where it gets counted and where its
        // failures get classified — and rendered back into this dialect.
        storage => {
            let translated = translate(storage)?;
            let reply = crate::dispatch::execute(state, &translated, crate::metrics::Dialect::Resp)
                .map_err(resp_error)?;
            render(storage, &reply, *version, out)
        }
    }
}

/// Answers `SCAN`.
///
/// The three steps the token table exists for: turn the client's integer into a
/// position, page the listing from it, and turn the position that comes back
/// into the next integer. Everything between is `LIST_KEYS`, gate and budget
/// included — `dispatch::execute` refuses this when `listing_enabled` is clear,
/// which is why there is no gate check here.
fn run_scan(
    state: &ServerState,
    scan: &vash_proto::resp::command::Scan<'_>,
    version: Version,
    out: &mut Vec<u8>,
) -> Answered {
    // Every value this server stores is a string, so any other type genuinely
    // has no keys. Answering "complete, nothing matched" is the truth, and it
    // costs no scan.
    if !scan.only_strings {
        return answer_scan(out, crate::scan::START, &[], version);
    }

    // `START` is Redis's "from the beginning" and names no stored position.
    let resumed;
    let cursor: &[u8] = if scan.token == crate::scan::START {
        &[]
    } else {
        resumed = state.scan_cursors.resolve(scan.token).ok_or_else(|| {
            // Never a silent restart from the beginning: that spins a client's
            // pager forever without ever saying why. The same rule
            // `listing::decode` applies to a fabricated VCP cursor.
            Failure::client("scan cursor expired; restart the iteration from 0")
        })?;
        &resumed
    };

    let request = vash_core::ListRequest {
        limit: scan.count,
        cursor,
        pattern: scan.pattern,
    };
    let reply = crate::dispatch::execute(
        state,
        &vash_core::Command::ListKeys(request),
        crate::metrics::Dialect::Resp,
    )
    .map_err(resp_error)?;

    let Reply::Listing(page) = reply else {
        error!(?reply, "SCAN produced a reply it cannot render");
        return Err(Failure::internal());
    };

    // An absent cursor is the listing's whole termination rule, and it maps onto
    // Redis's `0` exactly. A page that stopped on the scan budget still carries
    // one, so an empty page with a live cursor — which Redis permits and clients
    // handle — is what a run of dead or non-matching records produces.
    let next = match &page.cursor {
        Some(position) => state.scan_cursors.issue(position),
        None => crate::scan::START,
    };
    answer_scan(out, next, &page.entries, version)
}

/// `[cursor, [key …]]`, with the cursor as decimal text — which is what every
/// client library expects, and several of them parse as an integer.
fn answer_scan(
    out: &mut Vec<u8>,
    cursor: u64,
    entries: &[vash_core::ListEntry],
    _version: Version,
) -> Answered {
    encode::array(out, 2);
    encode::bulk(out, cursor.to_string().as_bytes());
    encode::array(out, entries.len());
    for entry in entries {
        encode::bulk(out, &entry.name);
    }
    Ok(())
}

/// Renders a reply in this dialect.
///
/// Takes the original command as well, because several of them share a reply
/// shape and differ only in how Redis words the answer: `TTL`, `TYPE` and
/// `EXISTS` are all one deadline query, and `MSET` and `MSETEX` are one batch
/// write.
fn render(command: &Command<'_>, reply: &Reply, version: Version, out: &mut Vec<u8>) -> Answered {
    match (command, reply) {
        (Command::Ping { message }, _) => match message {
            Some(message) => encode::bulk(out, message),
            None => encode::simple(out, "PONG"),
        },

        // The counter list is memcached's `stats` payload, unchanged: one
        // snapshot, and each dialect names the fields it reports. The table that
        // does the naming is `encode::info`'s, and it is a pure function of
        // these pairs — no state, no store.
        (Command::Info { sections }, Reply::Stats(counters)) => {
            encode::info(out, *sections, counters, version)
        }

        (_, Reply::Value(value)) => encode::bulk(out, &value.data),
        (Command::Get { .. }, Reply::NotFound) => encode::null(out, version),

        (_, Reply::Values(values)) => {
            encode::array(out, values.len());
            for value in values {
                match value {
                    Some(value) => encode::bulk(out, &value.data),
                    None => encode::null(out, version),
                }
            }
        }

        (Command::Exists { .. }, Reply::Deadlines(found)) => encode::integer(
            out,
            found.iter().filter(|entry| entry.is_some()).count() as i64,
        ),

        // Redis answers with a simple string in both dialects, and `none` rather
        // than a null for a key that is not there.
        (Command::Type { .. }, Reply::Deadline(deadline)) => {
            encode::simple(out, if deadline.is_some() { "string" } else { "none" })
        }

        (Command::Ttl { .. }, Reply::Deadline(deadline)) => {
            let answer = match deadline {
                None => TTL_MISSING,
                Some(vash_core::NEVER) => TTL_PERSISTENT,
                Some(at) => {
                    // Redis rounds the remaining milliseconds to the nearest
                    // second rather than truncating, so a key set with `EX 10`
                    // still reports 10 a moment later.
                    let remaining = at.saturating_sub(now_ms()) as i64;
                    (remaining + 500) / 1_000
                }
            };
            encode::integer(out, answer);
        }

        (Command::Delete { .. }, Reply::DeletedMany(hits)) => {
            encode::integer(out, hits.iter().filter(|hit| **hit).count() as i64)
        }

        // With `GET` the client is told what was there, and never whether the
        // write applied — that is Redis's contract, not an omission.
        (Command::Set(_), Reply::Swapped { previous, .. }) => match previous {
            Some(value) => encode::bulk(out, &value.data),
            None => encode::null(out, version),
        },
        (Command::Set(_), Reply::Stored(outcome)) => {
            if matches!(outcome, Stored::Stored(_)) {
                encode::ok(out)
            } else {
                encode::null(out, version)
            }
        }

        (Command::MSet { .. }, Reply::StoredMany(_)) => encode::ok(out),
        // The guard's verdict, which is all `MSETEX` reports.
        (Command::MSetEx { .. }, Reply::StoredMany(_)) => encode::integer(out, 1),
        (Command::MSetEx { .. }, Reply::Stored(Stored::NotStored)) => encode::integer(out, 0),

        (Command::Append { .. }, Reply::Length(length)) => encode::integer(out, *length as i64),

        (Command::Expire { .. } | Command::Persist { .. }, Reply::Applied(applied)) => {
            encode::integer(out, i64::from(*applied))
        }

        (Command::Incr { .. }, Reply::Arithmetic(applied)) => match number_of(applied.value)? {
            Number::Int(value) => encode::integer(out, value),
            Number::Float(value) => encode::bulk(out, encode::format_float(value).as_bytes()),
        },

        (Command::IncrEx(_), Reply::Arithmetic(applied)) => {
            return answer_increx(
                out,
                number_of(applied.value)?,
                number_of(applied.applied)?,
                version,
            );
        }

        // A reply shape no command of this dialect can produce is a bug in the
        // translation above, not anything a client did.
        (command, reply) => {
            error!(
                ?command,
                ?reply,
                "a Redis command produced a reply it cannot render"
            );
            return Err(Failure::internal());
        }
    }
    Ok(())
}

/// Maps a classified failure onto the error a Redis client expects.
///
/// The classification itself is `dispatch::to_failed`'s, shared with the other
/// two dialects. What is left here is the wording, which is Redis's own and
/// which client libraries match on — `OOM` above all, since libraries treat it
/// as "back off", exactly right for a full map.
///
/// `cause` is consulted first because Redis draws distinctions the status codes
/// do not: four arithmetic failures share `NotNumeric` and have four different
/// messages.
fn resp_error(failed: crate::dispatch::Failed) -> Failure {
    use vash_proto::vcp::Status;

    let message = match failed.cause {
        Some(CoreError::NotAnInteger) => Some(NOT_AN_INTEGER),
        Some(CoreError::NotAFloat) => Some(NOT_A_FLOAT),
        Some(CoreError::Overflow) => Some(OVERFLOW),
        Some(CoreError::NotFinite) => Some(NOT_FINITE),
        Some(CoreError::EmptyKey | CoreError::KeyTooLong { .. }) => Some(INVALID_KEY),
        Some(CoreError::ValueTooLarge { .. }) => Some("string exceeds maximum allowed size"),
        _ => None,
    };
    if let Some(message) = message {
        return Failure::executed(message);
    }

    match failed.status {
        Status::CapacityFull => Failure {
            code: "OOM",
            message: "command not allowed when used memory > 'maxmemory'",
            class: ErrorClass::Capacity,
            counted: true,
        },
        Status::Overloaded => Failure {
            code: "ERR",
            message: "server is overloaded, try again",
            class: ErrorClass::Overloaded,
            counted: true,
        },
        Status::Unsupported => Failure::executed("unsupported operation"),
        Status::Unauthorized => Failure::executed("command disabled by configuration"),
        Status::Internal => Failure {
            code: "ERR",
            message: "internal error",
            class: ErrorClass::Internal,
            counted: true,
        },
        _ => Failure::executed("invalid argument"),
    }
}

/// Translates a Redis command into the shared boundary type.
///
/// Only the commands that touch storage appear here; the connection ones are
/// answered before this is reached. Several collapse onto one variant —
/// `EXISTS`, `TYPE` and `TTL` are all deadline queries differing in how they
/// render — which is the point of the boundary being about *storage operations*
/// rather than about command names.
fn translate<'a>(command: &Command<'a>) -> Result<vash_core::Command<'a>, Failure> {
    use vash_core::Command as Domain;

    Ok(match command {
        Command::Ping { .. } => Domain::Ping,
        Command::Get { key } => Domain::Get { key: key_of(key)? },
        Command::MGet { keys } => Domain::GetMany(keys_of(keys.iter().copied())?),

        // Redis counts a key once per time it is named, so duplicates are not
        // folded together.
        Command::Exists { keys } => Domain::Deadlines(keys_of(keys.iter().copied())?),
        Command::Type { key } | Command::Ttl { key } => Domain::Deadline { key: key_of(key)? },

        Command::Delete { keys } => Domain::DeleteMany(keys_of(keys.iter().copied())?),

        Command::Set(set) => Domain::Set(vash_core::Set {
            key: key_of(set.key)?,
            value: set.value,
            ttl: ttl_change_for(set.expiry, now_ms() as i64, INVALID_EXPIRE_SET)?,
            mc_flags: 0,
            tags: Vec::new(),
            mode: match set.condition {
                Condition::Always => SetMode::Set,
                Condition::IfAbsent => SetMode::Add,
                Condition::IfPresent => SetMode::Replace,
            },
            return_previous: set.return_previous,
        }),

        Command::MSet { pairs } => Domain::SetMany {
            sets: pairs
                .iter()
                .map(|(key, value)| Ok(vash_core::Set::plain(key_of(key)?, value, 0)))
                .collect::<Result<_, Failure>>()?,
            guard: vash_core::BatchGuard::Always,
        },

        Command::MSetEx {
            pairs,
            condition,
            expiry,
        } => {
            // Resolved once for the whole batch, so every key in it is stamped
            // against one instant.
            let ttl = ttl_change_for(*expiry, now_ms() as i64, INVALID_EXPIRE_MSETEX)?;
            Domain::SetMany {
                sets: pairs
                    .iter()
                    .map(|(key, value)| Ok(vash_core::Set::with_ttl(key_of(key)?, value, ttl)))
                    .collect::<Result<_, Failure>>()?,
                guard: match condition {
                    Condition::Always => vash_core::BatchGuard::Always,
                    Condition::IfAbsent => vash_core::BatchGuard::IfAllAbsent,
                    Condition::IfPresent => vash_core::BatchGuard::IfAllPresent,
                },
            }
        }

        Command::Append { key, value } => Domain::Append {
            key: key_of(key)?,
            suffix: value,
        },

        Command::Expire {
            key,
            expiry,
            condition,
        } => {
            let now = now_ms() as i64;
            let deadline = match expiry {
                Expiry::After(millis) => now.saturating_add(*millis),
                Expiry::At(millis) => *millis,
                // The parser only produces the two relative forms for `EXPIRE`.
                Expiry::Unset | Expiry::Keep | Expiry::Persist => now,
            };
            Domain::Expire {
                key: key_of(key)?,
                ttl: vash_core::TtlChange::Set(ttl_from_deadline(deadline, now)),
                guard: match condition {
                    ExpireCondition::Always => vash_core::ExpireGuard::Always,
                    ExpireCondition::IfPersistent => vash_core::ExpireGuard::IfPersistent,
                    ExpireCondition::IfVolatile => vash_core::ExpireGuard::IfVolatile,
                    ExpireCondition::IfLater => vash_core::ExpireGuard::IfLater,
                    ExpireCondition::IfEarlier => vash_core::ExpireGuard::IfEarlier,
                },
            }
        }

        // `IfVolatile` is what makes this answer 0 for a key with no deadline to
        // clear, which is Redis's contract.
        Command::Persist { key } => Domain::Expire {
            key: key_of(key)?,
            ttl: vash_core::TtlChange::Set(0),
            guard: vash_core::ExpireGuard::IfVolatile,
        },

        Command::Incr { key, delta } => {
            Domain::Arithmetic(vash_core::Arithmetic::redis(key_of(key)?, delta_of(*delta)))
        }

        Command::IncrEx(op) => Domain::Arithmetic(translate_increx(op)?),

        // `INFO` and memcached's `stats` are one question about the server, so
        // they are one domain command; only the rendering differs.
        Command::Info { .. } => Domain::Stats,

        // Answered before translation is reached — `Scan` because its cursor
        // has to be resolved against server state first, the rest because they
        // are properties of this socket rather than operations on the cache.
        Command::Quit | Command::Auth(_) | Command::Hello { .. } | Command::Scan(_) => {
            error!("a connection command reached the storage boundary");
            return Err(Failure::internal());
        }
    })
}

/// `TTL` on a key that is not there.
const TTL_MISSING: i64 = -2;
/// `TTL` on a key with no expiry.
const TTL_PERSISTENT: i64 = -1;

/// Translates a Redis expiry option into the store's lifetime vocabulary.
///
/// `KEEPTTL` becomes [`TtlChange::Keep`] rather than a deadline read out and
/// written back, which is what removes the read and the race with it. The
/// absolute forms still resolve here, because only this side knows Redis
/// measures them in milliseconds.
///
/// [`TtlChange`]: vash_core::TtlChange
fn ttl_change_for(
    expiry: Expiry,
    now: i64,
    invalid_expire: &'static str,
) -> Result<vash_core::TtlChange, Failure> {
    use vash_core::TtlChange;
    Ok(match expiry {
        // Redis discards any existing TTL on a plain `SET`.
        Expiry::Unset | Expiry::Persist => TtlChange::Set(0),
        Expiry::Keep => TtlChange::Keep,
        // Checked, not saturating: `PX 9223372036854775807` overflows this, and
        // Redis refuses a deadline it cannot represent rather than silently
        // storing a different one.
        Expiry::After(millis) => TtlChange::Set(ttl_from_deadline(
            now.checked_add(millis)
                .ok_or_else(|| Failure::client(invalid_expire))?,
            now,
        )),
        Expiry::At(millis) => TtlChange::Set(ttl_from_deadline(millis, now)),
    })
}

/// Converts a parsed Redis delta into the domain's arithmetic.
///
/// The unbounded commands pass the limits of their own type, which is what turns
/// "overflowed" and "out of bounds" into one condition with one handler.
fn delta_of(delta: Number) -> vash_core::Delta {
    match delta {
        Number::Int(value) => vash_core::Delta::int(value),
        Number::Float(value) => vash_core::Delta::float(value),
    }
}

/// Reads a result back into the domain the reply is rendered from.
fn number_of(value: vash_core::Number) -> Result<Number, Failure> {
    match value {
        vash_core::Number::Int(value) => Ok(Number::Int(value)),
        vash_core::Number::Float(value) => Ok(Number::Float(value)),
        // Only memcached's counter operation produces this, and no Redis command
        // builds one.
        vash_core::Number::Counter(value) => {
            error!(value, "a Redis command produced a memcached counter");
            Err(Failure::internal())
        }
    }
}

/// `INCREX` answers with the new value and the increment actually applied, so
/// a caller learns from one round trip both where the counter stands and
/// whether it moved.
fn answer_increx(out: &mut Vec<u8>, value: Number, applied: Number, version: Version) -> Answered {
    encode::array(out, 2);
    for number in [value, applied] {
        match number {
            Number::Int(value) => encode::integer(out, value),
            Number::Float(value) => encode::double(out, value, version),
        }
    }
    Ok(())
}

// `write_number`, `read_int` and `read_float` lived here and are gone. Reading a
// stored counter and rendering the result back to text are now
// `vash_core::arith`'s, because they have to happen where the arithmetic does —
// inside the writer's transaction — for the update not to be lost.

/// `INCREX`: arithmetic, bounds and an expiry in one operation.
///
/// Everything about *how the number moves* — the bounds, the saturation, which
/// bound an overflow clamps to — belongs to `vash_core::arith` and is evaluated
/// inside the writer's transaction. What is left here is the translation from
/// Redis's option set into that vocabulary.
fn translate_increx<'a>(op: &IncrEx<'a>) -> Result<vash_core::Arithmetic<'a>, Failure> {
    let delta = match op.delta {
        Number::Int(delta) => vash_core::Delta::Int {
            delta,
            lower: bound_int(op.lower, i64::MIN)?,
            upper: bound_int(op.upper, i64::MAX)?,
        },
        Number::Float(delta) => vash_core::Delta::Float {
            delta,
            lower: bound_float(op.lower, -f64::MAX),
            upper: bound_float(op.upper, f64::MAX),
        },
    };

    // No deadline is read to decide this: `Keep` preserves the record's lifetime
    // by not touching it, and `SetIfPersistent` is `ENX` decided at the record.
    let ttl = match op.expiry {
        // No expiry option: the lifetime is left alone.
        None => vash_core::TtlChange::Keep,
        // Ahead of the `ENX` arm below, because `PERSIST` applies either way.
        Some(Expiry::Persist) => vash_core::TtlChange::Set(0),
        // Neither is an `INCREX` option, so the parser cannot produce them; both
        // would mean "leave the lifetime alone" if one ever arrived.
        Some(Expiry::Unset | Expiry::Keep) => vash_core::TtlChange::Keep,
        Some(expiry) => {
            let vash_core::TtlChange::Set(ttl_secs) =
                ttl_change_for(expiry, now_ms() as i64, INVALID_EXPIRE_INCREX)?
            else {
                // `Keep` is handled above, so what remains always resolves to a
                // deadline.
                return Err(Failure::internal());
            };
            if op.only_if_persistent {
                vash_core::TtlChange::SetIfPersistent(ttl_secs)
            } else {
                vash_core::TtlChange::Set(ttl_secs)
            }
        }
    };

    Ok(vash_core::Arithmetic {
        key: key_of(op.key)?,
        delta,
        // Out of bounds without `SATURATE` skips the write and reports a zero
        // increment, rather than failing as the unbounded commands do.
        on_bound: if op.saturate {
            vash_core::OnBound::Clamp
        } else {
            vash_core::OnBound::Skip
        },
        missing: vash_core::Missing::CreateAtZero,
        ttl,
    })
}

fn bound_int(bound: Option<Number>, default: i64) -> Result<i64, Failure> {
    match bound {
        None => Ok(default),
        Some(Number::Int(value)) => Ok(value),
        // The parser reads bounds in the increment's mode, so this cannot
        // happen; refusing beats silently truncating a float bound.
        Some(Number::Float(_)) => Err(Failure::client(NOT_AN_INTEGER)),
    }
}

fn bound_float(bound: Option<Number>, default: f64) -> f64 {
    match bound {
        None => default,
        Some(Number::Int(value)) => value as f64,
        Some(Number::Float(value)) => value,
    }
}

// `answer_bool`, `keep_ttl` and `ttl_for` lived here and are gone. The first
// was a one-line encoder the reply renderer now covers; the other two existed to
// reconstruct a deadline that the store keeps by not touching it.

fn key_of(bytes: &[u8]) -> Result<Key<'_>, Failure> {
    Key::new(bytes).map_err(Failure::from)
}

fn keys_of<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Result<Vec<Key<'a>>, Failure> {
    keys.into_iter().map(key_of).collect()
}

fn now_ms() -> u64 {
    vash_core::Clock::new().now_ms()
}

/// Converts an absolute deadline in unix milliseconds into the `ttl_secs` the
/// store's commands take.
///
/// That field is overloaded exactly as memcached overloads `exptime` — zero is
/// never, `u32::MAX` is already expired, and anything past 30 days is an
/// absolute unix *second* — so a Redis deadline goes in as the absolute form
/// and never round-trips through "seconds from now", which would drift by up to
/// a second on every `KEEPTTL`.
///
/// Sub-second precision is the one thing lost: that field is whole seconds, so
/// every deadline is rounded to one. To the **nearest** second, which is what
/// makes `SET k v EX 60` followed by `TTL k` answer 60 rather than 61 — and
/// then pushed forward if rounding landed at or before now, because a key that
/// vanishes the instant it is written is a worse failure than one that lingers
/// for under a second. `PX 100` therefore buys up to a full second.
///
/// `now` is a parameter rather than read here, and that is load-bearing rather
/// than tidiness: this used to read the clock itself while its callers had
/// already read one to compute the deadline they passed in. Two reads that
/// straddle a millisecond make the deadline and the "is it in the past" test
/// disagree by one, which turns a `PX 1` into an already-expired key about once
/// in a thousand. One instant per command, threaded.
fn ttl_from_deadline(deadline_ms: i64, now: i64) -> u32 {
    if deadline_ms <= now {
        return vash_core::clock::TTL_ALREADY_EXPIRED;
    }

    let mut seconds = deadline_ms.saturating_add(500) / 1_000;
    if seconds.saturating_mul(1_000) <= now {
        seconds = now / 1_000 + 1;
    }

    if seconds <= vash_core::MAX_TTL_SECS as i64 {
        // A stamp inside the first 30 days of 1970 would be read back as a
        // relative offset. It cannot be in the future, and the branch above
        // has already caught that, but the conversion has to be total.
        return vash_core::clock::TTL_ALREADY_EXPIRED;
    }

    // `u32::MAX` is the already-expired sentinel, so the furthest deadline this
    // can express is one second short of it — some time in 2106.
    u32::try_from(seconds).unwrap_or(u32::MAX).min(u32::MAX - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vash_proto::resp::command::{Credential, Set as RespSet};

    /// Every Redis command that touches storage, in one place.
    ///
    /// Deliberately a `Vec` of concrete commands rather than a match on the
    /// enum: what is being checked is that the *shortcut* agrees with the
    /// *translation*, and a match would let a new variant be added to both
    /// without ever being compared.
    fn storage_commands() -> Vec<Command<'static>> {
        let set = |condition, expiry, return_previous| {
            Command::Set(RespSet {
                key: b"k",
                value: b"v",
                condition,
                expiry,
                return_previous,
            })
        };
        vec![
            Command::Ping { message: None },
            Command::Get { key: b"k" },
            Command::MGet { keys: vec![b"k"] },
            Command::Exists { keys: vec![b"k"] },
            Command::Type { key: b"k" },
            Command::Ttl { key: b"k" },
            Command::Delete { keys: vec![b"k"] },
            set(Condition::Always, Expiry::Unset, false),
            set(Condition::IfAbsent, Expiry::Keep, true),
            Command::MSet {
                pairs: vec![(b"k".as_slice(), b"v".as_slice())],
            },
            Command::MSetEx {
                pairs: vec![(b"k".as_slice(), b"v".as_slice())],
                condition: Condition::IfAbsent,
                expiry: Expiry::After(1000),
            },
            Command::Append {
                key: b"k",
                value: b"v",
            },
            Command::Expire {
                key: b"k",
                expiry: Expiry::After(1000),
                condition: ExpireCondition::IfLater,
            },
            Command::Persist { key: b"k" },
            Command::Incr {
                key: b"k",
                delta: Number::Int(1),
            },
            Command::IncrEx(IncrEx {
                key: b"k",
                delta: Number::Int(1),
                lower: None,
                upper: None,
                saturate: false,
                expiry: None,
                only_if_persistent: false,
            }),
        ]
    }

    /// The shortcut this module uses to decide where a command may run must
    /// agree with the domain's answer for the command it translates into.
    ///
    /// Two judgements about one fact is the shape that drifts, and the cost of
    /// drift here is a write executed on a runtime worker, blocking it and every
    /// connection it serves. See `vash-proto/tests/read_only.rs` for the same
    /// check on the VCP side.
    #[test]
    fn the_shortcut_agrees_with_the_domain() {
        for command in storage_commands() {
            let translated = translate(&command).expect("a storage command translates");
            assert_eq!(
                inline_safe(&command),
                translated.inline_safe(),
                "{command:?} is classified differently by the shortcut and by the domain"
            );
        }
    }

    /// The connection commands never reach the boundary, so there is nothing to
    /// compare them against — but they must all be inline-safe, because none of
    /// them touches storage and refusing them a worker would cost a thread hop
    /// on every handshake.
    #[test]
    fn the_connection_commands_are_inline_safe() {
        let credential = Credential {
            name: None,
            secret: b"s",
        };
        for command in [
            Command::Quit,
            Command::Auth(credential),
            Command::Hello {
                version: Some(3),
                auth: None,
            },
        ] {
            assert!(inline_safe(&command), "{command:?} must be inline-safe");
            assert!(
                translate(&command).is_err(),
                "{command:?} must not reach the storage boundary"
            );
        }
    }
}
