//! Executing Redis commands.
//!
//! Kept apart from [`crate::dispatch`] because it works differently on purpose.
//! The VCP and memcached adapters decode into `vash_core::Command`, one command
//! to one storage operation. Some of Redis's string commands do not fit that
//! shape: `TTL` reports a lifetime nothing else asks for, and `HELLO` negotiates
//! a dialect only this protocol has. Those are composed here from the operations
//! the `Store` trait already offers.
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
use vash_core::{CoreError, Key, SetMode, Stored};
use vash_proto::resp::command::{Condition, ExpireCondition, Expiry, IncrEx, Number};
use vash_proto::resp::{Command, ErrorReply, Outcome, ProtocolError, Version, encode};
use vash_store::{Store, StoreError};

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
pub fn is_read_only(command: &Command<'_>) -> bool {
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
    )
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
            state.metrics.other();
            state.metrics.error(failure.class);
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
    if conn.is_authenticated() || !state.auth.current().required() {
        return None;
    }

    match command {
        Command::Auth(_) | Command::Quit => None,
        Command::Hello { auth: Some(_), .. } => None,
        Command::Hello { auth: None, .. } => Some(Failure {
            code: "NOAUTH",
            message: HELLO_UNAUTHENTICATED,
            class: ErrorClass::Client,
        }),
        _ => Some(Failure {
            code: "NOAUTH",
            message: "Authentication required.",
            class: ErrorClass::Client,
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
            })
        }
    }
}

/// A command that could not be answered.
///
/// Carries the wire error and how it should be counted, so every failure path
/// reports both and neither can be forgotten.
struct Failure {
    code: &'static str,
    message: &'static str,
    class: ErrorClass,
}

impl Failure {
    fn client(message: &'static str) -> Self {
        Self {
            code: "ERR",
            message,
            class: ErrorClass::Client,
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
            },
            // The parser produces the rest; they never reach the executor.
            _ => Failure::client("syntax error"),
        }
    }
}

/// Maps a storage failure onto the error a Redis client expects.
///
/// `OOM` is the one clients actually branch on — it is what Redis answers when
/// `maxmemory` is reached, and libraries treat it as "back off", which is
/// exactly right for a full map.
impl From<StoreError> for Failure {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::CapacityFull | StoreError::TagLimit(_) => Failure {
                code: "OOM",
                message: "command not allowed when used memory > 'maxmemory'",
                class: ErrorClass::Capacity,
            },
            StoreError::Overloaded | StoreError::ShuttingDown => Failure {
                code: "ERR",
                message: "server is overloaded, try again",
                class: ErrorClass::Overloaded,
            },
            StoreError::Core(CoreError::ValueTooLarge { .. }) => {
                Failure::client("string exceeds maximum allowed size")
            }
            StoreError::Core(CoreError::EmptyKey | CoreError::KeyTooLong { .. }) => {
                Failure::client(INVALID_KEY)
            }
            // The arithmetic failures, now that the store performs the
            // arithmetic. Each keeps Redis's own wording, which client libraries
            // match on.
            StoreError::Core(CoreError::NotAnInteger) => Failure::client(NOT_AN_INTEGER),
            StoreError::Core(CoreError::NotAFloat) => Failure::client(NOT_A_FLOAT),
            StoreError::Core(CoreError::Overflow) => Failure::client(OVERFLOW),
            StoreError::Core(CoreError::NotFinite) => Failure::client(NOT_FINITE),
            StoreError::Unsupported(what) => {
                warn!(what, "client used a feature this build does not implement");
                Failure::client("unsupported operation")
            }
            other => {
                error!(error = %other, "storage failure");
                Failure {
                    code: "ERR",
                    message: "internal error",
                    class: ErrorClass::Internal,
                }
            }
        }
    }
}

impl From<CoreError> for Failure {
    fn from(err: CoreError) -> Self {
        StoreError::Core(err).into()
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

        Command::Ping { message } => {
            state.metrics.other();
            match message {
                Some(message) => encode::bulk(out, message),
                None => encode::simple(out, "PONG"),
            }
            Ok(())
        }

        Command::Get { key } => {
            let value = state.store.get(key_of(key)?)?;
            state
                .metrics
                .read(u64::from(value.is_some()), u64::from(value.is_none()));
            match value {
                Some(value) => encode::bulk(out, &value.data),
                None => encode::null(out, *version),
            }
            Ok(())
        }

        Command::MGet { keys } => {
            let keys = keys_of(keys.iter().copied())?;
            let values = state.store.get_many(&keys)?;
            let hits = values.iter().filter(|value| value.is_some()).count() as u64;
            state.metrics.read(hits, values.len() as u64 - hits);

            encode::array(out, values.len());
            for value in &values {
                match value {
                    Some(value) => encode::bulk(out, &value.data),
                    None => encode::null(out, *version),
                }
            }
            Ok(())
        }

        Command::Exists { keys } => {
            // Redis counts a key once per time it is named, so duplicates are
            // not folded together.
            let keys = keys_of(keys.iter().copied())?;
            let found = state.store.deadlines(&keys)?;
            let live = found.iter().filter(|entry| entry.is_some()).count() as u64;
            state.metrics.read(live, found.len() as u64 - live);
            encode::integer(out, live as i64);
            Ok(())
        }

        Command::Type { key } => {
            let live = state.store.deadline(key_of(key)?)?.is_some();
            state.metrics.read(u64::from(live), u64::from(!live));
            // Redis answers with a simple string in both RESP2 and RESP3, and
            // `none` rather than a null for a key that is not there.
            encode::simple(out, if live { "string" } else { "none" });
            Ok(())
        }

        Command::Delete { keys } => {
            let keys = keys_of(keys.iter().copied())?;
            let hits = state.store.delete_many(&keys)?;
            state.metrics.write();
            encode::integer(out, hits.iter().filter(|hit| **hit).count() as i64);
            Ok(())
        }

        Command::Set(set) => execute_set(state, set, *version, out),

        Command::MSet { pairs } => {
            let sets = pairs
                .iter()
                .map(|(key, value)| Ok(vash_core::Set::plain(key_of(key)?, value, 0)))
                .collect::<Result<Vec<_>, Failure>>()?;
            state.store.set_many(&sets)?;
            state.metrics.write();
            encode::ok(out);
            Ok(())
        }

        Command::MSetEx {
            pairs,
            condition,
            expiry,
        } => execute_msetex(state, pairs, *condition, *expiry, out),

        Command::Append { key, value } => {
            // One atomic step in the shard writer, which also means the existing
            // value is concatenated from a slice of the memory map rather than
            // being copied out here and shipped back.
            let length = state.store.append(key_of(key)?, value)?;
            state.metrics.write();
            encode::integer(out, length as i64);
            Ok(())
        }

        Command::Expire {
            key,
            expiry,
            condition,
        } => execute_expire(state, key, *expiry, *condition, out),

        Command::Persist { key } => {
            // `IfVolatile` is what makes this answer 0 for a key that has no
            // deadline to clear, which is Redis's contract — and evaluating it
            // in the writer is what stops a deadline set concurrently from being
            // cleared by a `PERSIST` that had already decided there was none.
            let cleared = state.store.expire(
                key_of(key)?,
                vash_core::TtlChange::Set(0),
                vash_core::ExpireGuard::IfVolatile,
            )?;
            state.metrics.write();
            answer_bool(out, cleared)
        }

        Command::Ttl { key } => {
            let deadline = state.store.deadline(key_of(key)?)?;
            state
                .metrics
                .read(u64::from(deadline.is_some()), u64::from(deadline.is_none()));

            let answer = match deadline {
                None => TTL_MISSING,
                Some(deadline) => match deadline {
                    vash_core::NEVER => TTL_PERSISTENT,
                    at => {
                        // Redis rounds the remaining milliseconds to the
                        // nearest second rather than truncating, so a key set
                        // with `EX 10` still reports 10 a moment later.
                        let remaining = at.saturating_sub(now_ms()) as i64;
                        (remaining + 500) / 1_000
                    }
                },
            };
            encode::integer(out, answer);
            Ok(())
        }

        Command::Incr { key, delta } => execute_incr(state, key, *delta, out),

        Command::IncrEx(op) => execute_increx(state, op, *version, out),
    }
}

/// `TTL` on a key that is not there.
const TTL_MISSING: i64 = -2;
/// `TTL` on a key with no expiry.
const TTL_PERSISTENT: i64 = -1;

fn execute_set(
    state: &ServerState,
    set: &vash_proto::resp::command::Set<'_>,
    version: Version,
    out: &mut Vec<u8>,
) -> Answered {
    // One store call, whatever options were given. `KEEPTTL` and `GET` both used
    // to read first and then write against what they read, which is a race in
    // two directions: another client changing the deadline in between had it
    // overwritten with the older one, and `GET` could report a value this write
    // did not actually displace. Both are now settled inside the writer's
    // transaction, off the same lookup an overwrite performs anyway.
    let written = state.store.store(&vash_core::Set {
        key: key_of(set.key)?,
        value: set.value,
        ttl: ttl_change_for(set.expiry, INVALID_EXPIRE_SET)?,
        mc_flags: 0,
        tags: Vec::new(),
        mode: match set.condition {
            Condition::Always => SetMode::Set,
            Condition::IfAbsent => SetMode::Add,
            Condition::IfPresent => SetMode::Replace,
        },
        return_previous: set.return_previous,
    })?;
    state.metrics.write();

    if set.return_previous {
        // With `GET` the client is told what was there, and never whether the
        // write applied — that is Redis's contract, not an omission.
        match written.previous {
            Some(value) => encode::bulk(out, &value.data),
            None => encode::null(out, version),
        }
    } else if matches!(written.outcome, Stored::Stored(_)) {
        encode::ok(out);
    } else {
        encode::null(out, version);
    }
    Ok(())
}

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
            (now_ms() as i64)
                .checked_add(millis)
                .ok_or_else(|| Failure::client(invalid_expire))?,
        )),
        Expiry::At(millis) => TtlChange::Set(ttl_from_deadline(millis)),
    })
}

fn execute_msetex(
    state: &ServerState,
    pairs: &[(&[u8], &[u8])],
    condition: Condition,
    expiry: Expiry,
    out: &mut Vec<u8>,
) -> Answered {
    let keys = keys_of(pairs.iter().map(|(key, _)| *key))?;

    // Resolved once for the whole batch, so every key in it is stamped against
    // one instant. `KEEPTTL` needs no read at all now — it is carried into the
    // writer as `TtlChange::Keep` and settled per record there.
    let ttl = ttl_change_for(expiry, INVALID_EXPIRE_MSETEX)?;
    let sets: Vec<vash_core::Set<'_>> = keys
        .iter()
        .zip(pairs)
        .map(|(key, (_, value))| vash_core::Set::with_ttl(*key, value, ttl))
        .collect();

    let applied = state.store.set_many_if(
        &sets,
        match condition {
            Condition::Always => vash_core::BatchGuard::Always,
            Condition::IfAbsent => vash_core::BatchGuard::IfAllAbsent,
            Condition::IfPresent => vash_core::BatchGuard::IfAllPresent,
        },
    )?;

    if applied {
        state.metrics.write();
    } else {
        state.metrics.other();
    }
    encode::integer(out, i64::from(applied));
    Ok(())
}

fn execute_expire(
    state: &ServerState,
    key: &[u8],
    expiry: Expiry,
    condition: ExpireCondition,
    out: &mut Vec<u8>,
) -> Answered {
    // The guard travels to the writer rather than being decided here: a
    // `GT`/`LT` comparison judged against a deadline read a moment earlier can
    // be judged against one that has since moved. Deleting an already-past
    // deadline is the store's job too, for the same reason.
    let now = now_ms() as i64;
    let deadline = match expiry {
        Expiry::After(millis) => now.saturating_add(millis),
        Expiry::At(millis) => millis,
        // The parser only produces the two relative forms for `EXPIRE`.
        Expiry::Unset | Expiry::Keep | Expiry::Persist => now,
    };

    let applied = state.store.expire(
        key_of(key)?,
        vash_core::TtlChange::Set(ttl_from_deadline(deadline)),
        match condition {
            ExpireCondition::Always => vash_core::ExpireGuard::Always,
            ExpireCondition::IfPersistent => vash_core::ExpireGuard::IfPersistent,
            ExpireCondition::IfVolatile => vash_core::ExpireGuard::IfVolatile,
            ExpireCondition::IfLater => vash_core::ExpireGuard::IfLater,
            ExpireCondition::IfEarlier => vash_core::ExpireGuard::IfEarlier,
        },
    )?;
    state.metrics.write();
    answer_bool(out, applied)
}

/// `INCR`, `INCRBY`, `DECR`, `DECRBY` and `INCRBYFLOAT`.
///
/// No `Version` argument, unlike `INCREX`: `INCRBYFLOAT` answers with a bulk
/// string in RESP3 as well as RESP2, so nothing here depends on the dialect.
///
/// One store call, and it is a write. There is no read first — the deadline is
/// kept by [`TtlChange::Keep`] rather than by being read out and written back,
/// and the current value never leaves the writer's transaction. `INCR` keeps the
/// key's lifetime because it alters the value in place rather than replacing it,
/// which is the distinction `EXPIRE` documents.
fn execute_incr(state: &ServerState, key: &[u8], delta: Number, out: &mut Vec<u8>) -> Answered {
    let op = vash_core::Arithmetic::redis(key_of(key)?, delta_of(delta));
    let applied = created(state.store.arithmetic(&op)?)?;
    state.metrics.write();

    match number_of(applied.value)? {
        Number::Int(value) => encode::integer(out, value),
        Number::Float(value) => encode::bulk(out, encode::format_float(value).as_bytes()),
    }
    Ok(())
}

/// `INCREX`: arithmetic, bounds and an expiry in one atomic step.
///
/// Everything about *how the number moves* — the bounds, the saturation, which
/// bound an overflow clamps to — now lives in `vash_core::arith`, shared with
/// every other arithmetic command and evaluated inside the writer's transaction.
/// What is left here is the translation from Redis's option set into that
/// vocabulary.
fn execute_increx(
    state: &ServerState,
    op: &IncrEx<'_>,
    version: Version,
    out: &mut Vec<u8>,
) -> Answered {
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

    // No deadline is read to decide this, which is what removes the read that
    // used to precede the write: `Keep` preserves the record's lifetime by not
    // touching it, and `SetIfPersistent` is `ENX` decided at the record.
    let ttl = match op.expiry {
        // No expiry option: the lifetime is left alone.
        None => vash_core::TtlChange::Keep,
        // Ahead of the `ENX` arm below, because `PERSIST` applies either way.
        Some(Expiry::Persist) => vash_core::TtlChange::Set(0),
        // Neither is an `INCREX` option, so the parser cannot produce them;
        // both would mean "leave the lifetime alone" if one ever arrived.
        Some(Expiry::Unset | Expiry::Keep) => vash_core::TtlChange::Keep,
        Some(expiry) => {
            // `Expiry::Keep` is handled above, so no current deadline is needed
            // to resolve what remains.
            let ttl_secs = ttl_for(expiry, now_ms() as i64, None, INVALID_EXPIRE_INCREX)?;
            if op.only_if_persistent {
                vash_core::TtlChange::SetIfPersistent(ttl_secs)
            } else {
                vash_core::TtlChange::Set(ttl_secs)
            }
        }
    };

    let applied = created(state.store.arithmetic(&vash_core::Arithmetic {
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
    })?)?;

    // A skipped `INCREX` left the key and its lifetime exactly as they were, so
    // it is not counted as a write.
    if applied.wrote {
        state.metrics.write();
    } else {
        state.metrics.other();
    }
    answer_increx(
        out,
        number_of(applied.value)?,
        number_of(applied.applied)?,
        version,
    )
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
            Err(Failure {
                code: "ERR",
                message: "internal error",
                class: ErrorClass::Internal,
            })
        }
    }
}

/// Unwraps the outcome of a Redis arithmetic command.
///
/// Every one of them creates the key it did not find, so the miss a memcached
/// counter reports cannot arise here. Answering rather than unwrapping keeps a
/// logic slip off the request path.
fn created(applied: Option<vash_core::Applied>) -> Result<vash_core::Applied, Failure> {
    applied.ok_or_else(|| {
        error!("a Redis arithmetic command reported a miss it cannot produce");
        Failure {
            code: "ERR",
            message: "internal error",
            class: ErrorClass::Internal,
        }
    })
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

fn answer_bool(out: &mut Vec<u8>, applied: bool) -> Answered {
    encode::integer(out, i64::from(applied));
    Ok(())
}

fn key_of(bytes: &[u8]) -> Result<Key<'_>, Failure> {
    Key::new(bytes).map_err(Failure::from)
}

fn keys_of<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Result<Vec<Key<'a>>, Failure> {
    keys.into_iter().map(key_of).collect()
}

fn now_ms() -> u64 {
    vash_core::Clock::new().now_ms()
}

/// The `ttl_secs` that reproduces a key's current deadline, for the commands
/// that must not disturb it.
///
/// Takes the deadline rather than the value: `KEEPTTL` never looks at what is
/// stored, only at when it goes away.
fn keep_ttl(deadline: Option<u64>) -> u32 {
    match deadline.unwrap_or(vash_core::NEVER) {
        vash_core::NEVER => 0,
        at => ttl_from_deadline(at as i64),
    }
}

/// The `ttl_secs` an expiry option asks for, against the key's current
/// deadline.
///
/// One definition for `SET`, `MSETEX` and `INCREX`, which offer the same option
/// set and each used to spell this out again.
///
/// `now` is a parameter rather than read here so that a batch stamps every key
/// in it against one instant, and `invalid_expire` is the command's own name in
/// Redis's wording for the single case this refuses.
fn ttl_for(
    expiry: Expiry,
    now: i64,
    deadline: Option<u64>,
    invalid_expire: &'static str,
) -> Result<u32, Failure> {
    Ok(match expiry {
        // Redis discards any existing TTL on a plain `SET`.
        Expiry::Unset | Expiry::Persist => 0,
        // Checked, not saturating: `PX 9223372036854775807` overflows this, and
        // Redis refuses a deadline it cannot represent rather than silently
        // storing a different one.
        Expiry::After(millis) => ttl_from_deadline(
            now.checked_add(millis)
                .ok_or_else(|| Failure::client(invalid_expire))?,
        ),
        Expiry::At(millis) => ttl_from_deadline(millis),
        Expiry::Keep => keep_ttl(deadline),
    })
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
fn ttl_from_deadline(deadline_ms: i64) -> u32 {
    let now = now_ms() as i64;
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
