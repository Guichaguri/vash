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
//! translation is in `translate` and the wording in `render`.
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
    // Consecutive plain `SET`s cross the writer queue together; everything else
    // flushes the run first. See `dispatch::SetBatch`.
    let mut batch: crate::dispatch::SetBatch<'_, ()> = Default::default();

    let mut rest = block;
    while !rest.is_empty() {
        match vash_proto::resp::parse(rest) {
            Ok(Outcome::Command(parsed)) => {
                let consumed = parsed.consumed;

                if let Some(set) = batchable_set(state, conn, &parsed.command) {
                    if batch.holds(set.key) {
                        flush_sets(state, &mut batch, out);
                    }
                    batch.push(set, ());
                    rest = &rest[consumed..];
                    continue;
                }

                flush_sets(state, &mut batch, out);
                if execute(state, conn, &parsed.command, version, out) == Closing::Yes {
                    return Closing::Yes;
                }
                rest = &rest[consumed..];
            }
            Err(ProtocolError::Recoverable { reply, consumed }) => {
                flush_sets(state, &mut batch, out);
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
    flush_sets(state, &mut batch, out);
    Closing::No
}

/// The roles a Redis command needs, and the name to put in its refusal.
///
/// A third mapping beside the ones on `Command` and `Opcode`, and it is here
/// rather than reached through [`translate`] for two reasons: `SCAN` and
/// `DELBYTAG` never reach translation — they are answered before the storage
/// boundary — and translation allocates for the multi-key writes, which is not
/// work a gate should do on every command. `the_resp_mapping_agrees_with_the_domain`
/// keeps it honest against the authority in `auth::roles_for_command`.
///
/// The name is the command *family*: the parser keeps no verb, so `DECR` is
/// refused as `incr` and `UNLINK` as `del`. Redis names the exact command; this
/// names the one it collapsed onto, which is what the server has.
fn roles_for(command: &Command<'_>) -> (crate::auth::Roles, &'static str) {
    use crate::auth::Roles;
    use vash_proto::resp::command::SlowlogSub;

    match command {
        Command::Hello { .. } => (Roles::NONE, "hello"),
        Command::Ping { .. } => (Roles::NONE, "ping"),
        Command::Quit => (Roles::NONE, "quit"),
        Command::Auth(_) => (Roles::NONE, "auth"),
        // `INFO` is memcached's `stats` in another dialect, and ungated for the
        // same reason: it discloses counters, and client libraries call it.
        Command::Info { .. } => (Roles::NONE, "info"),

        Command::Get { .. } => (Roles::READ, "get"),
        Command::MGet { .. } => (Roles::READ, "mget"),
        Command::Exists { .. } => (Roles::READ, "exists"),
        Command::Type { .. } => (Roles::READ, "type"),
        Command::Ttl { .. } => (Roles::READ, "ttl"),

        // `SET … GET` reports the value it displaced, so it reads.
        Command::Set(set) => (
            if set.return_previous {
                Roles::READ_WRITE
            } else {
                Roles::WRITE
            },
            "set",
        ),
        Command::MSet { .. } => (Roles::WRITE, "mset"),
        Command::MSetEx { .. } => (Roles::WRITE, "msetex"),
        Command::Delete { .. } => (Roles::WRITE, "del"),
        Command::Append { .. } => (Roles::WRITE, "append"),
        Command::Expire { .. } => (Roles::WRITE, "expire"),
        Command::Persist { .. } => (Roles::WRITE, "persist"),

        // Answers with the resulting value, and `INCRBY 0` is a `GET` with
        // extra steps.
        Command::Incr { .. } => (Roles::READ_WRITE, "incr"),
        Command::IncrEx(_) => (Roles::READ_WRITE, "incrby"),

        Command::DelByTag { .. } => (Roles::INVALIDATE, "delbytag"),
        Command::Scan(_) => (Roles::LIST, "scan"),

        // `SLOWLOG` splits on its subcommand, as `SET … GET` splits on its
        // options. `GET` discloses key names where `listing_enabled` permits
        // it, which is `list` by the same reasoning that gates `SCAN`; `RESET`
        // destroys an operator's evidence mid-incident and is fenced with it,
        // so the authority that can read the log is the authority that can
        // clear it. `LEN` is a count and `HELP` is static text, and neither
        // discloses anything `INFO` does not.
        Command::Slowlog(sub) => match sub {
            SlowlogSub::Get(_) | SlowlogSub::Reset => (Roles::LIST, "slowlog"),
            SlowlogSub::Len | SlowlogSub::Help => (Roles::NONE, "slowlog"),
        },
    }
}

/// Whether this command must be refused for want of a role, and what to say.
///
/// Separate from [`refusal`] because the two are different questions with
/// different counters: that one is "has this connection authenticated", this is
/// "may this identity do that". Redis draws the same distinction on the wire
/// with `NOAUTH` against `NOPERM`, so this dialect needs them apart even though
/// both come from the one decision in [`crate::auth::AuthState::refusal`].
fn denial(
    state: &ServerState,
    conn: &ConnAuth,
    command: &Command<'_>,
) -> Option<(crate::auth::Roles, &'static str)> {
    let (needed, name) = roles_for(command);
    match state.auth.refusal(conn, needed)? {
        crate::auth::Refused::Missing(missing) => Some((missing, name)),
        // Answered by `refusal` instead, which knows this dialect's pre-auth
        // exemptions and Redis's wording for them.
        crate::auth::Refused::Unauthenticated => None,
    }
}

/// A plain `SET` this block may hold back, already translated.
///
/// Returns `None` for everything with a verdict to report — `NX`, `XX`, `GET` —
/// and for a connection that has not authenticated, whose `SET` owes a `NOAUTH`
/// that only [`execute`] knows how to render. Translation failures fall through
/// the same way, so a bad expiry is still reported by the ordinary path with the
/// message it has always had.
fn batchable_set<'a>(
    state: &ServerState,
    conn: &ConnAuth,
    command: &Command<'a>,
) -> Option<vash_core::Set<'a>> {
    if !matches!(command, Command::Set(_))
        || refusal(state, conn, command).is_some()
        || denial(state, conn, command).is_some()
    {
        return None;
    }
    let translated = translate(command).ok()?;
    crate::dispatch::batchable(&translated).cloned()
}

/// Submits a held run of `SET`s and writes `+OK` — or the error — for each.
fn flush_sets(
    state: &ServerState,
    batch: &mut crate::dispatch::SetBatch<'_, ()>,
    out: &mut Vec<u8>,
) {
    batch.flush(
        state,
        crate::metrics::Dialect::Resp,
        out,
        |out, (), result| match result {
            Ok(_) => encode::ok(out),
            // Through the same mapping `execute` uses for a storage failure, so
            // a batched `SET` reports a full map exactly as an unbatched one
            // does. Only the wire bytes are taken from it: `SetBatch::flush` has
            // already counted the failure, which is what `Failure::counted`
            // means on this path.
            Err(failed) => {
                let failure = resp_error_ref(failed);
                encode::error(out, failure.code, failure.message);
            }
        },
    );
}

/// Whether a `SET` is one the awaited write path can serve.
///
/// **A conservative structural test, deliberately not a translation.** This
/// runs inside `measure_resp`, once per command, purely to decide which tier a
/// run is offered to — and `translate` reads the clock and resolves an expiry
/// to answer it. That was a `SystemTime::now()` per `SET` spent on a question
/// with a cheaper answer, thrown away, and then asked again by
/// `parse_writes` on the very next line of the same block.
///
/// Everything [`crate::dispatch::awaitable`] needs is legible on the RESP
/// command without translating: an unconditional `SET`, with no `GET` to
/// report and no tags to register. What is *not* checked here is whether the
/// translation would succeed at all — a `SET` whose expiry cannot be
/// represented, or whose key this store will not hold — and it deliberately is
/// not, because being wrong in that direction costs nothing: `parse_writes`
/// still translates, still declines, and the block then takes the ordinary
/// route where `execute` words the error. Misjudging here picks a tier, not
/// an answer.
pub fn batchable_write(command: &Command<'_>) -> bool {
    let Command::Set(set) = command else {
        return false;
    };
    // Mirrors `dispatch::awaitable` — `SetMode::Set`, no `return_previous`, no
    // tags — read off the wire form rather than the domain one.
    // `the_shortcut_agrees_with_the_domain` pins that the two stay in step.
    matches!(set.condition, Condition::Always) && !set.return_previous && set.tags.is_empty()
}

/// Decodes a RESP block that is nothing but plain `SET`s.
///
/// `None` the moment anything else appears — see
/// [`crate::dispatch::parse_memcached_writes`], which this mirrors. RESP has no
/// `noreply`, so every write in the run is answered.
pub(crate) fn parse_writes(block: &[u8]) -> Option<crate::conn::WriteRun<'_>> {
    let mut run = crate::conn::WriteRun {
        sets: Vec::new(),
        suppress: Vec::new(),
    };
    let mut rest = block;
    while !rest.is_empty() {
        let Ok(Outcome::Command(parsed)) = vash_proto::resp::parse(rest) else {
            return None;
        };
        if !matches!(parsed.command, Command::Set(_)) {
            return None;
        }
        let domain = translate(&parsed.command).ok()?;
        let vash_core::Command::Set(set) = domain else {
            return None;
        };
        if !crate::dispatch::awaitable_set(&set) {
            return None;
        }
        run.sets.push(set);
        run.suppress.push(false);
        rest = &rest[parsed.consumed..];
    }
    Some(run)
}

/// Which tier a RESP command belongs to, **decided from its wire form alone**.
///
/// What `conn::measure_resp` asks of every command in a block, and the reason
/// that pass no longer decodes one. The two questions it used to ask —
/// [`inline_safe`] and [`batchable_write`] — both take a `Command`, so asking
/// them meant running `parse_command` over a block that `resp::execute_block`
/// is about to parse again anyway. See `docs/performance-proposals.md` §17.
///
/// **Conservative in one direction only.** A verb this does not recognise, and
/// any `SET` whose option tokens it cannot vouch for, answer [`RunKind::Other`]
/// — which is the tier everything took before runs existed, so the cost of
/// being wrong that way is a hop to the blocking pool. Being wrong the other
/// way would matter, and cannot happen: `Reads` is a closed list of verbs whose
/// every parse is [`inline_safe`], and `Writes` requires a `SET` with none of
/// the three tokens that could disqualify it.
///
/// A malformed command still lands in whichever run its verb names — `GET a b`
/// is measured as a read — and that is harmless: the executor parses it, meets
/// the arity error, and renders it in the position it occupied. It never
/// reaches storage.
///
/// `the_wire_classifier_agrees_with_the_parsed_one` pins this against the two
/// functions it replaces, verb by verb.
pub(crate) fn classify(args: &[&[u8]]) -> crate::conn::RunKind {
    use crate::conn::RunKind;

    let Some(raw_verb) = args.first() else {
        return RunKind::Other;
    };
    let mut buffer = [0u8; vash_proto::resp::command::MAX_VERB_LEN];
    let verb = vash_proto::resp::command::upper_verb(raw_verb, &mut buffer);

    match verb {
        // Exactly the variants `inline_safe` names, by the verbs that produce
        // them. `SCAN` and `DELBYTAG` are absent for the reasons recorded there.
        b"GET" | b"MGET" | b"EXISTS" | b"TYPE" | b"TTL" | b"PING" | b"HELLO" | b"AUTH"
        | b"QUIT" | b"INFO" => RunKind::Reads,

        // `SETTAGS` is deliberately not here: it carries tags, which is the one
        // thing `dispatch::awaitable` refuses beyond `batchable`.
        b"SET" if plainly_batchable(args) => RunKind::Writes,

        _ => RunKind::Other,
    }
}

/// Whether a `SET`'s option tokens leave it unconditional, untagged, and silent
/// about what it displaced — the three things [`batchable_write`] asks of the
/// parsed form.
///
/// **Only the option tokens are scanned.** The value is `args[2]` and is never
/// looked at, so a value that happens to spell `GET` cannot disqualify the write
/// carrying it. Everything that could change any of the three answers is a
/// token in `args[3..]`: `NX` and `XX` set the condition, `GET` asks for the
/// previous value, and tags arrive only under a different verb.
fn plainly_batchable(args: &[&[u8]]) -> bool {
    // `SET key value` at minimum; anything shorter is an arity error, and
    // rendering that is the executor's job.
    args.len() >= 3
        && !args[3..].iter().any(|token| {
            token.eq_ignore_ascii_case(b"NX")
                || token.eq_ignore_ascii_case(b"XX")
                || token.eq_ignore_ascii_case(b"GET")
        })
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
    // `SLOWLOG` is missing for a related reason: `Slowlog::entries` locks every
    // lane and clones what it finds, which is bounded by `slowlog_max_len`
    // times the lanes that have recorded — not by anything small enough to run
    // on a worker that other connections are waiting on.
    //
    // `SCAN` is missing and must stay missing: it is bounded by
    // `listing_max_scan` records, not by anything a runtime worker should be
    // blocked for. `Command::ListKeys` — what it becomes — answers the same.
    //
    // `DELBYTAG` is missing for the ordinary reason: it writes. Neither is
    // covered by `the_shortcut_agrees_with_the_domain`, because neither reaches
    // `translate`, so `the_multi_step_commands_are_not_inline_safe` pins them.
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

    // Authenticated, and the answer is still no. `-NOPERM` rather than VCP's
    // reuse of `UNAUTHORIZED`, because real clients know the code and the
    // alternative — "command disabled by configuration" — sends an operator to
    // the wrong config file.
    if let Some((missing, name)) = denial(state, conn, command) {
        crate::dispatch::deny(state, conn, missing, name);
        state.metrics.other();
        state.metrics.error(ErrorClass::Client);
        encode::error(
            out,
            "NOPERM",
            &format!("this user has no permissions to run the '{name}' command"),
        );
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
const INVALID_EXPIRE_SETTAGS: &str = "invalid expire time in 'settags' command";
const INVALID_EXPIRE_MSETEX: &str = "invalid expire time in 'msetex' command";
const INVALID_EXPIRE_MSETTAGS: &str = "invalid expire time in 'msettags' command";
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

        // Answered entirely from server state: the slowlog is not storage and
        // there is nothing to translate into a domain command.
        Command::Slowlog(sub) => run_slowlog(state, *sub, out),

        // Also more than one step, for a different reason: the boundary
        // invalidates one tag at a time and this command names several.
        Command::DelByTag { tags } => run_del_by_tag(state, tags, out),

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

        // The batch form of the arm above, and it steps around `Reply` for the
        // same reason: `Reply::Values` copies every hit out of the map into a
        // `Value` and allocates a vector to hold them, where this appends each
        // one straight into the reply buffer. The array header has to be written
        // before the first value, which is what makes the request-order
        // guarantee `get_many_with` gives load-bearing rather than convenient.
        Command::MGet { keys } => {
            let keys = keys_of(keys.iter().copied())?;
            // Read out of the `&mut` before the closure borrows `out`, because
            // a miss renders differently in the two RESP versions and the
            // closure needs to know which.
            let negotiated = *version;
            encode::array(out, keys.len());
            crate::dispatch::execute_get_many_into(
                state,
                &keys,
                crate::metrics::CommandKind::GetMany,
                crate::metrics::Dialect::Resp,
                &mut |_, value| match value {
                    Some(value) => encode::bulk(out, value.data),
                    None => encode::null(out, negotiated),
                },
            )
            .map_err(resp_error)?;
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
/// `SLOWLOG GET | LEN | RESET | HELP`.
///
/// # What the shape can and cannot carry
///
/// Redis's entry is **six fields**, and this renders exactly six: a client
/// library indexes them positionally, and `redis-cli slowlog get` is the thing
/// §7 of the proposal said must render unchanged. That fixes two costs worth
/// naming rather than discovering.
///
/// The first is that `queue_us` and `exec_us` — the split this slowlog exists
/// for — have nowhere to go. Field three is one duration, so it carries their
/// sum, which is what the request cost inside the server and is the honest
/// single number. The split is on `GET /slowlog`, and pointing at it is better
/// than inventing a seventh field that a positional client would trip over or
/// smuggling `queue_us=34` into the argument array as if the client had sent it.
///
/// The second is the client address and name, fields five and six, which this
/// server does not record — see `slowlog::Slowlog`. They render empty. That is
/// not a fabricated measurement: an empty client name is what Redis itself
/// returns for every connection that never sent `CLIENT SETNAME`.
#[cfg(feature = "introspect")]
fn run_slowlog(
    state: &ServerState,
    sub: vash_proto::resp::command::SlowlogSub,
    out: &mut Vec<u8>,
) -> Answered {
    use vash_proto::resp::command::SlowlogSub;
    use vash_proto::resp::encode;

    /// Redis answers a bare `SLOWLOG GET` with ten entries.
    const DEFAULT_COUNT: usize = 10;
    /// A ceiling on the request, not on the log. A caller asking for everything
    /// gets this many, which is the same bound `GET /slowlog` applies.
    const MAX_COUNT: usize = 1024;

    let log = &state.introspect.slowlog;
    match sub {
        SlowlogSub::Len => encode::integer(out, log.len() as i64),
        SlowlogSub::Reset => {
            // Entries only. `admitted` and `evicted` are Prometheus counters and
            // walking them backwards would corrupt every rate over the window
            // containing the reset — which is exactly why memcached's `stats
            // reset` is refused. See docs/slowlog-proposal.md §3.6.
            log.reset();
            encode::ok(out);
        }
        SlowlogSub::Help => {
            let lines = [
                "SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
                "GET [<count>]",
                "    Return top <count> entries from the slowlog (default: 10).",
                "    Entries are made of:",
                "    id, timestamp, time in microseconds, arguments array,",
                "    client IP and port, client name.",
                "LEN",
                "    Return the length of the slowlog.",
                "RESET",
                "    Reset the slowlog.",
                "HELP",
                "    Print this help.",
            ];
            encode::array(out, lines.len());
            for line in lines {
                encode::simple(out, line);
            }
        }
        SlowlogSub::Get(count) => {
            let count = match count {
                // Redis reads a negative count as "everything".
                Some(n) if n < 0 => MAX_COUNT,
                Some(n) => (n as usize).min(MAX_COUNT),
                None => DEFAULT_COUNT,
            };
            let entries = log.entries(count);
            encode::array(out, entries.len());
            for entry in &entries {
                encode::array(out, 6);
                encode::integer(out, entry.id as i64);
                encode::integer(out, entry.timestamp as i64);
                // Their sum, because the shape has one slot — see above.
                encode::integer(out, entry.queue_us.saturating_add(entry.exec_us) as i64);

                // The arguments array: what the request named, which is the
                // command family and the key it reached for.
                encode::array(out, 2);
                encode::bulk(out, entry.command.name().as_bytes());
                match state.protocol.listing_enabled {
                    true => encode::bulk(out, &entry.key),
                    // §3.3: the digest until enumeration is permitted. Still
                    // enough to answer "is it the same key every time".
                    false => encode::bulk(out, format!("xxh3:{:016x}", entry.digest).as_bytes()),
                }

                // Not recorded. Empty rather than absent: the field is
                // positional and a client counts on it being there.
                encode::bulk(out, b"");
                encode::bulk(out, b"");
            }
        }
    }
    Ok(())
}

/// Without the feature the log does not exist, so neither does anything to
/// report. Refused rather than answered with an empty array, which would read
/// as "nothing was slow" — the one answer that is never true and never useful.
#[cfg(not(feature = "introspect"))]
fn run_slowlog(
    _state: &ServerState,
    _sub: vash_proto::resp::command::SlowlogSub,
    _out: &mut Vec<u8>,
) -> Answered {
    Err(Failure::client(
        "the slowlog needs a build with --features introspect",
    ))
}

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

/// Answers `DELBYTAG`.
///
/// One `DELETE_BY_TAG` per name, because that is the shape of the operation the
/// boundary and the store have: each is a constant-time generation bump on one
/// tag, independent of the others, and there is no batch form to reach for.
/// Every one is counted and classified by `dispatch::execute` like any other
/// write, so a three-tag command is three invalidations in the metrics —
/// which is what it is.
///
/// **A failure part-way through leaves the earlier tags invalidated**, and the
/// reply is the error rather than a count. That is safe to retry: a generation
/// bump is idempotent, invalidating an already-invalidated tag costs one more
/// bump and changes nothing a client can observe, which is the same property
/// that lets the cluster replay these freely.
fn run_del_by_tag(state: &ServerState, tags: &[&[u8]], out: &mut Vec<u8>) -> Answered {
    let mut registered = 0i64;
    for tag in tags {
        let reply = crate::dispatch::execute(
            state,
            &vash_core::Command::DeleteByTag { tag },
            crate::metrics::Dialect::Resp,
        )
        .map_err(resp_error)?;

        match reply {
            // `false` is a tag this server has never registered: nothing could
            // have carried it, so nothing was invalidated. Counted the way
            // `DEL` counts keys that were not there — silently, as a zero.
            Reply::Invalidated(hit) => registered += i64::from(hit),
            other => {
                error!(?other, "DELBYTAG produced a reply it cannot render");
                return Err(Failure::internal());
            }
        }
    }
    encode::integer(out, registered);
    Ok(())
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
    resp_error_ref(&failed)
}

/// [`resp_error`] without taking ownership, for the batched write path, which
/// renders a shared failure for several commands.
fn resp_error_ref(failed: &crate::dispatch::Failed) -> Failure {
    use vash_proto::vcp::Status;

    let message = match &failed.cause {
        Some(CoreError::NotAnInteger) => Some(NOT_AN_INTEGER),
        Some(CoreError::NotAFloat) => Some(NOT_A_FLOAT),
        Some(CoreError::Overflow) => Some(OVERFLOW),
        Some(CoreError::NotFinite) => Some(NOT_FINITE),
        Some(CoreError::EmptyKey | CoreError::KeyTooLong { .. }) => Some(INVALID_KEY),
        Some(CoreError::ValueTooLarge { .. }) => Some("string exceeds maximum allowed size"),
        // The parser enforces the format's ceiling on a tag list; this is the
        // configured `store.tags.max_per_record`, which is lower and which only
        // the store knows. Named rather than left as `invalid argument`,
        // because the client can act on it — send fewer tags.
        Some(CoreError::TooManyTags { .. }) => Some("too many tags"),
        // Unreachable from this dialect, whose parser rejects both before the
        // command is built. Mapped anyway so a future caller that skips the
        // parser cannot land on `invalid argument` for a named condition.
        Some(CoreError::EmptyTag | CoreError::TagTooLong { .. }) => Some("invalid tag"),
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
        Command::MGet { keys } => Domain::GetMany(keys_of(keys.iter().copied())?.into()),

        // Redis counts a key once per time it is named, so duplicates are not
        // folded together.
        Command::Exists { keys } => Domain::Deadlines(keys_of(keys.iter().copied())?),
        Command::Type { key } | Command::Ttl { key } => Domain::Deadline { key: key_of(key)? },

        Command::Delete { keys } => Domain::DeleteMany(keys_of(keys.iter().copied())?),

        Command::Set(set) => Domain::Set(vash_core::Set {
            key: key_of(set.key)?,
            value: set.value,
            ttl: ttl_change_for(
                set.expiry,
                now_ms() as i64,
                if set.tagged {
                    INVALID_EXPIRE_SETTAGS
                } else {
                    INVALID_EXPIRE_SET
                },
            )?,
            mc_flags: 0,
            // Empty for `SET`, where cloning allocates nothing.
            tags: set.tags.clone(),
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
            tags,
            tagged,
        } => {
            // Resolved once for the whole batch, so every key in it is stamped
            // against one instant.
            let ttl = ttl_change_for(
                *expiry,
                now_ms() as i64,
                if *tagged {
                    INVALID_EXPIRE_MSETTAGS
                } else {
                    INVALID_EXPIRE_MSETEX
                },
            )?;
            Domain::SetMany {
                sets: pairs
                    .iter()
                    .map(|(key, value)| {
                        let mut set = vash_core::Set::with_ttl(key_of(key)?, value, ttl);
                        // One list, copied per record: the store attaches tags
                        // to each record separately, and the batch's list is
                        // what every record in it carries.
                        set.tags.clone_from(tags);
                        Ok(set)
                    })
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
        // has to be resolved against server state first, `DelByTag` because one
        // command carries several invalidations and the boundary takes one at a
        // time, the rest because they are properties of this socket rather than
        // operations on the cache.
        Command::Quit
        | Command::Auth(_)
        | Command::Hello { .. }
        | Command::Scan(_)
        | Command::Slowlog(_)
        | Command::DelByTag { .. } => {
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

    /// The third mapping, against the authority in `auth::roles_for_command`.
    ///
    /// RESP is gated on its own command enum rather than on the domain one,
    /// because `SCAN` and `DELBYTAG` never reach [`translate`] and translation
    /// allocates for the multi-key writes — which is not work a gate should do
    /// on every command. This is what stops the two drifting, and it reuses
    /// [`storage_commands`] so a command added there is compared here too.
    #[test]
    fn the_resp_mapping_agrees_with_the_domain() {
        for command in storage_commands() {
            let (needed, name) = roles_for(&command);
            let domain = translate(&command).expect("a storage command translates");
            assert_eq!(
                needed,
                crate::auth::roles_for_command(&domain),
                "`{name}` and the domain command it becomes disagree"
            );
        }
    }

    /// `SET … GET` is the only command whose roles depend on its arguments: the
    /// flag exists to report the value it displaced, so it reads as well.
    #[test]
    fn set_with_get_is_the_one_command_whose_roles_depend_on_its_arguments() {
        use crate::auth::Roles;

        let set = |return_previous| {
            Command::Set(RespSet {
                key: b"k",
                value: b"v",
                condition: Condition::Always,
                expiry: Expiry::Unset,
                return_previous,
                tagged: false,
                tags: vec![],
            })
        };

        assert_eq!(roles_for(&set(false)).0, Roles::WRITE);
        assert_eq!(roles_for(&set(true)).0, Roles::READ_WRITE);
    }

    /// The two that never reach [`translate`], pinned by hand for that reason:
    /// they are answered before the storage boundary, so the agreement test
    /// cannot cover them.
    #[test]
    fn the_commands_that_bypass_translation_are_still_classified() {
        use crate::auth::Roles;

        assert_eq!(
            roles_for(&Command::DelByTag { tags: vec![b"t"] }).0,
            Roles::INVALIDATE
        );
        assert_eq!(
            roles_for(&Command::Scan(vash_proto::resp::command::Scan {
                token: 0,
                pattern: b"",
                count: 10,
                only_strings: false,
            }))
            .0,
            Roles::LIST
        );

        // `SLOWLOG` is the one command here whose authority depends on its
        // subcommand, so each of the four is pinned rather than a sample.
        use vash_proto::resp::command::SlowlogSub;
        assert_eq!(
            roles_for(&Command::Slowlog(SlowlogSub::Get(None))).0,
            Roles::LIST,
            "GET discloses key names where listing_enabled permits it"
        );
        assert_eq!(
            roles_for(&Command::Slowlog(SlowlogSub::Reset)).0,
            Roles::LIST,
            "RESET destroys an operator's evidence and is fenced with GET"
        );
        assert_eq!(roles_for(&Command::Slowlog(SlowlogSub::Len)).0, Roles::NONE);
        assert_eq!(
            roles_for(&Command::Slowlog(SlowlogSub::Help)).0,
            Roles::NONE
        );
    }

    /// Every Redis command that touches storage, in one place.
    ///
    /// Deliberately a `Vec` of concrete commands rather than a match on the
    /// enum: what is being checked is that the *shortcut* agrees with the
    /// *translation*, and a match would let a new variant be added to both
    /// without ever being compared.
    fn storage_commands() -> Vec<Command<'static>> {
        let set = |condition, expiry, return_previous, tags: Vec<&'static [u8]>| {
            Command::Set(RespSet {
                key: b"k",
                value: b"v",
                condition,
                expiry,
                return_previous,
                tagged: !tags.is_empty(),
                tags,
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
            set(Condition::Always, Expiry::Unset, false, vec![]),
            set(Condition::IfAbsent, Expiry::Keep, true, vec![]),
            // `SETTAGS`, which is the same write carrying a tag list.
            set(Condition::Always, Expiry::After(1000), false, vec![b"news"]),
            Command::MSet {
                pairs: vec![(b"k".as_slice(), b"v".as_slice())],
            },
            Command::MSetEx {
                pairs: vec![(b"k".as_slice(), b"v".as_slice())],
                condition: Condition::IfAbsent,
                expiry: Expiry::After(1000),
                tags: vec![],
                tagged: false,
            },
            Command::MSetEx {
                pairs: vec![(b"k".as_slice(), b"v".as_slice())],
                condition: Condition::Always,
                expiry: Expiry::Keep,
                tags: vec![b"news"],
                tagged: true,
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

    /// **The awaited-write shortcut agrees with the domain it stands in for.**
    ///
    /// `batchable_write` reads an unconditional, untagged, non-`GET` `SET`
    /// straight off the wire form rather than translating one — that is the
    /// point of it, since it runs per command inside `measure_resp` and
    /// translating there costs a clock read. This is what keeps the cheap
    /// answer and the real one from drifting: for every storage command that
    /// translates, the shortcut must say exactly what `dispatch::awaitable`
    /// says about the translation.
    ///
    /// A command that does *not* translate is skipped rather than asserted
    /// about, because there the shortcut is allowed to be optimistic: it only
    /// decides which tier is tried first, and `parse_writes` declines for real.
    #[test]
    fn the_awaited_write_shortcut_agrees_with_the_domain() {
        let mut saw_batchable = false;
        for command in storage_commands() {
            let Ok(translated) = translate(&command) else {
                continue;
            };
            let domain = crate::dispatch::awaitable(&translated);
            saw_batchable |= domain;
            assert_eq!(
                batchable_write(&command),
                domain,
                "{command:?} is classified differently by the shortcut and by the domain"
            );
        }
        assert!(
            saw_batchable,
            "the fixture no longer contains a plain SET, so this proves nothing"
        );
    }

    /// Renders tokens as a RESP multibulk command, so the wire classifier can be
    /// asked the same question the connection asks it.
    fn wire(tokens: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", tokens.len()).into_bytes();
        for token in tokens {
            out.extend_from_slice(format!("${}\r\n", token.len()).as_bytes());
            out.extend_from_slice(token);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    /// **The wire classifier agrees with the parsed one, verb by verb.**
    ///
    /// `classify` decides a command's tier from its verb and, for `SET`, its
    /// option tokens — never from a decoded `Command`. That is what lets
    /// `measure_resp` walk a block without parsing it (§17). The judgement it
    /// replaces is the one written out longhand below, and this is what keeps
    /// the two from drifting: for every command the dialect recognises, the
    /// cheap answer and the real one must match exactly.
    ///
    /// Exactly, not conservatively. A `classify` that fell back to `Other` too
    /// eagerly would cost a hop to the blocking pool and no test would notice,
    /// which is precisely the kind of silent regression a §17 is written to
    /// avoid.
    #[test]
    fn the_wire_classifier_agrees_with_the_parsed_one() {
        use crate::conn::RunKind;

        // What `measure_resp` used to do, kept here as the reference answer.
        fn parsed_kind(input: &[u8]) -> RunKind {
            match vash_proto::resp::parse(input) {
                Ok(Outcome::Command(parsed)) => {
                    if inline_safe(&parsed.command) {
                        RunKind::Reads
                    } else if batchable_write(&parsed.command) {
                        RunKind::Writes
                    } else {
                        RunKind::Other
                    }
                }
                _ => RunKind::Other,
            }
        }

        let commands: &[&[&[u8]]] = &[
            // One well-formed example of every verb the dialect recognises.
            &[b"HELLO", b"3"],
            &[b"AUTH", b"secret"],
            &[b"PING"],
            &[b"QUIT"],
            &[b"GET", b"k"],
            &[b"SET", b"k", b"v"],
            &[b"SETTAGS", b"k", b"v", b"TAGS", b"1", b"news"],
            &[b"DEL", b"k"],
            &[b"UNLINK", b"k"],
            &[b"MGET", b"a", b"b"],
            &[b"MSET", b"a", b"1", b"b", b"2"],
            &[b"MSETEX", b"300", b"a", b"1"],
            &[b"MSETTAGS", b"300", b"TAGS", b"1", b"news", b"a", b"1"],
            &[b"DELBYTAG", b"news"],
            &[b"EXISTS", b"k"],
            &[b"TYPE", b"k"],
            &[b"APPEND", b"k", b"v"],
            &[b"EXPIRE", b"k", b"60"],
            &[b"EXPIREAT", b"k", b"99999999999"],
            &[b"PERSIST", b"k"],
            &[b"TTL", b"k"],
            &[b"INCR", b"k"],
            &[b"DECR", b"k"],
            &[b"INCRBY", b"k", b"2"],
            &[b"DECRBY", b"k", b"2"],
            &[b"INCRBYFLOAT", b"k", b"1.5"],
            &[b"SCAN", b"0"],
            &[b"INFO"],
            // The `SET` options, which are the whole of what `plainly_batchable`
            // has to get right.
            &[b"SET", b"k", b"v", b"EX", b"300"],
            &[b"SET", b"k", b"v", b"PX", b"300"],
            &[b"SET", b"k", b"v", b"KEEPTTL"],
            &[b"SET", b"k", b"v", b"NX"],
            &[b"SET", b"k", b"v", b"XX"],
            &[b"SET", b"k", b"v", b"GET"],
            &[b"SET", b"k", b"v", b"EX", b"300", b"XX", b"GET"],
            // Lower and mixed case: the wire form a client actually sends.
            &[b"get", b"k"],
            &[b"set", b"k", b"v"],
            &[b"Set", b"k", b"v", b"nx"],
            // A value that spells an option must not disqualify its own write.
            &[b"SET", b"k", b"GET"],
            &[b"SET", b"k", b"NX"],
            // Not ours.
            &[b"LPUSH", b"k", b"v"],
            &[b"AVERYLONGVERBINDEEDXX", b"k"],
        ];

        for tokens in commands {
            let input = wire(tokens);
            let Ok(vash_proto::resp::Framed::Command { classified, .. }) =
                vash_proto::resp::peek(&input, classify)
            else {
                panic!("the framing walk did not settle {tokens:?}")
            };
            assert_eq!(
                classified,
                parsed_kind(&input),
                "the wire classifier and the parsed one disagree about {tokens:?}"
            );
        }
    }

    /// A malformed command is measured by its verb, so it can land in a read run
    /// — and that must stay harmless. It is: the executor parses it, meets the
    /// arity error and renders it in place, without reaching storage.
    ///
    /// Asserted rather than assumed, because "a read run may contain something
    /// that is not a read" is the sort of claim that reads as a bug later.
    #[test]
    fn a_malformed_read_stays_in_the_read_run() {
        use crate::conn::RunKind;

        for tokens in [
            [b"GET".as_slice(), b"a", b"b"].as_slice(),
            [b"GET".as_slice()].as_slice(),
            [b"MGET".as_slice()].as_slice(),
        ] {
            let input = wire(tokens);
            let Ok(vash_proto::resp::Framed::Command { classified, .. }) =
                vash_proto::resp::peek(&input, classify)
            else {
                panic!("the framing walk did not settle {tokens:?}")
            };
            assert_eq!(classified, RunKind::Reads, "{tokens:?}");
            assert!(
                matches!(
                    vash_proto::resp::parse(&input),
                    Err(vash_proto::resp::ProtocolError::Recoverable { .. })
                ),
                "{tokens:?} should be a recoverable error, not something that runs"
            );
        }
    }

    /// The two commands answered in several steps do not reach `translate`, so
    /// the agreement test cannot see them — and both must be kept off the
    /// runtime's workers: `SCAN` walks records, `DELBYTAG` writes.
    #[test]
    fn the_multi_step_commands_are_not_inline_safe() {
        for command in [
            Command::Scan(vash_proto::resp::command::Scan {
                token: 0,
                pattern: b"",
                count: 10,
                only_strings: true,
            }),
            Command::DelByTag {
                tags: vec![b"news"],
            },
            // Locks every lane and clones what it finds. Bounded, but not by
            // anything small enough to block a worker other connections wait on.
            Command::Slowlog(vash_proto::resp::command::SlowlogSub::Get(None)),
        ] {
            assert!(!inline_safe(&command), "{command:?} must not run inline");
            assert!(
                translate(&command).is_err(),
                "{command:?} is answered before translation"
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
