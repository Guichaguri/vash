//! Executing Redis commands.
//!
//! Kept apart from [`crate::dispatch`] because it works differently on purpose.
//! The VCP and memcached adapters decode into `vash_core::Command`, one command
//! to one storage operation. Redis's string commands do not fit that shape:
//! `APPEND` reads then writes, `INCR` creates a key it did not find, `TTL`
//! reports a lifetime nothing else asks for, and `INCREX` does arithmetic,
//! bounds and an expiry in one step. Rather than push all of that into the
//! storage engine, this module composes the operations the `Store` trait
//! already has.
//!
//! **The consequence is that the read-modify-write commands are not atomic.**
//! `APPEND`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT` and `INCREX` are
//! a read followed by a write with no lock between them, so two clients
//! incrementing the same counter at the same moment can lose an update — where
//! Redis, single-threaded, cannot. `SET … KEEPTTL`, `SET … GET`, `EXPIRE` with
//! a condition and `MSETEX` with `NX`/`XX` have the same seam. Making them
//! atomic means new primitives inside the shard writer, where the single
//! writer thread already serialises everything; that is a storage-engine
//! change, and this milestone is deliberately the protocol only.

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
    if !table.configured() {
        return Err(Failure::client(
            "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
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
            StoreError::NotNumeric => Failure::client(NOT_AN_INTEGER),
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

            // Before the version is applied: a `HELLO 3 AUTH` with a bad
            // credential must leave the connection exactly as it was, rather
            // than switching it to RESP3 and then refusing.
            if let Some(credential) = auth {
                authenticate(state, conn, credential)?;
            }

            match requested {
                None => {}
                Some(2) => *version = Version::Resp2,
                Some(3) => *version = Version::Resp3,
                Some(_) => {
                    return Err(
                        (&ErrorReply::Coded("NOPROTO", "unsupported protocol version")).into(),
                    );
                }
            }
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
            let key = key_of(key)?;
            let current = state.store.get(key)?;

            // Redis creates the key when it is absent, and keeps the existing
            // expiry when it is not.
            let (combined, ttl_secs) = match &current {
                Some(existing) => {
                    let mut combined = Vec::with_capacity(existing.data.len() + value.len());
                    combined.extend_from_slice(&existing.data);
                    combined.extend_from_slice(value);
                    (combined, keep_ttl(existing.expires_at_ms))
                }
                None => (value.to_vec(), 0),
            };

            state
                .store
                .set(&vash_core::Set::plain(key, &combined, ttl_secs))?;
            state.metrics.write();
            encode::integer(out, combined.len() as i64);
            Ok(())
        }

        Command::Expire {
            key,
            expiry,
            condition,
        } => execute_expire(state, key, *expiry, *condition, out),

        Command::Persist { key } => {
            let key = key_of(key)?;
            let Some(current) = state.store.deadline(key)? else {
                state.metrics.read(0, 1);
                return answer_bool(out, false);
            };
            if current == vash_core::NEVER {
                state.metrics.read(1, 0);
                return answer_bool(out, false);
            }
            let touched = state.store.touch(key, 0)?;
            state.metrics.write();
            answer_bool(out, touched)
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
    let key = key_of(set.key)?;

    // `GET` needs the old value, which is a read Redis does not pay for.
    let previous = if set.return_previous {
        state.store.get(key)?
    } else {
        None
    };

    let deadline = if set.return_previous {
        // Already read; a miss means there is no deadline to keep either.
        previous.as_ref().and_then(|value| value.expires_at_ms)
    } else if set.expiry == Expiry::Keep {
        // `KEEPTTL` wants the old deadline and not the old value, so it reads
        // the header alone rather than copying the value out to discard it.
        state.store.deadline(key)?
    } else {
        None
    };

    let ttl_secs = ttl_for(set.expiry, now_ms() as i64, deadline, INVALID_EXPIRE_SET)?;

    let outcome = state.store.store(&vash_core::Set {
        key,
        value: set.value,
        ttl_secs,
        mc_flags: 0,
        tags: Vec::new(),
        mode: match set.condition {
            Condition::Always => SetMode::Set,
            Condition::IfAbsent => SetMode::Add,
            Condition::IfPresent => SetMode::Replace,
        },
    })?;
    state.metrics.write();

    if set.return_previous {
        // With `GET` the client is told what was there, and never whether the
        // write applied — that is Redis's contract, not an omission.
        match previous {
            Some(value) => encode::bulk(out, &value.data),
            None => encode::null(out, version),
        }
    } else if matches!(outcome, Stored::Stored(_)) {
        encode::ok(out);
    } else {
        encode::null(out, version);
    }
    Ok(())
}

fn execute_msetex(
    state: &ServerState,
    pairs: &[(&[u8], &[u8])],
    condition: Condition,
    expiry: Expiry,
    out: &mut Vec<u8>,
) -> Answered {
    let keys = keys_of(pairs.iter().map(|(key, _)| *key))?;

    // The guard and `KEEPTTL` both need to know what is already there — but
    // neither needs the values, only whether they exist and when they go away.
    // Under concurrency this read and the write below are not one step — see
    // the module note.
    let current = if condition != Condition::Always || expiry == Expiry::Keep {
        state.store.deadlines(&keys)?
    } else {
        Vec::new()
    };

    let applies = match condition {
        Condition::Always => true,
        Condition::IfAbsent => current.iter().all(Option::is_none),
        Condition::IfPresent => current.iter().all(Option::is_some),
    };
    if !applies {
        state.metrics.other();
        encode::integer(out, 0);
        return Ok(());
    }

    let now = now_ms() as i64;
    let sets: Vec<vash_core::Set<'_>> = keys
        .iter()
        .zip(pairs)
        .enumerate()
        .map(|(index, (key, (_, value)))| {
            let deadline = current.get(index).copied().flatten();
            let ttl_secs = ttl_for(expiry, now, deadline, INVALID_EXPIRE_MSETEX)?;
            Ok(vash_core::Set::plain(*key, value, ttl_secs))
        })
        .collect::<Result<_, Failure>>()?;

    state.store.set_many(&sets)?;
    state.metrics.write();
    encode::integer(out, 1);
    Ok(())
}

fn execute_expire(
    state: &ServerState,
    key: &[u8],
    expiry: Expiry,
    condition: ExpireCondition,
    out: &mut Vec<u8>,
) -> Answered {
    let key = key_of(key)?;
    let Some(current) = state.store.deadline(key)? else {
        state.metrics.read(0, 1);
        return answer_bool(out, false);
    };

    let now = now_ms() as i64;
    let deadline = match expiry {
        Expiry::After(millis) => now.saturating_add(millis),
        Expiry::At(millis) => millis,
        // The parser only produces the two relative forms for `EXPIRE`.
        Expiry::Unset | Expiry::Keep | Expiry::Persist => now,
    };

    // A key with no expiry is infinitely far off, which is what makes `GT`
    // never apply to one and `LT` always apply.
    let existing = match current {
        vash_core::NEVER => None,
        at => Some(at as i64),
    };
    let applies = match condition {
        ExpireCondition::Always => true,
        ExpireCondition::IfPersistent => existing.is_none(),
        ExpireCondition::IfVolatile => existing.is_some(),
        ExpireCondition::IfLater => existing.is_some_and(|at| deadline > at),
        ExpireCondition::IfEarlier => existing.is_none_or(|at| deadline < at),
    };
    if !applies {
        state.metrics.read(1, 0);
        return answer_bool(out, false);
    }

    // A deadline that has already passed deletes the key outright rather than
    // storing it pre-expired. Redis is explicit that the event is a `del`.
    let applied = if deadline <= now {
        state.store.delete(key)?
    } else {
        state.store.touch(key, ttl_from_deadline(deadline))?
    };
    state.metrics.write();
    answer_bool(out, applied)
}

/// `INCR`, `INCRBY`, `DECR`, `DECRBY` and `INCRBYFLOAT`.
///
/// No `Version` argument, unlike `INCREX`: `INCRBYFLOAT` answers with a bulk
/// string in RESP3 as well as RESP2, so nothing here depends on the dialect.
fn execute_incr(state: &ServerState, key: &[u8], delta: Number, out: &mut Vec<u8>) -> Answered {
    let key = key_of(key)?;
    let current = state.store.get(key)?;
    let ttl_secs = keep_ttl(current.as_ref().and_then(|value| value.expires_at_ms));

    let updated = match delta {
        Number::Int(delta) => {
            let value = read_int(current.as_ref())?;
            Number::Int(
                value
                    .checked_add(delta)
                    .ok_or_else(|| Failure::client(OVERFLOW))?,
            )
        }
        Number::Float(delta) => {
            let value = read_float(current.as_ref())?;
            let updated = value + delta;
            if !updated.is_finite() {
                return Err(Failure::client(NOT_FINITE));
            }
            Number::Float(updated)
        }
    };

    // `INCR` keeps the key's lifetime: it alters the value in place rather
    // than replacing it, which is the distinction `EXPIRE` documents.
    write_number(state, key, updated, ttl_secs)?;
    state.metrics.write();

    match updated {
        Number::Int(value) => encode::integer(out, value),
        Number::Float(value) => encode::bulk(out, encode::format_float(value).as_bytes()),
    }
    Ok(())
}

fn execute_increx(
    state: &ServerState,
    op: &IncrEx<'_>,
    version: Version,
    out: &mut Vec<u8>,
) -> Answered {
    let key = key_of(op.key)?;
    let current = state.store.get(key)?;

    let (value, applied) = match op.delta {
        Number::Int(delta) => {
            let current = read_int(current.as_ref())?;
            let lower = bound_int(op.lower, i64::MIN)?;
            let upper = bound_int(op.upper, i64::MAX)?;

            // Overflowing the type is a bound violation like any other, which
            // is what lets `SATURATE` clamp it instead of failing. Which bound
            // was breached comes from the result where there is one, and from
            // the sign of the increment where the addition overflowed and
            // there is not — reading it off the sign in both cases would clamp
            // `INCREX k BYINT 0 UBOUND 5` on a key holding 10 to the *floor*.
            let candidate = current.checked_add(delta);

            if let Some(updated) = candidate.filter(|v| (lower..=upper).contains(v)) {
                (Number::Int(updated), Number::Int(delta))
            } else if !op.saturate {
                // Skipped: the key and its lifetime are left exactly as they
                // were, and the zero delta is how the client is told.
                state.metrics.other();
                return answer_increx(out, Number::Int(current), Number::Int(0), version);
            } else {
                let clamped = match candidate {
                    Some(updated) if updated > upper => upper,
                    Some(_) => lower,
                    None if delta > 0 => upper,
                    None => lower,
                };
                let applied = clamped
                    .checked_sub(current)
                    .ok_or_else(|| Failure::client(OVERFLOW))?;
                (Number::Int(clamped), Number::Int(applied))
            }
        }
        Number::Float(delta) => {
            let current = read_float(current.as_ref())?;
            let lower = bound_float(op.lower, -f64::MAX);
            let upper = bound_float(op.upper, f64::MAX);

            let updated = current + delta;
            if updated.is_finite() && (lower..=upper).contains(&updated) {
                (Number::Float(updated), Number::Float(delta))
            } else if !op.saturate {
                state.metrics.other();
                return answer_increx(out, Number::Float(current), Number::Float(0.0), version);
            } else {
                let clamped = if updated > upper || (!updated.is_finite() && delta > 0.0) {
                    upper
                } else {
                    lower
                };
                let applied = clamped - current;
                if !applied.is_finite() {
                    return Err(Failure::client(NOT_FINITE));
                }
                (Number::Float(clamped), Number::Float(applied))
            }
        }
    };

    let deadline = current.as_ref().and_then(|value| value.expires_at_ms);
    let existing_ttl = keep_ttl(deadline);
    let ttl_secs = match op.expiry {
        // No expiry option: the lifetime is left alone.
        None => existing_ttl,
        Some(Expiry::Persist) => 0,
        // `ENX`: a key that already has a deadline keeps it.
        Some(_) if op.only_if_persistent && existing_ttl != 0 => existing_ttl,
        // Neither is an `INCREX` option, so the parser cannot produce them;
        // both would mean "leave the lifetime alone" if one ever arrived.
        Some(Expiry::Unset | Expiry::Keep) => existing_ttl,
        Some(expiry) => ttl_for(expiry, now_ms() as i64, deadline, INVALID_EXPIRE_INCREX)?,
    };

    write_number(state, key, value, ttl_secs)?;
    state.metrics.write();
    answer_increx(out, value, applied, version)
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

fn write_number(
    state: &ServerState,
    key: Key<'_>,
    value: Number,
    ttl_secs: u32,
) -> Result<(), Failure> {
    // Counters are stored as their decimal text, which is what makes a `GET`
    // of one return something a client can read — and what the memcached
    // adapter's `incr` already assumes.
    let text = match value {
        Number::Int(value) => value.to_string(),
        Number::Float(value) => encode::format_float(value),
    };
    state
        .store
        .set(&vash_core::Set::plain(key, text.as_bytes(), ttl_secs))?;
    Ok(())
}

/// Reads a stored counter as an integer. A key that is not there counts as
/// zero, which is how `INCR` creates one.
fn read_int(current: Option<&vash_core::Value>) -> Result<i64, Failure> {
    match current {
        None => Ok(0),
        Some(value) => vash_proto::resp::command::parse_int(&value.data)
            .ok_or_else(|| Failure::client(NOT_AN_INTEGER)),
    }
}

/// The float equivalent. Accepts a stored integer too, because an integer
/// promotes to a float without loss — which is exactly what `INCRBYFLOAT` and
/// `BYFLOAT` are specified to allow.
fn read_float(current: Option<&vash_core::Value>) -> Result<f64, Failure> {
    match current {
        None => Ok(0.0),
        Some(value) => vash_proto::resp::command::parse_float(&value.data)
            .filter(|value| value.is_finite())
            .ok_or_else(|| Failure::client(NOT_A_FLOAT)),
    }
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
