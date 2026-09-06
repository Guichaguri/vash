//! The Vash Cache Protocol: frame execution, the header gate, and `AUTH`.
//!
//! Split out of `dispatch`, which is the boundary all three dialects meet at
//! and should hold nothing that is true of only one of them. This is the VCP
//! half of what used to live there; [`crate::memcached`] is the other, and
//! [`crate::resp`] was already its own module.
//!
//! Two things here have no counterpart in the other dialects. The gate in
//! `header_refusal` decides from the twelve-byte header alone, before any
//! body decoder runs — which is what keeps the pre-auth attack surface to the
//! header plus one parser. And `offsets_to_store_ttls` reinterprets this
//! protocol's TTLs: VCP's are plain offsets at every magnitude, where the
//! store's field is memcached's `exptime`, in which an offset past 30 days is
//! instead an absolute stamp.

use tracing::{error, warn};
use vash_core::{Command, Reply};
use vash_proto::vcp::{
    AuthRequest, DecodeError, Decoded, Opcode, Status, ValuesFrame, decode, encode_error,
    encode_reply, encode_response,
};

use crate::auth::{ConnAuth, DEFAULT_NAME, Mechanism};
use crate::dispatch::{Closing, Origin, SetBatch, batchable, deny, execute, execute_get_into};
use crate::metrics::CommandKind;
use crate::state::ServerState;

/// Executes every complete VCP frame in `block`, appending the responses.
///
/// The block was already measured by the caller to end on a frame boundary, so
/// the lengths here cannot run short. Its shape mirrors
/// [`crate::memcached::execute_memcached_block`] and `resp::execute_block` so that all three
/// dialects can share one drain — see [`crate::conn`].
///
/// VCP has no command that closes the connection: `QUIT` is answered and the
/// socket stays open, because a client that wants it gone can simply drop it.
pub fn execute_vcp_block(
    state: &ServerState,
    conn: &mut ConnAuth,
    // Who is asking, built once per connection: the dialect the counters need,
    // and the connection a slowlog entry names.
    origin: Origin,
    block: &[u8],
    out: &mut Vec<u8>,
) -> Closing {
    use vash_proto::vcp::{FrameLen, peek_frame_len};

    let mut batch: SetBatch<'_, VcpReply> = SetBatch::default();

    let mut rest = block;
    while !rest.is_empty() {
        let FrameLen::Complete(len) = peek_frame_len(rest) else {
            // Unreachable: the caller only includes whole frames. Stopping
            // rather than looping keeps a logic slip from spinning a core.
            error!("a VCP block did not end on a frame boundary");
            break;
        };
        execute_frame_batched(state, conn, origin, &rest[..len], out, &mut batch);
        rest = &rest[len..];
    }
    flush_vcp(state, origin, &mut batch, out);
    Closing::No
}

/// What a held VCP write needs in order to be answered later.
struct VcpReply {
    opcode: Opcode,
    request_id: u32,
    no_reply: bool,
}

/// Submits a held run of writes and encodes each response.
fn flush_vcp(
    state: &ServerState,
    origin: Origin,
    batch: &mut SetBatch<'_, VcpReply>,
    out: &mut Vec<u8>,
) {
    batch.flush(state, origin, out, |out, reply, result| {
        if reply.no_reply {
            if let Err(failed) = result {
                warn!(status = ?failed.status, opcode = ?reply.opcode, "no-reply request failed");
            }
            return;
        }
        match result {
            Ok(cas) => encode_reply(
                out,
                reply.opcode,
                reply.request_id,
                &Reply::Stored(vash_core::Stored::Stored(cas)),
            ),
            Err(failed) => encode_error(out, reply.opcode as u8, reply.request_id, failed.status),
        }
    });
}

/// Decodes and executes one complete frame, appending the encoded response.
///
/// Runs on a blocking thread, and borrows the frame bytes, so the key and value
/// slices produced by the decoder never cross a task boundary and never need
/// copying. Appends nothing for a `NO_REPLY` request.
///
/// Takes the output buffer rather than returning one so a pipelined batch
/// builds a single response buffer instead of one allocation per frame.
pub fn execute_frame_into(
    state: &ServerState,
    conn: &mut ConnAuth,
    // Who is asking, built once per connection: the dialect the counters need,
    // and the connection a slowlog entry names.
    origin: Origin,
    frame: &[u8],
    out: &mut Vec<u8>,
) {
    // A batch of one, flushed at once: a caller executing a lone frame has no
    // run to coalesce, and this keeps that path answering exactly as it did.
    let mut batch = SetBatch::default();
    execute_frame_batched(state, conn, origin, frame, out, &mut batch);
    flush_vcp(state, origin, &mut batch, out);
}

/// [`execute_frame_into`], with a run of unconditional writes that this frame
/// may join instead of executing — see [`SetBatch`].
fn execute_frame_batched<'a>(
    state: &ServerState,
    conn: &mut ConnAuth,
    // Who is asking, built once per connection: the dialect the counters need,
    // and the connection a slowlog entry names.
    origin: Origin,
    frame: &'a [u8],
    out: &mut Vec<u8>,
    batch: &mut SetBatch<'a, VcpReply>,
) {
    // Refused from the **header alone**, before the body is parsed at all.
    //
    // Doing it here rather than after decoding is what shrinks the pre-auth
    // attack surface to the frame header plus one body parser: without it, an
    // unauthenticated stranger drives `decode_set`, `decode_tag_sync` and every
    // other body decoder just by naming the opcode. They are fuzzed, so this is
    // defence in depth rather than a hole being closed — but a gate that runs
    // after the parsing it is meant to protect is not much of a gate.
    //
    // It also stops an unauthenticated party enumerating which opcodes this
    // build implements: an unknown or unimplemented one is `UNAUTHORIZED` like
    // everything else, rather than `UNSUPPORTED`.
    if let Some(refused) = header_refusal(state, conn, frame) {
        flush_vcp(state, origin, batch, out);
        match refused.missing {
            Some(missing) => deny(state, conn, missing, OpcodeName(refused.opcode)),
            None => state.metrics.auth_refused(),
        }
        if !refused.no_reply {
            encode_error(
                out,
                refused.opcode,
                refused.request_id,
                Status::Unauthorized,
            );
        }
        return;
    }

    match decode(frame) {
        Ok(Decoded::Auth {
            request_id, auth, ..
        }) => {
            flush_vcp(state, origin, batch, out);
            execute_auth(state, conn, request_id, &auth, out)
        }

        Ok(Decoded::Request { mut request, .. }) => {
            offsets_to_store_ttls(&mut request.command);

            // An unconditional write joins the run rather than waiting for its
            // own commit. Everything below this point flushes first, so no
            // command is ever answered from a state the writes before it have
            // not reached.
            if let Some(set) = batchable(&request.command) {
                if batch.holds(set.key) {
                    flush_vcp(state, origin, batch, out);
                }
                let set = set.clone();
                batch.push(
                    set,
                    VcpReply {
                        opcode: request.opcode,
                        request_id: request.request_id,
                        no_reply: request.no_reply,
                    },
                );
                return;
            }
            flush_vcp(state, origin, batch, out);

            // The fused read path. A `NO_REPLY` `GET` is excluded on purpose:
            // it has nowhere to render into, and sending it round the ordinary
            // path keeps that case answering exactly as it did.
            if let Command::Get { key } = request.command
                && !request.no_reply
            {
                let (opcode, request_id) = (request.opcode, request.request_id);
                let rendered =
                    execute_get_into(state, key, CommandKind::Get, origin, &mut |value| {
                        vash_proto::vcp::encode::encode_value(out, opcode, request_id, value)
                    });
                match rendered {
                    Ok(true) => {}
                    // Through `encode_reply` rather than writing the status
                    // here, so a miss keeps whatever that mapping says it is.
                    Ok(false) => encode_reply(out, opcode, request_id, &Reply::NotFound),
                    Err(failed) => encode_error(out, opcode as u8, request_id, failed.status),
                }
                return;
            }

            // The batch form, fused the only way a length-prefixed frame can
            // be: the header goes down with a placeholder, the entries stream
            // straight out of the memory map in request order, and the length
            // is patched in at the end. See `vcp::encode::ValuesFrame` and
            // `docs/performance-proposals.md` §18.
            //
            // `NO_REPLY` is excluded for the same reason it is above — there is
            // nowhere to render into — and so is an empty batch, which the
            // ordinary path already answers and which would otherwise open a
            // frame for nothing.
            if let Command::GetMany(keys) = &request.command
                && !request.no_reply
                && !keys.is_empty()
            {
                let (opcode, request_id) = (request.opcode, request.request_id);
                let frame = ValuesFrame::open(out, opcode, request_id, keys.len() as u32);
                let rendered = crate::dispatch::execute_get_many_into(
                    state,
                    keys,
                    CommandKind::GetMany,
                    origin,
                    &mut |_, value| match value {
                        Some(value) => frame.hit(out, value),
                        None => frame.miss(out),
                    },
                );
                match rendered {
                    // A body too large to frame is not a reply that can be
                    // trimmed — the client asked for these keys and half of
                    // them is not an answer — so the frame is abandoned and
                    // the status carries it.
                    Ok(()) => {
                        if frame.finish(out).is_err() {
                            encode_error(out, opcode as u8, request_id, Status::TooLarge);
                        }
                    }
                    Err(failed) => {
                        frame.abandon(out);
                        encode_error(out, opcode as u8, request_id, failed.status);
                    }
                }
                return;
            }

            let result = execute(state, &request.command, origin);

            if request.no_reply {
                if let Err(failed) = result {
                    warn!(status = ?failed.status, opcode = ?request.opcode, "no-reply request failed");
                }
                return;
            }

            match result {
                Ok(reply) => encode_reply(out, request.opcode, request.request_id, &reply),
                Err(failed) => {
                    encode_error(out, request.opcode as u8, request.request_id, failed.status)
                }
            }
        }

        Err(DecodeError::Body {
            request_id,
            opcode,
            status,
            detail,
            ..
        }) => {
            // Before the error is written, so a refused frame's response still
            // lands after the responses to everything that preceded it.
            flush_vcp(state, origin, batch, out);
            warn!(request_id, opcode, detail, "rejected malformed request");
            encode_error(out, opcode, request_id, status);
        }

        // The caller only passes frames whose length it already validated, so
        // neither of these is reachable. Answering rather than panicking keeps a
        // logic slip from taking the process down.
        Ok(Decoded::Incomplete { .. }) | Err(DecodeError::Fatal { .. }) => {
            flush_vcp(state, origin, batch, out);
            error!("frame passed to execute_frame_into was not a complete, valid frame");
            encode_error(out, 0, 0, Status::Internal);
        }
    }
}

/// An opcode byte, rendered by name for the denial log.
///
/// Borrows the derived `Debug` rather than adding a fourth table of names to
/// keep in step with the other three. Unknown bytes render as hex, which is
/// only reachable through a path that does not refuse them.
struct OpcodeName(u8);

impl std::fmt::Display for OpcodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match vash_proto::vcp::Opcode::from_u8(self.0) {
            Some(opcode) => write!(f, "{opcode:?}"),
            None => write!(f, "0x{:02x}", self.0),
        }
    }
}

/// Enough of a refused frame's header to answer it.
struct Refused {
    /// The raw byte, echoed so a client can correlate even when the server does
    /// not recognise it.
    opcode: u8,
    request_id: u32,
    no_reply: bool,
    /// The roles the identity was missing, when this is a role denial rather
    /// than a pre-auth refusal. `None` means "has not authenticated".
    ///
    /// Both answer `UNAUTHORIZED` — auth.md §6.2 widened that status to
    /// "refused by policy", which covers both — so this exists to keep the
    /// *counters* apart, not the wire. An alert that cannot tell "nobody rolled
    /// their credential" from "someone's role set is wrong" is an alert that
    /// gets muted.
    missing: Option<crate::auth::Roles>,
}

/// Whether a frame must be refused — for want of authentication, or for want of
/// the roles the opcode needs — decided from the twelve-byte header without
/// touching the body.
///
/// Both questions are answered here, in one place, because both have the same
/// requirement: refuse before the body decoder runs. Checking roles on the
/// **opcode** is what buys that for the second one, and it is why there is a
/// mapping from `Opcode` beside the one from `Command`.
///
/// **`HELLO` is in the pre-auth set and has to be**: first-byte detection
/// requires a VCP connection to open with opcode `0x01`, so there is no way to
/// authenticate before announcing the dialect. It therefore discloses the
/// server's limits and capability bits to an unauthenticated party, which is
/// the deliberate minimum that lets a client discover it must authenticate
/// rather than guess from a refusal.
///
/// **`PING` is not**, though it looks harmless: everything it could tell an
/// unauthenticated party `HELLO` already told them, and `/health` on the admin
/// port is the liveness check an operator should be using.
fn header_refusal(state: &ServerState, conn: &ConnAuth, frame: &[u8]) -> Option<Refused> {
    // Short of a header there is nothing to refuse *with* — no request id to
    // echo. The decoder below reports it as the malformed frame it is.
    if frame.len() < vash_proto::vcp::HEADER_LEN {
        return None;
    }

    let opcode = frame[0];
    let refused = |missing| {
        Some(Refused {
            opcode,
            request_id: u32::from_le_bytes(frame[4..8].try_into().expect("four bytes")),
            no_reply: frame[1] & vash_proto::vcp::flags::NO_REPLY != 0,
            missing,
        })
    };

    // The pre-auth exemption, which is this dialect's own: first-byte detection
    // requires a VCP connection to open with `HELLO`, so there is no way to
    // announce the dialect before authenticating. Applied before the shared
    // decision because it is about *which commands* are exempt rather than
    // about what the connection may do.
    if !conn.is_authenticated() && (opcode == Opcode::Hello as u8 || opcode == Opcode::Auth as u8) {
        return None;
    }

    // An unknown opcode is left to the decoder, which reports it as
    // `UNSUPPORTED`. Refusing it here would be answering for a command this
    // build does not have — but only once the connection has authenticated:
    // for one that has not, the refusal below must not depend on whether the
    // byte is recognised, or an unauthenticated stranger could enumerate which
    // opcodes this build implements by watching for a different status.
    let Some(known) = vash_proto::vcp::Opcode::from_u8(opcode) else {
        return match state.auth.refusal(conn, crate::auth::Roles::NONE) {
            Some(crate::auth::Refused::Unauthenticated) => refused(None),
            _ => None,
        };
    };

    match state
        .auth
        .refusal(conn, crate::auth::roles_for_opcode(known))?
    {
        crate::auth::Refused::Unauthenticated => refused(None),
        crate::auth::Refused::Missing(missing) => refused(Some(missing)),
    }
}

/// Answers an `AUTH` frame.
///
/// Always replies, even under `NO_REPLY` — a client that cannot learn whether
/// it authenticated will pipeline a batch into a connection that refuses all of
/// it.
fn execute_auth(
    state: &ServerState,
    conn: &mut ConnAuth,
    request_id: u32,
    auth: &AuthRequest<'_>,
    out: &mut Vec<u8>,
) {
    state.metrics.other();

    // The granted roles, echoed in the reply body on success. Empty on every
    // failure — there is nothing to report about an identity that was not
    // established.
    let mut body = Vec::new();

    let status = match Mechanism::from_u8(auth.mechanism) {
        // Specified in docs/auth.md §6.3 and not built. `UNSUPPORTED` rather
        // than `UNAUTHORIZED` so a client can tell "this mechanism does not
        // exist here" from "your credential is wrong", and probe for it without
        // a capability bit.
        Some(Mechanism::HmacSha256) | None => {
            warn!(
                mechanism = auth.mechanism,
                "rejected auth: unsupported mechanism"
            );
            Status::Unsupported
        }

        Some(Mechanism::Plain) => {
            let name = if auth.name.is_empty() {
                DEFAULT_NAME.as_bytes()
            } else {
                auth.name
            };

            match state.auth.current().verify(name, auth.secret) {
                Some(identity) => {
                    // One byte, and old clients are unaffected because VCP
                    // frames are length-prefixed. It is what keeps the three
                    // meanings of `UNAUTHORIZED` distinguishable client-side: a
                    // client knows whether it authenticated, and now knows what
                    // that authentication granted, so it can fail at startup
                    // rather than at 3am on its first `FLUSH`.
                    body.push(identity.roles().bits());
                    conn.succeed(identity);
                    state.metrics.auth_ok();
                    Status::Ok
                }
                None => {
                    conn.fail();
                    // The name is logged because an operator needs to know
                    // which credential is failing; the secret never is, because
                    // a near-miss credential in a log file is a credential in a
                    // log file.
                    warn!(
                        name = ?crate::auth::log_name(name),
                        failures = conn.failures(),
                        "authentication failed"
                    );
                    state.metrics.auth_failed();
                    Status::Unauthorized
                }
            }
        }
    };

    encode_response(out, Opcode::Auth, request_id, status, &body);
}

/// Rewrites a decoded VCP request's TTLs from plain offsets into the overloaded
/// form the store reads.
///
/// VCP's `ttl_secs` is an offset at every magnitude — it is a new protocol and
/// owes memcached's clients nothing. The store's field is memcached's
/// `exptime`, where an offset past 30 days is instead an absolute stamp, so
/// without this a VCP client asking for 60 days would get a deadline in 1970
/// and a value that was gone the moment it landed.
///
/// Applied here, on the VCP side of the fork, rather than in the decoder: the
/// translation needs the clock, and a decoder that reads the clock is one whose
/// output no longer round-trips against its encoder.
fn offsets_to_store_ttls(command: &mut Command<'_>) {
    let clock = vash_core::Clock::new();
    match command {
        Command::Set(set) => set.ttl = offset_to_store_ttl(&clock, set.ttl),
        Command::SetMany { sets, .. } => {
            for set in sets {
                set.ttl = offset_to_store_ttl(&clock, set.ttl);
            }
        }
        Command::Touch { ttl_secs, .. } => *ttl_secs = clock.ttl_from_offset(*ttl_secs),
        _ => {}
    }
}

/// The same translation for a [`TtlChange`], which only the `Set` forms carry.
///
/// The variants that name no offset pass through: there is nothing to reinterpret
/// in "keep whatever is there".
///
/// [`TtlChange`]: vash_core::TtlChange
fn offset_to_store_ttl(
    clock: &vash_core::Clock,
    ttl: vash_core::TtlChange,
) -> vash_core::TtlChange {
    use vash_core::TtlChange;
    match ttl {
        TtlChange::Set(offset) => TtlChange::Set(clock.ttl_from_offset(offset)),
        TtlChange::SetIfPersistent(offset) => {
            TtlChange::SetIfPersistent(clock.ttl_from_offset(offset))
        }
        TtlChange::Keep => TtlChange::Keep,
    }
}
