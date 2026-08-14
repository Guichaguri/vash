//! Command execution: the only place that maps domain outcomes onto wire status
//! codes.

use tracing::{error, warn};
use vash_core::{Command, Reply, ServerInfo};
use vash_proto::vcp::{
    AuthRequest, DecodeError, Decoded, Opcode, Status, decode, encode_error, encode_reply,
    encode_response,
};
use vash_store::StoreError;

use crate::auth::{ConnAuth, DEFAULT_NAME, Mechanism};
use crate::metrics::{CommandKind, Dialect, ErrorClass};
use crate::state::ServerState;

/// Executes every complete memcached command in `block`, appending the replies.
///
/// The block was already measured by the caller to end on a command boundary,
/// so parsing here cannot run short; re-parsing rather than passing the parsed
/// form across is what keeps the borrowed key and value slices from crossing a
/// task boundary.
pub fn execute_memcached_block(
    state: &ServerState,
    conn: &mut ConnAuth,
    block: &[u8],
    out: &mut Vec<u8>,
) -> Closing {
    use vash_proto::memcached::{Outcome, ProtocolError, parse};

    let mut rest = block;
    while !rest.is_empty() {
        match parse(rest) {
            Ok(Outcome::Command(parsed)) => {
                let consumed = parsed.consumed;
                if execute_memcached(state, conn, &parsed, out) == Closing::Yes {
                    // `quit`: nothing after it on this connection matters.
                    return Closing::Yes;
                }
                rest = &rest[consumed..];
            }
            Err(ProtocolError::Recoverable { response, consumed }) => {
                // An unparseable command from an unauthenticated connection is
                // still an unauthenticated command, and upstream answers it as
                // one — an unknown verb, a bad command line and outright
                // garbage all get `CLIENT_ERROR unauthenticated` rather than
                // the parse error, so a stranger cannot probe which commands
                // the server understands. Verified against memcached 1.6.45.
                //
                // The same reasoning put the VCP gate ahead of opcode
                // recognition; this is that rule for the text dialect. The byte
                // count is the parser's either way, so a refused storage
                // command still steps over its data block.
                if !conn.is_authenticated() && state.auth.required() {
                    state.metrics.auth_refused();
                    vash_proto::memcached::encode::encode_error(
                        out,
                        vash_proto::memcached::ErrorKind::Client("unauthenticated"),
                    );
                } else {
                    vash_proto::memcached::encode::encode_error(out, response);
                }
                rest = &rest[consumed..];
            }
            // Unreachable: the caller only includes whole commands. Stopping
            // rather than looping keeps a logic slip from spinning a core.
            Ok(Outcome::Incomplete) | Err(ProtocolError::Fatal(_)) => {
                error!("a memcached block did not end on a command boundary");
                break;
            }
        }
    }
    Closing::No
}

/// Executes one memcached command, rendering the reply in that dialect.
///
/// Shares [`execute`] with the VCP path: the storage layer never learns which
/// wire format a request arrived on.
pub fn execute_memcached(
    state: &ServerState,
    conn: &mut ConnAuth,
    parsed: &vash_proto::memcached::Parsed<'_>,
    out: &mut Vec<u8>,
) -> Closing {
    use vash_proto::memcached::encode as mc;

    if !conn.is_authenticated() && state.auth.required() {
        return execute_memcached_unauthenticated(state, conn, parsed, out);
    }

    // A dump answers with as many lines as the keyspace has, which is more than
    // one `Reply` can carry — so it pages the listing itself and writes as it
    // goes. See [`execute_dump`].
    if let vash_proto::memcached::encode::ResponseStyle::Dump(dump) = &parsed.style {
        if let Err(failed) = execute_dump(state, *dump, out) {
            mc::encode_error(out, memcached_error(failed.status));
        }
        return Closing::No;
    }

    // `stats cachedump` is one page and writes `ITEM` lines, so it does not
    // share the paging loop above — but it is the same listing underneath.
    if let vash_proto::memcached::encode::ResponseStyle::CacheDump(dump) = &parsed.style {
        if let Err(failed) = execute_cachedump(state, *dump, out) {
            mc::encode_error(out, memcached_error(failed.status));
        }
        return Closing::No;
    }

    // Every `stats` section except the general one describes the *server* — its
    // configuration, its connections, the shape of its storage — rather than
    // the cache. There is no storage command for those to be, so they are
    // rendered from `crate::stats` directly instead of travelling as
    // `Command::Stats`. They are still counted, at the same point everything
    // else is.
    if let vash_proto::memcached::encode::ResponseStyle::Stats(section) = &parsed.style
        && *section != vash_proto::memcached::encode::StatsSection::General
    {
        state.metrics.other();
        mc::stat_lines(out, &crate::stats::section(state, *section));
        out.extend_from_slice(b"END\r\n");
        return Closing::No;
    }

    // The one thing that distinguishes the two memcached dialects at execution
    // time, and the only place that knows: the parser records which grammar a
    // command came from, and the storage layer never learns there are two.
    if matches!(
        parsed.style,
        vash_proto::memcached::encode::ResponseStyle::Meta(_)
    ) {
        state
            .metrics
            .outcomes
            .meta_commands
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // A plain `get` or `gets` naming exactly one key. That is what every
    // memcached client library sends for a single-key read — `pymemcache`'s
    // `get`, `libmemcached`'s `memcached_get` — so it is the most executed
    // command this server has, and it was on the copying path purely because
    // the *grammar* admits more than one key.
    //
    // Only the one-key case fuses. A genuine multi-key `get` cannot: its keys
    // are grouped by shard before they are read, so the values come back out of
    // request order, and putting them back in order is what the `Vec` this
    // avoids is for.
    //
    // `GetAndTouch` shares this response style and must not come through here —
    // it writes. Matching the command rather than the style is what excludes
    // it, exactly as it is for `mg` below.
    if let vash_proto::memcached::encode::ResponseStyle::Retrieval { with_cas } = parsed.style
        && let Command::GetMany(keys) = &parsed.command
        && let [key] = keys.as_slice()
        && !parsed.noreply
    {
        let rendered = execute_get_into(
            state,
            *key,
            CommandKind::GetMany,
            Dialect::Memcached,
            &mut |value| mc::encode_value_line(out, with_cas, key.as_bytes(), value),
        );
        match rendered {
            // A miss writes no `VALUE` line at all — memcached omits misses
            // rather than reporting them — so both outcomes end the same way.
            Ok(_) => out.extend_from_slice(mc::RETRIEVAL_END),
            Err(failed) => mc::encode_error(out, memcached_error(failed.status)),
        }
        return Closing::No;
    }

    // The fused read path for the meta dialect, which is `mg` alone. A `T` flag
    // turns `mg` into a get-and-touch, which writes — the parser renders that
    // as `GetAndTouch`, so matching the command rather than the style is what
    // keeps this off the write path.
    if let vash_proto::memcached::encode::ResponseStyle::Meta(
        vash_proto::memcached::encode::MetaStyle::Get(flags),
    ) = &parsed.style
        && let Command::Get { key } = parsed.command
        && !parsed.noreply
    {
        let rendered = execute_get_into(
            state,
            key,
            CommandKind::Get,
            Dialect::Memcached,
            &mut |value| mc::encode_meta_get_hit(out, flags, &parsed.command, value),
        );
        match rendered {
            Ok(true) => {}
            Ok(false) => out.extend_from_slice(b"EN\r\n"),
            Err(failed) => mc::encode_error(out, memcached_error(failed.status)),
        }
        return Closing::No;
    }

    let result = execute(state, &parsed.command, Dialect::Memcached);
    let closing = matches!(result, Ok(Reply::Closing));

    match result {
        Ok(reply) => {
            // `noreply` suppresses the response but never the work.
            if !parsed.noreply {
                mc::encode(out, &parsed.style, &parsed.command, &reply);
            }
        }
        Err(failed) => {
            if !parsed.noreply {
                mc::encode_error(out, memcached_error(failed.status));
            }
        }
    }

    if closing { Closing::Yes } else { Closing::No }
}

/// Handles a memcached command on a connection that has not authenticated.
///
/// The only thing accepted is upstream's ASCII authentication, which tunnels
/// credentials through a `set` whose key is the username and whose data block
/// is `<user> <pass>`:
///
/// ```text
/// set billing-api 0 0 34\r\n
/// billing-api s3cr3t-token-goes-here-here\r\n
/// → STORED
/// ```
///
/// It is an ugly mechanism — the text protocol had no room for a verb old
/// clients would tolerate — but it is the only one memcached clients implement,
/// and compatibility is the entire reason this dialect exists here. Inventing
/// our own verb would be a command nobody sends.
///
/// **The parser is not involved**, and that is the point: it sees an ordinary
/// storage command and consumes the length-delimited block exactly as it always
/// does. Deciding here rather than there is what keeps a refused command from
/// leaving its data block in the stream to be read as commands, and it means
/// the fuzz targets do not grow a mode.
fn execute_memcached_unauthenticated(
    state: &ServerState,
    conn: &mut ConnAuth,
    parsed: &vash_proto::memcached::Parsed<'_>,
    out: &mut Vec<u8>,
) -> Closing {
    use vash_core::{Command, SetMode, Stored};
    use vash_proto::memcached::encode as mc;

    // `quit` stays legal: hanging up is not a use of the cache, and refusing it
    // would leave a client with no way to close politely.
    if matches!(parsed.command, Command::Quit) {
        return Closing::Yes;
    }

    // Anything that is not a plain `set` is simply not the auth command. All
    // three of these strings are upstream's, verified against memcached 1.6.45
    // by `tests/compat/differential.py --dialect memcached --auth`.
    let Command::Set(set) = &parsed.command else {
        return refuse_memcached(state, parsed, out, "unauthenticated");
    };
    if set.mode != SetMode::Set {
        return refuse_memcached(state, parsed, out, "unauthenticated");
    }

    // A `set` that reaches here is an authentication attempt, so a block that
    // is not `<user> <pass>` is a malformed attempt rather than an
    // unauthenticated command — upstream distinguishes the two and so does
    // this.
    let Some((name, secret)) = split_credential(set.value) else {
        conn.fail();
        state.metrics.auth_failed();
        return refuse_memcached(state, parsed, out, "bad authentication token format");
    };

    match state.auth.current().verify(name, secret) {
        Some(identity) => {
            conn.succeed(identity);
            state.metrics.auth_ok();
            // `STORED`, which is what upstream answers a successful
            // authentication. The CAS token is meaningless — nothing was
            // stored — and no client reads it from this reply.
            if !parsed.noreply {
                mc::encode(
                    out,
                    &parsed.style,
                    &parsed.command,
                    &Reply::Stored(Stored::Stored(0)),
                );
            }
            Closing::No
        }
        None => {
            conn.fail();
            warn!(
                name = %String::from_utf8_lossy(name),
                failures = conn.failures(),
                "authentication failed"
            );
            state.metrics.auth_failed();
            refuse_memcached(state, parsed, out, "authentication failure")
        }
    }
}

/// Answers an `lru_crawler` dump, paging the listing until it runs out.
///
/// **The grammar has no cursor**, so resumption happens here: each iteration is
/// an ordinary `LIST_KEYS` that opens and closes its own read transaction, so
/// nothing pins a snapshot for the length of the dump — the footgun plan §9
/// exists to avoid, and the reason `LIST_KEYS` pages at all. Going through
/// [`execute`] each time is what keeps the gate, the counters and the error
/// classification on the shared boundary; only the line writing is this
/// dialect's.
///
/// A dump therefore counts as one listing per page rather than one in total.
/// That is honest — it is that many scans — and worth knowing when reading
/// `vash_command_total{command="listing"}`.
fn execute_dump(
    state: &ServerState,
    dump: vash_proto::memcached::encode::Dump,
    out: &mut Vec<u8>,
) -> Result<(), Failed> {
    use vash_proto::memcached::encode as mc;

    // Checked even when the class selects nothing: "this command is disabled"
    // is true regardless of which class was named, and answering `END` would
    // say the class was merely empty.
    listing_gate(state)?;

    // Upstream acknowledges before it streams anything, and does so even for a
    // class holding nothing. A client that mistook it for the first line would
    // lose a key.
    out.extend_from_slice(mc::DumpStyle::ACK);

    if !dump.selected {
        out.extend_from_slice(dump.style.terminator());
        return Ok(());
    }

    // One dump gets one `LIST_KEYS` call's budget, spent across as many pages as
    // it needs. Each page is allowed its own full budget by `execute`, so the
    // true ceiling is one page's overshoot past this — bounded, which is the
    // point, rather than exact.
    let budget = state.protocol.listing_max_scan as u64;
    let mut scanned = 0u64;
    let mut cursor: Option<Box<[u8]>> = None;

    loop {
        let request = vash_core::ListRequest {
            limit: vash_core::MAX_LIST_LIMIT,
            cursor: cursor.as_deref().unwrap_or(&[]),
            pattern: &[],
        };
        let reply = execute(state, &Command::ListKeys(request), Dialect::Memcached)?;
        let Reply::Listing(page) = reply else {
            error!("a listing produced a reply it cannot render");
            return Err(Failed::status(Status::Internal));
        };

        for entry in &page.entries {
            mc::dump_line(out, dump.style, entry);
        }
        scanned += page.scanned;

        match page.cursor {
            // More to walk, and budget left to walk it with.
            Some(next) if scanned < budget => cursor = Some(next),

            // Cut short. **The error replaces the terminator rather than
            // following it**, and that substitution is the whole point: a tool
            // reads lines until the terminator, so ending a truncated dump with
            // one would report a keyspace smaller than the real one. An error
            // where the terminator belongs cannot be mistaken for a complete
            // answer.
            Some(_) => {
                warn!(scanned, budget, "dump exceeded its scan budget");
                mc::encode_error(
                    out,
                    vash_proto::memcached::ErrorKind::Server(
                        "dump exceeded the scan budget; use SCAN or LIST_KEYS to page the keyspace",
                    ),
                );
                return Ok(());
            }

            // Walked to the end of the keyspace.
            None => {
                out.extend_from_slice(dump.style.terminator());
                return Ok(());
            }
        }
    }
}

/// Answers `stats cachedump <class> <limit>`.
///
/// One page, no acknowledgement and no cursor — the whole command is a single
/// `LIST_KEYS`, which is why this does not share [`execute_dump`]'s loop. Gated
/// with everything else that enumerates the keyspace; upstream gates its own
/// behind `dump_enabled`, which is the same setting under another name.
fn execute_cachedump(
    state: &ServerState,
    dump: vash_proto::memcached::encode::CacheDump,
    out: &mut Vec<u8>,
) -> Result<(), Failed> {
    use vash_proto::memcached::encode as mc;

    // Checked before the class and the limit, because "this command is
    // disabled" is true regardless of either — answering `END` would say the
    // class was merely empty.
    listing_gate(state)?;

    // A class this server does not have holds nothing, and needs no scan to say
    // so. The limit is already resolved to a page size by the parser, so there
    // is no zero to special-case here.
    if !dump.selected {
        out.extend_from_slice(b"END\r\n");
        return Ok(());
    }

    let request = vash_core::ListRequest {
        limit: dump.limit,
        cursor: &[],
        pattern: &[],
    };
    let reply = execute(state, &Command::ListKeys(request), Dialect::Memcached)?;
    let Reply::Listing(page) = reply else {
        error!("a listing produced a reply it cannot render");
        return Err(Failed::status(Status::Internal));
    };

    for entry in &page.entries {
        mc::cachedump_line(out, entry);
    }
    out.extend_from_slice(b"END\r\n");
    Ok(())
}

fn refuse_memcached(
    state: &ServerState,
    parsed: &vash_proto::memcached::Parsed<'_>,
    out: &mut Vec<u8>,
    reason: &'static str,
) -> Closing {
    if reason == "unauthenticated" {
        state.metrics.auth_refused();
    }
    if !parsed.noreply {
        vash_proto::memcached::encode::encode_error(
            out,
            vash_proto::memcached::ErrorKind::Client(reason),
        );
    }
    Closing::No
}

/// Splits an ASCII-auth data block into `(user, password)`.
///
/// Exactly one space separates them; a password may not contain one, which is
/// upstream's limitation and not worth diverging over.
///
/// The command's key also names the identity, and upstream repeats it in the
/// block — but only the block is read here. Both orderings are observably the
/// same: a block naming a different user either fails to verify, or verifies as
/// the user it actually named, and upstream answers `authentication failure`
/// for a mismatch either way.
fn split_credential(block: &[u8]) -> Option<(&[u8], &[u8])> {
    let space = block.iter().position(|b| *b == b' ')?;
    let (name, rest) = block.split_at(space);
    let secret = &rest[1..];
    if name.is_empty() || secret.is_empty() || secret.contains(&b' ') {
        return None;
    }
    Some((name, secret))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Closing {
    Yes,
    No,
}

/// Maps an internal status onto the memcached error vocabulary.
fn memcached_error(status: Status) -> vash_proto::memcached::ErrorKind {
    use vash_proto::memcached::ErrorKind;

    // The wording is memcached's, verbatim: the differential suite compares
    // response bytes against a real server.
    match status {
        Status::TooLarge => ErrorKind::Server("object too large for cache"),
        Status::BadRequest => ErrorKind::Client("bad command line format"),
        Status::NotNumeric => ErrorKind::Client("cannot increment or decrement non-numeric value"),
        Status::Unsupported => ErrorKind::Error,
        Status::Unauthorized => ErrorKind::Client("command disabled by configuration"),
        Status::CapacityFull => ErrorKind::Server("out of memory storing object"),
        Status::Overloaded => ErrorKind::Server("server is overloaded"),
        Status::NotStored => ErrorKind::Server("not stored"),
        _ => ErrorKind::Server("internal error"),
    }
}

/// Executes every complete VCP frame in `block`, appending the responses.
///
/// The block was already measured by the caller to end on a frame boundary, so
/// the lengths here cannot run short. Its shape mirrors
/// [`execute_memcached_block`] and `resp::execute_block` so that all three
/// dialects can share one drain — see [`crate::conn`].
///
/// VCP has no command that closes the connection: `QUIT` is answered and the
/// socket stays open, because a client that wants it gone can simply drop it.
pub fn execute_vcp_block(
    state: &ServerState,
    conn: &mut ConnAuth,
    block: &[u8],
    out: &mut Vec<u8>,
) -> Closing {
    use vash_proto::vcp::{FrameLen, peek_frame_len};

    let mut rest = block;
    while !rest.is_empty() {
        let FrameLen::Complete(len) = peek_frame_len(rest) else {
            // Unreachable: the caller only includes whole frames. Stopping
            // rather than looping keeps a logic slip from spinning a core.
            error!("a VCP block did not end on a frame boundary");
            break;
        };
        execute_frame_into(state, conn, &rest[..len], out);
        rest = &rest[len..];
    }
    Closing::No
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
    frame: &[u8],
    out: &mut Vec<u8>,
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
        state.metrics.auth_refused();
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
        }) => execute_auth(state, conn, request_id, &auth, out),

        Ok(Decoded::Request { mut request, .. }) => {
            offsets_to_store_ttls(&mut request.command);

            // The fused read path. A `NO_REPLY` `GET` is excluded on purpose:
            // it has nowhere to render into, and sending it round the ordinary
            // path keeps that case answering exactly as it did.
            if let Command::Get { key } = request.command
                && !request.no_reply
            {
                let (opcode, request_id) = (request.opcode, request.request_id);
                let rendered =
                    execute_get_into(state, key, CommandKind::Get, Dialect::Vcp, &mut |value| {
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

            let result = execute(state, &request.command, Dialect::Vcp);

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
            warn!(request_id, opcode, detail, "rejected malformed request");
            encode_error(out, opcode, request_id, status);
        }

        // The caller only passes frames whose length it already validated, so
        // neither of these is reachable. Answering rather than panicking keeps a
        // logic slip from taking the process down.
        Ok(Decoded::Incomplete { .. }) | Err(DecodeError::Fatal { .. }) => {
            error!("frame passed to execute_frame_into was not a complete, valid frame");
            encode_error(out, 0, 0, Status::Internal);
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
}

/// Whether a frame must be refused because the connection has not
/// authenticated, decided from the twelve-byte header without touching the
/// body.
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
    if conn.is_authenticated() || !state.auth.required() {
        return None;
    }
    // Short of a header there is nothing to refuse *with* — no request id to
    // echo. The decoder below reports it as the malformed frame it is.
    if frame.len() < vash_proto::vcp::HEADER_LEN {
        return None;
    }

    let opcode = frame[0];
    if opcode == Opcode::Hello as u8 || opcode == Opcode::Auth as u8 {
        return None;
    }

    Some(Refused {
        opcode,
        request_id: u32::from_le_bytes(frame[4..8].try_into().expect("four bytes")),
        no_reply: frame[1] & vash_proto::vcp::flags::NO_REPLY != 0,
    })
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
                        name = %String::from_utf8_lossy(name),
                        failures = conn.failures(),
                        "authentication failed"
                    );
                    state.metrics.auth_failed();
                    Status::Unauthorized
                }
            }
        }
    };

    encode_response(out, Opcode::Auth, request_id, status, &[]);
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

/// Executes a single-key `GET`, rendering a hit **while the store still has the
/// value in hand** — inside the read transaction, straight into the write
/// buffer — instead of copying it into a [`Reply`] on the way past. Returns
/// whether the key was live; the caller renders the miss in its own dialect.
///
/// This is the one command that steps around the `Reply` boundary, and it earns
/// it: `hot_path.rs` measures the copy it removes at ~200ns per read at 1 KiB
/// and ~400ns at 4 KiB, plus the allocation that building a `Value` needs on
/// every single hit.
///
/// The cost of stepping around that boundary is that the counting `execute`
/// does has to be reproduced here. For a `GET` that is exactly two things — the
/// command histogram and the hit/miss split — because [`Outcomes::observe`] has
/// no `Command::Get` arm, and none for `Command::GetMany` either. That is
/// load-bearing rather than lucky, so
/// `metrics::tests::observe_has_nothing_to_say_about_a_get` fails if anyone
/// adds one.
///
/// `kind` is which histogram to charge, and is **not** always
/// [`CommandKind::Get`]: a plain memcached `get` naming one key comes through
/// here too, and it is a `get_many` as far as an operator reading `stats` is
/// concerned. Taking it as an argument is what keeps the fused path from
/// quietly moving a command from one counter to another.
///
/// [`Outcomes::observe`]: crate::metrics::Outcomes::observe
pub fn execute_get_into(
    state: &ServerState,
    key: vash_core::Key<'_>,
    kind: CommandKind,
    dialect: Dialect,
    render: &mut dyn FnMut(vash_core::ValueRef<'_>),
) -> Result<bool, Failed> {
    // Timed and counted exactly as `execute` does it, including observing the
    // histogram before the error is propagated: a read that failed still took
    // the time it took.
    let started = std::time::Instant::now();
    let outcome = state.store.get_with(key, render);
    state
        .metrics
        .commands
        .observe(kind, dialect, started.elapsed());

    let hit = outcome.map_err(to_failed)?;
    state.metrics.read(u64::from(hit), u64::from(!hit));
    Ok(hit)
}

pub fn execute(
    state: &ServerState,
    command: &Command<'_>,
    dialect: Dialect,
) -> Result<Reply, Failed> {
    // Timed around `execute_inner` only, so this is the storage work and not the
    // wait to reach a thread that could do it. The wait is the shard writer's
    // and is measured there — subtracting the two is what answers plan §12's
    // question of whether the writer is the bottleneck.
    let started = std::time::Instant::now();
    let outcome = execute_inner(state, command);
    state
        .metrics
        .commands
        .observe(CommandKind::of(command), dialect, started.elapsed());

    // Counted here, at the single point every command passes through, so the
    // numbers cannot drift apart from what was actually served.
    match &outcome {
        Ok(reply) => {
            // What the command *found*, as opposed to how many ran: the
            // per-command hit and miss splits, and the stores that applied.
            state.metrics.outcomes.observe(command, reply);
            match reply {
                Reply::Value(_) => state.metrics.read(1, 0),
                Reply::Values(values) => {
                    let hits = values.iter().filter(|v| v.is_some()).count() as u64;
                    state.metrics.read(hits, values.len() as u64 - hits);
                }
                // A deadline query is a read that never copies a value: `TTL`,
                // `TYPE` and `EXISTS` all reduce to one, and all count their hits
                // the same way a `GET` does.
                Reply::Deadline(deadline) => state
                    .metrics
                    .read(u64::from(deadline.is_some()), u64::from(deadline.is_none())),
                Reply::Deadlines(deadlines) => {
                    let live = deadlines.iter().filter(|d| d.is_some()).count() as u64;
                    state.metrics.read(live, deadlines.len() as u64 - live);
                }
                Reply::NotFound if is_read(command) => state.metrics.read(0, 1),
                // A skipped conditional change wrote nothing, so it is not counted
                // as a write — the same rule the arithmetic path applies through
                // `Applied::wrote`.
                Reply::Applied(false) => state.metrics.other(),
                Reply::Arithmetic(applied) if !applied.wrote => state.metrics.other(),
                Reply::Stored(_)
                | Reply::Swapped { .. }
                | Reply::StoredMany(_)
                | Reply::Applied(true)
                | Reply::Length(_)
                | Reply::Deleted
                | Reply::DeletedMany(_)
                | Reply::Touched
                | Reply::Arithmetic(_)
                | Reply::Invalidated(_)
                | Reply::Flushed(_) => state.metrics.write(),
                _ => state.metrics.other(),
            }
        }
        Err(failed) => {
            state.metrics.other();
            // Its own counter as well as an error class: a client writing values
            // past the ceiling is a different problem from a full map, and
            // memcached reports the two separately for that reason.
            if failed.status == Status::TooLarge {
                state
                    .metrics
                    .outcomes
                    .store_too_large
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            state.metrics.error(match failed.status {
                Status::CapacityFull => ErrorClass::Capacity,
                Status::Overloaded => ErrorClass::Overloaded,
                Status::Internal => ErrorClass::Internal,
                _ => ErrorClass::Client,
            });
        }
    }

    outcome
}

fn is_read(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::Get { .. }
            | Command::GetMany(_)
            | Command::GetAndTouch { .. }
            | Command::Deadline { .. }
            | Command::Deadlines(_)
    )
}

fn execute_inner(state: &ServerState, command: &Command<'_>) -> Result<Reply, Failed> {
    match command {
        Command::Ping => Ok(Reply::Pong),

        Command::Hello { protocol_version } => {
            if *protocol_version != vash_core::PROTOCOL_VERSION {
                warn!(
                    client = protocol_version,
                    server = vash_core::PROTOCOL_VERSION,
                    "client requested an unsupported protocol version"
                );
                return Err(Failed::status(Status::Unsupported));
            }
            Ok(Reply::Hello(state.info))
        }

        Command::Get { key } => match state.store.get(*key).map_err(to_failed)? {
            Some(value) => Ok(Reply::Value(value)),
            None => Ok(Reply::NotFound),
        },

        Command::GetMany(keys) => Ok(Reply::Values(
            state.store.get_many(keys).map_err(to_failed)?,
        )),

        Command::Set(set) => {
            let written = state.store.store(set).map_err(to_failed)?;
            // The reply shape follows the request: only `SET … GET` asks to be
            // told what it displaced, and no VCP or memcached request does.
            if set.return_previous {
                Ok(Reply::Swapped {
                    outcome: written.outcome,
                    previous: written.previous,
                })
            } else {
                Ok(Reply::Stored(written.outcome))
            }
        }

        Command::GetAndTouch { keys, ttl_secs } => Ok(Reply::Values(
            state
                .store
                .get_and_touch(keys, *ttl_secs)
                .map_err(to_failed)?,
        )),

        Command::Deadline { key } => Ok(Reply::Deadline(
            state.store.deadline(*key).map_err(to_failed)?,
        )),

        Command::Deadlines(keys) => Ok(Reply::Deadlines(
            state.store.deadlines(keys).map_err(to_failed)?,
        )),

        Command::Expire { key, ttl, guard } => Ok(Reply::Applied(
            state.store.expire(*key, *ttl, *guard).map_err(to_failed)?,
        )),

        Command::Append { key, suffix } => Ok(Reply::Length(
            state.store.append(*key, suffix).map_err(to_failed)?,
        )),

        // Every dialect's arithmetic, in one arm: the operation already carries
        // its own numeric domain, so there are no semantics left here to get
        // wrong. A miss is only reachable for memcached's counter, which is the
        // one that does not create.
        Command::Arithmetic(op) => match state.store.arithmetic(op).map_err(to_failed)? {
            Some(applied) => Ok(Reply::Arithmetic(applied)),
            None => Ok(Reply::NotFound),
        },

        Command::Stats => Ok(Reply::Stats(crate::stats::collect(state))),
        Command::Version => Ok(Reply::Version(vash_proto::memcached::encode::VERSION)),
        Command::Quit => Ok(Reply::Closing),

        // An unguarded batch takes the plain path, which is what VCP and
        // memcached always send. A guard that rejects reports `NotStored`,
        // reusing the vocabulary conditional single writes already have rather
        // than inventing a batch-shaped one.
        Command::SetMany { sets, guard } => match guard {
            vash_core::BatchGuard::Always => Ok(Reply::StoredMany(
                state.store.set_many(sets).map_err(to_failed)?,
            )),
            guard => {
                if state.store.set_many_if(sets, *guard).map_err(to_failed)? {
                    Ok(Reply::StoredMany(Vec::new()))
                } else {
                    Ok(Reply::Stored(vash_core::Stored::NotStored))
                }
            }
        },

        Command::Delete { key } => {
            if state.store.delete(*key).map_err(to_failed)? {
                Ok(Reply::Deleted)
            } else {
                Ok(Reply::NotFound)
            }
        }

        Command::DeleteMany(keys) => Ok(Reply::DeletedMany(
            state.store.delete_many(keys).map_err(to_failed)?,
        )),

        Command::Touch { key, ttl_secs } => {
            if state.store.touch(*key, *ttl_secs).map_err(to_failed)? {
                Ok(Reply::Touched)
            } else {
                Ok(Reply::NotFound)
            }
        }

        Command::DeleteByTag { tag } => {
            let generation = state.store.delete_by_tag(tag).map_err(to_failed)?;

            // Propagated only once the local bump has committed, so a peer can
            // never be told about an invalidation this node did not perform. In
            // `fanout_sync` this blocks until reachable peers acknowledge; in
            // `fanout` it is a queue push.
            if let Some(generation) = generation {
                state.cluster.invalidate(tag, generation);
            }
            Ok(Reply::Invalidated(generation.is_some()))
        }

        Command::TagSync { full, entries } => Ok(Reply::TagSync(tag_sync(state, *full, entries)?)),

        Command::Cluster => Ok(Reply::Cluster(state.cluster.view())),

        Command::Flush => {
            if !state.protocol.flush_enabled {
                // A remote cache-wipe primitive stays off unless deliberately
                // enabled.
                warn!("rejected flush: disabled by configuration");
                return Err(Failed::status(Status::Unauthorized));
            }
            Ok(Reply::Flushed(state.store.flush().map_err(to_failed)?))
        }

        Command::ListKeys(request) => {
            listing_gate(state)?;
            Ok(Reply::Listing(
                state
                    .store
                    .list_keys(request, state.protocol.listing_max_scan)
                    .map_err(to_failed)?,
            ))
        }

        Command::ListTags(request) => {
            listing_gate(state)?;
            Ok(Reply::Listing(
                state.store.list_tags(request).map_err(to_failed)?,
            ))
        }
    }
}

/// Refuses a listing when enumeration is not enabled here.
///
/// Separate from the authentication gate and not redundant with it: that one
/// decides *who may connect*, this one decides what any connected client may
/// do, and every authenticated client on this server is equally trusted.
fn listing_gate(state: &ServerState) -> Result<(), Failed> {
    if !state.protocol.listing_enabled {
        warn!("rejected listing: disabled by configuration");
        return Err(Failed::status(Status::Unauthorized));
    }
    Ok(())
}

/// Answers a peer's `TAG_SYNC`: merge what it knows, report what it is behind
/// on.
///
/// The reply is computed from a snapshot taken **before** the merge. Afterwards
/// every offered generation is one this node also holds, so "we know better"
/// would be true of nothing and the peer would never learn anything from us.
fn tag_sync(
    state: &ServerState,
    full: bool,
    entries: &[(&[u8], u64)],
) -> Result<Vec<vash_core::TagGeneration>, Failed> {
    use std::collections::{HashMap, HashSet};

    let known = state.store.tag_generations().map_err(to_failed)?;
    let ours: HashMap<&[u8], u64> = known
        .iter()
        .map(|entry| (&*entry.name, entry.generation))
        .collect();

    let mut behind = Vec::new();
    for (name, offered) in entries {
        if let Some(mine) = ours.get(*name).copied()
            && mine > *offered
        {
            behind.push(vash_core::TagGeneration::new(*name, mine));
        }
    }

    // Only a peer that listed its whole table can be told about tags it never
    // mentioned; against a partial push there is no way to tell "does not know
    // it" from "did not fit in this message".
    if full {
        let named: HashSet<&[u8]> = entries.iter().map(|(name, _)| *name).collect();
        for entry in &known {
            if entry.generation > 0 && !named.contains(&*entry.name) {
                behind.push(entry.clone());
            }
        }
    }
    behind.truncate(vash_core::MAX_TAG_SYNC_ENTRIES);

    let offered: Vec<vash_core::TagGeneration> = entries
        .iter()
        .map(|(name, generation)| vash_core::TagGeneration::new(*name, *generation))
        .collect();
    state
        .cluster
        .metrics()
        .merged(crate::cluster::apply_merges(&*state.store, &offered));

    Ok(behind)
}

/// Maps a storage failure onto the status the client sees.
///
/// Internal failures are logged here and reported as a bare `INTERNAL`: the
/// client gets enough to retry or fall back, and nothing about the server's
/// internals leaks onto the wire.
/// Why a command could not be executed, classified **once** for every dialect.
///
/// `status` is the VCP status code, which memcached also renders from.
/// `cause` is the domain failure underneath it, carried because Redis draws
/// distinctions memcached has no vocabulary for: four arithmetic failures all
/// land on `NotNumeric`, and a Redis client library matches on the message text
/// of each.
///
/// Carrying the cause is what lets there be one classification. The alternative
/// — and what this replaced — was the Redis adapter classifying `StoreError`
/// again on its own, two independent judgements that had to agree forever about
/// which storage failures are the client's fault.
#[derive(Debug, Clone)]
pub struct Failed {
    pub status: Status,
    pub cause: Option<vash_core::CoreError>,
}

impl Failed {
    /// A failure with no domain cause worth reporting: capacity, overload, a
    /// gate, an internal error.
    pub fn status(status: Status) -> Self {
        Self {
            status,
            cause: None,
        }
    }
}

fn to_failed(err: StoreError) -> Failed {
    use vash_core::CoreError;

    let classified = |status: Status, cause: CoreError| Failed {
        status,
        cause: Some(cause),
    };

    match err {
        StoreError::CapacityFull => Failed::status(Status::CapacityFull),
        StoreError::Overloaded | StoreError::ShuttingDown => Failed::status(Status::Overloaded),
        StoreError::Unsupported(what) => {
            warn!(what, "client used a feature this build does not implement");
            Failed::status(Status::Unsupported)
        }
        StoreError::Core(
            cause @ (CoreError::ValueTooLarge { .. } | CoreError::KeyTooLong { .. }),
        ) => classified(Status::TooLarge, cause),
        StoreError::TagLimit(limit) => {
            warn!(limit, "tag registry is full");
            Failed::status(Status::CapacityFull)
        }
        // memcached reports a non-numeric value as a client error, not a miss,
        // and with its own wording — see `memcached_error`. The three arithmetic
        // failures it has no vocabulary for are folded into the same status,
        // because upstream's counter cannot produce them: it wraps instead of
        // overflowing and has no float mode. Redis can, and tells them apart
        // from `cause`.
        StoreError::Core(
            cause @ (CoreError::NotAnInteger
            | CoreError::NotAFloat
            | CoreError::Overflow
            | CoreError::NotFinite),
        ) => classified(Status::NotNumeric, cause),
        StoreError::Core(cause) => classified(Status::BadRequest, cause),
        other => {
            error!(error = %other, "storage failure");
            Failed::status(Status::Internal)
        }
    }
}

/// Builds the handshake response advertised to clients.
///
/// Every bit here reports what this node *does*, never what the build knows
/// how to do. `cluster` is whether this node actually forwards invalidations to
/// peers: claiming the capability on a node with no peers configured would make
/// a client trust a cluster-wide invalidation that stops at one node. The
/// command and dialect gates in `protocol` follow the same rule — a client that
/// cannot tell "disabled here" from "too old to know it" has to discover the
/// answer by trying, and for `FLUSH` that means wiping the cache to find out.
pub fn server_info(
    protocol: crate::config::ProtocolConfig,
    shards: u16,
    max_value_len: usize,
    max_tags_per_record: usize,
    cluster: bool,
    auth_required: bool,
) -> ServerInfo {
    let mut capabilities = vash_core::capability::TAGS;
    if cluster {
        capabilities |= vash_core::capability::CLUSTER;
    }
    if protocol.listing_enabled {
        capabilities |= vash_core::capability::LISTING;
    }
    if protocol.flush_enabled {
        capabilities |= vash_core::capability::FLUSH;
    }
    // A disabled dialect closes the connection at detection rather than
    // answering, so these two bits are the only warning a client gets — and the
    // client that needs the warning cannot read them, since it does not speak
    // VCP. They are for the operator's own tooling and for a client that speaks
    // more than one dialect.
    if protocol.memcached_enabled {
        capabilities |= vash_core::capability::MEMCACHED;
    }
    if protocol.resp_enabled {
        capabilities |= vash_core::capability::RESP;
    }
    // Set from enforcement, not from the build supporting it: a client reads
    // this as "you must authenticate on this connection".
    if auth_required {
        capabilities |= vash_core::capability::AUTH_REQUIRED;
    }

    ServerInfo {
        protocol_version: vash_core::PROTOCOL_VERSION,
        shards,
        max_key_len: vash_core::MAX_KEY_LEN as u32,
        max_value_len: max_value_len as u32,
        capabilities,
        // Validation caps this at `ABSOLUTE_MAX_TAGS`, so the cast cannot lose
        // anything a client would then enforce wrongly.
        max_tags_per_record: max_tags_per_record as u16,
    }
}
