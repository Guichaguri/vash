//! Command execution: the only place that maps domain outcomes onto wire status
//! codes.

use tracing::{error, warn};
use vash_core::{Command, Reply, ServerInfo};
use vash_proto::vcp::{
    AuthRequest, DecodeError, Decoded, Opcode, Status, decode, encode_error, encode_reply,
    encode_response,
};
use vash_store::{Store, StoreError};

use crate::auth::{ConnAuth, DEFAULT_NAME, Mechanism};
use crate::metrics::ErrorClass;
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
                if !conn.is_authenticated() && state.auth.current().required() {
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

    if !conn.is_authenticated() && state.auth.current().required() {
        return execute_memcached_unauthenticated(state, conn, parsed, out);
    }

    let result = execute(state, &parsed.command);
    let closing = matches!(result, Ok(Reply::Closing));

    match result {
        Ok(reply) => {
            // `noreply` suppresses the response but never the work.
            if !parsed.noreply {
                mc::encode(out, &parsed.style, &parsed.command, &reply);
            }
        }
        Err(status) => {
            if !parsed.noreply {
                mc::encode_error(out, memcached_error(status));
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
            let result = execute(state, &request.command);

            if request.no_reply {
                if let Err(status) = result {
                    warn!(?status, opcode = ?request.opcode, "no-reply request failed");
                }
                return;
            }

            match result {
                Ok(reply) => encode_reply(out, request.opcode, request.request_id, &reply),
                Err(status) => encode_error(out, request.opcode as u8, request.request_id, status),
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
    if conn.is_authenticated() || !state.auth.current().required() {
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
        Command::Set(set) => set.ttl_secs = clock.ttl_from_offset(set.ttl_secs),
        Command::SetMany(sets) => {
            for set in sets {
                set.ttl_secs = clock.ttl_from_offset(set.ttl_secs);
            }
        }
        Command::Touch { ttl_secs, .. } => *ttl_secs = clock.ttl_from_offset(*ttl_secs),
        _ => {}
    }
}

fn execute(state: &ServerState, command: &Command<'_>) -> Result<Reply, Status> {
    let outcome = execute_inner(state, command);

    // Counted here, at the single point every command passes through, so the
    // numbers cannot drift apart from what was actually served.
    match &outcome {
        Ok(reply) => match reply {
            Reply::Value(_) => state.metrics.read(1, 0),
            Reply::Values(values) => {
                let hits = values.iter().filter(|v| v.is_some()).count() as u64;
                state.metrics.read(hits, values.len() as u64 - hits);
            }
            Reply::NotFound if is_read(command) => state.metrics.read(0, 1),
            Reply::Stored(_)
            | Reply::StoredMany(_)
            | Reply::Deleted
            | Reply::DeletedMany(_)
            | Reply::Touched
            | Reply::Counter(_)
            | Reply::Invalidated(_)
            | Reply::Flushed(_) => state.metrics.write(),
            _ => state.metrics.other(),
        },
        Err(status) => {
            state.metrics.other();
            state.metrics.error(match status {
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
        Command::Get { .. } | Command::GetMany(_) | Command::GetAndTouch { .. }
    )
}

fn execute_inner(state: &ServerState, command: &Command<'_>) -> Result<Reply, Status> {
    match command {
        Command::Ping => Ok(Reply::Pong),

        Command::Hello { protocol_version } => {
            if *protocol_version != vash_core::PROTOCOL_VERSION {
                warn!(
                    client = protocol_version,
                    server = vash_core::PROTOCOL_VERSION,
                    "client requested an unsupported protocol version"
                );
                return Err(Status::Unsupported);
            }
            Ok(Reply::Hello(state.info))
        }

        Command::Get { key } => match state.store.get(*key).map_err(to_status)? {
            Some(value) => Ok(Reply::Value(value)),
            None => Ok(Reply::NotFound),
        },

        Command::GetMany(keys) => Ok(Reply::Values(
            state.store.get_many(keys).map_err(to_status)?,
        )),

        Command::Set(set) => Ok(Reply::Stored(state.store.store(set).map_err(to_status)?)),

        Command::GetAndTouch { keys, ttl_secs } => Ok(Reply::Values(
            state
                .store
                .get_and_touch(keys, *ttl_secs)
                .map_err(to_status)?,
        )),

        Command::Incr {
            key,
            delta,
            decrement,
        } => match state
            .store
            .incr(*key, *delta, *decrement)
            .map_err(to_status)?
        {
            Some(value) => Ok(Reply::Counter(value)),
            None => Ok(Reply::NotFound),
        },

        Command::Stats => Ok(Reply::Stats(collect_stats(state))),
        Command::Version => Ok(Reply::Version(vash_proto::memcached::encode::VERSION)),
        Command::Quit => Ok(Reply::Closing),

        Command::SetMany(sets) => Ok(Reply::StoredMany(
            state.store.set_many(sets).map_err(to_status)?,
        )),

        Command::Delete { key } => {
            if state.store.delete(*key).map_err(to_status)? {
                Ok(Reply::Deleted)
            } else {
                Ok(Reply::NotFound)
            }
        }

        Command::DeleteMany(keys) => Ok(Reply::DeletedMany(
            state.store.delete_many(keys).map_err(to_status)?,
        )),

        Command::Touch { key, ttl_secs } => {
            if state.store.touch(*key, *ttl_secs).map_err(to_status)? {
                Ok(Reply::Touched)
            } else {
                Ok(Reply::NotFound)
            }
        }

        Command::DeleteByTag { tag } => {
            let generation = state.store.delete_by_tag(tag).map_err(to_status)?;

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
            if !state.flush_enabled {
                // A remote cache-wipe primitive stays off unless deliberately
                // enabled.
                warn!("rejected flush: disabled by configuration");
                return Err(Status::Unauthorized);
            }
            Ok(Reply::Flushed(state.store.flush().map_err(to_status)?))
        }

        Command::ListKeys(request) => {
            listing_gate(state)?;
            Ok(Reply::Listing(
                state
                    .store
                    .list_keys(request, state.listing_max_scan)
                    .map_err(to_status)?,
            ))
        }

        Command::ListTags(request) => {
            listing_gate(state)?;
            Ok(Reply::Listing(
                state.store.list_tags(request).map_err(to_status)?,
            ))
        }
    }
}

/// Refuses a listing when enumeration is not enabled here.
///
/// Separate from the authentication gate and not redundant with it: that one
/// decides *who may connect*, this one decides what any connected client may
/// do, and every authenticated client on this server is equally trusted.
fn listing_gate(state: &ServerState) -> Result<(), Status> {
    if !state.listing_enabled {
        warn!("rejected listing: disabled by configuration");
        return Err(Status::Unauthorized);
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
) -> Result<Vec<vash_core::TagGeneration>, Status> {
    use std::collections::{HashMap, HashSet};

    let known = state.store.tag_generations().map_err(to_status)?;
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
        .merged(crate::cluster::apply_merges(&state.store, &offered));

    Ok(behind)
}

/// Maps a storage failure onto the status the client sees.
///
/// Internal failures are logged here and reported as a bare `INTERNAL`: the
/// client gets enough to retry or fall back, and nothing about the server's
/// internals leaks onto the wire.
fn to_status(err: StoreError) -> Status {
    use vash_core::CoreError;

    match err {
        StoreError::CapacityFull => Status::CapacityFull,
        StoreError::Overloaded | StoreError::ShuttingDown => Status::Overloaded,
        StoreError::Unsupported(what) => {
            warn!(what, "client used a feature this build does not implement");
            Status::Unsupported
        }
        StoreError::Core(CoreError::ValueTooLarge { .. } | CoreError::KeyTooLong { .. }) => {
            Status::TooLarge
        }
        StoreError::TagLimit(limit) => {
            warn!(limit, "tag registry is full");
            Status::CapacityFull
        }
        // memcached reports this as a client error, not a miss, and with its
        // own wording — see `memcached_error`.
        StoreError::NotNumeric => Status::NotNumeric,
        StoreError::Core(_) => Status::BadRequest,
        other => {
            error!(error = %other, "storage failure");
            Status::Internal
        }
    }
}

/// The `stats` payload.
///
/// A subset of memcached's counters, restricted to what this server actually
/// measures — plus its own. Reporting a plausible-looking zero for something we
/// do not track would mislead any dashboard reading it.
fn collect_stats(state: &ServerState) -> Vec<(String, String)> {
    let mut stats = vec![
        ("pid".into(), std::process::id().to_string()),
        (
            "version".into(),
            vash_proto::memcached::encode::VERSION.into(),
        ),
        ("pointer_size".into(), usize::BITS.to_string()),
    ];

    match state.store.stats() {
        Ok(s) => stats.extend([
            ("curr_items".into(), s.entries.to_string()),
            ("bytes".into(), s.used_bytes.to_string()),
            ("limit_maxbytes".into(), s.map_size.to_string()),
            // Beyond memcached's set, but they are what this server is actually
            // about.
            ("vash_utilisation".into(), format!("{:.4}", s.utilisation)),
            ("vash_expiry_entries".into(), s.expiry_entries.to_string()),
            ("vash_tags".into(), s.tags.to_string()),
            (
                "vash_tag_index_entries".into(),
                s.tag_index_entries.to_string(),
            ),
            (
                "vash_pending_reclaims".into(),
                s.pending_reclaims.to_string(),
            ),
            ("vash_commits".into(), s.commits.to_string()),
            ("vash_committed_ops".into(), s.committed_ops.to_string()),
            (
                "vash_mean_batch".into(),
                format!("{:.2}", s.mean_batch_size()),
            ),
            ("vash_sweeps".into(), s.sweeps.to_string()),
            ("vash_reclaimed".into(), s.reclaimed.to_string()),
            ("vash_tag_reclaimed".into(), s.tag_reclaimed.to_string()),
            ("vash_sweep_lag_ms".into(), s.sweep_lag_ms.to_string()),
            ("vash_epoch".into(), s.epoch.to_string()),
            ("vash_readers_in_use".into(), s.readers_in_use.to_string()),
        ]),
        Err(e) => error!(error = %e, "could not read store stats"),
    }

    let view = state.cluster.view();
    stats.extend([
        ("vash_cluster_mode".into(), view.mode.as_str().to_string()),
        ("vash_cluster_peers".into(), view.peers.len().to_string()),
        (
            "vash_cluster_peers_reachable".into(),
            state.cluster.peers_reachable().to_string(),
        ),
    ]);

    stats
}

/// Builds the handshake response advertised to clients.
///
/// `cluster` is whether this node actually forwards invalidations to peers —
/// not merely whether the build supports it. Claiming the capability on a node
/// with no peers configured would make a client trust a cluster-wide
/// invalidation that stops at one node.
pub fn server_info(
    shards: u16,
    max_value_len: usize,
    cluster: bool,
    auth_required: bool,
    listing: bool,
) -> ServerInfo {
    let mut capabilities = vash_core::capability::TAGS | vash_core::capability::MEMCACHED;
    if cluster {
        capabilities |= vash_core::capability::CLUSTER;
    }
    // Enablement, not support: the bit says the listing opcodes will answer
    // here, so a client discovers them without probing and without reading an
    // `UNAUTHORIZED` as "this build has no such command".
    if listing {
        capabilities |= vash_core::capability::LISTING;
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
    }
}
