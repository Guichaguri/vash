use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;
use vash_proto::memcached::{self, Outcome, ProtocolError};
use vash_proto::vcp::{FrameLen, peek_frame_len};
use vash_proto::{Protocol, detect};

use crate::auth::ConnAuth;
use crate::dispatch::{Closing, execute_frame_into, execute_memcached_block};
use crate::state::ServerState;

/// Most bytes an unauthenticated connection may have buffered, or ask the
/// server to reserve.
///
/// Generous against everything legal before authenticating — a VCP `AUTH` at
/// both ceilings is under 600 bytes, and the two text dialects' credentials are
/// one short line each — and small enough that filling the pre-auth connection
/// budget costs an attacker nothing worth having.
const PRE_AUTH_MAX_BUFFERED: usize = 4096;

/// Serves one connection until the peer disconnects or sends something
/// unintelligible.
///
/// The dialect is settled by the first byte and never revisited — see
/// [`vash_proto::detect`].
pub async fn handle(
    mut stream: TcpStream,
    state: Arc<ServerState>,
    read_buffer: usize,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    // Released the moment this connection authenticates, so the pre-auth cap
    // counts connections that have presented nothing rather than connections in
    // total. `None` when authentication is not being enforced.
    mut pre_auth: Option<tokio::sync::OwnedSemaphorePermit>,
) -> std::io::Result<()> {
    // Cache traffic is small and latency-sensitive; Nagle would batch a reply
    // against the next one and add up to 40ms for nothing.
    stream.set_nodelay(true)?;

    let mut read_buf = BytesMut::with_capacity(read_buffer);
    let mut write_buf: Vec<u8> = Vec::with_capacity(read_buffer);
    let mut protocol: Option<Protocol> = None;
    // Redis connections start at RESP2 and move to RESP3 only if the client
    // asks. Per-connection state, because that is exactly what `HELLO`
    // negotiates.
    let mut resp_version = vash_proto::resp::Version::default();
    // Likewise per connection: authentication is a property of this socket, and
    // it dies with it.
    let mut conn_auth = ConnAuth::default();

    let limits = state.auth.limits;
    let enforcing = state.auth.current().required();
    // An unauthenticated connection is the one thing here a stranger can
    // create, so it gets a deadline rather than the ordinary idle treatment. It
    // is folded into the `select!` that already handles shutdown, so it costs
    // no timer task of its own.
    let deadline = tokio::time::Instant::now() + limits.timeout;

    loop {
        // Shutdown is only ever noticed *here*, between requests, where nothing
        // is buffered and no reply is outstanding — so a client loses at most a
        // connection it was not using. Both branches are cancel-safe, so the
        // one that does not win has read nothing.
        //
        // Without this, an idle connection would hold the drain open until it
        // timed out, and the store could not be closed. That is not a corner
        // case in a cluster: peers keep their connections open indefinitely, so
        // every node would have one per peer.
        let unauthenticated = enforcing && !conn_auth.is_authenticated();
        let n = tokio::select! {
            read = stream.read_buf(&mut read_buf) => read?,
            _ = shutdown.changed() => {
                debug!("closing an idle connection to drain");
                return Ok(());
            }
            _ = tokio::time::sleep_until(deadline), if unauthenticated => {
                debug!(timeout = ?limits.timeout, "closing a connection that never authenticated");
                state.metrics.auth_timeout();
                return Ok(());
            }
        };
        if n == 0 {
            return Ok(()); // clean disconnect
        }

        // A connection that has presented nothing must not be able to make the
        // server hold arbitrary bytes. Everything legal before authenticating
        // is small — a VCP `HELLO` is 16 bytes and an `AUTH` at both ceilings
        // is under 600, and the two text dialects' credentials are one short
        // line each — so this is far above any honest pre-auth traffic and far
        // below anything worth holding.
        if unauthenticated && read_buf.len() > PRE_AUTH_MAX_BUFFERED {
            debug!(
                buffered = read_buf.len(),
                "closing an unauthenticated connection that buffered too much"
            );
            return Ok(());
        }

        if protocol.is_none() {
            match detect(&read_buf) {
                None => continue, // still empty
                Some(Ok(chosen)) => {
                    debug!(?chosen, "protocol selected");
                    protocol = Some(chosen);
                }
                Some(Err(unknown)) => {
                    debug!(
                        byte = unknown.0,
                        "closing connection: unrecognised protocol"
                    );
                    return Ok(());
                }
            }
        }

        let keep_going = match protocol.expect("set above") {
            Protocol::Vcp => {
                drain_vcp(&state, &mut conn_auth, &mut read_buf, &mut write_buf).await?
            }
            Protocol::Memcached => {
                drain_memcached(&state, &mut conn_auth, &mut read_buf, &mut write_buf).await?
            }
            Protocol::Resp => {
                drain_resp(
                    &state,
                    &mut conn_auth,
                    &mut read_buf,
                    &mut resp_version,
                    &mut write_buf,
                )
                .await?
            }
        };

        if pre_auth.is_some() && conn_auth.is_authenticated() {
            pre_auth = None;
        }

        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
            write_buf.clear();
        }
        if !keep_going {
            return Ok(());
        }

        // Bounds guessing on one connection. Deliberately not a lockout across
        // connections: an attacker who could trigger one would have a denial of
        // service against the legitimate holder instead of a break-in, which is
        // not a trade worth making. The reply above has already been written,
        // so the client learns why before the socket goes.
        if conn_auth.failures() >= limits.max_attempts {
            debug!(
                failures = conn_auth.failures(),
                "closing a connection after too many failed authentications"
            );
            return Ok(());
        }
    }
}

/// Handles every complete VCP frame in the buffer. Returns `false` when the
/// connection must close.
///
/// **Every buffered frame goes to the storage tier in one hop.** The hop is a
/// thread handoff, and a handoff costs far more than executing a cached
/// request: measured on Windows, one per frame capped a pipelined connection at
/// roughly 5k operations a second no matter how deep the pipeline, because the
/// depth bought nothing — the frames were still crossing to the pool one at a
/// time. Amortising it over whatever arrived in a single read is what makes
/// pipelining worth anything, and it costs the unpipelined case nothing: one
/// frame in the buffer is still one hop.
async fn drain_vcp(
    state: &Arc<ServerState>,
    conn_auth: &mut ConnAuth,
    read_buf: &mut BytesMut,
    write_buf: &mut Vec<u8>,
) -> std::io::Result<bool> {
    let mut frames: Vec<Bytes> = Vec::new();
    let mut closing = false;

    // Ordered so an authenticated connection never touches the lock.
    let gated = !conn_auth.is_authenticated() && state.auth.current().required();

    loop {
        match peek_frame_len(read_buf) {
            FrameLen::Incomplete { needed } => {
                // `needed` comes from the frame header, which an
                // unauthenticated stranger controls: without this, one 12-byte
                // header claiming a 64 MiB body reserves 64 MiB before anything
                // has been presented. The buffered-bytes check in `handle`
                // cannot cover it, because this reserves against a length that
                // has not arrived.
                if gated && needed > PRE_AUTH_MAX_BUFFERED {
                    debug!(needed, "closing: unauthenticated frame is too large");
                    closing = true;
                    break;
                }
                read_buf.reserve(needed.saturating_sub(read_buf.len()));
                break;
            }
            FrameLen::TooLarge => {
                debug!("closing connection: frame length exceeds the maximum");
                closing = true;
                break;
            }
            // `split_to` hands the frame's bytes over by reference count. No
            // copy, and the decoded key and value borrow directly from it.
            FrameLen::Complete(len) => frames.push(read_buf.split_to(len).freeze()),
        }
    }

    if frames.is_empty() {
        return Ok(!closing);
    }

    // Reads may run here; anything that can write must not, because a write
    // waits on the shard's writer queue and would block this worker — and every
    // other connection it serves — behind it.
    if state.inline_reads && frames.iter().all(|frame| is_read_only(frame)) {
        for frame in &frames {
            execute_frame_into(state, conn_auth, frame, write_buf);
        }
        return Ok(!closing);
    }

    let state = Arc::clone(state);
    // Carried into the blocking task and copied back, exactly as `drain_resp`
    // does with the negotiated version — and for the same reason: an `AUTH` and
    // the commands it authorises can arrive in one pipelined read, so it has to
    // take effect within a block rather than between reads.
    let mut authenticating = conn_auth.clone();
    // Store operations can page-fault or wait on the writer queue, neither of
    // which may happen on a runtime worker.
    let (response, authenticated) = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(frames.len() * 64);
        for frame in &frames {
            execute_frame_into(&state, &mut authenticating, frame, &mut out);
        }
        (out, authenticating)
    })
    .await
    .map_err(std::io::Error::other)?;

    *conn_auth = authenticated;
    write_buf.extend_from_slice(&response);
    Ok(!closing)
}

/// Whether a frame can be answered without any chance of writing.
///
/// Decided from the opcode byte alone — the frame is not decoded twice. An
/// unknown opcode is treated as a write, so a byte nobody recognises takes the
/// safe path rather than the fast one.
fn is_read_only(frame: &[u8]) -> bool {
    frame
        .first()
        .and_then(|byte| vash_proto::vcp::Opcode::from_u8(*byte))
        .is_some_and(|opcode| opcode.is_read_only())
}

/// The memcached equivalent, decided from the already-parsed command.
///
/// Note what is missing: `gat` re-stamps a TTL and `incr` rewrites the value,
/// so despite reading like retrievals they are writes.
fn is_read_only_command(command: &vash_core::Command<'_>) -> bool {
    use vash_core::Command;
    matches!(
        command,
        Command::Get { .. } | Command::GetMany(_) | Command::Version | Command::Stats
    )
}

/// Handles every complete memcached command in the buffer.
///
/// Measures the span of whole commands first, then executes the lot in one hop
/// to the storage tier — see [`drain_vcp`] for why the hop count is what
/// matters. Only the parser can say where a command ends, because a storage
/// command's framing is length-delimited rather than line-delimited: a value
/// may contain CRLF.
async fn drain_memcached(
    state: &Arc<ServerState>,
    conn_auth: &mut ConnAuth,
    read_buf: &mut BytesMut,
    write_buf: &mut Vec<u8>,
) -> std::io::Result<bool> {
    let mut complete = 0usize;
    let mut fatal = false;
    let mut all_reads = true;

    loop {
        match memcached::parse(&read_buf[complete..]) {
            Ok(Outcome::Incomplete) => break,
            Ok(Outcome::Command(parsed)) => {
                all_reads &= is_read_only_command(&parsed.command);
                complete += parsed.consumed;
            }
            // Counted in, not handled here: the error line has to land in the
            // response stream in the position the bad command occupied, which
            // only the executor knows how to do.
            Err(ProtocolError::Recoverable { consumed, .. }) => complete += consumed,
            Err(ProtocolError::Fatal(detail)) => {
                debug!(
                    detail,
                    "closing connection: memcached framing is unrecoverable"
                );
                fatal = true;
                break;
            }
        }
    }

    if complete == 0 {
        return Ok(!fatal);
    }

    if state.inline_reads && all_reads {
        let block: Bytes = read_buf.split_to(complete).freeze();
        let closing = execute_memcached_block(state, conn_auth, &block, write_buf);
        return Ok(closing == Closing::No && !fatal);
    }

    let block: Bytes = read_buf.split_to(complete).freeze();
    let state = Arc::clone(state);
    let mut authenticating = conn_auth.clone();

    // Re-parsed on the blocking thread so the borrowed key and value slices
    // never cross a task boundary, exactly as the VCP path does.
    let (response, closing, authenticated) = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(64);
        let closing = execute_memcached_block(&state, &mut authenticating, &block, &mut out);
        (out, closing, authenticating)
    })
    .await
    .map_err(std::io::Error::other)?;

    *conn_auth = authenticated;
    write_buf.extend_from_slice(&response);
    Ok(closing == Closing::No && !fatal)
}

/// Handles every complete RESP command in the buffer.
///
/// The same two-pass shape as [`drain_memcached`], for the same reason: measure
/// the span of whole commands first, then execute the lot in one hop to the
/// storage tier. RESP framing is length-delimited like memcached's storage
/// commands, so again only the parser can say where a command ends.
///
/// `version` is threaded through rather than kept in the executor because a
/// `HELLO` changes how every *later* reply on this connection is rendered,
/// including ones in the same pipelined block.
async fn drain_resp(
    state: &Arc<ServerState>,
    conn_auth: &mut ConnAuth,
    read_buf: &mut BytesMut,
    version: &mut vash_proto::resp::Version,
    write_buf: &mut Vec<u8>,
) -> std::io::Result<bool> {
    use vash_proto::resp;

    let mut complete = 0usize;
    let mut fatal = None;
    let mut all_reads = true;

    loop {
        match resp::parse(&read_buf[complete..]) {
            Ok(resp::Outcome::Incomplete) => break,
            Ok(resp::Outcome::Command(parsed)) => {
                all_reads &= crate::resp::is_read_only(&parsed.command);
                complete += parsed.consumed;
            }
            // Counted in, not handled here: the error has to land in the
            // response stream in the position the bad command occupied, which
            // only the executor knows how to do.
            Err(resp::ProtocolError::Recoverable { consumed, .. }) => complete += consumed,
            Err(resp::ProtocolError::Fatal(detail)) => {
                debug!(detail, "closing connection: RESP framing is unrecoverable");
                fatal = Some(detail);
                break;
            }
        }
    }

    let mut closing = Closing::No;
    if complete > 0 {
        let block: Bytes = read_buf.split_to(complete).freeze();

        if state.inline_reads && all_reads {
            closing = crate::resp::execute_block(state, conn_auth, &block, version, write_buf);
        } else {
            let state = Arc::clone(state);
            let mut negotiating = *version;
            let mut authenticating = conn_auth.clone();
            let (response, done, negotiated, authenticated) =
                tokio::task::spawn_blocking(move || {
                    let mut out = Vec::with_capacity(64);
                    let done = crate::resp::execute_block(
                        &state,
                        &mut authenticating,
                        &block,
                        &mut negotiating,
                        &mut out,
                    );
                    (out, done, negotiating, authenticating)
                })
                .await
                .map_err(std::io::Error::other)?;

            *version = negotiated;
            *conn_auth = authenticated;
            write_buf.extend_from_slice(&response);
            closing = done;
        }
    }

    // Redis answers a protocol error and *then* hangs up, so the client learns
    // why instead of seeing a bare disconnect. The reply goes after whatever
    // the commands before it produced, which is the position it occupied.
    if let Some(detail) = fatal {
        resp::encode::protocol_error(write_buf, detail);
        return Ok(false);
    }
    Ok(closing == Closing::No)
}
