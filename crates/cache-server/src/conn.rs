use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use cache_proto::kcp::{FrameLen, peek_frame_len};
use cache_proto::memcached::{self, Outcome, ProtocolError};
use cache_proto::{Protocol, detect};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::dispatch::{Closing, execute_frame_into, execute_memcached_block};
use crate::state::ServerState;

/// Serves one connection until the peer disconnects or sends something
/// unintelligible.
///
/// The dialect is settled by the first byte and never revisited — see
/// [`cache_proto::detect`].
pub async fn handle(
    mut stream: TcpStream,
    state: Arc<ServerState>,
    read_buffer: usize,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    // Cache traffic is small and latency-sensitive; Nagle would batch a reply
    // against the next one and add up to 40ms for nothing.
    stream.set_nodelay(true)?;

    let mut read_buf = BytesMut::with_capacity(read_buffer);
    let mut write_buf: Vec<u8> = Vec::with_capacity(read_buffer);
    let mut protocol: Option<Protocol> = None;

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
        let n = tokio::select! {
            read = stream.read_buf(&mut read_buf) => read?,
            _ = shutdown.changed() => {
                debug!("closing an idle connection to drain");
                return Ok(());
            }
        };
        if n == 0 {
            return Ok(()); // clean disconnect
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
            Protocol::Kcp => drain_kcp(&state, &mut read_buf, &mut write_buf).await?,
            Protocol::Memcached => drain_memcached(&state, &mut read_buf, &mut write_buf).await?,
        };

        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
            write_buf.clear();
        }
        if !keep_going {
            return Ok(());
        }
    }
}

/// Handles every complete KCP frame in the buffer. Returns `false` when the
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
async fn drain_kcp(
    state: &Arc<ServerState>,
    read_buf: &mut BytesMut,
    write_buf: &mut Vec<u8>,
) -> std::io::Result<bool> {
    let mut frames: Vec<Bytes> = Vec::new();
    let mut closing = false;

    loop {
        match peek_frame_len(read_buf) {
            FrameLen::Incomplete { needed } => {
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
            execute_frame_into(state, frame, write_buf);
        }
        return Ok(!closing);
    }

    let state = Arc::clone(state);
    // Store operations can page-fault or wait on the writer queue, neither of
    // which may happen on a runtime worker.
    let response = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(frames.len() * 64);
        for frame in &frames {
            execute_frame_into(&state, frame, &mut out);
        }
        out
    })
    .await
    .map_err(std::io::Error::other)?;

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
        .and_then(|byte| cache_proto::kcp::Opcode::from_u8(*byte))
        .is_some_and(|opcode| opcode.is_read_only())
}

/// The memcached equivalent, decided from the already-parsed command.
///
/// Note what is missing: `gat` re-stamps a TTL and `incr` rewrites the value,
/// so despite reading like retrievals they are writes.
fn is_read_only_command(command: &cache_core::Command<'_>) -> bool {
    use cache_core::Command;
    matches!(
        command,
        Command::Get { .. } | Command::GetMany(_) | Command::Version | Command::Stats
    )
}

/// Handles every complete memcached command in the buffer.
///
/// Measures the span of whole commands first, then executes the lot in one hop
/// to the storage tier — see [`drain_kcp`] for why the hop count is what
/// matters. Only the parser can say where a command ends, because a storage
/// command's framing is length-delimited rather than line-delimited: a value
/// may contain CRLF.
async fn drain_memcached(
    state: &Arc<ServerState>,
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
        let closing = execute_memcached_block(state, &block, write_buf);
        return Ok(closing == Closing::No && !fatal);
    }

    let block: Bytes = read_buf.split_to(complete).freeze();
    let state = Arc::clone(state);

    // Re-parsed on the blocking thread so the borrowed key and value slices
    // never cross a task boundary, exactly as the KCP path does.
    let (response, closing) = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(64);
        let closing = execute_memcached_block(&state, &block, &mut out);
        (out, closing)
    })
    .await
    .map_err(std::io::Error::other)?;

    write_buf.extend_from_slice(&response);
    Ok(closing == Closing::No && !fatal)
}
