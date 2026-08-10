use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use cache_proto::kcp::{FrameLen, peek_frame_len};
use cache_proto::memcached::{self, Outcome, ProtocolError};
use cache_proto::{Protocol, detect};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::dispatch::{Closing, execute_frame, execute_memcached};
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
async fn drain_kcp(
    state: &Arc<ServerState>,
    read_buf: &mut BytesMut,
    write_buf: &mut Vec<u8>,
) -> std::io::Result<bool> {
    loop {
        match peek_frame_len(read_buf) {
            FrameLen::Incomplete { needed } => {
                read_buf.reserve(needed.saturating_sub(read_buf.len()));
                return Ok(true);
            }
            FrameLen::TooLarge => {
                debug!("closing connection: frame length exceeds the maximum");
                return Ok(false);
            }
            FrameLen::Complete(len) => {
                // `split_to` hands the frame's bytes to the blocking task by
                // reference count. No copy, and the decoded key and value
                // borrow directly from it.
                let frame: Bytes = read_buf.split_to(len).freeze();
                let state = Arc::clone(state);

                // Store operations can page-fault or wait on the writer queue,
                // which must never happen on a runtime worker.
                let response = tokio::task::spawn_blocking(move || execute_frame(&state, &frame))
                    .await
                    .map_err(std::io::Error::other)?;

                write_buf.extend_from_slice(&response);
            }
        }
    }
}

/// Handles every complete memcached command in the buffer.
async fn drain_memcached(
    state: &Arc<ServerState>,
    read_buf: &mut BytesMut,
    write_buf: &mut Vec<u8>,
) -> std::io::Result<bool> {
    loop {
        // How much of the buffer this command occupies has to be known before
        // ownership moves to the blocking task, and only the parser can say —
        // a value may contain CRLF, so the framing is length-delimited, not
        // line-delimited.
        let consumed = match memcached::parse(read_buf) {
            Ok(Outcome::Incomplete) => return Ok(true),
            Ok(Outcome::Command(parsed)) => parsed.consumed,
            Err(ProtocolError::Recoverable { response, consumed }) => {
                memcached::encode::encode_error(write_buf, response);
                read_buf.advance_to(consumed);
                continue;
            }
            Err(ProtocolError::Fatal(detail)) => {
                debug!(
                    detail,
                    "closing connection: memcached framing is unrecoverable"
                );
                return Ok(false);
            }
        };

        let command: Bytes = read_buf.split_to(consumed).freeze();
        let state = Arc::clone(state);

        let (response, closing) = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            // Re-parsed on the blocking thread so the borrowed key and value
            // slices never cross a task boundary, exactly as the KCP path does.
            let closing = match memcached::parse(&command) {
                Ok(Outcome::Command(parsed)) => execute_memcached(&state, &parsed, &mut out),
                _ => {
                    // Unreachable: the same bytes parsed a moment ago.
                    memcached::encode::encode_error(
                        &mut out,
                        memcached::ErrorKind::Server("internal error"),
                    );
                    Closing::No
                }
            };
            (out, closing)
        })
        .await
        .map_err(std::io::Error::other)?;

        write_buf.extend_from_slice(&response);
        if closing == Closing::Yes {
            return Ok(false);
        }
    }
}

/// `BytesMut` has no "skip n bytes" that keeps the remainder, so this is the
/// discard used when a malformed command must be stepped over.
trait AdvanceTo {
    fn advance_to(&mut self, n: usize);
}

impl AdvanceTo for BytesMut {
    fn advance_to(&mut self, n: usize) {
        let _ = self.split_to(n.min(self.len()));
    }
}
