use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use cache_proto::kcp::{FrameLen, peek_frame_len};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::dispatch::execute_frame;
use crate::state::ServerState;

/// Serves one connection until the peer disconnects or sends something
/// unintelligible.
pub async fn handle(
    mut stream: TcpStream,
    state: Arc<ServerState>,
    read_buffer: usize,
) -> std::io::Result<()> {
    // Cache traffic is small and latency-sensitive; Nagle would batch a reply
    // against the next one and add up to 40ms for nothing.
    stream.set_nodelay(true)?;

    let mut read_buf = BytesMut::with_capacity(read_buffer);
    let mut write_buf: Vec<u8> = Vec::with_capacity(read_buffer);

    loop {
        let n = stream.read_buf(&mut read_buf).await?;
        if n == 0 {
            return Ok(()); // clean disconnect
        }

        // Drain every complete frame the read produced. Pipelined requests are
        // therefore handled without waiting for a round trip each.
        loop {
            match peek_frame_len(&read_buf) {
                FrameLen::Incomplete { needed } => {
                    read_buf.reserve(needed.saturating_sub(read_buf.len()));
                    break;
                }
                FrameLen::TooLarge => {
                    debug!("closing connection: frame length exceeds the maximum");
                    // The stream cannot be resynchronised, so flush whatever is
                    // already owed and hang up.
                    if !write_buf.is_empty() {
                        stream.write_all(&write_buf).await?;
                    }
                    return Ok(());
                }
                FrameLen::Complete(len) => {
                    // `split_to` hands the frame's bytes to the blocking task by
                    // reference count. No copy, and the decoded key and value
                    // borrow directly from it.
                    let frame: Bytes = read_buf.split_to(len).freeze();
                    let state = Arc::clone(&state);

                    // LMDB reads can page-fault and block for the length of a
                    // disk I/O, which must never happen on a runtime worker.
                    // M1 replaces this per-frame handoff with a channel to the
                    // dedicated storage threads (plan §9).
                    let response =
                        tokio::task::spawn_blocking(move || execute_frame(&state, &frame))
                            .await
                            .map_err(std::io::Error::other)?;

                    write_buf.extend_from_slice(&response);
                }
            }
        }

        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
            write_buf.clear();
        }
    }
}
