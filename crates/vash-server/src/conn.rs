use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;
use vash_proto::memcached::{self, Outcome, ProtocolError};
use vash_proto::vcp::{FrameLen, peek_frame_len};
use vash_proto::{Protocol, detect};

use crate::auth::ConnAuth;
use crate::dispatch::{Closing, execute_memcached_block};
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

        // One drain for all three dialects. What differs is the measuring —
        // only a dialect's own parser can say where its commands end — and what
        // a fatal framing error owes the client. Everything after that point is
        // identical, and used to be written out three times.
        let keep_going = match protocol.expect("set above") {
            Protocol::Vcp => {
                // Ordered so an authenticated connection never touches the lock.
                let gated = !conn_auth.is_authenticated() && state.auth.current().required();
                drain(
                    &state,
                    &mut conn_auth,
                    &mut read_buf,
                    &mut write_buf,
                    &mut (),
                    |buf| measure_vcp(buf, gated),
                    |state, conn, block, (), out| {
                        crate::dispatch::execute_vcp_block(state, conn, block, out)
                    },
                    |_, _| {},
                )
                .await?
            }
            Protocol::Memcached => {
                drain(
                    &state,
                    &mut conn_auth,
                    &mut read_buf,
                    &mut write_buf,
                    &mut (),
                    measure_memcached,
                    |state, conn, block, (), out| execute_memcached_block(state, conn, block, out),
                    |_, _| {},
                )
                .await?
            }
            Protocol::Resp => {
                drain(
                    &state,
                    &mut conn_auth,
                    &mut read_buf,
                    &mut write_buf,
                    &mut resp_version,
                    measure_resp,
                    crate::resp::execute_block,
                    // Redis answers a protocol error and *then* hangs up, so the
                    // client learns why instead of seeing a bare disconnect.
                    vash_proto::resp::encode::protocol_error,
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

/// How much of the read buffer forms whole commands, and what that implies.
///
/// The one thing each dialect works out for itself, because only its own parser
/// knows where a command ends — and for the storage commands of all three that
/// is length-delimited rather than line-delimited, since a value may contain
/// anything, CRLF included.
#[derive(Debug)]
struct Measured {
    /// Bytes at the front of the buffer that form complete commands.
    complete: usize,
    /// Every one of them can be answered without touching the writer.
    all_reads: bool,
    /// Framing is unrecoverable; the connection closes once any reply is out.
    fatal: Option<&'static str>,
    /// Total length of the command still arriving, so the buffer can be sized
    /// for it once rather than grown repeatedly.
    reserve: usize,
}

impl Default for Measured {
    fn default() -> Self {
        Self {
            complete: 0,
            all_reads: true,
            fatal: None,
            reserve: 0,
        }
    }
}

/// Executes a block of one dialect's commands, rendering the replies.
type RunBlock<S> = fn(&ServerState, &mut ConnAuth, &[u8], &mut S, &mut Vec<u8>) -> Closing;

/// Executes one block of whole commands in whichever tier is safe for it.
///
/// **The one place the hand-off to the storage tier happens, and the one place
/// the connection's mutable state crosses it.** That state — the authentication,
/// and for Redis the negotiated dialect — has to travel into the block and back
/// out, because an `AUTH` and the commands it authorises can arrive in a single
/// pipelined read, so it must take effect *within* a block rather than between
/// reads. Copying it back is the step that is easy to forget and silent when
/// forgotten: a lost authentication looks like a client that simply has to try
/// again. There is one of these so it can only be got wrong once.
///
/// **Every buffered command goes over in one hop.** The hop is a thread handoff,
/// and a handoff costs far more than executing a cached request: measured on
/// Windows, one per frame capped a pipelined connection at roughly 5k operations
/// a second no matter how deep the pipeline, because the depth bought nothing.
/// Amortising it over whatever arrived in a single read is what makes pipelining
/// worth anything, and it costs the unpipelined case nothing — one command in
/// the buffer is still one hop.
async fn run_block<S: Copy + Send + 'static>(
    state: &Arc<ServerState>,
    conn_auth: &mut ConnAuth,
    dialect: &mut S,
    block: Bytes,
    write_buf: &mut Vec<u8>,
    inline: bool,
    run: RunBlock<S>,
) -> std::io::Result<Closing> {
    if inline {
        return Ok(run(state, conn_auth, &block, dialect, write_buf));
    }

    let state = Arc::clone(state);
    let mut authenticating = conn_auth.clone();
    let mut negotiating = *dialect;

    // Store operations can page-fault or wait on the writer queue, neither of
    // which may happen on a runtime worker. The block is re-parsed on the
    // blocking thread so the borrowed key and value slices never cross a task
    // boundary.
    let (response, closing, authenticated, negotiated) = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(64);
        let closing = run(
            &state,
            &mut authenticating,
            &block,
            &mut negotiating,
            &mut out,
        );
        (out, closing, authenticating, negotiating)
    })
    .await
    .map_err(std::io::Error::other)?;

    *conn_auth = authenticated;
    *dialect = negotiated;
    write_buf.extend_from_slice(&response);
    Ok(closing)
}

/// Handles every complete command in the buffer. Returns `false` to close.
#[expect(clippy::too_many_arguments)]
async fn drain<S: Copy + Send + 'static>(
    state: &Arc<ServerState>,
    conn_auth: &mut ConnAuth,
    read_buf: &mut BytesMut,
    write_buf: &mut Vec<u8>,
    dialect: &mut S,
    measure: impl Fn(&[u8]) -> Measured,
    run: RunBlock<S>,
    on_fatal: fn(&mut Vec<u8>, &'static str),
) -> std::io::Result<bool> {
    let measured = measure(read_buf);
    let mut closing = measured.fatal.is_some();

    if measured.complete > 0 {
        // `split_to` hands the bytes over by reference count. No copy, and the
        // decoded keys and values borrow directly from them.
        let block = read_buf.split_to(measured.complete).freeze();

        // Reads may run on this worker; anything that can write must not,
        // because a write waits on the shard's writer queue and would block the
        // worker — and every other connection it serves — behind it.
        let inline = state.inline_reads && measured.all_reads;
        if run_block(state, conn_auth, dialect, block, write_buf, inline, run).await?
            == Closing::Yes
        {
            closing = true;
        }
    }

    if measured.reserve > 0 {
        read_buf.reserve(measured.reserve.saturating_sub(read_buf.len()));
    }

    // Written after whatever the commands before it produced, which is the
    // position the bad framing occupied.
    if let Some(detail) = measured.fatal {
        debug!(detail, "closing connection: framing is unrecoverable");
        on_fatal(write_buf, detail);
    }
    Ok(!closing)
}

/// Measures VCP frames from their length headers, without decoding a body.
fn measure_vcp(buf: &[u8], gated: bool) -> Measured {
    let mut measured = Measured::default();
    loop {
        match peek_frame_len(&buf[measured.complete..]) {
            FrameLen::Complete(len) => {
                let frame = &buf[measured.complete..measured.complete + len];
                measured.all_reads &= is_read_only_frame(frame);
                measured.complete += len;
            }
            FrameLen::Incomplete { needed } => {
                // `needed` comes from the frame header, which an unauthenticated
                // stranger controls: without this, one 12-byte header claiming a
                // 64 MiB body reserves 64 MiB before anything has been
                // presented. The buffered-bytes check in `handle` cannot cover
                // it, because this reserves against a length that has not
                // arrived.
                if gated && needed > PRE_AUTH_MAX_BUFFERED {
                    measured.fatal = Some("unauthenticated frame is too large");
                } else {
                    measured.reserve = needed;
                }
                break;
            }
            FrameLen::TooLarge => {
                measured.fatal = Some("frame length exceeds the maximum");
                break;
            }
        }
    }
    measured
}

/// Whether a VCP frame can be answered without any chance of writing.
///
/// Decided from the opcode byte alone — the frame is not decoded twice. An
/// unknown opcode is treated as a write, so a byte nobody recognises takes the
/// safe path rather than the fast one. That this table agrees with
/// [`vash_core::Command::inline_safe`] is pinned by a test in `vash-proto`.
fn is_read_only_frame(frame: &[u8]) -> bool {
    frame
        .first()
        .and_then(|byte| vash_proto::vcp::Opcode::from_u8(*byte))
        .is_some_and(|opcode| opcode.inline_safe())
}

fn measure_memcached(buf: &[u8]) -> Measured {
    let mut measured = Measured::default();
    loop {
        match memcached::parse(&buf[measured.complete..]) {
            Ok(Outcome::Incomplete) => break,
            Ok(Outcome::Command(parsed)) => {
                measured.all_reads &= parsed.command.inline_safe();
                measured.complete += parsed.consumed;
            }
            // Counted in, not handled here: the error line has to land in the
            // response stream in the position the bad command occupied, which
            // only the executor knows how to do.
            Err(ProtocolError::Recoverable { consumed, .. }) => measured.complete += consumed,
            Err(ProtocolError::Fatal(detail)) => {
                measured.fatal = Some(detail);
                break;
            }
        }
    }
    measured
}

fn measure_resp(buf: &[u8]) -> Measured {
    use vash_proto::resp;

    let mut measured = Measured::default();
    loop {
        match resp::parse(&buf[measured.complete..]) {
            Ok(resp::Outcome::Incomplete) => break,
            Ok(resp::Outcome::Command(parsed)) => {
                measured.all_reads &= crate::resp::inline_safe(&parsed.command);
                measured.complete += parsed.consumed;
            }
            Err(resp::ProtocolError::Recoverable { consumed, .. }) => measured.complete += consumed,
            Err(resp::ProtocolError::Fatal(detail)) => {
                measured.fatal = Some(detail);
                break;
            }
        }
    }
    measured
}
