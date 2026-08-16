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
    // This connection's row in `stats conns`. Held for its whole life; the
    // accept loop removes it.
    registered: std::sync::Arc<crate::connections::ConnInfo>,
) -> std::io::Result<()> {
    // Cache traffic is small and latency-sensitive; Nagle would batch a reply
    // against the next one and add up to 40ms for nothing.
    stream.set_nodelay(true)?;

    let mut read_buf = BytesMut::with_capacity(read_buffer);
    let mut write_buf: Vec<u8> = Vec::with_capacity(read_buffer);
    // Held for the life of the connection so measuring a block allocates
    // nothing once it has seen its widest mix of command kinds. A uniform
    // block — every `GET`, or every `SET` — measures to exactly one run.
    let mut runs: Vec<Run> = Vec::new();
    let mut protocol: Option<Protocol> = None;
    // Redis connections start at RESP2 and move to RESP3 only if the client
    // asks. Per-connection state, because that is exactly what `HELLO`
    // negotiates.
    let mut resp_version = vash_proto::resp::Version::default();
    // Likewise per connection: authentication is a property of this socket, and
    // it dies with it.
    let mut conn_auth = ConnAuth::default();

    let limits = state.auth.limits;
    let enforcing = state.auth.required();
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
        // One relaxed add per syscall, which is nothing beside the syscall. The
        // pair is what turns a request rate into a bandwidth figure.
        state
            .metrics
            .outcomes
            .bytes_read
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);

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
                    // A dialect turned off in configuration is closed here
                    // rather than refused by its own parser, because refusing
                    // would mean running the parser we were told not to serve.
                    // The client sees what an unrecognised opening byte gets: a
                    // disconnect, with the reason in the server's log.
                    if !state.protocol.dialect_enabled(chosen) {
                        debug!(?chosen, "closing connection: dialect disabled");
                        return Ok(());
                    }
                    debug!(?chosen, "protocol selected");
                    registered.dialect_chosen(chosen);
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
        // Ordered so an authenticated connection never touches the lock. A gated
        // connection's write owes a refusal that only the dialect's own executor
        // can render, so the awaited path is withheld from it entirely.
        let gated = !conn_auth.is_authenticated() && state.auth.required();
        let keep_going = match protocol.expect("set above") {
            Protocol::Vcp => {
                drain(
                    &state,
                    &mut conn_auth,
                    &mut read_buf,
                    &mut write_buf,
                    &mut (),
                    |buf, runs| measure_vcp(buf, gated, runs),
                    |state, conn, block, (), out| {
                        crate::dispatch::execute_vcp_block(state, conn, block, out)
                    },
                    |_, _| {},
                    None,
                    &mut runs,
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
                    (!gated).then_some((
                        crate::dispatch::parse_memcached_writes as ParseWrites,
                        crate::metrics::Dialect::Memcached,
                        memcached::encode::STORED,
                    )),
                    &mut runs,
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
                    (!gated).then_some((
                        crate::resp::parse_writes as ParseWrites,
                        crate::metrics::Dialect::Resp,
                        vash_proto::resp::encode::OK,
                    )),
                    &mut runs,
                )
                .await?
            }
        };

        if pre_auth.is_some() && conn_auth.is_authenticated() {
            pre_auth = None;
        }
        if conn_auth.is_authenticated() {
            registered.authenticated();
        }
        // One relaxed store per batch of commands, into a word only this
        // connection writes — so `stats conns` gets an idle clock without the
        // request path touching a lock.
        registered.touched(state.connections.epoch());

        if !write_buf.is_empty() {
            state
                .metrics
                .outcomes
                .bytes_written
                .fetch_add(write_buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
#[derive(Debug, Default)]
struct Measured {
    /// Bytes at the front of the buffer that form complete commands.
    complete: usize,
    /// Framing is unrecoverable; the connection closes once any reply is out.
    fatal: Option<&'static str>,
    /// Total length of the command still arriving, so the buffer can be sized
    /// for it once rather than grown repeatedly.
    reserve: usize,
}

/// How a stretch of commands may be served.
///
/// **The block used to be classified as a whole, and that cost more than
/// anything else left in the request path.** `all_reads` and `all_writes` were
/// folded with `&=` across every command, so a single write among fifteen reads
/// disqualified the whole block from the inline read path and sent all sixteen
/// to the blocking pool. A pipelined cache client sends exactly that: at
/// pipeline 16 with one write in ten, only `0.9^16` — 18.5% — of blocks are
/// uniform. Measured, one write per *hundred* operations cost 39% of
/// throughput, and a 25% write workload ran slower than a 100% write one, which
/// nothing but this explains. See `docs/performance-proposals.md` §14.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunKind {
    /// Answerable without touching the writer, so it may run on this worker
    /// when `inline_reads` is on.
    Reads,
    /// Unconditional writes that [`crate::dispatch::SetBatch`] may hold back, so
    /// the run is one submission and can be *awaited* rather than blocked on.
    /// See §8.
    Writes,
    /// Everything else: guarded writes, tagged writes, `AUTH`, `QUIT`, a
    /// recoverable parse error. These take the blocking pool, as the whole
    /// block used to.
    Other,
}

/// A maximal stretch of commands of one kind, measured in bytes of the block.
///
/// Byte lengths rather than command counts because that is what the executors
/// take, and because the block is a refcounted `Bytes` — a run is a slice of it
/// and costs no copy.
#[derive(Debug)]
struct Run {
    len: usize,
    kind: RunKind,
}

impl Run {
    /// Adds `len` bytes of `kind`, extending the run in progress when it
    /// matches so that the common uniform block still produces exactly one.
    fn extend(runs: &mut Vec<Run>, kind: RunKind, len: usize) {
        match runs.last_mut() {
            Some(last) if last.kind == kind => last.len += len,
            _ => runs.push(Run { len, kind }),
        }
    }
}

/// Executes a block of one dialect's commands, rendering the replies.
type RunBlock<S> = fn(&ServerState, &mut ConnAuth, &[u8], &mut S, &mut Vec<u8>) -> Closing;

/// A block that is nothing but unconditional writes, decoded and ready to send.
///
/// The two text dialects reduce to the same thing here — a list of writes and,
/// for each, whether the client asked to be told about it — so one run type and
/// one driver serve both.
pub(crate) struct WriteRun<'a> {
    pub sets: Vec<vash_core::Set<'a>>,
    /// `noreply` for memcached; always false for RESP, which has no such thing.
    pub suppress: Vec<bool>,
}

/// Decodes a whole block of writes, or declines.
type ParseWrites = for<'a> fn(&'a [u8]) -> Option<WriteRun<'a>>;

/// **Serves a block of writes without leaving the runtime worker.**
///
/// The ordinary path hands the block to `spawn_blocking`, and that thread then
/// sleeps on the writer queue for the whole commit — one OS thread parked per
/// in-flight write. Measured on a four-core container, a write's round trip was
/// 12.5 ms of which the storage layer accounted for 0.99 ms; the rest was that
/// hand-off and the contention behind it. Here the work is prepared on the
/// worker, handed to the shard writers, and *awaited*, so nothing is parked.
///
/// Returns `None` when it declines — a decode this path does not model, a
/// refused submission, or a failed batch — and the caller then runs the block
/// the ordinary way, which counts, classifies and retries per command exactly
/// as it always did. Re-running an unconditional write that a shard already
/// committed rewrites the same bytes, which is the same trade
/// [`crate::dispatch::SetBatch`] already documents for its own retry.
async fn run_writes_awaited(
    state: &Arc<ServerState>,
    block: &[u8],
    out: &mut Vec<u8>,
    parse: ParseWrites,
    dialect: crate::metrics::Dialect,
    stored: &'static [u8],
) -> Option<()> {
    let run = parse(block)?;
    if run.sets.is_empty() {
        return None;
    }

    // Held across the submit *and* the wait, so this bounds writes in flight
    // rather than writes being submitted. Without it the shard queues take
    // everything a hundred connections can offer at once — see
    // [`ServerState::write_permits`].
    let _permit = state.write_permits.acquire().await.ok()?;

    let started = std::time::Instant::now();
    let pending = state.store.submit_set_many(&run.sets).ok()?;
    let cas = pending.wait().await.ok()?;

    crate::dispatch::count_stored(state, dialect, &run.sets, &cas, started.elapsed());
    for (index, suppressed) in run.suppress.iter().enumerate() {
        if !suppressed {
            let _ = index;
            out.extend_from_slice(stored);
        }
    }
    Some(())
}

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

    // The connection's own buffer makes the trip rather than a fresh one, and
    // comes back with the replies appended. It used to be an allocation per
    // batch plus a copy of every reply byte back out of it — on a `GET` that is
    // the whole value moved a third time, after the copy out of the memory map
    // and the copy into the reply. Moving it costs three words each way and
    // keeps the capacity, so a connection stops reallocating once it has seen
    // its largest response.
    //
    // Appending rather than replacing, so this holds whether or not the caller
    // handed over an empty buffer.
    let mut out = std::mem::take(write_buf);

    // Store operations can page-fault or wait on the writer queue, neither of
    // which may happen on a runtime worker. The block is re-parsed on the
    // blocking thread so the borrowed key and value slices never cross a task
    // boundary.
    let joined = tokio::task::spawn_blocking(move || {
        let closing = run(
            &state,
            &mut authenticating,
            &block,
            &mut negotiating,
            &mut out,
        );
        (out, closing, authenticating, negotiating)
    })
    .await;

    // Only a panic in the block gets here, and it takes the connection with it —
    // so the buffer this leaves empty is about to be dropped along with
    // everything else. There is nothing to restore it from: it went into the
    // task that panicked.
    let (response, closing, authenticated, negotiated) = joined.map_err(std::io::Error::other)?;

    *conn_auth = authenticated;
    *dialect = negotiated;
    *write_buf = response;
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
    measure: impl Fn(&[u8], &mut Vec<Run>) -> Measured,
    run: RunBlock<S>,
    on_fatal: fn(&mut Vec<u8>, &'static str),
    // How this dialect decodes a run of nothing but writes, and what it says
    // for each one that stored. `None` for a dialect that stays on the ordinary
    // path — see `measure_vcp`.
    writes: Option<(ParseWrites, crate::metrics::Dialect, &'static [u8])>,
    // Reused across every block this connection ever measures, so a block costs
    // no allocation once the connection has seen its widest mix.
    runs: &mut Vec<Run>,
) -> std::io::Result<bool> {
    runs.clear();
    let measured = measure(read_buf, runs);
    let mut closing = measured.fatal.is_some();

    if measured.complete > 0 {
        // `split_to` hands the bytes over by reference count. No copy, and the
        // decoded keys and values borrow directly from them.
        let block = read_buf.split_to(measured.complete).freeze();
        debug_assert_eq!(
            measured.complete,
            runs.iter().map(|r| r.len).sum::<usize>(),
            "every measured byte belongs to exactly one run"
        );

        // **Splitting only pays when the reads have somewhere better to go.**
        // With `inline_reads` off a read run takes the same blocking pool an
        // `Other` run does, so peeling the writes out of a mixed block buys
        // nothing for them and fragments what used to be a single hop into
        // several. Measured on the default configuration, a 1:9 pipelined
        // workload fell from 147,180 to 85,521 ops/s that way. Collapsing to one
        // run restores exactly the old route; a block that is *only* writes is
        // already one run and still takes the awaited path.
        if !state.inline_reads && runs.len() > 1 {
            let whole = measured.complete;
            runs.clear();
            runs.push(Run {
                len: whole,
                kind: RunKind::Other,
            });
        }

        // **Each run goes to the tier that suits it, in order.** Order is what
        // makes this safe: a client may pipeline a write and then a read of the
        // same key, and running the runs in sequence answers them in sequence.
        let mut offset = 0;
        let mut index = 0;
        while index < runs.len() {
            let start = offset;
            let mut len = runs[index].len;

            // What this run can take. A write run needs a dialect that decodes
            // one, which a gated connection is denied because its refusal is
            // the executor's to word.
            let inline = runs[index].kind == RunKind::Reads && state.inline_reads;
            let awaitable = runs[index].kind == RunKind::Writes && writes.is_some();

            // Runs that both end up on the pool are merged into one hop, so a
            // block that gains nothing from splitting pays for nothing either —
            // with `inline_reads` off, that includes its read runs.
            if !inline && !awaitable {
                while let Some(next) = runs.get(index + 1) {
                    let next_inline = next.kind == RunKind::Reads && state.inline_reads;
                    let next_awaitable = next.kind == RunKind::Writes && writes.is_some();
                    if next_inline || next_awaitable {
                        break;
                    }
                    index += 1;
                    len += runs[index].len;
                }
            }
            offset += len;
            index += 1;

            let slice = block.slice(start..start + len);

            // A run of writes is submitted and awaited here, holding no thread
            // while the writer works. It declines — `None` — for anything it
            // does not model, and the ordinary path below then runs the same
            // bytes untouched, so this is a shortcut and never the only route.
            //
            // **Boxed, and reads are why.** Awaiting this future inline would
            // splice its state — a decoded `WriteRun`, a permit, the pending
            // submission — into `drain`'s, and `drain`'s into the future of
            // every connection task. Measured, that inflation cost pipelined
            // `GET` 6.7x (466k -> 70k ops/s with `inline_reads`) on blocks that
            // never take this branch at all. Behind a `Box` the read path polls
            // a small future again, and the allocation lands only on runs that
            // are entirely writes, where a commit dwarfs it. See
            // `docs/benchmarks.md`.
            if awaitable {
                let (parse, dialect_kind, stored) = writes.expect("checked by `awaitable`");
                if Box::pin(run_writes_awaited(
                    state,
                    &slice,
                    write_buf,
                    parse,
                    dialect_kind,
                    stored,
                ))
                .await
                .is_some()
                {
                    continue;
                }
            }

            // Reads may run on this worker; anything that can write must not,
            // because a write waits on the shard's writer queue and would block
            // the worker — and every other connection it serves — behind it.
            if run_block(state, conn_auth, dialect, slice, write_buf, inline, run).await?
                == Closing::Yes
            {
                // `quit`, and nothing after it on this connection matters — the
                // same answer the block executors already give mid-block.
                closing = true;
                break;
            }
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
fn measure_vcp(buf: &[u8], gated: bool, runs: &mut Vec<Run>) -> Measured {
    let mut measured = Measured::default();
    loop {
        match peek_frame_len(&buf[measured.complete..]) {
            FrameLen::Complete(len) => {
                let frame = &buf[measured.complete..measured.complete + len];
                // VCP never produces a write run: a reply carries the request id
                // from its own frame header, and the pre-auth gate is read from
                // that header too, neither of which the shared write run models.
                // Its reads still split out and still run inline.
                let kind = if is_read_only_frame(frame) {
                    RunKind::Reads
                } else {
                    RunKind::Other
                };
                Run::extend(runs, kind, len);
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

fn measure_memcached(buf: &[u8], runs: &mut Vec<Run>) -> Measured {
    let mut measured = Measured::default();
    loop {
        match memcached::parse(&buf[measured.complete..]) {
            Ok(Outcome::Incomplete) => break,
            Ok(Outcome::Command(parsed)) => {
                let storage = matches!(
                    parsed.style,
                    vash_proto::memcached::encode::ResponseStyle::Storage
                );
                let kind = if parsed.command.inline_safe() {
                    RunKind::Reads
                } else if storage && crate::dispatch::awaitable(&parsed.command) {
                    RunKind::Writes
                } else {
                    RunKind::Other
                };
                Run::extend(runs, kind, parsed.consumed);
                measured.complete += parsed.consumed;
            }
            // Counted in, not handled here: the error line has to land in the
            // response stream in the position the bad command occupied, which
            // only the executor knows how to do.
            Err(ProtocolError::Recoverable { consumed, .. }) => {
                Run::extend(runs, RunKind::Other, consumed);
                measured.complete += consumed;
            }
            Err(ProtocolError::Fatal(detail)) => {
                measured.fatal = Some(detail);
                break;
            }
        }
    }
    measured
}

fn measure_resp(buf: &[u8], runs: &mut Vec<Run>) -> Measured {
    use vash_proto::resp;

    let mut measured = Measured::default();
    loop {
        match resp::parse(&buf[measured.complete..]) {
            Ok(resp::Outcome::Incomplete) => break,
            Ok(resp::Outcome::Command(parsed)) => {
                let kind = if crate::resp::inline_safe(&parsed.command) {
                    RunKind::Reads
                } else if crate::resp::batchable_write(&parsed.command) {
                    RunKind::Writes
                } else {
                    RunKind::Other
                };
                Run::extend(runs, kind, parsed.consumed);
                measured.complete += parsed.consumed;
            }
            Err(resp::ProtocolError::Recoverable { consumed, .. }) => {
                Run::extend(runs, RunKind::Other, consumed);
                measured.complete += consumed;
            }
            Err(resp::ProtocolError::Fatal(detail)) => {
                measured.fatal = Some(detail);
                break;
            }
        }
    }
    measured
}
