use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::debug;
use vash_proto::memcached::{self, Outcome, ProtocolError};
use vash_proto::vcp::{FrameLen, peek_frame_len};
use vash_proto::{Protocol, detect};

use crate::auth::ConnAuth;
use crate::dispatch::Closing;
use crate::memcached::execute_memcached_block;
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
///
/// Generic over the stream rather than taking a `TcpStream`, so that the same
/// loop serves a plaintext socket and a TLS session — the whole of what TLS
/// costs this module. Monomorphised rather than boxed: the read below runs
/// once per syscall on every connection, and a vtable there would be paid
/// forever to save a type parameter.
pub async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
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
    // Who the TLS handshake said this is, when client certificates are in use.
    // `None` for every plaintext connection and for a TLS one that presented
    // nothing — those authenticate with an `AUTH` command or not at all.
    identity: Option<crate::auth::Identity>,
) -> std::io::Result<()> {
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
    // A certificate identity is established before the first byte of protocol,
    // so the connection starts authenticated and never sends `AUTH`. It may
    // still send one — re-authenticating as somebody else is allowed, and is
    // how a pooled connection follows a rotation.
    if let Some(identity) = identity {
        registered.authenticated(identity.roles());
        conn_auth.succeed(identity);
        // Released now rather than after the first command: this connection
        // has presented something, so it is no longer one of the unidentified
        // ones the pre-auth budget exists to cap.
        pre_auth = None;
    }
    // Fixed for the life of the connection: it arrived on one port or the
    // other and cannot change ports.
    let mut encrypted = registered.is_tls();

    let limits = state.auth.limits;
    let enforcing = state.auth.required();
    // An unauthenticated connection is the one thing here a stranger can
    // create, so it gets a deadline rather than the ordinary idle treatment. It
    // is folded into the `select!` that already handles shutdown, so it costs
    // no timer task of its own.
    let deadline = tokio::time::Instant::now() + limits.timeout;
    // **A second deadline, for a strictly smaller population.** The one above
    // only arms when `auth.required` is set, so with authentication off — the
    // default — nothing ever timed out, and `server.max_connections` sockets
    // that send zero bytes took the whole budget for free. This one does not
    // ask whether authentication is enforced; it asks only whether the peer has
    // said anything, which no pooled connection worth keeping has failed to do.
    // `docs/protocol.md`'s promise that an established connection is never
    // idle-reaped is untouched: once a byte arrives this stops arming for good.
    let silent_deadline = (state.limits.first_byte_timeout_ms > 0).then(|| {
        tokio::time::Instant::now()
            + std::time::Duration::from_millis(state.limits.first_byte_timeout_ms)
    });

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
        // Has this connection said anything at all? `protocol` is settled by
        // the first byte and never cleared, so it is `None` exactly while
        // nothing has arrived — which is the population `silent_deadline`
        // exists for, and it contains no legitimate client.
        let silent = protocol.is_none() && silent_deadline.is_some();
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
            _ = tokio::time::sleep_until(silent_deadline.unwrap_or(deadline)), if silent => {
                debug!(
                    timeout_ms = state.limits.first_byte_timeout_ms,
                    "closing a connection that never sent a byte"
                );
                state.metrics.silent_timeout();
                return Ok(());
            }
        };
        if n == 0 {
            return Ok(()); // clean disconnect
        }
        // Two relaxed adds per syscall, which is nothing beside the syscall.
        // The bytes turn a request rate into a bandwidth figure; the count
        // turns it into a block size, which is what decides what the dispatch
        // tier below costs — see `socket_reads`.
        state
            .metrics
            .outcomes
            .bytes_read
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        state
            .metrics
            .outcomes
            .socket_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // **No connection may make the server hold unbounded bytes**, and this
        // is the one check that says so without consulting the authentication
        // state. It used to arm only for `unauthenticated`, which is
        // `enforcing && !authenticated` — so with `auth.required` off, the
        // default, it never armed at all and there was no ceiling of any kind.
        //
        // A command is buffered whole before it is parsed, so until the parser
        // runs there is nothing to charge the bytes to but the socket they came
        // in on. RESP is where that bit: `MAX_ARGS` is 8200 and `MAX_BULK_LEN`
        // is 64 MiB, so one legal command may declare half a terabyte of
        // arguments, and every byte of it is held before the parser is allowed
        // an opinion. Measured, one connection reached 310 MB resident — 300
        // times `store.max_value_bytes` — and had no reason to stop.
        //
        // Two ceilings, because the populations differ. Everything legal before
        // authenticating is small — a VCP `HELLO` is 16 bytes and an `AUTH` at
        // both ceilings is under 600, and the two text dialects' credentials
        // are one short line each — so a connection that has presented nothing
        // gets the tighter one.
        let ceiling = if unauthenticated {
            PRE_AUTH_MAX_BUFFERED
        } else {
            state.limits.max_buffered_bytes
        };
        if read_buf.len() > ceiling {
            debug!(
                buffered = read_buf.len(),
                ceiling, unauthenticated, "closing a connection that buffered too much"
            );
            state.metrics.buffer_ceiling();
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
        // A gated connection's write owes a refusal that only the dialect's own
        // executor can render, so the awaited path is withheld from it entirely.
        // Used by `measure_vcp` for the reserve cap as well, which is why it is
        // asked separately from `may_write` below.
        let gated = !conn_auth.is_authenticated() && state.auth.required();
        // **The fourth path into storage.** The awaited write path decodes a run
        // of plain writes and submits it to the store directly — it passes
        // through neither `dispatch::execute` nor any dialect's executor, so it
        // is a third bypass beside the fused read and `SetBatch`, and it has to
        // be withheld from a connection whose writes owe a refusal. That covers
        // both reasons a write can be refused, which is exactly what
        // `AuthState::allows` answers: for want of authentication, and for want
        // of the `write` role. Either way the refusal is the executor's to word.
        //
        // Recomputed every iteration, so an `AUTH` that narrows the identity
        // takes effect on the next read. Within one read it cannot go stale in
        // the dangerous direction: a block carrying an `AUTH` is not a block of
        // plain writes, so it never reaches this path to begin with.
        let may_write = state.auth.allows(&conn_auth, crate::auth::Roles::WRITE);
        // Who is asking, for whichever dialect the first byte settled on. All
        // three fields are fixed for this connection's life, so this is a copy
        // of two loads rather than anything computed.
        let settled = protocol.expect("set above");
        let origin = registered.origin(match settled {
            Protocol::Vcp => crate::metrics::Dialect::Vcp,
            Protocol::Memcached => crate::metrics::Dialect::Memcached,
            Protocol::Resp => crate::metrics::Dialect::Resp,
        });
        let keep_going = match settled {
            Protocol::Vcp => {
                drain(
                    &state,
                    &mut conn_auth,
                    &mut read_buf,
                    &mut write_buf,
                    &mut (),
                    |buf, runs| measure_vcp(buf, gated, runs),
                    origin,
                    |state, conn, origin, block, (), out| {
                        crate::vcp::execute_vcp_block(state, conn, origin, block, out)
                    },
                    |_, _| {},
                    None,
                    &mut runs,
                )
                .await?
            }
            Protocol::Memcached => {
                // The per-connection slot `drain` already carries — RESP uses
                // it for the negotiated version — carries this dialect's one
                // per-connection fact instead: whether the socket underneath
                // is encrypted, which is what `stats settings` has to answer
                // for `ssl_enabled`.
                drain(
                    &state,
                    &mut conn_auth,
                    &mut read_buf,
                    &mut write_buf,
                    &mut encrypted,
                    measure_memcached,
                    origin,
                    |state, conn, origin, block, tls, out| {
                        execute_memcached_block(state, conn, origin, block, *tls, out)
                    },
                    |_, _| {},
                    may_write.then_some((
                        crate::memcached::parse_memcached_writes as ParseWrites,
                        origin,
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
                    origin,
                    crate::resp::execute_block,
                    // Redis answers a protocol error and *then* hangs up, so the
                    // client learns why instead of seeing a bare disconnect.
                    vash_proto::resp::encode::protocol_error,
                    may_write.then_some((
                        crate::resp::parse_writes as ParseWrites,
                        origin,
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
        if let Some(identity) = conn_auth.identity() {
            // Re-reported per batch rather than once, because an `AUTH` on a
            // live connection replaces the identity — and therefore the roles —
            // and `stats conns` must not keep showing the previous one.
            registered.authenticated(identity.roles());
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
            // Mandatory, not hygiene. Over a plaintext socket this is a no-op,
            // and the loop went without it for eleven milestones. Over TLS,
            // `write_all` only means the session *accepted* the bytes: if the
            // socket was not writable for all of them, the remainder sits as
            // ciphertext inside `rustls` and this task then blocks on the read
            // above, waiting for a request the client will not send because it
            // is waiting for the reply still sitting in our buffer. That is
            // the hang Phase 0 recorded as a write-write deadlock; it was
            // this. See tls-proposal.md §8.4.
            stream.flush().await?;
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
pub(crate) enum RunKind {
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
/// Byte lengths because that is what the executors take, and because the block
/// is a refcounted `Bytes` — a run is a slice of it and costs no copy.
///
/// `count` is carried beside `len` because the two answer different questions.
/// The executors need the bytes; [`drain`] needs to know how many commands will
/// share one hop, because that is what decides whether the hop is worth taking.
/// See `docs/performance-proposals.md` §23.
#[derive(Debug)]
struct Run {
    len: usize,
    count: usize,
    kind: RunKind,
}

impl Run {
    /// Adds one command of `len` bytes and `kind`, extending the run in progress
    /// when it matches so that the common uniform block still produces exactly
    /// one.
    ///
    /// One call is one command — every call site is inside a loop that has just
    /// framed a single command, and a recoverable parse error counts as the
    /// command whose place it occupies.
    fn extend(runs: &mut Vec<Run>, kind: RunKind, len: usize) {
        match runs.last_mut() {
            Some(last) if last.kind == kind => {
                last.len += len;
                last.count += 1;
            }
            _ => runs.push(Run {
                len,
                count: 1,
                kind,
            }),
        }
    }
}

/// Whether a run of `count` commands of `kind` is worth serving inline.
///
/// **The hop is paid once per run**, so one command pays all of it and sixteen
/// pay a sixteenth each — and once it is amortised, the pool's several threads
/// beat inline reads confined to the runtime workers. Measured on a four-core
/// container: closed loop 273,933 inline against 97,582 pooled, pipelined
/// 741,206 pooled against ~580,000 inline. Neither tier wins both shapes, so
/// the choice is per run. See `docs/performance-proposals.md` §23.
///
/// Extracted so the rule can be checked without a socket, and so `drain` and its
/// one-run lookahead cannot drift apart — both call this.
///
/// Sending *more* runs to the pool never weakens what `inline_reads` asserts:
/// the pool is the safe route by construction, and this only narrows where the
/// residency bet is taken.
fn worth_inlining(kind: RunKind, count: usize, enabled: bool, max_commands: usize) -> bool {
    kind == RunKind::Reads && enabled && count <= max_commands
}

/// Executes a block of one dialect's commands, rendering the replies.
/// A dialect's block executor.
///
/// A plain `fn` pointer rather than an `impl Fn`, and the [`Origin`] is a
/// parameter for that reason: a closure capturing it would no longer coerce
/// here, and this is moved into a `spawn_blocking` once per block — a boxed
/// closure per block, to carry two words that every one of these already needs,
/// is the wrong trade.
///
/// [`Origin`]: crate::dispatch::Origin
type RunBlock<S> = fn(
    &ServerState,
    &mut ConnAuth,
    crate::dispatch::Origin,
    &[u8],
    &mut S,
    &mut Vec<u8>,
) -> Closing;

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
///
/// # Why the block is parsed again here
///
/// `measure` has already walked these very bytes to find the run, so parsing
/// them a second time looks like waste. It is not avoidable: `measure` reads
/// `&BytesMut`, and the run only exists once `split_to` has moved those bytes
/// into a refcounted `Bytes` — which invalidates every borrow the measuring
/// parse produced. Carrying the parsed commands across that hand-off would
/// mean copying each key and value out of the buffer, which is the copy this
/// whole path exists to avoid.
///
/// So the boundary is deliberate, and the response to it is to keep the
/// *measuring* pass cheap rather than to try to reuse it: `measure` decides
/// only which tier a run is offered to, and both dialects answer that
/// structurally now — see [`crate::resp::batchable_write`], which used to read
/// the clock per `SET` to answer a question this function asks again anyway.
async fn run_writes_awaited(
    state: &Arc<ServerState>,
    block: &[u8],
    out: &mut Vec<u8>,
    parse: ParseWrites,
    origin: crate::dispatch::Origin,
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

    crate::dispatch::count_stored(state, origin, &run.sets, &cas, started.elapsed());
    for suppressed in &run.suppress {
        if !suppressed {
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
// Eight, and the eighth is the `Origin` the block executor needs. A parameter
// struct here would bundle a borrowed buffer, an owned block and two flags that
// have nothing to do with each other, to satisfy a lint.
#[expect(clippy::too_many_arguments)]
async fn run_block<S: Copy + Send + 'static>(
    state: &Arc<ServerState>,
    conn_auth: &mut ConnAuth,
    dialect: &mut S,
    block: Bytes,
    write_buf: &mut Vec<u8>,
    inline: bool,
    origin: crate::dispatch::Origin,
    run: RunBlock<S>,
) -> std::io::Result<Closing> {
    if inline {
        return Ok(run(state, conn_auth, origin, &block, dialect, write_buf));
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
            origin,
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
    origin: crate::dispatch::Origin,
    run: RunBlock<S>,
    on_fatal: fn(&mut Vec<u8>, &'static str),
    // How this dialect decodes a run of nothing but writes, and what it says
    // for each one that stored. `None` for a dialect that stays on the ordinary
    // path — see `measure_vcp`.
    writes: Option<(ParseWrites, crate::dispatch::Origin, &'static [u8])>,
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
        if !state.inline_reads() && runs.len() > 1 {
            let whole = measured.complete;
            let commands = runs.iter().map(|r| r.count).sum();
            runs.clear();
            runs.push(Run {
                len: whole,
                count: commands,
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
            //
            // A read run goes inline only while it is small enough to be worth
            // the hop it saves. See `worth_inlining`.
            let inline = worth_inlining(
                runs[index].kind,
                runs[index].count,
                state.inline_reads(),
                state.inline_max_commands,
            );
            let awaitable = runs[index].kind == RunKind::Writes && writes.is_some();

            // Runs that both end up on the pool are merged into one hop, so a
            // block that gains nothing from splitting pays for nothing either —
            // with `inline_reads` off, that includes its read runs.
            if !inline && !awaitable {
                while let Some(next) = runs.get(index + 1) {
                    // The same rule as `inline` above, or a read run headed for
                    // the pool would break a merge it belongs in.
                    let next_inline = worth_inlining(
                        next.kind,
                        next.count,
                        state.inline_reads(),
                        state.inline_max_commands,
                    );
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
            if run_block(
                state, conn_auth, dialect, slice, write_buf, inline, origin, run,
            )
            .await?
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
        // **Never size an allocation from a length the peer merely declared.**
        // `measured.reserve` comes from a frame header, and a VCP one may
        // announce 64 MiB in twelve bytes — so honouring it outright let a
        // client commit 64 MiB per connection for the cost of a header it never
        // followed up on. Measured before this clamp: twenty connections and
        // 560 bytes of traffic committed 1.28 GiB, and at the default
        // `max_connections` that is 640 GiB from under 300 KB.
        //
        // Clamping to twice what has already arrived keeps everything the
        // reserve was for and removes the leverage. It is amortised doubling —
        // which `BytesMut` would do anyway — with the target known, so it never
        // overshoots the frame: a client genuinely streaming a large one
        // converges on its size in a handful of steps and stops there, and a
        // client that has sent twelve bytes gets the buffer it already has.
        let ceiling = read_buf.capacity().max(read_buf.len().saturating_mul(2));
        let target = measured.reserve.min(ceiling);
        read_buf.reserve(target.saturating_sub(read_buf.len()));
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

/// Measures RESP commands from their multibulk framing, **without decoding
/// one**.
///
/// This used to run the whole of `resp::parse` — `parse_command` included — over
/// a block it was only measuring, and every byte of that block is then parsed
/// again by `resp::execute_block`. Measured, that throwaway parse was 86.6 ns
/// per `GET` against the 272 ns a whole storage read and encode costs, and it
/// allocated a `Vec` of key slices for each of the variadic commands. It now
/// does what `measure_vcp` above has always done: find the boundary, classify
/// from the verb, and leave the meaning to the executor. See
/// `docs/performance-proposals.md` §17.
///
/// A recoverable error no longer has an arm of its own. It does not need one:
/// `classify` answers `Other` for anything it does not recognise, the executor
/// meets the error where it always did, and both walks agree on `consumed` —
/// which `peek_and_parse_agree_on_the_boundary` pins.
fn measure_resp(buf: &[u8], runs: &mut Vec<Run>) -> Measured {
    use vash_proto::resp;

    let mut measured = Measured::default();
    loop {
        match resp::peek(&buf[measured.complete..], crate::resp::classify) {
            Ok(resp::Framed::Incomplete) => break,
            Ok(resp::Framed::Command {
                classified,
                consumed,
            }) => {
                Run::extend(runs, classified, consumed);
                measured.complete += consumed;
            }
            // Framing is the only thing this walk judges, and every framing
            // error is fatal — which is what `resp::Fatal` being its own type
            // says, rather than a comment saying it.
            Err(resp::Fatal(detail)) => {
                measured.fatal = Some(detail);
                break;
            }
        }
    }
    measured
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::{Run, RunKind, worth_inlining};

    #[test]
    fn extend_counts_one_command_per_call_and_merges_like_kinds() {
        let mut runs = Vec::new();
        for _ in 0..16 {
            Run::extend(&mut runs, RunKind::Reads, 30);
        }
        assert_eq!(runs.len(), 1, "a uniform block is still exactly one run");
        assert_eq!(runs[0].count, 16, "one call is one command");
        assert_eq!(runs[0].len, 480, "and the bytes still add up");

        // A change of kind starts a new run, and the count starts again with it.
        Run::extend(&mut runs, RunKind::Writes, 40);
        Run::extend(&mut runs, RunKind::Writes, 40);
        assert_eq!(runs.len(), 2);
        assert_eq!((runs[1].count, runs[1].len), (2, 80));
        assert_eq!(runs[0].count, 16, "the finished run is left alone");
    }

    #[test]
    fn a_closed_loop_read_goes_inline_and_a_pipelined_one_does_not() {
        // The whole of §23 in two assertions: at the same threshold, one
        // command takes the tier that suits one command and sixteen take the
        // tier that suits sixteen.
        assert!(worth_inlining(RunKind::Reads, 1, true, 4));
        assert!(!worth_inlining(RunKind::Reads, 16, true, 4));
        // And the boundary is inclusive, so the threshold names the largest run
        // that still goes inline rather than the smallest that does not.
        assert!(worth_inlining(RunKind::Reads, 4, true, 4));
        assert!(!worth_inlining(RunKind::Reads, 5, true, 4));
    }

    #[test]
    fn only_reads_are_ever_inlined() {
        // Writes wait on the writer queue by design and `Other` covers the
        // commands no fast path models, so neither may run on a runtime worker
        // however short the run is.
        for kind in [RunKind::Writes, RunKind::Other] {
            assert!(
                !worth_inlining(kind, 1, true, usize::MAX),
                "{kind:?} must never be inlined"
            );
        }
    }

    #[test]
    fn the_flag_still_governs_and_the_bounds_are_reachable() {
        // With inline reads off, the threshold says nothing at all — every run
        // takes the pool, which is what the default configuration does today.
        assert!(!worth_inlining(RunKind::Reads, 1, false, usize::MAX));
        // `0` turns inlining off without touching the other flags...
        assert!(!worth_inlining(RunKind::Reads, 1, true, 0));
        // ...and `usize::MAX` restores the unconditional behaviour every
        // measurement before §23 was taken against.
        assert!(worth_inlining(RunKind::Reads, usize::MAX, true, usize::MAX));
    }

    /// The clamp `drain` applies before honouring a declared frame length.
    ///
    /// Extracted so the arithmetic can be checked without a socket. It must
    /// stay in step with the call site; there is one line of it, and this is
    /// what says what that line is for.
    fn clamped(reserve: usize, buffered: usize, capacity: usize) -> usize {
        let ceiling = capacity.max(buffered.saturating_mul(2));
        reserve.min(ceiling)
    }

    #[test]
    fn a_declared_length_cannot_reserve_more_than_twice_what_arrived() {
        // The measured attack: twelve bytes of header announcing 64 MiB. Before
        // the clamp this reserved the whole 64 MiB, which at the default
        // connection limit is 640 GiB of commit charge for under 300 KB of
        // traffic.
        let declared = 64 * 1024 * 1024;
        assert_eq!(
            clamped(declared, 12, 16 * 1024),
            16 * 1024,
            "a header that arrived alone buys no more than the buffer already has"
        );
    }

    #[test]
    fn a_frame_being_streamed_still_converges_on_its_size() {
        // What the reserve is *for*: a client genuinely sending a large frame
        // reaches its size in a handful of doublings and stops there, rather
        // than growing past it or reallocating all the way up.
        let declared = 1024 * 1024;
        let mut capacity = 16 * 1024;
        let mut steps = 0;
        while capacity < declared {
            capacity = clamped(declared, capacity, capacity).max(capacity);
            // Half arrived; the buffer is asked to hold the rest.
            capacity = clamped(declared, capacity, capacity * 2).max(capacity);
            steps += 1;
            assert!(steps < 16, "the reserve must converge, not crawl");
        }
        assert_eq!(
            capacity, declared,
            "and it must stop exactly at the frame, never above it"
        );
    }

    #[test]
    fn the_clamp_never_shrinks_a_buffer_below_its_capacity() {
        // `reserve` is handed `target - len`, so a target under `len` has to
        // saturate rather than wrap. This is the guard on that.
        let target = clamped(8, 4096, 16 * 1024);
        assert_eq!(target.saturating_sub(4096), 0);
    }

    #[test]
    fn a_real_bytesmut_does_not_grow_to_a_declared_length() {
        // The property as the type actually behaves, rather than as the
        // arithmetic above models it.
        let mut buf = BytesMut::with_capacity(16 * 1024);
        buf.extend_from_slice(&[0u8; 12]);

        let declared = 64 * 1024 * 1024;
        let target = clamped(declared, buf.len(), buf.capacity());
        buf.reserve(target.saturating_sub(buf.len()));

        assert!(
            buf.capacity() < 1024 * 1024,
            "a twelve-byte header must not commit megabytes, let alone {declared}"
        );
    }
}
