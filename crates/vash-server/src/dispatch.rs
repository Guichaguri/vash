//! Command execution: the only place that maps domain outcomes onto wire status
//! codes.
//!
//! **The boundary the three dialects meet at, and nothing else.** This file
//! used to be 1,800 lines carrying that boundary plus two of the three
//! adapters, which made it the one place where "shared by every dialect" and
//! "true of memcached" sat side by side with nothing marking the difference —
//! and the batching, the gating and the domain executor are all things where
//! getting that distinction wrong is how a dialect ends up answering
//! differently from its siblings.
//!
//! The adapters are now siblings of each other, matching the shape [`crate::resp`]
//! already had:
//!
//! - [`crate::memcached`] — the text and meta grammars, the ASCII
//!   authentication tunnel, the dump pager, and the memcached error vocabulary.
//! - [`crate::vcp`] — frame execution, the header gate, `AUTH`, and the TTL
//!   translation that is this protocol's alone.
//! - [`crate::resp`] — the Redis dialect, unchanged.
//!
//! What stays here is what all three genuinely share: [`SetBatch`] and the
//! rules for what may join one, [`execute`] and the domain executor beneath it,
//! the fused read in [`execute_get_into`], the [`Failed`] classification, and
//! `deny`. A function in this file that only one dialect calls is a sign it
//! belongs in that dialect's module.

use tracing::{debug, error, warn};
use vash_core::{Command, Reply, ServerInfo};
use vash_proto::vcp::Status;
use vash_store::StoreError;

use crate::auth::ConnAuth;
use crate::metrics::{CommandKind, Dialect, ErrorClass};
use crate::state::ServerState;

/// A run of consecutive unconditional writes from one pipelined block, held
/// back so that they cross the writer queue **together**.
///
/// # Why this exists
///
/// A block executor used to call [`execute`] per command, and every write in it
/// blocked on its own commit before the next command was even parsed. Group
/// commit therefore only ever saw the concurrency *between* connections and
/// never the concurrency *within* one: measured, a 128-deep pipeline down a
/// single connection produced a mean batch of **1.13** and 505 writes a second,
/// against Redis's 391,129 on the identical command. The pipeline depth bought
/// nothing at all.
///
/// Buffering the run and submitting it as one [`Store::set_many`] turns those
/// 128 round trips into one per shard. It does not make a commit faster; it
/// stops a client having to open a hundred connections before the server will
/// batch anything.
///
/// # What may go in
///
/// Only writes whose reply cannot depend on what the store finds — `SetMode::Set`
/// with no `return_previous`. A guarded write (`add`, `replace`, `cas`,
/// `SET NX`) answers with a verdict, and `SET … GET` answers with the displaced
/// value; both have to be executed where they can be reported, so they flush
/// the run and go the ordinary way.
///
/// # When it must be flushed
///
/// The caller owns this, because only the caller knows what the next command
/// is. A run must be flushed before:
///
/// - **any read**, so a `get` after a `set` in the same block sees it;
/// - **any other storage command**, for the same reason;
/// - **a second write to a key already in the run** — see [`Self::holds`];
/// - **the end of the block**, which is the one every caller must not forget.
///
/// `T` is whatever the dialect needs to render one reply — a `noreply` flag for
/// memcached, a request id for VCP — kept here rather than in three parallel
/// vectors so an entry cannot lose its context.
pub struct SetBatch<'a, T> {
    sets: Vec<vash_core::Set<'a>>,
    notes: Vec<T>,
}

impl<'a, T> Default for SetBatch<'a, T> {
    fn default() -> Self {
        Self {
            sets: Vec::new(),
            notes: Vec::new(),
        }
    }
}

impl<'a, T> SetBatch<'a, T> {
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    /// Whether the run already writes `key`.
    ///
    /// Two writes to one key in a single batch would be applied in order inside
    /// the transaction and would give the right final value — but each one owes
    /// the client a CAS token describing a record that the next one replaces
    /// before anybody sees it. Flushing instead keeps every reply describing the
    /// state the client would have observed had the writes been sent one at a
    /// time, which is what a pipeline promises.
    pub fn holds(&self, key: vash_core::Key<'_>) -> bool {
        self.sets
            .iter()
            .any(|set| set.key.as_bytes() == key.as_bytes())
    }

    pub fn push(&mut self, set: vash_core::Set<'a>, note: T) {
        self.sets.push(set);
        self.notes.push(note);
    }

    /// Executes the run and renders every reply in request order.
    ///
    /// `render` is handed each entry's note and either the CAS token the write
    /// produced or the failure that stopped it — the whole [`Failed`], not just
    /// its status, because a dialect words `too many tags` and `value too large`
    /// from the cause and would otherwise report both as `invalid argument`.
    ///
    /// # A failed batch is retried one at a time
    ///
    /// The batch is one transaction per shard, so a single record the store
    /// refuses — a value past the ceiling, too many tags — takes the whole run
    /// down with it. That would make a client's verdict depend on whether it
    /// happened to pipeline, which is exactly the kind of difference this is not
    /// allowed to introduce. So a failure falls back to [`execute`] per command,
    /// where each one gets the verdict it would have had on its own.
    ///
    /// The fallback is not free and does not need to be: it runs only after
    /// something has already gone wrong, and a retried write is an unconditional
    /// overwrite of the same bytes, so re-applying one that a shard did commit
    /// before the batch failed leaves the same value with a fresher CAS.
    ///
    /// Counting matches [`execute`] exactly, per command rather than per batch,
    /// so `cmd_set` and the write counters cannot drift depending on whether a
    /// client happened to pipeline. The one thing it cannot reproduce is a
    /// per-command latency, since the batch has a single duration: that is
    /// divided across its members, which keeps the mean honest and makes the
    /// distribution narrower than reality.
    pub fn flush(
        &mut self,
        state: &ServerState,
        dialect: Dialect,
        out: &mut Vec<u8>,
        mut render: impl FnMut(&mut Vec<u8>, &T, Result<u64, &Failed>),
    ) {
        if self.sets.is_empty() {
            return;
        }

        let started = std::time::Instant::now();
        match state.store.set_many(&self.sets) {
            Ok(cas) => {
                count_stored(state, dialect, &self.sets, &cas, started.elapsed());
                for (index, note) in self.notes.iter().enumerate() {
                    render(out, note, Ok(cas.get(index).copied().unwrap_or_default()));
                }
            }
            Err(err) => {
                debug!(
                    error = %err,
                    held = self.sets.len(),
                    "a batched write failed; retrying the run one command at a time"
                );
                for (set, note) in self.sets.iter().zip(&self.notes) {
                    // `execute` counts and classifies, so the retry is indistinguishable
                    // from having never batched — which is the point of it.
                    let outcome = execute(state, &Command::Set(set.clone()), dialect);
                    match outcome {
                        Ok(Reply::Stored(vash_core::Stored::Stored(cas))) => {
                            render(out, note, Ok(cas))
                        }
                        // An unconditional write has no other verdict, so this
                        // is a mismatch rather than an outcome to render.
                        Ok(unexpected) => {
                            error!(
                                ?unexpected,
                                "an unconditional write answered with a verdict"
                            );
                            render(out, note, Err(&Failed::status(Status::Internal)));
                        }
                        Err(failed) => render(out, note, Err(&failed)),
                    }
                }
            }
        }

        self.sets.clear();
        self.notes.clear();
    }
}

/// Counts a run of writes that stored, exactly as [`execute`] counts one.
///
/// Shared by [`SetBatch::flush`] and the awaited write path in [`crate::conn`],
/// so `cmd_set` and the write counters cannot depend on which route a client's
/// pipelining happened to take. The batch has a single duration, so it is
/// divided across its members: the mean stays honest and the distribution comes
/// out narrower than reality.
pub fn count_stored(
    state: &ServerState,
    dialect: Dialect,
    sets: &[vash_core::Set<'_>],
    cas: &[u64],
    elapsed: std::time::Duration,
) {
    if sets.is_empty() {
        return;
    }
    let each = elapsed / sets.len() as u32;
    for (index, set) in sets.iter().enumerate() {
        state
            .metrics
            .commands
            .observe(CommandKind::Set, dialect, each);
        state.metrics.outcomes.observe(
            &Command::Set(set.clone()),
            &Reply::Stored(vash_core::Stored::Stored(
                cas.get(index).copied().unwrap_or_default(),
            )),
        );
        state.metrics.write();
    }
}

/// Whether an unconditional write can also be served **without blocking**.
///
/// Stricter than [`batchable`] by one thing: a tagged write cannot. Registering
/// a tag name that the store has not seen is itself a write through the same
/// queue, taken synchronously inside `submit_set_many` before anything is
/// queued — and a runtime worker may not block on that. `SetBatch` is unaffected
/// because it runs on the blocking pool, where such a wait is legal.
pub fn awaitable(command: &Command<'_>) -> bool {
    batchable(command).is_some_and(awaitable_set)
}

/// [`awaitable`], asked of a [`vash_core::Set`] the caller already holds.
///
/// The definition rather than a copy of one: `awaitable` unwraps a `Command` and
/// then asks this, so the two cannot drift.
///
/// It exists because `resp::parse_writes` holds a `Set` and nothing else, and
/// used to build a whole `Command` around it — a clone per write — purely to
/// match the signature above. That is once per `SET` on the awaited write run,
/// which is the path §8 built to be the fast one.
pub fn awaitable_set(set: &vash_core::Set<'_>) -> bool {
    matches!(set.mode, vash_core::SetMode::Set) && !set.return_previous && set.tags.is_empty()
}

/// Whether a command is an unconditional write that [`SetBatch`] may hold back.
///
/// Matching the *mode* rather than the dialect's verb is what keeps this honest
/// across three grammars: memcached's `add`, Redis's `SET NX` and VCP's
/// conditional opcode all arrive here as a guarded `SetMode`, and none of them
/// may be deferred.
pub fn batchable<'c, 'a>(command: &'c Command<'a>) -> Option<&'c vash_core::Set<'a>> {
    match command {
        Command::Set(set)
            if matches!(set.mode, vash_core::SetMode::Set) && !set.return_previous =>
        {
            Some(set)
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Closing {
    Yes,
    No,
}

/// Counts and logs one role denial.
///
/// Shared by all three dialects so a denial reads the same in the metrics and
/// the log whichever wire it arrived on, and so the two cannot be updated
/// apart.
///
/// **Logged at `warn`, and deliberately per denial rather than once per
/// connection.** A wrong role set will not fix itself, so the temptation is to
/// rate-limit it the way a peer's refusal is — but unlike that case the fix is
/// a one-line edit, and the operator needs the command name to make it. The
/// counter is what an alert reads; the log is what someone reads once, at the
/// moment they narrow a row and something breaks.
pub(crate) fn deny(
    state: &ServerState,
    conn: &ConnAuth,
    missing: crate::auth::Roles,
    command: impl std::fmt::Display,
) {
    state.metrics.auth_denied(missing);
    warn!(
        identity = conn.identity().map(crate::auth::Identity::name).unwrap_or("-"),
        %command,
        missing = %missing,
        "refused a command this identity has no role for"
    );
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

/// [`execute_get_into`] over a batch: **renders every key straight out of the
/// map, in request order.**
///
/// The multi-key half of the same trade. `dispatch::execute` answers a
/// `GetMany` with `Reply::Values`, which copies every hit out of the memory map
/// into a `Value` and allocates a vector to hold them — the two costs
/// `execute_get_into` was built to remove for a single key, still being paid by
/// every `MGET` and every memcached `get` naming more than one. Measured, that
/// is 21–35% per key. See `docs/performance-proposals.md` §18.
///
/// `render` is called once per key, in order, with `None` for a miss. **It must
/// not block or call back into the store**: a read transaction per shard is open
/// for the whole batch.
///
/// The counting is `execute`'s, reproduced for the same reason
/// [`execute_get_into`] reproduces it — one `commands` observation for the
/// command, and one `read` carrying the batch's hits and misses.
/// `OutcomeMetrics::observe` has no `Command::GetMany` arm to reproduce, which
/// `metrics::tests::observe_has_nothing_to_say_about_a_get` keeps true.
pub fn execute_get_many_into(
    state: &ServerState,
    keys: &[vash_core::Key<'_>],
    kind: CommandKind,
    dialect: Dialect,
    render: &mut dyn FnMut(usize, Option<vash_core::ValueRef<'_>>),
) -> Result<(), Failed> {
    let mut hits = 0u64;

    let started = std::time::Instant::now();
    let outcome = state.store.get_many_with(keys, &mut |index, value| {
        hits += u64::from(value.is_some());
        render(index, value);
    });
    state
        .metrics
        .commands
        .observe(kind, dialect, started.elapsed());

    // Counted before the error is propagated, exactly as `execute_get_into`
    // does: a read that failed still took the time it took. The hit split is
    // only counted for a batch that finished, because a partial one has not
    // decided about the keys it never reached.
    outcome.map_err(to_failed)?;
    state.metrics.read(hits, keys.len() as u64 - hits);
    Ok(())
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
pub(crate) fn listing_gate(state: &ServerState) -> Result<(), Failed> {
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
