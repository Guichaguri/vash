//! The single writer thread, and the group commit that makes it fast.
//!
//! LMDB allows one writer per environment. Rather than fight that, every write
//! is funnelled to one thread that packs as many as it can into a single
//! transaction. The commit cost â€” the expensive part â€” is then amortised across
//! the whole batch instead of being paid per operation.
//!
//! The batch is **whatever had already queued while the previous commit was in
//! flight**. There is no artificial linger: an idle server commits a lone write
//! immediately, and a loaded one naturally forms large batches because the
//! queue fills during each commit. Throughput therefore self-regulates against
//! load with no tuning knob and no latency penalty. `linger` exists for
//! deployments that would rather trade latency for throughput, and defaults to
//! zero.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use cache_core::{Key, SetMode, Stored};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded};
use tracing::{debug, error, info, warn};

use crate::SweepStats;
use crate::config::WriteConfig;
use crate::engine::{LmdbEngine, PreparedSet, Pressure, TagMerge};
use crate::error::{Result, StoreError};

pub(crate) enum WriteOp {
    Set(Vec<PreparedSet>),
    /// A single write under a guard. Kept apart from `Set` because its outcome
    /// is a verdict rather than a CAS token, and it must not be batched with
    /// other writes to the same key.
    ConditionalSet(PreparedSet, SetMode),
    Delete(Vec<Box<[u8]>>),
    Touch {
        key: Box<[u8]>,
        ttl_secs: u32,
    },
    Incr {
        key: Box<[u8]>,
        delta: u64,
        decrement: bool,
    },
    /// Register tag names, each with the generation the rest of the node
    /// already holds for it.
    CreateTags(Vec<(Box<[u8]>, u64)>),
    DeleteByTag(Box<[u8]>),
    /// Raise a tag's generation to at least this value, creating it if unknown.
    /// The receiving half of cluster invalidation.
    MergeTag {
        name: Box<[u8]>,
        generation: u64,
    },
    Flush,
    Sync,
}

impl WriteOp {
    /// Individual key operations this job carries.
    ///
    /// Counted rather than treating one job as one unit, because a `SET_MANY`
    /// of 256 is 256 writes sharing a commit â€” which is exactly what the batch
    /// metric is supposed to show.
    fn item_count(&self) -> usize {
        match self {
            Self::Set(items) => items.len(),
            Self::Delete(keys) => keys.len(),
            Self::ConditionalSet(..)
            | Self::Touch { .. }
            | Self::Incr { .. }
            | Self::DeleteByTag(_)
            | Self::MergeTag { .. }
            | Self::Flush => 1,
            Self::CreateTags(names) => names.len(),
            Self::Sync => 0,
        }
    }
}

pub(crate) enum WriteOutcome {
    Cas(Vec<u64>),
    Conditional(Stored),
    Deleted(Vec<bool>),
    Touched(bool),
    /// `None` when the key was absent.
    Counter(Option<u64>),
    /// `None` when the tag was never registered, so nothing referenced it.
    Invalidated(Option<u64>),
    /// The generation the tag holds after a max-merge.
    Merged(u64),
    Flushed(u32),
    Done,
}

/// A change to in-memory state that must not be published until the
/// transaction carrying it has committed.
///
/// Ordering matters in one direction only, and it is the dangerous one: a
/// generation bumped in RAM but lost to a failed commit would let invalidated
/// records come back to life after a restart. Applying RAM changes strictly
/// after the commit means a failure leaves RAM matching disk.
enum PostCommit {
    TagCreated {
        name: Box<[u8]>,
        id: u32,
        generation: u64,
    },
    TagGeneration {
        id: u32,
        generation: u64,
    },
    Epoch(u32),
}

struct WriteJob {
    op: WriteOp,
    reply: Sender<Result<WriteOutcome>>,
}

/// Counters describing how well group commit is working.
///
/// `committed_ops / commits` is the average batch size, which is the number
/// that says whether the writer is actually amortising commit cost or paying it
/// per operation.
#[derive(Debug, Default)]
pub(crate) struct WriterMetrics {
    pub commits: AtomicU64,
    pub committed_ops: AtomicU64,
    pub sweeps: AtomicU64,
    pub reclaimed: AtomicU64,
    pub sweep_lag_ms: AtomicU64,
    /// Records freed by tag reclamation, as opposed to expiry sweeping.
    pub tag_reclaimed: AtomicU64,
    /// Live records dropped to reclaim space under capacity pressure.
    pub evicted: AtomicU64,
}

/// Client handle onto the writer thread.
pub(crate) struct Writer {
    tx: Option<Sender<WriteJob>>,
    thread: Option<JoinHandle<()>>,
    metrics: Arc<WriterMetrics>,
}

impl Writer {
    pub fn spawn(engine: Arc<LmdbEngine>, config: WriteConfig) -> Self {
        let (tx, rx) = bounded(config.queue_depth);
        let metrics = Arc::new(WriterMetrics::default());
        let thread_metrics = Arc::clone(&metrics);
        let thread = std::thread::Builder::new()
            .name("kached-writer".into())
            .spawn(move || writer_loop(engine, rx, config, thread_metrics))
            .expect("spawning the writer thread");

        Self {
            tx: Some(tx),
            thread: Some(thread),
            metrics,
        }
    }

    pub fn metrics(&self) -> &WriterMetrics {
        &self.metrics
    }

    /// Submits an operation and blocks until it has been committed.
    ///
    /// Callers are on a blocking thread pool, never on an async runtime worker,
    /// so blocking here is the intended behaviour.
    fn submit(&self, op: WriteOp) -> Result<WriteOutcome> {
        let (reply_tx, reply_rx) = bounded(1);
        let job = WriteJob {
            op,
            reply: reply_tx,
        };

        let tx = self.tx.as_ref().ok_or(StoreError::ShuttingDown)?;
        match tx.try_send(job) {
            Ok(()) => {}
            // A full queue means the writer is already saturated. Failing fast
            // is better than queueing without bound: a client told "overloaded"
            // can fall back to its origin, a client left waiting cannot.
            Err(TrySendError::Full(_)) => return Err(StoreError::Overloaded),
            Err(TrySendError::Disconnected(_)) => return Err(StoreError::ShuttingDown),
        }

        reply_rx.recv().map_err(|_| StoreError::ShuttingDown)?
    }

    pub fn set_many(&self, prepared: Vec<PreparedSet>) -> Result<Vec<u64>> {
        match self.submit(WriteOp::Set(prepared))? {
            WriteOutcome::Cas(cas) => Ok(cas),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    pub fn delete_many(&self, keys: Vec<Box<[u8]>>) -> Result<Vec<bool>> {
        match self.submit(WriteOp::Delete(keys))? {
            WriteOutcome::Deleted(hits) => Ok(hits),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    pub fn conditional_set(&self, prepared: PreparedSet, mode: SetMode) -> Result<Stored> {
        match self.submit(WriteOp::ConditionalSet(prepared, mode))? {
            WriteOutcome::Conditional(outcome) => Ok(outcome),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    pub fn incr(&self, key: Key<'_>, delta: u64, decrement: bool) -> Result<Option<u64>> {
        let op = WriteOp::Incr {
            key: key.as_bytes().into(),
            delta,
            decrement,
        };
        match self.submit(op)? {
            WriteOutcome::Counter(value) => Ok(value),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    pub fn touch(&self, key: Key<'_>, ttl_secs: u32) -> Result<bool> {
        let op = WriteOp::Touch {
            key: key.as_bytes().into(),
            ttl_secs,
        };
        match self.submit(op)? {
            WriteOutcome::Touched(hit) => Ok(hit),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    /// Registers tags durably, each at the generation the rest of the node
    /// already holds for it. Must complete before any record referencing them
    /// is encoded.
    pub fn create_tags(&self, names: Vec<(Box<[u8]>, u64)>) -> Result<()> {
        self.submit(WriteOp::CreateTags(names)).map(|_| ())
    }

    /// Raises a tag's generation to at least `generation`, creating the tag if
    /// this shard has never seen the name. Returns the resulting generation.
    pub fn merge_tag(&self, name: &[u8], generation: u64) -> Result<u64> {
        let op = WriteOp::MergeTag {
            name: name.into(),
            generation,
        };
        match self.submit(op)? {
            WriteOutcome::Merged(generation) => Ok(generation),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    /// Invalidates a tag. Returns the new generation, or `None` if the tag was
    /// never registered and so nothing referenced it.
    pub fn delete_by_tag(&self, name: &[u8]) -> Result<Option<u64>> {
        match self.submit(WriteOp::DeleteByTag(name.into()))? {
            WriteOutcome::Invalidated(generation) => Ok(generation),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    pub fn flush(&self) -> Result<u32> {
        match self.submit(WriteOp::Flush)? {
            WriteOutcome::Flushed(epoch) => Ok(epoch),
            _ => Err(StoreError::Corrupt(
                "writer returned the wrong reply".into(),
            )),
        }
    }

    pub fn sync(&self) -> Result<()> {
        self.submit(WriteOp::Sync).map(|_| ())
    }

    /// Closes the queue and waits for the thread to drain and exit.
    pub fn shutdown(&mut self) {
        drop(self.tx.take());
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            error!("writer thread panicked");
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn writer_loop(
    engine: Arc<LmdbEngine>,
    rx: Receiver<WriteJob>,
    config: WriteConfig,
    metrics: Arc<WriterMetrics>,
) {
    let sweep_interval = Duration::from_millis(config.sweep_interval_ms);
    let mut last_sweep = Instant::now();
    let mut batch: Vec<WriteJob> = Vec::with_capacity(config.max_batch);

    info!(
        max_batch = config.max_batch,
        queue_depth = config.queue_depth,
        sweep_interval_ms = config.sweep_interval_ms,
        sweep_batch = config.sweep_batch,
        "writer thread started"
    );

    // While a tag reclamation is outstanding the loop stops waiting, so a
    // backlog drains at full speed instead of one batch per sweep interval.
    // A million-key tag would otherwise take minutes to reclaim.
    let mut reclaim_pending = true; // unknown at startup: a job may have survived a restart

    loop {
        let idle_timeout = if reclaim_pending {
            Duration::ZERO
        } else {
            sweep_interval
        };

        // Block until there is work, but wake up on the sweep interval so the
        // sweeper runs during idle periods for free.
        match rx.recv_timeout(idle_timeout) {
            Ok(job) => batch.push(job),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        if config.linger_us > 0 && !batch.is_empty() {
            std::thread::sleep(Duration::from_micros(config.linger_us));
        }

        // Take whatever else has arrived. This â€” and not a timer â€” is what
        // forms the batch.
        while batch.len() < config.max_batch {
            match rx.try_recv() {
                Ok(job) => batch.push(job),
                Err(_) => break,
            }
        }

        // Maintenance shares the batch's transaction, so it costs no extra
        // commit. Under sustained write load the interval check is what keeps
        // it from starving.
        let due_for_maintenance = reclaim_pending || last_sweep.elapsed() >= sweep_interval;
        if batch.is_empty() && !due_for_maintenance {
            continue;
        }

        match commit_batch(
            &engine,
            &mut batch,
            due_for_maintenance,
            &config,
            &metrics,
            &mut reclaim_pending,
        ) {
            Ok(()) => {}
            Err(e) if e.is_capacity() => {
                // The map is full. The batch that hit it was aborted, and with
                // it the maintenance pass that would have freed space — so
                // without this the store would never recover: every subsequent
                // write would open a transaction, hit the same wall, and abort
                // it again.
                //
                // Marking the pressure critical first means callers are
                // refused before they reach the queue, so the emergency
                // reclaim below is not competing with a stream of doomed
                // writes.
                warn!("store is full; refusing writes and reclaiming space");
                engine.set_pressure(Pressure::Critical);
                if let Err(e) = free_space(&engine, &config, &metrics) {
                    error!(error = %e, "could not reclaim space from a full store");
                }
                reclaim_pending = true;
            }
            Err(e) => {
                error!(error = %e, "write batch failed");
                // A failed commit says nothing about whether work remains, and
                // spinning on it would burn a core. Back off to the interval.
                reclaim_pending = false;
            }
        }
        if due_for_maintenance {
            last_sweep = Instant::now();
        }
    }

    // The channel is closed and no more jobs can arrive; anything still queued
    // was sent before shutdown and deserves to be committed.
    if !batch.is_empty()
        && let Err(e) = commit_batch(
            &engine,
            &mut batch,
            false,
            &config,
            &metrics,
            &mut reclaim_pending,
        )
    {
        error!(error = %e, "final write batch failed");
    }
    info!("writer thread stopped");
}

fn commit_batch(
    engine: &LmdbEngine,
    batch: &mut Vec<WriteJob>,
    maintenance: bool,
    config: &WriteConfig,
    metrics: &WriterMetrics,
    reclaim_pending: &mut bool,
) -> Result<()> {
    let batch_size = batch.len();
    let committed_items: usize = batch.iter().map(|job| job.op.item_count()).sum();
    let mut wtxn = match engine.write_txn() {
        Ok(txn) => txn,
        Err(e) => {
            // Nothing was applied, so every caller must be told so rather than
            // left waiting.
            let message = e.to_string();
            for job in batch.drain(..) {
                let _ = job.reply.send(Err(StoreError::Corrupt(message.clone())));
            }
            return Err(e);
        }
    };

    let mut outcomes = Vec::with_capacity(batch_size);
    let mut effects: Vec<PostCommit> = Vec::new();
    let mut poisoned: Option<StoreError> = None;

    for job in batch.iter_mut() {
        let outcome = apply(engine, &mut wtxn, &mut job.op, &mut effects);
        if let Err(e) = &outcome
            && e.poisons_transaction()
        {
            poisoned = Some(e.clone_shallow());
        }
        let stop = poisoned.is_some();
        outcomes.push(outcome);
        if stop {
            break;
        }
    }

    // One failed operation invalidates the transaction, so nothing in this
    // batch can land. Abort and tell every caller, rather than pressing on and
    // surfacing MDB_BAD_TXN for writes that were never even attempted.
    if let Some(err) = poisoned {
        drop(wtxn);
        for job in batch.drain(..) {
            let _ = job.reply.send(Err(err.clone_shallow()));
        }
        return Err(err);
    }

    let mut sweep_stats = None;
    let mut reclaim_stats = None;
    let mut evicted = 0usize;
    if maintenance {
        // None of these failures may lose the user writes sharing this
        // transaction; they simply retry next interval.
        match engine.sweep(&mut wtxn, config.sweep_batch) {
            Ok(stats) => sweep_stats = Some(stats),
            Err(e) => warn!(error = %e, "sweep failed"),
        }
        match engine.reclaim_step(&mut wtxn, config.reclaim_batch) {
            Ok(stats) => reclaim_stats = stats,
            Err(e) => warn!(error = %e, "tag reclamation failed"),
        }

        // Eviction comes last so it measures usage after the free work has
        // been done, and only evicts live data if reclaiming dead data was not
        // enough.
        evicted = match apply_pressure(engine, &mut wtxn, config) {
            Ok(freed) => freed,
            Err(e) => {
                warn!(error = %e, "eviction failed");
                0
            }
        };
    }

    let mut needs_sync = false;
    for job in batch.iter() {
        if matches!(job.op, WriteOp::Sync) {
            needs_sync = true;
        }
    }

    match wtxn.commit() {
        Ok(()) => {
            // Only now is it safe to publish: everything below is durable.
            for effect in effects {
                match effect {
                    PostCommit::TagCreated {
                        name,
                        id,
                        generation,
                    } => engine.tags().insert(name, id, generation),
                    PostCommit::TagGeneration { id, generation } => {
                        engine.tags().merge_generation(id, generation)
                    }
                    PostCommit::Epoch(epoch) => engine.set_epoch(epoch),
                }
            }

            if batch_size > 0 {
                metrics.commits.fetch_add(1, Ordering::Relaxed);
                metrics
                    .committed_ops
                    .fetch_add(committed_items as u64, Ordering::Relaxed);
            }
            for (job, outcome) in batch.drain(..).zip(outcomes) {
                let _ = job.reply.send(outcome);
            }
            if let Some(stats) = sweep_stats {
                metrics.sweeps.fetch_add(1, Ordering::Relaxed);
                metrics
                    .reclaimed
                    .fetch_add(stats.reclaimed as u64, Ordering::Relaxed);
                metrics.sweep_lag_ms.store(stats.lag_ms, Ordering::Relaxed);
                report_sweep(stats);
            }

            if evicted > 0 {
                metrics.evicted.fetch_add(evicted as u64, Ordering::Relaxed);
            }

            // Only a committed reclamation pass counts, and only an unfinished
            // one keeps the loop hot.
            *reclaim_pending = match reclaim_stats {
                Some(stats) => {
                    metrics
                        .tag_reclaimed
                        .fetch_add(stats.reclaimed as u64, Ordering::Relaxed);
                    if stats.scanned > 0 || stats.completed {
                        debug!(
                            scanned = stats.scanned,
                            reclaimed = stats.reclaimed,
                            orphaned = stats.orphaned,
                            retained = stats.retained,
                            completed = stats.completed,
                            "reclaimed tagged records"
                        );
                    }
                    !stats.completed
                }
                // No job to work on.
                None => false,
            };

            // Still evicting means still over the hard watermark, so the loop
            // must keep working rather than wait out the idle interval.
            *reclaim_pending |= evicted > 0;
            if needs_sync && let Err(e) = engine.sync() {
                error!(error = %e, "explicit sync failed");
            }
            Ok(())
        }
        Err(e) => {
            // The transaction is atomic: if the commit failed, nothing landed,
            // so no caller may be told it succeeded.
            let err = StoreError::from_heed(e);
            let message = err.to_string();
            for job in batch.drain(..) {
                let _ = job.reply.send(Err(match &err {
                    StoreError::CapacityFull => StoreError::CapacityFull,
                    _ => StoreError::Corrupt(message.clone()),
                }));
            }
            Err(err)
        }
    }
}

/// Frees space in a transaction of its own, after a batch failed because the
/// map was full.
///
/// Its own transaction because the failed one is gone, and a smaller retry
/// because the deletions themselves need pages: on a copy-on-write B-tree,
/// removing a record dirties the path to it, so an eviction batch sized for
/// normal running can be too large to commit on a full map.
fn free_space(engine: &LmdbEngine, config: &WriteConfig, metrics: &WriterMetrics) -> Result<()> {
    for budget in [config.eviction.batch, config.eviction.batch / 8, 8] {
        let budget = budget.max(1);
        let mut wtxn = engine.write_txn()?;

        let swept = engine
            .sweep(&mut wtxn, budget)
            .map(|s| s.reclaimed)
            .unwrap_or(0);
        let evicted = engine
            .evict(&mut wtxn, budget)
            .map(|s| s.reclaimed)
            .unwrap_or(0);

        match wtxn.commit() {
            Ok(()) => {
                let freed = swept + evicted;
                metrics.evicted.fetch_add(evicted as u64, Ordering::Relaxed);
                info!(freed, budget, "reclaimed space from a full store");
                return Ok(());
            }
            // Too big to commit even as a deletion; try a smaller bite.
            Err(e) => {
                debug!(budget, error = %e, "eviction batch did not fit; retrying smaller")
            }
        }
    }

    Err(StoreError::CapacityFull)
}

/// Measures capacity pressure and, past the hard watermark, evicts to get back
/// under the soft one. Returns how many records were freed.
///
/// The measurement is taken inside the write transaction, so it reflects the
/// space this batch just consumed rather than a stale reading.
fn apply_pressure(
    engine: &LmdbEngine,
    wtxn: &mut heed::RwTxn,
    config: &WriteConfig,
) -> Result<usize> {
    let limits = &config.eviction;
    // Measured inside this transaction, so it reflects the space this batch
    // just consumed and the pages the sweep just freed.
    let utilisation = engine.utilisation_in(wtxn)?;

    let level = if utilisation >= limits.critical {
        Pressure::Critical
    } else if utilisation >= limits.hard {
        Pressure::Hard
    } else if utilisation >= limits.soft {
        Pressure::Soft
    } else {
        Pressure::Normal
    };

    let previous = engine.pressure();
    engine.set_pressure(level);
    if level != previous {
        // Worth a line at info: crossing a watermark is the explanation for a
        // sudden change in hit rate or a burst of CAPACITY_FULL.
        info!(
            from = previous.as_str(),
            to = level.as_str(),
            utilisation = format!("{utilisation:.3}"),
            "capacity pressure changed"
        );
    }

    if level < Pressure::Hard {
        return Ok(0);
    }

    let stats = engine.evict(wtxn, limits.batch)?;
    if stats.reclaimed > 0 {
        debug!(
            evicted = stats.reclaimed,
            scanned = stats.scanned,
            utilisation = format!("{utilisation:.3}"),
            "evicted to reclaim space"
        );
    }
    Ok(stats.reclaimed)
}

fn apply(
    engine: &LmdbEngine,
    wtxn: &mut heed::RwTxn,
    op: &mut WriteOp,
    effects: &mut Vec<PostCommit>,
) -> Result<WriteOutcome> {
    match op {
        WriteOp::Set(prepared) => {
            let mut cas = Vec::with_capacity(prepared.len());
            for item in prepared.iter_mut() {
                cas.push(engine.apply_set(wtxn, item)?);
            }
            Ok(WriteOutcome::Cas(cas))
        }
        WriteOp::Delete(keys) => {
            let mut hits = Vec::with_capacity(keys.len());
            for key in keys.iter() {
                hits.push(engine.apply_delete(wtxn, key)?);
            }
            Ok(WriteOutcome::Deleted(hits))
        }
        WriteOp::ConditionalSet(prepared, mode) => Ok(WriteOutcome::Conditional(
            engine.apply_conditional_set(wtxn, prepared, *mode)?,
        )),
        WriteOp::Touch { key, ttl_secs } => Ok(WriteOutcome::Touched(
            engine.apply_touch(wtxn, key, *ttl_secs)?,
        )),
        WriteOp::Incr {
            key,
            delta,
            decrement,
        } => Ok(WriteOutcome::Counter(
            engine.apply_incr(wtxn, key, *delta, *decrement)?,
        )),
        WriteOp::CreateTags(names) => {
            for (name, start_generation) in names.iter() {
                let (id, generation) = engine.apply_create_tag(wtxn, name, *start_generation)?;
                effects.push(PostCommit::TagCreated {
                    name: name.clone(),
                    id,
                    generation,
                });
            }
            Ok(WriteOutcome::Done)
        }
        WriteOp::DeleteByTag(name) => match engine.apply_delete_by_tag(wtxn, name)? {
            Some((id, generation)) => {
                effects.push(PostCommit::TagGeneration { id, generation });
                Ok(WriteOutcome::Invalidated(Some(generation)))
            }
            None => Ok(WriteOutcome::Invalidated(None)),
        },
        WriteOp::MergeTag { name, generation } => {
            let merged = engine.apply_merge_tag(wtxn, name, *generation)?;
            match merged {
                TagMerge::Created { id, generation } => effects.push(PostCommit::TagCreated {
                    name: name.clone(),
                    id,
                    generation,
                }),
                TagMerge::Raised { id, generation } => {
                    effects.push(PostCommit::TagGeneration { id, generation })
                }
                // Already at or past it, so there is nothing to publish.
                TagMerge::Unchanged { .. } => {}
            }
            Ok(WriteOutcome::Merged(merged.generation()))
        }
        WriteOp::Flush => {
            let epoch = engine.apply_flush(wtxn)?;
            effects.push(PostCommit::Epoch(epoch));
            Ok(WriteOutcome::Flushed(epoch))
        }
        // The sync itself happens after the commit, since it is an environment
        // operation rather than a transactional one.
        WriteOp::Sync => Ok(WriteOutcome::Done),
    }
}

fn report_sweep(stats: SweepStats) {
    if stats.scanned == 0 {
        return;
    }
    debug!(
        scanned = stats.scanned,
        reclaimed = stats.reclaimed,
        stale = stats.stale,
        lag_ms = stats.lag_ms,
        budget_exhausted = stats.budget_exhausted,
        "swept expired records"
    );
}
