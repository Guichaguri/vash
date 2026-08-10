//! The single writer thread, and the group commit that makes it fast.
//!
//! LMDB allows one writer per environment. Rather than fight that, every write
//! is funnelled to one thread that packs as many as it can into a single
//! transaction. The commit cost — the expensive part — is then amortised across
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

use cache_core::Key;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded};
use tracing::{debug, error, info, warn};

use crate::SweepStats;
use crate::config::WriteConfig;
use crate::engine::{LmdbEngine, PreparedSet};
use crate::error::{Result, StoreError};

pub(crate) enum WriteOp {
    Set(Vec<PreparedSet>),
    Delete(Vec<Box<[u8]>>),
    Touch { key: Box<[u8]>, ttl_secs: u32 },
    CreateTags(Vec<Box<[u8]>>),
    DeleteByTag(Box<[u8]>),
    Flush,
    Sync,
}

impl WriteOp {
    /// Individual key operations this job carries.
    ///
    /// Counted rather than treating one job as one unit, because a `SET_MANY`
    /// of 256 is 256 writes sharing a commit — which is exactly what the batch
    /// metric is supposed to show.
    fn item_count(&self) -> usize {
        match self {
            Self::Set(items) => items.len(),
            Self::Delete(keys) => keys.len(),
            Self::Touch { .. } | Self::DeleteByTag(_) | Self::Flush => 1,
            Self::CreateTags(names) => names.len(),
            Self::Sync => 0,
        }
    }
}

pub(crate) enum WriteOutcome {
    Cas(Vec<u64>),
    Deleted(Vec<bool>),
    Touched(bool),
    /// `None` when the tag was never registered, so nothing referenced it.
    Invalidated(Option<u64>),
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

    /// Registers tags durably. Must complete before any record referencing them
    /// is encoded.
    pub fn create_tags(&self, names: Vec<Box<[u8]>>) -> Result<()> {
        self.submit(WriteOp::CreateTags(names)).map(|_| ())
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

        // Take whatever else has arrived. This — and not a timer — is what
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
    for job in batch.iter_mut() {
        outcomes.push(apply(engine, &mut wtxn, &mut job.op, &mut effects));
    }

    let mut sweep_stats = None;
    let mut reclaim_stats = None;
    if maintenance {
        // Neither failure may lose the user writes sharing this transaction;
        // both simply retry next interval.
        match engine.sweep(&mut wtxn, config.sweep_batch) {
            Ok(stats) => sweep_stats = Some(stats),
            Err(e) => warn!(error = %e, "sweep failed"),
        }
        match engine.reclaim_step(&mut wtxn, config.reclaim_batch) {
            Ok(stats) => reclaim_stats = stats,
            Err(e) => warn!(error = %e, "tag reclamation failed"),
        }
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
        WriteOp::Touch { key, ttl_secs } => Ok(WriteOutcome::Touched(
            engine.apply_touch(wtxn, key, *ttl_secs)?,
        )),
        WriteOp::CreateTags(names) => {
            for name in names.iter() {
                let (id, generation) = engine.apply_create_tag(wtxn, name)?;
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
