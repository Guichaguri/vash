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
            Self::Touch { .. } => 1,
            Self::Sync => 0,
        }
    }
}

pub(crate) enum WriteOutcome {
    Cas(Vec<u64>),
    Deleted(Vec<bool>),
    Touched(bool),
    Done,
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

    loop {
        // Block until there is work, but wake up on the sweep interval so the
        // sweeper runs during idle periods for free.
        match rx.recv_timeout(sweep_interval) {
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

        // Sweeping shares the batch's transaction, so reclamation costs no
        // extra commit. Under sustained write load the interval check is what
        // keeps it from starving.
        let due_for_sweep = last_sweep.elapsed() >= sweep_interval;
        if batch.is_empty() && !due_for_sweep {
            continue;
        }

        if let Err(e) = commit_batch(&engine, &mut batch, due_for_sweep, &config, &metrics) {
            error!(error = %e, "write batch failed");
        }
        if due_for_sweep {
            last_sweep = Instant::now();
        }
    }

    // The channel is closed and no more jobs can arrive; anything still queued
    // was sent before shutdown and deserves to be committed.
    if !batch.is_empty()
        && let Err(e) = commit_batch(&engine, &mut batch, false, &config, &metrics)
    {
        error!(error = %e, "final write batch failed");
    }
    info!("writer thread stopped");
}

fn commit_batch(
    engine: &LmdbEngine,
    batch: &mut Vec<WriteJob>,
    sweep: bool,
    config: &WriteConfig,
    metrics: &WriterMetrics,
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
    for job in batch.iter_mut() {
        outcomes.push(apply(engine, &mut wtxn, &mut job.op));
    }

    let sweep_stats = if sweep {
        match engine.sweep(&mut wtxn, config.sweep_batch) {
            Ok(stats) => Some(stats),
            Err(e) => {
                // A failed sweep must not lose the user writes sharing this
                // transaction; it simply retries next interval.
                warn!(error = %e, "sweep failed");
                None
            }
        }
    } else {
        None
    };

    let mut needs_sync = false;
    for job in batch.iter() {
        if matches!(job.op, WriteOp::Sync) {
            needs_sync = true;
        }
    }

    match wtxn.commit() {
        Ok(()) => {
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

fn apply(engine: &LmdbEngine, wtxn: &mut heed::RwTxn, op: &mut WriteOp) -> Result<WriteOutcome> {
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
