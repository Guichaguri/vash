use std::sync::Arc;

use cache_core::{Key, Set, Value};

use crate::config::StoreConfig;
use crate::engine::LmdbEngine;
use crate::error::Result;
use crate::writer::Writer;
use crate::{Store, StoreStats};

/// Composes the LMDB engine with the writer thread that owns its write
/// transaction.
///
/// Reads go straight to the engine — LMDB's MVCC lets any number of them run
/// concurrently with the writer and with each other, taking no locks. Writes go
/// through the queue so they can be batched into shared commits.
pub struct LmdbStore {
    engine: Arc<LmdbEngine>,
    writer: Writer,
}

impl LmdbStore {
    pub fn open(config: &StoreConfig) -> Result<Self> {
        let engine = Arc::new(LmdbEngine::open(config)?);
        let writer = Writer::spawn(Arc::clone(&engine), config.write);
        Ok(Self { engine, writer })
    }

    /// Stops the writer and releases the environment, blocking until LMDB has
    /// fully let go of it.
    pub fn close(mut self) {
        // Order matters: the writer must stop before the environment closes, or
        // it would commit into a closing env. Its queue is drained first, so
        // writes already accepted still land.
        self.writer.shutdown();

        match Arc::try_unwrap(self.engine) {
            Ok(engine) => engine.close(),
            Err(_) => tracing::warn!("engine still referenced; environment left open"),
        }
    }
}

impl Store for LmdbStore {
    fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        self.engine.get(key)
    }

    fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        self.engine.get_many(keys)
    }

    fn set(&self, set: &Set<'_>) -> Result<u64> {
        let prepared = self.engine.prepare_set(set)?;
        let mut cas = self.writer.set_many(vec![prepared])?;
        cas.pop()
            .ok_or_else(|| crate::StoreError::Corrupt("writer dropped a set result".into()))
    }

    fn set_many(&self, sets: &[Set<'_>]) -> Result<Vec<u64>> {
        // Encoding happens here, on the caller's thread, so the value copies
        // for a whole batch run in parallel with other connections instead of
        // serialising behind the single writer.
        let prepared = sets
            .iter()
            .map(|set| self.engine.prepare_set(set))
            .collect::<Result<Vec<_>>>()?;
        self.writer.set_many(prepared)
    }

    fn delete(&self, key: Key<'_>) -> Result<bool> {
        let hits = self.writer.delete_many(vec![key.as_bytes().into()])?;
        Ok(hits.first().copied().unwrap_or(false))
    }

    fn delete_many(&self, keys: &[Key<'_>]) -> Result<Vec<bool>> {
        let owned = keys.iter().map(|k| k.as_bytes().into()).collect();
        self.writer.delete_many(owned)
    }

    fn touch(&self, key: Key<'_>, ttl_secs: u32) -> Result<bool> {
        self.writer.touch(key, ttl_secs)
    }

    fn stats(&self) -> Result<StoreStats> {
        use std::sync::atomic::Ordering;

        let metrics = self.writer.metrics();
        Ok(StoreStats {
            commits: metrics.commits.load(Ordering::Relaxed),
            committed_ops: metrics.committed_ops.load(Ordering::Relaxed),
            sweeps: metrics.sweeps.load(Ordering::Relaxed),
            reclaimed: metrics.reclaimed.load(Ordering::Relaxed),
            sweep_lag_ms: metrics.sweep_lag_ms.load(Ordering::Relaxed),
            ..self.engine.stats()?
        })
    }

    fn sync(&self) -> Result<()> {
        // Routed through the writer so it lands after everything already queued,
        // rather than racing ahead of writes the caller believes are done.
        self.writer.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Store` requires `Send + Sync`; this must hold by construction rather
    /// than by an `unsafe impl` papering over a real aliasing problem.
    #[test]
    fn store_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LmdbStore>();
    }
}
