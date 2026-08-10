use std::sync::atomic::Ordering;

use cache_core::{Key, Set, Stored, Value};

use crate::config::StoreConfig;
use crate::engine::Pressure;
use crate::error::Result;
use crate::shard::Shards;
use crate::{Store, StoreStats};

/// The [`Store`] implementation: a set of independent LMDB environments, each
/// with its own writer thread, addressed by a hash of the key.
///
/// Reads go straight to the owning shard's engine — LMDB's MVCC lets any number
/// run concurrently with the writer and with each other, taking no locks.
/// Writes go through that shard's queue so they can be batched into shared
/// commits. Nothing is shared between shards, which is the point: N shards are
/// N concurrent writers.
pub struct LmdbStore {
    shards: Shards,
}

impl LmdbStore {
    pub fn open(config: &StoreConfig) -> Result<Self> {
        Ok(Self {
            shards: Shards::open(config)?,
        })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Highest capacity pressure across the shards.
    ///
    /// The maximum rather than an average: one full shard rejects writes for
    /// the keys that route to it, and reporting a comfortable mean would hide
    /// that.
    pub fn pressure(&self) -> Pressure {
        self.shards
            .all()
            .iter()
            .map(|shard| shard.engine.pressure())
            .max()
            .unwrap_or(Pressure::Normal)
    }

    /// Stops the writers and releases every environment, blocking until LMDB
    /// has fully let go.
    pub fn close(self) {
        self.shards.close();
    }

    /// Registers any tag names the owning shard has not seen.
    ///
    /// A record may never reference a tag that is not durably registered **in
    /// its own shard**: after a restart the id would either be unknown, a
    /// spurious miss, or reused for a different name, a spurious invalidation.
    /// Costs an extra writer round trip the first time a name is used in a
    /// given shard; afterwards resolution is a RAM lookup.
    fn ensure_tags_registered(&self, sets: &[Set<'_>]) -> Result<()> {
        for shard_items in self.shards.group(sets, |set| set.key.as_bytes()) {
            let mut missing: Vec<Box<[u8]>> = Vec::new();
            let mut shard = None;

            for (_, set) in &shard_items {
                if set.tags.is_empty() {
                    continue;
                }
                let owner = self.shards.for_key(set.key.as_bytes());
                shard = Some(owner);
                for name in owner.engine.tags().missing(&set.tags) {
                    if !missing.contains(&name) {
                        missing.push(name);
                    }
                }
            }

            if let Some(shard) = shard
                && !missing.is_empty()
            {
                shard.writer.create_tags(missing)?;
            }
        }
        Ok(())
    }
}

impl Store for LmdbStore {
    fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        self.shards.for_key(key.as_bytes()).engine.get(key)
    }

    fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        if self.shards.len() == 1 {
            return self.shards.all()[0].engine.get_many(keys);
        }

        // Each shard resolves its own keys against one snapshot; results are
        // written back into the caller's positions so the reply stays in
        // request order.
        let mut results: Vec<Option<Value>> = vec![None; keys.len()];
        for (index, group) in self
            .shards
            .group(keys, |key| key.as_bytes())
            .iter()
            .enumerate()
        {
            if group.is_empty() {
                continue;
            }
            let shard_keys: Vec<Key<'_>> = group.iter().map(|(_, key)| **key).collect();
            let values = self.shards.all()[index].engine.get_many(&shard_keys)?;
            for ((position, _), value) in group.iter().zip(values) {
                results[*position] = value;
            }
        }
        Ok(results)
    }

    fn set(&self, set: &Set<'_>) -> Result<u64> {
        let mut cas = self.set_many(std::slice::from_ref(set))?;
        cas.pop()
            .ok_or_else(|| crate::StoreError::Corrupt("writer dropped a set result".into()))
    }

    fn set_many(&self, sets: &[Set<'_>]) -> Result<Vec<u64>> {
        self.ensure_tags_registered(sets)?;

        let mut results = vec![0u64; sets.len()];
        for (index, group) in self
            .shards
            .group(sets, |set| set.key.as_bytes())
            .iter()
            .enumerate()
        {
            if group.is_empty() {
                continue;
            }
            let shard = &self.shards.all()[index];

            // Encoding happens here, on the caller's thread, so the value
            // copies run in parallel with other connections instead of
            // serialising behind the writer.
            let prepared = group
                .iter()
                .map(|(_, set)| {
                    let tags = shard.engine.tags().resolve(&set.tags).map_err(|name| {
                        crate::StoreError::Corrupt(format!(
                            "tag {name:?} vanished between registration and use"
                        ))
                    })?;
                    shard.engine.prepare_set(set, tags)
                })
                .collect::<Result<Vec<_>>>()?;

            // One transaction per shard. A batch spanning shards is therefore
            // atomic per shard, not overall — see the module note in `shard`.
            for ((position, _), cas) in group.iter().zip(shard.writer.set_many(prepared)?) {
                results[*position] = cas;
            }
        }
        Ok(results)
    }

    fn store(&self, set: &Set<'_>) -> Result<Stored> {
        self.ensure_tags_registered(std::slice::from_ref(set))?;
        let shard = self.shards.for_key(set.key.as_bytes());
        let tags = shard.engine.tags().resolve(&set.tags).map_err(|name| {
            crate::StoreError::Corrupt(format!(
                "tag {name:?} vanished between registration and use"
            ))
        })?;
        let prepared = shard.engine.prepare_set(set, tags)?;
        shard.writer.conditional_set(prepared, set.mode)
    }

    fn incr(&self, key: Key<'_>, delta: u64, decrement: bool) -> Result<Option<u64>> {
        self.shards
            .for_key(key.as_bytes())
            .writer
            .incr(key, delta, decrement)
    }

    fn get_and_touch(&self, keys: &[Key<'_>], ttl_secs: u32) -> Result<Vec<Option<Value>>> {
        // Read first so the reply reflects the values as they were found, then
        // re-stamp only the keys that were actually live.
        let values = self.get_many(keys)?;
        for (key, value) in keys.iter().zip(&values) {
            if value.is_some() {
                self.touch(*key, ttl_secs)?;
            }
        }
        Ok(values)
    }

    fn delete(&self, key: Key<'_>) -> Result<bool> {
        let hits = self
            .shards
            .for_key(key.as_bytes())
            .writer
            .delete_many(vec![key.as_bytes().into()])?;
        Ok(hits.first().copied().unwrap_or(false))
    }

    fn delete_many(&self, keys: &[Key<'_>]) -> Result<Vec<bool>> {
        let mut results = vec![false; keys.len()];
        for (index, group) in self
            .shards
            .group(keys, |key| key.as_bytes())
            .iter()
            .enumerate()
        {
            if group.is_empty() {
                continue;
            }
            let owned = group.iter().map(|(_, key)| key.as_bytes().into()).collect();
            let hits = self.shards.all()[index].writer.delete_many(owned)?;
            for ((position, _), hit) in group.iter().zip(hits) {
                results[*position] = hit;
            }
        }
        Ok(results)
    }

    fn touch(&self, key: Key<'_>, ttl_secs: u32) -> Result<bool> {
        self.shards
            .for_key(key.as_bytes())
            .writer
            .touch(key, ttl_secs)
    }

    fn delete_by_tag(&self, tag: &[u8]) -> Result<bool> {
        // Fans out rather than routing: a tag's keys are spread across every
        // shard by key hash, so an invalidation that reached only one would
        // leave the rest being served.
        let mut existed = false;
        for shard in self.shards.all() {
            if shard.writer.delete_by_tag(tag)?.is_some() {
                existed = true;
            }
        }
        Ok(existed)
    }

    fn flush(&self) -> Result<u32> {
        let mut epoch = 0;
        for shard in self.shards.all() {
            epoch = epoch.max(shard.writer.flush()?);
        }
        Ok(epoch)
    }

    fn stats(&self) -> Result<StoreStats> {
        let mut total = StoreStats {
            shards: self.shards.len() as u32,
            pressure: self.pressure().as_str(),
            ..StoreStats::default()
        };

        for shard in self.shards.all() {
            let engine = shard.engine.stats()?;
            let writer = shard.writer.metrics();

            total.entries += engine.entries;
            total.expiry_entries += engine.expiry_entries;
            total.tag_index_entries += engine.tag_index_entries;
            total.tags += engine.tags;
            total.pending_reclaims += engine.pending_reclaims;
            total.map_size += engine.map_size;
            total.used_bytes += engine.used_bytes;
            total.readers_in_use += engine.readers_in_use;
            total.max_readers += engine.max_readers;
            // A single epoch across shards: they advance together on flush.
            total.epoch = total.epoch.max(engine.epoch);

            total.commits += writer.commits.load(Ordering::Relaxed);
            total.committed_ops += writer.committed_ops.load(Ordering::Relaxed);
            total.sweeps += writer.sweeps.load(Ordering::Relaxed);
            total.reclaimed += writer.reclaimed.load(Ordering::Relaxed);
            total.tag_reclaimed += writer.tag_reclaimed.load(Ordering::Relaxed);
            total.evicted += writer.evicted.load(Ordering::Relaxed);
            // The worst shard, not the average: one lagging shard is the
            // problem, and a mean would hide it.
            total.sweep_lag_ms = total
                .sweep_lag_ms
                .max(writer.sweep_lag_ms.load(Ordering::Relaxed));
            total.utilisation = total.utilisation.max(engine.utilisation);
        }

        Ok(total)
    }

    fn sync(&self) -> Result<()> {
        for shard in self.shards.all() {
            // Routed through the writer so it lands after everything already
            // queued, rather than racing ahead of writes the caller believes
            // are done.
            shard.writer.sync()?;
        }
        Ok(())
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
