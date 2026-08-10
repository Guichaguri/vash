//! Routing across independent LMDB environments.
//!
//! LMDB allows one writer per environment, so a single environment caps write
//! throughput at whatever one thread can commit. Running several — each with
//! its own environment, writer thread and maintenance — multiplies that ceiling
//! by the shard count. See plan §9.
//!
//! Every key belongs to exactly one shard, chosen by a hash of the key, so
//! single-key operations are unaffected. Multi-key operations are grouped by
//! shard, run against each in turn, and reassembled into request order.
//!
//! # What sharding costs
//!
//! **A batch spanning shards is no longer atomic.** Each shard commits its own
//! transaction, so a `SET_MANY` covering three shards is three commits, and a
//! failure in one leaves the others applied. Within a shard it is still all or
//! nothing. This is the one semantic the shard count changes, and it is
//! documented in `docs/protocol.md`.
//!
//! # Tags across shards
//!
//! Tag ids are assigned per shard, so the same name can hold different ids in
//! different shards. That is fine: a record lives in one shard and is only ever
//! checked against that shard's registry. What must hold is that an
//! invalidation reaches **every** shard, which is why `delete_by_tag` fans out
//! rather than routing.

use std::sync::Arc;

use crate::config::StoreConfig;
use crate::engine::LmdbEngine;
use crate::error::Result;
use crate::writer::Writer;

/// One environment and the writer thread that owns its write transaction.
pub(crate) struct Shard {
    pub engine: Arc<LmdbEngine>,
    pub writer: Writer,
}

impl Shard {
    fn open(config: &StoreConfig, index: usize) -> Result<Self> {
        let mut shard_config = config.clone();
        shard_config.path = shard_path(config, index);
        // Each environment reserves the whole map size, so the configured value
        // is per shard rather than shared. Splitting it instead would make the
        // shard count silently change how much a single hot shard can hold.
        let engine = Arc::new(LmdbEngine::open(&shard_config, index, config.shards)?);
        let writer = Writer::spawn(Arc::clone(&engine), config.write);
        Ok(Self { engine, writer })
    }
}

/// Where a shard's environment lives.
///
/// A single-shard store keeps using the configured path directly, so the
/// common case has no extra directory level and databases from before sharding
/// still open.
pub(crate) fn shard_path(config: &StoreConfig, index: usize) -> std::path::PathBuf {
    if config.shards <= 1 {
        config.path.clone()
    } else {
        config.path.join(format!("shard-{index}"))
    }
}

pub(crate) struct Shards {
    shards: Vec<Shard>,
}

impl Shards {
    pub fn open(config: &StoreConfig) -> Result<Self> {
        let count = config.shards.max(1);
        let mut shards = Vec::with_capacity(count);
        for index in 0..count {
            shards.push(Shard::open(config, index)?);
        }
        Ok(Self { shards })
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn all(&self) -> &[Shard] {
        &self.shards
    }

    /// The shard owning `key`.
    ///
    /// XXH3 rather than the default hasher because this mapping is persistent:
    /// a randomly-seeded hash would send a key to a different shard after every
    /// restart, turning the whole cache into a miss.
    #[inline]
    pub fn for_key(&self, key: &[u8]) -> &Shard {
        &self.shards[self.index_of(key)]
    }

    #[inline]
    pub fn index_of(&self, key: &[u8]) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        (xxhash_rust::xxh3::xxh3_64(key) % self.shards.len() as u64) as usize
    }

    /// Groups items by the shard owning their key, keeping each item's position
    /// in the request so results can be put back in order.
    ///
    /// Borrows rather than copies, so it works for values as large as a whole
    /// `Set` without duplicating them.
    pub fn group<'a, T>(
        &self,
        items: &'a [T],
        key_of: impl Fn(&T) -> &[u8],
    ) -> Vec<Vec<(usize, &'a T)>> {
        let mut grouped: Vec<Vec<(usize, &'a T)>> = vec![Vec::new(); self.shards.len()];
        for (position, item) in items.iter().enumerate() {
            grouped[self.index_of(key_of(item))].push((position, item));
        }
        grouped
    }

    /// Releases every environment, blocking until LMDB has let go of each.
    pub fn close(self) {
        for mut shard in self.shards {
            shard.writer.shutdown();
            match Arc::try_unwrap(shard.engine) {
                Ok(engine) => engine.close(),
                Err(_) => tracing::warn!("engine still referenced; environment left open"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    fn shards(n: usize) -> Vec<usize> {
        // Routing only needs the hash, so the distribution can be checked
        // without opening any environments.
        (0..1000)
            .map(|i| {
                let key = format!("key-{i}");
                (xxhash_rust::xxh3::xxh3_64(key.as_bytes()) % n as u64) as usize
            })
            .collect()
    }

    #[test]
    fn routing_is_stable_for_a_given_key() {
        // Persistent mapping: a per-process seed would relocate every key on
        // restart and turn the cache into a permanent miss.
        let once = xxhash_rust::xxh3::xxh3_64(b"stable");
        let twice = xxhash_rust::xxh3::xxh3_64(b"stable");
        assert_eq!(once, twice);
    }

    #[test]
    fn keys_spread_across_shards() {
        for count in [2usize, 4, 8, 16] {
            let assignments = shards(count);
            let mut used = vec![0usize; count];
            for shard in assignments {
                used[shard] += 1;
            }
            // Not a uniformity proof — just a guard against a mapping that is
            // badly broken, such as one that sends everything to one shard.
            // The bound is a third to triple the mean, which random assignment
            // will not breach for these sizes.
            let mean = 1000.0 / count as f64;
            assert!(
                used.iter()
                    .all(|n| (*n as f64) > mean / 3.0 && (*n as f64) < mean * 3.0),
                "keys are badly distributed across {count} shards (mean {mean:.0}): {used:?}"
            );
        }
    }
}
