use std::sync::atomic::Ordering;

use vash_core::{Key, Listing, Set, Stored, Value};

use crate::config::StoreConfig;
use crate::engine::{LmdbEngine, Pressure};
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
    max_tags_per_record: usize,
}

impl LmdbStore {
    pub fn open(config: &StoreConfig) -> Result<Self> {
        Ok(Self {
            shards: Shards::open(config)?,
            max_tags_per_record: config.max_tags_per_record,
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

    /// Checks every set against the configured per-record tag limit.
    ///
    /// Runs before registration, not with the rest of write validation in
    /// `prepare_set`: a write that is going to be refused must not leave its
    /// tag names behind in the registry, where they would count against the
    /// name ceiling for the life of the process.
    fn check_tag_counts(&self, sets: &[Set<'_>]) -> Result<()> {
        for set in sets {
            vash_core::validate_tags(set.tags.len(), self.max_tags_per_record)?;
        }
        Ok(())
    }

    /// Registers any tag names the owning shard has not seen.
    ///
    /// A record may never reference a tag that is not durably registered **in
    /// its own shard**: after a restart the id would either be unknown, a
    /// spurious miss, or reused for a different name, a spurious invalidation.
    /// Costs an extra writer round trip the first time a name is used in a
    /// given shard; afterwards resolution is a RAM lookup.
    ///
    /// Each name is registered at the generation the rest of the node already
    /// holds for it rather than at zero — see [`LmdbStore::node_generation`].
    fn ensure_tags_registered(&self, sets: &[Set<'_>]) -> Result<()> {
        self.check_tag_counts(sets)?;

        for shard_items in self.shards.group(sets, |set| set.key.as_bytes()) {
            let mut missing: Vec<(Box<[u8]>, u64)> = Vec::new();
            let mut shard = None;

            for (_, set) in &shard_items {
                if set.tags.is_empty() {
                    continue;
                }
                let owner = self.shards.for_key(set.key.as_bytes());
                shard = Some(owner);
                for name in owner.engine.tags().missing(&set.tags) {
                    if !missing.iter().any(|(known, _)| *known == name) {
                        let generation = self.node_generation(&name).unwrap_or(0);
                        missing.push((name, generation));
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

    /// Runs a batch read per shard and reassembles the results in request
    /// order.
    ///
    /// Shared by every batch read: the grouping, the fast path for the
    /// single-shard case and the scatter-gather do not depend on what each key
    /// resolves to, only on which shard owns it.
    fn read_batch<T: Clone + Default>(
        &self,
        keys: &[Key<'_>],
        read: impl Fn(&LmdbEngine, &[Key<'_>]) -> Result<Vec<T>>,
    ) -> Result<Vec<T>> {
        if self.shards.len() == 1 {
            return read(&self.shards.all()[0].engine, keys);
        }

        let mut results = vec![T::default(); keys.len()];
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
            for ((position, _), value) in group
                .iter()
                .zip(read(&self.shards.all()[index].engine, &shard_keys)?)
            {
                results[*position] = value;
            }
        }
        Ok(results)
    }

    /// The generation this node holds for a tag name: the highest any of its
    /// shards has, or `None` if none has ever seen it.
    ///
    /// Generations are kept uniform across a node's shards — a shard that meets
    /// a name late adopts this value rather than starting at zero — because
    /// this single number is what the node reports to its peers, and it has to
    /// mean the same thing in every shard. Taking the maximum is the
    /// belt-and-braces reading: it is what the value would be even if the
    /// shards had somehow diverged.
    fn node_generation(&self, name: &[u8]) -> Option<u64> {
        self.shards
            .all()
            .iter()
            .filter_map(|shard| shard.engine.tags().generation_of(name))
            .max()
    }
}

impl Store for LmdbStore {
    fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        self.shards.for_key(key.as_bytes()).engine.get(key)
    }

    fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        self.read_batch(keys, LmdbEngine::get_many)
    }

    fn deadline(&self, key: Key<'_>) -> Result<Option<u64>> {
        self.shards.for_key(key.as_bytes()).engine.deadline(key)
    }

    fn deadlines(&self, keys: &[Key<'_>]) -> Result<Vec<Option<u64>>> {
        self.read_batch(keys, LmdbEngine::deadlines)
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
        let live: Vec<Key<'_>> = keys
            .iter()
            .zip(&values)
            .filter(|(_, value)| value.is_some())
            .map(|(key, _)| *key)
            .collect();
        if live.is_empty() {
            return Ok(values);
        }

        // One writer round trip per shard, not one per key: a `gat` over ten
        // keys used to queue ten operations and wait for ten commits.
        for (index, group) in self
            .shards
            .group(&live, |key| key.as_bytes())
            .iter()
            .enumerate()
        {
            if group.is_empty() {
                continue;
            }
            let owned = group.iter().map(|(_, key)| key.as_bytes().into()).collect();
            self.shards.all()[index]
                .writer
                .touch_many(owned, ttl_secs)?;
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

    fn delete_by_tag(&self, tag: &[u8]) -> Result<Option<u64>> {
        // Fans out rather than routing: a tag's keys are spread across every
        // shard by key hash, so an invalidation that reached only one would
        // leave the rest being served.
        let mut highest = None;
        for shard in self.shards.all() {
            if let Some(generation) = shard.writer.delete_by_tag(tag)? {
                highest = Some(highest.map_or(generation, |h: u64| h.max(generation)));
            }
        }

        // A shard left behind the others is levelled up, so the node holds one
        // generation for the name — the number it reports to its peers. Shards
        // only fall behind through a write registering the tag while an
        // invalidation is in flight, so this is normally a no-op and costs
        // nothing but the RAM check.
        //
        // Shards that do not hold the name at all are left alone: one that
        // registers it later adopts the node-wide value anyway (see
        // [`LmdbStore::node_generation`]), and creating it here would add an
        // entry to every shard's registry for every tag ever invalidated.
        if let Some(generation) = highest {
            for shard in self.shards.all() {
                if shard
                    .engine
                    .tags()
                    .generation_of(tag)
                    .is_some_and(|current| current < generation)
                {
                    shard.writer.merge_tag(tag, generation)?;
                }
            }
        }
        Ok(highest)
    }

    fn merge_tag_generation(&self, tag: &[u8], generation: u64) -> Result<u64> {
        // Every shard holding the name, because a record carrying the tag can
        // be in any of them and one left behind would keep serving records the
        // invalidation covered.
        //
        // Shards already at or past the offered generation are skipped from
        // RAM. That is the common case by far: gossip re-offers the same
        // generations every interval, so without this a converged cluster would
        // spend a write per shard per tag per round doing nothing.
        let mut effective = generation;
        let mut holders = 0;
        for shard in self.shards.all() {
            let Some(current) = shard.engine.tags().generation_of(tag) else {
                continue;
            };
            holders += 1;
            effective = effective.max(if current >= generation {
                current
            } else {
                shard.writer.merge_tag(tag, generation)?
            });
        }

        // No shard has the name, so nothing here can be carrying the tag — but
        // the generation still has to be recorded somewhere, or a later write
        // would register the tag at zero and be invalidated by the next gossip
        // round. One shard is enough, since the node's generation for a name is
        // the maximum across them.
        if holders == 0 {
            effective = effective.max(self.shards.for_key(tag).writer.merge_tag(tag, generation)?);
        }
        Ok(effective)
    }

    fn list_keys(&self, request: &vash_core::ListRequest<'_>, max_scan: usize) -> Result<Listing> {
        let (start_shard, after) = crate::listing::decode(request.cursor, self.shards.len())?;

        let limit = request.limit as usize;
        let mut listing = Listing::default();
        let mut after = after;

        // Shard-major, and each shard's own key order within that. Not a global
        // merge: that would mean one open cursor and one read transaction per
        // shard held simultaneously for the length of the request, which is
        // reader-slot pressure and complexity spent on prettiness in a
        // debugging command.
        for index in start_shard..self.shards.len() {
            // Both are non-zero here, and that is an invariant rather than
            // luck: the engine only ever returns without a `stopped_at` by
            // running off the end of its shard, so a filled limit or an
            // exhausted budget has already returned below. It matters because
            // a cursor names a key to resume *after*, and there is no way to
            // say "at the beginning of shard N" — nor any need to.
            let remaining = limit - listing.entries.len();
            let budget = max_scan.saturating_sub(listing.scanned as usize);

            let scan = self.shards.all()[index]
                .engine
                .list_keys(request, after, remaining, budget)?;

            listing.scanned += scan.scanned;
            listing.entries.extend(scan.entries);

            if let Some(stopped) = scan.stopped_at {
                listing.cursor = Some(crate::listing::encode(index, &stopped));
                listing.truncated = scan.budget_exhausted;
                return Ok(listing);
            }

            // Off the end of that shard: the next starts at its own beginning.
            after = None;
        }

        // Every shard walked to its end, so there is nothing to resume from and
        // the absent cursor is what tells the client it is done.
        Ok(listing)
    }

    fn list_tags(&self, request: &vash_core::ListRequest<'_>) -> Result<Listing> {
        // Entirely from RAM: the registry is loaded at boot and merged across
        // shards by maximum, which is the same number this node reports to its
        // peers. No transaction is opened and no reader slot is taken, which is
        // why this needs no scan budget and can never be `truncated`.
        let mut known = self.tag_generations()?;

        // Lexicographic, not registration order: ids are per shard and differ
        // between nodes, so registration order would make two nodes' listings
        // incomparable — and comparing them is how convergence is observed.
        known.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        // Resuming by name rather than by position is what makes a concurrently
        // registered tag harmless: it lands wherever it sorts and shifts
        // nothing, so every name present throughout the walk is returned once.
        let start = match request.cursor {
            [] => 0,
            cursor => known.partition_point(|entry| &*entry.name <= cursor),
        };

        let limit = request.limit as usize;
        let mut listing = Listing::default();
        // Consumed rather than borrowed so each name moves into its entry
        // instead of being copied out of a snapshot that is about to be dropped.
        for entry in known.into_iter().skip(start) {
            listing.scanned += 1;
            if !request.matches(&entry.name) {
                continue;
            }
            listing
                .entries
                .push(vash_core::ListEntry::new(entry.name, entry.generation));
            if listing.entries.len() >= limit {
                // Taken from the entry just pushed: the cursor is the last name
                // returned, so this is the one copy the page genuinely needs.
                listing.cursor = listing.entries.last().map(|last| last.name.clone());
                break;
            }
        }
        Ok(listing)
    }

    fn tag_generations(&self) -> Result<Vec<vash_core::TagGeneration>> {
        // Read from RAM, so a digest costs no transaction. Merged across shards
        // by maximum: they hold the same value for a name under normal running,
        // and the maximum is the safe reading if they ever do not.
        let mut merged: std::collections::HashMap<Box<[u8]>, u64> =
            std::collections::HashMap::new();
        for shard in self.shards.all() {
            for entry in shard.engine.tags().snapshot() {
                merged
                    .entry(entry.name)
                    .and_modify(|generation| *generation = (*generation).max(entry.generation))
                    .or_insert(entry.generation);
            }
        }
        Ok(merged
            .into_iter()
            .map(|(name, generation)| vash_core::TagGeneration { name, generation })
            .collect())
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
