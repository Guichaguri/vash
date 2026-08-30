//! Which tags are being invalidated, and how often.
//!
//! Phase 5 of `docs/slowlog-proposal.md`, whose one line is *"hot tags, ranked
//! the same way"* and whose reason is that `DELETE_BY_TAG` on a hot tag drops a
//! whole working set at once — the most likely trigger for the stampede this
//! server has no defence against.
//!
//! # Counted, not sampled — the one place this departs from the hot keys
//!
//! [`crate::introspect::hotkeys`] samples one request in 128 because reads
//! arrive by the million and a hot key is by definition one a uniform sample
//! hits often. Neither half of that holds here.
//!
//! Invalidations are **rare** — that is the entire point of tag invalidation:
//! one command retires an arbitrary number of keys, so a server doing tens of
//! thousands of them a second is already pathological. And each one **matters
//! individually**: a single `DELETE_BY_TAG` on the wrong tag is the incident. A
//! stampede trigger that the sampler happened to skip is worse than one nobody
//! measured, because the report would name a different tag with confidence.
//!
//! So every invalidation is counted, and the numbers here are counts rather
//! than estimates. There is no `estimated_` prefix on anything in this file,
//! and that is the difference being marked.
//!
//! # What is *not* here, and why
//!
//! **The blast radius.** The obvious next column is how many keys each
//! invalidation dropped, and the store cannot answer it: invalidation bumps a
//! generation in constant time and the records are reclaimed lazily afterwards,
//! which is what makes it O(1) in the first place. Counting them would mean
//! walking the tag index on the invalidating request — turning the one
//! constant-time destructive operation this server has into a linear one, to
//! produce a number for a dashboard. Named here so it reads as a decision
//! rather than an oversight.
//!
//! **Tag attachment on writes** — which tags are written most. It is a weaker
//! signal, and mixing it in would break the arithmetic: an exact count and a
//! sampled one in the same ranking cannot both be scaled, and a column that is
//! exact for one row and estimated for another is the kind of number §2 exists
//! to keep out.

use std::sync::Mutex;

use crate::metrics::LANES;

/// How much of a tag name is kept.
///
/// Tags are short by construction — they name a group, not a record — so this
/// is generous rather than a real constraint. The digest beside it covers the
/// whole name, exactly as it does for keys.
const MAX_KEPT_TAG: usize = 128;

/// One tracked tag and what has been seen of it.
#[derive(Clone)]
pub struct Counter {
    /// `xxh3` of the **whole** tag name. Identity, and the only thing compared.
    pub digest: u64,
    pub name: Box<[u8]>,
    pub truncated: bool,
    /// `DELETE_BY_TAG` commands that named this tag **and found it
    /// registered**.
    ///
    /// A count, not an estimate. Invalidations of a tag the registry has never
    /// seen are not counted: they change nothing, and counting them would let
    /// a client fill this ranking with names that never existed.
    pub invalidations: u64,
    /// Space-Saving's inherited count: what the slot held when this tag evicted
    /// the previous occupant. The true count lies in
    /// `(invalidations - error, invalidations]`.
    pub error: u64,
}

/// One lane's Space-Saving summary over tag names.
///
/// The eviction rule is `hotkeys::Summary`'s, written out again rather than
/// shared. The two differ in what they count and in what those counts *mean* —
/// samples against events — and the alternative was either a generic over two
/// field names or one type whose fields are named for neither use. Twenty lines
/// of duplication is the cheaper of the three, and §2's "a name matching is not
/// a meaning matching" is the reason.
#[derive(Default)]
struct Summary {
    counters: Vec<Counter>,
    /// Every invalidation this lane has seen, which is what a share is a share
    /// of.
    total: u64,
}

impl Summary {
    fn observe(&mut self, capacity: usize, digest: u64, name: &[u8]) {
        self.total += 1;

        if let Some(counter) = self.counters.iter_mut().find(|c| c.digest == digest) {
            counter.invalidations += 1;
            return;
        }

        if self.counters.len() < capacity {
            self.counters.push(Counter {
                digest,
                name: truncate(name),
                truncated: name.len() > MAX_KEPT_TAG,
                invalidations: 1,
                error: 0,
            });
            return;
        }

        let victim = self
            .counters
            .iter_mut()
            .min_by_key(|c| c.invalidations)
            .expect("capacity is non-zero");
        let inherited = victim.invalidations;
        victim.digest = digest;
        victim.name = truncate(name);
        victim.truncated = name.len() > MAX_KEPT_TAG;
        victim.error = inherited;
        victim.invalidations = inherited + 1;
    }
}

fn truncate(name: &[u8]) -> Box<[u8]> {
    name[..name.len().min(MAX_KEPT_TAG)].into()
}

/// The tag invalidation ranking.
///
/// Striped over the same lane allocator the counters and the slowlog use, so a
/// thread that exits hands its lane back and its counts stay behind.
pub struct HotTags {
    enabled: bool,
    capacity: usize,
    summaries: Box<[Mutex<Summary>]>,
}

impl HotTags {
    pub fn new(enabled: bool, capacity: usize) -> Self {
        Self {
            enabled,
            capacity: capacity.max(1),
            summaries: (0..LANES).map(|_| Mutex::new(Summary::default())).collect(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn counters_per_lane(&self) -> usize {
        self.capacity
    }

    /// Records one invalidation that took effect.
    ///
    /// `#[cold]`: this runs on `DELETE_BY_TAG`, which crosses the writer queue
    /// and commits a transaction. Against that, a mutex and a scan of 64
    /// counters is not worth inlining anywhere.
    #[cold]
    pub fn invalidated(&self, tag: &[u8]) {
        if !self.enabled {
            return;
        }
        let digest = xxhash_rust::xxh3::xxh3_64(tag);
        if let Ok(mut summary) = self.summaries[crate::metrics::lane().index].lock() {
            summary.observe(self.capacity, digest, tag);
        }
    }

    /// Merges every lane into one ranking, most-invalidated first.
    ///
    /// The merge is the heuristic §4.4 describes and gate 4 measured for keys:
    /// per-lane summaries added together lose Space-Saving's formal bound. It
    /// bites far less here, because invalidations are rare enough that eviction
    /// is unlikely to happen at all — with 64 counters a lane, a deployment
    /// would need to invalidate more than 64 distinct tags before anything is
    /// evicted, and `error` on a row is what says whether it did.
    pub fn merge(&self) -> Merged {
        let mut counters: Vec<Counter> = Vec::new();
        let mut total = 0u64;

        for summary in &self.summaries {
            let Ok(summary) = summary.lock() else {
                continue;
            };
            total += summary.total;
            for counter in &summary.counters {
                match counters.iter_mut().find(|c| c.digest == counter.digest) {
                    Some(existing) => {
                        existing.invalidations += counter.invalidations;
                        existing.error += counter.error;
                    }
                    None => counters.push(counter.clone()),
                }
            }
        }

        counters.sort_unstable_by(|a, b| {
            b.invalidations
                .cmp(&a.invalidations)
                .then(a.digest.cmp(&b.digest))
        });
        Merged { counters, total }
    }
}

/// The merged ranking.
pub struct Merged {
    pub counters: Vec<Counter>,
    /// Every invalidation that took effect, across all tags — including any the
    /// ranking evicted. A row's share of this is what makes it hot.
    pub total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalidations_are_counted_exactly() {
        let tags = HotTags::new(true, 8);
        for _ in 0..5 {
            tags.invalidated(b"news");
        }
        tags.invalidated(b"authors");

        let merged = tags.merge();
        assert_eq!(
            merged.total, 6,
            "every invalidation is counted, none sampled"
        );
        assert_eq!(&*merged.counters[0].name, b"news");
        assert_eq!(merged.counters[0].invalidations, 5);
        assert_eq!(merged.counters[0].error, 0);
        assert_eq!(merged.counters[1].invalidations, 1);
    }

    #[test]
    fn a_disabled_tracker_counts_nothing() {
        let tags = HotTags::new(false, 8);
        for _ in 0..100 {
            tags.invalidated(b"news");
        }
        let merged = tags.merge();
        assert_eq!(merged.total, 0);
        assert!(merged.counters.is_empty());
    }

    /// The same Space-Saving rule the keys use: a new name takes the least-seen
    /// slot and inherits its count, so a genuinely hot tag climbs out rather
    /// than being evicted on arrival.
    #[test]
    fn a_new_tag_inherits_the_minimum_it_evicted() {
        let mut summary = Summary::default();
        summary.observe(2, 1, b"a");
        summary.observe(2, 1, b"a");
        summary.observe(2, 2, b"b");
        summary.observe(2, 3, b"c");

        let c = summary
            .counters
            .iter()
            .find(|counter| counter.digest == 3)
            .expect("c took a slot");
        assert_eq!(c.invalidations, 2);
        assert_eq!(c.error, 1, "the inherited count is reported as error");
    }

    #[test]
    fn a_summary_never_grows_past_its_capacity() {
        let mut summary = Summary::default();
        for i in 0..500u64 {
            summary.observe(4, i, format!("tag:{i}").as_bytes());
        }
        assert_eq!(summary.counters.len(), 4);
        assert_eq!(summary.total, 500);
    }

    /// Counts outlive the thread that made them and merge across lanes — the
    /// property striping by lane buys, and the one a thread-local would lose.
    #[test]
    fn counts_from_several_threads_merge_and_outlive_them() {
        let tags = HotTags::new(true, 16);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..25 {
                        tags.invalidated(b"shared");
                    }
                });
            }
        });

        let merged = tags.merge();
        assert_eq!(merged.total, 100);
        assert_eq!(merged.counters.len(), 1);
        assert_eq!(merged.counters[0].invalidations, 100);
    }

    #[test]
    fn a_long_tag_is_truncated_but_keeps_its_identity() {
        let long = vec![b't'; MAX_KEPT_TAG + 32];
        let mut other = long.clone();
        *other.last_mut().expect("non-empty") = b'x';

        let tags = HotTags::new(true, 8);
        tags.invalidated(&long);
        tags.invalidated(&other);

        let merged = tags.merge();
        assert_eq!(merged.counters.len(), 2, "a shared prefix is not identity");
        assert!(merged.counters.iter().all(|c| c.truncated));
    }
}
