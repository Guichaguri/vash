//! Which keys are hot, estimated from a sample.
//!
//! Design and rationale: `docs/slowlog-proposal.md` §4. The short version, since
//! the reasons for each choice are the whole design:
//!
//! - **Sampled**, because exact counting is a lock and an unbounded map on the
//!   hottest path in the program (§4.1). A hot key is by definition one a
//!   uniform sample hits often, so sampling is not a compromise here — it is the
//!   property being measured.
//! - **Space-Saving**, not count-min, because the question is *which keys* and
//!   count-min cannot answer that without a companion structure holding the
//!   candidates (§4.3).
//! - **Per-thread summaries, merged on read** (§4.4), so the sampled update
//!   touches only cache lines its own thread owns.
//! - Every number this produces is an **estimate**, and §2 requires it be named
//!   as one. Nothing in here is called `ops`.

use std::sync::Mutex;

use crate::metrics::LANES;

/// How much of a key is kept in a summary slot.
///
/// `vash_core::MAX_KEY_LEN` is 511, and a summary of `m` slots per thread times
/// the thread count times 511 is most of a megabyte to answer a question the
/// first 128 bytes answer just as well. The full digest is stored beside the
/// truncated bytes, so two keys sharing a 128-byte prefix are still distinct
/// entries — the truncation costs rendering, never identity.
const MAX_KEPT_KEY: usize = 128;

/// One tracked key and what has been seen of it.
#[derive(Clone)]
pub struct Counter {
    /// `xxh3` of the **whole** key. Identity, and the only thing compared.
    pub digest: u64,
    /// The first [`MAX_KEPT_KEY`] bytes of the key.
    pub key: Box<[u8]>,
    /// Whether `key` is the whole of it.
    pub truncated: bool,
    /// Sampled reads and writes, kept apart because they are different problems
    /// with different fixes — §4.5. A hot read key wants a client-side cache; a
    /// hot written key is a serialisation point on one shard's single writer.
    pub read_samples: u64,
    pub write_samples: u64,
    /// Space-Saving's inherited count: what the slot held when this key evicted
    /// the previous occupant. The true count is somewhere in
    /// `(total - error, total]`, and reporting it is how the surface avoids
    /// claiming more precision than it has.
    pub error: u64,
}

impl Counter {
    #[inline]
    fn total(&self) -> u64 {
        self.read_samples + self.write_samples
    }
}

/// One thread's Space-Saving summary.
///
/// The counters are a flat array scanned linearly rather than the paper's
/// bucket-list Stream-Summary. That structure buys O(1) minimum-finding, which
/// matters at `m` in the thousands; here `m` is 64 and the scan is 64 `u64`
/// compares on one or two cache lines, reached once every `sample_rate`
/// requests. The simpler structure is the right one at this size, and saying so
/// is cheaper than a reader wondering whether the paper was misread.
#[derive(Default)]
pub struct Summary {
    /// Grown on demand rather than reserved: there is one of these per lane and
    /// only a handful of lanes ever admit a sample, so reserving `capacity`
    /// counters in each would be most of a megabyte of untouched memory.
    counters: Vec<Counter>,
    /// Every sample this summary has admitted, which is what turns a sample
    /// count back into an estimated operation count.
    pub sampled: u64,
}

impl Summary {
    /// Admits one sampled key, holding at most `capacity` counters.
    fn observe(&mut self, capacity: usize, digest: u64, key: &[u8], write: bool) {
        self.sampled += 1;

        if let Some(counter) = self.counters.iter_mut().find(|c| c.digest == digest) {
            if write {
                counter.write_samples += 1;
            } else {
                counter.read_samples += 1;
            }
            return;
        }

        if self.counters.len() < capacity {
            self.counters.push(Counter {
                digest,
                key: truncate(key),
                truncated: key.len() > MAX_KEPT_KEY,
                read_samples: u64::from(!write),
                write_samples: u64::from(write),
                error: 0,
            });
            return;
        }

        // Space-Saving proper: the new key takes the least-seen slot and
        // inherits its count, so a key that is genuinely hot climbs out of the
        // noise within a few samples instead of being evicted on arrival.
        let victim = self
            .counters
            .iter_mut()
            .min_by_key(|c| c.total())
            .expect("capacity is non-zero");
        let inherited = victim.total();
        victim.digest = digest;
        victim.key = truncate(key);
        victim.truncated = key.len() > MAX_KEPT_KEY;
        victim.error = inherited;
        victim.read_samples = inherited + u64::from(!write);
        victim.write_samples = u64::from(write);
    }
}

fn truncate(key: &[u8]) -> Box<[u8]> {
    key[..key.len().min(MAX_KEPT_KEY)].into()
}

/// The sampler and the estimator, as one thing a request touches.
///
/// The summaries are a fixed array indexed by the lane [`crate::metrics`] hands
/// each thread, not thread-local `Arc`s. Two reasons, both learned in phase 0
/// and both recorded on [`crate::introspect::slowlog::Slowlog`]: a summary hung
/// off a thread-local dies with its thread, and a bare thread-local is shared by
/// every instance in the process rather than being per-sampler.
pub struct HotKeys {
    /// Whether to sample at all. Checked before the PRNG so a disabled sampler
    /// costs one predictable branch and nothing else.
    enabled: bool,
    /// One in `sample_rate` requests is admitted. Always a power of two so the
    /// test is a mask rather than a division on the request path.
    mask: u32,
    sample_rate: u64,
    capacity: usize,
    summaries: Box<[Mutex<Summary>]>,
}

thread_local! {
    /// This thread's sampling PRNG.
    ///
    /// **Deliberately its own `Cell`**, and phase 0 measured why. Held together
    /// with the summary in one `RefCell<Option<..>>`, the borrow flag and the
    /// `Option` check were paid on every request rather than on every sample:
    /// `observed_get` cost 7.06 ns against the 3.92 ns it costs now. A `Cell`
    /// has no borrow flag, so the common path is a load, three shifts, a store
    /// and a compare.
    ///
    /// Shared across `HotKeys` instances, which is harmless: it carries entropy,
    /// not counts. The summaries are per-instance because they carry data.
    ///
    /// `0` means "not yet seeded" — xorshift is stuck at zero forever, so the
    /// value can never legitimately be zero and it doubles as the flag.
    static RNG: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

impl HotKeys {
    /// `sample_rate` is rounded up to a power of two so the sampling test is a
    /// mask. `0` disables sampling entirely.
    ///
    /// Disabled is its own flag rather than a mask of zero, and that is not
    /// fussiness: `rng & 0 == 0` is true for every request, so the sentinel that
    /// reads as "sample nothing" would have sampled *everything* — the most
    /// expensive possible reading of the cheapest possible setting.
    pub fn new(sample_rate: u64, capacity: usize) -> Self {
        let rounded = sample_rate.max(1).next_power_of_two();
        Self {
            enabled: sample_rate != 0,
            mask: (rounded - 1) as u32,
            sample_rate: rounded,
            capacity: capacity.max(1),
            summaries: (0..LANES).map(|_| Mutex::new(Summary::default())).collect(),
        }
    }

    pub fn sample_rate(&self) -> u64 {
        self.sample_rate
    }

    /// Offers one key to the sampler.
    ///
    /// The common path is an `xorshift32` step and a masked compare against a
    /// thread-local `Cell` — no atomics, no shared cache lines, no allocation,
    /// no borrow flag. Everything else is in [`Self::admit`], behind a `#[cold]`
    /// so it is not inlined into every call site on the hottest path in the
    /// program.
    #[inline]
    pub fn observe(&self, key: &[u8], write: bool) {
        if !self.enabled {
            return;
        }

        let admit = RNG.with(|cell| {
            let mut rng = cell.get();
            if rng == 0 {
                // Seeded per thread so two threads do not sample in lockstep.
                rng = seed();
            }
            rng ^= rng << 13;
            rng ^= rng >> 17;
            rng ^= rng << 5;
            cell.set(rng);
            rng & self.mask == 0
        });

        if admit {
            self.admit(key, write);
        }
    }

    /// Everything a sampled key costs: the digest, the summary lock, and the
    /// Space-Saving update.
    ///
    /// `#[cold]` and not inlined, because it runs once per `sample_rate`
    /// requests and inlining it would put its register pressure and its code
    /// size on the path of the other 127.
    #[cold]
    #[inline(never)]
    fn admit(&self, key: &[u8], write: bool) {
        let digest = xxhash_rust::xxh3::xxh3_64(key);
        if let Ok(mut summary) = self.summaries[crate::metrics::lane().index].lock() {
            summary.observe(self.capacity, digest, key, write);
        }
    }

    /// Merges every summary into one ranking, hottest first.
    ///
    /// **The merge is where the formal guarantee stops.** Space-Saving bounds
    /// its error over one stream; independent summaries added together are a
    /// heuristic, and a very good one, but the result is not provably the true
    /// top-K. §4.4 says so out loud, and it is why every count this returns is
    /// an estimate rather than a measurement.
    pub fn merge(&self) -> Merged {
        let mut counters: Vec<Counter> = Vec::new();
        let mut sampled = 0u64;

        for summary in &self.summaries {
            let Ok(summary) = summary.lock() else {
                continue;
            };
            sampled += summary.sampled;
            for counter in &summary.counters {
                match counters.iter_mut().find(|c| c.digest == counter.digest) {
                    Some(existing) => {
                        existing.read_samples += counter.read_samples;
                        existing.write_samples += counter.write_samples;
                        existing.error += counter.error;
                    }
                    None => counters.push(counter.clone()),
                }
            }
        }

        counters.sort_unstable_by(|a, b| b.total().cmp(&a.total()).then(a.digest.cmp(&b.digest)));
        Merged {
            counters,
            sampled,
            sample_rate: self.sample_rate,
        }
    }
}

/// The merged ranking, with the two numbers a reader needs to judge it.
pub struct Merged {
    pub counters: Vec<Counter>,
    /// Total samples behind this ranking.
    pub sampled: u64,
    /// What each sample stands for.
    pub sample_rate: u64,
}

impl Merged {
    /// Samples scaled back to operations. **An estimate**, named as one, with a
    /// relative standard error of roughly `1/sqrt(samples)` — §4.2.
    pub fn estimated_ops(&self, counter: &Counter) -> u64 {
        counter.total() * self.sample_rate
    }
}

/// A non-zero per-thread seed.
///
/// The address of a stack local is per-thread by construction and costs nothing
/// to obtain. Mixed, because consecutive thread stacks differ in their low bits
/// only, and xorshift takes several steps to spread a near-identical seed.
fn seed() -> u32 {
    let local = 0u8;
    let addr = std::ptr::addr_of!(local) as usize as u32;
    let mixed = (xxhash_rust::xxh3::xxh3_64(&addr.to_le_bytes()) & 0xffff_ffff) as u32;
    mixed.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Space-Saving's whole point: a key arriving into a full summary inherits
    /// the minimum rather than starting at one, so a genuinely hot key is not
    /// evicted on every arrival.
    #[test]
    fn a_new_key_inherits_the_minimum_it_evicted() {
        let mut summary = Summary::default();
        summary.observe(2, 1, b"a", false);
        summary.observe(2, 1, b"a", false);
        summary.observe(2, 2, b"b", false);

        // `b` holds 1 and is the minimum, so `c` takes its slot at 1 + 1.
        summary.observe(2, 3, b"c", false);
        let c = summary
            .counters
            .iter()
            .find(|counter| counter.digest == 3)
            .expect("c took a slot");
        assert_eq!(c.total(), 2);
        assert_eq!(c.error, 1, "the inherited count is reported as error");
    }

    #[test]
    fn a_summary_never_grows_past_its_capacity() {
        let mut summary = Summary::default();
        for i in 0..1_000u64 {
            summary.observe(4, i, format!("key:{i}").as_bytes(), false);
        }
        assert_eq!(summary.counters.len(), 4);
        assert_eq!(summary.sampled, 1_000);
    }

    #[test]
    fn reads_and_writes_are_counted_apart() {
        let mut summary = Summary::default();
        summary.observe(4, 1, b"a", false);
        summary.observe(4, 1, b"a", true);
        summary.observe(4, 1, b"a", true);
        let a = &summary.counters[0];
        assert_eq!(a.read_samples, 1);
        assert_eq!(a.write_samples, 2);
    }

    #[test]
    fn a_long_key_is_truncated_but_keeps_its_identity() {
        let long = vec![b'k'; MAX_KEPT_KEY + 64];
        let mut other = long.clone();
        *other.last_mut().expect("non-empty") = b'x';

        let mut summary = Summary::default();
        summary.observe(4, xxhash_rust::xxh3::xxh3_64(&long), &long, false);
        summary.observe(4, xxhash_rust::xxh3::xxh3_64(&other), &other, false);

        assert_eq!(summary.counters.len(), 2, "a shared prefix is not identity");
        assert!(summary.counters.iter().all(|c| c.truncated));
        assert!(summary.counters.iter().all(|c| c.key.len() == MAX_KEPT_KEY));
    }

    /// A rate of zero means *nothing*, and the mask it rounds to would mean
    /// *everything*. The two readings are as far apart as they could be, so this
    /// is asserted rather than left to the constructor's comment.
    #[test]
    fn a_rate_of_zero_samples_nothing() {
        let hot = HotKeys::new(0, 16);
        for i in 0..10_000u64 {
            hot.observe(format!("key:{i}").as_bytes(), false);
        }
        let merged = hot.merge();
        assert_eq!(merged.sampled, 0);
        assert!(merged.counters.is_empty());
    }

    /// The sampler must actually admit roughly one in `sample_rate`, or every
    /// estimate scaled by it is wrong.
    #[test]
    fn the_sample_rate_is_honoured_within_tolerance() {
        let hot = HotKeys::new(64, 16);
        for i in 0..64_000u64 {
            hot.observe(format!("key:{}", i % 8).as_bytes(), false);
        }
        let merged = hot.merge();
        let expected = 64_000 / 64;
        assert!(
            merged.sampled.abs_diff(expected) < expected / 2,
            "sampled {} against an expected {expected}",
            merged.sampled
        );
    }

    /// The ranking is the product, so it is what the test asserts: eight keys at
    /// known and very different rates must come back in that order.
    #[test]
    fn the_ranking_recovers_a_skewed_distribution() {
        let hot = HotKeys::new(8, 32);
        // Key 0 is sent 8 times as often as key 7, monotonically in between.
        for round in 0..20_000u64 {
            for key in 0..8u64 {
                if round % (key + 1) == 0 {
                    hot.observe(format!("key:{key}").as_bytes(), false);
                }
            }
        }

        let merged = hot.merge();
        let top = &merged.counters[0];
        assert_eq!(&*top.key, b"key:0", "the hottest key ranks first");
        assert!(merged.estimated_ops(top) > 0);
        assert!(merged.sampled > 0);
    }

    /// Samples outlive the thread that took them, and merge across threads. Both
    /// are properties of striping by lane rather than by thread-local ownership
    /// — the same correction `slowlog::Slowlog` records.
    #[test]
    fn samples_from_several_threads_merge_and_outlive_them() {
        let hot = HotKeys::new(1, 32);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..100 {
                        hot.observe(b"shared:key", false);
                    }
                });
            }
        });

        let merged = hot.merge();
        assert_eq!(merged.sampled, 400, "every thread's samples survived it");
        assert_eq!(merged.counters.len(), 1);
        assert_eq!(merged.counters[0].read_samples, 400);
    }

    /// Two samplers in one process must not feed each other. A bare thread-local
    /// summary is shared by every instance, which a server holding exactly one
    /// would never have revealed.
    #[test]
    fn two_samplers_do_not_share_a_summary() {
        let first = HotKeys::new(1, 8);
        let second = HotKeys::new(1, 8);

        first.observe(b"first:key", false);
        second.observe(b"second:key", false);

        assert_eq!(first.merge().sampled, 1);
        assert_eq!(second.merge().sampled, 1);
        assert_eq!(&*first.merge().counters[0].key, b"first:key");
        assert_eq!(&*second.merge().counters[0].key, b"second:key");
    }
}
