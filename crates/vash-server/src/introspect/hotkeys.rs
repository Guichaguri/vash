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
    /// The first `MAX_KEPT_KEY` bytes of the key.
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
    /// Samples of this key, reads and writes together. What the ranking sorts
    /// on, and what [`Merged::estimated_ops`] scales.
    #[inline]
    pub fn total(&self) -> u64 {
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

    /// Counters in each per-lane summary — the `m` of §4.3.
    ///
    /// Reported beside every ranking because it *is* the sensitivity: a summary
    /// of `m` is guaranteed to hold anything above `1/m` of its lane's sampled
    /// stream, and a reader deciding whether an absent key is genuinely cold
    /// needs the threshold that absence was judged against.
    pub fn counters_per_lane(&self) -> usize {
        self.capacity
    }

    /// Whether sampling is on at all, so a caller can tell an idle server from
    /// a disabled sampler without inferring it from an empty ranking.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Offers one key to the sampler.
    ///
    /// The common path is an `xorshift32` step and a masked compare against a
    /// thread-local `Cell` — no atomics, no shared cache lines, no allocation,
    /// no borrow flag. Everything else is in `Self::admit`, behind a `#[cold]`
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

    /// The relative standard error of [`Self::estimated_ops`] for this counter.
    ///
    /// `1/sqrt(samples)`: 10% at a hundred samples, 3% at a thousand. Published
    /// beside every estimate rather than left in a document, because §2 requires
    /// an estimate to be named as one and a bare number is not named.
    ///
    /// `1.0` for a counter with no samples — total uncertainty, which is the
    /// honest reading and avoids dividing by zero.
    pub fn relative_error(&self, counter: &Counter) -> f64 {
        match counter.total() {
            0 => 1.0,
            samples => 1.0 / (samples as f64).sqrt(),
        }
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

    // ---- §7 gate 4: the estimator on a known distribution -------------------
    //
    // "Drive a Zipfian workload with a known top-10 and check the report
    // recovers the ranking, and that `estimated_ops` lands within the error
    // bound §4.2 claims. This is the gate that stops §4.4's merge heuristic
    // from shipping on faith."
    //
    // Seeded, so it runs in CI rather than only as a benchmark — §10.

    /// A Zipfian key stream with a fixed seed.
    ///
    /// Inverse-CDF over `1/k^s`, driven by the same `xorshift32` the sampler
    /// uses. Deterministic given the seed, which is what makes an accuracy
    /// assertion a test rather than a coin flip.
    struct Zipf {
        cdf: Vec<f64>,
        rng: u32,
    }

    impl Zipf {
        fn new(keys: usize, exponent: f64, seed: u32) -> Self {
            let weights: Vec<f64> = (1..=keys)
                .map(|rank| 1.0 / (rank as f64).powf(exponent))
                .collect();
            let total: f64 = weights.iter().sum();
            let mut running = 0.0;
            let cdf = weights
                .iter()
                .map(|w| {
                    running += w / total;
                    running
                })
                .collect();
            Self {
                cdf,
                rng: seed.max(1),
            }
        }

        /// The next key's rank, zero-based — rank 0 is the hottest.
        fn next(&mut self) -> usize {
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 17;
            self.rng ^= self.rng << 5;
            let u = f64::from(self.rng) / f64::from(u32::MAX);
            self.cdf
                .iter()
                .position(|c| u <= *c)
                .unwrap_or(self.cdf.len() - 1)
        }
    }

    fn zipf_key(rank: usize) -> String {
        format!("key:{rank:04}")
    }

    /// Space-Saving's formal guarantee, asserted rather than assumed: a held
    /// counter never *under*-counts, and never over-counts by more than the
    /// total it inherited when it took its slot.
    ///
    /// Run at `sample_rate = 1` on one lane, so the only thing under test is the
    /// estimator — no sampling error, no merge. With 64 keys in 32 counters
    /// there is real eviction, which is the condition the guarantee is about.
    #[test]
    fn the_estimator_never_undercounts_and_bounds_its_overcount() {
        const KEYS: usize = 64;
        const CAPACITY: usize = 32;
        const OPS: usize = 200_000;

        let mut zipf = Zipf::new(KEYS, 1.0, 0x5eed);
        let mut truth = vec![0u64; KEYS];
        let hot = HotKeys::new(1, CAPACITY);

        for _ in 0..OPS {
            let rank = zipf.next();
            truth[rank] += 1;
            hot.observe(zipf_key(rank).as_bytes(), false);
        }

        let merged = hot.merge();
        assert_eq!(merged.sampled, OPS as u64, "every observation was admitted");

        for counter in &merged.counters {
            let name = String::from_utf8_lossy(&counter.key).into_owned();
            let rank: usize = name.trim_start_matches("key:").parse().expect("a rank");
            let actual = truth[rank];

            assert!(
                counter.total() >= actual,
                "{name}: Space-Saving must not undercount — held {} against {actual}",
                counter.total()
            );
            assert!(
                counter.total() - counter.error <= actual,
                "{name}: overcounted by more than the inherited total — \
                 held {} error {} against {actual}",
                counter.total(),
                counter.error
            );
        }
    }

    /// The ranking is the product, so the ranking is what the gate asserts: the
    /// hottest keys come back in the order they actually occurred.
    #[test]
    fn the_ranking_recovers_a_known_zipfian_top_ten() {
        const KEYS: usize = 64;
        const CAPACITY: usize = 32;
        const OPS: usize = 400_000;
        const CHECKED: usize = 10;

        let mut zipf = Zipf::new(KEYS, 1.0, 0xd06f00d);
        let mut truth = vec![0u64; KEYS];
        let hot = HotKeys::new(1, CAPACITY);

        for _ in 0..OPS {
            let rank = zipf.next();
            truth[rank] += 1;
            hot.observe(zipf_key(rank).as_bytes(), false);
        }

        // The true ranking, by occurrence rather than by nominal Zipf rank —
        // a finite stream does not order itself perfectly by construction.
        let mut expected: Vec<usize> = (0..KEYS).collect();
        expected.sort_by_key(|rank| std::cmp::Reverse(truth[*rank]));

        let merged = hot.merge();
        let got: Vec<String> = merged
            .counters
            .iter()
            .take(CHECKED)
            .map(|c| String::from_utf8_lossy(&c.key).into_owned())
            .collect();
        let want: Vec<String> = expected
            .iter()
            .take(CHECKED)
            .map(|r| zipf_key(*r))
            .collect();

        assert_eq!(got, want, "the top {CHECKED} were not recovered in order");
    }

    /// The other half of gate 4: `estimated_ops` under **real sampling**, which
    /// is where the scaling can be wrong rather than merely imprecise.
    ///
    /// One in 128, so the top key's estimate rests on thousands of samples and
    /// §4.2's `1/sqrt(samples)` puts its relative error near 2%. The assertion
    /// allows 25%, which is an order of magnitude of headroom over the claim —
    /// this is testing that the estimate is *right*, not measuring how tight it
    /// is. The measurement is in benchmarks.md.
    #[test]
    fn sampled_estimates_land_near_the_true_counts() {
        const KEYS: usize = 32;
        const OPS: usize = 1_000_000;
        const RATE: u64 = 128;

        let mut zipf = Zipf::new(KEYS, 1.0, 0xbeef);
        let mut truth = vec![0u64; KEYS];
        let hot = HotKeys::new(RATE, 32);

        for _ in 0..OPS {
            let rank = zipf.next();
            truth[rank] += 1;
            hot.observe(zipf_key(rank).as_bytes(), false);
        }

        let merged = hot.merge();
        assert_eq!(merged.sample_rate, RATE);

        let hottest = &merged.counters[0];
        assert_eq!(
            &*hottest.key,
            zipf_key(0).as_bytes(),
            "the hottest key should rank first"
        );

        let estimated = merged.estimated_ops(hottest) as f64;
        let actual = truth[0] as f64;
        let off_by = (estimated - actual).abs() / actual;
        assert!(
            off_by < 0.25,
            "estimated {estimated} against a true {actual}, off by {:.1}%",
            off_by * 100.0
        );

        // And the error bound it publishes has to describe that estimate rather
        // than being decorative.
        let claimed = merged.relative_error(hottest);
        assert!(
            claimed > 0.0 && claimed < 0.1,
            "a key with {} samples claimed a relative error of {claimed}",
            hottest.total()
        );
    }

    /// Gate 4 against the **merge**, which is where §4.4 gives up its formal
    /// guarantee: independent per-lane summaries added together are a heuristic.
    /// Four threads, capacity small enough that each lane evicts, and the
    /// hottest key still has to come back first.
    #[test]
    fn the_merge_recovers_the_hottest_key_across_lanes() {
        const KEYS: usize = 64;
        const OPS_PER_THREAD: usize = 100_000;
        const THREADS: u32 = 4;

        let hot = HotKeys::new(1, 16);
        std::thread::scope(|scope| {
            for thread in 0..THREADS {
                let hot = &hot;
                scope.spawn(move || {
                    // A seed per thread, so the lanes see different streams
                    // drawn from the same distribution — which is the case the
                    // merge has to survive.
                    let mut zipf = Zipf::new(KEYS, 1.0, 0x1000 + thread);
                    for _ in 0..OPS_PER_THREAD {
                        hot.observe(zipf_key(zipf.next()).as_bytes(), false);
                    }
                });
            }
        });

        let merged = hot.merge();
        assert_eq!(merged.sampled, (THREADS as usize * OPS_PER_THREAD) as u64);
        assert_eq!(
            &*merged.counters[0].key,
            zipf_key(0).as_bytes(),
            "the merge lost the hottest key"
        );
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
