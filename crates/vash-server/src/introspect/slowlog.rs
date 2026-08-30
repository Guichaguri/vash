//! The ring buffer of requests that crossed the threshold.
//!
//! Design and rationale: `docs/slowlog-proposal.md` §3.
//!
//! Read on the admin port as `GET /slowlog` (`crate::admin`). The RESP
//! `SLOWLOG` command is phase 2.
//!
//! Two fields §3.2 lists are **not here**: the connection id and its peer. They
//! cost nothing on the request path — an entry is built only once a request has
//! already lost — but reaching them means threading the connection through
//! `dispatch::execute` and the three dialect executors above it, some two dozen
//! call sites on the hottest signatures in the program. That is a decision worth
//! taking on its own rather than as a rider on the batch rule, so it is left
//! open. Everything else in §3.2 is recorded.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::{CommandKind, Dialect, LANES};

/// How much of a key an entry keeps.
///
/// Long enough that a namespaced key is recognisable, short enough that a full
/// buffer is tens of kilobytes rather than the 64 KB a 511-byte key would make
/// of it. The digest beside it is of the whole key, so truncation costs
/// rendering and never identity.
const MAX_KEPT_KEY: usize = 128;

/// One request that took longer than the threshold.
#[derive(Debug, Clone)]
pub struct Entry {
    /// Strictly increasing, never reused, does not survive a restart.
    pub id: u64,
    /// Unix seconds, for correlating against an application's own logs.
    pub timestamp: u64,
    /// Time in the shard writer's queue, before this job's transaction opened.
    ///
    /// `0` for a read, and for a write on a server whose queue was empty. Zero
    /// is the measured truth in both cases rather than an absence — §3.1.
    pub queue_us: u64,
    /// Time in the store, with [`Self::queue_us`] taken out.
    ///
    /// The two are disjoint and sum to what the request cost inside the server,
    /// which is the quantity the threshold is compared against. Splitting them
    /// is the whole reason this is not Redis's slowlog: it separates "the store
    /// is slow" from "the store is fine and the queue is 900 deep".
    pub exec_us: u64,
    pub command: CommandKind,
    pub dialect: Dialect,
    /// How many keys the command named.
    pub keys: u32,
    /// The first [`MAX_KEPT_KEY`] bytes of the first key.
    ///
    /// Rendered or withheld by the surface, not here: §3.3 gates the *bytes*
    /// behind `listing_enabled` and falls back to [`Self::digest`]. Phase 0
    /// keeps both so phase 2 is a rendering change and not a recording one.
    pub key: Box<[u8]>,
    /// `xxh3` of the whole first key. What the surface shows when the bytes are
    /// withheld, and enough on its own to answer "is it the same key every
    /// time".
    pub digest: u64,
    /// Value bytes this command wrote. `0` for a read.
    ///
    /// What separates "slow because it was 4 MiB" from "slow because it queued",
    /// and the reason the entry does not carry the value itself — §3.2.
    pub value_bytes: u64,
    /// Writes sharing one commit that this entry accounts for; `1` for an
    /// ordinary command. The batch rule is phase 1 — §3.4.
    pub batch: u32,
}

/// What a call site hands over when a request crossed the threshold.
///
/// A struct rather than eight positional arguments, because the two durations
/// are both `u64` microseconds and transposing them would produce entries that
/// look plausible and blame the wrong half of the server.
pub struct Record<'a> {
    pub queue_us: u64,
    pub exec_us: u64,
    pub command: CommandKind,
    pub dialect: Dialect,
    pub keys: u32,
    pub key: &'a [u8],
    pub value_bytes: u64,
    pub batch: u32,
}

/// The rings, their threshold, and the two counters that describe them.
///
/// **One ring per thread, not one for the server**, and phase 0 is the reason.
/// §3.5 argued a single mutex was safe because "the lock is only ever taken by a
/// request that already lost". That holds at the shipping threshold, where the
/// branch is never taken. It does not hold in the case §7's gate 3 was written
/// to test: with `threshold_us = 1`, so that every request records, closed-loop
/// p99 rose 16% and p99.9 38%, and all four ABBA blocks moved the same
/// direction. An operator lowering the threshold to catch an incident is exactly
/// the person who cannot afford the tail to move, so the contended case is the
/// one the design has to survive.
///
/// # The rings outlive the threads
///
/// The first attempt kept each ring in a thread-local `Arc` and the log a `Weak`
/// to it, which loses every entry when the thread exits. That is not an edge
/// case here: **writes execute on the blocking pool**, and tokio retires an idle
/// blocking thread after ten seconds, so the slow writes an operator most wants
/// would be the ones most likely to vanish. Instead the log owns a fixed array
/// of rings and a thread borrows an index into it, using the same lane allocator
/// [`crate::metrics`] stripes its counters with — including the part that hands
/// a lane back when its thread exits, so a churning pool reuses lanes rather
/// than exhausting them.
pub struct Slowlog {
    /// Requests at or above this many microseconds are recorded. `0` disables.
    threshold_us: u64,
    /// Entries **per lane**. The ceiling is this times the lanes that have ever
    /// recorded, which is the peak number of concurrent threads doing so rather
    /// than [`LANES`] — a lane is reused, entries and all, by whichever thread
    /// claims it next.
    max_len: usize,
    /// Ids come from here, so they order entries across lanes. A relaxed
    /// increment, and only on a request that already crossed the threshold.
    next_id: AtomicU64,
    rings: Box<[Mutex<VecDeque<Entry>>]>,
    /// Entries admitted and entries pushed out to make room.
    ///
    /// Ordinary monotonic counters, and explicitly **not** cleared by a future
    /// `SLOWLOG RESET` — §3.6. `evicted` climbing is what tells an operator
    /// `max_len` is too small to hold the incident.
    pub admitted: AtomicU64,
    pub evicted: AtomicU64,
}

impl Slowlog {
    pub fn new(threshold_us: u64, max_len: usize) -> Self {
        Self {
            threshold_us,
            max_len: max_len.max(1),
            next_id: AtomicU64::new(1),
            rings: (0..LANES).map(|_| Mutex::new(VecDeque::new())).collect(),
            admitted: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
        }
    }

    pub fn threshold_us(&self) -> u64 {
        self.threshold_us
    }

    /// Whether a request of this duration is worth building an [`Entry`] for.
    ///
    /// **This is the whole hot-path cost of the slowlog.** One compare against a
    /// value both call sites have already computed for the histogram, predicted
    /// false on every request that is not already in trouble. Everything
    /// expensive — the allocation, the clock, the ring — is on the other side of
    /// it, in [`Self::record`], which is `#[cold]` for that reason.
    #[inline]
    pub fn crosses(&self, total_us: u64) -> bool {
        self.threshold_us != 0 && total_us >= self.threshold_us
    }

    /// Admits one entry to this thread's ring, evicting its oldest if full.
    #[cold]
    #[inline(never)]
    pub fn record(&self, record: Record<'_>) {
        let entry = Entry {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            queue_us: record.queue_us,
            exec_us: record.exec_us,
            command: record.command,
            dialect: record.dialect,
            keys: record.keys,
            key: record.key[..record.key.len().min(MAX_KEPT_KEY)].into(),
            digest: xxhash_rust::xxh3::xxh3_64(record.key),
            value_bytes: record.value_bytes,
            batch: record.batch,
        };

        let Ok(mut ring) = self.rings[crate::metrics::lane().index].lock() else {
            return;
        };
        while ring.len() >= self.max_len {
            ring.pop_front();
            self.evicted.fetch_add(1, Ordering::Relaxed);
        }
        ring.push_back(entry);
        self.admitted.fetch_add(1, Ordering::Relaxed);
    }

    /// The entries, newest first, as `SLOWLOG GET` renders them.
    ///
    /// Ordered by id, which is one server-wide sequence, so the merge is a true
    /// ordering across lanes rather than per-lane runs interleaved arbitrarily.
    pub fn entries(&self, count: usize) -> Vec<Entry> {
        let mut merged: Vec<Entry> = Vec::new();
        for ring in &self.rings {
            let Ok(ring) = ring.lock() else { continue };
            merged.extend(ring.iter().cloned());
        }
        merged.sort_unstable_by(|a, b| b.id.cmp(&a.id));
        merged.truncate(count);
        merged
    }

    /// What `SLOWLOG LEN` answers.
    pub fn len(&self) -> usize {
        self.rings
            .iter()
            .filter_map(|ring| ring.lock().ok().map(|ring| ring.len()))
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Empties every ring, leaving [`Self::admitted`] and [`Self::evicted`]
    /// alone.
    ///
    /// Legal where memcached's `stats reset` is not, and the difference is exact
    /// rather than convenient: a ring buffer is already lossy by design and no
    /// Prometheus family is derived from its contents, where `stats reset` would
    /// walk the very counters `/metrics` exports backwards. §3.6.
    pub fn reset(&self) {
        for ring in &self.rings {
            if let Ok(mut ring) = ring.lock() {
                ring.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &[u8]) -> Record<'_> {
        Record {
            queue_us: 1,
            exec_us: 2,
            command: CommandKind::Set,
            dialect: Dialect::Vcp,
            keys: 1,
            key,
            value_bytes: 64,
            batch: 1,
        }
    }

    /// An off-by-one here silently changes what an operator sees, so the
    /// boundary is asserted from both sides.
    #[test]
    fn the_threshold_is_exact_at_its_boundary() {
        let log = Slowlog::new(10_000, 8);
        assert!(!log.crosses(9_999));
        assert!(log.crosses(10_000));
        assert!(log.crosses(10_001));
    }

    #[test]
    fn a_zero_threshold_records_nothing() {
        let log = Slowlog::new(0, 8);
        assert!(!log.crosses(u64::MAX));
    }

    #[test]
    fn the_ring_evicts_oldest_and_counts_it() {
        let log = Slowlog::new(1, 4);
        for i in 0..5u8 {
            log.record(record(&[b'k', i]));
        }
        assert_eq!(log.len(), 4);
        assert_eq!(log.admitted.load(Ordering::Relaxed), 5);
        assert_eq!(log.evicted.load(Ordering::Relaxed), 1);

        let entries = log.entries(10);
        assert_eq!(entries.len(), 4);
        // Newest first, and the ids strictly decreasing under that ordering.
        assert_eq!(entries[0].id, 5);
        assert!(entries.windows(2).all(|w| w[0].id > w[1].id));
    }

    #[test]
    fn reset_clears_entries_and_preserves_counters() {
        let log = Slowlog::new(1, 4);
        for i in 0..6u8 {
            log.record(record(&[b'k', i]));
        }
        let admitted = log.admitted.load(Ordering::Relaxed);
        let evicted = log.evicted.load(Ordering::Relaxed);

        log.reset();

        assert_eq!(log.len(), 0);
        assert_eq!(
            log.admitted.load(Ordering::Relaxed),
            admitted,
            "RESET must not walk a Prometheus counter backwards"
        );
        assert_eq!(log.evicted.load(Ordering::Relaxed), evicted);
    }

    #[test]
    fn a_long_key_is_truncated_and_the_digest_covers_the_whole_of_it() {
        let long = vec![b'k'; MAX_KEPT_KEY + 32];
        let log = Slowlog::new(1, 4);
        log.record(record(&long));

        let entry = &log.entries(1)[0];
        assert_eq!(entry.key.len(), MAX_KEPT_KEY);
        assert_eq!(entry.digest, xxhash_rust::xxh3::xxh3_64(&long));
    }

    /// The rings are per thread, so the ordering the surface renders is only
    /// correct if the merge sorts on the shared id. Recording from several
    /// threads and asserting one strictly decreasing sequence is what catches a
    /// merge that concatenates rings instead.
    #[test]
    fn entries_from_several_threads_merge_into_one_ordering() {
        let log = std::sync::Arc::new(Slowlog::new(1, 64));
        std::thread::scope(|scope| {
            for thread in 0..4u8 {
                let log = std::sync::Arc::clone(&log);
                scope.spawn(move || {
                    for i in 0..8u8 {
                        log.record(record(&[b'k', thread, i]));
                    }
                });
            }
        });

        assert_eq!(log.admitted.load(Ordering::Relaxed), 32);
        assert_eq!(log.evicted.load(Ordering::Relaxed), 0);

        let entries = log.entries(100);
        assert_eq!(entries.len(), 32, "every ring is merged, not just one");
        assert!(
            entries.windows(2).all(|w| w[0].id > w[1].id),
            "the merge orders on the shared id rather than concatenating rings"
        );
    }

    /// **Entries outlive the thread that recorded them.** Writes run on the
    /// blocking pool and tokio retires an idle blocking thread after ten
    /// seconds, so a log that hung its rings off thread-locals would drop
    /// exactly the slow writes it exists to catch. The first implementation did
    /// precisely that, and this test is what caught it.
    #[test]
    fn an_entry_outlives_the_thread_that_recorded_it() {
        let log = Slowlog::new(1, 8);
        std::thread::scope(|scope| {
            scope.spawn(|| log.record(record(b"gone")));
        });
        assert_eq!(log.admitted.load(Ordering::Relaxed), 1);
        assert_eq!(log.len(), 1, "the thread exited; its entry did not");
        assert_eq!(&*log.entries(1)[0].key, b"gone");
    }
}
