//! How long the oldest open read transaction has been open.
//!
//! Plan §15 lists "long-lived read txn blocks page reuse ⇒ unbounded file
//! growth" as a **High**-impact risk and names `oldest_reader_age_ms` with an
//! alarm as its mitigation. This is that metric, finally. It is the oldest
//! outstanding promise in the plan, and `docs/opcodes.md` already noted that M8's
//! listing scans — by far the longest read transactions the server opens — gave
//! it a reason to exist.
//!
//! **LMDB cannot answer this.** Its reader table records which snapshot each
//! reader is pinned to (`pid`, `tid`, `txnid`), but never when the reader
//! started, so no amount of reading it back gives an age. The age has to be
//! recorded on the way in, which is what this does.
//!
//! # Why a slot per thread
//!
//! The obvious design — a registry keyed by transaction — needs a lock or a
//! search for a free slot on **every read**, and reads are the hot path this
//! whole server is shaped around. Instead each thread claims one slot, once, and
//! reuses it forever: opening a transaction is then a single relaxed store, and
//! closing one is another. Finding the oldest is a linear scan, and that happens
//! once per scrape rather than once per request.
//!
//! This is the same shape LMDB's own reader table uses, for the same reason. It
//! relies on the same property: a thread holds at most one read transaction at a
//! time, which is true here because every read opens a transaction, uses it and
//! drops it inside one call.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Slots available for threads that open read transactions.
///
/// Bounded by the threads that can reach a read: the runtime's workers plus its
/// blocking pool. Generous, because a slot is eight bytes and a thread that
/// cannot claim one is simply not measured — never refused.
const SLOTS: usize = 1024;

/// A slot value meaning "this thread has no read transaction open".
const FREE: u64 = 0;

thread_local! {
    /// This thread's slot, claimed on its first read and kept for its lifetime.
    static SLOT: Cell<Option<usize>> = const { Cell::new(None) };
}

#[derive(Debug)]
pub(crate) struct ReaderAges {
    /// Unix milliseconds at which each thread's current read began, or [`FREE`].
    starts: Box<[AtomicU64]>,
    /// Next slot to hand out. Only ever increases.
    next: AtomicUsize,
}

impl Default for ReaderAges {
    fn default() -> Self {
        Self {
            starts: (0..SLOTS).map(|_| AtomicU64::new(FREE)).collect(),
            next: AtomicUsize::new(0),
        }
    }
}

impl ReaderAges {
    /// Records that a read transaction is opening now.
    ///
    /// The guard clears the slot when dropped, so an early return or a panic
    /// cannot leave a phantom reader ageing forever — which would turn this
    /// metric into a permanent false alarm, the one failure mode that would get
    /// it ignored.
    pub fn open(&self, now_ms: u64) -> ReaderGuard<'_> {
        let slot = SLOT.with(|cell| match cell.get() {
            Some(slot) => Some(slot),
            None => {
                let slot = self.next.fetch_add(1, Ordering::Relaxed);
                // Past the end simply means unmeasured. Refusing the read
                // instead would let an observability concern take the server
                // down, which is exactly backwards.
                let slot = (slot < SLOTS).then_some(slot);
                cell.set(slot);
                slot
            }
        });

        if let Some(slot) = slot {
            // `max(1)` because zero is the sentinel for "no reader". A clock
            // that answered zero would silently stop measuring.
            self.starts[slot].store(now_ms.max(1), Ordering::Relaxed);
        }
        ReaderGuard { ages: self, slot }
    }

    /// How long the oldest currently-open read transaction has been open.
    ///
    /// Zero when nothing is open, which is the honest answer rather than a
    /// missing series: no reader means no reader to be old.
    pub fn oldest_age_ms(&self, now_ms: u64) -> u64 {
        self.starts
            .iter()
            .map(|start| start.load(Ordering::Relaxed))
            .filter(|start| *start != FREE)
            .min()
            .map_or(0, |oldest| now_ms.saturating_sub(oldest))
    }
}

/// Clears a reader's slot when its transaction goes.
pub(crate) struct ReaderGuard<'a> {
    ages: &'a ReaderAges,
    slot: Option<usize>,
}

impl Drop for ReaderGuard<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot {
            self.ages.starts[slot].store(FREE, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_open_is_zero_rather_than_a_missing_number() {
        let ages = ReaderAges::default();
        assert_eq!(ages.oldest_age_ms(1_000), 0);
    }

    #[test]
    fn an_open_reader_ages() {
        let ages = ReaderAges::default();
        let _guard = ages.open(1_000);
        assert_eq!(ages.oldest_age_ms(1_500), 500);
    }

    #[test]
    fn a_closed_reader_stops_ageing() {
        // The failure this guards against is a permanent false alarm, which is
        // how a metric gets ignored.
        let ages = ReaderAges::default();
        {
            let _guard = ages.open(1_000);
        }
        assert_eq!(ages.oldest_age_ms(9_999), 0);
    }

    #[test]
    fn the_oldest_wins() {
        // Threads get their own slots, so two readers need two threads.
        let ages = ReaderAges::default();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let ages = &ages;
            let held = scope.spawn(move || {
                let _guard = ages.open(1_000);
                tx.send(()).unwrap();
                // Held until the assertion below has run.
                std::thread::sleep(std::time::Duration::from_millis(200));
            });
            rx.recv().unwrap();

            let _recent = ages.open(1_400);
            assert_eq!(
                ages.oldest_age_ms(1_500),
                500,
                "the older reader is the one worth alarming on"
            );
            held.join().unwrap();
        });
    }
}
