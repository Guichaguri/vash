//! The token table behind Redis `SCAN`.
//!
//! Every other dialect pages a listing by echoing back the cursor the server
//! gave it, and `docs/opcodes.md` explains why that cursor is *data* — a
//! position to seek back to — rather than a handle onto a held read
//! transaction. Redis cannot do that: its cursor has to reach the client as a
//! `u64`, because the major client libraries parse it as one (`redis-py` calls
//! `int()` on it, `go-redis` types it `uint64`), and this store's position is
//! `shard ‖ key`, which does not fit in eight bytes.
//!
//! The two ways to avoid a table are both worse. Deriving a token from the key
//! makes keys that share a prefix either repeat forever or vanish, and an
//! offset re-walks everything it skips — the quadratic resumption M2 already met
//! in `tagidx` and `docs/opcodes.md` refused for `LIST_KEYS`. Worse, an offset
//! *skips keys* whenever a record behind the cursor is removed, which in a cache
//! is not an edge case but the steady state: it would break the one guarantee
//! `SCAN` exists to provide.
//!
//! So the client holds an integer and the server holds the bytes.
//!
//! **Server-wide, not per connection.** A pooled client returns its connection
//! between iterations, so page two routinely arrives on a different socket —
//! which is how `redis-py`'s `scan_iter` drives this, and how nearly every real
//! caller does.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The cursor a client sends to start an iteration, and gets back to end one.
///
/// Reserved, never issued, so "begin" and "resume" cannot collide.
pub const START: u64 = 0;

/// One position, and the token standing in for it.
#[derive(Debug)]
struct Entry {
    token: u64,
    cursor: Box<[u8]>,
    issued: Instant,
}

/// Live `SCAN` positions, keyed by the token handed to the client.
///
/// Kept in issue order, which — because tokens come from a monotonic counter —
/// is also **token order**. That is the whole reason this is a `VecDeque` and
/// not a `HashMap`: lookup binary-searches it, and eviction pops the front,
/// where a map would need a second structure to answer "which is oldest".
#[derive(Debug)]
pub struct ScanCursors {
    live: Mutex<VecDeque<Entry>>,
    next: AtomicU64,
    capacity: usize,
    ttl: Duration,
}

impl ScanCursors {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            live: Mutex::new(VecDeque::new()),
            // Tokens begin past `START`, which is Redis's "from the beginning"
            // and must never name a real position.
            next: AtomicU64::new(START + 1),
            capacity,
            ttl,
        }
    }

    /// Records a position and returns the token that names it.
    ///
    /// The only operation that can grow the table, and therefore the only one
    /// that sweeps it — so the table does work when somebody is scanning and
    /// none at all otherwise.
    pub fn issue(&self, cursor: &[u8]) -> u64 {
        let token = self.next.fetch_add(1, Ordering::Relaxed);
        let entry = Entry {
            token,
            cursor: cursor.into(),
            issued: Instant::now(),
        };

        let mut live = self.lock();
        live.push_back(entry);

        // Oldest first, on both counts. A live iteration's token is the one it
        // was handed most recently and is therefore at the back, so what leaves
        // here is spent tokens and abandoned iterations.
        while live.len() > self.capacity {
            live.pop_front();
        }
        while live
            .front()
            .is_some_and(|entry| entry.issued.elapsed() >= self.ttl)
        {
            live.pop_front();
        }

        token
    }

    /// The position a token names, or `None` if it has expired or never existed.
    ///
    /// **The TTL is enforced here, not only by the sweep in [`issue`].** If
    /// eviction were the only enforcement, how long a cursor stayed resumable
    /// would depend on how much other traffic the server happened to see — a
    /// client's iteration succeeding or failing according to other clients'
    /// behaviour, which is the kind of thing that only shows up in production.
    /// The sweep is a memory bound; this is the contract.
    ///
    /// Looking a token up does **not** consume it: a client whose request timed
    /// out will retry with the same one and must get the same page.
    ///
    /// [`issue`]: Self::issue
    pub fn resolve(&self, token: u64) -> Option<Box<[u8]>> {
        let live = self.lock();
        let index = live
            .binary_search_by_key(&token, |entry| entry.token)
            .ok()?;
        let entry = &live[index];
        (entry.issued.elapsed() < self.ttl).then(|| entry.cursor.clone())
    }

    /// Positions currently held. Test-only; nothing on a request path asks.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().len()
    }

    /// A poisoned lock is recovered from rather than propagated.
    ///
    /// Nothing here can panic while the lock is held — the entries are plain
    /// data and every operation on them is total — so a poisoned lock means a
    /// panic elsewhere in the process, and refusing every `SCAN` for the rest of
    /// the server's life would be a strange way to react to it.
    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Entry>> {
        self.live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> ScanCursors {
        ScanCursors::new(4, Duration::from_secs(60))
    }

    #[test]
    fn a_position_round_trips_through_its_token() {
        let cursors = table();
        let token = cursors.issue(b"\x00\x00session:42");
        assert_eq!(
            cursors.resolve(token).as_deref(),
            Some(&b"\x00\x00session:42"[..])
        );
    }

    #[test]
    fn no_token_is_ever_the_start_sentinel() {
        // `0` means "from the beginning" on the wire. A position issued under
        // that name would make a client restart forever without ever saying so.
        let cursors = table();
        for _ in 0..8 {
            assert_ne!(cursors.issue(b"x"), START);
        }
    }

    #[test]
    fn resolving_does_not_consume() {
        // A client whose request timed out retries with the same token.
        let cursors = table();
        let token = cursors.issue(b"x");
        assert!(cursors.resolve(token).is_some());
        assert!(cursors.resolve(token).is_some());
    }

    #[test]
    fn an_unknown_token_resolves_to_nothing() {
        let cursors = table();
        let token = cursors.issue(b"x");
        assert!(cursors.resolve(token + 1).is_none());
        assert!(cursors.resolve(START).is_none());
        assert!(cursors.resolve(u64::MAX).is_none());
    }

    #[test]
    fn capacity_drops_the_oldest_and_keeps_the_newest() {
        let cursors = table();
        let first = cursors.issue(b"1");
        for _ in 0..4 {
            cursors.issue(b"n");
        }

        assert_eq!(cursors.len(), 4, "the table is bounded");
        assert!(
            cursors.resolve(first).is_none(),
            "the oldest token is the one that goes"
        );
    }

    #[test]
    fn a_live_iterations_token_survives_other_traffic() {
        // The token a pager needs is always the newest it was handed, which is
        // at the back — so a concurrent scan evicts only spent tokens.
        let cursors = table();
        for _ in 0..16 {
            // Another client pages, filling the table several times over.
            cursors.issue(b"theirs");
            cursors.issue(b"theirs");
            cursors.issue(b"theirs");

            let mine = cursors.issue(b"mine");
            assert!(cursors.resolve(mine).is_some());
        }
    }

    #[test]
    fn an_expired_token_is_refused_even_with_no_sweep() {
        // The case that separates the contract from the memory bound: nothing
        // has been issued since, so no sweep has run, and the entry is still
        // sitting in the deque.
        let cursors = ScanCursors::new(4, Duration::from_millis(1));
        let token = cursors.issue(b"x");
        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(cursors.len(), 1, "nothing swept it");
        assert!(cursors.resolve(token).is_none(), "and it is still refused");
    }

    #[test]
    fn the_sweep_reclaims_expired_entries() {
        let cursors = ScanCursors::new(64, Duration::from_millis(1));
        for _ in 0..8 {
            cursors.issue(b"x");
        }
        std::thread::sleep(Duration::from_millis(5));

        cursors.issue(b"fresh");
        assert_eq!(cursors.len(), 1, "the sweep runs on issue");
    }
}
