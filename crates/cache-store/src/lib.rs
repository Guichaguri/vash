//! Storage adapter.
//!
//! Everything here sits behind the [`Store`] trait. That boundary is what keeps
//! the LMDB decision reversible: `libmdbx` (an LMDB fork with better write
//! behaviour and a growable map) is a contained swap if benchmarks demand it,
//! and it is what lets the server be tested against an in-memory fake.
//!
//! The trait is **synchronous on purpose**. Reads can page-fault and writes
//! block on the writer queue, so callers must run these on a thread that is
//! allowed to block — never on an async runtime's worker. See plan §9.
//!
//! Internally the crate is three layers:
//!
//! - [`engine`] — LMDB operations, each taking the transaction to act in.
//! - [`writer`] — the single writer thread, packing operations into shared
//!   commits and running the expiry sweeper in the same transaction.
//! - [`lmdb`] — the [`Store`] implementation composing the two.

pub mod config;
pub mod engine;
pub mod error;
pub mod expiry;
pub mod lmdb;
pub mod reclaim;
pub mod schema;
pub mod tags;
mod writer;

use cache_core::{Key, Set, Stored, Value};

pub use config::{Durability, StoreConfig, WriteConfig};
pub use error::{Result, StoreError};
pub use lmdb::LmdbStore;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct StoreStats {
    pub entries: u64,
    /// Live entries in the expiry index. Compared against `entries`, this is
    /// how much of the keyspace carries a TTL — and, if it drifts far above,
    /// a sign the sweeper is falling behind.
    pub expiry_entries: u64,
    /// Live entries in the tag index, which the reclaimer walks.
    pub tag_index_entries: u64,
    /// Registered tag names. Bounded by `store.max_tags`.
    pub tags: u64,
    /// Tag invalidations whose space has not been fully reclaimed yet.
    /// Invalidation is instant; this is the background cleanup trailing it.
    pub pending_reclaims: u64,
    pub map_size: u64,
    pub used_bytes: u64,
    /// `used_bytes / map_size`, the input to the eviction watermarks (plan §6).
    pub utilisation: f64,
    pub readers_in_use: u32,
    pub max_readers: u32,
    pub epoch: u32,

    /// Write transactions committed.
    pub commits: u64,
    /// Operations those commits carried. Divided by `commits`, this is the
    /// average batch size — the number that says whether group commit is
    /// amortising the commit cost or paying it per operation.
    pub committed_ops: u64,
    /// Sweeper passes run, and records they reclaimed.
    pub sweeps: u64,
    pub reclaimed: u64,
    /// How far behind the oldest due expiry entry was on the last pass.
    /// Sustained growth means reclamation is losing to expiry.
    pub sweep_lag_ms: u64,
    /// Records freed by tag reclamation, as opposed to expiry sweeping.
    pub tag_reclaimed: u64,
}

impl StoreStats {
    /// Mean operations per commit, or 0 before anything has been committed.
    pub fn mean_batch_size(&self) -> f64 {
        if self.commits == 0 {
            0.0
        } else {
            self.committed_ops as f64 / self.commits as f64
        }
    }
}

/// What one pass of the expiry sweeper did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Index entries examined.
    pub scanned: usize,
    /// Records actually removed.
    pub reclaimed: usize,
    /// Entries that no longer described a live record — the key was overwritten
    /// or already deleted — and were simply dropped from the index.
    pub stale: usize,
    /// How far behind the oldest due entry is, in milliseconds. Sustained
    /// growth means reclamation is not keeping up with expiry.
    pub lag_ms: u64,
    /// The pass stopped on its budget rather than on catching up, so there is
    /// more to do next interval.
    pub budget_exhausted: bool,
}

pub trait Store: Send + Sync + 'static {
    /// Returns the value if the key is present **and live**. Expired, flushed
    /// and tag-invalidated records read as absent without being rewritten.
    fn get(&self, key: Key<'_>) -> Result<Option<Value>>;

    /// Resolves many keys against a single consistent snapshot.
    fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>>;

    /// Stores a value unconditionally, returning its new CAS token.
    ///
    /// Ignores `set.mode`; use [`Store::store`] for guarded writes.
    fn set(&self, set: &Set<'_>) -> Result<u64>;

    /// Applies a write under its [`SetMode`] guard.
    fn store(&self, set: &Set<'_>) -> Result<Stored>;

    /// Adds to or subtracts from a counter held as decimal text. `None` when
    /// the key is absent.
    fn incr(&self, key: Key<'_>, delta: u64, decrement: bool) -> Result<Option<u64>>;

    /// Fetches keys and re-stamps their TTL in one pass (memcached `gat`).
    fn get_and_touch(&self, keys: &[Key<'_>], ttl_secs: u32) -> Result<Vec<Option<Value>>>;

    /// Stores many values in one transaction: all of them apply, or none do.
    fn set_many(&self, sets: &[Set<'_>]) -> Result<Vec<u64>>;

    /// Removes a key. Returns whether it was live beforehand, so callers can
    /// distinguish a real delete from a miss.
    fn delete(&self, key: Key<'_>) -> Result<bool>;

    fn delete_many(&self, keys: &[Key<'_>]) -> Result<Vec<bool>>;

    /// Replaces a key's TTL without the client resending the value. Returns
    /// whether the key was live.
    fn touch(&self, key: Key<'_>, ttl_secs: u32) -> Result<bool>;

    /// Invalidates every record carrying `tag`, in **constant time** — one
    /// generation bump, regardless of how many keys are affected. They stop
    /// being served the moment this returns; freeing their space happens in the
    /// background.
    ///
    /// Returns `false` when the tag was never registered, so nothing could
    /// reference it.
    fn delete_by_tag(&self, tag: &[u8]) -> Result<bool>;

    /// Empties the cache, returning the new flush epoch.
    fn flush(&self) -> Result<u32>;

    fn stats(&self) -> Result<StoreStats>;

    /// Forces buffered data to stable storage. Called on shutdown and on the
    /// periodic flush in `relaxed` durability.
    fn sync(&self) -> Result<()>;
}
