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
mod listing;
pub mod lmdb;
pub mod reclaim;
pub mod schema;
mod shard;
pub mod tags;
mod writer;

use vash_core::{Key, Listing, Set, Stored, Value};

pub use config::{Durability, EvictionConfig, StoreConfig, WriteConfig};
pub use engine::Pressure;
pub use error::{Result, StoreError};
pub use lmdb::LmdbStore;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StoreStats {
    /// Environments in the shard set.
    pub shards: u32,
    /// Highest capacity pressure across them.
    pub pressure: &'static str,
    /// Live records dropped to reclaim space, as opposed to expired or
    /// tag-invalidated ones. A non-zero and rising value means the store is
    /// too small for its working set.
    pub evicted: u64,
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

impl Default for StoreStats {
    fn default() -> Self {
        Self {
            shards: 0,
            pressure: "normal",
            evicted: 0,
            entries: 0,
            expiry_entries: 0,
            tag_index_entries: 0,
            tags: 0,
            pending_reclaims: 0,
            map_size: 0,
            used_bytes: 0,
            utilisation: 0.0,
            readers_in_use: 0,
            max_readers: 0,
            epoch: 0,
            commits: 0,
            committed_ops: 0,
            sweeps: 0,
            reclaimed: 0,
            sweep_lag_ms: 0,
            tag_reclaimed: 0,
        }
    }
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

    /// A live key's expiry deadline in unix milliseconds, **without copying its
    /// value**.
    ///
    /// `None` for a key that is not live, `Some(NEVER)` for one with no expiry.
    /// This is what the commands that never look at the value — `EXISTS`,
    /// `TYPE`, `TTL`, `PERSIST`, `EXPIRE`, and `KEEPTTL` on a write — use
    /// instead of [`Store::get`], which would copy a megabyte out of the map to
    /// read eight bytes of header and discard the rest.
    fn deadline(&self, key: Key<'_>) -> Result<Option<u64>>;

    /// [`Store::deadline`] over a batch, against one consistent snapshot.
    fn deadlines(&self, keys: &[Key<'_>]) -> Result<Vec<Option<u64>>>;

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
    /// Returns the tag's new generation, or `None` when the tag was never
    /// registered, so nothing could reference it. The generation is what a
    /// caller forwards to cluster peers: it is the whole content of an
    /// invalidation message.
    fn delete_by_tag(&self, tag: &[u8]) -> Result<Option<u64>>;

    /// Raises a tag's generation to at least `generation`, registering the name
    /// if this node has never seen it. Returns the resulting generation.
    ///
    /// The receiving half of cluster invalidation. **Merges by maximum**, so it
    /// is idempotent, order-independent and safe to retry — which is what lets
    /// peers forward invalidations without any acknowledgement protocol. See
    /// [`vash_core::cluster`].
    fn merge_tag_generation(&self, tag: &[u8], generation: u64) -> Result<u64>;

    /// One page of the keys that are currently live, in shard-major key order.
    ///
    /// Administrative and diagnostic: a linear scan, bounded by `max_scan`
    /// records examined so one request cannot hold a read transaction open
    /// indefinitely. Entries are filtered by the same liveness rule as
    /// [`Store::get`], so a listed key is one a read at that instant would hit,
    /// and **nothing is written** — a listing does not reclaim the dead records
    /// it walks past.
    ///
    /// Resumption is by cursor, never by offset: an offset re-walks what it
    /// skips, which makes paging a large keyspace quadratic. See
    /// `docs/opcodes.md`.
    fn list_keys(&self, request: &vash_core::ListRequest<'_>, max_scan: usize) -> Result<Listing>;

    /// One page of the tag registry, in lexicographic name order.
    ///
    /// Takes no budget because it needs none: the registry is in RAM, so this
    /// opens no transaction and takes no reader slot.
    fn list_tags(&self, request: &vash_core::ListRequest<'_>) -> Result<Listing>;

    /// Every registered tag name with the generation this node holds for it.
    ///
    /// The digest a node offers a peer during anti-entropy.
    fn tag_generations(&self) -> Result<Vec<vash_core::TagGeneration>>;

    /// Empties the cache, returning the new flush epoch.
    fn flush(&self) -> Result<u32>;

    fn stats(&self) -> Result<StoreStats>;

    /// Forces buffered data to stable storage. Called on shutdown and on the
    /// periodic flush in `relaxed` durability.
    fn sync(&self) -> Result<()>;
}
