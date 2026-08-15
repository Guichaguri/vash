//! Storage adapter.
//!
//! Everything here sits behind the [`Store`] trait. That boundary is what keeps
//! the LMDB decision reversible: `libmdbx` (an LMDB fork with better write
//! behaviour and a growable map) is a contained swap if benchmarks demand it,
//! and it is what lets the server be tested against an in-memory fake.
//!
//! Both claims are **checked** rather than asserted. The server is built against
//! `Arc<dyn Store>`, and [`memory::MemoryStore`] — behind the `testing` feature —
//! is a second implementation that `crates/vash-server/tests/store_seam.rs`
//! drives the whole stack over. A caller that reaches past the trait stops
//! compiling.
//!
//! The trait is **synchronous on purpose**. Reads can page-fault and writes
//! block on the writer queue, so callers must run these on a thread that is
//! allowed to block — never on an async runtime's worker. See plan §9.
//!
//! Internally the crate is three layers:
//!
//! - [`engine`] — the environment handle, with each operation in the module
//!   that owns the layout it walks: `env` opens it, `read` reads records,
//!   `apply` writes them, and the sweeper, reclaimer, tag writes and listing
//!   scan sit beside their own key encodings in [`expiry`], [`reclaim`],
//!   [`tags`] and `listing`.
//! - [`writer`] — the single writer thread, packing operations into shared
//!   commits and running the expiry sweeper in the same transaction.
//! - [`lmdb`] — the [`Store`] implementation composing the two.

mod apply;
pub mod config;
pub mod engine;
mod env;
pub mod error;
pub mod expiry;
mod listing;
pub mod lmdb;
/// An in-memory implementation, for tests. See [`memory::MemoryStore`].
#[cfg(feature = "testing")]
pub mod memory;
mod prefault;
mod queue;
mod read;
mod readers;
pub mod reclaim;
pub mod schema;
mod shard;
pub mod tags;
mod writer;

use vash_core::{BatchGuard, ExpireGuard, Key, Listing, Set, Value, ValueRef};

pub use apply::Written;
pub use config::{Durability, EvictionConfig, StoreConfig, WriteConfig};
pub use engine::Pressure;
pub use error::{Result, StoreError};
pub use lmdb::LmdbStore;

/// A [`Store::submit_set_many`] in flight.
///
/// Owns everything it needs, so the caller can let its `Set`s go out of scope
/// and await this afterwards. Awaiting holds no thread: the shard writers do the
/// work on their own threads and wake this when they are done.
pub struct PendingSetMany(Submitted);

enum Submitted {
    /// Nothing to wait for — an implementation that applied the writes inline.
    Ready(Vec<u64>),
    /// One entry per shard the batch touched: where its results belong in the
    /// caller's ordering, and the reply it is waiting on.
    Sharded {
        len: usize,
        shards: Vec<(Vec<usize>, writer::SetManyPending)>,
    },
}

impl PendingSetMany {
    pub(crate) fn ready(cas: Vec<u64>) -> Self {
        Self(Submitted::Ready(cas))
    }

    pub(crate) fn sharded(len: usize, shards: Vec<(Vec<usize>, writer::SetManyPending)>) -> Self {
        Self(Submitted::Sharded { len, shards })
    }

    /// Blocks until every shard has committed.
    ///
    /// **Off a runtime worker only** — this is the path the synchronous
    /// [`Store::set_many`] takes, and its callers are on the blocking pool.
    pub fn wait_blocking(self) -> Result<Vec<u64>> {
        match self.0 {
            Submitted::Ready(cas) => Ok(cas),
            Submitted::Sharded { len, shards } => {
                let mut results = vec![0u64; len];
                // A batch spanning shards is atomic per shard, not overall — see
                // the module note in `shard`. Failing here leaves the shards
                // that did commit committed, which was already true when this
                // was one sequential loop.
                for (positions, sent) in shards {
                    for (position, cas) in positions.into_iter().zip(sent.wait()?) {
                        results[position] = cas;
                    }
                }
                Ok(results)
            }
        }
    }

    /// Awaits every shard, holding no thread while they work.
    pub async fn wait(self) -> Result<Vec<u64>> {
        match self.0 {
            Submitted::Ready(cas) => Ok(cas),
            Submitted::Sharded { len, shards } => {
                let mut results = vec![0u64; len];
                for (positions, sent) in shards {
                    for (position, cas) in positions.into_iter().zip(sent.wait_async().await?) {
                        results[position] = cas;
                    }
                }
                Ok(results)
            }
        }
    }
}

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
    /// How long the oldest currently-open read transaction has been open.
    ///
    /// Plan §15's mitigation for the one High-impact storage risk: a read
    /// transaction that stays open pins a snapshot, which stops LMDB reusing
    /// freed pages and grows the file without bound. Sustained growth here is
    /// the alarm; zero means nothing is open.
    pub oldest_reader_age_ms: u64,
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

    /// Total time write jobs spent waiting in a shard queue, and total time
    /// spent applying and committing them, in microseconds.
    ///
    /// Plan §12's queue-wait/execution split. Against `committed_ops` and
    /// `commits` these give the mean of each stage: when the wait dominates, the
    /// shard writer is the bottleneck and more shards would help; when the
    /// commit dominates, the device is, and more shards would make it worse
    /// (plan §9).
    pub queue_wait_us: u64,
    pub commit_us: u64,
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
            oldest_reader_age_ms: 0,
            epoch: 0,
            commits: 0,
            committed_ops: 0,
            sweeps: 0,
            reclaimed: 0,
            sweep_lag_ms: 0,
            tag_reclaimed: 0,
            queue_wait_us: 0,
            commit_us: 0,
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
    /// Independent storage environments, and therefore concurrent writers.
    ///
    /// On the trait rather than on the implementation because it is advertised
    /// in the `HELLO` handshake, which means the server needs it without knowing
    /// what is underneath. An implementation with no sharding answers 1.
    fn shard_count(&self) -> usize;

    /// Returns the value if the key is present **and live**. Expired, flushed
    /// and tag-invalidated records read as absent without being rewritten.
    fn get(&self, key: Key<'_>) -> Result<Option<Value>>;

    /// Hands a live value to `render` **without copying it out of storage
    /// first**, returning whether the key was live.
    ///
    /// The whole point is that `render` runs while the read is still in
    /// progress — for LMDB, inside the read transaction, with `data` pointing
    /// straight at the memory map — so a `GET` goes from the map to the write
    /// buffer once instead of twice. Measured at `hot_path.rs`: ~200ns saved at
    /// 1 KiB and ~400ns at 4 KiB, and it also drops the per-read allocation
    /// that building a [`Value`] requires.
    ///
    /// **`render` must not block or call back into the store.** It runs with a
    /// read transaction open, and holding one open is the LMDB footgun this
    /// crate is otherwise careful to avoid (plan §9). Encoding bytes into a
    /// buffer is what it is for.
    ///
    /// Takes `&mut dyn FnMut` rather than a generic so the trait stays
    /// object-safe: the server holds an `Arc<dyn Store>`.
    fn get_with(&self, key: Key<'_>, render: &mut dyn FnMut(ValueRef<'_>)) -> Result<bool>;

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
    ///
    /// Returns the verdict and, when `set.return_previous` asked for it, the
    /// value the key held beforehand — captured inside the write transaction, so
    /// `SET … GET` reports what this write actually displaced rather than what a
    /// read a moment earlier happened to see.
    fn store(&self, set: &Set<'_>) -> Result<Written>;

    /// Changes a key's deadline under a guard, atomically.
    ///
    /// Redis's `EXPIRE`/`EXPIREAT`/`PERSIST`. The guard is evaluated against the
    /// deadline the record holds inside the transaction that then writes it, so
    /// a `GT`/`LT` comparison cannot be decided against a deadline that has
    /// since moved. Returns whether it applied. A deadline already in the past
    /// deletes the key, which is the event Redis specifies.
    fn expire(&self, key: Key<'_>, ttl: vash_core::TtlChange, guard: ExpireGuard) -> Result<bool>;

    /// Stores many values under a batch-wide guard.
    ///
    /// Redis's `MSETEX` with `NX`/`XX`. Returns whether the batch applied.
    ///
    /// **Atomic within a shard, and only within a shard.** The guard has to see
    /// every key at once, and a batch spanning shards is several transactions —
    /// plan §16's standing non-goal, not an oversight here. It therefore holds
    /// exactly when the keys land in one shard, which includes every
    /// single-shard deployment; across shards the test can be stale by the time
    /// the later shards commit. Documented in `docs/protocol.md`.
    fn set_many_if(&self, sets: &[Set<'_>], guard: BatchGuard) -> Result<bool>;

    /// Applies an atomic read-modify-write to a counter held as decimal text.
    ///
    /// **The read and the write are one step**, taken on the shard's single
    /// writer thread inside one transaction, so concurrent callers cannot lose
    /// an update. Composing this out of [`Store::get`] and [`Store::set`] in the
    /// caller would look equivalent and would not be — which is what the Redis
    /// adapter used to do.
    ///
    /// `None` when the key is absent and the operation does not create one;
    /// otherwise where the counter ended up and how far it moved. Every dialect's
    /// arithmetic is expressed through [`vash_core::Arithmetic`] rather than
    /// through a method each, because they differ in the numbers and in nothing
    /// else.
    fn arithmetic(&self, op: &vash_core::Arithmetic<'_>) -> Result<Option<vash_core::Applied>>;

    /// Concatenates onto a value atomically, creating it if absent, and returns
    /// its new length.
    ///
    /// Redis's `APPEND`. memcached's is a conditional write instead — it refuses
    /// an absent key — and goes through [`Store::store`] with `SetMode::Append`.
    fn append(&self, key: Key<'_>, suffix: &[u8]) -> Result<u64>;

    /// Fetches keys and re-stamps their TTL in one pass (memcached `gat`).
    fn get_and_touch(&self, keys: &[Key<'_>], ttl_secs: u32) -> Result<Vec<Option<Value>>>;

    /// Stores many values in one transaction: all of them apply, or none do.
    fn set_many(&self, sets: &[Set<'_>]) -> Result<Vec<u64>>;

    /// [`Store::set_many`] split in two: hand the work to the shard writers and
    /// return without waiting for it.
    ///
    /// **This is what lets a write be awaited rather than blocked on.** The
    /// synchronous form parks the calling thread for the whole queue wait, so a
    /// server serving it from a pool holds one OS thread per in-flight write —
    /// measured on a four-core container as most of what a write costs, see
    /// `docs/performance-proposals.md` §8. The returned handle owns everything
    /// it needs, so the caller may drop `sets` and `await` it.
    ///
    /// Preparation — validation, encoding, the value copy — still happens here,
    /// on the caller's thread, exactly as it did.
    fn submit_set_many(&self, sets: &[Set<'_>]) -> Result<PendingSetMany>;

    /// Whether **every** shard's map is pinned in memory, so no read can fault
    /// to disk.
    ///
    /// The question `store.resident_mode` asks before it takes reads off the
    /// storage pool and runs them on a runtime worker, and the reason it is a
    /// method rather than a configuration flag: locking is best-effort and
    /// platform-dependent, so what matters is what actually happened, not what
    /// was asked for. Every shard, because one unlocked shard is one that can
    /// stall a worker.
    ///
    /// Implementations that cannot fault — anything holding its data in memory
    /// already — answer `true`.
    fn map_locked(&self) -> bool;

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
