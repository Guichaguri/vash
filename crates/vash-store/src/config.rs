use std::path::PathBuf;

/// Smallest map size allowed, per shard.
///
/// Below roughly 4 MiB, LMDB can reach a state where it reports `MDB_MAP_FULL`
/// permanently even after everything has been deleted: the free list needs
/// pages of its own to record freed pages, and on a map that small there are
/// none to spare. The store then refuses every write for good.
///
/// Measured on this workload (4 KiB values, sustained overfill): 2 and 3 MiB
/// wedge, 4 MiB limps, 6 MiB and up recover cleanly. The limit is set well
/// clear of that, and is still negligible for any real deployment — the map is
/// a lazy reservation, not an allocation.
pub const MIN_MAP_SIZE: usize = 16 * 1024 * 1024;

/// How aggressively LMDB is asked to get data onto stable storage.
///
/// A cache can trade durability for speed in a way a system of record cannot:
/// a lost write is a cache miss, and a cache miss is already a supported
/// outcome. See plan §9.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// `fsync` on every commit. Slowest, strongest.
    Durable,
    /// Skips only the meta-page sync. An OS crash loses at most the last few
    /// transactions and **cannot corrupt the database**.
    Relaxed,
    /// **Syncs on a timer rather than on every commit.**
    ///
    /// The per-commit `fsync` is what a write actually waits for: measured on a
    /// four-core container, `relaxed` spends 0.43 ms per record inside commit
    /// against `ephemeral`'s 0.033 ms, so **92% of it is the sync** — and the
    /// backlog that builds behind it is the writer queue wait that dominates a
    /// write's latency. See `docs/performance-proposals.md` §9.
    ///
    /// What it gives up is **durability only, and boundedly**: an OS crash loses
    /// writes newer than the last periodic sync, `write.sync_interval_ms`. What
    /// it keeps is integrity — the database is still consistent and still
    /// openable, unlike [`Self::Ephemeral`], which has to be wiped.
    ///
    /// **That guarantee has a condition**, and it is LMDB's rather than this
    /// crate's: `MDB_NOSYNC` preserves atomicity, consistency and isolation
    /// *"if the filesystem preserves write order and the `MDB_WRITEMAP` flag is
    /// not used"*. So this mode never sets `WRITE_MAP` — which costs nothing,
    /// since it measured as no gain — and an operator on a filesystem that
    /// reorders writes gets the `ephemeral` risk without the `ephemeral` label.
    #[default]
    Lazy,
    /// No syncing at all, and a writable map. Fastest. An OS crash or power loss
    /// can corrupt the file, which is handled by wiping it at startup and
    /// beginning empty.
    Ephemeral,
}

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub path: PathBuf,
    /// Address-space reservation for the memory map. Measured on Windows and
    /// Linux to be a lazy reservation, not a preallocation, so a generous value
    /// costs nothing until the data actually arrives.
    pub map_size: usize,
    pub max_readers: u32,
    pub durability: Durability,
    pub max_value_len: usize,
    /// Wipe any existing database on startup.
    pub wipe_on_start: bool,
    /// Read the whole data file at startup, so the map is resident before the
    /// first request rather than after it.
    ///
    /// Trades startup time — sequential-read bandwidth over the data file —
    /// for the page faults it removes from the read path. `src/prefault.rs`
    /// carries the detail, including why this is not `MAP_POPULATE`.
    pub prefault: bool,
    /// Pin the map in memory after warming it, so the kernel cannot reclaim it.
    ///
    /// Only meaningful with [`Self::prefault`], which is what puts the pages
    /// there in the first place. Warming makes the working set resident *now*;
    /// this is what keeps it resident, and it is the difference between an
    /// assertion the operator makes and one the server can check — see
    /// `server.store.resident_mode`, which is the setting operators actually
    /// reach for.
    ///
    /// Linux only, and best-effort even there: the result is reported by
    /// [`Store::map_locked`] rather than assumed, because a caller uses it to
    /// decide whether reads may run on a runtime worker.
    ///
    /// [`Store::map_locked`]: crate::Store::map_locked
    pub lock_map: bool,
    /// Granularity that expiry-index buckets are rounded up to. Coarser means
    /// fewer distinct index keys and less write amplification; it never delays
    /// a read from seeing the key as expired, because the read path checks the
    /// record's exact timestamp.
    pub bucket_granularity_ms: u64,
    /// Ceiling on registered tag names. The registry is held entirely in RAM,
    /// so without a limit a client inventing tag names is a memory leak.
    pub max_tags: usize,
    /// Ceiling on the tags a single record may carry. Bounded by
    /// [`ABSOLUTE_MAX_TAGS`], which the record header cannot exceed.
    ///
    /// Lowering it below what existing records carry is safe: it refuses new
    /// writes, and leaves what is already stored readable and touchable.
    ///
    /// [`ABSOLUTE_MAX_TAGS`]: vash_core::ABSOLUTE_MAX_TAGS
    pub max_tags_per_record: usize,
    /// Independent LMDB environments to run.
    ///
    /// LMDB permits one writer per environment, so this is the ceiling on
    /// concurrent writers. Fixed at creation: changing it would route every key
    /// somewhere else, and opening a database with a different count is refused.
    ///
    /// `map_size` applies to **each** shard, not to the total.
    pub shards: usize,
    pub write: WriteConfig,
}

/// Tuning for the writer thread and the sweeper that shares its transaction.
#[derive(Clone, Copy, Debug)]
pub struct WriteConfig {
    /// Most operations packed into one commit.
    pub max_batch: usize,
    /// Queued operations before writes are refused with `OVERLOADED`.
    pub queue_depth: usize,
    /// Artificial delay before sealing a batch. Zero — the default — means a
    /// batch is whatever queued during the previous commit, which adds no
    /// latency when idle and still batches under load.
    pub linger_us: u64,
    /// How often the sweeper runs. Also how long the writer waits for work
    /// before waking to sweep, so idle sweeps cost nothing.
    pub sweep_interval_ms: u64,
    /// How often the writer forces everything committed since the last one onto
    /// the device. `0` never does.
    ///
    /// This is what bounds the loss window of [`Durability::Lazy`], and it is
    /// also what finally makes [`Durability::Relaxed`]'s promise true: that mode
    /// has always been documented as "periodically forced" and nothing forced
    /// it — the only `force_sync` in the tree was at shutdown, so a killed
    /// process left the meta page wherever the OS had got to.
    pub sync_interval_ms: u64,
    /// Most expiry-index entries examined per sweep, bounding how long
    /// reclamation can hold the write transaction.
    pub sweep_batch: usize,
    /// Most tag-index entries examined per reclamation pass. While a job is
    /// outstanding these run back to back rather than once per interval, so
    /// this bounds transaction length, not drain rate.
    pub reclaim_batch: usize,
    pub eviction: EvictionConfig,
}

/// Capacity watermarks, as fractions of the LMDB map in use.
///
/// Each level does strictly more than the one below it. See plan §6.
#[derive(Clone, Copy, Debug)]
pub struct EvictionConfig {
    /// Reclamation stops waiting for its interval and runs continuously.
    pub soft: f64,
    /// Live records start being evicted, soonest-to-expire first, until usage
    /// falls back under `soft`.
    pub hard: f64,
    /// Writes are refused with `CAPACITY_FULL`. Reads and deletes still work.
    pub critical: f64,
    /// Records evicted per pass, bounding how long one pass holds the write
    /// transaction.
    pub batch: usize,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            soft: 0.75,
            hard: 0.88,
            critical: 0.96,
            batch: 512,
        }
    }
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            max_batch: 1024,
            queue_depth: 4096,
            linger_us: 0,
            sweep_interval_ms: 100,
            // Frequent enough that the loss window is a blink, rare enough that
            // the sync amortises across thousands of commits at any real write
            // rate.
            sync_interval_ms: 1000,
            sweep_batch: 512,
            reclaim_batch: 512,
            eviction: EvictionConfig::default(),
        }
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
            map_size: 4 * 1024 * 1024 * 1024,
            max_readers: 256,
            durability: Durability::default(),
            max_value_len: vash_core::DEFAULT_MAX_VALUE_LEN,
            wipe_on_start: false,
            prefault: false,
            lock_map: false,
            bucket_granularity_ms: 1000,
            max_tags: 100_000,
            max_tags_per_record: vash_core::DEFAULT_MAX_TAGS,
            shards: 1,
            write: WriteConfig::default(),
        }
    }
}
