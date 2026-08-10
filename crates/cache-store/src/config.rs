use std::path::PathBuf;

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
    /// transactions and **cannot corrupt the database**. Periodically forced.
    #[default]
    Relaxed,
    /// No syncing at all. Fastest. An OS crash or power loss can corrupt the
    /// file, which is handled by wiping it at startup and beginning empty.
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
    /// Granularity that expiry-index buckets are rounded up to. Coarser means
    /// fewer distinct index keys and less write amplification; it never delays
    /// a read from seeing the key as expired, because the read path checks the
    /// record's exact timestamp.
    pub bucket_granularity_ms: u64,
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
    /// Most expiry-index entries examined per sweep, bounding how long
    /// reclamation can hold the write transaction.
    pub sweep_batch: usize,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            max_batch: 1024,
            queue_depth: 4096,
            linger_us: 0,
            sweep_interval_ms: 100,
            sweep_batch: 512,
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
            max_value_len: cache_core::DEFAULT_MAX_VALUE_LEN,
            wipe_on_start: false,
            bucket_granularity_ms: 1000,
            write: WriteConfig::default(),
        }
    }
}
