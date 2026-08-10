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
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
            map_size: 4 * 1024 * 1024 * 1024,
            max_readers: 128,
            durability: Durability::default(),
            max_value_len: cache_core::DEFAULT_MAX_VALUE_LEN,
            wipe_on_start: false,
        }
    }
}
