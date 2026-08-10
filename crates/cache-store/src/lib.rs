//! Storage adapter.
//!
//! Everything here sits behind the [`Store`] trait. That boundary is what keeps
//! the LMDB decision reversible: `libmdbx` (an LMDB fork with better write
//! behaviour and a growable map) is a contained swap if benchmarks demand it,
//! and it is what lets the server be tested against an in-memory fake.
//!
//! The trait is **synchronous on purpose**. LMDB reads can page-fault and block
//! the calling thread for the duration of a disk I/O, so callers must run these
//! on a thread that is allowed to block — never on an async runtime's worker.
//! See plan §9.

pub mod config;
pub mod error;
pub mod lmdb;
pub mod schema;

use cache_core::{Key, Set, Value};

pub use config::{Durability, StoreConfig};
pub use error::{Result, StoreError};
pub use lmdb::LmdbStore;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct StoreStats {
    pub entries: u64,
    pub map_size: u64,
    pub used_bytes: u64,
    /// `used_bytes / map_size`, the input to the eviction watermarks (plan §6).
    pub utilisation: f64,
    pub readers_in_use: u32,
    pub max_readers: u32,
    pub epoch: u32,
}

pub trait Store: Send + Sync + 'static {
    /// Returns the value if the key is present **and live**. Expired, flushed
    /// and tag-invalidated records read as absent without being rewritten.
    fn get(&self, key: Key<'_>) -> Result<Option<Value>>;

    /// Stores a value, returning its new CAS token.
    fn set(&self, set: &Set<'_>) -> Result<u64>;

    /// Removes a key. Returns whether it was live beforehand, so callers can
    /// distinguish a real delete from a miss.
    fn delete(&self, key: Key<'_>) -> Result<bool>;

    fn stats(&self) -> Result<StoreStats>;

    /// Forces buffered data to stable storage. Called on shutdown and on the
    /// periodic flush in `relaxed` durability.
    fn sync(&self) -> Result<()>;
}
