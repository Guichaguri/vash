//! Storage operations, with no threading of their own.
//!
//! Every write method takes the transaction it should act in, so the writer
//! thread can pack many of them into one commit (see [`crate::writer`]). Keeping
//! the transaction out of this layer is what makes group commit possible
//! without duplicating the operations.
//!
//! Nothing here names an engine: the operations are expressed against
//! [`crate::backend`], which is what lets one set of them serve any backend.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use vash_core::Clock;

use crate::StoreStats;
use crate::backend::{Backend, LmdbBackend, ReadTxn};
use crate::error::Result;
use crate::tags::TagRegistry;

/// How close the store is to full.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Pressure {
    /// Business as usual.
    Normal = 0,
    /// Reclamation runs continuously instead of on its idle cadence.
    Soft = 1,
    /// Actively evicting live records to get back under the soft mark.
    Hard = 2,
    /// Writes are refused. Reads and deletes still work â€” a delete frees space,
    /// so refusing it would be self-defeating.
    Critical = 3,
}

impl Pressure {
    fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Normal,
            1 => Self::Soft,
            2 => Self::Hard,
            _ => Self::Critical,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Soft => "soft",
            Self::Hard => "hard",
            Self::Critical => "critical",
        }
    }
}

/// The store as it was before a second backend was conceivable.
///
/// Everything outside this crate — the benchmarks, the examples — names this,
/// and nothing outside this crate should have to care which engine is
/// underneath. `LmdbStore` in [`crate::lmdb`] is the same idea one layer up.
pub type LmdbEngine = Engine<LmdbBackend>;

/// One storage environment and everything derived from it.
///
/// The fields are `pub(crate)` rather than private because the operations that
/// act on them live in sibling modules — [`crate::env`], [`crate::read`],
/// [`crate::apply`], and the maintenance halves of [`crate::expiry`],
/// [`crate::reclaim`], [`crate::tags`] and [`crate::listing`]. Rust's privacy is
/// per-module, so co-locating each operation with the layout it walks costs this
/// widening. The boundary that matters is still the crate: nothing outside
/// `vash-store` can reach past the [`Store`] trait.
///
/// Generic over the engine, and the generic stops at `VashStore<B>`: the server
/// holds an `Arc<dyn Store>`, so nothing above this crate is parameterised. See
/// [`crate::backend`].
///
/// [`Store`]: crate::Store
pub struct Engine<B: Backend> {
    pub(crate) backend: B,
    pub(crate) main: B::Db,
    pub(crate) exp: B::Db,
    pub(crate) tagidx: B::Db,
    pub(crate) tag_meta: B::Db,
    pub(crate) jobs: B::Db,
    pub(crate) meta: B::Db,
    pub(crate) clock: Clock,
    pub(crate) epoch: AtomicU32,
    pub(crate) cas_next: AtomicU64,
    pub(crate) cas_watermark: AtomicU64,
    pub(crate) max_value_len: usize,
    pub(crate) bucket_granularity_ms: u64,
    pub(crate) tags: TagRegistry,
    pub(crate) pressure: AtomicU8,
    pub(crate) shard_index: usize,
    pub(crate) shard_count: usize,
    /// See [`crate::readers`].
    pub(crate) reader_ages: crate::readers::ReaderAges,
    /// Whether this shard's map was pinned in memory at open. Fixed for the
    /// life of the environment: nothing here unlocks it, and nothing relocks a
    /// map that has since grown past what was locked.
    pub(crate) map_locked: bool,
}

impl<B: Backend> Engine<B> {
    #[inline]
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    #[inline]
    pub fn epoch(&self) -> u32 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Publishes a bumped flush epoch. Applied only after its commit.
    pub fn set_epoch(&self, epoch: u32) {
        self.epoch.store(epoch, Ordering::Release);
    }

    #[inline]
    pub fn expiry_from_ttl(&self, ttl_secs: u32) -> u64 {
        self.clock.expiry_from_ttl(ttl_secs)
    }

    pub fn tags(&self) -> &TagRegistry {
        &self.tags
    }

    /// Every sub-database, for the operations that treat them as a set.
    ///
    /// One place to add the seventh, rather than six call sites to remember.
    pub(crate) fn all_dbs(&self) -> [B::Db; 6] {
        [
            self.main,
            self.exp,
            self.tagidx,
            self.tag_meta,
            self.jobs,
            self.meta,
        ]
    }

    /// Bytes genuinely occupied, excluding pages on the free list.
    ///
    /// **Not** the high-water mark. Neither engine returns freed pages to the OS
    /// nor lowers that mark â€” a deleted record's pages go onto a free list for
    /// reuse â€” so a high-water measure only ever rises. Using it for the
    /// capacity watermarks meant pressure could never fall, and the evictor
    /// would keep going until the cache was empty.
    ///
    /// Summed from the sub-databases' own page counts, which is exactly the
    /// non-free total, and works inside the caller's transaction rather than
    /// needing one of its own.
    pub fn used_bytes_in(&self, txn: &impl ReadTxn<B>) -> Result<u64> {
        self.used_bytes_at(txn, self.backend.info().page_size)
    }

    /// [`Self::used_bytes_in`] against an [`EnvInfo`] the caller already has.
    ///
    /// Exists so the callers that need both halves of `info()` take one
    /// snapshot rather than two — this runs once per commit, on the write path.
    fn used_bytes_at(&self, txn: &impl ReadTxn<B>, page_size: u64) -> Result<u64> {
        let mut pages = 0u64;
        for db in self.all_dbs() {
            pages += txn.db_stat(db)?.pages;
        }
        Ok(pages * page_size)
    }

    /// Fraction of the map in use, the input to the capacity watermarks.
    pub fn utilisation_in(&self, txn: &impl ReadTxn<B>) -> Result<f64> {
        let info = self.backend.info();
        Ok(self.used_bytes_at(txn, info.page_size)? as f64 / info.map_size as f64)
    }

    /// Current capacity pressure, as last measured by the writer's maintenance
    /// pass. Read on the write path to reject early, so a full store fails fast
    /// instead of queueing work it cannot commit.
    pub fn pressure(&self) -> Pressure {
        Pressure::from_u8(self.pressure.load(Ordering::Relaxed))
    }

    pub(crate) fn set_pressure(&self, pressure: Pressure) {
        self.pressure.store(pressure as u8, Ordering::Relaxed);
    }

    pub fn stats(&self) -> Result<StoreStats> {
        let info = self.backend.info();
        let rtxn = self.read_txn()?;

        let entries = rtxn.db_stat(self.main)?.entries;
        let expiry_entries = rtxn.db_stat(self.exp)?.entries;
        let tag_index_entries = rtxn.db_stat(self.tagidx)?.entries;
        let pending_reclaims = self.pending_jobs(&*rtxn)?;

        let used_bytes = self.used_bytes_at(&*rtxn, info.page_size)?;

        Ok(StoreStats {
            entries,
            expiry_entries,
            tag_index_entries,
            tags: self.tags.len() as u64,
            pending_reclaims,
            map_size: info.map_size,
            used_bytes,
            utilisation: used_bytes as f64 / info.map_size as f64,
            readers_in_use: info.readers_in_use,
            max_readers: info.max_readers,
            epoch: self.epoch(),
            oldest_reader_age_ms: self.reader_ages.oldest_age_ms(self.now_ms()),
            // Owned by the writer thread, merged in by `VashStore::stats`.
            ..StoreStats::default()
        })
    }
}
