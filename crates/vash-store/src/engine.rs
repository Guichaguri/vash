//! LMDB operations, with no threading of their own.
//!
//! Every write method takes the transaction it should act in, so the writer
//! thread can pack many of them into one commit (see [`crate::writer`]). Keeping
//! the transaction out of this layer is what makes group commit possible
//! without duplicating the operations.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use heed::types::Bytes as HeedBytes;
use heed::{AnyTls, Database, Env, RoTxn, WithTls};
use vash_core::Clock;

use crate::StoreStats;
use crate::error::{Result, StoreError};
use crate::tags::TagRegistry;

pub(crate) type Db = Database<HeedBytes, HeedBytes>;

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

/// One LMDB environment and everything derived from it.
///
/// The fields are `pub(crate)` rather than private because the operations that
/// act on them live in sibling modules — [`crate::env`], [`crate::read`],
/// [`crate::apply`], and the maintenance halves of [`crate::expiry`],
/// [`crate::reclaim`], [`crate::tags`] and [`crate::listing`]. Rust's privacy is
/// per-module, so co-locating each operation with the layout it walks costs this
/// widening. The boundary that matters is still the crate: nothing outside
/// `vash-store` can reach past the [`Store`] trait.
///
/// [`Store`]: crate::Store
pub struct LmdbEngine {
    pub(crate) env: Env<WithTls>,
    pub(crate) main: Db,
    pub(crate) exp: Db,
    pub(crate) tagidx: Db,
    pub(crate) tag_meta: Db,
    pub(crate) jobs: Db,
    pub(crate) meta: Db,
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

impl LmdbEngine {
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

    /// Bytes genuinely occupied, excluding pages on the free list.
    ///
    /// **Not** `last_page_number`. LMDB never returns freed pages to the OS nor
    /// lowers its high-water mark â€” a deleted record's pages go onto a free
    /// list for reuse â€” so a high-water measure only ever rises. Using it for
    /// the capacity watermarks meant pressure could never fall, and the evictor
    /// would keep going until the cache was empty.
    ///
    /// Summed from the sub-databases' own page counts, which is exactly the
    /// non-free total, and works inside the caller's transaction rather than
    /// needing one of its own.
    pub fn used_bytes_in(&self, txn: &RoTxn<'_, AnyTls>) -> Result<u64> {
        let page_size = self.env.stat().page_size as u64;
        let mut pages = 0u64;
        for db in [
            &self.main,
            &self.exp,
            &self.tagidx,
            &self.tag_meta,
            &self.jobs,
            &self.meta,
        ] {
            let stat = db.stat(txn).map_err(StoreError::from_heed)?;
            pages += stat.branch_pages as u64 + stat.leaf_pages as u64 + stat.overflow_pages as u64;
        }
        Ok(pages * page_size)
    }

    /// Fraction of the map in use, the input to the capacity watermarks.
    pub fn utilisation_in(&self, txn: &RoTxn<'_, AnyTls>) -> Result<f64> {
        Ok(self.used_bytes_in(txn)? as f64 / self.env.info().map_size as f64)
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
        let info = self.env.info();
        let stat = self.env.stat();
        let rtxn = self.read_txn()?;

        let entries = self
            .main
            .stat(&rtxn)
            .map_err(StoreError::from_heed)?
            .entries as u64;
        let expiry_entries = self.exp.stat(&rtxn).map_err(StoreError::from_heed)?.entries as u64;
        let tag_index_entries = self
            .tagidx
            .stat(&rtxn)
            .map_err(StoreError::from_heed)?
            .entries as u64;
        let pending_reclaims = self.pending_jobs(&rtxn)?;

        let used_bytes = self.used_bytes_in(&rtxn)?;
        let _ = stat;

        Ok(StoreStats {
            entries,
            expiry_entries,
            tag_index_entries,
            tags: self.tags.len() as u64,
            pending_reclaims,
            map_size: info.map_size as u64,
            used_bytes,
            utilisation: used_bytes as f64 / info.map_size as f64,
            readers_in_use: info.number_of_readers,
            max_readers: info.maximum_number_of_readers,
            epoch: self.epoch(),
            oldest_reader_age_ms: self.reader_ages.oldest_age_ms(self.now_ms()),
            // Owned by the writer thread, merged in by `LmdbStore::stats`.
            ..StoreStats::default()
        })
    }
}
