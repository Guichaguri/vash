//! Opening, closing and transacting on one environment.
//!
//! What is left here after [`crate::backend`] took the engine out is the part
//! that was never about LMDB: the on-disk schema version, the shard-identity
//! check, the tag registry load and the CAS resumption. Plan §8 named this file
//! before any of it was written; it took until M10 to separate it from the
//! operations, and until M11 to separate it from the engine.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64};
use tracing::{info, warn};

use vash_core::Clock;

use crate::backend::{Backend, FULL, ReadTxn, WriteTxn};
use crate::config::StoreConfig;
use crate::engine::{Engine, Pressure};
use crate::error::{Result, StoreError};
use crate::schema::{SCHEMA_VERSION, db, meta_key};
use crate::tags::TagRegistry;

fn read_u32<B: Backend>(txn: &B::RwTxn<'_>, db: B::Db, key: &[u8]) -> Result<Option<u32>> {
    match txn.get(db, key)? {
        Some(raw) => raw
            .try_into()
            .map(u32::from_le_bytes)
            .map(Some)
            .map_err(|_| StoreError::Corrupt(format!("meta key {key:?} is not 4 bytes"))),
        None => Ok(None),
    }
}

fn read_u64<B: Backend>(txn: &B::RwTxn<'_>, db: B::Db, key: &[u8]) -> Result<Option<u64>> {
    match txn.get(db, key)? {
        Some(raw) => raw
            .try_into()
            .map(u64::from_le_bytes)
            .map(Some)
            .map_err(|_| StoreError::Corrupt(format!("meta key {key:?} is not 8 bytes"))),
        None => Ok(None),
    }
}

impl<B: Backend> Engine<B> {
    /// Opens one environment. `shard_index` and `shard_count` identify its
    /// place in the shard set and are validated against what the database
    /// already records.
    pub fn open(config: &StoreConfig, shard_index: usize, shard_count: usize) -> Result<Self> {
        if config.wipe_on_start && config.path.exists() {
            warn!(path = %config.path.display(), "wiping existing database on start");
            std::fs::remove_dir_all(&config.path)?;
        }
        if config.map_size < crate::config::MIN_MAP_SIZE {
            // Refused rather than allowed to wedge later: below this, LMDB can
            // report a full map permanently even after everything is deleted.
            return Err(StoreError::Corrupt(format!(
                "map size {} is below the minimum of {} bytes; LMDB cannot reliably \
                 reclaim space on a map that small",
                config.map_size,
                crate::config::MIN_MAP_SIZE
            )));
        }
        std::fs::create_dir_all(&config.path)?;

        let backend = B::open(config, &config.path)?;

        let mut wtxn = backend.write_txn()?;
        let main = backend.create_db(&mut wtxn, db::MAIN)?;
        let exp = backend.create_db(&mut wtxn, db::EXPIRY)?;
        let tagidx = backend.create_db(&mut wtxn, db::TAG_INDEX)?;
        let tag_meta = backend.create_db(&mut wtxn, db::TAGS)?;
        let jobs = backend.create_db(&mut wtxn, db::JOBS)?;
        let meta = backend.create_db(&mut wtxn, db::META)?;

        match read_u32::<B>(&wtxn, meta, meta_key::SCHEMA_VERSION)? {
            None => {
                wtxn.put(
                    meta,
                    meta_key::SCHEMA_VERSION,
                    &SCHEMA_VERSION.to_le_bytes(),
                )?;
                wtxn.put(
                    meta,
                    meta_key::RECORD_VERSION,
                    &(vash_core::RECORD_VERSION as u32).to_le_bytes(),
                )?;
                wtxn.put(
                    meta,
                    meta_key::SHARD_INDEX,
                    &(shard_index as u32).to_le_bytes(),
                )?;
                wtxn.put(
                    meta,
                    meta_key::SHARD_COUNT,
                    &(shard_count as u32).to_le_bytes(),
                )?;
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => {
                return Err(StoreError::Corrupt(format!(
                    "database has schema version {v}, this build expects {SCHEMA_VERSION}"
                )));
            }
        }

        // Reopening with a different shard count would route every key to a
        // different environment: the data would still be on disk, taking up
        // space, but every read would miss. Refuse rather than silently lose
        // the cache.
        if let Some(stored) = read_u32::<B>(&wtxn, meta, meta_key::SHARD_COUNT)?
            && stored as usize != shard_count
        {
            return Err(StoreError::Corrupt(format!(
                "database at {} was built for {stored} shard(s), but {shard_count} were configured; \
                 keys would route to the wrong environment",
                config.path.display()
            )));
        }
        if let Some(stored) = read_u32::<B>(&wtxn, meta, meta_key::SHARD_INDEX)?
            && stored as usize != shard_index
        {
            return Err(StoreError::Corrupt(format!(
                "database at {} is shard {stored}, opened as shard {shard_index}",
                config.path.display()
            )));
        }

        let epoch = read_u32::<B>(&wtxn, meta, meta_key::EPOCH)?.unwrap_or(0);
        // Resume past the last reserved block: anything below the persisted
        // watermark may already have been handed out before an unclean
        // shutdown, so the whole block is skipped rather than risk reuse.
        let cas_start = read_u64::<B>(&wtxn, meta, meta_key::CAS_WATERMARK)?.unwrap_or(0);

        // The whole tag table lives in RAM: it is small, and the read path
        // consults it on every tagged record.
        let registry = TagRegistry::new(config.max_tags);
        let mut loaded = Vec::new();
        for entry in wtxn.range(tag_meta, FULL)? {
            let (name, raw) = entry?;
            let Some((id, generation)) = crate::tags::decode_entry(raw) else {
                return Err(StoreError::Corrupt(format!(
                    "tag registry entry for {name:?} is {} bytes, expected {}",
                    raw.len(),
                    crate::tags::TAG_RECORD_LEN
                )));
            };
            loaded.push((name.to_vec().into_boxed_slice(), id, generation));
        }
        let tag_count = loaded.len();
        registry.load_from(loaded);

        wtxn.commit()?;

        // After the commit above, so the file is at its final length and
        // nothing warmed here is about to be rewritten. Best-effort by
        // contract — see [`Backend::warm`].
        let warmed = backend.warm(config);

        info!(
            path = %config.path.display(),
            durability = ?config.durability,
            map_size = config.map_size,
            epoch,
            cas_start,
            tag_count,
            prefaulted = warmed.bytes,
            map_locked = warmed.locked,
            "opened store"
        );

        Ok(Self {
            backend,
            main,
            exp,
            tagidx,
            tag_meta,
            jobs,
            meta,
            clock: Clock::new(),
            epoch: AtomicU32::new(epoch),
            cas_next: AtomicU64::new(cas_start),
            // Equal to `cas_next`, so the first write reserves a block before
            // handing anything out.
            cas_watermark: AtomicU64::new(cas_start),
            max_value_len: config.max_value_len,
            bucket_granularity_ms: config.bucket_granularity_ms,
            tags: registry,
            pressure: AtomicU8::new(Pressure::Normal as u8),
            shard_index,
            shard_count: shard_count.max(1),
            reader_ages: crate::readers::ReaderAges::default(),
            map_locked: warmed.locked,
        })
    }

    /// Whether this shard's map is pinned in memory, so a read cannot fault to
    /// disk. What `store.resident_mode` checks before it puts reads on a
    /// runtime worker; see [`crate::prefault::Warmed::locked`].
    pub fn map_locked(&self) -> bool {
        self.map_locked
    }

    pub fn write_txn(&self) -> Result<B::RwTxn<'_>> {
        self.backend.write_txn()
    }

    /// Opens a read transaction, recording how long it stays open.
    ///
    /// The recording is one relaxed store here and another when the returned
    /// guard drops — see [`crate::readers`] for why it is a slot per thread
    /// rather than a registry, and why the engine cannot answer the question
    /// itself.
    ///
    /// The instant it opened is kept on the transaction, because the reads that
    /// follow need the same number for their liveness check and were each
    /// reading the clock a second time to get it.
    pub fn read_txn(&self) -> Result<TrackedTxn<'_, B>> {
        let opened_at_ms = self.now_ms();
        let guard = self.reader_ages.open(opened_at_ms);
        let txn = self.backend.read_txn()?;
        Ok(TrackedTxn {
            txn,
            _guard: guard,
            opened_at_ms,
        })
    }

    pub fn sync(&self) -> Result<()> {
        self.backend.sync()
    }

    /// Releases the environment, blocking if the engine needs it.
    pub fn close(self) {
        self.backend.close();
    }
}

/// A read transaction whose age is being recorded.
///
/// Derefs to the transaction, so callers pass `&rtxn` exactly as before and the
/// tracking is invisible to them. Bundling the guard with the transaction rather
/// than leaving it to the caller is what guarantees the two have the same
/// lifetime: a guard dropped early would under-report, and one dropped late
/// would report a reader that had already gone.
pub struct TrackedTxn<'e, B: Backend> {
    txn: B::RoTxn<'e>,
    _guard: crate::readers::ReaderGuard<'e>,
    opened_at_ms: u64,
}

impl<B: Backend> TrackedTxn<'_, B> {
    /// Unix milliseconds at which this transaction opened.
    ///
    /// **This is the "now" a read judges expiry against**, rather than the
    /// clock as it stands when each record is examined. The two differ by the
    /// microseconds between opening the transaction and reaching the record,
    /// against deadlines stored to the millisecond — so the only records the
    /// distinction can reach are ones expiring inside that window, and they are
    /// served for a moment longer instead of a moment less.
    ///
    /// That is the direction this design already accepts everywhere else:
    /// expiry is lazy, and an expired record is served by nothing but is
    /// removed by the sweeper whenever it gets there. Reading the clock again
    /// per record bought no guarantee it did not already have — and it cost a
    /// `SystemTime::now` on the most executed path in the server.
    #[inline]
    pub fn opened_at_ms(&self) -> u64 {
        self.opened_at_ms
    }
}

impl<'e, B: Backend> std::ops::Deref for TrackedTxn<'e, B> {
    type Target = B::RoTxn<'e>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
