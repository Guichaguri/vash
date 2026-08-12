//! Opening, closing and transacting on one LMDB environment.
//!
//! Everything that knows about `heed` flags, map sizing, sub-database creation
//! and the on-disk schema version lives here, so the operation modules beside it
//! see only an open environment and a transaction. Plan §8 named this file
//! before any of it was written; it took until M10 to actually separate it.

use heed::{Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn, WithTls};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64};
use tracing::{info, warn};

use vash_core::Clock;

use crate::config::{Durability, StoreConfig};
use crate::engine::{Db, LmdbEngine, Pressure};
use crate::error::{Result, StoreError};
use crate::schema::{MAX_DBS, SCHEMA_VERSION, db, meta_key};
use crate::tags::TagRegistry;

fn create_db(env: &Env<WithTls>, wtxn: &mut RwTxn, name: &str) -> Result<Db> {
    env.create_database(wtxn, Some(name))
        .map_err(StoreError::from_heed)
}

fn env_flags(durability: Durability) -> EnvFlags {
    let mut flags = EnvFlags::empty();
    match durability {
        Durability::Durable => {}
        Durability::Relaxed => flags |= EnvFlags::NO_META_SYNC,
        Durability::Ephemeral => {
            flags |= EnvFlags::NO_SYNC;
            // WRITE_MAP would add a further gain but fails at env-open on
            // Windows with OS error 6 at every map size tested. Unix only.
            #[cfg(unix)]
            {
                flags |= EnvFlags::WRITE_MAP;
            }
        }
    }
    flags
}

fn read_u32(db: &Db, txn: &RwTxn, key: &[u8]) -> Result<Option<u32>> {
    match db.get(txn, key).map_err(StoreError::from_heed)? {
        Some(raw) => raw
            .try_into()
            .map(u32::from_le_bytes)
            .map(Some)
            .map_err(|_| StoreError::Corrupt(format!("meta key {key:?} is not 4 bytes"))),
        None => Ok(None),
    }
}

fn read_u64(db: &Db, txn: &RwTxn, key: &[u8]) -> Result<Option<u64>> {
    match db.get(txn, key).map_err(StoreError::from_heed)? {
        Some(raw) => raw
            .try_into()
            .map(u64::from_le_bytes)
            .map(Some)
            .map_err(|_| StoreError::Corrupt(format!("meta key {key:?} is not 8 bytes"))),
        None => Ok(None),
    }
}

impl LmdbEngine {
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

        // SAFETY: LMDB maps the file into this process's address space. The
        // contract is that no other process mutates the file outside of LMDB's
        // own locking, which the lock file in the same directory enforces.
        let env = unsafe {
            EnvOpenOptions::new()
                // Thread-local reader slots, which is LMDB's fast path and the
                // single biggest thing measured in M6.
                //
                // Plan Â§9 specified `read_txn_without_tls()` so a `RoTxn` would
                // be `Send` and a hand-rolled reader pool could move one
                // between threads. That pool was never built â€” reads run on the
                // blocking pool, where a transaction is created and dropped
                // inside one call and never crosses a thread â€” so the flag was
                // being paid for and not used. And it is not cheap: without
                // TLS, every `mdb_txn_begin` claims a slot in a shared reader
                // table behind a process-wide mutex, which turns the read path
                // from lock-free into serialised. Measured by
                // `examples/txn_bench`: 344k lookups/s on one thread falling to
                // 91k on sixteen, against 948k rising to 5.3M with TLS.
                //
                // The cost is that a thread holds its reader slot until it
                // exits, so the slot table has to cover every thread that can
                // read at once. That is exactly the `store.max_readers >
                // server.max_blocking_threads` rule startup already enforces.
                .flags(env_flags(config.durability))
                .map_size(config.map_size)
                .max_dbs(MAX_DBS)
                .max_readers(config.max_readers)
                .open(&config.path)
        }
        .map_err(StoreError::from_heed)?;

        let mut wtxn = env.write_txn().map_err(StoreError::from_heed)?;
        let main: Db = create_db(&env, &mut wtxn, db::MAIN)?;
        let exp: Db = create_db(&env, &mut wtxn, db::EXPIRY)?;
        let tagidx: Db = create_db(&env, &mut wtxn, db::TAG_INDEX)?;
        let tag_meta: Db = create_db(&env, &mut wtxn, db::TAGS)?;
        let jobs: Db = create_db(&env, &mut wtxn, db::JOBS)?;
        let meta: Db = create_db(&env, &mut wtxn, db::META)?;

        match read_u32(&meta, &wtxn, meta_key::SCHEMA_VERSION)? {
            None => {
                meta.put(
                    &mut wtxn,
                    meta_key::SCHEMA_VERSION,
                    &SCHEMA_VERSION.to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
                meta.put(
                    &mut wtxn,
                    meta_key::RECORD_VERSION,
                    &(vash_core::RECORD_VERSION as u32).to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
                meta.put(
                    &mut wtxn,
                    meta_key::SHARD_INDEX,
                    &(shard_index as u32).to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
                meta.put(
                    &mut wtxn,
                    meta_key::SHARD_COUNT,
                    &(shard_count as u32).to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
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
        if let Some(stored) = read_u32(&meta, &wtxn, meta_key::SHARD_COUNT)?
            && stored as usize != shard_count
        {
            return Err(StoreError::Corrupt(format!(
                "database at {} was built for {stored} shard(s), but {shard_count} were configured; \
                 keys would route to the wrong environment",
                config.path.display()
            )));
        }
        if let Some(stored) = read_u32(&meta, &wtxn, meta_key::SHARD_INDEX)?
            && stored as usize != shard_index
        {
            return Err(StoreError::Corrupt(format!(
                "database at {} is shard {stored}, opened as shard {shard_index}",
                config.path.display()
            )));
        }

        let epoch = read_u32(&meta, &wtxn, meta_key::EPOCH)?.unwrap_or(0);
        // Resume past the last reserved block: anything below the persisted
        // watermark may already have been handed out before an unclean
        // shutdown, so the whole block is skipped rather than risk reuse.
        let cas_start = read_u64(&meta, &wtxn, meta_key::CAS_WATERMARK)?.unwrap_or(0);

        // The whole tag table lives in RAM: it is small, and the read path
        // consults it on every tagged record.
        let registry = TagRegistry::new(config.max_tags);
        let mut loaded = Vec::new();
        for entry in tag_meta.iter(&wtxn).map_err(StoreError::from_heed)? {
            let (name, raw) = entry.map_err(StoreError::from_heed)?;
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

        wtxn.commit().map_err(StoreError::from_heed)?;

        info!(
            path = %config.path.display(),
            durability = ?config.durability,
            map_size = config.map_size,
            epoch,
            cas_start,
            tag_count,
            "opened store"
        );

        Ok(Self {
            env,
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
        })
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        self.env.write_txn().map_err(StoreError::from_heed)
    }

    /// Opens a read transaction, recording how long it stays open.
    ///
    /// The recording is one relaxed store here and another when the returned
    /// guard drops — see [`crate::readers`] for why it is a slot per thread
    /// rather than a registry, and why LMDB cannot answer the question itself.
    pub fn read_txn(&self) -> Result<TrackedTxn<'_>> {
        let guard = self.reader_ages.open(self.now_ms());
        let txn = self.env.read_txn().map_err(StoreError::from_heed)?;
        Ok(TrackedTxn { txn, _guard: guard })
    }

    pub fn sync(&self) -> Result<()> {
        self.env.force_sync().map_err(StoreError::from_heed)
    }

    /// Closes the environment, blocking until LMDB has fully released it.
    ///
    /// Dropping an `Env` only schedules the close. LMDB keeps a process-wide
    /// registry of open environments and refuses to reopen a path still in it,
    /// so anything that reopens a database in-process has to wait for this.
    pub fn close(self) {
        self.env.prepare_for_closing().wait();
    }
}

/// A read transaction whose age is being recorded.
///
/// Derefs to the transaction, so callers pass `&rtxn` exactly as before and the
/// tracking is invisible to them. Bundling the guard with the transaction rather
/// than leaving it to the caller is what guarantees the two have the same
/// lifetime: a guard dropped early would under-report, and one dropped late
/// would report a reader that had already gone.
pub struct TrackedTxn<'e> {
    txn: RoTxn<'e, WithTls>,
    _guard: crate::readers::ReaderGuard<'e>,
}

impl<'e> std::ops::Deref for TrackedTxn<'e> {
    type Target = RoTxn<'e, WithTls>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
