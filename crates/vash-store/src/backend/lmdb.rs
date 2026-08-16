//! The LMDB backend, over `heed`.
//!
//! Everything that knows what `heed` is lives here: the flag mapping, map
//! sizing, sub-database creation, and the one error translation that matters —
//! `MDB_MAP_FULL` onto [`StoreError::CapacityFull`], so the evictor upstream
//! never learns which engine it is running on.
//!
//! This is a move rather than a rewrite. The flags, the `SAFETY` reasoning and
//! the thread-local reader decision all came from `env.rs` unchanged, and the
//! comments explaining *why* came with them, because they are about LMDB and
//! this is now where LMDB is.

use std::path::Path;

use heed::types::Bytes as HeedBytes;
use heed::{Env, EnvFlags, EnvOpenOptions, RwTxn, WithTls};

use crate::backend::{Backend, DbStat, EnvInfo, Range, ReadTxn, Warmed, WriteTxn};
use crate::config::{Durability, StoreConfig};
use crate::error::{Result, StoreError};
use crate::schema::MAX_DBS;

type HeedDb = heed::Database<HeedBytes, HeedBytes>;

/// Maps LMDB's `MDB_MAP_FULL` onto the dedicated capacity variant, and
/// everything else onto the engine-neutral one.
///
/// The dedicated variant is load-bearing: it is what tells the writer to free
/// space and retry rather than fail the batch, and what surfaces to clients as
/// `CAPACITY_FULL`. Matching on heed internals anywhere else in the crate is
/// what this function exists to prevent.
fn map_err(err: heed::Error) -> StoreError {
    match err {
        heed::Error::Mdb(heed::MdbError::MapFull) => StoreError::CapacityFull,
        other => StoreError::Engine(other.to_string()),
    }
}

fn env_flags(durability: Durability, write_map: bool) -> EnvFlags {
    let mut flags = EnvFlags::empty();
    match durability {
        Durability::Durable => {}
        Durability::Relaxed => flags |= EnvFlags::NO_META_SYNC,
        Durability::Lazy => flags |= EnvFlags::NO_SYNC,
    }

    // Separate from the durability mode because it is not a durability
    // decision: it trades LMDB's dirty-page copies for a lower memory
    // footprint. **Whether it is also faster depends on the platform, and the
    // measurements disagree by more than the flag's own effect.** On Linux it
    // is worth nothing — 1.04x, 0.96x and 1.01x under `lazy` on the same NVMe
    // the Windows numbers below were taken on — which is the result §6 and §9
    // of `docs/performance-proposals.md` were written against. Natively on
    // Windows the same build under `lazy` measures 1.08x closed loop, 1.26x
    // pipelined and 1.17x mixed, winning all fifteen paired runs. The device
    // was identical in both, so this is an mmap-and-filesystem difference, not
    // a storage one.
    //
    // No longer Unix-gated: the `mdb_env_open` failure that gate was written
    // for does not reproduce, at 4, 16 or 64 GiB map sizes, and while it stood
    // it made `store.write_map` a silent no-op on the one platform where the
    // flag pays. `store.write_map` documents what it costs `lazy` in exchange.
    if write_map {
        flags |= EnvFlags::WRITE_MAP;
    }
    flags
}

/// One open LMDB environment.
pub struct LmdbBackend {
    env: Env<WithTls>,
}

impl Backend for LmdbBackend {
    type Db = HeedDb;
    type RoTxn<'e> = LmdbRoTxn<'e>;
    type RwTxn<'e> = LmdbRwTxn<'e>;

    fn open(config: &StoreConfig, path: &Path) -> Result<Self> {
        // SAFETY: LMDB maps the file into this process's address space. The
        // contract is that no other process mutates the file outside of LMDB's
        // own locking, which the lock file in the same directory enforces.
        let env = unsafe {
            EnvOpenOptions::new()
                // Thread-local reader slots, which is LMDB's fast path and the
                // single biggest thing measured in M6.
                //
                // Plan §9 specified `read_txn_without_tls()` so a `RoTxn` would
                // be `Send` and a hand-rolled reader pool could move one
                // between threads. That pool was never built — reads run on the
                // blocking pool, where a transaction is created and dropped
                // inside one call and never crosses a thread — so the flag was
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
                //
                // **Any second backend has to make the same choice**, and the
                // equivalent flag is not always spelled as an opt-in: libmdbx's
                // `MDBX_NOSTICKYTHREADS` measured 56× worse on this same
                // benchmark, and every maintained Rust wrapper for it sets that
                // flag unconditionally. See `docs/mdbx-proposal.md` §Q3.
                .flags(env_flags(config.durability, config.write_map))
                .map_size(config.map_size)
                .max_dbs(MAX_DBS)
                .max_readers(config.max_readers)
                .open(path)
        }
        .map_err(map_err)?;

        Ok(Self { env })
    }

    fn create_db(&self, txn: &mut Self::RwTxn<'_>, name: &str) -> Result<Self::Db> {
        self.env
            .create_database(&mut txn.0, Some(name))
            .map_err(map_err)
    }

    fn read_txn(&self) -> Result<Self::RoTxn<'_>> {
        self.env.read_txn().map(LmdbRoTxn).map_err(map_err)
    }

    fn write_txn(&self) -> Result<Self::RwTxn<'_>> {
        self.env.write_txn().map(LmdbRwTxn).map_err(map_err)
    }

    fn info(&self) -> EnvInfo {
        let info = self.env.info();
        let stat = self.env.stat();
        let page_size = stat.page_size as u64;
        EnvInfo {
            map_size: info.map_size as u64,
            page_size,
            high_water_bytes: (info.last_page_number as u64 + 1) * page_size,
            readers_in_use: info.number_of_readers,
            max_readers: info.maximum_number_of_readers,
        }
    }

    fn sync(&self) -> Result<()> {
        self.env.force_sync().map_err(map_err)
    }

    fn warm(&self, config: &StoreConfig) -> Warmed {
        if !config.prefault {
            return Warmed::default();
        }
        // How far LMDB has ever written, which on Linux is the only thing
        // separating the data from a sparse file the size of the whole map.
        // See `prefault` — getting this wrong is expensive and silent.
        let high_water = self.info().high_water_bytes;
        match crate::prefault::prefault(&config.path, high_water, config.lock_map) {
            Ok(warmed) => warmed,
            Err(err) => {
                // Logged and ignored on purpose: a map that could not be warmed
                // serves every request correctly and merely faults on the way,
                // which is exactly what every deployment before this flag
                // existed did. Refusing to start over a performance measure
                // would be the worse trade.
                tracing::warn!(%err, "could not prefault the map; serving without it");
                Warmed::default()
            }
        }
    }

    /// Blocks until LMDB has fully released the environment.
    ///
    /// Dropping an `Env` only schedules the close. LMDB keeps a process-wide
    /// registry of open environments and refuses to reopen a path still in it,
    /// so anything that reopens a database in-process has to wait for this.
    fn close(self) {
        self.env.prepare_for_closing().wait();
    }
}

/// A read transaction, with thread-local reader slots.
pub struct LmdbRoTxn<'e>(heed::RoTxn<'e, WithTls>);

/// A write transaction. Reads through it as well, which is why it implements
/// both halves of the seam.
pub struct LmdbRwTxn<'e>(RwTxn<'e>);

impl ReadTxn<LmdbBackend> for LmdbRoTxn<'_> {
    fn get(&self, db: HeedDb, key: &[u8]) -> Result<Option<&[u8]>> {
        db.get(&self.0, key).map_err(map_err)
    }

    fn db_stat(&self, db: HeedDb) -> Result<DbStat> {
        stat_of(db.stat(&self.0).map_err(map_err)?)
    }

    fn range<'t>(
        &'t self,
        db: HeedDb,
        bounds: Range<'_>,
    ) -> Result<impl Iterator<Item = Result<(&'t [u8], &'t [u8])>> + 't> {
        Ok(db.range(&self.0, &bounds).map_err(map_err)?.map(entry))
    }
}

impl ReadTxn<LmdbBackend> for LmdbRwTxn<'_> {
    fn get(&self, db: HeedDb, key: &[u8]) -> Result<Option<&[u8]>> {
        db.get(&self.0, key).map_err(map_err)
    }

    fn db_stat(&self, db: HeedDb) -> Result<DbStat> {
        stat_of(db.stat(&self.0).map_err(map_err)?)
    }

    fn range<'t>(
        &'t self,
        db: HeedDb,
        bounds: Range<'_>,
    ) -> Result<impl Iterator<Item = Result<(&'t [u8], &'t [u8])>> + 't> {
        Ok(db.range(&self.0, &bounds).map_err(map_err)?.map(entry))
    }
}

impl WriteTxn<LmdbBackend> for LmdbRwTxn<'_> {
    fn put(&mut self, db: HeedDb, key: &[u8], value: &[u8]) -> Result<()> {
        db.put(&mut self.0, key, value).map_err(map_err)
    }

    fn delete(&mut self, db: HeedDb, key: &[u8]) -> Result<bool> {
        db.delete(&mut self.0, key).map_err(map_err)
    }

    fn clear(&mut self, db: HeedDb) -> Result<()> {
        db.clear(&mut self.0).map_err(map_err)
    }

    fn commit(self) -> Result<()> {
        self.0.commit().map_err(map_err)
    }
}

fn entry<'t>(
    item: std::result::Result<(&'t [u8], &'t [u8]), heed::Error>,
) -> Result<(&'t [u8], &'t [u8])> {
    item.map_err(map_err)
}

fn stat_of(stat: heed::DatabaseStat) -> Result<DbStat> {
    Ok(DbStat {
        entries: stat.entries as u64,
        pages: stat.branch_pages as u64 + stat.leaf_pages as u64 + stat.overflow_pages as u64,
    })
}
