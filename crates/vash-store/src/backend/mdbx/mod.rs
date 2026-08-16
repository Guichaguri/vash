//! The libmdbx backend, over a hand-written FFI.
//!
//! Everything that knows what libmdbx is lives here and in [`ffi`]. What it has
//! to get right, beyond the obvious, is two things Phase 0 of
//! `docs/mdbx-proposal.md` measured and neither of which fails loudly:
//!
//! 1. **`MDBX_NOSTICKYTHREADS` is never set.** It detaches reader slots from
//!    threads, which measured 56× worse on this project's own reader benchmark
//!    and is worse in absolute terms than LMDB's equivalent. Every maintained
//!    Rust wrapper sets it unconditionally — which is most of why this backend
//!    talks to the C library directly. [`tests::sticky_threads_stay_sticky`]
//!    is the guard.
//! 2. **Windows needs its working set raised before a map can be pinned.**
//!    `VirtualLock` refuses past the process working-set maximum with
//!    `ERROR_WORKING_SET_QUOTA`, so [`MdbxBackend::warm`] asks for the room it
//!    is about to use. Without it `map_locked()` silently reports `false` and
//!    `store.resident_mode` silently declines to engage.
//!
//! The geometry is the one place this backend is meaningfully *different*
//! rather than merely spelled differently: `store.map_size_mb` becomes an upper
//! bound that the file grows toward and can shrink back from, where LMDB takes
//! it as a fixed reservation. See [`MdbxBackend::open`].

pub mod ffi;

use std::ffi::{CStr, CString};
use std::ops::Bound;
use std::path::Path;
use std::ptr;

use crate::backend::{
    Backend, DbStat, EnvInfo, Range, ReadTxn, Warmed, WriteTxn, refuse_foreign_database,
};
use crate::config::{Durability, StoreConfig};
use crate::error::{Result, StoreError};
use crate::schema::MAX_DBS;

/// mdbx's data file. Distinct from LMDB's `data.mdb`, which is what lets a
/// store opened under the wrong engine be refused rather than mis-read.
pub(crate) const DATA_FILE: &str = "mdbx.dat";

fn strerror(rc: i32) -> String {
    // SAFETY: `mdbx_strerror` returns a static NUL-terminated string for any
    // code, including unknown ones.
    unsafe { CStr::from_ptr(ffi::mdbx_strerror(rc)) }
        .to_string_lossy()
        .into_owned()
}

/// Maps a return code onto the crate's error type.
///
/// `MDBX_MAP_FULL` and `MDBX_TXN_FULL` both become [`StoreError::CapacityFull`]:
/// the first is the geometry's ceiling and the second is a single transaction
/// growing past what it may dirty, and the writer's answer to both is to free
/// space and retry with a smaller batch. LMDB only has the first, which is why
/// this mapping lives per backend rather than in `error.rs`.
fn check(rc: i32, what: &str) -> Result<()> {
    match rc {
        ffi::MDBX_SUCCESS => Ok(()),
        ffi::MDBX_MAP_FULL | ffi::MDBX_TXN_FULL => Err(StoreError::CapacityFull),
        other => Err(StoreError::Engine(format!("{what}: {}", strerror(other)))),
    }
}

fn env_flags(config: &StoreConfig) -> u32 {
    let mut flags = match config.durability {
        Durability::Durable => ffi::MDBX_SYNC_DURABLE,
        Durability::Relaxed => ffi::MDBX_NOMETASYNC,
        // **`UTTERLY_NOSYNC`, not `SAFE_NOSYNC`.** The proposal picked the
        // latter for its stronger guarantee, and measuring it showed why the
        // header says what it says: "the number and volume of disk IOPs with
        // MDBX_SAFE_NOSYNC will [be] exactly the same as without any no-sync
        // flags". It buys integrity and no speed at all — under it, `lazy`
        // measured *slower* than `relaxed` and `durable`, which is the
        // durability ladder upside down.
        //
        // `UTTERLY_NOSYNC` is the mode mdbx documents as matching
        // `MDB_NOSYNC`, so it is what `lazy` means here: the same trade vash
        // already documents, with the loss window bounded by the periodic sync
        // set below rather than by the filesystem's goodwill.
        Durability::Lazy => ffi::MDBX_UTTERLY_NOSYNC,
    };

    if config.write_map {
        flags |= ffi::MDBX_WRITEMAP;
    }

    // Recycle the garbage list newest-first, so a reused page is one still warm
    // in the write-back cache. This is where mdbx's own throughput claim comes
    // from, and an overwrite-heavy cache is the workload it is aimed at.
    flags |= ffi::MDBX_LIFORECLAIM;

    // `MDBX_NOSTICKYTHREADS` is deliberately absent. See the module docs.
    flags
}

/// One open libmdbx environment.
pub struct MdbxBackend {
    env: *mut ffi::MDBX_env,
}

// SAFETY: an `MDBX_env` is explicitly shareable across threads — that is the
// whole model, with any number of concurrent readers and one writer. What is
// *not* shareable is a transaction, and without `MDBX_NOSTICKYTHREADS` mdbx
// requires each to be used only on the thread that began it. That invariant is
// enforced by the type system rather than by care: `MdbxRoTxn` and `MdbxRwTxn`
// hold raw pointers and are therefore neither `Send` nor `Sync`, so neither can
// cross a thread even by accident.
unsafe impl Send for MdbxBackend {}
unsafe impl Sync for MdbxBackend {}

impl MdbxBackend {
    fn info_raw(&self) -> ffi::Envinfo {
        let mut info = ffi::Envinfo::default();
        // SAFETY: a live environment, no transaction, and the length of the
        // prefix declared in `ffi` — which mdbx validates. See the `ffi` docs.
        let rc = unsafe {
            ffi::mdbx_env_info_ex(self.env, ptr::null(), &mut info, size_of::<ffi::Envinfo>())
        };
        debug_assert_eq!(rc, ffi::MDBX_SUCCESS, "mdbx_env_info_ex: {}", strerror(rc));
        info
    }
}

impl Backend for MdbxBackend {
    type Db = ffi::MDBX_dbi;
    type RoTxn<'e> = MdbxRoTxn<'e>;
    type RwTxn<'e> = MdbxRwTxn<'e>;

    fn open(config: &StoreConfig, path: &Path) -> Result<Self> {
        refuse_foreign_database(path, DATA_FILE, super::lmdb::DATA_FILE, "lmdb")?;

        let mut env: *mut ffi::MDBX_env = ptr::null_mut();
        // SAFETY: every call is checked before the handle is used again, and
        // the handle is closed by `Drop` on every path out of this function.
        unsafe {
            check(ffi::mdbx_env_create(&mut env), "env_create")?;
        }
        let backend = Self { env };

        // SAFETY: `backend` owns a live environment from here on, so a failure
        // below unwinds through its `Drop` and closes it.
        unsafe {
            check(
                ffi::mdbx_env_set_option(env, ffi::MDBX_OPT_MAX_DB, MAX_DBS as u64),
                "set max_dbs",
            )?;
            check(
                ffi::mdbx_env_set_option(env, ffi::MDBX_OPT_MAX_READERS, config.max_readers as u64),
                "set max_readers",
            )?;

            // **`map_size` is a ceiling here, not a reservation.** LMDB maps the
            // whole thing up front and leaves the file sparse; mdbx grows the
            // file toward the upper bound and shrinks it back, so the same
            // setting now bounds disk rather than address space. The eviction
            // watermarks are fractions of this either way, so their arithmetic
            // is unchanged — see `EnvInfo::map_size`.
            //
            // **Only the ceiling is stated.** `-1` leaves a parameter as it is,
            // which for a fresh database is mdbx's own default and for an
            // existing one is whatever it was opened with — so the file starts
            // small and grows, and `store.map_size_mb` keeps the property it
            // has always been documented with: it costs nothing until the data
            // arrives. Pinning the lower bound to `MIN_MAP_SIZE` instead made
            // every fresh database cost 16 MiB up front, per shard, which
            // `examples/mdbx_geometry.rs` measures.
            //
            // `MIN_MAP_SIZE` is still enforced, on `map_size` itself, in
            // `Engine::open`. It is LMDB's floor rather than a general one —
            // below it LMDB can report a full map permanently — and there is no
            // reason to make a growable file start there.
            // `size_now` is `store.preallocate_mb`, and `-1` — grow on demand —
            // is what it means when that is zero. Growth is the one thing this
            // backend does that LMDB never has to: see `StoreConfig::preallocate`
            // for what it costs on the write path and what it costs on disk.
            let now = match config.preallocate {
                0 => -1,
                bytes => bytes.min(config.map_size) as isize,
            };
            check(
                ffi::mdbx_env_set_geometry(env, -1, now, config.map_size as isize, -1, -1, -1),
                "set_geometry",
            )?;

            open_env(env, path, env_flags(config))?;

            // **After the open, not before.** This option lives in the shared
            // lock file rather than in the handle, so setting it on an unopened
            // environment is refused outright — `MDBX_EINVAL` on POSIX, and
            // `ERROR_INVALID_FUNCTION` here.
            //
            // Bounds the loss window from inside the engine as well as from the
            // writer's own timer: mdbx's docs recommend pairing a no-sync mode
            // with one of these thresholds rather than relying on the caller
            // alone, and the two agree by construction, since the same number
            // drives both.
            if config.durability == Durability::Lazy && config.write.sync_interval_ms > 0 {
                let period = config.write.sync_interval_ms.saturating_mul(65_536) / 1_000;
                check(
                    ffi::mdbx_env_set_option(env, ffi::MDBX_OPT_SYNC_PERIOD, period),
                    "set sync_period",
                )?;
            }
        }

        Ok(backend)
    }

    fn create_db(&self, txn: &mut Self::RwTxn<'_>, name: &str) -> Result<Self::Db> {
        let c_name = CString::new(name)
            .map_err(|_| StoreError::Corrupt(format!("sub-database name {name:?} has a NUL")))?;
        let mut dbi: ffi::MDBX_dbi = 0;
        // SAFETY: a live write transaction and a NUL-terminated name that
        // outlives the call.
        unsafe {
            check(
                ffi::mdbx_dbi_open(txn.txn, c_name.as_ptr(), ffi::MDBX_CREATE, &mut dbi),
                "dbi_open",
            )?;
        }
        Ok(dbi)
    }

    fn read_txn(&self) -> Result<Self::RoTxn<'_>> {
        let mut txn: *mut ffi::MDBX_txn = ptr::null_mut();
        // SAFETY: a live environment; the transaction is finished by `Drop`.
        unsafe {
            check(
                ffi::mdbx_txn_begin_ex(
                    self.env,
                    ptr::null_mut(),
                    ffi::MDBX_TXN_RDONLY,
                    &mut txn,
                    ptr::null_mut(),
                ),
                "txn_begin ro",
            )?;
        }
        Ok(MdbxRoTxn {
            txn,
            _env: std::marker::PhantomData,
        })
    }

    fn write_txn(&self) -> Result<Self::RwTxn<'_>> {
        let mut txn: *mut ffi::MDBX_txn = ptr::null_mut();
        // SAFETY: as above. mdbx serialises writers itself, so this blocks
        // rather than failing when another write transaction is open.
        unsafe {
            check(
                ffi::mdbx_txn_begin_ex(
                    self.env,
                    ptr::null_mut(),
                    ffi::MDBX_TXN_READWRITE,
                    &mut txn,
                    ptr::null_mut(),
                ),
                "txn_begin rw",
            )?;
        }
        Ok(MdbxRwTxn {
            txn,
            _env: std::marker::PhantomData,
        })
    }

    fn info(&self) -> EnvInfo {
        let info = self.info_raw();
        let page_size = info.dxb_pagesize as u64;
        EnvInfo {
            // The configured ceiling rather than the current mapping, so
            // `utilisation` means the same thing on both engines: how full the
            // store is against the size the operator asked for.
            map_size: info.geo.upper,
            page_size,
            high_water_bytes: info.last_pgno.saturating_add(1) * page_size,
            readers_in_use: info.numreaders,
            max_readers: info.maxreaders,
        }
    }

    fn sync(&self) -> Result<()> {
        // SAFETY: a live environment. `force`, and blocking, which is what the
        // callers — shutdown and the writer's periodic sync — both want.
        unsafe { check(ffi::mdbx_env_sync_ex(self.env, true, false), "env_sync") }
    }

    fn warm(&self, config: &StoreConfig) -> Warmed {
        if !config.prefault {
            return Warmed::default();
        }

        let bytes = self.info_raw().last_pgno.saturating_add(1)
            * self.info_raw().dxb_pagesize.max(1) as u64;

        // Unlike LMDB, mdbx has a warm-up primitive, so this is a call rather
        // than the read-the-file-and-madvise dance in `crate::prefault` — and it
        // works on every platform rather than on Linux alone, which is what
        // could take `store.resident_mode` off Linux. See
        // `docs/mdbx-proposal.md` §7.
        let mut flags = ffi::MDBX_WARMUP_FORCE;
        if config.lock_map {
            flags |= ffi::MDBX_WARMUP_LOCK;
            // Windows refuses `VirtualLock` past the process working-set
            // maximum, so ask for the room first. Measured: without this the
            // lock fails with winerror 1453 and `map_locked()` reports false.
            raise_lock_limit(bytes);
        }

        // SAFETY: a live environment, no transaction, a 60s ceiling in 16.16
        // fixed point so a slow device cannot hang startup indefinitely.
        let rc = unsafe { ffi::mdbx_env_warmup(self.env, ptr::null(), flags, 60 << 16) };

        let locked = config.lock_map && rc == ffi::MDBX_SUCCESS;
        if rc != ffi::MDBX_SUCCESS && rc != ffi::MDBX_RESULT_TRUE {
            // Logged and ignored, exactly as the LMDB path does: a map that
            // could not be warmed serves every request correctly and merely
            // faults on the way. Refusing to start over it would be worse.
            tracing::warn!(
                error = %strerror(rc),
                "could not warm the map; serving without it"
            );
            return Warmed {
                bytes: 0,
                locked: false,
            };
        }
        if config.lock_map && !locked {
            tracing::warn!("warmed the map but could not pin it; resident_mode will not engage");
        }

        Warmed { bytes, locked }
    }
}

impl Drop for MdbxBackend {
    fn drop(&mut self) {
        if self.env.is_null() {
            return;
        }
        // SAFETY: the environment is live and no transaction outlives it — the
        // store shuts its writer thread down and drops every engine before
        // this. `dont_sync = false`, so a `lazy` database is still flushed on
        // the way out, matching what `LmdbStore::close` does.
        unsafe { ffi::mdbx_env_close_ex(self.env, false) };
        self.env = ptr::null_mut();
    }
}

/// Opens the environment, using the wide-character entry point on Windows.
///
/// # Safety
///
/// `env` must be a live handle that has not yet been opened.
unsafe fn open_env(env: *mut ffi::MDBX_env, path: &Path, flags: u32) -> Result<()> {
    let text = path.to_str().ok_or_else(|| {
        StoreError::Corrupt(format!("store path {} is not valid UTF-8", path.display()))
    })?;
    let c_path = CString::new(text)
        .map_err(|_| StoreError::Corrupt("store path contains a NUL byte".into()))?;
    // SAFETY: the caller's contract, plus a NUL-terminated path that outlives
    // the call. 0o644 is the mode a fresh data file is created with, matching
    // what the LMDB path asks for.
    unsafe {
        check(
            ffi::mdbx_env_open(env, c_path.as_ptr(), flags, 0o644),
            "env_open",
        )
    }
}

/// Raises whatever this platform caps pinned memory with, best-effort.
#[cfg(windows)]
fn raise_lock_limit(bytes: u64) {
    use std::ffi::c_void;
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn GetProcessWorkingSetSize(h: *mut c_void, min: *mut usize, max: *mut usize) -> i32;
        fn SetProcessWorkingSetSize(h: *mut c_void, min: usize, max: usize) -> i32;
    }

    let (mut min, mut max) = (0usize, 0usize);
    // SAFETY: a pseudo-handle to this process and two out-parameters this
    // frame owns. No memory crosses the boundary.
    unsafe {
        if GetProcessWorkingSetSize(GetCurrentProcess(), &mut min, &mut max) == 0 {
            return;
        }
        // The room the map is about to need, on top of what the process
        // already has. Slack because the working set covers everything else
        // this process has touched, not only the map.
        let want = bytes.saturating_add(64 * 1024 * 1024) as usize;
        if SetProcessWorkingSetSize(
            GetCurrentProcess(),
            min.saturating_add(want),
            max.saturating_add(want),
        ) == 0
        {
            tracing::debug!("could not raise the working set; pinning the map will likely fail");
        }
    }
}

/// On Linux the cap is `RLIMIT_MEMLOCK`, which an unprivileged process cannot
/// raise past its hard limit — so there is nothing to do here but let the
/// warm-up report what happened. Same position `crate::prefault` is in.
#[cfg(not(windows))]
fn raise_lock_limit(_bytes: u64) {}

/// A read transaction.
///
/// Not `Send`, by construction rather than by a bound: it holds a raw pointer,
/// and without `MDBX_NOSTICKYTHREADS` mdbx requires a transaction to stay on
/// the thread that began it. Reads in this crate begin and end one inside a
/// single blocking-pool call, so nothing wants to move one.
pub struct MdbxRoTxn<'e> {
    txn: *mut ffi::MDBX_txn,
    _env: std::marker::PhantomData<&'e MdbxBackend>,
}

/// A write transaction. Reads through it too, so it implements both halves.
pub struct MdbxRwTxn<'e> {
    txn: *mut ffi::MDBX_txn,
    _env: std::marker::PhantomData<&'e MdbxBackend>,
}

impl Drop for MdbxRoTxn<'_> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            // SAFETY: a live read transaction, finished exactly once.
            unsafe { ffi::mdbx_txn_abort(self.txn) };
        }
    }
}

impl Drop for MdbxRwTxn<'_> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            // SAFETY: a live write transaction that was not committed —
            // `commit` nulls the pointer before returning, so this cannot
            // double-finish. Aborting is what the writer relies on when a batch
            // is poisoned: the whole transaction is discarded.
            unsafe { ffi::mdbx_txn_abort(self.txn) };
        }
    }
}

/// # Safety
///
/// `txn` must be live, and the returned bytes borrow from it.
unsafe fn get_in<'t>(
    txn: *const ffi::MDBX_txn,
    db: ffi::MDBX_dbi,
    key: &[u8],
) -> Result<Option<&'t [u8]>> {
    let k = ffi::Val::of(key);
    let mut v = ffi::Val::empty();
    // SAFETY: the caller's contract; `k` outlives the call and `v` is only read
    // once the code says it was written.
    let rc = unsafe { ffi::mdbx_get(txn, db, &k, &mut v) };
    match rc {
        ffi::MDBX_SUCCESS => {
            // SAFETY: mdbx wrote a pointer into the map, valid for as long as
            // the transaction is open — which is what `'t` is tied to.
            Ok(Some(unsafe { v.as_slice() }))
        }
        ffi::MDBX_NOTFOUND => Ok(None),
        other => Err(check(other, "get").unwrap_err()),
    }
}

/// # Safety
///
/// `txn` must be live.
unsafe fn stat_in(txn: *const ffi::MDBX_txn, db: ffi::MDBX_dbi) -> Result<DbStat> {
    let mut stat = ffi::Stat::default();
    // SAFETY: the caller's contract, and the full structure's length — mdbx
    // validates it the same way it validates `Envinfo`.
    unsafe {
        check(
            ffi::mdbx_dbi_stat(txn, db, &mut stat, size_of::<ffi::Stat>()),
            "dbi_stat",
        )?;
    }
    Ok(DbStat {
        entries: stat.entries,
        pages: stat.branch_pages + stat.leaf_pages + stat.overflow_pages,
    })
}

macro_rules! impl_read_txn {
    ($ty:ident) => {
        impl ReadTxn<MdbxBackend> for $ty<'_> {
            fn get(&self, db: ffi::MDBX_dbi, key: &[u8]) -> Result<Option<&[u8]>> {
                // SAFETY: this transaction is live for as long as `&self`.
                unsafe { get_in(self.txn, db, key) }
            }

            fn db_stat(&self, db: ffi::MDBX_dbi) -> Result<DbStat> {
                // SAFETY: as above.
                unsafe { stat_in(self.txn, db) }
            }

            fn range<'t>(
                &'t self,
                db: ffi::MDBX_dbi,
                bounds: Range<'_>,
            ) -> Result<impl Iterator<Item = Result<(&'t [u8], &'t [u8])>> + 't> {
                // SAFETY: as above; the cursor is closed when the iterator drops,
                // which the borrow of `self` guarantees happens before the
                // transaction is finished.
                unsafe { MdbxRange::open(self.txn, db, bounds) }
            }
        }
    };
}

impl_read_txn!(MdbxRoTxn);
impl_read_txn!(MdbxRwTxn);

impl WriteTxn<MdbxBackend> for MdbxRwTxn<'_> {
    fn put(&mut self, db: ffi::MDBX_dbi, key: &[u8], value: &[u8]) -> Result<()> {
        let k = ffi::Val::of(key);
        let mut v = ffi::Val::of(value);
        // SAFETY: a live write transaction and two slices that outlive the call.
        unsafe { check(ffi::mdbx_put(self.txn, db, &k, &mut v, 0), "put") }
    }

    fn delete(&mut self, db: ffi::MDBX_dbi, key: &[u8]) -> Result<bool> {
        let k = ffi::Val::of(key);
        // SAFETY: as above. A null `data` deletes whatever is under the key,
        // which without `DUPSORT` is the only entry there can be.
        let rc = unsafe { ffi::mdbx_del(self.txn, db, &k, ptr::null()) };
        match rc {
            ffi::MDBX_SUCCESS => Ok(true),
            ffi::MDBX_NOTFOUND => Ok(false),
            other => Err(check(other, "delete").unwrap_err()),
        }
    }

    fn clear(&mut self, db: ffi::MDBX_dbi) -> Result<()> {
        // SAFETY: a live write transaction. `del = false` empties the
        // sub-database and keeps its handle, which every `Db` in the engine is
        // still holding.
        unsafe { check(ffi::mdbx_drop(self.txn, db, false), "clear") }
    }

    fn commit(mut self) -> Result<()> {
        let txn = std::mem::replace(&mut self.txn, ptr::null_mut());
        // SAFETY: a live write transaction, finished exactly once — the pointer
        // is nulled above, so the `Drop` that follows this call does nothing.
        unsafe { check(ffi::mdbx_txn_commit_ex(txn, ptr::null_mut()), "commit") }
    }
}

/// A cursor walking one key range.
struct MdbxRange<'t> {
    cursor: *mut ffi::MDBX_cursor,
    /// The lower bound, when it is exclusive and so needs one extra step.
    skip_first: Option<Box<[u8]>>,
    /// Owned, because the iterator outlives the caller's borrow of the bounds.
    /// `None` for an unbounded walk, which is three of this crate's five range
    /// callers and therefore the case that allocates nothing.
    upper: Bound<Box<[u8]>>,
    lower: Option<Box<[u8]>>,
    started: bool,
    done: bool,
    _txn: std::marker::PhantomData<&'t ()>,
}

impl<'t> MdbxRange<'t> {
    /// # Safety
    ///
    /// `txn` must be live for at least `'t`.
    unsafe fn open(
        txn: *const ffi::MDBX_txn,
        db: ffi::MDBX_dbi,
        bounds: Range<'_>,
    ) -> Result<Self> {
        let mut cursor: *mut ffi::MDBX_cursor = ptr::null_mut();
        // SAFETY: the caller's contract; the cursor is closed by `Drop`.
        unsafe {
            check(ffi::mdbx_cursor_open(txn, db, &mut cursor), "cursor_open")?;
        }

        let (lower, skip_first) = match bounds.0 {
            Bound::Unbounded => (None, None),
            Bound::Included(key) => (Some(key.into()), None),
            Bound::Excluded(key) => (Some(key.into()), Some(key.into())),
        };
        let upper = match bounds.1 {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(key.into()),
            Bound::Excluded(key) => Bound::Excluded(key.into()),
        };

        Ok(Self {
            cursor,
            skip_first,
            upper,
            lower,
            started: false,
            done: false,
            _txn: std::marker::PhantomData,
        })
    }

    /// Whether `key` is still inside the upper bound.
    fn within(&self, key: &[u8]) -> bool {
        match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(limit) => key <= &**limit,
            Bound::Excluded(limit) => key < &**limit,
        }
    }

    fn step(&mut self, op: i32, seek: Option<&[u8]>) -> Option<Result<(&'t [u8], &'t [u8])>> {
        let mut k = match seek {
            Some(key) => ffi::Val::of(key),
            None => ffi::Val::empty(),
        };
        let mut v = ffi::Val::empty();
        // SAFETY: a live cursor on a live transaction; `seek` outlives the call.
        let rc = unsafe { ffi::mdbx_cursor_get(self.cursor, &mut k, &mut v, op) };
        match rc {
            ffi::MDBX_SUCCESS => {
                // SAFETY: mdbx wrote pointers into the map, valid for the life
                // of the transaction, which outlives `'t`.
                let (key, value) = unsafe { (k.as_slice(), v.as_slice()) };
                if !self.within(key) {
                    self.done = true;
                    return None;
                }
                Some(Ok((key, value)))
            }
            ffi::MDBX_NOTFOUND => {
                self.done = true;
                None
            }
            other => {
                self.done = true;
                Some(Err(check(other, "cursor_get").unwrap_err()))
            }
        }
    }
}

impl<'t> Iterator for MdbxRange<'t> {
    type Item = Result<(&'t [u8], &'t [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if !self.started {
            self.started = true;
            let first = match self.lower.take() {
                Some(from) => self.step(ffi::MDBX_SET_RANGE, Some(&from)),
                None => self.step(ffi::MDBX_FIRST, None),
            };
            // An exclusive lower bound lands *on* its own key when that key
            // exists, so one extra step is needed — and only when it matched,
            // since `SET_RANGE` otherwise lands past it already.
            return match (first, self.skip_first.take()) {
                (Some(Ok((key, _))), Some(excluded)) if key == &*excluded => {
                    self.step(ffi::MDBX_NEXT, None)
                }
                (other, _) => other,
            };
        }

        self.step(ffi::MDBX_NEXT, None)
    }
}

impl Drop for MdbxRange<'_> {
    fn drop(&mut self) {
        if !self.cursor.is_null() {
            // SAFETY: a live cursor, closed exactly once, before its
            // transaction is finished — the borrow of the transaction in
            // `range` is what guarantees the ordering.
            unsafe { ffi::mdbx_cursor_close(self.cursor) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The structure prefix in [`ffi::Envinfo`] has to match the C library's,
    /// and mdbx will only tell us by refusing a length it does not recognise.
    /// A silent mismatch would misreport the map size, which is the input to
    /// every capacity watermark.
    #[test]
    fn env_info_layout_matches_the_c_library() {
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig {
            path: dir.path().join("db"),
            ..StoreConfig::default()
        };
        std::fs::create_dir_all(&config.path).unwrap();
        let backend = MdbxBackend::open(&config, &config.path).unwrap();

        let mut info = ffi::Envinfo::default();
        // SAFETY: a live environment and the declared prefix length.
        let rc = unsafe {
            ffi::mdbx_env_info_ex(
                backend.env,
                ptr::null(),
                &mut info,
                size_of::<ffi::Envinfo>(),
            )
        };
        assert_eq!(
            rc,
            ffi::MDBX_SUCCESS,
            "mdbx rejected our MDBX_envinfo prefix ({} bytes): {}",
            size_of::<ffi::Envinfo>(),
            strerror(rc)
        );

        // And the values have to be plausible, since a same-sized but shuffled
        // prefix would pass the length check.
        assert_eq!(info.geo.upper, config.map_size as u64, "geometry ceiling");
        assert!(
            info.dxb_pagesize.is_power_of_two() && info.dxb_pagesize >= 256,
            "page size {} is not a plausible one",
            info.dxb_pagesize
        );
        // **`max_readers` is a floor under mdbx, where LMDB takes it exactly.**
        // mdbx rounds the reader table up to fill the pages it occupies — 256
        // becomes 368 here — so a deployment gets at least what it asked for
        // and usually more. That direction is the safe one: the startup rule
        // this setting exists for is `store.max_readers >
        // server.max_blocking_threads`, and more slots cannot break it.
        assert!(
            info.maxreaders >= config.max_readers,
            "reader slots: asked for {}, got {}",
            config.max_readers,
            info.maxreaders
        );
    }

    /// `MDBX_NOSTICKYTHREADS` costs 56× on reads and every maintained Rust
    /// wrapper sets it. Nothing breaks visibly when it is on, so this asserts
    /// the flag computation rather than trusting a comment.
    #[test]
    fn sticky_threads_stay_sticky() {
        for durability in [Durability::Durable, Durability::Relaxed, Durability::Lazy] {
            for write_map in [false, true] {
                let flags = env_flags(&StoreConfig {
                    durability,
                    write_map,
                    ..StoreConfig::default()
                });
                assert_eq!(
                    flags & ffi::MDBX_NOSTICKYTHREADS,
                    0,
                    "NOSTICKYTHREADS set for {durability:?}/write_map={write_map}"
                );
            }
        }
    }

    /// Two impls that overlap exactly when `T: Send`, so a type-relative call
    /// to `check` resolves only while the type is **not** `Send`.
    ///
    /// The blunt version — a generic `fn assert_not_send<T>()` — asserts
    /// nothing at all, because an unbounded parameter accepts everything.
    trait NotSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> NotSend<()> for T {}
    impl<T: ?Sized + Send> NotSend<u8> for T {}

    /// A transaction must not be able to cross a thread: without
    /// `MDBX_NOSTICKYTHREADS`, mdbx binds one to the thread that began it, and
    /// using it elsewhere is undefined rather than merely wrong. Nothing but
    /// the missing `Send` stops a future refactor from moving one, so this
    /// stops compiling if that ever changes.
    #[test]
    fn transactions_cannot_cross_threads() {
        <MdbxRoTxn<'static>>::check();
        <MdbxRwTxn<'static>>::check();

        // The environment, by contrast, must be shareable: every shard's reads
        // run concurrently against it.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MdbxBackend>();
    }
}
