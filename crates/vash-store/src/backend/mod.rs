//! The storage engine seam.
//!
//! Everything above this — records, the expiry index, the tag registry, group
//! commit, the sweeper, the evictor, sharding — is engine-neutral, and was
//! already engine-neutral before this module existed. What is not is the
//! handful of operations below: open an environment, begin a transaction, get,
//! put, delete, clear, walk a range, ask how much space is used.
//!
//! That is the whole contract. [`docs/mdbx-proposal.md`] counts about a
//! hundred call sites across ten shapes, with no cursor positioning beyond a
//! range seek, no `DUPSORT`, no nested transactions and no custom comparators —
//! which is why a second engine is a file rather than a fork.
//!
//! **Why here rather than at [`Store`].** That trait has 27 methods and sits on
//! top of all the machinery listed above; implementing it twice would duplicate
//! a sweeper, an evictor, a writer thread and the record format, and leave two
//! copies of the eviction watermarks to drift apart. The seam that keeps the
//! engine decision reversible is this one; the seam that keeps it out of the
//! *server* is [`Store`], which stays exactly as it is — `vash-server` holds an
//! `Arc<dyn Store>` and sees nothing below it.
//!
//! **Static dispatch, and why the lifetimes are what they are.** A transaction
//! borrows its environment and a value borrows its transaction, which no
//! object-safe trait can express — and this is the innermost path in the
//! server, one call per B-tree descent rather than one per request. So
//! [`Backend`] carries associated types with lifetime parameters and every
//! layer above takes it as a generic parameter, up to `VashStore<B>`, where the
//! `dyn` boundary of [`Store`] absorbs it.
//!
//! [`Store`]: crate::Store
//! [`docs/mdbx-proposal.md`]: https://github.com/guichaguri/vash/blob/main/docs/mdbx-proposal.md

pub mod lmdb;

use std::ops::Bound;
use std::path::Path;

use crate::config::StoreConfig;
use crate::error::Result;

pub use lmdb::LmdbBackend;

/// The half-open key range a scan walks.
///
/// Borrowed rather than owned because every caller already has the bytes: the
/// reclaimer holds its tag's low and high keys, and the listing holds the key
/// it is resuming after.
pub type Range<'k> = (Bound<&'k [u8]>, Bound<&'k [u8]>);

/// Every entry, in key order. What the sweeper and the tag registry load walk.
pub const FULL: Range<'static> = (Bound::Unbounded, Bound::Unbounded);

/// What warming achieved for one environment.
///
/// Lives here rather than beside the warming code because it is what
/// [`Backend::warm`] returns, and a public trait cannot hand back a type its
/// callers are not allowed to name.
#[derive(Debug, Default, Clone, Copy)]
pub struct Warmed {
    /// Bytes of the data file pulled into the page cache.
    pub bytes: u64,
    /// Whether the mapping was **locked** into memory, so the kernel cannot
    /// reclaim it again under pressure.
    ///
    /// This is the difference between "resident now" and "resident from here
    /// on", and it is the whole reason `store.resident_mode` can enable inline
    /// reads when `store.prefault` alone cannot: warming makes the promise true
    /// at startup, and only the lock keeps it true.
    ///
    /// `false` whenever locking was not asked for, could not be applied, or is
    /// not reachable on this platform — never optimistic, because a caller uses
    /// it to decide whether to put reads on a runtime worker.
    pub locked: bool,
}

/// What an environment reports about itself.
///
/// One call rather than heed's `info()`/`stat()` split, because the two engines
/// divide these numbers between their own calls differently and no caller cares
/// which side they came from.
#[derive(Debug, Clone, Copy)]
pub struct EnvInfo {
    /// The map's size. A fixed reservation under LMDB; under a growable
    /// geometry it is the ceiling. The capacity watermarks are fractions of it
    /// either way.
    pub map_size: u64,
    pub page_size: u64,
    /// How far the engine has ever written — the high-water mark.
    ///
    /// **Not** a measure of space in use: neither engine lowers it when pages
    /// are freed. It is here for the warm-up, which needs to know how much of
    /// the file is real, and nothing else should reach for it. See
    /// [`crate::engine::Engine::used_bytes_in`] for the number that can fall.
    pub high_water_bytes: u64,
    pub readers_in_use: u32,
    pub max_readers: u32,
}

/// What one sub-database reports about itself.
#[derive(Debug, Clone, Copy, Default)]
pub struct DbStat {
    pub entries: u64,
    /// Pages genuinely occupied by this sub-database, excluding the free list.
    /// Summing these across the sub-databases is what makes `used_bytes` a
    /// number that can fall as well as rise.
    pub pages: u64,
}

/// One storage engine: an open environment and everything it can be asked to do.
pub trait Backend: Send + Sync + Sized + 'static {
    /// A named sub-database handle. `Copy` because the engine holds six of them
    /// by value and passes them into every operation.
    type Db: Copy + Send + Sync + 'static;

    type RoTxn<'e>: ReadTxn<Self>
    where
        Self: 'e;
    type RwTxn<'e>: WriteTxn<Self>
    where
        Self: 'e;

    /// Opens one environment at `path`.
    ///
    /// Takes the whole [`StoreConfig`] because mapping `map_size`, `durability`
    /// and `max_readers` onto engine flags is the backend's business — it is
    /// the one decision that genuinely differs between engines rather than
    /// merely being spelled differently. `path` is passed separately because a
    /// sharded store puts each environment in a subdirectory of `config.path`.
    fn open(config: &StoreConfig, path: &Path) -> Result<Self>;

    /// Opens a named sub-database, creating it if absent.
    fn create_db(&self, txn: &mut Self::RwTxn<'_>, name: &str) -> Result<Self::Db>;

    fn read_txn(&self) -> Result<Self::RoTxn<'_>>;
    fn write_txn(&self) -> Result<Self::RwTxn<'_>>;

    fn info(&self) -> EnvInfo;

    /// Forces everything committed onto stable storage.
    fn sync(&self) -> Result<()>;

    /// Warms the map, and pins it if asked, reporting what actually happened.
    ///
    /// On the backend rather than as a free function because how you warm a map
    /// is an engine question: LMDB has no such call, so this reads its data file
    /// and `madvise`s it (see [`crate::prefault`]), while an engine with a
    /// warm-up primitive of its own would call that instead.
    ///
    /// Best-effort by contract. The result is *reported* rather than assumed
    /// because a caller uses it to decide whether reads may run on a runtime
    /// worker — see [`crate::Store::map_locked`].
    fn warm(&self, config: &StoreConfig) -> Warmed;

    /// Releases the environment.
    ///
    /// Defaulted to nothing because it exists for one engine's lifecycle rather
    /// than for the storage contract: LMDB only *schedules* a release when its
    /// handle drops and refuses to reopen a path still registered in the
    /// process, so anything reopening a database in-process has to wait for it.
    /// An engine that closes synchronously on drop takes this default.
    fn close(self) {}
}

/// Reads, available on read and write transactions alike.
///
/// The write path reads constantly — a guarded write has to see the record it
/// is replacing — so this is a supertrait of [`WriteTxn`] rather than a
/// separate capability.
pub trait ReadTxn<B: Backend> {
    /// The stored bytes, **borrowed from the map rather than copied out**.
    ///
    /// This is what the whole record layout rests on: the value is a suffix of
    /// the blob, so reading it is a subslice with no parsing and no copy, and
    /// [`Store::get_with`] hands it to the encoder while the transaction is
    /// still open. Measured against libmdbx in `docs/mdbx-proposal.md` §Q2,
    /// where a 4 MiB value costs 1.03× an 8 B one — an engine that copied here
    /// would show three orders of magnitude instead, and would not be usable
    /// behind this trait.
    ///
    /// [`Store::get_with`]: crate::Store::get_with
    fn get(&self, db: B::Db, key: &[u8]) -> Result<Option<&[u8]>>;

    fn db_stat(&self, db: B::Db) -> Result<DbStat>;

    /// Walks `bounds` in key order.
    ///
    /// The iterator borrows the transaction, so it must be dropped before the
    /// transaction is used mutably again. Every caller already collects a
    /// bounded batch and then acts on it — see [`crate::reclaim`] and
    /// [`crate::expiry`] — which is a discipline heed's own borrow rules
    /// imposed long before this trait existed.
    fn range<'t>(
        &'t self,
        db: B::Db,
        bounds: Range<'_>,
    ) -> Result<impl Iterator<Item = Result<(&'t [u8], &'t [u8])>> + 't>;
}

/// Writes. Consumed by [`WriteTxn::commit`], so a committed transaction cannot
/// be used again.
pub trait WriteTxn<B: Backend>: ReadTxn<B> {
    fn put(&mut self, db: B::Db, key: &[u8], value: &[u8]) -> Result<()>;

    /// Removes a key, reporting whether it was there.
    fn delete(&mut self, db: B::Db, key: &[u8]) -> Result<bool>;

    /// Empties a sub-database without deleting it.
    fn clear(&mut self, db: B::Db) -> Result<()>;

    fn commit(self) -> Result<()>;
}
