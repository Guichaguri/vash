use cache_core::CoreError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Core(#[from] CoreError),

    #[error("lmdb: {0}")]
    Lmdb(#[from] heed::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The LMDB map is full. Surfaced to clients as `CAPACITY_FULL` so they can
    /// fall back to the origin rather than block.
    #[error("store is at capacity")]
    CapacityFull,

    /// A feature the on-disk format supports but this milestone has not wired up.
    #[error("{0} is not supported yet")]
    Unsupported(&'static str),

    /// The writer queue is full. Reported rather than queued without bound: a
    /// client told "overloaded" can fall back to its origin, a client left
    /// waiting cannot.
    #[error("write queue is full")]
    Overloaded,

    /// The store is closing and cannot accept more writes.
    #[error("store is shutting down")]
    ShuttingDown,

    /// The tag registry is full. It lives entirely in RAM, so an unbounded one
    /// is a memory leak a client could drive by inventing tag names.
    #[error("tag registry is full ({0} tags)")]
    TagLimit(usize),

    /// `incr`/`decr` on a value that is not decimal text. memcached reports
    /// this as a client error rather than a miss.
    #[error("value is not a decimal number")]
    NotNumeric,

    #[error("database is corrupt or was written by an incompatible build: {0}")]
    Corrupt(String),
}

impl StoreError {
    /// Maps LMDB's `MDB_MAP_FULL` onto the dedicated capacity variant so callers
    /// do not have to pattern-match on heed internals.
    pub(crate) fn from_heed(err: heed::Error) -> Self {
        match err {
            heed::Error::Mdb(heed::MdbError::MapFull) => Self::CapacityFull,
            other => Self::Lmdb(other),
        }
    }
}

pub type Result<T, E = StoreError> = std::result::Result<T, E>;
