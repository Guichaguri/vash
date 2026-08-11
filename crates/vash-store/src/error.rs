use vash_core::CoreError;

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
    /// Whether this failure leaves the write transaction unusable.
    ///
    /// LMDB invalidates a transaction as soon as one of its operations fails —
    /// a full map being the common case — and every later call on it returns
    /// `MDB_BAD_TXN`. The writer has to abort the whole batch at that point
    /// rather than carry on and report a confusing secondary error.
    ///
    /// Validation failures are different: they are rejected before LMDB is
    /// touched, so the transaction is still good and the rest of the batch can
    /// proceed.
    pub(crate) fn poisons_transaction(&self) -> bool {
        matches!(self, Self::Lmdb(_) | Self::CapacityFull)
    }

    /// Whether this failure means the store is out of room, and so calls for
    /// freeing space rather than simply retrying.
    pub(crate) fn is_capacity(&self) -> bool {
        matches!(self, Self::CapacityFull)
    }

    /// A copy suitable for fanning one failure out to every caller in a batch.
    ///
    /// `heed::Error` is not `Clone`, so an LMDB failure keeps its message but
    /// loses its type. The variants callers act on — `CapacityFull` above all —
    /// are preserved exactly.
    pub(crate) fn clone_shallow(&self) -> Self {
        match self {
            Self::CapacityFull => Self::CapacityFull,
            Self::Overloaded => Self::Overloaded,
            Self::ShuttingDown => Self::ShuttingDown,
            Self::NotNumeric => Self::NotNumeric,
            Self::TagLimit(n) => Self::TagLimit(*n),
            Self::Unsupported(what) => Self::Unsupported(what),
            Self::Core(e) => Self::Core(e.clone()),
            Self::Corrupt(detail) => Self::Corrupt(detail.clone()),
            other => Self::Corrupt(other.to_string()),
        }
    }

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
