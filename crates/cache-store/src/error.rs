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
