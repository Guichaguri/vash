use vash_core::CoreError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Core(#[from] CoreError),

    /// The storage engine refused an operation.
    ///
    /// Carries the engine's message rather than its error type: the type is
    /// per-engine, and the only distinction any caller in this crate draws is
    /// [`Self::CapacityFull`], which each backend maps for itself. Keeping the
    /// message as a `String` is also what makes this variant `Clone` — see
    /// [`StoreError::clone_shallow`], which used to have to downgrade an LMDB
    /// failure to `Corrupt` because `heed::Error` is not.
    #[error("engine: {0}")]
    Engine(String),

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

    #[error("database is corrupt or was written by an incompatible build: {0}")]
    Corrupt(String),
}

impl StoreError {
    /// Whether this failure leaves the write transaction unusable.
    ///
    /// A copy-on-write B-tree invalidates a transaction as soon as one of its
    /// operations fails — a full map being the common case — and every later
    /// call on it fails too. The writer has to abort the whole batch at that
    /// point rather than carry on and report a confusing secondary error.
    ///
    /// Validation failures are different: they are rejected before the engine
    /// is touched, so the transaction is still good and the rest of the batch
    /// can proceed.
    pub(crate) fn poisons_transaction(&self) -> bool {
        matches!(self, Self::Engine(_) | Self::CapacityFull)
    }

    /// Whether this failure means the store is out of room, and so calls for
    /// freeing space rather than simply retrying.
    pub(crate) fn is_capacity(&self) -> bool {
        matches!(self, Self::CapacityFull)
    }

    /// A copy suitable for fanning one failure out to every caller in a batch.
    ///
    /// Every variant a caller acts on survives intact. Only [`Self::Io`] cannot
    /// be cloned, and it is the one that never reaches a batch: the engine's own
    /// failures arrive as [`Self::Engine`], which carries a `String` precisely
    /// so this stays lossless.
    pub(crate) fn clone_shallow(&self) -> Self {
        match self {
            Self::CapacityFull => Self::CapacityFull,
            Self::Overloaded => Self::Overloaded,
            Self::ShuttingDown => Self::ShuttingDown,
            Self::TagLimit(n) => Self::TagLimit(*n),
            Self::Unsupported(what) => Self::Unsupported(what),
            Self::Core(e) => Self::Core(e.clone()),
            Self::Corrupt(detail) => Self::Corrupt(detail.clone()),
            Self::Engine(detail) => Self::Engine(detail.clone()),
            other => Self::Corrupt(other.to_string()),
        }
    }
}

pub type Result<T, E = StoreError> = std::result::Result<T, E>;
