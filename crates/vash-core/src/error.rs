/// Errors originating in the domain layer.
///
/// These are all input-validation or data-integrity failures; nothing here is
/// an I/O error, because this crate performs no I/O.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CoreError {
    #[error("key is empty")]
    EmptyKey,

    #[error("key is {len} bytes, limit is {max}")]
    KeyTooLong { len: usize, max: usize },

    #[error("key contains a byte that is not allowed: 0x{byte:02x} at offset {offset}")]
    InvalidKeyByte { byte: u8, offset: usize },

    #[error("value is {len} bytes, limit is {max}")]
    ValueTooLarge { len: usize, max: usize },

    #[error("{count} tags supplied, limit is {max}")]
    TooManyTags { count: usize, max: usize },

    #[error("tag name is empty")]
    EmptyTag,

    #[error("tag name is {len} bytes, limit is {max}")]
    TagTooLong { len: usize, max: usize },

    #[error("listing limit is {limit}, must be 1..={max}")]
    BadLimit { limit: u32, max: u32 },

    /// A listing pattern ended in a lone escape, naming a byte that is not
    /// there.
    #[error("pattern ends in an unterminated escape")]
    BadPattern,

    /// A listing cursor did not decode. Cursors are opaque to clients and are
    /// only ever echoed back, so this means a client fabricated one, corrupted
    /// one, or carried one across a change of shard count.
    #[error("listing cursor is malformed: {0}")]
    BadCursor(&'static str),

    /// Arithmetic against a value that is not an integer in decimal text.
    ///
    /// Every dialect reports this differently — memcached calls it a client
    /// error, Redis folds it together with a range failure — so the wording
    /// lives in the adapters and this carries only the fact.
    #[error("value is not an integer")]
    NotAnInteger,

    /// Arithmetic against a value that is not a float in decimal text.
    #[error("value is not a valid float")]
    NotAFloat,

    /// The result will not fit the operation's own type.
    #[error("increment or decrement would overflow")]
    Overflow,

    /// The result would be NaN or infinite.
    #[error("increment would produce NaN or Infinity")]
    NotFinite,

    /// A stored record could not be interpreted. This means on-disk corruption
    /// or a format written by an incompatible build.
    #[error("malformed record: {0}")]
    MalformedRecord(&'static str),

    #[error("unsupported record version {found}, this build understands {expected}")]
    RecordVersion { found: u8, expected: u8 },
}

pub type Result<T, E = CoreError> = std::result::Result<T, E>;
