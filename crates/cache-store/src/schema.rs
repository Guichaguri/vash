//! On-disk schema constants.

/// Bumped when the arrangement of sub-databases changes incompatibly. The
/// record layout has its own version, in `cache_core::record`.
pub const SCHEMA_VERSION: u32 = 1;

/// Maximum named sub-databases. Fixed at environment-open time and impossible
/// to raise afterwards without reopening, so it is sized now for every
/// sub-database the plan calls for, not just the ones M0 creates.
pub const MAX_DBS: u32 = 16;

/// How many CAS values are reserved per durable write of the watermark.
/// Larger means fewer meta writes and a bigger gap in the CAS sequence after an
/// unclean shutdown — which is harmless, since CAS only has to be unique and
/// monotonic, never dense.
pub const CAS_BLOCK: u64 = 1 << 20;

pub mod db {
    /// key -> encoded record
    pub const MAIN: &str = "main";
    /// schema and counters
    pub const META: &str = "meta";

    // Created from M1/M2 onward; named here so the set is reviewable in one place.
    /// (expires_at_ms BE || cas BE) -> key
    pub const EXPIRY: &str = "exp";
    /// DUP_SORT: tag_id BE -> [key]
    pub const TAG_INDEX: &str = "tagidx";
    /// tag name -> { tag_id, generation }
    pub const TAGS: &str = "tags";
    /// reclaim job checkpoints
    pub const JOBS: &str = "jobs";
}

pub mod meta_key {
    pub const SCHEMA_VERSION: &[u8] = b"schema_version";
    pub const RECORD_VERSION: &[u8] = b"record_version";
    /// Global flush epoch (see plan §5).
    pub const EPOCH: &[u8] = b"epoch";
    /// High-water mark of handed-out CAS values.
    pub const CAS_WATERMARK: &[u8] = b"cas_watermark";
}
