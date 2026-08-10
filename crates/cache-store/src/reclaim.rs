//! The tag index and the resumable reclaimer that drains it.
//!
//! Invalidating a tag is O(1) (see [`crate::tags`]), which makes the data
//! *invisible* immediately. Reclaiming its disk space is a separate, background
//! concern, and this is it.
//!
//! # Why not `DUP_SORT`
//!
//! The plan specified a `DUP_SORT` database mapping `tag_id -> [user key]`.
//! Implementing the resumable cursor showed that to be the wrong structure:
//! LMDB can seek to a *key*, but heed exposes no way to seek to a position
//! **within** a key's duplicate list, so resuming a half-finished job means
//! re-walking every duplicate already processed. For a tag with a million keys
//! and a batch of 256 that is quadratic.
//!
//! A compound key gives the same ordering and makes resumption an O(log n)
//! range seek to the exact cursor position:
//!
//! ```text
//! key   = tag_id u32 BE || xxh3_64(user key) BE     (12 bytes, fixed)
//! value = user key
//! ```
//!
//! The user key is hashed rather than embedded because LMDB caps keys at 511
//! bytes and a 4-byte prefix plus a 511-byte key would not fit. A hash
//! collision between two keys under the same tag would drop one index entry, so
//! that record is not reclaimed proactively — it stays correct (reads still
//! check liveness, TTLs still apply), it just lingers. At 64 bits that needs
//! billions of keys on one tag to become likely.

use crate::error::{Result, StoreError};

/// `tag_id u32 BE || key hash u64 BE`
pub const INDEX_KEY_LEN: usize = 12;

#[inline]
pub fn index_key(tag_id: u32, user_key: &[u8]) -> [u8; INDEX_KEY_LEN] {
    let mut key = [0u8; INDEX_KEY_LEN];
    key[..4].copy_from_slice(&tag_id.to_be_bytes());
    // Must be a stable hash: this value is persisted, so a per-process seed
    // would orphan the whole index on restart.
    key[4..].copy_from_slice(&xxhash_rust::xxh3::xxh3_64(user_key).to_be_bytes());
    key
}

/// Lower and upper bounds covering every entry for one tag.
pub fn index_range(tag_id: u32) -> ([u8; INDEX_KEY_LEN], [u8; INDEX_KEY_LEN]) {
    let mut low = [0u8; INDEX_KEY_LEN];
    low[..4].copy_from_slice(&tag_id.to_be_bytes());

    let mut high = [0xffu8; INDEX_KEY_LEN];
    high[..4].copy_from_slice(&tag_id.to_be_bytes());

    (low, high)
}

/// A reclamation job: everything tagged `tag_id` written before
/// `target_generation` is dead and its space can be freed.
///
/// Keyed by tag id, so at most one job exists per tag. Re-invalidating a tag
/// mid-reclaim raises the target and restarts the scan rather than queueing a
/// second pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub target_generation: u64,
    /// Index key of the last entry processed. Resumption seeks just past it.
    /// Empty means the scan has not started.
    pub cursor: Option<[u8; INDEX_KEY_LEN]>,
}

impl Job {
    pub fn new(target_generation: u64) -> Self {
        Self {
            target_generation,
            cursor: None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + INDEX_KEY_LEN);
        buf.extend_from_slice(&self.target_generation.to_le_bytes());
        if let Some(cursor) = &self.cursor {
            buf.extend_from_slice(cursor);
        }
        buf
    }

    pub fn decode(raw: &[u8]) -> Result<Self> {
        if raw.len() < 8 {
            return Err(StoreError::Corrupt(format!(
                "reclaim job is {} bytes, expected at least 8",
                raw.len()
            )));
        }
        let target_generation = u64::from_le_bytes(raw[..8].try_into().expect("8 bytes"));

        let cursor = match raw.len() {
            8 => None,
            n if n == 8 + INDEX_KEY_LEN => Some(raw[8..].try_into().expect("checked length")),
            n => {
                return Err(StoreError::Corrupt(format!(
                    "reclaim job is {n} bytes, expected 8 or {}",
                    8 + INDEX_KEY_LEN
                )));
            }
        };

        Ok(Self {
            target_generation,
            cursor,
        })
    }
}

/// What one reclamation pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReclaimStats {
    /// Index entries examined.
    pub scanned: usize,
    /// Records deleted because they were dead.
    pub reclaimed: usize,
    /// Entries dropped whose record was already gone.
    pub orphaned: usize,
    /// Entries left alone because their record was rewritten after the
    /// invalidation and is live again.
    pub retained: usize,
    /// The job finished and was removed.
    pub completed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_keys_group_and_order_by_tag() {
        let a = index_key(1, b"alpha");
        let b = index_key(1, b"beta");
        let c = index_key(2, b"alpha");

        assert_eq!(&a[..4], &1u32.to_be_bytes());
        assert!(a < c, "tag 1 must sort entirely before tag 2");
        assert!(b < c);
    }

    #[test]
    fn the_range_covers_every_entry_for_its_tag_and_nothing_else() {
        let (low, high) = index_range(7);
        for key in [b"".as_slice(), b"a", b"zzzzzzzzzzzzzzzzzzzz"] {
            let entry = index_key(7, key);
            assert!(
                entry >= low && entry <= high,
                "entry escaped its tag's range"
            );
        }
        // Neighbouring tags must fall outside.
        assert!(index_key(6, b"x") < low);
        assert!(index_key(8, b"x") > high);
    }

    #[test]
    fn hashing_is_stable_across_runs() {
        // Persisted, so a randomly-seeded hasher would orphan the index on
        // every restart.
        assert_eq!(index_key(3, b"stable"), index_key(3, b"stable"));
        assert_eq!(
            &index_key(0, b"known")[4..],
            &xxhash_rust::xxh3::xxh3_64(b"known").to_be_bytes()
        );
    }

    #[test]
    fn jobs_roundtrip_with_and_without_a_cursor() {
        let fresh = Job::new(42);
        assert_eq!(Job::decode(&fresh.encode()).unwrap(), fresh);

        let resumed = Job {
            target_generation: 7,
            cursor: Some(index_key(1, b"somewhere")),
        };
        assert_eq!(Job::decode(&resumed.encode()).unwrap(), resumed);
    }

    #[test]
    fn malformed_jobs_are_rejected_not_guessed() {
        assert!(Job::decode(&[]).is_err());
        assert!(Job::decode(&[0u8; 7]).is_err());
        assert!(Job::decode(&[0u8; 13]).is_err());
    }
}
