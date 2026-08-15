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

use std::ops::Bound;

use heed::{AnyTls, RoTxn, RwTxn};
use vash_core::{NEVER, RecordRef};

use crate::engine::LmdbEngine;

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

// ---- the resumable reclaimer -------------------------------------------
//
// The pass that drains the index encoded above, beside it for the same reason:
// the compound key is what makes resumption an O(log n) seek rather than a
// re-walk, and that property is only legible with both halves in view.

impl LmdbEngine {
    /// Advances the oldest outstanding tag-reclamation job by up to `budget`
    /// index entries.
    ///
    /// Resumable by design: the cursor is persisted in the same transaction as
    /// the deletions, so a crash or restart continues from where it stopped
    /// rather than starting the tag over.
    pub fn reclaim_step(&self, wtxn: &mut RwTxn, budget: usize) -> Result<Option<ReclaimStats>> {
        let Some((tag_id, job)) = self.next_job(wtxn)? else {
            return Ok(None);
        };

        let (low, high) = index_range(tag_id);
        let start = match &job.cursor {
            // Exclusive: resume just past the last entry processed.
            Some(cursor) => Bound::Excluded(cursor.to_vec()),
            None => Bound::Included(low.to_vec()),
        };

        let mut batch: Vec<([u8; INDEX_KEY_LEN], Vec<u8>)> = Vec::new();
        {
            let bounds = (
                match &start {
                    Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
                    _ => Bound::Included(low.as_slice()),
                },
                Bound::Included(high.as_slice()),
            );
            let iter = self
                .tagidx
                .range(wtxn, &bounds)
                .map_err(StoreError::from_heed)?;

            for entry in iter {
                let (index_key, user_key) = entry.map_err(StoreError::from_heed)?;
                let mut owned = [0u8; INDEX_KEY_LEN];
                if index_key.len() != INDEX_KEY_LEN {
                    return Err(StoreError::Corrupt(format!(
                        "tag index key is {} bytes, expected {}",
                        index_key.len(),
                        INDEX_KEY_LEN
                    )));
                }
                owned.copy_from_slice(index_key);
                batch.push((owned, user_key.to_vec()));

                if batch.len() >= budget {
                    break;
                }
            }
        }

        let mut stats = ReclaimStats {
            scanned: batch.len(),
            ..ReclaimStats::default()
        };
        let mut cursor = job.cursor;

        let now_ms = self.now_ms();
        let epoch = self.epoch();

        for (index_key, user_key) in batch {
            cursor = Some(index_key);

            let Some(blob) = self
                .main
                .get(wtxn, &user_key)
                .map_err(StoreError::from_heed)?
            else {
                // The record is already gone; its index entry is litter.
                self.tagidx
                    .delete(wtxn, &index_key)
                    .map_err(StoreError::from_heed)?;
                stats.orphaned += 1;
                continue;
            };

            let record = RecordRef::parse(blob)?;

            // Deadness is judged against the job's own target generation, not
            // the live registry.
            //
            // This pass can share a transaction with the very `DELETE_BY_TAG`
            // that queued it, and the registry is only updated *after* that
            // commits â€” so the in-memory generation still reads as the old one
            // here. Trusting it would mark every record live, advance the
            // cursor past them, and leak them permanently. The job carries the
            // target precisely so this decision needs no RAM state.
            let doomed = match record.tags.iter().find(|t| t.tag_id.get() == tag_id) {
                Some(tag) => tag.generation.get() < job.target_generation,
                None => {
                    // The record no longer carries this tag, so the entry is
                    // litter â€” but the record itself is somebody else's.
                    self.tagidx
                        .delete(wtxn, &index_key)
                        .map_err(StoreError::from_heed)?;
                    stats.orphaned += 1;
                    continue;
                }
            };

            // A record the registry already knows to be dead â€” expired, flushed
            // or invalidated via another tag â€” is fair game too. RAM can lag
            // behind the truth but never runs ahead of it, so "dead" is always
            // trustworthy even when "alive" is not.
            let known_dead = {
                let lookup = self.tags.lookup();
                !record.is_alive(now_ms, epoch, |id| lookup.generation(id))
            };

            if !doomed && !known_dead {
                // Rewritten since the invalidation, so this entry is current
                // and must survive.
                stats.retained += 1;
                continue;
            }

            let expires_at_ms = record.expires_at_ms();
            let tag_ids: Vec<u32> = record.tags.iter().map(|t| t.tag_id.get()).collect();

            self.main
                .delete(wtxn, &user_key)
                .map_err(StoreError::from_heed)?;
            if expires_at_ms != NEVER {
                self.exp
                    .delete(
                        wtxn,
                        &crate::expiry::encode_key(
                            expires_at_ms,
                            &user_key,
                            self.bucket_granularity_ms,
                        ),
                    )
                    .map_err(StoreError::from_heed)?;
            }
            for other in tag_ids {
                self.tagidx
                    .delete(wtxn, &self::index_key(other, &user_key))
                    .map_err(StoreError::from_heed)?;
            }
            stats.reclaimed += 1;
        }

        // Fewer than the budget means the range ran out, so the tag is done.
        if stats.scanned < budget {
            self.jobs
                .delete(wtxn, &tag_id.to_be_bytes())
                .map_err(StoreError::from_heed)?;
            stats.completed = true;
        } else {
            let resumed = Job {
                target_generation: job.target_generation,
                cursor,
            };
            self.jobs
                .put(wtxn, &tag_id.to_be_bytes(), &resumed.encode())
                .map_err(StoreError::from_heed)?;
        }

        Ok(Some(stats))
    }

    fn next_job(&self, wtxn: &RwTxn) -> Result<Option<(u32, Job)>> {
        let Some(entry) = self
            .jobs
            .iter(wtxn)
            .map_err(StoreError::from_heed)?
            .next()
            .transpose()
            .map_err(StoreError::from_heed)?
        else {
            return Ok(None);
        };

        let (raw_id, raw_job) = entry;
        let id: [u8; 4] = raw_id.try_into().map_err(|_| {
            StoreError::Corrupt("reclaim job key is not a 4-byte tag id".to_string())
        })?;
        Ok(Some((u32::from_be_bytes(id), Job::decode(raw_job)?)))
    }

    pub fn pending_jobs(&self, txn: &RoTxn<'_, AnyTls>) -> Result<u64> {
        Ok(self.jobs.stat(txn).map_err(StoreError::from_heed)?.entries as u64)
    }
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
