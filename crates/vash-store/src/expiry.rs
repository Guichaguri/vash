//! The expiry index.
//!
//! Entries are keyed `expires_at_bucket BE || cas BE` and valued with the user
//! key. Two properties come out of that layout:
//!
//! 1. **Big-endian means LMDB's byte ordering is time ordering.** The sweeper
//!    opens a cursor at the start of the index and walks forward until it sees
//!    a bucket in the future, so its cost is proportional to the number of
//!    genuinely expired items, never to the size of the database. The idle case
//!    costs one cursor seek.
//! 2. **`cas` in the key makes stale entries harmless.** Overwriting a key with
//!    a new TTL leaves the old entry pointing at it; the sweeper compares the
//!    entry's `cas` against the record's and skips the mismatch. Writers do
//!    delete the superseded entry, but the check means a crash between the two
//!    puts cannot cause data loss.
//!
//! The bucket is the expiry time rounded **up** to a granularity, which
//! clusters entries onto shared B-tree pages and cuts the write amplification
//! LMDB's copy-on-write incurs when timestamps are spread thinly. Rounding up
//! is the safe direction: an entry can only fire later than the true expiry,
//! never earlier, and the record's exact timestamp is what the read path
//! checks.

pub const EXPIRY_KEY_LEN: usize = 16;

/// Bucket assigned to records that never expire.
///
/// They are indexed too, at the very end of the ordering, for one reason: the
/// capacity evictor walks this index to pick victims, and a record that is not
/// in it cannot be evicted. Without this, a cache holding only TTL-less keys
/// would fill up and have nothing to free. Because `u64::MAX` is always in the
/// future, the expiry sweeper stops before reaching them — they are evictable
/// but never *expire*.
///
/// Within the bucket, entries are ordered by CAS, which is insertion order.
pub const NEVER_BUCKET: u64 = u64::MAX;

/// Rounds an expiry timestamp up to the next multiple of `granularity_ms`.
#[inline]
pub fn bucket(expires_at_ms: u64, granularity_ms: u64) -> u64 {
    if granularity_ms <= 1 {
        return expires_at_ms;
    }
    // Saturating so a timestamp near u64::MAX cannot wrap into the past.
    match expires_at_ms.checked_add(granularity_ms - 1) {
        Some(sum) => (sum / granularity_ms) * granularity_ms,
        None => u64::MAX,
    }
}

/// The bucket a record belongs in, including the never-expires case.
#[inline]
pub fn bucket_for(expires_at_ms: u64, granularity_ms: u64) -> u64 {
    if expires_at_ms == vash_core::record::NEVER {
        NEVER_BUCKET
    } else {
        bucket(expires_at_ms, granularity_ms)
    }
}

#[inline]
pub fn encode_key(expires_at_ms: u64, cas: u64, granularity_ms: u64) -> [u8; EXPIRY_KEY_LEN] {
    let mut key = [0u8; EXPIRY_KEY_LEN];
    key[..8].copy_from_slice(&bucket_for(expires_at_ms, granularity_ms).to_be_bytes());
    key[8..].copy_from_slice(&cas.to_be_bytes());
    key
}

/// Splits an index key back into `(bucket, cas)`.
#[inline]
pub fn decode_key(key: &[u8]) -> Option<(u64, u64)> {
    if key.len() != EXPIRY_KEY_LEN {
        return None;
    }
    Some((
        u64::from_be_bytes(key[..8].try_into().ok()?),
        u64::from_be_bytes(key[8..].try_into().ok()?),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_up_never_down() {
        assert_eq!(bucket(0, 1000), 0);
        assert_eq!(bucket(1, 1000), 1000);
        assert_eq!(bucket(999, 1000), 1000);
        assert_eq!(bucket(1000, 1000), 1000);
        assert_eq!(bucket(1001, 1000), 2000);
    }

    #[test]
    fn a_bucket_is_never_earlier_than_the_true_expiry() {
        // The sweeper must not reclaim a record that is still live.
        for expiry in [0u64, 1, 500, 999, 1000, 123_456_789, u64::MAX / 2] {
            for granularity in [1u64, 10, 1000, 60_000] {
                assert!(
                    bucket(expiry, granularity) >= expiry,
                    "bucket({expiry}, {granularity}) fired early"
                );
            }
        }
    }

    #[test]
    fn granularity_of_one_is_the_identity() {
        assert_eq!(bucket(12_345, 1), 12_345);
        assert_eq!(bucket(12_345, 0), 12_345);
    }

    #[test]
    fn saturates_instead_of_wrapping_into_the_past() {
        assert_eq!(bucket(u64::MAX, 1000), u64::MAX);
        assert_eq!(bucket(u64::MAX - 1, 60_000), u64::MAX);
    }

    #[test]
    fn keys_roundtrip() {
        let key = encode_key(1_700_000_000_123, 42, 1000);
        assert_eq!(decode_key(&key), Some((1_700_000_001_000, 42)));
        assert_eq!(decode_key(&key[..15]), None);
    }

    #[test]
    fn records_that_never_expire_sort_last() {
        // They must be in the index so the evictor can reach them, and last so
        // it takes everything with a TTL first.
        let never = encode_key(vash_core::record::NEVER, 1, 1000);
        assert_eq!(decode_key(&never).unwrap().0, NEVER_BUCKET);

        for expiry in [1u64, 1000, u64::MAX / 2] {
            assert!(encode_key(expiry, 0, 1000) < never);
        }
    }

    #[test]
    fn never_expiring_entries_are_ordered_by_insertion() {
        // Same bucket, so the CAS tiebreaker decides — and CAS is assigned in
        // commit order, making eviction oldest-first.
        let first = encode_key(vash_core::record::NEVER, 10, 1000);
        let second = encode_key(vash_core::record::NEVER, 11, 1000);
        assert!(first < second);
    }

    #[test]
    fn byte_order_matches_time_order() {
        // This is the property the whole sweeper design rests on.
        let mut keys: Vec<_> = [(5000u64, 9u64), (1000, 3), (1000, 1), (9000, 0)]
            .into_iter()
            .map(|(e, c)| encode_key(e, c, 1000))
            .collect();
        keys.sort();

        let decoded: Vec<_> = keys.iter().map(|k| decode_key(k).unwrap()).collect();
        assert_eq!(decoded, vec![(1000, 1), (1000, 3), (5000, 9), (9000, 0)]);
    }
}
