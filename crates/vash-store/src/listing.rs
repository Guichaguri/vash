//! The cursor a key listing resumes from.
//!
//! A page cannot hold an LMDB cursor open between requests — that would pin a
//! read transaction, which blocks page reuse and grows the file without bound,
//! the footgun plan §9 exists to avoid. So what travels to the client is
//! **data**: the position to seek back to. This is the same trick the tag
//! reclaimer already uses when it persists a `Job` cursor in the `jobs`
//! sub-database, with the client holding the position instead of a table here.
//!
//! Opaque to clients, which only ever echo it back. The encoding is this
//! module's business and may change; nothing outside interprets it.

use std::ops::Bound;

use tracing::warn;
use vash_core::RecordRef;

use crate::engine::LmdbEngine;

use vash_core::{CoreError, MAX_KEY_LEN};

use crate::error::{Result, StoreError};

/// `shard_index u16` ahead of the key.
const SHARD_PREFIX_LEN: usize = 2;

/// The wire refuses a cursor longer than this before it reaches us, so an
/// encoding that could exceed it would produce cursors the next request
/// rejects — a pager that stops dead one page in. Checked at compile time
/// rather than discovered at that point.
const _: () = assert!(SHARD_PREFIX_LEN + MAX_KEY_LEN <= vash_core::MAX_LIST_CURSOR_LEN);

/// Builds the cursor for "resume strictly after `key` in `shard`".
pub(crate) fn encode(shard: usize, key: &[u8]) -> Box<[u8]> {
    let mut out = Vec::with_capacity(SHARD_PREFIX_LEN + key.len());
    out.extend_from_slice(&(shard as u16).to_le_bytes());
    out.extend_from_slice(key);
    out.into_boxed_slice()
}

/// Reads a cursor into the shard to resume in and the key to resume after.
///
/// An empty cursor is the start of the listing, not an error — that is how a
/// client asks for the first page.
///
/// Everything else is validated rather than trusted. A cursor is bytes from the
/// network: it may have been fabricated, corrupted, or carried across a change
/// of shard count. A malformed one is refused so a client's pager fails loudly,
/// **never silently restarted from the beginning**, which would loop forever
/// returning the same first page and never say why.
pub(crate) fn decode(cursor: &[u8], shards: usize) -> Result<(usize, Option<&[u8]>)> {
    if cursor.is_empty() {
        return Ok((0, None));
    }

    let Some(raw) = cursor.get(..SHARD_PREFIX_LEN) else {
        return Err(bad("shorter than its shard index"));
    };
    let shard = u16::from_le_bytes(raw.try_into().expect("two bytes")) as usize;
    if shard >= shards {
        // Reachable without malice: a cursor from before a reshard names a
        // shard that no longer exists.
        return Err(bad("names a shard this server does not have"));
    }

    let key = &cursor[SHARD_PREFIX_LEN..];
    if key.is_empty() {
        return Err(bad("names no key"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(bad("key is longer than any key can be"));
    }

    Ok((shard, Some(key)))
}

fn bad(detail: &'static str) -> StoreError {
    StoreError::Core(CoreError::BadCursor(detail))
}

// ---- the scan the cursor resumes ----------------------------------------
//
// One shard's contribution to a page. Beside the cursor encoding because the
// two only make sense together: what the scan stops on is exactly what the
// cursor has to be able to name.

/// What one shard produced for a page of a key listing.
#[derive(Debug, Default)]
pub struct ShardScan {
    pub entries: Vec<vash_core::ListEntry>,
    /// Records examined, including the dead and non-matching ones — which is
    /// what makes it worth reporting.
    pub scanned: u64,
    /// The key the walk stopped on, or `None` if it reached the end of this
    /// shard.
    pub stopped_at: Option<Box<[u8]>>,
    /// It stopped on the scan budget rather than on `limit`.
    pub budget_exhausted: bool,
}

impl LmdbEngine {
    /// One shard's contribution to a key listing.
    ///
    /// Walks `main` in key order from `after` (exclusive), collecting the live
    /// keys that match, and stops at whichever of `limit` or `budget` runs out
    /// first. **The only cursor walk over `main` in the server** — every other
    /// read is a point lookup, and the sweeper and reclaimer walk their indexes
    /// instead.
    ///
    /// Administrative, and specified to be a linear scan: the pattern is applied
    /// after the record is read, not turned into a range seek. See
    /// `docs/opcodes.md` for the optimisation deliberately not made.
    pub fn list_keys(
        &self,
        request: &vash_core::ListRequest<'_>,
        after: Option<&[u8]>,
        limit: usize,
        budget: usize,
    ) -> Result<ShardScan> {
        let rtxn = self.read_txn()?;
        let lookup = self.tags.lookup();
        let now_ms = self.now_ms();
        let epoch = self.epoch();

        let bounds: (Bound<&[u8]>, Bound<&[u8]>) = match after {
            // Exclusive: resume just past the entry the last page ended on,
            // exactly as the reclaimer resumes a half-finished job.
            Some(key) => (Bound::Excluded(key), Bound::Unbounded),
            None => (Bound::Unbounded, Bound::Unbounded),
        };

        let mut scan = ShardScan::default();
        for entry in self
            .main
            .range(&rtxn, &bounds)
            .map_err(StoreError::from_heed)?
        {
            let (key, blob) = entry.map_err(StoreError::from_heed)?;
            scan.scanned += 1;

            match RecordRef::parse(blob) {
                Ok(record) => {
                    // The same liveness rule as `GET`, so a listed key is one a
                    // read at this instant would hit. A dead record costs a
                    // `scanned` and nothing else: a listing never writes, so it
                    // does not reclaim what it finds — that stays the sweeper's
                    // and the reclaimer's job.
                    if record.is_alive(now_ms, epoch, |id| lookup.generation(id))
                        && request.matches(key)
                    {
                        scan.entries
                            .push(vash_core::ListEntry::new(key.to_vec(), record.cas()));
                    }
                }
                // Skipped rather than propagated, unlike every point read.
                // Failing the page would make the keyspace past a corrupt
                // record unlistable — the tool would break exactly when the
                // database did, which is when it is wanted most.
                Err(error) => warn!(?key, %error, "skipping an unreadable record while listing"),
            }

            if scan.entries.len() >= limit {
                scan.stopped_at = Some(key.into());
                return Ok(scan);
            }
            if scan.scanned as usize >= budget {
                scan.stopped_at = Some(key.into());
                scan.budget_exhausted = true;
                return Ok(scan);
            }
        }

        // Ran off the end of this shard, so the next one starts at its own
        // beginning and this shard is never revisited.
        Ok(scan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_roundtrips() {
        let cursor = encode(3, b"session:42");
        assert_eq!(
            decode(&cursor, 4).unwrap(),
            (3, Some(b"session:42".as_slice()))
        );
    }

    #[test]
    fn an_empty_cursor_starts_at_the_beginning() {
        assert_eq!(decode(&[], 4).unwrap(), (0, None));
    }

    #[test]
    fn a_cursor_for_a_shard_that_is_gone_is_refused() {
        // What a client holds after the shard count changed. Restarting the
        // listing silently would hand back keys it had already seen and never
        // explain why.
        let cursor = encode(7, b"k");
        assert!(decode(&cursor, 4).is_err());
    }

    #[test]
    fn malformed_cursors_are_refused_rather_than_guessed_at() {
        assert!(decode(&[1], 4).is_err(), "shorter than the shard index");
        assert!(decode(&[0, 0], 4).is_err(), "no key");

        let mut too_long = vec![0, 0];
        too_long.extend_from_slice(&vec![b'k'; MAX_KEY_LEN + 1]);
        assert!(decode(&too_long, 4).is_err());
    }
}
