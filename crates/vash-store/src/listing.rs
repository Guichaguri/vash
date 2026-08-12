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
