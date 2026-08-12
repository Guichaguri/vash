//! The paginated listing shared by `LIST_KEYS` and `LIST_TAGS`.
//!
//! Both commands take the same request and return the same page, field for
//! field — they differ only in what the entries name and where the server reads
//! them from. One decoder, one validator, one matcher, one client loop. See
//! `docs/opcodes.md`.
//!
//! These are administrative and diagnostic commands. Correctness and bounded
//! cost matter; throughput does not, and neither is on any hot path.

use crate::error::Result;
use crate::glob;

/// Most entries one page may carry.
///
/// One ceiling for both commands, so a client's paging logic has no per-command
/// case. Rejected rather than clamped when a client asks for more: a client that
/// asked for 10000 and silently got 1024 would page incorrectly.
pub const MAX_LIST_LIMIT: u32 = 1024;

/// A decoded listing request, borrowing from the connection's read buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRequest<'a> {
    /// Entries to return at most, 1..=[`MAX_LIST_LIMIT`].
    pub limit: u32,
    /// Where to resume. Empty starts from the beginning.
    ///
    /// **Opaque to clients**, which only ever echo back what they were given.
    /// The encoding belongs to whichever side of the store produces it, so it
    /// travels as bytes and is decoded where it means something.
    pub cursor: &'a [u8],
    /// The [`glob`] pattern entries must match. Empty matches everything.
    pub pattern: &'a [u8],
}

impl<'a> ListRequest<'a> {
    /// A request for the first page of everything.
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            cursor: &[],
            pattern: &[],
        }
    }

    /// Checks the bounds a decoder must enforce before this reaches the store.
    ///
    /// The pattern is validated here rather than at match time so a bad one
    /// costs a rejected frame instead of a scan that returns nothing.
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 || self.limit > MAX_LIST_LIMIT {
            return Err(crate::error::CoreError::BadLimit {
                limit: self.limit,
                max: MAX_LIST_LIMIT,
            });
        }
        glob::validate(self.pattern)
    }

    /// Whether an entry's name belongs in the reply.
    #[inline]
    pub fn matches(&self, name: &[u8]) -> bool {
        glob::matches(self.pattern, name)
    }
}

/// One listed entry: a name and the version the server holds for it.
///
/// `version` is the record's CAS token for a key and the tag's generation for a
/// tag. Both are opaque monotonic version numbers — comparable against an
/// earlier reading of the *same* name and against nothing else — and both are
/// free to report, since the record header is parsed for the liveness check
/// whether or not the CAS is sent.
///
/// Laid out on the wire exactly as a [`crate::TagGeneration`] is in `TAG_SYNC`,
/// so a client that decodes a gossip digest decodes a listing too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListEntry {
    pub name: Box<[u8]>,
    pub version: u64,
}

impl ListEntry {
    pub fn new(name: impl Into<Box<[u8]>>, version: u64) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }
}

/// One page of a listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    pub entries: Vec<ListEntry>,
    /// Entries examined to produce this page, including the dead and
    /// non-matching ones. A page of ten keys that cost ninety thousand records
    /// to find is how an operator learns their pattern is not selective.
    pub scanned: u64,
    /// Where to resume, or `None` when the listing is complete.
    ///
    /// **An absent cursor is the whole termination rule.** There is no separate
    /// "more" flag: a flag beside a field that is present exactly when there is
    /// more is one of the two lying eventually.
    pub cursor: Option<Box<[u8]>>,
    /// The page ended on the scan budget rather than on `limit`.
    ///
    /// Diagnostic only — paging behaves identically either way, because a
    /// budget exhaustion still advances the cursor.
    pub truncated: bool,
}

impl Listing {
    /// A complete, empty listing.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Whether a client must ask for another page.
    #[inline]
    pub fn has_more(&self) -> bool {
        self.cursor.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_bounds_are_enforced() {
        assert!(ListRequest::new(1).validate().is_ok());
        assert!(ListRequest::new(MAX_LIST_LIMIT).validate().is_ok());
        assert!(ListRequest::new(0).validate().is_err());
        assert!(ListRequest::new(MAX_LIST_LIMIT + 1).validate().is_err());
    }

    #[test]
    fn a_bad_pattern_is_refused_by_validation() {
        let request = ListRequest {
            limit: 10,
            cursor: &[],
            pattern: br"trailing\",
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn an_empty_pattern_matches_every_name() {
        let request = ListRequest::new(10);
        assert!(request.matches(b"anything"));
        assert!(request.matches(b""));
    }

    #[test]
    fn a_complete_listing_carries_no_cursor() {
        let done = Listing::empty();
        assert!(!done.has_more());

        let more = Listing {
            cursor: Some(b"somewhere".to_vec().into_boxed_slice()),
            ..Listing::empty()
        };
        assert!(more.has_more());
    }
}
