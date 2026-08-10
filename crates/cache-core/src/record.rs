//! The on-disk record format.
//!
//! Layout (all integers little-endian, all fields unaligned):
//!
//! ```text
//! offset  size  field
//!      0     1  version
//!      1     1  tag_count
//!      2     2  reserved
//!      4     4  epoch            global flush epoch at write time
//!      8     4  mc_flags         memcached 32-bit client flags
//!     12     8  expires_at_ms    absolute unix ms; 0 = never expires
//!     20     8  cas              version counter / memcached CAS token
//!     28    12  tag_ref[0]       (tag_id u32, generation u64)
//!    ...   ...  tag_ref[n]
//!  28+12n   ..  value bytes
//! ```
//!
//! Two properties drive this design:
//!
//! 1. **The value is a suffix**, so reading it is a subslice of the mmap with no
//!    parsing and no copy.
//! 2. **Liveness is decidable from the header alone**, using only RAM-resident
//!    state (the clock, the global epoch, the tag registry). A read never needs
//!    a second disk lookup to know whether a record is still valid. This is what
//!    makes O(1) tag invalidation and lazy TTL possible — see plan §4 and §5.
//!
//! The unaligned little-endian integer types are load-bearing: with native
//! `u32`/`u64` the struct would gain padding (a `TagRef` would be 16 bytes, not
//! 12) and could not be cast from an arbitrary offset in the mmap.

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{CoreError, Result};

/// Current record format version. Bump only for incompatible layout changes.
pub const RECORD_VERSION: u8 = 1;

pub const RECORD_HEADER_LEN: usize = 28;
pub const TAG_REF_LEN: usize = 12;

/// Maximum tags attachable to a single record. Bounded because `tag_count` is a
/// `u8` and because the liveness check is O(tags) on every read.
pub const MAX_TAGS: usize = 32;

/// Maximum tag name length, in bytes. Tag names are LMDB keys in the registry.
pub const MAX_TAG_LEN: usize = 255;

/// Sentinel in `expires_at_ms` meaning "no expiry".
pub const NEVER: u64 = 0;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
pub struct RecordHeader {
    pub version: u8,
    pub tag_count: u8,
    pub reserved: U16,
    pub epoch: U32,
    pub mc_flags: U32,
    pub expires_at_ms: U64,
    pub cas: U64,
}

/// A record's reference to one of its tags, capturing the tag's generation as
/// it stood when the record was written. See plan §5.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
pub struct TagRef {
    pub tag_id: U32,
    pub generation: U64,
}

impl TagRef {
    pub fn new(tag_id: u32, generation: u64) -> Self {
        Self {
            tag_id: U32::new(tag_id),
            generation: U64::new(generation),
        }
    }
}

/// Mutable fields of a record, supplied at write time.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordMeta {
    pub epoch: u32,
    pub mc_flags: u32,
    /// Absolute unix milliseconds, or [`NEVER`].
    pub expires_at_ms: u64,
    pub cas: u64,
}

/// A borrowed view over a stored record. The lifetime ties it to the read
/// transaction that produced it, so it cannot outlive the mmap it points into.
#[derive(Clone, Copy, Debug)]
pub struct RecordRef<'a> {
    pub header: &'a RecordHeader,
    pub tags: &'a [TagRef],
    pub value: &'a [u8],
}

impl<'a> RecordRef<'a> {
    /// Interprets a stored blob. Performs no copying.
    pub fn parse(buf: &'a [u8]) -> Result<Self> {
        let (header, rest) = RecordHeader::ref_from_prefix(buf)
            .map_err(|_| CoreError::MalformedRecord("shorter than the header"))?;

        if header.version != RECORD_VERSION {
            return Err(CoreError::RecordVersion {
                found: header.version,
                expected: RECORD_VERSION,
            });
        }

        let tag_count = header.tag_count as usize;
        let (tags, value) = <[TagRef]>::ref_from_prefix_with_elems(rest, tag_count)
            .map_err(|_| CoreError::MalformedRecord("tag table extends past the record"))?;

        Ok(Self {
            header,
            tags,
            value,
        })
    }

    /// True when the record carries a TTL that has passed.
    ///
    /// This is checked on **every read**, which is what allows the expiry index
    /// sweeper to be lazy: an expired record is never served regardless of
    /// whether the sweeper has reached it yet.
    #[inline]
    pub fn is_expired(&self, now_ms: u64) -> bool {
        let expires_at = self.header.expires_at_ms.get();
        expires_at != NEVER && expires_at <= now_ms
    }

    /// True when a global flush has happened since this record was written.
    #[inline]
    pub fn is_flushed(&self, current_epoch: u32) -> bool {
        self.header.epoch.get() != current_epoch
    }

    /// True when every tag this record carries is still at the generation the
    /// record was written with. A mismatch means the tag was invalidated.
    ///
    /// `current_generation` returns `None` for a tag the registry has never
    /// heard of, which is treated as invalidated — that can only happen if the
    /// registry was rebuilt, and failing closed turns it into a cache miss
    /// rather than a stale hit.
    #[inline]
    pub fn tags_alive(&self, current_generation: impl Fn(u32) -> Option<u64>) -> bool {
        self.tags.iter().all(|t| {
            current_generation(t.tag_id.get()).is_some_and(|current| current == t.generation.get())
        })
    }

    /// The full liveness check. All three conditions read RAM-resident state
    /// only — no additional disk I/O.
    #[inline]
    pub fn is_alive(
        &self,
        now_ms: u64,
        current_epoch: u32,
        current_generation: impl Fn(u32) -> Option<u64>,
    ) -> bool {
        !self.is_expired(now_ms)
            && !self.is_flushed(current_epoch)
            && self.tags_alive(current_generation)
    }

    #[inline]
    pub fn cas(&self) -> u64 {
        self.header.cas.get()
    }

    #[inline]
    pub fn mc_flags(&self) -> u32 {
        self.header.mc_flags.get()
    }

    #[inline]
    pub fn expires_at_ms(&self) -> u64 {
        self.header.expires_at_ms.get()
    }
}

/// Exact encoded size of a record, for pre-sizing the write buffer.
#[inline]
pub const fn record_len(tag_count: usize, value_len: usize) -> usize {
    RECORD_HEADER_LEN + tag_count * TAG_REF_LEN + value_len
}

/// Serialises a record into `out`, which is cleared first.
pub fn encode_record(
    out: &mut Vec<u8>,
    meta: RecordMeta,
    tags: &[TagRef],
    value: &[u8],
) -> Result<()> {
    if tags.len() > MAX_TAGS {
        return Err(CoreError::TooManyTags {
            count: tags.len(),
            max: MAX_TAGS,
        });
    }

    let header = RecordHeader {
        version: RECORD_VERSION,
        tag_count: tags.len() as u8,
        reserved: U16::ZERO,
        epoch: U32::new(meta.epoch),
        mc_flags: U32::new(meta.mc_flags),
        expires_at_ms: U64::new(meta.expires_at_ms),
        cas: U64::new(meta.cas),
    };

    out.clear();
    out.reserve(record_len(tags.len(), value.len()));
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(tags.as_bytes());
    out.extend_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Guard the wire format against accidental change: a padding byte creeping
    // in here would silently corrupt every database in existence.
    #[test]
    fn layout_is_exactly_as_documented() {
        assert_eq!(size_of::<RecordHeader>(), RECORD_HEADER_LEN);
        assert_eq!(size_of::<TagRef>(), TAG_REF_LEN);
        assert_eq!(align_of::<RecordHeader>(), 1);
        assert_eq!(align_of::<TagRef>(), 1);
    }

    fn roundtrip(meta: RecordMeta, tags: &[TagRef], value: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_record(&mut buf, meta, tags, value).unwrap();
        assert_eq!(buf.len(), record_len(tags.len(), value.len()));
        buf
    }

    #[test]
    fn roundtrips_without_tags() {
        let meta = RecordMeta {
            epoch: 7,
            mc_flags: 0xdead_beef,
            expires_at_ms: 1_700_000_000_000,
            cas: 42,
        };
        let buf = roundtrip(meta, &[], b"hello world");
        let rec = RecordRef::parse(&buf).unwrap();

        assert_eq!(rec.value, b"hello world");
        assert_eq!(rec.cas(), 42);
        assert_eq!(rec.mc_flags(), 0xdead_beef);
        assert_eq!(rec.expires_at_ms(), 1_700_000_000_000);
        assert!(rec.tags.is_empty());
    }

    #[test]
    fn roundtrips_with_tags() {
        let tags = [TagRef::new(1, 100), TagRef::new(9, u64::MAX)];
        let buf = roundtrip(RecordMeta::default(), &tags, b"v");
        let rec = RecordRef::parse(&buf).unwrap();

        assert_eq!(rec.tags.len(), 2);
        assert_eq!(rec.tags[0].tag_id.get(), 1);
        assert_eq!(rec.tags[0].generation.get(), 100);
        assert_eq!(rec.tags[1].generation.get(), u64::MAX);
        assert_eq!(rec.value, b"v");
    }

    #[test]
    fn empty_value_is_valid() {
        let buf = roundtrip(RecordMeta::default(), &[], b"");
        assert_eq!(RecordRef::parse(&buf).unwrap().value, b"");
    }

    #[test]
    fn expiry_boundary_is_inclusive() {
        let meta = RecordMeta {
            expires_at_ms: 1000,
            ..Default::default()
        };
        let buf = roundtrip(meta, &[], b"v");
        let rec = RecordRef::parse(&buf).unwrap();

        assert!(!rec.is_expired(999));
        assert!(rec.is_expired(1000)); // expiry is inclusive: at the instant, it is gone
        assert!(rec.is_expired(1001));
    }

    #[test]
    fn zero_expiry_never_expires() {
        let buf = roundtrip(RecordMeta::default(), &[], b"v");
        let rec = RecordRef::parse(&buf).unwrap();
        assert!(!rec.is_expired(u64::MAX));
    }

    #[test]
    fn tag_generation_mismatch_kills_the_record() {
        let tags = [TagRef::new(3, 5)];
        let buf = roundtrip(RecordMeta::default(), &tags, b"v");
        let rec = RecordRef::parse(&buf).unwrap();

        assert!(rec.tags_alive(|_| Some(5)));
        assert!(
            !rec.tags_alive(|_| Some(6)),
            "bumped generation must invalidate"
        );
        assert!(!rec.tags_alive(|_| None), "unknown tag must fail closed");
    }

    #[test]
    fn flush_epoch_mismatch_kills_the_record() {
        let meta = RecordMeta {
            epoch: 1,
            ..Default::default()
        };
        let buf = roundtrip(meta, &[], b"v");
        let rec = RecordRef::parse(&buf).unwrap();

        assert!(!rec.is_flushed(1));
        assert!(rec.is_flushed(2));
        assert!(!rec.is_alive(0, 2, |_| Some(0)));
    }

    #[test]
    fn rejects_truncated_and_wrong_version() {
        let buf = roundtrip(RecordMeta::default(), &[TagRef::new(1, 1)], b"value");

        // Truncated below the header.
        assert!(matches!(
            RecordRef::parse(&buf[..10]).unwrap_err(),
            CoreError::MalformedRecord(_)
        ));
        // Header present but the tag table is cut short.
        assert!(matches!(
            RecordRef::parse(&buf[..RECORD_HEADER_LEN + 4]).unwrap_err(),
            CoreError::MalformedRecord(_)
        ));
        // Unknown version.
        let mut bad = buf.clone();
        bad[0] = 99;
        assert!(matches!(
            RecordRef::parse(&bad).unwrap_err(),
            CoreError::RecordVersion { found: 99, .. }
        ));
    }

    #[test]
    fn rejects_too_many_tags() {
        let tags = vec![TagRef::new(1, 1); MAX_TAGS + 1];
        let mut buf = Vec::new();
        assert!(matches!(
            encode_record(&mut buf, RecordMeta::default(), &tags, b"v").unwrap_err(),
            CoreError::TooManyTags { .. }
        ));
    }
}
