//! Property tests for the record format and the liveness rules.
//!
//! The record header is read on every single request, from bytes that came off
//! a disk this process may not have written — an older build, a truncated file,
//! a corrupted page. Example-based tests cover the shapes someone thought of;
//! these cover the ones nobody did.

use cache_core::record::{RECORD_HEADER_LEN, RecordHeader, TAG_REF_LEN};
use cache_core::{
    MAX_TAGS, NEVER, RecordMeta, RecordRef, TagRef, encode_record, patch_cas, record_len,
};
use proptest::prelude::*;

fn tags() -> impl Strategy<Value = Vec<TagRef>> {
    proptest::collection::vec(
        (any::<u32>(), any::<u64>()).prop_map(|(id, generation)| TagRef::new(id, generation)),
        0..=MAX_TAGS,
    )
}

fn meta() -> impl Strategy<Value = RecordMeta> {
    (any::<u32>(), any::<u32>(), any::<u64>(), any::<u64>()).prop_map(
        |(epoch, mc_flags, expires_at_ms, cas)| RecordMeta {
            epoch,
            mc_flags,
            expires_at_ms,
            cas,
        },
    )
}

proptest! {
    /// Every field must come back exactly as it went in. A record is written
    /// once and read for the rest of its life, so a field that survives
    /// encoding but not decoding is a silent data loss, not a crash.
    #[test]
    fn a_record_survives_encoding_and_decoding(
        meta in meta(),
        tags in tags(),
        value in proptest::collection::vec(any::<u8>(), 0..2048),
    ) {
        let mut buf = Vec::new();
        encode_record(&mut buf, meta, &tags, &value).expect("within the tag limit");

        prop_assert_eq!(buf.len(), record_len(tags.len(), value.len()));

        let record = RecordRef::parse(&buf).expect("what we just encoded");
        prop_assert_eq!(record.header.epoch.get(), meta.epoch);
        prop_assert_eq!(record.mc_flags(), meta.mc_flags);
        prop_assert_eq!(record.expires_at_ms(), meta.expires_at_ms);
        prop_assert_eq!(record.cas(), meta.cas);
        prop_assert_eq!(record.tags, tags.as_slice());
        prop_assert_eq!(record.value, value.as_slice());
    }

    /// The writer stamps the CAS token into an already-encoded record, off the
    /// thread that built it. Hitting a neighbouring field would corrupt the
    /// expiry or the tag table of every write.
    #[test]
    fn patching_the_cas_moves_nothing_else(
        meta in meta(),
        tags in tags(),
        value in proptest::collection::vec(any::<u8>(), 0..256),
        cas in any::<u64>(),
    ) {
        let mut buf = Vec::new();
        encode_record(&mut buf, meta, &tags, &value).expect("within the tag limit");
        patch_cas(&mut buf, cas).expect("long enough");

        let record = RecordRef::parse(&buf).expect("still valid");
        prop_assert_eq!(record.cas(), cas);
        prop_assert_eq!(record.header.epoch.get(), meta.epoch);
        prop_assert_eq!(record.mc_flags(), meta.mc_flags);
        prop_assert_eq!(record.expires_at_ms(), meta.expires_at_ms);
        prop_assert_eq!(record.tags, tags.as_slice());
        prop_assert_eq!(record.value, value.as_slice());
    }

    /// Parsing must be total. These bytes come off disk, and a panic in the
    /// read path takes the process down for every other connection.
    #[test]
    fn parsing_arbitrary_bytes_never_panics(raw in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = RecordRef::parse(&raw);
    }

    /// The same, but starting from a well-formed header so the tag-count and
    /// length arithmetic is actually reached rather than rejected up front.
    #[test]
    fn parsing_a_plausible_but_corrupt_record_never_panics(
        tag_count in any::<u8>(),
        trailing in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let mut buf = Vec::new();
        encode_record(&mut buf, RecordMeta::default(), &[], b"").expect("no tags");
        // Claim a tag table the record does not actually carry.
        buf[1] = tag_count;
        buf.extend_from_slice(&trailing);

        match RecordRef::parse(&buf) {
            Ok(record) => {
                // If it parsed, the claim has to have been backed by real bytes.
                prop_assert_eq!(record.tags.len(), tag_count as usize);
                prop_assert!(
                    RECORD_HEADER_LEN + record.tags.len() * TAG_REF_LEN + record.value.len()
                        == buf.len()
                );
            }
            Err(_) => prop_assert!(true),
        }
    }

    /// Liveness is decided on every read from three RAM-resident facts. Each one
    /// must be able to kill a record on its own — a record is alive only if
    /// nothing objects.
    #[test]
    fn any_single_condition_can_kill_a_record(
        epoch in any::<u32>(),
        expires_at_ms in 1u64..u64::MAX,
        generation in 0u64..u64::MAX,
        now_ms in 0u64..u64::MAX,
    ) {
        let tags = [TagRef::new(7, generation)];
        let meta = RecordMeta { epoch, expires_at_ms, ..RecordMeta::default() };
        let mut buf = Vec::new();
        encode_record(&mut buf, meta, &tags, b"v").expect("one tag");
        let record = RecordRef::parse(&buf).expect("valid");

        let alive = record.is_alive(now_ms, epoch, |_| Some(generation));
        prop_assert_eq!(alive, expires_at_ms > now_ms);

        // A bumped generation invalidates regardless of everything else.
        prop_assert!(!record.is_alive(now_ms, epoch, |_| Some(generation.wrapping_add(1))));
        // So does a flush.
        prop_assert!(!record.is_alive(now_ms, epoch.wrapping_add(1), |_| Some(generation)));
        // So does a tag the registry has never heard of: unknown fails closed,
        // because a rebuilt registry must produce misses, not stale hits.
        prop_assert!(!record.is_alive(now_ms, epoch, |_| None));
    }

    /// `NEVER` is the one expiry that outlives any clock. Getting this wrong
    /// would quietly expire every key written without a TTL.
    #[test]
    fn a_record_without_a_ttl_never_expires(now_ms in any::<u64>()) {
        let meta = RecordMeta { expires_at_ms: NEVER, ..RecordMeta::default() };
        let mut buf = Vec::new();
        encode_record(&mut buf, meta, &[], b"v").expect("no tags");
        prop_assert!(!RecordRef::parse(&buf).expect("valid").is_expired(now_ms));
    }
}

/// The header is cast straight out of the memory map, so its layout is the
/// on-disk format. A padding byte creeping in would silently invalidate every
/// database in existence.
#[test]
fn the_header_layout_is_the_on_disk_format() {
    assert_eq!(size_of::<RecordHeader>(), RECORD_HEADER_LEN);
    assert_eq!(size_of::<TagRef>(), TAG_REF_LEN);
    assert_eq!(align_of::<RecordHeader>(), 1);
    assert_eq!(align_of::<TagRef>(), 1);
}
