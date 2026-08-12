//! Property tests for both wire parsers.
//!
//! These two decoders are the only code in the system that reads bytes from
//! anonymous strangers, so the security budget goes here. The fuzz targets in
//! `fuzz/` cover the same functions with coverage-guided input; these run on
//! every `cargo test`, on every platform, and state the invariants explicitly
//! rather than only asserting the absence of a crash.
//!
//! Three properties matter more than the round-trips:
//!
//! 1. **Decoding is total.** No input panics.
//! 2. **Decoding always makes progress.** A recoverable error must consume a
//!    non-zero, in-bounds number of bytes — the connection loop advances by that
//!    count and continues, so a zero would spin a core forever on one bad byte.
//! 3. **A frame boundary is agreed on.** `peek_frame_len` decides how many bytes
//!    to hand to the executor; if it disagreed with the decoder, requests would
//!    be silently misattributed.

use proptest::prelude::*;
use vash_proto::memcached::{self, Outcome, ProtocolError};
use vash_proto::vcp::{
    self, DecodeError, Decoded, FrameLen, Opcode, encode_request, peek_frame_len,
};

/// Bytes that look enough like a frame to reach the interesting code paths.
///
/// Purely random bytes almost always fail at the opcode, so a generator that
/// starts from a real header exercises the body decoders — which is where the
/// arithmetic worth breaking lives.
fn plausible_frame() -> impl Strategy<Value = Vec<u8>> {
    let opcodes = prop::sample::select(vec![
        Opcode::Hello,
        Opcode::Ping,
        Opcode::Get,
        Opcode::Set,
        Opcode::Delete,
        Opcode::Touch,
        Opcode::GetMany,
        Opcode::SetMany,
        Opcode::DeleteMany,
        Opcode::DeleteByTag,
        Opcode::Flush,
        Opcode::TagSync,
        Opcode::Cluster,
    ]);
    (
        opcodes,
        any::<u32>(),
        proptest::collection::vec(any::<u8>(), 0..192),
    )
        .prop_map(|(opcode, request_id, body)| {
            let mut out = Vec::new();
            encode_request(&mut out, opcode, request_id, &body);
            out
        })
}

proptest! {
    #[test]
    fn vcp_decoding_arbitrary_bytes_never_panics(raw in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = vcp::decode(&raw);
        let _ = peek_frame_len(&raw);
    }

    #[test]
    fn vcp_decoding_a_plausible_frame_never_panics(raw in plausible_frame()) {
        let _ = vcp::decode(&raw);
    }

    /// A rejected frame must be skippable. The connection answers with an error
    /// and carries on from `consumed`; out of bounds would panic the slice, and
    /// zero would re-read the same bytes forever.
    #[test]
    fn a_rejected_vcp_frame_reports_a_length_that_makes_progress(raw in plausible_frame()) {
        if let Err(DecodeError::Body { consumed, .. }) = vcp::decode(&raw) {
            prop_assert!(consumed > 0, "a zero-length skip would loop forever");
            prop_assert!(consumed <= raw.len(), "skipping past the buffer would panic");
        }
    }

    /// `peek_frame_len` is what splits a frame off the read buffer before the
    /// body is ever looked at. The two must not disagree about where the frame
    /// ends, or a request would be executed against another one's bytes.
    #[test]
    fn peeking_and_decoding_agree_on_the_frame_boundary(raw in plausible_frame()) {
        match (peek_frame_len(&raw), vcp::decode(&raw)) {
            (FrameLen::Complete(peeked), Ok(Decoded::Request { consumed, .. })) => {
                prop_assert_eq!(peeked, consumed);
            }
            (FrameLen::Complete(peeked), Err(DecodeError::Body { consumed, .. })) => {
                prop_assert_eq!(peeked, consumed);
            }
            // Everything else is a refusal or a short read, where there is no
            // boundary to agree on.
            _ => prop_assert!(true),
        }
    }

    /// Pipelining: several frames arrive in one read, and each must be decoded
    /// from exactly where the last one ended.
    #[test]
    fn pipelined_frames_decode_one_after_another(
        keys in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 1..=64),
            1..8,
        ),
    ) {
        let mut buf = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            encode_request(&mut buf, Opcode::Get, index as u32, key);
        }

        let mut rest = buf.as_slice();
        for (index, key) in keys.iter().enumerate() {
            let Ok(Decoded::Request { request, consumed }) = vcp::decode(rest) else {
                prop_assert!(false, "frame {index} did not decode");
                unreachable!()
            };
            prop_assert_eq!(request.request_id, index as u32);
            match request.command {
                vash_core::Command::Get { key: decoded } => {
                    prop_assert_eq!(decoded.as_bytes(), key.as_slice());
                }
                other => prop_assert!(false, "expected a get, got {:?}", other),
            }
            rest = &rest[consumed..];
        }
        prop_assert!(rest.is_empty(), "every byte should have been consumed");
    }

    /// A `SET` body carries a key, a value and a tag table with their own length
    /// fields; the round-trip is what proves those fields agree with the reader.
    #[test]
    fn a_set_body_survives_encoding_and_decoding(
        key in proptest::collection::vec(any::<u8>(), 1..=511),
        value in proptest::collection::vec(any::<u8>(), 0..1024),
        ttl_secs in any::<u32>(),
        tags in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 1..=255),
            0..8,
        ),
    ) {
        let tag_refs: Vec<&[u8]> = tags.iter().map(|t| t.as_slice()).collect();
        let mut body = Vec::new();
        vcp::encode_set_body(&mut body, &key, &value, ttl_secs, &tag_refs);

        let mut frame = Vec::new();
        encode_request(&mut frame, Opcode::Set, 1, &body);

        let Ok(Decoded::Request { request, .. }) = vcp::decode(&frame) else {
            prop_assert!(false, "a set we encoded ourselves must decode");
            unreachable!()
        };
        let vash_core::Command::Set(set) = request.command else {
            prop_assert!(false, "expected a set");
            unreachable!()
        };
        prop_assert_eq!(set.key.as_bytes(), key.as_slice());
        prop_assert_eq!(set.value, value.as_slice());
        prop_assert_eq!(set.ttl, vash_core::TtlChange::Set(ttl_secs));
        prop_assert_eq!(set.tags, tag_refs);
    }

    /// The cluster codec, which a peer feeds directly.
    #[test]
    fn a_tag_sync_body_survives_encoding_and_decoding(
        full in any::<bool>(),
        entries in proptest::collection::vec(
            (proptest::collection::vec(any::<u8>(), 1..=255), any::<u64>()),
            0..64,
        ),
    ) {
        let offered: Vec<(&[u8], u64)> = entries
            .iter()
            .map(|(name, generation)| (name.as_slice(), *generation))
            .collect();

        let mut body = Vec::new();
        vcp::encode_tag_sync_body(&mut body, full, offered.iter().copied());

        let (decoded_full, decoded) = vcp::decode_tag_sync(&body).expect("what we encoded");
        prop_assert_eq!(decoded_full, full);
        prop_assert_eq!(decoded, offered);
    }

    // ---- memcached ---------------------------------------------------------

    #[test]
    fn memcached_parsing_arbitrary_bytes_never_panics(
        raw in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let _ = memcached::parse(&raw);
    }

    /// Same again, but shaped like a command line so the token parsing is
    /// reached rather than bailing at the verb.
    #[test]
    fn memcached_parsing_a_plausible_line_never_panics(
        verb in prop::sample::select(vec![
            "get", "gets", "set", "add", "replace", "append", "prepend", "cas", "delete",
            "touch", "gat", "gats", "incr", "decr", "flush_all", "stats", "version",
            "verbosity", "quit", "delete_by_tag", "mg", "ms", "md", "mn", "ma", "me", "mdt",
        ]),
        args in proptest::collection::vec("[!-~]{0,24}", 0..6),
        trailing in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut line = verb.to_string();
        for arg in &args {
            line.push(' ');
            line.push_str(arg);
        }
        let mut raw = line.into_bytes();
        raw.extend_from_slice(b"\r\n");
        raw.extend_from_slice(&trailing);

        let _ = memcached::parse(&raw);
    }

    /// The invariant the connection loop depends on. `drain_memcached` advances
    /// the read buffer by `consumed` and loops; a zero would spin a core on one
    /// malformed byte, and an over-long count would discard a following command.
    #[test]
    fn memcached_parsing_always_makes_bounded_progress(
        raw in proptest::collection::vec(
            prop::sample::select(vec![b'a', b'g', b'e', b't', b's', b'm', b' ', b'0', b'\r', b'\n', 0xff]),
            1..128,
        ),
    ) {
        match memcached::parse(&raw) {
            Ok(Outcome::Command(parsed)) => {
                prop_assert!(parsed.consumed > 0, "a zero-length command would loop forever");
                prop_assert!(parsed.consumed <= raw.len());
            }
            Err(ProtocolError::Recoverable { consumed, .. }) => {
                prop_assert!(consumed > 0, "a zero-length skip would loop forever");
                prop_assert!(consumed <= raw.len());
            }
            // Incomplete waits for more bytes; Fatal closes the connection.
            // Neither advances the buffer, so neither can spin.
            _ => prop_assert!(true),
        }
    }

    /// A storage command is length-delimited, not line-delimited: the value may
    /// contain CRLF. Framing has to come from the `<bytes>` token, or a value
    /// with a newline in it would be parsed as commands.
    #[test]
    fn a_storage_command_is_framed_by_its_length_not_its_content(
        value in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let mut raw = format!("set k 0 0 {}\r\n", value.len()).into_bytes();
        raw.extend_from_slice(&value);
        raw.extend_from_slice(b"\r\n");
        // A following command that must not be swallowed.
        let boundary = raw.len();
        raw.extend_from_slice(b"version\r\n");

        let Ok(Outcome::Command(parsed)) = memcached::parse(&raw) else {
            prop_assert!(false, "a well-formed set must parse");
            unreachable!()
        };
        prop_assert_eq!(parsed.consumed, boundary, "the value's own CRLFs must not frame it");

        let vash_core::Command::Set(set) = parsed.command else {
            prop_assert!(false, "expected a set");
            unreachable!()
        };
        prop_assert_eq!(set.value, value.as_slice());
    }
}
