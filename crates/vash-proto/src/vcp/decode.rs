use vash_core::{Command, CoreError, Key, Set};
use zerocopy::FromBytes;
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{Immutable, IntoBytes, KnownLayout, Unaligned};

use super::frame::{FrameHeader, HEADER_LEN, MAX_BODY_LEN, Opcode, Status};

/// A fully decoded request, borrowing from the connection's read buffer.
#[derive(Debug)]
pub struct Request<'a> {
    pub request_id: u32,
    pub opcode: Opcode,
    pub no_reply: bool,
    pub command: Command<'a>,
}

#[derive(Debug)]
pub enum Decoded<'a> {
    /// Not enough bytes yet. `needed` is the total frame length once known, or
    /// just the header length while the header itself is incomplete, so the
    /// caller can size its read exactly instead of guessing.
    Incomplete { needed: usize },
    Request {
        request: Request<'a>,
        consumed: usize,
    },
}

/// A decode failure.
///
/// The split matters for robustness: once the header is readable the frame
/// boundary is known, so a bad *body* lets the server answer with an error and
/// carry on with the connection. A bad *header* means the stream is
/// unintelligible and the connection has to go.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    Fatal {
        detail: &'static str,
    },
    Body {
        request_id: u32,
        opcode: u8,
        consumed: usize,
        status: Status,
        detail: &'static str,
    },
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
pub struct SetBodyHeader {
    pub ttl_secs: U32,
    pub key_len: U16,
    pub tag_count: u8,
    pub reserved: u8,
    pub value_len: U32,
}

pub const SET_BODY_HEADER_LEN: usize = 12;
pub const HELLO_BODY_LEN: usize = 4;
/// `ttl_secs u32` ahead of the key in a `TOUCH` body.
pub const TOUCH_PREFIX_LEN: usize = 4;
/// `kind u8 | reserved u8 * 3 | count u32` ahead of a `TAG_SYNC` entry list.
pub const TAG_SYNC_HEADER_LEN: usize = 8;

/// A bounds-checked forward reader over a frame body.
///
/// Every accessor returns `None` rather than panicking on a short read, so
/// truncated input from an unauthenticated client is an ordinary error path.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn rest(&mut self) -> &'a [u8] {
        let slice = &self.buf[self.pos..];
        self.pos = self.buf.len();
        slice
    }
}

/// Reads a batch item count, rejecting anything past the limit **before** it is
/// used to size an allocation.
fn decode_count(c: &mut Cursor<'_>) -> Result<usize, (Status, &'static str)> {
    let count = c
        .u32()
        .ok_or((Status::BadRequest, "batch is missing its item count"))? as usize;
    if count > vash_core::MAX_BATCH_ITEMS {
        return Err((Status::BadRequest, "batch exceeds the maximum item count"));
    }
    Ok(count)
}

fn decode_key_list<'a>(c: &mut Cursor<'a>) -> Result<Vec<Key<'a>>, (Status, &'static str)> {
    let count = decode_count(c)?;
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        let len = c
            .u16()
            .ok_or((Status::BadRequest, "truncated key length"))? as usize;
        let bytes = c.take(len).ok_or((Status::BadRequest, "truncated key"))?;
        keys.push(decode_key(bytes)?);
    }
    Ok(keys)
}

/// A decoded `TAG_SYNC` body: whether the sender listed its whole table, and
/// the name/generation pairs it offered.
pub type TagSyncBody<'a> = (bool, Vec<(&'a [u8], u64)>);

/// Reads a `TAG_SYNC` body: a kind byte, then length-prefixed names with the
/// generation the sender holds for each.
///
/// Used for requests and responses alike, and by the client, so there is one
/// definition of the layout rather than several that can drift.
pub fn decode_tag_sync(body: &[u8]) -> Result<TagSyncBody<'_>, (Status, &'static str)> {
    let mut c = Cursor::new(body);
    let header = c.take(TAG_SYNC_HEADER_LEN).ok_or((
        Status::BadRequest,
        "tag sync body is shorter than its header",
    ))?;
    let full = match header[0] {
        0 => false,
        1 => true,
        // Rejected rather than defaulted: a kind this build does not know might
        // mean the reply is expected to carry something it will not.
        _ => return Err((Status::BadRequest, "unknown tag sync kind")),
    };

    // Bounded before it is trusted to size an allocation.
    let count = u32::from_le_bytes(header[4..8].try_into().expect("8-byte header")) as usize;
    if count > vash_core::MAX_TAG_SYNC_ENTRIES {
        return Err((
            Status::BadRequest,
            "tag sync exceeds the maximum entry count",
        ));
    }

    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let generation = c
            .u64()
            .ok_or((Status::BadRequest, "truncated tag generation"))?;
        let len = c
            .u16()
            .ok_or((Status::BadRequest, "truncated tag name length"))? as usize;
        let name = c
            .take(len)
            .ok_or((Status::BadRequest, "truncated tag name"))?;
        if name.is_empty() {
            return Err((Status::BadRequest, "tag name is empty"));
        }
        if name.len() > vash_core::MAX_TAG_LEN {
            return Err((Status::BadRequest, "tag name is too long"));
        }
        entries.push((name, generation));
    }

    Ok((full, entries))
}

/// Result of looking at just enough of `buf` to find the frame boundary.
#[derive(Debug, PartialEq, Eq)]
pub enum FrameLen {
    Incomplete {
        needed: usize,
    },
    Complete(usize),
    /// `body_len` is beyond [`MAX_BODY_LEN`]. The connection must be closed:
    /// without a trustworthy length there is no way to resynchronise.
    TooLarge,
}

/// Reads only the header to determine the total frame length.
///
/// This lets the connection split a complete frame off its read buffer and hand
/// ownership to the thread that will decode and execute it, so the borrowed
/// key and value slices never have to cross a task boundary.
pub fn peek_frame_len(buf: &[u8]) -> FrameLen {
    let Ok((header, _)) = FrameHeader::ref_from_prefix(buf) else {
        return FrameLen::Incomplete { needed: HEADER_LEN };
    };
    let body_len = header.body_len.get();
    if body_len > MAX_BODY_LEN {
        return FrameLen::TooLarge;
    }
    let total = HEADER_LEN + body_len as usize;
    if buf.len() < total {
        FrameLen::Incomplete { needed: total }
    } else {
        FrameLen::Complete(total)
    }
}

/// Attempts to decode one frame from the front of `buf`.
pub fn decode(buf: &[u8]) -> Result<Decoded<'_>, DecodeError> {
    let Ok((header, rest)) = FrameHeader::ref_from_prefix(buf) else {
        return Ok(Decoded::Incomplete { needed: HEADER_LEN });
    };

    let body_len = header.body_len.get();
    let request_id = header.request_id.get();

    // Bound the length *before* trusting it for anything, so a hostile
    // body_len cannot drive an allocation or an integer overflow.
    if body_len > MAX_BODY_LEN {
        return Err(DecodeError::Fatal {
            detail: "body_len exceeds the maximum frame size",
        });
    }
    let body_len = body_len as usize;

    if rest.len() < body_len {
        return Ok(Decoded::Incomplete {
            needed: HEADER_LEN + body_len,
        });
    }

    let body = &rest[..body_len];
    let consumed = HEADER_LEN + body_len;

    let Some(opcode) = Opcode::from_u8(header.opcode) else {
        return Err(DecodeError::Body {
            request_id,
            opcode: header.opcode,
            consumed,
            status: Status::Unsupported,
            detail: "unknown opcode",
        });
    };

    let fail = |status, detail| DecodeError::Body {
        request_id,
        opcode: header.opcode,
        consumed,
        status,
        detail,
    };

    let command = match opcode {
        Opcode::Ping => Command::Ping,

        Opcode::Hello => {
            if body.len() < HELLO_BODY_LEN {
                return Err(fail(Status::BadRequest, "hello body is too short"));
            }
            Command::Hello {
                protocol_version: u16::from_le_bytes([body[0], body[1]]),
            }
        }

        // The whole body is the key: no inner length prefix to parse, and the
        // key is a direct subslice of the read buffer.
        Opcode::Get => Command::Get {
            key: decode_key(body).map_err(|(s, d)| fail(s, d))?,
        },
        Opcode::Delete => Command::Delete {
            key: decode_key(body).map_err(|(s, d)| fail(s, d))?,
        },

        Opcode::Set => {
            let mut cursor = Cursor::new(body);
            Command::Set(decode_set(&mut cursor).map_err(|(s, d)| fail(s, d))?)
        }

        Opcode::Touch => {
            let mut cursor = Cursor::new(body);
            let ttl_secs = cursor
                .u32()
                .ok_or_else(|| fail(Status::BadRequest, "touch body is too short"))?;
            Command::Touch {
                key: decode_key(cursor.rest()).map_err(|(s, d)| fail(s, d))?,
                ttl_secs,
            }
        }

        Opcode::GetMany => {
            let mut cursor = Cursor::new(body);
            Command::GetMany(decode_key_list(&mut cursor).map_err(|(s, d)| fail(s, d))?)
        }

        Opcode::DeleteMany => {
            let mut cursor = Cursor::new(body);
            Command::DeleteMany(decode_key_list(&mut cursor).map_err(|(s, d)| fail(s, d))?)
        }

        Opcode::SetMany => {
            let mut cursor = Cursor::new(body);
            let count = decode_count(&mut cursor).map_err(|(s, d)| fail(s, d))?;
            let mut sets = Vec::with_capacity(count);
            for _ in 0..count {
                sets.push(decode_set(&mut cursor).map_err(|(s, d)| fail(s, d))?);
            }
            Command::SetMany(sets)
        }

        // The whole body is the tag name.
        Opcode::DeleteByTag => {
            if body.is_empty() {
                return Err(fail(Status::BadRequest, "tag name is empty"));
            }
            if body.len() > vash_core::MAX_TAG_LEN {
                return Err(fail(Status::TooLarge, "tag name is too long"));
            }
            Command::DeleteByTag { tag: body }
        }

        Opcode::Flush => Command::Flush,

        Opcode::TagSync => {
            let (full, entries) = decode_tag_sync(body).map_err(|(s, d)| fail(s, d))?;
            Command::TagSync { full, entries }
        }

        Opcode::Cluster => Command::Cluster,

        Opcode::Auth | Opcode::Stats => {
            return Err(fail(Status::Unsupported, "opcode not implemented yet"));
        }
    };

    Ok(Decoded::Request {
        request: Request {
            request_id,
            opcode,
            no_reply: header.no_reply(),
            command,
        },
        consumed,
    })
}

fn decode_key(bytes: &[u8]) -> Result<Key<'_>, (Status, &'static str)> {
    Key::new(bytes).map_err(|e| match e {
        CoreError::EmptyKey => (Status::BadRequest, "key is empty"),
        CoreError::KeyTooLong { .. } => (Status::TooLarge, "key exceeds the maximum length"),
        _ => (Status::BadRequest, "invalid key"),
    })
}

fn decode_set<'a>(c: &mut Cursor<'a>) -> Result<Set<'a>, (Status, &'static str)> {
    let raw = c
        .take(SET_BODY_HEADER_LEN)
        .ok_or((Status::BadRequest, "set body is shorter than its header"))?;
    let header = SetBodyHeader::ref_from_bytes(raw)
        .map_err(|_| (Status::BadRequest, "malformed set header"))?;

    let key = decode_key(c.take(header.key_len.get() as usize).ok_or((
        Status::BadRequest,
        "set body is shorter than its declared key",
    ))?)?;
    let value = c.take(header.value_len.get() as usize).ok_or((
        Status::BadRequest,
        "set body is shorter than its declared value",
    ))?;

    let tag_count = header.tag_count as usize;
    if tag_count > vash_core::MAX_TAGS {
        return Err((Status::BadRequest, "too many tags"));
    }

    let mut tags = Vec::with_capacity(tag_count);
    for _ in 0..tag_count {
        let len = c
            .u16()
            .ok_or((Status::BadRequest, "truncated tag length"))? as usize;
        let name = c
            .take(len)
            .ok_or((Status::BadRequest, "truncated tag name"))?;
        if name.is_empty() {
            return Err((Status::BadRequest, "tag name is empty"));
        }
        if name.len() > vash_core::MAX_TAG_LEN {
            return Err((Status::BadRequest, "tag name is too long"));
        }
        tags.push(name);
    }

    Ok(Set {
        key,
        value,
        ttl_secs: header.ttl_secs.get(),
        mc_flags: 0,
        tags,
        // VCP has no conditional writes yet; the guarded modes reach the store
        // only through the memcached adapter.
        mode: vash_core::SetMode::Set,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vcp::encode;

    fn frame(opcode: Opcode, request_id: u32, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode::encode_request(&mut out, opcode, request_id, body);
        out
    }

    #[test]
    fn layout_is_exactly_as_documented() {
        assert_eq!(size_of::<SetBodyHeader>(), SET_BODY_HEADER_LEN);
        assert_eq!(align_of::<SetBodyHeader>(), 1);
    }

    #[test]
    fn decodes_get() {
        let buf = frame(Opcode::Get, 7, b"mykey");
        let Ok(Decoded::Request { request, consumed }) = decode(&buf) else {
            panic!("expected a request");
        };
        assert_eq!(consumed, buf.len());
        assert_eq!(request.request_id, 7);
        assert!(matches!(request.command, Command::Get { key } if key.as_bytes() == b"mykey"));
    }

    #[test]
    fn decodes_set_with_tags() {
        let mut body = Vec::new();
        encode::encode_set_body(&mut body, b"k", b"value", 300, &[b"a".as_slice(), b"bb"]);
        let buf = frame(Opcode::Set, 1, &body);

        let Ok(Decoded::Request { request, .. }) = decode(&buf) else {
            panic!("expected a request");
        };
        let Command::Set(set) = request.command else {
            panic!("expected a set");
        };
        assert_eq!(set.key.as_bytes(), b"k");
        assert_eq!(set.value, b"value");
        assert_eq!(set.ttl_secs, 300);
        assert_eq!(set.tags, vec![b"a".as_slice(), b"bb"]);
    }

    #[test]
    fn reports_incomplete_with_the_exact_length_needed() {
        let buf = frame(Opcode::Get, 1, b"mykey");

        // Partial header.
        assert!(matches!(
            decode(&buf[..5]),
            Ok(Decoded::Incomplete { needed: HEADER_LEN })
        ));
        // Full header, partial body.
        assert!(matches!(
            decode(&buf[..HEADER_LEN + 2]),
            Ok(Decoded::Incomplete { needed }) if needed == buf.len()
        ));
    }

    #[test]
    fn decodes_frames_back_to_back() {
        let mut buf = frame(Opcode::Get, 1, b"a");
        buf.extend_from_slice(&frame(Opcode::Get, 2, b"bb"));

        let Ok(Decoded::Request { request, consumed }) = decode(&buf) else {
            panic!()
        };
        assert_eq!(request.request_id, 1);

        let Ok(Decoded::Request { request, .. }) = decode(&buf[consumed..]) else {
            panic!()
        };
        assert_eq!(request.request_id, 2);
    }

    #[test]
    fn oversized_body_len_is_fatal_and_allocates_nothing() {
        let mut buf = frame(Opcode::Get, 1, b"");
        buf[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(decode(&buf), Err(DecodeError::Fatal { .. })));
    }

    #[test]
    fn bad_body_is_recoverable_and_reports_the_frame_length() {
        // key_len claims more bytes than the body holds.
        let mut body = vec![0u8; SET_BODY_HEADER_LEN];
        body[4..6].copy_from_slice(&999u16.to_le_bytes());
        let buf = frame(Opcode::Set, 42, &body);

        let Err(DecodeError::Body {
            request_id,
            consumed,
            status,
            ..
        }) = decode(&buf)
        else {
            panic!("expected a recoverable body error");
        };
        assert_eq!(request_id, 42);
        assert_eq!(consumed, buf.len(), "caller must be able to skip the frame");
        assert_eq!(status, Status::BadRequest);
    }

    #[test]
    fn peek_finds_the_frame_boundary() {
        let buf = frame(Opcode::Get, 1, b"mykey");

        assert_eq!(
            peek_frame_len(&[]),
            FrameLen::Incomplete { needed: HEADER_LEN }
        );
        assert_eq!(
            peek_frame_len(&buf[..HEADER_LEN + 1]),
            FrameLen::Incomplete { needed: buf.len() }
        );
        assert_eq!(peek_frame_len(&buf), FrameLen::Complete(buf.len()));

        // Trailing bytes from a pipelined next frame must not extend the length.
        let mut two = buf.clone();
        two.extend_from_slice(&frame(Opcode::Get, 2, b"x"));
        assert_eq!(peek_frame_len(&two), FrameLen::Complete(buf.len()));
    }

    #[test]
    fn peek_rejects_an_oversized_body_len() {
        let mut buf = frame(Opcode::Get, 1, b"");
        buf[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(peek_frame_len(&buf), FrameLen::TooLarge);
    }

    #[test]
    fn unknown_opcode_is_recoverable() {
        let mut buf = frame(Opcode::Get, 3, b"k");
        buf[0] = 0xfe;
        assert!(matches!(
            decode(&buf),
            Err(DecodeError::Body {
                status: Status::Unsupported,
                request_id: 3,
                ..
            })
        ));
    }

    #[test]
    fn empty_key_is_rejected() {
        let buf = frame(Opcode::Get, 1, b"");
        assert!(matches!(
            decode(&buf),
            Err(DecodeError::Body {
                status: Status::BadRequest,
                ..
            })
        ));
    }

    #[test]
    fn oversized_key_is_rejected_as_too_large() {
        let buf = frame(Opcode::Get, 1, &[b'k'; vash_core::MAX_KEY_LEN + 1]);
        assert!(matches!(
            decode(&buf),
            Err(DecodeError::Body {
                status: Status::TooLarge,
                ..
            })
        ));
    }

    #[test]
    fn tag_sync_roundtrips_both_kinds() {
        for full in [false, true] {
            let entries: Vec<(&[u8], u64)> = vec![(b"news", 3), (b"sport", u64::MAX)];
            let mut body = Vec::new();
            encode::encode_tag_sync_body(&mut body, full, entries.iter().copied());

            let buf = frame(Opcode::TagSync, 11, &body);
            let Ok(Decoded::Request { request, .. }) = decode(&buf) else {
                panic!("expected a request");
            };
            let Command::TagSync {
                full: decoded_full,
                entries: decoded,
            } = request.command
            else {
                panic!("expected a tag sync");
            };
            assert_eq!(decoded_full, full);
            assert_eq!(decoded, entries);
        }
    }

    #[test]
    fn an_empty_tag_sync_is_valid() {
        // What a node with nothing invalidated yet offers: a legitimate
        // message, not a malformed one.
        let mut body = Vec::new();
        encode::encode_tag_sync_body(&mut body, true, std::iter::empty());
        assert_eq!(decode_tag_sync(&body), Ok((true, Vec::new())));
    }

    #[test]
    fn an_oversized_tag_sync_count_allocates_nothing() {
        // The count is attacker-controlled and is used to size a Vec, so it has
        // to be bounded before it is trusted.
        let mut body = vec![0u8; TAG_SYNC_HEADER_LEN];
        body[4..8].copy_from_slice(&u32::MAX.to_le_bytes());

        let buf = frame(Opcode::TagSync, 1, &body);
        assert!(matches!(
            decode(&buf),
            Err(DecodeError::Body {
                status: Status::BadRequest,
                ..
            })
        ));
    }

    #[test]
    fn a_truncated_tag_sync_entry_is_rejected() {
        let entries: Vec<(&[u8], u64)> = vec![(b"news", 3)];
        let mut body = Vec::new();
        encode::encode_tag_sync_body(&mut body, false, entries.iter().copied());
        body.truncate(body.len() - 1);
        assert!(decode_tag_sync(&body).is_err());
    }

    #[test]
    fn an_unknown_tag_sync_kind_is_rejected_rather_than_defaulted() {
        // Guessing would risk answering a partial push as though it were a full
        // digest, which is a different and much larger reply.
        let mut body = vec![0u8; TAG_SYNC_HEADER_LEN];
        body[0] = 7;
        assert!(decode_tag_sync(&body).is_err());
    }

    #[test]
    fn truncated_tag_table_is_rejected() {
        let mut body = Vec::new();
        encode::encode_set_body(&mut body, b"k", b"v", 0, &[b"tag".as_slice()]);
        body.truncate(body.len() - 1); // clip the last byte of the tag name
        let buf = frame(Opcode::Set, 1, &body);
        assert!(matches!(decode(&buf), Err(DecodeError::Body { .. })));
    }
}
