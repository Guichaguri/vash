use cache_core::{Reply, ServerInfo};
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32};

use super::decode::SetBodyHeader;
use super::frame::{FrameHeader, Opcode, Status};

/// Appends a request frame.
pub fn encode_request(out: &mut Vec<u8>, opcode: Opcode, request_id: u32, body: &[u8]) {
    let header = FrameHeader::request(opcode, request_id, body.len() as u32);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(body);
}

/// Appends a response frame with an explicit status and body.
pub fn encode_response(
    out: &mut Vec<u8>,
    opcode: Opcode,
    request_id: u32,
    status: Status,
    body: &[u8],
) {
    let header = FrameHeader::response(opcode, request_id, status, body.len() as u32);
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(body);
}

/// Appends an error response. `opcode` is the raw byte from the request, which
/// may not be a known [`Opcode`] â€” it is echoed so the client can correlate.
pub fn encode_error(out: &mut Vec<u8>, raw_opcode: u8, request_id: u32, status: Status) {
    let header = FrameHeader {
        opcode: raw_opcode,
        flags: super::frame::flags::RESPONSE,
        status: U16::new(status as u16),
        request_id: U32::new(request_id),
        body_len: U32::ZERO,
    };
    out.extend_from_slice(header.as_bytes());
}

/// Appends the response frame for a [`Reply`].
pub fn encode_reply(out: &mut Vec<u8>, opcode: Opcode, request_id: u32, reply: &Reply) {
    match reply {
        Reply::Pong => encode_response(out, opcode, request_id, Status::Ok, &[]),
        Reply::Deleted => encode_response(out, opcode, request_id, Status::Ok, &[]),
        Reply::Touched => encode_response(out, opcode, request_id, Status::Ok, &[]),
        Reply::NotFound => encode_response(out, opcode, request_id, Status::NotFound, &[]),

        // An unregistered tag is reported as a miss rather than an error: the
        // caller asked for nothing tagged that way to be served, and nothing is.
        Reply::Invalidated(existed) => {
            let status = if *existed {
                Status::Ok
            } else {
                Status::NotFound
            };
            encode_response(out, opcode, request_id, status, &[]);
        }

        Reply::Flushed(epoch) => {
            encode_response(out, opcode, request_id, Status::Ok, &epoch.to_le_bytes())
        }

        // Batch bodies are built first because the frame header carries their
        // total length. They are sized up front so the payload copy is the only
        // one that happens.
        Reply::Values(values) => {
            let payload: usize = values
                .iter()
                .map(|v| match v {
                    Some(value) => 1 + VALUE_PREFIX_LEN + 4 + value.data.len(),
                    None => 1,
                })
                .sum();

            let mut body = Vec::with_capacity(4 + payload);
            body.extend_from_slice(&(values.len() as u32).to_le_bytes());
            for value in values {
                match value {
                    Some(value) => {
                        body.push(1);
                        body.extend_from_slice(&value.mc_flags.to_le_bytes());
                        body.extend_from_slice(&value.cas.to_le_bytes());
                        body.extend_from_slice(&(value.data.len() as u32).to_le_bytes());
                        body.extend_from_slice(&value.data);
                    }
                    None => body.push(0),
                }
            }
            encode_response(out, opcode, request_id, Status::Ok, &body);
        }

        Reply::StoredMany(cas_tokens) => {
            let mut body = Vec::with_capacity(4 + cas_tokens.len() * 8);
            body.extend_from_slice(&(cas_tokens.len() as u32).to_le_bytes());
            for cas in cas_tokens {
                body.extend_from_slice(&cas.to_le_bytes());
            }
            encode_response(out, opcode, request_id, Status::Ok, &body);
        }

        Reply::DeletedMany(hits) => {
            let mut body = Vec::with_capacity(4 + hits.len());
            body.extend_from_slice(&(hits.len() as u32).to_le_bytes());
            body.extend(hits.iter().map(|hit| u8::from(*hit)));
            encode_response(out, opcode, request_id, Status::Ok, &body);
        }

        Reply::Hello(info) => {
            let mut body = [0u8; HELLO_RESPONSE_LEN];
            body[0..2].copy_from_slice(&info.protocol_version.to_le_bytes());
            body[2..4].copy_from_slice(&info.shards.to_le_bytes());
            body[4..8].copy_from_slice(&info.max_key_len.to_le_bytes());
            body[8..12].copy_from_slice(&info.max_value_len.to_le_bytes());
            body[12..16].copy_from_slice(&info.capabilities.to_le_bytes());
            encode_response(out, opcode, request_id, Status::Ok, &body);
        }

        // The guarded outcomes only arise over the memcached adapter today, but
        // they are mapped rather than lumped into a generic error so that
        // adding conditional writes to KCP needs no wire change.
        Reply::Stored(outcome) => match outcome {
            cache_core::Stored::Stored(cas) => {
                encode_response(out, opcode, request_id, Status::Ok, &cas.to_le_bytes())
            }
            cache_core::Stored::NotStored => {
                encode_response(out, opcode, request_id, Status::NotStored, &[])
            }
            cache_core::Stored::Exists => {
                encode_response(out, opcode, request_id, Status::Exists, &[])
            }
            cache_core::Stored::NotFound => {
                encode_response(out, opcode, request_id, Status::NotFound, &[])
            }
        },

        Reply::Counter(value) => {
            encode_response(out, opcode, request_id, Status::Ok, &value.to_le_bytes())
        }

        // Protocol-level replies with no KCP representation yet. Reported as
        // unsupported rather than silently rendered as success.
        Reply::Stats(_) | Reply::Version(_) | Reply::Closing => {
            encode_response(out, opcode, request_id, Status::Unsupported, &[])
        }

        Reply::Value(value) => {
            // Written as one frame without an intermediate buffer: header, then
            // the value metadata, then the payload straight out of the store.
            let body_len = (VALUE_PREFIX_LEN + value.data.len()) as u32;
            let header = FrameHeader::response(opcode, request_id, Status::Ok, body_len);
            out.reserve(super::frame::HEADER_LEN + body_len as usize);
            out.extend_from_slice(header.as_bytes());
            out.extend_from_slice(&value.mc_flags.to_le_bytes());
            out.extend_from_slice(&value.cas.to_le_bytes());
            out.extend_from_slice(&value.data);
        }
    }
}

pub const HELLO_RESPONSE_LEN: usize = 16;
/// `mc_flags u32 | cas u64` ahead of the value bytes.
pub const VALUE_PREFIX_LEN: usize = 12;

/// Builds a `SET` body. Shared with the client and the tests so there is one
/// definition of the layout rather than two that can drift.
pub fn encode_set_body(out: &mut Vec<u8>, key: &[u8], value: &[u8], ttl_secs: u32, tags: &[&[u8]]) {
    let header = SetBodyHeader {
        ttl_secs: U32::new(ttl_secs),
        key_len: U16::new(key.len() as u16),
        tag_count: tags.len() as u8,
        reserved: 0,
        value_len: U32::new(value.len() as u32),
    };
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    for tag in tags {
        out.extend_from_slice(&(tag.len() as u16).to_le_bytes());
        out.extend_from_slice(tag);
    }
}

/// Builds a `TOUCH` body: `ttl_secs u32` followed by the key.
pub fn encode_touch_body(out: &mut Vec<u8>, key: &[u8], ttl_secs: u32) {
    out.extend_from_slice(&ttl_secs.to_le_bytes());
    out.extend_from_slice(key);
}

/// Builds a `GET_MANY` or `DELETE_MANY` body: a count, then length-prefixed keys.
pub fn encode_key_list_body(out: &mut Vec<u8>, keys: &[&[u8]]) {
    out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    for key in keys {
        out.extend_from_slice(&(key.len() as u16).to_le_bytes());
        out.extend_from_slice(key);
    }
}

/// Writes the item count that prefixes a `SET_MANY` body. The caller then
/// appends one [`encode_set_body`] per item.
pub fn encode_batch_count(out: &mut Vec<u8>, count: usize) {
    out.extend_from_slice(&(count as u32).to_le_bytes());
}

/// Parses a `HELLO` response body.
pub fn decode_hello_response(body: &[u8]) -> Option<ServerInfo> {
    if body.len() < HELLO_RESPONSE_LEN {
        return None;
    }
    Some(ServerInfo {
        protocol_version: u16::from_le_bytes(body[0..2].try_into().ok()?),
        shards: u16::from_le_bytes(body[2..4].try_into().ok()?),
        max_key_len: u32::from_le_bytes(body[4..8].try_into().ok()?),
        max_value_len: u32::from_le_bytes(body[8..12].try_into().ok()?),
        capabilities: u32::from_le_bytes(body[12..16].try_into().ok()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kcp::decode::{Decoded, decode};
    use bytes::Bytes;
    use cache_core::Value;
    use zerocopy::FromBytes;

    #[test]
    fn value_reply_carries_flags_cas_and_payload() {
        let mut out = Vec::new();
        let reply = Reply::Value(Value {
            data: Bytes::from_static(b"payload"),
            mc_flags: 0xaabb_ccdd,
            cas: 99,
            expires_at_ms: None,
        });
        encode_reply(&mut out, Opcode::Get, 5, &reply);

        let (header, body) = super::FrameHeader::ref_from_prefix(&out).unwrap();
        assert!(header.is_response());
        assert_eq!(header.request_id.get(), 5);
        assert_eq!(header.status.get(), Status::Ok as u16);
        assert_eq!(body.len(), VALUE_PREFIX_LEN + 7);
        assert_eq!(
            u32::from_le_bytes(body[0..4].try_into().unwrap()),
            0xaabb_ccdd
        );
        assert_eq!(u64::from_le_bytes(body[4..12].try_into().unwrap()), 99);
        assert_eq!(&body[12..], b"payload");
    }

    #[test]
    fn hello_response_roundtrips() {
        let info = ServerInfo {
            protocol_version: 1,
            shards: 8,
            max_key_len: 511,
            max_value_len: 1 << 20,
            capabilities: 0b101,
        };
        let mut out = Vec::new();
        encode_reply(&mut out, Opcode::Hello, 1, &Reply::Hello(info));
        let body = &out[super::super::frame::HEADER_LEN..];
        assert_eq!(decode_hello_response(body), Some(info));
    }

    #[test]
    fn not_found_uses_the_status_not_an_empty_value() {
        let mut out = Vec::new();
        encode_reply(&mut out, Opcode::Get, 1, &Reply::NotFound);
        let (header, body) = super::FrameHeader::ref_from_prefix(&out).unwrap();
        assert_eq!(header.status.get(), Status::NotFound as u16);
        assert!(body.is_empty());
    }

    #[test]
    fn set_body_roundtrips_through_the_decoder() {
        let mut body = Vec::new();
        encode_set_body(&mut body, b"key", b"val", 60, &[]);

        let mut frame = Vec::new();
        encode_request(&mut frame, Opcode::Set, 1, &body);

        let Ok(Decoded::Request { request, consumed }) = decode(&frame) else {
            panic!("expected a request")
        };
        assert_eq!(consumed, frame.len());
        let cache_core::Command::Set(set) = request.command else {
            panic!("expected a set")
        };
        assert_eq!(set.key.as_bytes(), b"key");
        assert_eq!(set.value, b"val");
        assert_eq!(set.ttl_secs, 60);
    }
}
