//! KCP framing.
//!
//! ```text
//!  0               1               2               3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +---------------+---------------+-------------------------------+
//! |   opcode u8   |   flags u8    |          status u16           |
//! +---------------+---------------+-------------------------------+
//! |                        request_id u32                         |
//! +---------------------------------------------------------------+
//! |                         body_len u32                          |
//! +---------------------------------------------------------------+
//! |                       body (body_len bytes)                   |
//! ```
//!
//! Twelve bytes, little-endian, unaligned, so the header decodes as a single
//! cast rather than five field reads.
//!
//! `request_id` is echoed by the server. That is what allows responses to come
//! back **out of order**: a pipelined batch can be dispatched across shards in
//! parallel and each reply sent the instant it is ready. It is the main reason
//! this protocol exists rather than an extension of memcached's, whose replies
//! are strictly ordered (plan §3).

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub const HEADER_LEN: usize = 12;

/// Upper bound on a single frame body, enforced *before* any allocation so a
/// hostile `body_len` cannot make the server reserve gigabytes.
pub const MAX_BODY_LEN: u32 = 64 * 1024 * 1024;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
pub struct FrameHeader {
    pub opcode: u8,
    pub flags: u8,
    pub status: U16,
    pub request_id: U32,
    pub body_len: U32,
}

impl FrameHeader {
    pub fn request(opcode: Opcode, request_id: u32, body_len: u32) -> Self {
        Self {
            opcode: opcode as u8,
            flags: 0,
            status: U16::ZERO,
            request_id: U32::new(request_id),
            body_len: U32::new(body_len),
        }
    }

    pub fn response(opcode: Opcode, request_id: u32, status: Status, body_len: u32) -> Self {
        Self {
            opcode: opcode as u8,
            flags: flags::RESPONSE,
            status: U16::new(status as u16),
            request_id: U32::new(request_id),
            body_len: U32::new(body_len),
        }
    }

    #[inline]
    pub fn is_response(&self) -> bool {
        self.flags & flags::RESPONSE != 0
    }

    #[inline]
    pub fn no_reply(&self) -> bool {
        self.flags & flags::NO_REPLY != 0
    }
}

pub mod flags {
    /// Set on frames sent by the server.
    pub const RESPONSE: u8 = 1 << 0;
    /// Client does not want a reply to this request (fire-and-forget writes).
    pub const NO_REPLY: u8 = 1 << 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Hello = 0x01,
    Ping = 0x02,
    Auth = 0x03,
    Stats = 0x04,
    Cluster = 0x05,

    Get = 0x10,
    Set = 0x11,
    Delete = 0x12,
    Touch = 0x13,

    GetMany = 0x20,
    SetMany = 0x21,
    DeleteMany = 0x22,

    DeleteByTag = 0x30,
    Flush = 0x31,
}

impl Opcode {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0x01 => Self::Hello,
            0x02 => Self::Ping,
            0x03 => Self::Auth,
            0x04 => Self::Stats,
            0x05 => Self::Cluster,
            0x10 => Self::Get,
            0x11 => Self::Set,
            0x12 => Self::Delete,
            0x13 => Self::Touch,
            0x20 => Self::GetMany,
            0x21 => Self::SetMany,
            0x22 => Self::DeleteMany,
            0x30 => Self::DeleteByTag,
            0x31 => Self::Flush,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    Exists = 2,
    BadRequest = 3,
    TooLarge = 4,
    Unauthorized = 5,
    Overloaded = 6,
    CapacityFull = 7,
    Unsupported = 8,
    Internal = 9,
    /// A guarded write whose condition was not met.
    NotStored = 10,
}

impl Status {
    pub fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            0 => Self::Ok,
            1 => Self::NotFound,
            2 => Self::Exists,
            3 => Self::BadRequest,
            4 => Self::TooLarge,
            5 => Self::Unauthorized,
            6 => Self::Overloaded,
            7 => Self::CapacityFull,
            8 => Self::Unsupported,
            9 => Self::Internal,
            10 => Self::NotStored,
            _ => return None,
        })
    }

    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_layout_is_exactly_as_documented() {
        assert_eq!(size_of::<FrameHeader>(), HEADER_LEN);
        assert_eq!(align_of::<FrameHeader>(), 1);
    }

    #[test]
    fn header_roundtrips_through_bytes() {
        let h = FrameHeader::response(Opcode::Get, 0xdead_beef, Status::NotFound, 1234);
        let bytes = h.as_bytes().to_vec();
        assert_eq!(bytes.len(), HEADER_LEN);

        let (parsed, rest) = FrameHeader::ref_from_prefix(&bytes).unwrap();
        assert!(rest.is_empty());
        assert_eq!(parsed.opcode, Opcode::Get as u8);
        assert_eq!(parsed.request_id.get(), 0xdead_beef);
        assert_eq!(parsed.status.get(), Status::NotFound as u16);
        assert_eq!(parsed.body_len.get(), 1234);
        assert!(parsed.is_response());
    }

    #[test]
    fn opcode_and_status_roundtrip() {
        for op in [Opcode::Hello, Opcode::Get, Opcode::Set, Opcode::DeleteByTag] {
            assert_eq!(Opcode::from_u8(op as u8), Some(op));
        }
        assert_eq!(Opcode::from_u8(0xfe), None);

        for st in [Status::Ok, Status::NotFound, Status::CapacityFull] {
            assert_eq!(Status::from_u16(st as u16), Some(st));
        }
        assert_eq!(Status::from_u16(999), None);
    }
}
