//! Wire protocol adapters.
//!
//! Every decoder is a pure function from `&[u8]` to a `cache_core::Command`,
//! borrowing rather than copying and allocating nothing beyond a tag list. That
//! shape is deliberate: it makes the parsers directly fuzzable and benchable,
//! and it is the only code in the system that reads bytes from unauthenticated
//! clients, so it is where the security budget goes.
//!
//! The memcached adapter arrives in M3; the module layout leaves room for it
//! beside `kcp` rather than inside it, because the two share only `cache_core`.

pub mod kcp;
pub mod memcached;

/// Which dialect a connection is speaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Kcp,
    Memcached,
}

/// Decides a connection's protocol from its very first byte.
///
/// KCP requires `HELLO` as its opening frame, so a connection starts with the
/// opcode `0x01`. Every memcached command begins with a lowercase letter. The
/// two sets cannot overlap, so one byte settles it and nothing is re-parsed.
///
/// Returns `None` while the buffer is still empty.
pub fn detect(buf: &[u8]) -> Option<Result<Protocol, UnknownProtocol>> {
    let first = *buf.first()?;
    Some(match first {
        b if b == kcp::Opcode::Hello as u8 => Ok(Protocol::Kcp),
        b'a'..=b'z' => Ok(Protocol::Memcached),
        other => Err(UnknownProtocol(other)),
    })
}

/// The opening byte matched neither dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownProtocol(pub u8);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_decided_on_an_empty_buffer() {
        assert!(detect(b"").is_none());
    }

    #[test]
    fn a_hello_frame_is_kcp() {
        assert_eq!(detect(&[kcp::Opcode::Hello as u8]), Some(Ok(Protocol::Kcp)));
    }

    #[test]
    fn memcached_verbs_are_recognised() {
        for command in [
            b"get k\r\n".as_slice(),
            b"set k 0 0 1\r\n",
            b"mg k\r\n",
            b"version\r\n",
            b"quit\r\n",
        ] {
            assert_eq!(
                detect(command),
                Some(Ok(Protocol::Memcached)),
                "{command:?}"
            );
        }
    }

    #[test]
    fn a_kcp_frame_that_does_not_start_with_hello_is_rejected() {
        // Requiring HELLO first is what keeps the one-byte test unambiguous.
        assert_eq!(
            detect(&[kcp::Opcode::Get as u8]),
            Some(Err(UnknownProtocol(kcp::Opcode::Get as u8)))
        );
    }

    #[test]
    fn junk_is_rejected_rather_than_guessed() {
        for byte in [0x00u8, 0x80, 0xff, b'A', b' '] {
            assert_eq!(
                detect(&[byte]),
                Some(Err(UnknownProtocol(byte))),
                "{byte:#04x}"
            );
        }
    }
}
