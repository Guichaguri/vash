//! Wire protocol adapters.
//!
//! Every decoder is a pure function from `&[u8]` to a `vash_core::Command`,
//! borrowing rather than copying and allocating nothing beyond a tag list. That
//! shape is deliberate: it makes the parsers directly fuzzable and benchable,
//! and it is the only code in the system that reads bytes from unauthenticated
//! clients, so it is where the security budget goes.
//!
//! Each dialect sits beside the others rather than inside one of them, because
//! they share only `vash_core`. `vcp` and `memcached` decode into
//! `vash_core::Command`; `resp` has a command type of its own, because Redis's
//! string commands do not map one-to-one onto storage operations — see that
//! module and `vash_server::resp`.

mod digits;
pub mod memcached;
pub mod resp;
pub mod vcp;

/// Which dialect a connection is speaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Vcp,
    Memcached,
    /// Redis, in either RESP2 or RESP3 — the request framing is the same and
    /// the reply dialect is negotiated later with `HELLO`.
    Resp,
}

/// Decides a connection's protocol from its very first byte.
///
/// VCP requires `HELLO` as its opening frame, so a connection starts with the
/// opcode `0x01`. Every memcached command begins with a lowercase letter. A
/// RESP request is always an array, so it starts with `*`. The three sets
/// cannot overlap, so one byte settles it and nothing is re-parsed.
///
/// This is also why RESP *inline* commands are not accepted: `get foo\r\n` is a
/// valid inline Redis command and a valid memcached one, and no amount of
/// look-ahead makes that choice for us. See [`resp`].
///
/// Returns `None` while the buffer is still empty.
pub fn detect(buf: &[u8]) -> Option<Result<Protocol, UnknownProtocol>> {
    let first = *buf.first()?;
    Some(match first {
        b if b == vcp::Opcode::Hello as u8 => Ok(Protocol::Vcp),
        b'*' => Ok(Protocol::Resp),
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
    fn a_hello_frame_is_vcp() {
        assert_eq!(detect(&[vcp::Opcode::Hello as u8]), Some(Ok(Protocol::Vcp)));
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
    fn a_vcp_frame_that_does_not_start_with_hello_is_rejected() {
        // Requiring HELLO first is what keeps the one-byte test unambiguous.
        assert_eq!(
            detect(&[vcp::Opcode::Get as u8]),
            Some(Err(UnknownProtocol(vcp::Opcode::Get as u8)))
        );
    }

    #[test]
    fn resp_requests_are_recognised() {
        // Always an array, so always `*`. The inline form a telnet session
        // would send is deliberately not accepted — it is indistinguishable
        // from memcached.
        assert_eq!(detect(b"*1\r\n$4\r\nPING\r\n"), Some(Ok(Protocol::Resp)));
        assert_eq!(detect(b"get foo\r\n"), Some(Ok(Protocol::Memcached)));
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
