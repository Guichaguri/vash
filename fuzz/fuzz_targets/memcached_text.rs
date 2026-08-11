//! Fuzzes the memcached parser on classic-dialect input.
//!
//! ```text
//! cargo +nightly fuzz run memcached_text fuzz/seeds/memcached_text
//! ```
//!
//! Text framing is where the subtle bugs live: storage commands are
//! length-delimited rather than line-delimited, so the parser has to trust a
//! `<bytes>` token from the client to know where the value ends and the next
//! command begins. Getting that wrong does not crash — it makes the server
//! execute part of somebody's value as a command.

#![no_main]

use libfuzzer_sys::fuzz_target;
use vash_proto::memcached::{Outcome, ProtocolError, parse};

fuzz_target!(|data: &[u8]| {
    match parse(data) {
        // The connection advances by `consumed` and parses again. Zero spins a
        // core forever; past the end panics.
        Ok(Outcome::Command(parsed)) => {
            assert!(parsed.consumed > 0, "a command must advance the buffer");
            assert!(parsed.consumed <= data.len(), "consumed past the buffer");
        }
        Err(ProtocolError::Recoverable { consumed, .. }) => {
            assert!(consumed > 0, "a rejected command must advance the buffer");
            assert!(consumed <= data.len(), "skip length is outside the buffer");
        }
        // Incomplete waits for more bytes and Fatal closes the connection;
        // neither advances anything, so neither can spin.
        Ok(Outcome::Incomplete) | Err(ProtocolError::Fatal(_)) => {}
    }
});
