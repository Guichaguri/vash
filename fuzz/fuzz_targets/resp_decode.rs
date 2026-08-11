//! Fuzzes the RESP parser.
//!
//! ```text
//! cargo +nightly fuzz run resp_decode fuzz/seeds/resp_decode
//! ```
//!
//! RESP is length-delimited throughout: an array announces how many arguments
//! follow and each argument announces its own length, both from the client.
//! Trusting either number is how a parser ends up reading past its buffer, and
//! mis-measuring one is how the *next* command in a pipeline gets read out of
//! the middle of somebody's value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use vash_proto::resp::{Outcome, ProtocolError, parse};

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
