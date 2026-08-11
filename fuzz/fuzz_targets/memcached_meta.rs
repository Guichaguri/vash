//! Fuzzes the memcached parser on meta-dialect input.
//!
//! ```text
//! cargo +nightly fuzz run memcached_meta fuzz/seeds/memcached_meta
//! ```
//!
//! Shares an entry point with the classic dialect but reaches quite different
//! code: single-character flags with optional inline arguments (`T120`, `Ofoo`,
//! `Gnews,sport`), parsed in any order and any combination. That is a much
//! larger input space than the fixed positional arguments of the classic
//! commands, so it gets its own target and its own corpus rather than competing
//! for coverage with `memcached_text`.
//!
//! Every input is prefixed with a meta verb, so the fuzzer spends its budget on
//! flag combinations instead of rediscovering that commands start with `m`.

#![no_main]

use cache_proto::memcached::{Outcome, ProtocolError, parse};
use libfuzzer_sys::fuzz_target;

const VERBS: [&[u8]; 7] = [b"mg", b"ms", b"md", b"mn", b"ma", b"me", b"mdt"];

fuzz_target!(|data: &[u8]| {
    let Some((selector, rest)) = data.split_first() else {
        return;
    };
    let verb = VERBS[*selector as usize % VERBS.len()];

    let mut input = Vec::with_capacity(verb.len() + rest.len() + 1);
    input.extend_from_slice(verb);
    input.push(b' ');
    input.extend_from_slice(rest);

    match parse(&input) {
        Ok(Outcome::Command(parsed)) => {
            assert!(parsed.consumed > 0, "a command must advance the buffer");
            assert!(parsed.consumed <= input.len(), "consumed past the buffer");
        }
        Err(ProtocolError::Recoverable { consumed, .. }) => {
            assert!(consumed > 0, "a rejected command must advance the buffer");
            assert!(consumed <= input.len(), "skip length is outside the buffer");
        }
        Ok(Outcome::Incomplete) | Err(ProtocolError::Fatal(_)) => {}
    }
});
