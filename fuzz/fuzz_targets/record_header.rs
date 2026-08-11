//! Fuzzes the on-disk record parser.
//!
//! ```text
//! cargo +nightly fuzz run record_header fuzz/seeds/record_header
//! ```
//!
//! Unlike the two protocol parsers, this one reads bytes from the memory map
//! rather than from a socket — so the threat is not an attacker but a database
//! written by an older build, truncated by a crash, or corrupted on a page that
//! `ephemeral` durability never synced. All three produce input this parser has
//! to reject rather than trust, and it runs on the hot path of every read, so a
//! panic here takes the process down while serving.
//!
//! The interesting field is `tag_count`: it sizes a slice cast out of the
//! remaining bytes, so a count that does not match the record's real length has
//! to fail rather than read past it.

#![no_main]

use cache_core::{RecordRef, record_len};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(record) = RecordRef::parse(data) else {
        return;
    };

    // Anything that parsed must account for exactly the bytes it was given —
    // the tag table and the value together, with nothing left over and nothing
    // borrowed from beyond the end.
    assert_eq!(
        record_len(record.tags.len(), record.value.len()),
        data.len(),
        "a parsed record must span its whole buffer"
    );
    assert_eq!(record.tags.len(), record.header.tag_count as usize);

    // Liveness runs on every read and must be total for any record that parsed.
    let _ = record.is_alive(u64::MAX, record.header.epoch.get(), |_| Some(0));
    let _ = record.is_alive(0, 0, |_| None);
});
