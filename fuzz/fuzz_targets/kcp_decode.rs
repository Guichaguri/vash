//! Fuzzes the native protocol decoder.
//!
//! ```text
//! cargo +nightly fuzz run kcp_decode fuzz/seeds/kcp_decode
//! ```
//!
//! The decoder reads a length-prefixed binary format from an anonymous socket,
//! which is the classic shape for an out-of-bounds read or an allocation driven
//! by an attacker-supplied count. It contains no `unsafe`, so the failures worth
//! hunting are panics — a slice out of range, an arithmetic overflow — each of
//! which takes down every other connection in the process.
//!
//! The assertions below are the invariants the connection loop relies on, not
//! decoration: violating either one hangs or corrupts a live server rather than
//! merely crashing it, so the fuzzer has to be told to look for them.

#![no_main]

use cache_proto::kcp::{DecodeError, Decoded, FrameLen, decode, peek_frame_len};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let peeked = peek_frame_len(data);
    let decoded = decode(data);

    match &decoded {
        // A frame the connection answers and steps over. Zero would spin the
        // loop forever on one bad frame; past the end would panic the slice.
        Err(DecodeError::Body { consumed, .. }) => {
            assert!(*consumed > 0, "a rejected frame must advance the buffer");
            assert!(*consumed <= data.len(), "skip length is outside the buffer");
        }
        Ok(Decoded::Request { consumed, .. }) => {
            assert!(*consumed > 0);
            assert!(*consumed <= data.len());
        }
        _ => {}
    }

    // The connection splits a frame off using `peek_frame_len` and hands those
    // bytes to the executor. If the two disagreed about where a frame ends, one
    // request would be executed against another's bytes.
    if let FrameLen::Complete(peeked) = peeked {
        match &decoded {
            Ok(Decoded::Request { consumed, .. }) | Err(DecodeError::Body { consumed, .. }) => {
                assert_eq!(peeked, *consumed, "peek and decode disagree on the boundary");
            }
            _ => {}
        }
    }
});
