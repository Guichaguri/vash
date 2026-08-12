//! Fuzzes the `AUTH` body parser.
//!
//! ```text
//! cargo +nightly fuzz run vcp_auth fuzz/seeds/vcp_auth
//! ```
//!
//! The highest-value target in the set, for a reason that has nothing to do
//! with how complicated it is: it is the only parser that runs on input from a
//! connection that has presented **no credential at all**. Everything else in
//! the protocol sits behind the gate this body opens, so a panic here is
//! reachable by anyone who can complete a TCP handshake, where a panic in
//! `decode_set` needs a credential first.
//!
//! The interesting fields are the two lengths. `name_len` and `secret_len`
//! together decide three slices out of one buffer, and each must be checked
//! against its ceiling before it is used to cut anything.

#![no_main]

use libfuzzer_sys::fuzz_target;
use vash_proto::vcp::{
    AUTH_BODY_HEADER_LEN, MAX_AUTH_NAME_LEN, MAX_AUTH_SECRET_LEN, decode_auth,
};

fuzz_target!(|data: &[u8]| {
    let Ok(auth) = decode_auth(data) else {
        return;
    };

    // Anything that parsed must account for exactly the bytes it was given.
    // Trailing bytes are refused rather than ignored, because a body the server
    // would accept two readings of is one a client and a server can disagree
    // about.
    assert_eq!(
        AUTH_BODY_HEADER_LEN + auth.name.len() + auth.secret.len(),
        data.len(),
        "a parsed auth body must span its whole buffer"
    );

    // The ceilings bound what an unauthenticated connection can make the server
    // hold and compare. Both are checked before either is used to slice, so a
    // parse that succeeded cannot have exceeded them.
    assert!(auth.name.len() <= MAX_AUTH_NAME_LEN);
    assert!(auth.secret.len() <= MAX_AUTH_SECRET_LEN);
});
