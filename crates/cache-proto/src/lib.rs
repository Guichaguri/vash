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
