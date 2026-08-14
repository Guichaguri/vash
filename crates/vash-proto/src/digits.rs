//! Decimal integers, appended without allocating.
//!
//! Both text dialects spell their lengths, flags and CAS values as decimal
//! ASCII, and both used to reach for `to_string()` to get there — RESP through
//! an `itoa` helper that was `value.to_string()` under the name, memcached
//! three times in a single `VALUE` line. Every one of those is a heap
//! allocation and a free, on a path whose entire job is appending bytes to a
//! buffer that is already there.
//!
//! The longest `u64` is 20 digits, so the workspace fits in a fixed array and
//! the digits are written back-to-front straight into it. That is the whole
//! trick; there is no fast-path table and no attempt to beat the `itoa` crate,
//! because the point is to stop allocating rather than to win a formatting
//! benchmark.

/// Appends `value` as decimal ASCII.
pub(crate) fn push_u64(out: &mut Vec<u8>, mut value: u64) {
    // `u64::MAX` is 20 digits, so this never needs to grow and the indexing
    // below can never run off the front.
    let mut buf = [0u8; 20];
    let mut at = buf.len();
    loop {
        at -= 1;
        buf[at] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[at..]);
}

/// The same, for the signed values RESP admits.
///
/// `unsigned_abs` rather than `-value`, so `i64::MIN` renders instead of
/// overflowing — RESP has no command that produces it today, but a decoder
/// helper that panics on one input is not one worth having.
pub(crate) fn push_i64(out: &mut Vec<u8>, value: i64) {
    if value < 0 {
        out.push(b'-');
    }
    push_u64(out, value.unsigned_abs());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered_u64(value: u64) -> String {
        let mut out = Vec::new();
        push_u64(&mut out, value);
        String::from_utf8(out).unwrap()
    }

    fn rendered_i64(value: i64) -> String {
        let mut out = Vec::new();
        push_i64(&mut out, value);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn the_edges_render_as_the_standard_library_does() {
        for value in [0, 1, 9, 10, 99, 100, u64::from(u32::MAX), u64::MAX] {
            assert_eq!(rendered_u64(value), value.to_string());
        }
        for value in [0, -1, 1, -9, i64::MAX, i64::MIN] {
            assert_eq!(rendered_i64(value), value.to_string());
        }
    }

    #[test]
    fn appending_leaves_what_was_already_there() {
        let mut out = b"VALUE k ".to_vec();
        push_u64(&mut out, 1024);
        assert_eq!(out, b"VALUE k 1024");
    }

    proptest::proptest! {
        #[test]
        fn every_u64_matches_to_string(value: u64) {
            proptest::prop_assert_eq!(rendered_u64(value), value.to_string());
        }

        #[test]
        fn every_i64_matches_to_string(value: i64) {
            proptest::prop_assert_eq!(rendered_i64(value), value.to_string());
        }
    }
}
