//! The byte-wise pattern the listing opcodes filter on.
//!
//! Two tokens and an escape, and deliberately nothing else — see
//! `docs/opcodes.md`. Character classes, negation and anchoring would each add
//! matcher surface that takes untrusted input from an unauthenticated stranger,
//! to buy expressiveness that key naming schemes, which are overwhelmingly
//! `prefix:id`, do not use.
//!
//! Matching is over **bytes**: no case folding, no UTF-8 interpretation. Keys
//! are arbitrary byte strings, so anything else would mean deciding what an
//! invalid code point matches.

use crate::error::{CoreError, Result};

/// Matches any run of bytes, including empty.
pub const STAR: u8 = b'*';
/// Matches exactly one byte.
pub const QUESTION: u8 = b'?';
/// Escapes the next byte, whatever it is.
pub const ESCAPE: u8 = b'\\';

/// Rejects a pattern that cannot be matched.
///
/// The only such pattern is one ending in a lone escape, which names a byte that
/// is not there. Validated once when the request is decoded rather than
/// rediscovered on every candidate, and reported rather than treated as a
/// literal backslash — a client that sent `foo\` meant something, and guessing
/// which is worse than saying no.
pub fn validate(pattern: &[u8]) -> Result<()> {
    let mut i = 0;
    while i < pattern.len() {
        if pattern[i] == ESCAPE {
            if i + 1 >= pattern.len() {
                return Err(CoreError::BadPattern);
            }
            i += 2;
        } else {
            i += 1;
        }
    }
    Ok(())
}

/// Whether `candidate` matches `pattern`. An empty pattern matches everything.
///
/// Linear in `pattern.len() * candidate.len()` worst case and **allocation
/// free**. The greedy two-pointer walk with a single remembered star position is
/// what keeps it linear: the classic recursive glob is exponential on inputs
/// like `a*a*a*a*b`, which is a denial-of-service vector when the pattern comes
/// from the network and is applied to every record in a scan.
///
/// A pattern ending in a lone escape never matches; [`validate`] refuses it at
/// decode time, so this only has to be well defined, not useful.
pub fn matches(pattern: &[u8], candidate: &[u8]) -> bool {
    if pattern.is_empty() {
        return true;
    }

    let (mut p, mut c) = (0usize, 0usize);
    // Where to resume from if the current `*` turns out to have consumed too
    // little: the star itself, and the candidate byte it was first tried at.
    let (mut star, mut retry) = (None, 0usize);

    while c < candidate.len() {
        let token = pattern.get(p).copied();

        match token {
            Some(STAR) => {
                star = Some(p);
                p += 1;
                retry = c;
                continue;
            }
            Some(QUESTION) => {
                p += 1;
                c += 1;
                continue;
            }
            Some(ESCAPE) => {
                // A trailing escape has nothing to match; fall through to the
                // backtrack below rather than reading past the end.
                if let Some(&literal) = pattern.get(p + 1)
                    && literal == candidate[c]
                {
                    p += 2;
                    c += 1;
                    continue;
                }
            }
            Some(literal) => {
                if literal == candidate[c] {
                    p += 1;
                    c += 1;
                    continue;
                }
            }
            None => {}
        }

        // Mismatch. Give the last star one more byte and try again from there.
        let Some(position) = star else {
            return false;
        };
        p = position + 1;
        retry += 1;
        c = retry;
    }

    // Trailing stars may still match the empty remainder; nothing else can.
    pattern[p..].iter().all(|&byte| byte == STAR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_pattern_matches_anything() {
        assert!(matches(b"", b""));
        assert!(matches(b"", b"anything at all"));
    }

    #[test]
    fn literals_match_exactly() {
        assert!(matches(b"session:1", b"session:1"));
        assert!(!matches(b"session:1", b"session:2"));
        assert!(!matches(b"session:1", b"session:11"));
        assert!(!matches(b"session:11", b"session:1"));
    }

    #[test]
    fn star_spans_any_run_including_empty() {
        assert!(matches(b"*", b""));
        assert!(matches(b"*", b"whatever"));
        assert!(matches(b"session:*", b"session:"));
        assert!(matches(b"session:*", b"session:abc"));
        assert!(!matches(b"session:*", b"other:abc"));
        assert!(matches(b"*:id", b"anything:id"));
        assert!(matches(b"a*b*c", b"axxbyyc"));
        assert!(!matches(b"a*b*c", b"axxbyy"));
    }

    #[test]
    fn question_is_exactly_one_byte() {
        assert!(matches(b"user:?", b"user:1"));
        assert!(!matches(b"user:?", b"user:"));
        assert!(!matches(b"user:?", b"user:12"));
    }

    #[test]
    fn escape_makes_a_token_literal() {
        assert!(matches(br"a\*b", b"a*b"));
        assert!(!matches(br"a\*b", b"axxb"));
        assert!(matches(br"a\?b", b"a?b"));
        assert!(matches(br"a\\b", br"a\b"));
    }

    #[test]
    fn brackets_are_literal_bytes_not_a_character_class() {
        // Anyone reaching for a regex should get nothing rather than something
        // surprising.
        assert!(matches(b"[abc]", b"[abc]"));
        assert!(!matches(b"[abc]", b"a"));
    }

    #[test]
    fn a_trailing_escape_is_refused_at_validation() {
        assert!(validate(b"ok").is_ok());
        assert!(validate(br"ok\*").is_ok());
        assert!(validate(br"ok\\").is_ok());
        assert!(validate(br"bad\").is_err());
        assert!(validate(br"bad\\\").is_err());
    }

    #[test]
    fn matching_is_byte_wise_not_utf8_or_case_folded() {
        assert!(!matches(b"ABC", b"abc"));
        assert!(matches(b"*", &[0xff, 0x00, 0xfe]));
        assert!(matches(&[0xff, STAR], &[0xff, 0x01, 0x02]));
    }

    #[test]
    fn adjacent_stars_do_not_change_the_meaning() {
        assert!(matches(b"**", b"anything"));
        assert!(matches(b"a**b", b"ab"));
        assert!(matches(b"a**b", b"axb"));
    }

    #[test]
    fn the_pathological_pattern_stays_linear() {
        // The recursive matcher is exponential here. This is the shape a
        // hostile client would send, so it is a regression test and not a
        // curiosity: if it ever hangs, the algorithm was replaced with a
        // backtracking one.
        let pattern = b"a*a*a*a*a*a*a*a*b";
        let candidate = vec![b'a'; 4096];
        assert!(!matches(pattern, &candidate));
    }
}
