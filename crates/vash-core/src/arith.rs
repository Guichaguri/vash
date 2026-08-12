//! Counter arithmetic, and the decimal text a counter is stored as.
//!
//! Counters are held as their decimal text rather than as a packed integer,
//! because that is what the memcached protocol defines and what makes a plain
//! `GET` of a counter return something the client can read. Every dialect that
//! does arithmetic therefore agrees on the *representation* and disagrees only
//! on the *domain*: memcached counts unsigned and wraps, Redis counts signed
//! and refuses to overflow, and Redis also counts in floats.
//!
//! Those differences live in [`Delta`], [`OnBound`] and [`Missing`], which
//! together describe every arithmetic command all three dialects offer. The
//! evaluation itself — [`Arithmetic::evaluate`] — is a pure function of the
//! stored bytes, which is what lets the storage engine run it **inside the
//! write transaction** and therefore atomically. Before this existed, the Redis
//! adapter read the value, computed in the network tier and wrote it back, and
//! two clients incrementing one counter could lose an update.
//!
//! Nothing here performs I/O or knows what a shard is. The store supplies the
//! current bytes and applies the result; this module decides what the result is.

use crate::error::{CoreError, Result};
use crate::key::Key;

/// A number in whichever domain its operation runs in.
///
/// The variant is load-bearing twice over: it decides how the value is rendered
/// back to text, and it decides how the dialect renders the *reply* — Redis
/// answers `INCR` with an integer and `INCRBYFLOAT` with a bulk string.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    Int(i64),
    Float(f64),
    /// memcached's counter domain: unsigned, and 64 bits wide.
    Counter(u64),
}

impl Number {
    /// The decimal text this number is stored as.
    ///
    /// One allocation per arithmetic write, on a path that is already writing a
    /// record and descending a B-tree. Rendering into a fixed stack buffer
    /// instead would save it for the integer cases and be impossible for the
    /// float one: Rust's `f64` `Display` never uses exponent notation, so
    /// `f64::MAX` prints as 309 digits.
    pub fn to_text(self) -> String {
        match self {
            Self::Int(value) => value.to_string(),
            Self::Counter(value) => value.to_string(),
            Self::Float(value) => format_float(value),
        }
    }
}

/// The arithmetic to perform, and the range the result must land in.
///
/// Mode and bounds are one type rather than two so an integer delta cannot be
/// paired with float bounds. The commands with no bounds of their own — `INCR`,
/// `INCRBY`, `INCRBYFLOAT` — pass the limits of their own type, which turns
/// "overflowed" and "out of bounds" into one condition with one handler.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Delta {
    /// memcached `incr`/`decr`. An increment wraps at 64 bits and a decrement
    /// floors at zero — upstream's behaviour, and not ours to improve on.
    /// Unbounded by construction, so [`OnBound`] never applies to it.
    Counter { delta: u64, decrement: bool },
    /// Redis integer arithmetic, confined to `lower..=upper`.
    Int { delta: i64, lower: i64, upper: i64 },
    /// Redis float arithmetic, likewise.
    Float { delta: f64, lower: f64, upper: f64 },
}

impl Delta {
    /// `INCR`/`INCRBY`/`DECR`/`DECRBY`: the full range of `i64`.
    pub const fn int(delta: i64) -> Self {
        Self::Int {
            delta,
            lower: i64::MIN,
            upper: i64::MAX,
        }
    }

    /// `INCRBYFLOAT`: the full finite range of `f64`.
    pub const fn float(delta: f64) -> Self {
        Self::Float {
            delta,
            lower: f64::MIN,
            upper: f64::MAX,
        }
    }
}

/// What to do when the result will not fit the bounds.
///
/// Three behaviours because the commands genuinely have three. Collapsing
/// [`Skip`] and [`Clamp`] into a boolean would work until `INCR` needed to fail,
/// which it does.
///
/// [`Skip`]: OnBound::Skip
/// [`Clamp`]: OnBound::Clamp
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnBound {
    /// Refuse the operation. `INCR` and friends, where overflow is an error and
    /// the bounds are the type's own.
    #[default]
    Fail,
    /// Leave the value where it is and report a zero increment. `INCREX`
    /// without `SATURATE`, which also leaves the key's lifetime alone.
    Skip,
    /// Clamp to whichever bound was breached. `INCREX SATURATE`.
    Clamp,
}

/// What an operation does about a key that is not there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Missing {
    /// Report a miss and write nothing. memcached's `incr`/`decr`.
    #[default]
    Fail,
    /// Treat the absent key as holding zero and create it. Every Redis
    /// arithmetic command.
    CreateAtZero,
}

/// What an operation does to the record's deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TtlChange {
    /// Leave it exactly as it is; a record created here gets no expiry.
    ///
    /// This is what makes `INCR` keep a key's lifetime without anyone reading
    /// it first: the deadline is preserved by not being touched, rather than by
    /// being read out and written back.
    #[default]
    Keep,
    /// Replace it. `0` means no expiry, in the store's usual encoding.
    Set(u32),
    /// `INCREX … ENX`: apply only to a record that currently has no deadline.
    SetIfPersistent(u32),
}

/// An atomic read-modify-write against a counter.
#[derive(Debug, Clone, Copy)]
pub struct Arithmetic<'a> {
    pub key: Key<'a>,
    pub delta: Delta,
    pub on_bound: OnBound,
    pub missing: Missing,
    pub ttl: TtlChange,
}

impl<'a> Arithmetic<'a> {
    /// memcached `incr`/`decr`: unsigned, never creates, never touches the
    /// deadline.
    pub fn counter(key: Key<'a>, delta: u64, decrement: bool) -> Self {
        Self {
            key,
            delta: Delta::Counter { delta, decrement },
            on_bound: OnBound::Fail,
            missing: Missing::Fail,
            ttl: TtlChange::Keep,
        }
    }

    /// The Redis default: create at zero, fail on overflow, keep the lifetime.
    pub fn redis(key: Key<'a>, delta: Delta) -> Self {
        Self {
            key,
            delta,
            on_bound: OnBound::Fail,
            missing: Missing::CreateAtZero,
            ttl: TtlChange::Keep,
        }
    }
}

/// Where a counter ended up, and how far it moved to get there.
///
/// Both, because after clamping only the evaluator knows the distance — and
/// `INCREX` reports it, so a caller learns in one round trip both where the
/// counter stands and whether it actually moved.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Applied {
    pub value: Number,
    pub applied: Number,
    /// Whether anything was actually stored.
    ///
    /// `false` when a bound held the value exactly where it was, which also
    /// means the record kept its deadline. Callers need it to count the
    /// operation honestly — a skipped `INCREX` is not a write.
    pub wrote: bool,
}

/// What the store should do with the result.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The key is absent and this operation does not create one. Nothing is
    /// written and the caller reports a miss.
    Missing,
    /// A bound held the value where it was. **Nothing is written** — not even
    /// the deadline — so the record keeps its lifetime as well as its value.
    Unchanged(Applied),
    /// Store this text under the key.
    Store { text: String, applied: Applied },
}

impl Arithmetic<'_> {
    /// Decides the outcome from the bytes currently stored, or `None` if the key
    /// is absent.
    ///
    /// Pure: no clock, no I/O, no shared state. That is what lets the writer
    /// thread call it with the record still borrowed from the memory map.
    pub fn evaluate(&self, stored: Option<&[u8]>) -> Result<Outcome> {
        let current = match (stored, self.missing) {
            (None, Missing::Fail) => return Ok(Outcome::Missing),
            // The absent key reads as zero, which is how `INCR` creates one.
            (None, Missing::CreateAtZero) => None,
            (Some(bytes), _) => Some(bytes),
        };

        match self.delta {
            Delta::Counter { delta, decrement } => {
                let current = match current {
                    Some(bytes) => parse_counter(bytes)?,
                    None => 0,
                };
                // memcached clamps a decrement at zero rather than wrapping, and
                // lets an increment wrap at 64 bits. Bounds cannot apply here,
                // so there is no `on_bound` arm to take.
                let updated = if decrement {
                    current.saturating_sub(delta)
                } else {
                    current.wrapping_add(delta)
                };
                Ok(stored_at(
                    Number::Counter(updated),
                    Number::Counter(updated.wrapping_sub(current)),
                ))
            }

            Delta::Int {
                delta,
                lower,
                upper,
            } => {
                let current = match current {
                    Some(bytes) => parse_int(bytes).ok_or(CoreError::NotAnInteger)?,
                    None => 0,
                };
                let candidate = current.checked_add(delta);

                if let Some(updated) = candidate.filter(|value| (lower..=upper).contains(value)) {
                    return Ok(stored_at(Number::Int(updated), Number::Int(delta)));
                }

                match self.on_bound {
                    OnBound::Fail => Err(CoreError::Overflow),
                    OnBound::Skip => Ok(Outcome::Unchanged(Applied {
                        value: Number::Int(current),
                        applied: Number::Int(0),
                        wrote: false,
                    })),
                    OnBound::Clamp => {
                        // Which bound was breached comes from the result where
                        // there is one, and from the sign of the increment where
                        // the addition overflowed and there is not. Reading it
                        // off the sign in both cases would clamp
                        // `INCREX k BYINT 0 UBOUND 5` on a key holding 10 to the
                        // *floor*.
                        let clamped = match candidate {
                            Some(updated) if updated > upper => upper,
                            Some(_) => lower,
                            None if delta > 0 => upper,
                            None => lower,
                        };
                        let applied = clamped.checked_sub(current).ok_or(CoreError::Overflow)?;
                        Ok(stored_at(Number::Int(clamped), Number::Int(applied)))
                    }
                }
            }

            Delta::Float {
                delta,
                lower,
                upper,
            } => {
                let current = match current {
                    Some(bytes) => parse_float(bytes)
                        .filter(|value| value.is_finite())
                        .ok_or(CoreError::NotAFloat)?,
                    None => 0.0,
                };
                let updated = current + delta;

                if updated.is_finite() && (lower..=upper).contains(&updated) {
                    return Ok(stored_at(Number::Float(updated), Number::Float(delta)));
                }

                match self.on_bound {
                    // The unbounded float commands pass ±`f64::MAX`, so the only
                    // way to reach here with `Fail` is a result that is not
                    // finite — which is the error Redis names for it.
                    OnBound::Fail => Err(CoreError::NotFinite),
                    OnBound::Skip => Ok(Outcome::Unchanged(Applied {
                        value: Number::Float(current),
                        applied: Number::Float(0.0),
                        wrote: false,
                    })),
                    OnBound::Clamp => {
                        let clamped = if updated > upper || (!updated.is_finite() && delta > 0.0) {
                            upper
                        } else {
                            lower
                        };
                        let applied = clamped - current;
                        if !applied.is_finite() {
                            return Err(CoreError::NotFinite);
                        }
                        Ok(stored_at(Number::Float(clamped), Number::Float(applied)))
                    }
                }
            }
        }
    }
}

fn stored_at(value: Number, applied: Number) -> Outcome {
    Outcome::Store {
        text: value.to_text(),
        applied: Applied {
            value,
            applied,
            wrote: true,
        },
    }
}

/// Reads a stored memcached counter.
///
/// Surrounding whitespace is tolerated because upstream's `strtoull` tolerates
/// it, and a value written by a memcached client is exactly what this has to
/// read back. Redis is stricter — see [`parse_int`] — and the difference is a
/// property of the dialect's number domain, which is why each mode parses with
/// its own rule rather than sharing one.
fn parse_counter(stored: &[u8]) -> Result<u64> {
    std::str::from_utf8(stored)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
        .ok_or(CoreError::NotAnInteger)
}

/// Reads an integer the way Redis does: no leading zeros, no sign but `-`, no
/// surrounding space.
///
/// Lives here rather than in the protocol crate because it is now needed in two
/// places — parsing a command's argument and reading a *stored* counter — and
/// two copies of a rule this exacting is how a value gets written that the
/// server cannot read back.
pub fn parse_int(token: &[u8]) -> Option<i64> {
    if token == b"0" {
        return Some(0);
    }

    let (negative, digits) = match token.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, token),
    };
    if digits.is_empty() || digits[0] == b'0' || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }

    let magnitude: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    if negative {
        // `i64::MIN` has no positive counterpart, so it is admitted by
        // magnitude and negated by wrapping — which lands exactly on it.
        (magnitude <= i64::MAX as u64 + 1).then(|| (magnitude as i64).wrapping_neg())
    } else {
        i64::try_from(magnitude).ok()
    }
}

/// The float equivalent. NaN is rejected on the way in, as Redis does, so the
/// only NaN an arithmetic command can meet is one it computed itself.
pub fn parse_float(token: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(token).ok()?;
    if text.is_empty() || text.trim() != text {
        return None;
    }
    let value: f64 = text.parse().ok()?;
    (!value.is_nan()).then_some(value)
}

/// Renders a float the way Redis does: plain decimal notation, no exponent, and
/// no trailing zeros — `3.0` comes back as `3`.
///
/// Redis computes in `long double` and this server in `f64`, so the last digits
/// of a long chain of `INCRBYFLOAT` calls can differ. Rust has no 80-bit float,
/// and the alternative — carrying a decimal library for one command — is not
/// worth it for a cache.
pub fn format_float(value: f64) -> String {
    // Rust's `Display` for `f64` already prints the shortest text that reads
    // back as the same value, and never uses exponent notation.
    let text = format!("{value}");
    if let Some(trimmed) = text.strip_suffix(".0") {
        return trimmed.to_string();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Key<'static> {
        Key::new(b"k").expect("valid")
    }

    fn stored(outcome: Outcome) -> (String, Applied) {
        match outcome {
            Outcome::Store { text, applied } => (text, applied),
            other => panic!("expected a write, got {other:?}"),
        }
    }

    // ---- memcached counters ------------------------------------------------

    #[test]
    fn a_counter_increments_and_decrements() {
        let op = Arithmetic::counter(key(), 5, false);
        assert_eq!(stored(op.evaluate(Some(b"10")).unwrap()).0, "15");

        let op = Arithmetic::counter(key(), 5, true);
        assert_eq!(stored(op.evaluate(Some(b"10")).unwrap()).0, "5");
    }

    #[test]
    fn a_counter_floors_at_zero_and_wraps_at_the_top() {
        // Both are upstream memcached's behaviour, and clients depend on them.
        let decrement = Arithmetic::counter(key(), 100, true);
        assert_eq!(stored(decrement.evaluate(Some(b"10")).unwrap()).0, "0");

        let increment = Arithmetic::counter(key(), 2, false);
        let text = stored(
            increment
                .evaluate(Some(&u64::MAX.to_string().into_bytes()))
                .unwrap(),
        )
        .0;
        assert_eq!(text, "1");
    }

    #[test]
    fn a_counter_never_creates_a_missing_key() {
        let op = Arithmetic::counter(key(), 1, false);
        assert_eq!(op.evaluate(None).unwrap(), Outcome::Missing);
    }

    #[test]
    fn a_counter_tolerates_surrounding_space() {
        // memcached's strtoull does, and a value it wrote must read back.
        let op = Arithmetic::counter(key(), 1, false);
        assert_eq!(stored(op.evaluate(Some(b" 41 ")).unwrap()).0, "42");
    }

    #[test]
    fn a_counter_refuses_a_non_numeric_value() {
        let op = Arithmetic::counter(key(), 1, false);
        assert_eq!(op.evaluate(Some(b"abc")), Err(CoreError::NotAnInteger));
    }

    // ---- Redis integers ----------------------------------------------------

    #[test]
    fn an_integer_creates_a_missing_key_at_zero() {
        let op = Arithmetic::redis(key(), Delta::int(5));
        let (text, applied) = stored(op.evaluate(None).unwrap());
        assert_eq!(text, "5");
        assert_eq!(applied.value, Number::Int(5));
    }

    #[test]
    fn an_integer_overflow_is_an_error_not_a_clamp() {
        let op = Arithmetic::redis(key(), Delta::int(1));
        let at_max = i64::MAX.to_string().into_bytes();
        assert_eq!(op.evaluate(Some(&at_max)), Err(CoreError::Overflow));
    }

    #[test]
    fn redis_rejects_the_leading_zeros_memcached_accepts() {
        // The two domains parse differently on purpose; this is the difference.
        let redis = Arithmetic::redis(key(), Delta::int(1));
        assert_eq!(redis.evaluate(Some(b"007")), Err(CoreError::NotAnInteger));

        let memcached = Arithmetic::counter(key(), 1, false);
        assert_eq!(stored(memcached.evaluate(Some(b"007")).unwrap()).0, "8");
    }

    #[test]
    fn a_bound_skips_the_write_and_reports_no_movement() {
        let op = Arithmetic {
            on_bound: OnBound::Skip,
            ..Arithmetic::redis(
                key(),
                Delta::Int {
                    delta: 5,
                    lower: 0,
                    upper: 10,
                },
            )
        };
        assert_eq!(
            op.evaluate(Some(b"8")).unwrap(),
            Outcome::Unchanged(Applied {
                value: Number::Int(8),
                applied: Number::Int(0),
                // The record keeps its deadline as well as its value, because
                // nothing is stored at all.
                wrote: false,
            })
        );
    }

    #[test]
    fn saturating_clamps_to_the_bound_that_was_breached() {
        let op = Arithmetic {
            on_bound: OnBound::Clamp,
            ..Arithmetic::redis(
                key(),
                Delta::Int {
                    delta: 5,
                    lower: 0,
                    upper: 10,
                },
            )
        };
        let (text, applied) = stored(op.evaluate(Some(b"8")).unwrap());
        assert_eq!(text, "10");
        assert_eq!(
            applied.applied,
            Number::Int(2),
            "moved only as far as it could"
        );
    }

    #[test]
    fn saturating_reads_the_breached_bound_from_the_result_not_the_sign() {
        // A zero increment on a key already above the ceiling must clamp to the
        // ceiling. Deciding from the sign of the delta would send it to the
        // floor.
        let op = Arithmetic {
            on_bound: OnBound::Clamp,
            ..Arithmetic::redis(
                key(),
                Delta::Int {
                    delta: 0,
                    lower: 0,
                    upper: 5,
                },
            )
        };
        assert_eq!(stored(op.evaluate(Some(b"10")).unwrap()).0, "5");
    }

    #[test]
    fn saturating_an_overflow_clamps_by_the_sign_of_the_increment() {
        // Here there is no result to read the breached bound off, so the sign is
        // the only signal left.
        let op = Arithmetic {
            on_bound: OnBound::Clamp,
            ..Arithmetic::redis(key(), Delta::int(1))
        };
        let at_max = i64::MAX.to_string().into_bytes();
        assert_eq!(
            stored(op.evaluate(Some(&at_max)).unwrap()).0,
            i64::MAX.to_string()
        );
    }

    // ---- Redis floats ------------------------------------------------------

    #[test]
    fn a_float_adds_and_renders_without_a_trailing_zero() {
        let op = Arithmetic::redis(key(), Delta::float(1.5));
        assert_eq!(stored(op.evaluate(Some(b"1.5")).unwrap()).0, "3");
    }

    #[test]
    fn a_float_promotes_a_stored_integer() {
        // An integer promotes to a float without loss, which is exactly what
        // `INCRBYFLOAT` is specified to allow.
        let op = Arithmetic::redis(key(), Delta::float(0.5));
        assert_eq!(stored(op.evaluate(Some(b"10")).unwrap()).0, "10.5");
    }

    #[test]
    fn a_non_finite_float_result_is_refused() {
        let op = Arithmetic::redis(key(), Delta::float(f64::MAX));
        let at_max = format_float(f64::MAX).into_bytes();
        assert_eq!(op.evaluate(Some(&at_max)), Err(CoreError::NotFinite));
    }

    #[test]
    fn a_stored_nan_is_not_a_float() {
        let op = Arithmetic::redis(key(), Delta::float(1.0));
        assert_eq!(op.evaluate(Some(b"nan")), Err(CoreError::NotAFloat));
    }

    // ---- text ---------------------------------------------------------------

    #[test]
    fn integers_round_trip_through_their_text() {
        for value in [0i64, 1, -1, i64::MAX, i64::MIN] {
            let text = Number::Int(value).to_text();
            assert_eq!(parse_int(text.as_bytes()), Some(value), "{value}");
        }
    }

    #[test]
    fn floats_round_trip_through_their_text() {
        for value in [0.0f64, 1.5, -0.25, 1e300] {
            let text = Number::Float(value).to_text();
            assert_eq!(parse_float(text.as_bytes()), Some(value), "{value}");
        }
    }
}
