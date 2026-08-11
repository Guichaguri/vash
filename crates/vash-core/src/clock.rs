use std::time::{SystemTime, UNIX_EPOCH};

/// Threshold above which a TTL is an absolute unix timestamp rather than a
/// relative offset, in seconds (30 days).
///
/// This is memcached's rule, and it is the encoding the store's `ttl_secs`
/// field speaks — one interpretation, in one place, because two of them is
/// exactly how a value ends up with the wrong lifetime.
///
/// It is *not* what every protocol exposes. Only the memcached adapter passes a
/// client's TTL through untranslated, because only memcached clients expect the
/// overloading; VCP and RESP take an offset at every magnitude and convert at
/// their adapter, through [`Clock::ttl_from_offset`] and `ttl_from_deadline`
/// respectively.
pub const MAX_TTL_SECS: u32 = 60 * 60 * 24 * 30;

/// TTL sentinel meaning "already expired".
///
/// memcached's `exptime` accepts a negative number to store something
/// pre-expired, which a `u32` cannot represent, so the protocol adapters
/// translate it to this. It costs the ability to express an absolute expiry in
/// 2106, which is not a trade worth worrying about.
pub const TTL_ALREADY_EXPIRED: u32 = u32::MAX;

/// Source of wall-clock time for expiry decisions.
///
/// Deliberately a struct rather than a bare function: expiry is evaluated on
/// every read, so if profiling later shows the syscall matters this becomes a
/// coarse clock (an `AtomicU64` refreshed by a ticker) without touching a single
/// call site. It reads the system clock directly for now — roughly 20ns via the
/// vDSO, which is noise next to a B-tree descent — because a cached clock that
/// stops ticking fails silently and serves expired data.
#[derive(Clone, Copy, Debug, Default)]
pub struct Clock;

impl Clock {
    pub const fn new() -> Self {
        Self
    }

    /// Milliseconds since the unix epoch.
    #[inline]
    pub fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Converts a TTL into the absolute expiry stamp stored in a record.
    ///
    /// The field is overloaded exactly as memcached overloads `exptime`:
    ///
    /// - `0` means never, encoded as [`NEVER`].
    /// - [`TTL_ALREADY_EXPIRED`] stores something pre-expired.
    /// - Anything above [`MAX_TTL_SECS`] is an **absolute unix timestamp**, not
    ///   an offset. Clamping it to 30 days instead — as this used to — silently
    ///   gives a value a lifetime the client never asked for.
    /// - Anything else is a relative offset in seconds.
    ///
    /// [`NEVER`]: crate::record::NEVER
    #[inline]
    pub fn expiry_from_ttl(&self, ttl_secs: u32) -> u64 {
        match ttl_secs {
            0 => crate::record::NEVER,
            // Must be tested before the absolute-timestamp branch, which it
            // would otherwise fall into.
            TTL_ALREADY_EXPIRED => ALREADY_EXPIRED_MS,
            secs if secs > MAX_TTL_SECS => (secs as u64) * 1000,
            secs => self.now_ms() + (secs as u64) * 1000,
        }
    }

    /// Encodes a plain relative offset in seconds as the `ttl_secs` above.
    ///
    /// The protocols that do not share memcached's overloading pass their TTL
    /// through here: an offset past [`MAX_TTL_SECS`] is handed on as the
    /// absolute stamp it means, rather than being read back as a date in 1970
    /// and expiring the value on the spot.
    ///
    /// An offset at or below the threshold is already unambiguous and passes
    /// through untouched, which keeps the clock out of the common path.
    #[inline]
    pub fn ttl_from_offset(&self, offset_secs: u32) -> u32 {
        if offset_secs <= MAX_TTL_SECS {
            return offset_secs;
        }

        // Rounded up, so the deadline is never *shorter* than what was asked
        // for. At these magnitudes the millisecond is noise either way.
        let stamp = self.now_ms().div_ceil(1000) + offset_secs as u64;

        // `u32::MAX` is the already-expired sentinel, so the furthest offset
        // this can express lands one second short of it, in 2106.
        u32::try_from(stamp).unwrap_or(u32::MAX).min(u32::MAX - 1)
    }
}

/// Expiry stamp used for "already expired": the earliest value that is not
/// [`NEVER`], so it is always in the past.
///
/// [`NEVER`]: crate::record::NEVER
const ALREADY_EXPIRED_MS: u64 = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::NEVER;

    #[test]
    fn zero_ttl_means_never() {
        assert_eq!(Clock::new().expiry_from_ttl(0), NEVER);
    }

    #[test]
    fn ttl_is_relative_to_now() {
        let clock = Clock::new();
        let before = clock.now_ms();
        let expiry = clock.expiry_from_ttl(60);
        assert!(expiry >= before + 60_000);
        assert!(expiry <= clock.now_ms() + 60_000);
    }

    #[test]
    fn the_already_expired_sentinel_lands_in_the_past() {
        // memcached's negative exptime. Clamping it to the maximum TTL instead
        // would store the value for 30 days — the exact opposite of the ask.
        let clock = Clock::new();
        let expiry = clock.expiry_from_ttl(TTL_ALREADY_EXPIRED);

        assert_ne!(expiry, NEVER);
        assert!(expiry < clock.now_ms(), "must already be expired");
    }

    #[test]
    fn a_ttl_beyond_thirty_days_is_an_absolute_timestamp() {
        let clock = Clock::new();
        // 2033-05-18, comfortably past the 30-day threshold.
        let stamp = 2_000_000_000u32;
        assert_eq!(clock.expiry_from_ttl(stamp), stamp as u64 * 1000);

        // And a value just under the threshold is still relative.
        let relative = clock.expiry_from_ttl(MAX_TTL_SECS);
        assert!(relative >= clock.now_ms());
    }

    #[test]
    fn a_short_offset_needs_no_translation() {
        let clock = Clock::new();
        assert_eq!(clock.ttl_from_offset(0), 0);
        assert_eq!(clock.ttl_from_offset(60), 60);
        assert_eq!(clock.ttl_from_offset(MAX_TTL_SECS), MAX_TTL_SECS);
    }

    #[test]
    fn a_long_offset_becomes_the_stamp_it_means() {
        let clock = Clock::new();
        // 90 days: a perfectly ordinary TTL, and one that would otherwise be
        // read as a timestamp in April 1970.
        let offset = 90 * 24 * 60 * 60;
        let before = clock.now_ms();
        let expiry = clock.expiry_from_ttl(clock.ttl_from_offset(offset));

        assert!(expiry >= before + offset as u64 * 1000);
        assert!(expiry <= clock.now_ms() + (offset as u64 + 1) * 1000);
    }

    #[test]
    fn the_furthest_offset_stops_short_of_the_expired_sentinel() {
        let clock = Clock::new();
        let encoded = clock.ttl_from_offset(u32::MAX);

        assert_ne!(encoded, TTL_ALREADY_EXPIRED, "must not read as expired");
        assert!(clock.expiry_from_ttl(encoded) > clock.now_ms());
    }
}
