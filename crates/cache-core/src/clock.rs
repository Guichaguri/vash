use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum TTL accepted, in seconds (30 days).
///
/// Memcached treats a TTL above this as an absolute unix timestamp rather than
/// a relative offset. We keep the same threshold so the memcached adapter can
/// implement that rule without a second, inconsistent constant.
pub const MAX_TTL_SECS: u32 = 60 * 60 * 24 * 30;

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

    /// Converts a relative TTL in seconds into the absolute expiry stamp stored
    /// in a record. A TTL of zero means "never", encoded as [`NEVER`].
    ///
    /// [`NEVER`]: crate::record::NEVER
    #[inline]
    pub fn expiry_from_ttl(&self, ttl_secs: u32) -> u64 {
        if ttl_secs == 0 {
            crate::record::NEVER
        } else {
            self.now_ms() + (ttl_secs.min(MAX_TTL_SECS) as u64) * 1000
        }
    }
}

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
    fn ttl_is_clamped_to_the_maximum() {
        let clock = Clock::new();
        let capped = clock.expiry_from_ttl(u32::MAX);
        assert!(capped <= clock.now_ms() + (MAX_TTL_SECS as u64) * 1000);
    }
}
