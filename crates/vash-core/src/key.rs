use crate::error::{CoreError, Result};

/// Maximum key length, in bytes.
///
/// This is LMDB's `MDB_MAXKEYSIZE`, which is a **compile-time** constant in the
/// C library (511 for the default build) and therefore cannot be raised at
/// runtime. `heed` exposes a `longer-keys` feature that rebuilds LMDB with a
/// larger value, but doing so changes the on-disk format, so the limit is
/// pinned here deliberately rather than inherited implicitly.
///
/// Memcached's own limit is 250 bytes, so this is strictly more permissive and
/// every memcached-legal key fits.
pub const MAX_KEY_LEN: usize = 511;

/// A key that has been validated against the universal constraints.
///
/// Holding one of these is proof the bytes are non-empty and short enough to be
/// used as an LMDB key. Protocol adapters may impose stricter rules on top
/// (memcached, for instance, forbids spaces and control characters); those live
/// in the adapter, not here.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key<'a>(&'a [u8]);

impl<'a> Key<'a> {
    pub fn new(bytes: &'a [u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(CoreError::EmptyKey);
        }
        if bytes.len() > MAX_KEY_LEN {
            return Err(CoreError::KeyTooLong {
                len: bytes.len(),
                max: MAX_KEY_LEN,
            });
        }
        Ok(Self(bytes))
    }

    /// Bypasses validation. Only for bytes that came out of the store, which
    /// were validated on the way in.
    pub fn from_stored(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        false // guaranteed by the constructor
    }
}

impl std::ops::Deref for Key<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.0
    }
}

impl std::fmt::Debug for Key<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match std::str::from_utf8(self.0) {
            Ok(s) => write!(f, "Key({s:?})"),
            Err(_) => write!(f, "Key(<{} bytes>)", self.0.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty() {
        assert_eq!(Key::new(b"").unwrap_err(), CoreError::EmptyKey);
    }

    #[test]
    fn accepts_max_length_and_rejects_one_more() {
        assert!(Key::new(&[b'k'; MAX_KEY_LEN]).is_ok());
        assert!(matches!(
            Key::new(&[b'k'; MAX_KEY_LEN + 1]).unwrap_err(),
            CoreError::KeyTooLong { .. }
        ));
    }

    #[test]
    fn accepts_arbitrary_bytes() {
        // VCP keys are binary-safe; only the memcached adapter restricts the charset.
        assert!(Key::new(&[0x00, 0xff, b' ', b'\n']).is_ok());
    }
}
