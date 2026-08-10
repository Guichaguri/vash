use crate::error::{CoreError, Result};

/// Default maximum value size, matching memcached's default item size limit so
/// that a client migrating over does not discover a new failure mode.
///
/// The KCP protocol has no inherent limit; this is policy, and configurable.
pub const DEFAULT_MAX_VALUE_LEN: usize = 1024 * 1024;

/// Hard ceiling on the configurable limit. LMDB stores the whole value in a
/// single record, so very large values defeat the page cache and make every
/// write copy a lot of pages.
pub const ABSOLUTE_MAX_VALUE_LEN: usize = 64 * 1024 * 1024;

pub fn validate_value(value: &[u8], max: usize) -> Result<()> {
    if value.len() > max {
        return Err(CoreError::ValueTooLarge {
            len: value.len(),
            max,
        });
    }
    Ok(())
}
