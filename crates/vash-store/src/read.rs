//! Reading records.
//!
//! Every read here opens its own short transaction and drops it before
//! returning. That is deliberate and is the number one LMDB operational
//! footgun avoided: a long-lived read transaction pins a version and stops the
//! writer reusing freed pages, so the file grows without bound (plan §9).
//!
//! Liveness is decided from RAM alone — the coarse clock, the flush epoch and
//! the tag registry — so a read never needs a second lookup to know whether a
//! record still counts. See [`vash_core::RecordRef::is_alive`].

use bytes::Bytes;
use heed::{AnyTls, RoTxn};
use vash_core::{Key, RecordRef, Value};

use crate::engine::LmdbEngine;
use crate::error::{Result, StoreError};
use crate::tags::TagLookup;

impl LmdbEngine {
    /// Looks a key up, applies the liveness check, and hands the caller
    /// whatever it needs off the record.
    ///
    /// Generic over the projection so the reads that want only the header —
    /// `EXISTS`, `TYPE`, `TTL`, `PERSIST`, `EXPIRE` — share one definition of
    /// "live" with `GET` without also paying for `GET`'s copy of the value.
    /// `at` is the snapshot every key in one call is judged against — see
    /// [`Snapshot`].
    fn read_alive<'txn, T>(
        &self,
        txn: &'txn RoTxn<'_, AnyTls>,
        lookup: &TagLookup<'_>,
        at: Snapshot,
        key: &[u8],
        project: impl FnOnce(&RecordRef<'txn>) -> T,
    ) -> Result<Option<T>> {
        let Some(blob) = self.main.get(txn, key).map_err(StoreError::from_heed)? else {
            return Ok(None);
        };

        let record = RecordRef::parse(blob)?;
        if !record.is_alive(at.now_ms, at.epoch, |id| lookup.generation(id)) {
            // Expired, flushed or tag-invalidated: logically absent. Reclaiming
            // the space belongs to the sweeper and the reclaimer; a read never
            // writes.
            return Ok(None);
        }

        Ok(Some(project(&record)))
    }

    fn read_record(
        &self,
        txn: &RoTxn<'_, AnyTls>,
        lookup: &TagLookup<'_>,
        at: Snapshot,
        key: &[u8],
    ) -> Result<Option<Value>> {
        self.read_alive(txn, lookup, at, key, |record| Value {
            data: Bytes::copy_from_slice(record.value),
            mc_flags: record.mc_flags(),
            cas: record.cas(),
            expires_at_ms: Some(record.expires_at_ms()),
        })
    }

    pub fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        let rtxn = self.read_txn()?;
        let lookup = self.tags.lookup();
        self.read_record(&rtxn, &lookup, self.snapshot(), key.as_bytes())
    }

    /// Resolves a whole batch inside one read transaction, so every key in a
    /// `GET_MANY` sees the same consistent snapshot â€” and under a single tag
    /// registry lock rather than one per key.
    pub fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        let rtxn = self.read_txn()?;
        let lookup = self.tags.lookup();
        let at = self.snapshot();
        keys.iter()
            .map(|key| self.read_record(&rtxn, &lookup, at, key.as_bytes()))
            .collect()
    }

    /// A live key's deadline, without copying its value.
    ///
    /// `None` means not live; `Some(NEVER)` means live with no expiry.
    pub fn deadline(&self, key: Key<'_>) -> Result<Option<u64>> {
        let rtxn = self.read_txn()?;
        let lookup = self.tags.lookup();
        self.read_alive(
            &rtxn,
            &lookup,
            self.snapshot(),
            key.as_bytes(),
            RecordRef::expires_at_ms,
        )
    }

    /// [`Engine::deadline`] over a batch, against one snapshot.
    pub fn deadlines(&self, keys: &[Key<'_>]) -> Result<Vec<Option<u64>>> {
        let rtxn = self.read_txn()?;
        let lookup = self.tags.lookup();
        let at = self.snapshot();
        keys.iter()
            .map(|key| {
                self.read_alive(&rtxn, &lookup, at, key.as_bytes(), RecordRef::expires_at_ms)
            })
            .collect()
    }

    /// The RAM-resident half of the liveness check, read once per call.
    ///
    /// Both halves are already fixed for the length of a call — the read
    /// transaction pins its own view of the data, and a batch is documented to
    /// see one consistent snapshot — so reading them per key bought nothing and
    /// cost a `SystemTime::now` and an acquire load each time round. On a
    /// hundred-key `GET_MANY` that was a hundred clock calls to answer a
    /// question whose answer had not moved.
    ///
    /// It also makes the guarantee load-bearing rather than incidental: with the
    /// clock read per key, a batch spanning a millisecond boundary could count
    /// one key live and the next one expired against the *same* deadline.
    #[inline]
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            now_ms: self.now_ms(),
            epoch: self.epoch(),
        }
    }
}

/// The clock and flush epoch a batch of keys is judged against.
#[derive(Clone, Copy)]
struct Snapshot {
    now_ms: u64,
    epoch: u32,
}
