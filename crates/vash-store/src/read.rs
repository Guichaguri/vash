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
use vash_core::{Key, RecordRef, Value, ValueRef};

use crate::backend::{Backend, ReadTxn};
use crate::engine::Engine;
use crate::env::TrackedTxn;
use crate::error::Result;
use crate::tags::TagLookup;

impl<B: Backend> Engine<B> {
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
        txn: &'txn B::RoTxn<'_>,
        tags: &mut LazyTags<'_>,
        at: Snapshot,
        key: &[u8],
        project: impl FnOnce(&RecordRef<'txn>) -> T,
    ) -> Result<Option<T>> {
        let Some(blob) = txn.get(self.main, key)? else {
            return Ok(None);
        };

        let record = RecordRef::parse(blob)?;

        // Taken only when this record actually carries tags — see
        // [`RecordRef::needs_tag_registry`]. `None` here means `is_alive` will
        // not call the closure at all, so what it would have returned cannot
        // matter; answering `None` from it keeps that total rather than
        // resting on an `expect` that is unreachable by construction.
        let lookup = record.needs_tag_registry().then(|| tags.lookup());
        if !record.is_alive(at.now_ms, at.epoch, |id| {
            lookup.and_then(|held| held.generation(id))
        }) {
            // Expired, flushed or tag-invalidated: logically absent. Reclaiming
            // the space belongs to the sweeper and the reclaimer; a read never
            // writes.
            return Ok(None);
        }

        Ok(Some(project(&record)))
    }

    fn read_record(
        &self,
        txn: &B::RoTxn<'_>,
        tags: &mut LazyTags<'_>,
        at: Snapshot,
        key: &[u8],
    ) -> Result<Option<Value>> {
        self.read_alive(txn, tags, at, key, |record| Value {
            data: Bytes::copy_from_slice(record.value),
            mc_flags: record.mc_flags(),
            cas: record.cas(),
            expires_at_ms: Some(record.expires_at_ms()),
        })
    }

    pub fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        let rtxn = self.read_txn()?;
        let mut tags = LazyTags::new(&self.tags);
        let at = self.snapshot(&rtxn);
        self.read_record(&rtxn, &mut tags, at, key.as_bytes())
    }

    /// Runs `project` against a live record **while the read transaction is
    /// still open**, so the value can be encoded straight out of the map
    /// instead of being copied into a [`Value`] first.
    ///
    /// This is the shape plan §8 wanted and M6 recorded as not taken. It is
    /// here to be measured before it is adopted: the copy it removes is not
    /// free to remove, because it is also what pulls the value into cache for
    /// whatever copies it next. See `hot_path.rs`.
    pub fn get_with<R>(
        &self,
        key: Key<'_>,
        project: impl FnOnce(ValueRef<'_>) -> R,
    ) -> Result<Option<R>> {
        let rtxn = self.read_txn()?;
        let mut tags = LazyTags::new(&self.tags);
        let at = self.snapshot(&rtxn);
        self.read_alive(&rtxn, &mut tags, at, key.as_bytes(), |record| {
            project(ValueRef {
                data: record.value,
                mc_flags: record.mc_flags(),
                cas: record.cas(),
                expires_at_ms: Some(record.expires_at_ms()),
            })
        })
    }

    /// Opens the reader a batch reuses for every key that lands on this shard.
    ///
    /// **The half of [`Engine::get_with`] that a sharded batch needs.**
    /// `get_with` opens a transaction, reads one key and drops it, which is
    /// right for a single `GET` and wrong for a hundred-key `MGET`: the batch
    /// wants the transaction open across all of its keys, so that the values can
    /// be rendered straight out of the map in *request* order rather than
    /// collected into a `Vec<Option<Value>>` and put back in order afterwards.
    /// See `docs/performance-proposals.md` §18.
    pub fn batch_reader(&self) -> Result<BatchReader<'_, B>> {
        let txn = self.read_txn()?;
        let at = self.snapshot(&txn);
        Ok(BatchReader {
            engine: self,
            txn,
            tags: LazyTags::new(&self.tags),
            at,
        })
    }

    /// Resolves a whole batch inside one read transaction, so every key in a
    /// `GET_MANY` sees the same consistent snapshot — and under at most one tag
    /// registry lock rather than one per key. At most, because a batch in which
    /// no key carries tags takes none at all: see [`LazyTags`].
    pub fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        let rtxn = self.read_txn()?;
        let mut tags = LazyTags::new(&self.tags);
        let at = self.snapshot(&rtxn);
        keys.iter()
            .map(|key| self.read_record(&rtxn, &mut tags, at, key.as_bytes()))
            .collect()
    }

    /// A live key's deadline, without copying its value.
    ///
    /// `None` means not live; `Some(NEVER)` means live with no expiry.
    pub fn deadline(&self, key: Key<'_>) -> Result<Option<u64>> {
        let rtxn = self.read_txn()?;
        let mut tags = LazyTags::new(&self.tags);
        let at = self.snapshot(&rtxn);
        self.read_alive(
            &rtxn,
            &mut tags,
            at,
            key.as_bytes(),
            RecordRef::expires_at_ms,
        )
    }

    /// [`Engine::deadline`] over a batch, against one snapshot.
    pub fn deadlines(&self, keys: &[Key<'_>]) -> Result<Vec<Option<u64>>> {
        let rtxn = self.read_txn()?;
        let mut tags = LazyTags::new(&self.tags);
        let at = self.snapshot(&rtxn);
        keys.iter()
            .map(|key| {
                self.read_alive(
                    &rtxn,
                    &mut tags,
                    at,
                    key.as_bytes(),
                    RecordRef::expires_at_ms,
                )
            })
            .collect()
    }

    /// The RAM-resident half of the liveness check, read once per call.
    ///
    /// The clock comes off the transaction rather than being read here: it was
    /// taken when the transaction opened, which is the instant this snapshot
    /// describes. See [`TrackedTxn::opened_at_ms`].
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
    fn snapshot(&self, txn: &TrackedTxn<'_, B>) -> Snapshot {
        Snapshot {
            now_ms: txn.opened_at_ms(),
            epoch: self.epoch(),
        }
    }
}

/// One shard's open read transaction, reused across the keys of a batch that
/// land on it.
///
/// Holds the three things a read needs and a batch should only establish once:
/// the transaction, the snapshot it is judged against, and the tag registry
/// lock — still taken lazily, so a batch touching no tagged record takes none.
///
/// **This is a reader held open across several keys**, which `read.rs` opens by
/// warning against. The warning is about *long-lived* readers pinning a version
/// and starving page reuse — a listing scan, bounded by `listing_max_scan`. A
/// batch is bounded by `MAX_BATCH_ITEMS` lookups against a map that is usually
/// resident, and `oldest_reader_age_ms` is what says so if that judgement is
/// ever wrong.
pub struct BatchReader<'e, B: Backend> {
    engine: &'e Engine<B>,
    txn: TrackedTxn<'e, B>,
    tags: LazyTags<'e>,
    at: Snapshot,
}

impl<B: Backend> BatchReader<'_, B> {
    /// Runs `project` against a live record without copying its value, exactly
    /// as [`Engine::get_with`] does — but on a transaction that outlives the
    /// call, so the next key in the batch reuses it.
    ///
    /// `None` means the key is absent or not live. The caller renders that in
    /// its own dialect, which is the same division of labour `get_with` already
    /// has.
    pub fn get_with<R>(
        &mut self,
        key: Key<'_>,
        project: impl FnOnce(ValueRef<'_>) -> R,
    ) -> Result<Option<R>> {
        self.engine.read_alive(
            &self.txn,
            &mut self.tags,
            self.at,
            key.as_bytes(),
            |record| {
                project(ValueRef {
                    data: record.value,
                    mc_flags: record.mc_flags(),
                    cas: record.cas(),
                    expires_at_ms: Some(record.expires_at_ms()),
                })
            },
        )
    }
}

/// The clock and flush epoch a batch of keys is judged against.
#[derive(Clone, Copy)]
struct Snapshot {
    now_ms: u64,
    epoch: u32,
}

/// The tag registry's read lock, taken at most once per call and only if some
/// record in it actually carries tags.
///
/// **Why it is worth deferring.** The lock is shared across every reader on the
/// shard, and acquiring it for reading is still a read-modify-write on one
/// word — so taking it per read puts a contended cache line in front of every
/// `GET` the server serves, whether or not the workload uses tags at all. A
/// cache that never invalidates by tag stores no tags on any record and so
/// never needed it once.
///
/// **Why it is still hoisted.** A batch keeps the property the eager version
/// had — one acquisition for the whole call rather than one per key — because
/// the same holder is threaded through every key. What changes is only that the
/// first key carrying tags takes it, instead of the call taking it up front.
/// Zero acquisitions when no key has tags, one when any does; never more.
struct LazyTags<'a> {
    registry: &'a crate::tags::TagRegistry,
    held: Option<TagLookup<'a>>,
}

impl<'a> LazyTags<'a> {
    fn new(registry: &'a crate::tags::TagRegistry) -> Self {
        Self {
            registry,
            held: None,
        }
    }

    #[inline]
    fn lookup(&mut self) -> &TagLookup<'a> {
        let registry = self.registry;
        self.held.get_or_insert_with(|| registry.lookup())
    }
}
