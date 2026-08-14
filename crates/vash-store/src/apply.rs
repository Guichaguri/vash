//! The record operations the writer thread applies.
//!
//! Every function here takes the caller's `RwTxn` rather than opening one, which
//! is what lets [`crate::writer`] pack many of them into a single commit. None
//! of them spawns anything, blocks on anything, or knows that a writer thread
//! exists.
//!
//! They share three private helpers, and the sharing is the point:
//! [`LmdbEngine::displace`] performs the one lookup an overwrite has to do
//! anyway and hands back everything three different callers need from it;
//! [`LmdbEngine::rewrite`] is the encode-and-store tail they all end in; and
//! [`LmdbEngine::deadline_after`] is the single definition of what each
//! [`TtlChange`] means against a record.
//!
//! [`TtlChange`]: vash_core::TtlChange

use std::sync::atomic::Ordering;

use crate::schema::{CAS_BLOCK, meta_key};

use bytes::Bytes;
use vash_core::{
    Applied, Arithmetic, ExpireGuard, RecordMeta, RecordRef, Set, SetMode, Stored, TagRef,
    TtlChange, Value, arith, encode_record, patch_cas, patch_expiry, record::NEVER, validate_value,
};

use crate::engine::{LmdbEngine, Pressure};
use crate::error::{Result, StoreError};
use crate::reclaim;

use heed::RwTxn;

/// A record encoded and ready to store, still missing its CAS token.
///
/// Built on the calling thread so that the value copy and the record framing â€”
/// the expensive part of a write â€” happen in parallel across connections, and
/// the single writer thread is left with only the B-tree work.
pub struct PreparedSet {
    pub key: Box<[u8]>,
    pub record: Vec<u8>,
    /// The resolved deadline, or `None` when [`Self::ttl`] could only be settled
    /// against the record being replaced — which is the writer's job.
    pub expires_at_ms: Option<u64>,
    pub ttl: TtlChange,
    pub tags: Vec<TagRef>,
    /// Capture what the key held before replacing it (Redis `SET … GET`).
    pub return_previous: bool,
}

/// What a guarded write did, and what it displaced.
#[derive(Debug, Clone)]
pub struct Written {
    pub outcome: Stored,
    /// The value the key held beforehand, when the write asked for it. Always
    /// `None` otherwise, which is why the caller has to know what it requested.
    pub previous: Option<Value>,
}

/// What the record an overwrite replaced was holding.
#[derive(Debug, Default)]
struct Displaced {
    /// Whether a record was there and a client would have been served it.
    ///
    /// Distinct from `deadline.is_some()`, which happens to carry the same
    /// answer today only because every live record has a deadline field — a
    /// coincidence, and not one a caller should have to know about.
    live: bool,
    /// Its deadline, or `None` if there was no live record to take one from.
    deadline: Option<u64>,
    /// Its value, captured only when the caller asked for it.
    previous: Option<Value>,
}

/// The result of storing a prepared record.
#[derive(Debug)]
pub struct SetApplied {
    pub cas: u64,
    /// What the write displaced, when it asked to be told.
    pub previous: Option<Value>,
}

impl LmdbEngine {
    pub fn prepare_set(&self, set: &Set<'_>, tags: Vec<TagRef>) -> Result<PreparedSet> {
        // Refused here, on the caller's thread, rather than after a trip
        // through the write queue: there is no point encoding a record and
        // queueing it for a store that cannot accept it.
        if self.pressure() == Pressure::Critical {
            return Err(StoreError::CapacityFull);
        }
        validate_value(set.value, self.max_value_len)?;

        // Resolved here when it can be, and left to the writer when it cannot.
        // `Keep` needs the record's current deadline, which only the writer's
        // transaction can read consistently — and which it reads anyway, so
        // deferring costs nothing. See [`Self::apply_set`].
        let expires_at_ms = match set.ttl {
            TtlChange::Set(ttl_secs) => Some(self.expiry_from_ttl(ttl_secs)),
            TtlChange::Keep | TtlChange::SetIfPersistent(_) => None,
        };
        let meta = RecordMeta {
            epoch: self.epoch(),
            mc_flags: set.mc_flags,
            // Patched by the writer along with the CAS token when the deadline
            // was not resolvable here.
            expires_at_ms: expires_at_ms.unwrap_or(NEVER),
            // Stamped by the writer in commit order; see `patch_cas`.
            cas: 0,
        };

        let mut record = Vec::with_capacity(vash_core::record_len(tags.len(), set.value.len()));
        encode_record(&mut record, meta, &tags, set.value)?;

        Ok(PreparedSet {
            key: set.key.as_bytes().into(),
            record,
            expires_at_ms,
            ttl: set.ttl,
            tags,
            return_previous: set.return_previous,
        })
    }

    /// Allocates the next CAS token, extending the durable reservation when the
    /// current block runs out. Called only from the writer thread, whose single
    /// transaction serialises it.
    fn next_cas(&self, wtxn: &mut RwTxn) -> Result<u64> {
        let counter = self.cas_next.fetch_add(1, Ordering::Relaxed) + 1;
        if counter >= self.cas_watermark.load(Ordering::Relaxed) {
            let watermark = counter + CAS_BLOCK;
            self.meta
                .put(wtxn, meta_key::CAS_WATERMARK, &watermark.to_le_bytes())
                .map_err(StoreError::from_heed)?;
            self.cas_watermark.store(watermark, Ordering::Relaxed);
        }

        // Each shard counts independently, so the raw counters collide across
        // shards. Striping them keeps every token unique server-wide, which is
        // what clients are told to expect, while staying strictly increasing
        // within a shard â€” and therefore within any single key, which is the
        // only ordering compare-and-swap depends on.
        Ok(counter * self.shard_count as u64 + self.shard_index as u64)
    }

    /// Clears the index entries of whatever is stored under `key`, and reports
    /// what it held.
    ///
    /// Without the clearing, overwriting a key would leave its old expiry and
    /// tag index entries behind, and a hot key rewritten every second would
    /// accumulate dead entries until its buckets came due.
    ///
    /// The reporting rides along because **every overwrite has to read the
    /// record it replaces anyway**. One lookup then serves three jobs: dropping
    /// the stale index rows, resolving [`TtlChange::Keep`], and capturing the
    /// previous value for `SET … GET`. Doing the latter two in the caller
    /// instead would be two more descents of the same B-tree, and — the reason
    /// this exists — they would be *outside* the write transaction, which is
    /// exactly where those two commands used to lose a race.
    fn displace(&self, wtxn: &mut RwTxn, key: &[u8], want_value: bool) -> Result<Displaced> {
        let Some(blob) = self.main.get(wtxn, key).map_err(StoreError::from_heed)? else {
            return Ok(Displaced::default());
        };
        let record = RecordRef::parse(blob)?;

        let expires_at_ms = record.expires_at_ms();
        let cas = record.cas();
        let tag_ids: Vec<u32> = record.tags.iter().map(|t| t.tag_id.get()).collect();

        // Everything the caller learns is gathered before the deletes below,
        // because those need the transaction mutably and would end this borrow
        // of the memory map.
        let displaced = {
            let lookup = self.tags.lookup();
            let live = record.is_alive(self.now_ms(), self.epoch(), |id| lookup.generation(id));
            Displaced {
                live,
                // A record that is not live is already invisible to clients, so
                // it has no deadline worth keeping and no value worth reporting.
                deadline: live.then_some(expires_at_ms),
                previous: (live && want_value).then(|| Value {
                    data: Bytes::copy_from_slice(record.value),
                    mc_flags: record.mc_flags(),
                    cas,
                    expires_at_ms: Some(expires_at_ms),
                }),
            }
        };

        // Every record is indexed, including those that never expire, so the
        // delete is unconditional too.
        let index_key = crate::expiry::encode_key(expires_at_ms, cas, self.bucket_granularity_ms);
        self.exp
            .delete(wtxn, &index_key)
            .map_err(StoreError::from_heed)?;

        for tag_id in tag_ids {
            self.tagidx
                .delete(wtxn, &reclaim::index_key(tag_id, key))
                .map_err(StoreError::from_heed)?;
        }

        Ok(displaced)
    }

    /// Reads the record currently under `key`, if it is live.
    ///
    /// Every conditional write is judged against what a *client* would see, so
    /// an expired-but-unswept record counts as absent. Otherwise `add` would
    /// fail against a key that reads as a miss.
    fn live_record<'t>(&self, wtxn: &'t RwTxn, key: &[u8]) -> Result<Option<RecordRef<'t>>> {
        let Some(blob) = self.main.get(wtxn, key).map_err(StoreError::from_heed)? else {
            return Ok(None);
        };
        let record = RecordRef::parse(blob)?;

        let lookup = self.tags.lookup();
        if !record.is_alive(self.now_ms(), self.epoch(), |id| lookup.generation(id)) {
            return Ok(None);
        }
        Ok(Some(record))
    }

    /// Evaluates a conditional write's guard, and for the concatenating modes
    /// builds the combined record.
    ///
    /// Returns `Ok(None)` when the guard rejected the write.
    pub fn apply_conditional_set(
        &self,
        wtxn: &mut RwTxn,
        prepared: &mut PreparedSet,
        mode: SetMode,
    ) -> Result<Written> {
        // Only two callers ever read the displaced value: `SET … GET`, which
        // reports it, and the concatenating modes, which build on it. For
        // everything else — every `set`, `add`, `replace` and `cas` — copying it
        // out of the memory map would be a copy of the whole old value, taken on
        // the shard's single writer thread and thrown away unread. The header
        // fields below are what the guards actually judge, and they are three
        // integers.
        let wants_value =
            prepared.return_previous || matches!(mode, SetMode::Append | SetMode::Prepend);
        let existing = self.live_record(wtxn, &prepared.key)?.map(|r| {
            (
                r.cas(),
                r.mc_flags(),
                r.expires_at_ms(),
                wants_value.then(|| r.value.to_vec()),
            )
        });

        // A guard that refuses the write still has to answer `SET … GET`, which
        // reports what the key holds whether or not the write applied — that is
        // Redis's contract for `SET k v NX GET` on a key that already exists.
        let refused = |outcome: Stored| -> Result<Written> {
            Ok(Written {
                outcome,
                // The value is present exactly when `return_previous` asked for
                // it, so matching on both rather than unwrapping keeps this
                // total: there is no arm here that can panic if the two ever
                // drift apart.
                previous: match (&existing, prepared.return_previous) {
                    (Some((cas, mc_flags, expires_at_ms, Some(value))), true) => Some(Value {
                        data: Bytes::copy_from_slice(value),
                        mc_flags: *mc_flags,
                        cas: *cas,
                        expires_at_ms: Some(*expires_at_ms),
                    }),
                    _ => None,
                },
            })
        };

        match (mode, &existing) {
            (SetMode::Set, _) => {}
            (SetMode::Add, Some(_)) => return refused(Stored::NotStored),
            (SetMode::Add, None) => {}
            (SetMode::Replace, None) => return refused(Stored::NotStored),
            (SetMode::Replace, Some(_)) => {}
            (SetMode::Append | SetMode::Prepend, None) => return refused(Stored::NotStored),
            (SetMode::Cas(_), None) => return refused(Stored::NotFound),
            (SetMode::Cas(expected), Some((cas, ..))) if *cas != expected => {
                return refused(Stored::Exists);
            }
            (SetMode::Cas(_), Some(_)) => {}
            (SetMode::Append | SetMode::Prepend, Some(_)) => {}
        }

        // Concatenation keeps the stored value's TTL and client flags, matching
        // memcached: append/prepend carry no metadata of their own.
        if matches!(mode, SetMode::Append | SetMode::Prepend) {
            // The value is captured for exactly these two modes, so it is
            // present by the same construction the match above relies on.
            let (_, mc_flags, expires_at_ms, Some(current)) =
                existing.expect("guarded above: concatenation requires an existing record")
            else {
                return Err(StoreError::Corrupt(
                    "concatenation reached the writer without the value it builds on".into(),
                ));
            };

            let addition = RecordRef::parse(&prepared.record)?.value.to_vec();
            let mut combined = Vec::with_capacity(current.len() + addition.len());
            if mode == SetMode::Append {
                combined.extend_from_slice(&current);
                combined.extend_from_slice(&addition);
            } else {
                combined.extend_from_slice(&addition);
                combined.extend_from_slice(&current);
            }

            validate_value(&combined, self.max_value_len)?;

            let mut record =
                Vec::with_capacity(vash_core::record_len(prepared.tags.len(), combined.len()));
            encode_record(
                &mut record,
                RecordMeta {
                    epoch: self.epoch(),
                    mc_flags,
                    expires_at_ms,
                    cas: 0,
                },
                &prepared.tags,
                &combined,
            )?;
            prepared.record = record;
            // Settled from the record being concatenated onto, so `apply_set`
            // has nothing left to resolve.
            prepared.expires_at_ms = Some(expires_at_ms);
        }

        let applied = self.apply_set(wtxn, prepared)?;
        Ok(Written {
            outcome: Stored::Stored(applied.cas),
            previous: applied.previous,
        })
    }

    /// Stores a replacement for a record, keeping the tag table it is given.
    ///
    /// The tail every in-place rewrite shares. An LMDB value is an immutable
    /// blob, so "changing" a record means encoding a whole new one — there is no
    /// patching a stored value in place.
    ///
    /// `value` must not borrow from `wtxn`: this takes the transaction mutably,
    /// so anything still pointing into the memory map would keep a shared borrow
    /// alive across the write. That is why [`Self::apply_touch`] does not use it
    /// — its value is a map slice, and copying it out to satisfy this helper
    /// would be a real cost for a cosmetic gain.
    fn rewrite(
        &self,
        wtxn: &mut RwTxn,
        key: &[u8],
        tags: Vec<TagRef>,
        meta: RecordMeta,
        value: &[u8],
    ) -> Result<u64> {
        let mut record = Vec::with_capacity(vash_core::record_len(tags.len(), value.len()));
        encode_record(&mut record, meta, &tags, value)?;

        let mut prepared = PreparedSet {
            key: key.into(),
            record,
            // Already settled by the caller, which had the record in hand.
            expires_at_ms: Some(meta.expires_at_ms),
            ttl: TtlChange::Keep,
            tags,
            return_previous: false,
        };
        self.apply_set(wtxn, &mut prepared)
            .map(|applied| applied.cas)
    }

    /// Changes a key's deadline under a guard, atomically.
    ///
    /// Redis's `EXPIRE`/`EXPIREAT`/`PERSIST`. The guard is evaluated against the
    /// deadline the record holds **inside this transaction**, so a `GT`/`LT`
    /// comparison cannot be decided against a deadline that has since moved —
    /// the seam this replaces read the deadline from the network tier and wrote
    /// back against it.
    ///
    /// A deadline that has already passed deletes the key outright rather than
    /// storing it pre-expired. Redis is explicit that the event is a `del`, and
    /// the record would be invisible either way; doing it here frees the space
    /// now instead of leaving it for the sweeper.
    pub fn apply_expire(
        &self,
        wtxn: &mut RwTxn,
        key: &[u8],
        ttl: TtlChange,
        guard: ExpireGuard,
    ) -> Result<bool> {
        let Some(record) = self.live_record(wtxn, key)? else {
            return Ok(false);
        };
        let current = record.expires_at_ms();
        let tags = record.tags.to_vec();
        let mc_flags = record.mc_flags();
        let epoch = record.header.epoch.get();

        let target = self.deadline_after(ttl, current);

        // A key with no deadline is infinitely far off, which is what makes `GT`
        // never apply to one and `LT` always apply.
        let existing = (current != NEVER).then_some(current);
        let applies = match guard {
            ExpireGuard::Always => true,
            ExpireGuard::IfPersistent => existing.is_none(),
            ExpireGuard::IfVolatile => existing.is_some(),
            ExpireGuard::IfLater => existing.is_some_and(|at| target != NEVER && target > at),
            ExpireGuard::IfEarlier => target != NEVER && existing.is_none_or(|at| target < at),
        };
        if !applies {
            return Ok(false);
        }

        if target != NEVER && target <= self.now_ms() {
            // `apply_delete` re-reads the key, which is the price of not holding
            // the borrow of the memory map across a mutable use of the
            // transaction. One extra lookup on a path that is deleting anyway.
            return self.apply_delete(wtxn, key);
        }

        // Re-encoded rather than patched: the value has to be copied out of the
        // map before the transaction is used mutably, and once it is copied the
        // ordinary encode path is the simpler one.
        let value = record.value.to_vec();
        self.rewrite(
            wtxn,
            key,
            tags,
            RecordMeta {
                epoch,
                mc_flags,
                expires_at_ms: target,
                cas: 0,
            },
            &value,
        )?;
        Ok(true)
    }

    /// The deadline a rewrite lands on, given what the record carries now.
    fn deadline_after(&self, change: TtlChange, current: u64) -> u64 {
        match change {
            // Preserved by not being touched, rather than by being read out and
            // written back — which is what lets `INCR` keep a key's lifetime
            // without anyone reading the deadline first.
            TtlChange::Keep => current,
            TtlChange::Set(ttl_secs) => self.expiry_from_ttl(ttl_secs),
            // `ENX`: a record that already has a deadline keeps it.
            TtlChange::SetIfPersistent(ttl_secs) => {
                if current == NEVER {
                    self.expiry_from_ttl(ttl_secs)
                } else {
                    current
                }
            }
        }
    }

    /// Applies an atomic read-modify-write to a counter.
    ///
    /// Evaluated **inside the caller's transaction**, which is the whole point:
    /// reading the current value and writing the new one are one step on the
    /// shard's single writer thread, so two clients incrementing the same key
    /// cannot lose an update. The Redis adapter used to do this as a `get`
    /// followed by a `set` from the network tier, where they could interleave.
    ///
    /// The arithmetic itself belongs to [`vash_core::arith`], which keeps every
    /// dialect's numeric rules in one place and leaves this function the storage
    /// around them.
    pub fn apply_arithmetic(
        &self,
        wtxn: &mut RwTxn,
        op: &Arithmetic<'_>,
    ) -> Result<Option<Applied>> {
        let key = op.key.as_bytes();
        let existing = self.live_record(wtxn, key)?;

        // Both readings of `existing` happen here so its borrow of the
        // transaction ends before the write below needs it mutably. The value is
        // handed to the evaluator as a **map slice**: the outcome owns whatever
        // it produces, so nothing is copied out of a record about to be replaced.
        let (outcome, carried) = match &existing {
            Some(record) => (
                op.evaluate(Some(record.value))?,
                (
                    record.tags.to_vec(),
                    record.mc_flags(),
                    record.expires_at_ms(),
                ),
            ),
            // Created here, if the operation creates at all: no tags, no client
            // flags, and no expiry unless one is asked for.
            None => (op.evaluate(None)?, (Vec::new(), 0, NEVER)),
        };

        let (text, applied) = match outcome {
            arith::Outcome::Missing => return Ok(None),
            // A bound held the value where it was, so **nothing is written** —
            // not the value, and not the deadline either.
            arith::Outcome::Unchanged(applied) => return Ok(Some(applied)),
            arith::Outcome::Store { text, applied } => (text, applied),
        };

        // A counter is short, but the configured ceiling applies to every path
        // that can grow a value, not only to the ones a client sends bytes on.
        validate_value(text.as_bytes(), self.max_value_len)?;

        let (tags, mc_flags, current_deadline) = carried;
        self.rewrite(
            wtxn,
            key,
            tags,
            RecordMeta {
                epoch: self.epoch(),
                mc_flags,
                expires_at_ms: self.deadline_after(op.ttl, current_deadline),
                cas: 0,
            },
            text.as_bytes(),
        )?;

        Ok(Some(applied))
    }

    /// Concatenates onto a value atomically, returning the new length.
    ///
    /// This is Redis's `APPEND`, which differs from memcached's in exactly one
    /// respect: it creates the key when absent, where memcached answers
    /// `NOT_STORED`. That case is [`Self::apply_conditional_set`]'s, through
    /// `SetMode::Append`, and the two stay separate rather than growing a flag
    /// because the memcached path also has to report a `Stored` verdict this one
    /// has no use for.
    ///
    /// The existing value is concatenated from a slice borrowed straight out of
    /// the memory map. The Redis adapter used to copy it out to the network tier,
    /// concatenate there and ship the result back, so appending one byte to a
    /// megabyte moved two megabytes between the tiers.
    pub fn apply_append(&self, wtxn: &mut RwTxn, key: &[u8], suffix: &[u8]) -> Result<u64> {
        let existing = self.live_record(wtxn, key)?;

        let (combined, tags, mc_flags, expires_at_ms) = match &existing {
            Some(record) => {
                let mut combined = Vec::with_capacity(record.value.len() + suffix.len());
                combined.extend_from_slice(record.value);
                combined.extend_from_slice(suffix);
                (
                    combined,
                    record.tags.to_vec(),
                    record.mc_flags(),
                    // Redis keeps the existing expiry: `APPEND` alters a value
                    // in place rather than replacing it.
                    record.expires_at_ms(),
                )
            }
            None => (suffix.to_vec(), Vec::new(), 0, NEVER),
        };

        validate_value(&combined, self.max_value_len)?;

        self.rewrite(
            wtxn,
            key,
            tags,
            RecordMeta {
                epoch: self.epoch(),
                mc_flags,
                expires_at_ms,
                cas: 0,
            },
            &combined,
        )?;

        Ok(combined.len() as u64)
    }

    pub fn apply_set(&self, wtxn: &mut RwTxn, prepared: &mut PreparedSet) -> Result<SetApplied> {
        let cas = self.next_cas(wtxn)?;
        patch_cas(&mut prepared.record, cas)?;

        let displaced = self.displace(wtxn, &prepared.key, prepared.return_previous)?;

        // A deadline `prepare_set` could not settle off-thread is settled here,
        // against the record this write is replacing, and patched into the
        // header that was encoded without it. This is what makes `KEEPTTL` keep
        // the deadline the key *actually* has at the moment of the write rather
        // than one read a moment earlier.
        let expires_at_ms = match prepared.expires_at_ms {
            Some(resolved) => resolved,
            None => {
                let resolved =
                    self.deadline_after(prepared.ttl, displaced.deadline.unwrap_or(NEVER));
                patch_expiry(&mut prepared.record, resolved)?;
                prepared.expires_at_ms = Some(resolved);
                resolved
            }
        };

        self.main
            .put(wtxn, &prepared.key, &prepared.record)
            .map_err(StoreError::from_heed)?;

        // Indexed unconditionally: a record outside this index cannot be chosen
        // as an eviction victim, so a cache of TTL-less keys would have nothing
        // to free under pressure. Never-expiring records sort last (see
        // `expiry::NEVER_BUCKET`), so they go only after everything with a TTL.
        let index_key = crate::expiry::encode_key(expires_at_ms, cas, self.bucket_granularity_ms);
        self.exp
            .put(wtxn, &index_key, &prepared.key)
            .map_err(StoreError::from_heed)?;

        for tag in &prepared.tags {
            self.tagidx
                .put(
                    wtxn,
                    &reclaim::index_key(tag.tag_id.get(), &prepared.key),
                    &prepared.key,
                )
                .map_err(StoreError::from_heed)?;
        }

        Ok(SetApplied {
            cas,
            previous: displaced.previous,
        })
    }

    /// Returns whether the key was live before the delete. A record that has
    /// expired but not yet been swept is already invisible to clients, so
    /// removing it counts as a miss even though it frees a row.
    pub fn apply_delete(&self, wtxn: &mut RwTxn, key: &[u8]) -> Result<bool> {
        // `displace` already reads the record and judges its liveness, on the
        // way to clearing its index rows. Asking it rather than repeating the
        // lookup halves the B-tree descents a delete costs.
        let displaced = self.displace(wtxn, key, false)?;
        self.main.delete(wtxn, key).map_err(StoreError::from_heed)?;

        Ok(displaced.live)
    }

    /// Re-stamps a record's expiry without the client resending the value.
    ///
    /// LMDB values are immutable blobs, so this rewrites the record â€” the value
    /// is copied within the transaction. That is the cost of `TOUCH` being a
    /// bandwidth optimisation rather than a storage one.
    pub fn apply_touch(&self, wtxn: &mut RwTxn, key: &[u8], ttl_secs: u32) -> Result<bool> {
        let now_ms = self.now_ms();
        let epoch = self.epoch();

        let Some(blob) = self.main.get(wtxn, key).map_err(StoreError::from_heed)? else {
            return Ok(false);
        };
        let record = RecordRef::parse(blob)?;
        {
            let lookup = self.tags.lookup();
            if !record.is_alive(now_ms, epoch, |id| lookup.generation(id)) {
                return Ok(false);
            }
        }

        let expires_at_ms = self.expiry_from_ttl(ttl_secs);
        let tags = record.tags.to_vec();
        let mut rewritten = Vec::with_capacity(blob.len());
        encode_record(
            &mut rewritten,
            RecordMeta {
                epoch: record.header.epoch.get(),
                mc_flags: record.mc_flags(),
                expires_at_ms,
                cas: 0,
            },
            &tags,
            record.value,
        )?;

        let mut prepared = PreparedSet {
            key: key.into(),
            record: rewritten,
            expires_at_ms: Some(expires_at_ms),
            ttl: TtlChange::Set(ttl_secs),
            tags,
            return_previous: false,
        };
        self.apply_set(wtxn, &mut prepared)?;

        Ok(true)
    }

    /// Empties the cache, returning the new flush epoch.
    ///
    /// The epoch bump and the clear do different jobs, and both are needed. The
    /// clear frees the space â€” an epoch bump alone would leak every record
    /// without a TTL, since nothing would ever come looking for them. The epoch
    /// closes the MVCC window: a read transaction opened before this commit
    /// still sees the old snapshot, and comparing those records against the new
    /// epoch is what stops them being served.
    pub fn apply_flush(&self, wtxn: &mut RwTxn) -> Result<u32> {
        let epoch = self.epoch().wrapping_add(1);
        self.meta
            .put(wtxn, meta_key::EPOCH, &epoch.to_le_bytes())
            .map_err(StoreError::from_heed)?;

        self.main.clear(wtxn).map_err(StoreError::from_heed)?;
        self.exp.clear(wtxn).map_err(StoreError::from_heed)?;
        self.tagidx.clear(wtxn).map_err(StoreError::from_heed)?;
        // Their index entries are gone, so the jobs have nothing left to walk.
        self.jobs.clear(wtxn).map_err(StoreError::from_heed)?;

        // Tag registrations survive: a flush empties the data, it does not
        // un-declare the tags, and their generations must keep advancing.
        Ok(epoch)
    }
}
