//! An in-memory [`Store`], for tests.
//!
//! This exists to make the [`Store`] trait load-bearing rather than decorative.
//! Before it there was one implementation and every caller named it, so the
//! trait's two stated benefits — that the LMDB decision stays reversible, and
//! that the server can be tested against a fake — were both untrue as wired. A
//! second implementation is the only thing that can prove otherwise, because it
//! is the only thing that makes the compiler check the boundary.
//!
//! **It is not a cache.** Nothing here is evicted, nothing is reclaimed, no
//! space is bounded, and one mutex serialises every operation. That is fine for
//! what it is for and disqualifying for anything else, which is why it is behind
//! the `testing` feature.
//!
//! What it *does* reproduce faithfully is the semantics the trait documents:
//! liveness by expiry, flush epoch and tag generation; CAS tokens that increase;
//! guarded writes; and atomic read-modify-write. The arithmetic is not
//! reimplemented — it is [`vash_core::Arithmetic::evaluate`], the same pure
//! function the LMDB writer calls, which is the payoff of having put it in the
//! domain crate. Only the *storage* is faked.

use std::collections::BTreeMap;
use std::sync::Mutex;

use bytes::Bytes;
use vash_core::{
    Applied, Arithmetic, BatchGuard, Clock, ExpireGuard, Key, ListEntry, ListRequest, Listing,
    NEVER, Set, SetMode, Stored, TagGeneration, TtlChange, Value, ValueRef, arith,
};

use crate::apply::Written;
use crate::error::{Result, StoreError};
use crate::{Store, StoreStats};

/// One stored record, in the shape the trait's replies need.
#[derive(Clone)]
struct Entry {
    value: Bytes,
    mc_flags: u32,
    cas: u64,
    /// Absolute unix milliseconds, or [`NEVER`].
    expires_at_ms: u64,
    /// The flush epoch at write time.
    epoch: u32,
    /// `(tag id, generation as it stood when this was written)`.
    tags: Vec<(u32, u64)>,
}

#[derive(Clone, Copy)]
struct TagState {
    id: u32,
    generation: u64,
}

#[derive(Default)]
struct Inner {
    /// Ordered, because the key listing walks in key order and a `BTreeMap`
    /// gives that for free — the same order LMDB's cursor would.
    entries: BTreeMap<Box<[u8]>, Entry>,
    tags: BTreeMap<Box<[u8]>, TagState>,
    next_tag_id: u32,
    epoch: u32,
    next_cas: u64,
}

/// A [`Store`] held entirely in memory.
pub struct MemoryStore {
    inner: Mutex<Inner>,
    clock: Clock,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            clock: Clock::new(),
        }
    }

    /// The lock, with a poisoned mutex reported as corruption rather than
    /// panicking a request thread.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| StoreError::Corrupt("memory store mutex was poisoned".into()))
    }
}

impl Inner {
    /// Whether a record is still visible, by the same three rules the on-disk
    /// record header encodes: flush epoch, expiry, and every tag's generation.
    fn alive(&self, entry: &Entry, now_ms: u64) -> bool {
        if entry.epoch != self.epoch {
            return false;
        }
        if entry.expires_at_ms != NEVER && entry.expires_at_ms <= now_ms {
            return false;
        }
        entry.tags.iter().all(|(id, generation)| {
            self.tags
                .values()
                .find(|state| state.id == *id)
                .is_some_and(|state| state.generation == *generation)
        })
    }

    fn live(&self, key: &[u8], now_ms: u64) -> Option<&Entry> {
        self.entries
            .get(key)
            .filter(|entry| self.alive(entry, now_ms))
    }

    fn next_cas(&mut self) -> u64 {
        self.next_cas += 1;
        self.next_cas
    }

    /// Registers a tag if it is new, and resolves the ids and generations a
    /// record should capture.
    fn resolve_tags(&mut self, names: &[&[u8]]) -> Vec<(u32, u64)> {
        names
            .iter()
            .map(|name| {
                let next = self.next_tag_id;
                let state = self.tags.entry((*name).into()).or_insert_with(|| TagState {
                    id: next,
                    generation: 0,
                });
                if state.id == next {
                    self.next_tag_id += 1;
                }
                (state.id, state.generation)
            })
            .collect()
    }

    /// The deadline a write lands on, given what the record it replaces holds.
    fn deadline(&self, ttl: TtlChange, current: u64, clock: &Clock) -> u64 {
        match ttl {
            TtlChange::Keep => current,
            TtlChange::Set(ttl_secs) => clock.expiry_from_ttl(ttl_secs),
            TtlChange::SetIfPersistent(ttl_secs) => {
                if current == NEVER {
                    clock.expiry_from_ttl(ttl_secs)
                } else {
                    current
                }
            }
        }
    }

    fn value_of(entry: &Entry) -> Value {
        Value {
            data: entry.value.clone(),
            mc_flags: entry.mc_flags,
            cas: entry.cas,
            expires_at_ms: Some(entry.expires_at_ms),
        }
    }

    /// Applies one write, resolving its lifetime against whatever it replaces.
    /// Returns the new CAS token and the value displaced, when asked for.
    fn put(&mut self, set: &Set<'_>, clock: &Clock, now_ms: u64) -> (u64, Option<Value>) {
        let key = set.key.as_bytes();
        let existing = self.live(key, now_ms).cloned();

        let previous = set
            .return_previous
            .then(|| existing.as_ref().map(Inner::value_of))
            .flatten();
        let current_deadline = existing.as_ref().map_or(NEVER, |e| e.expires_at_ms);

        let tags = self.resolve_tags(&set.tags);
        let cas = self.next_cas();
        let entry = Entry {
            value: Bytes::copy_from_slice(set.value),
            mc_flags: set.mc_flags,
            cas,
            expires_at_ms: self.deadline(set.ttl, current_deadline, clock),
            epoch: self.epoch,
            tags,
        };
        self.entries.insert(key.into(), entry);
        (cas, previous)
    }
}

impl Store for MemoryStore {
    fn shard_count(&self) -> usize {
        1
    }

    fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        let now = self.clock.now_ms();
        let inner = self.lock()?;
        Ok(inner.live(key.as_bytes(), now).map(Inner::value_of))
    }

    /// The borrowing read, which for this store borrows out of the map it holds
    /// under its own lock.
    ///
    /// The lock is held across `render` for the same reason LMDB holds its read
    /// transaction: that is what makes the borrow valid. It is also why the
    /// trait forbids `render` from calling back into the store — here that would
    /// deadlock rather than merely pin a snapshot, which makes the fake a
    /// stricter check of the contract than the real thing.
    fn get_with(&self, key: Key<'_>, render: &mut dyn FnMut(ValueRef<'_>)) -> Result<bool> {
        let now = self.clock.now_ms();
        let inner = self.lock()?;
        let Some(entry) = inner.live(key.as_bytes(), now) else {
            return Ok(false);
        };
        render(ValueRef {
            data: &entry.value,
            mc_flags: entry.mc_flags,
            cas: entry.cas,
            expires_at_ms: Some(entry.expires_at_ms),
        });
        Ok(true)
    }

    fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        let now = self.clock.now_ms();
        let inner = self.lock()?;
        Ok(keys
            .iter()
            .map(|key| inner.live(key.as_bytes(), now).map(Inner::value_of))
            .collect())
    }

    fn deadline(&self, key: Key<'_>) -> Result<Option<u64>> {
        let now = self.clock.now_ms();
        let inner = self.lock()?;
        Ok(inner.live(key.as_bytes(), now).map(|e| e.expires_at_ms))
    }

    fn deadlines(&self, keys: &[Key<'_>]) -> Result<Vec<Option<u64>>> {
        let now = self.clock.now_ms();
        let inner = self.lock()?;
        Ok(keys
            .iter()
            .map(|key| inner.live(key.as_bytes(), now).map(|e| e.expires_at_ms))
            .collect())
    }

    fn set(&self, set: &Set<'_>) -> Result<u64> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;
        Ok(inner.put(set, &self.clock, now).0)
    }

    /// Nothing here is mapped from a file, so no read can fault to disk and the
    /// residency question is already answered.
    fn map_locked(&self) -> bool {
        true
    }

    /// Applied inline — there is no writer thread to wait for — so the handle
    /// comes back already resolved.
    fn submit_set_many(&self, sets: &[Set<'_>]) -> Result<crate::PendingSetMany> {
        self.set_many(sets).map(crate::PendingSetMany::ready)
    }

    fn set_many(&self, sets: &[Set<'_>]) -> Result<Vec<u64>> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;
        Ok(sets
            .iter()
            .map(|set| inner.put(set, &self.clock, now).0)
            .collect())
    }

    fn set_many_if(&self, sets: &[Set<'_>], guard: BatchGuard) -> Result<bool> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;

        // One lock covers the test and the writes, so this fake is atomic where
        // the sharded implementation is atomic only within a shard.
        let present: Vec<bool> = sets
            .iter()
            .map(|set| inner.live(set.key.as_bytes(), now).is_some())
            .collect();
        let applies = match guard {
            BatchGuard::Always => true,
            BatchGuard::IfAllAbsent => present.iter().all(|p| !p),
            BatchGuard::IfAllPresent => present.iter().all(|p| *p),
        };
        if !applies {
            return Ok(false);
        }

        for set in sets {
            inner.put(set, &self.clock, now);
        }
        Ok(true)
    }

    fn store(&self, set: &Set<'_>) -> Result<Written> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;
        let existing = inner.live(set.key.as_bytes(), now).cloned();

        let refused = |outcome: Stored| Written {
            outcome,
            previous: set
                .return_previous
                .then(|| existing.as_ref().map(Inner::value_of))
                .flatten(),
        };

        match (set.mode, &existing) {
            (SetMode::Set, _) => {}
            (SetMode::Add, Some(_)) => return Ok(refused(Stored::NotStored)),
            (SetMode::Add, None) => {}
            (SetMode::Replace, None) => return Ok(refused(Stored::NotStored)),
            (SetMode::Replace, Some(_)) => {}
            (SetMode::Append | SetMode::Prepend, None) => return Ok(refused(Stored::NotStored)),
            (SetMode::Cas(_), None) => return Ok(refused(Stored::NotFound)),
            (SetMode::Cas(expected), Some(entry)) if entry.cas != expected => {
                return Ok(refused(Stored::Exists));
            }
            (SetMode::Cas(_), Some(_)) => {}
            (SetMode::Append | SetMode::Prepend, Some(_)) => {}
        }

        // Concatenation keeps the stored value's lifetime and client flags, as
        // memcached does.
        let mut owned;
        let mut effective = set.clone();
        if matches!(set.mode, SetMode::Append | SetMode::Prepend) {
            let entry = existing.as_ref().expect("guarded above");
            owned = Vec::with_capacity(entry.value.len() + set.value.len());
            if set.mode == SetMode::Append {
                owned.extend_from_slice(&entry.value);
                owned.extend_from_slice(set.value);
            } else {
                owned.extend_from_slice(set.value);
                owned.extend_from_slice(&entry.value);
            }
            effective.value = &owned;
            effective.ttl = TtlChange::Keep;
            effective.mc_flags = entry.mc_flags;
        }

        let (cas, previous) = inner.put(&effective, &self.clock, now);
        Ok(Written {
            outcome: Stored::Stored(cas),
            previous,
        })
    }

    fn arithmetic(&self, op: &Arithmetic<'_>) -> Result<Option<Applied>> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;
        let existing = inner.live(op.key.as_bytes(), now).cloned();

        // The arithmetic itself is the domain's, not this module's — the same
        // function the LMDB writer calls.
        let outcome = op.evaluate(existing.as_ref().map(|entry| &*entry.value))?;
        let (text, applied) = match outcome {
            arith::Outcome::Missing => return Ok(None),
            arith::Outcome::Unchanged(applied) => return Ok(Some(applied)),
            arith::Outcome::Store { text, applied } => (text, applied),
        };

        let cas = inner.next_cas();
        let entry = Entry {
            value: Bytes::from(text.into_bytes()),
            mc_flags: existing.as_ref().map_or(0, |e| e.mc_flags),
            cas,
            expires_at_ms: inner.deadline(
                op.ttl,
                existing.as_ref().map_or(NEVER, |e| e.expires_at_ms),
                &self.clock,
            ),
            epoch: inner.epoch,
            tags: existing.map(|e| e.tags).unwrap_or_default(),
        };
        inner.entries.insert(op.key.as_bytes().into(), entry);
        Ok(Some(applied))
    }

    fn append(&self, key: Key<'_>, suffix: &[u8]) -> Result<u64> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;
        let existing = inner.live(key.as_bytes(), now).cloned();

        let mut combined = Vec::new();
        if let Some(entry) = &existing {
            combined.extend_from_slice(&entry.value);
        }
        combined.extend_from_slice(suffix);
        let length = combined.len() as u64;

        let cas = inner.next_cas();
        let entry = Entry {
            value: Bytes::from(combined),
            mc_flags: existing.as_ref().map_or(0, |e| e.mc_flags),
            cas,
            expires_at_ms: existing.as_ref().map_or(NEVER, |e| e.expires_at_ms),
            epoch: inner.epoch,
            tags: existing.map(|e| e.tags).unwrap_or_default(),
        };
        inner.entries.insert(key.as_bytes().into(), entry);
        Ok(length)
    }

    fn expire(&self, key: Key<'_>, ttl: TtlChange, guard: ExpireGuard) -> Result<bool> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;
        let Some(entry) = inner.live(key.as_bytes(), now).cloned() else {
            return Ok(false);
        };

        let target = inner.deadline(ttl, entry.expires_at_ms, &self.clock);
        // A key with no deadline is infinitely far off, which is what makes `GT`
        // never apply to one and `LT` always apply.
        let existing = (entry.expires_at_ms != NEVER).then_some(entry.expires_at_ms);
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

        if target != NEVER && target <= now {
            inner.entries.remove(key.as_bytes());
            return Ok(true);
        }

        let cas = inner.next_cas();
        inner.entries.insert(
            key.as_bytes().into(),
            Entry {
                expires_at_ms: target,
                cas,
                ..entry
            },
        );
        Ok(true)
    }

    fn touch(&self, key: Key<'_>, ttl_secs: u32) -> Result<bool> {
        self.expire(key, TtlChange::Set(ttl_secs), ExpireGuard::Always)
    }

    fn get_and_touch(&self, keys: &[Key<'_>], ttl_secs: u32) -> Result<Vec<Option<Value>>> {
        let values = self.get_many(keys)?;
        for (key, value) in keys.iter().zip(&values) {
            if value.is_some() {
                self.touch(*key, ttl_secs)?;
            }
        }
        Ok(values)
    }

    fn delete(&self, key: Key<'_>) -> Result<bool> {
        let now = self.clock.now_ms();
        let mut inner = self.lock()?;
        let was_live = inner.live(key.as_bytes(), now).is_some();
        inner.entries.remove(key.as_bytes());
        Ok(was_live)
    }

    fn delete_many(&self, keys: &[Key<'_>]) -> Result<Vec<bool>> {
        keys.iter().map(|key| self.delete(*key)).collect()
    }

    fn delete_by_tag(&self, tag: &[u8]) -> Result<Option<u64>> {
        let mut inner = self.lock()?;
        let Some(state) = inner.tags.get_mut(tag) else {
            return Ok(None);
        };
        state.generation += 1;
        Ok(Some(state.generation))
    }

    fn merge_tag_generation(&self, tag: &[u8], generation: u64) -> Result<u64> {
        let mut inner = self.lock()?;
        // Registering through `resolve_tags` keeps the id allocation in one
        // place, rather than repeating the "bump the counter only if this was
        // new" dance and getting the borrow wrong.
        inner.resolve_tags(&[tag]);
        let state = inner
            .tags
            .get_mut(tag)
            .expect("just registered by resolve_tags");
        state.generation = state.generation.max(generation);
        Ok(state.generation)
    }

    fn tag_generations(&self) -> Result<Vec<TagGeneration>> {
        let inner = self.lock()?;
        Ok(inner
            .tags
            .iter()
            .map(|(name, state)| TagGeneration {
                name: name.clone(),
                generation: state.generation,
            })
            .collect())
    }

    fn list_keys(&self, request: &ListRequest<'_>, max_scan: usize) -> Result<Listing> {
        let now = self.clock.now_ms();
        let inner = self.lock()?;
        let (_, after) = crate::listing::decode(request.cursor, 1)?;

        let mut listing = Listing::default();
        let limit = request.limit as usize;

        for (key, entry) in &inner.entries {
            if after.as_ref().is_some_and(|from| key.as_ref() <= &**from) {
                continue;
            }
            if listing.scanned as usize >= max_scan {
                listing.truncated = true;
                listing.cursor = Some(crate::listing::encode(0, key));
                return Ok(listing);
            }
            listing.scanned += 1;
            if !inner.alive(entry, now) || !request.matches(key) {
                continue;
            }
            listing.entries.push(ListEntry::record(
                key.clone(),
                entry.cas,
                entry.expires_at_ms,
            ));
            if listing.entries.len() >= limit {
                listing.cursor = Some(crate::listing::encode(0, key));
                return Ok(listing);
            }
        }
        Ok(listing)
    }

    fn list_tags(&self, request: &ListRequest<'_>) -> Result<Listing> {
        let inner = self.lock()?;
        let mut listing = Listing::default();
        let limit = request.limit as usize;

        for (name, state) in &inner.tags {
            if !request.cursor.is_empty() && name.as_ref() <= request.cursor {
                continue;
            }
            listing.scanned += 1;
            if !request.matches(name) {
                continue;
            }
            listing
                .entries
                .push(ListEntry::new(name.clone(), state.generation));
            if listing.entries.len() >= limit {
                listing.cursor = Some(name.clone());
                break;
            }
        }
        Ok(listing)
    }

    fn flush(&self) -> Result<u32> {
        let mut inner = self.lock()?;
        inner.epoch += 1;
        inner.entries.clear();
        Ok(inner.epoch)
    }

    fn stats(&self) -> Result<StoreStats> {
        let inner = self.lock()?;
        Ok(StoreStats {
            shards: 1,
            entries: inner.entries.len() as u64,
            tags: inner.tags.len() as u64,
            epoch: inner.epoch,
            // Everything else is a property of the on-disk layout, and reporting
            // a plausible-looking zero for a number this store does not have is
            // exactly what `metrics.rs` warns against. They stay at their
            // defaults, which is what "not measured here" looks like.
            ..StoreStats::default()
        })
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }
}
