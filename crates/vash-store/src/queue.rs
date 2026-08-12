//! What crosses the writer queue, and how each item is applied.
//!
//! Split from [`crate::writer`] because it answers a different question. This
//! module is the *vocabulary*: the operations a caller can ask for, the outcomes
//! they answer with, and the one `match` that turns one into the other. The
//! writer thread beside it is the *mechanism*: when to commit, what to do when a
//! commit fails, and how maintenance rides along.
//!
//! The pairing of [`WriteOp`] and [`WriteOutcome`] is deliberately loose — the
//! queue cannot express "this operation answers with that outcome" in the type
//! system — so a mismatch is a bug caught at runtime by [`mismatched_reply`],
//! and keeping both enums and the dispatch in one file is what makes such a
//! mismatch visible on one screen.

use vash_core::{
    Applied, Arithmetic, Delta, ExpireGuard, Key, Missing, OnBound, SetMode, TtlChange,
};

use crate::apply::{PreparedSet, Written};
use crate::engine::LmdbEngine;
use crate::error::{Result, StoreError};
use crate::tags::TagMerge;

pub(crate) enum WriteOp {
    Set(Vec<PreparedSet>),
    /// A single write under a guard. Kept apart from `Set` because its outcome
    /// is a verdict rather than a CAS token, and it must not be batched with
    /// other writes to the same key.
    ConditionalSet(PreparedSet, SetMode),
    Delete(Vec<Box<[u8]>>),
    /// Re-stamps every key with the same TTL. A batch because memcached's
    /// `gat` takes a key list, and one round trip through the writer per key
    /// means one commit per key.
    Touch {
        keys: Vec<Box<[u8]>>,
        ttl_secs: u32,
    },
    /// An atomic read-modify-write on a counter.
    ///
    /// The key is owned because the operation outlives the caller's buffer, and
    /// the rest of [`OwnedArithmetic`] is `Copy` — a counter operation carries no
    /// payload beyond its numbers.
    Arithmetic(OwnedArithmetic),
    /// Concatenate onto a value, creating it if absent. Redis's `APPEND`.
    Append {
        key: Box<[u8]>,
        suffix: Box<[u8]>,
    },
    /// Change a key's deadline under a guard. Redis's `EXPIRE` family.
    Expire {
        key: Box<[u8]>,
        ttl: TtlChange,
        guard: ExpireGuard,
    },
    /// Register tag names, each with the generation the rest of the node
    /// already holds for it.
    CreateTags(Vec<(Box<[u8]>, u64)>),
    DeleteByTag(Box<[u8]>),
    /// Raise a tag's generation to at least this value, creating it if unknown.
    /// The receiving half of cluster invalidation.
    MergeTag {
        name: Box<[u8]>,
        generation: u64,
    },
    Flush,
    Sync,
}

/// The writer answered a job with an outcome its operation cannot produce.
///
/// A bug in this module rather than anything a caller can cause: the queue is
/// typed by `WriteOp` and answered by `WriteOutcome`, and only a mismatched pair
/// here reaches it.
pub(crate) fn mismatched_reply() -> StoreError {
    StoreError::Corrupt("writer returned the wrong reply".into())
}

/// An [`Arithmetic`] that owns its key, so the operation can cross the queue.
///
/// `Arithmetic` borrows its key from the connection's read buffer, which is gone
/// by the time the writer thread runs. Everything else a counter operation
/// carries is a handful of numbers, so the key is the only field to copy.
pub(crate) struct OwnedArithmetic {
    key: Box<[u8]>,
    delta: Delta,
    on_bound: OnBound,
    missing: Missing,
    ttl: TtlChange,
}

pub(crate) enum WriteOutcome {
    Cas(Vec<u64>),
    /// A guarded write's verdict, and whatever it displaced.
    Written(Written),
    /// A guarded change that either applied or did not.
    Applied(bool),
    Deleted(Vec<bool>),
    Touched(Vec<bool>),
    /// Where a counter ended up, and how far it moved. `None` when the key was
    /// absent and the operation does not create one.
    Arithmetic(Option<Applied>),
    /// The length a value reached after being concatenated onto.
    Length(u64),
    /// `None` when the tag was never registered, so nothing referenced it.
    Invalidated(Option<u64>),
    /// The generation the tag holds after a max-merge.
    Merged(u64),
    Flushed(u32),
    Done,
}

/// A change to in-memory state that must not be published until the
/// transaction carrying it has committed.
///
/// Ordering matters in one direction only, and it is the dangerous one: a
/// generation bumped in RAM but lost to a failed commit would let invalidated
/// records come back to life after a restart. Applying RAM changes strictly
/// after the commit means a failure leaves RAM matching disk.
pub(crate) enum PostCommit {
    TagCreated {
        name: Box<[u8]>,
        id: u32,
        generation: u64,
    },
    TagGeneration {
        id: u32,
        generation: u64,
    },
    Epoch(u32),
}

pub(crate) fn apply(
    engine: &LmdbEngine,
    wtxn: &mut heed::RwTxn,
    op: &mut WriteOp,
    effects: &mut Vec<PostCommit>,
) -> Result<WriteOutcome> {
    match op {
        WriteOp::Set(prepared) => {
            let mut cas = Vec::with_capacity(prepared.len());
            for item in prepared.iter_mut() {
                cas.push(engine.apply_set(wtxn, item)?.cas);
            }
            Ok(WriteOutcome::Cas(cas))
        }
        WriteOp::Expire { key, ttl, guard } => Ok(WriteOutcome::Applied(
            engine.apply_expire(wtxn, key, *ttl, *guard)?,
        )),
        WriteOp::Delete(keys) => {
            let mut hits = Vec::with_capacity(keys.len());
            for key in keys.iter() {
                hits.push(engine.apply_delete(wtxn, key)?);
            }
            Ok(WriteOutcome::Deleted(hits))
        }
        WriteOp::ConditionalSet(prepared, mode) => Ok(WriteOutcome::Written(
            engine.apply_conditional_set(wtxn, prepared, *mode)?,
        )),
        WriteOp::Touch { keys, ttl_secs } => Ok(WriteOutcome::Touched(
            keys.iter()
                .map(|key| engine.apply_touch(wtxn, key, *ttl_secs))
                .collect::<Result<Vec<bool>>>()?,
        )),
        WriteOp::Arithmetic(op) => Ok(WriteOutcome::Arithmetic(
            engine.apply_arithmetic(wtxn, &op.borrow())?,
        )),
        WriteOp::Append { key, suffix } => Ok(WriteOutcome::Length(
            engine.apply_append(wtxn, key, suffix)?,
        )),
        WriteOp::CreateTags(names) => {
            for (name, start_generation) in names.iter() {
                let (id, generation) = engine.apply_create_tag(wtxn, name, *start_generation)?;
                effects.push(PostCommit::TagCreated {
                    name: name.clone(),
                    id,
                    generation,
                });
            }
            Ok(WriteOutcome::Done)
        }
        WriteOp::DeleteByTag(name) => match engine.apply_delete_by_tag(wtxn, name)? {
            Some((id, generation)) => {
                effects.push(PostCommit::TagGeneration { id, generation });
                Ok(WriteOutcome::Invalidated(Some(generation)))
            }
            None => Ok(WriteOutcome::Invalidated(None)),
        },
        WriteOp::MergeTag { name, generation } => {
            let merged = engine.apply_merge_tag(wtxn, name, *generation)?;
            match merged {
                TagMerge::Created { id, generation } => effects.push(PostCommit::TagCreated {
                    name: name.clone(),
                    id,
                    generation,
                }),
                TagMerge::Raised { id, generation } => {
                    effects.push(PostCommit::TagGeneration { id, generation })
                }
                // Already at or past it, so there is nothing to publish.
                TagMerge::Unchanged { .. } => {}
            }
            Ok(WriteOutcome::Merged(merged.generation()))
        }
        WriteOp::Flush => {
            let epoch = engine.apply_flush(wtxn)?;
            effects.push(PostCommit::Epoch(epoch));
            Ok(WriteOutcome::Flushed(epoch))
        }
        // The sync itself happens after the commit, since it is an environment
        // operation rather than a transactional one.
        WriteOp::Sync => Ok(WriteOutcome::Done),
    }
}

impl WriteOp {
    /// Individual key operations this job carries.
    ///
    /// Counted rather than treating one job as one unit, because a `SET_MANY`
    /// of 256 is 256 writes sharing a commit â€” which is exactly what the batch
    /// metric is supposed to show.
    pub(crate) fn item_count(&self) -> usize {
        match self {
            Self::Set(items) => items.len(),
            Self::Delete(keys) => keys.len(),
            Self::Touch { keys, .. } => keys.len(),
            Self::ConditionalSet(..)
            | Self::Arithmetic(_)
            | Self::Append { .. }
            | Self::Expire { .. }
            | Self::DeleteByTag(_)
            | Self::MergeTag { .. }
            | Self::Flush => 1,
            Self::CreateTags(names) => names.len(),
            Self::Sync => 0,
        }
    }
}

impl OwnedArithmetic {
    pub(crate) fn new(op: &Arithmetic<'_>) -> Self {
        Self {
            key: op.key.as_bytes().into(),
            delta: op.delta,
            on_bound: op.on_bound,
            missing: op.missing,
            ttl: op.ttl,
        }
    }

    /// Borrows it back into the form the engine takes.
    ///
    /// `from_stored` rather than `Key::new`: these bytes were validated when the
    /// caller built the operation, and re-checking them would put work on the
    /// single writer thread that every other write keeps off it.
    pub(crate) fn borrow(&self) -> Arithmetic<'_> {
        Arithmetic {
            key: Key::from_stored(&self.key),
            delta: self.delta,
            on_bound: self.on_bound,
            missing: self.missing,
            ttl: self.ttl,
        }
    }
}
