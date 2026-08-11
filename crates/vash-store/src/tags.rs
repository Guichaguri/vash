//! The tag registry: names, dense ids, and generation counters.
//!
//! Invalidating a tag is a single counter bump — constant time no matter how
//! many keys carry the tag. Records store the generation each of their tags had
//! when they were written, so a read decides liveness by comparing those
//! against the registry, entirely in RAM, with no extra disk lookup.
//!
//! The naive alternative — find every key with the tag and delete it — costs
//! O(n) *while holding the shard's only write transaction*, stalling every
//! other write on the node. For a tag covering a million keys that is a
//! multi-second stop-the-world pause. See plan §5.
//!
//! **Names are the global identity; ids are node-local.** Ids are a dense
//! counter used only to keep the per-record tag table small (4 bytes instead of
//! a name), and they must never be shared between nodes — cluster messages
//! carry names, and a peer resolves them against its own registry.
//!
//! Ids are per *shard*, not even per node. Generations, by contrast, are held
//! uniform across a node's shards: a shard that meets a name late adopts the
//! node-wide value rather than starting at zero, because the number this node
//! reports to its peers has to mean the same thing everywhere inside it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use vash_core::{TagGeneration, TagRef};

use crate::error::{Result, StoreError};

/// Bytes of a persisted registry entry: `tag_id u32 LE || generation u64 LE`.
pub const TAG_RECORD_LEN: usize = 12;

#[derive(Debug)]
pub struct TagEntry {
    pub id: u32,
    pub name: Box<[u8]>,
    /// Bumped on every invalidation. Records written before the bump compare
    /// unequal and are therefore dead.
    pub generation: AtomicU64,
}

impl TagEntry {
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct Inner {
    by_name: HashMap<Box<[u8]>, Arc<TagEntry>>,
    /// Indexed by tag id. Dense because ids come from a counter, so lookup on
    /// the read path is an array index rather than a hash.
    by_id: Vec<Option<Arc<TagEntry>>>,
}

pub struct TagRegistry {
    inner: RwLock<Inner>,
    next_id: AtomicU32,
    max_tags: usize,
}

impl TagRegistry {
    pub fn new(max_tags: usize) -> Self {
        Self {
            inner: RwLock::new(Inner::default()),
            next_id: AtomicU32::new(0),
            max_tags,
        }
    }

    /// Rebuilds the registry from persisted entries. Called once at startup;
    /// the whole table lives in RAM because it is small and read constantly.
    pub fn load_from(&self, entries: impl IntoIterator<Item = (Box<[u8]>, u32, u64)>) {
        let mut inner = self.inner.write().expect("tag registry lock poisoned");
        let mut highest = 0u32;

        for (name, id, generation) in entries {
            let entry = Arc::new(TagEntry {
                id,
                name: name.clone(),
                generation: AtomicU64::new(generation),
            });
            highest = highest.max(id);
            put_by_id(&mut inner.by_id, id, Arc::clone(&entry));
            inner.by_name.insert(name, entry);
        }

        // Resuming past the highest persisted id keeps ids from being reused
        // for a different name, which would silently invalidate the wrong data.
        if !inner.by_name.is_empty() {
            self.next_id.store(highest + 1, Ordering::Release);
        }
    }

    /// A read handle over the registry, so a record's whole tag table is
    /// checked under one lock acquisition rather than one per tag.
    pub fn lookup(&self) -> TagLookup<'_> {
        TagLookup {
            inner: self.inner.read().expect("tag registry lock poisoned"),
        }
    }

    pub fn by_name(&self, name: &[u8]) -> Option<Arc<TagEntry>> {
        self.inner
            .read()
            .expect("tag registry lock poisoned")
            .by_name
            .get(name)
            .cloned()
    }

    /// Current generation for a name, or `None` if this shard has never seen it.
    pub fn generation_of(&self, name: &[u8]) -> Option<u64> {
        self.inner
            .read()
            .expect("tag registry lock poisoned")
            .by_name
            .get(name)
            .map(|entry| entry.generation())
    }

    /// Every registered name with the generation it currently holds.
    ///
    /// This is what a node offers a peer during anti-entropy. Taken from RAM
    /// rather than from disk because the two agree by construction — a
    /// generation is only published here after the transaction carrying it has
    /// committed — and a gossip round must not open a transaction per node.
    pub fn snapshot(&self) -> Vec<TagGeneration> {
        let inner = self.inner.read().expect("tag registry lock poisoned");
        inner
            .by_name
            .values()
            .map(|entry| TagGeneration {
                name: entry.name.clone(),
                generation: entry.generation(),
            })
            .collect()
    }

    /// Resolves tag names to the `(id, generation)` pairs a record stores.
    ///
    /// Returns `Err` naming the first tag that does not exist yet, so the
    /// caller can create it before encoding the record — a record may never
    /// reference a tag that is not durably registered, or a restart would
    /// resurrect it under a different name.
    pub fn resolve(&self, names: &[&[u8]]) -> std::result::Result<Vec<TagRef>, Box<[u8]>> {
        let inner = self.inner.read().expect("tag registry lock poisoned");
        let mut refs = Vec::with_capacity(names.len());
        for name in names {
            match inner.by_name.get(*name) {
                Some(entry) => refs.push(TagRef::new(entry.id, entry.generation())),
                None => return Err((*name).into()),
            }
        }
        Ok(refs)
    }

    /// Names among `names` that have no registry entry yet, deduplicated.
    pub fn missing(&self, names: &[&[u8]]) -> Vec<Box<[u8]>> {
        let inner = self.inner.read().expect("tag registry lock poisoned");
        let mut missing: Vec<Box<[u8]>> = Vec::new();
        for name in names {
            if !inner.by_name.contains_key(*name) && !missing.iter().any(|m| m.as_ref() == *name) {
                missing.push((*name).into());
            }
        }
        missing
    }

    /// Reserves the next id. Called only from the writer thread.
    pub fn allocate_id(&self) -> Result<u32> {
        let inner = self.inner.read().expect("tag registry lock poisoned");
        if inner.by_name.len() >= self.max_tags {
            return Err(StoreError::TagLimit(self.max_tags));
        }
        Ok(self.next_id.fetch_add(1, Ordering::AcqRel))
    }

    /// Publishes a newly created tag.
    ///
    /// Applied **after** the transaction that persisted it commits: a tag
    /// visible in RAM but absent from disk would let a record reference an id
    /// that does not survive a restart.
    pub fn insert(&self, name: Box<[u8]>, id: u32, generation: u64) {
        let mut inner = self.inner.write().expect("tag registry lock poisoned");
        let entry = Arc::new(TagEntry {
            id,
            name: name.clone(),
            generation: AtomicU64::new(generation),
        });
        put_by_id(&mut inner.by_id, id, Arc::clone(&entry));
        inner.by_name.insert(name, entry);
    }

    /// Publishes a bumped generation, likewise only after its commit.
    ///
    /// Merges by maximum rather than assigning. That makes the operation
    /// idempotent and order-independent, which is what lets M5 replay
    /// invalidations between nodes without any coordination.
    pub fn merge_generation(&self, id: u32, generation: u64) {
        let inner = self.inner.read().expect("tag registry lock poisoned");
        if let Some(Some(entry)) = inner.by_id.get(id as usize) {
            entry.generation.fetch_max(generation, Ordering::AcqRel);
        }
    }

    pub fn len(&self) -> usize {
        self.inner
            .read()
            .expect("tag registry lock poisoned")
            .by_name
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn put_by_id(by_id: &mut Vec<Option<Arc<TagEntry>>>, id: u32, entry: Arc<TagEntry>) {
    let index = id as usize;
    if by_id.len() <= index {
        by_id.resize_with(index + 1, || None);
    }
    by_id[index] = Some(entry);
}

/// A held read lock over the registry.
pub struct TagLookup<'a> {
    inner: RwLockReadGuard<'a, Inner>,
}

impl TagLookup<'_> {
    /// Current generation of a tag, or `None` if the id is unknown.
    ///
    /// Unknown fails closed at the call sites: a record referencing a tag the
    /// registry has never heard of reads as a miss rather than a stale hit.
    #[inline]
    pub fn generation(&self, id: u32) -> Option<u64> {
        match self.inner.by_id.get(id as usize) {
            Some(Some(entry)) => Some(entry.generation()),
            _ => None,
        }
    }
}

/// Encodes a registry entry for storage.
pub fn encode_entry(id: u32, generation: u64) -> [u8; TAG_RECORD_LEN] {
    let mut buf = [0u8; TAG_RECORD_LEN];
    buf[..4].copy_from_slice(&id.to_le_bytes());
    buf[4..].copy_from_slice(&generation.to_le_bytes());
    buf
}

pub fn decode_entry(raw: &[u8]) -> Option<(u32, u64)> {
    if raw.len() != TAG_RECORD_LEN {
        return None;
    }
    Some((
        u32::from_le_bytes(raw[..4].try_into().ok()?),
        u64::from_le_bytes(raw[4..].try_into().ok()?),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> TagRegistry {
        TagRegistry::new(1000)
    }

    #[test]
    fn entries_roundtrip() {
        let raw = encode_entry(7, u64::MAX);
        assert_eq!(decode_entry(&raw), Some((7, u64::MAX)));
        assert_eq!(decode_entry(&raw[..11]), None);
    }

    #[test]
    fn resolve_reports_the_missing_name() {
        let reg = registry();
        reg.insert(b"known".to_vec().into(), 0, 3);

        assert_eq!(
            reg.resolve(&[b"known".as_slice()]).unwrap(),
            vec![TagRef::new(0, 3)]
        );
        assert_eq!(
            reg.resolve(&[b"known".as_slice(), b"other"]).unwrap_err(),
            b"other".to_vec().into_boxed_slice()
        );
    }

    #[test]
    fn missing_deduplicates() {
        let reg = registry();
        reg.insert(b"a".to_vec().into(), 0, 0);

        let missing = reg.missing(&[b"a".as_slice(), b"b", b"b", b"c"]);
        assert_eq!(
            missing.len(),
            2,
            "each unknown name reported once: {missing:?}"
        );
    }

    #[test]
    fn lookup_fails_closed_on_unknown_ids() {
        let reg = registry();
        reg.insert(b"a".to_vec().into(), 0, 5);

        let lookup = reg.lookup();
        assert_eq!(lookup.generation(0), Some(5));
        assert_eq!(lookup.generation(1), None, "unknown ids must fail closed");
        assert_eq!(lookup.generation(u32::MAX), None);
    }

    #[test]
    fn generations_merge_by_maximum() {
        // Idempotent and order-independent, so a replayed or reordered
        // invalidation converges rather than moving the counter backwards.
        let reg = registry();
        reg.insert(b"a".to_vec().into(), 0, 5);

        reg.merge_generation(0, 9);
        assert_eq!(reg.lookup().generation(0), Some(9));

        reg.merge_generation(0, 7);
        assert_eq!(
            reg.lookup().generation(0),
            Some(9),
            "must never go backwards"
        );

        reg.merge_generation(0, 9);
        assert_eq!(
            reg.lookup().generation(0),
            Some(9),
            "replay must be a no-op"
        );
    }

    #[test]
    fn ids_resume_past_the_highest_loaded() {
        let reg = registry();
        reg.load_from([
            (b"a".to_vec().into_boxed_slice(), 0, 1),
            (b"b".to_vec().into_boxed_slice(), 7, 2),
        ]);

        assert_eq!(
            reg.allocate_id().unwrap(),
            8,
            "reusing an id would invalidate the wrong tag's data after a restart"
        );
    }

    #[test]
    fn allocation_stops_at_the_limit() {
        let reg = TagRegistry::new(2);
        reg.insert(b"a".to_vec().into(), reg.allocate_id().unwrap(), 0);
        reg.insert(b"b".to_vec().into(), reg.allocate_id().unwrap(), 0);

        assert!(
            matches!(reg.allocate_id(), Err(StoreError::TagLimit(2))),
            "an unbounded registry is a memory leak a client can drive"
        );
    }
}
