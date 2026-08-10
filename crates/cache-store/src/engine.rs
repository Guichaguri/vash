//! LMDB operations, with no threading of their own.
//!
//! Every write method takes the transaction it should act in, so the writer
//! thread can pack many of them into one commit (see [`crate::writer`]). Keeping
//! the transaction out of this layer is what makes group commit possible
//! without duplicating the operations.

use std::ops::Bound;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use cache_core::{
    Clock, Key, RecordMeta, RecordRef, Set, SetMode, Stored, TagRef, Value, encode_record,
    patch_cas, record::NEVER, validate_value,
};
use heed::types::Bytes as HeedBytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn, WithoutTls};
use tracing::{info, warn};

use crate::config::{Durability, StoreConfig};
use crate::error::{Result, StoreError};
use crate::reclaim::{self, Job, ReclaimStats};
use crate::schema::{CAS_BLOCK, MAX_DBS, SCHEMA_VERSION, db, meta_key};
use crate::tags::{TagLookup, TagRegistry};
use crate::{StoreStats, SweepStats};

type Db = Database<HeedBytes, HeedBytes>;

/// Which records a pass over the expiry index is allowed to take.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Victims {
    /// Sweeping: only records whose TTL has passed.
    ExpiredOnly,
    /// Evicting under capacity pressure: whatever comes first in the index.
    Anything,
}

/// How close the store is to full.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Pressure {
    /// Business as usual.
    Normal = 0,
    /// Reclamation runs continuously instead of on its idle cadence.
    Soft = 1,
    /// Actively evicting live records to get back under the soft mark.
    Hard = 2,
    /// Writes are refused. Reads and deletes still work — a delete frees space,
    /// so refusing it would be self-defeating.
    Critical = 3,
}

impl Pressure {
    fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Normal,
            1 => Self::Soft,
            2 => Self::Hard,
            _ => Self::Critical,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Soft => "soft",
            Self::Hard => "hard",
            Self::Critical => "critical",
        }
    }
}

/// What a max-merge of a tag generation did.
///
/// The distinction matters after the commit: a created tag has to be published
/// into the in-memory registry as a new entry, a raised one only needs its
/// counter advanced, and an unchanged one needs nothing at all — which is the
/// common case, since a peer that is already up to date receives the same
/// generation over and over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagMerge {
    /// This shard had never seen the name; it now exists at `generation`.
    Created { id: u32, generation: u64 },
    /// The stored generation was raised, and reclamation queued.
    Raised { id: u32, generation: u64 },
    /// Already at or past the offered generation.
    Unchanged { id: u32, generation: u64 },
}

impl TagMerge {
    pub fn generation(self) -> u64 {
        match self {
            Self::Created { generation, .. }
            | Self::Raised { generation, .. }
            | Self::Unchanged { generation, .. } => generation,
        }
    }

    fn id(self) -> u32 {
        match self {
            Self::Created { id, .. } | Self::Raised { id, .. } | Self::Unchanged { id, .. } => id,
        }
    }
}

fn validate_tag_name(name: &[u8]) -> Result<()> {
    if name.is_empty() || name.len() > cache_core::MAX_TAG_LEN {
        return Err(StoreError::Core(cache_core::CoreError::TagTooLong {
            len: name.len(),
            max: cache_core::MAX_TAG_LEN,
        }));
    }
    Ok(())
}

/// A record encoded and ready to store, still missing its CAS token.
///
/// Built on the calling thread so that the value copy and the record framing —
/// the expensive part of a write — happen in parallel across connections, and
/// the single writer thread is left with only the B-tree work.
pub struct PreparedSet {
    pub key: Box<[u8]>,
    pub record: Vec<u8>,
    pub expires_at_ms: u64,
    pub tags: Vec<TagRef>,
}

pub struct LmdbEngine {
    env: Env<WithoutTls>,
    main: Db,
    exp: Db,
    tagidx: Db,
    tag_meta: Db,
    jobs: Db,
    meta: Db,
    clock: Clock,
    epoch: AtomicU32,
    cas_next: AtomicU64,
    cas_watermark: AtomicU64,
    max_value_len: usize,
    bucket_granularity_ms: u64,
    tags: TagRegistry,
    pressure: AtomicU8,
    shard_index: usize,
    shard_count: usize,
}

impl LmdbEngine {
    /// Opens one environment. `shard_index` and `shard_count` identify its
    /// place in the shard set and are validated against what the database
    /// already records.
    pub fn open(config: &StoreConfig, shard_index: usize, shard_count: usize) -> Result<Self> {
        if config.wipe_on_start && config.path.exists() {
            warn!(path = %config.path.display(), "wiping existing database on start");
            std::fs::remove_dir_all(&config.path)?;
        }
        if config.map_size < crate::config::MIN_MAP_SIZE {
            // Refused rather than allowed to wedge later: below this, LMDB can
            // report a full map permanently even after everything is deleted.
            return Err(StoreError::Corrupt(format!(
                "map size {} is below the minimum of {} bytes; LMDB cannot reliably \
                 reclaim space on a map that small",
                config.map_size,
                crate::config::MIN_MAP_SIZE
            )));
        }
        std::fs::create_dir_all(&config.path)?;

        // SAFETY: LMDB maps the file into this process's address space. The
        // contract is that no other process mutates the file outside of LMDB's
        // own locking, which the lock file in the same directory enforces.
        let env = unsafe {
            EnvOpenOptions::new()
                // Detaches read transactions from thread-local storage so a
                // RoTxn is Send. Required by the storage-tier design, plan §9.
                .read_txn_without_tls()
                .flags(env_flags(config.durability))
                .map_size(config.map_size)
                .max_dbs(MAX_DBS)
                .max_readers(config.max_readers)
                .open(&config.path)
        }
        .map_err(StoreError::from_heed)?;

        let mut wtxn = env.write_txn().map_err(StoreError::from_heed)?;
        let main: Db = create_db(&env, &mut wtxn, db::MAIN)?;
        let exp: Db = create_db(&env, &mut wtxn, db::EXPIRY)?;
        let tagidx: Db = create_db(&env, &mut wtxn, db::TAG_INDEX)?;
        let tag_meta: Db = create_db(&env, &mut wtxn, db::TAGS)?;
        let jobs: Db = create_db(&env, &mut wtxn, db::JOBS)?;
        let meta: Db = create_db(&env, &mut wtxn, db::META)?;

        match read_u32(&meta, &wtxn, meta_key::SCHEMA_VERSION)? {
            None => {
                meta.put(
                    &mut wtxn,
                    meta_key::SCHEMA_VERSION,
                    &SCHEMA_VERSION.to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
                meta.put(
                    &mut wtxn,
                    meta_key::RECORD_VERSION,
                    &(cache_core::RECORD_VERSION as u32).to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
                meta.put(
                    &mut wtxn,
                    meta_key::SHARD_INDEX,
                    &(shard_index as u32).to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
                meta.put(
                    &mut wtxn,
                    meta_key::SHARD_COUNT,
                    &(shard_count as u32).to_le_bytes(),
                )
                .map_err(StoreError::from_heed)?;
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => {
                return Err(StoreError::Corrupt(format!(
                    "database has schema version {v}, this build expects {SCHEMA_VERSION}"
                )));
            }
        }

        // Reopening with a different shard count would route every key to a
        // different environment: the data would still be on disk, taking up
        // space, but every read would miss. Refuse rather than silently lose
        // the cache.
        if let Some(stored) = read_u32(&meta, &wtxn, meta_key::SHARD_COUNT)?
            && stored as usize != shard_count
        {
            return Err(StoreError::Corrupt(format!(
                "database at {} was built for {stored} shard(s), but {shard_count} were configured; \
                 keys would route to the wrong environment",
                config.path.display()
            )));
        }
        if let Some(stored) = read_u32(&meta, &wtxn, meta_key::SHARD_INDEX)?
            && stored as usize != shard_index
        {
            return Err(StoreError::Corrupt(format!(
                "database at {} is shard {stored}, opened as shard {shard_index}",
                config.path.display()
            )));
        }

        let epoch = read_u32(&meta, &wtxn, meta_key::EPOCH)?.unwrap_or(0);
        // Resume past the last reserved block: anything below the persisted
        // watermark may already have been handed out before an unclean
        // shutdown, so the whole block is skipped rather than risk reuse.
        let cas_start = read_u64(&meta, &wtxn, meta_key::CAS_WATERMARK)?.unwrap_or(0);

        // The whole tag table lives in RAM: it is small, and the read path
        // consults it on every tagged record.
        let registry = TagRegistry::new(config.max_tags);
        let mut loaded = Vec::new();
        for entry in tag_meta.iter(&wtxn).map_err(StoreError::from_heed)? {
            let (name, raw) = entry.map_err(StoreError::from_heed)?;
            let Some((id, generation)) = crate::tags::decode_entry(raw) else {
                return Err(StoreError::Corrupt(format!(
                    "tag registry entry for {name:?} is {} bytes, expected {}",
                    raw.len(),
                    crate::tags::TAG_RECORD_LEN
                )));
            };
            loaded.push((name.to_vec().into_boxed_slice(), id, generation));
        }
        let tag_count = loaded.len();
        registry.load_from(loaded);

        wtxn.commit().map_err(StoreError::from_heed)?;

        info!(
            path = %config.path.display(),
            durability = ?config.durability,
            map_size = config.map_size,
            epoch,
            cas_start,
            tag_count,
            "opened store"
        );

        Ok(Self {
            env,
            main,
            exp,
            tagidx,
            tag_meta,
            jobs,
            meta,
            clock: Clock::new(),
            epoch: AtomicU32::new(epoch),
            cas_next: AtomicU64::new(cas_start),
            // Equal to `cas_next`, so the first write reserves a block before
            // handing anything out.
            cas_watermark: AtomicU64::new(cas_start),
            max_value_len: config.max_value_len,
            bucket_granularity_ms: config.bucket_granularity_ms,
            tags: registry,
            pressure: AtomicU8::new(Pressure::Normal as u8),
            shard_index,
            shard_count: shard_count.max(1),
        })
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        self.env.write_txn().map_err(StoreError::from_heed)
    }

    pub fn read_txn(&self) -> Result<RoTxn<'_, WithoutTls>> {
        self.env.read_txn().map_err(StoreError::from_heed)
    }

    #[inline]
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    #[inline]
    pub fn epoch(&self) -> u32 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Publishes a bumped flush epoch. Applied only after its commit.
    pub fn set_epoch(&self, epoch: u32) {
        self.epoch.store(epoch, Ordering::Release);
    }

    #[inline]
    pub fn expiry_from_ttl(&self, ttl_secs: u32) -> u64 {
        self.clock.expiry_from_ttl(ttl_secs)
    }

    pub fn tags(&self) -> &TagRegistry {
        &self.tags
    }

    // ---- reads -------------------------------------------------------------

    fn read_record(
        &self,
        txn: &RoTxn<'_, WithoutTls>,
        lookup: &TagLookup<'_>,
        key: &[u8],
    ) -> Result<Option<Value>> {
        let Some(blob) = self.main.get(txn, key).map_err(StoreError::from_heed)? else {
            return Ok(None);
        };

        let record = RecordRef::parse(blob)?;
        if !record.is_alive(self.now_ms(), self.epoch(), |id| lookup.generation(id)) {
            // Expired, flushed or tag-invalidated: logically absent. Reclaiming
            // the space belongs to the sweeper and the reclaimer; a read never
            // writes.
            return Ok(None);
        }

        Ok(Some(Value {
            data: Bytes::copy_from_slice(record.value),
            mc_flags: record.mc_flags(),
            cas: record.cas(),
            expires_at_ms: Some(record.expires_at_ms()),
        }))
    }

    pub fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        let rtxn = self.read_txn()?;
        let lookup = self.tags.lookup();
        self.read_record(&rtxn, &lookup, key.as_bytes())
    }

    /// Resolves a whole batch inside one read transaction, so every key in a
    /// `GET_MANY` sees the same consistent snapshot — and under a single tag
    /// registry lock rather than one per key.
    pub fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        let rtxn = self.read_txn()?;
        let lookup = self.tags.lookup();
        keys.iter()
            .map(|key| self.read_record(&rtxn, &lookup, key.as_bytes()))
            .collect()
    }

    // ---- write preparation (runs off the writer thread) --------------------

    pub fn prepare_set(&self, set: &Set<'_>, tags: Vec<TagRef>) -> Result<PreparedSet> {
        // Refused here, on the caller's thread, rather than after a trip
        // through the write queue: there is no point encoding a record and
        // queueing it for a store that cannot accept it.
        if self.pressure() == Pressure::Critical {
            return Err(StoreError::CapacityFull);
        }
        validate_value(set.value, self.max_value_len)?;

        let expires_at_ms = self.expiry_from_ttl(set.ttl_secs);
        let meta = RecordMeta {
            epoch: self.epoch(),
            mc_flags: set.mc_flags,
            expires_at_ms,
            // Stamped by the writer in commit order; see `patch_cas`.
            cas: 0,
        };

        let mut record = Vec::with_capacity(cache_core::record_len(tags.len(), set.value.len()));
        encode_record(&mut record, meta, &tags, set.value)?;

        Ok(PreparedSet {
            key: set.key.as_bytes().into(),
            record,
            expires_at_ms,
            tags,
        })
    }

    // ---- writes (take the caller's transaction) ----------------------------

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
        // within a shard — and therefore within any single key, which is the
        // only ordering compare-and-swap depends on.
        Ok(counter * self.shard_count as u64 + self.shard_index as u64)
    }

    /// Removes the index entries belonging to whatever is currently stored
    /// under `key`.
    ///
    /// Without this, overwriting a key would leave its old expiry and tag index
    /// entries behind, and a hot key rewritten every second would accumulate
    /// dead entries until its buckets came due.
    fn drop_index_entries(&self, wtxn: &mut RwTxn, key: &[u8]) -> Result<()> {
        let Some(blob) = self.main.get(wtxn, key).map_err(StoreError::from_heed)? else {
            return Ok(());
        };
        let record = RecordRef::parse(blob)?;

        let expires_at_ms = record.expires_at_ms();
        let cas = record.cas();
        let tag_ids: Vec<u32> = record.tags.iter().map(|t| t.tag_id.get()).collect();

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

        Ok(())
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
    ) -> Result<Stored> {
        let existing = self
            .live_record(wtxn, &prepared.key)?
            .map(|r| (r.cas(), r.mc_flags(), r.expires_at_ms(), r.value.to_vec()));

        match (mode, &existing) {
            (SetMode::Set, _) => {}
            (SetMode::Add, Some(_)) => return Ok(Stored::NotStored),
            (SetMode::Add, None) => {}
            (SetMode::Replace, None) => return Ok(Stored::NotStored),
            (SetMode::Replace, Some(_)) => {}
            (SetMode::Append | SetMode::Prepend, None) => return Ok(Stored::NotStored),
            (SetMode::Cas(_), None) => return Ok(Stored::NotFound),
            (SetMode::Cas(expected), Some((cas, ..))) if *cas != expected => {
                return Ok(Stored::Exists);
            }
            (SetMode::Cas(_), Some(_)) => {}
            (SetMode::Append | SetMode::Prepend, Some(_)) => {}
        }

        // Concatenation keeps the stored value's TTL and client flags, matching
        // memcached: append/prepend carry no metadata of their own.
        if matches!(mode, SetMode::Append | SetMode::Prepend) {
            let (_, mc_flags, expires_at_ms, current) =
                existing.expect("guarded above: concatenation requires an existing record");

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
                Vec::with_capacity(cache_core::record_len(prepared.tags.len(), combined.len()));
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
            prepared.expires_at_ms = expires_at_ms;
        }

        Ok(Stored::Stored(self.apply_set(wtxn, prepared)?))
    }

    /// Adds to or subtracts from a value held as decimal text.
    ///
    /// The memcached protocol defines counters this way, so the value stays a
    /// plain string that a `get` returns unchanged.
    pub fn apply_incr(
        &self,
        wtxn: &mut RwTxn,
        key: &[u8],
        delta: u64,
        decrement: bool,
    ) -> Result<Option<u64>> {
        let Some(record) = self.live_record(wtxn, key)? else {
            return Ok(None);
        };

        let current = std::str::from_utf8(record.value)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .ok_or(StoreError::NotNumeric)?;

        // memcached clamps a decrement at zero rather than wrapping, and lets
        // an increment wrap at 64 bits.
        let updated = if decrement {
            current.saturating_sub(delta)
        } else {
            current.wrapping_add(delta)
        };

        let mc_flags = record.mc_flags();
        let expires_at_ms = record.expires_at_ms();
        let tags = record.tags.to_vec();
        let text = updated.to_string();

        let mut encoded = Vec::with_capacity(cache_core::record_len(tags.len(), text.len()));
        encode_record(
            &mut encoded,
            RecordMeta {
                epoch: self.epoch(),
                mc_flags,
                expires_at_ms,
                cas: 0,
            },
            &tags,
            text.as_bytes(),
        )?;

        let mut prepared = PreparedSet {
            key: key.into(),
            record: encoded,
            expires_at_ms,
            tags,
        };
        self.apply_set(wtxn, &mut prepared)?;

        Ok(Some(updated))
    }

    pub fn apply_set(&self, wtxn: &mut RwTxn, prepared: &mut PreparedSet) -> Result<u64> {
        let cas = self.next_cas(wtxn)?;
        patch_cas(&mut prepared.record, cas)?;

        self.drop_index_entries(wtxn, &prepared.key)?;

        self.main
            .put(wtxn, &prepared.key, &prepared.record)
            .map_err(StoreError::from_heed)?;

        // Indexed unconditionally: a record outside this index cannot be chosen
        // as an eviction victim, so a cache of TTL-less keys would have nothing
        // to free under pressure. Never-expiring records sort last (see
        // `expiry::NEVER_BUCKET`), so they go only after everything with a TTL.
        let index_key =
            crate::expiry::encode_key(prepared.expires_at_ms, cas, self.bucket_granularity_ms);
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

        Ok(cas)
    }

    /// Returns whether the key was live before the delete. A record that has
    /// expired but not yet been swept is already invisible to clients, so
    /// removing it counts as a miss even though it frees a row.
    pub fn apply_delete(&self, wtxn: &mut RwTxn, key: &[u8]) -> Result<bool> {
        let was_live = match self.main.get(wtxn, key).map_err(StoreError::from_heed)? {
            Some(blob) => {
                let lookup = self.tags.lookup();
                RecordRef::parse(blob)
                    .map(|r| r.is_alive(self.now_ms(), self.epoch(), |id| lookup.generation(id)))
                    .unwrap_or(false)
            }
            None => false,
        };

        self.drop_index_entries(wtxn, key)?;
        self.main.delete(wtxn, key).map_err(StoreError::from_heed)?;

        Ok(was_live)
    }

    /// Re-stamps a record's expiry without the client resending the value.
    ///
    /// LMDB values are immutable blobs, so this rewrites the record — the value
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
            expires_at_ms,
            tags,
        };
        self.apply_set(wtxn, &mut prepared)?;

        Ok(true)
    }

    /// Registers a tag, returning its id and starting generation.
    ///
    /// A record may never reference a tag that is not durably registered: after
    /// a restart the id would either be unknown (a spurious miss) or reused for
    /// a different name (a spurious invalidation).
    ///
    /// `start_generation` is the generation the rest of the node already holds
    /// for this name, which is normally 0 but is not when another shard has
    /// seen the tag and invalidated it. Starting from it keeps the generation
    /// uniform across a node's shards. Starting from zero instead would mean a
    /// record written here captured a *lower* number than the node reports to
    /// its peers, and the first gossip round would kill it — an invalidation of
    /// a record that was written after the last invalidation.
    pub fn apply_create_tag(
        &self,
        wtxn: &mut RwTxn,
        name: &[u8],
        start_generation: u64,
    ) -> Result<(u32, u64)> {
        validate_tag_name(name)?;

        // Another job in the same batch may have created it already.
        if let Some(raw) = self
            .tag_meta
            .get(wtxn, name)
            .map_err(StoreError::from_heed)?
            && let Some(existing) = crate::tags::decode_entry(raw)
        {
            return Ok(existing);
        }

        let id = self.tags.allocate_id()?;
        self.tag_meta
            .put(wtxn, name, &crate::tags::encode_entry(id, start_generation))
            .map_err(StoreError::from_heed)?;

        Ok((id, start_generation))
    }

    /// Raises a tag's generation to `generation`, creating the tag if this
    /// shard has never seen the name.
    ///
    /// **Merges by maximum**, never assigns, which is what makes an
    /// invalidation received from a peer idempotent, order-independent and safe
    /// to retry — see [`cache_core::cluster`]. It is also how a local
    /// invalidation is applied, so both paths share one implementation and one
    /// set of consequences.
    pub fn apply_merge_tag(
        &self,
        wtxn: &mut RwTxn,
        name: &[u8],
        generation: u64,
    ) -> Result<TagMerge> {
        validate_tag_name(name)?;

        let existing = match self
            .tag_meta
            .get(wtxn, name)
            .map_err(StoreError::from_heed)?
        {
            Some(raw) => Some(crate::tags::decode_entry(raw).ok_or_else(|| {
                StoreError::Corrupt(format!("tag registry entry for {name:?} is malformed"))
            })?),
            None => None,
        };

        let Some((id, current)) = existing else {
            // Nothing here can carry a tag this shard has never registered, so
            // there is no reclamation to queue — only the registration itself,
            // so that records written from now on capture the right number.
            let (id, generation) = self.apply_create_tag(wtxn, name, generation)?;
            return Ok(TagMerge::Created { id, generation });
        };

        if generation <= current {
            return Ok(TagMerge::Unchanged {
                id,
                generation: current,
            });
        }

        self.tag_meta
            .put(wtxn, name, &crate::tags::encode_entry(id, generation))
            .map_err(StoreError::from_heed)?;

        // One job per tag: raising the target mid-reclaim restarts the scan
        // rather than queueing a second pass.
        self.jobs
            .put(wtxn, &id.to_be_bytes(), &Job::new(generation).encode())
            .map_err(StoreError::from_heed)?;

        Ok(TagMerge::Raised { id, generation })
    }

    /// Invalidates a tag by bumping its generation.
    ///
    /// **Constant time**, regardless of how many keys carry the tag: every
    /// record referencing it compares unequal from the moment this commits.
    /// Freeing their space is handed to the reclaimer.
    ///
    /// Returns `None` when the tag has never been registered, in which case no
    /// record can reference it and there is nothing to do.
    pub fn apply_delete_by_tag(&self, wtxn: &mut RwTxn, name: &[u8]) -> Result<Option<(u32, u64)>> {
        let Some(raw) = self
            .tag_meta
            .get(wtxn, name)
            .map_err(StoreError::from_heed)?
        else {
            return Ok(None);
        };
        let Some((id, generation)) = crate::tags::decode_entry(raw) else {
            return Err(StoreError::Corrupt(format!(
                "tag registry entry for {name:?} is malformed"
            )));
        };

        // Read from disk rather than RAM so the counter advances from the
        // durable value; a RAM-only bump lost to a crash would resurrect data.
        // Saturating rather than wrapping: going backwards would resurrect
        // every record ever invalidated under this tag, which is far worse than
        // an invalidation that cannot advance.
        let next = generation.saturating_add(1);
        if next == generation {
            warn!(
                ?name,
                "tag generation has saturated; invalidation had no effect"
            );
            return Ok(Some((id, generation)));
        }

        let merged = self.apply_merge_tag(wtxn, name, next)?;
        Ok(Some((merged.id(), merged.generation())))
    }

    /// Empties the cache, returning the new flush epoch.
    ///
    /// The epoch bump and the clear do different jobs, and both are needed. The
    /// clear frees the space — an epoch bump alone would leak every record
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

    /// Reclaims up to `budget` expired records.
    ///
    /// Walks the expiry index from the front and stops at the first bucket in
    /// the future, so the cost is proportional to what has actually expired.
    /// When nothing is due this is a single cursor seek.
    pub fn sweep(&self, wtxn: &mut RwTxn, budget: usize) -> Result<SweepStats> {
        self.drain_expiry_index(wtxn, budget, Victims::ExpiredOnly)
    }

    /// Frees up to `budget` records to reclaim space, whether or not they have
    /// expired.
    ///
    /// Takes them from the front of the expiry index, which is soonest-to-expire
    /// first and never-expiring last. That ordering is free — the index already
    /// exists for TTLs — and it is the right policy for a cache: a TTL is the
    /// client's own statement of how long a value is worth keeping, so the item
    /// dying in three seconds is the cheapest thing in the store to lose.
    ///
    /// Deliberately not LRU. Tracking recency means writing on every read, which
    /// on a single-writer engine would put every GET behind the write queue.
    /// See plan §6.
    pub fn evict(&self, wtxn: &mut RwTxn, budget: usize) -> Result<SweepStats> {
        self.drain_expiry_index(wtxn, budget, Victims::Anything)
    }

    fn drain_expiry_index(
        &self,
        wtxn: &mut RwTxn,
        budget: usize,
        accept: Victims,
    ) -> Result<SweepStats> {
        let now_ms = self.now_ms();
        let mut stats = SweepStats::default();

        // Collected up front because LMDB will not let the cursor and the
        // deletes share the transaction cleanly. `budget` bounds the memory.
        let mut victims: Vec<([u8; crate::expiry::EXPIRY_KEY_LEN], Vec<u8>)> = Vec::new();
        {
            let iter = self.exp.iter(wtxn).map_err(StoreError::from_heed)?;
            for entry in iter {
                let (index_key, user_key) = entry.map_err(StoreError::from_heed)?;
                let Some((bucket, _)) = crate::expiry::decode_key(index_key) else {
                    return Err(StoreError::Corrupt(format!(
                        "expiry index key is {} bytes, expected {}",
                        index_key.len(),
                        crate::expiry::EXPIRY_KEY_LEN
                    )));
                };

                // A sweep stops at the first bucket in the future; an eviction
                // keeps going, because it is freeing space rather than
                // honouring TTLs.
                if accept == Victims::ExpiredOnly && bucket > now_ms {
                    stats.lag_ms = 0;
                    break;
                }
                if victims.is_empty() && bucket <= now_ms {
                    stats.lag_ms = now_ms.saturating_sub(bucket);
                }

                let mut owned = [0u8; crate::expiry::EXPIRY_KEY_LEN];
                owned.copy_from_slice(index_key);
                victims.push((owned, user_key.to_vec()));

                if victims.len() >= budget {
                    stats.budget_exhausted = true;
                    break;
                }
            }
        }

        stats.scanned = victims.len();

        for (index_key, user_key) in victims {
            let (_, entry_cas) = crate::expiry::decode_key(&index_key).expect("validated above");

            match self
                .main
                .get(wtxn, &user_key)
                .map_err(StoreError::from_heed)?
            {
                Some(blob) => {
                    let record = RecordRef::parse(blob)?;
                    // The CAS check is what makes a stale entry harmless: if the
                    // key was overwritten, this entry no longer describes the
                    // record and must not delete it.
                    let current = record.cas() == entry_cas;
                    let take = current
                        && match accept {
                            Victims::ExpiredOnly => record.is_expired(now_ms),
                            Victims::Anything => true,
                        };

                    if take {
                        let tag_ids: Vec<u32> =
                            record.tags.iter().map(|t| t.tag_id.get()).collect();
                        self.main
                            .delete(wtxn, &user_key)
                            .map_err(StoreError::from_heed)?;
                        for tag_id in tag_ids {
                            self.tagidx
                                .delete(wtxn, &reclaim::index_key(tag_id, &user_key))
                                .map_err(StoreError::from_heed)?;
                        }
                        stats.reclaimed += 1;
                    } else {
                        stats.stale += 1;
                    }
                }
                None => stats.stale += 1,
            }

            self.exp
                .delete(wtxn, &index_key)
                .map_err(StoreError::from_heed)?;
        }

        Ok(stats)
    }

    /// Bytes genuinely occupied, excluding pages on the free list.
    ///
    /// **Not** `last_page_number`. LMDB never returns freed pages to the OS nor
    /// lowers its high-water mark — a deleted record's pages go onto a free
    /// list for reuse — so a high-water measure only ever rises. Using it for
    /// the capacity watermarks meant pressure could never fall, and the evictor
    /// would keep going until the cache was empty.
    ///
    /// Summed from the sub-databases' own page counts, which is exactly the
    /// non-free total, and works inside the caller's transaction rather than
    /// needing one of its own.
    pub fn used_bytes_in(&self, txn: &RoTxn<'_, WithoutTls>) -> Result<u64> {
        let page_size = self.env.stat().page_size as u64;
        let mut pages = 0u64;
        for db in [
            &self.main,
            &self.exp,
            &self.tagidx,
            &self.tag_meta,
            &self.jobs,
            &self.meta,
        ] {
            let stat = db.stat(txn).map_err(StoreError::from_heed)?;
            pages += stat.branch_pages as u64 + stat.leaf_pages as u64 + stat.overflow_pages as u64;
        }
        Ok(pages * page_size)
    }

    /// Fraction of the map in use, the input to the capacity watermarks.
    pub fn utilisation_in(&self, txn: &RoTxn<'_, WithoutTls>) -> Result<f64> {
        Ok(self.used_bytes_in(txn)? as f64 / self.env.info().map_size as f64)
    }

    /// Current capacity pressure, as last measured by the writer's maintenance
    /// pass. Read on the write path to reject early, so a full store fails fast
    /// instead of queueing work it cannot commit.
    pub fn pressure(&self) -> Pressure {
        Pressure::from_u8(self.pressure.load(Ordering::Relaxed))
    }

    pub(crate) fn set_pressure(&self, pressure: Pressure) {
        self.pressure.store(pressure as u8, Ordering::Relaxed);
    }

    /// Advances the oldest outstanding tag-reclamation job by up to `budget`
    /// index entries.
    ///
    /// Resumable by design: the cursor is persisted in the same transaction as
    /// the deletions, so a crash or restart continues from where it stopped
    /// rather than starting the tag over.
    pub fn reclaim_step(&self, wtxn: &mut RwTxn, budget: usize) -> Result<Option<ReclaimStats>> {
        let Some((tag_id, job)) = self.next_job(wtxn)? else {
            return Ok(None);
        };

        let (low, high) = reclaim::index_range(tag_id);
        let start = match &job.cursor {
            // Exclusive: resume just past the last entry processed.
            Some(cursor) => Bound::Excluded(cursor.to_vec()),
            None => Bound::Included(low.to_vec()),
        };

        let mut batch: Vec<([u8; reclaim::INDEX_KEY_LEN], Vec<u8>)> = Vec::new();
        {
            let bounds = (
                match &start {
                    Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
                    _ => Bound::Included(low.as_slice()),
                },
                Bound::Included(high.as_slice()),
            );
            let iter = self
                .tagidx
                .range(wtxn, &bounds)
                .map_err(StoreError::from_heed)?;

            for entry in iter {
                let (index_key, user_key) = entry.map_err(StoreError::from_heed)?;
                let mut owned = [0u8; reclaim::INDEX_KEY_LEN];
                if index_key.len() != reclaim::INDEX_KEY_LEN {
                    return Err(StoreError::Corrupt(format!(
                        "tag index key is {} bytes, expected {}",
                        index_key.len(),
                        reclaim::INDEX_KEY_LEN
                    )));
                }
                owned.copy_from_slice(index_key);
                batch.push((owned, user_key.to_vec()));

                if batch.len() >= budget {
                    break;
                }
            }
        }

        let mut stats = ReclaimStats {
            scanned: batch.len(),
            ..ReclaimStats::default()
        };
        let mut cursor = job.cursor;

        let now_ms = self.now_ms();
        let epoch = self.epoch();

        for (index_key, user_key) in batch {
            cursor = Some(index_key);

            let Some(blob) = self
                .main
                .get(wtxn, &user_key)
                .map_err(StoreError::from_heed)?
            else {
                // The record is already gone; its index entry is litter.
                self.tagidx
                    .delete(wtxn, &index_key)
                    .map_err(StoreError::from_heed)?;
                stats.orphaned += 1;
                continue;
            };

            let record = RecordRef::parse(blob)?;

            // Deadness is judged against the job's own target generation, not
            // the live registry.
            //
            // This pass can share a transaction with the very `DELETE_BY_TAG`
            // that queued it, and the registry is only updated *after* that
            // commits — so the in-memory generation still reads as the old one
            // here. Trusting it would mark every record live, advance the
            // cursor past them, and leak them permanently. The job carries the
            // target precisely so this decision needs no RAM state.
            let doomed = match record.tags.iter().find(|t| t.tag_id.get() == tag_id) {
                Some(tag) => tag.generation.get() < job.target_generation,
                None => {
                    // The record no longer carries this tag, so the entry is
                    // litter — but the record itself is somebody else's.
                    self.tagidx
                        .delete(wtxn, &index_key)
                        .map_err(StoreError::from_heed)?;
                    stats.orphaned += 1;
                    continue;
                }
            };

            // A record the registry already knows to be dead — expired, flushed
            // or invalidated via another tag — is fair game too. RAM can lag
            // behind the truth but never runs ahead of it, so "dead" is always
            // trustworthy even when "alive" is not.
            let known_dead = {
                let lookup = self.tags.lookup();
                !record.is_alive(now_ms, epoch, |id| lookup.generation(id))
            };

            if !doomed && !known_dead {
                // Rewritten since the invalidation, so this entry is current
                // and must survive.
                stats.retained += 1;
                continue;
            }

            let expires_at_ms = record.expires_at_ms();
            let cas = record.cas();
            let tag_ids: Vec<u32> = record.tags.iter().map(|t| t.tag_id.get()).collect();

            self.main
                .delete(wtxn, &user_key)
                .map_err(StoreError::from_heed)?;
            if expires_at_ms != NEVER {
                self.exp
                    .delete(
                        wtxn,
                        &crate::expiry::encode_key(expires_at_ms, cas, self.bucket_granularity_ms),
                    )
                    .map_err(StoreError::from_heed)?;
            }
            for other in tag_ids {
                self.tagidx
                    .delete(wtxn, &reclaim::index_key(other, &user_key))
                    .map_err(StoreError::from_heed)?;
            }
            stats.reclaimed += 1;
        }

        // Fewer than the budget means the range ran out, so the tag is done.
        if stats.scanned < budget {
            self.jobs
                .delete(wtxn, &tag_id.to_be_bytes())
                .map_err(StoreError::from_heed)?;
            stats.completed = true;
        } else {
            let resumed = Job {
                target_generation: job.target_generation,
                cursor,
            };
            self.jobs
                .put(wtxn, &tag_id.to_be_bytes(), &resumed.encode())
                .map_err(StoreError::from_heed)?;
        }

        Ok(Some(stats))
    }

    fn next_job(&self, wtxn: &RwTxn) -> Result<Option<(u32, Job)>> {
        let Some(entry) = self
            .jobs
            .iter(wtxn)
            .map_err(StoreError::from_heed)?
            .next()
            .transpose()
            .map_err(StoreError::from_heed)?
        else {
            return Ok(None);
        };

        let (raw_id, raw_job) = entry;
        let id: [u8; 4] = raw_id.try_into().map_err(|_| {
            StoreError::Corrupt("reclaim job key is not a 4-byte tag id".to_string())
        })?;
        Ok(Some((u32::from_be_bytes(id), Job::decode(raw_job)?)))
    }

    pub fn pending_jobs(&self, txn: &RoTxn<'_, WithoutTls>) -> Result<u64> {
        Ok(self.jobs.stat(txn).map_err(StoreError::from_heed)?.entries as u64)
    }

    // ---- housekeeping ------------------------------------------------------

    pub fn stats(&self) -> Result<StoreStats> {
        let info = self.env.info();
        let stat = self.env.stat();
        let rtxn = self.read_txn()?;

        let entries = self
            .main
            .stat(&rtxn)
            .map_err(StoreError::from_heed)?
            .entries as u64;
        let expiry_entries = self.exp.stat(&rtxn).map_err(StoreError::from_heed)?.entries as u64;
        let tag_index_entries = self
            .tagidx
            .stat(&rtxn)
            .map_err(StoreError::from_heed)?
            .entries as u64;
        let pending_reclaims = self.pending_jobs(&rtxn)?;

        let used_bytes = self.used_bytes_in(&rtxn)?;
        let _ = stat;

        Ok(StoreStats {
            entries,
            expiry_entries,
            tag_index_entries,
            tags: self.tags.len() as u64,
            pending_reclaims,
            map_size: info.map_size as u64,
            used_bytes,
            utilisation: used_bytes as f64 / info.map_size as f64,
            readers_in_use: info.number_of_readers,
            max_readers: info.maximum_number_of_readers,
            epoch: self.epoch(),
            // Owned by the writer thread, merged in by `LmdbStore::stats`.
            ..StoreStats::default()
        })
    }

    pub fn sync(&self) -> Result<()> {
        self.env.force_sync().map_err(StoreError::from_heed)
    }

    /// Closes the environment, blocking until LMDB has fully released it.
    ///
    /// Dropping an `Env` only schedules the close. LMDB keeps a process-wide
    /// registry of open environments and refuses to reopen a path still in it,
    /// so anything that reopens a database in-process has to wait for this.
    pub fn close(self) {
        self.env.prepare_for_closing().wait();
    }
}

fn create_db(env: &Env<WithoutTls>, wtxn: &mut RwTxn, name: &str) -> Result<Db> {
    env.create_database(wtxn, Some(name))
        .map_err(StoreError::from_heed)
}

fn env_flags(durability: Durability) -> EnvFlags {
    let mut flags = EnvFlags::empty();
    match durability {
        Durability::Durable => {}
        Durability::Relaxed => flags |= EnvFlags::NO_META_SYNC,
        Durability::Ephemeral => {
            flags |= EnvFlags::NO_SYNC;
            // WRITE_MAP would add a further gain but fails at env-open on
            // Windows with OS error 6 at every map size tested. Unix only.
            #[cfg(unix)]
            {
                flags |= EnvFlags::WRITE_MAP;
            }
        }
    }
    flags
}

fn read_u32(db: &Db, txn: &RwTxn, key: &[u8]) -> Result<Option<u32>> {
    match db.get(txn, key).map_err(StoreError::from_heed)? {
        Some(raw) => raw
            .try_into()
            .map(u32::from_le_bytes)
            .map(Some)
            .map_err(|_| StoreError::Corrupt(format!("meta key {key:?} is not 4 bytes"))),
        None => Ok(None),
    }
}

fn read_u64(db: &Db, txn: &RwTxn, key: &[u8]) -> Result<Option<u64>> {
    match db.get(txn, key).map_err(StoreError::from_heed)? {
        Some(raw) => raw
            .try_into()
            .map(u64::from_le_bytes)
            .map(Some)
            .map_err(|_| StoreError::Corrupt(format!("meta key {key:?} is not 8 bytes"))),
        None => Ok(None),
    }
}
