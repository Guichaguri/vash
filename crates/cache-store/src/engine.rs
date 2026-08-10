//! LMDB operations, with no threading of their own.
//!
//! Every write method takes the transaction it should act in, so the writer
//! thread can pack many of them into one commit (see [`crate::writer`]). Keeping
//! the transaction out of this layer is what makes group commit possible
//! without duplicating the operations.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use cache_core::{
    Clock, Key, RecordMeta, RecordRef, Set, Value, encode_record, patch_cas, record::NEVER,
    validate_value,
};
use heed::types::Bytes as HeedBytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn, WithoutTls};
use tracing::{info, warn};

use crate::config::{Durability, StoreConfig};
use crate::error::{Result, StoreError};
use crate::expiry;
use crate::schema::{CAS_BLOCK, MAX_DBS, SCHEMA_VERSION, db, meta_key};
use crate::{StoreStats, SweepStats};

type Db = Database<HeedBytes, HeedBytes>;

/// A record encoded and ready to store, still missing its CAS token.
///
/// Built on the calling thread so that the value copy and the record framing —
/// the expensive part of a write — happen in parallel across connections, and
/// the single writer thread is left with only the B-tree work.
pub struct PreparedSet {
    pub key: Box<[u8]>,
    pub record: Vec<u8>,
    pub expires_at_ms: u64,
}

pub struct LmdbEngine {
    env: Env<WithoutTls>,
    main: Db,
    exp: Db,
    meta: Db,
    clock: Clock,
    epoch: AtomicU32,
    cas_next: AtomicU64,
    cas_watermark: AtomicU64,
    max_value_len: usize,
    bucket_granularity_ms: u64,
}

impl LmdbEngine {
    pub fn open(config: &StoreConfig) -> Result<Self> {
        if config.wipe_on_start && config.path.exists() {
            warn!(path = %config.path.display(), "wiping existing database on start");
            std::fs::remove_dir_all(&config.path)?;
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
        let main: Db = env
            .create_database(&mut wtxn, Some(db::MAIN))
            .map_err(StoreError::from_heed)?;
        let exp: Db = env
            .create_database(&mut wtxn, Some(db::EXPIRY))
            .map_err(StoreError::from_heed)?;
        let meta: Db = env
            .create_database(&mut wtxn, Some(db::META))
            .map_err(StoreError::from_heed)?;

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
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => {
                return Err(StoreError::Corrupt(format!(
                    "database has schema version {v}, this build expects {SCHEMA_VERSION}"
                )));
            }
        }

        let epoch = read_u32(&meta, &wtxn, meta_key::EPOCH)?.unwrap_or(0);
        // Resume past the last reserved block: anything below the persisted
        // watermark may already have been handed out before an unclean
        // shutdown, so the whole block is skipped rather than risk reuse.
        let cas_start = read_u64(&meta, &wtxn, meta_key::CAS_WATERMARK)?.unwrap_or(0);

        wtxn.commit().map_err(StoreError::from_heed)?;

        info!(
            path = %config.path.display(),
            durability = ?config.durability,
            map_size = config.map_size,
            epoch,
            cas_start,
            "opened store"
        );

        Ok(Self {
            env,
            main,
            exp,
            meta,
            clock: Clock::new(),
            epoch: AtomicU32::new(epoch),
            cas_next: AtomicU64::new(cas_start),
            // Equal to `cas_next`, so the first write reserves a block before
            // handing anything out.
            cas_watermark: AtomicU64::new(cas_start),
            max_value_len: config.max_value_len,
            bucket_granularity_ms: config.bucket_granularity_ms,
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
        self.epoch.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn expiry_from_ttl(&self, ttl_secs: u32) -> u64 {
        self.clock.expiry_from_ttl(ttl_secs)
    }

    /// Tag generation lookup. No record can carry a tag until the registry
    /// lands in M2; returning `None` fails closed if one somehow appears,
    /// turning a stale hit into a miss.
    #[inline]
    fn tag_generation(_tag_id: u32) -> Option<u64> {
        None
    }

    // ---- reads -------------------------------------------------------------

    pub fn get_in(&self, rtxn: &RoTxn<'_, WithoutTls>, key: Key<'_>) -> Result<Option<Value>> {
        let Some(blob) = self
            .main
            .get(rtxn, key.as_bytes())
            .map_err(StoreError::from_heed)?
        else {
            return Ok(None);
        };

        let record = RecordRef::parse(blob)?;
        if !record.is_alive(self.now_ms(), self.epoch(), Self::tag_generation) {
            // Logically absent. Reclaiming the space is the sweeper's job; a
            // read never writes.
            return Ok(None);
        }

        Ok(Some(Value {
            data: Bytes::copy_from_slice(record.value),
            mc_flags: record.mc_flags(),
            cas: record.cas(),
        }))
    }

    /// Resolves a whole batch inside one read transaction, so every key in a
    /// `GET_MANY` sees the same consistent snapshot.
    pub fn get_many(&self, keys: &[Key<'_>]) -> Result<Vec<Option<Value>>> {
        let rtxn = self.read_txn()?;
        keys.iter().map(|key| self.get_in(&rtxn, *key)).collect()
    }

    pub fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        let rtxn = self.read_txn()?;
        self.get_in(&rtxn, key)
    }

    // ---- write preparation (runs off the writer thread) --------------------

    pub fn prepare_set(&self, set: &Set<'_>) -> Result<PreparedSet> {
        validate_value(set.value, self.max_value_len)?;
        if !set.tags.is_empty() {
            return Err(StoreError::Unsupported("tagging"));
        }

        let expires_at_ms = self.expiry_from_ttl(set.ttl_secs);
        let meta = RecordMeta {
            epoch: self.epoch(),
            mc_flags: set.mc_flags,
            expires_at_ms,
            // Stamped by the writer in commit order; see `patch_cas`.
            cas: 0,
        };

        let mut record = Vec::with_capacity(cache_core::record_len(0, set.value.len()));
        encode_record(&mut record, meta, &[], set.value)?;

        Ok(PreparedSet {
            key: set.key.as_bytes().into(),
            record,
            expires_at_ms,
        })
    }

    // ---- writes (take the caller's transaction) ----------------------------

    /// Allocates the next CAS token, extending the durable reservation when the
    /// current block runs out. Called only from the writer thread, whose single
    /// transaction serialises it.
    fn next_cas(&self, wtxn: &mut RwTxn) -> Result<u64> {
        let cas = self.cas_next.fetch_add(1, Ordering::Relaxed) + 1;
        if cas >= self.cas_watermark.load(Ordering::Relaxed) {
            let watermark = cas + CAS_BLOCK;
            self.meta
                .put(wtxn, meta_key::CAS_WATERMARK, &watermark.to_le_bytes())
                .map_err(StoreError::from_heed)?;
            self.cas_watermark.store(watermark, Ordering::Relaxed);
        }
        Ok(cas)
    }

    /// Removes the expiry-index entry belonging to whatever is currently stored
    /// under `key`.
    ///
    /// Without this, overwriting a key would leave its old index entry behind,
    /// and a hot key rewritten every second would accumulate one dead entry per
    /// write until its bucket came due.
    fn drop_expiry_entry(&self, wtxn: &mut RwTxn, key: &[u8]) -> Result<()> {
        let existing = self
            .main
            .get(wtxn, key)
            .map_err(StoreError::from_heed)?
            .map(RecordRef::parse)
            .transpose()?
            .map(|rec| (rec.expires_at_ms(), rec.cas()));

        if let Some((expires_at_ms, cas)) = existing
            && expires_at_ms != NEVER
        {
            let index_key = expiry::encode_key(expires_at_ms, cas, self.bucket_granularity_ms);
            self.exp
                .delete(wtxn, &index_key)
                .map_err(StoreError::from_heed)?;
        }
        Ok(())
    }

    pub fn apply_set(&self, wtxn: &mut RwTxn, prepared: &mut PreparedSet) -> Result<u64> {
        let cas = self.next_cas(wtxn)?;
        patch_cas(&mut prepared.record, cas)?;

        self.drop_expiry_entry(wtxn, &prepared.key)?;

        self.main
            .put(wtxn, &prepared.key, &prepared.record)
            .map_err(StoreError::from_heed)?;

        if prepared.expires_at_ms != NEVER {
            let index_key =
                expiry::encode_key(prepared.expires_at_ms, cas, self.bucket_granularity_ms);
            self.exp
                .put(wtxn, &index_key, &prepared.key)
                .map_err(StoreError::from_heed)?;
        }

        Ok(cas)
    }

    /// Returns whether the key was live before the delete. A record that has
    /// expired but not yet been swept is already invisible to clients, so
    /// removing it counts as a miss even though it frees a row.
    pub fn apply_delete(&self, wtxn: &mut RwTxn, key: &[u8]) -> Result<bool> {
        let was_live = match self.main.get(wtxn, key).map_err(StoreError::from_heed)? {
            Some(blob) => RecordRef::parse(blob)
                .map(|r| r.is_alive(self.now_ms(), self.epoch(), Self::tag_generation))
                .unwrap_or(false),
            None => false,
        };

        self.drop_expiry_entry(wtxn, key)?;
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
        if !record.is_alive(now_ms, epoch, Self::tag_generation) {
            return Ok(false);
        }

        let expires_at_ms = self.expiry_from_ttl(ttl_secs);
        let mut rewritten = Vec::with_capacity(blob.len());
        encode_record(
            &mut rewritten,
            RecordMeta {
                epoch: record.header.epoch.get(),
                mc_flags: record.mc_flags(),
                expires_at_ms,
                cas: 0,
            },
            record.tags,
            record.value,
        )?;

        let cas = self.next_cas(wtxn)?;
        patch_cas(&mut rewritten, cas)?;

        self.drop_expiry_entry(wtxn, key)?;
        self.main
            .put(wtxn, key, &rewritten)
            .map_err(StoreError::from_heed)?;

        if expires_at_ms != NEVER {
            let index_key = expiry::encode_key(expires_at_ms, cas, self.bucket_granularity_ms);
            self.exp
                .put(wtxn, &index_key, key)
                .map_err(StoreError::from_heed)?;
        }

        Ok(true)
    }

    /// Reclaims up to `budget` expired records.
    ///
    /// Walks the expiry index from the front and stops at the first bucket in
    /// the future, so the cost is proportional to what has actually expired.
    /// When nothing is due this is a single cursor seek.
    pub fn sweep(&self, wtxn: &mut RwTxn, budget: usize) -> Result<SweepStats> {
        let now_ms = self.now_ms();
        let mut stats = SweepStats::default();

        // Collected up front because LMDB will not let the cursor and the
        // deletes share the transaction cleanly. `budget` bounds the memory.
        let mut victims: Vec<([u8; expiry::EXPIRY_KEY_LEN], Vec<u8>)> = Vec::new();
        {
            let iter = self.exp.iter(wtxn).map_err(StoreError::from_heed)?;
            for entry in iter {
                let (index_key, user_key) = entry.map_err(StoreError::from_heed)?;
                let Some((bucket, _)) = expiry::decode_key(index_key) else {
                    return Err(StoreError::Corrupt(format!(
                        "expiry index key is {} bytes, expected {}",
                        index_key.len(),
                        expiry::EXPIRY_KEY_LEN
                    )));
                };

                if bucket > now_ms {
                    // The index is time-ordered, so the first future bucket
                    // ends the scan.
                    stats.lag_ms = 0;
                    break;
                }
                if victims.is_empty() {
                    stats.lag_ms = now_ms.saturating_sub(bucket);
                }

                let mut owned = [0u8; expiry::EXPIRY_KEY_LEN];
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
            let (_, entry_cas) = expiry::decode_key(&index_key).expect("validated above");

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
                    if record.cas() == entry_cas && record.is_expired(now_ms) {
                        self.main
                            .delete(wtxn, &user_key)
                            .map_err(StoreError::from_heed)?;
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

        let page_size = stat.page_size as u64;
        let used_bytes = (info.last_page_number as u64 + 1) * page_size;

        Ok(StoreStats {
            entries,
            expiry_entries,
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
