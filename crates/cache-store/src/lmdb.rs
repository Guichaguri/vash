use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use bytes::Bytes;
use cache_core::{Clock, Key, RecordMeta, RecordRef, Set, Value, encode_record, validate_value};
use heed::types::Bytes as HeedBytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RwTxn, WithoutTls};
use tracing::{info, warn};

use crate::config::{Durability, StoreConfig};
use crate::error::{Result, StoreError};
use crate::schema::{CAS_BLOCK, MAX_DBS, SCHEMA_VERSION, db, meta_key};
use crate::{Store, StoreStats};

type Db = Database<HeedBytes, HeedBytes>;

pub struct LmdbStore {
    env: Env<WithoutTls>,
    main: Db,
    meta: Db,
    clock: Clock,
    /// Global flush epoch. Records written with a different value are dead.
    epoch: AtomicU32,
    cas_next: AtomicU64,
    cas_watermark: AtomicU64,
    max_value_len: usize,
}

impl LmdbStore {
    pub fn open(config: &StoreConfig) -> Result<Self> {
        if config.wipe_on_start && config.path.exists() {
            warn!(path = %config.path.display(), "wiping existing database on start");
            std::fs::remove_dir_all(&config.path)?;
        }
        std::fs::create_dir_all(&config.path)?;

        let flags = env_flags(config.durability);

        // SAFETY: LMDB maps the file into this process's address space. The
        // contract is that no other process mutates the file concurrently
        // outside of LMDB's own locking, which is satisfied by the lock file
        // LMDB maintains in the same directory.
        let env = unsafe {
            EnvOpenOptions::new()
                // Detaches read transactions from thread-local storage so a
                // RoTxn is Send and can move between reader threads. Required
                // by the storage-tier design in plan §9.
                .read_txn_without_tls()
                .flags(flags)
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
        let meta: Db = env
            .create_database(&mut wtxn, Some(db::META))
            .map_err(StoreError::from_heed)?;

        let stored_schema = read_u32(&meta, &wtxn, meta_key::SCHEMA_VERSION)?;
        match stored_schema {
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

        // Resume the CAS sequence past the last reserved block. Anything below
        // the persisted watermark may already have been handed out before an
        // unclean shutdown, so we skip the whole block rather than risk reusing
        // a token.
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
            meta,
            clock: Clock::new(),
            epoch: AtomicU32::new(epoch),
            cas_next: AtomicU64::new(cas_start),
            // Equal to `cas_next`, so the first write reserves a block before
            // handing anything out.
            cas_watermark: AtomicU64::new(cas_start),
            max_value_len: config.max_value_len,
        })
    }

    /// Closes the environment, blocking until LMDB has fully released it.
    ///
    /// Dropping an `Env` only *schedules* the close: LMDB keeps a process-wide
    /// registry of open environments, and reopening the same path before the
    /// previous handle is gone fails with "environment already open in this
    /// program". Anything that reopens a database in the same process — a
    /// restart test, a reload — has to wait for this.
    pub fn close(self) {
        self.env.prepare_for_closing().wait();
    }

    /// Allocates the next CAS token, extending the durable reservation when the
    /// current block runs out.
    ///
    /// Called while holding the write transaction, which LMDB serialises, so the
    /// read-modify-write of the watermark cannot race.
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

    #[inline]
    fn epoch(&self) -> u32 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Tag generation lookup. Until the registry lands in M2 no record can carry
    /// a tag, so this is never consulted; returning `None` fails closed if one
    /// somehow appears, turning a stale hit into a miss.
    #[inline]
    fn tag_generation(_tag_id: u32) -> Option<u64> {
        None
    }
}

impl Store for LmdbStore {
    fn get(&self, key: Key<'_>) -> Result<Option<Value>> {
        let rtxn = self.env.read_txn().map_err(StoreError::from_heed)?;
        let Some(blob) = self
            .main
            .get(&rtxn, key.as_bytes())
            .map_err(StoreError::from_heed)?
        else {
            return Ok(None);
        };

        let record = RecordRef::parse(blob)?;
        if !record.is_alive(self.clock.now_ms(), self.epoch(), Self::tag_generation) {
            // Logically absent. Reclaiming the space is the sweeper's job (M1);
            // a read never writes.
            return Ok(None);
        }

        Ok(Some(Value {
            data: Bytes::copy_from_slice(record.value),
            mc_flags: record.mc_flags(),
            cas: record.cas(),
        }))
    }

    fn set(&self, set: &Set<'_>) -> Result<u64> {
        validate_value(set.value, self.max_value_len)?;
        if !set.tags.is_empty() {
            return Err(StoreError::Unsupported("tagging"));
        }

        let expires_at_ms = self.clock.expiry_from_ttl(set.ttl_secs);

        let mut wtxn = self.env.write_txn().map_err(StoreError::from_heed)?;
        let cas = self.next_cas(&mut wtxn)?;

        let meta = RecordMeta {
            epoch: self.epoch(),
            mc_flags: set.mc_flags,
            expires_at_ms,
            cas,
        };

        // M1 replaces this per-call allocation with a buffer owned by the shard
        // writer thread and reused across the whole commit batch.
        let mut buf = Vec::with_capacity(cache_core::record_len(0, set.value.len()));
        encode_record(&mut buf, meta, &[], set.value)?;

        self.main
            .put(&mut wtxn, set.key.as_bytes(), &buf)
            .map_err(StoreError::from_heed)?;
        wtxn.commit().map_err(StoreError::from_heed)?;

        Ok(cas)
    }

    fn delete(&self, key: Key<'_>) -> Result<bool> {
        let mut wtxn = self.env.write_txn().map_err(StoreError::from_heed)?;

        // Report whether the key was *live*, not merely present: a record that
        // has expired but not yet been swept is already invisible to clients, so
        // deleting it is a miss even though it frees a row.
        let was_live = match self
            .main
            .get(&wtxn, key.as_bytes())
            .map_err(StoreError::from_heed)?
        {
            Some(blob) => RecordRef::parse(blob)
                .map(|r| r.is_alive(self.clock.now_ms(), self.epoch(), Self::tag_generation))
                .unwrap_or(false),
            None => false,
        };

        self.main
            .delete(&mut wtxn, key.as_bytes())
            .map_err(StoreError::from_heed)?;
        wtxn.commit().map_err(StoreError::from_heed)?;

        Ok(was_live)
    }

    fn stats(&self) -> Result<StoreStats> {
        let info = self.env.info();
        let rtxn = self.env.read_txn().map_err(StoreError::from_heed)?;
        let stat = self.env.stat();
        let entries = self
            .main
            .stat(&rtxn)
            .map_err(StoreError::from_heed)?
            .entries as u64;

        let page_size = stat.page_size as u64;
        let used_bytes = (info.last_page_number as u64 + 1) * page_size;

        Ok(StoreStats {
            entries,
            map_size: info.map_size as u64,
            used_bytes,
            utilisation: used_bytes as f64 / info.map_size as f64,
            readers_in_use: info.number_of_readers,
            max_readers: info.maximum_number_of_readers,
            epoch: self.epoch(),
        })
    }

    fn sync(&self) -> Result<()> {
        self.env.force_sync().map_err(StoreError::from_heed)
    }
}

fn env_flags(durability: Durability) -> EnvFlags {
    let mut flags = EnvFlags::empty();
    match durability {
        Durability::Durable => {}
        Durability::Relaxed => flags |= EnvFlags::NO_META_SYNC,
        Durability::Ephemeral => {
            flags |= EnvFlags::NO_SYNC;
            // WRITE_MAP would add a further gain, but it fails at env-open on
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `Store` requires `Send + Sync`; this must hold by construction (`Env` and
    /// the database handles are already thread-safe) rather than by an `unsafe
    /// impl` papering over a real aliasing problem.
    #[test]
    fn store_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LmdbStore>();
    }
}
