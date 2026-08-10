//! Storage-engine tests, driven directly against the `Store` trait.
//!
//! These cover behaviour that is invisible from the wire â€” expiry-index
//! bookkeeping, sweeper reclamation, group commit â€” and would otherwise only be
//! observable as a slow disk leak.

use cache_core::{Key, Set};
use cache_store::{LmdbStore, Store, StoreConfig, WriteConfig};
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Harness {
    store: Option<LmdbStore>,
    _dir: TempDir,
}

impl Harness {
    fn new() -> Self {
        Self::with(|_| {})
    }

    fn with(tweak: impl FnOnce(&mut StoreConfig)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StoreConfig {
            path: dir.path().join("db"),
            map_size: 64 * 1024 * 1024,
            // Buckets of 1ms so tests do not have to wait a whole second for an
            // entry to become due.
            bucket_granularity_ms: 1,
            write: WriteConfig {
                sweep_interval_ms: 10,
                ..WriteConfig::default()
            },
            ..StoreConfig::default()
        };
        tweak(&mut config);

        Self {
            store: Some(LmdbStore::open(&config).unwrap()),
            _dir: dir,
        }
    }

    fn store(&self) -> &LmdbStore {
        self.store.as_ref().unwrap()
    }

    fn set(&self, key: &[u8], value: &[u8], ttl_secs: u32) -> u64 {
        self.set_tagged(key, value, ttl_secs, &[])
    }

    fn set_tagged(&self, key: &[u8], value: &[u8], ttl_secs: u32, tags: &[&[u8]]) -> u64 {
        self.store()
            .set(&Set {
                key: Key::new(key).unwrap(),
                value,
                ttl_secs,
                mc_flags: 0,
                tags: tags.to_vec(),
                mode: cache_core::SetMode::Set,
            })
            .unwrap()
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.store()
            .get(Key::new(key).unwrap())
            .unwrap()
            .map(|v| v.data.to_vec())
    }

    fn expiry_entries(&self) -> u64 {
        self.store().stats().unwrap().expiry_entries
    }

    fn entries(&self) -> u64 {
        self.store().stats().unwrap().entries
    }

    fn tag_index_entries(&self) -> u64 {
        self.store().stats().unwrap().tag_index_entries
    }

    fn pending_reclaims(&self) -> u64 {
        self.store().stats().unwrap().pending_reclaims
    }

    /// Waits for a condition the background sweeper is expected to bring about.
    fn wait_for(&self, what: &str, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if done(self) {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("timed out waiting for {what}");
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(store) = self.store.take() {
            store.close();
        }
    }
}

#[test]
fn a_ttl_write_adds_exactly_one_expiry_entry() {
    let h = Harness::new();
    h.set(b"k", b"v", 60);
    assert_eq!(h.expiry_entries(), 1);
}

#[test]
fn a_write_without_a_ttl_is_indexed_too() {
    // Indexed so the capacity evictor can reach it: a record outside the index
    // can never be chosen as a victim, so a cache of TTL-less keys would fill
    // up with nothing to free.
    let h = Harness::new();
    h.set(b"k", b"v", 0);
    assert_eq!(h.expiry_entries(), 1);
}

#[test]
fn a_record_without_a_ttl_is_never_swept() {
    // Being in the index must not make it expire — it sorts after every real
    // expiry time, so the sweeper stops before reaching it.
    let h = Harness::with(|c| c.write.sweep_interval_ms = 5);

    h.set(b"forever", b"v", 0);
    h.set(b"doomed", b"v", 1);

    h.wait_for("the expiring key to be reclaimed", |h| h.entries() == 1);
    std::thread::sleep(Duration::from_millis(100)); // several more sweeps

    assert_eq!(h.get(b"forever").as_deref(), Some(&b"v"[..]));
    assert_eq!(h.entries(), 1);
}

#[test]
fn overwriting_does_not_accumulate_index_entries() {
    // The failure this guards against is a slow leak: a hot key rewritten on
    // every request would otherwise leave one dead index entry per write,
    // growing the database without bound until the buckets came due.
    let h = Harness::new();
    for _ in 0..200 {
        h.set(b"hot", b"v", 3600);
    }
    assert_eq!(
        h.expiry_entries(),
        1,
        "each overwrite must replace its predecessor"
    );
    assert_eq!(h.entries(), 1);
}

#[test]
fn overwriting_with_a_different_ttl_replaces_the_entry() {
    let h = Harness::new();
    h.set(b"k", b"v", 60);
    h.set(b"k", b"v", 7200);
    h.set(b"k", b"v", 30);
    assert_eq!(h.expiry_entries(), 1);
}

#[test]
fn dropping_a_ttl_moves_the_index_entry_rather_than_duplicating_it() {
    let h = Harness::with(|c| c.write.sweep_interval_ms = 5);
    h.set(b"k", b"v", 1);
    assert_eq!(h.expiry_entries(), 1);

    h.set(b"k", b"v", 0);
    assert_eq!(
        h.expiry_entries(),
        1,
        "the old entry must be replaced, not added to"
    );

    // And the record must outlive the TTL it used to have.
    std::thread::sleep(Duration::from_millis(1_200));
    assert_eq!(h.get(b"k").as_deref(), Some(&b"v"[..]));
}

#[test]
fn deleting_removes_the_index_entry() {
    let h = Harness::new();
    h.set(b"k", b"v", 3600);
    h.store().delete(Key::new(b"k").unwrap()).unwrap();

    assert_eq!(h.expiry_entries(), 0);
    assert_eq!(h.entries(), 0);
}

#[test]
fn the_sweeper_reclaims_expired_records() {
    let h = Harness::with(|c| c.write.sweep_interval_ms = 10);

    h.set(b"doomed", b"v", 1);
    assert_eq!(h.entries(), 1);

    // Invisible to readers immediately at expiry...
    h.wait_for("the key to expire", |h| h.get(b"doomed").is_none());

    // ...and reclaimed from disk shortly after, by the sweeper rather than by
    // any read.
    h.wait_for("the sweeper to reclaim the record", |h| {
        h.entries() == 0 && h.expiry_entries() == 0
    });
}

#[test]
fn the_sweeper_leaves_live_records_alone() {
    let h = Harness::with(|c| c.write.sweep_interval_ms = 10);

    h.set(b"short", b"v", 1);
    h.set(b"long", b"v", 3600);
    h.set(b"forever", b"v", 0);

    h.wait_for("the short key to be reclaimed", |h| h.entries() == 2);

    assert_eq!(h.get(b"long").as_deref(), Some(&b"v"[..]));
    assert_eq!(h.get(b"forever").as_deref(), Some(&b"v"[..]));
    assert_eq!(
        h.expiry_entries(),
        2,
        "both survivors stay indexed — the TTL-less one so it remains evictable"
    );
}

#[test]
fn the_sweeper_catches_up_across_several_passes() {
    // A budget far below the backlog forces multiple passes, which is the
    // normal case after a burst of short-lived writes.
    let h = Harness::with(|c| {
        c.write.sweep_interval_ms = 5;
        c.write.sweep_batch = 8;
    });

    for i in 0..200u32 {
        h.set(format!("k{i}").as_bytes(), b"v", 1);
    }
    assert_eq!(h.entries(), 200);

    h.wait_for("the backlog to drain", |h| h.entries() == 0);
    assert_eq!(h.expiry_entries(), 0);
}

#[test]
fn a_stale_index_entry_never_deletes_a_live_record() {
    // Bucket granularity of a full second means the first write's index entry
    // and the rewrite's land in the same bucket, maximising the chance of the
    // sweeper confusing them.
    let h = Harness::with(|c| {
        c.bucket_granularity_ms = 1000;
        c.write.sweep_interval_ms = 5;
    });

    h.set(b"k", b"first", 1);
    // Replace it with a long-lived value before the first expiry falls due.
    h.set(b"k", b"second", 3600);

    std::thread::sleep(Duration::from_millis(1_200));
    h.wait_for("a sweep to run", |h| h.expiry_entries() <= 1);

    assert_eq!(
        h.get(b"k").as_deref(),
        Some(&b"second"[..]),
        "the CAS check must stop a superseded entry from reclaiming the live record"
    );
}

#[test]
fn touch_extends_a_lifetime_without_the_value() {
    let h = Harness::new();
    h.set(b"k", b"payload", 1);

    assert!(h.store().touch(Key::new(b"k").unwrap(), 3600).unwrap());

    std::thread::sleep(Duration::from_millis(1_200));
    assert_eq!(
        h.get(b"k").as_deref(),
        Some(&b"payload"[..]),
        "the value must survive the touch intact"
    );
    assert_eq!(
        h.expiry_entries(),
        1,
        "touch must not leave a second index entry"
    );
}

#[test]
fn touch_can_clear_a_ttl() {
    let h = Harness::new();
    h.set(b"k", b"v", 1);
    assert!(h.store().touch(Key::new(b"k").unwrap(), 0).unwrap());

    // Still one entry — it moved to the never-expires bucket rather than
    // leaving the index, so the record stays evictable.
    assert_eq!(h.expiry_entries(), 1);
    std::thread::sleep(Duration::from_millis(1_200));
    assert_eq!(h.get(b"k").as_deref(), Some(&b"v"[..]));
}

#[test]
fn touch_misses_on_absent_and_expired_keys() {
    let h = Harness::with(|c| c.write.sweep_interval_ms = 10_000);

    assert!(!h.store().touch(Key::new(b"absent").unwrap(), 60).unwrap());

    h.set(b"expired", b"v", 1);
    std::thread::sleep(Duration::from_millis(1_200));
    assert!(
        !h.store().touch(Key::new(b"expired").unwrap(), 60).unwrap(),
        "an expired record must not be resurrectable"
    );
}

#[test]
fn batch_writes_apply_atomically_and_return_ordered_cas() {
    let h = Harness::new();
    let sets: Vec<Set<'_>> = (0..64u32)
        .map(|i| Set {
            key: Key::from_stored(match i % 4 {
                0 => b"a",
                1 => b"b",
                2 => b"c",
                _ => b"d",
            }),
            value: b"v",
            ttl_secs: 0,
            mc_flags: 0,
            tags: Vec::new(),
            mode: cache_core::SetMode::Set,
        })
        .collect();

    let cas = h.store().set_many(&sets).unwrap();
    assert_eq!(cas.len(), sets.len());
    assert!(
        cas.windows(2).all(|w| w[1] > w[0]),
        "CAS must increase in application order within a batch"
    );
    assert_eq!(h.entries(), 4);
}

#[test]
fn batch_reads_see_one_consistent_snapshot() {
    let h = Harness::new();
    h.set(b"a", b"1", 0);
    h.set(b"c", b"3", 0);

    let keys = [
        Key::new(b"a").unwrap(),
        Key::new(b"b").unwrap(),
        Key::new(b"c").unwrap(),
    ];
    let values = h.store().get_many(&keys).unwrap();

    assert_eq!(values.len(), 3);
    assert_eq!(
        values[0].as_ref().map(|v| v.data.to_vec()),
        Some(b"1".to_vec())
    );
    assert!(
        values[1].is_none(),
        "misses must hold their slot in the result"
    );
    assert_eq!(
        values[2].as_ref().map(|v| v.data.to_vec()),
        Some(b"3".to_vec())
    );
}

#[test]
fn batch_delete_reports_each_key_separately() {
    let h = Harness::new();
    h.set(b"live", b"v", 0);

    let keys = [Key::new(b"live").unwrap(), Key::new(b"absent").unwrap()];
    assert_eq!(h.store().delete_many(&keys).unwrap(), vec![true, false]);
    assert_eq!(h.entries(), 0);
}

#[test]
fn concurrent_writers_share_commits_and_never_reuse_a_cas() {
    use std::collections::HashSet;
    use std::sync::Arc;

    let h = Harness::new();
    let store: Arc<&LmdbStore> = Arc::new(h.store());

    // Group commit is the point of the writer thread: many threads submitting
    // at once must all land, with unique CAS tokens, and no lost updates.
    let cas_tokens: Vec<u64> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8u32)
            .map(|worker| {
                let store = Arc::clone(&store);
                scope.spawn(move || {
                    (0..100u32)
                        .map(|i| {
                            let key = format!("w{worker}-{i}");
                            store
                                .set(&Set {
                                    key: Key::new(key.as_bytes()).unwrap(),
                                    value: b"v",
                                    ttl_secs: 0,
                                    mc_flags: 0,
                                    tags: Vec::new(),
                                    mode: cache_core::SetMode::Set,
                                })
                                .unwrap()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    });

    assert_eq!(cas_tokens.len(), 800);
    assert_eq!(
        cas_tokens.iter().collect::<HashSet<_>>().len(),
        800,
        "every CAS token must be unique"
    );
    assert_eq!(h.entries(), 800, "no write may be lost to batching");
}

// ---- tags ------------------------------------------------------------------

#[test]
fn invalidating_a_tag_hides_every_record_carrying_it() {
    let h = Harness::new();

    h.set_tagged(b"a", b"1", 0, &[b"news"]);
    h.set_tagged(b"b", b"2", 0, &[b"news", b"sport"]);
    h.set_tagged(b"c", b"3", 0, &[b"sport"]);
    h.set(b"untagged", b"4", 0);

    assert!(h.store().delete_by_tag(b"news").unwrap());

    assert!(h.get(b"a").is_none(), "single-tagged record must go");
    assert!(
        h.get(b"b").is_none(),
        "one dead tag is enough to kill a record"
    );
    assert_eq!(
        h.get(b"c").as_deref(),
        Some(&b"3"[..]),
        "other tags unaffected"
    );
    assert_eq!(h.get(b"untagged").as_deref(), Some(&b"4"[..]));
}

#[test]
fn invalidation_takes_effect_before_any_space_is_reclaimed() {
    // The whole point of generation counters: the data stops being served
    // immediately, and freeing it is a separate background concern.
    let h = Harness::with(|c| {
        // Long enough that no reclamation can run during the assertion.
        c.write.sweep_interval_ms = 60_000;
        c.write.reclaim_batch = 1;
    });

    for i in 0..100u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"v", 0, &[b"batch"]);
    }
    h.store().delete_by_tag(b"batch").unwrap();

    assert!(h.get(b"k0").is_none());
    assert!(h.get(b"k99").is_none());
    assert!(
        h.entries() > 0,
        "records should still be on disk; invalidation must not have deleted them synchronously"
    );
}

#[test]
fn rewriting_after_an_invalidation_brings_a_key_back() {
    let h = Harness::new();

    h.set_tagged(b"k", b"old", 0, &[b"t"]);
    h.store().delete_by_tag(b"t").unwrap();
    assert!(h.get(b"k").is_none());

    // The rewrite captures the bumped generation, so it is live again.
    h.set_tagged(b"k", b"new", 0, &[b"t"]);
    assert_eq!(h.get(b"k").as_deref(), Some(&b"new"[..]));
}

#[test]
fn repeated_invalidations_keep_advancing() {
    let h = Harness::new();

    for round in 0..5u32 {
        let value = format!("v{round}");
        h.set_tagged(b"k", value.as_bytes(), 0, &[b"t"]);
        assert_eq!(h.get(b"k").as_deref(), Some(value.as_bytes()));

        h.store().delete_by_tag(b"t").unwrap();
        assert!(
            h.get(b"k").is_none(),
            "round {round} should have invalidated"
        );
    }
}

#[test]
fn invalidating_an_unknown_tag_is_a_miss_not_an_error() {
    let h = Harness::new();
    assert!(!h.store().delete_by_tag(b"never-used").unwrap());
}

#[test]
fn reclamation_sharing_a_transaction_with_its_own_invalidation_still_frees_everything() {
    // A reclamation pass can land in the same commit as the DELETE_BY_TAG that
    // queued it. The in-memory tag generation is only published after that
    // commit, so a pass that judged deadness from RAM would see every record as
    // live, advance its cursor past them, and leak them for good. Judging
    // against the job's own target generation is what makes it correct.
    //
    // A zero sweep interval makes maintenance run on every batch, so the
    // overlap happens every time rather than by timing luck.
    let h = Harness::with(|c| {
        c.write.sweep_interval_ms = 1;
        c.write.reclaim_batch = 8;
    });

    for i in 0..64u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"v", 0, &[b"t"]);
    }
    h.store().delete_by_tag(b"t").unwrap();

    // Waiting on the job rather than on the record count: the last records go
    // in one pass, and the pass after that is what finds the range empty and
    // retires the job. Checking `entries` first would race that gap.
    h.wait_for("the reclamation job to finish", |h| {
        h.pending_reclaims() == 0
    });
    assert_eq!(h.entries(), 0);
    assert_eq!(h.tag_index_entries(), 0);
}

#[test]
fn the_reclaimer_frees_invalidated_records() {
    let h = Harness::with(|c| {
        c.write.sweep_interval_ms = 10;
        c.write.reclaim_batch = 16;
    });

    for i in 0..200u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"v", 0, &[b"bulk"]);
    }
    h.set(b"survivor", b"v", 0);

    h.store().delete_by_tag(b"bulk").unwrap();

    // Only the reclaimer can remove these: nothing reads them, and they have
    // no TTL for the sweeper to act on.
    h.wait_for("the reclaimer to free the invalidated records", |h| {
        h.entries() == 1
    });
    assert_eq!(h.get(b"survivor").as_deref(), Some(&b"v"[..]));
    assert_eq!(h.tag_index_entries(), 0, "index entries must go too");
    assert_eq!(
        h.pending_reclaims(),
        0,
        "the job must complete and be removed"
    );
}

#[test]
fn the_reclaimer_keeps_records_rewritten_after_the_invalidation() {
    let h = Harness::with(|c| {
        c.write.sweep_interval_ms = 10;
        c.write.reclaim_batch = 4;
    });

    for i in 0..50u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"old", 0, &[b"t"]);
    }
    h.store().delete_by_tag(b"t").unwrap();

    // Rewritten immediately, so these are live again and must survive a
    // reclamation pass that is already walking the same index entries.
    for i in 0..10u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"new", 0, &[b"t"]);
    }

    h.wait_for("reclamation to finish", |h| h.pending_reclaims() == 0);

    for i in 0..10u32 {
        assert_eq!(
            h.get(format!("k{i}").as_bytes()).as_deref(),
            Some(&b"new"[..]),
            "k{i} was rewritten after the invalidation and must not be reclaimed"
        );
    }
    assert_eq!(h.entries(), 10);
}

#[test]
fn reclamation_resumes_across_a_restart() {
    // The M2 exit criterion: a job interrupted mid-scan must continue rather
    // than restart, and must not lose or over-delete anything.
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 64 * 1024 * 1024,
        write: WriteConfig {
            // Effectively frozen: reclamation only advances when we say so.
            sweep_interval_ms: 60_000,
            reclaim_batch: 8,
            ..WriteConfig::default()
        },
        ..StoreConfig::default()
    };

    {
        let store = LmdbStore::open(&config).unwrap();
        for i in 0..100u32 {
            let key = format!("k{i}");
            store
                .set(&Set {
                    key: Key::new(key.as_bytes()).unwrap(),
                    value: b"v",
                    ttl_secs: 0,
                    mc_flags: 0,
                    tags: vec![b"t"],
                    mode: cache_core::SetMode::Set,
                })
                .unwrap();
        }
        store.delete_by_tag(b"t").unwrap();

        // Let a few bounded passes run, then stop mid-job.
        let deadline = Instant::now() + Duration::from_secs(5);
        while store.stats().unwrap().entries > 50 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        let stats = store.stats().unwrap();
        assert!(
            stats.entries > 0,
            "test needs an unfinished job to be meaningful"
        );
        assert_eq!(stats.pending_reclaims, 1, "a job must still be outstanding");
        store.close();
    }

    // Reopen: the persisted job and cursor must carry on.
    let store = LmdbStore::open(&config).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while store.stats().unwrap().entries > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    let stats = store.stats().unwrap();
    assert_eq!(
        stats.entries, 0,
        "reclamation must finish after the restart"
    );
    assert_eq!(stats.pending_reclaims, 0);
    assert_eq!(stats.tag_index_entries, 0);
    store.close();
}

#[test]
fn tag_generations_survive_a_restart() {
    // If a bumped generation were lost, every invalidated record would come
    // back to life on restart.
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 64 * 1024 * 1024,
        write: WriteConfig {
            sweep_interval_ms: 60_000,
            ..WriteConfig::default()
        },
        ..StoreConfig::default()
    };

    {
        let store = LmdbStore::open(&config).unwrap();
        store
            .set(&Set {
                key: Key::new(b"k").unwrap(),
                value: b"v",
                ttl_secs: 0,
                mc_flags: 0,
                tags: vec![b"t"],
                mode: cache_core::SetMode::Set,
            })
            .unwrap();
        store.delete_by_tag(b"t").unwrap();
        store.close();
    }

    let store = LmdbStore::open(&config).unwrap();
    assert!(
        store.get(Key::new(b"k").unwrap()).unwrap().is_none(),
        "an invalidated record must not be resurrected by a restart"
    );
    store.close();
}

#[test]
fn tag_ids_are_stable_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 64 * 1024 * 1024,
        write: WriteConfig {
            sweep_interval_ms: 60_000,
            ..WriteConfig::default()
        },
        ..StoreConfig::default()
    };

    {
        let store = LmdbStore::open(&config).unwrap();
        for (i, tag) in [b"a".as_slice(), b"b", b"c"].iter().enumerate() {
            store
                .set(&Set {
                    key: Key::new(format!("k{i}").as_bytes()).unwrap(),
                    value: b"v",
                    ttl_secs: 0,
                    mc_flags: 0,
                    tags: vec![tag],
                    mode: cache_core::SetMode::Set,
                })
                .unwrap();
        }
        store.close();
    }

    let store = LmdbStore::open(&config).unwrap();
    // Invalidating "b" must hit only k1. If ids were reassigned on load, this
    // would kill the wrong record.
    store.delete_by_tag(b"b").unwrap();

    assert!(store.get(Key::new(b"k0").unwrap()).unwrap().is_some());
    assert!(store.get(Key::new(b"k1").unwrap()).unwrap().is_none());
    assert!(store.get(Key::new(b"k2").unwrap()).unwrap().is_some());
    store.close();
}

#[test]
fn overwriting_does_not_accumulate_tag_index_entries() {
    let h = Harness::new();
    for _ in 0..100 {
        h.set_tagged(b"hot", b"v", 0, &[b"t"]);
    }
    assert_eq!(h.tag_index_entries(), 1);
}

#[test]
fn changing_a_records_tags_drops_the_old_index_entry() {
    let h = Harness::new();
    h.set_tagged(b"k", b"v", 0, &[b"old"]);
    assert_eq!(h.tag_index_entries(), 1);

    h.set_tagged(b"k", b"v", 0, &[b"new"]);
    assert_eq!(h.tag_index_entries(), 1);

    // The record no longer carries "old", so invalidating it must not matter.
    h.store().delete_by_tag(b"old").unwrap();
    assert_eq!(h.get(b"k").as_deref(), Some(&b"v"[..]));
}

#[test]
fn deleting_a_record_drops_its_tag_index_entries() {
    let h = Harness::new();
    h.set_tagged(b"k", b"v", 0, &[b"a", b"b"]);
    assert_eq!(h.tag_index_entries(), 2);

    h.store().delete(Key::new(b"k").unwrap()).unwrap();
    assert_eq!(h.tag_index_entries(), 0);
}

#[test]
fn the_tag_registry_is_bounded() {
    let h = Harness::with(|c| c.max_tags = 4);

    for i in 0..4u32 {
        h.set_tagged(b"k", b"v", 0, &[format!("t{i}").as_bytes()]);
    }

    let err = h.store().set(&Set {
        key: Key::new(b"k").unwrap(),
        value: b"v",
        ttl_secs: 0,
        mc_flags: 0,
        tags: vec![b"one-too-many"],
        mode: cache_core::SetMode::Set,
    });
    assert!(
        matches!(err, Err(cache_store::StoreError::TagLimit(4))),
        "an unbounded RAM registry is a leak a client could drive: {err:?}"
    );
}

#[test]
fn flush_empties_everything_and_bumps_the_epoch() {
    let h = Harness::new();

    h.set(b"plain", b"v", 0);
    h.set_tagged(b"tagged", b"v", 0, &[b"t"]);
    h.set(b"expiring", b"v", 3600);

    let before = h.store().stats().unwrap().epoch;
    let epoch = h.store().flush().unwrap();
    assert_eq!(epoch, before + 1);

    assert!(h.get(b"plain").is_none());
    assert!(h.get(b"tagged").is_none());
    assert!(h.get(b"expiring").is_none());

    let stats = h.store().stats().unwrap();
    assert_eq!(
        stats.entries, 0,
        "an epoch bump alone would leak non-expiring records"
    );
    assert_eq!(stats.expiry_entries, 0);
    assert_eq!(stats.tag_index_entries, 0);
    assert_eq!(stats.epoch, epoch);
}

#[test]
fn writes_after_a_flush_survive_it() {
    let h = Harness::new();
    h.set(b"before", b"v", 0);
    h.store().flush().unwrap();

    h.set(b"after", b"v", 0);
    assert_eq!(h.get(b"after").as_deref(), Some(&b"v"[..]));
    assert!(h.get(b"before").is_none());
}

#[test]
fn tags_still_work_after_a_flush() {
    let h = Harness::new();
    h.set_tagged(b"k", b"v", 0, &[b"t"]);
    h.store().flush().unwrap();

    // Registrations survive a flush; only the data goes.
    h.set_tagged(b"k", b"v", 0, &[b"t"]);
    assert!(h.store().delete_by_tag(b"t").unwrap());
    assert!(h.get(b"k").is_none());
}

// ---- sharding --------------------------------------------------------------

#[test]
fn a_sharded_store_behaves_like_a_single_one() {
    let h = Harness::with(|c| c.shards = 4);

    for i in 0..500u32 {
        h.set(format!("k{i}").as_bytes(), format!("v{i}").as_bytes(), 0);
    }
    for i in 0..500u32 {
        assert_eq!(
            h.get(format!("k{i}").as_bytes()).as_deref(),
            Some(format!("v{i}").as_bytes()),
            "k{i} must be readable from whichever shard owns it"
        );
    }
    assert_eq!(h.entries(), 500, "every key lands in exactly one shard");
}

#[test]
fn batches_are_reassembled_into_request_order_across_shards() {
    // The failure this guards against is subtle and silent: results coming back
    // grouped by shard rather than in the order the client asked.
    let h = Harness::with(|c| c.shards = 8);

    let keys: Vec<String> = (0..64).map(|i| format!("key{i}")).collect();
    let sets: Vec<Set<'_>> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| Set {
            key: Key::new(key.as_bytes()).unwrap(),
            value: if i % 3 == 0 { b"a" } else { b"b" },
            ttl_secs: 0,
            mc_flags: 0,
            tags: Vec::new(),
            mode: cache_core::SetMode::Set,
        })
        .collect();
    h.store().set_many(&sets).unwrap();

    // Delete every third key so hits and misses interleave.
    let lookups: Vec<Key<'_>> = keys
        .iter()
        .map(|k| Key::new(k.as_bytes()).unwrap())
        .collect();
    let doomed: Vec<Key<'_>> = lookups.iter().step_by(2).copied().collect();
    let deleted = h.store().delete_many(&doomed).unwrap();
    assert!(deleted.iter().all(|hit| *hit));

    let values = h.store().get_many(&lookups).unwrap();
    assert_eq!(values.len(), lookups.len());
    for (i, value) in values.iter().enumerate() {
        if i % 2 == 0 {
            assert!(value.is_none(), "key{i} was deleted");
        } else {
            let expected: &[u8] = if i % 3 == 0 { b"a" } else { b"b" };
            assert_eq!(
                value.as_ref().map(|v| v.data.to_vec()).as_deref(),
                Some(expected),
                "key{i} came back in the wrong slot"
            );
        }
    }
}

#[test]
fn cas_tokens_are_unique_across_shards() {
    use std::collections::HashSet;

    // Each shard counts independently, so the raw counters collide; the tokens
    // are striped by shard to stay unique server-wide.
    let h = Harness::with(|c| c.shards = 8);
    let tokens: HashSet<u64> = (0..400u32)
        .map(|i| h.set(format!("k{i}").as_bytes(), b"v", 0))
        .collect();

    assert_eq!(
        tokens.len(),
        400,
        "cas tokens must not repeat between shards"
    );
}

#[test]
fn tags_invalidate_across_every_shard() {
    // A tag's keys are spread by key hash, so an invalidation that reached only
    // one shard would leave most of them being served.
    let h = Harness::with(|c| c.shards = 4);

    for i in 0..200u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"v", 0, &[b"everything"]);
    }
    assert!(h.store().delete_by_tag(b"everything").unwrap());

    for i in 0..200u32 {
        assert!(
            h.get(format!("k{i}").as_bytes()).is_none(),
            "k{i} survived the invalidation"
        );
    }
}

#[test]
fn reopening_with_a_different_shard_count_is_refused() {
    // Silently accepting it would route every key elsewhere: the data would
    // still occupy disk while every read missed.
    let dir = tempfile::tempdir().unwrap();
    let mut config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 32 * 1024 * 1024,
        shards: 4,
        ..StoreConfig::default()
    };

    let store = LmdbStore::open(&config).unwrap();
    store
        .set(&Set {
            key: Key::new(b"k").unwrap(),
            value: b"v",
            ttl_secs: 0,
            mc_flags: 0,
            tags: Vec::new(),
            mode: cache_core::SetMode::Set,
        })
        .unwrap();
    store.close();

    config.shards = 8;
    let Err(err) = LmdbStore::open(&config) else {
        panic!("must refuse a changed shard count");
    };
    assert!(
        err.to_string().contains("shard"),
        "the error should say why: {err}"
    );
}

// ---- capacity --------------------------------------------------------------

#[test]
fn a_sustained_overfill_evicts_instead_of_failing() {
    // The M4 exit criterion. A map far smaller than the data written, filled
    // continuously: the store must shed old records and keep serving rather
    // than wedge or lose its mind.
    let h = Harness::with(|c| {
        c.map_size = cache_store::config::MIN_MAP_SIZE;
        c.write.sweep_interval_ms = 1;
        c.write.eviction.batch = 64;
    });

    // Several times the map, written continuously.
    let value = vec![b'x'; 16 * 1024];
    let total = 4_000u32;
    let mut accepted = 0usize;
    let mut refused = 0usize;
    let mut accepted_late = 0usize;

    for i in 0..total {
        let key = format!("overfill{i}");
        let result = h.store().set(&Set {
            key: Key::new(key.as_bytes()).unwrap(),
            value: &value,
            ttl_secs: 0,
            mc_flags: 0,
            tags: Vec::new(),
            mode: cache_core::SetMode::Set,
        });
        match result {
            Ok(_) => {
                accepted += 1;
                if i >= total * 3 / 4 {
                    accepted_late += 1;
                }
            }
            // Refusing under critical pressure is a legitimate outcome; a panic
            // or a corrupt store is not.
            Err(cache_store::StoreError::CapacityFull) => refused += 1,
            Err(e) => panic!("write {i} failed unexpectedly: {e}"),
        }
    }

    // The point is not how many writes got through — that depends on how fast
    // eviction keeps up — but that the store was *still accepting them at the
    // end*. A cache that filled up and stopped would score zero here.
    assert!(
        accepted_late > 0,
        "no writes were accepted in the last quarter: the store filled up and stayed full \
         ({accepted} accepted, {refused} refused overall)"
    );
    assert!(
        h.store().stats().unwrap().evicted > 0,
        "nothing was evicted, so the map must have been big enough after all"
    );

    // Still usable afterwards: reads work, and writing again succeeds once
    // eviction has caught up.
    h.wait_for("pressure to fall back", |h| {
        h.store()
            .set(&Set {
                key: Key::new(b"after-the-storm").unwrap(),
                value: b"v",
                ttl_secs: 0,
                mc_flags: 0,
                tags: Vec::new(),
                mode: cache_core::SetMode::Set,
            })
            .is_ok()
    });
    assert_eq!(h.get(b"after-the-storm").as_deref(), Some(&b"v"[..]));

    println!("overfill: {accepted} accepted, {refused} refused under pressure");
}

#[test]
fn eviction_takes_the_soonest_to_expire_first() {
    let h = Harness::with(|c| {
        c.map_size = cache_store::config::MIN_MAP_SIZE;
        c.write.sweep_interval_ms = 1;
        c.write.eviction.batch = 32;
    });

    // A long-lived key written first; it must outlast the short-lived flood
    // even though it is older.
    h.set(b"long-lived", b"keep-me", 86_400);

    // Comfortably more than the map holds, so eviction has to run.
    let value = vec![b'x'; 16 * 1024];
    for i in 0..1_600u32 {
        let key = format!("churn{i}");
        let _ = h.store().set(&Set {
            key: Key::new(key.as_bytes()).unwrap(),
            value: &value,
            // Sooner than the long-lived key, so these are taken first.
            ttl_secs: 600,
            mc_flags: 0,
            tags: Vec::new(),
            mode: cache_core::SetMode::Set,
        });
    }

    assert!(
        h.store().stats().unwrap().evicted > 0,
        "the test needs eviction to have happened"
    );
    assert_eq!(
        h.get(b"long-lived").as_deref(),
        Some(&b"keep-me"[..]),
        "eviction should have taken the sooner-expiring records first"
    );
}

#[test]
fn data_and_the_expiry_index_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 64 * 1024 * 1024,
        // Long enough that nothing is reclaimed while the test runs.
        write: WriteConfig {
            sweep_interval_ms: 60_000,
            ..WriteConfig::default()
        },
        ..StoreConfig::default()
    };

    let cas = {
        let store = LmdbStore::open(&config).unwrap();
        let cas = store
            .set(&Set {
                key: Key::new(b"k").unwrap(),
                value: b"v",
                ttl_secs: 3600,
                mc_flags: 0,
                tags: Vec::new(),
                mode: cache_core::SetMode::Set,
            })
            .unwrap();
        store.close();
        cas
    };

    let store = LmdbStore::open(&config).unwrap();
    let stats = store.stats().unwrap();
    assert_eq!(stats.entries, 1);
    assert_eq!(
        stats.expiry_entries, 1,
        "the index must be durable, not rebuilt"
    );

    let next = store
        .set(&Set {
            key: Key::new(b"other").unwrap(),
            value: b"v",
            ttl_secs: 0,
            mc_flags: 0,
            tags: Vec::new(),
            mode: cache_core::SetMode::Set,
        })
        .unwrap();
    assert!(next > cas, "CAS must not go backwards across a restart");
    store.close();
}
