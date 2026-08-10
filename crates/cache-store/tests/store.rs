//! Storage-engine tests, driven directly against the `Store` trait.
//!
//! These cover behaviour that is invisible from the wire — expiry-index
//! bookkeeping, sweeper reclamation, group commit — and would otherwise only be
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
        self.store()
            .set(&Set {
                key: Key::new(key).unwrap(),
                value,
                ttl_secs,
                mc_flags: 0,
                tags: Vec::new(),
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
fn a_write_without_a_ttl_adds_no_expiry_entry() {
    let h = Harness::new();
    h.set(b"k", b"v", 0);
    assert_eq!(
        h.expiry_entries(),
        0,
        "keys that never expire must not be indexed"
    );
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
fn dropping_a_ttl_removes_the_index_entry() {
    let h = Harness::new();
    h.set(b"k", b"v", 60);
    assert_eq!(h.expiry_entries(), 1);

    h.set(b"k", b"v", 0);
    assert_eq!(
        h.expiry_entries(),
        0,
        "a key that no longer expires must leave the index"
    );
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
        1,
        "only the long key should remain indexed"
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

    assert_eq!(h.expiry_entries(), 0);
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
        })
        .unwrap();
    assert!(next > cas, "CAS must not go backwards across a restart");
    store.close();
}
