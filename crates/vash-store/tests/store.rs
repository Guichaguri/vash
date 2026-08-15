//! Storage-engine tests, driven directly against the `Store` trait.
//!
//! These cover behaviour that is invisible from the wire â€” expiry-index
//! bookkeeping, sweeper reclamation, group commit â€” and would otherwise only be
//! observable as a slow disk leak.

use std::time::{Duration, Instant};
use tempfile::TempDir;
use vash_core::{Key, Set};
use vash_store::{LmdbStore, Store, StoreConfig, WriteConfig};

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
                ttl: vash_core::TtlChange::Set(ttl_secs),
                return_previous: false,
                mc_flags: 0,
                tags: tags.to_vec(),
                mode: vash_core::SetMode::Set,
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

    fn registered_tags(&self) -> u64 {
        self.store().stats().unwrap().tags
    }

    /// A tagged write that is allowed to fail, for the limit tests.
    fn try_set_tagged(&self, key: &[u8], tags: &[&[u8]]) -> vash_store::Result<u64> {
        self.store().set(&Set {
            key: Key::new(key).unwrap(),
            value: b"v",
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: tags.to_vec(),
            mode: vash_core::SetMode::Set,
        })
    }

    fn pending_reclaims(&self) -> u64 {
        self.store().stats().unwrap().pending_reclaims
    }

    /// Records the expiry sweeper has actually reclaimed, cumulative.
    ///
    /// Unlike [`Harness::entries`] this only ever rises, which is what makes it
    /// safe to assert against while the sweeper is running.
    fn reclaimed(&self) -> u64 {
        self.store().stats().unwrap().reclaimed
    }

    /// One atomic counter step, for the arithmetic tests below.
    fn arithmetic(&self, op: &vash_core::Arithmetic<'_>) -> Option<vash_core::Applied> {
        self.store().arithmetic(op).unwrap()
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

/// **Distinct keys sharing a bucket are distinct entries.** The expiry index is
/// keyed by the deadline bucket and a hash of the user key, so every record
/// expiring in the same second lands next to the others — if that second half
/// did not separate them they would overwrite one another's rows and all but
/// one record would be left unreachable by the sweeper.
#[test]
fn many_keys_in_one_bucket_are_all_indexed_and_all_swept() {
    let h = Harness::with(|c| c.write.sweep_interval_ms = 5);

    // The same TTL, so the same bucket, for every one of them.
    for i in 0..64u32 {
        h.set(format!("shared-{i:03}").as_bytes(), b"v", 1);
    }
    assert_eq!(
        h.expiry_entries(),
        64,
        "every key needs its own row, however crowded the bucket"
    );

    h.wait_for("the whole bucket to be reclaimed", |h| h.entries() == 0);
    assert_eq!(h.expiry_entries(), 0);
}

/// Extending a deadline must move the record out of reach of the old one. The
/// entry is keyed by the bucket, so the sweeper reaching the old bucket has to
/// find nothing that still points at this record.
#[test]
fn extending_a_ttl_saves_the_record_from_its_old_deadline() {
    let h = Harness::with(|c| c.write.sweep_interval_ms = 5);

    h.set(b"reprieved", b"v", 1);
    h.set(b"reprieved", b"v", 3600);
    assert_eq!(h.expiry_entries(), 1);

    // Well past the deadline it was first given.
    std::thread::sleep(Duration::from_millis(1_500));
    assert_eq!(
        h.get(b"reprieved").as_deref(),
        Some(&b"v"[..]),
        "the old deadline must not reach a record that has moved past it"
    );
    assert_eq!(h.expiry_entries(), 1);
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

    h.wait_for("the backlog to drain", |h| h.entries() == 0);
    assert_eq!(h.expiry_entries(), 0);

    // **This is the assertion the test is named for**, and it replaces a
    // `assert_eq!(h.entries(), 200)` taken before the wait.
    //
    // That earlier check was a race against the very thing under test. The
    // sweeper runs throughout — every 5ms — and the TTL is one second, so on a
    // machine slow enough for 200 writes to outlast a second, the earliest keys
    // fall due and are reclaimed before the last one is written. Observed under
    // load: 186 of 200. Widening the TTL would only move the threshold.
    //
    // Counting the work the sweeper actually did is immune to that, and says
    // more: 200 records reclaimed against a budget of 8 per pass cannot have
    // happened in fewer than 25 passes, which is precisely the catching-up this
    // test exists to prove. The counter only rises, so nothing can move it back
    // under the assertion.
    assert!(
        h.reclaimed() >= 200,
        "the sweeper reclaimed {} records; all 200 had to go through it, and at \
         a budget of 8 a pass that is at least 25 passes",
        h.reclaimed()
    );
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
fn delete_misses_on_an_expired_record_the_sweeper_has_not_reached() {
    // The row is still on disk, so the delete frees real space — but a client
    // could no longer read the key, so removing it is a miss and not a hit. The
    // sweep interval is pushed out to keep the sweeper from reaching it first,
    // which would make this pass for the wrong reason.
    let h = Harness::with(|c| c.write.sweep_interval_ms = 10_000);

    h.set(b"expired", b"v", 1);
    assert_eq!(h.expiry_entries(), 1, "still indexed, so still on disk");
    std::thread::sleep(Duration::from_millis(1_200));

    assert!(
        !h.store().delete(Key::new(b"expired").unwrap()).unwrap(),
        "an expired record is already invisible, so deleting it is a miss"
    );
    assert!(
        !h.store().delete(Key::new(b"absent").unwrap()).unwrap(),
        "and a key that was never written is the same miss"
    );

    let live = h.set(b"live", b"v", 60);
    assert!(live > 0);
    assert!(
        h.store().delete(Key::new(b"live").unwrap()).unwrap(),
        "a record a client could still read is a hit"
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
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
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
                                    ttl: vash_core::TtlChange::Set(0),
                                    return_previous: false,
                                    mc_flags: 0,
                                    tags: Vec::new(),
                                    mode: vash_core::SetMode::Set,
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

    assert!(h.store().delete_by_tag(b"news").unwrap().is_some());

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
    assert!(h.store().delete_by_tag(b"never-used").unwrap().is_none());
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
                    ttl: vash_core::TtlChange::Set(0),
                    return_previous: false,
                    mc_flags: 0,
                    tags: vec![b"t"],
                    mode: vash_core::SetMode::Set,
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
                ttl: vash_core::TtlChange::Set(0),
                return_previous: false,
                mc_flags: 0,
                tags: vec![b"t"],
                mode: vash_core::SetMode::Set,
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
                    ttl: vash_core::TtlChange::Set(0),
                    return_previous: false,
                    mc_flags: 0,
                    tags: vec![tag],
                    mode: vash_core::SetMode::Set,
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
        ttl: vash_core::TtlChange::Set(0),
        return_previous: false,
        mc_flags: 0,
        tags: vec![b"one-too-many"],
        mode: vash_core::SetMode::Set,
    });
    assert!(
        matches!(err, Err(vash_store::StoreError::TagLimit(4))),
        "an unbounded RAM registry is a leak a client could drive: {err:?}"
    );
}

#[test]
fn deadlines_agree_with_get_without_reading_the_value() {
    let h = Harness::new();

    h.set(b"forever", b"v", 0);
    h.set(b"expiring", b"v", 600);
    h.set_tagged(b"tagged", b"v", 0, &[b"t"]);

    let keys = [
        Key::new(b"forever").unwrap(),
        Key::new(b"expiring").unwrap(),
        Key::new(b"absent").unwrap(),
    ];
    let deadlines = h.store().deadlines(&keys).unwrap();

    assert_eq!(deadlines[0], Some(vash_core::NEVER), "no expiry");
    assert!(
        deadlines[1].is_some_and(|at| at > vash_core::NEVER),
        "a real deadline, got {:?}",
        deadlines[1]
    );
    assert_eq!(deadlines[2], None, "absent");

    // Whatever hides a key from `get` must hide it here too, or `EXISTS` and
    // `TTL` would answer for records `GET` refuses to serve.
    h.store().delete_by_tag(b"t").unwrap();
    assert_eq!(h.get(b"tagged"), None);
    assert_eq!(
        h.store().deadline(Key::new(b"tagged").unwrap()).unwrap(),
        None,
        "a tag-invalidated record is absent to both"
    );

    h.store().flush().unwrap();
    assert_eq!(
        h.store().deadline(Key::new(b"forever").unwrap()).unwrap(),
        None,
        "a flushed record is absent to both"
    );
}

#[test]
fn tags_per_record_are_bounded_by_the_default() {
    let h = Harness::new();
    let names: Vec<String> = (0..=vash_core::DEFAULT_MAX_TAGS)
        .map(|i| format!("t{i}"))
        .collect();
    let refs: Vec<&[u8]> = names.iter().map(|n| n.as_bytes()).collect();

    h.try_set_tagged(b"at-the-limit", &refs[..vash_core::DEFAULT_MAX_TAGS])
        .expect("the limit itself must be allowed");

    let err = h.try_set_tagged(b"over", &refs);
    assert!(
        matches!(
            err,
            Err(vash_store::StoreError::Core(
                vash_core::CoreError::TooManyTags { max: 32, .. }
            ))
        ),
        "32 is the default per-record tag limit: {err:?}"
    );
}

#[test]
fn the_per_record_tag_limit_is_configurable() {
    let h = Harness::with(|c| c.max_tags_per_record = 2);

    h.try_set_tagged(b"k", &[b"a", b"b"]).expect("within two");

    let err = h.try_set_tagged(b"k", &[b"a", b"b", b"c"]);
    assert!(
        matches!(
            err,
            Err(vash_store::StoreError::Core(
                vash_core::CoreError::TooManyTags { count: 3, max: 2 }
            ))
        ),
        "the configured limit must be the one enforced: {err:?}"
    );

    // The refused write must not have left its names behind: the registry is
    // RAM-resident and capped, so a rejected write that still registered would
    // be a leak a client could drive on purpose.
    assert_eq!(
        h.registered_tags(),
        2,
        "only the names of writes that were accepted may be registered"
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
    assert!(h.store().delete_by_tag(b"t").unwrap().is_some());
    assert!(h.get(b"k").is_none());
}

// ---- cluster merge ---------------------------------------------------------

#[test]
fn merging_a_higher_generation_invalidates_like_a_local_delete() {
    // The receiving half of cluster invalidation. A peer's message is a name
    // and a number; applying it must hide exactly what a local
    // `delete_by_tag` would have.
    let h = Harness::new();
    h.set_tagged(b"a", b"1", 0, &[b"news"]);
    h.set_tagged(b"b", b"2", 0, &[b"sport"]);

    let generation = h.store().merge_tag_generation(b"news", 1).unwrap();
    assert_eq!(generation, 1);

    assert!(h.get(b"a").is_none());
    assert_eq!(h.get(b"b").as_deref(), Some(&b"2"[..]));
}

#[test]
fn merging_is_idempotent_and_never_moves_backwards() {
    // The CRDT property the whole cluster design rests on: at-least-once
    // delivery in any order has to be enough, so a replay must change nothing
    // and a stale message must not undo a newer invalidation.
    let h = Harness::new();
    h.set_tagged(b"k", b"v", 0, &[b"t"]);
    h.store().delete_by_tag(b"t").unwrap();

    h.set_tagged(b"k", b"fresh", 0, &[b"t"]);
    assert_eq!(h.get(b"k").as_deref(), Some(&b"fresh"[..]));

    // A duplicate of the invalidation that already happened.
    assert_eq!(h.store().merge_tag_generation(b"t", 1).unwrap(), 1);
    assert_eq!(
        h.get(b"k").as_deref(),
        Some(&b"fresh"[..]),
        "a replayed invalidation must not kill a record written after it"
    );

    // An older one, arriving late.
    assert_eq!(h.store().merge_tag_generation(b"t", 0).unwrap(), 1);
    assert_eq!(h.get(b"k").as_deref(), Some(&b"fresh"[..]));
}

#[test]
fn merging_registers_a_tag_this_node_has_never_seen() {
    // Required for convergence: without the registration, a later local write
    // with that tag would capture generation 0 and the next gossip round would
    // invalidate a record written after the last invalidation.
    let h = Harness::new();
    assert_eq!(h.store().merge_tag_generation(b"remote", 7).unwrap(), 7);

    h.set_tagged(b"k", b"v", 0, &[b"remote"]);
    assert_eq!(
        h.get(b"k").as_deref(),
        Some(&b"v"[..]),
        "a write after the merge captures the merged generation and is live"
    );

    // And the node now reports that generation to its peers.
    let digest = h.store().tag_generations().unwrap();
    let entry = digest
        .iter()
        .find(|e| &*e.name == b"remote")
        .expect("the tag is registered");
    assert_eq!(entry.generation, 7);
}

#[test]
fn a_digest_reports_one_entry_per_name_regardless_of_shards() {
    let h = Harness::with(|c| c.shards = 4);
    for i in 0..40u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"v", 0, &[b"spread"]);
    }
    h.store().delete_by_tag(b"spread").unwrap();

    let digest = h.store().tag_generations().unwrap();
    assert_eq!(
        digest.iter().filter(|e| &*e.name == b"spread").count(),
        1,
        "a name is one cluster-visible fact, not one per shard: {digest:?}"
    );
    assert_eq!(digest[0].generation, 1);
}

#[test]
fn a_merge_that_changes_nothing_costs_no_writes() {
    // Gossip re-offers the same generations every interval. If each round
    // turned into a write per shard, a converged cluster would spend its write
    // capacity agreeing with itself.
    let h = Harness::with(|c| {
        c.shards = 4;
        // Nothing may be attributed to background maintenance.
        c.write.sweep_interval_ms = 60_000;
    });
    h.set_tagged(b"k", b"v", 0, &[b"news"]);
    h.store().delete_by_tag(b"news").unwrap();

    let before = h.store().stats().unwrap().commits;
    for _ in 0..20 {
        assert_eq!(h.store().merge_tag_generation(b"news", 1).unwrap(), 1);
    }
    assert_eq!(
        h.store().stats().unwrap().commits,
        before,
        "a merge of a generation already held must not reach the writer"
    );
}

#[test]
fn merging_an_unknown_tag_registers_it_once_not_once_per_shard() {
    let h = Harness::with(|c| c.shards = 4);
    h.store().merge_tag_generation(b"remote", 3).unwrap();

    assert_eq!(
        h.store().stats().unwrap().tags,
        1,
        "nothing can be carrying the tag yet, so one shard recording the \
         generation is enough"
    );

    // And it is still the generation every shard will use.
    for i in 0..40u32 {
        h.set_tagged(format!("k{i}").as_bytes(), b"v", 0, &[b"remote"]);
        assert_eq!(
            h.get(format!("k{i}").as_bytes()).as_deref(),
            Some(&b"v"[..])
        );
    }
}

#[test]
fn a_shard_that_meets_a_tag_late_adopts_the_node_wide_generation() {
    // Shards register tags independently, so one can meet a name long after
    // another has invalidated it. If it started at zero, the node would report
    // one generation to its peers while holding a lower one in that shard — and
    // the first gossip round back would invalidate records written after the
    // last invalidation.
    let h = Harness::with(|c| c.shards = 4);

    // One key, so only one shard registers the tag.
    h.set_tagged(b"first", b"v", 0, &[b"t"]);
    let generation = h.store().delete_by_tag(b"t").unwrap().unwrap();
    assert_eq!(generation, 1);

    // Enough keys to be certain every shard now carries the tag.
    for i in 0..40u32 {
        h.set_tagged(format!("late{i}").as_bytes(), b"v", 0, &[b"t"]);
    }

    // Whatever a peer echoes back, nothing written after the invalidation dies.
    h.store().merge_tag_generation(b"t", generation).unwrap();
    for i in 0..40u32 {
        assert_eq!(
            h.get(format!("late{i}").as_bytes()).as_deref(),
            Some(&b"v"[..]),
            "late{i} was written after the invalidation and must survive it"
        );
    }
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
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
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
fn a_batch_smaller_than_the_shard_count_still_lands_in_order() {
    // The test above uses more keys than shards, so every shard has work and the
    // empty case never arises. A batch this small leaves most of them with
    // nothing, which is where grouping goes wrong: an empty shard whose run is
    // computed from the wrong end of the buffer either swallows another shard's
    // items or hands back a slice that overlaps one.
    let h = Harness::with(|c| c.shards = 8);

    for count in [1usize, 2, 3] {
        let names: Vec<String> = (0..count).map(|i| format!("small{count}-{i}")).collect();
        let sets: Vec<Set<'_>> = names
            .iter()
            .enumerate()
            .map(|(i, name)| Set {
                key: Key::new(name.as_bytes()).unwrap(),
                // Distinct per position, so a result landing in the wrong slot
                // is visible rather than accidentally equal.
                value: name.as_bytes(),
                ttl: vash_core::TtlChange::Set(0),
                return_previous: false,
                mc_flags: 0,
                tags: Vec::new(),
                mode: vash_core::SetMode::Set,
            })
            .chain(std::iter::empty())
            .collect();
        assert_eq!(sets.len(), count);

        let cas = h.store().set_many(&sets).unwrap();
        assert_eq!(cas.len(), count, "one token per set, in request order");
        assert!(cas.iter().all(|token| *token > 0));

        let keys: Vec<Key<'_>> = names
            .iter()
            .map(|n| Key::new(n.as_bytes()).unwrap())
            .collect();
        let values = h.store().get_many(&keys).unwrap();
        for (i, value) in values.iter().enumerate() {
            assert_eq!(
                value.as_ref().map(|v| v.data.to_vec()).as_deref(),
                Some(names[i].as_bytes()),
                "{} keys: position {i} came back holding the wrong value",
                count
            );
        }

        let hits = h.store().delete_many(&keys).unwrap();
        assert_eq!(hits.len(), count);
        assert!(hits.iter().all(|hit| *hit), "every key was there to delete");
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
    assert!(h.store().delete_by_tag(b"everything").unwrap().is_some());

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
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
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
        c.map_size = vash_store::config::MIN_MAP_SIZE;
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
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
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
            Err(vash_store::StoreError::CapacityFull) => refused += 1,
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
                ttl: vash_core::TtlChange::Set(0),
                return_previous: false,
                mc_flags: 0,
                tags: Vec::new(),
                mode: vash_core::SetMode::Set,
            })
            .is_ok()
    });
    assert_eq!(h.get(b"after-the-storm").as_deref(), Some(&b"v"[..]));

    println!("overfill: {accepted} accepted, {refused} refused under pressure");
}

#[test]
fn eviction_takes_the_soonest_to_expire_first() {
    let h = Harness::with(|c| {
        c.map_size = vash_store::config::MIN_MAP_SIZE;
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
            ttl: vash_core::TtlChange::Set(600),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
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
                ttl: vash_core::TtlChange::Set(3600),
                return_previous: false,
                mc_flags: 0,
                tags: Vec::new(),
                mode: vash_core::SetMode::Set,
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
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
        })
        .unwrap();
    assert!(next > cas, "CAS must not go backwards across a restart");
    store.close();
}

// ---- listing ---------------------------------------------------------------

/// Pages a whole listing the way a client must, and returns the names in the
/// order they arrived.
///
/// Deliberately a real paging loop rather than one big call: the cursor
/// arithmetic across shard boundaries is the part worth testing, and a single
/// call with a large limit would never exercise it.
fn page_keys(store: &LmdbStore, limit: u32, pattern: &[u8], max_scan: usize) -> Vec<Vec<u8>> {
    let mut names = Vec::new();
    let mut cursor: Vec<u8> = Vec::new();
    // A listing must terminate; a bound turns "the cursor stopped advancing"
    // from a hung test into a failing one.
    for _ in 0..10_000 {
        let request = vash_core::ListRequest {
            limit,
            cursor: &cursor,
            pattern,
        };
        let page = store.list_keys(&request, max_scan).unwrap();
        assert!(
            page.entries.len() <= limit as usize,
            "a page must never exceed the limit it was given"
        );
        names.extend(page.entries.iter().map(|e| e.name.to_vec()));

        match page.cursor {
            Some(next) => cursor = next.to_vec(),
            None => return names,
        }
    }
    panic!("listing did not terminate");
}

#[test]
fn listing_returns_every_live_key_exactly_once() {
    let h = Harness::new();
    for i in 0..250 {
        h.set(format!("key:{i:04}").as_bytes(), b"v", 0);
    }

    let listed = page_keys(h.store(), 7, b"", 100_000);
    let mut unique = listed.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(listed.len(), 250, "no key returned twice");
    assert_eq!(unique.len(), 250, "no key missed");
}

#[test]
fn the_page_size_does_not_change_the_result() {
    // The property that matters for pagination: a limit is a transport detail,
    // never a filter. A cursor bug at a shard boundary shows up here as a
    // difference between page sizes.
    let h = Harness::with(|c| c.shards = 4);
    for i in 0..120 {
        h.set(format!("k{i:03}").as_bytes(), b"v", 0);
    }

    let one_at_a_time = page_keys(h.store(), 1, b"", 100_000);
    let in_bulk = page_keys(h.store(), 1024, b"", 100_000);
    assert_eq!(one_at_a_time, in_bulk);
    assert_eq!(one_at_a_time.len(), 120);
}

#[test]
fn a_listing_never_shows_a_key_a_get_would_miss() {
    let h = Harness::new();
    h.set(b"live", b"v", 0);
    h.set(b"tagged", b"v", 0);
    h.set_tagged(b"doomed", b"v", 0, &[b"news"]);
    h.set(b"expiring", b"v", 1);

    h.store().delete_by_tag(b"news").unwrap();
    h.wait_for("the TTL to pass", |h| h.get(b"expiring").is_none());

    let listed = page_keys(h.store(), 100, b"", 100_000);
    assert!(listed.contains(&b"live".to_vec()));
    assert!(listed.contains(&b"tagged".to_vec()));
    assert!(!listed.contains(&b"doomed".to_vec()), "tag-invalidated");
    assert!(!listed.contains(&b"expiring".to_vec()), "expired");
}

#[test]
fn a_pattern_filters_without_changing_the_paging() {
    let h = Harness::with(|c| c.shards = 2);
    for i in 0..40 {
        h.set(format!("session:{i:03}").as_bytes(), b"v", 0);
        h.set(format!("user:{i:03}").as_bytes(), b"v", 0);
    }

    let sessions = page_keys(h.store(), 6, b"session:*", 100_000);
    assert_eq!(sessions.len(), 40);
    assert!(sessions.iter().all(|k| k.starts_with(b"session:")));

    let one = page_keys(h.store(), 3, b"user:007", 100_000);
    assert_eq!(one, vec![b"user:007".to_vec()]);
}

#[test]
fn a_scan_budget_truncates_a_page_but_still_advances() {
    // The failure this guards against is a pager that cannot get past a region
    // of non-matching records: if a truncated page did not advance its cursor,
    // the client would re-scan the same prefix forever.
    let h = Harness::new();
    for i in 0..500 {
        h.set(format!("noise:{i:04}").as_bytes(), b"v", 0);
    }
    h.set(b"zzz:wanted", b"v", 0);

    let request = vash_core::ListRequest {
        limit: 100,
        cursor: &[],
        pattern: b"zzz:*",
    };
    let first = h.store().list_keys(&request, 10).unwrap();
    assert!(first.truncated, "the budget should have stopped this page");
    assert!(first.entries.is_empty(), "nothing matching in the first 10");
    assert!(
        first.cursor.is_some(),
        "a truncated page must still advance"
    );

    // And paging through with that tiny budget still finds the needle.
    let found = page_keys(h.store(), 100, b"zzz:*", 10);
    assert_eq!(found, vec![b"zzz:wanted".to_vec()]);
}

#[test]
fn scanned_counts_the_records_walked_not_the_ones_returned() {
    let h = Harness::new();
    for i in 0..50 {
        h.set(format!("k{i:02}").as_bytes(), b"v", 0);
    }

    let request = vash_core::ListRequest {
        limit: 1024,
        cursor: &[],
        pattern: b"k49",
    };
    let page = h.store().list_keys(&request, 100_000).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.scanned, 50, "the cost of a non-selective pattern");
}

#[test]
fn a_malformed_cursor_is_refused_rather_than_restarting_the_listing() {
    // Silently restarting would loop a pager forever, handing back the first
    // page every time and never saying why.
    let h = Harness::new();
    h.set(b"k", b"v", 0);

    for bad in [
        vec![1u8],              // shorter than the shard index
        vec![0, 0],             // names no key
        vec![0xff, 0xff, b'k'], // a shard this server does not have
    ] {
        let request = vash_core::ListRequest {
            limit: 10,
            cursor: &bad,
            pattern: b"",
        };
        assert!(
            h.store().list_keys(&request, 100_000).is_err(),
            "cursor {bad:?} should be refused"
        );
    }
}

#[test]
fn listing_tags_pages_in_name_order_with_generations() {
    let h = Harness::new();
    h.set_tagged(b"a", b"v", 0, &[b"sport", b"news", b"weather"]);
    h.store().delete_by_tag(b"news").unwrap();

    let mut entries = Vec::new();
    let mut cursor: Vec<u8> = Vec::new();
    loop {
        let request = vash_core::ListRequest {
            limit: 2,
            cursor: &cursor,
            pattern: b"",
        };
        let page = h.store().list_tags(&request).unwrap();
        assert!(!page.truncated, "a RAM walk has no budget to exhaust");
        entries.extend(page.entries.iter().map(|e| (e.name.to_vec(), e.version)));
        match page.cursor {
            Some(next) => cursor = next.to_vec(),
            None => break,
        }
    }

    let names: Vec<Vec<u8>> = entries.iter().map(|(n, _)| n.clone()).collect();
    assert_eq!(
        names,
        vec![b"news".to_vec(), b"sport".to_vec(), b"weather".to_vec()],
        "lexicographic, so two nodes' listings are comparable"
    );

    let generation = |want: &[u8]| entries.iter().find(|(n, _)| n == want).unwrap().1;
    assert!(generation(b"news") > 0, "invalidated once");
    assert_eq!(generation(b"sport"), 0, "registered but never invalidated");
}

#[test]
fn a_tag_registered_mid_walk_does_not_shift_the_page_after_it() {
    // Resuming by name rather than by position is what buys this: under offset
    // paging a new entry sorting before the cursor would shift everything after
    // it and skip a tag that was there the whole time.
    let h = Harness::new();
    h.set_tagged(b"a", b"v", 0, &[b"bbb", b"ddd"]);

    let request = vash_core::ListRequest {
        limit: 1,
        cursor: &[],
        pattern: b"",
    };
    let first = h.store().list_tags(&request).unwrap();
    assert_eq!(&*first.entries[0].name, b"bbb");

    // Sorts before the cursor, so it is missed — but nothing else moves.
    h.set_tagged(b"b", b"v", 0, &[b"aaa"]);

    let cursor = first.cursor.unwrap();
    let request = vash_core::ListRequest {
        limit: 10,
        cursor: &cursor,
        pattern: b"",
    };
    let rest = h.store().list_tags(&request).unwrap();
    let names: Vec<Vec<u8>> = rest.entries.iter().map(|e| e.name.to_vec()).collect();
    assert_eq!(
        names,
        vec![b"ddd".to_vec()],
        "the tag present throughout the walk is still returned exactly once"
    );
}

// ---- atomic arithmetic ---------------------------------------------------

/// A counter carrying tags must keep them across an increment.
///
/// An increment alters the value in place, so the record's tag table has to
/// survive the rewrite. One that silently shed its tags would go on being served
/// after the invalidation it was tagged for — a stale hit, which is the failure
/// direction a cache must never take.
#[test]
fn writing_over_an_expired_record_clears_the_rows_it_left() {
    // A record that has expired but not been swept is invisible to a client and
    // still entirely present on disk, index rows and all. A write over it counts
    // as creating the key — there is nothing live to build on — but it must
    // still clear what the dead record owned, or the expiry entry and the tag
    // rows outlive the record they describe and the reclaimer keeps finding
    // work that no longer exists.
    //
    // The distinction is invisible through the liveness check alone, which is
    // exactly what makes it worth pinning: an implementation that decides "was
    // anything here?" by asking "would a client see it?" passes every other
    // test in this file and leaks a row on every one of these.
    let h = Harness::with(|c| c.write.sweep_interval_ms = 10_000);

    h.set_tagged(b"n", b"1", 1, &[b"news"]);
    assert_eq!(h.expiry_entries(), 1);
    assert_eq!(h.tag_index_entries(), 1);

    std::thread::sleep(Duration::from_millis(1_200));
    assert!(
        h.get(b"n").is_none(),
        "expired, though still on disk: the sweeper has not run"
    );

    // Redis's arithmetic, not memcached's: this one creates the key at zero when
    // nothing live is there, which is what makes it a *write* over the dead
    // record rather than the `NOT_FOUND` that memcached's counter would answer.
    h.arithmetic(&vash_core::Arithmetic::redis(
        Key::new(b"n").unwrap(),
        vash_core::Delta::Int {
            delta: 5,
            lower: i64::MIN,
            upper: i64::MAX,
        },
    ));

    assert_eq!(
        h.expiry_entries(),
        1,
        "the dead record's expiry row must go with it, not accumulate beside the new one"
    );
    assert_eq!(
        h.tag_index_entries(),
        0,
        "the new record carries no tags, so the dead one's tag row must not survive it"
    );

    // The same for a concatenation, which takes the other branch.
    h.set_tagged(b"a", b"x", 1, &[b"news"]);
    std::thread::sleep(Duration::from_millis(1_200));
    h.store().append(Key::new(b"a").unwrap(), b"y").unwrap();

    assert_eq!(
        h.expiry_entries(),
        2,
        "one row for the counter and one for the appended key, and no leftovers"
    );
    assert_eq!(h.tag_index_entries(), 0);

    // And for a guarded write, which reads the record to judge its guard and
    // carries that read forward. `add` is the sharpest case: it applies only
    // because the expired record reads as absent, so the very thing that lets
    // the write through is the thing that hides the rows it has to clear.
    h.set_tagged(b"g", b"x", 1, &[b"news"]);
    std::thread::sleep(Duration::from_millis(1_200));
    let written = h
        .store()
        .store(&Set {
            key: Key::new(b"g").unwrap(),
            value: b"fresh",
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Add,
        })
        .unwrap();
    assert!(
        matches!(written.outcome, vash_core::Stored::Stored(_)),
        "an expired key reads as absent, so `add` applies: {:?}",
        written.outcome
    );

    assert_eq!(
        h.expiry_entries(),
        3,
        "the guarded write must clear the expired record's row too"
    );
    assert_eq!(h.tag_index_entries(), 0);
}

#[test]
fn arithmetic_keeps_the_tag_table() {
    let h = Harness::new();
    h.set_tagged(b"n", b"1", 0, &[b"news"]);

    let applied = h
        .arithmetic(&vash_core::Arithmetic::counter(
            Key::new(b"n").unwrap(),
            4,
            false,
        ))
        .expect("the key is live");
    assert_eq!(applied.value, vash_core::Number::Counter(5));
    assert_eq!(h.get(b"n").as_deref(), Some(b"5".as_slice()));

    h.store().delete_by_tag(b"news").unwrap();
    assert_eq!(
        h.get(b"n"),
        None,
        "the incremented record must still answer to its tag"
    );
}

/// `TtlChange::Keep` must preserve the deadline exactly, not re-derive it.
///
/// Re-deriving would round the remaining lifetime to whole seconds on every
/// increment, so a counter incremented once a second would drift its own expiry
/// forward indefinitely.
#[test]
fn arithmetic_leaves_the_deadline_untouched() {
    let h = Harness::new();
    h.set(b"n", b"1", 3600);
    let before = h.store().deadline(Key::new(b"n").unwrap()).unwrap();

    for _ in 0..5 {
        h.arithmetic(&vash_core::Arithmetic::counter(
            Key::new(b"n").unwrap(),
            1,
            false,
        ));
    }

    assert_eq!(
        h.store().deadline(Key::new(b"n").unwrap()).unwrap(),
        before,
        "the deadline is preserved by not being touched"
    );
    assert_eq!(h.get(b"n").as_deref(), Some(b"6".as_slice()));
}

/// A Redis-style increment creates the key it did not find; a memcached-style
/// one reports a miss and writes nothing.
#[test]
fn creation_on_a_missing_key_is_the_dialects_choice() {
    let h = Harness::new();

    assert!(
        h.arithmetic(&vash_core::Arithmetic::counter(
            Key::new(b"absent").unwrap(),
            1,
            false
        ))
        .is_none()
    );
    assert_eq!(h.entries(), 0, "a miss must not leave a record behind");

    let applied = h
        .arithmetic(&vash_core::Arithmetic::redis(
            Key::new(b"fresh").unwrap(),
            vash_core::Delta::int(7),
        ))
        .expect("creates at zero");
    assert_eq!(applied.value, vash_core::Number::Int(7));
    assert_eq!(h.get(b"fresh").as_deref(), Some(b"7".as_slice()));
}

/// Appending must keep the record's deadline and its client flags.
#[test]
fn append_preserves_the_deadline_and_the_flags() {
    let h = Harness::new();
    h.store()
        .set(&Set {
            key: Key::new(b"k").unwrap(),
            value: b"a",
            ttl: vash_core::TtlChange::Set(3600),
            return_previous: false,
            mc_flags: 0xbeef,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
        })
        .unwrap();
    let before = h.store().deadline(Key::new(b"k").unwrap()).unwrap();

    assert_eq!(h.store().append(Key::new(b"k").unwrap(), b"bc").unwrap(), 3);
    assert_eq!(h.get(b"k").as_deref(), Some(b"abc".as_slice()));
    assert_eq!(h.store().deadline(Key::new(b"k").unwrap()).unwrap(), before);

    // The flags belong to whichever memcached client wrote the value; a Redis
    // append has no opinion about them and must not clear them.
    let value = h.store().get(Key::new(b"k").unwrap()).unwrap().unwrap();
    assert_eq!(value.mc_flags, 0xbeef);
}

// ---- prefault --------------------------------------------------------------

#[test]
fn a_prefaulted_store_opens_and_serves_every_shard() {
    // The warming pass runs once per environment, inside `LmdbEngine::open`,
    // and reaches for a `data.mdb` that LMDB has just created. A shard whose
    // file was missing or held open exclusively would fail here rather than in
    // the unit tests, which supply their own file.
    let dir = tempfile::tempdir().unwrap();
    let mut config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 32 * 1024 * 1024,
        shards: 4,
        ..StoreConfig::default()
    };

    {
        let store = LmdbStore::open(&config).unwrap();
        for i in 0..64u32 {
            store
                .set(&Set {
                    key: Key::new(format!("k{i}").as_bytes()).unwrap(),
                    value: b"v",
                    ttl: vash_core::TtlChange::Set(3600),
                    return_previous: false,
                    mc_flags: 0,
                    tags: Vec::new(),
                    mode: vash_core::SetMode::Set,
                })
                .unwrap();
        }
        store.close();
    }

    config.prefault = true;
    let store = LmdbStore::open(&config).unwrap();
    for i in 0..64u32 {
        let key = format!("k{i}");
        assert!(
            store
                .get(Key::new(key.as_bytes()).unwrap())
                .unwrap()
                .is_some(),
            "{key} must survive a prefaulted open"
        );
    }
    store.close();
}

#[test]
fn prefaulting_an_empty_database_is_not_an_error() {
    // Nothing has been written, so the file is whatever LMDB creates for an
    // empty environment. Warming it must be a no-op, not a startup failure.
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 32 * 1024 * 1024,
        prefault: true,
        ..StoreConfig::default()
    };

    let store = LmdbStore::open(&config).unwrap();
    assert_eq!(store.stats().unwrap().entries, 0);
    store.close();
}

/// **`lazy` keeps the database, it only loosens when the data reaches the
/// device.** That is the whole distinction from `ephemeral`, which has to be
/// wiped at startup, and it is the claim the mode is sold on — so it is pinned
/// rather than reasoned about: write under `lazy`, close cleanly, reopen, and
/// find everything still there and still readable.
#[test]
fn a_lazy_store_reopens_with_its_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        path: dir.path().join("db"),
        map_size: 64 * 1024 * 1024,
        durability: vash_store::Durability::Lazy,
        ..StoreConfig::default()
    };

    {
        let store = LmdbStore::open(&config).unwrap();
        for i in 0..64u32 {
            store
                .set(&Set {
                    key: Key::new(format!("k{i}").as_bytes()).unwrap(),
                    value: b"v",
                    ttl: vash_core::TtlChange::Set(0),
                    return_previous: false,
                    mc_flags: 0,
                    tags: Vec::new(),
                    mode: vash_core::SetMode::Set,
                })
                .unwrap();
        }
        store.close();
    }

    let store = LmdbStore::open(&config).unwrap();
    for i in 0..64u32 {
        let key = format!("k{i}");
        assert_eq!(
            store
                .get(Key::new(key.as_bytes()).unwrap())
                .unwrap()
                .map(|v| v.data.to_vec())
                .as_deref(),
            Some(&b"v"[..]),
            "{key} did not survive the restart"
        );
    }
    store.close();
}

/// The periodic sync runs on its own, without a client asking for one. It is
/// what bounds `lazy`'s loss window, and — since nothing but shutdown ever
/// forced a sync before — what makes `relaxed`'s documented promise true.
#[test]
fn the_writer_syncs_on_its_own_timer() {
    let h = Harness::with(|c| {
        c.durability = vash_store::Durability::Lazy;
        c.write.sync_interval_ms = 10;
    });

    h.set(b"k", b"v", 0);
    // Several intervals, so a sync that only ever ran on demand would not have
    // happened by now.
    std::thread::sleep(Duration::from_millis(120));

    // The store is still serving, which is the observable part: a sync that
    // panicked or wedged the writer would show up as this hanging or failing.
    assert_eq!(h.get(b"k").as_deref(), Some(&b"v"[..]));
    h.set(b"k2", b"v2", 0);
    assert_eq!(h.get(b"k2").as_deref(), Some(&b"v2"[..]));
}

/// **`write_map` is a memory setting, not a durability one**, and it is now
/// separable from both. The old `ephemeral` mode welded it to `NO_SYNC`; it
/// measured slower than going without, twice, so what it buys — LMDB not
/// allocating a copy of every dirty page — is available on its own to anyone who
/// wants that trade. This pins that a store which sets it still works.
#[test]
fn a_store_with_write_map_still_serves() {
    let h = Harness::with(|c| {
        c.write_map = true;
        c.wipe_on_start = true;
    });

    h.set(b"k", b"v", 0);
    assert_eq!(h.get(b"k").as_deref(), Some(&b"v"[..]));
    h.set(b"k", b"w", 60);
    assert_eq!(h.get(b"k").as_deref(), Some(&b"w"[..]));
    assert_eq!(h.expiry_entries(), 1);
}
