//! Measures whether `DELETE_BY_TAG` is really constant time.
//!
//! ```text
//! cargo run --release -p vash-store --example tag_bench
//! ```
//!
//! This is the claim the whole tag design rests on: invalidating a tag must
//! cost the same whether it covers ten keys or a million, because it is one
//! counter bump rather than a walk over the affected keys. If this number grows
//! with cardinality, the design has silently regressed into the O(n) version.
//!
//! Reclamation of the freed space happens afterwards, in the background, and is
//! reported separately â€” it *is* proportional to the number of keys, and is
//! meant to be.

use std::time::{Duration, Instant};

use vash_core::{Key, Set};
use vash_store::{LmdbStore, Store, StoreConfig, WriteConfig};

const CARDINALITIES: [usize; 5] = [100, 1_000, 10_000, 100_000, 500_000];
const VALUE: &[u8] = &[b'x'; 128];

fn main() {
    println!("invalidating one tag, varying how many keys carry it\n");
    println!(
        "{:>10}  {:>14}  {:>16}  {:>18}",
        "keys", "populate", "DELETE_BY_TAG", "reclaim (bg)"
    );

    for keys in CARDINALITIES {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = LmdbStore::open(&StoreConfig {
            path: dir.path().join("db"),
            map_size: 4 * 1024 * 1024 * 1024,
            write: WriteConfig {
                sweep_interval_ms: 10,
                reclaim_batch: 4096,
                ..WriteConfig::default()
            },
            ..StoreConfig::default()
        })
        .expect("opening the store");

        // Populate in batches so setup is not dominated by commit cost.
        let started = Instant::now();
        let mut written = 0usize;
        while written < keys {
            let batch = 512.min(keys - written);
            let names: Vec<String> = (0..batch).map(|i| format!("k{}", written + i)).collect();
            let sets: Vec<Set<'_>> = names
                .iter()
                .map(|key| Set {
                    key: Key::new(key.as_bytes()).unwrap(),
                    value: VALUE,
                    ttl: vash_core::TtlChange::Set(0),
                    return_previous: false,
                    mc_flags: 0,
                    tags: vec![b"everything"],
                    mode: vash_core::SetMode::Set,
                })
                .collect();
            store.set_many(&sets).expect("set_many");
            written += batch;
        }
        let populate = started.elapsed();

        // The measurement that matters.
        let started = Instant::now();
        store.delete_by_tag(b"everything").expect("delete_by_tag");
        let invalidate = started.elapsed();

        // Confirm it actually took effect, then time the background cleanup.
        assert!(
            store.get(Key::new(b"k0").unwrap()).unwrap().is_none(),
            "invalidation must be visible immediately"
        );

        let started = Instant::now();
        let deadline = started + Duration::from_secs(120);
        while store.stats().unwrap().pending_reclaims > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let reclaim = started.elapsed();

        println!(
            "{keys:>10}  {:>12.2?}  {:>16.3?}  {:>18.2?}",
            populate, invalidate, reclaim
        );

        assert_eq!(
            store.stats().unwrap().entries,
            0,
            "reclamation left records behind"
        );
        store.close();
    }

    println!(
        "\nDELETE_BY_TAG should be flat across the column; reclaim should grow with the key count."
    );
}
