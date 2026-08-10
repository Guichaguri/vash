//! Measures whether group commit actually amortises the commit cost.
//!
//! ```text
//! cargo run --release -p cache-store --example write_bench
//! ```
//!
//! LMDB allows one writer, so the only way writes scale is by fitting more of
//! them into each transaction. This reports throughput alongside the mean batch
//! size the writer achieved, so the two can be seen moving together rather than
//! taken on faith.

use std::sync::Arc;
use std::time::Instant;

use cache_core::{Key, Set};
use cache_store::{Durability, LmdbStore, Store, StoreConfig, WriteConfig};

const OPS: usize = 20_000;
const VALUE: &[u8] = &[b'x'; 256];

fn open(dir: &std::path::Path, durability: Durability) -> LmdbStore {
    LmdbStore::open(&StoreConfig {
        path: dir.to_path_buf(),
        map_size: 1024 * 1024 * 1024,
        durability,
        // Long interval: this measures writes, not reclamation.
        write: WriteConfig {
            sweep_interval_ms: 60_000,
            ..WriteConfig::default()
        },
        ..StoreConfig::default()
    })
    .expect("opening the store")
}

fn set(store: &LmdbStore, key: &str) {
    store
        .set(&Set {
            key: Key::new(key.as_bytes()).unwrap(),
            value: VALUE,
            ttl_secs: 0,
            mc_flags: 0,
            tags: Vec::new(),
            mode: cache_core::SetMode::Set,
        })
        .expect("set");
}

fn report(label: &str, ops: usize, elapsed: std::time::Duration, store: &LmdbStore) {
    let stats = store.stats().expect("stats");
    println!(
        "{label:<34} {:>10.0} ops/s   mean batch {:>7.1}   commits {:>7}",
        ops as f64 / elapsed.as_secs_f64(),
        stats.mean_batch_size(),
        stats.commits
    );
}

fn scenario(name: &str, durability: Durability, run: impl FnOnce(&Arc<LmdbStore>)) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(open(&dir.path().join("db"), durability));

    let started = Instant::now();
    run(&store);
    let elapsed = started.elapsed();

    report(name, OPS, elapsed, &store);
    Arc::try_unwrap(store)
        .unwrap_or_else(|_| panic!("store still shared"))
        .close();
}

fn main() {
    println!(
        "{OPS} writes of a {}-byte value, single LMDB environment\n",
        VALUE.len()
    );

    for durability in [Durability::Relaxed, Durability::Durable] {
        println!("-- durability: {durability:?}");

        // One caller at a time: every write is its own transaction, so this is
        // the un-batched baseline.
        scenario("1 thread, one write at a time", durability, |store| {
            for i in 0..OPS {
                set(store, &format!("k{i}"));
            }
        });

        // Concurrent callers: batches form on their own, because each commit
        // leaves the queue holding whatever arrived while it ran.
        for threads in [8usize, 64] {
            scenario(
                &format!("{threads} threads, one write at a time"),
                durability,
                |store| {
                    let per_thread = OPS / threads;
                    std::thread::scope(|scope| {
                        for t in 0..threads {
                            let store = Arc::clone(store);
                            scope.spawn(move || {
                                for i in 0..per_thread {
                                    set(&store, &format!("t{t}-k{i}"));
                                }
                            });
                        }
                    });
                },
            );
        }

        // Explicit batching: one round trip carries many writes.
        for batch in [16usize, 256] {
            scenario(
                &format!("1 thread, set_many of {batch}"),
                durability,
                |store| {
                    let mut written = 0;
                    while written < OPS {
                        let keys: Vec<String> =
                            (0..batch).map(|i| format!("b{}", written + i)).collect();
                        let sets: Vec<Set<'_>> = keys
                            .iter()
                            .map(|key| Set {
                                key: Key::new(key.as_bytes()).unwrap(),
                                value: VALUE,
                                ttl_secs: 0,
                                mc_flags: 0,
                                tags: Vec::new(),
                                mode: cache_core::SetMode::Set,
                            })
                            .collect();
                        store.set_many(&sets).expect("set_many");
                        written += batch;
                    }
                },
            );
        }
        println!();
    }
}
