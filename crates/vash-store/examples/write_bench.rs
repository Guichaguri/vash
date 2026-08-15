//! Measures whether group commit actually amortises the commit cost.
//!
//! ```text
//! cargo run --release -p vash-store --example write_bench
//! ```
//!
//! LMDB allows one writer, so the only way writes scale is by fitting more of
//! them into each transaction. This reports throughput alongside the mean batch
//! size the writer achieved, so the two can be seen moving together rather than
//! taken on faith.

use std::sync::Arc;
use std::time::Instant;

use vash_core::{Key, Set};
use vash_store::{Durability, LmdbStore, Store, StoreConfig, WriteConfig};

/// Total writes per scenario. Raise it to see how shard scaling depends on
/// offered load: `VASH_BENCH_OPS=200000 cargo run --release ...`
fn ops() -> usize {
    std::env::var("VASH_BENCH_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000)
}

const VALUE: &[u8] = &[b'x'; 256];

fn open(dir: &std::path::Path, durability: Durability, shards: usize) -> LmdbStore {
    LmdbStore::open(&StoreConfig {
        path: dir.to_path_buf(),
        map_size: 1024 * 1024 * 1024,
        shards,
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
            ttl: vash_core::TtlChange::Set(0),
            return_previous: false,
            mc_flags: 0,
            tags: Vec::new(),
            mode: vash_core::SetMode::Set,
        })
        .expect("set");
}

/// Repetitions per scenario. Throughput noise is additive — a slow run means
/// something else stole the disk — so the best of several is a better estimate
/// of what the code can do than any single run.
fn repeats() -> usize {
    std::env::var("VASH_BENCH_REPEATS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

fn scenario(
    name: &str,
    durability: Durability,
    shards: usize,
    run: impl Fn(&Arc<LmdbStore>) + Copy,
) {
    let mut best = 0.0f64;
    let mut best_batch = 0.0;
    let mut best_commits = 0;

    for _ in 0..repeats() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(open(&dir.path().join("db"), durability, shards));

        let started = Instant::now();
        run(&store);
        let elapsed = started.elapsed();

        let rate = ops() as f64 / elapsed.as_secs_f64();
        if rate > best {
            let stats = store.stats().expect("stats");
            best = rate;
            best_batch = stats.mean_batch_size();
            best_commits = stats.commits;
        }
        Arc::try_unwrap(store)
            .unwrap_or_else(|_| panic!("store still shared"))
            .close();
    }

    println!(
        "{name:<34} {best:>10.0} ops/s   mean batch {best_batch:>7.1}   commits {best_commits:>7}"
    );
}

/// Writes `ops()` values from `threads` callers at once.
///
/// The shape that matters for sharding: LMDB serialises writers per
/// environment, so more shards should mean more of these running at once.
fn concurrent(store: &Arc<LmdbStore>, threads: usize) {
    let per_thread = ops() / threads;
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
}

fn main() {
    println!("{} writes of a {}-byte value\n", ops(), VALUE.len());

    // How batching alone scales, on one environment.
    println!("-- group commit (1 shard, relaxed durability)");
    scenario(
        "1 thread, one write at a time",
        Durability::Relaxed,
        1,
        |store| {
            for i in 0..ops() {
                set(store, &format!("k{i}"));
            }
        },
    );
    for threads in [8usize, 64] {
        scenario(
            &format!("{threads} threads, one write at a time"),
            Durability::Relaxed,
            1,
            |store| concurrent(store, threads),
        );
    }
    for batch in [16usize, 256] {
        scenario(
            &format!("1 thread, set_many of {batch}"),
            Durability::Relaxed,
            1,
            |store| {
                let mut written = 0;
                while written < ops() {
                    let keys: Vec<String> =
                        (0..batch).map(|i| format!("b{}", written + i)).collect();
                    let sets: Vec<Set<'_>> = keys
                        .iter()
                        .map(|key| Set::plain(Key::new(key.as_bytes()).unwrap(), VALUE, 0))
                        .collect();
                    store.set_many(&sets).expect("set_many");
                    written += batch;
                }
            },
        );
    }

    // How sharding scales, holding the client concurrency fixed. LMDB permits
    // one writer per environment, so this is the axis that adds writers.
    println!("\n-- sharding (64 concurrent writers, relaxed durability)");
    for shards in [1usize, 2, 4, 8] {
        scenario(
            &format!("{shards} shard(s)"),
            Durability::Relaxed,
            shards,
            |store| concurrent(store, 64),
        );
    }

    println!("\n-- sharding (64 concurrent writers, durable)");
    for shards in [1usize, 4, 8] {
        scenario(
            &format!("{shards} shard(s)"),
            Durability::Durable,
            shards,
            |store| concurrent(store, 64),
        );
    }

    // The same axis with syncing switched off. If throughput jumps here but not
    // above, the ceiling was the disk rather than the single writer thread —
    // and sharding cannot fix a disk.
    println!("\n-- sharding (64 concurrent writers, ephemeral: no syncing)");
    for shards in [1usize, 2, 4, 8] {
        scenario(
            &format!("{shards} shard(s)"),
            Durability::Lazy,
            shards,
            |store| concurrent(store, 64),
        );
    }
}
