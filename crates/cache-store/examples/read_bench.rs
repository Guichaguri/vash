//! Measures the read path without a socket in front of it.
//!
//! ```text
//! cargo run --release -p cache-store --example read_bench
//! ```
//!
//! LMDB reads are supposed to be lock-free and to scale linearly with threads:
//! that property is the whole reason the single-writer tax is worth paying
//! (plan §9). This checks it, and separates two costs that the end-to-end
//! numbers cannot tell apart — the lookup itself, and beginning the read
//! transaction it runs in.
//!
//! The gap between the two columns is the per-request transaction overhead. If
//! it is large, the answer is to amortise a transaction across a batch rather
//! than to look for a faster lookup.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cache_core::{Key, Set};
use cache_store::{Durability, LmdbStore, Store, StoreConfig, WriteConfig};

const KEYS: u64 = 100_000;
const VALUE: &[u8] = &[b'x'; 1024];
const DURATION: Duration = Duration::from_secs(3);

fn key_for(index: u64) -> Vec<u8> {
    format!("bench:key:{index:012}").into_bytes()
}

fn populate(store: &LmdbStore) {
    let mut written = 0u64;
    while written < KEYS {
        let batch = 512.min(KEYS - written);
        let keys: Vec<Vec<u8>> = (0..batch).map(|i| key_for(written + i)).collect();
        let sets: Vec<Set<'_>> = keys
            .iter()
            .map(|key| Set::plain(Key::new(key).unwrap(), VALUE, 0))
            .collect();
        store.set_many(&sets).expect("set_many");
        written += batch;
    }
}

/// Runs `body` on `threads` threads for a fixed duration, returning ops/s.
fn measure(threads: usize, body: impl Fn(u64) -> u64 + Send + Sync) -> f64 {
    let total = AtomicU64::new(0);
    let started = Instant::now();

    std::thread::scope(|scope| {
        for t in 0..threads {
            let total = &total;
            let body = &body;
            scope.spawn(move || {
                let mut cursor = (t as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let mut done = 0u64;
                while started.elapsed() < DURATION {
                    // A chunk between clock reads, so `Instant::now` is not
                    // what the benchmark ends up measuring.
                    for _ in 0..256 {
                        cursor = cursor.wrapping_add(0x9E37_79B9_7F4A_7C15);
                        done += body(cursor % KEYS);
                    }
                }
                total.fetch_add(done, Ordering::Relaxed);
            });
        }
    });

    total.load(Ordering::Relaxed) as f64 / started.elapsed().as_secs_f64()
}

fn main() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(
        LmdbStore::open(&StoreConfig {
            path: dir.path().join("db"),
            map_size: 2 * 1024 * 1024 * 1024,
            durability: Durability::Ephemeral,
            write: WriteConfig {
                sweep_interval_ms: 60_000,
                ..WriteConfig::default()
            },
            ..StoreConfig::default()
        })
        .expect("opening the store"),
    );

    println!("populating {KEYS} keys of {} bytes...", VALUE.len());
    populate(&store);
    println!(
        "{} keys resident, {:.1} MiB\n",
        store.stats().unwrap().entries,
        store.stats().unwrap().used_bytes as f64 / (1024.0 * 1024.0)
    );

    println!(
        "{:>8}  {:>16}  {:>16}  {:>10}",
        "threads", "get (1 txn each)", "get_many of 64", "ratio"
    );

    for threads in [1usize, 2, 4, 8, 16] {
        // One transaction per lookup: what a single GET costs today.
        let single = {
            let store = Arc::clone(&store);
            measure(threads, move |index| {
                let key = key_for(index);
                store.get(Key::new(&key).unwrap()).expect("get").is_some() as u64
            })
        };

        // Sixty-four lookups sharing one transaction: what the same work costs
        // when the transaction is amortised across a batch.
        let batched = {
            let store = Arc::clone(&store);
            measure(threads, move |index| {
                let keys: Vec<Vec<u8>> = (0..64).map(|i| key_for((index + i) % KEYS)).collect();
                let refs: Vec<Key<'_>> = keys.iter().map(|k| Key::new(k).unwrap()).collect();
                store.get_many(&refs).expect("get_many").len() as u64
            })
        };

        println!(
            "{threads:>8}  {single:>16.0}  {batched:>16.0}  {:>9.1}x",
            batched / single.max(1.0)
        );
    }

    println!(
        "\nColumn one is one read transaction per lookup; column two amortises one\n\
         across 64. The ratio is the per-transaction overhead, and scaling across\n\
         rows is whether LMDB's lock-free reads really are lock-free here."
    );

    Arc::try_unwrap(store)
        .unwrap_or_else(|_| panic!("store still shared"))
        .close();
}
