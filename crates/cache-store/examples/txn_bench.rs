//! Isolates the cost of *beginning* a read transaction, with and without
//! thread-local reader slots.
//!
//! ```text
//! cargo run --release -p cache-store --example txn_bench
//! ```
//!
//! Plan §9 chose `read_txn_without_tls()` so a `RoTxn` would be `Send` and could
//! be moved between threads by a hand-rolled reader pool. The pool was never
//! built — reads run on the blocking pool, where a transaction is created and
//! dropped inside one call and never crosses a thread — so the flag was being
//! paid for without being used.
//!
//! What it costs is this: without thread-local storage, every `mdb_txn_begin`
//! has to claim a slot in the environment's shared reader table, and that table
//! is guarded by a process-wide mutex. The lookup itself stays lock-free; the
//! transaction around it does not. This measures the two separately so the
//! difference is not a matter of opinion.
//!
//! Nothing here goes through `LmdbStore`: it is a question about LMDB, and
//! answering it with the store in the way would leave room to blame the store.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, WithTls, WithoutTls};

const KEYS: u64 = 50_000;
const VALUE: &[u8] = &[b'x'; 1024];
const DURATION: Duration = Duration::from_secs(2);

fn key_for(index: u64) -> Vec<u8> {
    format!("bench:key:{index:012}").into_bytes()
}

/// Fills a fresh environment and returns it with its database.
fn populate<T>(env: &Env<T>) -> Database<Bytes, Bytes> {
    let mut wtxn = env.write_txn().expect("write txn");
    let db: Database<Bytes, Bytes> = env
        .create_database(&mut wtxn, Some("main"))
        .expect("create db");
    for i in 0..KEYS {
        db.put(&mut wtxn, &key_for(i), VALUE).expect("put");
    }
    wtxn.commit().expect("commit");
    db
}

fn measure(threads: usize, body: impl Fn(u64) -> u64 + Sync) -> f64 {
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

    let without_path = dir.path().join("without-tls");
    let with_path = dir.path().join("with-tls");
    std::fs::create_dir_all(&without_path).expect("mkdir");
    std::fs::create_dir_all(&with_path).expect("mkdir");

    // SAFETY: fresh directories this process owns, per LMDB's contract.
    let without: Env<WithoutTls> = unsafe {
        EnvOpenOptions::new()
            .read_txn_without_tls()
            .map_size(2 * 1024 * 1024 * 1024)
            .max_dbs(4)
            .max_readers(512)
            .open(&without_path)
    }
    .expect("open without tls");

    // SAFETY: as above.
    let with: Env<WithTls> = unsafe {
        EnvOpenOptions::new()
            .map_size(2 * 1024 * 1024 * 1024)
            .max_dbs(4)
            .max_readers(512)
            .open(&with_path)
    }
    .expect("open with tls");

    let without_db = populate(&without);
    let with_db = populate(&with);

    println!("{KEYS} keys of {} bytes\n", VALUE.len());
    println!(
        "{:>8}  {:>18}  {:>18}  {:>18}",
        "threads", "NO_TLS txn+get", "TLS txn+get", "one txn, N gets"
    );

    for threads in [1usize, 2, 4, 8, 16] {
        let no_tls = measure(threads, |index| {
            let rtxn = without.read_txn().expect("read txn");
            let found = without_db
                .get(&rtxn, &key_for(index))
                .expect("get")
                .is_some();
            found as u64
        });

        let tls = measure(threads, |index| {
            let rtxn = with.read_txn().expect("read txn");
            let found = with_db.get(&rtxn, &key_for(index)).expect("get").is_some();
            found as u64
        });

        // The ceiling: the lookup with no transaction cost at all, which is what
        // LMDB's lock-free read path can actually do.
        let amortised = measure(threads, |index| {
            let rtxn = without.read_txn().expect("read txn");
            let mut found = 0;
            for i in 0..64 {
                found += without_db
                    .get(&rtxn, &key_for((index + i) % KEYS))
                    .expect("get")
                    .is_some() as u64;
            }
            found
        });

        println!("{threads:>8}  {no_tls:>18.0}  {tls:>18.0}  {amortised:>18.0}");
    }

    println!(
        "\nColumns one and two are one transaction per lookup. Column three shares one\n\
         across 64 lookups, so it shows what the lookups alone cost. A column that\n\
         falls as threads rise is contending on something."
    );
}
