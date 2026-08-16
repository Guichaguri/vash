//! LMDB against libmdbx, paired, in one process.
//!
//! ```text
//! cargo run --release -p vash-store --features mdbx --example engine_ab
//! ```
//!
//! Phase 3 of `docs/mdbx-proposal.md`. The point of putting both engines behind
//! one config parameter was to make this possible: a host whose write numbers
//! swing by a factor of two between runs cannot answer "which engine is faster"
//! from two separate runs, so every scenario here alternates the engines inside
//! one process and reports the best of several repeats for each.
//!
//! **Reads come first on purpose.** Phase 0 measured mdbx ~25% behind LMDB on
//! transaction begin, which is what a read costs in this server. If that holds,
//! an engine worth a couple of percent on writes costs a quarter of the read
//! path, and the rest of the table stops mattering.
//!
//! **Run one section per process.** `reads` writes and then re-reads about a
//! gigabyte across its repeats, and the kernel is still flushing that when the
//! next section starts — which lands entirely on whichever engine is growing a
//! file rather than writing into a preallocated one. Measured: batched `lazy`
//! on Linux reads 0.16× for libmdbx when `reads` ran first in the same process
//! and 1.02× when it did not, and the second number is the true one. `all` runs
//! them in order and says so; it is for a quick look, not for numbers anyone
//! quotes.
//!
//! Environment overrides, matching `write_bench.rs`:
//!
//! - `VASH_BENCH_OPS` — operations per scenario (default 20,000)
//! - `VASH_BENCH_REPEATS` — repeats per scenario, best wins (default 3)
//! - `VASH_BENCH_SOAK_SECS` — seconds per engine in the soak (default 20)

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use vash_core::{Key, Set, SetMode, TtlChange};
use vash_store::{BackendKind, Durability, Store, StoreConfig, StoreHandle, WriteConfig};

const ENGINES: [BackendKind; 2] = [BackendKind::Lmdb, BackendKind::Mdbx];

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn ops() -> usize {
    env_usize("VASH_BENCH_OPS", 20_000)
}
fn repeats() -> usize {
    env_usize("VASH_BENCH_REPEATS", 3)
}

fn key_for(i: usize) -> Vec<u8> {
    format!("bench:key:{i:012}").into_bytes()
}

fn open(dir: &std::path::Path, backend: BackendKind, durability: Durability) -> StoreHandle {
    vash_store::open(&StoreConfig {
        path: dir.to_path_buf(),
        backend,
        durability,
        map_size: 512 * 1024 * 1024,
        shards: 1,
        write: WriteConfig {
            sync_interval_ms: 1000,
            ..WriteConfig::default()
        },
        ..StoreConfig::default()
    })
    .expect("open")
}

fn set_of<'a>(key: Key<'a>, value: &'a [u8]) -> Set<'a> {
    Set {
        key,
        value,
        ttl: TtlChange::Set(0),
        return_previous: false,
        mc_flags: 0,
        tags: Vec::new(),
        mode: SetMode::Set,
    }
}

/// Runs one scenario against both engines and prints them side by side.
///
/// Alternating inside a repeat rather than running all of one engine and then
/// all of the other: whatever else the machine is doing drifts over seconds, and
/// interleaving is what keeps that drift from landing on one column.
///
/// **Reports the median and the spread, not just the best.** Best-of-N hides
/// variance, and on this workload the variance is the story: LMDB's own batched
/// `lazy` figure spanned 2.3× across five runs of one machine, which is wider
/// than any difference between the engines. A ratio of medians whose ranges
/// overlap has not measured anything, and the table should show that rather
/// than imply a winner.
fn compare(name: &str, durability: Durability, run: impl Fn(&Arc<dyn Store>) -> f64) {
    let mut samples: [Vec<f64>; 2] = [Vec::new(), Vec::new()];
    for _ in 0..repeats() {
        for (slot, backend) in ENGINES.iter().enumerate() {
            let dir = tempfile::tempdir().expect("temp dir");
            let handle = open(&dir.path().join("db"), *backend, durability);
            let rate = run(handle.store());
            handle.close();
            samples[slot].push(rate);
        }
    }

    let stat = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        (v[v.len() / 2], v[0], v[v.len() - 1])
    };
    let (lmdb, lmdb_lo, lmdb_hi) = stat(&mut samples[0]);
    let (mdbx, mdbx_lo, mdbx_hi) = stat(&mut samples[1]);

    let ratio = if lmdb > 0.0 { mdbx / lmdb } else { 0.0 };
    // Overlapping ranges mean the repeats cannot tell the engines apart, whatever
    // the ratio of their medians happens to be.
    let overlap = lmdb_lo <= mdbx_hi && mdbx_lo <= lmdb_hi;
    println!(
        "{name:<34} {lmdb:>9.0} [{:>7.0}-{:>7.0}] {mdbx:>9.0} [{:>7.0}-{:>7.0}] {ratio:>6.2}x {}",
        lmdb_lo,
        lmdb_hi,
        mdbx_lo,
        mdbx_hi,
        if overlap { "(overlap)" } else { "" }
    );
    std::io::stdout().flush().ok();
}

fn populate(store: &Arc<dyn Store>, count: usize, value: &[u8]) {
    // In blocks, so the writer batches them the way a loaded server would
    // rather than paying a commit per key.
    for block in (0..count).step_by(256) {
        let keys: Vec<Vec<u8>> = (block..(block + 256).min(count)).map(key_for).collect();
        let sets: Vec<Set<'_>> = keys
            .iter()
            .map(|k| set_of(Key::new(k).expect("valid"), value))
            .collect();
        store.set_many(&sets).expect("populate");
    }
}

/// Reads from `threads` callers at once for a fixed wall-clock window.
///
/// **Duration-based, not a fixed op count.** Dividing a fixed total among
/// threads shrinks the measurement window as threads rise — at four threads it
/// came to 7 ms, which measures thread startup and noise, and produced an
/// apparent 14× scaling from one thread to four. This is the shape
/// `examples/txn_bench.rs` already uses, for the same reason.
///
/// The store is walked once before the clock starts. A freshly written database
/// has its value pages resident in one engine and not the other — LMDB sizes its
/// file to the whole `map_size` where mdbx grows to fit — so without this the
/// first scenario measures first-touch page faults and calls it a read.
fn read_at(store: &Arc<dyn Store>, keys: usize, threads: usize, inline: bool) -> f64 {
    for i in 0..keys {
        let raw = key_for(i);
        let mut sink = 0usize;
        store
            .get_with(Key::new(&raw).expect("valid"), &mut |value| {
                sink += value.data.len()
            })
            .expect("warm");
        std::hint::black_box(sink);
    }

    let done = AtomicU64::new(0);
    let window = Duration::from_secs(env_usize("VASH_BENCH_READ_SECS", 2) as u64);
    let started = Instant::now();
    std::thread::scope(|scope| {
        for t in 0..threads {
            let store = Arc::clone(store);
            let done = &done;
            scope.spawn(move || {
                // Reused across reads, exactly as a connection's write buffer
                // is: the point of `get_with` is to render out of the map once
                // rather than copy into a fresh allocation first.
                let mut buf: Vec<u8> = Vec::with_capacity(4096);
                let mut hits = 0u64;
                let mut cursor = t.wrapping_mul(0x9E37_79B9);
                while started.elapsed() < window {
                    for _ in 0..256 {
                        cursor = cursor.wrapping_add(0x9E37_79B9);
                        let raw = key_for(cursor % keys);
                        let key = Key::new(&raw).expect("valid");
                        if inline {
                            // **Consumes the bytes.** Taking only `data.len()`
                            // here never touches the value pages at all, which
                            // made `get_with` look like a read and measure a
                            // header parse.
                            buf.clear();
                            store
                                .get_with(key, &mut |value| buf.extend_from_slice(value.data))
                                .expect("get_with");
                            hits += (!buf.is_empty()) as u64;
                        } else {
                            hits += store.get(key).expect("get").is_some() as u64;
                        }
                    }
                }
                std::hint::black_box(&buf);
                done.fetch_add(hits, Ordering::Relaxed);
            });
        }
    });
    done.load(Ordering::Relaxed) as f64 / started.elapsed().as_secs_f64()
}

fn reads() {
    let keys = env_usize("VASH_BENCH_KEYS", 50_000);
    println!(
        "
== reads ({}s per engine per scenario, {keys} keys of 1 KiB, warmed first)
",
        env_usize("VASH_BENCH_READ_SECS", 2)
    );
    println!(
        "{:<34} {:>9} {:>17} {:>9} {:>17} {:>7}",
        "", "lmdb", "[min-max]", "mdbx", "[min-max]", "ratio"
    );

    let value = vec![b'v'; 1024];
    for threads in [1usize, 4, 8] {
        for (label, inline) in [("get", false), ("get_with", true)] {
            let value = value.clone();
            compare(
                &format!("{label}, {threads} thread(s)"),
                Durability::Lazy,
                move |store| {
                    populate(store, keys, &value);
                    read_at(store, keys, threads, inline)
                },
            );
        }
    }
}

fn writes() {
    println!("\n== writes ({} ops per scenario, 256 B values)\n", ops());
    println!(
        "{:<34} {:>9} {:>17} {:>9} {:>17} {:>7}",
        "", "lmdb", "[min-max]", "mdbx", "[min-max]", "ratio"
    );

    let value = vec![b'w'; 256];
    for durability in [Durability::Lazy, Durability::Relaxed, Durability::Durable] {
        // One write per call: what a closed-loop client does, and where the
        // commit cost is paid per operation rather than amortised.
        let v = value.clone();
        compare(
            &format!("set, one at a time, {durability:?}"),
            durability,
            move |store| {
                let started = Instant::now();
                for i in 0..ops() {
                    let raw = key_for(i);
                    store
                        .set(&set_of(Key::new(&raw).expect("valid"), &v))
                        .expect("set");
                }
                ops() as f64 / started.elapsed().as_secs_f64()
            },
        );

        // Blocks of 256: the pipelined shape, where group commit is doing its
        // job and the engine's own cost is a larger share of what is left.
        let v = value.clone();
        compare(
            &format!("set_many, blocks of 256, {durability:?}"),
            durability,
            move |store| {
                let started = Instant::now();
                for block in (0..ops()).step_by(256) {
                    let keys: Vec<Vec<u8>> =
                        (block..(block + 256).min(ops())).map(key_for).collect();
                    let sets: Vec<Set<'_>> = keys
                        .iter()
                        .map(|k| set_of(Key::new(k).expect("valid"), &v))
                        .collect();
                    store.set_many(&sets).expect("set_many");
                }
                ops() as f64 / started.elapsed().as_secs_f64()
            },
        );
    }
}

fn dir_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().expect("metadata");
            total += if meta.is_dir() {
                dir_bytes(&entry.path())
            } else {
                meta.len()
            };
        }
    }
    total
}

/// Sustained overfill of a small map.
///
/// **This is the measurement §8 called the honest reason to consider mdbx at
/// all**, and neither engine had been run under it. A cache above its soft
/// watermark is evicting continuously, so the free list is being churned as
/// fast as the device allows — which is where LMDB's never-shrinking file and
/// mdbx's LIFO recycling and compactification should actually differ. A
/// sixty-second throughput benchmark cannot see any of it.
fn soak() {
    let secs = env_usize("VASH_BENCH_SOAK_SECS", 20) as u64;
    println!("\n== soak: {secs}s of sustained overfill, 32 MiB map, 4 KiB values\n");
    println!(
        "{:<8} {:>10} {:>12} {:>12} {:>10} {:>12}",
        "backend", "ops/s", "utilisation", "used MiB", "file MiB", "evicted"
    );

    let value = vec![b's'; 4096];
    for backend in ENGINES {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("db");
        let handle = vash_store::open(&StoreConfig {
            path: path.clone(),
            backend,
            durability: Durability::Lazy,
            // Small on purpose: the point is to be permanently above the
            // watermarks, not to measure how long filling takes.
            map_size: 32 * 1024 * 1024,
            shards: 1,
            ..StoreConfig::default()
        })
        .expect("open");
        let store = handle.store();

        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut attempted = 0usize;
        // **Accepted, not attempted.** A store above the critical watermark
        // refuses writes rather than blocking — by design — and a refusal
        // returns fast, so counting attempts rewards whichever engine rejects
        // more. That inflated the first run of this soak.
        let mut accepted = 0usize;
        let started = Instant::now();
        while Instant::now() < deadline {
            for block in 0..64 {
                let keys: Vec<Vec<u8>> = (0..64)
                    .map(|i| key_for(attempted + block * 64 + i))
                    .collect();
                let sets: Vec<Set<'_>> = keys
                    .iter()
                    .map(|k| set_of(Key::new(k).expect("valid"), &value))
                    .collect();
                if store.set_many(&sets).is_ok() {
                    accepted += sets.len();
                }
            }
            attempted += 64 * 64;
        }
        let elapsed = started.elapsed();
        let written = accepted;

        let stats = store.stats().expect("stats");
        let file = dir_bytes(&path);
        println!(
            "{:<8} {:>10.0} {:>12.3} {:>12.1} {:>10.1} {:>12}",
            backend.as_str(),
            written as f64 / elapsed.as_secs_f64(),
            stats.utilisation,
            stats.used_bytes as f64 / (1024.0 * 1024.0),
            file as f64 / (1024.0 * 1024.0),
            stats.evicted
        );
        handle.close();
    }
}

fn main() {
    println!(
        "engine A/B — {} / {}, best of {} repeats, engines alternated within each",
        std::env::consts::OS,
        std::env::consts::ARCH,
        repeats()
    );

    let which = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    if which == "all" {
        eprintln!(
            "warning: sections contaminate each other in one process — `reads` leaves the 
             kernel flushing ~1 GiB, which costs the growing-file engine several times over.
             Run `reads`, `writes` and `soak` separately for numbers worth quoting."
        );
    }
    if which == "all" || which == "reads" {
        reads();
    }
    if which == "all" || which == "writes" {
        writes();
    }
    // Just the batched-`lazy` row, which is the only scenario the two engines
    // genuinely disagree on under Linux. Its own mode so a geometry sweep is
    // seconds rather than minutes.
    if which == "lazy" {
        println!(
            "
== batched lazy only ({} ops)
",
            ops()
        );
        println!(
            "{:<34} {:>9} {:>17} {:>9} {:>17} {:>7}",
            "", "lmdb", "[min-max]", "mdbx", "[min-max]", "ratio"
        );
        let value = vec![b'w'; 256];
        compare(
            "set_many, blocks of 256, Lazy",
            Durability::Lazy,
            move |store| {
                let started = Instant::now();
                for block in (0..ops()).step_by(256) {
                    let keys: Vec<Vec<u8>> =
                        (block..(block + 256).min(ops())).map(key_for).collect();
                    let sets: Vec<Set<'_>> = keys
                        .iter()
                        .map(|k| set_of(Key::new(k).expect("valid"), &value))
                        .collect();
                    store.set_many(&sets).expect("set_many");
                }
                ops() as f64 / started.elapsed().as_secs_f64()
            },
        );
    }
    if which == "all" || which == "soak" {
        soak();
    }

    println!(
        "\nRatios are mdbx relative to lmdb: above 1.00 means mdbx did more work.\n\
         An `(overlap)` row means the two engines' ranges intersect, so these
         repeats did not separate them. Read the caveats in docs/benchmarks.md
         before quoting any of this."
    );
}
