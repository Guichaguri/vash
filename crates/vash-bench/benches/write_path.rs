//! Write-path benchmarks, driven against a real [`LmdbStore`].
//!
//! ```text
//! cargo bench -p vash-bench --bench write_path
//! ```
//!
//! Separate from `hot_path.rs`, which measures pure functions over bytes in
//! cache. Everything here opens an environment and goes through the shard's
//! writer thread and a real commit, because the costs it is built to see —
//! whether a write copies the value it displaces, how many times it descends
//! the B-tree — only exist there.
//!
//! It sits below the socket deliberately. The end-to-end `load` binary shares
//! its cores with the server it is driving, and at these magnitudes the
//! scheduler noise is larger than everything measured here put together: an
//! A/B over `load` could not separate a removed 64 KiB copy from run-to-run
//! drift. This can, because the only thing between the timer and the storage
//! engine is the writer queue.

use tempfile::TempDir;
use vash_core::{Key, Set, SetMode, TtlChange};
use vash_store::{LmdbStore, Store, StoreConfig, WriteConfig};

fn main() {
    divan::main();
}

/// A store on a temporary directory, with the sweeper pushed far enough out
/// that a maintenance pass does not land inside a sample.
///
/// **Ephemeral durability**, which is not the server's default. Under `Relaxed`
/// every commit reaches the file, and the resulting disk time — hundreds of
/// microseconds on Windows, with millisecond outliers — is an order of
/// magnitude larger than anything measured here and swamps it completely. The
/// question these benchmarks ask is how much CPU a write spends before it
/// commits, so the commit is made as cheap as the engine allows. It does mean
/// no number here is a throughput figure for a real deployment.
struct Fixture {
    store: Option<LmdbStore>,
    _dir: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self::sharded(1)
    }

    /// A store across `shards` environments.
    ///
    /// The default of one is what most of these benchmarks want, but it takes
    /// the single-shard fast path through every batch operation and so never
    /// reaches the grouping. The server's own default resolves to
    /// `min(cpus, 4)`, so four is the shape worth measuring.
    fn sharded(shards: usize) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = StoreConfig {
            shards,
            path: dir.path().join("db"),
            map_size: 1024 * 1024 * 1024,
            durability: vash_store::Durability::Ephemeral,
            write: WriteConfig {
                sweep_interval_ms: 3_600_000,
                ..WriteConfig::default()
            },
            ..StoreConfig::default()
        };
        Self {
            store: Some(LmdbStore::open(&config).expect("open")),
            _dir: dir,
        }
    }

    fn store(&self) -> &LmdbStore {
        self.store.as_ref().expect("open")
    }

    fn write(&self, key: &[u8], value: &[u8]) {
        self.store()
            .set(&set_of(key, value, SetMode::Set))
            .expect("write");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // The writer threads have to stop and LMDB has to release the
        // environment before the temp directory goes, or the cleanup fails on
        // Windows.
        if let Some(store) = self.store.take() {
            store.close();
        }
    }
}

fn set_of<'a>(key: &'a [u8], value: &'a [u8], mode: SetMode) -> Set<'a> {
    Set {
        key: Key::new(key).expect("valid key"),
        value,
        ttl: TtlChange::Set(300),
        mc_flags: 0,
        tags: Vec::new(),
        mode,
        return_previous: false,
    }
}

/// A guard that refuses the write, over a key that already holds a value.
///
/// The purest view of what a conditional write costs before it decides: nothing
/// is stored, so the commit is empty and what remains is the lookup and
/// whatever the guard needed from the record it found. Scaling with the value
/// size here means the value is being copied to answer a question about three
/// header fields.
#[divan::bench(args = [1024, 65536])]
fn add_refused_over_existing(bencher: divan::Bencher, value_len: usize) {
    let fixture = Fixture::new();
    let value = vec![b'x'; value_len];
    fixture.write(b"hot", &value);

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .store(&set_of(b"hot", divan::black_box(&value), SetMode::Add))
                .expect("refused, not failed"),
        )
    });
}

/// Overwriting a key that already holds a value of the same size — the shape a
/// cache actually sees, since a hot key is written far more than once.
#[divan::bench(args = [1024, 65536])]
fn overwrite_existing(bencher: divan::Bencher, value_len: usize) {
    let fixture = Fixture::new();
    let value = vec![b'x'; value_len];
    fixture.write(b"hot", &value);

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .store(&set_of(b"hot", divan::black_box(&value), SetMode::Set))
                .expect("stored"),
        )
    });
}

/// `SET k v GET` over a key that already holds a value: Redis's set-and-return.
///
/// The one write that legitimately wants the displaced value, and so the one
/// where it matters how many times that value is copied out of the map. The
/// `overwrite_existing` benchmark above is the same write without `GET`, which
/// wants no copy at all — read the two together.
#[divan::bench(args = [1024, 65536])]
fn set_get_over_existing(bencher: divan::Bencher, value_len: usize) {
    let fixture = Fixture::new();
    let value = vec![b'x'; value_len];
    fixture.write(b"hot", &value);

    bencher.bench(|| {
        let mut set = set_of(b"hot", divan::black_box(&value), SetMode::Set);
        set.return_previous = true;
        let written = fixture.store().store(&set).expect("stored");
        // Asserted, not assumed: a benchmark that quietly stopped returning the
        // value would be measuring the cheaper path and reporting a win.
        debug_assert!(written.previous.is_some());
        divan::black_box(written)
    });
}

/// An untagged single write through the batch entry point, which is where the
/// tag registration scan sits.
#[divan::bench]
fn set_many_untagged(bencher: divan::Bencher) {
    let fixture = Fixture::new();
    let value = vec![b'x'; 1024];

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .set_many(std::slice::from_ref(&set_of(b"hot", &value, SetMode::Set)))
                .expect("stored"),
        )
    });
}

/// Deleting a key that is there. The record is written in the setup closure,
/// which divan runs outside the timed region, so what is measured is the
/// delete alone.
#[divan::bench]
fn delete_hit(bencher: divan::Bencher) {
    let fixture = Fixture::new();
    let value = vec![b'x'; 1024];
    let mut next = 0u64;

    bencher
        .with_inputs(|| {
            next += 1;
            let key = format!("k{next}").into_bytes();
            fixture.write(&key, &value);
            key
        })
        .bench_local_values(|key| {
            divan::black_box(
                fixture
                    .store()
                    .delete(Key::new(&key).expect("valid key"))
                    .expect("deleted"),
            )
        });
}

/// Deleting a key that is not there, which never reaches a record at all.
/// Present as the floor the hit above is read against.
#[divan::bench]
fn delete_miss(bencher: divan::Bencher) {
    let fixture = Fixture::new();

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .delete(Key::new(b"absent").expect("valid key"))
                .expect("missed"),
        )
    });
}

// ---- read-modify-write, below the commit -----------------------------------
//
// These go straight to the engine rather than through the store, and recycle
// one write transaction across many operations rather than committing per call.
// Through the store a commit is tens of microseconds and swamps everything —
// which is what made the delete and tag-scan changes unmeasurable when this
// file only had store-level benchmarks. The commit is the part group commit
// exists to amortise, so taking it out leaves the per-operation work that does
// *not* amortise, which is the part worth counting.
//
// # What they were built to answer, and did
//
// Each of these operations reads its record and then used to read it again on
// the way to storing the replacement, and the obvious fix is to carry the first
// read forward. Measured here — including against `..._large`, a keyspace big
// enough for a descent to cost real memory traffic — that fix came to under a
// microsecond and never cleared the noise floor, while `rmw_plain_set`, the
// control on the path that always read once, moved as much in both directions.
//
// The reason is worth keeping even though the change was not: **the second
// lookup is always warm**. It is the same key in the same transaction
// microseconds after the first, so its pages are in cache by construction, it
// can never be a page fault, and the value's size does not enter into it
// because parsing is a pointer into the map. There is no working set that makes
// that redundant descent expensive, which is why no benchmark here could show
// one. Anyone tempted to try again should start by disproving that.

/// An engine to run write operations against directly.
struct Rmw {
    engine: Option<std::sync::Arc<vash_store::engine::LmdbEngine>>,
    _dir: TempDir,
}

impl Rmw {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = StoreConfig {
            path: dir.path().join("db"),
            map_size: 1024 * 1024 * 1024,
            durability: vash_store::Durability::Ephemeral,
            ..StoreConfig::default()
        };
        let engine = vash_store::engine::LmdbEngine::open(&config, 0, 1).expect("open");
        Self {
            engine: Some(std::sync::Arc::new(engine)),
            _dir: dir,
        }
    }

    fn engine(&self) -> &vash_store::engine::LmdbEngine {
        self.engine.as_ref().expect("open")
    }
}

impl Drop for Rmw {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.take()
            && let Ok(engine) = std::sync::Arc::try_unwrap(engine)
        {
            engine.close();
        }
    }
}

/// Operations per transaction.
///
/// A transaction that is never committed is not a cheap transaction: LMDB
/// tracks every dirty page in it, and a benchmark's worth of writes into one
/// degrades until the page bookkeeping is all that is being measured — 40µs an
/// operation, against roughly 2µs when the transaction is kept to a sane size.
/// Recycling it is also the shape the server actually runs in, since group
/// commit puts a batch in each transaction rather than everything.
const OPS_PER_TXN: u32 = 256;

/// `INCR` over a key that exists: read the counter, write it back.
#[divan::bench]
fn rmw_arithmetic(bencher: divan::Bencher) {
    let fixture = Rmw::new();
    let engine = fixture.engine();
    let mut wtxn = Some(engine.write_txn().expect("txn"));

    let key = Key::new(b"counter").expect("valid");
    let op = vash_core::Arithmetic::redis(
        key,
        vash_core::Delta::Int {
            delta: 1,
            lower: i64::MIN,
            upper: i64::MAX,
        },
    );

    let mut n = 0u32;
    bencher.bench_local(|| {
        n += 1;
        if n.is_multiple_of(OPS_PER_TXN) {
            wtxn.take().expect("open").commit().expect("commit");
            wtxn = Some(engine.write_txn().expect("txn"));
        }
        let txn = wtxn.as_mut().expect("open");
        divan::black_box(
            engine
                .apply_arithmetic(txn, divan::black_box(&op))
                .expect("applied"),
        )
    });
    wtxn.take().expect("open").commit().expect("commit");
}

/// `APPEND` onto a key that exists. The value is reset periodically, or it
/// grows without bound and the memcpy rather than the lookups is what is
/// being measured.
#[divan::bench]
fn rmw_append(bencher: divan::Bencher) {
    let fixture = Rmw::new();
    let engine = fixture.engine();
    let mut wtxn = Some(engine.write_txn().expect("txn"));

    let mut n = 0u32;
    bencher.bench_local(|| {
        n += 1;
        if n.is_multiple_of(OPS_PER_TXN) {
            wtxn.take().expect("open").commit().expect("commit");
            wtxn = Some(engine.write_txn().expect("txn"));
        }
        let txn = wtxn.as_mut().expect("open");
        if n.is_multiple_of(64) {
            engine.apply_delete(txn, b"log").expect("reset");
        }
        divan::black_box(
            engine
                .apply_append(txn, b"log", divan::black_box(b"x"))
                .expect("appended"),
        )
    });
    wtxn.take().expect("open").commit().expect("commit");
}

/// `TOUCH`: re-stamp the deadline, which rewrites the record.
#[divan::bench]
fn rmw_touch(bencher: divan::Bencher) {
    let fixture = Rmw::new();
    let engine = fixture.engine();
    let mut wtxn = Some(engine.write_txn().expect("txn"));

    {
        let txn = wtxn.as_mut().expect("open");
        let mut prepared = engine
            .prepare_set(
                &set_of(b"session", &vec![b'x'; 64], SetMode::Set),
                Vec::new(),
            )
            .expect("prepared");
        engine.apply_set(txn, &mut prepared).expect("seed");
    }

    let mut n = 0u32;
    bencher.bench_local(|| {
        n += 1;
        if n.is_multiple_of(OPS_PER_TXN) {
            wtxn.take().expect("open").commit().expect("commit");
            wtxn = Some(engine.write_txn().expect("txn"));
        }
        let txn = wtxn.as_mut().expect("open");
        divan::black_box(engine.apply_touch(txn, b"session", 300).expect("touched"))
    });
    wtxn.take().expect("open").commit().expect("commit");
}

/// Keys seeded into the large-tree fixtures below.
///
/// The point of the number is that the B-tree is several levels deep and far
/// larger than the CPU's caches, so a descent costs real memory traffic. On the
/// handful of keys the benchmarks above use, a lookup is nearly free and a
/// removed one is worth almost nothing — which flatters the code and tells you
/// nothing about a cache holding a working set.
const SEEDED_KEYS: u64 = 200_000;

/// The seeded value.
///
/// A decimal integer, because the counter benchmark increments these keys and a
/// value it cannot parse would fail before doing any work at all — a benchmark
/// measuring an error path and reporting it as an operation.
const SEEDED_VALUE: &[u8] = b"1000000";

fn seed(engine: &vash_store::engine::LmdbEngine, count: u64) {
    let value = SEEDED_VALUE.to_vec();
    let mut wtxn = Some(engine.write_txn().expect("txn"));
    for i in 0..count {
        if i.is_multiple_of(2000) {
            wtxn.take().expect("open").commit().expect("commit");
            wtxn = Some(engine.write_txn().expect("txn"));
        }
        let key = format!("user:{i:08}:profile").into_bytes();
        let mut prepared = engine
            .prepare_set(&set_of(&key, &value, SetMode::Set), Vec::new())
            .expect("prepared");
        engine
            .apply_set(wtxn.as_mut().expect("open"), &mut prepared)
            .expect("seeded");
    }
    wtxn.take().expect("open").commit().expect("commit");
}

/// A cheap deterministic walk over the seeded keyspace, so successive
/// operations land on unrelated pages instead of re-hitting one hot leaf.
fn scatter(n: u64) -> u64 {
    (n.wrapping_mul(2_654_435_761)) % SEEDED_KEYS
}

/// `INCR` across a large keyspace — the same operation as `rmw_arithmetic`, on a
/// tree deep enough for a descent to cost something.
#[divan::bench]
fn rmw_arithmetic_large(bencher: divan::Bencher) {
    let fixture = Rmw::new();
    let engine = fixture.engine();
    seed(engine, SEEDED_KEYS);

    let mut wtxn = Some(engine.write_txn().expect("txn"));
    let mut n = 0u64;
    bencher.bench_local(|| {
        n += 1;
        if n.is_multiple_of(OPS_PER_TXN as u64) {
            wtxn.take().expect("open").commit().expect("commit");
            wtxn = Some(engine.write_txn().expect("txn"));
        }
        let key = format!("user:{:08}:profile", scatter(n)).into_bytes();
        let op = vash_core::Arithmetic::redis(
            Key::new(&key).expect("valid"),
            vash_core::Delta::Int {
                delta: 1,
                lower: i64::MIN,
                upper: i64::MAX,
            },
        );
        divan::black_box(
            engine
                .apply_arithmetic(wtxn.as_mut().expect("open"), &op)
                .expect("applied")
                .expect("the key is there and its value is a number"),
        )
    });
    wtxn.take().expect("open").commit().expect("commit");
}

/// The control for the above: a plain `SET` across the same keyspace, on the
/// path that reads the record once and always did.
#[divan::bench]
fn rmw_plain_set_large(bencher: divan::Bencher) {
    let fixture = Rmw::new();
    let engine = fixture.engine();
    seed(engine, SEEDED_KEYS);

    let value = SEEDED_VALUE.to_vec();
    let mut wtxn = Some(engine.write_txn().expect("txn"));
    let mut n = 0u64;
    bencher.bench_local(|| {
        n += 1;
        if n.is_multiple_of(OPS_PER_TXN as u64) {
            wtxn.take().expect("open").commit().expect("commit");
            wtxn = Some(engine.write_txn().expect("txn"));
        }
        let key = format!("user:{:08}:profile", scatter(n)).into_bytes();
        let mut prepared = engine
            .prepare_set(&set_of(&key, &value, SetMode::Set), Vec::new())
            .expect("prepared");
        divan::black_box(
            engine
                .apply_set(wtxn.as_mut().expect("open"), &mut prepared)
                .expect("stored"),
        )
    });
    wtxn.take().expect("open").commit().expect("commit");
}

/// A plain `SET` over an existing key, which reads the record once and always
/// did. The control: nothing about this path changed, so a difference here
/// would mean the differences above are drift rather than the saved lookup.
#[divan::bench]
fn rmw_plain_set(bencher: divan::Bencher) {
    let fixture = Rmw::new();
    let engine = fixture.engine();
    let mut wtxn = Some(engine.write_txn().expect("txn"));
    let value = vec![b'x'; 64];

    let mut n = 0u32;
    bencher.bench_local(|| {
        n += 1;
        if n.is_multiple_of(OPS_PER_TXN) {
            wtxn.take().expect("open").commit().expect("commit");
            wtxn = Some(engine.write_txn().expect("txn"));
        }
        let txn = wtxn.as_mut().expect("open");
        let mut prepared = engine
            .prepare_set(&set_of(b"plain", &value, SetMode::Set), Vec::new())
            .expect("prepared");
        divan::black_box(engine.apply_set(txn, &mut prepared).expect("stored"))
    });
    wtxn.take().expect("open").commit().expect("commit");
}

// ---- single-key reads ------------------------------------------------------

/// The clock call itself, which is the whole of what a removed one saves and
/// therefore the ceiling on any of the reads below. Worth having as a number
/// rather than an assumption: it is a vDSO read on Linux and a rather different
/// thing on Windows, and the case for hoisting it rests on which.
#[divan::bench]
fn clock_now_ms(bencher: divan::Bencher) {
    let clock = vash_core::Clock::new();
    bencher.bench(|| divan::black_box(divan::black_box(&clock).now_ms()));
}

/// A single `GET` that hits — the most executed operation in the server.
#[divan::bench]
fn get_hit(bencher: divan::Bencher) {
    let fixture = Fixture::new();
    fixture.write(b"user:1234:profile", &vec![b'x'; 64]);
    let key = Key::new(b"user:1234:profile").expect("valid");

    bencher.bench(|| divan::black_box(fixture.store().get(divan::black_box(key)).expect("read")));
}

/// The same lookup through the header-only projection, so the value copy is out
/// of the way and a removed clock call is a larger share of what is left.
#[divan::bench]
fn deadline_hit(bencher: divan::Bencher) {
    let fixture = Fixture::new();
    fixture.write(b"user:1234:profile", &vec![b'x'; 64]);
    let key = Key::new(b"user:1234:profile").expect("valid");

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .deadline(divan::black_box(key))
                .expect("read"),
        )
    });
}

// ---- batch reads -----------------------------------------------------------

/// A multi-get over keys that are all present.
///
/// The values are deliberately small, because what is under examination is the
/// per-key overhead rather than the copy: at 64 bytes a clock call is a visible
/// fraction of what a key costs, and at 64 KiB it would be lost in the memcpy.
/// The one-key case is the control — nothing about a batch of one changes when
/// per-key work moves out of the loop, so a difference there would mean the
/// difference elsewhere is drift.
#[divan::bench(args = [1, 16, 128])]
fn get_many_hits(bencher: divan::Bencher, key_count: usize) {
    let fixture = Fixture::new();
    let value = vec![b'x'; 64];
    let names: Vec<Vec<u8>> = (0..key_count)
        .map(|i| format!("user:{i}:profile").into_bytes())
        .collect();
    for name in &names {
        fixture.write(name, &value);
    }
    let keys: Vec<Key<'_>> = names.iter().map(|n| Key::new(n).expect("valid")).collect();

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .get_many(divan::black_box(&keys))
                .expect("read"),
        )
    });
}

/// A multi-get across four shards, which is where the batch grouping lives: the
/// single-shard store short-circuits past it entirely.
#[divan::bench(args = [4, 16, 128])]
fn get_many_sharded(bencher: divan::Bencher, key_count: usize) {
    let fixture = Fixture::sharded(4);
    let value = vec![b'x'; 64];
    let names: Vec<Vec<u8>> = (0..key_count)
        .map(|i| format!("user:{i}:profile").into_bytes())
        .collect();
    for name in &names {
        fixture.write(name, &value);
    }
    let keys: Vec<Key<'_>> = names.iter().map(|n| Key::new(n).expect("valid")).collect();

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .get_many(divan::black_box(&keys))
                .expect("read"),
        )
    });
}

/// The same batch through the header-only projection, which copies no values at
/// all — so the per-key overhead is nearly all of what is left.
#[divan::bench(args = [1, 16, 128])]
fn deadlines_hits(bencher: divan::Bencher, key_count: usize) {
    let fixture = Fixture::new();
    let value = vec![b'x'; 64];
    let names: Vec<Vec<u8>> = (0..key_count)
        .map(|i| format!("user:{i}:profile").into_bytes())
        .collect();
    for name in &names {
        fixture.write(name, &value);
    }
    let keys: Vec<Key<'_>> = names.iter().map(|n| Key::new(n).expect("valid")).collect();

    bencher.bench(|| {
        divan::black_box(
            fixture
                .store()
                .deadlines(divan::black_box(&keys))
                .expect("read"),
        )
    });
}

// ---- the reply buffer across the blocking hop ------------------------------
//
// Every command goes through a `spawn_blocking` hop, because `inline_reads`
// defaults to off. The hop itself is unchanged and identical in both arms
// below, so it is measured on its own and left out of them: at roughly a
// microsecond with millisecond outliers it is far larger and far noisier than
// the buffer handling, and including it only buries what is under test.
//
// What changed is what crosses the hop. It used to be a buffer allocated per
// batch, whose bytes were then copied back into the connection's own; it is now
// the connection's buffer itself, moved each way.

/// The hop alone, carrying nothing. The floor both arms below sit on, and the
/// figure their difference should be read against.
#[divan::bench]
fn blocking_hop_roundtrip(bencher: divan::Bencher) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    bencher.bench(|| {
        rt.block_on(async {
            divan::black_box(
                tokio::task::spawn_blocking(|| divan::black_box(0u8))
                    .await
                    .expect("joined"),
            )
        })
    });
}

/// What the buffer handling used to cost: a fresh allocation per batch, and
/// every reply byte copied back out of it.
#[divan::bench(args = [64, 1024, 16384])]
fn reply_buffer_via_copy(bencher: divan::Bencher, reply_len: usize) {
    let payload = vec![b'r'; reply_len];
    let mut write_buf: Vec<u8> = Vec::with_capacity(64 * 1024);

    bencher.bench_local(|| {
        // The task's own buffer, as it was.
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(divan::black_box(&payload));
        write_buf.extend_from_slice(&out);
        let len = write_buf.len();
        write_buf.clear();
        divan::black_box(len)
    });
}

/// What it costs now: the connection's buffer moved out and back.
#[divan::bench(args = [64, 1024, 16384])]
fn reply_buffer_via_move(bencher: divan::Bencher, reply_len: usize) {
    let payload = vec![b'r'; reply_len];
    let mut write_buf: Vec<u8> = Vec::with_capacity(64 * 1024);

    bencher.bench_local(|| {
        let mut out = std::mem::take(&mut write_buf);
        out.extend_from_slice(divan::black_box(&payload));
        write_buf = out;
        let len = write_buf.len();
        write_buf.clear();
        divan::black_box(len)
    });
}

// ---- the per-command authentication gate -----------------------------------
//
// Both forms below are compiled from the same tree, so this is a straight
// comparison of the two expressions rather than of two builds — which is what
// makes it readable at all, since the difference is nanoseconds and a
// cross-build A/B could not resolve it.
//
// The gate runs once per command in every dialect. What it costs alone is
// almost beside the point: `current()` takes a read lock and clones an `Arc`,
// so it writes to two cachelines that every connection thread shares, and the
// cost that matters is the one that appears when they contend for them. That is
// why these are run at several thread counts.

/// Building an `AuthState` with enforcement off, which is the default
/// deployment and the one where the gate is reached on every command — the
/// `is_authenticated()` short-circuit in front of it never fires, because a
/// connection that is never asked to authenticate never becomes authenticated.
fn gate() -> vash_server::auth::AuthState {
    // An empty credential path and no environment secret: the table is empty
    // and nothing is enforced, which is the shape the default config produces.
    let config = vash_server::config::AuthConfig {
        required: false,
        ..vash_server::config::AuthConfig::default()
    };
    vash_server::auth::AuthState::new(
        vash_server::auth::Auth::load(&config).expect("an empty table always loads"),
        vash_server::auth::Limits {
            timeout: std::time::Duration::from_secs(30),
            max_attempts: 3,
            max_connections: 64,
        },
    )
}

/// What the gate used to cost: a lock acquisition and an `Arc` refcount round
/// trip, per command.
#[divan::bench(threads = [1, 4, 12])]
fn auth_gate_via_lock(bencher: divan::Bencher) {
    let state = gate();
    bencher.bench(|| divan::black_box(divan::black_box(&state).current().required()));
}

/// What it costs now.
#[divan::bench(threads = [1, 4, 12])]
fn auth_gate_via_atomic(bencher: divan::Bencher) {
    let state = gate();
    bencher.bench(|| divan::black_box(divan::black_box(&state).required()));
}
