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
        let dir = tempfile::tempdir().expect("temp dir");
        let config = StoreConfig {
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
