# A second storage engine: libmdbx beside LMDB

`vash-store/src/lib.rs` has claimed since M0 that "`libmdbx` … is a contained
swap if benchmarks demand it", and [plan.md](plan.md) §15 lists that swap as the
mitigation for its one High-impact write risk. This document says what the swap
would actually be: where the seam goes, how a build and a deployment choose an
engine, and — with the numbers this repo has already measured — what it is
honestly worth.

**The recommendation, up front:**

1. The seam is **not** the `Store` trait. It is a new `Backend` trait *below*
   `LmdbEngine`, covering the ~15 heed operations the crate actually uses.
   Everything above it — records, expiry index, tag registry, group commit,
   sweeper, evictor, sharding — is engine-neutral already and does not move.
2. Selection is **a cargo feature and a config parameter**, not one or the
   other. The feature decides whether the mdbx C code is compiled and linked;
   the parameter decides which engine a given deployment opens. Both are needed,
   for reasons in [§4](#4-choosing-between-them).
3. **Expect roughly 2–3% on end-to-end writes from the engine itself**, not the
   30% libmdbx claims, because [performance-proposals.md](performance-proposals.md)
   §2 already measured the storage engine at 8% of what a write costs. The
   reasons to do this anyway are the *features* — growable geometry, a
   cross-platform warm-up, `SAFE_NOSYNC`, and a handle for the slow-reader risk —
   and they are worth more than the throughput. [§8](#8-expected-performance)
   does that arithmetic in the open.
4. **The backend talks to libmdbx through raw FFI over a vendored `mdbx.c`, not
   through a wrapper crate.** This was the spike's finding, not the plan: every
   maintained Rust wrapper hardcodes a flag that costs 56× on reads. See
   [Phase 0 results](#phase-0-results) — read that before the rest of this
   document, because it contradicts parts of it.

---

> **Outcome, now that all four phases have run: LMDB stays the default and
> libmdbx ships as an option.** Measured on both platforms with five repeats:
> reads are parity on Linux and ~10% behind on Windows; libmdbx wins every
> batched write mode on Windows by 1.5-2.5x and wins the sustained-overfill soak
> on both platforms by ~1.2x -- but loses batched `lazy` writes on Linux by 17x,
> and that is the default durability on the deployment platform, so it decides.
> The likely cause is file growth, which a preallocating geometry may fix at the
> cost of the disk Phase 2 saved; that test has not been run.
> [Phase 3](#phase-3--measure-then-decide-the-default--done) has the reasoning,
> including two sets of numbers it published wrong and corrected, and
> [benchmarks.md](benchmarks.md#lmdb-against-libmdbx) the measurements. The rest
> of this document is kept as written, including the parts the measurements went
> on to contradict.

---

## Phase 0 results

Run on 2026-08-16, on one host: Windows 11 natively (i7-9750H, 6 cores /
12 threads, MSVC 2022) and Linux in `rust:1.92-alpine` on the same machine's
Rancher Desktop WSL2 VM — the same host family
[benchmarks.md](benchmarks.md#what-this-cannot-tell-you) warns about.
libmdbx v0.13.7, the version `signet-mdbx-sys` 0.1.0 vendors.

The spike talks to the C library through a hand-written FFI — about fifteen
`extern` declarations — rather than through a wrapper crate, because two of the
four questions are about platforms the candidate wrappers do not all support.
Same amalgamated `mdbx.c`, same build defines `signet-mdbx-sys` uses. It also
carries `heed` at the workspace's pinned version, so the LMDB baseline is
measured in the same binary, on the same host, in the same run.

**Verdict: not a stop, but the binding recommendation in [§10](#10-which-rust-binding--withdrawn)
is wrong and is corrected below.** Q2 passes outright. Q1 passes with a
one-line fix in a build script we would have to own. Q3 fails as packaged —
but in the wrappers, not in the engine.

### Q1 — does it build and link statically for musl? **Conditionally.**

Better than assumed in one way: **nothing runs `bindgen` at build time.**
`signet-mdbx-sys` ships pre-generated per-platform bindings and its only build
dependency is `cc`. The alpine image needs no `libclang`, and neither does the
`--all-features` clippy job. The C toolchain LMDB already requires is the whole
requirement.

Worse in another. **The unmodified `signet-libmdbx` crate does not link in
`rust:1.92-alpine`**, under the repo's exact release profile:

```
mdbx.c:(.text.scan4seq_resolver+0x12): undefined reference to `__cpu_indicator_init_local'
mdbx.c:(.text.scan4seq_resolver+0x19): undefined reference to `__cpu_model'
```

mdbx dispatches its free-list scan across SSE2/AVX2/AVX-512 at runtime using
GCC's `__builtin_cpu_supports`, and musl's libgcc does not carry the CPU-model
symbols that emits. glibc Linux and Windows/MSVC are unaffected — this is
musl-specific, and **musl is the release artifact**: the static binary is a CI
gate and the Docker image is `scratch`.

The fix is one define, `MDBX_HAVE_BUILTIN_CPU_SUPPORTS=0`, and it must come
from the build script that compiles `mdbx.c` — adding `-lgcc` downstream does
*not* work. With it, alpine produces a working `static-pie` binary carrying
both engines. What it costs is the runtime AVX2/AVX-512 choice for the
free-list scan; the compile-time path remains, which on x86-64 is SSE2.

> **A trap worth recording.** The first probe linked fine and was a false pass:
> it only called `Environment::builder()`, and a linker never pulls an archive
> member nothing references. A probe has to actually open an environment and
> commit a write. Any future spike here should do the same.

### Q2 — does a `get` borrow, or copy? **Borrows. Decisively.**

| | Windows | Linux |
|---|---:|---:|
| 8 B value | 67.0 ns/get | 45.7 ns/get |
| 4 MiB value | 68.8 ns/get | 55.2 ns/get |
| ratio | **1.03×** | **1.21×** |
| 4 MiB dirtied by the *same write txn* | 81.6 ns/get | 73.2 ns/get |

A memcpy of 4 MiB would be ~10³×. The address is also byte-identical across two
independent read transactions, which is only true of a pointer into the shared
mapping.

The last row is the case `apply.rs` actually hits — 28 of the crate's `get`
calls are inside the write transaction, and `signet-libmdbx`'s documentation
warns that read-write transactions "require a check to see if the data is
dirty". They do come back from a different address region, but they are still
borrowed, not copied. `get_with`, `ValueRef` and the value-as-suffix record
layout all survive the port intact.

### Q3 — what does `NOSTICKYTHREADS` cost? **56×, and every wrapper sets it.**

Windows, one transaction per lookup, 50,000 keys of 1 KiB, 2 s per cell:

| threads | mdbx sticky | mdbx NOSTICKY | lmdb TLS | lmdb NO_TLS | control |
|---:|---:|---:|---:|---:|---:|
| 1 | 732,683 | 186,591 | 778,236 | 347,230 | 985,877 |
| 2 | 1,611,723 | 282,866 | 1,814,145 | 110,009 | 2,689,924 |
| 4 | 2,570,953 | 67,739 | 3,131,601 | 98,132 | 4,570,461 |
| 8 | 3,351,612 | 64,597 | 4,626,970 | 95,451 | 6,014,480 |
| 16 | 3,931,970 | **69,737** | 5,234,201 | 100,135 | 5,524,307 |

The control shares one transaction across 64 lookups, so it is the lock-free
descent alone; it has to scale, and it does. **The LMDB columns reproduce this
repo's own documented figures almost exactly** — `env.rs:110` records 344k
falling to 91k without TLS and 948k rising to 5.3M with it, against 347k → 100k
and 778k → 5.23M here. That is the harness validating itself, and it is why the
mdbx columns can be trusted.

Two findings:

1. **`NOSTICKYTHREADS` collapses reads**: 56× below sticky mdbx at 16 threads,
   and 75× below LMDB. It is worse in absolute terms than LMDB's own no-TLS
   mode. This is the failure this document predicted, and it is not a default
   to override —

   | Crate | Where | Configurable? |
   |---|---|---|
   | `signet-libmdbx` 0.8.3 | `src/flags.rs`, `make_flags()` | No — `flags \|= ffi::MDBX_NOTLS;` unconditional |
   | `reth-libmdbx` | `src/flags.rs`, `make_flags()` | No — `flags \|= ffi::MDBX_NOSTICKYTHREADS;` unconditional |
   | `libmdbx` 0.6.6 | `src/database.rs:162` | No — `flags \|= ffi::MDBX_NOTLS;` unconditional |

   All three descend from the same lineage and set it for the same reason: reth
   wants `Send` transactions. vash does not — reads begin and end a transaction
   inside one blocking-pool call and never cross a thread, which is exactly the
   argument `env.rs:110` already makes about heed's `read_txn_without_tls()`.

2. **Sticky mdbx is still ~25% below LMDB** on this path: 3.93M against 5.23M at
   16 threads, and 733k against 778k at one. Transaction begin is what one
   read costs in this server, so this is representative, and it contradicts the
   "predict parity on reads" in [§8](#8-expected-performance).

**The Linux half of this question is unanswered.** In the container the control
column does not scale either — 1.27M at one thread, 1.18M at sixteen — so the
host is saturated and every number in that table is unreadable. All four
engines measured the same, which is the signature of a ceiling that is not
theirs. Answering Q3 on Linux needs a quiet multi-core box.

### Q4 — does `mdbx_env_warmup` work, with the lock? **Yes, with a prerequisite.**

| | Windows | Linux |
|---|---|---|
| `force` | OK, 43–58 ms | OK, 8.8–10.3 ms |
| `force\|lock`, as-is | **FAILED** — winerror 1453, working-set quota | **FAILED** — `ENOMEM`, `RLIMIT_MEMLOCK` 64 MiB |
| `force\|lock`, after raising the limit | **OK**, 7.5 ms | **OK**, 11.5 ms |

Neither failure is a missing capability; both are process limits. On Windows
`VirtualLock` refuses past the process working-set maximum until
`SetProcessWorkingSetSize` raises it — one `kernel32` call, and then it
succeeds. On Linux it is `RLIMIT_MEMLOCK`, which is the same limit
`prefault.rs` already lives under.

So the [§7](#7-what-libmdbx-actually-gives-us) claim holds with a qualifier:
warming is genuinely cross-platform, and *pinning* is achievable everywhere
tested but the store has to raise the limit itself on Windows and document it
on Linux. 11.5 ms to pin ~50 MiB also confirms it locks the resident region
rather than the geometry's upper bound, which matters when that bound is 4 GiB.

**macOS is unanswered** — no machine. It stays open.

### What this changes

- **[§10](#10-which-rust-binding--withdrawn) is withdrawn.** The mdbx backend should be
  raw FFI over a vendored `mdbx.c`, sized to the `Backend` trait — which the
  spike shows is about fifteen `extern` declarations. Three reasons, all
  measured above: every wrapper hardcodes a flag that costs 56×; the musl fix
  has to live in the build script that compiles the C; and a wrapper is a
  second copy of the abstraction `Backend` already is. It also settles the
  licence question, since only the Apache-2.0 C is vendored and no MPL-2.0
  crate enters the graph.
- **`signet-libmdbx` could not have been the recommendation anyway**:
  `signet-mdbx-sys` 0.1.0 ships an *empty* `bindings_windows.rs` and links no
  Windows system libraries. It fails with 355 errors on Windows — a platform
  two of this document's own arguments are about. The C library itself builds
  and runs there under MSVC without complaint.
- **[§8](#8-expected-performance)'s read prediction was wrong** in the one
  direction that matters. Reads may regress ~25% on transaction begin, and
  Phase 3 has to measure reads, not assume them.
- **[§9](#9-what-it-costs) gets cheaper and more precise**: no `libclang`
  anywhere, and the musl risk is real but closed, with a known fix and a known
  cost.

The spike lives outside the tree, as Phase 0 intended. Whether to land it as
`crates/vash-store/examples/mdbx_bench.rs` — it is `txn_bench` with two more
engines — is a Phase 1 decision.

---

## 1. The `Store` trait is the wrong seam

The trait is a real seam — `crates/vash-server/tests/store_seam.rs` drives the
whole stack over `MemoryStore`, and [m10.md](m10.md) Phase 3 is the work that
made that true. It is the wrong place for *this*.

`Store` has 27 methods, and behind them sits everything that makes vash a cache
rather than a key-value wrapper:

| Module | Lines | Engine-specific? |
|---|---:|---|
| `apply.rs` | 907 | Only the `get`/`put`/`delete` calls |
| `writer.rs` | 749 | No — a thread, a queue, group commit, batch sealing |
| `tags.rs` | 565 | Registry is RAM; only its persistence touches the engine |
| `prefault.rs` | 438 | Platform, not engine |
| `reclaim.rs` | 389 | Only the range scan and the deletes |
| `expiry.rs` | 359 | Only the forward scan and the deletes |
| `shard.rs`, `queue.rs`, `config.rs`, `listing.rs`, `read.rs`, `readers.rs`, `schema.rs` | 1,401 | Almost none |

The scale of the duplication has a measured floor: `memory::MemoryStore`, the
in-RAM fake with no durability, no sweeper, no evictor, no group commit and no
persistence of any kind, is **597 lines**. A second *real* implementation starts
there and adds all of the above.

A second `impl Store` on top of mdbx would duplicate all of it: a second
sweeper, a second evictor, a second writer thread, a second copy of the record
format and the CAS block reservation. Two implementations of the eviction
watermarks is two places for the watermarks to drift, and
`crates/vash-store/tests/store.rs` (2,197 lines) would be asserting the same
invariants against two independent implementations of them. That is not a
contained swap; it is a fork.

**What is actually LMDB-specific is small.** Counting call sites across the
crate:

| Operation | Sites |
|---|---:|
| `db.get(txn, key)` | 28 |
| `db.put(txn, key, value)` | 17 |
| `db.delete(txn, key)` | 13 |
| `db.stat(txn)` | 8 |
| `env.read_txn()` / `write_txn()` / `commit()` | 15 |
| `db.clear(txn)` | 5 |
| `db.iter(txn)` / `db.range(txn, bounds)` | 5 |
| `env.info()` / `env.stat()` | 5 |
| `EnvOpenOptions`, `create_database`, `force_sync`, `prepare_for_closing` | 5 |

Ten distinct shapes, about a hundred call sites, no cursor positioning beyond a
range seek, no `DUPSORT` (deliberately — see
[storage.md](storage.md#the-tag-index)), no nested transactions, no custom
comparators. That is the whole contract, and it is one both engines satisfy
natively.

## 2. The seam: a `Backend` trait under the engine

```rust
// crates/vash-store/src/backend/mod.rs
pub trait Backend: Send + Sync + Sized + 'static {
    /// A named sub-database handle. `Copy` because `LmdbEngine` holds six of
    /// them by value and passes them around freely.
    type Db: Copy + Send + Sync + 'static;
    type RoTxn<'e>: ReadTxn<Self> where Self: 'e;
    type RwTxn<'e>: WriteTxn<Self> where Self: 'e;

    /// Opens one environment. Takes the whole `StoreConfig` because the
    /// mapping from `map_size`/`durability`/`max_readers` onto engine flags is
    /// the backend's business, not the caller's — see §6.
    fn open(config: &StoreConfig, path: &Path) -> Result<Self>;

    fn create_db(&self, txn: &mut Self::RwTxn<'_>, name: &str) -> Result<Self::Db>;
    fn read_txn(&self) -> Result<Self::RoTxn<'_>>;
    fn write_txn(&self) -> Result<Self::RwTxn<'_>>;
    fn sync(&self) -> Result<()>;

    /// `map_size`, `page_size`, `readers_in_use`, `max_readers`, high-water
    /// bytes. One call instead of heed's `info()`/`stat()` split, because the
    /// two engines divide the same numbers between their calls differently.
    fn info(&self) -> EnvInfo;

    /// Warms and optionally pins the map, reporting what actually happened.
    /// The Linux-only `madvise` dance in `prefault.rs` for LMDB; `mdbx_env_warmup`
    /// for mdbx — which is why this is a backend method and not a free function.
    fn warm(&self, config: &StoreConfig) -> Warmed;

    /// LMDB only *schedules* a release on drop and refuses to reopen a path
    /// still registered in-process, so it needs `prepare_for_closing().wait()`.
    /// mdbx's drop is synchronous, so it takes the default.
    fn close(self) {}
}

pub trait ReadTxn<B: Backend> {
    /// **Borrowed for the transaction's life, never copied.** This is what
    /// `Store::get_with` and the whole zero-copy record layout rest on
    /// (storage.md, "the value is a suffix").
    fn get<'t>(&'t self, db: B::Db, key: &[u8]) -> Result<Option<&'t [u8]>>;
    fn stat(&self, db: B::Db) -> Result<DbStat>;
    fn range<'t>(
        &'t self,
        db: B::Db,
        bounds: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> Result<impl Iterator<Item = Result<(&'t [u8], &'t [u8])>> + 't>;
}

pub trait WriteTxn<B: Backend>: ReadTxn<B> {
    fn put(&mut self, db: B::Db, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&mut self, db: B::Db, key: &[u8]) -> Result<bool>;
    fn clear(&mut self, db: B::Db) -> Result<()>;
    fn commit(self) -> Result<()>;
}
```

`LmdbEngine` becomes `Engine<B: Backend>`, and the generic travels up through
`Shards<B>` and `LmdbStore` → `VashStore<B>`. (Not the writer: it owns a queue
and a thread handle, and the engine only exists inside the thread it spawned, so
only `Writer::spawn` is generic — a detail Phase 1 found rather than planned.)
**It stops there**, because `ServerState` holds `Arc<dyn Store>`: the server, the
cluster, the executors, the protocol layer and every server test see no change
at all.
That containment is the reason the m10 seam was worth building even though this
document says it is the wrong seam for the engine — it is the right seam for
keeping the engine swap out of the server.

Static dispatch rather than `dyn Backend`: this trait is on the innermost path
(a `get` is one virtual call per B-tree lookup, where `Store::get` is one per
request), and the GATs are unavoidable anyway — a transaction borrows its
environment and a value borrows its transaction, which no object-safe trait can
express.

**The borrow discipline the trait needs is one the code already follows.** heed
takes `&RwTxn` for reads and `&mut RwTxn` for writes, so `reclaim.rs:172` and
`expiry.rs:176` already collect a batch out of an iterator, drop it, and only
then delete. Nothing has to be restructured to satisfy `ReadTxn`/`WriteTxn`;
the trait is a description of what the code does today.

### What else moves

- **`StoreError::Lmdb(heed::Error)` → `StoreError::Engine(String)`.**
  `poisons_transaction()` and `is_capacity()` keep working verbatim, and
  `clone_shallow` gets *simpler*: `heed::Error` is not `Clone`, which is why
  fanning one failure out to a batch currently downgrades it to
  `Corrupt(String)`. A `String` payload is `Clone`, so the downgrade goes away.
- **`MapFull` mapping moves into each backend.** LMDB raises `MDB_MAP_FULL`;
  mdbx raises `MDBX_MAP_FULL` at the geometry's upper bound and
  `MDBX_TXN_FULL`/`ENOSPC` in cases LMDB does not have. Each backend maps its
  own onto `StoreError::CapacityFull`, and the evictor upstream never learns
  which engine it is running on.
- **`schema.rs` does not change.** Same six sub-databases, same key encodings,
  same `SCHEMA_VERSION`. The on-disk *file* format differs between engines; the
  *logical* schema does not, and keeping it identical is what lets one test
  suite assert one set of invariants.

## 3. Where the mdbx implementation lives

```
crates/vash-store/src/backend/
    mod.rs          the traits above, EnvInfo, DbStat, Warmed
    lmdb.rs         heed, moved out of env.rs — no behaviour change
    mdbx.rs         new, behind #[cfg(feature = "mdbx")]
    mdbx/ffi.rs     ~15 extern declarations, hand-written
    mdbx/libmdbx/   vendored mdbx.c + mdbx.h, Apache-2.0
```

`env.rs` keeps what it is really about — schema version checks, shard-identity
checks, the tag registry load, CAS resumption — and loses only the
`EnvOpenOptions` block and the flag mapping, which become `LmdbBackend::open`.

The vendored C and the hand-written externs are [Phase 0](#phase-0-results)'s
conclusion rather than the original plan; `mdbx.rs` is where the `Backend` impl
sits either way. The build script beside it owns two things no wrapper crate
does correctly for us: the musl define, and *not* setting `NOSTICKYTHREADS`.

## 4. Choosing between them

**A cargo feature `mdbx`, additive and off by default**, plus **a config
parameter `store.backend = "lmdb" | "mdbx"`**, defaulting to `lmdb`.

Neither alone does the job:

- *Feature only.* Cargo features are additive, so `lmdb`/`mdbx` as mutually
  exclusive features breaks under unification the first time two crates in the
  workspace disagree. Worse, it makes an A/B impossible: the benchmark that
  decides this question wants both engines in one process, on one host, in one
  run — that is the only way to compare them without the ±2× run-to-run swing
  [benchmarks.md](benchmarks.md#what-this-cannot-tell-you) documents.
- *Parameter only.* Then every build compiles and links the mdbx C library,
  every CI job needs its toolchain, and the scratch-image static binary carries
  an engine most deployments will not use. The C dependency should be opt-in
  until the measurements say otherwise.

So:

```toml
# crates/vash-store/Cargo.toml
[features]
default = []
# The second storage engine. Off by default: it compiles and links a second C
# library, and until the benchmarks say it wins there is no reason to make
# every build pay for it.
mdbx = ["dep:signet-libmdbx"]
```

```toml
[store]
# lmdb (default) or mdbx. The two write different file formats and cannot share
# a directory — see §5. Requires a build with the `mdbx` feature; a binary
# without it refuses to start rather than silently falling back.
backend = "lmdb"
```

Construction is the one place that names both, and it is four lines in
`Server::bind`:

```rust
let store: Arc<dyn Store> = match config.store.backend {
    Backend::Lmdb => Arc::new(VashStore::<LmdbBackend>::open(&cfg)?),
    #[cfg(feature = "mdbx")]
    Backend::Mdbx => Arc::new(VashStore::<MdbxBackend>::open(&cfg)?),
    #[cfg(not(feature = "mdbx"))]
    Backend::Mdbx => anyhow::bail!(
        "store.backend = \"mdbx\" needs a build with the `mdbx` feature"
    ),
};
```

**Refusing rather than falling back** is the same rule the store already applies
to a shard-count mismatch: a silent substitution here means an operator who
asked for one engine gets another and reads a benchmark as if it were the
engine they configured.

`Server::lmdb: Option<Arc<LmdbStore>>` — the one field that names the
implementation, kept for `close()` — becomes an enum or a boxed
`FnOnce()`-style closer. It exists only so shutdown can block on LMDB releasing
its handles, and m10 already decided it stays off the trait.

## 5. The two engines cannot share a directory

mdbx is a fork of LMDB's *design*, not its file format. An LMDB `data.mdb`
cannot be opened by mdbx and the reverse is worse — mdbx writes `mdbx.dat` and
`mdbx.lck` beside a `data.mdb` LMDB would still claim.

Since the file names differ, detection is free: on open, if the directory
contains the other engine's data file, refuse with the same wording the schema
check uses —

> database at `data/shard-0` was written by the lmdb backend; this server is
> configured for mdbx. The two file formats are not interchangeable; wipe the
> directory or change `store.backend`.

For a cache that is the right answer, and it is the answer
[storage.md](storage.md#changing-the-format) already gives for a format change:
the data is reconstructible by definition, so "refuse to open, let the operator
wipe" beats a migration that has to be correct on a format nobody runs.

Switching engines therefore **empties the cache**. That belongs in the config
comment and the operations doc, not in a footnote.

## 6. Configuration mapping

Everything in `StoreConfig` keeps its name and meaning. What changes is what
each backend does with it.

| Setting | LMDB | mdbx |
|---|---|---|
| `map_size` | `mdb_env_set_mapsize` — a fixed reservation; the file is sparse until data arrives | `size_upper` in `mdbx_env_set_geometry`, with `size_lower = MIN_MAP_SIZE`, `size_now` at the current file size, a growth step and a shrink threshold. **A ceiling rather than a reservation.** |
| `durability = durable` | no flags | `MDBX_SYNC_DURABLE` |
| `durability = relaxed` | `MDB_NOMETASYNC` | `MDBX_NOMETASYNC` |
| `durability = lazy` | `MDB_NOSYNC`, and never `MDB_WRITEMAP` | `MDBX_SAFE_NOSYNC` — **and `WRITE_MAP` is permitted with it**, see §7 |
| `write_map` | `MDB_WRITEMAP`; fails on Windows with OS error 6 | `MDBX_WRITEMAP`, supported on Windows |
| `max_readers` | `mdb_env_set_maxreaders`, thread-local slots | `mdbx_env_set_maxreaders`; **must not set `MDBX_NOTLS`** — see below |
| `prefault` / `lock_map` | `prefault.rs`: read the file, `madvise`, `mlock`; Linux only | `mdbx_env_warmup` with `MDBX_warmup_force` \| `MDBX_warmup_lock`, cross-platform |
| `shards` | one writer per environment | **unchanged — mdbx is also single-writer** |
| `bucket_granularity_ms`, `max_tags`, eviction watermarks, `write.*` | untouched | untouched |

Three of these deserve more than a table row.

**Reader slots are load-bearing.** `env.rs:110` records why: thread-local slots
measured 948k lookups/s rising to 5.3M on sixteen threads, against 344k falling
to 91k without them, because the shared reader table sits behind a process-wide
mutex. mdbx has the same flag under the name `MDBX_NOTLS`, and several Rust
wrappers set it by default so that a read transaction can be `Send`. **A port
that inherits that default silently gives back the single biggest read win in
this project's history.** It is the first thing the spike in
[§11](#11-staging) must check, and it belongs in a test, not a comment.

**Geometry changes what `map_size` costs.** Today the map is a lazy reservation
verified on both platforms: a 16 GiB map produces a 16 KiB file, so a generous
value is free. Under mdbx's geometry the upper bound is still free, but the file
genuinely grows toward it — and, unlike LMDB, can shrink back. The eviction
watermarks are fractions of `map_size`, so their arithmetic is unchanged and
they now bound *disk* rather than a reservation, which is what an operator
reading `store.map_size_mb` probably assumed all along.

**`MIN_MAP_SIZE` keeps its floor** even though its stated reason is an LMDB
free-list wedge that mdbx handles better. 16 MiB costs nothing and a per-backend
minimum is a second number to keep true.

## 7. What libmdbx actually gives us

Each of these is listed because it lands on something this repo has already
written down as a problem — not because it appears in a feature comparison.

**1. `MDBX_SAFE_NOSYNC` removes a caveat `lazy` currently carries.**
`config.rs` spells out the condition on the default durability mode: LMDB
preserves integrity under `MDB_NOSYNC` only *"if the filesystem preserves write
order and the `MDB_WRITEMAP` flag is not used"*, so `lazy` refuses `WRITE_MAP`
and an operator on a reordering filesystem gets the `ephemeral` risk without the
label. mdbx's `SAFE_NOSYNC` is documented as avoiding corruption after a system
crash outright, and is compatible with `WRITEMAP`. That is a smaller footgun and
one fewer condition in the durability table.

**2. It makes the Windows `write_map` measurement usable.** `env.rs` records
that natively on Windows, `lazy` + `WRITE_MAP` measured 1.08× closed loop,
1.26× pipelined and 1.17× mixed, winning all fifteen paired runs — and
`storage.md` records that `mdb_env_open` fails there with OS error 6. mdbx
supports writemap on Windows and permits it under `SAFE_NOSYNC`. The gain is
already measured; the engine is what blocks collecting it.

**3. `mdbx_env_warmup` could take `resident_mode` off Linux.** *(Measured — see
[Q4](#q4--does-mdbx_env_warmup-work-with-the-lock-yes-with-a-prerequisite).
Warming works on both platforms tested; pinning needs the store to raise a
process limit first.)*
`prefault.rs` is Linux-only because Linux is the one platform exposing both the
mapping table and an advice meaning "prefault this", and `Store::map_locked`
exists precisely so the server can *check* rather than assume. mdbx ships a
warm-up call with force and lock flags across platforms. Given that
`resident_mode` is what took GET from 17,443 to 173,659 closed loop and 221,380
to 781,427 pipelined, **making it available on Windows and macOS is the largest
single number in this proposal** — and it comes from a feature, not from mdbx
being a faster B-tree.

**4. Handle-Slow-Readers turns a monitored risk into a handled one.**
[plan.md](plan.md) §15's one High-impact storage risk is a read transaction left
open pinning a snapshot, stopping page reuse and growing the file without bound.
Today the mitigation is observability: `oldest_reader_age_ms`, a metric, and an
alert. mdbx offers a Handle-Slow-Readers callback the writer invokes when it
cannot find reusable pages, letting the store abort the offending reader instead
of watching the file grow. That is the difference between a documented risk and
a closed one.

**5. LIFO reclaim and continuous compactification address the free-list.**
`engine.rs:117` documents the workaround for LMDB never returning freed pages
nor lowering its high-water mark: `used_bytes` is summed from per-sub-database
page counts because the high-water measure only ever rises, and using it meant
pressure could never fall and the evictor ran until the cache was empty. mdbx
recycles the garbage list LIFO — reusing pages still hot in the write-back cache
— and compacts continuously. This is where mdbx's own "up to 30% faster in CRUD"
claim comes from, and it is also where an overwrite-heavy cache is most likely
to see it.

**6. Longer keys, if we ever want them.** mdbx allows ~2022-byte keys at a 4 KiB
page against LMDB's 511. `storage.md` records that the tag index hashes the user
key *because* of the 511-byte cap, accepting that a hash collision leaves a
record unreclaimed. Not a reason to port — but if the port happens, that
compromise becomes optional.

**7. A configurable page size.** The expiry index is deliberately bucketed to
cluster entries into shared B-tree pages and cut copy-on-write amplification
(`bucket_granularity_ms`). mdbx lets the page size be chosen at creation, which
is a second dial on the same trade — larger pages cluster more index entries but
copy more per dirty page. Listed as something to measure, not as a claim.

## 8. Expected performance

### Reads: predicted parity, measured ~25% worse

> **Superseded by [Q3](#q3--what-does-nostickythreads-cost-56-and-every-wrapper-sets-it).**
> The reasoning below is sound and the conclusion was wrong. On Windows, mdbx
> with sticky threads does 3.93M transaction-begin-plus-get per second at 16
> threads against LMDB's 5.23M — a 25% regression on the path a vash read
> actually takes, since a read is one transaction. Left in place because the
> prediction and its failure are both worth keeping.

The read path is a B-tree descent into a resident memory map, a header parse and
a subslice. Both engines do the same work with the same page layout, and
`performance-proposals.md` §3 established that vash's read numbers come from
`resident_mode` and from `get_with` rendering out of the map — not from the
engine. **Predict no measurable change on Linux**, and treat any large
difference in either direction as a bug in the port (most likely `NOTLS`, see
§6).

What the prediction missed is that a read is not only a descent: it is a
transaction begin, and mdbx's does more work than LMDB's. The descent itself
was never in question — Q2 shows the value comes back borrowed either way.

The exception is item 3 above: on Windows and macOS, where `resident_mode`
cannot currently engage at all, the ceiling is whatever the platform's page
faults cost. There the change is potentially an order of magnitude — but it is
`mdbx_env_warmup` doing it, not the B-tree.

### Writes: 2–3% end to end, and here is the arithmetic

`performance-proposals.md` §2 decomposed a write and §§4–5, 8–9 acted on it. The
result, in its own words: the storage engine is **8% of what a write costs with
syncing off; the remaining 92% is the request path**. Take mdbx's own headline
claim at face value — 30% faster CRUD — and apply it to the part it can touch:

```
0.08 × 0.30 = 2.4% end to end
```

At the current `lazy` SET numbers (24,902 closed loop, 109,839 pipelined) that
is roughly 600 and 2,600 ops/s. **The host's own run-to-run swing on writes is a
factor of two.** A 2.4% effect is not measurable on that host at all; it needs
paired runs on quiet hardware, which is exactly the A/B-in-one-binary the
selection design in §4 is built for.

The picture changes with syncing on. Under `relaxed`, commit costs 0.43 ms per
record against `lazy`'s 0.033 ms — 92% of it the sync — so the engine's share of
a durable write is far larger than 8%, and LIFO reclaim's premise (reuse pages
still in the write-back cache) is aimed precisely at that. **Predict single
digits under `lazy`, plausibly 10–20% under `relaxed` and `durable`.** The
deployments that chose durability are the ones with something to gain.

### Sustained behaviour: the honest reason to do this

The measurable case for mdbx is not a throughput row. It is that this codebase
has spent real effort on LMDB free-list behaviour — the 16 MiB minimum below
which a map wedges permanently after everything is deleted; `used_bytes` summed
from page counts because the high-water mark only rises; an evictor that once
ran until the cache was empty because pressure could not fall. Every one of
those is the free list, and every one is what mdbx's GC rework and
compactification target.

That shows up over days of overwrite churn, not in a 60-second benchmark, and
neither `benchmarks.md` nor the bench suite currently measures it. **A soak test
— sustained overfill for hours, plotting `used_bytes`, `utilisation`, `evicted`
and file size — is the measurement that would actually decide this**, and it is
worth building whether or not mdbx ever ships, because it is the one shape of
failure the current suite is blind to.

### What will not change

Sharding stays exactly as it is. mdbx is single-writer per environment like
LMDB, so `shards` remains the ceiling on concurrent writers, and the measured
result that 2 is the best count on a four-core box is a queue-and-batching
property, not an engine property. It should be re-measured under mdbx, and it
should come out the same.

## 9. What it costs

**Build.** *(Measured — [Q1](#q1--does-it-build-and-link-statically-for-musl-conditionally).)*
mdbx is vendored C, and the `musl-dev` the alpine stage already installs for
LMDB is the whole requirement: **no `bindgen`, no `libclang`**, in the build or
in the `--all-features` clippy job. CI runs
`cargo clippy --all-targets --all-features`, so an additive `mdbx` feature is
built by CI the moment it exists — but it needs no new toolchain to do it.

**The musl static build is a release gate**, and it is where the one real build
problem lives: mdbx's runtime SIMD dispatch does not link against musl's
libgcc. Closed, at the cost of one define in the build script and the runtime
AVX2/AVX-512 choice for the free-list scan — which is a cost worth restating,
because the free-list is [§7](#7-what-libmdbx-actually-gives-us) item 5, the
main performance reason to want mdbx at all. Whether the SSE2 fallback gives
that back is a Phase 3 measurement.

**Licensing.** libmdbx the C library is Apache-2.0 (relicensed in 2024), which
is compatible with this workspace's `MIT OR Apache-2.0`. Vendoring it directly
means that is the only licence involved, and the wrapper table below becomes
moot.

**A second engine is a second thing to keep correct.** `tests/store.rs` is 2,197
lines of invariants; they all have to run against both backends, which means
parameterising the suite over `B: Backend` and running it twice in CI. That is
the right cost to pay — it is also what makes the claim "the swap is contained"
checkable rather than asserted, exactly as `MemoryStore` did for the `Store`
seam.

**Two engines is two sets of operational advice.** `docs/operations.md`,
`docs/storage.md` and `vash.example.toml` all describe LMDB behaviour as fact
("the map is a reservation, not an allocation"). Under mdbx some of that is
false. Each such statement needs a backend qualifier or a table row.

## 10. Which Rust binding — **withdrawn**

> **Superseded by [Phase 0](#phase-0-results).** The answer is *none of them*:
> use raw FFI over a vendored `mdbx.c`, sized to the `Backend` trait. Every
> crate below hardcodes `NOSTICKYTHREADS`, which measured 56× on reads;
> `signet-libmdbx` additionally does not build on Windows at all, its sys crate
> shipping an empty `bindings_windows.rs`. The survey is kept because the
> licence and maintenance picture is still what a future reader would want if
> the wrappers ever fix the flag.

| Crate | Version | Licence | Notes |
|---|---|---|---|
| `signet-libmdbx` | 0.8.3 (May 2026) | MIT OR Apache-2.0 | Fork of `reth-libmdbx`, actively released. Cursor lifetimes tied to their transaction, which is what the `ReadTxn` GAT wants. **Validates arguments only in debug builds** — release builds trust the caller. |
| `reth-libmdbx` | tracks reth | Apache-2.0 lineage | The conservative choice: exercised continuously by a large production workload. Versioned with reth rather than independently. |
| `libmdbx` (vorot93) | 0.6.6 (Feb 2026) | **MPL-2.0** | The most-downloaded. The licence does not match the workspace's `MIT OR Apache-2.0` and would change what the release notice has to carry. |

**Recommend `signet-libmdbx`**, on the licence match and the transaction-bound
cursor lifetimes, with `reth-libmdbx` as the fallback if the spike hits
anything. Its debug-only validation is worth naming in the backend module: the
`Backend` impl is the layer that must not pass an invalid argument, because in
release nothing below it will complain.

Note that heed has no mdbx backend — this is a second dependency, not a feature
of the existing one.

## 11. Staging

### Phase 0 — spike ✅ **done**

Four questions, answered in [Phase 0 results](#phase-0-results) above: musl
links with one define in a build script we own; `get` borrows; every wrapper
hardcodes the flag that costs 56×, so the backend goes to raw FFI; warm-up and
pinning work on both platforms tested, given a raised process limit.

**Two things it left open, and neither blocks Phase 1:**

- **macOS warm-up.** No machine. It is the third platform `resident_mode` would
  reach and nobody has run a line of this on it.
- **Q3 on Linux.** The container could not answer it: its own lock-free control
  column did not scale, so the host was the ceiling. Needs a quiet multi-core
  Linux box, and the same run should re-check the 25% read regression Windows
  showed — one platform is not a result.

### Phase 1 — extract the `Backend` trait, LMDB only ✅ **done**

`crates/vash-store/src/backend/` holds the trait and the heed implementation;
`env.rs` kept the schema check, the shard-identity check, the tag registry load
and the CAS resumption, and lost everything else. `Engine<B>`, `Shards<B>` and
`VashStore<B>` carry the parameter; `LmdbEngine` and `LmdbStore` remain as type
aliases, so the benchmarks, the examples and `vash-server` compile untouched.

**Exit — met.** 651 tests pass unchanged, and the `hot_path` read benchmarks are
within run-to-run noise against the previous commit, mostly slightly ahead. The
diff removes 482 lines and adds 405: `.map_err(StoreError::from_heed)?` on every
operation is gone, because a backend maps its own errors.

Three things worth recording, none of them the point:

- **`Writer` needed no parameter.** It owns a queue and a thread handle; the
  engine only exists inside the thread it spawned, so only `Writer::spawn` is
  generic. `Shard<B>` holds an `Arc<Engine<B>>` and a plain `Writer`.
- **`StoreError::Lmdb(heed::Error)` became `Engine(String)`**, which made
  `clone_shallow` lossless — it used to downgrade an engine failure to `Corrupt`
  because `heed::Error` is not `Clone`.
- **The borrow discipline needed no restructuring**, exactly as §2 predicted:
  heed's own rules had already forced every scan to collect a bounded batch
  before deleting from it.

One thing the naive conversion got wrong and review caught: `utilisation_in`
took two `info()` snapshots per commit where it had taken one, because
`used_bytes_in` now needs the page size from the same call. It takes one.

### Phase 2 — the mdbx backend behind the feature ✅ **done**

`crates/vash-store/src/backend/mdbx/` over `vendor/libmdbx/mdbx.c`, behind the
`mdbx` feature and `store.backend`. Both engines run the whole store suite and
the whole server protocol suite, and CI runs each twice — once per engine — with
the musl link asserted separately.

**What it found, none of which was in the plan:**

- **`max_readers` is a floor under mdbx, not an exact count.** It rounds the
  reader table up to fill its pages — 256 became 368. Harmless in the direction
  it goes, since the startup rule this setting exists for is
  `store.max_readers > server.max_blocking_threads`, but it is a semantic
  difference and `docs/storage.md` now says so.
- **Stating a geometry lower bound cost 16 MiB per fresh shard.** Pinning it to
  `MIN_MAP_SIZE` made every new database allocate that much up front, which
  quietly contradicts the property `map_size` has always been documented with.
  Stating only the ceiling brings a fresh store to **0.3 MiB**, and
  `examples/mdbx_geometry.rs` is the measurement — LMDB 0.0 MiB, mdbx 0.3 MiB, at
  64 MiB, 1 GiB and 4 GiB alike. `MIN_MAP_SIZE` is still enforced on `map_size`
  itself; it is LMDB's floor, and there is no reason a growable file starts
  there.
- **Both `MDBX_MAP_FULL` and `MDBX_TXN_FULL` have to map to `CapacityFull`.**
  LMDB has only the first. The second is one transaction dirtying more than it
  may, and the writer's answer — free space, retry smaller — is right for both.
- **mdbx pins the map on Windows**, once the process working set is raised,
  which is item 3 of [§7](#7-what-libmdbx-actually-gives-us) delivered rather
  than predicted. `tests/backends.rs` asserts it there.

**Exit — met.** The store suite passes on both engines (77 tests each), the
server suites pass on both, and a database written by one engine is refused by
the other with an instruction to wipe.

**One caveat on how "both" is achieved.** The suites are parameterised by which
engine is *compiled*, not by a runtime flag, so covering both takes two CI runs
rather than one. Running both in a single pass would mean a generic parameter on
all 77 tests, which buys nothing a second job does not — the invariants are the
same invariants.

### Phase 2 — the original plan

`backend/mdbx.rs` over raw FFI, the vendored `mdbx.c` and its build script
(including the musl define), the `mdbx` cargo feature, `store.backend`, the
wrong-format detection from §5, and the test suite parameterised over both. CI
gains one job: `--features mdbx`, full store suite, **including the musl
static-link assertion** — that is where Phase 0's one real build failure lives,
and it must be a gate rather than a comment.

Two things Phase 0 says this phase must get right, because they are silent
failures rather than loud ones: **do not set `NOSTICKYTHREADS`**, and raise the
process working set before asking for a locked warm-up on Windows. Both belong
in a test — a reader-slot scaling assertion and a `map_locked()` assertion —
not in a comment, since neither breaks anything visible when it regresses.

**Exit:** every invariant in `tests/store.rs` holds on both backends; a server
started on each answers the same protocol suite; an LMDB directory opened as
mdbx refuses to start with a wipe instruction.

### Phase 3 — measure, then decide the default ✅ **done**

**Decision: LMDB stays the default. libmdbx stays an option, and is not
removed.** Full tables in [benchmarks.md](benchmarks.md#lmdb-against-libmdbx).

**Measured on both platforms, five repeats each, every section in its own
process, ranges reported.** Full tables in
[benchmarks.md](benchmarks.md#lmdb-against-libmdbx).

| | Windows | Linux |
|---|---:|---:|
| Reads | 0.82-0.94x | 0.94-1.03x, all overlapping |
| `set_many`, `lazy` | **1.83x** | **0.06x** |
| `set_many`, `relaxed` | **2.49x** | 0.93x (overlap) |
| `set_many`, `durable` | **1.53x** | 1.03x (overlap) |
| Soak, sustained overfill | 1.20x | 1.24x |

**Why not promote it.** One row decides it: on Linux, batched `lazy` writes are
**17x slower** under libmdbx, with no overlap across five repeats. `lazy` is the
default durability and batching is what group commit does under load, so that is
the shipping configuration on the deployment platform. Reads are parity there and
everything that syncs is parity, so nothing offsets it.

**Why not remove it.** It wins more rows than it loses. On Windows it takes every
batched write mode by 1.5-2.5x, and it wins the **soak on both platforms** by
about 1.2x -- sustained overfill above the watermark, which is the state a full
cache actually lives in. It also pins the map on Windows, which LMDB cannot, so
`store.resident_mode` is only reachable there through this backend. And it earned
its keep as a second implementation of the `Backend` trait.

**The 17x and the 1.2x are the same engine in the same durability mode**, which
means one of them is about something other than the engine. The soak's store is
already at its ceiling and no longer growing; the write scenario writes into a
fresh store that grows continuously. LMDB never pays for growth -- it sizes its
file to `map_size` at creation and leaves it sparse -- while libmdbx grows on
demand, which is exactly the property [§6](#6-configuration-mapping) advertised
as an advantage. A geometry sweep pointed the same way: preallocating `size_now`
took libmdbx's batched `lazy` figure to parity with LMDB's.

**That is a hypothesis, not a result** -- the sweep predates the methodology fix
below. The test that settles it is batched `lazy` on Linux with and without
`size_now` preallocated, five repeats, quiet machine. If it holds, the fix is a
geometry that preallocates, and the price is the disk that Phase 2 deliberately
gave up to make a fresh database cost 0.3 MiB instead of 16 MiB per shard. That
trade is worth putting to an operator rather than choosing silently.

**This phase published wrong numbers twice, both from single measurements.**
Best-of-two on Windows gave reads at 0.55x where five repeats give 0.94x -- right
direction, wrong magnitude. And the Linux batched-`lazy` figure came out at
0.16x, then 1.02x, then 0.06x across three attempts; the variable was not process
isolation but whether a read workload had filled the page cache first, which is
kernel-wide and survives a fresh process. The harness now reports ranges and
flags overlapping ones, because a single measurement of a write scenario on this
host carries no information.

**What did not survive contact.** §7 item 5 — LIFO recycling and continuous
compactification showing up as better space behaviour under churn — is a
**negative result**. Over 30 s of sustained overfill both engines sit at the same
used bytes, the same file size and the same watermark; neither drifts. That was
billed as "the honest reason to do this", and the soak that was built to check it
says it is not one.

**Two harness bugs found by disbelieving the first numbers**, both of which would
have produced a confident wrong answer:

- The first read harness divided a fixed op count among threads, so at four
  threads the measurement window was 7 ms and reported 14× scaling from one
  thread to four. It is duration-based now, like `examples/txn_bench.rs`.
- `get_with` was handed a closure that read `data.len()` and never touched the
  value bytes, so it measured a header parse and called it a read. It reported
  mdbx 2.45× *ahead*; consuming the bytes reversed that to 0.78×.

**And one real bug in the backend**, which the benchmark existed to catch:
`lazy` was mapped to `MDBX_SAFE_NOSYNC` on the strength of §7 item 1. libmdbx's
own header says the quiet part — "the number and volume of disk IOPs with
`MDBX_SAFE_NOSYNC` will [be] exactly the same as without any no-sync flags" — so
it buys integrity and no speed whatever. Under it, `lazy` measured *slower* than
`relaxed` and `durable`: the durability ladder upside down, and single writes at
77 ops/s against LMDB's 7,625. `MDBX_UTTERLY_NOSYNC` is the mode libmdbx
documents as matching `MDB_NOSYNC`, and it is what `lazy` means now, with the
loss window bounded by `MDBX_opt_sync_period` set from `write.sync_interval_ms`.
That one flag was the difference between 0.01× and 2.81×.

**On the host.** Both platforms are the same laptop, with Linux in a container
on WSL2 rather than bare metal. Phase 0's attempt at this was reported as
unreadable because its own LMDB column would not scale; this one does - LMDB
goes 4.84x from one thread to eight - which is the control that makes the rest
of the table quotable. A real server is still a third data point nobody has, and
given how far these two move apart, that is not a hypothetical concern.

### Phase 3 — the original plan

A/B in one binary, on one host, paired runs: read and write, closed loop and
pipelined, all three durability modes, plus the soak from §8 that neither engine
has been measured under.

**Reads are no longer a formality.** Phase 0 measured mdbx 25% behind LMDB on
transaction begin, on the one platform whose numbers were readable. If that
holds on Linux, an engine worth ~2–3% on writes costs a quarter of the read
path, and the arithmetic stops favouring the swap regardless of what the
free-list does. Measure reads first; if they regress, the rest may not be worth
running.

**Exit:** numbers in `benchmarks.md`, and a decision recorded here — mdbx
becomes the default, stays an option, or is removed. **Removing it is a real
outcome**, and Phase 1 is worth keeping either way: the `Backend` trait makes
the LMDB code honest about what is engine-specific, which is documentation the
crate currently gives in prose.

### As a milestone

| # | Scope | Exit criteria |
|---|---|---|
| **M11** | Storage backend seam: `Backend` trait under the engine, a libmdbx implementation over raw FFI behind a cargo feature and `store.backend`, both engines under one test suite, an A/B and a soak | The full store suite passes on both backends; the musl static build is green with mdbx compiled in; reader-slot scaling and `map_locked()` are asserted by tests rather than assumed; switching engines is one config line and a wipe, refused rather than silently mis-opened; the engines are measured against each other in one binary on one host, **reads included**, and the default is set from that measurement rather than from either project's claims |

## 12. How to back out

Phase 2 is additive: dropping the feature and the config variant removes mdbx
entirely and leaves Phase 1's refactor, which is behaviour-preserving by
construction. There is no on-disk migration to unwind, because the two engines
never share a directory — an operator reverting wipes and refills, which for a
cache is a cold start, not a data loss.

## 13. Alternatives considered

- **Implement `Store` twice.** §1. It is a fork of the cache logic wearing a
  trait.
- **A `dyn Backend` object instead of a generic.** Cannot express "this value
  borrows this transaction" without boxing every read, which is a copy on the
  hottest path in the server.
- **`cfg` the backend module with no trait at all** — `#[cfg(feature = "mdbx")]
  use backend::mdbx as be;`. The smallest possible diff, and it fails the one
  requirement that matters: both engines in one binary, so the comparison can be
  paired on one host. It also makes the two backends' divergence invisible,
  since only one ever compiles.
- **Use one of the wrapper crates instead of vendoring.** This was the
  recommendation until Phase 0 measured it: all three hardcode
  `NOSTICKYTHREADS`, which costs 56× on reads, and the one whose licence
  matched does not build on Windows. Patching a fork of a wrapper to unset one
  flag is a fork either way, and a smaller one is fifteen `extern` lines. If a
  wrapper ever makes the flag configurable, revisit — the `Backend` trait is
  where that swap would land, and it would be a file.
- **Wait for heed to add an mdbx backend.** It has not, and the design of the
  seam is the same work either way.
- **Port to mdbx outright, no LMDB.** Rejected on evidence: the arithmetic in §8
  says the engine is a few percent of a write, and every measurement in this
  repo warns that the host swings by more than that. A change this size needs a
  paired comparison to justify itself, and deleting the incumbent removes the
  only thing to compare against.

---

## How to reproduce Phase 0

The spike is a single crate outside the tree: `build.rs` compiling the
amalgamated `mdbx.c` with `cc`, about fifteen `extern` declarations, and the
three questions as subcommands. It carries `heed` at the workspace's pinned
version so both engines are measured in one binary.

```bash
cargo run --release -- borrow   # Q2   (also: warmup, tls)
```

For the musl half, `rust:1.92-alpine` plus `apk add musl-dev` — no other
package — and `file` on the output to confirm `static-pie linked`. Raise the
limits before believing a failed lock: `--ulimit memlock=-1` on Linux,
`SetProcessWorkingSetSize` on Windows.

Two ways to get a false answer, both hit during this spike: a probe that only
calls `Environment::builder()` never pulls the archive member in and links
cleanly whatever is wrong with it; and a benchmark host that cannot scale a
lock-free lookup will report every engine as equal. Always open and commit, and
always keep the control column.

---

**Sources for the libmdbx claims in §7 and §10**, none of which are measurements
taken here: the upstream
[libmdbx README](https://github.com/erthink/libmdbx) for the feature and licence
claims and the "up to 30% faster in CRUD" figure,
[signet-libmdbx](https://github.com/init4tech/mdbx) and
[libmdbx-rs](https://github.com/vorot93/libmdbx-rs) for the wrapper comparison.
Everything else — the 8% decomposition, the 0.43 ms commit, the reader-slot
numbers, the Windows `write_map` ratios, the `resident_mode` results — is from
this repository's own documents and is cited inline.
