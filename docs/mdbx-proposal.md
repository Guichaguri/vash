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
`Shards<B>`, `ShardWriter<B>` and `LmdbStore` → `VashStore<B>`. **It stops
there**, because `ServerState` holds `Arc<dyn Store>`: the server, the cluster,
the executors, the protocol layer and every server test see no change at all.
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
```

`env.rs` keeps what it is really about — schema version checks, shard-identity
checks, the tag registry load, CAS resumption — and loses only the
`EnvOpenOptions` block and the flag mapping, which become `LmdbBackend::open`.

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

**3. `mdbx_env_warmup` could take `resident_mode` off Linux.**
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

### Reads: parity, ±noise

The read path is a B-tree descent into a resident memory map, a header parse and
a subslice. Both engines do the same work with the same page layout, and
`performance-proposals.md` §3 established that vash's read numbers come from
`resident_mode` and from `get_with` rendering out of the map — not from the
engine. **Predict no measurable change on Linux**, and treat any large
difference in either direction as a bug in the port (most likely `NOTLS`, see
§6).

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

**Build.** mdbx is vendored C. The Dockerfile's alpine stage installs
`musl-dev` for LMDB; mdbx needs at least that, and the `-sys` crates in this
family generate bindings with `bindgen`, which needs `libclang`. CI runs
`cargo clippy --all-targets --all-features`, so **an additive `mdbx` feature is
built by CI the moment it exists** — the clippy job needs the toolchain too.

**The musl static build is a release gate**, and CI asserts the binary is not a
dynamic executable. Whether `mdbx-sys` compiles and links statically for
`x86_64-unknown-linux-musl` is a go/no-go question that must be answered
*before* any refactor lands, not after. It is item one of the spike.

**Licensing.** libmdbx the C library is Apache-2.0 (relicensed in 2024), which
is compatible with this workspace's `MIT OR Apache-2.0`. The Rust wrappers are
not uniform, which drives the choice below.

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

## 10. Which Rust binding

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

### Phase 0 — spike, ~1 day, no refactor

Answer the questions that can kill the whole thing, in a throwaway branch, with
nothing merged:

1. Does `mdbx-sys` build and link statically for `x86_64-unknown-linux-musl` on
   the alpine image? Does the clippy job's toolchain need `libclang`?
2. Does a `get` return a borrow valid for the transaction's life, with no copy?
   (If it copies, `get_with` and the whole zero-copy record layout lose their
   point and this stops here.)
3. Is `MDBX_NOTLS` off by default in the chosen wrapper? Reproduce
   `examples/txn_bench` against mdbx and confirm the sixteen-thread number rises
   rather than falls.
4. Does `mdbx_env_warmup` with lock report success on Windows and macOS?

**Exit:** four yes/no answers written into this document. Any "no" on 1–3 is a
stop.

### Phase 1 — extract the `Backend` trait, LMDB only

The whole refactor, one backend, no new dependency, no feature flag. `env.rs`
splits, `Engine<B>` gains its parameter, `StoreError::Lmdb` becomes
`StoreError::Engine`.

**Exit:** `tests/store.rs` and the server suite pass unchanged; the benchmark
suite shows no regression against the commit before it (this is a
zero-behaviour-change refactor, and a measurable delta means the generic did
something the `dyn` boundary was hiding).

### Phase 2 — the mdbx backend behind the feature

`backend/mdbx.rs`, the `mdbx` cargo feature, `store.backend`, the wrong-format
detection from §5, and the test suite parameterised over both. CI gains one job:
`--features mdbx`, full store suite.

**Exit:** every invariant in `tests/store.rs` holds on both backends; a server
started on each answers the same protocol suite; an LMDB directory opened as
mdbx refuses to start with a wipe instruction.

### Phase 3 — measure, then decide the default

A/B in one binary, on one host, paired runs: read and write, closed loop and
pipelined, all three durability modes, plus the soak from §8 that neither engine
has been measured under.

**Exit:** numbers in `benchmarks.md`, and a decision recorded here — mdbx
becomes the default, stays an option, or is removed. **Removing it is a real
outcome**, and Phase 1 is worth keeping either way: the `Backend` trait makes
the LMDB code honest about what is engine-specific, which is documentation the
crate currently gives in prose.

### As a milestone

| # | Scope | Exit criteria |
|---|---|---|
| **M11** | Storage backend seam: `Backend` trait under the engine, a libmdbx implementation behind a cargo feature and `store.backend`, both engines under one test suite, an A/B and a soak | The full store suite passes on both backends; switching engines is one config line and a wipe, refused rather than silently mis-opened; the engines are measured against each other in one binary on one host, and the default is set from that measurement rather than from either project's claims |

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
- **Wait for heed to add an mdbx backend.** It has not, and the design of the
  seam is the same work either way.
- **Port to mdbx outright, no LMDB.** Rejected on evidence: the arithmetic in §8
  says the engine is a few percent of a write, and every measurement in this
  repo warns that the host swings by more than that. A change this size needs a
  paired comparison to justify itself, and deleting the incumbent removes the
  only thing to compare against.

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
