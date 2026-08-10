# Cache Server — Technical Plan

Working name: **kachedb** (binary `kached`). Rename freely; it only appears in crate names.

This document turns [project.md](project.md) into a set of concrete, defensible engineering decisions.
Every section states the decision first, then the reasoning, then the rejected alternatives.

**One concern up front, stated once:** LMDB is mandated by the brief, but it has a *single writer per
environment* and a copy-on-write B-tree, which is the least comfortable shape for a write-heavy cache.
The plan below neutralises this with environment sharding and group commit, and keeps the storage
engine behind a `Store` trait so `libmdbx` (an LMDB fork with better write behaviour and dynamic
sizing) is a contained swap if benchmarks demand it. Everything else proceeds on LMDB as specified.

---

## 1. Language — **Rust**

Confirmed. The brief already says Rust and it is the right call; here is the comparison that backs it.

| | Rust | C++ | Go | Java |
|---|---|---|---|---|
| Tail latency (p99/p999) | No GC, fully predictable | No GC, predictable | GC pauses sub-ms but non-zero; write barriers on every pointer store | ZGC gets to sub-ms but with a large heap-overhead tax |
| Memory safety on untrusted input | Guaranteed | Manual; protocol parsers are the classic CVE surface | Guaranteed | Guaranteed |
| Throughput ceiling | C-equivalent | Baseline | ~10–30% behind on hot loops; escape analysis is unreliable | JIT-competitive after warmup |
| Zero-copy over `mmap` | `&[u8]` slices with lifetimes tied to the txn — safe *and* free | Free, unsafe | Requires copy out of the mapping (GC can't own foreign memory safely) | Requires copy or `MemorySegment` gymnastics |
| LMDB integration | `heed` — mature, safe, actively maintained | Native | cgo call overhead (~50–100ns) on the hottest path in the program | JNI/FFM overhead, same problem |
| Ops footprint | Single static binary, ~10 MB RSS idle | Same | Single binary, larger runtime | JVM |

The decider is the combination of **zero-copy reads out of the LMDB mmap** and **no GC**. A cache
server's entire job is `network byte → mmap byte → network byte`; Go and Java both force a copy out of
the memory map because their collectors cannot hold references into foreign memory, and that copy is
on the hot path of every single GET. C++ matches Rust on performance but loses on safety exactly where
it hurts most: two hand-written parsers consuming bytes from anonymous network clients.

---

## 2. Libraries

Kept deliberately small. Every dependency on the request path must earn its place.

### Core

| Concern | Choice | Why |
|---|---|---|
| Storage | `heed` (LMDB) | The maintained Rust LMDB wrapper (Meilisearch's). Safe txn lifetimes, `NoTls` support, typed databases. `lmdb-rkv` is unmaintained. |
| Async runtime | `tokio` (multi-thread) | Only mature option. Used for **network I/O only** — see §9. |
| Sockets | `socket2` | Needed for `SO_REUSEPORT`, `TCP_NODELAY`, backlog tuning. |
| Buffers | `bytes` | `BytesMut` read buffers, `Bytes` for refcounted response slices, vectored writes. |
| Channels | `crossbeam-channel` (net→store), `oneshot` from tokio (store→net) | Bounded MPMC, ~100 ns/hop, supports `try_recv` draining — which is what group commit is built on. |
| Byte scanning | `memchr` | SIMD `\r\n` and space scanning in the memcached text parser. |
| Hashing (in-memory) | `foldhash` | Fastest general-purpose hasher for `HashMap`; not stable across runs, so in-memory only. |
| Hashing (stable) | `xxhash-rust` (XXH3) | Shard selection must be stable across restarts and across nodes. **Never use `ahash`/`foldhash` for this** — they are randomly seeded per process. |
| Struct↔bytes | `zerocopy` | Record headers and frame headers parsed by transmute-with-validation, no field-by-field decode. |
| Allocator | `mimalloc` | 10–20% on allocation-heavy connection handling vs. system malloc; better than jemalloc on Windows, which matters for dev. |

### Supporting

| Concern | Choice |
|---|---|
| Config | `serde` + `toml` + `clap` (flags override file override env) |
| Errors | `thiserror` in libraries, `anyhow` in the binary |
| Logging/tracing | `tracing` + `tracing-subscriber` (JSON in prod, pretty in dev) |
| Metrics | `metrics` + `metrics-exporter-prometheus` |
| Admin/metrics HTTP | `hyper` (no framework — two routes) |
| Benchmarks | `divan` (micro), `criterion` (regression tracking) |
| Property tests | `proptest` |
| Fuzzing | `cargo-fuzz` + `libfuzzer-sys` + `arbitrary` |
| Load testing | External: `memtier_benchmark`, `rpc-perf`, `mc-crusher` |

### Explicitly rejected

- **`moka` / `dashmap` as a value cache in front of LMDB.** LMDB is memory-mapped; hot pages already
  live in the OS page cache. An application-level value cache stores a *second copy* of the same bytes,
  halving effective RAM, and adds a coherence problem on every write and invalidation. LMDB's mmap **is**
  the in-memory tier. (A tiny hot-key cache stays on the table as a measured, flag-gated optimisation in
  M6 — not before there is a benchmark demanding it.)
- **`nom` / `winnow` for protocol parsing.** See §7.
- **`serde` on the wire.** Hand-rolled fixed-layout framing is faster and version-stable. `serde` is for
  the config file only.
- **A web framework.** Two admin endpoints do not justify one.

---

## 3. Protocol — **dedicated binary protocol, with memcached as a compatibility surface**

**Decision: a dedicated binary protocol (`KCP`) is the primary interface. The memcached text and meta
protocols are supported for drop-in compatibility, on the same port via first-byte detection.**

### Why not extend memcached

The memcached meta protocol (`mg`/`ms`/`md`) *is* extensible — flags are single-char tokens and unknown
ones are simply rejected, so new ones can be added. But it cannot deliver what this project needs:

1. **No out-of-order responses.** Memcached responses are strictly in request order over a connection.
   That serialises every pipelined batch behind its slowest member, which forfeits the entire benefit of
   sharded parallel execution (§9).
2. **Text framing costs.** Every integer is parsed from ASCII, every field delimited by scanning. At
   1M ops/s that is measurable.
3. **Delete-by-tag has no natural expression.** It would be a bare non-standard command, at which point
   compatibility is already lost.

So: a real protocol for clients that want the features, plus honest memcached compatibility for
everyone else.

### KCP frame

Fixed 12-byte header, little-endian, so it decodes as a single `zerocopy` cast:

```
 0               1               2               3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------+---------------+-------------------------------+
|   opcode u8   |   flags u8    |          status u16           |
+---------------+---------------+-------------------------------+
|                        request_id u32                         |
+---------------------------------------------------------------+
|                         body_len u32                          |
+---------------------------------------------------------------+
|                          body (body_len)                      |
```

- `status` is 0 in requests; carries the result code in responses.
- `request_id` is client-assigned and **echoed**. This is the key property: it lets the server dispatch a
  pipelined batch across shards in parallel and return each response the moment it is ready, in any
  order. Clients correlate by id.
- `flags`: bit 0 `RESPONSE`, bit 1 `NOREPLY` (fire-and-forget writes), bit 2 `RESERVED_COMPRESSED`.

**Opcodes:** `HELLO 0x01`, `PING 0x02`, `AUTH 0x03`, `STATS 0x04`, `CLUSTER 0x05`, `GET 0x10`,
`SET 0x11`, `DELETE 0x12`, `TOUCH 0x13`, `GET_MANY 0x20`, `SET_MANY 0x21`, `DELETE_MANY 0x22`,
`DELETE_BY_TAG 0x30`, `FLUSH 0x31`.

**Status codes:** `OK 0`, `NOT_FOUND 1`, `EXISTS 2`, `BAD_REQUEST 3`, `TOO_LARGE 4`, `UNAUTHORIZED 5`,
`OVERLOADED 6`, `CAPACITY_FULL 7`, `UNSUPPORTED 8`, `INTERNAL 9`.

`SET` body layout (illustrative; the normative spec lives in `docs/protocol.md`):

```
ttl_secs u32 | key_len u16 | tag_count u8 | _pad u8 | value_len u32
key bytes | value bytes | [ tag_len u16, tag bytes ] * tag_count
```

Batch opcodes (`*_MANY`) carry a `count u32` followed by repeated single-op bodies, and reply with a
single response frame containing `count` results in request order. One frame in, one frame out —
pipelining and batching are independent mechanisms.

`HELLO` negotiates protocol version and advertises server capabilities (shard count, max value size,
tags supported, cluster peers). Version negotiation up front means no per-frame version byte.

### Memcached surface

Same port. On the first byte of a connection: `0x00..0x7F` printable ASCII → memcached text; a valid KCP
opcode is distinguished by the fact that KCP requires `HELLO` (`0x01`) as the first frame, which is not a
printable character. Unambiguous, zero cost after the first byte. A separate port is available via config
for anyone who prefers explicit separation.

**Tags for memcached clients** are exposed via a meta-protocol flag extension on `ms` carrying a
comma-separated tag list, plus a non-standard `mdt <tag>` command for invalidation. The specific flag
letter must be picked from the currently-unassigned set in upstream `protocol.txt` at implementation
time and is a single constant in the code. Clients discover support via the `HELLO`/`stats` capability
list; classic memcached clients that never send it are unaffected.

**UDP will not be implemented.** Memcached's UDP support is a well-known reflection/amplification vector
and upstream disables it by default. TCP (and optionally Unix domain sockets) only.

---

## 4. TTL — expiry index + lazy checks

**Decision: an ordered on-disk expiry index swept incrementally by a background task, combined with a
lazy liveness check on every read. Never a per-key timer, never a full scan.**

### Read path (correctness)

Every record header carries `expires_at_ms: u64` (absolute, 0 = never). A read checks it against a
cached coarse clock and returns `NOT_FOUND` if expired. **An expired item is never served, regardless of
whether the sweeper has reached it.** This makes the sweeper purely a space-reclamation concern, not a
correctness one — which is what allows it to be lazy and bounded.

### Reclamation path (space)

A dedicated `exp` sub-database per shard:

```
key   = expires_at_ms u64 BE || cas u64 BE     (big-endian ⇒ LMDB's byte order == time order)
value = user key bytes
```

The sweeper opens a read cursor at the start of `exp`, walks forward while `expires_at_ms <= now`,
collects up to `batch_size` (default 512) victims, and hands them to the shard writer as one
transaction. Cost is **O(number of actually-expired items)**, never O(dataset). Because the index is
sorted by time, the very first key tells the sweeper whether there is any work at all — the idle case
costs one cursor seek.

**Stale index entries** are the classic bug here: overwrite a key with a new TTL and the old `exp` entry
still points at it. Solved by including `cas` (a per-record monotonic version) in the index key. Before
deleting, the sweeper compares the record's current `cas` against the entry's; a mismatch means the
entry is stale, so it drops the index entry and leaves the record alone. No read-modify-write of the old
entry is needed on the write path — writes stay a single insert.

**Bucketing.** `expires_at_ms` is rounded up to a configurable granularity (default 1s) *for the index
key only*; the record keeps the exact millisecond. This clusters index entries into shared B-tree pages,
cutting the write amplification LMDB's copy-on-write incurs when TTLs are spread across a wide key range.
Precision is unaffected because expiry is decided by the record, not the index.

### Rejected

- **Timer wheel in RAM.** Cannot survive restart, and its memory cost scales with item count — a second
  full index in RAM next to a database designed to not need one.
- **Redis-style random sampling.** Probabilistic, no bound on how long garbage lingers, and it is only
  the right answer when there is no ordered index available. Here there is one, essentially for free.
- **Full periodic scan.** O(dataset) per pass, destroys the page cache. Non-starter.

---

## 5. Tags — O(1) invalidation via generation counters

**Decision: tags are invalidated by bumping an in-memory generation counter (O(1), constant time
regardless of how many keys carry the tag). Space is reclaimed afterwards by a resumable background
reclaimer driven by an inverse index.**

This is the single most important design decision in the document. The naive implementation — look up
every key with the tag and delete it — makes `DELETE_BY_TAG` cost O(n) *while holding the shard's only
write transaction*, stalling every other write on the node. For a tag covering a million keys that is a
multi-second stop-the-world event.

### The mechanism

A **tag registry**, persisted in a `tags` sub-database and loaded fully into RAM at boot:

```
tags:  key = tag name bytes  →  value = { tag_id u32, generation u64, created_ms u64 }
```

Tags are few (thousands, not millions), so the whole table fits in RAM as
`Vec<Arc<TagEntry { name, id, generation: AtomicU64 }>>` indexed by `tag_id`, plus a name→id `HashMap`.
`tag_id` is a dense counter, node-local. **Names are the global identity; ids are not shared between
nodes** (this matters in §10).

Each record stores, in its header, the `(tag_id, generation)` pairs *as they were at write time*:

- **`SET` with tags:** read the current generation of each tag (one atomic load each) and write it into
  the record. No index maintenance required for correctness.
- **`DELETE_BY_TAG`:** `generation.fetch_add(1)` plus one small durable write of the new generation.
  **Constant time.** Every record referencing that tag is now, by definition, stale.
- **`GET`:** for each of the record's tags, compare the stored generation against the registry's current
  generation. Any mismatch ⇒ the record is logically dead ⇒ return `NOT_FOUND`. All atomic loads from a
  RAM array; **zero additional disk I/O**.

Liveness is therefore three RAM-only checks, evaluated on every read:

```
alive = record.epoch == global_epoch                       // §6 flush
     && (record.expires_at == 0 || record.expires_at > now) // §4 TTL
     && record.tags.all(|(id, gen)| registry[id].gen == gen) // §5 tags
```

`FLUSH`/`flush_all` reuses the same trick with a single global epoch counter — also O(1).

### Reclamation

Correctness is instant; disk space is not. A `tagidx` sub-database (LMDB `DUPSORT`) maps
`tag_id u32 BE → [user key bytes]` and is maintained on write. On a generation bump, a job is enqueued
in a `jobs` sub-database recording `{ tag_id, target_generation, resume_cursor }`. The reclaimer walks
the tag's duplicate list in bounded batches, re-checks each record's liveness, and deletes the dead ones
plus their index entries — checkpointing its cursor so a restart resumes rather than restarts. It shares
the writer queue with the sweeper and yields to user traffic under load.

`tag_reclaim` is configurable: `index` (default), `sweep` (no `tagidx`, rely on the TTL sweeper to notice
dead records — cheaper writes, slower reclamation), or `off`.

### Rejected

- **Inverse index as the primary invalidation mechanism.** O(n) inside the write lock, as above.
- **Hashing tag names to a `u64` id.** Saves the registry but a collision silently invalidates an
  unrelated tag's data. Storing names makes collisions structurally impossible for a table this small.
- **Bloom/roaring-bitmap tag membership.** Elegant for queries, but membership is not the operation
  needed — invalidation is, and generations do it in O(1) with less machinery.

---

## 6. Eviction — expired first, then soonest-to-expire; **no on-disk LRU**

**Decision: a three-stage policy — (1) reclaim expired items, (2) evict the soonest-to-expire live items,
(3) optionally bias stage 2 by an in-memory frequency sketch.**

### Why not LRU

LRU requires updating recency metadata **on every read**. On an LMDB-backed store that turns every GET
into a write, which means every GET queues behind the shard's single writer. It converts a lock-free,
fully-parallel read path into a globally serialised one. For this architecture, on-disk LRU is not a
tuning choice — it is a correctness-of-design failure.

### The policy

Watermarks over LMDB map utilisation (`env.info()`: `last_pgno` vs `map_size`) plus optional `max_items`:

| Level | Default | Action |
|---|---|---|
| Normal | < 75% | Sweeper runs at its idle cadence |
| Soft | ≥ 75% | Sweeper goes continuous; reclaimer priority raised |
| Hard | ≥ 88% | Active eviction: cursor from the head of `exp`, evict in batches until back under soft |
| Critical | ≥ 96% | Writes reply `CAPACITY_FULL`; reads unaffected |

Stage 2 costs nothing extra: the `exp` index built for TTL is already ordered by expiry time, so
"evict what dies soonest" is a cursor read from position zero. And it is semantically right — a TTL is
the client's own statement of how long the value is worth keeping; the item expiring in 3 seconds is
the cheapest thing in the store to lose.

Items with no TTL sit in a separate region of the index (`expires_at = u64::MAX` bucket) and are evicted
only after all TTL'd items, in insertion order.

### Optional frequency bias

A fixed-size in-memory Count-Min sketch (4 × 2^20 × 4-bit counters ≈ 2 MB, halved periodically for
aging) is incremented on GET — **atomically, in RAM, never touching disk**. When enabled
(`eviction.frequency_bias = true`), stage 2 samples K candidates (default 32) from the head of `exp` and
evicts the least-frequently-accessed among them rather than strictly the first. This is sampled
TinyLFU restricted to the near-expiry window: it protects hot keys from being dropped just because
their TTL happens to be short, at the cost of ~2 MB and one atomic increment per read.

Default: **off**. Turn it on when a workload demonstrates it helps.

---

## 7. Memcached protocol — **implemented by hand**

**Decision: hand-written parser and serialiser, in a dedicated crate, fuzzed.**

Reasons:

1. **No suitable library exists.** The Rust `memcache`/`memcached-rs` crates are *clients*. There is no
   maintained server-side protocol implementation to adopt. The build-vs-buy question does not actually
   have a "buy" option.
2. **The text protocol is genuinely small.** Line-oriented, space-delimited, `\r\n`-terminated.
   `memchr` finds the terminator; a split-on-space iterator does the rest. The classic command set
   (`get`, `gets`, `set`, `add`, `replace`, `append`, `prepend`, `cas`, `delete`, `incr`, `decr`, `touch`,
   `gat`, `gats`, `flush_all`, `stats`, `version`, `verbosity`, `quit`) plus the meta commands
   (`mg`, `ms`, `md`, `mn`, `me`, `ma`) is on the order of 1500 lines.
3. **Parser combinators are the wrong tool.** `nom`/`winnow` shine on nested grammars. This grammar has
   no nesting, and combinators would add an abstraction layer and a dependency between the socket buffer
   and the command for zero expressive gain.
4. **Zero-copy demands control.** Keys and values must be borrowed slices of the read buffer with no
   intermediate `String`/`Vec`. That requires owning the parser.

The parser is a pure function — `&[u8] → Result<Command<'_>, ParseError>` — with no I/O and no
allocation, which makes it directly fuzzable and benchable. **Both parsers get `cargo-fuzz` targets from
day one**; they are the only code in the system reading bytes from unauthenticated strangers.

The **legacy binary protocol (magic `0x80`) will not be implemented.** Upstream memcached has deprecated
it in favour of the meta text commands, modern clients have moved, and it would be a third parser to
fuzz and maintain. If a specific client forces the issue it can be added later behind a feature flag.

Compatibility is verified against **real clients** in CI — `libmemcached`, `pymemcache`, `php-memcached`,
and `mc-crusher` — not against our own understanding of the spec.

---

## 8. Code organisation

A Cargo workspace. The boundary that matters: **`cache-core` defines the domain; `cache-store` and
`cache-proto` are interchangeable adapters on either side of it.** Both protocols compile down to the
same `Command`/`Reply` types, so the storage engine has no idea which wire format a request arrived on,
and the protocol crates have no idea what LMDB is.

```
cache-server/
├── Cargo.toml                  # workspace
├── crates/
│   ├── cache-core/             # domain. no I/O, no async, no dependencies of consequence
│   │   ├── key.rs              # Key newtype, length validation (LMDB max key = 511 bytes)
│   │   ├── value.rs            # Value, size limits
│   │   ├── record.rs           # on-disk record header layout, liveness check (zerocopy)
│   │   ├── ttl.rs              # Ttl, Expiry, coarse Clock
│   │   ├── tag.rs              # TagId, TagName, TagRegistry trait
│   │   ├── command.rs          # Command / Reply — THE boundary type
│   │   └── error.rs
│   │
│   ├── cache-store/            # storage adapter
│   │   ├── lib.rs              # Store trait (the libmdbx escape hatch)
│   │   ├── lmdb/
│   │   │   ├── env.rs          # env setup, flags, map sizing, integrity check on boot
│   │   │   ├── schema.rs       # sub-databases: main, exp, tagidx, tags, jobs, meta
│   │   │   ├── read.rs         # read txn handling, RoTxn reuse + renew
│   │   │   └── write.rs        # writer thread, group commit
│   │   ├── shard.rs            # shard router, per-shard threads and queues
│   │   ├── expiry.rs           # exp index maintenance + sweeper
│   │   ├── tags.rs             # in-RAM tag registry, generation bumps, persistence
│   │   ├── reclaim.rs          # resumable tag reclaimer
│   │   └── evict.rs            # watermarks, victim selection, frequency sketch
│   │
│   ├── cache-proto/            # wire adapters. pure codecs: bytes in, Command out
│   │   ├── kcp/                # native binary protocol
│   │   │   ├── frame.rs        # header (zerocopy)
│   │   │   ├── decode.rs
│   │   │   └── encode.rs
│   │   ├── memcached/
│   │   │   ├── text.rs         # classic commands
│   │   │   ├── meta.rs         # mg/ms/md/mn/me/ma + tag extension
│   │   │   └── encode.rs
│   │   └── detect.rs           # first-byte protocol detection
│   │
│   ├── cache-server/           # the binary: everything with a socket or a thread
│   │   ├── main.rs
│   │   ├── config.rs
│   │   ├── listener.rs         # accept loops, SO_REUSEPORT
│   │   ├── conn.rs             # per-connection state machine, pipelining, backpressure
│   │   ├── dispatch.rs         # Command → shard routing, batch fan-out/fan-in
│   │   ├── cluster/            # peer list, tag fan-out, anti-entropy (§10)
│   │   ├── admin.rs            # /metrics, /health, /stats
│   │   └── shutdown.rs         # drain, final sync
│   │
│   ├── cache-client/           # Rust KCP client. also the integration-test driver
│   └── cache-bench/            # divan/criterion harnesses
│
├── fuzz/                       # kcp_decode, memcached_text, memcached_meta, record_header
├── tests/                      # cross-crate integration + memcached client compat
└── docs/
    ├── project.md
    ├── plan.md                 # this file
    ├── protocol.md             # normative KCP spec
    ├── storage.md              # on-disk format, sub-database schemas
    └── operations.md           # tuning, capacity planning, failure modes
```

**On-disk record layout** (`cache-core/record.rs`), 28-byte header, zero-copy value slice:

```
version u8 | tag_count u8 | _reserved u16 | epoch u32 | mc_flags u32
expires_at_ms u64 | cas u64
[ tag_id u32, tag_generation u64 ] * tag_count      // 12 bytes each
value bytes                                          // returned as a borrowed slice
```

`mc_flags` is the memcached 32-bit client flags field, stored so a value written by a KCP client and
read by a memcached client round-trips correctly. `cas` doubles as the memcached CAS token and as the
version used to detect stale expiry-index entries (§4).

**Sub-databases per shard:** `main` (key → record), `exp` (expiry index), `tagidx` (DUPSORT, tag → keys),
`tags` (registry), `jobs` (reclaim checkpoints), `meta` (schema version, node id, shard count, epoch).
Shard count is written to `meta` and validated on open — reopening with a different count is a hard
error, not a silent remap.

---

## 9. Multithreading

**Decision: split the process into a network tier and a storage tier, connected by bounded channels.
Async I/O never touches LMDB; LMDB threads never touch a socket.**

```
┌─────────────────────────────────────────────────────────────┐
│  tokio multi-thread runtime  —  W = num_cpus network workers │
│  accept, TLS-less framing, parse, encode, write              │
└───────────────┬─────────────────────────────────────────────┘
                │  crossbeam bounded channel, routed by XXH3(key) % S
┌───────────────▼─────────────────────────────────────────────┐
│  storage tier — S independent shards                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ shard i:  own LMDB environment                       │   │
│  │   R reader threads   (concurrent, MVCC, no locking)  │   │
│  │   1 writer thread    (group commit)                  │   │
│  └──────────────────────────────────────────────────────┘   │
│  + 1 sweeper thread, 1 reclaimer thread (shared, low prio)   │
└──────────────────────────────────────────────────────────────┘
```

### Why the split

**LMDB reads can page-fault.** A cold read touches an unmapped page and blocks the calling thread for
the duration of a disk I/O — ~100 µs on NVMe, far worse on anything else. Doing that on a tokio worker
stalls every other connection assigned to it, and under memory pressure it collapses tail latency across
the whole server. The channel hop costs ~200 ns and a `oneshot`; a LAN round-trip is ~50 µs. Trading
0.4% of the latency budget for immunity to page-fault stalls is not a close call.

`store.inline_reads = true` skips the hop for deployments that guarantee the working set is resident.
Default off, and it stays off until a benchmark says otherwise.

### Reads

LMDB's MVCC gives unlimited concurrent readers with **no locks and no interference with the writer**.
This is the property that makes LMDB worth the single-writer tax.

- The environment is opened with `read_txn_without_tls()`, so read transactions are not pinned to a
  thread slot and are `Send`.
- Each reader thread keeps one long-lived `RoTxn` and **renews it per request batch**. Renewal is cheap;
  the alternative — holding a transaction open — pins the version and **blocks LMDB from reusing freed
  pages**, so the file grows without bound. This is the number one LMDB operational footgun and gets a
  dedicated metric (`store.oldest_reader_age_ms`) plus an alarm.
- `max_readers` is set from `S × (R + slack)` and exposed as a metric; exhaustion is a hard failure mode
  worth alerting on before it happens.

### Writes — group commit

Each shard's writer thread owns the only `RwTxn`:

```rust
loop {
    let first = rx.recv()?;                 // block when idle — zero added latency
    let mut batch = vec![first];
    while batch.len() < max_batch {         // drain whatever ALREADY queued
        match rx.try_recv() {
            Ok(op) => batch.push(op),
            Err(_) => break,
        }
    }
    let mut txn = env.write_txn()?;
    let results = batch.iter().map(|op| apply(op, &mut txn)).collect();
    txn.commit()?;                          // one commit for the whole batch
    for (op, r) in batch.into_iter().zip(results) { op.reply(r); }
}
```

The crucial detail is **no artificial linger**. The batch is whatever had already arrived while the
previous commit was in flight, so under load batches grow naturally and commit cost amortises across
hundreds of operations, while an idle server commits a lone write immediately. Throughput self-regulates
against load with no tuning knob and no latency penalty. (`write.linger_us` exists for
throughput-over-latency deployments; default 0.)

### Sharding

S independent LMDB environments (default `min(num_cpus, 8)`), key routed by `XXH3(key) % S`. This is the
direct answer to the single-writer limitation: **S concurrent writers instead of one.** It is also why
`XXH3` and not `foldhash` — routing must be identical across restarts.

Consequences, accepted deliberately:

- **Multi-key operations are grouped by shard and executed in parallel**, then fanned back in. With
  out-of-order KCP responses this is a throughput *gain*, not a cost.
- **Batches are not atomic across shards.** A `SET_MANY` spanning shards can be partially visible. For a
  cache this is a non-issue and the protocol documents it explicitly.
- **The tag registry is replicated to every shard** (it is small, and it must be, since a tagged record
  can land in any shard). Generation bumps write to all S shards — still O(S), not O(keys).

### Durability

For a cache, a lost write is a cache miss, and a cache miss is *already a supported outcome*. That
reframes the whole durability question and buys a lot of performance:

| Mode | LMDB flags | Semantics |
|---|---|---|
| `durable` | default sync | fsync per commit. Slowest. Available for anyone who wants it. |
| `relaxed` **(default)** | `MDB_NOMETASYNC` + periodic `force_sync` | Loses at most the last few transactions on OS crash; **cannot corrupt the database**. |
| `ephemeral` | `MDB_NOSYNC` (+ `MDB_WRITEMAP` on Unix only — it fails on Windows, see §11) | Fastest. An OS crash or power loss *can* corrupt the file — handled by verifying integrity at boot and, on failure, **wiping and starting empty**. Legitimate for a cache; must never be used for a system of record. |

A `--ephemeral` mode that also wipes on clean startup, and support for placing the database on tmpfs,
cover the brief's "in-memory caching" option without a separate code path.

### Backpressure

Every channel is bounded. A full shard queue returns `OVERLOADED` immediately rather than growing an
unbounded backlog — a cache that queues is worse than a cache that says no, because the client's
fallback path is cheaper than a 30-second wait. Per-connection limits on pipeline depth and buffer size
prevent a single client from consuming the server's memory.

---

## 10. Horizontal scaling — shared-nothing, client-sharded

**Decision: instances are completely independent. Clients shard the keyspace with rendezvous hashing.
No replication, no consensus, no server-side data movement.** The one exception is tag invalidation,
which needs a fan-out (below).

This is the memcached model, and it is memcached's model precisely because it is why memcached scales:
no coordination on the request path means adding a node adds capacity linearly and removing one costs
`1/N` of the cache. Replication and consensus buy durability guarantees a cache does not need — a lost
node means misses, and misses are the failure mode the client already handles.

- **Placement:** rendezvous (HRW) hashing client-side. Preferred over ketama consistent hashing: no ring
  construction, no virtual nodes to tune, better key distribution, and adding/removing a node moves only
  that node's share.
- **Discovery:** static peer list in config, or DNS SRV. The `CLUSTER` opcode returns the server's view
  of the peer list so smart clients can self-configure and detect membership drift.
- **Failure:** a dead node is removed from the client's hash set; its keys redistribute and re-populate
  from the origin. No failover machinery.

### The one thing that must cross nodes: tag invalidation

A tag's keys are spread by key hash across *every* node, so `DELETE_BY_TAG` on one node invalidates only
that node's share. Three modes:

| Mode | Behaviour |
|---|---|
| `local` | No fan-out. The client is responsible for calling every node. Zero server coupling. |
| `fanout` **(default)** | The receiving node bumps its own generation, replies immediately, and forwards to peers asynchronously with retry. |
| `fanout_sync` | As above, but replies only after all reachable peers acknowledge. Higher latency, tighter bound on staleness. |

**This is safe to build because tag generations are a max-merge counter — a CRDT.** Each node applies
`generation = max(local, received)`. That makes forwarding:

- **idempotent** — replaying a message changes nothing;
- **order-independent** — messages can arrive in any order and converge to the same value;
- **retry-safe** — at-least-once delivery is sufficient, so no acknowledgement protocol or consensus is
  needed.

Because `tag_id` is node-local and **tag names are the global identity**, fan-out messages carry
`(name, generation)`. A peer that has never seen the tag creates it with the received generation.

**Anti-entropy** closes the gap for nodes that were down or partitioned: every `cluster.gossip_interval`
(default 5s) each node exchanges a digest of its tag→generation table with a random peer and max-merges
the differences. Since generations are persisted in LMDB, a restarted node resumes with its last known
state and converges within one interval.

**Consistency statement, stated plainly in the docs:** tag invalidation is *strongly consistent within a
node* and *eventually consistent across the cluster*, with a staleness bound of the gossip interval in
`fanout` mode. For a cache this is the correct trade; for anyone who needs better, `fanout_sync` narrows
it to the acknowledgement round-trip.

### Explicitly rejected

- **Server-side sharding / proxy layer.** Adds a network hop to every request — roughly doubling latency —
  to solve a problem clients already solve.
- **Replication.** Doubles write cost and memory to protect data that is by definition reconstructible.
- **Raft/consensus for membership.** A cache cluster does not need agreement on membership; each client
  having a *good enough* view is sufficient, and disagreement costs a miss.

---

## 11. Configuration surface (sketch)

```toml
[server]
listen = "0.0.0.0:11211"
unix_socket = ""                  # optional
workers = 0                       # 0 = num_cpus
max_connections = 10_000
max_key_bytes = 511               # LMDB hard limit
max_value_bytes = 1_048_576       # 1 MiB, memcached-compatible default

[store]
path = "/var/lib/kached"
shards = 0                        # 0 = min(num_cpus, 8)
map_size_gb = 64                  # per shard; sparse on Linux, PREALLOCATED ON WINDOWS
readers_per_shard = 4
durability = "relaxed"            # durable | relaxed | ephemeral
sync_interval_ms = 1000
inline_reads = false
[store.write]
max_batch = 1024
linger_us = 0

[ttl]
default_secs = 0                  # 0 = no expiry
max_secs = 2_592_000              # 30d, memcached-compatible
bucket_granularity_ms = 1000
sweep_interval_ms = 100
sweep_batch = 512

[tags]
enabled = true
reclaim = "index"                 # index | sweep | off
reclaim_batch = 256

[eviction]
soft_watermark = 0.75
hard_watermark = 0.88
critical_watermark = 0.96
max_items = 0                     # 0 = unlimited
frequency_bias = false

[cluster]
peers = []
delete_by_tag = "fanout"          # local | fanout | fanout_sync
gossip_interval_ms = 5000

[protocol]
memcached = true
flush_all_enabled = false         # off by default: it is a remote cache-wipe primitive
auth_secret = ""                  # empty = no auth (bind to a private network)

[observability]
admin_listen = "127.0.0.1:9090"
log_format = "json"
```

**Windows notes (dev environment) — measured on heed 0.22.1 / LMDB, Windows 11, MSVC 2019:**

1. **`map_size` is *not* preallocated.** A 16 GiB map produced a 16 KiB file. The commonly-repeated
   claim that Windows materialises the full map size is false for this build, so dev and prod configs
   can use the same generous sizing.
2. **`MDB_WRITEMAP` does not work on Windows** — `mdb_env_open` fails with OS error 6 (invalid handle)
   at every map size tested (64 MiB, 1 GiB, 16 GiB). The `ephemeral` durability mode must therefore
   drop `WRITE_MAP` on Windows and use `NO_SYNC` alone; see §9.
3. Read transactions use `EnvOpenOptions::read_txn_without_tls()`. The `EnvFlags::NO_TLS` flag is
   deprecated in heed 0.22 and changes the env type to `Env<WithoutTls>`, which is what makes a
   `RoTxn` `Send` and therefore movable between reader threads.

---

## 12. Observability

Prometheus metrics on the admin port, plus memcached-compatible `stats` output so existing dashboards
keep working.

- **Traffic:** ops/s by command and protocol, hit ratio, error rate by status code.
- **Latency:** histograms per command, split into queue-wait vs. execution — the split is what tells you
  whether the shard writer is the bottleneck.
- **Storage:** map utilisation per shard, page count, free-list size, entry count, dirty pages.
- **Writer:** queue depth, batch size distribution, commit duration, `OVERLOADED` rejections.
- **Sweeper/reclaimer:** expiry index lag (`now − oldest unswept entry`), items reclaimed/s, pending
  reclaim jobs, reclaim backlog age.
- **LMDB health:** `oldest_reader_age_ms` (the long-read-txn footgun), reader slots in use vs.
  `max_readers`.
- **Cluster:** peers reachable, tag fan-out failures, gossip convergence lag.

Alarms that matter from day one: map utilisation past the soft watermark, `oldest_reader_age_ms` growth,
sweeper lag growth, and any `OVERLOADED`.

---

## 13. Testing

| Layer | Approach |
|---|---|
| Unit | Per-module, especially record encode/decode and liveness evaluation |
| Property | `proptest` round-trips for both codecs and the record header; TTL/tag/epoch liveness invariants |
| **Fuzzing** | `cargo-fuzz` on `kcp_decode`, `memcached_text`, `memcached_meta`, `record_header`. **Non-negotiable** — these parse bytes from unauthenticated clients. Run in CI on every PR, plus a long-running nightly. |
| Integration | Real server over a real socket via `cache-client`; TTL expiry, tag invalidation, eviction under pressure, restart persistence |
| **Compatibility** | Real memcached clients — `libmemcached`, `pymemcache`, `php-memcached` — run against our server *and* against real memcached, comparing outputs |
| Chaos | `kill -9` mid-write and verify recovery in each durability mode; disk-full; map-full; reader-slot exhaustion; peer partition during tag fan-out |
| Load | `memtier_benchmark` and `rpc-perf` in CI on a fixed runner, with regression gates on throughput and p99 |
| Concurrency | `loom` on the tag registry and writer queue handoff; ThreadSanitizer in a nightly job |

**Performance goals** — to be validated in M6, not assumed (16-core node, 64 GB RAM, NVMe, 1 KiB values,
resident working set):

- GET: ≥ 1,000,000 ops/s, p99 < 1 ms at 500k ops/s
- SET: ≥ 250,000 ops/s with group commit in `relaxed` mode
- `DELETE_BY_TAG`: < 1 ms regardless of tag cardinality (this is the O(1) claim, and it is the headline
  benchmark)
- Cold start to serving: < 1 s for a 10 GB database (mmap, so no load phase)

---

## 14. Milestones

| # | Scope | Exit criteria |
|---|---|---|
| **M0** | Workspace, config, `Store` trait, LMDB env, single-shard GET/SET/DELETE, KCP framing, `cache-client` | End-to-end round-trip over a socket; CI green |
| **M1** | TTL: `exp` index, sweeper, lazy checks, `TOUCH`; batch ops; group commit | Expired keys never served; sweeper reclaims within one interval; write throughput scales with batch size |
| **M2** | Tags: registry, generations, `DELETE_BY_TAG`, `tagidx`, resumable reclaimer; global epoch/`FLUSH` | `DELETE_BY_TAG` is O(1) under benchmark; reclaimer resumes correctly across restart |
| **M3** | Memcached text + meta protocols, first-byte detection, tag flag extension | Real-client compat suite passes against both our server and memcached |
| **M4** | Sharding, capacity watermarks and eviction, full metrics, admin endpoints | Throughput scales with shard count; server survives a sustained overfill |
| **M5** | Cluster: peer list, tag fan-out, anti-entropy gossip, `CLUSTER` opcode | Invalidation converges across a 3-node cluster including a partitioned/restarted node |
| **M6** | Perf hardening, fuzz corpus, benchmark suite, packaging (static musl binary, Docker, systemd), docs | Performance goals in §13 met or consciously revised with data |

---

## 15. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| LMDB single writer caps write throughput | High | Sharded environments (§9) + group commit; `Store` trait keeps `libmdbx` a contained swap |
| Copy-on-write write amplification under high churn | Medium | TTL bucketing to cluster index writes; batch commits; measure early in M1 — **this is the number to watch first** |
| Long-lived read txn blocks page reuse ⇒ unbounded file growth | High | Per-batch txn renewal; `oldest_reader_age_ms` metric with an alarm; documented in operations.md |
| `map_size` is fixed at open and cannot grow while txns are live | Medium | Generous sizing (verified not preallocated, §11); capacity metrics and watermarks well before the map fills |
| `MDB_WRITEMAP` unusable on Windows (measured, §11) | Low | `ephemeral` mode drops the flag on Windows; dev parity is otherwise unaffected |
| Two hand-written parsers on untrusted input | High | Continuous fuzzing from M0; strict length caps before any allocation; no `unsafe` in the parsers |
| Memcached compat subtly wrong in ways clients notice | Medium | Differential testing against real memcached with real client libraries (§13) |
| Tag fan-out silently drops invalidations during a partition | Medium | CRDT max-merge makes retries free; anti-entropy bounds staleness; `fanout_sync` for stricter needs |
| Tag registry grows unboundedly if clients generate unique tags | Medium | Registry size metric; configurable cap with `BAD_REQUEST` past it; GC for tags with no live records |
| `ephemeral` mode corruption on power loss | Low (by design) | Integrity check at boot, wipe-and-continue; documented as cache-only and never the default |

---

## 16. Non-goals

Stated so they do not get re-litigated: no replication, no consensus/Raft, no server-side proxy or
sharding tier, **no UDP** (amplification vector), no Lua or server-side scripting, no pub/sub, no
secondary indexes or query language beyond tags, no multi-key atomicity across shards, no TLS in v1
(rustls behind a feature flag if a deployment needs it), no on-disk LRU (§6), and no legacy memcached
binary protocol (§7).
