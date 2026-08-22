# vash — Technical Plan

Working name: **vash** (binary `vash-server`). Rename freely; it only appears in crate names.

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
| Sockets | ~~`socket2`~~ **not adopted** | Was for `SO_REUSEPORT`, `TCP_NODELAY` and backlog tuning. `TCP_NODELAY` turned out to be on `tokio::net::TcpStream` already, and the other two answer a problem this workload does not have — see §9. |
| Buffers | `bytes` | `BytesMut` read buffers, `Bytes` for refcounted response slices, vectored writes. |
| Channels | `crossbeam-channel` (net→store), `oneshot` from tokio (store→net) | Bounded MPMC, ~100 ns/hop, supports `try_recv` draining — which is what group commit is built on. |
| Byte scanning | `memchr` | SIMD `\r\n` and space scanning in the memcached text parser. |
| Hashing (in-memory) | `foldhash` | Fastest general-purpose hasher for `HashMap`; not stable across runs, so in-memory only. |
| Hashing (stable) | `xxhash-rust` (XXH3) | Shard selection must be stable across restarts and across nodes. **Never use `ahash`/`foldhash` for this** — they are randomly seeded per process. |
| Struct↔bytes | `zerocopy` | Record headers and frame headers parsed by transmute-with-validation, no field-by-field decode. |
| Allocator | `mimalloc` | 10–20% on allocation-heavy connection handling vs. system malloc; better than jemalloc on Windows, which matters for dev. **Measured in M6: worth nothing on this workload** (1.13M vs 1.11M ops/s), so it ships as an opt-in feature rather than the default. |

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
  the in-memory tier. (A tiny hot-key cache stayed on the table as a measured, flag-gated optimisation
  for M6. **The benchmark never demanded it**: with thread-local reader slots a lookup costs about
  200 ns and scales linearly to 5.3M/s across sixteen threads, so a hot-key cache would be adding a
  coherence problem to save a couple of hundred nanoseconds. Not built.)
- **`nom` / `winnow` for protocol parsing.** See §7.
- **`serde` on the wire.** Hand-rolled fixed-layout framing is faster and version-stable. `serde` is for
  the config file only.
- **A web framework.** Two admin endpoints do not justify one.

---

## 3. Protocol — **dedicated binary protocol, with memcached as a compatibility surface**

**Decision: a dedicated binary protocol (`VCP`) is the primary interface. The memcached text and meta
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

### VCP frame

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
`SET 0x11`, `DELETE 0x12`, `TOUCH 0x13`, `ARITHMETIC 0x14`, `GET_MANY 0x20`, `SET_MANY 0x21`, `DELETE_MANY 0x22`,
`DELETE_BY_TAG 0x30`, `FLUSH 0x31`, `TAG_SYNC 0x40`, `LIST_KEYS 0x50`, `LIST_TAGS 0x51`.

`TAG_SYNC` was added in M5 for peer-to-peer traffic (§10). Peers speak the ordinary protocol on
the ordinary port — a peer is just another VCP client — which meant no second listener, no second
codec and no second thing to fuzz.

`ARITHMETIC 0x14` was added in M10. It joins the single-key group because that
is what it is, and it exists because the compatibility dialects had atomic
counters from the start and the native one did not — so a first-party client had
to speak memcached or Redis to increment a number. The primitive underneath it is
shared with both (`vash_core::arith`), so the three dialects agree by
construction.

`LIST_KEYS 0x50` and `LIST_TAGS 0x51` were added in M8. They open a new group
rather than extending `0x3x`, whose members are whole-keyspace *mutations*. Both are administrative:
gated off by default, permitted to be a linear scan, and paginated by an **opaque cursor** rather
than an offset — an offset re-walks what it skips, which is the quadratic resumption §5 already met
once in `tagidx`. The per-opcode implementation contract for the whole set lives in
`docs/opcodes.md`.

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

Same port. On the first byte of a connection: `0x00..0x7F` printable ASCII → memcached text; a valid VCP
opcode is distinguished by the fact that VCP requires `HELLO` (`0x01`) as the first frame, which is not a
printable character. Unambiguous, zero cost after the first byte. A separate port is available via config
for anyone who prefers explicit separation.

**Tags for memcached clients** are exposed via a meta-protocol flag extension on `ms` carrying a
comma-separated tag list, plus a non-standard `mdt <tag>` command for invalidation. Clients discover
support via the `HELLO`/`stats` capability list; classic memcached clients that never send it are
unaffected.

**As implemented (M3):** the flag letter is `G`, picked from the letters upstream leaves unassigned,
and defined as a single constant (`memcached::meta::TAG_FLAG`). The classic dialect also gets
`delete_by_tag <tag>`, named distinctly enough that it cannot collide with a future upstream command.
The meta flag set is the documented core (`v f c t s k O q T F C M D N`); an unrecognised flag is
rejected with `CLIENT_ERROR`, as upstream does, so a client never believes a flag took effect.

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

`FLUSH`/`flush_all` reuses the same trick with a single global epoch counter. **As implemented (M2) it
bumps the epoch *and* clears the data sub-databases in the same transaction**, because the two do
different jobs: the clear frees the space, which an epoch bump alone would never do for records without
a TTL (nothing would come looking for them), while the epoch closes the MVCC window — a read
transaction opened before the flush still sees the old snapshot, and comparing those records against
the new epoch is what stops them being served.

### Reclamation

Correctness is instant; disk space is not. A `tagidx` sub-database is maintained on write. On a
generation bump, a job is enqueued in a `jobs` sub-database recording `{ tag_id, target_generation,
resume_cursor }`. The reclaimer walks the tag's entries in bounded batches, re-checks each record, and
deletes the dead ones plus their index entries — checkpointing its cursor so a restart resumes rather
than restarts. It shares the writer's transaction with the sweeper and yields to user traffic under load.

**Two corrections found while implementing this (M2):**

1. **`tagidx` is a compound key, not `DUPSORT`.** The original design mapped `tag_id → [user key]` as
   duplicates. LMDB can seek to a *key* but offers no way to seek to a position *within* a duplicate
   list, so resuming a half-finished job meant re-walking every duplicate already processed —
   quadratic for a large tag. The layout is now `tag_id u32 BE || xxh3_64(user key) BE → user key`,
   which preserves the ordering and makes resumption an O(log n) range seek. The key is hashed because
   LMDB caps keys at 511 bytes and a prefix plus a full-length user key would not fit; a collision
   costs one unreclaimed record, never a wrong answer.
2. **Deadness is judged against the job's `target_generation`, never the live registry.** A reclamation
   pass can share a transaction with the `DELETE_BY_TAG` that queued it, and the in-memory generation
   is only published *after* that commit — so the registry still reads the old value during that pass.
   Judging from RAM there marks every record live, advances the cursor past them, and leaks them
   permanently. This was caught by a test and is now a regression test in its own right.

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

**Three corrections found while implementing this (M4):**

1. **Every record is indexed, including those that never expire.** The original design only indexed
   records with a TTL, which meant a record with no TTL could never be chosen as a victim — a cache of
   TTL-less keys would fill up with nothing to free. They now go in the `u64::MAX` bucket, which the
   sweeper never reaches (it is always in the future) but the evictor does, last. This changed the
   on-disk layout, so `SCHEMA_VERSION` went to 2 and a version-1 database is refused rather than opened
   under-indexed.
2. **Utilisation is measured from non-free pages, not the high-water mark.** LMDB never returns freed
   pages nor lowers `last_page_number`; deleted pages go on a free list. Measuring with the high-water
   mark meant pressure could only ever rise, so the first time the cache crossed the hard watermark the
   evictor ran until it had deleted everything. It now sums the sub-databases' own page counts, inside
   the transaction it already holds.
3. **`MDB_MAP_FULL` needs its own recovery path.** A failed operation invalidates the whole
   transaction, and the batch that hit the wall was aborted along with the maintenance pass that would
   have freed space — so every later write hit the same wall forever. A capacity failure now marks the
   shard critical (so callers are refused before they reach the queue) and runs a reclaim in a fresh
   transaction, retrying with smaller batches because deletions need pages of their own.

**A minimum map size of 16 MiB per shard is enforced.** Below roughly 4 MiB, LMDB can report a full map
permanently even after everything has been deleted, because the free list needs pages to record freed
pages and there are none to spare. Measured on a sustained overfill of 4 KiB values: 2 and 3 MiB wedge,
4 MiB limps, 6 MiB and up recover cleanly.

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

A Cargo workspace. The boundary that matters: **`vash-core` defines the domain; `vash-store` and
`vash-proto` are interchangeable adapters on either side of it.** Both protocols compile down to the
same `Command`/`Reply` types, so the storage engine has no idea which wire format a request arrived on,
and the protocol crates have no idea what LMDB is.

```
vash-server/
├── Cargo.toml                  # workspace
├── crates/
│   ├── vash-core/             # domain. no I/O, no async, no dependencies of consequence
│   │   ├── key.rs              # Key newtype, length validation (LMDB max key = 511 bytes)
│   │   ├── value.rs            # Value, size limits
│   │   ├── record.rs           # on-disk record header layout, liveness check (zerocopy)
│   │   ├── ttl.rs              # Ttl, Expiry, coarse Clock
│   │   ├── tag.rs              # TagId, TagName, TagRegistry trait
│   │   ├── command.rs          # Command / Reply — THE boundary type
│   │   └── error.rs
│   │
│   ├── vash-store/            # storage adapter
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
│   ├── vash-proto/            # wire adapters. pure codecs: bytes in, Command out
│   │   ├── vcp/                # native binary protocol
│   │   │   ├── frame.rs        # header (zerocopy)
│   │   │   ├── decode.rs
│   │   │   └── encode.rs
│   │   ├── memcached/
│   │   │   ├── text.rs         # classic commands
│   │   │   ├── meta.rs         # mg/ms/md/mn/me/ma + tag extension
│   │   │   └── encode.rs
│   │   └── detect.rs           # first-byte protocol detection
│   │
│   ├── vash-server/           # the binary: everything with a socket or a thread
│   │   ├── main.rs
│   │   ├── config.rs
│   │   ├── listener.rs         # accept loops, SO_REUSEPORT
│   │   ├── conn.rs             # per-connection state machine, pipelining, backpressure
│   │   ├── dispatch.rs         # Command → shard routing, batch fan-out/fan-in
│   │   ├── resp.rs             # Redis commands, composed from Store operations
│   │   ├── cluster/            # peer list, tag fan-out, anti-entropy (§10)
│   │   ├── admin.rs            # /metrics, /health, /stats
│   │   └── shutdown.rs         # drain, final sync
│   │
│   ├── vash-client/           # Rust VCP client. also the integration-test driver
│   └── vash-bench/            # divan/criterion harnesses
│
├── fuzz/                       # vcp_decode, memcached_text, memcached_meta, resp_decode, record_header
├── tests/                      # cross-crate integration + memcached client compat
└── docs/
    ├── project.md
    ├── plan.md                 # this file
    ├── protocol.md             # normative VCP spec
    ├── opcodes.md              # per-opcode implementation contract
    ├── auth.md                 # authentication design (M9, not built)
    ├── storage.md              # on-disk format, sub-database schemas
    └── operations.md           # tuning, capacity planning, failure modes
```

**On-disk record layout** (`vash-core/record.rs`), 28-byte header, zero-copy value slice:

```
version u8 | tag_count u8 | _reserved u16 | epoch u32 | mc_flags u32
expires_at_ms u64 | cas u64
[ tag_id u32, tag_generation u64 ] * tag_count      // 12 bytes each
value bytes                                          // returned as a borrowed slice
```

`mc_flags` is the memcached 32-bit client flags field, stored so a value written by a VCP client and
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

**As implemented (M1):** writes go to a dedicated writer thread as described below, but reads run on
the async runtime's **bounded blocking pool** rather than a hand-rolled reader pool. That already
satisfies the property this section is about — a page fault stalls a blocking thread, never a runtime
worker — with less machinery. The consequence is that `server.max_blocking_threads` is the ceiling on
concurrent readers, and therefore on LMDB reader slots in use: `store.max_readers` must exceed it or
reads fail with `MDB_READERS_FULL` under load. Startup refuses a config where it does not.

**Two corrections found while measuring this (M6):**

1. **`read_txn_without_tls()` was the single largest cost in the system, and it was paying for a
   design that was never built.** The flag exists to make a `RoTxn` `Send` so the hand-rolled reader
   pool above could move one between threads. That pool was replaced by the blocking pool in M1,
   where a transaction is created and dropped inside one call and never crosses a thread — so
   nothing needed it. What it cost: without thread-local storage, every `mdb_txn_begin` claims a slot
   in a shared reader table behind a process-wide mutex, which turns the read path from lock-free
   into serialised. The lookups really are lock-free, as this section claims; the *transaction around
   them* was not. Measured by `examples/txn_bench`: **344k lookups/s on one thread falling to 91k on
   sixteen without TLS, against 948k rising to 5.3M with it** — a 58× difference at sixteen threads,
   and the difference between negative and linear scaling. Removing the flag took reads through the
   whole server from 86k to 1.13M ops/s. The cost of the fix is that a thread holds its reader slot
   until it exits, which is exactly what the `max_readers > max_blocking_threads` rule above already
   guarantees room for.

2. **The hand-off turned out not to matter, once the transaction cost above was gone.**
   `store.inline_reads` — which this section proposed and defaulted off — now exists, and measures
   *within noise* of the hand-off: 1.18M against 1.28M ops/s on `GET`, against a run-to-run variance
   of roughly ±25% on a box where the load generator is competing with the server. It stays off by
   default and stays available, because what it removes is a property of the platform's thread
   wake-ups rather than of this code, and because the premise it trades on — a resident working set
   — is the operator's to assert. Writes never take it: they wait on the writer queue by design.

   This is worth recording as a **near-miss**. The hop was the obvious suspect and the first thing
   measured; it looked like a 30% win in one campaign and turned out to be an artifact of a disk that
   had quietly filled up. The genuine cause was one line of environment setup, and the only reason it
   was found is that `examples/txn_bench` measured the transaction and the lookup separately instead
   of measuring "a read".

**`SO_REUSEPORT` was not adopted, and the decision is recorded rather than
measured (M10).** The plan proposed several accept loops on one port, each with
its own listener socket, so the kernel spreads incoming connections across them.
What that buys is *accept* throughput, and accept is not on this server's hot
path: a cache client library opens a pooled connection and keeps it, so a busy
node accepts a handful of connections a minute and serves millions of requests a
second over them. Sharding the accept loop would divide a number that is already
near zero.

It stops being the right call for a workload with connection churn — short-lived
clients, a proxy that does not pool, or a serverless caller that reconnects per
request. The tell is `vash_connections_total` climbing at a rate comparable to
`vash_commands_total`; today it is orders of magnitude below it. Recording this
rather than measuring it is deliberate: measuring would mean building the sharded
listener first, and the number that would justify building it is one the existing
connection counter already reports.

Requests are also handed over **in batches rather than one at a time**: whatever complete frames a
single read produced cross to the storage tier together. Pipelining is what makes that worth having,
and it costs the unpipelined case nothing.

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

**Measured (M4): sharding helps far less than this section assumed.** Two effects the plan did not
anticipate, both visible in the benchmark:

1. **Sharding and group commit pull against each other.** At a fixed offered write rate, splitting
   across N queues divides the mean batch size by roughly N — measured falling from ~61 on one shard to
   ~2 on eight — so each shard's commits amortise over fewer operations and some of the gain from N
   writers is handed straight back. It therefore pays off with *offered load*, not with shard count
   alone: at 20k writes the gain peaked at 4 shards and regressed at 8; at 200k it kept climbing.
2. **It only helps when the writer thread is the bottleneck.** With syncing off, 8 shards gave 1.5×.
   With syncing on every commit — `relaxed`, which was the default when this was measured — throughput is set by the disk, and splitting one device
   between more environments fragments its I/O and makes things *worse*: 12.4k ops/s at one shard
   against 10.6k at eight, and `durable` fell from 10.9k to 5.5k. Sharding cannot fix a disk.

It also does nothing for reads, which were never the constraint: LMDB readers are already lock-free and
concurrent within one environment.

The default is therefore `min(num_cpus, 4)` rather than the 8 this section originally proposed.

**Re-measured under `lazy` (2026-08-15), and lowered again to `min(num_cpus, 2)`.** Both effects above
were measured while commits waited for the device. `lazy` stops them waiting, which removes the benefit
of a second writer and leaves effect 1 — the batch division — in full. On a four-core container,
`SET`-only, medians of three alternating rounds: at pipeline 1, one shard reaches 66,605 ops/s against
four shards' 24,901, and the mean batch falls from 42.3 to 1.9, which is group commit doing nothing.
At pipeline 16 two shards lead at 140,767 against four's 126,292 and eight's 47,554. Two is the only
count not beaten by four in either shape. See `docs/performance-proposals.md` §9.

Consequences, accepted deliberately:

- **Multi-key operations are grouped by shard and executed in parallel**, then fanned back in. With
  out-of-order VCP responses this is a throughput *gain*, not a cost.
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
| `relaxed` | `MDB_NOMETASYNC` + periodic `force_sync` | Loses at most the last few transactions on OS crash; **cannot corrupt the database**. |
| `lazy` **(default)** | `MDB_NOSYNC` + periodic `force_sync`, never `MDB_WRITEMAP` | Loses writes newer than `write.sync_interval_ms`; **cannot corrupt the database** where the filesystem preserves write order. Measured 1.7–4.5× faster than `relaxed`, because the per-commit `fsync` is 92% of commit time and the writer queue backs up behind it — `docs/performance-proposals.md` §9. |

**`ephemeral` was retired as a durability mode.** It was `MDB_NOSYNC` plus
`MDB_WRITEMAP`, and that flag measured slower than going without twice, so what it
named was `lazy` with a worse guarantee. `--ephemeral` now means `lazy` durability
plus `wipe_on_start` — a startup policy — and `store.write_map` carries the flag
for the memory profile it genuinely buys. That, and support for placing the
database on tmpfs, cover the brief's "in-memory caching" option without a separate
code path.

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

**As implemented (M5):** a node gossips with **every peer on its own timer**, not with one sampled
peer per interval. Sampling is the right shape when membership is large and discovered; here it is a
static list of a handful of addresses, and the shared loop had a flaw worth more than the saving.
Because a round is awaited inline, an unresponsive peer blocked the loop for its whole timeout — so
*one* node being down slowed convergence between all the healthy ones. Measured against a killed
node, gossip fell from one round a second to roughly one every three, and the reachability metric
stayed at "all peers reachable" throughout, because the connect-timeout path returned before the flag
was cleared. Both are fixed: a task per peer, and reachability settled once from the round's outcome
rather than on each failure path. The bound improves too — every peer every interval, rather than
every `peers × interval`.

The first round is immediate rather than one interval in, so a restarted node converges now rather
than later. Digests carry only tags with a non-zero generation, since one that has never been
invalidated anywhere says nothing; a node with more than 8192 such tags sends a rotating window
instead of its whole table, which converges in more rounds rather than not at all.

**Consistency statement, stated plainly in the docs:** tag invalidation is *strongly consistent within a
node* and *eventually consistent across the cluster*, with a staleness bound of the gossip interval in
`fanout` mode. For a cache this is the correct trade; for anyone who needs better, `fanout_sync` narrows
it to the acknowledgement round-trip.

**Four things found while implementing this (M5):**

1. **A node's tag generations have to be uniform across its own shards.** Ids are per shard, and a
   shard only registers a name when a record carrying it lands there — so one shard could hold
   `news` at generation 4 while another had never heard of it. Locally that is harmless, because a
   record is only ever compared against its own shard's registry. It stops being harmless the moment
   the node has to export **one** number for the name: a shard registering the tag later would start
   at 0, records written there would capture 0, and the first gossip round back would invalidate
   records written after the last invalidation. Tag creation now adopts the node-wide generation for
   the name, and an invalidation levels every shard to the same target.
2. **A peer that has never seen a tag must register it at the received generation**, for the same
   reason and by the same mechanism. This is why fan-out messages carry `(name, generation)` and not
   just the name.
3. **`fanout` has a write-side window the plan did not name.** A record written on node B after node
   A invalidated a tag, but before the message reaches B, captures B's *old* generation and dies when
   the message lands. It is not a staleness bound in the "still serving old data" direction — it is
   the opposite, an invalidation that reaches slightly too far. It errs towards a miss, which is the
   safe direction for a cache, and `fanout_sync` closes it for reachable peers. Documented rather
   than fixed: fixing it would need a cross-node clock.
4. **Idle connections made a clean shutdown impossible.** The drain waited for every in-flight
   connection, and peers keep theirs open indefinitely, so a clustered node hit the drain timeout
   every time and left its LMDB environment open. Connections are now released at the point they are
   waiting for a request — nothing buffered, no reply outstanding — which is the only place it is
   safe. This was pre-existing (any idle client did it) but a cluster makes it the normal case.

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
path = "/var/lib/vash"
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
fanout_timeout_ms = 2000          # per exchange, and how long fanout_sync waits
queue_depth = 1024                # invalidations queued per peer before dropping

[protocol]
memcached = true
flush_all_enabled = false         # off by default: it is a remote cache-wipe primitive
# Authentication is not built. It gets its own `[auth]` section rather than a
# key here; the design is in auth.md and this sketch's `auth_secret` is dropped.

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
| **Fuzzing** | `cargo-fuzz` on `vcp_decode`, `memcached_text`, `memcached_meta`, `resp_decode`, `record_header`. **Non-negotiable** — these parse bytes from unauthenticated clients. Run in CI on every PR, plus a long-running nightly. |
| Integration | Real server over a real socket via `vash-client`; TTL expiry, tag invalidation, eviction under pressure, restart persistence |
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

### Measured (M6)

Not the hardware the goals were written for: a 12-core Windows 11 dev box, NVMe, with the load
generator **on the same machine** competing for the same cores and talking over loopback. That
inflates nothing and depresses two things badly — closed-loop latency, and any throughput number
that moves a lot of bytes. Read them as a floor.

Run-to-run variance is roughly ±25% between identical runs, for the same reason, so these are
magnitudes rather than figures.

| Goal | Measured | Verdict |
|---|---|---|
| GET ≥ 1M ops/s | 2.04M at 64 B, 1.82M at 256 B, 0.92–1.03M at the 1 KiB the goal names | Met, though 1 KiB straddles the line |
| GET p99 < 1 ms at 500k ops/s | p99 0.66 ms — but at 35k ops/s. Closed-loop throughput on one box is bounded by round trips | **Half met, not measurable as stated** |
| SET ≥ 250k ops/s | ~40k with syncing off; 12.4k in `relaxed` (M4) | **Not met** — see below |
| `DELETE_BY_TAG` < 1 ms | 197µs–1.77ms, flat across 100 → 500,000 keys (M4) | Met |
| Cold start < 1 s for 10 GB | Not measured at that size | Unverified |

**The SET goal was wrong, and the honest revision is to say so rather than to keep chasing it.**
250k writes/s through a copy-on-write B-tree that fsyncs would need the commit cost to vanish, and
M4 already measured where it actually goes: with syncing on, throughput is set by the device, and
sharding makes it *worse* by fragmenting one disk between more environments. Group commit is doing
its job — throughput tracks batch size exactly as §9 predicted — but the ceiling is the storage
engine and the disk, not the code above them. A realistic goal for this design on this hardware is
tens of thousands of writes a second in `relaxed`, and the number to state alongside it is the
read-to-write ratio a cache actually sees: at 9:1 the mixed workload measured 271k ops/s.

**The GET goal is met.** Throughput falls with value size — 337k ops/s at 4 KiB — but that is data
movement rather than per-request work: it is 1.4 GB/s of value bytes leaving the process with the
client reading them on the same memory bus. At the time of measurement the value was still copied
three times on the read path (out of the map, into the reply, into the write buffer) where §8
promised one. Removing the first means encoding the response while the read transaction is still
open, which trades away the clean `Store`/`Reply` boundary; it was the obvious next move and was
deliberately not made **during M6**. It has since been taken — see *Since M6* below.

**What M6 actually found**, in order of size:

1. **`read_txn_without_tls()` was the ceiling on every read**, and it was there to serve a design
   that was never built. 86k → 1.28M ops/s through the whole server. See §9.
2. **The native protocol is about 6× cheaper to parse than the text one** — 15.6ns against 99.8ns to
   decode a `GET`, and 67ns against 452ns to encode a 1 KiB hit. That is §3's "text framing costs",
   measured. Most of the encoding half turned out not to be framing at all — see *Since M6*.
3. **The hot path is otherwise where it should be**: parsing a record is 12.5ns and flat in tag
   count, the liveness check is 1.5ns untagged and 16.8ns with the full 32 tags, and decoding a
   `SET` does not scale with the value — the zero-copy claims in §8 hold.
4. **Two expected wins were not wins.** The hand-off to the storage tier (§9) and `mimalloc` (§2)
   both measured within noise. Recording that is worth as much as recording the one that worked:
   both were plausible, both had a number attached in advance, and neither survived being measured.

### Since M6

Three changes to the read path, each measured against the arm it replaced rather than end to end —
a loopback run on this box has ±25% between identical runs, which is wider than any of them.

| Change | Measured |
|---|---|
| Render a `GET` hit straight out of the store, inside the read transaction (`Store::get_with`) | ~200ns at 1 KiB, ~400ns at 4 KiB, plus the allocation every hit made |
| Spell reply integers into a stack buffer instead of a `String` each | memcached `VALUE` line with cas 174–187ns → 43–47ns; RESP bulk 75–82ns → 22–26ns |
| Fuse the single-key plain memcached `get`, the one every client library sends | 384ns → 230ns at 1 KiB, of which ~75ns is the batch reply's vector and is flat in value size |
| Take the tag registry's lock only when a record carries tags | ~15–25ns, single-threaded. The contention it was meant to remove did not measure at all — see below |
| Hold a one-key retrieval's key inline instead of in a `Vec` (`KeyList`) | memcached `get` parse 93–116ns → 48–49ns, against an unchanged control at 46–50ns. ~100ns per command, because the parse happens twice |
| Hold a RESP command's arguments inline instead of in a `Vec` (`Args`) | `GET` 102–105ns → 66–75ns, `SET` 181–186ns → 145–163ns, interleaved. ~35ns and ~20ns per parse, doubled per command. Approximate: see the control note |

**The second one is the correction to M6's finding 2.** The text encoders looked expensive because
they were measured as framing, and roughly 45ns of each integer they wrote was a malloc and a free
rather than any part of the format. A `VALUE` line spells three. What is left after removing them is
the framing cost §3 actually predicted, and it is a good deal smaller than 452ns.

**The fourth one is a recorded non-win**, in the sense §13's finding 4 already uses. The tag
registry's lock is shared across every reader on a shard, so acquiring it per read should have been
a contended cache line, and `read_bench` scaling only 1.14x from four threads to eight looked
exactly like one. It is not: interleaved A/B over four pairs shows no difference at any thread
count. The single-threaded saving is real and is just the uncontended acquire. Whatever bounds
concurrent reads on this box, it is not that lock, and finding out what does is open.

**The largest remaining cost on the text path is that a block is parsed twice.**
`conn::measure_memcached` runs the full parser to find where each command ends, and the executor
runs it again to execute — so a `get` pays ~50ns of decode twice, and every `set` pays its own
decode twice. `KeyList` halved what that duplication costs but did not remove the duplication.

The fix is not a second, cheaper scanner. Boundaries for the text dialects are decided by the parser
today, and that is a **security property rather than an accident**: a framing pass that disagreed
with the decoder about where a command ends is precisely a request-smuggling bug, and these are the
bytes plan §13 calls out as coming from unauthenticated clients. Splitting `parse` into a `frame`
step and a `decode` step that *shares* the boundary logic would be sound — one definition, two entry
points — and is the shape to reach for. It is a refactor of a fuzzed parser and was not attempted
here. VCP already has this shape and pays nothing: `peek_frame_len` reads the length header, and the
body is decoded once.

### Measuring on this box, which took three tries to get right

Every one of these changes is 20–150ns on a path that costs a few hundred, which is below what any
end-to-end run here can resolve. Three separate ways of getting that wrong turned up in one session,
and all three produced *plausible* numbers rather than obviously broken ones.

**Divan's default sampling cannot see them.** An LMDB read transaction throws an occasional 20µs
outlier, which collapses the auto-tuned sample size to a single iteration; the 100ns timer then
quantises every arm to the same figure and small differences vanish into rounding. The read-path
arms pin `sample_size = 500` for that reason.

**Sequential A/B manufactures results.** Running every round of B and then every round of A reported
the tag-lock change as +65% at eight threads and +130% at one. Interleaving the same two binaries
pair by pair reported no difference — and the *unchanged* arm's own throughput moved 2.6x between
the first run of the session and the fourth, which is the whole of the effect. Anything compared
here has to alternate, and an untouched control has to be one of the things measured.

**A benchmark can be too coarse for the thing it is pointed at.** `read_bench` builds its key with
`format!` inside the measured loop, so a per-operation cost in microseconds sits on top of every
figure it reports. It answers what it was written to answer — whether reads scale with threads — and
cannot answer whether a 20ns change helped.

**An untouched control still moves, and the amount is worth knowing.** `memcached_parse_delete` sits
in both binaries of the RESP argument change and is not on any path it touches; across three
interleaved pairs it moved from ~42.7ns to ~38.9ns, consistently and in the same direction. Nothing
in that parser changed, so it is code layout shifting under a smaller binary. **Roughly 4ns, or
about 9%, is the resolution floor here** — a measured difference of that order is not a result no
matter how many times it repeats, and every figure in the table above should be read as carrying it.
This is also the argument for a control that is *in the same process* rather than a second run: the
memcached/`KeyList` measurement is the strongest of the set precisely because its control sat beside
it in one binary and did not move.

**And the disk filled again.** `read_bench` refused to populate with `os error 112`: the system
drive had 4.7 MB free, and `TEMP` is on it, so every store-backed benchmark in this session had been
running against a full disk. The pure-memory encoder measurements were unaffected and the read-path
ones re-measured the same on a drive with room, so nothing above rests on it — but that is luck,
established afterwards, and the note below is now something that has happened twice.

**A methodological note, because it nearly produced wrong numbers.** An earlier campaign measured
`inline_reads` as a 30% win and GET at 648k. Both were artifacts of a disk that had silently filled
to 26 MB free during the run. The tell was that re-running a single measurement after freeing space
moved it by 60%, which no code change explains. Benchmarks on a shared machine need the machine
checked as well as the code — and any result that survives only one run is not a result.

---

## 14. Milestones

| # | Scope | Exit criteria |
|---|---|---|
| **M0** | Workspace, config, `Store` trait, LMDB env, single-shard GET/SET/DELETE, VCP framing, `vash-client` | End-to-end round-trip over a socket; CI green |
| **M1** | TTL: `exp` index, sweeper, lazy checks, `TOUCH`; batch ops; group commit | Expired keys never served; sweeper reclaims within one interval; write throughput scales with batch size |
| **M2** | Tags: registry, generations, `DELETE_BY_TAG`, `tagidx`, resumable reclaimer; global epoch/`FLUSH` | `DELETE_BY_TAG` is O(1) under benchmark; reclaimer resumes correctly across restart |
| **M3** | Memcached text + meta protocols, first-byte detection, tag flag extension | Real-client compat suite passes against both our server and memcached |
| **M4** | Sharding, capacity watermarks and eviction, full metrics, admin endpoints | Throughput scales with shard count; server survives a sustained overfill |
| **M5** | Cluster: peer list, tag fan-out, anti-entropy gossip, `CLUSTER` opcode | Invalidation converges across a 3-node cluster including a partitioned/restarted node |
| **M6** | Perf hardening, fuzz corpus, benchmark suite, packaging (static musl binary, Docker, systemd), docs | Performance goals in §13 met or consciously revised with data |
| **M7** | Redis protocol: RESP2/RESP3 framing, the string and expiry command subset, `HELLO` negotiation | Real Redis clients drive the supported commands unchanged; the read-modify-write seam is documented rather than hidden |
| **M8** | Listing: `LIST_KEYS`/`LIST_TAGS`, glob matching, cursor pagination, the `listing_enabled` gate and `LISTING` capability bit | Paging a sharded keyspace is linear, not quadratic, and returns every key present throughout the walk exactly once; no request holds a read txn past its scan budget; disabled by default, and the cursor and pattern are fuzzed |
| **M9** | Authentication: credential table, `AUTH` in all three dialects, the pre-auth gate and abuse budget, peer credentials — designed in [auth.md](auth.md) | Real memcached and Redis clients authenticate unchanged; no command in any dialect executes unauthenticated; a cluster converges with auth required, and a node missing a peer credential refuses to start |
| **M10** | Architecture remediation: atomic read-modify-write, one command boundary for all three dialects, a real `Store` seam, module cohesion, the promised observability — planned in [m10.md](m10.md) | `INCR` is atomic in every dialect; RESP is counted and mapped by the same code as the other two; a `Store` fake runs a server test; queue-wait and execution latency are separately visible |
| **M12** | TLS: `rustls` behind a feature flag, termination on a second listener, the connection loop generic over its stream, `ssl_enabled` and `vash_tls` per connection — proposed and measured in [tls-proposal.md](tls-proposal.md) | Phase 0's numbers published and its wrong answers corrected; a 1 MiB pipelined batch completes over TLS in both directions; both ports serve one store; a `tls.listen` in a build without the feature refuses to start |

---

## 15. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| LMDB single writer caps write throughput | High | Sharded environments (§9) + group commit; `Store` trait keeps `libmdbx` a contained swap |
| Copy-on-write write amplification under high churn | **Materialised** | TTL bucketing to cluster index writes; batch commits; measure early in M1 — **this is the number to watch first**. It was, and it is the binding constraint: 0.23 ms per record beyond the commit's fixed cost, capping writes near 17,400/s however well they batch. Decomposed and costed in [performance-proposals.md](performance-proposals.md) |
| Long-lived read txn blocks page reuse ⇒ unbounded file growth | High | Per-batch txn renewal; `oldest_reader_age_ms` metric with an alarm; documented in operations.md |
| `map_size` is fixed at open and cannot grow while txns are live | Medium | Generous sizing (verified not preallocated, §11); capacity metrics and watermarks well before the map fills |
| `MDB_WRITEMAP` unusable on Windows (measured, §11) | Low | `ephemeral` mode drops the flag on Windows; dev parity is otherwise unaffected |
| Two hand-written parsers on untrusted input | High | Continuous fuzzing from M0; strict length caps before any allocation; no `unsafe` in the parsers |
| Cache traffic is readable, and modifiable, on the wire | **Was High, now optional** | Should have been a row here from the start. TLS on a second port (M12) covers it; the credential M9 added is itself plaintext without it. Off by default, so a deployment that does not turn it on still carries the risk |
| Memcached compat subtly wrong in ways clients notice | Medium | Differential testing against real memcached with real client libraries (§13) |
| Tag fan-out silently drops invalidations during a partition | Medium | CRDT max-merge makes retries free; anti-entropy bounds staleness; `fanout_sync` for stricter needs |
| Tag registry grows unboundedly if clients generate unique tags | Medium | Registry size metric; configurable cap with `BAD_REQUEST` past it; GC for tags with no live records |
| `ephemeral` mode corruption on power loss | Low (by design) | Integrity check at boot, wipe-and-continue; documented as cache-only and never the default |

---

## 16. Non-goals

Stated so they do not get re-litigated: no replication, no consensus/Raft, no server-side proxy or
sharding tier, **no UDP** (amplification vector), no Lua or server-side scripting, no pub/sub, no
secondary indexes or query language beyond tags, no multi-key atomicity across shards, no on-disk LRU
(§6), and no legacy memcached binary protocol (§7).

**TLS was on this list and no longer is.** It shipped as `rustls` behind the `tls` feature, off by
default, terminating on a second port — the escape hatch this section always named, cashed in
[tls-proposal.md](tls-proposal.md) and measured in [benchmarks.md](benchmarks.md#what-tls-costs).
Client certificates, cluster peers over TLS and certificate reload are not built.
