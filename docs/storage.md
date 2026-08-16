# Storage format

What is on disk, and why. Enough to read a database with an independent tool,
to reason about what a corrupt one would look like, and to change the format
without breaking every deployment.

The design rationale lives in [plan.md](plan.md) §4–§6 and §8; this describes
the bytes.

**Schema version: 2. Record version: 1.**

---

## Two engines, one format

Everything below describes the **logical** format — the sub-databases, the key
encodings, the record layout — and it is identical on both engines. What differs
is the file the engine writes it into, and the engines cannot read each other's.

| | `store.backend = "lmdb"` (default) | `store.backend = "mdbx"` |
|---|---|---|
| Files | `data.mdb`, `lock.mdb` | `mdbx.dat`, `mdbx.lck` |
| `map_size_mb` | A fixed reservation; the file is sparse | A **ceiling** the file grows toward and can shrink back from |
| `max_readers` | Exact | A floor — mdbx rounds the reader table up to fill its pages |
| `lazy` durability | `MDB_NOSYNC`, and never `MDB_WRITEMAP` | `MDBX_SAFE_NOSYNC`, which keeps its integrity guarantee without LMDB's caveat about filesystem write ordering, and composes with `WRITEMAP` |
| Pinning the map | Linux only | Every platform, via `mdbx_env_warmup` |

**Opening a database with the wrong engine is refused**, by looking for the
other one's data file. The alternative is worse than a crash: mdbx would create
an empty database beside the LMDB one, so the old data would sit on disk taking
up space while every read missed — the same silent, total cache loss the
[shard count](#shard-count-is-fixed) check exists to prevent. Changing
`store.backend` therefore means wiping the directory.

mdbx requires a binary built with the `mdbx` feature; without it the setting is
refused at startup rather than quietly served by LMDB. The rationale for
carrying two engines at all, and the measurements behind the default, are in
[mdbx-proposal.md](mdbx-proposal.md).

## Layout on disk

One directory per shard, each a complete, independent environment:

```
<store.path>/                 a single-shard store, or
<store.path>/shard-0/         shard N of a multi-shard store
<store.path>/shard-1/
    data.mdb                  the memory-mapped database (mdbx.dat under mdbx)
    lock.mdb                  the reader table and write lock (mdbx.lck)
```

A single-shard store uses `store.path` directly, so a database created before
sharding still opens and the common case has no extra directory level.

**Nothing is shared between shards.** A key belongs to exactly one, chosen by
`xxh3_64(key) % shards`, and each has its own writer thread, its own sequence of
CAS tokens and its own tag ids. That is what makes N shards N concurrent
writers; it is also why the shard count is fixed once a database exists — see
[shard count](#shard-count-is-fixed).

## Sub-databases

Each environment holds six named sub-databases. `max_dbs` is 16, sized for the
whole plan rather than for what exists today, because it is fixed at environment
creation.

| Name | Key | Value | Purpose |
|---|---|---|---|
| `main` | user key | [record](#the-record) | The data. |
| `exp` | `bucket u64 BE ‖ cas u64 BE` | user key | [Expiry and eviction order](#the-expiry-index). |
| `tagidx` | `tag_id u32 BE ‖ xxh3_64(key) BE` | user key | [Which keys carry a tag](#the-tag-index). |
| `tags` | tag name | `tag_id u32 LE ‖ generation u64 LE` | [The tag registry](#the-tag-registry). |
| `jobs` | `tag_id u32 BE` | `target_generation u64 LE ‖ cursor?` | [Reclamation checkpoints](#reclamation-jobs). |
| `meta` | see below | fixed-width integers | Schema version and counters. |

Big-endian where the byte order has to *be* the sort order — LMDB compares keys
as byte strings, so a big-endian integer key sorts numerically. Little-endian
everywhere else, matching the wire format and the host.

## The record

`main` maps a key to one blob, 28-byte header then an optional tag table then
the value. All integers little-endian, all fields unaligned.

```
offset  size  field
     0     1  version           record format version (1)
     1     1  tag_count         0..=255; policy caps it lower, default 32
     2     2  reserved          zero
     4     4  epoch             flush epoch at write time
     8     4  mc_flags          memcached client flags, stored verbatim
    12     8  expires_at_ms     absolute unix ms; 0 = never
    20     8  cas               version counter and memcached CAS token
    28    12  tag_ref[0]        tag_id u32, generation u64
   ...   ...  tag_ref[n]
 28+12n   ..  value             to the end of the blob
```

Two properties drive this:

1. **The value is a suffix**, so reading it is a subslice of the memory map —
   no parsing, no copy.
2. **Liveness is decidable from the header alone**, against RAM-resident state
   only. A read never needs a second lookup to know whether a record is valid:

```
alive = record.epoch == current_epoch                        (flush)
     && (expires_at_ms == 0 || expires_at_ms > now_ms)        (TTL)
     && every (id, gen) matches the registry's current gen    (tags)
```

That is what makes lazy TTL and O(1) tag invalidation possible at all. An
expired, flushed or invalidated record stays on disk until a background pass
removes it, and is never served in the meantime.

The unaligned little-endian integer types are load-bearing: with native `u32`
and `u64` the compiler would insert padding — a `tag_ref` would be 16 bytes, not
12 — and the header could not be cast from an arbitrary offset in the map.

## The expiry index

```
key   = bucket u64 BE ‖ cas u64 BE
value = user key
```

Big-endian, so LMDB's byte order is time order and a cursor from the start of
`exp` yields records in expiry order. That single property serves two features:
the sweeper walks forward while `bucket <= now`, and the evictor walks forward
regardless — soonest-to-expire first, which is why eviction is TTL-ordered
rather than LRU.

**`bucket` is `expires_at_ms` rounded up to `bucket_granularity_ms`** (default
1s). Coarser buckets cluster index entries into shared B-tree pages, which cuts
the write amplification a copy-on-write tree incurs when TTLs are spread across
a wide range. Precision is unaffected: expiry is decided by the record's exact
`expires_at_ms`, never by the bucket.

**Records that never expire are indexed too**, in a `u64::MAX` bucket. The
sweeper never reaches them, because that bucket is always in the future; the
evictor reaches them last, after everything with a TTL. Without this a cache of
TTL-less keys would fill up with nothing it was allowed to free — which is why
this changed the schema version to 2 and a version-1 database is refused rather
than opened under-indexed.

**`cas` is in the key** so that overwriting a record cannot orphan the entry
that described its predecessor. Before deleting, a pass compares the record's
current `cas` against the entry's; a mismatch means the entry is stale, so the
entry is dropped and the record left alone. Writes stay a single insert, with no
read-modify-write of the old entry.

## The tag registry

```
key   = tag name (1..=255 bytes)
value = tag_id u32 LE ‖ generation u64 LE
```

Loaded into RAM in full at startup and consulted on every read of a tagged
record. It is small by construction: `store.tags.max_tags` (default 100000)
bounds it, because it lives in memory and a client inventing names would
otherwise be a memory leak.

**Names are the global identity; ids are node-local — per shard, in fact.** An
id is a dense counter that exists only to keep the per-record tag table at 4
bytes instead of a name, and two shards will happily assign different ids to the
same name. That is safe because a record is only ever compared against its own
shard's registry. Cluster messages therefore carry names.

**Generations are held uniform across a node's shards**, which ids are not: a
shard meeting a name for the first time adopts the node-wide generation rather
than starting at zero. The node reports one number per name to its peers, and it
has to mean the same thing in every shard — see [protocol.md](protocol.md#cluster).

Invalidation is `generation += 1` here plus one durable write. Every record
carrying that tag now compares unequal, so all of them are dead at once, in
constant time.

## The tag index

```
key   = tag_id u32 BE ‖ xxh3_64(user key) BE      (12 bytes, fixed)
value = user key
```

Only for reclaiming space — correctness never consults it. Invalidation makes
records invisible immediately; this is what lets a background pass find them
afterwards and free their pages.

A compound key rather than `DUPSORT`, which the plan originally specified. LMDB
can seek to a *key* but exposes no way to seek to a position *within* a
duplicate list, so resuming a half-finished job would mean re-walking every
duplicate already processed — quadratic for a large tag. A compound key
preserves the same grouping and makes resumption an O(log n) range seek.

The user key is **hashed** because LMDB caps a key at 511 bytes and a 4-byte
prefix plus a full-length user key would not fit. A collision between two keys
under one tag drops one index entry, so that record is not reclaimed
proactively — it stays correct, since reads still check liveness and TTLs still
apply, it just lingers. At 64 bits that needs billions of keys on a single tag.

## Reclamation jobs

```
key   = tag_id u32 BE
value = target_generation u64 LE ‖ cursor [12 bytes, optional]
```

One job per tag, so re-invalidating a tag mid-reclaim raises the target and
restarts the scan rather than queueing a second pass. The cursor is the last
index key processed, written in the **same transaction as the deletions it
describes** — which is what makes a job resumable across a crash or a restart
rather than restartable only from the beginning.

Deadness is judged against the job's own `target_generation`, never the live
registry. A pass can share a transaction with the very invalidation that queued
it, and the in-memory generation is published only after that commit, so the
registry still reads the old value during the pass. Trusting it would mark every
record live, advance the cursor past them, and leak them permanently.

## Meta keys

| Key | Width | Meaning |
|---|---|---|
| `schema_version` | u32 LE | Refused if it is not 2. |
| `record_version` | u32 LE | The record format written by the build that created this. |
| `epoch` | u32 LE | Flush epoch. A record whose epoch differs is dead. |
| `cas_watermark` | u64 LE | Highest CAS value *reserved*, not issued. |
| `shard_index` | u32 LE | This environment's position in the shard set. |
| `shard_count` | u32 LE | How many environments the set has. |

### CAS tokens

Reserved in blocks of 2^20 rather than persisted per write: one meta write per
million tokens instead of one per write. A restart resumes past the whole
reserved block, so an unclean shutdown leaves a gap in the sequence. That is
harmless — a token has to be unique and increasing, never dense.

Each shard counts independently, so the raw counters collide. The token handed
out is `counter * shard_count + shard_index`, which is unique server-wide while
staying strictly increasing within a shard — and therefore within any single
key, which is the only ordering compare-and-swap depends on.

### Shard count is fixed

`shard_count` is validated on open, and a mismatch is a hard error. Silently
accepting a different count would route every key to a different environment:
the data would still be on disk, occupying space, while every read missed. A
total, silent cache loss is worse than a refusal to start.

## Durability

`store.durability` selects the LMDB flags:

| Mode | Flags | On an OS crash |
|---|---|---|
| `durable` | none | Nothing is lost. |
| `relaxed` | `MDB_NOMETASYNC` | The last few transactions may be lost. **Cannot corrupt the database.** |
| `lazy` (default) | `MDB_NOSYNC`, and never `MDB_WRITEMAP` | Writes newer than the last `write.sync_interval_ms` may be lost. **Cannot corrupt the database**, provided the filesystem preserves write order — LMDB's condition, and the reason this mode refuses `WRITE_MAP`. |

`MDB_WRITEMAP` is no longer part of a durability mode. It was welded to
`MDB_NOSYNC` as `ephemeral`, and measured slower than going without twice, so it
is now `store.write_map` — off by default, Unix only (on Windows
`mdb_env_open` fails with OS error 6 at every map size tested), and documented
for what it actually buys: LMDB stops allocating a copy of every dirty page, so
a large transaction has a lower peak footprint. Setting it removes `lazy`'s
integrity guarantee, so pair it with `wipe_on_start` — which together is exactly
what `--ephemeral` now means.

## Reader slots

The environment uses **thread-local reader slots** (LMDB's default; heed's
`read_txn_without_tls()` is deliberately *not* used). A thread holds its slot in
`lock.mdb` until it exits, so the slot table has to cover every thread that can
read at once — which is what the `store.max_readers > server.max_blocking_threads`
check at startup enforces.

The alternative claims a slot per transaction from a shared table behind a
process-wide mutex, which turns the read path from lock-free into serialised.
Measured by `cargo run --release -p vash-store --example txn_bench`: 344k
lookups/s on one thread falling to 91k on sixteen, against 948k rising to 5.3M
with thread-local slots.

A process that dies without releasing its slots leaves them stale; LMDB reclaims
them on the next open. A *live* process that leaks them cannot, which is why
`vash_readers_in_use` is worth an alert.

## Space and its accounting

`used_bytes` is summed from the sub-databases' own page counts, **not** from
LMDB's high-water mark. LMDB never returns freed pages to the OS nor lowers
`last_page_number` — a deleted record's pages go onto a free list for reuse — so
a high-water measure only ever rises. Using it for the capacity watermarks meant
pressure could never fall, and the evictor ran until the cache was empty.

The map is a lazy reservation rather than an allocation, verified on both Linux
and Windows: a 16 GiB map produces a 16 KiB file. A generous `map_size_mb` costs
nothing until the data arrives.

**Minimum 16 MiB per shard**, enforced at startup. Below roughly 4 MiB LMDB can
report a full map permanently even after everything has been deleted, because
the free list needs pages of its own to record freed pages and there are none to
spare. Measured on a sustained overfill of 4 KiB values: 2 and 3 MiB wedge, 4
MiB limps, 6 MiB and up recover cleanly.

## Changing the format

- **Record layout** → bump `RECORD_VERSION` in `vash-core/src/record.rs`. A
  record whose version does not match is rejected by `RecordRef::parse`, so old
  records read as corrupt rather than as garbage.
- **Sub-database arrangement** → bump `SCHEMA_VERSION` in
  `vash-store/src/schema.rs`. A database with a different version is refused at
  open.

Neither has a migration path, and for a cache that is the right answer: the data
is reconstructible by definition, so "refuse to open, let the operator wipe" is
safer and far less code than a migration that has to be correct on a format
nobody is running any more.
