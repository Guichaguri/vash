# Closing the gap with Redis and memcached

[benchmarks.md](benchmarks.md) measured vash beside Redis and memcached on one
box with one client. This document proposes what to do about the result. The
objective it is written against is the ambitious one: **not to narrow the gap
but to end up in front.**

Everything below is costed against measurements taken on the benchmark host
described in that document — a four-core container on a Rancher Desktop WSL2 VM.
Read its [What this cannot tell you](benchmarks.md#what-this-cannot-tell-you)
section before quoting any number here, and note in particular that vash's write
throughput on that host swings by a factor of two between runs. The
*decomposition* below is more trustworthy than any single throughput figure in
it, because it comes from the writer's own counters rather than from the client.

---

## 1. The gap is two different problems

| Workload | when this was written | **now** | Redis | memcached |
|---|---:|---:|---:|---:|
| GET, closed loop | 17,443 | **173,659** ✅ | 50,484 | 130,922 |
| GET, pipelined | 221,380 | **781,427** ✅ | 475,430 | 651,154 |
| SET, closed loop | 5,774 | 8,754 | 55,401 | 148,314 |
| SET, pipelined | 14,140 | 22,754 – 52,237 | 554,785 | 912,234 |

The read rows are [§3](#3-implemented-resident_mode) and the write rows are
[§4](#4-implemented-one-writer-submission-per-block-not-per-command), both since
implemented. The two conclusions below survived them, and one piece of arithmetic
did not — see the correction in §2.

**Reads were not a research problem.** vash was already faster than both servers
on both read workloads with a setting that existed, shipped, and was off by
default. Everything the read section of `benchmarks.md` agonises over was one
flag and the safety argument behind it — now `store.resident_mode`, §3.

**Writes are a research problem**, and the rest of this document is mostly about
why, and what it would actually take. §4 shortened the gap by 5–6× without
touching the storage engine; closing it still means not touching the storage
engine on the request path at all, which is §7.

---

## 2. Where the write time goes

The writer thread keeps its own counters — queue wait, and apply-plus-commit —
and they are exposed on the admin port as
`vash_writer_queue_wait_seconds_total` and `vash_writer_commit_seconds_total`.
Scraping them across three shapes of write load separates the fixed cost of a
commit from its marginal cost per record:

| Offered load | mean batch | commit per batch | commit per record |
|---|---:|---:|---:|
| 4 connections, pipeline 128 | 1.13 | 2.63 ms | 2.32 ms |
| 100 connections, pipeline 1 | 13.77 | 5.45 ms | 0.40 ms |
| 100 connections, pipeline 16 | 17.10 | 6.30 ms | 0.37 ms |

Those three points fit a straight line to within 1.5%:

```
commit_ms ≈ 2.37 + 0.23 × batch_size
```

**A commit costs 2.37 ms before it writes anything, and 0.23 ms for each record
it carries.** Both halves matter, and they fail in opposite directions:

- The **2.37 ms fixed cost** is what group commit exists to amortise, and group
  commit works — mean batch reaches 13–17 under concurrent load, which drops the
  per-record share of it from 2.32 ms to under 0.2 ms. This half is solved.
- The **0.23 ms marginal cost** is not amortised by anything in the data above,
  and appeared to set a ceiling near `4,350 × 4 ≈ 17,400 writes/s`.

> **Corrected by measurement — see [§4](#4-implemented-one-writer-submission-per-block-not-per-command).**
> That ceiling was an artefact of the fit. Every point above was taken when a
> job carried exactly **one** record, so "per record" and "per job" were the same
> column and the fit could not tell them apart. With writes coalesced, a batch of
> 89 records costs 0.174 ms each rather than the 0.37 ms the two-term model
> predicted, and pipelined `SET` measures well past 17,400. **The fixed cost is
> per commit, and a large part of what looked marginal was per queue hop.** The
> genuinely per-record component is far smaller — near the 0.084 ms the
> syncing-off floor below implies.

What survives the correction is the shape of the problem rather than the number:
a commit has a large fixed cost, group commit is what pays it down, and before
§4 the server could only form a batch out of *separate connections*.

### The floor is not durability

The obvious suspect is `fsync`. It is not the main one. `--ephemeral` turns
syncing off entirely — `MDB_NOSYNC` plus `MDB_WRITEMAP` — and measures 47,656
pipelined `SET`, which back-solves to a marginal cost of **0.084 ms per record**
and a ceiling near 47,600. That matches the measurement to three digits.

So the write gap decomposes as:

| Layer | Marginal cost per record | Implied ceiling |
|---|---:|---:|
| vash, `relaxed` (default) | 0.23 ms | ~17,400/s |
| vash, `ephemeral` (no syncing at all) | 0.084 ms | ~47,600/s |
| Redis, measured | — | 554,785/s |
| memcached, measured | — | 912,234/s |

**Durability is 2.7× of the gap. The storage engine is the other 12–19×.** Even
with every sync disabled and the file allowed to corrupt on power loss, an LMDB
write costs 84 µs where Redis needs one to cost about 2 µs. That is the finding
this document is built on, and it is the one that decides which proposals are
worth anything.

### Why an LMDB write costs 84 µs

Two things, both structural:

1. **Copy-on-write amplification.** Every write dirties a fresh copy of every
   B-tree page on the path from leaf to root. A 512-byte record is not a
   512-byte write. [plan.md](plan.md) §15 lists this as a risk and says it is
   *"the number to watch first"*. It was watched, and this is the number.
2. **Two B-trees per record, plus a delete.** `apply.rs` puts the record into
   `main` and an entry into the `exp` expiry index, and an overwrite also
   deletes the displaced index entry. The index key is built from the deadline
   *and the CAS token*, and CAS is monotonic — so **every overwrite relocates its
   index entry to a new position rather than updating one in place.** A steady
   overwrite workload, which is exactly what the benchmark runs, is therefore an
   insert-and-delete churn against a second tree on every single `SET`.

---

## 3. Implemented: `resident_mode`

**Status: shipped as `store.resident_mode`.**

| | ops/s | p50 |
|---|---:|---:|
| GET closed loop, off | 18,023 | 5.47 ms |
| GET closed loop, **on** | **180,027** | **0.52 ms** |
| GET pipelined, off | 222,892 | 6.62 ms |
| GET pipelined, **on** | **753,498** | 1.94 ms |

Reproduced across six runs and two rounds, with the closed-loop figure landing
within 5% each time. It puts vash ahead of memcached and Redis on both read
workloads on the same four cores.

The flag is off because a read that page-faults while running on a runtime
worker stalls every connection that worker is serving. That risk is real and the
default is currently the honest one — but the flag as it stands **asks the
operator to assert residency and then does nothing to enforce it.** Three ways
to close that, in increasing order of strength:

- **(a) Pair it with `prefault`.** Already in-tree: read the whole data file at
  startup so the map is resident before the first request. Cheap, and it makes
  the assertion true at t=0 — but nothing keeps it true once the kernel starts
  reclaiming under memory pressure.
- **(b) Lock the map.** `mlock` the mapped region after prefaulting, so
  residency is guaranteed rather than hoped for. Needs `RLIMIT_MEMLOCK` headroom
  or `CAP_IPC_LOCK`; when the lock is refused the server keeps the hand-off and
  logs why, so the failure mode is "slower", never "stalls".
- **(c) Trip back adaptively.** Serve inline, sample read latency, and fall back
  to the pool on a fault-rate threshold with hysteresis. Costs a clock read per
  request and adds a mode that is hard to test.

**(b) was taken, with (a) implied and (c) rejected.** `store.resident_mode`
prefaults every shard, `mlock`s each map — raising `RLIMIT_MEMLOCK`'s soft limit
to the hard one first, since the common container default is a modest soft limit
against an unlimited hard one — and enables inline reads **only if every shard
came back locked**. `store.inline_reads` still works and still skips the check,
for an operator who knows their deployment better than the check does.

The mapping is found the way `prefault` already found it, by inode in
`/proc/self/maps`, which is also why locking is Linux-only: everywhere else the
mapping cannot be located, `map_locked` answers `false`, and reads keep the
hand-off. `stats settings` reports both `vash_map_locked` and
`vash_inline_reads`, because "I asked for this and did not get it" must not have
to be inferred from a throughput graph.

### Measured

A four-core container with `--ulimit memlock=-1:-1`, all four shards locked:

| Workload | before | **resident_mode** | Redis | memcached |
|---|---:|---:|---:|---:|
| GET, closed loop | 18,023 | **173,659** | 50,484 | 130,922 |
| GET, pipelined | 222,892 | **781,427** | 475,430 | 651,154 |

**vash now leads both servers on both read workloads**, with the assertion
enforced rather than requested. Closed-loop p50 falls from 5.47 ms to 0.54 ms.

The fallback was measured too, because a safety valve that has never been seen
to close is not one:

| `--ulimit memlock` | outcome |
|---|---|
| `-1:-1` | all shards locked, reads inline |
| `8192:8192` | `mlock` refused with `ENOMEM`, logged, **reads keep the hand-off** |
| `8192:-1` | soft limit raised to the hard one, locked, reads inline |

---

## 4. Implemented: one writer submission per block, not per command

**Status: shipped, in all three dialects.**

The block executors — `execute_memcached_block`, `resp::execute_block`,
`execute_vcp_block` — walk a pipelined block command by command, and every
`SET` in it calls `Store::store`, which submits one job to the shard writer and
**blocks until that job's own commit completes** before the next command in the
block is even parsed.

The consequence is visible in the populate row of the benchmark: 20,000 writes
down one connection at pipeline depth 128 reaches **505 ops/s**, against 391,129
for Redis and 337,587 for memcached on the identical command. The writer
counters say why — at 4 connections and pipeline 128 the **mean batch is 1.13**.
A 128-deep pipeline contributes nothing to batching, because its 128 commands
are 128 sequential round trips through the writer queue.

Group commit only ever sees concurrency *between* connections. It has never seen
the concurrency *within* one.

**The fix**: coalesce a run of consecutive unconditional writes in a block into
a single `Store::set_many`, which already exists, already groups by shard, and
already travels as one `WriteOp::Set(Vec<PreparedSet>)` carrying a real
`item_count`. The run must be flushed before:

- any read, so a `get` after a `set` in the same block still sees it;
- any conditional write (`add`, `replace`, `cas`, `SET NX/XX`), whose verdict
  depends on state the run may have changed;
- any second write to a key already in the run, so last-write-wins ordering is
  preserved without relying on within-batch ordering;
- the end of the block.

Replies are rendered in request order from the returned CAS vector, so `noreply`
and pipelining semantics are untouched.

### The second half: let the shards commit in parallel

`Store::set_many` groups a batch by shard and then walks the groups **one at a
time**, blocking on each shard's writer before submitting to the next:

```rust
for (index, group) in grouped.runs() {
    // ...prepare...
    for (placed, cas) in group.iter().zip(shard.writer.set_many(prepared)?) { … }
    //                                   ^ blocks here, before the next shard
    //                                     has been given any work at all
}
```

Four shards therefore serialise into four commits back to back, which is most of
the reason sharding does less for batched writes than §9 of the plan expected.
Submitting every shard's job first and collecting the replies afterwards makes
the four commits overlap, and needs nothing new: the queue is already
`bounded(1)` per job and the reply channel already exists. This is worth doing
whether or not the block coalescing above lands, because `MSET` and `SET_MANY`
take the same path today.

### Measured

Both halves landed together. A/B on the same box, same image pair:

| Workload | before | **after** | | mean batch |
|---|---:|---:|---:|---|
| Populate — 1 conn, pipeline 128, RESP | 511 | **3,143** | 6.2× | 1.00 → 7.2 |
| Populate — 1 conn, pipeline 128, memcached | 519 | **3,310** | 6.4× | 1.00 → 7.5 |
| `SET` — 100 conns, pipeline 16, RESP | 9,760 | **52,237** | 5.4× | 16.8 → 75.6 |
| `SET` — 100 conns, pipeline 16, memcached | 8,625 | **50,328** | 5.8× | 17.1 → 77.7 |

The concurrent rows are the surprise. The model only predicted the
single-connection case; concurrency was supposed to be the one thing group commit
already handled. It was not — each of a hundred connections was still submitting
sixteen separate jobs, so the writer saw 1,600 queue hops where it now sees 100,
and its mean batch went from 17 to 76.

That is also what corrects §2. On the sustained random-key workload the writer
counters read **0.174 ms of commit per record at a mean batch of 89**, against
0.37 ms at a batch of 17 before. A cost that falls when batches grow was never
the per-record cost the two-term fit called it; most of it was per queue hop, and
queue hops are exactly what this removes.

**Read the multipliers as workload-dependent, not as a single number.** The rows
above use sequential keys; the same change on uniformly random keys over the
20,000-key space measures 14,140 → 22,754, because random writes scatter across
the B-tree and pay more page churn per commit. The direction is the same
everywhere and the size is not.

### What it cost in semantics — nothing, and there are tests

The risk in deferring a write is that a pipelined client sees something a
sequential one would not. Pinned by tests in `tests/memcached.rs` and
`tests/redis.rs`:

- a `get`/`GET` later in the same block sees the writes before it;
- a guarded write (`add`, `SET … NX`) is judged against them too;
- a repeated key keeps last-write-wins, and each write still gets its own reply;
- `noreply` suppresses its own response and nobody else's;
- an error keeps its position in the reply stream;
- **one refused write does not fail the run around it** — a batch is one
  transaction per shard, so a record the store rejects fails the whole
  submission, which then retries the run a command at a time through the ordinary
  path. A client's verdict must not depend on whether it happened to pipeline.

That last one was found by an existing test rather than by inspection: batching a
`SETTAGS` carrying too many tags reported `invalid argument` where it had
reported `too many tags`, because the batch was rendering from a status and
throwing away the cause.

---

## 5. Implemented: stop relocating the expiry index

**Status: shipped. A large win in the CPU-bound regime, and no measurable
end-to-end change on this device-bound box — both results below, because only
one of them supports the change.**

From §2: every `SET` is two B-tree puts and, on overwrite, a delete. The `exp`
key is `(bucketed deadline, cas)` and CAS advances on every write, so an
overwrite that does not change the deadline still moves its index entry.

Three candidate changes, in increasing order of what they give up:

- **Make the index key stable across overwrites** — replace the CAS component
  with something that does not change when the record is rewritten (a hash of
  the key, say). An overwrite with an unchanged deadline then becomes an
  idempotent put onto the same page instead of an insert plus a distant delete.
  Gives up nothing obvious; the disambiguator only has to be unique per key
  within a bucket.
- **Defer index maintenance to the sweeper.** The write leaves the stale entry
  and the sweeper reconciles it inside the transaction it already opens. Trades
  write cost for sweep cost and a window where the index over-reports.
- **Skip the index for records with no deadline.** Cheapest to implement,
  and it takes the most away: [plan.md](plan.md) §6 indexes `NEVER` records
  precisely so a cache of TTL-less keys still has eviction victims. Doing this
  needs eviction to gain a separate sampler over `main`, which is its own
  project. Listed for completeness, not recommended.

### Measured: the index is most of the write

That measurement was taken. The index put and the displaced delete were stubbed
out of `apply.rs` — a correctness-breaking hack, run only to price the ceiling —
and `cargo bench -p vash-bench --bench write_path` run against it. Divan medians,
in-process and under `ephemeral`, so no `fsync` and no queue hop is in these
numbers; what is left is B-tree work:

| benchmark | baseline | index stubbed | |
|---|---:|---:|---:|
| `overwrite_existing` 1 KiB | 116.9 µs | 68.1 µs | 1.72× |
| `overwrite_existing` 64 KiB | 321.1 µs | 157.2 µs | 2.04× |
| `set_many_untagged` | 177.9 µs | 60.2 µs | 2.96× |
| `rmw_plain_set` — one hot key | 31.7 µs | 0.60 µs | **53×** |
| `rmw_plain_set_large` — scattered keys | 104.5 µs | 70.8 µs | 1.47× |

**The expiry index is between a third and almost all of the cost of a write**,
and it is the largest single write cost this document has found. §5 guessed "up
to 2×"; the honest number is at the top of that range for ordinary writes and far
past it for a repeatedly overwritten key, which is the shape a cache actually
sees.

A second pass stubbed **only the delete**, leaving the put: `rmw_plain_set` went
from 31.7 µs to 1.30 µs against 0.60 µs with both removed. **The delete is the
dominant half.** That fits the structure — the put appends at the end of the
bucket, and the delete removes an entry from wherever the previous write left it,
dirtying a second page and rebalancing as the tree churns. (Medians across
separate bench processes on this box are not directly comparable; the effect is
large enough to survive that, the 1.7–2× ones less comfortably so.)

### Every implementation hits something, and the codebase has already ruled on two

Working through the three candidates above against the constraints actually in
the tree:

- **`bucket || user_key`** — exact, no collisions, and **blocked outright**.
  LMDB caps a key at 511 bytes and `vash_core::MAX_KEY_LEN` is *also* 511, so an
  8-byte prefix plus a maximum-length key does not fit. This is the same wall
  [`reclaim.rs`](../crates/vash-store/src/reclaim.rs) hit, and its module docs
  say so.
- **`DUP_SORT`, `bucket -> [user_key]`** — the tidiest layout on paper, and the
  plan originally specified exactly this for the tag index. `reclaim.rs` rejected
  it after implementing it: heed cannot seek to a position *within* a duplicate
  list, so a budgeted, resumable cursor has to re-walk everything it already
  processed, which is quadratic. The expiry sweeper is the same shape of cursor,
  so it inherits the same verdict.
- **`bucket || xxh3_64(user_key)`** — 16 bytes, fits, stable across overwrites.
  This is precisely the layout and precisely the trade `reclaim.rs` settled on
  for the tag index, collision behaviour included: a 64-bit collision within one
  bucket drops an index entry, so that record is not reclaimed proactively — it
  stays correct, because reads check liveness and TTLs independently, it just
  lingers. **This is the one to build.**

CAS then leaves the key, and the staleness check it exists for gets *better*
rather than worse: the sweeper can compare the entry's bucket against
`bucket_for(record.expires_at_ms)` and drop an entry that no longer describes its
record, which is exact where the CAS comparison was a proxy.

### The two things it costs, which are decisions rather than details

**Eviction ordering inside a bucket changes.** Today entries within one bucket
are ordered by CAS, which is write order, so the capacity evictor takes the
least-recently-*written* record first — a decent policy that
[expiry.rs](../crates/vash-store/src/expiry.rs) documents as insertion order and
that nothing claims as a design goal. Keyed by hash, that ordering becomes stable
but arbitrary, and stable is the problem: a low-hash key that is evicted and then
rewritten lands back at the same position and is evicted again, where CAS order
would have moved it to the end. Eviction only runs above the 0.75 watermark, so
this is a cost paid rarely against a benefit paid on every write — but it is a
real regression in eviction quality, not a free win.

**It is an on-disk format change**, so `SCHEMA_VERSION` goes to 3 and existing
databases are refused at startup. There is precedent — version 1 was rejected
the same way when never-expiring records started being indexed — and for a cache
that is a documented, survivable operation. Verified end to end: a store written
by the previous build refuses to open with

```text
database is corrupt or was written by an incompatible build:
database has schema version 2, this build expects 3
```

Both were accepted deliberately. A migration — rebuilding the index from `main`
at startup, one full scan — was available and not taken, because nothing is
running on this yet.

### What it measured

Two regimes, and they disagree.

**In-process, `ephemeral`, commits amortised over 256 operations** — so no
`fsync` and no queue hop, and what is left is B-tree work. Measured as a
same-session A/B, stashing the change and restoring it between runs rather than
comparing across sessions, because medians on this box do not survive that:

| benchmark | before | after | |
|---|---:|---:|---:|
| `rmw_plain_set` — one hot key | 68.3 µs | **1.30 µs** | 53× |
| `rmw_plain_set_large` — scattered over 200,000 keys | 244.1 µs | **6.00 µs** | 41× |
| `set_many_untagged` | 100.8 µs | 64.4 µs | 1.6× |
| `overwrite_existing` 1 KiB | 51.7 µs | 66.7 µs | noise |

The last row goes through the writer thread and a real commit, which dominate it;
it swung from 116.9 µs to 50.8 µs across other runs of the same build. Only the
engine-level rows say anything.

**Through the server, four cores, `relaxed`, alternating rounds** — nothing:

| keyspace | before, commit/record | after, commit/record |
|---|---:|---:|
| 20,000 keys | 0.129, 0.164 ms | 0.195 ms |
| 1,000,000 keys | 0.185, 0.268 ms | 0.255, 0.294, 0.294 ms |

Two to three clean samples per arm, with the device stalling in half the rounds
— `exp-off` twice at over 6 ms per record, `exp-on` once. **On what survives,
the change is flat to slightly worse.**

### Why the two disagree, and why it stayed

The engine benchmark removes device time entirely, so B-tree work is 100% of what
it measures. The container pays 0.13–0.3 ms per record of commit on a virtualised
disk, and the tens of microseconds of tree work this saves are a small share of
that, inside the noise of a box that stalls every other round.

There is also a real countervailing effect. The old scheme's *put* was a
sequential append — CAS is monotonic, so it always landed at the right edge of
the bucket — and its *delete* was at a random position. The new scheme has one
random put and no delete. It removes the expensive half and makes the cheap half
expensive, which is still a net removal of work but not the clean subtraction the
stub measured.

It stayed because it strictly removes a B-tree operation from every write, it is
covered by tests, and the regime it wins in is the one a real device and an
`--ephemeral` deployment both sit closer to than this container does. **The
end-to-end benefit on this hardware is unproven, and that is the honest state of
it** — the outstanding validation is a run on a machine whose disk does not stall.

---

## 6. Tried and rejected: `WRITE_MAP` under `relaxed`

**Status: implemented, measured, reverted.**

`env_flags` sets `MDB_WRITEMAP` only for `ephemeral`. It writes dirty pages
straight through the map instead of allocating and copying them, and it is
independent of the sync flags — `MDB_NOMETASYNC` and `MDB_WRITEMAP` compose, so
`relaxed`'s guarantee that a crash cannot corrupt the database would have been
kept. **Expected: single-digit percent to perhaps 1.5×** on the marginal cost.

It was a three-line change. Two images were built from the same tree differing
only in that flag, and the mapping was checked in `/proc/1/maps` to confirm the
difference reached the binary — `r--s` without it, `rw-s` with — because "no
measurable difference" is also what comparing two identical binaries looks like.

### What it measured

Ten alternating rounds, `SET`-only at pipeline 16 over 100 connections, on the
four-core container. The box stalls: four to five rounds in ten had a commit cost
an order of magnitude above the rest, and the alternating order caught one round
where **both** images collapsed together (675 and 670 ops/s), which is what says
those are the device rather than the build.

Splitting the rounds where the device behaved from the rounds where it did not:

| | clean rounds | median ops/s | median commit/record |
|---|---:|---:|---:|
| without `WRITE_MAP` | 6 of 10 | 21,497 | 0.176 ms |
| **with `WRITE_MAP`** | 5 of 10 | 20,254 | 0.186 ms |

**A 6% difference, in favour of not having it, against a run-to-run spread of
19,240–26,193 on the same configuration.** There is no effect here to adopt.

One difference was consistent, and it points the wrong way: **the stalls are far
worse with the flag on.** The worst commit costs without it were 5.6–7.1 ms per
record; with it, 52.8 and 77.0 ms, with the writer's batch swelling to 114 as the
queue backed up behind them. That is what `msync` over a dirty 4 GiB mapping
costs when the device is already struggling, against `pwrite` of exactly the
pages that changed — plausible as a mechanism, and unmeasured beyond the fact
that it happened twice and never happened without the flag.

### Why it stays reverted

`WRITE_MAP` gives up LMDB's protection against a stray write corrupting the map
through a dangling pointer. In a codebase with no `unsafe` in the write path that
risk is small, but `relaxed` is the **default** durability, and the bar for
trading a safety property on a default is a clear and repeatable win. A 6%
difference of the wrong sign, plus a worse tail, is not that.

Worth re-testing on a real device rather than a virtualised one, where the
`msync`-versus-`pwrite` difference should be smaller and the stalls should not
exist. Nothing here generalises past the hardware described at the top.

---

## 7. Proposal 5 — a RAM-resident write-back tier

**Status: the only proposal that can meet the objective. An M11-sized milestone.**

§2 establishes that the request path cannot both touch LMDB and be fast. Every
proposal above moves vash toward the ~17,400/s ceiling or lifts it to maybe
35,000/s. The objective is 554,785/s. **The gap is not a tuning gap and no
combination of §4–§6 closes it.**

The only architecture that does is one where a write does not reach the B-tree
before it is acknowledged:

- An **in-memory index** (key → value) is the authority for reads and writes.
- A **write** applies to RAM, acks immediately, and joins a per-shard dirty
  list.
- A **background flusher** drains that list into LMDB through the group-commit
  machinery that already exists — unchanged, but off the request path, where its
  2.37 ms fixed cost and 0.23 ms marginal cost stop being anybody's latency.
- A **read** hits RAM; a miss falls through to LMDB and populates RAM.
- **LMDB becomes the persistence and restart layer**, which is what it is
  actually good at, rather than the serving layer, which is what it is being
  asked to be now.

This is what makes vash's writes cost a hash insert — the ~2 µs Redis pays —
instead of the 84 µs that §2 measures as LMDB's floor with durability already
switched off.

### What it costs, stated plainly

This is a large change and the costs are not incidental:

- **The durability contract changes.** Today a `STORED` means committed. After
  this it means "in memory, and on disk within the flush interval". Plan §9
  already argues the ground for this — *"a lost write is a cache miss, and a
  cache miss is already a supported outcome"* — but the contract is currently
  stronger than that argument requires, and clients may be relying on it. It has
  to be stated in [protocol.md](protocol.md) and it needs a `write_through` mode
  that restores today's behaviour for anyone who wants it.
- **Memory accounting becomes the hard problem**, and it is precisely the
  problem memcached solved with slab allocation and vash has never had to solve.
  Today memory is bounded by the LMDB map and the OS page cache manages it.
  RAM-first needs its own capacity watermarks, its own eviction, and its own
  fragmentation story. **This, not the write path, is where the work is.**
- **Resident set roughly doubles for hot data**, since the RAM copy and the page
  cache hold the same bytes, unless reads are served from the same buffers the
  flusher writes from.
- **Restart is cold** until the RAM tier is repopulated, which puts the existing
  `prefault` work on the critical path for a different reason than it was built.
- **The semantics have to move or be duplicated.** Expiry, tag generations, the
  global epoch and CAS all currently live in one place, in one transaction. A
  second tier means either moving them up or keeping them below and reconciling.
  A bug here is a correctness bug, not a performance one.

### Staging

It should not be built in one step, and each step is independently useful:

1. **Read-through cache only.** RAM in front, LMDB still authoritative for
   writes. Wins nothing on writes, removes the read hand-off completely without
   needing §3's residency argument, and builds the memory accounting that stage 3
   depends on.
2. **Write-back for values, semantics still below.** Dirty list plus flusher;
   expiry and tags continue to be evaluated in LMDB's transaction. This is where
   the write number moves.
3. **RAM as the authority.** Only if 1 and 2 measure well and the accounting
   holds under a sustained overfill.

**Honest expectation**: memcached's 912,234 pipelined `SET` is about 4.4 µs per
operation end to end including the network, on four cores. That is close to what
a lean server can do at all, so the realistic target for stage 3 is **parity
with memcached and a clear lead over Redis**, not a rout. Combined with §3's
read result, that would make vash faster than both across the matrix — which is
the objective, and it is reachable, and it costs a milestone.

---

## 8. Explicitly rejected

Recorded so they do not get proposed again:

- **More shards.** Plan §9 already measured it: with syncing on, splitting one
  device between more environments fragments its I/O and throughput *falls* —
  12.4k ops/s at one shard against 10.6k at eight, and `durable` fell from 10.9k
  to 5.5k. Sharding cannot fix a disk, and §2 says the disk and the tree are the
  problem. The default of 4 stands.
- **Optimising the parsers.** `cargo bench --bench hot_path` prices them at
  53.5 ns for a memcached `get` and 90.9 ns for a RESP one. At 220,000 requests
  a second that is 0.8% of one core. There is nothing here.
- **Raising `linger_us`.** It buys batch size at the cost of latency, and §2
  shows batching is already the half that works. It would move the mean batch
  from 17 toward `max_batch` and buy at most the 20% that separates the measured
  14,140 from the 17,400 ceiling — while making every write wait.
- **Switching `relaxed` to `ephemeral` by default.** Worth 2.7× and gives up the
  guarantee that the file cannot be corrupted by an OS crash. It is a legitimate
  deployment choice and it is already a flag; it is not a default, and it is not
  a substitute for §7.
- **Replacing LMDB with `libmdbx` or another B-tree.** The `Store` trait keeps
  this contained, and it is the right escape hatch for a *correctness* or
  *operational* problem. It is not an answer to §2: another copy-on-write B-tree
  has the same amplification, and the gap to close is 12–19× with durability
  already off.

---

## 9. Sequencing

| Order | Proposal | Effort | Outcome | Confidence |
|---|---|---|---|---|
| ~~1~~ | §3 `resident_mode` | done | **reads lead both servers** — 173,659 and 781,427 | **measured** |
| ~~2~~ | §4 one submission per block, shards in parallel | done | **5.4–6.4× on pipelined writes** | **measured** |
| ~~3~~ | §6 `WRITE_MAP` under `relaxed` | done | **no effect, reverted** — 6% the wrong way, worse stall tail | **measured** |
| ~~4~~ | §5 expiry-index relocation | done | **41–53× less B-tree work**; no measurable end-to-end change on this box | **measured**, and the two regimes disagree — see §5 |
| 5 | §7 RAM write-back tier | a milestone | writes competitive with memcached | design-level only |

**Where that leaves the objective.** Reads are done: vash is ahead of both
servers on both read workloads. Writes moved from 39–65× behind to roughly
10–20× behind, on a change that turned out to be worth more than it was costed
at. The remaining gap is still not a tuning gap — §7 is still what closes it —
but §2's ceiling argument has to be rebuilt before the next write proposal is
believed, because the first version of it was wrong in the direction of despair.

---

## 10. How to know it worked

The last two rounds of benchmarking made the methodology a first-class concern,
and any of this work should inherit it:

- **Run the matrix twice**, hours apart, and report the second. A single
  20-second sample of vash's write rate is not a finding — the same command has
  returned 3,309 and 12,480 ops/s on the same image.
- **A/B by alternating images**, so neither build gets the warmer machine. This
  is what made the `inline_reads` result believable and it is the pattern to
  copy.
- **Read the writer counters, not just the client.** On the admin port,
  `vash_writer_queue_wait_seconds_total` and
  `vash_writer_commit_seconds_total` over `vash_committed_ops_total` and
  `vash_commits_total` decompose a throughput number into batching, queueing and
  device time; `stats` reports the same batch size as `vash_mean_batch`. §2
  exists only because they were scraped. A change that moves ops/s without
  moving one of those has probably not done what its author thinks.
- **State the acceptance criterion before the run.** For §4 that is "mean batch
  at pipeline 128 on one connection rises from 1.13 to above 16"; for §6 it is a
  fall in commit-per-record from 0.23 ms; for §3 it is that the map stays
  resident under a memory-pressure test. Those are properties, not throughput
  figures, and they survive a noisy host.
