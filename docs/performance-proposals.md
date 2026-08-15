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
| SET, closed loop | 5,774 | 15,110 relaxed, **24,902** lazy | 55,401 | 148,314 |
| SET, pipelined | 14,140 | 24,675 relaxed, **109,839** lazy | 554,785 | 912,234 |

The read rows are [§3](#3-implemented-resident_mode) and the write rows are
[§4](#4-implemented-one-writer-submission-per-block-not-per-command), both since
implemented. The two conclusions below survived them, and one piece of arithmetic
did not — see the correction in §2.

**Reads were not a research problem.** vash was already faster than both servers
on both read workloads with a setting that existed, shipped, and was off by
default. Everything the read section of `benchmarks.md` agonises over was one
flag and the safety argument behind it — now `store.resident_mode`, §3.

**Writes were assumed to be a storage problem**, and four changes later they are
not. §4 and §5 took the storage engine down to **8% of what a write costs** with
syncing off; the remaining 92% was the request path, and
[§8](#8-implemented-writes-off-the-blocking-pool) took the hand-off out of it.
That is why the write-back tier in §7 is measured *against* rather than built:
it addresses the 8%. §9 then took the `fsync` out of the commit, which is what
the writer queue had been backed up behind all along.

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
| vash, `relaxed` | 0.23 ms | ~17,400/s |
| vash, `ephemeral` (no syncing at all) | 0.084 ms | ~47,600/s |
| Redis, measured | — | 554,785/s |
| memcached, measured | — | 912,234/s |

**Durability is 2.7× of the gap. The storage engine is the other 12–19×.**

> **Also corrected — see [§7](#7-a-ram-resident-write-back-tier--not-the-bottleneck).**
> The 84 µs was derived from container throughput the same way the 0.23 ms was,
> so it carried the same per-queue-hop and per-hand-off cost that §4 later
> removed. Decomposed directly against the writer's counters, the storage terms
> of an `--ephemeral` write are 0.94 ms of queue wait and 0.05 ms of commit
> against an 12.5 ms round trip. **The storage engine is not the other 12–19×;
> the request path is.**

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

### Re-tested off the VM, and the answer changed — but not for the reason expected

**Status: still off by default, no longer Unix-gated.**

The re-test above was run under `lazy` rather than `relaxed`, since `lazy` is now
the default, and natively on Windows against the NVMe directly — no WSL2, no
container, no VHDX. Server and load generator were both native, over vash's own
VCP loopback, five alternating rounds:

| Workload | `write_map` off | on | |
|---|---:|---:|---:|
| `SET`, closed loop | 19,309 | **20,918** | 1.08× |
| `SET`, pipeline 16 | 101,844 | **128,448** | 1.26× |
| mixed, pipeline 16 | 196,941 | **229,442** | 1.17× |

`on` won **all fifteen paired runs**, so unlike the original this is not a
question of medians.

**The obvious explanation is wrong.** If the gain came from the device, then the
same physical NVMe under Linux should show it too. It does not — same binaries,
same load generator, same protocol, same disk, in a container instead:

| Workload | `write_map` off | on | |
|---|---:|---:|---:|
| `SET`, closed loop | 9,821 | 10,252 | 1.04× |
| `SET`, pipeline 16 | 79,094 | 76,302 | 0.96× |
| mixed, pipeline 16 | 123,990 | 124,771 | 1.01× |

Flat, which agrees with the original rejection and with §9. (The absolute
numbers are not comparable across the two tables — the container was capped at
four cores and the native run was not. Only the within-table ratios mean
anything.)

So the variable is not the storage device. What differs between the two tables is
the operating system, the filesystem, and the virtualisation layer, all at once,
and this pair of runs cannot separate them. What it does establish is the shape
of the answer: **`WRITE_MAP` is a platform-dependent setting, not a
storage-dependent one**, and an operator should measure it where they run rather
than inherit either row.

Two changes fell out. The `#[cfg(unix)]` gate is gone: the `mdb_env_open`
failure it was written for does not reproduce at 4, 16 or 64 GiB map sizes, and
while it stood it silently ignored `store.write_map` on the one platform where
the flag pays — the worst of both, an option that reads as supported and does
nothing. And the default stays `false`, for the reason in the previous section
plus a sharper one now that `lazy` is the default: under `NOSYNC`, `WRITEMAP`
writes pages in place with no ordering, so a crash can leave the database
**corrupt** rather than merely a second stale. That is a much larger promise to
trade than 1.26× on one platform, and it should be the operator's trade to make.

---

## 7. A RAM-resident write-back tier — **not the bottleneck**

**Status: costed, and the measurement says do not build it. §8 replaces it.**

This section was written when §2's arithmetic said the storage engine was the
wall. §4 corrected that arithmetic, §5 removed most of the B-tree work, and the
premise below no longer holds — so before building a milestone, the write path
was decomposed again. **Storage is now a small minority of what a write costs.**

Measured on the current build, 100 connections, one request in flight each, so a
round trip is exactly `100 / throughput`. The writer's own counters give the two
storage terms and the remainder is everything else — socket, parse, dispatch,
and the hand-off to the storage pool:

| | round trip | queue wait | commit | **everything else** |
|---|---:|---:|---:|---:|
| `--ephemeral` | 12.52 ms | 0.94 ms | 0.05 ms | **11.54 ms — 92%** |
| `relaxed` (default) | 16.57 ms | 4.84 ms | 0.60 ms | **11.15 ms — 67%** |

**With syncing switched off entirely, the storage engine accounts for 8% of a
write.** A tier that makes storage free therefore buys at most that 8%, and the
same shape shows in throughput: `--ephemeral` reaches 53,177 pipelined `SET`
against `relaxed`'s 29,506 — 1.8×, not the order of magnitude this section
assumed — and it gets there with its writers *idle*, mean batch collapsing from
88 to 13 because the queue drains faster than it fills.

The 11 ms is not mysterious. It is the same hand-off §3 measured for reads: a
`GET` costs 5.47 ms with it and 0.54 ms without. Writes cannot take §3's
shortcut — a write must not run on a runtime worker, because it blocks on the
writer queue — so every in-flight write parks an OS thread for its whole
duration, and a hundred of them contend for four cores. That is the cost, and a
RAM tier does not remove it: the write would still cross the same hand-off to
reach the RAM tier.

**So this is the wrong change**, and [§8](#8-implemented-writes-off-the-blocking-pool) is what the numbers actually point at. The design is kept below because it
remains correct, and because if the hand-off is fixed and the device becomes the
limit again, this is what comes next.

<details>
<summary>The design, kept for when it is the bottleneck</summary>

The architecture is one where a write does not reach the B-tree before it is
acknowledged:

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
with memcached and a clear lead over Redis**, not a rout.

</details>

---

## 8. Implemented: writes off the blocking pool

**Status: shipped. 1.2–2.5× on writes, and it took two attempts — the first one
regressed the default and is recorded below because the reason is the finding.**

A write is handed to `tokio::task::spawn_blocking`, and that thread then
**blocks on a channel** waiting for the shard writer to commit it. The blocking
is not the storage work — the thread is asleep for almost all of it, since the
writer does the work on its own thread. The pool exists so a call that *may*
block does not stall a runtime worker; what it costs here is one OS thread
parked per in-flight write, a hundred of them on four cores, and two thread
wake-ups per request.

§7 measures that at **11 ms of an ephemeral write's 12.5 ms round trip**, and §3
measures the same hand-off independently at 4.9 ms for a `GET` (5.47 ms with it,
0.54 ms without).

**Awaiting a channel is what async is for.** The work the blocking thread does
is: prepare the record, hand it to the shard queue, sleep, wake, render the
reply. Only the sleep needs the thread, and only because the reply arrives on a
`crossbeam` channel that cannot be awaited. Swapping the reply channel for a
`tokio::sync::oneshot` lets the connection task submit and `.await`, holding no
thread at all while the writer works.

- `prepare_set` stays on the caller — it is bounded CPU work with no I/O, and
  running it on the runtime worker is what already happens for `inline_reads`.
- The submit is already non-blocking: `Writer::send` is a `try_send` that fails
  fast with `Overloaded` rather than waiting.
- Reads keep both paths: inline under `resident_mode`, the pool otherwise, since
  a read genuinely can fault.

### The cheap version first, because it was an hour rather than a week

`tokio::task::block_in_place` tests the same hypothesis without touching the
storage trait: it keeps the work on the current worker and tells the runtime to
move that worker's *other* tasks elsewhere, so the task allocation, the queue and
the double wake all go. Four alternating rounds each:

| | `spawn_blocking` | `block_in_place` | |
|---|---:|---:|---:|
| ephemeral, pipeline 1 | 9,857 | 18,310 | 1.86× |
| ephemeral, pipeline 16 | 69,484 | 127,007 | 1.83× |
| relaxed, pipeline 1 | 6,762 | 4,612 | **0.68×** |
| relaxed, pipeline 16 | 30,775 | 20,863 | **0.68×** |

**1.85× when the wait is short and 0.68× when it is long**, split exactly by
durability. That is the hypothesis confirmed *and* the cheap version disqualified
in one table: `block_in_place` still holds a thread, so a 5 ms writer-queue wait
converts workers faster than the runtime can replace them. It was reverted.

### What shipped

`Store::submit_set_many` hands the prepared records to the shard writers and
returns; the reply arrives on a `tokio::sync::oneshot` that the connection task
**awaits**. No thread is held. A block that is nothing but unconditional writes
takes this path; anything else — a read, a guarded write, a tagged write, an
unauthenticated connection — declines and runs exactly as before, so this is a
shortcut and never the only route.

Tagged writes are excluded for a specific reason: registering a tag name is
itself a synchronous write through the same queue, taken inside the submit, and a
runtime worker may not block on it. That was found by the test suite rather than
by inspection.

### It needed admission control, which the pool had been providing by accident

The first working version regressed the one case the pool had been protecting:

| | before | awaited, unbounded | awaited, bounded |
|---|---:|---:|---:|
| relaxed, pipeline 16 | 19,047 | **10,822** | 9,986 → see below |

A write submitted from the pool was bounded by the size of that pool — at most
`server.max_blocking_threads` could be queued at the writers. Awaiting removes
the thread and with it the bound, so a hundred connections flooded the shard
queues: throughput fell to 0.57× and the writers' queue wait climbed from 5.2 ms
to 17.1 ms. `ServerState::write_permits` restores the same ceiling explicitly, a
semaphore held across the submit *and* the wait.

With it, all four cases improve:

| | `spawn_blocking` | awaited | |
|---|---:|---:|---:|
| ephemeral, pipeline 1 | 9,615 | **22,673** | 2.36× |
| ephemeral, pipeline 16 | 57,189 | **69,305** | 1.21× |
| relaxed, pipeline 1 | 2,884 | **7,347** | 2.55× |
| relaxed, pipeline 16 | 4,929 | **9,986** | 2.03× |

**Read the ephemeral rows and distrust the relaxed ones.** Ephemeral reproduced
across both rounds — 2.32× and 2.36× at pipeline 1, 1.19× and 1.21× at 16. The
relaxed baselines swung by a factor of four between rounds on this box (19,047
then 4,929 for the same build and command), so their ratios are direction, not
magnitude. The server-side signal agrees with the direction: queue wait fell from
18.0 ms to 10.3 ms at pipeline 16 and from 14.3 ms to 4.8 ms at pipeline 1.

### It also cost 7× on reads, which took three rounds to notice

**This change carried a bug for two commits, and the benchmark that caught it
issues no writes.** Awaiting a future inline splices its state into the caller's,
so the decoded `WriteRun`, the permit and the pending submission became part of
`drain`'s future — and `drain`'s future is part of every connection task's,
polled on every block whether or not it contains a write. Pipelined `GET` with
`resident_mode` fell from 484,861 to 70,032, and the ordinary read path lost
about 26% alongside it. It showed up in
[benchmarks.md](benchmarks.md#resident_mode-reversed-on-pipelined-reads-and-why)
as an unexplained reversal in a setting three sections above this one.

`Box::pin` on the single call site fixes it: 762,859 against 101,852 with
`resident_mode`, 237,584 against 135,231 without, and pipelined `SET` at 0.97×
which is inside this box's noise. **The lesson generalises past this change: an
`.await` costs the function it sits in, not only the path that reaches it.** A
cold branch holding a large future taxes every poll of the task containing it.

---

## 9. Implemented: stop syncing on every commit

**Status: shipped as `store.durability = "lazy"`. 1.7–4.5× on writes, and it
collapses the queue wait §8 left behind.**

After §8 the dominant term in a write was the **writer queue wait** — 4.8 ms at
pipeline 1 and 10.3 ms at 16. That is not an independent cost to attack: it is
Little's law against a saturated writer, so it only falls if the writer gets
faster or there are more of them. [plan.md](plan.md) §9 already measured more
shards going backwards on a disk-bound workload, which leaves making the writer
faster.

The writer's own counters say what it is waiting for. Per record inside commit:
**0.43 ms under `relaxed` against 0.033 ms under `ephemeral`** — so 92% of it is
the `fsync`, and everything queued behind it is the queue wait.

### The mode that was already documented and never existed

`relaxed` is `MDB_NOMETASYNC`: the meta page is not synced, the data is, on every
commit. Its documentation has always said *"periodically forced"* — and nothing
forced it. The only `force_sync` in the tree ran at shutdown, so a killed process
left the meta page wherever the OS had got to.

So the writer now has a sync timer, `write.sync_interval_ms`, which does two
things at once: it makes `relaxed`'s promise true, and it makes a new mode
possible.

`durability = "lazy"` is `MDB_NOSYNC` — no sync on commit at all — with that
timer bounding the loss window. **It gives up durability, boundedly, and keeps
integrity**: after a crash the database is still consistent and still openable,
where `ephemeral` has to be wiped. LMDB's condition for that is exactly stated —
`MDB_NOSYNC` preserves atomicity, consistency and isolation *"if the filesystem
preserves write order and the `MDB_WRITEMAP` flag is not used"* — so this mode
never sets `WRITE_MAP`, which [§6](#6-tried-and-rejected-write_map-under-relaxed)
had already measured as worth nothing.

### Measured

Three modes, one build, alternating order, medians of four:

| | ops/s | queue wait | commit/record |
|---|---:|---:|---:|
| **pipeline 1** | | | |
| `relaxed` | 15,110 | 2.23 ms | 0.260 ms |
| **`lazy`** | **24,902** | **1.17 ms** | **0.062 ms** |
| `ephemeral` | 26,154 | 0.87 ms | 0.049 ms |
| **pipeline 16** | | | |
| `relaxed` | 24,675 | 3.98 ms | 0.159 ms |
| **`lazy`** | **109,839** | **0.77 ms** | **0.021 ms** |
| `ephemeral` | 56,233 | 1.34 ms | 0.028 ms |

**1.65× and 4.45× over `relaxed`**, and the queue wait falls with it — 2.23 to
1.17 ms and 3.98 to 0.77 ms — which is the point: the backlog was the sync, and
removing the sync drained it.

**`lazy` is now the default.** What that trades is a one-second window of writes
against an OS crash, on a server whose own plan §9 opens by saying a lost write
is a cache miss and a cache miss is already a supported outcome. What it does not
trade is the database: integrity is preserved, so there is nothing to wipe and
nothing to rebuild. Set `durability = "relaxed"` to have every commit on the
device before it is acknowledged, and pay the 1.7–4.5×.

### And the shard cap, which rested on the thing that changed

[plan.md](plan.md) §9 measured sharding twice and found two effects: it divides
the mean batch by roughly the shard count, and it only pays back when the writer
thread is the bottleneck. Under `relaxed` the writer *was* the bottleneck —
commits waited for the device — so a second writer had real work to take, and 4
was the compromise. `lazy` removes the wait, which removes the benefit and leaves
the cost.

Re-measured, `SET`-only, medians of three alternating rounds:

| shards | pipeline 16 | pipeline 1 | mean batch at pipeline 1 |
|---:|---:|---:|---:|
| 1 | 83,925 | **66,605** | 42.3 |
| **2** | **140,767** | 42,357 | 5.5 |
| 4 — the old default | 126,292 | 24,901 | 1.9 |
| 8 | 47,554 | 19,922 | 1.5 |

**At pipeline 1, one shard beats four by 2.7×**, and the batch column is the
mechanism: four shards see a mean batch of 1.9, which is group commit doing
nothing at all. At pipeline 16 the offered load is enough to keep two writers
busy and two leads, but eight still collapses.

The default is now `min(num_cpus, 2)`. Two is the only count not beaten by four
in either shape, and the old cap of 4 was right for a bottleneck that no longer
exists. One box and one workload — a machine with many more cores under sustained
write load may want more writers, which is what the setting is for.

Note that `map_size` is **per shard**, so halving the count halves total capacity
unless `map_size_mb` goes up. That costs nothing: the map is a reservation, not
an allocation.

### The thing that fell out: `ephemeral` is leaving 2× on the floor

`lazy` beats `ephemeral` at pipeline 16 by **109,839 against 56,233**, and the
only difference between them is `WRITE_MAP`. That is the second independent
measurement pointing the same way — §6 found it worth nothing under `relaxed`
and far worse in the tail — so `ephemeral` is now the slowest way to run with
syncing off *and* the only one that has to be wiped at startup.

**Resolved: `ephemeral` was retired as a durability mode.** Without `WRITE_MAP`
it *is* `lazy`, so what the name added was a wipe — which was always a startup
policy rather than a durability guarantee. `--ephemeral` now means `lazy` plus
`wipe_on_start`, and `store.write_map` carries the flag on its own, off by
default and documented for the one thing it buys: LMDB stops allocating a copy of
every dirty page, so a large transaction has a lower peak footprint. That is
memory, not speed, and it costs `lazy` its integrity guarantee, so the config
pairs it with `wipe_on_start`.

---

## 10. Tried and reverted: the adaptive writer

**Status: implemented, measured, reverted. The shard cap already fixed what it
was for, and better.**

§9's shard sweep ended on a tension: pipeline 1 wants one shard and pipeline 16
wants two, and a fixed cap serves neither perfectly. The idea was a writer that
adapts — which needs one correction before anything else. **"Merging shards" is
not possible.** A shard is an independent LMDB environment and a key's shard is
`XXH3(key) % S`, fixed by the data's location; writes cannot be rerouted to a
different one at runtime. What *can* adapt is when a writer decides its batch is
finished.

So: when the queue runs dry with a small batch, wait briefly for company. Bounded
by what a commit currently costs — waiting longer than the thing you are
amortising can never pay — and capped so a stalling device cannot produce a long
linger.

### The governor, which the test suite demanded

The first version had no feedback and the store test suite went from **6 seconds
to 42**. Sequential single writes were each waiting the full linger for company
that could not arrive until the write in hand was answered — and an empty queue
looks identical whether the next write is 160 µs away or is not coming at all.

So the wait tracks what it actually gathers and turns itself off when that falls
below one record, probing every 64 batches so a writer that fell quiet can
discover load returning. The suite went back to 5.6 s, and the case is now a
regression test.

### Measured, and it does not pay

Against the same build with the wait disabled, medians of three alternating
rounds. With the target at 64 — wait until the batch reaches 64:

| | 1 shard | 2 shards | 4 shards |
|---|---:|---:|---:|
| pipeline 1 | 0.57× | 0.93× | **1.23×** |
| pipeline 16 | 0.85× | 0.83× | 0.81× |

**A loss in five of six.** The reason is the target: at a batch of 40 the fixed
cost is long since amortised, so waiting buys almost nothing and costs latency —
and a closed-loop client turns latency into lost throughput one for one. Lowering
the target to 8, where the fixed cost genuinely dominates:

| | 1 shard | 2 shards |
|---|---:|---:|
| pipeline 1 | 0.95× | 1.00× |
| pipeline 16 | 0.91× | 1.09× |

Neutral — every figure inside this box's noise. The one real gain anywhere is
four shards at pipeline 1, where a batch of 1.9 rises to 6.6, and **that is the
shard count §9 moved the default away from for the same reason.** Fewer shards
fixes batch fragmentation better than lingering does.

**So it was reverted rather than shipped off by default.** The argument for
keeping it was a deployment that needs many writers for reasons of its own and
takes the batching hit at low load — but that is a configuration this document
recommends against, and code the default never runs is code nothing tests in
anger. Two knobs, a moving average and a feedback governor are a lot of surface
to carry for one row of one table.

What survives is the measurement and the shape of the answer, which is what the
next person needs:

- **The wait has to be bounded by what a commit costs.** Waiting longer than the
  thing you are amortising cannot pay, and that bound needs no tuning.
- **It has to prove it is gathering something**, or a single sequential client
  waits on every write for company that cannot arrive until the write in hand is
  answered. That is not a subtle failure — it is a 7× slowdown, and it is what
  the test suite found.
- **The target matters more than the wait.** Above a batch of roughly 40 the
  fixed cost is already amortised and lingering is pure latency; the whole effect
  lives below about 8.
- **A closed-loop client turns latency into lost throughput one for one**, which
  is the ceiling on anything of this shape. It is why the honest version of this
  idea is worth so little: the batch has to grow by more than the wait cost, and
  by the time the batch is worth growing it is already big enough not to need it.

`write.linger_us` remains as the unconditional version for anyone who wants to
trade latency for throughput on every batch, and plan §9's "no artificial linger"
survives as the default — now as a measured position rather than an assumed
one.

---

## 11. Explicitly rejected

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

## 12. Sequencing

| Order | Proposal | Effort | Outcome | Confidence |
|---|---|---|---|---|
| ~~1~~ | §3 `resident_mode` | done | **reads lead both servers** — 173,659 and 781,427 | **measured** |
| ~~2~~ | §4 one submission per block, shards in parallel | done | **5.4–6.4× on pipelined writes** | **measured** |
| ~~3~~ | §6 `WRITE_MAP` under `relaxed` | done | **no effect, reverted** — 6% the wrong way, worse stall tail | **measured** |
| ~~4~~ | §5 expiry-index relocation | done | **41–53× less B-tree work**; no measurable end-to-end change on this box | **measured**, and the two regimes disagree — see §5 |
| ~~5~~ | §8 writes off the blocking pool | done | **1.2–2.5× on writes** | **measured**, and it needed a semaphore the pool had been standing in for |
| ~~6~~ | §9 `lazy` durability | done | **1.7–4.5× on writes**, and the queue wait with it | **measured** |
| ~~7~~ | §10 adaptive group-commit wait | tried, reverted | neutral to negative; the shard cap fixes it better | **measured** |
| 8 | §7 RAM write-back tier | a milestone | **not the bottleneck**; revisit only if the device becomes the limit again | measured against, see §7 |
| 9 | §14 split a block into runs | a day | **2.11x on a 1:9 workload**, measured as a bound | **measured** as a bound, unbuilt |
| 10 | §15 skip the redundant expiry put | an hour | commit is 54% of CPU and this is one of its two B-tree inserts | decomposed, unbuilt |

**What changed since this table was written.** The writes row is no longer the
only gap, and the bottleneck moved: §15's decomposition puts 54% of server CPU
inside the LMDB commit, which is the first time storage rather than the path to
it has been the limit. Meanwhile §14 says the mixed workload has been losing
2.11x to a dispatch decision rather than to either tier.

**Where that leaves the objective.** Reads are done: vash is ahead of both
servers on both read workloads. Writes moved from 39–65× behind to roughly
10–20× behind, on a change that turned out to be worth more than it was costed
at. The remaining gap is still not a tuning gap — §7 is still what closes it —
but §2's ceiling argument has to be rebuilt before the next write proposal is
believed, because the first version of it was wrong in the direction of despair.

---

## 13. How to know it worked

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
---

## 14. The next one: a block is dispatched whole, and mixed blocks lose both fast paths

**Status: proposed, and the prize is measured rather than modelled.**

Every fast path this document has added is decided **per block**, all or nothing.
`measure_resp` computes `all_reads` and `all_writes` by `&=` across every command
in the block, and `drain` then picks one route for the whole thing: inline reads
if every command is a read, the awaited write path if every command is an
unconditional write, and otherwise the blocking pool for all of it.

A pipelined cache client does not send blocks like that. At pipeline 16 with one
write in ten, the chance a block is entirely reads is `0.9^16 = 18.5%`. **Four
blocks in five lose inline reads because of a single write sitting among fifteen
reads.**

### The sweep says so

`inline_reads` on, pipeline 16, RESP, sweeping the ratio. `P(all-read)` is
`(1 - writeFraction)^16` — the chance a block takes the read fast path:

| ratio | write fraction | P(all-read block) | ops/s |
|---|---:|---:|---:|
| 0:1 | 0 | 1.000 | 800,311 |
| 1:199 | 0.5% | 0.923 | 593,554 |
| 1:99 | 1% | 0.851 | 488,230 |
| 1:19 | 5% | 0.440 | 213,950 |
| 1:9 | 10% | 0.185 | 149,872 |
| 1:3 | 25% | 0.010 | 107,867 |
| 1:0 | 100% | 0.000 | **157,331** |

Throughput tracks `P(all-read)`, not the write fraction. **One write per hundred
operations costs 39% of throughput.** And the last row rules out every other
explanation: **a workload of 25% writes is slower than a workload of 100%
writes** — 107,867 against 157,331. No cost model in which writes are simply
expensive produces that. Block homogeneity does: at 1:3 almost no block is
uniform so nothing takes any fast path, while at 1:0 every block takes the
awaited one.

Fitting per-block time to `T = P·T_inline + (1-P)·T_slow` gives `T_inline` ≈ 20 µs
and `T_slow` ≈ 127 µs, and that pair reproduces every row in the sweep to within
about 10%.

This is also why `resident_mode` is worth 3.2× on pure `GET` in
[benchmarks.md](benchmarks.md) and only 1.06× on the mixed workload: the setting
is working correctly and almost never getting the chance to.

### The prize, measured

The same 1:9 work over the same 100 connections, arranged two ways — mixed on
every connection, or 90 read connections beside 10 write connections so that
every block is uniform by construction:

| | ops/s |
|---|---:|
| mixed on every connection | 159,369 |
| split across connections | **336,392** |

**2.11×**, with the achieved write share at 9.1–9.2% in both. That is an
empirical upper bound on splitting blocks into runs, and it is lower than the
naive model predicts (~3.8×) because in the split arrangement the writes still
compete for the same cores. 2.11× is the number to hold this proposal to.

### What it would take

`measure` stops returning two booleans about the whole block and starts returning
**runs**: consecutive commands of the same class, as byte ranges. `drain` then
walks the runs in order — a read run served inline, an unconditional-write run
awaited, anything else on the pool — appending replies as it goes. Order is
preserved because the runs are processed in sequence, which is also what keeps
this correct for a client that pipelines a write and a read of the same key.

The block is already a refcounted `Bytes`, so a run is a slice of it and costs no
copy. The executors already take a byte range and render into a shared buffer,
so they need no change. The new cost is one small `Vec` of runs per block, at
roughly 44,000 blocks a second, plus more dispatches per block for workloads that
alternate — which is why the acceptance criterion is a sweep and not one row.

**Acceptance criterion, stated before the run**: the 1:9 row moves from ~150,000
toward the 336,392 bound, *and* the 0:1 and 1:0 rows do not regress — this change
adds bookkeeping to the uniform blocks that are today's best case, and that is
where it can both pay for itself and lose.

**Risks.** `drain` is the function that already cost pipelined reads 6.7× once by
growing a future it never polled (§8), so the awaited run must stay boxed and the
read path must stay allocation-free. VCP keeps its current whole-block route: its
replies carry a request id from each frame header and its pre-auth gate is read
from the same header, neither of which the shared run model describes.

---

## 15. Writes are bound by the writer threads, and half of that is one redundant put

**Status: proposed, from a decomposition rather than a guess.**

`SET`-only through the admin counters, four-core container, at HEAD:

| | pipeline 1 | pipeline 16 |
|---|---:|---:|
| client ops/s | 47,849 | 159,772 |
| records per commit | 5.6 | 117.2 |
| mean queue wait | 0.411 ms | 0.316 ms |
| mean commit | 0.153 ms | 1.318 ms |
| server CPU | 348% of 400 | 335% of 400 |
| **CPU per record** | **72.8 µs** | **20.9 µs** |

Two things fall out.

**The commit is now the write path.** At pipeline 16 the commit costs 11.2 µs per
record against 20.9 µs of total server CPU per record — **54% of everything the
server burns is inside LMDB's commit**, on two writer threads. Two threads at
11.2 µs a record is a ceiling of about 178,000 records a second, and the measured
159,772 is **90% of it**. This is the first round in which the storage engine
rather than the path to it is the limit; §2 and §7 both concluded the opposite,
and both were right when they were written.

Solving the two batch sizes for a fixed and a marginal part gives **~94 µs per
commit plus ~10.5 µs per record**. At 117 records the fixed part is already down
to 7%, so batching harder — §10's idea — cannot help. The marginal cost is the
target.

**Where the marginal cost goes.** Every write does a `get` on the main table, a
`put` on the main table, and an **unconditional `put` into the expiry index**. §5
already made the matching *delete* conditional, because an overwrite that stays
in the same bucket writes a byte-identical index key, and deleting it to put it
back was "most of the cost of a write". The put was deliberately left
unconditional, as self-healing. It is the same churn: a second B-tree insert and
a second copy-on-write page per write, which in the same-bucket case
re-establishes a row that is already there.

**The proposal**: skip the expiry-index put when `displaced.indexed_at` exists and
falls in the same bucket as the new deadline — exactly the condition under which
§5 already skips the delete. For a cache that overwrites existing keys, and for
every key with no TTL at all (they share one bucket by construction), that
removes one of the two B-tree inserts per write.

**Acceptance criterion**: commit-per-record falls from 11.2 µs, read off the
writer counters rather than inferred from ops/s. **What it costs**: the
self-healing property — an index row lost some other way stops being silently
restored by the next overwrite. That is a real trade, and it should be made
deliberately rather than as a side effect.

**Not the answer: more shards.** More writer threads is the obvious response to a
writer-thread ceiling, and the arithmetic says there is no room — the server is
already at 335% of a 400% quota with about 180% of that inside commits. Splitting
the same work across four writer threads does not create CPU, and it halves the
batch, which raises per-record cost through the 94 µs fixed part. Reducing the
work is available; spreading it is not. Plan §9 and the round after `lazy` both
measured two shards ahead, and this is why.

---

## 16. Closed-loop writes are still mostly not storage

For completeness, from the same table: at pipeline 1 a write costs **72.8 µs of
server CPU** against 20.9 µs at pipeline 16, and the commit accounts for 27.3 µs
of it. So roughly 45 µs per write is per-request path — the wake-up, the read
syscall, the parse, the submission, the reply wake, the write syscall — that
pipelining amortises over sixteen and a closed-loop client pays in full.

That is the same class of cost §8 attacked, and it is now the larger half of a
closed-loop write. It is recorded rather than proposed because the obvious
attacks are the ones already tried: §8 removed the parked thread, §10's linger
made it worse, and what is left is syscalls per request — which needs either
batched I/O (`io_uring`, not on every platform vash runs on) or fewer round
trips, which is the client's decision rather than the server's.

`SET` closed loop is 47,849 here against Redis's 60,810 — the closest the write
path has been. Closing it further means attacking per-request syscalls, not
anything in the storage tier.
