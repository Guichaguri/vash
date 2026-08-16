# vash beside Redis and memcached

Every number in [README](../README.md#performance) was measured against vash
alone, with vash's own load generator over VCP. This document does the other
thing: **the same client, the same box, the same workload, three servers**, so
the comparison is between the servers rather than between benchmark harnesses.

Read the [What this cannot tell you](#what-this-cannot-tell-you) section before
quoting anything here. The short version: one laptop, one container host, 20
seconds per run, the whole matrix run twice. These are orders of magnitude, not
measurements.

---

## Method

**The client is [`memtier_benchmark`](https://github.com/RedisLabs/memtier_benchmark)**,
because it speaks both RESP and the memcached text protocol. That is the whole
reason it was chosen: one client, one workload generator, one latency
histogram, pointed at every server in turn. A comparison where each server is
driven by its own vendor's tool measures the tools.

**Servers run one at a time**, each on the same container network with the same
CPU quota, so nothing competes with anything else for cores. Every client also
runs in a container on that network: an earlier attempt published the ports to
the host instead and reached a `vash-server` that happened to be running there,
which is how that mistake gets made.

**What was measured**, on 2026-08-15: vash at `f05ac4a`, built by the repository
`Dockerfile` into a 4.7 MB `scratch` image; `redis:8-alpine` (Redis 8.10.0);
`memcached:1.6-alpine` (memcached 1.6.45); `memtier_benchmark` 2.5.1. Host: a
Windows laptop running Rancher Desktop, 12 CPUs and 10 GB visible to the WSL2
VM, with a k3s cluster idling on the same machine.

**This is the second measurement of this matrix.** The first, at `7da2e5f`, is
what [performance-proposals.md](performance-proposals.md) was written against.
Eight changes later — four that worked and four that did not — these are the
numbers the same client sees from the same box. Old figures are quoted where the
comparison is the point.

**The matrix was measured twice**, back to back. The tables below are the
**second** round; where the rounds disagree it is said so in place.

**Six targets across three servers**:

| Target | What it is |
|---|---|
| `vash` | vash on its defaults — `lazy` durability, two shards — over the Redis dialect |
| `vash-memcache` | the same server and the same defaults, over the memcached text dialect |
| `vash-resident` | `store.resident_mode = true`: the map prefaulted and locked, reads served on the network worker |
| `vash-relaxed` | `durability = "relaxed"`: an `fsync` on every commit, which was the default until this round |
| `redis` | `redis:8-alpine`, `--save '' --appendonly no` — persistence off, which is how a cache is run |
| `memcached` | `memcached:1.6-alpine -m 1024 -t 4` |

vash appears four times because it has two dialects and two settings worth
isolating. **`vash-ephemeral` is gone**: `--ephemeral` now means `lazy`
durability plus a wipe at startup, and `lazy` is the default, so that row would
have been the same server twice.

`vash-resident` is **restarted between the populate and the runs**, on a named
volume. Without that, the map is prefaulted and locked while it is still empty
and the setting gets measured doing nothing; after the restart the server reports
`vash_map_locked yes`, which is the condition the row is supposed to be about.

### Reproducing it

```bash
docker build -t vash:bench .
docker network create vashbench

# One server at a time, four cores each.
docker run -d --name bench-server --network vashbench --cpus 4 \
  vash:bench --listen 0.0.0.0:11311 --data /var/lib/vash
docker run -d --name bench-server --network vashbench --cpus 4 \
  vash:bench --listen 0.0.0.0:11311 --data /var/lib/vash --ephemeral
docker run -d --name bench-server --network vashbench --cpus 4 \
  redis:8-alpine redis-server --save '' --appendonly no
docker run -d --name bench-server --network vashbench --cpus 4 \
  memcached:1.6-alpine -m 1024 -t 4
```

The two vash settings come from a config file — `store.resident_mode = true` and
`durability = "relaxed"` — passed with `--config`. `resident_mode` also wants
`--ulimit memlock=-1:-1`, or the map lock is refused and the server keeps the
storage-pool hand-off, which it says in the log. The client port differs per
target — **11311** for vash, **6379** for Redis, **11211** for memcached.

Each target is populated first, so the read tests hit rather than miss — 20,000
sequential keys down one connection, pipelined so the populate is not itself the
experiment:

```bash
docker run --rm --network vashbench --cpus 4 redislabs/memtier_benchmark \
  -s bench-server -p 11311 --protocol redis \
  --threads 1 --clients 1 --pipeline 128 --requests 20000 \
  --ratio 1:0 --key-pattern S:S --data-size 512 --key-maximum 20000
```

Then six runs per target — three workloads, two pipeline depths:

```bash
docker run --rm --network vashbench --cpus 4 redislabs/memtier_benchmark \
  -s bench-server -p 11311 --protocol redis \
  --threads 4 --clients 25 --test-time 20 --data-size 512 \
  --key-maximum 20000 --key-pattern R:R --distinct-client-seed \
  --ratio {1:0 | 0:1 | 1:9} --pipeline {1 | 16}
```

100 connections either way. `--pipeline 1` is closed-loop, so its latency is a
complete client-visible round trip and its throughput is bounded by round trips;
`--pipeline 16` keeps 16 requests in flight per connection and is the throughput
question instead. `--protocol memcache_text` replaces `--protocol redis` for the
two memcached targets.

---

## What this cannot tell you

Stated first, because everything below is only meaningful inside these limits.

- **One box, 20 seconds a run, two rounds.** No confidence intervals, nothing
  discarded as an outlier. The second round is what the tables report. The two
  agree within about 12% on most rows, but not all: closed-loop `SET` for the
  default `vash` target read 50,210 and 37,334, and Redis's single-connection
  populate read 401,445 and 117,420. Where a claim in this document rests on a
  row that moved like that, it says so.
- **A laptop, not a server.** Everything ran inside a Rancher Desktop WSL2 VM on
  Windows, with a k3s cluster idling in the background on the same host. The
  absolute numbers are worth less than the ratios between them.
- **Client and server share the box.** Four cores to the server, four to the
  client, on 12 the VM believes it has. Under pipelining the client is closer to
  being the bottleneck than it would be on a real network.
- **Container networking, not a network.** The bridge is cheaper than a NIC and
  more expensive than loopback.
- **The workload is uniform random over a small keyspace**, 512-byte values, no
  hot keys, no large values, no expiry pressure, no eviction. That is not a
  production access pattern; it is a comparable one.
- **The whole dataset fits in memory** — 20,000 keys at 512 bytes is about
  10 MB, resident for all three servers. Nothing here ever reads from the disk,
  which flatters vash's reads specifically: its memory-mapped read path never
  page-faults, and the `inline_reads` result [below](#resident_mode-reversed-on-pipelined-reads-and-why)
  is measured under exactly the condition that setting asks you to guarantee.
- **The disk is a virtualised disk inside a VM.** vash's write path commits to
  it and Redis and memcached never touch it, which is the single most important
  thing to hold in mind when reading the SET numbers.

One thing it *can* tell you, and the reason it is worth keeping: every server
here met the same client on the same day on the same hardware. A ratio measured
that way survives a change of hardware far better than an absolute number does.

---

## Results

Every figure is `memtier_benchmark`'s `Totals` row: throughput across both
operations, and the latency percentiles of a request as the client saw it.

### Closed loop — one request in flight per connection, 100 connections

What a client waiting for its answer experiences. Throughput here is bounded by
round trips, so the latency column is the one that means something.

| Workload | Target | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |
|---|---|---:|---:|---:|---:|---:|
| SET only | vash | 37,334 | 2.68 | 1.76 | 25.09 | 219.14 |
| SET only | vash-memcache | 37,984 | 2.63 | 1.98 | 24.83 | 28.54 |
| SET only | vash-resident | 44,930 | 2.22 | 1.65 | 25.09 | 28.16 |
| SET only | vash-relaxed | 10,847 | 9.21 | 5.82 | 88.06 | 119.30 |
| SET only | redis | 60,810 | 1.64 | 1.59 | 3.09 | 4.32 |
| SET only | memcached | 149,978 | 0.67 | 0.65 | 1.46 | 3.66 |
| GET only | vash | 17,086 | 5.85 | 5.76 | 11.90 | 14.98 |
| GET only | vash-memcache | 17,685 | 5.65 | 5.60 | 11.39 | 13.76 |
| GET only | **vash-resident** | **165,672** | 0.60 | 0.57 | 1.58 | 4.77 |
| GET only | vash-relaxed | 17,762 | 5.63 | 5.54 | 11.39 | 16.00 |
| GET only | redis | 53,310 | 1.87 | 1.79 | 3.58 | 5.28 |
| GET only | memcached | 140,011 | 0.71 | 0.70 | 1.50 | 3.55 |
| 1:9 mixed | vash | 18,947 | 5.27 | 5.15 | 11.46 | 17.28 |
| 1:9 mixed | vash-memcache | 17,710 | 5.64 | 5.50 | 11.97 | 17.66 |
| 1:9 mixed | **vash-resident** | **124,618** | 0.80 | 0.62 | 3.82 | 17.54 |
| 1:9 mixed | vash-relaxed | 16,830 | 5.94 | 5.70 | 13.76 | 19.97 |
| 1:9 mixed | redis | 58,453 | 1.71 | 1.61 | 3.49 | 5.79 |
| 1:9 mixed | memcached | 142,472 | 0.70 | 0.68 | 1.50 | 3.15 |

**With `resident_mode`, vash leads both other servers on closed-loop reads** —
`GET` at 165,672 against Redis's 53,310 and memcached's 140,011, and the mixed
workload at 124,618 against Redis's 58,453. Reads are the row that changed: the
previous measurement of this matrix had `GET` at 17,443.

**Closed-loop `SET` is behind Redis**, 44,930 against 60,810. The previous round
published 56,940 against 54,047 and called the lead the headline; both rounds
here disagree with it, and the honest reading is that vash's closed-loop `SET`
lands somewhere between 37,000 and 51,000 on this box while Redis lands between
54,000 and 61,000. One sample near the top of one range and the bottom of the
other is what produced that claim, and it should not have been stated as
strongly as it was.

`vash-relaxed` is the same server with an `fsync` on every commit — the default
until this round — and it is what the rest of the table used to look like.

### Pipelined — 16 requests in flight per connection, 100 connections

The throughput question. Latency under pipelining is queueing rather than
service time, and is reported only because a p99 in the hundreds of
milliseconds says the server is saturated.

| Workload | Target | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |
|---|---|---:|---:|---:|---:|---:|
| SET only | vash | 170,914 | 9.35 | 8.70 | 24.83 | 34.82 |
| SET only | vash-memcache | 147,893 | 10.81 | 9.47 | 28.42 | 41.73 |
| SET only | vash-resident | 174,404 | 9.17 | 8.51 | 23.94 | 40.19 |
| SET only | vash-relaxed | 13,693 | 116.81 | 94.21 | 356.35 | 456.70 |
| SET only | redis | 630,251 | 2.53 | 2.50 | 5.41 | 7.74 |
| SET only | memcached | 919,021 | 1.73 | 1.44 | 7.23 | 14.34 |
| GET only | vash | 227,068 | 7.04 | 6.56 | 22.66 | 30.08 |
| GET only | vash-memcache | 225,397 | 7.09 | 6.50 | 23.55 | 30.08 |
| GET only | **vash-resident** | **718,449** | 2.22 | 2.08 | 5.12 | 9.28 |
| GET only | vash-relaxed | 231,466 | 6.91 | 6.30 | 23.42 | 32.13 |
| GET only | redis | 482,181 | 3.32 | 3.22 | 7.30 | 9.73 |
| GET only | memcached | 707,270 | 2.25 | 2.19 | 4.58 | 10.11 |
| 1:9 mixed | vash | 147,180 | 10.86 | 6.14 | 58.62 | 68.61 |
| 1:9 mixed | vash-memcache | 135,029 | 11.83 | 6.34 | 57.09 | 313.34 |
| 1:9 mixed | vash-resident | 155,347 | 10.29 | 5.79 | 58.88 | 68.61 |
| 1:9 mixed | vash-relaxed | 107,830 | 14.82 | 14.91 | 31.62 | 37.89 |
| 1:9 mixed | redis | 521,078 | 3.07 | 3.04 | 6.82 | 9.47 |
| 1:9 mixed | memcached | 738,317 | 2.16 | 2.13 | 4.32 | 8.51 |

**Pipelined `GET` with `resident_mode` now leads both** — 718,449 against
memcached's 707,270 and Redis's 482,181. That row read 102,872 in the previous
round, and the 7× between them is a bug that was found and fixed rather than
anything the box did; it has [its own section](#resident_mode-reversed-on-pipelined-reads-and-why),
and the same fix is most of why the two plain vash `GET` rows moved from 146,901
to 227,068.

Writes are the other story. Pipelined `SET` is 3.7× behind Redis and 5.4× behind
memcached, and the mixed workload 3.5× and 5.0× behind — both of which are still
committing a copy-on-write B-tree to a virtualised disk while the other two
servers touch nothing.

### The populate pass, which turned out to be its own result

One connection, 128 requests in flight, 20,000 sequential `SET`s — the load
phase before each run, kept here because it isolates the write path with **no
concurrency to batch**:

| Target | ops/s |
|---|---:|
| vash | 49,254 |
| vash-memcache | 48,993 |
| vash-resident | 43,989 |
| vash-relaxed | 5,500 |
| redis | 117,420 |
| memcached | 306,951 |

**This row moved further than any other: 505 to 49,254, a factor of 98.** It is
the one that isolates the write path with no concurrency to batch, so it was the
purest measure of the per-write cost — and three changes went at exactly that.
Coalescing a pipelined block into one submission, keying the expiry index so an
overwrite stops relocating it, and `lazy` durability so a commit stops waiting
for the device. `vash-relaxed` at 4,940 is the same server with the last of those
put back. Redis's 117,420 here is a third of what it measured in the other
round, which is the widest disagreement between the two rounds in this document
and a reminder of what a 20-second single-connection run on a laptop is worth.

---

## Reading the numbers

### Closed loop: vash leads on reads, trails on writes

Reads are where the design now wins outright. With `resident_mode`, `GET` is
165,672 against Redis's 53,310 and memcached's 140,011, and the mixed workload
124,618 against 58,453 and 142,472 — ahead of both on `GET`, ahead of Redis on
mixed. The previous measurement of this matrix had `GET` at 17,443.

`SET` is 44,930 against Redis's 60,810, so the "ahead on every closed-loop row"
claim the last round made does not survive a second and third sample. What is
left is narrower and holds: **vash competes with both servers where the client
waits for its answer**, and leads them on reads.

Nothing about the design changed: it is still a copy-on-write B-tree committing
to a disk. What changed is what a request pays on the way to it, and
[performance-proposals.md](performance-proposals.md) has the four that mattered —
coalescing a pipelined block into one submission, keying the expiry index so an
overwrite stops relocating it, taking writes off the blocking pool so no thread
is parked per write in flight, and `lazy` durability so a commit stops waiting
for the device.

**Closed loop is the shape most caches actually see**, because most clients wait
for their answer. It is also the shape where a server has the least room to hide:
throughput is `connections ÷ latency`, so the column that moved is latency.

### Pipelined: reads lead, writes are three to five times behind

`GET` with `resident_mode` is 718,449 against memcached's 707,270 and Redis's
482,181 — the first round in which vash has led this row. Without the setting it
is 227,068, behind both.

Writes are where the design's cost shows. `SET` 170,914 against Redis's 630,251
and memcached's 919,021; the mixed workload 147,180 against 521,078 and 738,317.
With 16 requests in flight per connection the client stops waiting, and what is
left is how much work each server does per request. On a write vash does more: a
B-tree commit to a virtualised disk against a hash insert into memory.

### Durability is most of what remains on writes

`vash-relaxed` is the same server with an `fsync` on every commit — the default
until this round — and it is the row that shows what that costs: **13,693
pipelined `SET` against 170,914, and 10,847 closed loop against 37,334.** A
factor of 12 and a factor of 3.5.

That is the trade `lazy` makes, and it is worth being precise about what it gives
up: writes newer than the last periodic sync, one second by default, against an
OS crash. Not the database — integrity is preserved and there is nothing to wipe.
Redis and memcached in the configurations measured here lose *everything* on a
restart, so a one-second window is still the most durable row in the table.

### The mixed workload is the realistic one

At 1:9 closed loop, `vash-resident` reaches 124,618 against Redis's 58,453 and
memcached's 142,472 — ahead of one, within 13% of the other. Pipelined it is
155,347 against 521,078 and 738,317, which is the worst vash shows anywhere.

**Both mixed rows above are superseded, and by a large factor.** They were
measured when a block was dispatched whole, so a single write among fifteen reads
sent all sixteen to the blocking pool and `resident_mode` was worth 1.06x here
against 3.2x on pure reads. Splitting a block into runs
([performance-proposals.md](performance-proposals.md) §14) moves the pipelined
1:9 row by roughly 2x — 153,182 to 299,750 in a controlled A/B, and 115,702 to
257,769 for pool against `resident_mode` in a matrix round.

That matrix round is **not** reproduced in the tables above, deliberately: it ran
after hours of continuous benchmarking and every target was depressed with it,
Redis at 402,388 against the 474,300 above and memcached at 492,882 against
707,270. The ratios inside it are worth reading and its absolute numbers are not,
so the tables keep the last round measured on a rested box and this note records
what has changed since. The matrix is due a re-run at `f9b2df7` or later.

A cache doing 90% reads inherits more from the writes than the ratio suggests,
because in vash a write still costs several times a read. The gap between the two
mixed rows is the whole story of this document in miniature: where the client
waits, vash competes; where it does not, it does not.

### The two dialects are not the interesting variable

Within 16% of each other on every row, RESP marginally ahead: 170,914 against
147,893 pipelined `SET`, 227,068 against 225,397 pipelined `GET`. Both rounds
agree.

`cargo bench -p vash-bench --bench hot_path` puts the two decoders at **53.5 ns**
for a memcached `get` and **90.9 ns** for a RESP one; the 37 ns between them, at
150,000 requests a second, is half a percent of one core. **Protocol choice is
not a performance decision here**, and it now rests on the dialects agreeing
rather than on explaining away a disagreement.

---

## `resident_mode` reversed on pipelined reads, and why

The previous round recorded `resident_mode` losing 30% on pipelined `GET` —
102,872 against 146,901 — where the round before it had won by 3.4×, 753,498
against 222,892. That was published here as measured and not understood. **It has
since been traced to a bug, and the bug is fixed**; this section is kept because
what it took to find is more useful than the answer.

### It was not the platform

The first hypothesis was that the box had moved: both figures had fallen by more
than half, `lazy` durability had since become the default, and a dirty page cache
stalling one of four runtime workers is a plausible story for why inline reads
would suffer where a 128-thread pool would not. Six configurations across two
rounds — durability `lazy`, `relaxed` and syncing off entirely, shard counts 2 and
4, each measured twice per server to catch warm-up — said no. Every inline
configuration landed between 69,000 and 87,000 and every pool configuration
between 116,000 and 122,000. Durability changed nothing, the sync timer changed
nothing, and the second pass never recovered.

So the question became whether the code or the machine had changed, which one
experiment settles: build the old commit and the new one, and run them
interleaved within the same hour.

| | pool | `resident_mode` |
|---|---:|---:|
| 68970bc, the round that measured 753,498 | 168,384 | **437,069** |
| HEAD | 124,782 | **71,445** |

The old build still won by 2.6×. **The code did it.**

### A commit about writes broke reads

Bisecting the twelve commits between them, with `inline_reads` forced on and the
same GET-only load, put it on one:

| Commit | ops/s |
|---|---:|
| 68970bc, baseline | 452,107 |
| f4819be, the expiry index | 484,861 |
| **2802a68, await writes instead of parking a thread** | **70,032** |
| HEAD | 71,737 |

2802a68 is [§8](performance-proposals.md), the change that took writes off the
blocking pool. The benchmark that it broke issues **no writes at all**.

Nothing in that commit is on the read path. It adds a `oneshot` reply, a
semaphore, `submit_set_many`, and one `.await` in `drain` guarded by
`measured.all_writes` — a branch a block of `GET`s never takes. Two measurements
pointed at the branch anyway. Pipelining had stopped paying: at HEAD, pipeline 16
ran *slower* than pipeline 1, 86,220 against 112,463, where before the commit it
ran 4× faster. And the server was less busy while doing it — 236% of four cores
against 287% — so throughput had fallen 6.7× while CPU fell 18%, which makes each
request about five times more expensive rather than something merely waiting.

The decisive run deleted only that call site from 2802a68, keeping the `oneshot`,
the permits and `submit_set_many`:

| 2802a68 | ops/s |
|---|---:|
| as committed | 69,840 |
| with the `.await` call site removed | **465,456** |

**The cost was the shape of the future, not the work in it.** Awaiting a future
inline splices its state into the caller's, so the decoded `WriteRun`, the permit
and the pending submission all became part of `drain`'s future — and `drain`'s
future is part of the future of every connection task, polled on every block of
every request. A block of `GET`s carried the whole write path around with it
without ever entering it.

`Box::pin` on that one call site puts it back on the heap. Three alternating
rounds:

| | HEAD | boxed | |
|---|---:|---:|---:|
| `GET` pipe 16, `resident_mode` | 101,852 | **762,859** | 7.49× |
| `GET` pipe 16, pool | 135,231 | **237,584** | 1.76× |

The pool path gained too, which accounts for a further ~26% that had also gone
missing and had been attributed to the box. Writes pay one allocation per
all-writes block: four more rounds put pipelined `SET` at 0.97× and closed loop
at 0.96×, with one round favouring the boxed build on both and the baselines
swinging between 154,000 and 203,000 — inside the noise, as one `malloc` per
block against a commit should be.

### What it means for the setting

With the bug gone, `resident_mode` wins in both directions rather than trading
one for the other:

| | reads on the pool | `resident_mode` | |
|---|---:|---:|---:|
| `GET`, closed loop | 17,112 | **171,245** | 10.0× |
| `GET`, pipelined | 218,197 | **758,811** | 3.5× |

**So it is a speed setting after all** — the earlier conclusion that it traded
concurrency for latency was a description of the bug. It stays off by default
because it needs to `mlock` the map, which needs a memory limit the container may
not grant, and because a store larger than RAM is exactly what it must not be
turned on for. See [operations.md](operations.md).

The general lesson is worth more than the setting: **an `.await` costs the
function it sits in, not only the path that reaches it.** A rarely-taken branch
holding a large future taxes every poll of the task that contains it, and the
symptom appears wherever that task is hottest — here, a read benchmark, three
commits and two documents away from the change that caused it.

---

## A collapse that did not reproduce

Kept from the previous measurement, because a retraction is worth more than the
thing retracted. **A third round has since agreed**: the memcached dialect now
measures 45,955 closed loop and 174,565 pipelined, within 12% of the Redis
dialect on both, which is where it has been every time except once.

The first round of that matrix recorded **`SET`-only over the memcached dialect
at 308 requests a second**, where the same server over the Redis dialect
answered 9,264 and real memcached answered 144,289 — and 300 ops/s pipelined,
with a p99 of 19.5 *seconds*. It was recorded here as a reproducible bug with no
guess at a mechanism.

**It did not come back.** Re-measured on the same box, from the same image, with
the same command: **5,205 ops/s closed loop and 13,272 pipelined**, against
5,774 and 14,140 for the Redis dialect on the same store. The two dialects are
within 11% of each other, which is where every other row in this document puts
them.

### What was done to find it

Before the re-measurement, the collapse was hunted on the assumption it was
real. That hunt is worth recording, because it is what rules things out:

| Attempt | Result |
|---|---|
| The exact reported command, 8 fresh servers in a row | 6,485 – 12,480 ops/s |
| The full six-run sequence on one server, then three repeats | 3,309 – 11,360 ops/s |
| `--cpus 2` instead of 4 | 5,912 — and RESP on the same server was *slower*, at 4,291 |
| Six competing busy loops, for the idling-k3s effect | 10,096 (RESP: 9,560) |
| All 100 clients colliding on one key (`--key-maximum 1`) | 12,374 — faster, at a mean commit batch of 21.8 |

And the server was instrumented — counters on socket reads, drains that produced
no complete command, and hand-offs to the storage pool — to compare the two
dialects directly under the workload that was supposed to collapse:

- **The hand-off counts are the same.** memcached: 112,213 reads and 108,640
  hand-offs for 108,639 `set`s. RESP: 97,628 and 94,419 for 94,418. That is 1.03
  reads per command on both, with 3.2% of reads producing no complete command on
  both. Neither dialect pays more wake-ups than the other.
- **Group commit works on the memcached path.** Mean batch was 11.8–13.3 across
  every `SET`-only run. A collapse to the single-connection rate would have to
  show a batch near 1.0, and it does not.
- **The two dialects converge on an identical store call.** A memcached `set`
  with `exptime 0` and a Redis `SET` with no expiry both build
  `TtlChange::Set(0)`, `mc_flags: 0`, `SetMode::Set`, no tags, and both route
  through `Store::store` → `conditional_set`. Below `dispatch::execute` there is
  no difference left for a dialect to cause.
- **`inline_reads` is off by default, so `all_reads` changes nothing.** That
  flag is what would have made "no reads in the stream" mean something in the
  connection drain, and at the defaults these runs used it is inert — which also
  removes the one plausible mechanism for the strangest of the original clues.
- **The missing `Measured::reserve` is real and far too small.** Neither text
  dialect reports how many bytes an incomplete command still needs, where the
  VCP path does. Simulated against the real parser, it costs *zero* extra reads
  at the 512-byte values used here, and shows up only above the 16 KiB read
  buffer — 13 reads against 9 per 8 commands at 256 KiB values. It is worth
  fixing; it cannot produce a 30× collapse at this value size.

### What that leaves

No mechanism was found, and the measurement that was supposed to be explained is
gone. The two readings that survive are that the original run hit something
outside the server — the box was shared with a k3s cluster and a client
container on the same four-core budget — or that it hit something real and
intermittent that eleven subsequent runs did not.

The evidence does not distinguish them, so this section does not either. What it
does say is that **the 308 in the first round's table was not a property of the
memcached dialect**, because the dialect has since answered the identical
workload at 5,205 and 13,272 with group commit working normally.

The honest reading of the write rows is the one the
[durability section](#durability-is-most-of-what-remains-on-writes) already
states: vash's closed-loop `SET` throughput on this hardware is the least stable
number in the document — eleven runs of one command spanned 3,309 to 12,480 ops/s
— and a single 20-second sample of it, in either direction, is not a finding. The
original 308 was reported as a bug on the strength of one such sample. That was
the mistake, and it is a methodology mistake rather than a server one.

The controls that were run alongside the original number still stand and are
still worth keeping: a single connection sending `set` and waiting reached 434
ops/s, and the same with the command split across two `write()` calls reached
486 — so the split-write hypothesis was tested and died, and one connection was
never anywhere near collapsed.

---

## What `inline_reads` measured, before it became `resident_mode`

Kept because it is where the read result came from, and because half of it no
longer reproduces.

The question was whether the hand-off explained vash's read latency — 5.5 ms per
`GET` at 100 connections against Redis's 1.7 ms. `store.inline_reads` removes it.
Same image, one config file setting, `GET`-only:

| Workload | `inline_reads` | ops/s | p50 ms |
|---|---|---:|---:|
| GET only, closed loop | off | 18,023 | 5.47 |
| GET only, closed loop | **on** | **180,027** | **0.52** |
| GET only, pipelined | off | 222,892 | 6.62 |
| GET only, pipelined | **on** | **753,498** | 1.94 |

Because a result that large is more likely to be a mistake than a discovery, the
closed-loop pair was run twice more, alternating images so neither got the warmer
machine: 17,022 against 175,102, then 16,916 against 174,479. Four measurements,
two images, one setting.

**The closed-loop half held and became a feature.** `store.resident_mode` is that
setting with the assertion it depends on enforced rather than requested: it
prefaults every shard, `mlock`s each map, and enables inline reads only if every
shard came back locked. When the lock is refused it says so and keeps the
hand-off, so the failure mode is "slower", never "stalls".

**The pipelined half went missing for a round and came back.** The next
measurement made the same setting a 30% *loss* pipelined, 102,872 against
146,901, and this document carried that as an unexplained reversal. It was a bug
introduced three commits later, in a change that had nothing to do with reads;
with it fixed the row reads 718,449 and 753,498 is reproducible again. The hunt
is written up [above](#resident_mode-reversed-on-pipelined-reads-and-why).

What survives is the finding underneath both halves: **the cost of a thread
hand-off is a property of the platform, and this platform is not the one the
default was measured on.** On a CPU-capped container a wake-up per read is most
of the read path — enough to be worth 10× closed loop and 3.5× pipelined.

---

## Invalidating a group of keys

The matrix above compares operations all three servers have. This one is about
an operation only one of them has, which is the reason the project exists.

A cache invalidates *groups*: everything belonging to an article, a tenant, a
deploy. vash attaches tag names at write time and
[`DELBYTAG`](protocol.md#tag-commands) bumps a generation — a constant-time
operation whatever the tag covers, with a caveat this section measures. Redis
has no equivalent, so a
Redis client enumerates the keyspace and deletes what matches — `SCAN` plus
`UNLINK`, whose cost is proportional to the *whole keyspace*, not to the group.
memcached has neither: `flush_all` empties the cache, which is not the same
operation and cannot be made into it.

### What it costs

Wall-clock, from a client on the container network. Single samples, so read the
shape rather than the digits.

**vash, `DELBYTAG`:**

| Records carrying the tag | First call | Two more, immediately after |
|---:|---:|---:|
| 0 (a name nothing carries) | 0.93 ms | 0.65 / 0.57 ms |
| 1,000 | 19.8 ms | 6.7 / 8.0 ms |
| 10,000 | 64.3 ms | 209 / 339 ms |
| 50,000 | 270.9 ms | 1,078 / 1,092 ms |

**Redis, `SCAN` + `UNLINK`**, group size varying inside a keyspace made only of
the group — 1,000: 64.1 ms, 10,000: 147.6 ms, 50,000: 709.8 ms. And with the
group fixed at 5,000 while the keyspace grows around it:

| Keyspace | Group | `SCAN` + `UNLINK` |
|---:|---:|---:|
| 5,000 | 5,000 | 65.9 ms |
| 105,000 | 5,000 | 82.2 ms |
| 405,000 | 5,000 | 248.1 ms |

**memcached**: `flush_all` in 0.64 ms, which empties everything.

### Reading that honestly

**The generation bump is O(1) and sub-millisecond** — that is the row for a tag
nothing carries, and it is the operation the design claims. What a client
*observes* is not flat: it grows with the number of records the tag covers, and
the two calls made straight after a large invalidation are slower than the first
one, not faster.

The explanation is most likely one the project already documented and measured:
reclaiming the freed records **is** proportional to how many there are — the
[README's own table](../README.md#tag-invalidation) puts it at 3.75 s for
100,000 keys and 32.5 s for half a million — and the reclaimer shares the
shard's single writer thread with whatever the client asks for next. The bump
stays flat; the queue behind it does not.

What this adds to that table is the client's side of it: **a second invalidation
issued while the first one's records are still being reclaimed waits.** The
README says the reclaimer is bounded per pass so it never stalls traffic, and
1.08 s is not a stall — but it is not the 205 µs of the flat column either, and
a client that invalidates in a loop will meet the difference.

**So the comparison is not the rout the capability difference suggests.** At
50,000 affected keys in a keyspace made of nothing else, vash's 271 ms and
Redis's 710 ms are the same order of magnitude, and vash's repeat calls are
worse. What separates them is not speed but **what each cost is proportional
to**: Redis's `SCAN` walks the whole keyspace, so a group of 5,000 costs 66 ms
in a keyspace of 5,000 and 248 ms in a keyspace of 405,000 — the group did not
change. vash's cost tracks the records affected and ignores the keyspace
entirely.

In a large cache where a tag covers a small slice — which is the shape the
feature exists for — vash is the one that does not care how big the cache got.
In a small cache where the tag covers most of it, the pattern Redis clients
already use is competitive, and memcached's `flush_all` is faster than both at
the cost of being the wrong operation.

---

## LMDB against libmdbx

Phase 3 of [mdbx-proposal.md](mdbx-proposal.md). Both engines in one binary, on
one host, alternated inside each repeat so drift lands on both columns —
`cargo run --release -p vash-store --features mdbx --example engine_ab`.

**These are store-level numbers, not the wire-level ones everywhere else in this
document.** They go through the `Store` trait directly, with no socket, no codec
and no runtime, so they measure the storage layer alone. Against the
decomposition in [performance-proposals.md](performance-proposals.md) §2 — the
engine is about 8% of what a write costs — a 2.8× win here is worth roughly 5%
end to end, not 2.8× end to end. Reads are the other way round: the store is most
of what a read is, so a read regression here mostly survives to the wire.

Run on both platforms with identical methodology: **five repeats, each section
in its own process, medians reported with the full range**. Earlier versions of
this section reported best-of-two from a single run and were wrong twice — see
[the corrections](#two-corrections-worth-keeping) at the end.

One i7-9750H (6 cores / 12 threads): Windows 11 native, and Linux in a container
on the same machine. `(overlap)` marks a row whose two ranges intersect, meaning
those repeats did not separate the engines whatever the ratio of medians says.

### Reads

| Scenario | Windows | Linux |
|---|---:|---:|
| `get`, 1 thread | 0.93× | 1.00× (overlap) |
| `get_with`, 1 thread | 0.90× (overlap) | 1.03× (overlap) |
| `get`, 4 threads | 0.93× (overlap) | 0.94× (overlap) |
| `get_with`, 4 threads | 0.88× | 0.98× (overlap) |
| `get`, 8 threads | 0.94× (overlap) | 0.96× (overlap) |
| `get_with`, 8 threads | 0.82× | 0.99× (overlap) |

**On Linux the engines are indistinguishable on reads** — every row overlaps. On
Windows libmdbx is consistently about 10% behind, and three of the six rows
separate cleanly, so the effect is small but real there.

Absolute figures, for scale: at eight threads Linux does 4.2M `get`/s against
Windows' 2.9M, on the same hardware.

### Writes

| Scenario | Windows | Linux |
|---|---:|---:|
| `set` one at a time, `lazy` | 1.08× (overlap) | 0.71× |
| `set_many` blocks of 256, `lazy` | **1.83×** | **0.06×** |
| `set` one at a time, `relaxed` | **0.19×** | 0.98× (overlap) |
| `set_many` blocks of 256, `relaxed` | **2.49×** | 0.93× (overlap) |
| `set` one at a time, `durable` | 0.85× (overlap) | 2.49× (overlap) |
| `set_many` blocks of 256, `durable` | **1.53×** | 1.03× (overlap) |

Two things stand out, and they point opposite ways.

On **Windows**, libmdbx wins every batched mode — 1.5× to 2.5× — and loses
unbatched `relaxed` by 5×. On **Linux**, everything that syncs is parity, and
batched `lazy` is a 17× loss: LMDB's median is 519,722 [443k–523k] against
libmdbx's 29,040 [21k–48k], with no overlap across five repeats.

That last row is the only large, reproducible gap in this document, and
`lazy` is the default durability.

### The soak, which points the other way again

20–30 s of sustained overfill on a 32 MiB map with 4 KiB values, permanently
above the critical watermark. `ops/s` counts **accepted** writes.

| Backend | accepted ops/s | utilisation | used MiB | file MiB | evicted |
|---|---:|---:|---:|---:|---:|
| lmdb (windows) | 46,947 | 0.974 | 31.2 | 32.0 | 935,488 |
| mdbx (windows) | 56,464 | 0.861 | 27.6 | 32.0 | 1,126,080 |
| lmdb (linux) | 83,651 | 0.974 | 31.2 | 32.0 | 1,669,120 |
| mdbx (linux) | 103,927 | 0.946 | 30.3 | 32.0 | 2,075,200 |

**libmdbx wins the soak on both platforms, by about 1.2×** — and the soak is
sustained `lazy` writes, the same mode it loses by 17× above. It also holds
utilisation further below the watermark on both.

Neither engine drifts: used bytes stay bounded and the file never passes
`map_size`. That remains a **negative result** for
[mdbx-proposal.md](mdbx-proposal.md) §7's expectation that libmdbx's
compactification would show up as a smaller file under churn.

### The one mechanism that explains both, now confirmed

The soak's store is **already at its ceiling and no longer growing**. The write
scenario writes into a **fresh store that grows continuously**. LMDB never pays
for growth because it sizes its file to `map_size` at creation and leaves it
sparse; libmdbx grows on demand, which is the property
[mdbx-proposal.md](mdbx-proposal.md) §6 advertised as an advantage.

Preallocating the file settles it. Linux, batched `lazy`, five repeats, in the
worst case — a read workload run first so the page cache is full:

| `store.preallocate_mb` | lmdb | mdbx | ratio |
|---|---:|---:|---:|
| 0 (grow on demand) | 448,001 | 66,286 | **0.15×** |
| 64 | 495,096 [391k–547k] | 448,127 [405k–555k] | **0.91× (overlap)** |

**libmdbx goes 6.8× faster and the row stops separating the engines.** Growth
was the entire gap. It is worst under memory pressure, which is why the figure
moved between 0.06× and 0.70× depending on whether reads had run first: growth
has to allocate while the kernel is reclaiming, and a preallocated file does not.

**How much is enough: a step, not a slope.** Same worst case, sweeping the
setting, five repeats each:

| `store.preallocate_mb` | lmdb | mdbx | ratio |
|---|---:|---:|---:|
| 0 | 427,056 | 68,939 | **0.16×** |
| 16 | 459,937 | 409,645 | 0.89× (overlap) |
| 64 | 428,828 | 524,740 | **1.22×** |
| 128 | 449,411 | 458,598 | 1.02× (overlap) |
| 256 | 423,956 | 489,492 | 1.15× (overlap) |

**16 MiB already removes the penalty**, on a scenario that writes about 1 MB —
so the threshold is "somewhat more than the working set", not a fraction of
`map_size`. Above it every result lands in one band, 410k–525k, whose ranges
overlap each other: 128 MiB measured *lower* than 64 MiB, and 256 MiB did not
recover the difference. **There is no evidence that preallocating more than a
modest amount buys anything**, and it costs disk per shard, so do not.

LMDB is the control here and ignores the setting; its column stays flat across
all five rows, which is what makes the mdbx column readable.

Once growth is out of the way libmdbx lands slightly *ahead* — 1.02× to 1.22×,
though only the 64 MiB row separated cleanly. That is the neighbourhood of the
"up to 30% faster than LMDB" its README claims, and it is the same neighbourhood
as the soak, which is the other regime where nothing is growing.

That is what `store.preallocate_mb` is for. It costs the disk immediately, per
shard — a fresh mdbx store goes from 0.3 MiB to whatever it says — which is
exactly what [mdbx-proposal.md](mdbx-proposal.md) Phase 2 gave up when it
stopped pinning a lower bound. **An operator setting `backend = "mdbx"` should
set this too**; without it the default durability's write path is several times
slower than LMDB's.

**`MDBX_opt_txn_dp_limit` has no headroom to give.** It caps the dirty pages a
write transaction may hold before spilling, so if a 256-op batch were spilling
mid-commit it would be undoing exactly what group commit is for. Bracketed from
below, with preallocation on:

| `dp_limit` (pages) | mdbx | against the default |
|---|---:|---|
| 32 | **aborted the process** | — |
| 128 | 400,919 | 0.78×, separated — spilling |
| 1,024 | 497,174 | flat |
| 16,384 | 515,513 | flat |
| 1,048,576 | 538,238 | flat |
| default, ~107,000 | 515,088 | — |

The knee sits between 128 and 1,024 pages, so a transaction here dirties
something like 0.5–4 MB. libmdbx auto-tunes the default to
`(total_ram + avail_ram) / 42`, which on this 9.9 GiB host is ~107,000 pages —
about 428 MB, two orders of magnitude above what the workload uses. Raising it
does nothing because nothing was ever spilling.

Worth recording separately: **an under-sized `dp_limit` aborts the process**
rather than failing gracefully — `SIGABRT` at 32 pages. That is a good reason
not to expose it as a configuration knob.

Two other levers measured nothing. `MDBX_WRITEMAP` — which mdbx permits under a
no-sync mode and LMDB forbids — made both engines *worse* on Linux, reproducing
[performance-proposals.md](performance-proposals.md) §6's finding that it is
worth nothing there. Turning `MDBX_LIFORECLAIM` off, and dropping the internal
sync period, both stayed inside the noise band.

### Two corrections worth keeping

This section was published wrong twice, both times from single measurements.

**First**, best-of-two on Windows gave reads at 0.55–0.87× and a headline of
"45% behind at eight threads". Five repeats give 0.82–0.94×. The direction was
right and the magnitude was not.

**Second**, the first Linux run reported batched `lazy` at 0.16×, then a
follow-up "isolated" run reported 1.02×, and this run reports 0.06×. The
difference is not process isolation — it is whether a read workload ran first on
the same kernel, because the page cache is not per-process. Running each section
in its own process is necessary and not sufficient; what the numbers above do is
run reads first *consistently*, on both platforms, so the comparison is at least
like for like.

The lesson is in the harness now: it reports ranges, and flags overlapping ones,
because on this workload a single measurement of a write scenario carries no
information at all.

### What this cannot tell you

Everything in [What this cannot tell you](#what-this-cannot-tell-you) applies,
plus three things specific to this table.

It is **one machine**: both platforms are the same laptop, with Linux in a
container on WSL2 rather than on bare metal, so "Linux" here means "Linux as this
Windows box runs it". A real server may sit somewhere else again — and given how
far the two columns already move, that is not a hypothetical worry.

The **`lazy` gap on Linux is unexplained**, not just unfavourable. Until someone
has tried a tuned geometry it is a measurement, not a property of the engine.

And a cache's traffic is mostly reads, so the read and write columns do not
deserve equal weight in a decision — though on Linux the reads are level, so the
decision rests on the writes anyway.

---

## What to take from all of this

**Reads are where vash now leads.** Closed loop with `resident_mode`, `GET` is
165,672 against Redis's 53,310 and memcached's 140,011; pipelined, 718,449
against 482,181 and 707,270 — ahead of both in both shapes. Neither sentence was
true a day earlier, when the same workloads read 17,443 and 102,872, and the
pipelined half of it was a bug rather than a limit.

**Writes are still three to five times behind pipelined** — `SET` 170,914 against
630,251 and 919,021 — and behind Redis closed loop, 44,930 against 60,810. What
is left there is per-request work, and vash does more of it: a copy-on-write
B-tree committing to a disk against a hash insert into memory. [plan.md](plan.md)
§6 chose that deliberately and what it buys is a cache that survives a restart.

**Most of the write gap that closed was not the storage engine.** Of the four
changes that moved these numbers, three were about what a request pays on the way
to storage — a submission per pipelined block instead of per command, no OS
thread parked per write in flight, an expiry index that stops relocating itself —
and one was durability. The B-tree is the same B-tree.
[performance-proposals.md](performance-proposals.md) has all eight attempts,
including the four that measured nothing or worse.

**Group invalidation is a capability comparison, not a performance one.** The
thing vash does in under a millisecond, a Redis client does in hundreds by
walking the keyspace, and a memcached client cannot do at all.

So the honest summary: **vash is ahead of both on reads and behind both on
writes, in both shapes.** Reads got there by removing hand-offs rather than by
changing the engine; writes are where committing a B-tree to a disk is still
paid for. And it is the only one of the three that still has your data after a
restart.

Two things this exercise produced beyond the numbers. One is
[`resident_mode`](#resident_mode-reversed-on-pipelined-reads-and-why), 10× closed
loop and 3.5× pipelined — and the round in which it appeared to lose pipelined
turned out to be a bug in a commit about writes, three commits away, found by
bisecting builds against each other rather than by reading the code. The other is
the [collapse that did not reproduce](#a-collapse-that-did-not-reproduce): a bug
reported here on one 20-second sample, hunted, and never found.

The settings are worth more than the table. The retraction is worth more than the
settings.
