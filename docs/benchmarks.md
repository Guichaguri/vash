# vash beside Redis and memcached

Every number in [README](../README.md#performance) was measured against vash
alone, with vash's own load generator over VCP. This document does the other
thing: **the same client, the same box, the same workload, three servers**, so
the comparison is between the servers rather than between benchmark harnesses.

Read the [What this cannot tell you](#what-this-cannot-tell-you) section before
quoting anything here. The short version: one laptop, one container host, 20
seconds per run, one run each. These are orders of magnitude, not measurements.

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

**What was measured**, on 2026-08-14: vash at `7c71098`, built by the repository
`Dockerfile` into a 4.7 MB `scratch` image; `redis:8-alpine` (Redis 8.10.0);
`memcached:1.6-alpine` (memcached 1.6.45); `memtier_benchmark` 2.5.1. Host: a
Windows laptop running Rancher Desktop, 12 CPUs and 10 GB visible to the WSL2
VM, with a k3s cluster idling on the same machine.

**Five targets across three servers**:

| Target | What it is |
|---|---|
| `vash-resp` | vash on its defaults, driven over the Redis dialect |
| `vash-memcache` | the same server and the same defaults, driven over the memcached text dialect |
| `vash-ephemeral` | vash with `--ephemeral`: nothing is synced to disk |
| `redis` | `redis:8-alpine`, `--save '' --appendonly no` — persistence off, which is how a cache is run |
| `memcached` | `memcached:1.6-alpine -m 1024 -t 4` |

vash appears three times on purpose. Twice because it speaks two of the three
dialects being compared and the cost of a dialect is a thing this document can
isolate; a third time because its default is a **disk-backed store** and the two
servers it is being compared against are memory. `--ephemeral` is the closest
vash gets to what Redis and memcached are, and the gap between it and
`vash-resp` is the price of the design rather than of the code.

### Reproducing it

```bash
docker build -t vash:bench .
docker network create vashbench

# One server at a time, four cores each.
docker run -d --name bench-server --network vashbench --cpus 4 \
  vash:bench --listen 0.0.0.0:11311 --data /var/lib/vash
docker run -d --name bench-server --network vashbench --cpus 4 \
  redis:8-alpine redis-server --save '' --appendonly no
docker run -d --name bench-server --network vashbench --cpus 4 \
  memcached:1.6-alpine -m 1024 -t 4
```

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

- **One box, one run, 20 seconds.** No repetitions, no confidence intervals,
  nothing discarded as an outlier. A second run would move these numbers.
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
  page-faults, and the `inline_reads` result [below](#does-inline_reads-explain-the-read-latency)
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
| SET only | vash-resp | 9,264 | 10.79 | 8.03 | 108.54 | 219.13 |
| SET only | vash-memcache | **308** | 324.55 | 280.57 | 839.68 | 1073.15 |
| SET only | vash-ephemeral | 10,824 | 9.23 | 5.89 | 47.87 | 53.50 |
| SET only | redis | 54,410 | 1.84 | 1.66 | 4.22 | 6.82 |
| SET only | memcached | 144,289 | 0.69 | 0.64 | 2.17 | 5.95 |
| GET only | vash-resp | 17,950 | 5.57 | 5.50 | 11.13 | 13.38 |
| GET only | vash-memcache | 16,950 | 5.89 | 5.79 | 12.10 | 15.62 |
| GET only | vash-ephemeral | 16,343 | 6.11 | 6.01 | 12.67 | 16.25 |
| GET only | redis | 52,872 | 1.89 | 1.74 | 4.19 | 6.53 |
| GET only | memcached | 125,407 | 0.79 | 0.74 | 2.22 | 4.77 |
| 1:9 mixed | vash-resp | 12,838 | 7.79 | 5.95 | 22.66 | 577.53 |
| 1:9 mixed | vash-memcache | 14,533 | 6.87 | 6.17 | 19.84 | 32.26 |
| 1:9 mixed | vash-ephemeral | 16,025 | 6.23 | 6.11 | 13.12 | 18.30 |
| 1:9 mixed | redis | 51,464 | 1.94 | 1.76 | 4.86 | 7.71 |
| 1:9 mixed | memcached | 124,306 | 0.80 | 0.74 | 2.29 | 5.95 |

### Pipelined — 16 requests in flight per connection, 100 connections

The throughput question. Latency under pipelining is queueing rather than
service time, and is reported only because a p99 in the hundreds of
milliseconds says the server is saturated.

| Workload | Target | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |
|---|---|---:|---:|---:|---:|---:|
| SET only | vash-resp | 9,691 | 164.74 | 156.67 | 327.68 | 475.13 |
| SET only | vash-memcache | **300** | 5015.56 | 235.52 | 19529.73 | 19529.73 |
| SET only | vash-ephemeral | 48,249 | 33.14 | 17.54 | 83.97 | 92.67 |
| SET only | redis | 542,201 | 2.94 | 2.77 | 7.17 | 9.98 |
| SET only | memcached | 868,101 | 1.83 | 1.47 | 8.10 | 14.21 |
| GET only | vash-resp | 194,201 | 8.23 | 7.52 | 26.11 | 33.02 |
| GET only | vash-memcache | 221,782 | 7.20 | 6.66 | 23.81 | 30.08 |
| GET only | vash-ephemeral | 209,076 | 7.65 | 6.88 | 26.62 | 35.58 |
| GET only | redis | 455,211 | 3.51 | 3.31 | 8.45 | 11.39 |
| GET only | memcached | 644,484 | 2.47 | 2.33 | 5.50 | 9.41 |
| 1:9 mixed | vash-resp | 59,351 | 27.23 | 15.55 | 346.11 | 544.77 |
| 1:9 mixed | vash-memcache | 77,655 | 20.58 | 21.25 | 39.94 | 49.66 |
| 1:9 mixed | vash-ephemeral | 121,487 | 13.16 | 7.07 | 63.23 | 70.14 |
| 1:9 mixed | redis | 458,677 | 3.48 | 3.28 | 8.77 | 14.02 |
| 1:9 mixed | memcached | 651,020 | 2.45 | 2.30 | 6.11 | 11.52 |

### The populate pass, which turned out to be its own result

One connection, 128 requests in flight, 20,000 sequential `SET`s — the load
phase before each run, kept here because it isolates the write path with **no
concurrency to batch**:

| Target | ops/s |
|---|---:|
| vash-resp | 402 |
| vash-memcache | 336 |
| vash-ephemeral | 9,211 |
| redis | 380,272 |
| memcached | 359,712 |

---

## Reading the numbers

### Reads: the same order of magnitude, two to three times behind

Pipelined `GET` is 194k–222k on vash against 455k for Redis and 644k for
memcached. That is the honest shape of it: vash is in the same class, a factor
of two to three back, and the factor is not the protocol — its two dialects are
14% apart and sit at the same distance from the other two servers.

Closed loop is where the distance shows: **5.5–6 ms per round trip at 100
connections against Redis's 1.7 ms and memcached's 0.74 ms.** Throughput follows
directly (100 connections ÷ 6 ms ≈ 17k), so the read *rate* in that table is a
restatement of the read *latency*, not an independent finding. Something in
vash's read path costs several milliseconds per request under this concurrency
that neither of the other two pays.

The suspect is the hand-off: vash runs reads on the storage thread pool rather
than on the network worker, because a read that page-faults must not block a
worker serving other connections. `store.inline_reads` turns that off, and its
documentation says it measured within noise on the development machine — a
machine that was not a four-core cgroup. Measuring it here is
[below](#does-inline_reads-explain-the-read-latency).

### Writes: this is the design, and the design has a price

Pipelined `SET`: **9,691 for vash against 542k for Redis and 868k for
memcached** — 56× and 90×. The ordering is not close and it is not a surprise:
vash writes through a copy-on-write B-tree that commits to a disk, and the other
two write to memory. [plan.md](plan.md) §6 chose that, and the README already
records the original 250k/s write goal as *wrong rather than missed*. This is
what the choice costs on the wire, in the environment described above.

Two things soften and sharpen it:

- **Concurrency is what makes vash's writes work at all.** 402 ops/s on one
  connection, 9,264 across a hundred — 23× from group commit, which forms a
  batch out of whatever queued during the previous commit. It cannot help a
  single-threaded writer, and there is nothing to be done about that.
- **The disk is most of it.** `--ephemeral` — same code, nothing synced — is
  48,249 against 9,691 pipelined, a 5× penalty for durability under load, and
  9,211 against 402 with a single connection, which is 23×. What that says is
  that the sync cost is amortised by batching *and* by concurrency, and that a
  workload with neither pays it in full.

Even at 48k, ephemeral vash is 11× behind Redis on writes. The gap is not all
persistence.

### The mixed workload is the realistic one

At 1:9, pipelined: vash 59k (relaxed) or 121k (ephemeral), Redis 459k, memcached
651k. A cache doing 90% reads sits between the read and write results and
inherits more from the writes than the ratio suggests, because in vash a write
is ~20× the cost of a read rather than ~1×.

### The two dialects are not the interesting variable — except once

On reads and on the mixed workload, vash's memcached dialect is slightly
*faster* than its Redis one: 222k against 194k pipelined `GET`, 78k against 59k
mixed — 14% and 31%.

That is more than parsing can account for. `cargo bench -p vash-bench --bench
hot_path` prices the two decoders at **53.5 ns** for a memcached `get` and
**90.9 ns** for a RESP one; the 37 ns between them, at 222,000 requests a
second, is 0.8% of one core. The dialect gap is an order of magnitude larger
than the dialect's own cost, so whatever separates these two runs is downstream
of the parser — which is the useful thing to know, because it means protocol
choice is not a performance decision here.

And then there is the row this document exists to be honest about.

---

## An unexplained collapse: memcached-dialect writes, alone, at concurrency

**`SET`-only over the memcached dialect answers 308 requests a second where the
same server over the Redis dialect answers 9,264 and real memcached answers
144,289.** Under pipelining it does not improve — 300 ops/s, with a p99 of 19.5
*seconds*. This is on vash's side, it is reproducible, and as of writing it is
not explained.

What is established:

| Observation | Number |
|---|---|
| The collapse, closed loop and pipelined | 308 and 300 ops/s |
| The same store, same workload, Redis dialect | 9,264 ops/s |
| Real memcached, same client, same protocol, same workload | 144,289 ops/s |
| **The same dialect on a 1:9 mixed workload** | 14,533 ops/s total — about 1,450 writes/s, no collapse |
| A single connection sending `set` and waiting, container network | 434 ops/s — the commit latency, and nothing worse |
| The same, with the command split across two `write()` calls | 486 ops/s — no difference |

So it is not the protocol's parse cost, not the store, not the disk, and not the
command arriving split across TCP segments, which was the first hypothesis and
died on the last row. It needs **writes, that dialect, and concurrency, with no
reads in the stream** — remove any one and it goes away. The mixed row is the
one that rules out everything simple: the same server, over the same dialect, at
the same concurrency, writes fine as long as reads are interleaved.

The controls in that table are single-connection on purpose. A 32-connection
version of the same raw client was run and thrown away: at that width the client
itself became the limit and its numbers disagreed with `memtier` in both
directions, which makes it evidence about the client rather than about the
server.

Two things this document will not do: guess at a mechanism, and pretend the
matrix above is unaffected. The `vash-memcache` `SET`-only rows are a bug being
measured, not a protocol being compared.

---

## Does `inline_reads` explain the read latency?

The read section left a question: 5.5 ms per `GET` at 100 connections, against
1.7 ms for Redis and 0.74 ms for memcached. The suspect was the hand-off — vash
answers reads on the storage thread pool, not on the network worker.

`store.inline_reads` turns the hand-off off. Its documentation in
[vash.example.toml](../vash.example.toml) says it measured within run-to-run
noise, and invites exactly this: *"worth leaving off unless your own
measurements disagree."* In a four-core container, they disagree.

Same image, same flags, one config file setting `inline_reads = true`, same
`GET`-only workload:

| Workload | `inline_reads` | ops/s | p50 ms | p99 ms |
|---|---|---:|---:|---:|
| GET only, closed loop | off (default) | 17,950 | 5.50 | 11.13 |
| GET only, closed loop | **on** | **171,170** | **0.54** | 1.68 |
| GET only, pipelined | off (default) | 194,201 | 7.52 | 26.11 |
| GET only, pipelined | **on** | **875,405** | 1.71 | 3.70 |

**9.5× closed loop and 4.5× pipelined**, and it moves vash's pipelined read
throughput from *behind* both other servers to ahead of both — 875k against
memcached's 644k and Redis's 455k, on the same four cores.

Because a result that large is more likely to be a mistake than a discovery, the
closed-loop pair was run twice more, alternating between the two images so that
neither got the warmer machine:

| Round | `inline_reads` off | `inline_reads` on |
|---|---:|---:|
| 1 | 16,800 ops/s | 162,223 ops/s |
| 2 | 15,018 ops/s | 158,640 ops/s |

Four measurements, two images, one setting between them.

The setting is not free and the reason it is off by default has not changed: a
read that page-faults now blocks a network worker and every connection sharing
it, so the assertion it asks for — that the working set is resident — has to be
true. What this measurement adds is that **the cost of the hand-off is a
property of the platform, and this platform is not the one the default was
measured on.** On a CPU-capped container, a thread wake-up per read is most of
the read path.

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

## What to take from all of this

**Reads are competitive, and with `inline_reads` they lead** — 875k pipelined
`GET` against memcached's 644k and Redis's 455k on the same four cores. On the
default, they are a factor of two to three behind.

**Writes are not competitive, and that is a design decision rather than a
defect.** 9,691 pipelined `SET` against 542k and 868k. A copy-on-write B-tree
committing to a disk cannot be made to look like an in-memory hash table, and
[plan.md](plan.md) §6 chose the B-tree deliberately: what it buys is a cache
that survives a restart. The README already records the original write goal as
wrong rather than missed; this is that admission with the other two servers
standing next to it.

**Group invalidation is not a performance comparison, it is a capability one.**
The thing vash does in under a millisecond, a Redis client does in hundreds by
walking the keyspace, and a memcached client does not do at all.

So the honest summary: if the workload is write-heavy and the data is
disposable, memcached and Redis are faster by a margin no tuning here will
close. If the workload is read-heavy, needs the cache to survive a restart, and
needs to invalidate by group rather than by key, that is the trade this server
is making — and the read side of it is competitive on the same hardware.

Two things this exercise produced beyond the numbers: a [read-path
setting](#does-inline_reads-explain-the-read-latency) worth 4–9× in a container
and off by default, and a [reproducible
bug](#an-unexplained-collapse-memcached-dialect-writes-alone-at-concurrency).
Both are worth more than the table.
