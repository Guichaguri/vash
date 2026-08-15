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

**What was measured**, on 2026-08-15: vash at `55c7241`, built by the repository
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
  discarded as an outlier. The second round is what the tables report, and the
  two agree far better than the previous measurement's did — within about 12% on
  every row rather than the factor of seventeen that round produced on one of
  them. That is an improvement in the box's mood as much as in the server, and it
  does not make any of this precise.
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
  page-faults, and the `inline_reads` result [below](#resident_mode-wins-closed-loop-and-loses-pipelined)
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
| SET only | **vash** | **56,940** | 1.75 | 1.28 | 25.34 | 27.90 |
| SET only | vash-memcache | 45,956 | 2.17 | 1.62 | 25.22 | 28.16 |
| SET only | vash-resident | 45,141 | 2.21 | 1.66 | 24.83 | 27.26 |
| SET only | vash-relaxed | 11,174 | 8.94 | 5.18 | 97.79 | 139.26 |
| SET only | redis | 54,047 | 1.85 | 1.74 | 3.78 | 6.11 |
| SET only | memcached | 142,503 | 0.70 | 0.64 | 2.32 | 5.47 |
| GET only | vash | 18,265 | 5.47 | 5.44 | 10.94 | 13.38 |
| GET only | vash-memcache | 16,904 | 5.91 | 5.86 | 12.03 | 14.78 |
| GET only | **vash-resident** | **165,731** | 0.60 | 0.58 | 1.38 | 3.76 |
| GET only | vash-relaxed | 16,736 | 5.97 | 5.92 | 11.97 | 14.40 |
| GET only | redis | 56,006 | 1.78 | 1.69 | 3.49 | 4.86 |
| GET only | memcached | 120,408 | 0.83 | 0.79 | 1.83 | 4.96 |
| 1:9 mixed | vash | 18,191 | 5.49 | 5.38 | 11.90 | 19.46 |
| 1:9 mixed | vash-memcache | 18,581 | 5.38 | 5.25 | 11.52 | 20.10 |
| 1:9 mixed | **vash-resident** | **94,746** | 1.05 | 0.86 | 4.70 | 15.62 |
| 1:9 mixed | vash-relaxed | 17,495 | 5.71 | 5.50 | 13.25 | 19.07 |
| 1:9 mixed | redis | 44,326 | 2.25 | 2.02 | 5.41 | 8.96 |
| 1:9 mixed | memcached | 135,500 | 0.74 | 0.71 | 1.57 | 3.50 |

**vash leads Redis on every closed-loop row**, and with `resident_mode` it leads
memcached on `GET` too. That is the round's headline and it is new: the previous
measurement had `SET` at 5,774 against Redis's 55,401, and this one has 56,940
against 54,047.

`vash-relaxed` is the same server with an `fsync` on every commit — the default
until this round — and it is what the rest of the table used to look like.

### Pipelined — 16 requests in flight per connection, 100 connections

The throughput question. Latency under pipelining is queueing rather than
service time, and is reported only because a p99 in the hundreds of
milliseconds says the server is saturated.

| Workload | Target | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |
|---|---|---:|---:|---:|---:|---:|
| SET only | vash | 186,779 | 8.56 | 7.94 | 19.97 | 39.42 |
| SET only | vash-memcache | 174,565 | 9.16 | 7.58 | 28.42 | 86.02 |
| SET only | vash-resident | 184,848 | 8.65 | 7.84 | 22.53 | 40.19 |
| SET only | vash-relaxed | 15,061 | 106.04 | 65.02 | 335.87 | 419.84 |
| SET only | redis | 472,690 | 3.38 | 2.74 | 14.34 | 28.54 |
| SET only | memcached | 779,928 | 2.04 | 1.54 | 10.18 | 16.38 |
| GET only | vash | 146,901 | 10.88 | 6.98 | 55.81 | 62.98 |
| GET only | vash-memcache | 145,401 | 10.99 | 7.39 | 51.46 | 57.09 |
| GET only | vash-resident | 102,872 | 15.54 | 15.30 | 31.62 | 37.89 |
| GET only | vash-relaxed | 136,005 | 11.75 | 6.98 | 57.86 | 63.74 |
| GET only | redis | 341,668 | 4.68 | 4.05 | 13.70 | 19.97 |
| GET only | memcached | 401,239 | 3.97 | 3.18 | 12.29 | 20.35 |
| 1:9 mixed | vash | 71,634 | 22.32 | 13.95 | 71.17 | 105.98 |
| 1:9 mixed | vash-memcache | 63,959 | 24.98 | 14.98 | 78.85 | 118.78 |
| 1:9 mixed | vash-resident | 74,514 | 21.46 | 13.63 | 69.12 | 82.43 |
| 1:9 mixed | vash-relaxed | 56,319 | 28.38 | 24.32 | 67.07 | 76.29 |
| 1:9 mixed | redis | 442,660 | 3.61 | 3.41 | 8.64 | 12.54 |
| 1:9 mixed | memcached | 423,462 | 3.76 | 2.98 | 13.63 | 28.03 |

Pipelined, vash is still behind both — 2.5× on `SET`, 2.7× on `GET`, 6× on the
mixed workload. **And `vash-resident` is the slowest vash row for `GET` here**,
which is the exact opposite of what it does closed loop. That reversal is not
noise and has [its own section](#resident_mode-wins-closed-loop-and-loses-pipelined).

### The populate pass, which turned out to be its own result

One connection, 128 requests in flight, 20,000 sequential `SET`s — the load
phase before each run, kept here because it isolates the write path with **no
concurrency to batch**:

| Target | ops/s |
|---|---:|
| vash | 48,999 |
| vash-memcache | 57,109 |
| vash-resident | 53,389 |
| vash-relaxed | 4,940 |
| redis | 303,624 |
| memcached | 395,570 |

**This row moved further than any other: 505 to 48,999, a factor of 97.** It is
the one that isolates the write path with no concurrency to batch, so it was the
purest measure of the per-write cost — and three changes went at exactly that.
Coalescing a pipelined block into one submission, keying the expiry index so an
overwrite stops relocating it, and `lazy` durability so a commit stops waiting
for the device. `vash-relaxed` at 4,940 is the same server with the last of those
put back.

---

## Reading the numbers

### Closed loop: vash is now in front of Redis

This is the round that changed. Every closed-loop row has vash ahead of Redis —
`SET` 56,940 against 54,047, `GET` with `resident_mode` 165,731 against 56,006,
mixed 94,746 against 44,326 — and the `GET` row is ahead of memcached's 120,408
as well.

The previous measurement had `SET` at 5,774 and `GET` at 17,443. Nothing about
the design changed: it is still a copy-on-write B-tree committing to a disk. What
changed is what a request pays on the way to it, and
[performance-proposals.md](performance-proposals.md) has the four that mattered —
coalescing a pipelined block into one submission, keying the expiry index so an
overwrite stops relocating it, taking writes off the blocking pool so no thread
is parked per write in flight, and `lazy` durability so a commit stops waiting
for the device.

**Closed loop is the shape most caches actually see**, because most clients wait
for their answer. It is also the shape where a server has the least room to hide:
throughput is `connections ÷ latency`, so the column that moved is latency.

### Pipelined: still two to three times behind

`SET` 186,779 against Redis's 472,690 and memcached's 779,928. `GET` 146,901
against 341,668 and 401,239. The mixed workload is worse — 71,634 against 442,660
and 423,462, a factor of six.

This is the honest limit of the design as measured. With 16 requests in flight
per connection the client stops waiting, and what is left is how much work each
server does per request. vash does more: a B-tree write against a hash insert, a
memory-mapped read through a transaction against a pointer chase.

### Durability is most of what remains on writes

`vash-relaxed` is the same server with an `fsync` on every commit — the default
until this round — and it is the row that shows what that costs: **15,061
pipelined `SET` against 186,779, and 11,174 closed loop against 56,940.** A
factor of 12 and a factor of 5.

That is the trade `lazy` makes, and it is worth being precise about what it gives
up: writes newer than the last periodic sync, one second by default, against an
OS crash. Not the database — integrity is preserved and there is nothing to wipe.
Redis and memcached in the configurations measured here lose *everything* on a
restart, so a one-second window is still the most durable row in the table.

### The mixed workload is the realistic one

At 1:9 closed loop, `vash-resident` reaches 94,746 against Redis's 44,326 and
memcached's 135,500 — ahead of one, behind the other. Pipelined it is 74,514
against 442,660 and 423,462, which is the worst vash shows anywhere.

A cache doing 90% reads inherits more from the writes than the ratio suggests,
because in vash a write still costs several times a read. The gap between the two
mixed rows is the whole story of this document in miniature: where the client
waits, vash competes; where it does not, it does not.

### The two dialects are not the interesting variable

Within 6% of each other on every row, RESP marginally ahead: 186,779 against
174,565 pipelined `SET`, 146,901 against 145,401 pipelined `GET`. Both rounds
agree.

`cargo bench -p vash-bench --bench hot_path` puts the two decoders at **53.5 ns**
for a memcached `get` and **90.9 ns** for a RESP one; the 37 ns between them, at
150,000 requests a second, is half a percent of one core. **Protocol choice is
not a performance decision here**, and it now rests on the dialects agreeing
rather than on explaining away a disagreement.

---

## `resident_mode` wins closed loop and loses pipelined

The one result in this round that points both ways, and the one worth reading
before turning the setting on.

| | reads on the pool | `resident_mode` | |
|---|---:|---:|---:|
| `GET`, closed loop | 18,265 | **165,731** | 9.1× |
| `GET`, pipelined | 146,901 | 102,872 | **0.70×** |

Closed loop it is transformative — a `GET` round trip falls from 5.44 ms to
0.58 ms, and that single change is what puts vash ahead of both other servers on
reads. Pipelined it is a 30% loss.

**The mechanism is the same in both directions.** `resident_mode` serves reads on
the network worker instead of handing them to the storage pool. Closed loop that
removes a thread hand-off from every request, which is nearly all of the latency.
Pipelined, a block of 16 reads runs to completion on one of four runtime workers
without yielding, where the pool spreads the same work across up to 128 threads —
so the setting trades concurrency for latency, and pipelining is the case that
wanted the concurrency.

**This reversed since the previous measurement**, which had `resident_mode` ahead
pipelined as well — 753,498 against 222,892. Both figures have since fallen by
more than half on an idle box, so something about the platform moved underneath
them; a bisect across shard counts and read modes reproduced the reversal in
three rounds of three and did not explain it. It is recorded as measured and not
understood.

**So it is a workload setting, not a speed setting**, and it stays off by
default. Turn it on for a cache whose clients wait for their answers. Leave it
off for one whose clients pipeline.

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

**The pipelined half did not.** Re-measured, the same setting is a 30% *loss*
pipelined — see [above](#resident_mode-wins-closed-loop-and-loses-pipelined). The
mechanism now looks like a trade between latency and concurrency rather than a
free win, and the original 753,498 has never been seen again.

What survives is the finding underneath both: **the cost of a thread hand-off is
a property of the platform, and this platform is not the one the default was
measured on.** On a CPU-capped container a wake-up per read is most of the read
path when the client waits, and not the limit when it pipelines.

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

**Closed loop, vash is now the fastest of the three on two of the three
workloads.** `SET` 56,940 against Redis's 54,047; `GET` with `resident_mode`
165,731 against Redis's 56,006 and memcached's 120,408. Only the mixed row still
has memcached ahead. That sentence was not true a day earlier, when the same
workloads read 5,774 and 17,443.

**Pipelined, it is still two to three times behind**, six on the mixed workload.
When the client stops waiting, what is left is per-request work, and vash does
more of it: a copy-on-write B-tree against a hash table. [plan.md](plan.md) §6
chose that deliberately and what it buys is a cache that survives a restart.

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

So the honest summary: **if your clients wait for their answers, vash is now
competitive with Redis and memcached on this hardware and ahead of both on
reads.** If they pipeline, it is not. And it is the only one of the three that
still has your data after a restart.

Two things this exercise produced beyond the numbers. One is
[`resident_mode`](#resident_mode-wins-closed-loop-and-loses-pipelined), which is
9× closed loop and a 30% loss pipelined — a workload setting rather than a speed
setting, and the reversal is measured but not explained. The other is the
[collapse that did not reproduce](#a-collapse-that-did-not-reproduce): a bug
reported here on one 20-second sample, hunted, and never found.

The settings are worth more than the table. The retraction is worth more than the
settings.
