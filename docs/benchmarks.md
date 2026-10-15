# vash beside Redis and memcached

Every number in [README](../README.md#performance) was measured against vash
alone, with vash's own load generator over VCP. This document does the other
thing: **the same client, the same box, the same workload, three servers**, so
the comparison is between the servers rather than between benchmark harnesses.

Read the [What this cannot tell you](#what-this-cannot-tell-you) section before
quoting anything here. The short version: one laptop, one container host, 20
seconds per run, the whole matrix run twice. These are orders of magnitude, not
measurements.

**The current measurement is
[the fourth](#the-fourth-measurement-a-protocol-that-survives-what-the-third-one-taught)**,
and it is the one to quote: a fresh server per run, repopulated, hits verified,
and every row carrying its own spread and sample count. The three rounds before
it are kept as the record of how it got there, each with its corrections
attached.

**The matrix was re-run on a dedicated server, and the read lead below
did not survive it.** The write ratios did, almost exactly. Read
[The third measurement](#the-third-measurement-a-dedicated-server-and-a-read-lead-that-did-not-survive)
before quoting any read figure from the laptop tables — it is the one that ran on
hardware, and where the two disagree it is the one to believe.

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

**And a third time, on 2026-09-06, on a dedicated EC2 server** — same six
targets, same client, same workloads, on hardware that is not a laptop and a data
directory that is not a virtualised disk. That round has
[its own section](#the-third-measurement-a-dedicated-server-and-a-read-lead-that-did-not-survive)
with its own method note. Everything in this section and the tables that follow
it still describes the laptop.

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
  absolute numbers are worth less than the ratios between them. **This turned out
  to be understated**: the matrix was later re-run on a dedicated server and the
  host moved the *ratios* too, not only the absolutes — Redis and memcached
  roughly doubled on pipelined reads while vash did not, which reversed the read
  standings entirely. See
  [The third measurement](#the-third-measurement-a-dedicated-server-and-a-read-lead-that-did-not-survive).
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

On a dedicated server this narrows to a lead over Redis and a tie with memcached
— 284,318 against 149,368 and 284,823. See
[The third measurement](#the-third-measurement-a-dedicated-server-and-a-read-lead-that-did-not-survive).

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
memcached's 707,270 and Redis's 482,181. **This is the row a dedicated server
reversed**: on hardware it is 608,662 against 792,115 and 1,102,280, behind both,
because the other two servers roughly doubled off this host and vash did not. Do
not quote this lead —
[The third measurement](#the-third-measurement-a-dedicated-server-and-a-read-lead-that-did-not-survive)
supersedes it. That row read 102,872 in the previous
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

## The third measurement: a dedicated server, and a read lead that did not survive

Everything above ran on a laptop, and the section that says so
[predicted](#what-this-cannot-tell-you) that the ratios would survive a change of
hardware better than the absolute numbers would. Half of that held. The write
ratios survived almost exactly. **The read lead did not: it was substantially an
artifact of the host, and on a server vash trails both other servers on pipelined
reads rather than leading them.**

### What changed in the method

Measured on **2026-09-06**, vash at `1644c2f`, same images, same client, same
workloads. Host: an **EC2 `c6id.2xlarge`** — 8 vCPUs on 4 physical cores, 16 GiB,
Amazon Linux 2023, kernel 6.18.44, Docker 25.0.14.

**The matrix ran four times, and every table here is the median of the four, per
column** — not a single round, which is what the laptop tables report and what
this document has twice been burned by. Two of these rows would have been
overstated by 35% and 50% had the second round been published on its own; the
[variance section](#the-rounds-agree-except-where-they-emphatically-do-not)
says which and by how much.

Two deliberate departures from the published protocol:

**The data directory is on the instance-store NVMe**, not on the EBS root volume.
The single most important caveat on the laptop's `SET` rows was that vash was
committing to a virtualised disk inside a VM while Redis and memcached touched
nothing. That caveat is gone: this is a local NVMe device with no hypervisor
storage layer under it.

**`--cpuset-cpus` replaces `--cpus`, and each side gets two physical cores rather
than four.** `--cpus 4` is a CFS quota, not an affinity — the scheduler stays
free to run the server and the client on the same core. On 12 vCPUs with two
quotas of four that was slack; on 8 vCPUs it is a collision on every run. The
server is pinned to `0,1,4,5` and the client to `2,3,6,7`, which `lscpu -p` on
this instance confirms are two complete physical cores each, both SMT siblings
included, with no core shared across the split.

So the client is isolated from the server in a way it never was on the laptop,
and both have half the cores they had. **Absolute numbers here are not comparable
to the tables above.** The comparison that matters is between the six targets
within this round, which met the same client on the same box on the same day.

### On the server: closed loop, one request in flight per connection

| Workload | Target | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |
|---|---|---:|---:|---:|---:|---:|
| SET only | vash | 99,580 | 1.00 | 0.95 | 2.29 | 4.29 |
| SET only | vash-memcache | 83,411 | 1.20 | 1.15 | 2.62 | 4.51 |
| SET only | vash-resident | 97,729 | 1.02 | 0.97 | 2.34 | 4.33 |
| SET only | vash-relaxed | 28,214 | 3.54 | 3.62 | 5.23 | 5.90 |
| SET only | redis | 150,252 | 0.67 | 0.64 | 1.29 | 1.52 |
| SET only | **memcached** | **297,343** | 0.34 | 0.32 | 0.50 | 0.61 |
| GET only | vash | 100,399 | 1.00 | 0.98 | 2.10 | 2.90 |
| GET only | vash-memcache | 99,134 | 1.01 | 0.99 | 2.16 | 2.98 |
| GET only | **vash-resident** | **284,318** | 0.35 | 0.35 | 0.63 | 0.90 |
| GET only | vash-relaxed | 101,956 | 0.98 | 0.96 | 2.07 | 2.85 |
| GET only | redis | 149,368 | 0.67 | 0.65 | 1.31 | 1.47 |
| GET only | **memcached** | **284,823** | 0.35 | 0.33 | 0.53 | 0.61 |
| 1:9 mixed | vash | 98,374 | 1.02 | 0.99 | 2.19 | 2.90 |
| 1:9 mixed | vash-memcache | 99,896 | 1.00 | 0.97 | 2.21 | 3.01 |
| 1:9 mixed | vash-resident | 203,257 | 0.49 | 0.46 | 1.33 | 2.92 |
| 1:9 mixed | vash-relaxed | 91,174 | 1.10 | 1.07 | 2.38 | 3.02 |
| 1:9 mixed | redis | 149,117 | 0.67 | 0.65 | 1.31 | 1.42 |
| 1:9 mixed | **memcached** | **283,260** | 0.35 | 0.34 | 0.53 | 0.57 |

**`vash-resident` and memcached tie on closed-loop `GET`** — 284,318 against
284,823, a gap of 0.2% with memcached fractionally ahead, and both rows are
stable to within 1.3% across the four rounds, so the tie is a real one rather
than two noisy numbers happening to cross. Both are 1.90× Redis. The laptop had
this row as a vash lead over both; on a server it is a lead over Redis and a tie
with memcached.

**The tails are the laptop's, not vash's.** Closed-loop `SET` p99.9 was 219.14 ms
in the table above and is 4.29 ms here, with p99 down from 25.09 ms to 2.29 ms.
Nothing in the code changed; the 219 ms was the host.

### On the server: pipelined, 16 requests in flight per connection

| Workload | Target | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |
|---|---|---:|---:|---:|---:|---:|
| SET only | vash | 280,159 | 5.71 | 5.47 | 10.34 | 13.44 |
| SET only | vash-memcache | 246,659 | 6.48 | 6.24 | 12.61 | 16.00 |
| SET only | vash-resident | 281,955 | 5.67 | 5.44 | 10.34 | 13.31 |
| SET only | vash-relaxed | 48,950 | 32.67 | 33.41 | 51.97 | 53.76 |
| SET only | redis | 951,838 | 1.68 | 1.66 | 2.27 | 3.78 |
| SET only | **memcached** | **1,504,356** | 1.06 | 0.86 | 4.27 | 9.09 |
| GET only | vash | 582,170 | 2.75 | 2.56 | 6.73 | 8.57 |
| GET only | vash-memcache | 444,248 | 3.60 | 3.28 | 9.57 | 12.35 |
| GET only | vash-resident | 608,662 | 2.63 | 2.61 | 5.12 | 6.37 |
| GET only | vash-relaxed | 521,813 | 3.06 | 2.81 | 7.73 | 9.73 |
| GET only | redis | 792,115 | 2.02 | 2.13 | 2.89 | 5.36 |
| GET only | **memcached** | **1,102,280** | 1.44 | 1.35 | 2.09 | 4.93 |
| 1:9 mixed | vash | 353,615 | 4.52 | 4.25 | 10.62 | 13.47 |
| 1:9 mixed | vash-memcache | 241,939 | 6.61 | 6.27 | 15.10 | 18.75 |
| 1:9 mixed | vash-resident | 364,655 | 4.38 | 4.29 | 9.66 | 12.03 |
| 1:9 mixed | vash-relaxed | 263,072 | 6.08 | 5.98 | 12.10 | 14.46 |
| 1:9 mixed | redis | 818,156 | 1.95 | 2.03 | 2.81 | 3.81 |
| 1:9 mixed | **memcached** | **1,209,791** | 1.32 | 1.23 | 1.91 | 3.54 |

**This is the reversal.** Pipelined `GET` with `resident_mode` is 608,662 against
Redis's 792,115 and memcached's 1,102,280 — behind both, where the laptop had it
ahead of both at 718,449 against 482,181 and 707,270. All three of those rows are
stable to within 4% across the four rounds, so this is not a sampling accident.

Nothing regressed. The other two servers roughly doubled and vash did not:
memcached went 707,270 → 1,102,280 and Redis 482,181 → 792,115 on the same
workload, while `vash-resident` went 718,449 → 608,662 on half the cores. **The
laptop was penalising Redis and memcached more than it was penalising vash**, and
the pipelined read lead recorded above was mostly that penalty rather than vash's
read path.

Writes land where the document already said they would: `SET` 3.4× behind Redis
and 5.4× behind memcached, the mixed workload 2.3× and 3.4× — the "three to five
times behind" claim, on hardware that removes its most-cited excuse.

> **The plain `vash` row here is depressed by this document's own protocol, and
> by about 27%.** The matrix runs pipelined `GET` immediately after closed-loop
> `GET`, and a closed-loop run drives `tokio`'s blocking pool from 7 threads to
> ~90 which then outlive it. Measured on servers that had served nothing else,
> the same configuration medians **741,206** rather than the 582,170 above —
> level with Redis rather than 1.3× behind it. Redis and memcached have no such
> pool, so the effect is vash's alone and the protocol is biased against it.
> `vash-resident` is unaffected, serving reads inline and never growing the pool,
> so its 608,662 stands. See
> [performance-proposals.md §23](performance-proposals.md#23-implemented-a-read-is-routed-by-the-size-of-its-block).
> **The tables are left as measured.** The re-run that corrects them has since
> been done — see
> [The fourth measurement](#the-fourth-measurement-a-protocol-that-survives-what-the-third-one-taught),
> where the same configuration reads 834,427 rather than 582,170 and leads
> Redis rather than trailing it.

### The populate pass

One connection, 128 in flight, 20,000 sequential `SET`s, median of four passes:

| Target | ops/s |
|---|---:|
| vash | 128,448 |
| vash-memcache | 132,182 |
| vash-resident | 129,278 |
| vash-relaxed | 68,433 |
| redis | 731,196 |
| memcached | 747,884 |

This was the laptop's most volatile row — Redis measured 401,445 and 117,420 in
two rounds of it. On this host it is one of the steadiest: no target moved more
than 3.5% across the four passes.

vash moved 49,254 → 128,448 and Redis 117,420 → 731,196, so the gap on this row
went from 2.4× to 5.7×. The laptop's two Redis samples bracketed nothing useful;
neither was representative of what the software does on hardware.

`vash-relaxed` at 68,433 against `lazy`'s 128,448 is a 1.9× penalty for an
`fsync` per commit, against 9× on the laptop — the one place where real hardware
substantially improves vash's standing rather than worsening it.

### The rounds agree, except where they emphatically do not

The matrix ran four times. **29 of 36 rows have a total spread under 5% across
all four, and 34 under 16%** — far tighter than the laptop, and tight enough that
most of this table could have been trusted from a single round. Every Redis row,
every memcached row and every `vash-resident` row is inside 4%.

Two rows are not, and they are not close:

| Row | r1 | r2 | r3 | r4 | median | spread |
|---|---:|---:|---:|---:|---:|---:|
| `vash-memcache` 1:9, pipelined | 248,784 | **481,420** | 235,095 | 204,883 | 241,939 | **114%** |
| `vash-memcache` `GET`, pipelined | 459,955 | **686,015** | 428,540 | 413,781 | 444,248 | **61%** |
| `vash` 1:9, pipelined | 304,810 | 348,818 | 360,412 | 358,412 | 353,615 | 16% |
| `vash-relaxed` `GET`, pipelined | 572,217 | 507,971 | 531,405 | 512,221 | 521,813 | 12% |

**Every unstable row is a pipelined vash read without `resident_mode`.** All six
`vash-resident` rows agree within 3.3%, as do all of Redis's and all of
memcached's.

**The mechanism was found later, and it is the blocking pool's size.** A
pipelined read's throughput depends on how many threads the *previous* workload
left alive in `tokio`'s blocking pool — 836,387 on a server that had served
nothing else against 540,658 on one that had just served a closed-loop run, which
grows the pool from 7 threads to ~90. The spread here is that inheritance varying
between rounds. `resident_mode` serves reads inline, never touches the pool, and
is stable for exactly that reason — a second, independent argument for the setting
beyond the throughput it buys. See
[performance-proposals.md §23](performance-proposals.md#23-implemented-a-read-is-routed-by-the-size-of-its-block),
which proposes routing by block size so the pool is never inflated by traffic it
is the wrong tier for.

**Two rounds were not enough, and this document has now made that mistake three
times.** The first two rounds of this matrix were published with the second
round's figures, following the convention the laptop tables use. On the two
`vash-memcache` rows the second round was the outlier — 481,420 against a median
of 241,939, and 686,015 against 444,248 — so those numbers were overstated by
99% and 54% until rounds three and four arrived. That is the same failure mode as
the [closed-loop `SET` overclaim](#closed-loop-vash-leads-on-reads-trails-on-writes)
the laptop rounds produced: one sample near the top of a wide range, reported as
a point. Hence medians of four, and hence this table.

### The ~100k plateau

vash's three closed-loop rows are 99,580, 100,399 and 98,374 — `SET`, `GET` and a
9:1 read-heavy mix, three workloads with very different per-request work, landing
within 2% of each other, and each stable to within 4.3% across the four rounds.
That flatness is the finding. It is not a work bound; at 100 connections it is
1 ms of something structural per request.

`resident_mode` breaks reads through it to 284,318 and leaves `SET` at 97,729,
which names the structure: the hand-off to the storage threads. That is exactly
what `inline_reads` removes and exactly what writes cannot escape, because they
wait on the writer queue by design. On the laptop this was invisible — the
default `GET` row read 17,086 and the ceiling was somewhere else entirely. It is
the clearest read yet on what a request pays before it reaches storage, and it is
the number to attack next.

### Reproducing the server round

As [above](#reproducing-it), with the data directory on a local NVMe mount and
the two quotas replaced by pinned core sets:

```bash
docker run -d --name bench-server --network vashbench --cpuset-cpus 0,1,4,5 \
  -v /bench/data-vash:/var/lib/vash vash:bench \
  --listen 0.0.0.0:11311 --data /var/lib/vash
```

```bash
docker run --rm --network vashbench --cpuset-cpus 2,3,6,7 \
  redislabs/memtier_benchmark -s bench-server -p 11311 --protocol redis \
  --threads 4 --clients 25 --test-time 20 --data-size 512 \
  --key-maximum 20000 --key-pattern R:R --distinct-client-seed \
  --ratio {1:0 | 0:1 | 1:9} --pipeline {1 | 16}
```

A bind-mounted data directory arrives with the host directory's ownership rather
than the image's, so it needs `chown 65534:65534` before the unprivileged server
can write to it — the same failure the [Dockerfile](../Dockerfile) warns about
for volumes.

---

---

## The fourth measurement: a protocol that survives what the third one taught

The third measurement's tables were published with two corrections attached and
a third owed: its pipelined rows were depressed ~27% by the protocol itself,
and the re-run that would fix them was recorded as not done. This is that
re-run, on **2026-10-15**, vash at `dbe7e2e`, on an EC2 `c6id.2xlarge` with the
data on the instance-store NVMe and the same core split — server `0,1,4,5`,
client `2,3,6,7`.

### Five changes to the protocol, each from something that went wrong

1. **A fresh server per run, not per target.** The old protocol ran six
   workloads against one server, so a closed-loop run left ~90 blocking threads
   alive and the pipelined run after it measured 553,000 against 836,000 on a
   server that had served nothing else. Redis and memcached have no such pool,
   so that bias was vash's alone.
2. **Repopulated after every restart.** Restarting alone empties Redis and
   memcached — persistence is off by design — and an earlier attempt at this
   had them "winning" at 1.36M and 2.31M while serving an empty keyspace.
3. **Hits checked on every read run**, and a miss-serving run discarded rather
   than reported.
4. **Seven samples on vash's pipelined rows**, three elsewhere. That row is
   bimodal between roughly 550,000 and 860,000, and five samples disagreed with
   five in the third measurement — 766,970 against 579,794.
5. **The resident targets wait for the data to be warm**, not for the flag.
   `vash_inline_reads` turns yes moments after startup while the store is still
   empty and locking nothing trivially succeeds; waiting on it alone began each
   run before the populated data was resident and served the first seconds at
   the pooled rate. That cost closed-loop `GET` 214,034 against the 241,960
   below. `warmed_bytes >= high_water_bytes` is no better — both read 32,768 on
   an empty store — so the mark is captured *after* populating and waited for
   by value.

**Every row carries its own spread and sample count.** A median over a bimodal
distribution mostly reports which mode won, and a reader is entitled to see
which rows that applies to.

### The target list changed with the default

`resident_mode` is on by default since §27, so `vash` **is** the resident
configuration and the old `vash-resident` row is gone. In its place is
**`vash-pooled`** — `resident_mode = false`, which is what the shipped default
used to be, and now the more informative contrast.

### Closed loop — one request in flight per connection, 100 connections

| Workload | Target | ops/s | avg ms | p50 | p99 | p99.9 | spread | n |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SET only | vash | 105,396 | 0.95 | 0.90 | 2.06 | 5.02 | 1% | 3 |
| SET only | vash-memcache | 88,344 | 1.13 | 1.09 | 2.33 | 5.47 | 4% | 3 |
| SET only | vash-pooled | 105,228 | 0.95 | 0.90 | 2.08 | 5.34 | 4% | 3 |
| SET only | vash-relaxed | 43,284 | 2.31 | 2.21 | 5.21 | 8.13 | 0% | 3 |
| SET only | redis | 139,490 | 0.72 | 0.69 | 1.39 | 1.72 | 1% | 3 |
| SET only | **memcached** | **291,590** | 0.34 | 0.32 | 0.52 | 0.65 | 0% | 3 |
| GET only | **vash** | **217,452** | 0.46 | 0.44 | 0.96 | 1.30 | 7% | 3 |
| GET only | vash-memcache | 205,157 | 0.49 | 0.46 | 1.10 | 1.49 | 7% | 3 |
| GET only | vash-pooled | 75,520 | 1.32 | 1.31 | 2.73 | 3.57 | 10% | 3 |
| GET only | vash-relaxed | 217,842 | 0.46 | 0.44 | 0.97 | 1.29 | 3% | 3 |
| GET only | redis | 136,473 | 0.73 | 0.71 | 1.42 | 1.76 | 1% | 3 |
| GET only | **memcached** | **278,509** | 0.36 | 0.34 | 0.54 | 0.61 | 2% | 3 |
| 1:9 mixed | vash | 183,658 | 0.54 | 0.53 | 1.14 | 2.45 | 3% | 3 |
| 1:9 mixed | vash-memcache | 177,141 | 0.56 | 0.55 | 1.18 | 2.11 | 2% | 3 |
| 1:9 mixed | vash-pooled | 97,332 | 1.03 | 1.01 | 2.19 | 2.94 | 10% | 3 |
| 1:9 mixed | vash-relaxed | 162,212 | 0.62 | 0.57 | 1.83 | 5.02 | 0% | 3 |
| 1:9 mixed | redis | 137,839 | 0.73 | 0.70 | 1.41 | 1.54 | 1% | 3 |
| 1:9 mixed | **memcached** | **276,967** | 0.36 | 0.34 | 0.54 | 0.60 | 3% | 3 |

**vash leads Redis on closed-loop reads by 1.78×** — 241,960 against 136,151 —
and reaches 87% of memcached. On the mixed workload it leads Redis by 1.33×.
**It trails both on writes**, 102,742 against 139,787 and 287,554, which is
where this document has had it since the engine was chosen.

**`vash-pooled` is the row that shows what §27 bought**: 72,571 against vash's
241,960 on the same hardware and the same binary, differing only in whether the
map is pinned and reads served inline. That is the 2.9× the setting was turned
on for, reproduced under a protocol built to doubt it.

### Pipelined — 16 requests in flight per connection, 100 connections

| Workload | Target | ops/s | avg ms | p50 | p99 | p99.9 | spread | n |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SET only | vash | 329,914 | 4.85 | 4.64 | 9.66 | 16.25 | 3% | 7 |
| SET only | vash-memcache | 261,739 | 6.11 | 5.70 | 13.76 | 18.05 | 1% | 7 |
| SET only | vash-pooled | 336,822 | 4.75 | 4.51 | 9.66 | 15.81 | 1% | 7 |
| SET only | vash-relaxed | 72,752 | 21.98 | 22.78 | 36.86 | 47.36 | 2% | 7 |
| SET only | redis | 870,818 | 1.83 | 1.81 | 2.48 | 4.09 | 0% | 3 |
| SET only | **memcached** | **1,408,515** | 1.13 | 0.92 | 4.25 | 8.45 | 2% | 3 |
| GET only | **vash** | **838,074** | 1.91 | 1.80 | 4.51 | 6.59 | **12%** | 7 |
| GET only | vash-memcache | 468,346 | 3.41 | 3.10 | 8.83 | 11.58 | **50%** | 7 |
| GET only | vash-pooled | 834,832 | 1.91 | 1.80 | 4.67 | 6.78 | **39%** | 7 |
| GET only | vash-relaxed | 775,745 | 2.06 | 1.91 | 5.41 | 7.29 | 27% | 7 |
| GET only | redis | 722,374 | 2.21 | 2.32 | 3.12 | 5.82 | 0% | 3 |
| GET only | **memcached** | **1,123,937** | 1.42 | 1.33 | 2.03 | 5.05 | 1% | 3 |
| 1:9 mixed | **vash** | **475,712** | 3.36 | 3.13 | 7.74 | 12.54 | **54%** | 7 |
| 1:9 mixed | vash-memcache | 360,336 | 4.43 | 4.19 | 10.24 | 16.00 | **77%** | 7 |
| 1:9 mixed | vash-pooled | 581,197 | 2.75 | 2.61 | 6.62 | 12.99 | 46% | 7 |
| 1:9 mixed | vash-relaxed | 289,072 | 5.53 | 5.25 | 12.67 | 16.51 | 9% | 7 |
| 1:9 mixed | redis | 750,481 | 2.13 | 2.22 | 3.07 | 4.32 | 0% | 3 |
| 1:9 mixed | **memcached** | **1,174,632** | 1.35 | 1.27 | 1.95 | 3.89 | 1% | 3 |

**vash now leads Redis on pipelined reads too** — 834,427 against 714,525 —
which the third measurement had backwards at 608,662 against 792,115, because
its protocol was depressing that row and not Redis's. memcached remains ahead
at 1,113,769.

**The spread column is doing real work here.** Every vash pipelined read row
carries 25–43%; every Redis and memcached row carries 0–2%. That asymmetry is
the subject of
[performance-proposals.md §30](performance-proposals.md#30-the-pipelined-reads-two-modes-are-latency-not-work),
which established it is latency rather than work, eliminated six explanations,
and could not name a seventh.

### The regression this run found, and the fix

**The first pass of this matrix ran against the build that shipped §27**, and
one row was a defect rather than a result: `vash` served **179,079** on the
mixed pipelined workload where `vash-pooled` served **579,306** — the same
binary, 3.2× apart, with `resident_mode` the only difference. `vash-memcache`
and `vash-relaxed` collapsed the same way.

The cause was in [`conn.rs`](../crates/vash-server/src/conn.rs). A mixed block
is split into runs so each can take the tier that suits it, and the split is
undone when it would not pay:

```rust
if !state.inline_reads() && runs.len() > 1 { /* collapse to one run */ }
```

That guard is §14's, added because a mixed block fragmenting into several hops
instead of one cost a 1:9 pipelined workload 147,180 → 85,521. Turning
`resident_mode` on by default turned `inline_reads` on, which skipped the
collapse, which reinstated exactly that fragmentation. §27's own measurements
could not have caught it: they were pure-read and pure-write workloads, where a
block is one run and there is nothing to fragment.

The deeper error was §23's. Its argument is that **a hop is paid per block**,
and the implementation then applied the threshold to the **run** — the same
thing in a uniform block, and not in a mixed one, where sixteen commands split
into short read runs that each clear a threshold the block does not.

**Fixed in
[performance-proposals.md §31](performance-proposals.md#31-implemented-23s-threshold-was-applied-to-the-run-and-a-hop-is-paid-per-block),
and the tables above are the fixed build.** The two builds interleaved rep by
rep inside one session, because comparing across sessions on this host cannot
separate a change from drift:

| | before §31 | after §31 |
|---|---:|---:|
| 1:9 pipelined, median of 7 | 196,202 | **588,445** |
| 1:9 pipelined, mean | 266,988 | 538,559 |
| closed-loop `GET`, median of 7 | 222,544 | 219,394 |

**3.0× on the mixed row**, and 588,445 is above the 581,197 the same workload
reaches with `resident_mode` off — full recovery rather than partial.
Closed-loop `GET` is unchanged within noise, which the code predicts: a
one-command block takes the identical branch either way.

### Absolute numbers on this host drift about 10% between sessions

Closed-loop `GET` on one unchanged configuration measured **276,571**,
**241,960**, **217,452** and **~220,000** across four sessions on two
`c6id.2xlarge` instances. The interleaved A/B above is what established that:
both builds read ~220,000 in the same session, where the matrix runs hours
apart had read 241,960 and 217,452 and would have made §31 look like a 10%
regression.

**So read the comparisons within a table, not between tables.** Every figure in
this document that matters is a ratio measured against another server or
another configuration on the same box in the same session, and that is the only
kind of number this hardware supports to better than ~10%.

### What this does not re-measure

The populate pass has no fourth-measurement row. It is a single-connection load
phase rather than a workload, the third measurement found it one of the
steadiest rows on this hardware, and nothing since has touched the write path.

These numbers also come from a different `c6id.2xlarge` than the third
measurement's, which is worth remembering before reading small differences
between the two as changes: closed-loop `GET` measured 276,571 there against
241,960 here on the same configuration, and nothing in between explains 13%.

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

**Half of that narrower claim survived the move to a server, and half did not.**
On an EC2 `c6id.2xlarge` the same row reads 284,318 against Redis's 149,368 and
memcached's 284,823 — still a 1.9× lead over Redis, but a tie with memcached to
within 0.2%, memcached fractionally ahead, rather than a lead over it. Both rows
hold to within 1.3% over four rounds, so the tie is measured rather than
inferred. "Competes with both where the client
waits" is the part that holds, and it is the part worth quoting.

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

### Pipelined: reads led on this host, writes are three to five times behind

`GET` with `resident_mode` is 718,449 against memcached's 707,270 and Redis's
482,181 — the first round in which vash has led this row. Without the setting it
is 227,068, behind both.

> **Retracted on hardware.** This row is the one claim in the document that a
> dedicated server reversed outright. On an EC2 `c6id.2xlarge` it reads 608,662
> against Redis's 792,115 and memcached's 1,102,280 — **behind both**. The lead
> above was not vash's read path; it was the laptop holding the other two servers
> down harder than it held vash. They roughly doubled on that hardware and vash
> did not. See
> [The third measurement](#the-third-measurement-a-dedicated-server-and-a-read-lead-that-did-not-survive).
> The write half of this heading survived intact.

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

**Page size does nothing either, in either direction.** This was the last
structural lever rather than a threshold — libmdbx can set it at creation and
LMDB cannot change it at all, so it was the one place the two engines could have
been given genuinely different geometry. Writes, mdbx medians: 530,665 at 4 KiB,
559,863 at 8 KiB, 559,221 at 16 KiB, 489,708 at 32 KiB — the middle two sit
inside the 4 KiB row's own spread, so only the 32 KiB decline is real.

Reads needed more care, because the read section runs long enough that the
machine drifts between invocations: LMDB, which ignores the setting, moved 16%
across two runs on its own. The ratio within each run is the noise-controlled
number:

| read row | 4 KiB | 8 KiB | 16 KiB |
|---|---|---|---|
| `get`, 1t | 0.96 | 0.97 | 0.88 |
| `get_with`, 1t | 0.96 | 0.96 | 0.95 |
| `get`, 4t | 0.95 | 0.88 | 0.96 |
| `get_with`, 4t | 0.94 | 0.95 | 0.96 |
| `get`, 8t | 0.97 | 0.88 | 0.94 |
| `get_with`, 8t | 1.20 | 1.01 | 1.00 |

There is no trend: mdbx sits at 0.88–0.97 of LMDB whatever the page size, and
the spread between page sizes is no wider than the spread within a single row.
That is not surprising once stated — these reads are served warm from the page
cache, so page size is changing B-tree fanout rather than I/O size, and at
1 KiB values the difference across this range is at most one level of the tree.
One pointer chase does not show up against a page-cache hit.

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

## What TLS costs

Phase 0 of [tls-proposal.md](tls-proposal.md), which proposed TLS and costed it
from the literature rather than from this box. These are the measurements that
were supposed to confirm that arithmetic. Two of them contradict it, one found
a bug that invalidated a third of them, and the diagnosis of that bug was
itself wrong the first time.

Three harnesses, because the question has three shapes:

```bash
cargo run --release -p vash-bench --example tls_spike            # handshakes and encryption, in isolation
cargo run --release -p vash-bench --bin load -- --tls            # end to end, against the TLS port
cargo test -p vash-server --features tls --test tls              # the regression tests for the stall below
cargo build --release -p vash-server --features tls              # what the feature costs to ship
```

One i7-9750H (6 cores / 12 threads): Windows 11 native, and Alpine musl in a
container on the same machine — the same pair, and the same
[caveat](#what-this-cannot-tell-you), as the engine comparison above. **Five
repeats, medians reported with the full range.** `rustls` 0.23.43, TLS 1.3
only, an ECDSA P-256 leaf under a P-256 CA unless a row says otherwise.

### The build, which was supposed to be the easy part

The proposal assumed `aws-lc-rs` would need `cmake` and NASM and that `ring`
would need nothing, and recommended `ring` on that basis. Neither half is true.
`cmake`, `nasm` and `perl` are absent from both toolchains here, and **both
providers built anyway** — 33s and 96s on Windows, 44s and 115s on musl.

The reason is in `aws-lc-sys`'s build log, and it is worse than a build
failure:

```
cargo:warning=Building with: CC
cargo:rustc-cfg=universal
```

That is the fallback path: no cmake, so no assembly, so no AES-NI-tuned code —
a portable C build that compiles anywhere and runs at about half speed. It does
not warn that it is slower. It does not fail. **The recommendation survives and
the reasoning does not**: `ring` is still right, not because `aws-lc-rs` cannot
be built here but because what gets built here is not the `aws-lc-rs` anyone
means when they benchmark it.

### Handshakes

Microseconds of server CPU per handshake, one thread, both ends driven in
memory. The socket version of this measured the Windows ephemeral port table
instead — thousands of connect-and-close cycles a second exhaust it, and each
one sits in `TIME_WAIT` for two minutes.

| leaf | resumed | Windows `ring` | Windows `aws-lc-rs` | musl `ring` | musl `aws-lc-rs` |
|---|---|---:|---:|---:|---:|
| ECDSA P-256 | no | **308 µs** | 749 µs | 528 µs | 621 µs |
| ECDSA P-256 | yes | 298 µs | 813 µs | 476 µs | 493 µs |
| RSA-2048 | no | 804 µs | 1,283 µs | 1,133 µs | 1,176 µs |
| RSA-2048 | yes | 297 µs | 720 µs | 448 µs | 449 µs |

Four things fall out:

1. **auth.md §3.7's "~1 ms" was right for RSA and 3× pessimistic for P-256.**
   The certificate's key algorithm is the whole variable, and it is chosen when
   the certificate is issued, not in any configuration file this project owns.
2. **Resumption erases the difference** — 297 µs for a resumed RSA handshake
   against 804 µs for a full one — because what resumption skips is the
   signature. A deployment stuck on RSA gets most of P-256's benefit from
   tickets alone.
3. **`ring` beats the no-assembly `aws-lc-rs` by 2.4× on Windows** and by 18% on
   musl. This is the build finding above, priced.
4. **Twelve threads buy about 5×, not 12×** (15,587/s against 3,245/s on
   Windows for full P-256): six physical cores, and public-key arithmetic does
   not share one well.

### Encryption, by record size

What one core turns into TLS records, with no socket in the path. One record
per write, because that is what a reply becomes.

| record | Windows GiB/s | ns/record | musl GiB/s | ns/record |
|---:|---:|---:|---:|---:|
| 64 B | 0.18 | 336 | 0.12 | 504 |
| 256 B | 0.57 | 418 | 0.40 | 593 |
| 1 KiB | 1.17 | 818 | 1.16 | 822 |
| 4 KiB | 1.86 | 2,053 | 1.79 | 2,132 |
| 16 KiB | 2.28 | 6,699 | 2.19 | 6,961 |

**The proposal's arithmetic was sound and its framing was wrong.** It predicted
"a whole core of AES-GCM" at 4 KiB and 1.4 GB/s, and 1.86 GiB/s per core is
exactly that. But the cost that matters at cache value sizes is not per byte:
at 64 bytes a record costs 336 ns whatever is in it, so a 64-byte reply pays
about the same as a 1 KiB one. **Small values are charged per reply, not per
byte** — and small values are what a cache mostly serves.

### End to end

`GET`, 16 connections, pipeline 128, 20,000 keys, 10 seconds. Medians of five,
ranges in brackets, TLS as a percentage of plaintext.

**These are the post-fix numbers.** The first set taken here was measured
against a server with the flush bug below and is retracted; what it was
measuring is recorded after the table, because the shape of the error is worth
keeping.

| workload | platform | plaintext | TLS | |
|---|---|---:|---:|---:|
| 64 B | Windows | 1,578,004 [1.50–1.67M] | 1,409,905 [1.31–1.47M] | **89%** |
| 64 B | musl | 186,720 [171–192k] | 302,655 [261–309k] | **162%** |
| 1 KiB | Windows | 918,703 [885–923k] | 692,193 [642–731k] | **75%** |
| 1 KiB | musl | 236,586 [227–248k] | 161,243 [160–192k] | **68%** |
| 4 KiB | Windows | 274,387 [254–284k] | 259,304 [257–269k] | **95%** |
| 4 KiB | musl | 190,144 [184–211k] | 109,362 [107–113k] | **58%** |
| `SET` 1 KiB | Windows | 42,080 [33.3–45.9k] | 30,143 [27.4–33.5k] | 72% |
| `SET` 1 KiB | musl | 54,464 [52.8–60.6k] | 35,736 [32.3–44.5k] | 66% |

Closed loop, 8 connections, one request in flight:

| platform | plaintext | TLS | p50 |
|---|---:|---:|---|
| Windows | 20,481 ops/s | 19,756 | 0.344 → 0.353 ms |
| musl | 14,569 ops/s | 13,932 | 0.298 → 0.329 ms |

**Latency barely moves.** At one request in flight the round trip is most of
the cost and the cryptography disappears into it — 96% of plaintext throughput
on both platforms, and a p50 that moves by hundredths of a millisecond. Anyone
whose concern is p99 can stop reading here.

**Throughput costs somewhere between nothing and a third, and the spread is
wider than the platforms are apart.** 95% at 4 KiB on Windows against 58% on
musl is not a story about TLS; it is the same [caveat](#what-this-cannot-tell-you)
as everywhere else in this document — one of these is a native process pair on
a fast loopback, the other is a container on a WSL2 VM where syscalls cost more
and both ends compete for the same twelve threads.

**At 64 bytes on musl, TLS is 62% *faster* than plaintext.** It reproduced
across five repeats, before and after the fix, in both measurement rounds. The
likely mechanism is batching: rustls buffers into records, so a pipelined
client's requests arrive in bigger reads and the server measures fewer, larger
blocks — worth more, on that platform, than 336 ns of cryptography per record.
It has not been chased, and it remains the row here least worth trusting.

**What the retracted numbers were measuring.** The first round reported 63% at
1 KiB and could not fill the 4 KiB Windows cell at all, because writes were
being stranded (below). Re-measured with one line fixed, 1 KiB went 63% → 75%
and 4 KiB became 95%. Two smaller cautions come with that comparison: the
plaintext column moved as well, by up to 27% on musl, which no flush can
explain — the first musl round shared its host with hung processes left over
from the Windows round, and `SET` here has a range wide enough (33–46k) that
its ratio should be read as "somewhat worse", not as 72%.

### The stall, and why it was not what it looked like

**Over TLS, a single `write_all` of more than about 256 KiB stopped the
connection dead.** Deterministic, and plaintext at the same sizes never did:

| one batch | plaintext | TLS |
|---|---:|---:|
| 32 × 4 KiB = 128 KiB | 2,911 ops/s | 2,163 ops/s |
| 64 × 4 KiB = 256 KiB | 2,483 ops/s | **hung** |
| 128 × 4 KiB = 512 KiB | 4,724 ops/s | **hung** |
| 256 × 4 KiB = 1 MiB | 4,157 ops/s | **hung** |

This document first reported that as a write-write deadlock: both ends write a
whole batch before reading any of it, and TLS spends the socket-buffer slack
that plaintext survives on. It fit the threshold, it fit the plaintext/TLS
split, and it was wrong.

Instrumenting both loops took twenty minutes and showed something the theory
forbade: **nobody was blocked on a write.** The client had finished writing and
was reading. The server was reading. Fifty-five replies were missing.

**It was a missing flush.** `write_all` on a TLS stream means the session
*accepted* the bytes, not that they reached the socket — if the socket was not
writable for all of them, the remainder stays as ciphertext inside `rustls` and
nothing sends it until something polls the write side again. Both ends then
wait on reads for data that was accepted and never transmitted. Over a
plaintext `TcpStream` `flush` is a no-op, which is why eleven milestones never
needed one: the requirement arrives with the first buffered transport.

Both ends needed it, and each was confirmed by removing it again: without the
server's flush, `GET` hangs (large replies); without the client's, `SET` hangs
(large requests). 256 KiB is simply where a write stops fitting in this
platform's socket buffer, which is where a remainder first gets stranded — a
symptom, not a limit on anything.

`tests/tls.rs` now sends a 1 MiB batch in each direction under a timeout, so
the regression fails a test in seconds rather than wedging a benchmark for an
afternoon.

### What the feature costs to ship

| | without `tls` | with `tls` |
|---|---:|---:|
| `vash-server.exe` | 2,843,648 B | 3,948,032 B (+39%) |
| crate rebuild | 53 s | 63 s |

A megabyte of binary and ten seconds of build, which is the cheapest number in
this section and the argument for the feature flag staying a feature flag.

---

## What the slowlog and the hot-key sampler cost

Phase 0 of [slowlog-proposal.md](slowlog-proposal.md). Its §7 set four gates
before any number existed, so they could not be moved afterwards. Three of them
ran. **One of the three failed and changed the design; the other two turned out
not to be measurable as written**, which is its own finding and the one worth
reading first.

Everything below is `--features introspect` against a build without it, on the
same 12-core Windows 11 box as the rest of this file.

### The end-to-end gates cannot resolve what they ask for

Gates 1 and 2 ask for **≤1% throughput loss** on pipelined `GET` and `SET`. Six
ABBA blocks each — `off, on, on, off`, so a linear drift cancels inside a block —
five-second runs, 16 connections, pipeline 128, 64-byte values:

| | off, median | on, median | paired deltas, per block |
|---|---:|---:|---|
| `GET` | 1,644,958 | 1,595,689 | −2.97, +6.34, −9.44, −2.52, −2.16, −0.72 % |
| `SET` | 174,543 | 174,785 | +0.52, −6.78, −4.32, −0.11, **+14.52**, +1.10 % |

Within a single arm the spread is 27.8% on `GET` and 37.8% on `SET`. That is the
±25% this file has warned about throughout, and it is more than an order of
magnitude coarser than the 1% the gate names.

**The `SET` arm is the control, and it is what settles the matter.** The
instrumentation cannot make writes faster. It nevertheless measured +14.5% in one
block and +0.20% at the median. A harness that reports a 14% speed-up from a
change that cannot produce one is not a harness that can rule on 1% — and a
reader should be equally sceptical of the −2.34% median on the `GET` side, which
is the same noise wearing a plausible sign.

An earlier round made exactly that mistake. Ten alternating runs put `GET` at
−2.4% and the change was treated as real; the raw series then showed both arms
falling monotonically — 1.85M to 1.55M on `off`, 1.81M to 1.31M on `on` — across
the ten runs. The measurement was thermal, not causal.

### So the instrument is priced directly

`cargo bench -p vash-bench --features introspect --bench hot_path`, which is
where this project already prices per-request work, and where the noise floor is
in nanoseconds rather than in hundreds of thousands of ops/s:

| | median | what it is |
|---|---:|---|
| `observed_get` | **3.81 ns** | everything a `GET` pays, at shipping defaults |
| `observed_get_sampling_off` | 2.61 ns | the slowlog's threshold compare alone |
| `sampler`, rate 0 | 2.61 ns | sampling disabled — the floor |
| `sampler`, rate 16 | 5.41 ns | |
| `sampler`, rate 128 | 3.81 ns | the default |
| `sampler`, rate 1024 | 3.71 ns | |

So a request pays **3.81 ns**, of which about 1.2 ns is the hot-key sampler and
the rest is the threshold compare and the call around it.

Re-measured after the later phases, and unchanged: 3.45 ns against a 2.25 ns
floor, where phase 0 read 3.81 against 2.61. The whole board moved by the same
0.36 ns, so the instrument still costs 1.2 ns over a build that does nothing —
which is the number to carry, being the only one that does not move with the
box. It also settles the one thing that looked expensive in the later work:
widening the dialect argument into an `Origin` carrying the connection put a
48-byte value on the hot path and cost nothing measurable.

Put beside the numbers this project already has: `hot_path` prices a RESP `GET`
parse at 90.9 ns, and [performance-proposals.md](performance-proposals.md) §11
dismissed optimising the parsers as "0.8% of one core" at 220,000 requests a
second. The instrument is **4.2% of that parse**, or about 0.03% of one core at
the same rate. That is the honest answer to gates 1 and 2, and it is two orders
of magnitude inside what they were trying to protect.

### Gate 3 failed, and the ring buffer changed

Gate 3 sets `slowlog_threshold_us = 1` so that **every** request records, and
asks that the tail not move. One binary against itself, two servers, closed loop
at 16 connections and 1 KiB values, four ABBA blocks.

The proposal's §3.5 had argued for a single mutex on the grounds that "the lock
is only ever taken by a request that already lost". True at the shipping
threshold. Not true here:

| | one shared ring | a ring per lane |
|---|---:|---:|
| ops/s | −3.8% | +3.6% |
| p99 | **+16.0%** | −12.0% |
| p99.9 | **+38.1%** | −26.0% |
| per-block p99 | +24.0, +3.4, +4.2, +17.9 % | −6.3, −21.4, +8.0, −7.7 % |
| every block the same sign | **yes** | no |

The last row is the whole test, and it is the answer to the noise problem above:
four blocks all moving the same way is a signal that a ±20% harness can still
carry, where a median cannot. After the change the signs are mixed and the
medians move the direction the instrument cannot cause, which is what "within
noise" looks like.

The remedy was the one §7 pre-committed: rings striped per thread and merged on
read. They are indexed by the same lane allocator `metrics.rs` stripes its
counters with, reusing the part that hands a lane back when its thread exits.
`observed_get` was re-priced afterwards at 3.81 ns against 3.92 before, so the
indirection is free.

### Two things the first implementation got wrong

Neither was caught by a benchmark.

**A `RefCell` on the common path cost 45%.** The sampler first held its PRNG and
its summary together in one `RefCell<Option<..>>`, so the borrow flag and the
`Option` check were paid on every request rather than on every sample. Splitting
the PRNG into a bare `Cell` and marking the admit path `#[cold]` took
`observed_get` from **7.06 ns to 3.92 ns**.

The decomposition is the interesting half. With sampling *disabled* — a path that
returns before touching either thread-local — the same split moved 4.66 ns to
2.52 ns. That is 2.1 of the 3.1 ns saved, on a path whose code did not change at
all: it is `observe` no longer being inlined whole into its call sites. Only
about 1 ns was the borrow flag. The `#[cold]` mattered more than the `Cell`.

**Rings hung off thread-locals lose the entries that matter most.** The first
per-thread version held each ring in a thread-local `Arc` with the log keeping a
`Weak`. Writes execute on the blocking pool, and tokio retires an idle blocking
thread after ten seconds — so the slow writes an operator is hunting are exactly
the ones most likely to have vanished before anyone reads the log. A test asserting
that an entry outlives its thread is what found it; the lane array is what fixed
it. The same flaw was in the hot-key summaries and was fixed with it.

A third, found by a test rather than measured: a `sample_rate` of `0` means
*sample nothing*, and rounding it to a mask of `0` makes `rng & mask == 0` true
for every request — the cheapest possible setting silently selecting the most
expensive possible behaviour.

### What phase 0 concludes

- The instrument costs **3.81 ns a request**, which is 4% of one RESP parse.
- **Gates 1 and 2 are not evaluable on this box** and are answered by the
  micro-benchmark instead. The end-to-end runs bound the effect; they do not
  measure it.
- **Gate 3 failed as designed and has been fixed**, and it is the one gate whose
  end-to-end form worked — because a contention regression shows up as four
  blocks agreeing on a sign, where a 1% cost cannot show up at all.
- Gate 4 belongs to phase 3, with the surface it validates.

### Gate 4: what the hot-key estimate is actually worth

Phase 3. §7's fourth gate: *"Drive a Zipfian workload with a known top-10 and
check the report recovers the ranking, and that `estimated_ops` lands within the
error bound §4.2 claims."* It is the gate that stops §4.4's merge heuristic —
per-lane Space-Saving summaries added together, which has no formal guarantee —
from shipping on faith.

Two million `GET`s over 64 keys drawn from a Zipf(1.0) distribution, eight
connections, at the **shipping** sample rate of one in 128. The driver counts
what it sent, so the truth column is exact. Three runs, each against a freshly
started server.

The sample rate is honoured first: 2,000,000 requests produced 15,680, 15,819
and 15,7xx samples against a nominal 15,625 — within 1.2%, which is what makes
scaling by 128 legitimate at all.

One run in full:

| rank | true ops | estimated | off by | published error | samples |
|---:|---:|---:|---:|---:|---:|
| 0 | 421,213 | 423,680 | +0.6% | 1.74% | 3,310 |
| 1 | 210,941 | 203,776 | −3.4% | 2.51% | 1,592 |
| 2 | 140,703 | 140,800 | +0.1% | 3.02% | 1,100 |
| 3 | 105,822 | 115,584 | +9.2% | 3.33% | 903 |
| 4 | 84,368 | 82,176 | −2.6% | 3.95% | 642 |
| 5 | 70,671 | 69,760 | −1.3% | 4.28% | 545 |
| 6 | 59,969 | 61,568 | +2.7% | 4.56% | 481 |
| 7 | 52,486 | 48,512 | −7.6% | 5.14% | 379 |
| 8 | 46,535 | 45,952 | −1.3% | 5.28% | 359 |
| 9 | 42,448 | 44,800 | +5.5% | 5.35% | 350 |

**The published error is not decorative.** Expressed in units of the error each
row claims, the ten misses are 0.34, 1.35, 0.03, 2.76, 0.66, 0.30, 0.59, 1.48,
0.25 and 1.03 — a mean of 0.88 against the 0.80 a correctly calibrated bound
predicts. `1/sqrt(samples)` is describing this estimator, not decorating it.

### And the part a single run would have hidden

The first run recovered the top ten exactly. The second and third did not:

| run | exact to rank | what swapped | their true gap | their published error |
|---|---:|---|---:|---:|
| 1 | 9 | — | — | — |
| 2 | 7 | 8 ↔ 9 | 9.6% | 5.3% each |
| 3 | 6 | 7 ↔ 8 | 11.4% | 4.9% each |

This is not the estimator failing. Two estimates each carrying ±5% differ with
roughly 7% error, so a pair of keys whose true counts are 10% apart is separated
by about 1.4 standard errors — a coin weighted about 4:1, which is exactly the
rate at which these swapped. **The ranking is exact while keys are separated by
more than the combined error of their estimates, and interleaves below that.**

That is the finding worth carrying out of gate 4, and it changes how the surface
should be read rather than what it should do:

- The top of the ranking — where a hot key is *hot*, several times its
  neighbours — is recovered exactly and repeatably. That is the case the
  instrument exists for.
- The tail, where keys differ by a few per cent, is a set rather than an order.
  An operator reading positions 7 through 10 as a ranking is reading noise.
- **`relative_error` is what tells them which of the two they are looking at**,
  which is the argument for publishing it beside every row rather than
  documenting it once. Two rows whose estimates differ by less than the sum of
  their errors are not ordered by this instrument, and the numbers to check that
  are in the response.

Raising `hotkeys_sample_rate` sharpens the tail — the error falls with the
square root of the samples, so a rate of 16 would roughly halve it — at a cost
[priced above](#what-the-slowlog-and-the-hot-key-sampler-cost) at 5.41 ns a
request against 3.81. Nothing about the top of the ranking needs it.

The CI test is the same check made deterministic: `introspect::hotkeys::tests`
drives a seeded Zipf through the estimator, asserts Space-Saving's formal
guarantee directly — a held counter never undercounts, and never overcounts by
more than the total it inherited — and pins the recovered top ten. §10 asked for
a seeded test rather than only a benchmark, and the reason is this section: the
benchmark's answer moves between runs.

## What to take from all of this

**Reads are where vash is competitive, and where the host mattered most.** On the
laptop, closed loop with `resident_mode`, `GET` read 165,672 against Redis's
53,310 and memcached's 140,011; pipelined, 718,449 against 482,181 and 707,270 —
ahead of both in both shapes. On a dedicated server only the closed-loop half
survives, and narrowed: 284,318 against 149,368 and 284,823, a clear lead over
Redis and a tie with memcached. Pipelined, vash trails both. **The pipelined lead
was the laptop, not the read path** — see
[The third measurement](#the-third-measurement-a-dedicated-server-and-a-read-lead-that-did-not-survive).

**Writes are still three to five times behind pipelined** — `SET` 170,914 against
630,251 and 919,021 — and behind Redis closed loop, 44,930 against 60,810. What
is left there is per-request work, and vash does more of it: a copy-on-write
B-tree committing to a disk against a hash insert into memory. [plan.md](plan.md)
§6 chose that deliberately and what it buys is a cache that survives a restart.

**This is the claim the server round confirmed rather than moved**: 3.4× behind
Redis and 5.4× behind memcached on pipelined `SET`, measured with the data
directory on a local NVMe device — which removes the excuse the laptop rounds
always had to hand, that vash was committing to a virtualised disk while the
other two touched nothing. The gap is the design, not the host.

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

So the honest summary, as it stands after a server has had a say: **where the
client waits for its answer, vash competes — ahead of Redis on reads, level with
memcached. Where the client pipelines, vash is behind both, on reads as well as
writes.** Reads got where they are by removing hand-offs rather than by changing
the engine; writes are where committing a B-tree to a disk is still paid for. And
it is the only one of the three that still has your data after a restart.

The remaining hand-off is now measurable rather than inferred. vash's three
closed-loop rows on a server land within 3% of each other at ~100k ops/s across
`SET`, `GET` and a 9:1 mix — three workloads with quite different per-request
work, all stopped by the same thing. `resident_mode` takes reads through it to
284,318 and leaves writes at 97,729, which names it: the dispatch to the storage
threads, which reads can skip and writes cannot.

Two things this exercise produced beyond the numbers. One is
[`resident_mode`](#resident_mode-reversed-on-pipelined-reads-and-why), 10× closed
loop and 3.5× pipelined — and the round in which it appeared to lose pipelined
turned out to be a bug in a commit about writes, three commits away, found by
bisecting builds against each other rather than by reading the code. The other is
the [collapse that did not reproduce](#a-collapse-that-did-not-reproduce): a bug
reported here on one 20-second sample, hunted, and never found.

The settings are worth more than the table. The retraction is worth more than the
settings.
