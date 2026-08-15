# vash beside Redis and memcached

Every number in [README](../README.md#performance) was measured against vash
alone, with vash's own load generator over VCP. This document does the other
thing: **the same client, the same box, the same workload, three servers**, so
the comparison is between the servers rather than between benchmark harnesses.

Read the [What this cannot tell you](#what-this-cannot-tell-you) section before
quoting anything here. The short version: one laptop, one container host, 20
seconds per run, the whole matrix run twice. These are orders of magnitude, not
measurements — and the two rounds disagree by enough on the write rows to prove
it.

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

**What was measured**, on 2026-08-14: vash at `7da2e5f`, built by the repository
`Dockerfile` into a 4.7 MB `scratch` image; `redis:8-alpine` (Redis 8.10.0);
`memcached:1.6-alpine` (memcached 1.6.45); `memtier_benchmark` 2.5.1. Host: a
Windows laptop running Rancher Desktop, 12 CPUs and 10 GB visible to the WSL2
VM, with a k3s cluster idling on the same machine.

**The matrix was measured twice**, a few hours apart on that machine, against
the same image — `7da2e5f` is `7c71098` plus a documentation commit, so the two
rounds ran identical code. The tables below are the **second** round. The first
is not discarded quietly: where the rounds disagree it is said so in place, and
the largest disagreement — a `SET`-only collapse on the memcached dialect that
did not come back — has [its own section](#a-collapse-that-did-not-reproduce).

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
  vash:bench --listen 0.0.0.0:11311 --data /var/lib/vash --ephemeral
docker run -d --name bench-server --network vashbench --cpus 4 \
  redis:8-alpine redis-server --save '' --appendonly no
docker run -d --name bench-server --network vashbench --cpus 4 \
  memcached:1.6-alpine -m 1024 -t 4
```

`--ephemeral` still needs `--data`: it changes the durability of the store
rather than removing it, and without a writable path the server exits with
`opening store at data: Permission denied`. The client port differs per target —
**11311** for vash, **6379** for Redis, **11211** for memcached.

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
  discarded as an outlier. The second round is what the tables report, and it is
  the reason to distrust them: between rounds vash's closed-loop `SET` figure
  moved by 38% and its memcached-dialect `SET` figure by a factor of seventeen.
  Everything here is an order of magnitude; the write rows are barely that.
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
| SET only | vash-resp | 5,774 | 17.31 | 7.20 | 481.28 | 987.14 |
| SET only | vash-memcache | 5,205 | 19.21 | 9.73 | 28.42 | 2080.77 |
| SET only | vash-ephemeral | 11,120 | 8.99 | 5.82 | 48.64 | 53.76 |
| SET only | redis | 55,401 | 1.80 | 1.65 | 3.90 | 5.76 |
| SET only | memcached | 148,314 | 0.67 | 0.63 | 1.95 | 4.77 |
| GET only | vash-resp | 17,443 | 5.73 | 5.66 | 11.71 | 14.27 |
| GET only | vash-memcache | 16,885 | 5.92 | 5.86 | 12.10 | 14.72 |
| GET only | vash-ephemeral | 17,448 | 5.73 | 5.66 | 11.71 | 14.46 |
| GET only | redis | 50,484 | 1.98 | 1.80 | 4.64 | 6.82 |
| GET only | memcached | 130,922 | 0.76 | 0.73 | 1.94 | 3.98 |
| 1:9 mixed | vash-resp | 14,881 | 6.71 | 6.27 | 19.07 | 25.73 |
| 1:9 mixed | vash-memcache | 14,103 | 7.08 | 6.50 | 19.33 | 44.54 |
| 1:9 mixed | vash-ephemeral | 16,210 | 6.16 | 6.05 | 12.99 | 17.28 |
| 1:9 mixed | redis | 55,398 | 1.80 | 1.64 | 4.06 | 6.46 |
| 1:9 mixed | memcached | 135,070 | 0.74 | 0.70 | 1.90 | 4.13 |

The `SET`-only rows for the two vash dialects are the ones that moved between
rounds, in both directions: `vash-resp` was 9,264 in the first round and 5,774
here, and `vash-memcache` was 308 and is 5,205. See
[below](#a-collapse-that-did-not-reproduce) — vash's closed-loop write rate is
the least repeatable number in this document.

### Pipelined — 16 requests in flight per connection, 100 connections

The throughput question. Latency under pipelining is queueing rather than
service time, and is reported only because a p99 in the hundreds of
milliseconds says the server is saturated.

| Workload | Target | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |
|---|---|---:|---:|---:|---:|---:|
| SET only | vash-resp | 14,140 | 112.98 | 89.60 | 172.03 | 3817.47 |
| SET only | vash-memcache | 13,272 | 122.04 | 105.47 | 294.91 | 2195.46 |
| SET only | vash-ephemeral | 47,656 | 33.54 | 18.18 | 84.48 | 96.26 |
| SET only | redis | 554,785 | 2.88 | 2.72 | 6.62 | 8.70 |
| SET only | memcached | 912,234 | 1.75 | 1.42 | 7.65 | 15.36 |
| GET only | vash-resp | 221,380 | 7.22 | 6.69 | 23.55 | 30.21 |
| GET only | vash-memcache | 212,125 | 7.53 | 6.85 | 25.34 | 32.51 |
| GET only | vash-ephemeral | 218,200 | 7.33 | 6.59 | 25.98 | 32.00 |
| GET only | redis | 475,430 | 3.36 | 3.20 | 7.74 | 12.03 |
| GET only | memcached | 651,154 | 2.45 | 2.30 | 5.76 | 11.46 |
| 1:9 mixed | vash-resp | 86,583 | 18.64 | 15.10 | 47.36 | 352.26 |
| 1:9 mixed | vash-memcache | 83,653 | 19.10 | 18.43 | 45.06 | 52.48 |
| 1:9 mixed | vash-ephemeral | 125,401 | 12.75 | 6.82 | 63.74 | 71.17 |
| 1:9 mixed | redis | 452,420 | 3.53 | 3.28 | 9.47 | 14.66 |
| 1:9 mixed | memcached | 675,864 | 2.36 | 2.24 | 5.63 | 11.90 |

### The populate pass, which turned out to be its own result

One connection, 128 requests in flight, 20,000 sequential `SET`s — the load
phase before each run, kept here because it isolates the write path with **no
concurrency to batch**:

| Target | ops/s |
|---|---:|
| vash-resp | 505 |
| vash-memcache | 473 |
| vash-ephemeral | 9,825 |
| redis | 391,129 |
| memcached | 337,587 |

This is the most repeatable number vash has: both rounds put the two dialects
within 40% of each other and both within 25% of 400 ops/s. It is the commit
latency of the container's disk and nothing else, which is why **the two vash
dialects agree here even in the round where they disagreed everywhere else.**

---

## Reading the numbers

### Reads: the same order of magnitude, two to three times behind

Pipelined `GET` is 212k–221k on vash against 475k for Redis and 651k for
memcached. That is the honest shape of it: vash is in the same class, a factor
of two to three back, and the factor is not the protocol — its two dialects are
4% apart and sit at the same distance from the other two servers.

Closed loop is where the distance shows: **5.7–5.9 ms per round trip at 100
connections against Redis's 1.8 ms and memcached's 0.73 ms.** Throughput follows
directly (100 connections ÷ 5.8 ms ≈ 17k), so the read *rate* in that table is a
restatement of the read *latency*, not an independent finding. Something in
vash's read path costs several milliseconds per request under this concurrency
that neither of the other two pays.

Both rounds agree on this to within 6% on every read row, which is what makes it
worth reasoning about at all.

The suspect is the hand-off: vash runs reads on the storage thread pool rather
than on the network worker, because a read that page-faults must not block a
worker serving other connections. `store.inline_reads` turns that off, and its
documentation says it measured within noise on the development machine — a
machine that was not a four-core cgroup. Measuring it here is
[below](#does-inline_reads-explain-the-read-latency).

### Writes: this is the design, and the design has a price

Pipelined `SET`: **14,140 for vash against 555k for Redis and 912k for
memcached** — 39× and 65×. The ordering is not close and it is not a surprise:
vash writes through a copy-on-write B-tree that commits to a disk, and the other
two write to memory. [plan.md](plan.md) §6 chose that, and the README already
records the original 250k/s write goal as *wrong rather than missed*. This is
what the choice costs on the wire, in the environment described above.

Two things soften and sharpen it:

- **Concurrency is what makes vash's writes work at all.** 505 ops/s on one
  connection, 5,774 closed loop across a hundred and 14,140 pipelined — 11× and
  28× from group commit, which forms a batch out of whatever queued during the
  previous commit. It cannot help a single-threaded writer, and there is nothing
  to be done about that.
- **The disk is most of it.** `--ephemeral` — same code, nothing synced — is
  47,656 against 14,140 pipelined, a 3.4× penalty for durability under load, and
  9,825 against 505 with a single connection, which is 19×. What that says is
  that the sync cost is amortised by batching *and* by concurrency, and that a
  workload with neither pays it in full.

Even at 48k, ephemeral vash is 12× behind Redis on writes. The gap is not all
persistence.

**Treat the vash write figures as an order of magnitude and nothing finer.**
Between the two rounds, pipelined `SET` over RESP moved from 9,691 to 14,140 and
closed-loop `SET` from 9,264 to 5,774 — 46% up and 38% down, same image, same
box, hours apart. Nothing above this paragraph is precise enough for those
swings to matter, and nothing below should be read as though it were.

### The mixed workload is the realistic one

At 1:9, pipelined: vash 84k–87k, or 125k ephemeral, Redis 452k, memcached 676k.
A cache doing 90% reads sits between the read and write results and inherits
more from the writes than the ratio suggests, because in vash a write is ~16×
the cost of a read rather than ~1×.

### The two dialects are not the interesting variable

In this round they are within 5% of each other on every row, with RESP
marginally ahead: 221k against 212k pipelined `GET`, 87k against 84k mixed,
14,140 against 13,272 pipelined `SET`.

The first round had the memcached dialect *ahead* by 14% on pipelined `GET` and
31% on mixed, and this document reasoned about why. The gap did not survive
re-measurement — and the parser prices say it should not have been believed the
first time. `cargo bench -p vash-bench --bench hot_path` puts the two decoders
at **53.5 ns** for a memcached `get` and **90.9 ns** for a RESP one; the 37 ns
between them, at 220,000 requests a second, is 0.8% of one core. A dialect gap
an order of magnitude larger than the dialect's own cost was always more likely
to be the machine than the protocol.

The conclusion outlived the numbers that suggested it: **protocol choice is not
a performance decision here.** It now rests on the two dialects agreeing rather
than on explaining away their disagreement.

And then there is the row this document exists to be honest about.

---

## A collapse that did not reproduce

The first round of this matrix recorded **`SET`-only over the memcached dialect
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
[writes section](#writes-this-is-the-design-and-the-design-has-a-price) already
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
| GET only, closed loop | off (default) | 18,023 | 5.47 | 11.39 |
| GET only, closed loop | **on** | **180,027** | **0.52** | 1.69 |
| GET only, pipelined | off (default) | 222,892 | 6.62 | 23.55 |
| GET only, pipelined | **on** | **753,498** | 1.94 | 5.15 |

**10× closed loop and 3.4× pipelined**, and it moves vash's pipelined read
throughput from *behind* both other servers to ahead of both — 753k against
memcached's 651k and Redis's 475k, on the same four cores.

Because a result that large is more likely to be a mistake than a discovery, the
closed-loop pair was run twice more, alternating between the two images so that
neither got the warmer machine:

| Round | `inline_reads` off | `inline_reads` on |
|---|---:|---:|
| 1 | 17,022 ops/s | 175,102 ops/s |
| 2 | 16,916 ops/s | 174,479 ops/s |

Four measurements, two images, one setting between them.

**This is the one large effect in this document that reproduced.** Measured
again hours later it gave 10× and 3.4× where it first gave 9.5× and 4.5×, with
the "on" closed-loop figure landing within 5% across six separate runs. Set
against the `SET`-only rows that moved by a factor of two and the collapse that
vanished, that stability is most of the reason to believe it.

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

**Reads are competitive, and with `inline_reads` they lead** — 753k pipelined
`GET` against memcached's 651k and Redis's 475k on the same four cores. On the
default, they are a factor of two to three behind.

**Writes are not competitive, and that is a design decision rather than a
defect.** 14,140 pipelined `SET` against 555k and 912k. A copy-on-write B-tree
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

Two things this exercise produced beyond the numbers, and they are not the two it
first claimed. One is a [read-path
setting](#does-inline_reads-explain-the-read-latency) worth 3–10× in a container
and off by default, which reproduced across six runs and two rounds. The other is
the [collapse that did not](#a-collapse-that-did-not-reproduce): a bug reported
here on one 20-second sample, hunted, and then not found — because on this
hardware vash's write throughput moves by a factor of two between runs, and a
single sample of it was never enough to call anything.

The setting is worth more than the table. The retraction is worth more than the
setting.

What to *do* about the gap is a separate question from measuring it, and it has
its own document: [performance-proposals.md](performance-proposals.md) decomposes
the write cost from the writer's own counters — a commit costs 2.37 ms before it
writes anything and 0.23 ms per record, which caps vash near 17,400 writes/s no
matter how well it batches — and works through what each candidate change is
worth against that ceiling.
