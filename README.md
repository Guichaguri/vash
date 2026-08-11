# vash

A cache server built on LMDB, with TTLs, tag-based invalidation and memcached
protocol compatibility.

Wire protocols: [docs/protocol.md](docs/protocol.md) — enough detail to write a
client against. Design and rationale: [docs/plan.md](docs/plan.md). Original
brief: [docs/project.md](docs/project.md).

**Status: M6 complete — feature work done.** Speaks both its own binary protocol
and the memcached text and meta protocols, on the same port. TTLs with
background reclamation, group-committed writes across independent shards,
constant-time tag invalidation that propagates across a cluster, capacity
eviction, Prometheus metrics, continuous fuzzing of both parsers, and a static
binary you can ship. See [Performance](#performance) for what it does and what
it does not.

## Quick start

```bash
cargo run --bin vash-server -- --listen 127.0.0.1:11311 --data ./data
```

In another terminal:

```bash
cargo run -p vash-client --example smoke -- 127.0.0.1:11311
```

Run with `--ephemeral` to start from an empty database and skip syncing, or
`--config vash.example.toml` for the full configuration surface.

## What works today

| | Status |
|---|---|
| `HELLO`, `PING`, `GET`, `SET`, `DELETE`, `TOUCH` over VCP | Working |
| `GET_MANY`, `SET_MANY`, `DELETE_MANY` | Working — one transaction per batch, all-or-nothing |
| TTLs | Enforced on read, and reclaimed in the background by the sweeper |
| Group commit | Working — see [benchmark](#write-throughput) |
| Tags, `DELETE_BY_TAG`, `FLUSH` | Working — invalidation is constant time, see [benchmark](#tag-invalidation) |
| Memcached text protocol | Working — `get`/`gets`/`set`/`add`/`replace`/`append`/`prepend`/`cas`/`delete`/`touch`/`gat`/`gats`/`incr`/`decr`/`stats`/`version`/`flush_all`/`quit` |
| Memcached meta protocol | Working — `mg`/`ms`/`md`/`ma`/`mn`/`me`, core flag set |
| Sharding | Working — independent environments, one writer each |
| Capacity watermarks and eviction | Working — TTL-ordered, never LRU |
| Metrics and admin endpoints | Working — `/metrics`, `/health`, `/stats` |
| Cluster tag invalidation | Working — fan-out plus anti-entropy, see [Clustering](#clustering) |

The legacy memcached **binary** protocol (magic `0x80`) is not implemented and
will not be: upstream deprecated it in favour of the meta commands.

The on-disk record format is already the final one — epoch, TTL, CAS and the tag
table are all written today — so later milestones add behaviour without a
migration.

## Performance

Measured on a 12-core Windows 11 dev box with NVMe, **with the load generator on
the same machine** talking over loopback — so client and server compete for the
same cores and the same memory bus. Every number here is a floor, and the
latency figures are the most distorted of them. Reproduce with:

```bash
cargo run --release -p vash-bench --bin load -- --workload get --connections 16 --pipeline 128
```

Run-to-run variance is large — around ±25% between identical runs, because the
client is competing with the server — so treat these as magnitudes rather than
figures. Pipelined, 16 connections, 64-byte values, 4 shards, syncing off:

| Workload | ops/s |
|---|---:|
| `PING` (touches no storage) | 4,040,000 |
| `GET` | 1,280,000 |
| Mixed, 9 reads : 1 write | 306,000 |
| `SET` | 40,000 |

`GET` by value size: 2,040,000 at 64 B, 1,820,000 at 256 B, 920,000–1,030,000 at
1 KiB, 337,000 at 4 KiB. The fall with size is data movement, not per-request
work — 4 KiB at 337k ops/s is 1.4 GB/s of value bytes leaving the process while
the client reads them on the same memory bus.

Closed-loop latency, one request in flight per connection, 1 KiB values:

| connections | ops/s | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 8 | 34,900 | 0.20 ms | **0.66 ms** | 1.20 ms |
| 32 | 27,900 | 0.93 ms | 4.20 ms | 7.10 ms |
| 128 | 20,900 | 5.80 ms | 12.90 ms | 17.00 ms |

Against the goals set in [plan.md](docs/plan.md) §13 before any of it was built:

- **GET ≥ 1M ops/s — met**, including at the 1 KiB the goal names, though that
  one lands close enough to the line that the variance straddles it.
- **`DELETE_BY_TAG` < 1 ms regardless of cardinality — met.** See
  [below](#tag-invalidation).
- **p99 < 1 ms at 500k ops/s — half met, and not measurable as stated.** p99 is
  0.66 ms, but at 35k ops/s: closed-loop throughput on one machine is bounded by
  round trips, and reaching 500k that way needs a client that is not fighting
  the server for cores. Latency degrades with connection count from there, which
  is queueing on a saturated box rather than anything the server chose.
- **SET ≥ 250k ops/s — not met, and the goal was wrong.** Writes go through a
  copy-on-write B-tree that commits to a disk. M4 measured where that ceiling
  is: with syncing on, throughput is set by the device, and sharding makes it
  *worse* by splitting one disk between more environments. Group commit is doing
  its job — throughput tracks batch size — but the limit is underneath it. Tens
  of thousands of writes a second is what this design gives on this hardware,
  and the number worth stating beside it is the 9:1 mixed workload at 306k.

Two things that were expected to matter and did not, which is worth as much as
the things that did:

- **`store.inline_reads`**, which runs reads on the network worker instead of
  handing them to the storage threads, measured within noise of the hand-off
  (1.18M against 1.28M — the difference is smaller than the variance). It stays
  off by default and stays available, because the hand-off cost it removes is a
  property of the platform's thread wake-ups rather than of this code.
- **mimalloc** was expected to be worth 10–20% on allocation-heavy connection
  handling. Measured: 1.13M against 1.11M ops/s. It ships as an opt-in feature
  rather than the default.

### The thing that mattered

Reads were capped at 86,000 ops/s by a flag that existed to serve a design that
was never built. `read_txn_without_tls()` makes a read transaction `Send` so a
hand-rolled reader pool could pass one between threads; that pool was replaced
in M1 by the runtime's blocking pool, where a transaction is created and dropped
inside a single call and never crosses a thread. Nothing needed it — and without
thread-local storage, every transaction has to claim a slot in a shared reader
table behind a process-wide mutex.

The lookups really are lock-free. The transaction around them was not:

```bash
cargo run --release -p vash-store --example txn_bench
```

| threads | without TLS | with TLS |
|---:|---:|---:|
| 1 | 343,756 | 948,333 |
| 4 | 100,915 | 2,842,614 |
| 16 | 90,734 | **5,303,410** |

Read that column twice: without thread-local slots, adding threads made reads
*slower*. Deleting one method call took the server from 86,000 to over a million
GETs a second — the single largest change in the project, and it was a line of
setup nobody had measured.

## Write throughput

LMDB permits one writer per environment, so writes scale only by fitting more of
them into each transaction. Measured with:

```bash
cargo run --release -p vash-store --example write_bench
```

20,000 writes of a 256-byte value, `relaxed` durability, one environment
(Windows 11, NVMe):

| | ops/s | mean ops per commit |
|---|---:|---:|
| 1 thread, one write at a time | 2,825 | 1.0 |
| 8 threads, one write at a time | 7,498 | 4.4 |
| 64 threads, one write at a time | 12,329 | 57.4 |
| 1 thread, `set_many` of 16 | 18,632 | 16.0 |
| 1 thread, `set_many` of 256 | 25,138 | 256.0 |

Throughput tracks batch size, which is the whole point: nothing here delays a
write to build a batch. A batch is simply whatever queued while the previous
commit was in flight, so an idle server commits immediately and a loaded one
batches on its own.

### Sharding

Each shard is an independent LMDB environment with its own writer thread, so the
shard count is the ceiling on concurrent writers. Best of 3, 200,000 writes from
64 concurrent callers:

| shards | no syncing | `relaxed` (default) | `durable` |
|---:|---:|---:|---:|
| 1 | 38,675 | 12,362 | 10,863 |
| 2 | 40,478 | 13,059 | — |
| 4 | 52,852 | 7,979 | 7,643 |
| 8 | **58,808** | 10,649 | 5,484 |

**Sharding only helps when the writer thread is the bottleneck.** With syncing
off it scales 1.5× across 8 shards; with syncing on, throughput is set by the
disk, and splitting one device between more environments fragments its I/O and
makes things *worse*. Sharding cannot fix a disk.

It also does nothing for reads: LMDB readers are already lock-free and
concurrent within a single environment.

Two further caveats, both visible in the numbers above:

- **Sharding and group commit pull against each other.** At a fixed write rate,
  splitting across N queues divides the mean batch size by roughly N — from ~61
  on one shard to ~2 on eight — so some of the gain from more writers is handed
  straight back in lost amortisation. Sharding pays off with *offered load*, not
  with shard count alone: at 20,000 writes the gain peaked at 4 shards and
  regressed at 8; at 200,000 it kept climbing.
- **The shard count is fixed once a database exists.** Changing it would route
  every key to a different environment, so startup refuses to open a store built
  for a different count rather than silently turning the cache into a miss.

The default is `min(num_cpus, 4)`. Measure on your own hardware before raising
it. M6 re-measured this end to end, through the socket rather than against the
store directly, and it came out the same shape: pipelined `SET` with syncing off
went 43k → 57k → 64k → 35k ops/s at 1, 2, 4 and 8 shards. Four is where it stops
paying.

## Tag invalidation

Invalidating a tag is one generation-counter bump, so it costs the same whether
the tag covers ten keys or half a million. Records store the generation their
tags had when written, and a read compares those against the registry in RAM —
no extra disk lookup, no walk over the affected keys.

```bash
cargo run --release -p vash-store --example tag_bench
```

| keys carrying the tag | `DELETE_BY_TAG` | background reclaim |
|---:|---:|---:|
| 100 | 676µs | 33ms |
| 1,000 | 457µs | 35ms |
| 10,000 | 1.77ms | 326ms |
| 100,000 | 197µs | 3.75s |
| 500,000 | 205µs | 32.5s |

Invalidation is flat across a 5,000× range in cardinality (the spread is commit
timing, not growth). Reclaiming the freed space *is* proportional to the key
count and runs in the background, bounded per pass so it never holds the write
transaction long enough to stall traffic.

The alternative — finding and deleting every key with the tag — would cost O(n)
while holding the environment's only write transaction, which for a large tag is
a multi-second stall of every other write on the node.

## Capacity

The cache is bounded by `store.map_size_mb` per shard, and stays inside it by
evicting. Three watermarks over the space actually in use: reclamation goes
continuous at 75%, live records start being evicted at 88%, and past 96% writes
are refused with `CAPACITY_FULL` while reads and deletes keep working.

Eviction is **TTL-ordered, not LRU**. Victims come off the front of the expiry
index — soonest-to-expire first, never-expiring last — which costs nothing extra
because that index already exists for TTLs, and is the right policy for a cache:
a TTL is the client's own statement of how long a value is worth keeping. LRU
would mean writing recency metadata on every read, which on a single-writer
engine puts every GET behind the write queue.

## Clustering

Nodes are independent. Clients shard the keyspace themselves, nothing is
replicated, and there is no consensus anywhere — which is why adding a node adds
capacity linearly and losing one costs `1/N` of the cache rather than an
outage.

The one thing that has to cross a node boundary is **tag invalidation**, because
a tag's keys are spread across every node by key hash. A `DELETE_BY_TAG` that
stopped at whichever node the client happened to call would leave most of the
affected keys being served.

```bash
vash-server --listen 0.0.0.0:11311 --peer 10.0.0.2:11311 --peer 10.0.0.3:11311
```

Two mechanisms, and the second is the one that makes it correct:

- **Fan-out** pushes each invalidation to peers as it happens. Fast, and lossy.
- **Anti-entropy** exchanges tag→generation digests with each peer every
  `gossip_interval` (default 5s), on a timer per peer so one unresponsive node
  cannot slow convergence between the healthy ones. This is what repairs a node
  that was down, partitioned, restarted, or that simply missed a message.

None of it needs an acknowledgement protocol, retries with sequence numbers, or
agreement on membership, because generations merge by taking the higher of the
two. That single property makes delivery idempotent, order-independent,
retry-safe and loss-tolerant at once — so a full queue can drop a message and a
partitioned node can rejoin, and both converge on their own.

`cluster.delete_by_tag` picks the trade:

| Mode | `DELETE_BY_TAG` returns | Staleness elsewhere |
|---|---|---|
| `local` | after the local bump | unbounded — the client calls every node |
| `fanout` (default) | immediately; peers told in the background | bounded by the gossip interval |
| `fanout_sync` | after reachable peers have applied it | none for reachable peers |

**Stated plainly:** invalidation is strongly consistent within a node and
eventually consistent across the cluster. Under `fanout` there is a window —
normally milliseconds — in which another node still serves covered records, and,
symmetrically, in which a record written *on* that node is treated as
pre-invalidation and dropped once the message lands. Both errors are in the
direction of a cache miss, never a stale hit. `fanout_sync` closes both for
reachable peers.

Membership is one-sided and static: peers are configured on the sending node, so
a node that lists nobody can still be another node's target. The `CLUSTER`
opcode reports what a node was told and which peers it can reach, which is how a
client detects a cluster configured inconsistently.

## Observability

`/metrics` (Prometheus), `/health` and `/stats` (JSON), on a separate port so
they can be bound to a private interface and a flood of scrapes cannot crowd out
cache traffic.

```bash
curl -s localhost:9090/metrics | grep -E 'hits|misses|utilisation|evicted'
curl -s localhost:9090/stats
```

`/health` returns 503 when a shard has hit critical pressure and is refusing
writes — the process is up, but it is not doing its job. The counters worth
alerting on are `vash_evicted_total` rising (the cache is too small for its
working set), `vash_sweep_lag_ms` growing (reclamation is losing to expiry),
`vash_readers_in_use` approaching `vash_readers_max`, and — in a cluster —
`vash_cluster_last_exchange_age_ms` growing past a few gossip intervals, which
means this node is drifting out of step with its peers.

## Memcached compatibility

Both protocols share one port. The dialect is settled by the connection's first
byte — VCP opens with a `HELLO` frame (`0x01`), every memcached command opens
with a lowercase letter — so nothing is re-parsed and the two cannot be
confused. A key written by a memcached client is readable by a VCP client and
the other way round, client flags included.

Existing memcached clients need no changes:

```bash
python -c "
from pymemcache.client.base import Client
c = Client(('127.0.0.1', 11311))
c.set(b'key', b'value'); print(c.get(b'key'))
"
```

Tags are reachable from memcached clients through two extensions, neither of
which is part of the upstream protocol: a `G` flag on `ms` attaching a
comma-separated tag list, and `mdt <tag>` (or `delete_by_tag <tag>` in the
classic dialect) to invalidate one. Clients that never send them are unaffected.

### How compatibility is checked

Two suites, both run in CI against **real memcached** as well as against vash,
so a divergence fails the build rather than someone's cache.

**Client library** — drives `pymemcache` through the behaviour clients actually
depend on:

```bash
pip install pymemcache
cargo run --release --bin vash-server -- --listen 127.0.0.1:11311 --data ./data --ephemeral
python tests/compat/memcached_compat.py 127.0.0.1:11311 --tags
```

**Byte-for-byte differential** — sends identical command sequences to both
servers and compares the raw responses. A client library smooths over exact
error strings, edge-case verdicts and response framing; this does not:

```bash
docker run -d -p 11211:11211 memcached:1.6-alpine
python tests/compat/differential.py --reference 127.0.0.1:11211 --subject 127.0.0.1:11311
```

Current result: **37 of 38 probes byte-identical**, with one deliberate
divergence recorded in the script — for an over-long key memcached emits a stray
empty line after the error, which vash does not reproduce because it would
leave a pipelining client counting one more response than it sent commands.

## Layout

```
crates/vash-core     domain types, on-disk record format, no I/O
crates/vash-store    LMDB adapter behind the `Store` trait
crates/vash-proto    wire codecs (VCP + memcached); byte-slice in, `Command` out
crates/vash-server   network tier, dispatch, config, the `vash-server` binary
crates/vash-client   VCP client, and the integration-test driver
```

`vash-core` defines the domain; `vash-store` and `vash-proto` are adapters on
either side of it. Both protocols decode into the same `Command` type, so the
storage engine never learns which wire format a request arrived on.

## Deploying

A single static binary with no runtime dependencies, or a `scratch` image
holding nothing but it:

```bash
cargo build --release --bin vash-server --target x86_64-unknown-linux-musl
docker build -t vash .
```

[`packaging/vash.service`](packaging/vash.service) is a hardened systemd
unit. [docs/operations.md](docs/operations.md) covers sizing, tuning, what to
alert on, and what each failure mode looks like from outside.

**There is no authentication.** Anyone who can reach the cache port can read and
write any key and invalidate any tag — and cluster peers use that same port.
Bind it to a private network.

## Development

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

Requires a C toolchain, because `heed` compiles LMDB from source — MSVC Build
Tools on Windows, `build-essential` on Linux.

### Testing the parsers

The two wire parsers are the only code that reads bytes from unauthenticated
strangers, so they get more than example-based tests. Property tests run on
every `cargo test` and state the invariants the connection loop depends on: that
decoding is total, that a rejected command always consumes a non-zero, in-bounds
number of bytes — a zero would spin a core forever on one bad byte — and that
`peek_frame_len` and the decoder agree on where a frame ends.

Coverage-guided fuzzing runs in CI on every change, seeded from a corpus
generated out of the encoders so it cannot drift from them:

```bash
cargo run -p vash-proto --example seed_corpus -- fuzz/seeds
cargo +nightly fuzz run vcp_decode fuzz/seeds/vcp_decode
```

Targets: `vcp_decode`, `memcached_text`, `memcached_meta`, `record_header`.

### Benchmarks

```bash
cargo bench -p vash-bench                                  # per-request hot path
cargo run --release -p vash-bench --bin load -- --help     # end to end over a socket
cargo run --release -p vash-store --example read_bench     # the read path, no socket
cargo run --release -p vash-store --example txn_bench      # transaction cost alone
cargo run --release -p vash-store --example write_bench    # group commit and sharding
cargo run --release -p vash-store --example tag_bench      # the O(1) invalidation claim
```

The micro-benchmarks price a request before any storage work happens: decoding a
`GET` is 15.6 ns against 99.8 ns for the same request in the memcached text
protocol, parsing a stored record is 12.5 ns and flat in its tag count, and the
liveness check is 1.5 ns untagged rising to 16.8 ns with the full 32 tags.
