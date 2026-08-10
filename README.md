# kached

A cache server built on LMDB, with TTLs, tag-based invalidation and memcached
protocol compatibility.

Wire protocols: [docs/protocol.md](docs/protocol.md) — enough detail to write a
client against. Design and rationale: [docs/plan.md](docs/plan.md). Original
brief: [docs/project.md](docs/project.md).

**Status: M4 complete.** Speaks both its own binary protocol and the memcached
text and meta protocols, on the same port. TTLs with background reclamation,
group-committed writes across independent shards, constant-time tag
invalidation, capacity eviction, and Prometheus metrics. Not yet
production-usable — see [What works today](#what-works-today).

## Quick start

```bash
cargo run --bin kached -- --listen 127.0.0.1:11311 --data ./data
```

In another terminal:

```bash
cargo run -p cache-client --example smoke -- 127.0.0.1:11311
```

Run with `--ephemeral` to start from an empty database and skip syncing, or
`--config kached.example.toml` for the full configuration surface.

## What works today

| | Status |
|---|---|
| `HELLO`, `PING`, `GET`, `SET`, `DELETE`, `TOUCH` over KCP | Working |
| `GET_MANY`, `SET_MANY`, `DELETE_MANY` | Working — one transaction per batch, all-or-nothing |
| TTLs | Enforced on read, and reclaimed in the background by the sweeper |
| Group commit | Working — see [benchmark](#write-throughput) |
| Tags, `DELETE_BY_TAG`, `FLUSH` | Working — invalidation is constant time, see [benchmark](#tag-invalidation) |
| Memcached text protocol | Working — `get`/`gets`/`set`/`add`/`replace`/`append`/`prepend`/`cas`/`delete`/`touch`/`gat`/`gats`/`incr`/`decr`/`stats`/`version`/`flush_all`/`quit` |
| Memcached meta protocol | Working — `mg`/`ms`/`md`/`ma`/`mn`/`me`, core flag set |
| Sharding | Working — independent environments, one writer each |
| Capacity watermarks and eviction | Working — TTL-ordered, never LRU |
| Metrics and admin endpoints | Working — `/metrics`, `/health`, `/stats` |
| Cluster tag invalidation | M5 |

The legacy memcached **binary** protocol (magic `0x80`) is not implemented and
will not be: upstream deprecated it in favour of the meta commands.

The on-disk record format is already the final one — epoch, TTL, CAS and the tag
table are all written today — so later milestones add behaviour without a
migration.

## Write throughput

LMDB permits one writer per environment, so writes scale only by fitting more of
them into each transaction. Measured with:

```bash
cargo run --release -p cache-store --example write_bench
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
it.

## Tag invalidation

Invalidating a tag is one generation-counter bump, so it costs the same whether
the tag covers ten keys or half a million. Records store the generation their
tags had when written, and a read compares those against the registry in RAM —
no extra disk lookup, no walk over the affected keys.

```bash
cargo run --release -p cache-store --example tag_bench
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
alerting on are `kached_evicted_total` rising (the cache is too small for its
working set), `kached_sweep_lag_ms` growing (reclamation is losing to expiry),
and `kached_readers_in_use` approaching `kached_readers_max`.

## Memcached compatibility

Both protocols share one port. The dialect is settled by the connection's first
byte — KCP opens with a `HELLO` frame (`0x01`), every memcached command opens
with a lowercase letter — so nothing is re-parsed and the two cannot be
confused. A key written by a memcached client is readable by a KCP client and
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

Two suites, both run in CI against **real memcached** as well as against kached,
so a divergence fails the build rather than someone's cache.

**Client library** — drives `pymemcache` through the behaviour clients actually
depend on:

```bash
pip install pymemcache
cargo run --release --bin kached -- --listen 127.0.0.1:11311 --data ./data --ephemeral
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
empty line after the error, which kached does not reproduce because it would
leave a pipelining client counting one more response than it sent commands.

## Layout

```
crates/cache-core     domain types, on-disk record format, no I/O
crates/cache-store    LMDB adapter behind the `Store` trait
crates/cache-proto    wire codecs (KCP + memcached); byte-slice in, `Command` out
crates/cache-server   network tier, dispatch, config, the `kached` binary
crates/cache-client   KCP client, and the integration-test driver
```

`cache-core` defines the domain; `cache-store` and `cache-proto` are adapters on
either side of it. Both protocols decode into the same `Command` type, so the
storage engine never learns which wire format a request arrived on.

## Development

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

Requires a C toolchain, because `heed` compiles LMDB from source — MSVC Build
Tools on Windows, `build-essential` on Linux.
