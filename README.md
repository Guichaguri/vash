# kached

A cache server built on LMDB, with TTLs, tag-based invalidation and memcached
protocol compatibility.

Design and rationale: [docs/plan.md](docs/plan.md). Original brief:
[docs/project.md](docs/project.md).

**Status: M2 complete.** Single-key and batch operations over the native binary
protocol (KCP), TTLs with background reclamation, group-committed writes, and
constant-time tag invalidation. Not yet production-usable — see
[What works today](#what-works-today).

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
| Memcached protocol | M3 |
| Sharding, capacity eviction, Prometheus metrics | M4 |
| Cluster tag invalidation | M5 |

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
batches on its own. Sharding (M4) multiplies this by running independent
environments with a writer each.

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

## Layout

```
crates/cache-core     domain types, on-disk record format, no I/O
crates/cache-store    LMDB adapter behind the `Store` trait
crates/cache-proto    wire codecs; pure byte-slice in, `Command` out
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
