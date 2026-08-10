# kached

A cache server built on LMDB, with TTLs, tag-based invalidation and memcached
protocol compatibility.

Design and rationale: [docs/plan.md](docs/plan.md). Original brief:
[docs/project.md](docs/project.md).

**Status: M1 complete.** Single-key and batch operations over the native binary
protocol (KCP), TTLs with background reclamation, and group-committed writes.
Not yet production-usable — see [What works today](#what-works-today).

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
| Tags and `DELETE_BY_TAG` | M2 — currently **rejected** with `UNSUPPORTED` rather than silently ignored |
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
