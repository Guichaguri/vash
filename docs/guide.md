# User guide

Everything needed to run vash and use it from an application: what it gives
you, how to start it, how to talk to it, and which switches matter.

Reference material lives elsewhere — the wire formats in
[protocol.md](protocol.md), the production checklist in
[operations.md](operations.md), every configuration key in
[vash.example.toml](../vash.example.toml).

---

## What vash is

A cache server. Keys map to opaque byte values with a TTL, values live in an
LMDB memory map so a restart does not empty the cache, and the whole thing is
one static binary with no runtime dependencies.

Two things distinguish it from what you are probably running today:

- **Tags.** A write can attach tag names to a record, and invalidating a tag
  drops every record carrying it in constant time — the same cost for ten keys
  or half a million.
- **One port, three protocols.** Existing memcached and Redis clients connect
  and work unchanged, alongside vash's own binary protocol. A key written by one
  is readable by the others.

It is a cache and behaves like one: data is bounded by a capacity you set, and
records are evicted when that fills. Nothing here is a system of record.

---

## Feature highlights

| | |
|---|---|
| **Protocol compatibility** | memcached text and meta, Redis RESP2 and RESP3, and the native VCP, all on the same port. The dialect is settled by the connection's first byte. |
| **TTLs** | Per-record, enforced on read, and reclaimed in the background so expired data does not keep occupying space. |
| **Tag invalidation** | Attach tags at write time; `DELETE_BY_TAG` invalidates all of them at once, in constant time, and propagates across a cluster. Reachable from all three dialects. |
| **Batches** | `GET_MANY` / `SET_MANY` / `DELETE_MANY`, and memcached / Redis multi-key commands. One transaction per batch, one round trip. |
| **Atomic counters** | Read-modify-write inside one transaction, with optional bounds, saturation and TTL. Counters are decimal text, so all three dialects move the same counter. |
| **CAS** | Every record carries a CAS token; memcached `gets`/`cas` work as upstream, and tokens never go backwards across a restart. |
| **Persistence** | LMDB-backed, so the cache survives a restart. `lazy` (default), `relaxed` or `durable` durability. |
| **Bounded capacity** | Watermarked eviction, **TTL-ordered rather than LRU** — soonest-to-expire goes first, never-expiring last. |
| **Write throughput** | Group commit across independent shards: batches form on their own from whatever queued during the previous commit, with no added delay. |
| **Clustering** | Independent nodes, no replication and no consensus. Only tag invalidation crosses node boundaries, by fan-out plus anti-entropy gossip. |
| **Authentication** | Optional, off by default; credentials in a separate file, reloadable with `SIGHUP` on Unix. |
| **Observability** | Prometheus `/metrics`, plus `/health` and `/stats`, on a separate admin port that stays closed until you name an address. |
| **Deployment** | One static binary. systemd unit and a `scratch` container image included. |

Dangerous capabilities are off until asked for: `FLUSH` / `flush_all`, key
listing (`LIST_KEYS`, `lru_crawler metadump`, Redis `SCAN`), and the admin port.
Either compatibility dialect can also be switched off so only the parsers you
use are reachable.

---

## Running the server

### From source

```bash
cargo run --release --bin vash-server -- --listen 127.0.0.1:11311 --data ./data
```

The defaults are `127.0.0.1:11311` and a `data` directory in the working
directory, so a bare `cargo run --bin vash-server` is a working server too.

To try it without leaving anything on disk:

```bash
cargo run --release --bin vash-server -- --ephemeral
```

`--ephemeral` starts from an empty database and keeps the default `lazy`
durability — the fastest
mode, and the right one for tests and local development.

### Container

The [`Dockerfile`](../Dockerfile) builds a statically linked binary onto a
`scratch` image — nothing in it but vash, and no shell to exec into.

```bash
docker build -t vash .
docker run --rm -p 11311:11311 -v vash-data:/var/lib/vash vash
```

### systemd

[`packaging/vash.service`](../packaging/vash.service) runs it under
`DynamicUser` with the kernel hardening a network-facing parser wants:

```bash
install -m0755 target/release/vash-server /usr/local/bin/vash-server
install -m0644 packaging/vash.service /etc/systemd/system/
install -m0644 vash.example.toml /etc/vash.toml
systemctl daemon-reload && systemctl enable --now vash
```

### Command-line flags

Flags override the config file.

| Flag | Effect |
|---|---|
| `--config <PATH>` | TOML config file. Defaults are used when omitted. |
| `--listen <HOST:PORT>` | Cache port, serving all enabled protocols. |
| `--data <DIR>` | Directory holding the database. |
| `--ephemeral` | Start empty, never sync. |
| `--admin-listen <HOST:PORT>` | Serve `/metrics`, `/health`, `/stats`. Off unless given. |
| `--enable-flush` | Allow `FLUSH` / `flush_all`. |
| `--enable-listing` | Allow `LIST_KEYS` / `LIST_TAGS` / `SCAN` / `lru_crawler` dumps. |
| `--disable-memcached` | Stop serving the memcached dialects. |
| `--disable-resp` | Stop serving the Redis dialect. |
| `--peer <HOST:PORT>` | Another node's cache port. Repeatable; replaces the config file's list. |
| `--require-auth` | Require authentication on the cache port. |
| `--auth-file <PATH>` | Credential file to authenticate against. |

There is one subcommand, `vash-server auth-gen [name]`, covered under
[Authentication](#authentication).

### Config file

```bash
vash-server --config /etc/vash.toml
```

[`vash.example.toml`](../vash.example.toml) is the full surface with every key
documented inline. Nothing in it needs changing to get a working server; the
keys worth knowing first:

```toml
[server]
listen = "127.0.0.1:11311"

[store]
path = "data"
map_size_mb = 4096      # per shard, not in total
shards = 0              # 0 = min(num_cpus, 4)
durability = "relaxed"  # or "durable" / "ephemeral"

[observability]
admin_listen = ""       # e.g. "127.0.0.1:9090"
```

**`map_size_mb` is per shard.** With the default 4 shards, `4096` reserves
16 GiB — but only of address space, which costs nothing until data arrives. Set
it to the largest the cache may ever grow to.

**The shard count is fixed once a database exists.** Starting with a different
one is refused rather than silently routing every key to a different
environment.

---

## Connecting

### With an existing memcached client

Nothing to change — point it at the port.

```python
from pymemcache.client.base import Client

c = Client(("127.0.0.1", 11311))
c.set(b"article:1", b"hello", expire=60)
print(c.get(b"article:1"))
```

`get`, `gets`, `set`, `add`, `replace`, `append`, `prepend`, `cas`, `delete`,
`touch`, `gat`, `gats`, `incr`, `decr`, `stats`, `version`, `flush_all` and
`quit` are all implemented with upstream semantics, as are the meta commands
`mg`, `ms`, `md`, `ma`, `mn` and `me`.

The legacy memcached **binary** protocol (magic `0x80`) is not implemented and
will not be — upstream deprecated it in favour of the meta commands. Configure
your client for the text protocol.

### With a Redis client

```bash
redis-cli -p 11311 SET article:1 hello EX 60
redis-cli -p 11311 GET article:1
```

The string and expiry families are supported — `GET`, `SET`, `MGET`, `MSET`,
`MSETEX`, `DEL`/`UNLINK`, `EXISTS`, `TYPE`, `EXPIRE`, `EXPIREAT`, `PERSIST`,
`TTL`, `APPEND`, `INCR`/`DECR`, `INCRBY`/`DECRBY`, `INCRBYFLOAT`, `INCREX`,
`SCAN`, `INFO`, `HELLO`, `AUTH`, `PING`, `QUIT` — in both RESP2 and RESP3.
Anything else answers `-ERR unknown command` and the connection carries on, so a
client library discovers what is missing rather than breaking. Lists, hashes,
sets, pub/sub and transactions do not exist here: every value is a string.

Tags are not reachable from the Redis dialect. Use memcached's extensions or
VCP for those.

### With the native client

```bash
cargo run --release --bin vash-server -- --listen 127.0.0.1:11311 --ephemeral
cargo run -p vash-client --example smoke -- 127.0.0.1:11311
```

[`crates/vash-client`](../crates/vash-client) is a Rust VCP client, and doubles
as the reference for what the protocol looks like from the outside:

```rust
let mut client = Client::connect("127.0.0.1:11311").await?;

client.set_tagged(b"article:1", b"hello", 60, &[b"news"]).await?;
let value = client.get(b"article:1").await?;
client.delete_by_tag(b"news").await?;
```

To write a client in another language, [protocol.md](protocol.md) has the frame
format, every opcode, and a client implementation checklist.

---

## Using the features

### TTLs

A TTL is seconds from now; `0` means no expiry. Records are checked on read, so
an expired key is a miss the moment it expires, and the background sweeper
reclaims the space afterwards. `TOUCH` (memcached `touch`/`gat`, Redis `EXPIRE`)
re-stamps a deadline without resending the value.

### Tags

Attach tag names when writing, then invalidate a whole set at once. Over
memcached, tags use two extensions to the protocol — clients that never send
them are unaffected:

```
ms article:1 5 Gnews,sport
value
HD
mdt news
HD
mg article:1 v
EN
```

`delete_by_tag <tag>` does the same in the classic dialect, and `DELETE_BY_TAG`
in VCP. Redis clients have three commands of their own, which any library can
send through its raw-command escape hatch:

```
SETTAGS article:1 value 2 news sport
+OK
MSETTAGS 2 a 1 b 2 1 news EX 60
:1
DELBYTAG news
:1
```

`SETTAGS` is `SET` and `MSETTAGS` is `MSETEX`, each with a counted tag list
before the usual options; `DELBYTAG` takes one or more tag names and answers how
many of them the server had registered. A tag is 1–255 bytes; a record carries up to 32 by default
(`store.tags.max_per_record`), and the server registers up to 100,000 distinct
names (`store.tags.max_tags`).

The guarantee to build on:

> When an invalidation response is received, no subsequent read on any
> connection will return a record that carried that tag and was written before
> the invalidation.

Within one node that is exact. Across a cluster it is eventual — see
[Clustering](#clustering). Writing a key again after an invalidation makes it
live once more, since the new write captures the new generation.

### Counters

Counters are stored as decimal text, so a plain `GET` of one returns something
readable and all three dialects move the same counter. memcached `incr`/`decr`
and Redis `INCR`/`INCRBY`/`INCRBYFLOAT` work as upstream. The read and the write
happen inside one transaction, so concurrent clients cannot lose an update.

Redis `INCREX` and VCP `ARITHMETIC` extend that with bounds, saturation and a
TTL applied in the same operation.

### Batches

`GET_MANY`, `SET_MANY` and `DELETE_MANY` — and their memcached and Redis
equivalents — take one transaction per batch and one round trip. A multi-get
reads from one consistent snapshot; a multi-write is all-or-nothing.

Batching is also where the throughput is: pipelining requests on one connection
is worth far more than adding connections, because the writer amortises commits
over whatever arrived while the previous one was in flight.

### Listing keys

Off by default, in every dialect, because cache keys routinely embed user and
session identifiers and these are the only reads whose cost is not bounded by
the request.

```bash
vash-server --listen 127.0.0.1:11311 --enable-listing

printf 'lru_crawler metadump all\r\n' | nc 127.0.0.1 11311
redis-cli -p 11311 SCAN 0 MATCH 'article:*'
```

The same gate covers VCP `LIST_KEYS` / `LIST_TAGS`, Redis `SCAN`, and memcached
`lru_crawler metadump` / `mgdump`. All are cursor-paged and glob-filtered.

### Flush

`FLUSH` and `flush_all` empty the whole cache for anyone who can reach the port,
so they need `--enable-flush` (or `protocol.flush_enabled`). Without it the
command is refused rather than silently ignored, and the VCP handshake reports
which it is.

---

## Authentication

Off by default: with `auth.required = false`, anyone who can reach the cache
port can read and write any key. **Bind the port to a private network
regardless** — there is no TLS, so authentication decides who may *use* the
cache, not who may read it in flight.

Generate a credential:

```bash
vash-server auth-gen billing-api >> /etc/vash/credentials
```

The secret is printed once on stderr and the file line on stdout, so the
redirect above appends the row while still showing you the secret. Nothing is
written to disk on your behalf.

The file is one credential per line, in the shape of `authorized_keys`:

```
# /etc/vash/credentials
default      sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
billing-api  sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae
```

Only the digest is stored, so the file holds no usable credential. Startup
refuses a file that is group- or world-readable on Unix. Turn enforcement on
once clients are configured:

```toml
[auth]
required = true
file = "/etc/vash/credentials"
```

Clients authenticate with their dialect's own mechanism — memcached's ASCII
authentication (upstream's `-Y` scheme, which is what memcached clients
implement), Redis `AUTH` / `HELLO … AUTH`, VCP `AUTH`. In a container,
`VASH_AUTH_SECRET` configures a single `default` identity without a file.
`SIGHUP` re-reads the file on Unix; on Windows, rotation means a restart.

Design and rollout: [auth.md](auth.md).

---

## Clustering

Nodes are independent. Clients shard the keyspace themselves, nothing is
replicated, and there is no consensus — adding a node adds capacity linearly,
and losing one costs `1/N` of the cache rather than an outage.

The one thing that crosses a node boundary is tag invalidation, because a tag's
keys are spread over every node by key hash:

```bash
vash-server --listen 0.0.0.0:11311 --peer 10.0.0.2:11311 --peer 10.0.0.3:11311
```

Peers talk over the same cache port. Each invalidation is pushed to peers as it
happens, and every node also exchanges tag generations with each peer every
`cluster.gossip_interval_ms` (default 5s) — which is what repairs a node that
was down, partitioned or restarted.

`cluster.delete_by_tag` picks the trade:

| Mode | `DELETE_BY_TAG` returns | Staleness elsewhere |
|---|---|---|
| `local` | after the local bump | unbounded — the client calls every node |
| `fanout` (default) | immediately; peers told in the background | bounded by the gossip interval |
| `fanout_sync` | after reachable peers have applied it | none for reachable peers |

Invalidation is strongly consistent within a node and eventually consistent
across the cluster. Under `fanout` there is a window — normally milliseconds —
in which another node still serves covered records. Both directions of that
error are a cache miss, never a stale hit.

---

## Monitoring

The admin port serves nothing until you name an address. It has no
authentication of its own and `/stats` describes the store and the cluster, so
give it a private interface.

```bash
vash-server --listen 0.0.0.0:11311 --admin-listen 127.0.0.1:9090

curl -s localhost:9090/metrics | grep -E 'hits|misses|utilisation|evicted'
curl -s localhost:9090/stats
curl -s localhost:9090/health
```

`/health` returns 503 when a shard has hit critical pressure and is refusing
writes — the process is up, but it is not doing its job, which is what a load
balancer needs to know.

Worth an alert:

| Signal | Means |
|---|---|
| `vash_evicted_total` rising | The cache is too small for its working set |
| `vash_errors_total{class="capacity"}` | Writes are being refused |
| `vash_sweep_lag_ms` growing | Reclamation is losing to expiry |
| `vash_readers_in_use` near `vash_readers_max` | Reader slots about to run out |
| `vash_cluster_peers_reachable` < `vash_cluster_peers` | A peer is down; its invalidations are not arriving |

The hit rate is a property of your workload, not of the server's health — watch
it, do not alert on it. Full list and thresholds: [operations.md](operations.md).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `schema version N, this build expects 2` | Database built by a different version | Wipe the data directory |
| `built for N shard(s), but M were configured` | `store.shards` changed | Restore the old count, or wipe |
| `map size … below the minimum` | `map_size_mb` under 16 | Raise it |
| `store.max_readers … must exceed …` | Reader table too small for the thread pool | Raise `max_readers` |
| Writes fail with `CAPACITY_FULL` | The store is full and eviction cannot keep up | Raise `map_size_mb` |
| Writes fail with `OVERLOADED` | The write queue is full | Retry with backoff; shard, or slow down |
| `ERR command disabled by configuration` from `SCAN` | Listing is off | `--enable-listing`, deliberately |
| A client connects and is immediately dropped | Its dialect is disabled | Drop `--disable-memcached` / `--disable-resp` |

Startup problems are always a refusal to start rather than a silent
misbehaviour, and for every one of them the safe fix is to wipe the data
directory — the contents are reconstructible from the origin by definition.

**Do not back this up.** A stale restore is worse than an empty cache.

---

## Where to go next

- [protocol.md](protocol.md) — wire formats, in enough detail to write a client
- [operations.md](operations.md) — sizing, tuning, failure modes, upgrades
- [auth.md](auth.md) — the credential design and how to roll it out
- [introspection.md](introspection.md) — what memcached `stats`, Redis `INFO`
  and `SCAN` report, and why
- [vash.example.toml](../vash.example.toml) — every configuration key
- [README](../README.md) — benchmarks and design rationale
