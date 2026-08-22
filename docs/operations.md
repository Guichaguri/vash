# Operations

Running vash: what to set, what to watch, and what each failure looks like
from the outside.

Configuration reference: [vash.example.toml](../vash.example.toml), which
documents every key inline. Wire behaviour: [protocol.md](protocol.md). On-disk
format: [storage.md](storage.md).

---

## Deploying

```bash
vash-server --config /etc/vash.toml
```

A single static binary with no runtime dependencies. Three ways to run it:

- **systemd** — [`packaging/vash.service`](../packaging/vash.service),
  hardened and using `DynamicUser`.
- **Container** — the [`Dockerfile`](../Dockerfile) builds a `scratch` image
  holding nothing but the binary.
- **By hand** — it is one process; nothing needs root.

Flags override the config file: `--listen`, `--data`, `--peer` (repeatable),
`--ephemeral`, `--enable-flush`, `--enable-listing`, `--admin-listen`,
`--disable-memcached`, `--disable-resp`.

### Ports

| Port | Default | Expose to |
|---|---|---|
| Cache | 11311 | Clients **and cluster peers**. All three protocols share it. |
| Admin | **off** | Your monitoring only, once you turn it on. |

**Authentication is off by default.** With `auth.required = false` — the
default — anyone who can reach the cache port can read and write any key,
invalidate any tag, and, because peer traffic uses the same port, raise any
tag's generation.

**Bind it to a private network regardless.** A network boundary stops a party
who never sends a byte, where a credential only stops them after they have
reached a parser — so authentication decides who may *use* the cache, not who
may read it in flight. Turning it on adds a layer; it does not replace the
firewall rule. See [auth.md](auth.md) for the design and the rollout.

**What covers the wire is TLS**, on a second port, in a binary built with
`--features tls`:

```toml
[tls]
listen = "0.0.0.0:11312"
cert = "/etc/vash/cert.pem"   # PEM chain, leaf first
key = "/etc/vash/key.pem"
```

Both ports serve the same store, so a rollout moves clients across one at a
time and then empties `server.listen`. Watch `vash_tls` in `stats conns`, or
`vash_tls_connections_active` against `vash_connections_active`, to see what is
still arriving in the clear. Prefer an ECDSA P-256 certificate: the server
signs once per full handshake, and that is 308µs against RSA-2048's 804µs —
see [benchmarks.md](benchmarks.md#what-tls-costs). A `tls.listen` in a binary
built without the feature refuses to start rather than serving that port
unencrypted. The design is in [tls-proposal.md](tls-proposal.md).

**The admin port serves nothing until you name an address**, with
`observability.admin_listen` or `--admin-listen 127.0.0.1:9090`. It has no
authentication of its own, and `/stats` describes the store's size, its hit rate
and the cluster's membership to anyone who reaches it — so it is opened
deliberately, on an interface your monitoring can reach and nothing else can.

`flush_all` and the VCP `FLUSH` are off unless enabled: they empty the whole
cache for anyone who can reach the port.

## Sizing

**`store.map_size_mb` is per shard, not in total.** With the default 4 shards,
`map_size_mb = 4096` reserves 16 GiB of address space — but only address space:
the map is a lazy reservation, so a generous value costs nothing until data
arrives. Set it to the largest the cache may ever grow to, and control actual
memory with a cgroup limit or `MemoryMax` if you need one. The minimum is 16 MiB
per shard, enforced at startup.

**`store.shards` defaults to `min(num_cpus, 4)`.** It is the ceiling on
concurrent writers and does nothing for reads, so raise it only if writes are
the bottleneck *and* the disk is not — with syncing on, more environments
fragment I/O across the same device and throughput falls. Measure before
changing it. **The count is fixed once a database exists**; starting with a
different one is refused rather than silently routing every key elsewhere.

**`store.max_readers` must exceed `server.max_blocking_threads`**, which startup
enforces. Each thread holds a reader slot until it exits, so the table has to
cover every thread that can read at once.

## Tuning

Nothing here needs changing to get a working server. Change one thing at a time
and measure — `cargo run --release -p vash-bench --bin load -- --help` drives a
real server over a real socket.

| Setting | Raise it when | Costs |
|---|---|---|
| `store.inline_reads` | The working set is resident and reads dominate | A cold read stalls every connection on that worker |
| `store.prefault` | You want the map resident before the first request, not after it | Startup time proportional to the data |
| `store.write.max_batch` | Writes are the bottleneck and commits are small | Longer write transactions |
| `store.write.queue_depth` | `OVERLOADED` under bursts that would drain | Memory, and a longer queue to wait behind |
| `store.ttl.sweep_interval_ms` (lower) | Expired data lingers | Maintenance competes with traffic |
| `store.eviction.batch` | Eviction cannot keep up under pressure | A longer pause holding the write transaction |
| `cluster.gossip_interval_ms` (lower) | Cross-node staleness matters | More peer traffic |

### `inline_reads`

The biggest single knob, and the one most worth understanding.

Reads normally hand off to a pool of threads that are allowed to block, so a
read that page-faults stalls one of those rather than an async worker serving
other connections. `inline_reads = true` runs read-only requests directly on the
network worker instead. Writes always take the hand-off regardless, because they
wait on the writer queue by design.

**Measured on the development machine it makes no difference** — 1.18M against
1.28M ops/s on `GET`, well inside the run-to-run variance. It is off by default
and worth leaving off unless your own measurements disagree: what it removes is
the cost of a thread wake-up, which varies by platform far more than anything in
this code does.

If you do turn it on, turn it on because the working set is resident. When it is
not, a cold read blocks a worker and every connection assigned to it — which is
exactly what the default exists to prevent.

### `prefault`

The companion to `inline_reads`, and the way to stop that knob being a guess.
`prefault = true` reads each shard's `data.mdb` end to end before the server
accepts anything, so the pages are in the OS page cache and a read that would
have waited ~100 µs on the device takes a minor fault instead — page-table work
against memory that is already there, well under a microsecond, and never a
block on I/O.

**Measured** by `cargo run --release -p vash-store --example prefault_bench`,
which evicts the page cache with `posix_fadvise` and proves it with `mincore`.
On a 5.9 GiB four-shard store, 20,000 random `GET`s:

| | cold | prefaulted |
|---|---|---|
| p50 | 800 µs | **6.6 µs** |
| p99 | 2.9 ms – 58 ms | **80 µs** |
| throughput | 445 – 1,094 ops/s | **~75,000 ops/s** |

The p99 swinging from 2.9 ms to 58 ms between otherwise identical rounds is the
point, more than the averages are: that is the tail-latency collapse the
hand-off exists to prevent, arriving unpredictably with the page cache's mood.

It is off by default because it trades the one thing a memory-mapped store is
unusually good at: a cold start that serves immediately because nothing is
loaded. That costs **~2.2–3.7 s per GiB cold**, and ~260 ms per GiB when the
file is already cached. A good trade for a long-lived node whose tail latency
matters; a bad one for anything that restarts often.

Those absolutes came off WSL2 against a virtual disk, where a cold read costs
~800 µs rather than the ~100 µs plan §9 assumes of NVMe — so treat the ratio as
a ceiling. The direction is not in doubt.

On Linux the warmed mapping is additionally handed to `madvise` — `MADV_POPULATE_READ`
on 5.14 and later — which builds the page tables too and removes even the minor
fault. It is a hint, not the mechanism: on older kernels it degrades to
`MADV_WILLNEED`, which is advisory and mostly declines, and the sequential read
has already done the part that matters. Nothing about the flag's behaviour
depends on the platform beyond that.

Note that **`MAP_POPULATE` itself is not available**: it is a flag on `mmap`,
LMDB owns that call, and it exposes neither the flag nor the mapping's address.
`src/prefault.rs` records the detail.

### Huge pages — there is no knob, and that is deliberate

Worth knowing if you were about to go looking for one. `MADV_HUGEPAGE` on
LMDB's mapping **returns success and has no effect**, so vash does not offer it:
a setting that reports having worked while changing nothing is worse than its
absence.

Measured with a 256 MiB mapping of each shape, reading `THPeligible` back out
of `/proc/self/smaps` — an anonymous mapping reports eligible and gets 168 MiB
of huge pages, while a shared file mapping on ext4, which is exactly what LMDB
creates, reports **not eligible** and gets none. Transparent huge pages cover
anonymous memory and shmem, not shared mappings of ordinary files.

If you want huge pages behind a cache, the only route is an `ephemeral` store on
tmpfs with `/sys/kernel/mm/transparent_hugepage/shmem_enabled` moved off its
`never` default. That is a root-level, machine-wide kernel setting and therefore
yours to make, not something the server can or should do on your behalf.

### Durability

| Mode | Use when |
|---|---|
| `lazy` (default) | Almost always. Syncs on a timer rather than on every commit, so an OS crash loses writes newer than `write.sync_interval_ms` — one second by default — and **cannot corrupt the database**. |
| `relaxed` | You want every commit on the device and can pay for it: measured 4.5× slower on a pipelined write workload, because the `fsync` is what the writer queue backs up behind. |
| `durable` | You are treating a cache as a system of record. Reconsider. |

`ephemeral` is no longer one of these. It named `lazy` plus `MDB_WRITEMAP`, and
that flag measured slower than going without — so what was left was `lazy` with a
worse name and a wipe. `--ephemeral` still does what it always did, and now says
what it is: **`lazy` durability plus `wipe_on_start`**, a startup policy rather
than a durability guarantee. `store.write_map` is separately available for the
one thing that flag genuinely buys, which is memory rather than speed.

A lost write is a cache miss, and a cache miss is already a supported outcome.
That is what makes `lazy` the right default rather than a compromise — and note
what it still is compared with the alternatives it replaces: Redis and memcached
in their usual configurations lose *everything* on a restart.

**The one condition worth checking before trusting it.** `lazy`'s integrity
guarantee is LMDB's, and LMDB states it precisely: `MDB_NOSYNC` keeps the
database consistent *"if the filesystem preserves write order and the
`MDB_WRITEMAP` flag is not used"*. vash never sets `WRITE_MAP` in this mode, so
the remaining half is yours. Journalled filesystems in their ordinary
configurations qualify — ext4 with `data=ordered`, XFS, ZFS. If yours reorders
writes, you have `ephemeral`'s risk without `ephemeral`'s wipe, and should set
`durability = "relaxed"`.

## Monitoring

`/metrics` (Prometheus), `/health` and `/stats` (JSON) on the admin port, which
serves nothing until `observability.admin_listen` or `--admin-listen` names an
address. `/health` returns 503 when a shard is refusing writes — the process is
up but not doing its job, which is what a load balancer needs to know.

### Alert on these

| Signal | Means | Do |
|---|---|---|
| `vash_evicted_total` rising | Live data is being dropped: the cache is too small for its working set | Raise `map_size_mb`, or accept the hit rate |
| `vash_errors_total{class="capacity"}` | Writes are being refused | Same, urgently |
| `vash_sweep_lag_ms` growing | Reclamation is losing to expiry | Lower `sweep_interval_ms`, raise `sweep_batch` |
| `vash_readers_in_use` near `vash_readers_max` | Reader slots are about to run out; reads will start failing | Raise `max_readers`, or lower `max_blocking_threads` |
| `vash_errors_total{class="overloaded"}` | The write queue is full | The writer is saturated: shard, or slow down |
| `vash_cluster_last_exchange_age_ms` > a few gossip intervals | This node is drifting from its peers | Check peer reachability |
| `vash_cluster_peers_reachable` < `vash_cluster_peers` | A peer is down; its invalidations are not arriving | Check that node |

### Watch, but do not alert

- **`vash_hits_total` / `vash_misses_total`** — the hit rate is a property
  of the workload, not of the server's health.
- **`vash_committed_ops_total / vash_commits_total`** — the mean write batch
  size. If it is near 1 under write load, group commit is not amortising
  anything and the writer is not the bottleneck.
- **`vash_utilisation`** — crossing 0.75 is normal and means reclamation went
  continuous. Crossing 0.88 means live data is being evicted.

## Failure modes

### The store fills up

Three watermarks over the space actually in use, per shard: reclamation goes
continuous at 75%, live records start being evicted at 88%, and past 96% writes
are refused with `CAPACITY_FULL` while reads and deletes keep working.

Eviction takes the soonest-to-expire first and never-expiring last. If eviction
cannot keep up, writes fail — reads do not, and neither do deletes, since a
delete frees space.

**A full map is recoverable but not free.** A failed operation invalidates the
whole LMDB transaction, so a batch that hits the wall is aborted along with the
maintenance pass that would have freed space. The writer detects this, marks the
shard critical so callers are refused before they reach the queue, and runs a
reclaim in a fresh transaction with progressively smaller batches — deletions
need pages of their own.

### The writer is saturated

Writes get `OVERLOADED` once the queue is full. This is deliberate: a cache that
queues is worse than one that says no, because the client's fallback is cheaper
than a 30-second wait. If it is sustained, the writer thread or the disk is the
limit — check the mean batch size to tell which.

### A reader slot leaks

Reads fail with an LMDB error. A process that died without releasing its slots
has them reclaimed on the next open; a live process that leaks them does not.
Watch `vash_readers_in_use` against `vash_readers_max`.

### A peer is unreachable

Fan-out to it fails and is counted; anti-entropy retries every gossip interval.
Nothing is lost — generations merge by maximum, so the invalidation is delivered
whenever the peer comes back. Meanwhile that node serves data another node has
invalidated. `vash_cluster_peers_reachable` is the signal.

### The database will not open

| Message | Cause | Fix |
|---|---|---|
| `schema version N, this build expects 2` | Built by a different version | Wipe the directory |
| `built for N shard(s), but M were configured` | `store.shards` changed | Restore the old count, or wipe |
| `map size … below the minimum` | `map_size_mb` under 16 | Raise it |
| `store.max_readers … must exceed …` | Reader table too small for the pool | Raise `max_readers` |

Every one of these is a refusal to start rather than a silent misbehaviour, and
for every one the safe fix is to wipe: the data is reconstructible by
definition.

## Restarting and upgrading

Shutdown drains in-flight connections, stops the cluster tasks, syncs and closes
the environment. Idle connections — including a peer's — are released between
requests rather than held until a timeout.

`relaxed` durability may lose the last few transactions on an *unclean* stop; a
clean one syncs first. Data survives a restart, and CAS tokens never go
backwards across one.

Rolling a cluster one node at a time is safe. The node that is down misses
invalidations and catches up by anti-entropy on restart — immediately, since the
first gossip round happens at startup rather than one interval later. Its
clients see misses for its share of the keyspace while it is gone.

## Backups

Don't. This is a cache: the data is reconstructible from the origin by
definition, a stale restore is worse than an empty one, and `map_size` is
address space rather than a file size so a naive copy is misleading.

If you have a reason anyway, LMDB's `mdb_copy` on each shard directory produces
a consistent snapshot without stopping writes.
