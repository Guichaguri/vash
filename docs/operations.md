# Operations

Running kached: what to set, what to watch, and what each failure looks like
from the outside.

Configuration reference: [kached.example.toml](../kached.example.toml), which
documents every key inline. Wire behaviour: [protocol.md](protocol.md). On-disk
format: [storage.md](storage.md).

---

## Deploying

```bash
kached --config /etc/kached.toml
```

A single static binary with no runtime dependencies. Three ways to run it:

- **systemd** — [`packaging/kached.service`](../packaging/kached.service),
  hardened and using `DynamicUser`.
- **Container** — the [`Dockerfile`](../Dockerfile) builds a `scratch` image
  holding nothing but the binary.
- **By hand** — it is one process; nothing needs root.

Flags override the config file: `--listen`, `--data`, `--peer` (repeatable),
`--ephemeral`, `--enable-flush`.

### Ports

| Port | Default | Expose to |
|---|---|---|
| Cache | 11311 | Clients **and cluster peers**. Both protocols share it. |
| Admin | 9090 | Your monitoring only. |

**There is no authentication.** Anyone who can reach the cache port can read and
write any key, invalidate any tag, and — because peer traffic uses the same port
— raise any tag's generation. Bind it to a private network. Set
`observability.admin_listen = ""` to switch the admin port off entirely.

`flush_all` and the KCP `FLUSH` are off unless enabled: they empty the whole
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
and measure — `cargo run --release -p cache-bench --bin load -- --help` drives a
real server over a real socket.

| Setting | Raise it when | Costs |
|---|---|---|
| `store.inline_reads` | The working set is resident and reads dominate | A cold read stalls every connection on that worker |
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

### Durability

| Mode | Use when |
|---|---|
| `relaxed` (default) | Almost always. Loses at most the last few transactions on an OS crash, and cannot corrupt the database. |
| `durable` | You are treating a cache as a system of record. Reconsider. |
| `ephemeral` | The cache is genuinely disposable. Fastest; a crash means starting empty. |

A lost write is a cache miss, and a cache miss is already a supported outcome.
That is what makes `relaxed` the right default rather than a compromise.

## Monitoring

`/metrics` (Prometheus), `/health` and `/stats` (JSON) on the admin port.
`/health` returns 503 when a shard is refusing writes — the process is up but
not doing its job, which is what a load balancer needs to know.

### Alert on these

| Signal | Means | Do |
|---|---|---|
| `kached_evicted_total` rising | Live data is being dropped: the cache is too small for its working set | Raise `map_size_mb`, or accept the hit rate |
| `kached_errors_total{class="capacity"}` | Writes are being refused | Same, urgently |
| `kached_sweep_lag_ms` growing | Reclamation is losing to expiry | Lower `sweep_interval_ms`, raise `sweep_batch` |
| `kached_readers_in_use` near `kached_readers_max` | Reader slots are about to run out; reads will start failing | Raise `max_readers`, or lower `max_blocking_threads` |
| `kached_errors_total{class="overloaded"}` | The write queue is full | The writer is saturated: shard, or slow down |
| `kached_cluster_last_exchange_age_ms` > a few gossip intervals | This node is drifting from its peers | Check peer reachability |
| `kached_cluster_peers_reachable` < `kached_cluster_peers` | A peer is down; its invalidations are not arriving | Check that node |

### Watch, but do not alert

- **`kached_hits_total` / `kached_misses_total`** — the hit rate is a property
  of the workload, not of the server's health.
- **`kached_committed_ops_total / kached_commits_total`** — the mean write batch
  size. If it is near 1 under write load, group commit is not amortising
  anything and the writer is not the bottleneck.
- **`kached_utilisation`** — crossing 0.75 is normal and means reclamation went
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
Watch `kached_readers_in_use` against `kached_readers_max`.

### A peer is unreachable

Fan-out to it fails and is counted; anti-entropy retries every gossip interval.
Nothing is lost — generations merge by maximum, so the invalidation is delivered
whenever the peer comes back. Meanwhile that node serves data another node has
invalidated. `kached_cluster_peers_reachable` is the signal.

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
