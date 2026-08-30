# The slowlog and hot keys — a proposal

[m10.md](m10.md) promised "the promised observability", and plan §14 records it
delivered: queue-wait and execution latency are separately visible. What shipped
is a set of **aggregates** — per-command counters split by dialect, eleven
latency buckets, and a queue-wait total. They answer *how is the server*.

Neither of the two questions an operator actually asks during an incident is an
aggregate:

1. **What was slow just now?** `vash_command_latency_seconds` says the 99th
   percentile of `set` crossed 25 ms. It cannot say which `set`, from which
   client, against which key, or whether the time went into the shard queue or
   into the commit.
2. **Which key is hot?** Nothing in the server has ever counted a key. Not
   `stats`, not `INFO`, not `/metrics`. A single key taking a third of the
   traffic is invisible, and it is the most common cause of the tail this
   server's own README calls "queueing on a saturated box".

This document proposes the two instruments that answer them: a **slowlog** and a
**sampled hot-key summary**. Same house style as the rest: decision first, then
the reasoning, then what was rejected.

**The recommendation, up front:**

1. **A slowlog entry records two durations, not one.** Redis's `SLOWLOG` records
   execution time and excludes I/O. Copying that here would build an instrument
   blind to this server's actual failure mode, because `dispatch::execute` times
   `execute_inner` alone — deliberately, and its comment says so: *"this is the
   storage work and not the wait to reach a thread that could do it."*
   [§3.1](#31-what-slow-means-here-and-why-rediss-answer-is-the-wrong-one).
2. **Key names are gated; the rest of the entry is not.** A slowlog that only
   works when `listing_enabled` is set is a slowlog that is off during the
   incident. So an entry always carries command, dialect, durations, connection
   and sizes, and carries the **key bytes only under the existing listing gate**
   — an `xxh3` digest of the key otherwise, which still answers "is it the same
   key every time" without disclosing which.
   [§3.3](#33-the-key-and-what-it-costs-to-disclose-one).
3. **A batched write produces one entry, not one per member.**
   `dispatch::count_stored` divides a commit's duration across the batch and
   documents that the distribution "comes out narrower than reality". That is
   the right call for a histogram and the wrong one for a slowlog, where the
   whole point is the outlier.
   [§3.4](#34-the-batch-problem-which-the-metrics-path-already-solved-differently).
4. **Hot keys are sampled and estimated, and the surface says so in its field
   names.** Exact per-key counting at 1.28M ops/s is a lock and an unbounded map
   on the hottest path in the program. A per-worker Space-Saving summary fed by
   a ~1-in-128 sample costs a counter bump, and names its output
   `estimated_ops`. [§4](#4-hot-keys).
5. **Neither ever becomes a Prometheus label.** `metrics.rs` already refused an
   identity label on the grounds that "the cardinality is fixed at six, where an
   identity label is unbounded". A key label is worse: it is unbounded *and*
   attacker-chosen. Both surfaces are pull-only.
   [§2](#2-the-rules-that-govern-both).
6. **RESP gets `SLOWLOG`, memcached gets nothing, VCP waits.** `SLOWLOG` is real
   upstream Redis grammar with real client support. memcached has no equivalent,
   and inventing one repeats the mistake [introspection.md](introspection.md)
   §10 declined when it kept a `MATCH` filter off `metadump`.
   [§5](#5-where-the-surfaces-go).

---

> **Phase 0 has run. One gate failed and changed the design, and two of the
> three could not be evaluated as written.**
> Measured in [benchmarks.md](benchmarks.md#what-the-slowlog-and-the-hot-key-sampler-cost):
>
> 1. **Gates 1 and 2 are not measurable on this box, and the control proves
>    it.** They ask for ≤1% end to end. Six ABBA blocks put pipelined `GET` at
>    −2.34% median with per-block deltas from −9.4% to +6.3%, and the spread
>    within a single arm is 27.8%. The `SET` arm is the control — the
>    instrumentation cannot make writes faster — and it measured **+14.5% in one
>    block**. A harness reporting a 14% speed-up from a change that cannot cause
>    one cannot rule on 1%, so the plausible-looking −2.34% is the same noise
>    wearing a better sign. §7's gates 1 and 2 are answered by the
>    micro-benchmark instead, and the end-to-end runs bound the effect rather
>    than measuring it.
> 2. **The instrument costs 3.81 ns a request**, priced directly on
>    `cargo bench --bench hot_path`: about 1.2 ns of hot-key sampler and the rest
>    the slowlog's threshold compare. That is 4.2% of the 90.9 ns RESP parse
>    [performance-proposals.md](performance-proposals.md) §11 dismissed as "0.8%
>    of one core" — roughly 0.03% of a core at the same request rate.
> 3. **[§3.5](#35-where-the-buffer-lives-and-what-the-threshold-check-costs) was
>    wrong, and gate 3 caught it.** Its argument that one mutex is safe because
>    "the lock is only ever taken by a request that already lost" holds at the
>    shipping threshold and fails at `threshold_us = 1`, which is what an
>    operator sets during an incident: closed-loop p99 rose 16% and p99.9 38%,
>    with **all four ABBA blocks moving the same direction** — the one signal a
>    ±20% harness can still carry. The rings are now striped per thread and
>    merged on read, as §7 pre-committed, and the signs go mixed. The section
>    below is kept as written.
> 4. **Two bugs the benchmarks did not find.** A `RefCell` holding the sampler's
>    PRNG and its summary together cost 7.06 ns against the 3.92 ns of splitting
>    the PRNG into a `Cell` — and two thirds of that saving was the `#[cold]`
>    split rather than the borrow flag, since the *disabled* path improved by as
>    much. And rings hung off thread-locals lose their entries when the thread
>    exits, which for a pool tokio retires after ten seconds idle means losing
>    exactly the slow **writes** the log exists to catch. A test asserting an
>    entry outlives its thread is what found it.
>
> The rest of this document is kept as written, including the parts the
> measurements went on to contradict.

---

## 1. Why the existing metrics cannot be extended to cover this

Not "they were built wrong" — they were built for a different question, and the
shape that makes them good at it is exactly what stops them here.

**A histogram discards identity by construction.** `CommandMetrics` holds
`latency: Vec<Histogram>`, one per `CommandKind`, and its doc comment defends
dropping the dialect from the histogram axis: *"a command's latency is a
property of the storage work rather than of the wire format that asked for it.
Splitting the histograms by dialect as well would triple the series to separate
numbers that measure the same thing."* That reasoning is correct and it
generalises: an axis cannot be added to a histogram for a thing with unbounded
values. A key is such a thing.

**The buckets run out exactly where the interesting requests are.**
`LATENCY_BOUNDS_US` is `[10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000,
25_000, 100_000]`. Above 5 ms there are two buckets and then an overflow. A
request that took 40 ms and one that took 90 ms are the same observation. The
comment says this is deliberate — *"by then the answer is 'something is wrong'
rather than a number worth resolving further"* — and it is right that the
*number* is not worth resolving. What is worth resolving is **which request it
was**, and no bucket count fixes that.

**Queue wait exists only as a total.** `vash_writer_queue_wait_seconds_total` is
a counter, rendered with the instruction to divide it by
`vash_committed_ops_total` for a mean. There is no per-request queue wait
anywhere, which is the single largest gap this proposal closes: under write
pressure the slow request is slow *before* `execute` is entered, and every
instrument in the server today is looking after that point.

## 2. The rules that govern both

Inherited from [stats-subcommands.md](stats-subcommands.md) §2, plus two
corollaries this design needs.

**Nothing is reported that is not measured.** Unchanged, and it is why the
hot-key report has no "cache hit rate for this key" column: the sampler sees one
in every hundred-odd requests and cannot honestly produce a ratio.

**An estimate is named as an estimate.** New, and forced by §4. A sampled count
is not a count. The field is `estimated_ops`, the report carries the sample rate
it was taken at, and the documentation states the error bound rather than
leaving a reader to assume there isn't one. A number that looks exact and isn't
is the same failure mode as a counter reading zero because the feature does not
exist.

**Cardinality on the scrape path stays bounded.** `ServerMetrics` split role
refusals into six fixed counters specifically to avoid an unbounded label, and
`metrics.rs` says so. Keys are unbounded and chosen by whoever is connected; a
`vash_key_ops_total{key="…"}` family is a way for a client to exhaust the
monitoring system. Neither surface here emits a Prometheus series with a key in
it, ever.

**Key names are data.** `listing_enabled` defaults to `false` and
`dispatch::listing_gate` refuses enumeration when it is clear, on the stated
grounds that it "decides what any connected client may do". A hot-key report is
enumeration by another name — it discloses key names the caller did not supply —
and it sits behind the same gate and the same `Roles::LIST`.

## 3. The slowlog

### 3.1 What "slow" means here, and why Redis's answer is the wrong one

Redis logs the execution time of a command, explicitly excluding time spent in
I/O with the client. The analogous choice here is `started.elapsed()` around
`execute_inner`, which is already computed in `dispatch::execute` and thrown
into a bucket.

That number is misleading on this server, and the code says why in its own
comment:

> Timed around `execute_inner` only, so this is the storage work and not the
> wait to reach a thread that could do it. The wait is the shard writer's and is
> measured there — subtracting the two is what answers plan §12's question of
> whether the writer is the bottleneck.

So on a server whose writes are queued to per-shard writer threads, and whose
README states the write ceiling is the commit rather than anything above it, a
slowlog built on this number reports **fast entries during exactly the incident
it was built for**. Every `SET` executes in 84 µs; the client saw 40 ms; the
slowlog is empty. It would be worse than nothing, because it would exonerate the
writer.

**Decision: an entry carries `queue_us` and `exec_us` separately, and is
admitted on their sum.** The threshold is compared against total time in the
server, which is the only quantity corresponding to something a client
experienced. The split is what makes the entry actionable — the two columns
distinguish "the store is slow" from "the store is fine and the queue is 900
deep", and those have different fixes.

This needs one thing that does not exist: a write job must carry its enqueue
instant through the shard queue so the writer can report the wait per job rather
than only adding it to a total. That is a `vash-store` change, and it is the only
one in this proposal. §9 phases it first for that reason.

For a read, `queue_us` is the hand-off to the storage thread, or `0` when
`store.inline_reads` is set and there was no hand-off. Reporting `0` rather than
omitting the field is correct here and not a violation of §2: zero is the
measured truth, because the wait really did not happen.

### 3.2 The entry

| Field | Source | Notes |
|---|---|---|
| `id` | monotonic counter | Redis semantics: strictly increasing, never reused, does not survive a restart |
| `timestamp` | wall clock, seconds | Redis renders this; operators correlate against application logs with it |
| `queue_us` | shard queue, per job | §3.1. `0` for an inline read |
| `exec_us` | `execute` / `execute_get_into` | The number the histogram already observes |
| `command` | `CommandKind::name()` | Reuses the existing coarse classification — `EXISTS`, `TYPE` and `TTL` are one `deadline`, as they are everywhere else |
| `dialect` | `Dialect::name()` | Recorded here where the histogram deliberately drops it: at one entry per incident the cardinality argument does not apply |
| `conn_id` | `ConnInfo.id` | **Not built — phase 1 left this open.** The registry in `connections.rs` assigns one and `stats conns` renders it, so sourcing it is free. *Reaching* it from `dispatch::execute` is not: it means threading the connection through three dialect executors, some two dozen call sites on the hottest signatures in the program. Worth deciding on its own rather than as a rider on the batch rule |
| `peer` | `ConnInfo.peer` | Not built, with `conn_id`, and the weaker of the two: a pooled application server is a handful of repeated addresses. Discloses nothing `stats conns` does not |
| `keys` | command | *Count* of keys, always present |
| `key` | command | The first key. Gated — §3.3 |
| `value_bytes` | command | Sum of value bytes written, `0` for a read. What separates "slow because 4 MiB" from "slow because queued" |
| `batch` | `SetBatch` | Members in the batch this entry accounts for; `1` for an ordinary command. §3.4 |

Deliberately absent: the full argument list. Redis truncates arguments to 32 and
each to 128 bytes; here the arguments to a `SET` are the *value*, which is user
data up to `store.max_value_bytes` and has no business being copied into a ring
buffer that a later `SLOWLOG GET` prints. `value_bytes` is the part that
diagnoses anything.

### 3.3 The key, and what it costs to disclose one

The tension is real and worth stating plainly rather than resolving quietly.
Without the key, the slowlog answers "`SET` was slow at 14:03 from 10.0.0.7".
With it, it answers "`SET` of `session:a9f…` was slow", which is usually the
whole diagnosis. But a key name is data, `listing_enabled` is `false` by default
for exactly that reason, and a slowlog that inherits that default is off when it
is needed.

**Decision: the entry always carries a key *identifier*, and what that
identifier is depends on the gate.**

- `listing_enabled = true`, caller holds `Roles::LIST` → `key` is the key bytes,
  rendered as `metadump` renders keys.
- Otherwise → `key` is `xxh3:<16 hex digits>`, using the `xxhash_rust::xxh3`
  already in the workspace and already used to pick a shard.

The digest is not a consolation prize. The question a slowlog answers about a key
is nearly always *"is it the same one every time?"* — a hot key, a poison value,
a client retrying one entry in a loop — and a stable digest answers that
completely. It is a one-way function of data the caller already sent, so it
discloses nothing to the client that sent it, and to an operator reading
`/slowlog` it discloses nothing at all. Recovering the name once the pattern is
established costs one `listing_enabled` restart, which is a decision an operator
can now make *with evidence* instead of ahead of time.

### 3.4 The batch problem, which the metrics path already solved differently

`dispatch::count_stored` takes one `elapsed` for a whole `set_many` and divides:

```rust
let each = elapsed / sets.len() as u32;
```

with the comment that "the mean stays honest and the distribution comes out
narrower than reality". For a histogram that is the right trade. For a slowlog it
inverts the instrument: a 40 ms commit of 64 sets becomes 64 observations of
625 µs, every one of them under any sane threshold, and the batch that actually
stalled the connection is invisible.

**Decision: `SetBatch::flush` admits at most one entry, for the batch, charged
the undivided duration, with `batch = sets.len()`.** The per-member division
stays exactly as it is for `CommandMetrics` and `OutcomeMetrics` — this proposal
changes nothing about the counters. The two instruments disagree on purpose, and
`protocol.md`'s divergences table is the place to say so.

The fallback path in `flush`, where a failed batch is retried one command at a
time through `execute`, needs no special handling: those go through the ordinary
single-command path and are logged as ordinary entries, which is correct, because
at that point they really are ordinary commands.

### 3.5 Where the buffer lives, and what the threshold check costs

A `Mutex<VecDeque<Entry>>` of fixed capacity, on `ServerState`, one for the
server. Not per-worker, and not lock-free.

The reasoning is that **the lock is only ever taken by a request that already
lost**. The hot path gains one comparison:

```rust
if queue_us + exec_us >= threshold_us { slowlog.record(…) }
```

against a `Duration` both call sites have already computed for the histogram. At
the default 10 ms threshold on a server whose `GET` p50 is tens of microseconds,
the branch is never taken, predicts perfectly, and the entry construction — which
is where the allocation and the lock are — is not reached. A request that *does*
cross 10 ms can afford a mutex: it has already spent 10,000 µs, and the
acquisition is nanoseconds against that.

This is the same argument `metrics.rs` makes for relaxed atomic adds, run in the
other direction: there, contention is accepted because the work is trivial; here,
a lock is accepted because it is unreachable in the common case.

Capacity is `slowlog.max_len` entries, default 128, evicting oldest. Bounded
memory: 128 × (fixed fields + at most `MAX_KEY_LEN`) ≈ 70 KB worst case, and a
few KB in practice.

### 3.6 `SLOWLOG RESET`, and why it is allowed where `stats reset` is not

[introspection.md](introspection.md) §3.2 is emphatic: *"`stats reset` in
particular must not be implemented"*, because the counters behind it are the same
atomics `/metrics` exports and "a Prometheus counter that goes backwards corrupts
every rate over the window containing the reset."

`SLOWLOG RESET` is not that, and the distinction is exact rather than convenient:

- The slowlog is a **ring buffer, not a counter**. It is already lossy by design;
  entry 1 is evicted by entry 129 with no ceremony. Emptying it early destroys no
  monotonic series.
- **No Prometheus family is derived from its contents.** The two counters that
  *are* exported — how many entries the log has admitted, and how many it has
  evicted — are ordinary monotonic counters and are explicitly **not** reset by
  `SLOWLOG RESET`. `RESET` clears the buffer; the counters keep counting.
- Clearing it is the normal operating procedure: an operator resets, reproduces,
  and reads the entries their reproduction produced. Refusing that would make the
  instrument worse for no gain.

`SLOWLOG LEN` and `SLOWLOG HELP` come free. `SLOWLOG GET [count]` renders newest
first, as upstream does.

## 4. Hot keys

### 4.1 Why exact counting is not on the table

A `HashMap<Vec<u8>, u64>` behind a lock, updated on every request. At the
README's 1.28M `GET`/s that is 1.28M lock acquisitions a second across every
worker onto one cache line, plus a hash and a probe, plus an allocation the first
time each key is seen, plus unbounded growth — a client can mint a new key per
request and the map is the memory leak. Every one of those four is disqualifying
on its own. It is not a tuning problem.

Sharding the map by key hash removes the contention and none of the other three.

### 4.2 Sampling, and the arithmetic that sets the rate

**A hot key is, by definition, one a uniform sample hits often.** That is not a
compromise in this application; it is the property being measured.

At `1/128` and 1.28M ops/s the sampler admits ~10,000 keys a second. A key taking
1% of traffic (12,800 ops/s) contributes ~100 samples a second — ranked
confidently within a second. A key taking 0.01% contributes ~1 a second and sits
in the noise, which is correct, because at 0.01% it is not hot and an operator
who acted on it would be acting on nothing.

The estimate is `samples × rate`, with relative standard error of roughly
`1/√samples` — 10% at 100 samples, 3% at 1,000. **The report states the sample
count beside every estimate**, so a reader can see which rows are solid, and §2
requires the field be called `estimated_ops`.

**Sampling by a per-worker PRNG, not by a hash of the key.** Hashing the key and
admitting a fixed `1/128` subset of the *keyspace* is tempting — it is one `xxh3`
the shard router may already have computed — and it is wrong for top-K: it
decides once and forever whether a key can ever be counted, so the hottest key in
the system is invisible 127 times out of 128. A per-worker `xorshift32` advanced
once per request costs a few nanoseconds of thread-local work with no shared
state, and is unbiased. A plain "every 128th request" counter is cheaper still
and rejected only because it can alias with a client's own round-robin over N
connections; if phase 0 shows the PRNG costing anything measurable, the counter is
the fallback and the aliasing risk goes in the docs.

### 4.3 Space-Saving, not count-min

Count-min is the reflex and it is the wrong structure. It answers *"roughly how
many times did key X occur"* for an X already in hand. The question here is
*"which keys"*, which count-min cannot answer at all without a companion
structure holding candidate keys — at which point the companion is doing the work
and the sketch is overhead.

**Space-Saving** (Metwally, Agrawal & El Abbadi, 2005) answers top-K directly:
`m` counters, each holding a key and a count; a sampled key already present
increments; a new key evicts the minimum and inherits its count. Fixed memory, no
allocation after warm-up, and the guarantee that any element exceeding `N/m` of
the stream is present in the summary.

With `m = 64` per worker, anything above ~1.6% of that worker's sampled stream is
guaranteed to be held. That is the right sensitivity: this instrument exists to
find the key taking 30% of the traffic, not the one taking 0.5%.

### 4.4 Per-worker summaries, merged on read

One summary per thread that executes commands, in a `thread_local!`, registered
into a `Mutex<Vec<Arc<…>>>` on first touch and merged only when `/hotkeys` is
called. No shared mutable state on the request path at all — the sampled update
touches one thread's own cache lines.

Thread count is bounded and small: the tokio workers plus
`store.max_blocking_threads`. Memory is `threads × m × (key + 16 bytes)`; at 24
threads, `m = 64` and the 511-byte worst-case key that is ~800 KB, and with
realistic keys well under 100 KB. Keys longer than 128 bytes are stored truncated
with their full `xxh3` beside them, which bounds the worst case without losing
identity.

**The honest limitation, stated because §2 requires it:** Space-Saving's formal
error bound does not survive merging independent summaries. Merged, it is a
heuristic — a very good one, and the standard practice — but the top-K it returns
is not provably the true top-K. This is why every count it produces is
`estimated_ops` and why the report is documented as a ranking to investigate
rather than a measurement to bill against.

### 4.5 Reads and writes are counted separately

Two counters per summary entry, ranked on their sum. A hot *read* key is a
candidate for a client-side cache or a replica; a hot *written* key is a
serialisation point on one shard's single writer thread, which is a completely
different problem with a completely different fix — and on this server, with one
writer per environment, it is the more damaging of the two. An instrument
returning them added together would name the key and hide the diagnosis.

## 5. Where the surfaces go

| Surface | Decision | Why |
|---|---|---|
| Admin port, `GET /slowlog` | **Build.** JSON, newest first, `?count=` | The primary home. `admin.rs` already serves read-only JSON on a separate port operators already scrape, and `stats_json` is the pattern to copy |
| Admin port, `GET /hotkeys` | **Build**, behind `listing_enabled` | Same, plus §2's gate — it discloses key names on a port with no authentication |
| RESP `SLOWLOG GET/LEN/RESET/HELP` | **Build** | Real upstream grammar with real client and tooling support. Key rendering follows §3.3 |
| RESP `INFO` | **One field**: `slowlog_len` in a `Stats` section | Upstream reports it there; it is a count, not a disclosure |
| memcached | **Nothing** | Upstream has no slowlog and no hot-key command. [introspection.md](introspection.md) §10 refused a `MATCH` filter on `metadump` because "a vash-only argument on a memcached command is a trap for tooling that passes class ids positionally". Inventing `stats slowlog` is the same trap with a bigger surface |
| VCP opcode | **Defer** | `vash-client` is used by cluster peers and by the smoke example, neither of which needs this. It is one opcode whenever something does — [opcodes.md](opcodes.md) is the contract to extend, and nothing here forecloses it |
| Prometheus | **Two counters only**: entries admitted, entries evicted | §2. No key labels, no per-entry series. The evicted counter is the one that tells an operator `max_len` is too small |

## 6. Configuration

```toml
[observability.slowlog]
# 0 disables recording. There is no "log everything" setting, because
# everything is the histogram's job.
threshold_us = 10_000
max_len = 128

[observability.hotkeys]
enabled = true
# One in N requests is sampled. 0 disables.
sample_rate = 128
# Counters per worker summary. Bounds both memory and sensitivity.
counters = 64
```

Both default **on**, which is a departure from `listing_enabled` and
`auth.required` and needs its own justification: those two are off because they
expose data or change who may connect. These expose counters and — until the
listing gate is opened — digests. The cost is a predicted-false branch and a
thread-local increment. An observability feature that defaults to off is an
observability feature that is not running when the incident starts.

`threshold_us = 10_000` matches Redis's default of 10,000 µs, which is a
well-calibrated number operators already have intuitions about, and is ~200× this
server's `GET` p50.

## 7. What it costs, and what phase 0 must prove

Nothing in §6 is believed until measured. The existing rigs are
`cargo bench --bench hot_path` for per-operation cost and `vash-bench` for end to
end.

The prior, for calibration rather than for trust: `hot_path` prices a memcached
`get` parse at 53.5 ns and a RESP one at 90.9 ns, and
[performance-proposals.md](performance-proposals.md) §11 dismissed optimising
them as "0.8% of one core" at 220k ops/s. A thread-local xorshift and a
predicted-false compare should be ~1–2 ns, i.e. about 2% of what that section
called negligible. Should be. Measure it.

**The gates, agreed before the numbers exist so they cannot be moved
afterwards:**

1. **`GET` throughput loss ≤ 1%** with both features enabled and idle (no entry
   crossing the threshold), against the same build with them compiled out. Above
   1%, the sample rate or the structure is wrong.
2. **`SET` throughput loss ≤ 1%**, same conditions. Writes carry the new enqueue
   instant through the shard queue, which is the one change on the store side,
   and it must not show up.
3. **A saturated slowlog does not change the tail.** Threshold set to `1` so
   every request records, at the pipelined `GET` rate: this is the abuse case and
   the number that decides whether the single mutex in §3.5 is defensible. If p99
   moves, the buffer becomes per-worker and the merge moves to the read side, as
   §4.4 already does for hot keys.
4. **The estimator is accurate on a known distribution.** Drive a Zipfian
   workload with a known top-10 and check the report recovers the ranking, and
   that `estimated_ops` lands within the error bound §4.2 claims. This is the gate
   that stops §4.4's merge heuristic from shipping on faith.

Variance discipline as everywhere else: five repeats, ranges reported, and the
README's ±25% caveat applies here too — which is precisely why gates 1 and 2 are
stated against a *compiled-out* build rather than against a historical number.

## 8. Rejected

| | Why |
|---|---|
| Per-key Prometheus series | §2. Unbounded and attacker-chosen cardinality. This is the whole reason the surfaces are pull-only |
| Exact per-key counting | §4.1. A lock on the hottest path and an unbounded map |
| Count-min sketch | §4.3. Answers a question nobody asked and still needs a candidate structure |
| Logging full command arguments, as Redis does | §3.2. The argument to a `SET` is the value; copying user data into a printable buffer is a disclosure with no diagnostic value `value_bytes` does not already provide |
| `stats slowlog` / any memcached surface | §5. Not upstream grammar |
| Driving eviction from the hot-key data | plan §6 rejected an on-disk LRU and nothing here reopens it. This instrument informs an operator; it does not steer the store |
| Persisting either across a restart | The same reasoning [introspection.md](introspection.md) §4.2 gave for SCAN cursors: it needs a table, an expiry policy for it, and a reason. An incident is diagnosed on a running server |
| A `SLOWLOG` in `vash-client` | §5. Nothing in the cluster or the smoke path needs it yet |
| Resetting the admitted/evicted counters on `SLOWLOG RESET` | §3.6. Those two really are Prometheus counters and really would corrupt a rate |

## 9. Work breakdown

Phased so each lands green on its own, and so the measurement gate comes before
the surface area rather than after it. This is M13; M11 and M12 are taken by
[mdbx-proposal.md](mdbx-proposal.md) and [tls-proposal.md](tls-proposal.md).

| Phase | Scope | Exit |
|---|---|---|
| **0** — **done** | Instrument only, no surface: the enqueue instant through the shard queue, the threshold compare, the sampled summary, and the ring the saturated case needed in order to be measurable at all. Behind `--features introspect` | **Ran.** Gate 3 failed and rewrote §3.5; gates 1 and 2 proved unmeasurable end to end and were answered at 3.81 ns on `hot_path`. Published in [benchmarks.md](benchmarks.md#what-the-slowlog-and-the-hot-key-sampler-cost) |
| **1** — **done** | The entry, the batch rule, and `GET /slowlog` with the §3.3 key gate. The ring buffer landed in phase 0, which needed one in order to saturate. The entry's `conn_id` and `peer` did not — §3.2 says why | **Met.** Against a real store a batch of six records one entry at `batch: 6`, `queue_us: 34`, `exec_us: 1032` — the two durations populated and distinguishable from a real writer queue. The fused read path records too, and keys render as digests until `listing_enabled` |
| **2** | RESP `SLOWLOG GET/LEN/RESET/HELP`, `INFO`'s `slowlog_len`, the `roles_for_command` mapping, the §3.3 key gate | `redis-cli slowlog get` renders unchanged; keys are digests until `listing_enabled`; `RESET` clears the buffer and not the counters |
| **3** | Space-Saving summaries, the merge, `GET /hotkeys` behind the listing gate | §7's gate 4 passes against a known Zipfian; the route refuses when `listing_enabled` is clear |
| **4** | The two Prometheus counters, an [operations.md](operations.md) runbook entry, a `protocol.md` divergences row for the batch rule | An operator can go from "p99 crossed 25 ms" to a key digest and a queue-versus-commit verdict by following written steps |
| **5** — *not committed* | Hot **tags**, ranked the same way | `DELETE_BY_TAG` on a hot tag invalidates a whole working set at once and is the most likely trigger for the stampede this server has no defence against. It is the natural companion to a stale-while-revalidate design and should be decided with that, not before it |

## 10. Testing

- **The threshold is exact at its boundary.** A synthetic duration one
  microsecond under records nothing; one over records one entry. An off-by-one
  here silently changes what an operator sees.
- **The batch rule.** A `SetBatch::flush` of N members that crosses the threshold
  produces exactly one entry with `batch = N` and the undivided duration — and
  `CommandMetrics` still observes N divided ones. The test asserts both, because
  the two instruments disagreeing is the design, and a future refactor "fixing"
  it would be a regression.
- **The retry path.** A batch that fails and retries member-by-member through
  `execute` produces ordinary single entries, not one batch entry.
- **The key gate flips.** The same slow request under `listing_enabled` true and
  false yields key bytes and a stable `xxh3:` digest respectively, and the digest
  is the same across both runs.
- **The ring evicts oldest and the counter counts it.** `max_len + 1` slow
  requests leave `max_len` entries, ids strictly increasing, and the evicted
  counter at 1.
- **`RESET` clears entries and preserves counters.** Directly asserting §3.6.
- **The estimator on a known distribution**, as §7 gate 4 — as a test with a fixed
  seed, not only as a benchmark, so it runs in CI.
- **A `Store` fake drives a slow request.** [m10.md](m10.md) built the seam that
  lets a server test run against a fake store; a fake that sleeps is how
  `queue_us` and `exec_us` get asserted separately without a real stall.
- **Allow-list the JSON fields**, as [stats-subcommands.md](stats-subcommands.md)
  §14's test does for `stats`, so a field cannot be added to an operator-facing
  surface without review. That test "earned itself on its first run"; this surface
  has the same failure mode.
