# Introspection: memcached `stats`, Redis `SCAN` and `INFO`

**Implemented.** This was the design proposal; it is kept as the record of what
was decided and why. Where building it changed the design, the text says so and
names what the real servers turned out to do — every framing claim below was
checked byte for byte against `memcached:1.6-alpine` and `redis:7-alpine`, and
the resulting behaviour is driven end to end by `redis-py`, `pymemcache` and the
suites in `crates/vash-server/tests/`.

Five things moved between the proposal and the build, all of them found by
probing the real servers:

| Found | Consequence |
|---|---|
| A dump answers `OK\r\n` **before** any key | Reproduced. A client reading it as a data line would lose one. |
| `metadump` lines end in a **bare `\n` with a trailing space**; `mgdump` lines end in CRLF | Reproduced exactly. |
| `metadump` percent-encodes `%` and `mgdump` does **not** | Upstream's own asymmetry, reproduced — an `mg` line has to name the key it will look up. |
| A missing dump class is a bare `ERROR`, not a client error | Matched. |
| `vash_expiry_entries` counts index rows, not keys-with-a-TTL | `INFO`'s `db0` line **omits `expires`** rather than reporting a different quantity. |

---

The original proposal follows.

What clients and operator tooling reach for and this server answers badly or not
at all:

| Dialect | Command | Today | Proposed |
|---|---|---|---|
| memcached | `stats` | A flat pair list, thin: six upstream counters plus `vash_*`. | Same command, more measured counters. |
| memcached | `stats <sub>` | Silently answered as plain `stats`. | `CLIENT_ERROR bad command line format`. |
| memcached | `lru_crawler metadump` | `ERROR` | The key listing for this dialect. |
| memcached | `lru_crawler mgdump` | `ERROR` | The same walk, keys only. |
| Redis | `SCAN` | `-ERR unknown command 'SCAN'` | Implemented. |
| Redis | `INFO` | `-ERR unknown command 'INFO'` | Implemented. |

---

## 1. The governing rule

**The engine gains no primitive for either compatibility dialect.** memcached
and Redis are surfaces over the same store, and a store shaped around them would
be a store shaped around two protocols it does not speak. Everything below is
built out of primitives M8 and M4 already shipped:

| Feature | Existing primitive |
|---|---|
| `SCAN` | `Store::list_keys(&ListRequest, max_scan) -> Listing` |
| `lru_crawler metadump`, `mgdump` | the same, called in a loop until the page cursor runs out |
| `stats` | `Store::stats() -> StoreStats` + `ServerMetrics` + `Cluster::view()` |
| `INFO` | the same three |
| gating for `SCAN` and both dumps | `protocol.listing_enabled`, via `dispatch::listing_gate` |
| scan budget | `protocol.listing_max_scan` |
| pattern matching | `vash_core::glob` |

The domain boundary gains **no new `Command` variant**. `SCAN` and `lru_crawler
metadump` travel as `Command::ListKeys`; `INFO` and `stats` travel as
`Command::Stats`. Both therefore go through `dispatch::execute` and are counted,
gated and error-classified by the code that already does it for the other two
dialects — which is the whole point of M10's single boundary.

One domain type does change, by a single optional field, and §6 argues for it
separately because it is the only storage-crate edit in the proposal.

---

## 2. Where the code goes

| File | Change |
|---|---|
| `vash-proto/src/memcached/text.rs` | Reject a `stats` subcommand; parse `lru_crawler`. |
| `vash-proto/src/memcached/encode.rs` | `ResponseStyle::MetaDump`; the `key=…` line writer and its URL encoder. |
| `vash-proto/src/resp/command.rs` | `Command::Scan`, `Command::Info` + their parsers. |
| `vash-proto/src/resp/encode.rs` | `info()` renderer + `verbatim()`; the section table. |
| `vash-server/src/resp.rs` | Two arms in `run()`; `inline_safe` entries. |
| `vash-server/src/scan.rs` | **New.** The SCAN cursor table (~90 lines). |
| `vash-server/src/stats.rs` | **New.** `collect_stats` moves here and grows (~150 lines). |
| `vash-server/src/dispatch.rs` | `collect_stats` moves out; the metadump paging loop moves in. |
| `vash-server/src/metrics.rs` | One accessor on `CommandMetrics` (~6 lines). |
| `vash-server/src/state.rs` | `started: Instant`, `scan_cursors: ScanCursors`. |
| `vash-server/src/config.rs` | Two settings under `[protocol]`. |
| `vash-core/src/listing.rs` | Two optional fields on `ListEntry` (§6). |
| `vash-store/src/listing.rs`, `memory.rs` | Fill them. Two lines each. |

`vash-server/src/resp.rs` keeps its stated invariant — `state.store` does not
appear in it — because both new commands reach storage through
`dispatch::execute` like every other one.

---

## 3. memcached

Two commands, and the division of labour between them is upstream's own:
**`stats` counts, `lru_crawler` lists.** Every `stats` subcommand is refused.

### 3.1 `stats` — the general counters

Keep the existing rule verbatim: **nothing is reported that is not measured.** A
counter that is always zero because the feature does not exist reads as healthy
silence, which is worse than an absent field.

What is measured today and not yet reported:

| memcached name | Source |
|---|---|
| `uptime` | `ServerState::started.elapsed()` — new field, one `Instant`. |
| `time` | `Clock::now_ms() / 1000`. |
| `curr_connections` | `metrics.connections_active` |
| `total_connections` | `metrics.connections_total` |
| `rejected_connections` | `metrics.connections_rejected` |
| `cmd_get` | `commands.total(Get) + total(GetMany) + total(GetAndTouch)` |
| `cmd_set` | `commands.total(Set) + total(SetMany)` |
| `cmd_touch` | `commands.total(Touch)` |
| `cmd_flush` | `commands.total(Flush)` |
| `get_hits` | `metrics.hits` — per key, which is memcached's own definition. |
| `get_misses` | `metrics.misses` |
| `evictions` | `StoreStats::evicted` |

`CommandMetrics` already holds a counter per `(CommandKind, Dialect)`. It needs
one reader:

```rust
/// This command's count across every dialect. `stats` and `INFO` report the
/// server, not the port the request arrived on.
pub fn total(&self, kind: CommandKind) -> u64 {
    Dialect::ALL
        .iter()
        .map(|d| self.counts[kind.index() * Dialect::ALL.len() + *d as usize].load(Relaxed))
        .sum()
}
```

**Deliberately still absent**, because nothing measures them: `threads`,
`bytes_read`, `bytes_written`, `total_items`, `delete_hits`/`delete_misses`
(deletes are not split into hit and miss), `expired_unfetched` and
`evicted_unfetched` ("unfetched" is not tracked at all), `curr_items` per slab
class, `reclaimed`. `vash_reclaimed` already reports the honest neighbour of the
last one.

### 3.2 Every `stats` subcommand is refused

```text
stats items\r\n      → CLIENT_ERROR bad command line format
stats slabs\r\n      → CLIENT_ERROR bad command line format
stats cachedump 1 0  → CLIENT_ERROR bad command line format
stats reset\r\n      → CLIENT_ERROR bad command line format
```

One rule, no table of special cases: **`stats` takes no arguments here.** The
parser already has `BAD_LINE` as its constant for a command line it cannot
honour, and this is one.

The alternative — implementing a few and refusing the rest — buys a per-
subcommand argument every time one comes up, and the two that would be worth
having are worth having for bad reasons:

- `stats items` exists upstream so tooling can discover slab class ids to feed
  `cachedump`. There are no slab classes here, so it would report a synthetic
  one whose only purpose is to be passed to a command this proposal does not
  implement either.
- `stats cachedump` is upstream's *old* key dump. It is capped at 2 MB, walks
  one class's LRU head, and its own documentation calls it debug-only and
  warns it may be removed. `lru_crawler metadump` replaced it, and that is what
  current tooling drives.

**`stats reset` in particular must not be implemented**, and would have been
refused even under the previous plan: the counters behind it are the same
atomics `/metrics` exports, and a Prometheus counter that goes backwards
corrupts every rate over the window containing the reset. Answering `RESET` and
quietly doing nothing is the only other option, and it is a lie.

This is a divergence from upstream, which implements all of them. It belongs in
`protocol.md`'s divergences table.

### 3.3 `lru_crawler metadump` and `mgdump` — listing keys

The listing commands of this dialect. **One walk, two line formats**, which is
the whole reason to implement both: everything below the rendering is shared.

```text
lru_crawler metadump all\r\n
→ key=session%3A0001 exp=1755043200 cas=41 cls=1
  key=session%3A0002 exp=-1 cas=57 cls=1
  END

lru_crawler mgdump all\r\n
→ mg session%3A0001
  mg session%3A0002
  EN
```

| | `metadump` | `mgdump` |
|---|---|---|
| Line | `key=… exp=… cas=… cls=1` | `mg <key>` |
| Terminator | `END` | `EN` |
| Needs §6's `ListEntry` field | yes, for `exp` | **no** — the name is all it prints |
| For | a human or an exporter | piping straight back in as meta commands |

`mgdump` emits a **ready-to-send `mg` command line**, which is its point
upstream: the dump is its own replay script. It also means `mgdump` is the
cheaper of the two here — it needs nothing from a record beyond its key, so it
would work today without §6.

⚠ `EN` versus `END` is the one byte-level detail to confirm against
`memcached:1.6-alpine` in `tests/compat/docker_differential.py` while
implementing. `EN` is the meta protocol's end token and is what makes the dump
pipeable, but it is a one-byte difference on the line every parser keys off.

**Class ids.** This store has no slab classes, so **everything lives in class
1**, and both commands take the same argument: `all`, `hash` and `1` are three
spellings of one dump. The rule is "the class ids this server reports, plus
`all` and `hash`", not a hard-coded list, so `cls=1` in the metadump line and
the accepted argument cannot drift apart.

Any other argument — `2`…`63`, or anything unrecognised — answers a bare
terminator, because the class it names is genuinely empty. A *missing* argument
is `CLIENT_ERROR bad command line format`, the same arity rule the rest of the
parser applies.

**Every other `lru_crawler` subcommand is refused**, named rather than lumped
into `ERROR`:

```text
lru_crawler enable      → CLIENT_ERROR lru_crawler enable is not implemented
lru_crawler crawl 1     → CLIENT_ERROR lru_crawler crawl is not implemented
lru_crawler sleep 1000  → CLIENT_ERROR lru_crawler sleep is not implemented
lru_crawler tocrawl 100 → CLIENT_ERROR lru_crawler tocrawl is not implemented
```

They all steer a background LRU crawler, and there is no LRU here to crawl —
plan §6 rejected an on-disk one. Upstream answers a bare `ERROR` for a
subcommand it does not recognise, but it *does* recognise these, so the bytes
diverge whatever we send; given that, saying which command was refused and why
is worth more than a shorter divergence. This is the same call `SET … IFEQ`
already makes on the Redis side.

#### Execution: paging a command that has no cursor

Both stream the **whole** keyspace and then terminate. There is no cursor in
either grammar, so resumption has to happen server-side:

```rust
let mut cursor: Option<Box<[u8]>> = None;
loop {
    let request = ListRequest { limit: MAX_LIST_LIMIT, cursor: cursor.as_deref().unwrap_or(&[]), pattern: &[] };
    let Reply::Listing(page) = execute(state, &Command::ListKeys(request), Dialect::Memcached)? else { … };
    for entry in &page.entries { mc::dump_line(out, style, entry); }
    scanned += page.scanned;
    match page.cursor { Some(next) if scanned < budget => cursor = Some(next), _ => break }
}
```

Each iteration opens and closes its own read transaction, so nothing pins a
snapshot for the length of the dump — the footgun plan §9 exists to avoid and
the reason `LIST_KEYS` pages at all. The loop lives in `dispatch.rs` beside the
other execution paths and calls `execute` each time, so gating, counting and
error classification stay on the shared boundary; only the line writer is in
`memcached::encode`.

A dump therefore counts as N listings rather than one. That is honest — it is N
scans — and it is worth a sentence in `operations.md` so a spike in
`vash_command_total{command="listing"}` is readable.

**Bounding.** One dump gets one `LIST_KEYS` call's budget —
`protocol.listing_max_scan`, default 100 000 records — spent across as many
internal pages as it needs. A cache smaller than that dumps completely.

When the budget runs out first, the dump ends with

```text
SERVER_ERROR metadump exceeded the scan budget after 100000 records; use SCAN or LIST_KEYS to page the keyspace
```

**in place of the terminator, and that substitution is the whole point.** A tool
reads lines until `END` (or `EN`); ending a truncated dump with one would tell it
the keyspace is smaller than it is, which is exactly the silent wrongness this
project refuses elsewhere. An error line where the terminator should be cannot
be mistaken for a complete answer.

**Consistency.** A dump is a sequence of pages, not a snapshot — the same
statement `docs/opcodes.md` already makes for `LIST_KEYS`. Because resumption is
by key, a key present unchanged for the whole walk appears exactly once.

#### The metadump line

`mgdump`'s line is `mg ` plus the encoded key and nothing else, so only the
URL-encoding paragraph below applies to it.

Upstream's `doc/protocol.txt` guarantees only that the keys are "subject to
change, but will include at least" `key`, `exp`, `la`, `cas` and `fetch`. That
sentence is the whole specification, and it is what the table below is measured
against — not against whatever a particular build happens to print.

| Field | Value | Note |
|---|---|---|
| `key=` | URL-encoded key | Guaranteed. See below. |
| `exp=` | absolute unix seconds, `-1` for never | Guaranteed. From the new `ListEntry::expires_at_ms` (§6). Matches the `-1` convention `Value::remaining_ttl_secs` already uses. |
| `cas=` | `ListEntry::version` | Guaranteed, and already carried. |
| `cls=` | always `1` | Not guaranteed, but a constant: it costs no per-entry storage, and it is what a reader keys on to know which class it is looking at. |

**The key is URL-encoded in both dumps, and that is load-bearing rather than
cosmetic.** `main`
is shared across dialects, so a Redis or VCP client can store a key containing a
space, a control byte or a CRLF — none of which the memcached parser could have
produced, and any of which would corrupt a line and desynchronise the reader.
Upstream encodes for its own binary-protocol reasons; here it turns a
cross-dialect framing hazard into a non-issue, with no keys skipped and no
escape scheme nobody parses. Percent-encode anything outside
`0x21..=0x7e` plus `%` itself.

For `mgdump` it is doubly load-bearing: the output is meant to be replayed as
commands, and an unencoded space in a key would replay as an `mg` with a flag
argument rather than as a lookup of that key.

**`la=`, `fetch=` and `size=` are omitted, not zeroed**, for two different
reasons that land in the same place.

`la` and `fetch` — last-access time and the fetched-since-stored bit — are
guaranteed by upstream's wording and are still not emitted, because both are LRU
bookkeeping and there is no LRU here (plan §6). A `la=0` would claim every key
was last touched at the epoch. The standing rule: no plausible-looking zero for
something that is not measured.

`size` is the opposite case — it is measurable, and free at the scan, and it is
*not* in the guaranteed set at all. It was in an earlier draft of this proposal
and is dropped, because carrying it means carrying a `value_len` on `ListEntry`
(§6) that every VCP listing pays for and never reads. Three arguments, and the
first is the one that decides it:

- **We already fail the guaranteed set.** `la` and `fetch` are absent by
  necessity. A reader strict enough to need `size` is a reader already broken by
  those two; a reader tolerant enough to cope with them does not need `size`
  either. Adding an unspecified field while omitting specified ones is not a
  compatibility gain, it is incoherence.
- **It would have needed a divergence note of its own.** Upstream's `size` is
  `ITEM_ntotal` — header, key and value together — and ours would have been the
  value length alone. A field nobody promised, reported in a unit nobody
  expects.
- **The two-step answer already exists**, and `docs/opcodes.md` already argues
  for it: list, then ask about what you care about. `mg <key> s` returns a size
  per key, and `mgdump` emits `mg` lines that take exactly that flag. The dump
  says which keys exist; the meta protocol says how big one is.

A page of a dump was never going to answer "which of my keys are big" across a
keyspace anyway.

---

## 4. Redis `SCAN`

`SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`

### 4.1 The cursor is the whole design problem

vash's listing cursor is opaque bytes — `shard u16 ‖ last key`, up to 513 of
them. Redis's is a `u64` rendered as a decimal string, and while the Redis docs
call it opaque, **the major clients do not treat it as opaque**: `redis-py` does
`int(cursor)`, `go-redis` types it `uint64`, `StackExchange.Redis` parses a
`long`. Handing them 513 bytes of key breaks iteration in the client library,
not in the server.

A `u64` cannot hold a key. Two escape routes exist and both are worse:

- **Derive the token from the key** (say, its first eight bytes). Keys sharing a
  prefix then either get skipped or repeat forever. A pager that loops is worse
  than one that errors.
- **Use an offset.** This is exactly the quadratic resumption `docs/opcodes.md`
  rejected for `LIST_KEYS`, having already met it once in `tagidx`. Not twice.

**Decision: a server-side token table.** The client gets an integer; the server
holds the bytes.

### 4.2 The table

```rust
// vash-server/src/scan.rs
pub struct ScanCursors { inner: Mutex<Inner> }
struct Inner { next: u64, live: VecDeque<Entry> }
struct Entry { token: u64, cursor: Box<[u8]>, issued: Instant }
```

- **Server-wide, not per connection.** A pooled client returns the connection
  between iterations, so page 2 routinely lands on a different socket. Scoping
  the table to a connection would break `redis-py`'s `scan_iter` under a pool,
  which is how nearly every real caller drives this.
- **Token 0 is reserved** for "start", which is Redis's contract. Issued tokens
  begin at 1.
- **Lookup does not consume.** A client that times out and retries the same
  token must get the same page.
- **Miss is an error, never a silent restart.** `-ERR scan cursor expired;
  restart the iteration from 0`. The same reasoning as `listing::decode`
  refusing a fabricated cursor: a pager that silently restarts loops forever and
  never says why.

Lookup is a linear walk of at most 1024 `u64` comparisons on a command that is
by construction administrative. It does not need to be a `HashMap`.

#### When entries go away

An entry is removed by exactly two things, and **neither is a background task**:

| Trigger | Rule |
|---|---|
| **Capacity** | `issue` pushes to the back, then drops from the front while the deque exceeds `protocol.scan_cursors` (default 1024). |
| **Age** | `issue` also drops from the front while the oldest entry is older than `protocol.scan_cursor_ttl_ms` (default 60 000). |

Both sweeps run on `issue`, which is the only operation that can grow the
table — so the table does work only when somebody is scanning, and a server
nobody scans holds an empty `VecDeque` and a `Mutex`.

**The TTL is enforced at `resolve`, not only by the sweep.** A token older than
the TTL is refused even if nothing has swept it yet:

```rust
match self.find(token) {
    Some(entry) if entry.issued.elapsed() < self.ttl => Some(entry.cursor.clone()),
    _ => None,   // expired or never existed — the same error either way
}
```

That separation is the point: **the sweep is a memory bound, the check at
`resolve` is the contract.** If eviction were the only enforcement, how long a
cursor stayed valid would depend on how busy the server happened to be — a
client's iteration would succeed or fail based on other clients' traffic, which
is the kind of behaviour that only shows up in production.

**FIFO by issue order is safe for a live iteration.** The token a client needs is
always the one it was handed most recently, which is at the *back*; eviction
takes from the front. Two concurrent full scans interleave their tokens, and
each evicts only the other's already-spent ones. The only way to lose a live
token is to pause longer than the TTL, or longer than 1024 other tokens' worth
of traffic — which is the same failure the TTL already describes.

**Nothing is cleared on connection close**, deliberately: the next page of a
pooled client's scan arrives on a different socket, and clearing on close would
break precisely the case this table is shared for. An abandoned iteration costs
one slot until age or capacity reclaims it.

**Everything is lost on restart.** The tokens live in this process and nowhere
else, so a `SCAN` cannot survive a restart the way a VCP `LIST_KEYS` can — the
underlying position is still valid *as bytes*, but the mapping from the integer
the client holds is gone. The next call answers `scan cursor expired`, which is
the right answer, and it is worth stating in `protocol.md` beside the note that a
`LIST_KEYS` cursor does survive.

Worst case is `scan_cursors × (513 + overhead)` ≈ **0.5 MB**, reached only while
1024 iterations are genuinely in flight.

### 4.3 Options

| Option | Mapping |
|---|---|
| `COUNT n` | → `ListRequest.limit`, default 10 (Redis's), **clamped** to 1024. |
| `MATCH p` | → `ListRequest.pattern`. |
| `TYPE t` | `string` scans normally; anything else answers `["0", []]` immediately, which is true — no key here is another type. |

**`COUNT` is clamped where VCP's `limit` is rejected**, and the asymmetry is
deliberate. `docs/opcodes.md` rejects an over-limit because a VCP client that
asked for 10000 and got 1024 would page incorrectly. Redis specifies `COUNT` as
a hint the server may ignore, clients pass 10000 freely, and the reply's cursor
is what drives the loop — so clamping is both legal and invisible.

**`MATCH` and character classes.** `vash_core::glob` is `*`, `?` and `\` and
deliberately nothing else; Redis's `stringmatchlen` also has `[a-z]` and
`[^x]`. Treating `[` as a literal byte would make `MATCH k[0-9]*` silently match
nothing. **Reject an unescaped `[` with `-ERR character classes are not
supported in MATCH`** — the same move `SET … IFEQ` already makes, naming the
real reason rather than hiding behind `syntax error`.

### 4.4 Rendering and the contract that comes free

```text
*2
$3
417            ← next token, or "0" when Listing::cursor is None
*2
$12
session:0001
$12
session:0002
```

`Listing::cursor == None` is the termination rule on both sides, so it maps
onto Redis's `"0"` exactly.

**An empty page with a non-zero cursor is legal in Redis and clients handle
it** — which is precisely what `listing_max_scan` exhaustion produces when a
page's worth of budget lands entirely on dead or non-matching records. The
budget and the protocol agree without either being bent.

Redis's guarantee is that a key present for the whole iteration is returned at
least once. `LIST_KEYS` resumes by key rather than by count, so vash returns it
**exactly** once — stronger than the contract, and worth stating.

### 4.5 Gating, and where SCAN may run

`Command::ListKeys` already passes `dispatch::listing_gate`, so `SCAN` is behind
`protocol.listing_enabled` with no new code, and inherits its `Status::
Unauthorized` → `-ERR command disabled by configuration` rendering.

That means **`SCAN` is off by default**, which will surprise a Redis user and is
the right default anyway: enumerating a keyspace is the same capability whatever
dialect asks for it. It belongs in the README's Redis section, not only here.

`resp::inline_safe(&Command::Scan { .. })` must be `false` — a scan is bounded by
`listing_max_scan` records, not by anything a runtime worker should be blocked
for. `Command::ListKeys` already answers `false`, and the existing
`the_shortcut_agrees_with_the_domain` test is what keeps the two from drifting.
`SCAN` and `INFO` get their own arms in `run()` rather than going through
`translate()` (the cursor has to be resolved first), so add them to that test's
list with a directly asserted expectation instead of leaving them uncovered.

---

## 5. Redis `INFO`

`INFO [section [section …]]`. A bulk string in RESP2; a **verbatim string**
(`=<len>\r\ntxt:…`) in RESP3, which is what real Redis sends and what
`encode.rs` is already structured for — `null` and `double` take the same
`Version` parameter for the same reason.

### 5.1 Sections

Default (no argument) is everything except `vash`. `all` and `everything` add
it. Named sections are case-insensitive, may repeat, and an unknown one
contributes nothing — Redis answers an empty string rather than an error.

```text
# Server
redis_version:7.4.0-vash        ← the RESP VERSION constant, not memcached's
redis_mode:standalone
os:linux x86_64
arch_bits:64
process_id:4711
uptime_in_seconds:8123
uptime_in_days:0
vash_version:0.1.0              ← CARGO_PKG_VERSION: what this actually is

# Clients
connected_clients:12
maxclients:10000                ← server.max_connections
blocked_clients:0               ← measured, and always 0: nothing blocks a client

# Memory
used_memory:104857600
used_memory_human:100.00M
maxmemory:1073741824            ← StoreStats::map_size
maxmemory_policy:volatile-ttl

# Persistence
loading:0
rdb_bgsave_in_progress:0
aof_enabled:0

# Stats
total_connections_received:918
total_commands_processed:41022
rejected_connections:0
keyspace_hits:39001
keyspace_misses:2021
expired_keys:1204               ← StoreStats::reclaimed
evicted_keys:0
total_reads_processed:33110
total_writes_processed:7912

# Replication
role:master
connected_slaves:0

# Cluster
cluster_enabled:0

# Keyspace
db0:keys=48120,expires=47990,avg_ttl=0

# Vash
vash_shards:4
vash_utilisation:0.6210
vash_epoch:1
vash_tags:88
vash_tag_index_entries:12004
vash_pending_reclaims:0
vash_sweeps:9120
vash_reclaimed:1204
vash_tag_reclaimed:44
vash_sweep_lag_ms:12
vash_commits:8801
vash_committed_ops:41002
vash_mean_batch:4.66
vash_readers_in_use:2
vash_oldest_reader_age_ms:0
vash_cluster_mode:fanout
vash_cluster_peers:2
vash_cluster_peers_reachable:2
```

### 5.2 Three fields that are load-bearing

- **`cluster_enabled:0`.** Client libraries read this to decide whether to speak
  Redis Cluster — `CLUSTER SLOTS`, `MOVED`/`ASK` redirection, hash-slot routing,
  none of which exists here. vash's clustering is tag invalidation between
  shared-nothing nodes and is not the same thing under the same name. Reporting
  `1` would break every cluster-aware client on connect.
- **`role:master`.** Sentinel-aware clients and health checks parse it. There is
  no replication, so `master` with `connected_slaves:0` is true.
- **`maxmemory_policy:volatile-ttl`.** The closest true statement in Redis's
  vocabulary for "expired first, then soonest-to-expire" (plan §6). It is an
  approximation and belongs in the divergences table.

**Deliberately absent**, all unmeasured: `used_memory_rss`,
`mem_fragmentation_ratio`, `instantaneous_ops_per_sec`, `total_net_input_bytes`,
`latest_fork_usec`, `commandstats`, `latencystats`, `pubsub_channels`,
`rdb_last_save_time`.

### 5.3 How INFO is assembled without a second source of truth

`collect_stats` moves to `vash-server/src/stats.rs` and becomes the **superset**:
every counter, under one stable set of names, still returned as
`Reply::Stats(Vec<(String, String)>)`. `Command::Stats` and its reply type do not
change.

Each dialect then renders from that one list:

- memcached prints every pair as a `STAT` line, as it does today.
- `INFO` walks a static table:

```rust
const INFO_FIELDS: &[(Section, &str, Source)] = &[
    (Section::Server,   "redis_version", Source::Literal(encode::VERSION)),
    (Section::Server,   "uptime_in_seconds", Source::Stat("uptime")),
    (Section::Stats,    "keyspace_hits", Source::Stat("get_hits")),
    // …
];
```

`Source` is `Stat(name)`, `Literal(text)`, or one of the four computed fields
(`used_memory_human`, `uptime_in_days`, `db0:…`, `os`). The renderer is a pure
function of the pair list, so it lives in `vash-proto/src/resp/encode.rs`,
touches no state, and is trivially testable.

The lookup is linear over ~45 pairs per field. On a command nobody calls in a
loop, a `HashMap` would be machinery bought with nothing.

---

## 6. The one storage-crate change: `ListEntry` gains an expiry

`lru_crawler metadump` needs `exp=` per key, and `exp` is one of the fields
upstream guarantees. It is not in `ListEntry`, and `docs/opcodes.md` excluded a
key's TTL from the VCP listing on purpose.

**This is the only step `mgdump` does not need**, which is worth knowing if the
work is ever split: `mgdump` prints names, so it can ship before this section
does. `value_len` was in an earlier draft, for a `size=` field, and is gone —
see §3.3.

The alternative is real, not a straw man: `Store::deadlines` takes a batch of
keys and returns their deadlines against one snapshot, without copying a single
value. Calling it once per page would work. It costs:

- **A second B-tree descent per key**, on keys the scan just walked past. A
  100 000-key dump is 100 000 extra point lookups, roughly doubling the dump.
- **A second snapshot.** The page and its deadlines come from two read
  transactions, so a key can expire between them and be printed with a deadline
  that no longer describes it.

The scan **already has the number in hand**: `engine::list_keys` parses every
record for the liveness check, and `RecordRef::expires_at_ms()` is a field read
on a header that is already in registers. Zero descents, one snapshot.

```rust
pub struct ListEntry {
    pub name: Box<[u8]>,
    pub version: u64,
    /// Absolute expiry in unix milliseconds, or `NEVER`.
    ///
    /// `None` for a listing whose entries are not records — `LIST_TAGS` — which
    /// is why this is an `Option` and not a bare `NEVER`: a tag has no
    /// expiry at all, and that is a different statement from "never expires".
    pub expires_at_ms: Option<u64>,
}
```

`ListEntry::new(name, version)` stays and leaves it `None`, so `LIST_TAGS`, the
VCP decoder, `vash-client` and every test constructing an entry are untouched. A
second constructor fills it at the one site that can.

**Nothing on the wire changes.** `vcp::encode` writes name and version field by
field and simply does not write the new one, so no client sees a difference and
`MAX_LIST_CURSOR_LEN` and the response layout are unaffected. Cost is 16 bytes
per entry — at most 16 KB on a full 1024-entry page, and only while that page is
in flight.

Sites to touch: `vash-core/src/listing.rs` (the field), `vash-store/src/
listing.rs:152` and `vash-store/src/memory.rs:500` (fill it). Three files.

⚠ A VCP listing round-trip test that compares a store-produced `Listing` against
a decoded one will now see `Some(..)` on one side and `None` on the other.
Compare names and versions there, or normalise before comparing.

---

## 7. Configuration

```toml
[protocol]
# Already exists. Now also gates Redis SCAN and `lru_crawler metadump`, and
# caps how much one metadump may walk before it gives up (§3.3).
listing_enabled = false
listing_max_scan = 100000

# New. Live SCAN cursor tokens held server-wide, and how long an idle one
# survives. Exceeding either answers "-ERR scan cursor expired".
scan_cursors = 1024
scan_cursor_ttl_ms = 60000
```

Both validate as `> 0` alongside `listing_max_scan` in `Config::validate`.

No new capability bit: `LISTING` already advertises exactly this, and a Redis or
memcached client cannot read the VCP handshake anyway.

---

## 8. Errors

| Reply | When |
|---|---|
| `-ERR command disabled by configuration` | `SCAN` with `listing_enabled` clear. Free, via `listing_gate`. |
| `-ERR invalid cursor` | A cursor argument that is not a non-negative integer. Redis's own wording. |
| `-ERR scan cursor expired; restart the iteration from 0` | A token the table no longer holds. |
| `-ERR character classes are not supported in MATCH` | An unescaped `[` in the pattern. |
| `-ERR syntax error` | Unknown option, `COUNT <= 0`, repeated option. |
| `-ERR wrong number of arguments for 'scan' command` | No cursor. |
| `CLIENT_ERROR bad command line format` | Any argument to `stats`; a dump with no class argument. |
| `CLIENT_ERROR lru_crawler <sub> is not implemented` | Any `lru_crawler` subcommand other than `metadump` and `mgdump`. |
| `CLIENT_ERROR command disabled by configuration` | Either dump with `listing_enabled` clear. Free, via `listing_gate`. |
| `SERVER_ERROR metadump exceeded the scan budget …` | In place of the terminator on a truncated dump. §3.3. |
| `END` / `EN` with no lines | A dump of a class other than `all`, `hash` or `1`. |
| `ERROR` | An unknown verb, unchanged. |

---

## 9. Testing

**Unit.** The cursor table — issue, resolve, resolve-twice, capacity eviction,
TTL eviction, and **a resolve past the TTL with no sweep having run**, which is
the case that separates the contract from the memory bound; the `INFO` section
filter including `all`/`everything`/unknown;
the `[` rejection; `COUNT` clamping; the metadump URL encoder, including a key
holding a space, a CRLF and a `%`.

**Integration** (`vash-server/tests/redis.rs`, `tests/memcached.rs`):

- Write N keys, walk them with `SCAN` at `COUNT 10`, assert each is returned
  **exactly once** and the walk terminates at `"0"`.
- Same walk while a concurrent writer inserts: no key present throughout is
  missed.
- `SCAN` with `listing_enabled` off is refused.
- `SCAN` returning an empty page with a non-zero cursor is not the end.
- `MATCH` filters; `TYPE hash` answers `["0", []]`.
- An expired token errors rather than restarting.
- `stats` shape; `stats items` and every other subcommand are refused.
- `lru_crawler metadump all` over N keys returns each exactly once and ends in
  `END`; `1` and `hash` return the same set; `2` returns a bare `END`; every
  other `lru_crawler` subcommand is refused.
- `lru_crawler mgdump all` returns the **same key set** as `metadump all` —
  asserted against each other, since one walk feeding two renderers is the claim
  worth pinning — and ends in `EN`.
- Every line of an `mgdump` is re-sendable: replay the dump into the server and
  every `mg` hits.
- A key written over Redis containing a space and a CRLF round-trips through a
  metadump **without breaking line framing**, and decodes back to the original
  bytes.
- A metadump over a keyspace larger than `listing_max_scan` ends in
  `SERVER_ERROR`, **not** `END`. (Set the budget low in the test rather than
  writing 100 000 keys.)
- `INFO` parses under `redis-py`'s `parse_info`, and `cluster_enabled` is `0`.

**Differential.** `tests/compat/docker_differential.py` compares bytes, and
these commands' *values* differ from upstream by construction. Compare the
**shape** instead: field names present, line format, section headers,
`ITEM`/`STAT`/`END` framing. That is the part a client parses.

**Fuzz.** `SCAN`'s arguments ride the existing RESP target for free. The cursor
token is a `u64` parse and the pattern already goes through `glob::validate`.

---

## 10. Deliberately not built

| Command | Why |
|---|---|
| `KEYS pattern` | An unbounded scan with no cursor. `listing_max_scan` exists precisely so no request can hold a read transaction open across the whole keyspace, and `KEYS` cannot be expressed without breaking it. `SCAN` is the answer, and it is the answer in Redis too. |
| `RANDOMKEY` | Needs a uniform random position in a B-tree. No cheap correct implementation. |
| `HSCAN` / `SSCAN` / `ZSCAN` | There are no hashes, sets or sorted sets. |
| `SCAN … NOVALUES` | A `HSCAN` option; not applicable. |
| `DBSIZE` | Three lines off `StoreStats::entries` and genuinely useful — but out of the scope asked for. Named here so it is a decision rather than an omission. |
| `COMMAND` / `COMMAND DOCS` | Some clients probe it on connect and all of them tolerate the error. A full command table is a large static structure that has to stay true. |
| `CLIENT LIST` / `CLIENT INFO` | There is no client registry, which is also why `HELLO … SETNAME` is refused and `id` is `0`. |
| Every `stats` subcommand | §3.2. |
| `stats cachedump` | Upstream's own docs call it debug-only and warn it may be removed; `lru_crawler metadump` replaced it and is what current tooling drives. |
| `lru_crawler` other than the two dumps | `enable`, `disable`, `sleep`, `tocrawl`, `crawl` all steer a background LRU crawler. Plan §6 rejected an on-disk LRU, so there is nothing to enable, sleep, or crawl. |
| A `MATCH`-style filter on either dump | The listing takes a pattern and it would be nearly free — but it is not upstream grammar, and a vash-only argument on a memcached command is a trap for tooling that passes class ids positionally. `SCAN … MATCH` and `LIST_KEYS` both have it. |
| Persisting SCAN cursors across a restart | It would need a table on disk, an expiry policy for it, and a reason. §4.2. |

---

## 11. Work breakdown

| Step | Files | Rough size |
|---|---|---|
| 1. `ListEntry::expires_at_ms` | `vash-core`, `vash-store` ×2 | ~10 lines |
| 2. `stats.rs`: move + extend `collect_stats`, `CommandMetrics::total`, `ServerState::started` | `vash-server` ×4 | ~200 lines |
| 3. `stats` arity, `lru_crawler` parse, both dump lines + URL encoder, the paging loop | `vash-proto/memcached` ×2, `vash-server/dispatch.rs` | ~190 lines |
| 4. `scan.rs` cursor table | `vash-server` ×2 | ~110 lines |
| 5. `SCAN` parse, translate, render | `vash-proto/resp`, `vash-server/resp.rs` | ~180 lines |
| 6. `INFO` parse, section table, verbatim strings | `vash-proto/resp` ×2, `vash-server/resp.rs` | ~220 lines |
| 7. Config, docs (`protocol.md` ×3 sections, `README.md` ×2, `vash.example.toml`), tests | — | ~400 lines |

Steps 1–3 and 4–6 are independent of each other. Step 2 is a prerequisite for
both 3 and 6, and is the one worth doing first.
