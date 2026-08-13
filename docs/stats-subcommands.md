# The memcached `stats` subcommands

**Implemented.** This was the design proposal and is kept as the record of what
was decided and why. It **supersedes [introspection.md](introspection.md) §3.2**,
which argued for refusing every subcommand and shipped that way.

Every field table below was built from two sources and checked against both: the
upstream specification, and the bytes `memcached:1.6-alpine` (1.6.45) actually
sends.

Three subcommands stay refused — `reset`, `cachedump` and `detail` — and §9 says
why each.

Two things changed while building it, both for the same reason the rest of this
document gives:

- **`threads` and `num_threads` are not reported**, though §4 and §5 proposed
  mapping them to `store.max_blocking_threads`. Upstream's count the workers
  that serve *connections*; this pool does storage work while connections are
  served by the async runtime. Two pools, neither of them memcached's — so it is
  `vash_max_blocking_threads` instead. The rule that kept `reclaimed` out
  applies here too.
- **`stats items` reports no `mem_requested`**, as §6 anticipated, and gained
  `vash_reclaimed` / `vash_tag_reclaimed` so the two reclamation paths are
  visible under names that cannot be mistaken for upstream's.

The allow-list test §14 asked for is in place, and it earned itself on its first
run by catching a field name that had not been reviewed.

---

## 1. What "compatible" means here

The specification declines to specify these commands at all:

> The kinds of arguments and the data sent are not documented in this version of
> the protocol, and are subject to change for the convenience of memcache
> developers.

Two things follow, and they set the standard for everything below.

**The reference implementation is the contract**, not the document. So the
framing — `STAT <name> <value>\r\n` lines and an `END\r\n` terminator, the
`items:<class>:<field>` and `<id>:<field>` shapes — is matched byte for byte
against 1.6.45, exactly as the `lru_crawler` dumps already are.

**The field list is explicitly not a contract.** Upstream reserves the right to
change it release to release, and `stats settings` says so in its own output
("not guaranteed to return in any specific order and this list may not be
exhaustive"). A tool that reads these has to tolerate a field it does not
recognise and one it expected being gone — which is what makes it legitimate to
report a subset, and what makes the rule below affordable.

## 2. The rule that still holds

**Nothing is reported that is not measured.** A counter reading zero because the
feature does not exist is worse than an absent field: it looks like healthy
silence. This is why `metadump` has no `la=`, why `INFO` has no
`mem_fragmentation_ratio`, and why roughly two thirds of upstream's `stats`
fields do not appear below.

Two corollaries worth stating, because both bit during this design:

- **A name matching is not a meaning matching.** memcached's `reclaimed` is
  "number of times an entry was stored using memory from an expired entry" — a
  slab-reuse counter. This server's sweeper reclaim count is a different
  quantity that happens to share a word, so it stays `vash_reclaimed` and
  memcached's `reclaimed` is absent. Same reasoning already removed `expires`
  from `INFO`'s `db0` line.
- **A field that is genuinely, permanently zero is honest.** `udpport 0`,
  `proxy_enabled no` and `ssl_enabled no` are measurements of a decision, not
  placeholders.

---

## 3. At a glance

| Subcommand | Verdict | Needs building |
|---|---|---|
| `stats` | Extend the existing reply with ~16 fields | [P1](#p1-per-command-outcome-counters) |
| `stats settings` | Implement | [P2](#p2-the-listen-address) |
| `stats items` | Implement, one synthetic class | P1 |
| `stats slabs` | Implement, thin: the per-class command counters and the two totals | P1 |
| `stats conns` | Implement | P2, [P3](#p3-a-connection-registry) |
| `stats sizes` | Implement as `sizes_status disabled` — **byte-identical to a stock memcached** | nothing |
| `stats sizes_enable` / `sizes_disable` | `ERROR`, which is 1.6.45's answer | nothing |
| `stats extstore` | Bare `END`, which is 1.6.45's answer | nothing |
| `stats proxy` | Bare `END`, which is 1.6.45's answer | nothing |
| `stats reset` | **Refused by name** — §9 | nothing |
| `stats cachedump <class> <limit>` | **Refused by name** — §9 | nothing |
| `stats detail on\|off\|dump` | **Refused by name** — §9 | nothing |
| anything else | `ERROR`, which is upstream's answer | nothing |

Note the last group: **seven of the thirteen forms are answered correctly
without new state** — four because a stock memcached without the relevant
compile-time option answers exactly what this server would, and three because
they are refused.

---

## 4. `stats`

Unchanged in shape; the field list grows once [P1](#p1-per-command-outcome-counters)
lands. Fields already shipped are marked ✓.

### Reported

| Field | Source | |
|---|---|---|
| `pid` | `std::process::id()` | ✓ |
| `uptime` | `ServerState::started.elapsed()` | ✓ |
| `time` | `Clock::now_ms() / 1000` | ✓ |
| `version` | `encode::VERSION` | ✓ |
| `pointer_size` | `usize::BITS` | ✓ |
| `max_connections` | `server.max_connections` | ✓ |
| `curr_connections` | `connections_active` | ✓ |
| `total_connections` | `connections_total` | ✓ |
| `rejected_connections` | `connections_rejected` | ✓ |
| `accepting_conns` | `1` — the listener is never disabled; over the limit a connection is rejected, not queued | new |
| `threads` | `store.max_blocking_threads` — the ceiling on concurrent storage work, which is the number an operator tunes | new |
| `cmd_get` | `Get + GetMany + GetAndTouch` | ✓ |
| `cmd_set` | `Set + SetMany` | ✓ |
| `cmd_touch` | `Touch` | ✓ |
| `cmd_flush` | `Flush` | ✓ |
| `cmd_meta` | meta-dialect commands | new |
| `get_hits` | `hits` | ✓ |
| `get_misses` | `misses` | ✓ |
| `delete_hits` / `delete_misses` | P1 | new |
| `incr_hits` / `incr_misses` | P1 | new |
| `decr_hits` / `decr_misses` | P1 | new |
| `cas_hits` / `cas_misses` / `cas_badval` | P1 | new |
| `touch_hits` / `touch_misses` | P1 | new |
| `store_too_large` | P1 — writes refused with `TooLarge` | new |
| `store_no_memory` | `errors_capacity` | new |
| `total_items` | P1 — stores that actually applied | new |
| `auth_cmds` | `auth_ok + auth_failed` | new |
| `auth_errors` | `auth_failed` | new |
| `bytes_read` / `bytes_written` | P1 — two relaxed adds per syscall | new |
| `curr_items` | `StoreStats::entries` | ✓ |
| `bytes` | `used_bytes` | ✓ |
| `limit_maxbytes` | `map_size` | ✓ |
| `evictions` | `evicted` | ✓ |
| `vash_*` | the 22 already shipped | ✓ |

### Absent, and why

| Field | Why |
|---|---|
| `get_expired`, `get_flushed` | The read path folds expired, flushed and tag-invalidated into one miss — `is_alive` returns a bool, not a reason. Splitting them means the store reporting *why* a read missed, which is an engine change made for one counter. |
| `reclaimed` | Upstream's is slab reuse on store; ours is sweeper reclamation. Different quantity, same word. Reported as `vash_reclaimed`. |
| `expired_unfetched`, `evicted_unfetched`, `evicted_active` | "Unfetched" needs a per-item touched bit, which is LRU bookkeeping. |
| `rusage_user`, `rusage_system` | `getrusage` has no portable Windows equivalent, and this server runs there. |
| `libevent` | Not used. |
| `connection_structures`, `reserved_fds`, `conn_yields`, `listen_disabled_num`, `time_in_listen_disabled_us`, `idle_kicks` | Internals of an event loop this server does not have. |
| `response_obj_*`, `read_buf_*` | Upstream's response-object and read-buffer pools. Buffers here are per connection and sized by `server.read_buffer`, which `stats settings` reports. |
| `hash_power_level`, `hash_bytes`, `hash_is_expanding` | There is no hash table; the index is a B-tree. |
| `slab_*`, `slabs_moved`, `lru_*`, `moves_to_*`, `direct_reclaims`, `lrutail_reflocked`, `crawler_*` | Slab allocator and LRU. Plan §6 rejected an on-disk LRU. |
| `log_*`, `unexpected_napi_ids`, `round_robin_fallback`, `proxy_*` | Features not present. |

---

## 5. `stats settings`

`STAT <name> <value>` lines, `END`. This is the subcommand with the best fit:
nearly every field is a configuration value, and this server has configuration.

### Reported

| Field | Value |
|---|---|
| `maxbytes` | `map_size` — total across shards |
| `maxconns` | `server.max_connections` |
| `tcpport` | the listen port ([P2](#p2-the-listen-address)) |
| `udpport` | `0` — UDP is a standing non-goal (amplification vector, plan §16) |
| `inter` | the listen address |
| `verbosity` | `0` — `verbosity` is accepted and ignored, so it is never anything else |
| `evictions` | `on` — capacity pressure always evicts |
| `domain_socket` | `NULL` |
| `shutdown_command` | `no` |
| `num_threads` | `store.max_blocking_threads` |
| `cas_enabled` | `yes` |
| `auth_enabled_sasl` | `no` — SASL lives only in the binary protocol, which is a standing non-goal |
| `auth_enabled_ascii` | `auth.required` |
| `item_size_max` | `store.max_value_bytes` |
| `maxconns_fast` | `yes` — over the limit a connection is refused rather than starved |
| `flush_enabled` | `protocol.flush_enabled` — an exact match in meaning |
| `dump_enabled` | `protocol.listing_enabled` — likewise: upstream's `dump_enabled` gates its key dumps, ours gates every enumeration |
| `lru_crawler` | `protocol.listing_enabled` — the dumps are what a crawler is here |
| `lru_crawler_tocrawl` | `protocol.listing_max_scan` — "records one crawl may examine" is the same quantity |
| `lru_maintainer_thread` | `no` — there is no LRU to maintain |
| `temp_lru` | `no` |
| `track_sizes` | `no` — see §10 |
| `detail_enabled` | `no` — `stats detail` is not implemented (§9), so it is never anything else |
| `ssl_enabled` | `no` — no TLS in v1 (plan §16); a client that checks before sending a credential must not be told otherwise |
| `proxy_enabled` | `no` |
| `client_flags_size` | `4` — `mc_flags` is a `u32` |

### `vash_*` settings

The configuration that has no memcached name, and is what an operator actually
needs to see:

`vash_shards`, `vash_durability`, `vash_map_size_mb`, `vash_max_readers`,
`vash_read_buffer`, `vash_inline_reads`, `vash_max_tags`,
`vash_max_tags_per_record`, `vash_evict_soft`, `vash_evict_hard`,
`vash_evict_critical`, `vash_sweep_interval_ms`, `vash_sweep_batch`,
`vash_write_batch`, `vash_queue_depth`, `vash_scan_cursors`,
`vash_scan_cursor_ttl_ms`, `vash_memcached_enabled`, `vash_resp_enabled`,
`vash_cluster_mode`, `vash_cluster_peers`.

### Absent

`umask`, `growth_factor`, `chunk_size`, `reqs_per_event`, `tcp_backlog`,
`hashpower_init`, `hash_algorithm`, `slab_reassign`, `slab_automove*`,
`slab_chunk_max`, `hot_lru_pct`, `warm_lru_pct`, `hot_max_factor`,
`warm_max_factor`, `temporary_ttl`, `tail_repair_time`, `idle_timeout`,
`watcher_logbuf_size`, `worker_logbuf_size`, `read_buf_mem_limit`,
`inline_ascii_response`, `drop_privileges`, `stat_key_prefix`, `ext_*`, `ssl_*`
beyond `ssl_enabled`, `proxy_uring_enabled`, `num_napi_ids`, `memory_file`,
`binding_protocol`, `oldest`, `lru_crawler_sleep`.

Two deserve a word. **`binding_protocol`** would be `auto-negotiate`, which is
even true — the dialect is settled by the connection's first byte — but the
value names a *binary/ascii* choice this server does not offer, so it would be
answering a different question with the right word. **`oldest`** is the age of
the oldest item honoured after a `flush_all`; the flush epoch here is a
generation counter, not an age.

---

## 6. `stats items`

`STAT items:<class>:<field> <value>` lines, `END`.

**One synthetic class, id `1`** — the same constant `lru_crawler metadump`
prints as `cls=`, so the class a tool discovers here is the class the dumps
accept.

| Field | Source |
|---|---|
| `items:1:number` | `curr_items` |
| `items:1:evicted` | `evicted` |
| `items:1:outofmemory` | `errors_capacity` — stores refused for want of space, which is exactly upstream's meaning |
| `items:1:vash_expiry_entries` | rows in the expiry index |
| `items:1:vash_tag_index_entries` | rows in the tag index |
| `items:1:vash_pending_reclaims` | tag invalidations not yet swept |
| `items:1:vash_reclaimed` | records freed by the expiry sweeper |
| `items:1:vash_tag_reclaimed` | records freed by tag reclamation |

**Absent**: `number_hot`, `number_warm`, `number_cold`, `number_temp`,
`age_hot`, `age_warm`, `age`, `evicted_nonzero`, `evicted_time`, `tailrepairs`,
`reclaimed`, `expired_unfetched`, `evicted_unfetched`, `evicted_active`,
`crawler_reclaimed`, `lrutail_reflocked`, `moves_to_cold`, `moves_to_warm`,
`moves_within_lru`, `direct_reclaims`, `hits_to_hot`, `hits_to_warm`,
`hits_to_cold`, `hits_to_temp` — every one is LRU segmentation or item age, and
there is neither.

`mem_requested` is absent for a subtler reason: it is bytes of item data, where
`used_bytes` is LMDB pages in use and includes index and page overhead. Close
enough to be tempting, different enough to mislead a capacity calculation.

---

## 7. `stats slabs`

`STAT <class>:<field> <value>` lines plus two totals, `END`.

LMDB is not a slab allocator, so the geometry fields have no honest value. What
survives is the **per-class command counters**, which are real and which this
server has — with one class, the class totals are the server totals.

| Field | Source |
|---|---|
| `1:get_hits` | `hits` |
| `1:cmd_set` | `cmd_set` |
| `1:delete_hits` | P1 |
| `1:incr_hits` / `1:decr_hits` | P1 |
| `1:cas_hits` / `1:cas_badval` | P1 |
| `1:touch_hits` | P1 |
| `1:used_chunks` | `StoreStats::entries` — see below |
| `active_slabs` | `1` |
| `total_malloced` | `map_size` |

**`used_chunks` is the one geometry field with an exact meaning here.** Upstream
counts the chunks allocated to live items and allocates one per item — measured
against 1.6.45, where it tracked the item count exactly and fell by one on a
delete, matching `items:1:number`. There is no chunking at all in this store, so
one record is one unit of storage in use and the mapping is exact rather than an
approximation.

**Absent**: `chunk_size`, `chunks_per_page`, `total_pages`, `total_chunks`,
`free_chunks`, `free_chunks_end`. A page here is an LMDB page holding records of
many sizes; reporting it as a chunk geometry would let a tool compute a slab
efficiency that means nothing.

That is also what keeps `used_chunks` honest while these stay out: the
meaningless number a slab geometry invites is `used_chunks / total_chunks`, and
without a denominator nobody can compute it.

This is the thinnest of the subcommands, and that is the honest outcome for a
question about a slab allocator asked of a B-tree. It is worth implementing
anyway because tooling calls it unconditionally and an `ERROR` reads as a broken
server.

---

## 8. `stats conns`

`STAT <id>:<field> <value>` lines, `END`. Needs
[P3](#p3-a-connection-registry).

| Field | Value |
|---|---|
| `<id>:addr` | the peer address, `tcp:<ip>:<port>` |
| `<id>:listen_addr` | the address this server accepted on ([P2](#p2-the-listen-address)) |
| `<id>:secs_since_last_cmd` | now minus the connection's last command |
| `<id>:vash_dialect` | `vcp`, `memcached` or `resp` |
| `<id>:vash_authenticated` | `yes` / `no` |

`<id>` is a monotonic connection id, not a file descriptor. Upstream's is an fd
and is reused as fds are; a client correlating two `stats conns` calls is better
served by an id that never repeats. The listener itself is reported too, as
upstream does, with `state conn_listening` — which is the one state that is
unambiguous.

**`state` is absent for a live connection.** Upstream's ten values
(`conn_new_cmd`, `conn_parse_cmd`, `conn_nread`, `conn_swallow`, `conn_mwrite`,
…) name positions in an event-loop state machine that does not exist here; a
connection is an async task, and the honest answer is "somewhere in a `select`".
Picking a plausible one would be inventing an internal this server does not
have. `vash_dialect` is offered in its place, and answers the question an
operator was actually asking.

**Cost.** One relaxed atomic store per command, on a word the connection already
owns. Registration and deregistration take a lock; the hot path does not.

---

## 9. The three that stay refused

`stats reset`, `stats cachedump` and `stats detail` are answered

```text
CLIENT_ERROR stats reset is not implemented
CLIENT_ERROR stats cachedump is not implemented
CLIENT_ERROR stats detail is not implemented
```

Named rather than lumped into `ERROR`, which is the call this codebase already
makes for `lru_crawler enable` and for `SET … IFEQ`: upstream *does* implement
these, so the bytes diverge whatever is sent, and saying which command was
refused is worth more than a shorter divergence. An unrecognised subcommand
still gets the bare `ERROR` upstream sends, so the two cases stay
distinguishable.

**`stats reset`** is implementable and the design is known — a baseline snapshot
that `stats` subtracts, leaving the raw atomics for `/metrics`, so no Prometheus
counter ever goes backwards. It is not built because the value does not carry
the complexity: it buys a human a zeroed counter in a terminal, no tooling drives
it, and it leaves `stats` and `/metrics` reporting different numbers for the same
thing — a discrepancy someone eventually has to be told about. `/metrics` with a
time range is the better answer to the question it asks.

**`stats cachedump`** is upstream's older key dump, and this server already
serves the command that replaced it. Three reasons compound:

- `lru_crawler metadump` and `mgdump` are implemented, carry more per key, and
  page across the whole keyspace where `cachedump` returns one capped page.
- Its `ITEM <key> [<b> b; <ts> s]` is a **positional bracket format with an
  unencoded key**. The keyspace is shared across dialects, so a key holding a
  space or a CRLF — which a Redis or VCP client can write — would break the
  line, and unlike the `key=…` format there is nowhere to put an encoding.
  Entries would have to be silently skipped.
- Implementing it would mean putting `value_len` back on `ListEntry`, which
  [introspection.md](introspection.md) §6 removed on the argument that `size` is
  not in the guaranteed field set and `mg <key> s` answers it per key. **That
  decision now stands unreversed**, and every VCP listing keeps the 8 bytes per
  entry it would otherwise have paid.

**`stats detail`** is per-key-prefix hit counters, and it is the only proposal
here that would put work on the retrieval hot path — a hash lookup and four
counters on every get and store. It is off by default upstream for that reason,
which also means no tooling depends on it. Upstream's table is uncapped, so
a client that puts an id in the first path segment turns it into an unbounded
leak; building it safely means a cap, a cap means a dropped-prefix counter, and
that is a lot of machinery for a feature whose own author ships it disabled.
Per-prefix hit rates are better answered by the caller, which knows its own
naming scheme.

Consequently **`stat_key_prefix` is absent from `stats settings`**, and
`detail_enabled` reports a constant `no`.

---

## 10. `stats sizes`, and the three that need nothing

**`stats sizes`** answers:

```text
STAT sizes_status disabled
END
```

**This is byte-identical to a stock memcached.** Upstream tracks item sizes only
when started with `-o track_sizes`, and 1.6.45 answers exactly these two lines
otherwise. Reporting it costs one constant and is not an approximation of
anything.

Were it ever wanted, the shape is known: `STAT <size> <count>` where size is a
32-byte bucket. The implementation is a bounded scan reading record lengths,
which needs a size-only read on the `Store` trait — an engine change for a
diagnostic, and the reason it is not proposed now.

`stats sizes_enable` and `stats sizes_disable` answer `ERROR`, which is what
1.6.45 answers — the two verbs were removed upstream.

**`stats extstore`** and **`stats proxy`** answer a bare `END`, which is what a
memcached without external storage or the proxy compiled in answers. There is no
external storage here and no proxy, so the empty reply is exact.

---

## 11. Prerequisites

Three, down from six: dropping `reset`, `cachedump` and `detail` removed the
counter baseline, the `ListEntry` field and the prefix table with them.

### P1. Per-command outcome counters

The largest piece, and the one that unlocks the most fields: sixteen counters on
`ServerMetrics`, incremented in `dispatch::execute`'s existing classification
block, which already inspects every reply.

`delete_hits`, `delete_misses`, `incr_hits`, `incr_misses`, `decr_hits`,
`decr_misses`, `cas_hits`, `cas_misses`, `cas_badval`, `touch_hits`,
`touch_misses`, `store_too_large`, `total_items`, `cmd_meta`, `bytes_read`,
`bytes_written`.

Every one is already derivable where it is counted:

| Counter | Derived from |
|---|---|
| delete | `Reply::Deleted` vs `Reply::NotFound`; `DeletedMany` per entry |
| touch | `Reply::Touched` vs `Reply::NotFound` |
| incr / decr | `Reply::Arithmetic` vs `NotFound`, direction from `Delta::Counter { decrement }` |
| cas | `SetMode::Cas` with `Stored::Stored` / `Exists` / `NotFound` — hit, badval, miss |
| `store_too_large` | `Status::TooLarge` |
| `total_items` | `Stored::Stored(_)` |
| `bytes_read` / `bytes_written` | two relaxed adds in `conn`, one per syscall |

**These are not memcached-shaped.** A delete hit rate is an operational number in
any dialect, so they are exported to Prometheus as
`vash_command_outcome_total{command,outcome}` as well, and `INFO` can pick them
up later. A counter visible over one wire format and not the others would be an
odd thing to have built.

### P2. The listen address

`ServerState` gains the bound `SocketAddr`. Needed by `settings.tcpport`,
`settings.inter` and `conns.listen_addr`. It is already known at `bind` time.

### P3. A connection registry

`Mutex<HashMap<u64, Arc<ConnInfo>>>`, where `ConnInfo` holds the peer address and
dialect (written once) and an `AtomicU64` of the last-command instant. Inserted
on accept, removed on close, and the hot path touches only its own atomic
through the `Arc` it already holds — no lock per command.

This is also what a future Redis `CLIENT LIST` would need, and what makes
`HELLO`'s `id: 0` stop being a placeholder.

---

## 12. Configuration

**None.** Every subcommand implemented here reads state the server already has,
and the three that would have needed a setting are the three being refused.
`cachedump` would have ridden `listing_enabled` with the dumps; `detail` would
have needed an enable flag and a prefix cap.

---

## 13. Errors

| Reply | When |
|---|---|
| `ERROR` | An unrecognised subcommand, `sizes_enable`, `sizes_disable`. Upstream's answer, and it replaces today's blanket `CLIENT_ERROR bad command line format`. |
| `CLIENT_ERROR stats <name> is not implemented` | `reset`, `cachedump`, `detail` — recognised upstream, deliberately not built. §9. |
| `CLIENT_ERROR bad command line format` | A recognised subcommand given arguments it does not take. |
| `CLIENT_ERROR unauthenticated` | Any of them before authenticating, as today. |
| `END` | `extstore`, `proxy`, and any implemented subcommand with nothing to report. |

---

## 14. Testing

**Differential.** Built, as the `memcached/stats` suite in
`tests/compat/docker_differential.py` — 11 identical and 3 known divergences
against `memcached:1.6-alpine`.

It compares two ways, because two different things are being claimed. `sizes`,
`extstore`, `proxy` and the error replies go through the ordinary byte-for-byte
path, since all four are exact. The sections carrying live counters declare a
**shape reduction** instead: a leaf name collapses to `<field>` and a numeric
segment to `<n>`, leaving the namespace structure a tool actually parses —
`STAT <field>`, `STAT items:<n>:<field>`, `STAT <n>:<field>`, `END`. A line that
is not a `STAT` survives verbatim, so a malformed reply still shows up as a
difference rather than being reduced into agreement.

The three refusals are probes too, with their reasons in `KNOWN_DIVERGENCES`.
Recording a deliberate divergence is what keeps the suite green without losing
the fact that it exists.

**Unit.** The `settings` mapping table renders every configured value; the
`items` class id is the same constant the dumps print.

**Integration.** Each subcommand parses under `pymemcache`'s `stats()` — which
takes an argument and is the client path these actually travel. `stats conns`
lists a connection that exists and drops it on close. `stats reset`,
`cachedump` and `detail` are refused by name, and an unknown subcommand is
still a bare `ERROR` — the two cases must stay distinguishable.

**A test that pins the rule.** Every field name any subcommand emits is either
in a reviewed allow-list or carries the `vash_` prefix — so a counter added
later cannot quietly claim an upstream name whose meaning it does not share.
That is the check that would have caught `reclaimed`.

---

## 15. Work breakdown

| Step | Scope | Rough size |
|---|---|---|
| 1. Subcommand parsing, the three refusals, `sizes`/`extstore`/`proxy`/`sizes_enable` | `memcached/text.rs`, `encode.rs` | ~130 lines |
| 2. P1 outcome counters + Prometheus series | `metrics.rs`, `dispatch.rs`, `conn.rs` | ~180 lines |
| 3. `stats` field extension | `stats.rs` | ~60 lines |
| 4. `settings` + P2 | `stats.rs`, `state.rs`, `lib.rs` | ~150 lines |
| 5. `items`, `slabs` | `stats.rs`, `memcached/encode.rs` | ~110 lines |
| 6. `conns` + P3 | new module, `conn.rs`, `state.rs` | ~200 lines |
| 7. Tests, `protocol.md`, `README.md` | — | ~400 lines |

**Step 1 alone is worth shipping.** It needs no new state and turns seven of the
thirteen forms from a blanket client error into the right answer — four exact
matches with a stock memcached, and three refusals that name themselves.

Step 6 is the only one that touches the connection hot path, and is the one to
defer if any is.
