# VCP opcode specifications

How each VCP opcode is **implemented**: the path a frame takes from the socket
to the storage engine and back, what is validated where, which statuses can
arise and why, and what the opcode costs.

This sits between the other two documents and duplicates neither:

- [protocol.md](protocol.md) is the normative wire format — what a client must
  put on the socket. It is written to be sufficient without reading the source.
- [plan.md](plan.md) is the design rationale — why there is a native protocol at
  all (§3), why tags invalidate in O(1) (§5), why the storage tier is a separate
  thread pool (§9).
- **This document** is the implementation contract. Read it to change the
  server, to add an opcode, or to explain a behaviour that the wire format
  permits but does not pin down.

Where the two disagree, protocol.md is normative for clients and this document
is wrong and should be fixed.

**Protocol version:** 1. Opcode values are permanent: a value is never reused
for a different command, and a retired one stays reserved.

---

## Opcode index

| Value | Name | State | Storage | Runs on |
|---|---|---|---|---|
| `0x01` | [`HELLO`](#hello-0x01) | implemented | none | any thread |
| `0x02` | [`PING`](#ping-0x02) | implemented | none | any thread |
| `0x03` | [`AUTH`](#auth-0x03) | implemented | none | any thread |
| `0x04` | [`STATS`](#stats-0x04) | reserved | — | rejected in the decoder |
| `0x05` | [`CLUSTER`](#cluster-0x05) | implemented | none | any thread |
| `0x10` | [`GET`](#get-0x10) | implemented | read | reader |
| `0x11` | [`SET`](#set-0x11) | implemented | write | shard writer |
| `0x12` | [`DELETE`](#delete-0x12) | implemented | write | shard writer |
| `0x13` | [`TOUCH`](#touch-0x13) | implemented | write | shard writer |
| `0x20` | [`GET_MANY`](#get_many-0x20) | implemented | read | reader, per shard |
| `0x21` | [`SET_MANY`](#set_many-0x21) | implemented | write | shard writer, per shard |
| `0x22` | [`DELETE_MANY`](#delete_many-0x22) | implemented | write | shard writer, per shard |
| `0x30` | [`DELETE_BY_TAG`](#delete_by_tag-0x30) | implemented | write | every shard writer |
| `0x31` | [`FLUSH`](#flush-0x31) | implemented, gated | write | every shard writer |
| `0x40` | [`TAG_SYNC`](#tag_sync-0x40) | implemented | write | shard writers, as needed |
| `0x50` | [`LIST_KEYS`](#list_keys-0x50) | **planned** | read (scan) | blocking pool |
| `0x51` | [`LIST_TAGS`](#list_tags-0x51) | **planned** | none (RAM) | blocking pool |

"Runs on" is where the *work* happens, not where the frame is decoded. Every
frame is decoded on whichever thread executes it — see below.

---

## The shared path

Everything an opcode does happens inside this sequence. An entry below only
describes where it departs from it.

### 1. Dialect selection

`vash_proto::detect` reads the connection's **first byte**, once. `0x01` — the
`HELLO` opcode — selects VCP. Any other opening byte selects memcached, Redis,
or closes the connection.

This is what makes `HELLO` mandatory as the opening frame: the requirement is
enforced by the detector, not by per-connection session state. Nothing later
checks that a handshake happened or succeeded.

### 2. Framing

`peek_frame_len` reads only the 12-byte header and reports the total frame
length. `conn::drain_vcp` splits each complete frame off the read buffer with
`BytesMut::split_to`, which hands over the bytes by reference count — the key
and value slices the decoder produces borrow directly from the frame, and the
value is never copied on the way in.

A `body_len` above 64 MiB is `FrameLen::TooLarge`: the frame boundary is
unknowable, so the connection closes with no response. Everything else is
recoverable.

### 3. The hop to the storage tier

**Whatever complete frames one socket read produced cross to the storage tier
together, in a single `spawn_blocking`.** The hop is a thread handoff and costs
far more than executing a cached request; one hop per frame capped a pipelined
connection at roughly 5k ops/s regardless of pipeline depth, because the depth
bought nothing. Amortising it over a read costs the unpipelined case nothing.

With `store.inline_reads = true` **and** every buffered frame's opcode passing
`Opcode::is_read_only()`, the batch runs on the runtime worker instead. That
predicate lists what is known to be safe rather than excluding what is not: an
opcode added later is a write until someone says otherwise, because being wrong
in the permissive direction would let a write block a runtime worker behind the
shard's writer queue.

### 4. Decode → execute → encode

`dispatch::execute_frame_into` does all three, per frame, on the executing
thread:

1. `vcp::decode` → `Request { request_id, opcode, no_reply, command }`, where
   `command` is a `vash_core::Command` borrowing the frame.
2. `offsets_to_store_ttls` rewrites VCP's TTL offsets into the store's
   `exptime`-shaped field. VCP's `ttl_secs` is an offset at every magnitude; the
   store's field is memcached's, where a value past 30 days is an absolute
   stamp. Without this a VCP client asking for 60 days would get a deadline in
   1970. Applied here rather than in the decoder because the translation needs
   the clock, and a decoder that reads the clock no longer round-trips against
   its encoder.
3. `execute` runs the command and classifies the outcome for metrics.
4. `encode_reply` on success, `encode_error` on failure.

Decode failures split two ways, and the split is the whole robustness story:

| Failure | Effect |
|---|---|
| `DecodeError::Fatal` (`body_len` over the ceiling) | Connection closed. There is no way to find the next frame. |
| `DecodeError::Body` | One error frame carrying the **raw opcode byte** and the request id, then the connection continues. |

An unknown opcode is a `Body` error with `UNSUPPORTED`, which is why the client
can still correlate a reply to a command this build has never heard of.

### 5. `NO_REPLY`

Set on the request, the work runs to completion and **nothing is written to the
socket** — including on failure, which is logged at `warn` with the status and
opcode. It is a property of the response, never of the execution: a `NO_REPLY`
`SET` still commits, still allocates a CAS token, still fans tags out.

### 6. Status mapping

Every storage failure passes through one function, `dispatch::to_status`:

| `StoreError` | Status | Notes |
|---|---|---|
| `CapacityFull` | `CAPACITY_FULL` (7) | The map is full, or the pressure gauge reads critical. |
| `TagLimit` | `CAPACITY_FULL` (7) | The tag registry is full. Logged with the limit. |
| `Overloaded`, `ShuttingDown` | `OVERLOADED` (6) | Retryable. |
| `Unsupported` | `UNSUPPORTED` (8) | Logged. |
| `Core(ValueTooLarge)`, `Core(KeyTooLong)` | `TOO_LARGE` (4) | |
| `NotNumeric` | `NOT_NUMERIC` (11) | Only reachable via memcached today. |
| `Core(_)` | `BAD_REQUEST` (3) | Includes the per-record tag limit. |
| anything else | `INTERNAL` (9) | Logged in full; the client is told nothing about internals. |

### 7. Metrics

`execute` classifies each outcome exactly once, at the single point every
command passes through, so the counters cannot drift from what was served:

| Reply | Counted as |
|---|---|
| `Value`, `Values` | reads, split into hits and misses |
| `NotFound` from a read command | one read miss |
| `Stored`, `StoredMany`, `Deleted`, `DeletedMany`, `Touched`, `Counter`, `Invalidated`, `Flushed` | one write |
| everything else, and every error | `other` |

Errors additionally increment an error class: capacity, overloaded, internal, or
client.

---

## Session and metadata opcodes

### `HELLO` (0x01)

**Body** — `protocol_version` u16, `reserved` u16.

**Decode** — requires at least 4 bytes; a longer body is accepted and the excess
ignored. Only the first two bytes are read.

**Execute** — compares against `vash_core::PROTOCOL_VERSION`. A mismatch logs at
`warn` and returns `UNSUPPORTED` with an empty body; **the connection stays
open**, so a client can report a clear error instead of hanging. On a match the
reply is `state.info`, a `ServerInfo` built once at startup by
`dispatch::server_info` and never recomputed: shard count, `MAX_KEY_LEN` (511,
LMDB's compile-time ceiling), the configured `max_value_len`, and the capability
bits.

`TAGS` and `MEMCACHED` are always set. `CLUSTER` is set only when this node has
peers **and** is configured to forward to them — not merely because the build
supports it, because a client seeing the bit must be able to trust that one
invalidation covers the cluster.

**Storage** — none.

**Note.** The handshake is advisory. The server keeps no per-connection state,
so a client that ignores an `UNSUPPORTED` reply and carries on issuing commands
is served normally. Version negotiation exists so the client can fail loudly,
not to gate access.

### `PING` (0x02)

**Body** — empty by convention; **any body is accepted and ignored**, because
the decoder maps the opcode straight to `Command::Ping` without looking at it.
The reply body is always empty.

**Storage** — none, which makes it a liveness check and not a health check: it
says the accept loop and a runtime worker are alive, nothing about whether the
store can be read.

### `AUTH` (0x03)

**Body** — `mechanism u8 | name_len u8 | secret_len u16 | name | secret`, with
the ceilings and semantics in [protocol.md](protocol.md#auth-0x03). Both lengths
are checked before either is used to slice, and trailing bytes are refused.

**Decode** — its own `Decoded::Auth` variant rather than a `vash_core::Command`.
Authentication is a property of a *connection*, not an operation on a cache: the
domain crate has no variant for it, the storage tier never sees one, and
`execute` is never reached.

**Storage** — none. One `HashMap` lookup and one constant-time comparison of
32-byte digests, against a table held in RAM.

**Gating** — this is the gate. The pre-authentication set is `HELLO` and `AUTH`
and nothing else; see the [refusal path](#the-refusal-path) below.

**Statuses** — `OK`; `UNAUTHORIZED` (5) for a bad name or a bad secret, which
are deliberately indistinguishable; `UNSUPPORTED` (8) for a mechanism this build
does not implement, which today means everything except `PLAIN`; `BAD_REQUEST`
(3) for a malformed body, counted as a failed attempt because a malformed body
is as good a brute-force vehicle as a well-formed one.

**`NO_REPLY` is ignored**, uniquely. See protocol.md for why.

#### The refusal path

An unauthenticated frame is refused **from the twelve-byte header, before the
body is parsed at all**. That ordering is the point rather than an
implementation detail: it keeps the pre-authentication attack surface down to
the frame header plus `decode_auth`, instead of every body decoder in the
protocol. They are all fuzzed, so this is defence in depth rather than a hole
being closed — but a gate that runs after the parsing it protects is not much of
a gate.

It has a second effect worth stating: because the refusal happens before opcode
recognition, an unknown or unimplemented opcode answers `UNAUTHORIZED` like
everything else rather than `UNSUPPORTED`. An unauthenticated party therefore
cannot enumerate which opcodes this build has.

### `STATS` (0x04)

**Reserved over VCP**, though the underlying command exists: `Command::Stats` is
served over the memcached `stats` command and gathers a real payload.

It is refused **twice**, independently. The decoder rejects the opcode, and the
VCP encoder maps `Reply::Stats` to `UNSUPPORTED` rather than rendering something
plausible. The decoder's refusal is the one that fires; the encoder's exists so
that routing a `Stats` reply here in future is a visible failure rather than a
silently empty frame.

Implementing it means choosing a binary encoding for a string-keyed map that
memcached renders as text. Until then the admin HTTP port and the memcached
dialect are the two ways to read the counters.

### `CLUSTER` (0x05)

**Body** — empty; ignored if present.

**Execute** — `state.cluster.view()`: the configured mode (`local`, `fanout`,
`fanout_sync`), and each peer's address with a `reachable` byte set from the
outcome of the last exchange. `reachable` is 0 before one has been attempted.

**Storage** — none; the peer list is static configuration held in RAM.

Membership is what this node was *told*, never a discovered set, which is
exactly what makes comparing two nodes' views a drift detector.

---

## Single-key opcodes

### `GET` (0x10)

**Body** — the whole body is the key. No inner length prefix: `body_len` already
gives it, and the key is a direct subslice of the frame.

**Decode** — `Key::new`: empty → `BAD_REQUEST`, over 511 bytes → `TOO_LARGE`.

**Execute** — `XXH3(key) % shards` selects the environment (XXH3 rather than the
default hasher because the mapping must survive a restart), then one read
transaction on a thread-local reader slot, `main.get`, `RecordRef::parse`, and
the liveness check:

```
alive = record.epoch == global_epoch                        // flush
     && (expires_at_ms == NEVER || expires_at_ms > now_ms)   // TTL
     && record.tags.all(|(id, gen)| registry[id].gen == gen) // tags
```

All three are RAM-only — the tag registry is a `Vec` indexed by id — so a miss
costs no extra I/O. **A read never writes**, so an expired or invalidated record
found here is reported absent and left for the sweeper and reclaimer.

**Reply** — `mc_flags`, `cas`, then the value. Not live → `NOT_FOUND` with an
empty body.

**Note.** The store fills in `expires_at_ms` on the `Value` and the VCP encoder
drops it, because the wire format has no field for it. The alternative — a
plausible-looking zero — would be a lie; the memcached `mg … t` flag is the way
to read a TTL.

### `SET` (0x11)

**Body** — a 12-byte `SetBodyHeader` (`ttl_secs`, `key_len`, `tag_count`,
`reserved`, `value_len`), then key, value, and `tag_count` length-prefixed tag
names.

**Decode** — the header is a `zerocopy` cast. The key goes through `Key::new`;
tag names are checked for emptiness and the 255-byte ceiling. Two things are
deliberately **not** checked here:

- `value_len` against `max_value_len` — that is store policy, applied by
  `validate_value` during preparation, and a codec has no business knowing it.
- `tag_count` against `store.tags.max_per_record` — the field is a `u8`, so it
  is already bounded at 255 structurally, and the configured limit is enforced
  by `check_tag_counts` with the same `BAD_REQUEST` the decoder would have sent.

The decoder always sets `mc_flags = 0` and `mode = SetMode::Set`. VCP has no
flags field and no conditional writes, so `NOT_STORED` and `EXISTS` cannot arise
on this path even though the encoder can render them.

**Execute** — `Store::store`, which is four steps:

1. `ensure_tags_registered` — any tag name the owning shard has not seen costs
   one extra writer round trip to register it durably. A record may never
   reference an unregistered tag: after a restart the id would be unknown (a
   spurious miss) or reused for another name (a spurious invalidation). A new
   name is registered at the generation **the rest of the node already holds**,
   not at zero, so a shard meeting the name late cannot capture a lower number
   and be killed by the next gossip round.
2. `resolve` — names to `(tag_id, generation)` pairs, from RAM.
3. `prepare_set` — **on the calling thread**, so the value copy and record
   framing run in parallel across connections and the single writer is left with
   only B-tree work. Refuses immediately with `CAPACITY_FULL` if the shard's
   pressure gauge reads critical, rather than encoding a record and queueing it
   for a store that cannot accept it.
4. The shard writer queue → group commit. Inside the transaction: `next_cas`,
   `patch_cas` into the already-encoded record, `drop_index_entries` for
   whatever was there before, `main.put`, `exp.put`, and one `tagidx.put` per
   tag.

Every record is indexed in `exp`, including records with no TTL — they go in the
`NEVER` bucket, which sorts last. A record outside that index could never be
chosen as an eviction victim, so a cache of TTL-less keys would fill with
nothing to free.

**Reply** — `OK` with the new `cas` u64.

**CAS tokens** are allocated per shard and striped (`counter * shards + index`)
so they are unique server-wide while staying strictly increasing within a shard,
and therefore within any single key — the only ordering compare-and-swap
depends on. A durable watermark is reserved in blocks, and a restart resumes
past the whole reserved block rather than risking reuse.

### `DELETE` (0x12)

**Body** — the whole body is the key.

**Execute** — routed to the owning shard's **writer queue**, as a one-item
`delete_many`. The reply is whether the record was *live* before the delete;
`drop_index_entries` and `main.delete` run either way, so deleting an
expired-but-unswept record frees its space while still reporting `NOT_FOUND` —
it was already invisible.

Deletes are permitted under critical capacity pressure. Refusing them would be
self-defeating: a delete is how space comes back.

**Note.** A `NOT_FOUND` from `DELETE` is counted as `other`, not as a read miss —
`is_read` covers only the retrieval commands, so a delete miss cannot skew the
hit ratio.

### `TOUCH` (0x13)

**Body** — `ttl_secs` u32, then the key to the end of the body.

**Execute** — writer queue. LMDB values are immutable blobs, so re-stamping an
expiry **rewrites the record**: the value is copied inside the transaction, the
old index entries are dropped and new ones written, and the CAS token advances.
That is the price of `TOUCH` being a bandwidth optimisation rather than a
storage one — the client does not resend the value, but the server still moves
it.

Client flags, tags and their captured generations survive; the epoch is carried
over from the existing record rather than re-read.

`ttl_secs = 0` clears the expiry and moves the record to the `NEVER` bucket. Not
live → `NOT_FOUND`, and nothing is written.

---

## Batch opcodes

All three carry `count u32` first, which is **bounded against
`MAX_BATCH_ITEMS` (4096) before it is used to size any allocation**. This is the
one number in the protocol an attacker controls that directly drives a `Vec`
capacity.

### `GET_MANY` (0x20)

**Body** — `count`, then `count` × (`key_len` u16, key).

**Execute** — keys are grouped by owning shard, keeping each key's position, and
each group is resolved by one `get_many` on its shard; results are scattered
back into request order. Duplicated keys produce duplicated slots.

**Snapshot scope.** Within one shard every key is resolved against a single read
transaction and a single tag-registry lock. Across shards there is **one
transaction per shard**, so a batch spanning shards is a set of per-shard
snapshots taken at slightly different instants, not one global snapshot. With
`shards = 1` — which the single-shard fast path takes without any grouping — it
is exactly one snapshot.

**Reply** — `count`, then one byte per key and a payload only where that byte is
1. Never `NOT_FOUND`: a miss is reported per item inside an `OK` body.

### `SET_MANY` (0x21)

**Body** — `count`, then `count` `SET` bodies back to back.

**Execute** — tag registration runs for the whole batch first, then items are
grouped by shard, prepared on the calling thread, and handed to each shard's
writer as **one transaction per shard**.

**Atomicity is per shard.** A batch touching three shards is three commits, and
a failure in one leaves the others applied. Within a shard it is all or nothing.
Clients read `shards` from the handshake to know which applies.

There is no per-item status because there is nothing left to report per item:
everything rejectable per item — key length, value size, tag limits — is
rejected while decoding or preparing, which fails the whole frame, and a failure
at execution time fails the whole transaction.

**CAS tokens do not increase across a batch** when there is more than one shard,
because each shard numbers independently.

### `DELETE_MANY` (0x22)

**Body** — identical to `GET_MANY`.

**Execute** — grouped by shard, **one writer round trip per shard** rather than
one per key. Reply is `count` then one byte per key: 1 where it was live.

---

## Keyspace opcodes

### `DELETE_BY_TAG` (0x30)

**Body** — the whole body is the tag name. Empty → `BAD_REQUEST`, over 255 bytes
→ `TOO_LARGE`.

**Execute** — the invalidation is a generation bump, so it costs the same for
ten keys or a million.

It **fans out across every shard**, because a tag's keys are spread by key hash
across all of them and an invalidation reaching one would leave the rest being
served. Per shard: read the current generation **from disk, not RAM** (a RAM-only
bump lost to a crash would resurrect data), add one *saturating*, write it back,
and queue a reclamation job carrying the new generation as its target. A
levelling pass then raises any shard that lagged, so the node holds one
generation for the name — the number it reports to peers. Shards that have never
seen the name are left alone; one that registers it later adopts the node-wide
value anyway.

After the local commit — never before — `cluster.invalidate` forwards it: a
queue push under `fanout`, a wait for reachable peers under `fanout_sync`,
nothing under `local`. A peer can therefore never be told about an invalidation
this node did not perform.

**Reply** — `OK` if the tag existed, `NOT_FOUND` if it was never registered,
which means nothing could have carried it.

**Two things worth knowing.** Affected records stop being served before the
response is sent; their space comes back later, from the resumable reclaimer,
which judges deadness against the **job's** target generation rather than the
live registry — the registry is only published after the commit, so trusting it
during a pass that shares that transaction would mark every record live and leak
them permanently. And a generation that has saturated at `u64::MAX` logs a
warning and still replies `OK`, because the alternative — wrapping — would
resurrect every record ever invalidated under that tag.

### `FLUSH` (0x31)

**Body** — empty.

**Gate** — returns `UNAUTHORIZED` (5), logged, unless `protocol.flush_enabled`
or `--enable-flush` is set. It is a remote cache-wipe primitive available to
anyone who can reach the port, so it is off by default.

**Execute** — per shard, in one transaction: bump the epoch (wrapping), persist
it to `meta`, and clear `main`, `exp`, `tagidx` and `jobs`. Both halves are
needed. The clear frees the space — an epoch bump alone would leak every record
without a TTL, since nothing would ever come looking for them. The epoch closes
the MVCC window — a read transaction opened before the commit still sees the old
snapshot, and comparing those records against the new epoch is what stops them
being served.

**Tag registrations survive.** A flush empties the data; it does not un-declare
the tags, and their generations must keep advancing.

**Reply** — the highest epoch across shards. Node-local: never gossiped, and
there is no cluster-wide flush.

### `TAG_SYNC` (0x40)

Peer-to-peer, not a client command — but an ordinary command on the ordinary
port, because a peer is just another VCP client. That is why there is no second
listener, no second codec and no second thing to fuzz.

**Body** — `kind` u8 (0 partial, 1 full digest), 3 reserved bytes, `count` u32
bounded against `MAX_TAG_SYNC_ENTRIES` (8192) before allocating, then `count` ×
(`generation` u64, `name_len` u16, name). An unknown `kind` is rejected rather
than defaulted: guessing would risk answering a partial push as though it were a
full digest, which is a different and much larger reply.

**Execute**, in this order, and the order is load-bearing:

1. Snapshot the registry (`tag_generations`, read from RAM across shards, merged
   by maximum — a digest costs no transaction).
2. Compute what the sender is behind on: every offered name this node holds a
   **strictly higher** generation for. Plus, only if the request was a full
   digest, every locally-known tag at a non-zero generation the request did not
   name — against a partial message there is no way to tell "does not know it"
   from "did not fit". Truncated to 8192.
3. Merge the offered entries: `generation = max(local, received)` per shard,
   skipping from RAM any shard already at or past the offered value. That skip
   is the common case by far — gossip re-offers the same generations every
   interval, and without it a converged cluster would spend a write per shard
   per tag per round doing nothing.

Computing the answer **before** merging is what makes the exchange work at all:
afterwards every offered generation is one this node also holds, "we know
better" would be true of nothing, and the peer would never learn anything.

**Reply** — the same layout, always `kind = 0`, since a reply answers what was
asked and must not be read as a whole table.

Because the merge is by maximum, the command is idempotent, order-independent
and safe to retry — which is precisely why cluster invalidation needs no
acknowledgement protocol.

---

## Planned opcodes

Not implemented. `Opcode::from_u8` does not know these values yet, so a server
of this generation answers `UNSUPPORTED` (8) with the opcode echoed. The
specification is here so that a client written against it does not have to be
rewritten, and so the values cannot be claimed by anything else.

They are **administrative and diagnostic commands**, and are specified as such:
correctness and bounded cost matter, throughput does not. Neither is on any hot
path, neither may be used to build an application-level index, and the
implementation is explicitly permitted to be a linear scan.

`0x50` opens a new group rather than extending `0x3x`. `0x30` and `0x31` are
whole-keyspace *mutations*; grouping a listing next to them invites the
assumption that the same gate and the same danger apply in the same way.

### One shape for both

**Decision: `LIST_KEYS` and `LIST_TAGS` take the same request body and return
the same response body, field for field.** They differ only in what the entries
name and where the server reads them from. One decoder, one validator, one
matcher, one pagination loop — in the server, in `vash-client`, and in whatever
tooling drives them.

The unification is not free, and the price is named here rather than discovered
later: `scanned` and `TRUNCATED` are load-bearing for a keyspace scan and nearly
vacuous for a RAM-resident tag table, which never exhausts a budget. They are
still *true* there, just uninteresting — which is a different thing from a field
that is false, and the line this project already draws when it refuses to report
an unmeasured `stats` counter as a plausible zero.

**Two opcodes rather than one `LIST` with a subject byte.** The subject would
have to be validated, would need an error path for an unknown value, and would
put both commands behind one entry in the opcode table, one metrics label and
one `is_read_only` answer. An unknown *opcode* already has all of that, for
free, and the table stays self-describing. Symmetry of layout is worth having;
symmetry that erases which command was called is not.

### Shared request layout

A 16-byte header followed by an optional pattern:

| Offset | Field |
|---|---|
| 0 | `limit` u32 — maximum entries in the reply, 1–1024 |
| 4 | `offset` u64 — entries to skip |
| 12 | `pattern_len` u16 |
| 14 | `reserved` u16 — zero |
| 16 | `pattern` bytes[pattern_len] |

**Bytes after the pattern are rejected with `BAD_REQUEST`.** Extension happens
through `reserved`, and silently ignoring a trailing field would let a client
believe something took effect that this build never read.

`limit` of 0 is `BAD_REQUEST` — a client asking for nothing is a bug, not a
cheap way to probe a count. `limit` above 1024 is `BAD_REQUEST` rather than
silently clamped, for the same reason: a client that asked for 10000 and got
1024 back with no indication would page incorrectly. One ceiling for both
commands, so a client's paging logic has no per-command case; a tag table large
enough to feel it is a table nobody reads interactively anyway.

### Shared response layout

On `OK`:

| Offset | Field |
|---|---|
| 0 | `count` u32 — entries in this page |
| 4 | `flags` u8 — bit 0 `MORE`, bit 1 `TRUNCATED` |
| 5 | `reserved` u8 × 3 — zero |
| 8 | `scanned` u64 — entries examined to produce this page |
| 16 | `next_offset` u64 — the `offset` to send for the next page |
| 24 | `count` × entry |

An entry is `version` u64, `name_len` u16, `name` bytes — **`TAG_SYNC`'s entry
layout, byte for byte**, so a client that already decodes a digest reuses that
code for both commands.

`version` is the u64 the server holds for that name: the record's CAS token for
a key, the tag's generation for a tag. Both are opaque monotonic version
numbers, which is exactly what CAS is already documented to be — comparable
against an earlier reading of the *same* name and against nothing else. Two
listings diffed by version is how a client sees what changed, and it is the only
metadata that is genuinely free: the record header is parsed for the liveness
check whether or not the CAS is sent, and the tag generation is the value being
listed.

`MORE` is set whenever the walk stopped for any reason other than reaching the
end. A client pages by echoing `next_offset` back and stops when `MORE` is
clear — the same loop for both commands.

**No `total` field.** It would be honest for tags, where the whole population is
in RAM, and unaffordable for keys, where it means a full scan. Rather than carry
a field that is meaningful in one command and a placeholder in the other, there
is none: the registry size is already reported as `vash_tags` in `stats` and on
the metrics port, and a listing is not the place to learn a count.

### Pattern matching

**Decision: a byte-wise glob with `*` and `?`, and nothing else.**

| Token | Meaning |
|---|---|
| `*` | any run of bytes, including empty |
| `?` | exactly one byte |
| `\x` | the literal byte `x`, for any `x` — this is how `*`, `?` and `\` are matched literally |
| any other byte | itself |

An empty pattern matches everything. Matching is byte-wise: no case folding, no
UTF-8 interpretation, no character classes — `[a-z]` is five literal bytes. A
pattern ending in a lone `\` is `BAD_REQUEST`. `pattern_len` may not exceed 511,
the key ceiling.

**Why have it at all.** Without server-side filtering, finding `session:*` in a
million-key cache means moving every key over the wire and filtering client-side
— exactly what a diagnostic tool must not do to a live cache. The filter itself
is free: the scan already parses every record it walks, so matching is a
comparison against bytes that are already in hand.

**Why not more.** Full glob character classes, negation, or a regular expression
each add a matcher that takes untrusted input and therefore needs fuzzing, to
buy expressiveness that key-naming schemes — which are overwhelmingly
`prefix:id` — do not use. `*` and `?` cover them, and a matcher for two tokens
is small enough to reason about. Redis's `SCAN … MATCH` is the same shape for
the same reason.

The matcher must be non-backtracking or bounded; the greedy two-pointer glob
algorithm is linear in `pattern_len × key_len` worst case and both are capped at
511, which is the bound this specification requires.

### Gating

Both commands are refused with `UNAUTHORIZED` (5) unless
`protocol.listing_enabled` is set. Default **false**, mirroring `FLUSH`, for two
reasons that are separately sufficient:

- **Disclosure.** Cache keys routinely embed user and session identifiers, and
  enumeration turns a port a client can reach into a dump of who and what is in
  the cache. `FLUSH` is gated because it destroys; this is gated because it
  reveals. Authentication (see [auth.md](auth.md)) does not retire this
  argument: it decides *who may connect*, where the gate decides what any
  connected client may do, and every authenticated client here is equally
  trusted.
- **Cost.** These are the only reads in the protocol whose cost is not bounded
  by the request. Every other read touches as many records as the client named.

A new capability bit, `LISTING = 0x08`, is set in the `HELLO` reply when the
commands are enabled, so a client discovers support without probing — the same
contract as `CLUSTER`, which is set only when fan-out is genuinely configured.

### Scheduling

Both are read-only in the sense that matters to correctness — they never touch a
writer queue — but they are **deliberately excluded from
`Opcode::is_read_only()`**, so they always take the `spawn_blocking` hop and
never run inline on a runtime worker. That predicate's real question is "is this
cheap enough to run on a runtime worker", and a scan holding a read transaction
for milliseconds is not, however read-only it is. If the predicate ever grows a
second caller meaning "does not touch the writer queue", it has to be split
rather than reused.

The reader-slot consequence is bounded by construction: a scan holds one LMDB
read transaction per shard **one shard at a time**, never all of them at once,
so a listing costs one slot in `vash_readers_in_use` no matter how many shards
it walks. It is still by far the longest-lived read transaction the server
opens, and a long-lived read transaction blocks LMDB from reusing freed pages —
which is what the scan budget below is really protecting. This is the workload
`store.oldest_reader_age_ms` was proposed for in plan §12 and which nothing
currently measures; M8 is the milestone that gives it a reason to exist.

### `LIST_KEYS` (0x50)

Lists the keys a `GET` would currently hit.

**Body** — the shared request and response layouts above. `name` is the key,
`version` is its CAS token, and `scanned` counts records walked — including the
dead and non-matching ones, which is what makes it worth reporting: a page of 10
keys that cost 90000 records to find is the signal that a pattern is not
selective.

**Order.** Shard index major, LMDB key order within a shard. With `shards = 1`
that is plain lexicographic order over the whole keyspace; above that it is not,
and the response makes no claim that it is. A global merge would mean one open
cursor and one read transaction per shard held simultaneously for the length of
the request, plus a heap — reader-slot pressure and complexity spent on
prettiness in a debugging command.

**Liveness.** Every record walked is parsed and put through the same
`is_alive` check as `GET`, against the same clock, epoch and tag registry. A key
that appears in a listing is a key that a `GET` issued at that instant would
hit. Records that are expired, flushed or tag-invalidated but not yet reclaimed
are skipped and counted only in `scanned`. The scan **never writes**, so it does
not reclaim what it discovers to be dead — that stays the sweeper's and the
reclaimer's job, and a listing must not be a way to trigger deletion.

**Pagination.** `offset` counts *matching live keys*, not records walked, so
paging is defined in terms of what the client sees rather than what the store
happens to hold. Cost is therefore O(`offset` + `limit`) walked records, plus
whatever dead or non-matching ones lie among them. That is accepted: this is a
diagnostic command and the offset form is what makes it usable from a shell one
page at a time.

`next_offset` is the position to resume from — `offset` plus the matching live
keys this call skipped *or* returned. A client pages by echoing it back and
stops when `MORE` is clear.

**Bounding.** A single call examines at most `protocol.listing_max_scan` records
(default **100000**). Hitting it sets `TRUNCATED`, and `next_offset` still
reflects the ground covered, so a client that keeps paging **makes progress**
rather than re-scanning the same prefix forever. `MORE` is set whenever the walk
stopped for any reason other than running off the end of the last shard.

**Consistency.** A page is a snapshot of one shard at one instant; a sequence of
pages is not a snapshot of anything. Concurrent writes mean a key can be missed
or repeated across pages, and `count` may be less than `limit` on a page that is
not the last. Clients must not infer a total from a listing, and must not treat
the absence of a key from a page as evidence it does not exist. There is no
attempt to fix this — the fix is a long-lived read transaction across every
shard, which is the one thing LMDB deployments must never do.

**Statuses** — `OK`, `BAD_REQUEST` (limit 0 or over the ceiling, malformed
pattern, trailing bytes, short body), `UNAUTHORIZED` (disabled), `INTERNAL`.
Never `NOT_FOUND`: an empty result is `count = 0`, because no matches is not a
miss.

**Metrics** — counted as `other`. A listing must not touch the hit/miss
counters, since a hit ratio computed over scanned records would be meaningless
and would corrupt a real one. `scanned` is worth its own counter.

**Deliberately not included in the reply:** value, TTL, size, tag names. A
listing carrying them is `GET_MANY` with extra steps and a frame size set by the
data rather than by the request; the two-step "list, then get what you care
about" is what keeps a page's size a function of the key lengths alone. The CAS
is the exception, and only because it is 8 fixed bytes already in hand — see the
shared response layout. The TTL and tags of one key are reachable through the
memcached `mg … t` flag and `me`.

**The one optimisation deliberately not made:** a pattern with a literal prefix
(`session:*`) could seek each shard's `main` cursor to that prefix and stop at
its end, turning a full scan into a range scan. `main` is key-ordered, so it
would work, and it is the obvious first move if this command ever stops being
purely administrative. It is not built, because the correctness of the linear
version does not depend on it and building both means maintaining both.

### `LIST_TAGS` (0x51)

Lists the tag registry: every name this node has registered, with the generation
it holds.

**Body** — the shared request and response layouts above. `name` is the tag
name, `version` is its generation, and `scanned` counts registry entries
considered.

**Execute** — entirely from RAM. The registry is loaded fully at boot and lives
as an array plus a name index; `tag_generations()` already merges it across
shards by maximum, which is the same value the node reports to peers. No
transaction is opened, no page can fault, and no reader slot is taken — the
whole population is in hand before the first entry is matched.

That is why `TRUNCATED` is never set here and `scanned` never exceeds the
registry size: the scan budget exists to bound a walk over the map, and there is
no map to walk. The fields are carried anyway so that both listings decode
identically, and a client that checks `TRUNCATED` is not wrong to, merely never
surprised.

Sorted **lexicographically by name**. Registration order is per-shard and
differs between nodes, so it would make two nodes' listings incomparable —
and comparing them is a legitimate use, since it is how convergence is observed.
Sorting costs O(t log t) on a table bounded by `store.tags.max_tags` (default
100000), per call, which is acceptable for a command nothing calls in a loop.

**Semantics.** A generation of 0 means the tag has been registered but never
invalidated anywhere. Every registered name is listed, including names no live
record carries: registrations survive `FLUSH`, and **nothing removes a tag
today** — which makes this command the tool for seeing the registry grow toward
`max_tags`, a risk plan §15 names and does not otherwise instrument.

The listing is this node's view. Generations converge across the cluster within
one gossip interval, so a difference between two nodes' listings is either
convergence in flight or a peer that cannot be reached — `CLUSTER` says which.

**Statuses** — as `LIST_KEYS`. Never `NOT_FOUND`: a pattern matching no tag is
`count = 0`.

**Pagination** — `offset` counts matching entries, and because the population is
a stable sorted array rather than a live keyspace, paging is consistent within
one call and only as consistent as the registry between calls. A tag registered
mid-sequence can shift later pages by one entry; nothing removes a tag today, so
an entry never disappears from under a pager.

**Metrics** — `other`.

---

## Adding an opcode

The checklist, in the order the compiler will find them for you:

1. `vcp::frame::Opcode` — the variant, the value in `from_u8`, and a decision
   about `is_read_only` (default to no).
2. `vcp::decode` — a body decoder that **bounds every attacker-controlled length
   before it sizes an allocation**, returns `DecodeError::Body` so the connection
   survives a malformed frame, and stays a pure function of `&[u8]` with no
   clock and no I/O.
3. `vash_core::Command` and `Reply` — the boundary types. If the new opcode
   needs neither a new variant nor a new `Store` method, it is probably an
   existing command with a different body.
4. `vash_store::Store` — the operation, if it needs one. Writes take the
   caller's transaction so the writer can pack many into one commit.
5. `dispatch::execute_inner` — execution and status mapping. Nothing else may
   map a domain outcome onto a wire status.
6. `vcp::encode::encode_reply` — the response body. A non-`OK` status has an
   empty body.
7. `fuzz/fuzz_targets/vcp_decode` covers the new path automatically; add a seed
   to `examples/seed_corpus.rs` so it reaches it in the first minutes rather
   than the first hours.
8. Both documents: the wire format in [protocol.md](protocol.md), the
   implementation here.
