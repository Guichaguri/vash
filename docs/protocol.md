# Protocol reference

Normative specification of the three protocols vash speaks, written to be
sufficient for implementing a client without reading the server source.

- [VCP](#vcp--the-native-binary-protocol) — the native binary protocol.
- [Memcached](#memcached-compatibility) — what is supported, what is extended,
  and where behaviour deliberately differs from upstream.
- [Redis](#redis-compatibility) — the RESP subset, in RESP2 and RESP3.

Design rationale for these choices lives in [plan.md](plan.md) §3 and §7; this
document describes only what is on the wire. How each VCP opcode is implemented
— the path from socket to storage engine, what is validated where, and what it
costs — is in [opcodes.md](opcodes.md).

**Protocol version:** 1. **Default port:** 11311.

---

## Choosing between them

All three protocols are served on the **same port**. Use VCP for anything new:
it is cheaper to parse, supports batch operations in one round trip, exposes
tags natively, and is the only one that can pipeline without its replies being
strictly ordered. Use the memcached or Redis protocol when an existing client
library must work unchanged.

### First-byte detection

The server decides which dialect a connection speaks from its **first byte**,
once, and never revisits the decision:

| First byte | Dialect |
|---|---|
| `0x01` | VCP (the `HELLO` opcode) |
| `*` (`0x2A`) | Redis (RESP; a request is always an array) |
| `a`–`z` (`0x61`–`0x7A`) | memcached |
| anything else | connection closed immediately |

This is why **a VCP connection must open with a `HELLO` frame**. Sending any
other opcode first closes the connection, because the leading byte would be
ambiguous. There is no in-band negotiation beyond this.

Either compatibility dialect can be turned off — `protocol.memcached_enabled`
and `protocol.resp_enabled`, both on by default, or `--disable-memcached` and
`--disable-resp` — and a disabled one is **closed right here**, before its
parser sees a byte, exactly as an unrecognised first byte is. A client speaking
it gets a connect that succeeds and then EOF, with the reason only in the
server's log; there is no error frame, because producing one would mean running
the parser the operator turned off. The `MEMCACHED` and `RESP` [capability
bits](#hello-0x01) are where a dialect's availability is stated, and VCP —
which cannot be disabled — is the one dialect that can report it.

It is also why **RESP inline commands are not accepted**: `get foo\r\n` is a
valid inline Redis command *and* a valid memcached one, and no amount of
look-ahead settles which was meant. Every real Redis client library sends the
array form, so nothing is lost but the ability to drive the server by hand from
`telnet` — for which the memcached dialect is right there.

Values are shared across all three: a key written by a memcached client is
readable by a VCP or Redis client and vice versa, including its client-flags
field.

---

# VCP — the native binary protocol

All integers are **little-endian** and **unaligned**. There is no padding beyond
fields explicitly named `reserved`, which must be written as zero and ignored on
read.

## Framing

Every message, in both directions, is a 12-byte header followed by `body_len`
bytes of body.

```
 0               1               2               3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------+---------------+-------------------------------+
|   opcode u8   |   flags u8    |          status u16           |
+---------------+---------------+-------------------------------+
|                        request_id u32                         |
+---------------------------------------------------------------+
|                         body_len u32                          |
+---------------------------------------------------------------+
|                      body (body_len bytes)                    |
```

| Field | Type | In requests | In responses |
|---|---|---|---|
| `opcode` | u8 | the command | echoed from the request, **even if unknown** |
| `flags` | u8 | see below | bit 0 (`RESPONSE`) is always set |
| `status` | u16 | must be 0 | the result code |
| `request_id` | u32 | client-assigned | echoed verbatim |
| `body_len` | u32 | body length | body length |

### Flags

| Bit | Name | Meaning |
|---|---|---|
| 0 (`0x01`) | `RESPONSE` | Set by the server on every frame it sends. |
| 1 (`0x02`) | `NO_REPLY` | Request only. The server performs the work and sends **nothing at all**. |
| 2 (`0x04`) | — | Reserved. Must be zero. |

`NO_REPLY` suppresses the response even on failure, so a client using it cannot
learn that a write was rejected. Errors are still logged server-side.

### Frame size

`body_len` must not exceed **67108864** (64 MiB). A larger value is
unrecoverable — the server cannot find the next frame boundary — so it **closes
the connection** without a response. Clients must apply the same ceiling to
inbound frames.

## Connection lifecycle

1. Connect (TCP; `TCP_NODELAY` recommended, the server sets it on its side).
2. Send `HELLO`. This must be the first frame.
3. Check `protocol_version` in the reply and the capability bits.
4. If `AUTH_REQUIRED` is set, send [`AUTH`](#auth-0x03). Everything else is
   refused with `UNAUTHORIZED` (5) until it succeeds.
5. Issue commands.
6. Close the socket. There is no `QUIT` opcode in VCP.

A `HELLO` carrying a version other than `1` is answered with
`UNSUPPORTED` (8) and the connection stays open, so a client can report a clear
error rather than hanging. The handshake is **advisory**: the server keeps no
per-connection session state, so a client that ignores the refusal and issues
commands anyway is served normally. Version negotiation exists so a client can
fail loudly, not to gate access — do not rely on the server to stop you.

An authenticated connection is **never idle-reaped**. There is no idle timeout
and no server-side keepalive, so a pooled connection may sit unused
indefinitely. Two things still close one without warning:

- **Shutdown.** A draining server closes connections that are idle between
  requests. Nothing is buffered and no reply is outstanding when it happens, so
  a client loses at most a connection it was not using — but it does lose it,
  and must reconnect rather than treat the close as fatal.
- **The connection cap.** Past `server.max_connections` (default 10000) a new
  connection is accepted and then dropped immediately, with no bytes sent. A
  client sees a connect that succeeds and a read that returns EOF; that is
  backpressure, not a broken server.

### Pre-authentication limits

These apply only while `AUTH_REQUIRED` is set and the connection has not yet
authenticated. All four are enforced by closing the socket, most of them without
a reply, so a client that trips one sees a bare disconnect and has to know why:

| Limit | Default | On breach |
|---|---|---|
| Bytes buffered at once | 4096 | Connection closed, **no reply at all** — including to frames in that buffer the server had not run yet. |
| An arriving frame's declared total length | 4096 | Complete frames ahead of it are executed and answered, then the connection closes. |
| Time to authenticate | `auth.timeout_ms`, 5000 ms | Connection closed, no reply. |
| Failed attempts on one connection | `auth.max_attempts`, 3 | The last refusal **is** sent, then the connection closes. |

The 4096-byte ceilings are the ones that surprise a client author. Everything
legal before authenticating is small — `HELLO` is 16 bytes on the wire and an
`AUTH` at both ceilings is under 600 — so the budget is generous against honest
traffic and deliberately useless to anyone holding a connection open with
nothing presented.

The consequence is concrete: **do not pipeline real work into the same write as
`AUTH`.** Land more than 4096 bytes in one read and the connection is closed
before the `AUTH` in that buffer is even looked at; land a frame header
announcing a body past the ceiling and the handshake is answered but the socket
still goes. Either way the client sees a half-finished handshake and a
disconnect it has no status code for. Send `HELLO` and `AUTH`, wait for the
`OK`, then pipeline freely.

Concurrent *unauthenticated* connections are capped separately
(`auth.max_unauthenticated_connections`, defaulting to a tenth of
`server.max_connections`), so a stalled pool cannot consume the whole connection
budget. Connections past that cap are dropped at accept, exactly as the overall
cap does.

## Request correlation and pipelining

A client may send any number of frames without waiting. The server echoes
`request_id`, and **clients must correlate replies by `request_id` rather than
by arrival order**.

The current server happens to answer one connection's frames in the order it
received them. That is not a guarantee: the format exists to allow a sharded
server to answer out of order, and a client that assumes ordering will break
against a later version. `request_id` values are opaque to the server — any
scheme works, including reuse, as long as ids in flight on one connection are
distinct.

Exactly one response frame is produced per request frame, except for `NO_REPLY`
requests, which produce none.

## Opcodes

| Value | Name | Implemented |
|---|---|---|
| `0x01` | `HELLO` | yes |
| `0x02` | `PING` | yes |
| `0x03` | `AUTH` | yes, if authentication is configured |
| `0x04` | `STATS` | reserved — returns `UNSUPPORTED` |
| `0x05` | `CLUSTER` | yes |
| `0x10` | `GET` | yes |
| `0x11` | `SET` | yes |
| `0x12` | `DELETE` | yes |
| `0x13` | `TOUCH` | yes |
| `0x14` | `ARITHMETIC` | yes |
| `0x20` | `GET_MANY` | yes |
| `0x21` | `SET_MANY` | yes |
| `0x22` | `DELETE_MANY` | yes |
| `0x30` | `DELETE_BY_TAG` | yes |
| `0x31` | `FLUSH` | yes, if enabled server-side |
| `0x40` | `TAG_SYNC` | yes — peer-to-peer, see [Cluster](#cluster) |
| `0x50` | `LIST_KEYS` | yes, if enabled server-side — see [Listings](#list_keys-0x50-and-list_tags-0x51) |
| `0x51` | `LIST_TAGS` | yes, if enabled server-side — see [Listings](#list_keys-0x50-and-list_tags-0x51) |

Opcode values are **permanent**: a value is never reused for a different
command, and a retired one stays reserved.

An unknown opcode is answered with `UNSUPPORTED` (8), an empty body, and the
**original opcode byte echoed**, so the client can still correlate. The
connection stays open.

## Status codes

| Value | Name | Meaning |
|---|---|---|
| 0 | `OK` | Success. |
| 1 | `NOT_FOUND` | Key absent or no longer live; also an unregistered tag on `DELETE_BY_TAG`. |
| 2 | `EXISTS` | Reserved for CAS mismatch. Not emitted over VCP today. |
| 3 | `BAD_REQUEST` | Malformed body, empty key, oversized batch. |
| 4 | `TOO_LARGE` | Key, value or tag over its limit. |
| 5 | `UNAUTHORIZED` | Refused by policy: the connection has not authenticated, or the command is disabled by configuration (`FLUSH`, `LIST_KEYS`, `LIST_TAGS`). |
| 6 | `OVERLOADED` | Write queue full, or the server is shutting down. Retryable. |
| 7 | `CAPACITY_FULL` | The store is out of space, or the tag registry is full. Retryable only after something frees space. |
| 8 | `UNSUPPORTED` | Unknown or unimplemented opcode, an unsupported protocol version, or an `AUTH` mechanism this build does not have. |
| 9 | `INTERNAL` | Server-side failure. Details are logged, not sent. |
| 10 | `NOT_STORED` | Reserved for a guarded write whose condition failed. Not emitted over VCP today. |
| 11 | `NOT_NUMERIC` | Arithmetic on a value that is not a number in the requested domain, or a result that will not fit. See [`ARITHMETIC`](#arithmetic-0x14). |

Codes 2 and 10 are defined so that conditional writes can be added to VCP
without a wire change; today they arise only through the memcached adapter,
because a VCP `SET` is always unconditional. **Clients should handle unknown
status codes as generic failures** rather than rejecting the frame.

Which statuses are worth retrying is worth stating plainly, because the
distinction is what a client's retry policy should be built on:

| Status | Retry |
|---|---|
| `OVERLOADED` (6) | Yes, with backoff. The condition is transient by construction. |
| `CAPACITY_FULL` (7), `INTERNAL` (9) | Only with backoff and a ceiling; neither clears because you asked again quickly. |
| `BAD_REQUEST` (3), `TOO_LARGE` (4), `UNSUPPORTED` (8) | Never. The same bytes will be refused the same way. |
| `UNAUTHORIZED` (5) | Only after re-authenticating, and never in a loop — see the [abuse budget](#pre-authentication-limits). |
| `NOT_FOUND` (1) | Not an error. A cache miss is the ordinary case. |

Any response with a non-`OK` status has an **empty body**, with one exception:
`NOT_FOUND` on `GET_MANY` does not occur — misses are reported per item inside
an `OK` body.

## Command reference

Notation: `u16`/`u32`/`u64` are little-endian; `bytes[n]` is a raw byte run.

### `HELLO` (0x01)

**Request body** (4 bytes):

| Offset | Field |
|---|---|
| 0 | `protocol_version` u16 — must be 1 |
| 2 | `reserved` u16 — zero |

**Response body** (18 bytes), status `OK`:

| Offset | Field |
|---|---|
| 0 | `protocol_version` u16 |
| 2 | `shards` u16 |
| 4 | `max_key_len` u32 |
| 8 | `max_value_len` u32 |
| 12 | `capabilities` u32 |
| 16 | `max_tags_per_record` u16 |

Only the first four bytes of the request body are read; a longer one is accepted
and the excess ignored. Decode the response by offset and tolerate a body
**longer** than 18 bytes rather than requiring exactly 18 — that is how this
field was added without a version bump, and how the next one will be.

`max_value_len` and `max_tags_per_record` are the server's configured limits.
`max_key_len` is fixed at 511 by the storage engine and is reported rather than
configured. Enforce all three locally rather than discovering them through
errors — over `NO_REPLY` there is no error to discover, and the refusals carry
no detail: an oversized value is a bare `TOO_LARGE` and an over-tagged record a
bare `BAD_REQUEST`, neither of which says what the limit was.

`shards` is how many independent storage environments back the server. It
matters to a client for one reason: it decides whether a multi-key write is
atomic. With `shards = 1` a `SET_MANY` is all-or-nothing; above that it is
atomic per shard only.

**Capability bits:**

| Bit | Name | Meaning |
|---|---|---|
| `0x01` | `TAGS` | Tags and `DELETE_BY_TAG` are available. |
| `0x02` | `MEMCACHED` | The memcached protocol is served on this port. |
| `0x04` | `CLUSTER` | An invalidation sent here reaches the rest of the cluster. |
| `0x08` | `LISTING` | `LIST_KEYS` and `LIST_TAGS` are enabled here. |
| `0x10` | `AUTH_REQUIRED` | This connection must send `AUTH` before anything else. |
| `0x20` | `FLUSH` | `FLUSH` is enabled here. |
| `0x40` | `RESP` | The Redis protocol is served on this port. |

**Every bit reports what this node does, never what the build knows how to do.**
`CLUSTER` is set only when this node has peers configured **and** is set to
forward, so a client seeing it clear must invalidate on every node itself.
`LISTING` and `FLUSH` report that those opcodes are enabled here, not that the
build knows them — without which the only way to test for `FLUSH` would be to
wipe the cache and see. `AUTH_REQUIRED` reports that this server is enforcing
authentication, not that it understands `AUTH`. `MEMCACHED` and `RESP` report
that those dialects are being served: a disabled one is closed at first-byte
detection, so a connection speaking it sees a bare disconnect and never an
error frame. All five follow from the same rule — a client that cannot tell
"disabled here" from "too old to know it" has to find out by trying.

A default server answers `0x43` — `TAGS | MEMCACHED | RESP`.

Unlisted bits are reserved; ignore them.

### `PING` (0x02)

Empty request body — any body is accepted and ignored. Response is `OK` with an
empty body. Does not touch storage, so it is a liveness check, not a health
check.

`PING` is **not** in the pre-authentication set. Everything it could tell an
unauthenticated party `HELLO` already told them, and `/health` on the admin port
— off until an operator enables it — is the liveness check for an operator.

### `AUTH` (0x03)

**Request body:**

| Offset | Field |
|---|---|
| 0 | `mechanism` u8 — 0 `PLAIN`, 1 `HMAC_SHA256` |
| 1 | `name_len` u8 — 0 to 64; **0 means the `default` identity** |
| 2 | `secret_len` u16 — 0 to 512 |
| 4 | `name` bytes[`name_len`] |
| 4 + `name_len` | `secret` bytes[`secret_len`] |

**Response:** `OK` with an empty body, or `UNAUTHORIZED` (5) with an empty body.
A bad name and a bad secret are the same answer, so the reply does not confirm
which names exist. Trailing bytes, an over-long name or an over-long secret are
`BAD_REQUEST` (3), and count as a failed attempt.

`mechanism` **0 `PLAIN`** is the only one implemented: the secret crosses the
wire and the server holds `SHA-256(secret)`. `1 HMAC_SHA256` is specified in
[auth.md](auth.md#63-the-challengeresponse-mechanism-specified-not-built) and
answered `UNSUPPORTED` (8) — as is any other value, so a client can probe for a
mechanism without needing a capability bit for it.

**`NO_REPLY` is ignored on `AUTH`; the response is always sent.** It is the only
opcode that overrides the flag. A client that cannot learn whether it
authenticated would pipeline a whole batch into a connection that refuses all of
it.

Re-authenticating on a live connection is allowed and replaces the identity, so
a pooled connection can follow a credential rotation without reconnecting. A
failed re-authentication leaves the existing identity intact — it is an attempt
that did not land, not a logout.

**Only `HELLO` and `AUTH` are accepted before authenticating.** `HELLO` has to
be, because [first-byte detection](#first-byte-detection) requires a VCP
connection to open with it; it therefore discloses the server's limits and
capability bits to an unauthenticated party, which is the deliberate minimum
that lets a client discover it must authenticate. Every other opcode — including
ones this build does not implement, and bytes that are not opcodes at all — is
`UNAUTHORIZED` (5), so an unauthenticated party cannot enumerate what the server
supports.

A connection that does not authenticate within `auth.timeout_ms` is dropped, and
one that fails `auth.max_attempts` times is closed after its last refusal is
sent.

### `GET` (0x10)

**Request body:** the key, raw. The whole body is the key — there is no length
prefix, because `body_len` already gives it.

**Response body** on `OK`:

| Offset | Field |
|---|---|
| 0 | `mc_flags` u32 — the memcached client-flags field |
| 4 | `cas` u64 |
| 12 | `value` bytes, to the end of the body |

A miss — absent, expired, flushed or tag-invalidated — is `NOT_FOUND` with an
empty body. A VCP `GET` does **not** report the remaining TTL; use the memcached
`mg` command with the `t` flag if that is needed.

### `SET` (0x11)

**Request body:** a 12-byte header, then the key, the value, and the tag list.

| Offset | Field |
|---|---|
| 0 | `ttl_secs` u32 — see [TTL](#ttl-semantics) |
| 4 | `key_len` u16 |
| 6 | `tag_count` u8 — see [tags](#tags) for the limit |
| 7 | `reserved` u8 — zero |
| 8 | `value_len` u32 |
| 12 | key `bytes[key_len]` |
| 12 + key_len | value `bytes[value_len]` |
| … | `tag_count` × (`tag_len` u16, tag `bytes[tag_len]`) |

**Response body** on `OK`: `cas` u64 — the new CAS token.

Notes:

- A VCP `SET` is unconditional. `add`/`replace`/`append`/`prepend`/`cas`
  semantics are reachable only through the memcached protocol.
- A VCP `SET` always stores `mc_flags` as **0**; there is no flags field in the
  body. A value written over VCP and read by a memcached client therefore has
  flags 0. Write it over `ms` with an `F` flag if the flags field matters.
- Tag names must be 1–255 bytes. Unknown tag names are registered on first use.
- Keys and values are **binary-safe**: any byte, including NUL, and no encoding
  is assumed. Only the memcached dialect restricts the key charset.
- `value_len` of 0 is legal; an empty value is a value, and reads back as a hit
  with a zero-length body.
- A key outside 1–511 bytes is `BAD_REQUEST` (3) when empty and `TOO_LARGE` (4)
  when over; a value past the server's ceiling is `TOO_LARGE` (4).

### `DELETE` (0x12)

**Request body:** the key, raw.

`OK` with an empty body if the key was **live** before the delete; `NOT_FOUND`
otherwise. A record that has expired but not yet been reclaimed reports
`NOT_FOUND`, because it was already invisible.

### `TOUCH` (0x13)

**Request body:**

| Offset | Field |
|---|---|
| 0 | `ttl_secs` u32 — see [TTL](#ttl-semantics) |
| 4 | key bytes, to the end of the body |

`OK` if the key was live, `NOT_FOUND` otherwise. The value is unchanged; its CAS
token advances.

### `ARITHMETIC` (0x14)

An atomic read-modify-write on a counter. The read of the current value and the
write of the new one happen **inside one transaction**, so concurrent clients
cannot lose an update. One round trip, one storage operation.

Counters are stored as their **decimal text**, which is what makes a plain `GET`
of a counter return something readable and what lets all three dialects move the
same counter. A value written by `SET` is a valid counter exactly when it parses
in the requested domain.

**Request body:** a fixed 32-byte prefix, then the key to the end of the body.

| Offset | Field |
|---|---|
| 0 | `mode` u8 — the numeric domain |
| 1 | `flags` u8 |
| 2 | `on_bound` u8 |
| 3 | `ttl_kind` u8 |
| 4 | `ttl_secs` u32 — see [TTL](#ttl-semantics) |
| 8 | `delta` u64 |
| 16 | `lower` u64 |
| 24 | `upper` u64 |
| 32 | key bytes, to the end of the body |

`mode` decides how the three eight-byte numbers are read. One fixed layout
carries all three domains rather than a variable-length encoding, because this
is a single-key operation on the hot path and sixteen wasted bytes cost less
than a length prefix would.

| `mode` | Domain | The three numbers |
|---|---|---|
| 0 `COUNTER` | Unsigned 64-bit: an increment wraps, a decrement floors at zero. memcached's `incr`/`decr`. | `delta` as `u64`; `lower` and `upper` are ignored |
| 1 `INT` | Signed 64-bit | all three as `i64`, two's complement |
| 2 `FLOAT` | `f64` | all three as IEEE-754 bit patterns |

**`flags`:**

| Bit | Name | Meaning |
|---|---|---|
| 0 (`0x01`) | `CREATE_AT_ZERO` | An absent key reads as zero and is created. Clear means an absent key is `NOT_FOUND` and nothing is written. |
| 1 (`0x02`) | `DECREMENT` | Subtract rather than add. **Counter mode only** — the other two carry the sign in `delta`. |

**`on_bound`** — what happens when the result will not fit `lower..=upper`:

| Value | Meaning |
|---|---|
| 0 | Fail. `NOT_NUMERIC` (11), nothing written. |
| 1 | Skip. The value stays exactly where it is, the reply reports a zero increment, and **nothing is written — not even the deadline**. |
| 2 | Clamp to whichever bound was breached, and store that. |

An operation that wants no bounds passes the limits of its own type, which turns
"overflowed" and "out of bounds" into one condition with one handler.

**`ttl_kind`:**

| Value | Meaning |
|---|---|
| 0 | Leave the deadline alone. A record created here gets none. |
| 1 | Set it to `ttl_secs`. |
| 2 | Set it only if the record currently has no deadline. |

An unrecognised `mode`, `on_bound` or `ttl_kind` is `BAD_REQUEST` (3). None of
them is defaulted: a byte this build does not know might mean the numbers below
it are to be read as something else, and guessing would apply an operation the
client did not ask for.

**Response body** on `OK` (20 bytes):

| Offset | Field |
|---|---|
| 0 | `mode` u8 — echoed from the request |
| 1 | `wrote` u8 — 1 if anything was stored |
| 2 | `reserved` u16 — zero |
| 4 | `value` u64 — where the counter ended up |
| 12 | `applied` u64 — how far it moved |

`value` and `applied` are read in the domain `mode` names. **The mode is echoed
rather than assumed**, so a client decodes the reply without having to remember
what it asked for — which matters because replies may arrive out of order.

`wrote` of 0 means a bound held the value where it was, so the key kept both its
value and its lifetime. It is the only way to tell that apart from an increment
that legitimately moved the counter by zero.

Notes:

- Tags and client flags **survive** an arithmetic write; the CAS token advances,
  and the reply does not carry it. Read it with a `GET` if you need it.
- The counter text is subject to the configured value ceiling like any other
  value, though a 64-bit number never approaches it.

**Statuses:** `NOT_FOUND` (1) for an absent key without `CREATE_AT_ZERO`;
`NOT_NUMERIC` (11) for a stored value that does not parse in the requested
domain and for a bound breached under `on_bound = 0`; `BAD_REQUEST` (3) for a
malformed body or an unknown enum byte.

### `GET_MANY` (0x20)

**Request body:**

| Offset | Field |
|---|---|
| 0 | `count` u32 — at most 4096 |
| 4 | `count` × (`key_len` u16, key `bytes[key_len]`) |

The 4096 ceiling is the same for all three batch opcodes, and it is checked
**before `count` sizes any allocation**. A larger value is `BAD_REQUEST` (3), not
a truncated batch. A `count` of 0 is legal and answers with an empty result.

**Response body** on `OK`:

| Field |
|---|
| `count` u32 — always equal to the request's count |
| `count` × item |

Each item is one byte, then a payload only if that byte is 1:

| Field |
|---|
| `found` u8 — 1 hit, 0 miss |
| if found: `mc_flags` u32, `cas` u64, `value_len` u32, value `bytes[value_len]` |

Results are in **request order**, one slot per requested key, so duplicates in
the request produce duplicates in the reply.

**The snapshot is per shard, not per batch**, for the same reason `SET_MANY`'s
atomicity is: keys are distributed across independent storage environments and
each is read in its own transaction. Every key owned by one shard is resolved
against one consistent snapshot; keys in different shards are read at slightly
different instants. With `shards = 1` the whole batch is one snapshot.

### `SET_MANY` (0x21)

**Request body:** `count` u32 — at most 4096 — then `count` repetitions of the
`SET` body layout (header, key, value, tags) back to back.

**Response body** on `OK`: `count` u32, then `count` × `cas` u64, in request
order.

There is no per-item status, because everything rejectable per item — key
length, value size, tag limits — is rejected while decoding, which fails the
whole frame with `BAD_REQUEST` or `TOO_LARGE`.

**Atomicity is per shard, not per batch.** Keys are distributed across
independent storage environments, and each commits its own transaction, so a
batch touching three shards is three commits: a failure in one leaves the others
applied. Within a single shard it is still all or nothing. Read `shards` from
the [handshake](#hello-0x01) to know which applies — with `shards = 1` a batch
is fully atomic.

Later items overwrite earlier ones within a batch. **CAS tokens do not increase
across a batch** when there is more than one shard, because each shard numbers
independently; see [CAS tokens](#cas-tokens).

### `DELETE_MANY` (0x22)

**Request body:** same as `GET_MANY`.

**Response body** on `OK`: `count` u32, then `count` × u8 (1 if that key was
live, 0 otherwise), in request order.

### `DELETE_BY_TAG` (0x30)

**Request body:** the tag name, raw, 1–255 bytes.

`OK` with an empty body if the tag existed; `NOT_FOUND` if it was never
registered, which means nothing could have carried it.

Constant time regardless of how many keys carry the tag. Affected keys stop
being served **before the response is sent**; the disk space is reclaimed in the
background afterwards. See [Tags](#tags).

### `FLUSH` (0x31)

Empty request body. **Response body** on `OK`: `epoch` u32 — the new flush
epoch.

Returns `UNAUTHORIZED` (5) unless the server was started with
`protocol.flush_enabled` or `--enable-flush`. Empties the entire cache; tag
registrations survive.

The `FLUSH` capability bit in the [handshake](#hello-0x01) is how a client tells
"disabled here" apart from an older build that has never heard of the opcode —
check the bit rather than probing, because probing this one means wiping the
cache when it turns out to be enabled.

A flush is **node-local**. It is not propagated to peers, and there is no
cluster-wide flush.

### `CLUSTER` (0x05)

Empty request body. **Response body** on `OK`:

| Offset | Field |
|---|---|
| 0 | `mode` u8 — 0 `local`, 1 `fanout`, 2 `fanout_sync` |
| 1 | `reserved` u8 — zero |
| 2 | `peer_count` u16 |
| 4 | `peer_count` × (`addr_len` u16, addr `bytes[addr_len]`, `reachable` u8) |

Membership is static configuration, not a negotiated set: this is what one node
was *told*, so comparing views across nodes is how a client detects drift.
`reachable` is whether the last exchange with that peer succeeded, and is 0
before one has been attempted.

### `TAG_SYNC` (0x40)

Merges tag generations. This is how nodes propagate invalidations to each other;
a client normally has no reason to send it, but it is an ordinary command on the
ordinary port and is documented so a peer can be implemented against it.

**Request body:**

| Offset | Field |
|---|---|
| 0 | `kind` u8 — 0 partial, 1 full digest |
| 1 | `reserved` u8 × 3 — zero |
| 4 | `count` u32 — at most 8192 |
| 8 | `count` × (`generation` u64, `name_len` u16, name `bytes[name_len]`) |

**Response body** on `OK`: the same layout, always `kind` 0, carrying every
offered name the receiver holds a **strictly higher** generation for — plus,
when the request was a full digest, every tag the receiver holds at a non-zero
generation that the request did not name. The sender merges those in turn, so
one round trip converges both directions.

`kind` says whether the sender listed its whole table. Only then can the
receiver volunteer tags the sender never mentioned; against a partial message
there is no way to tell "does not know it" from "did not fit". A generation of
0 carries no information and is ignored on receipt, so it is never sent.

Each entry is applied as `generation = max(local, received)`, creating the name
if this node has never seen it. That makes the command **idempotent,
order-independent and safe to retry**, which is the whole reason cluster
invalidation needs no acknowledgement protocol. See [Cluster](#cluster).

The reply is truncated at 8192 entries like the request, so a receiver with more
to say than fits says the rest on the next gossip round.

### `LIST_KEYS` (0x50) and `LIST_TAGS` (0x51)

Administrative, paginated enumeration: `LIST_KEYS` lists the keys a `GET` would
currently hit, `LIST_TAGS` lists the tag registry with the generation held for
each name.

Both are **off by default** and answer `UNAUTHORIZED` (5) unless the server has
`protocol.listing_enabled`. The `LISTING` capability bit in the
[handshake](#hello-0x01) is how a client tells "disabled here" apart from an
older build that has never heard of the opcodes — check the bit rather than
probing.

They are diagnostic commands, not an index. A `LIST_KEYS` is a linear scan of
the keyspace; nothing on a hot path should call either one, and no application
feature should be built on them.

**The two share a request body and a response body, field for field.** They
differ only in what the entries name. One decoder, one pagination loop, one set
of tests.

**Request body** — a 12-byte header, then two optional variable-length fields:

| Offset | Field |
|---|---|
| 0 | `limit` u32 — entries per page, 1–1024 |
| 4 | `cursor_len` u16 |
| 6 | `pattern_len` u16 |
| 8 | `reserved` u32 — zero |
| 12 | `cursor` bytes[`cursor_len`] — empty starts from the beginning |
| 12 + `cursor_len` | `pattern` bytes[`pattern_len`] |

**Bytes after the pattern are `BAD_REQUEST` (3).** Extension happens through
`reserved`; silently ignoring a trailing field would let a client believe
something took effect that this build never read.

A `limit` of 0 or above 1024 is `BAD_REQUEST` rather than clamped — a client
that asked for 10000 and silently got 1024 would page incorrectly. `pattern_len`
may not exceed 511, and `cursor_len` may not exceed 519.

**Response body** on `OK`:

| Offset | Field |
|---|---|
| 0 | `count` u32 — entries in this page |
| 4 | `flags` u8 — bit 0 `TRUNCATED` |
| 5 | `reserved` u8 × 3 — zero |
| 8 | `scanned` u64 — entries examined to produce this page |
| 16 | `cursor_len` u16 — **0 when the listing is complete** |
| 18 | `reserved` u16 — zero |
| 20 | `cursor` bytes[`cursor_len`] |
| 20 + `cursor_len` | `count` × entry |

An entry is `version` u64, `name_len` u16, `name` bytes — **`TAG_SYNC`'s entry
layout, byte for byte**, so a client that decodes a gossip digest reuses that
code here.

`version` is the u64 the server holds for that name: the record's CAS token for
a key, the tag's generation for a tag. Both are opaque monotonic version
numbers, comparable against an earlier reading of the *same* name and against
nothing else. Diffing two listings by version is how a client sees what changed.

`scanned` counts everything walked, including dead and non-matching entries: a
page of 10 keys that cost 90000 records to find is how an operator learns a
pattern is not selective. `TRUNCATED` says the page ended on the server's scan
budget rather than on `limit`. It is **diagnostic only** — paging behaves
identically either way, because a budget exhaustion still advances the cursor.

#### Paging

**An empty cursor in the reply means the listing is complete, and that is the
whole termination rule.** There is no `MORE` flag, because a flag beside a field
that is present exactly when there is more is one of the two lying eventually.

The client loop is: send, consume the entries, resend with the cursor you were
given, stop when it comes back empty. **Expect an empty last page** — a page
that fills `limit` exactly and happens to have consumed the last entry still
returns a cursor, because the server would have to walk one entry further to
know otherwise. `count` may also be less than `limit` on a page that is not the
last.

**A cursor is opaque.** Never parse one, never construct one, never compare two:
echo back exactly the bytes you were given. It is a saved position, not a handle
— there is no server-side state, nothing to expire, and it survives a server
restart. A malformed cursor is `BAD_REQUEST` (3), never a silent restart from
the beginning.

**Consistency.** A page is a snapshot of one shard at one instant; a sequence of
pages is a snapshot of nothing. Do not infer a total from a listing, and do not
read a key's absence from a page as evidence it does not exist. What *is*
guaranteed, because resumption is by name rather than by count: **an entry that
exists unchanged for the whole walk is returned exactly once.** Entries created
behind the cursor are missed and ones created ahead of it are seen, but neither
shifts the position of anything else.

Order is shard-major, then key order within a shard — which is plain
lexicographic order only when `shards = 1`. `LIST_TAGS` is sorted by name
throughout, since the registry is in RAM and comparing two nodes' tag listings
is a legitimate use.

#### Patterns

A byte-wise glob with two tokens and an escape, and deliberately nothing else:

| Token | Meaning |
|---|---|
| `*` | any run of bytes, including empty |
| `?` | exactly one byte |
| `\x` | the literal byte `x`, for any `x` — this is how `*`, `?` and `\` are matched literally |
| any other byte | itself |

An empty pattern matches everything. Matching is byte-wise: no case folding, no
UTF-8 interpretation, no character classes — `[a-z]` is five literal bytes. A
pattern ending in a lone `\` is `BAD_REQUEST` (3) at decode time rather than a
pattern that matches nothing.

**Statuses:** `OK`; `BAD_REQUEST` (3) for a limit out of range, a malformed
pattern or cursor, trailing bytes, or a short body; `UNAUTHORIZED` (5) when
disabled; `INTERNAL` (9). **Never `NOT_FOUND`** — a pattern matching nothing is
`count = 0`, because no matches is not a miss.

Value, TTL, size and tag names are **deliberately not in the reply**. A listing
carrying them is `GET_MANY` with extra steps and a frame size set by the data
rather than by the request; list, then fetch what you care about.

## Worked example

Bytes on the wire for a handshake, a write and a read. `→` is client-to-server.
These are the actual bytes a freshly started server exchanges, so a client can
be checked against them directly — the CAS token is 1 because it is the first
write to an empty store.

```
→ HELLO, request_id 1
  01 00 00 00  01 00 00 00  04 00 00 00     header: opcode 0x01, flags 0, status 0, id 1, body 4
  01 00 00 00                               version 1, reserved 0

← 01 01 00 00  01 00 00 00  12 00 00 00     flags 0x01 = RESPONSE, status 0, body 18
  01 00                                     protocol_version 1
  01 00                                     shards 1
  ff 01 00 00                               max_key_len 511
  00 00 10 00                               max_value_len 1048576
  43 00 00 00                               capabilities TAGS|MEMCACHED|RESP
  20 00                                     max_tags_per_record 32

→ SET "foo" = "bar", ttl 300, request_id 2
  11 00 00 00  02 00 00 00  12 00 00 00     opcode 0x11, body 18
  2c 01 00 00                               ttl_secs 300
  03 00                                     key_len 3
  00                                        tag_count 0
  00                                        reserved
  03 00 00 00                               value_len 3
  66 6f 6f                                  "foo"
  62 61 72                                  "bar"

← 11 01 00 00  02 00 00 00  08 00 00 00     status 0, body 8
  01 00 00 00 00 00 00 00                   cas 1

→ GET "foo", request_id 3
  10 00 00 00  03 00 00 00  03 00 00 00     opcode 0x10, body 3
  66 6f 6f                                  "foo"

← 10 01 00 00  03 00 00 00  0f 00 00 00     status 0, body 15
  00 00 00 00                               mc_flags 0
  01 00 00 00 00 00 00 00                   cas 1
  62 61 72                                  "bar"

→ GET "nope", request_id 4
← 10 01 01 00  04 00 00 00  00 00 00 00     status 1 = NOT_FOUND, empty body
```

## Client implementation checklist

Requirements. A client that gets one of these wrong is wrong on the wire, not
merely slow.

- Send `HELLO` first; nothing else is accepted as an opening frame.
- Buffer inbound bytes; a frame may arrive split across reads, and several may
  arrive in one. Read the 12-byte header, then wait for `body_len` more.
- Reject an inbound `body_len` above 64 MiB and close the connection.
- Correlate by `request_id`. Do not assume replies arrive in order.
- Treat unknown status codes as failures, not as protocol errors.
- Treat unknown *opcodes* in a reply as correlatable: the raw request byte is
  echoed even when the server does not recognise it.
- Enforce `max_key_len`, `max_value_len` and `max_tags_per_record` from the
  handshake locally.
- Expect no reply at all for `NO_REPLY` requests, including failures.
- Expect a reply to `AUTH` even under `NO_REPLY`; it is the one opcode that
  overrides the flag.
- Write `reserved` fields as zero and ignore them on read. Send exactly the
  bytes a body specifies: `AUTH` and the two listing bodies reject trailing
  bytes with `BAD_REQUEST`, and the others ignoring them today is not a promise.
- Do not pipeline anything but `HELLO` and `AUTH` before authentication
  completes — see [pre-authentication limits](#pre-authentication-limits).

## Recommendations

Not requirements. These are the things that decide whether a client is fast and
survivable, written down because each one has a reason in how the server is
built rather than in general good taste.

### Pipelining is where the throughput is

**Whatever complete frames arrive in a single socket read cross to the storage
tier together, in one thread handoff.** That handoff costs far more than
executing a cached request: before it was amortised, one handoff per frame
capped a pipelined connection at roughly 5k ops/s on Windows no matter how deep
the pipeline, because the depth bought nothing. The number is platform-specific;
the shape of the cost is not.

The consequence for a client is direct: **coalesce outbound frames into one
write.** Ten frames in one `write` are one handoff; ten frames in ten writes are
probably ten. An unpipelined client pays the handoff per request and will not
approach the server's ceiling however many connections it opens.

- Buffer outbound frames and flush once per event-loop turn, or once per
  batch the caller submitted, rather than writing each frame as it is built.
- Set `TCP_NODELAY`. The server sets it on its side; Nagle on yours will batch
  a request against the next one and add up to 40 ms for nothing.
- Prefer `GET_MANY`/`SET_MANY`/`DELETE_MANY` over N single-key frames. One
  frame, one handoff, one transaction per shard, and — for `GET_MANY` — one
  consistent snapshot per shard rather than N unrelated ones.

If the server has `store.inline_reads` enabled (it is off by default), a block
in which **every** buffered frame is a read runs on the network worker and skips
the handoff entirely. A single write mixed in sends the whole block down the
slow path. A client that can group a burst of reads separately from its writes
gets that for free; one that cannot loses nothing it had.

### Connections

- **Pool and keep them.** There is no idle timeout for an authenticated
  connection, and accept is not on the server's hot path — it expects a handful
  of connections that live for the life of the process, not a connection per
  request.
- **One in-flight map per connection**, keyed by `request_id`. A wrapping
  counter is fine; ids need only be distinct among the requests currently in
  flight on that connection.
- **A timed-out request does not require tearing the connection down.**
  Correlation is by id, so a late reply can be discarded. Bound the in-flight
  map anyway, and drop the connection if it grows past what you are willing to
  hold.
- **Reconnect on EOF rather than treating it as fatal.** A drain, the connection
  cap and every pre-auth limit all present as a bare disconnect.
- On reconnect, re-run the whole opening sequence: `HELLO`, then `AUTH` if
  `AUTH_REQUIRED` is set. There is no session to resume.
- Do not attempt to switch dialect on a live connection. The first byte settles
  it permanently; speaking VCP means opening with `HELLO`.

### Correctness traps worth designing around

- **`NO_REPLY` cannot tell you anything.** A rejected write is invisible to the
  client and only logged server-side. Use it for writes whose loss you can
  tolerate, and do not use a later reply as a completion barrier for earlier
  `NO_REPLY` frames — replies are not ordered against them by contract.
- **CAS is per key.** Tokens are unique server-wide and strictly increasing for
  any one key, and say nothing across keys. Never treat one as a clock or a
  sequence number for the store.
- **Batch atomicity is per shard.** Read `shards` from the handshake. Above 1, a
  `SET_MANY` spanning shards is several commits and a failure in one leaves the
  others applied; below it, the batch is all-or-nothing. The key-to-shard
  mapping is not part of this specification — do not try to compute it. If you
  need batch atomicity, that is a deployment decision (`shards = 1`), not
  something a client can arrange.
- **Enforce the handshake's limits locally.** Discovering `max_value_len` by
  sending a value and reading `TOO_LARGE` costs a round trip and, with
  `NO_REPLY`, silently drops the write. The same goes for
  `max_tags_per_record`, whose refusal is a bare `BAD_REQUEST` that does not
  say what the limit was.
- **TTL is an offset at every magnitude on VCP**, unlike memcached's `exptime`.
  If your client also speaks the memcached dialect, do not share the TTL
  conversion between the two.
- **Keep the tag vocabulary small and bounded.** Names are registered on first
  use, the registry is capped (`store.tags.max_tags`, default 100000) and
  **nothing removes a tag today** — a flush does not, and neither does deleting
  every record that carried it. A tag per user or per request will fill the
  registry and start answering `CAPACITY_FULL` (7). Tags are for groups of keys
  invalidated together, and a client library should make that hard to misuse.
- **If the `CLUSTER` capability bit is clear, an invalidation stops at this
  node.** A client that needs cluster-wide invalidation must send
  `DELETE_BY_TAG` to every node itself.

### Checking a client against this document

The [worked example](#worked-example) above is real bytes and can be diffed
directly. Beyond it, `vash-proto`'s `emit_vectors` example emits a conformance
corpus — request and response frames with their decoded fields — generated from
the server's own encoders:

```
cargo run -p vash-proto --example emit_vectors -- <output-dir>
```

Generating the corpus rather than writing it by hand is the point: a hand-written
one encodes its author's reading of this document, which is the reading the
corpus exists to check.

---

# Memcached compatibility

vash speaks the classic text protocol and the meta commands. The **legacy
binary protocol (magic `0x80`) is not implemented and will not be** — upstream
deprecated it in favour of the meta commands.

Served by default, and turned off with `protocol.memcached_enabled = false` or
`--disable-memcached`, in which case a connection opening with a memcached
command is closed unanswered — see
[First-byte detection](#first-byte-detection).

Compatibility is checked in CI two ways: a real client library
(`pymemcache`) driven against both vash and real memcached, and a byte-for-byte
differential that sends identical command sequences to both and compares raw
responses. The differential's reference is a pinned Docker image rather than
whatever the runner has installed, and it covers Redis too. See
`tests/compat/docker_differential.py`.

## Limits

| | vash | Notes |
|---|---|---|
| Key length | 250 bytes | memcached's limit, enforced even though the storage engine allows 511. |
| Key charset | no spaces, no control bytes, no `0x7f` | Same as memcached. |
| Value size | 1 MiB default, configurable | `SERVER_ERROR object too large for cache` past it. |
| Command line | 16 KiB | Connection closed past it. |
| Keys per `get` | 4096 | |

## Classic commands

All are implemented with upstream semantics.

| Command | Responses |
|---|---|
| `get <key>+` | `VALUE <key> <flags> <bytes>\r\n<data>\r\n` per hit, then `END` |
| `gets <key>+` | as `get`, with `<cas>` appended to each `VALUE` line |
| `gat <exptime> <key>+` | as `get`; also re-stamps the TTL |
| `gats <exptime> <key>+` | as `gets`; also re-stamps the TTL |
| `set <key> <flags> <exptime> <bytes> [noreply]` | `STORED` |
| `add …` | `STORED` / `NOT_STORED` |
| `replace …` | `STORED` / `NOT_STORED` |
| `append …` | `STORED` / `NOT_STORED` |
| `prepend …` | `STORED` / `NOT_STORED` |
| `cas <key> <flags> <exptime> <bytes> <cas> [noreply]` | `STORED` / `EXISTS` / `NOT_FOUND` |
| `delete <key> [noreply]` | `DELETED` / `NOT_FOUND` |
| `touch <key> <exptime> [noreply]` | `TOUCHED` / `NOT_FOUND` |
| `incr <key> <delta> [noreply]` | the new value / `NOT_FOUND` / `CLIENT_ERROR cannot increment or decrement non-numeric value` |
| `decr <key> <delta> [noreply]` | as `incr`; clamps at zero |
| `flush_all [delay] [noreply]` | `OK`, or `CLIENT_ERROR` when disabled |
| `stats [<section>]` | `STAT <name> <value>` lines, then `END` — see below |
| `lru_crawler metadump <all\|hash\|1>` | `OK`, then `key=…` lines, then `END` |
| `lru_crawler mgdump <all\|hash\|1>` | `OK`, then `mg <key>` lines, then `EN` |
| `version` | `VERSION <string>` |
| `verbosity <level> [noreply]` | `OK` — accepted and ignored |
| `quit` | connection closes, no response |

Storage commands are followed by exactly `<bytes>` of data and then `\r\n`. The
framing is **length-delimited, not line-delimited**: a value may contain `\r\n`.

`append` and `prepend` keep the existing item's client flags and TTL; the
`<flags>` and `<exptime>` on their command line are ignored, as upstream does.

`incr` wraps at 64 bits; `decr` clamps at zero.

### Authentication

When the server requires it, a connection authenticates with upstream's ASCII
mechanism (memcached's `-Y authfile`): a `set` whose key is the username and
whose data block is `<user> <pass>`.

```text
set billing-api 0 0 44\r\n
billing-api 0f1e2d3c4b5a69788796a5b4c3d2e1f0\r\n
→ STORED
```

It is an ugly shape — credentials tunnelled through a storage command — but it
is the only one memcached clients implement, and compatibility is why this
dialect exists here. Nothing is stored either way.

| Reply | When |
|---|---|
| `STORED` | The credential was accepted. |
| `CLIENT_ERROR authentication failure` | Wrong secret, unknown name, or a block naming a different identity. |
| `CLIENT_ERROR bad authentication token format` | The block is not `<user> <pass>`. |
| `CLIENT_ERROR unauthenticated` | Any other command — **including an unknown verb, a malformed command line, or outright garbage**, so a stranger cannot probe which commands the server understands. |

Before authenticating, the only other command accepted is `quit`. Everything
else, meta commands included, answers `CLIENT_ERROR unauthenticated`. The meta
protocol has no authentication command of its own upstream, so a meta-only
client must send the classic `set` first.

These replies are checked byte for byte against `memcached:1.6-alpine` in
`tests/compat/docker_differential.py`. Two upstream behaviours are deliberately
not copied — see [auth.md §7](auth.md#7-memcached).

A refused storage command still consumes its declared data block, exactly as
[stream resynchronisation](#stream-resynchronisation) requires — so a client
pipelining through the gate gets one error line per command, not one per line of
a value.

**The binary protocol and its SASL commands are not implemented and will not
be.** SASL lives only in the binary protocol upstream, so supporting it would
mean adding the third parser this project decided not to have.

### `stats`

A subset of memcached's counters — only what is actually measured — plus
vash's own under a `vash_` prefix. Nothing is reported as a plausible zero
just to fill the field out.

`pid`, `version`, `pointer_size`, `uptime`, `time`, `max_connections`,
`curr_connections`, `total_connections`, `rejected_connections`,
`accepting_conns`, `cmd_get`, `cmd_set`, `cmd_touch`, `cmd_flush`, `cmd_meta`,
`get_hits`, `get_misses`, `delete_hits`, `delete_misses`, `incr_hits`,
`incr_misses`, `decr_hits`, `decr_misses`, `cas_hits`, `cas_misses`,
`cas_badval`, `touch_hits`, `touch_misses`, `total_items`, `store_too_large`,
`store_no_memory`, `auth_cmds`, `auth_errors`, `bytes_read`, `bytes_written`,
`curr_items`, `bytes`, `limit_maxbytes`, `evictions`, and: `vash_commands`,
`vash_reads`, `vash_writes`, `vash_shards`, `vash_utilisation`,
`vash_expiry_entries`, `vash_tags`, `vash_tag_index_entries`,
`vash_pending_reclaims`, `vash_commits`, `vash_committed_ops`,
`vash_mean_batch`, `vash_sweeps`, `vash_reclaimed`, `vash_tag_reclaimed`,
`vash_sweep_lag_ms`, `vash_epoch`, `vash_readers_in_use`,
`vash_oldest_reader_age_ms`, `vash_cluster_mode`, `vash_cluster_peers`,
`vash_cluster_peers_reachable`.

`cmd_get` counts retrievals, `get_hits` counts keys — a multi-get adds one to
the first and several to the second, which is upstream's own arithmetic.

**Absent because nothing measures them**: `threads`, `reclaimed`,
`get_expired`, `get_flushed`, `expired_unfetched`, `evicted_unfetched`,
`evicted_active`, `rusage_user`, `rusage_system`, `libevent`,
`connection_structures`, `reserved_fds`, `conn_yields`, `hash_power_level`,
and every `slab_*`, `lru_*`, `log_*` and `proxy_*` counter.

`reclaimed` is the one worth explaining. Upstream's counts entries stored into
the memory of an expired one — a slab-reuse number — where this server's
sweeper reclaim count is a different quantity that happens to share the word. It
is reported as `vash_reclaimed`, and an integration test refuses any field name
that claims an upstream name without being on a reviewed list.

### `stats` subcommands

The specification declines to document these at all — "the kinds of arguments
and the data sent are not documented in this version of the protocol, and are
subject to change for the convenience of memcache developers" — so the
**framing** is matched byte for byte against memcached 1.6.45 and the **field
list** is deliberately a subset. Full tables in
[stats-subcommands.md](stats-subcommands.md).

| Subcommand | Answer |
|---|---|
| `stats settings` | The configuration in force. `flush_enabled` and `dump_enabled` match upstream's meaning exactly; the slab, LRU, SSL and extstore geometry is absent. |
| `stats items` | One synthetic class, `1` — the same id `lru_crawler metadump` prints as `cls=`, so the discover-then-dump loop every tool runs works. |
| `stats slabs` | The per-class command counters, `used_chunks`, `active_slabs` and `total_malloced`. The rest of the chunk geometry is absent: a page here holds records of many sizes, and reporting one would let a tool compute a slab efficiency that means nothing. `used_chunks` is exact — upstream allocates one chunk per item, and one record here is one unit of storage in use — and has no denominator left to be divided by. |
| `stats conns` | One block per open connection, plus the listener. `<id>` is a **monotonic connection id, not a file descriptor** — an fd is reused the moment one closes, so the same number can mean two clients a second apart. |
| `stats sizes` | `STAT sizes_status disabled`, which is **byte-identical** to a stock memcached: upstream tracks item sizes only under `-o track_sizes`. |
| `stats extstore`, `stats proxy` | A bare `END`, which is what a memcached built without them answers. There is neither here. |
| `stats cachedump <class> <limit>` | `ITEM <key> [<size> b; <exp> s]` lines, then `END`. **`size` is always `0`** — see below. |
| `stats reset`, `stats detail` | `CLIENT_ERROR stats <name> is not implemented`. See below. |
| `stats sizes_enable`, `stats sizes_disable` | `ERROR` — 1.6.45 removed both verbs. |
| anything else | `ERROR`, upstream's answer for a subcommand it does not recognise. |

`stats conns` reports no `state` for a live connection. Upstream's ten values
name positions in an event-loop state machine that does not exist here — a
connection is an async task — so `vash_dialect` and `vash_authenticated` are
offered instead, which answer the question an operator was actually asking. The
listener does carry `state conn_listening`, which is the one value that is
unambiguous.

#### `stats cachedump`

```text
stats cachedump 1 10
→ ITEM __MEM_INDEX_01__ [0 b; 1786683924 s]
  ITEM teste2 [0 b; 0 s]
  ITEM teste [0 b; 0 s]
  END
```

Upstream's older key dump, superseded there by `lru_crawler metadump` — which is
also served here, carries more per key, and pages the whole keyspace where this
returns one page. Both arguments are required. A class other than `1` holds
nothing and answers a bare `END`; a **limit of `0` means no limit**, not
"nothing", and is capped at 1024 like every other page.

`exp` is an absolute unix timestamp and **`0` means "never expires"** — note
that `metadump` spells the same thing `-1`. That asymmetry is upstream's and is
reproduced rather than tidied.

**`size` is always `0` and must not be read.** It is the one field in this
server that is not a measurement. The field cannot be dropped — this is a
positional bracket format, unlike `metadump`'s `key=value` pairs, so a parser
would break — and carrying a real length would put a value length on every
listing entry that the native protocol pays for and never reads. `mg <key> s`
answers the size of one key without that.

Keys are percent-encoded only where they would otherwise break the line, so
every key a memcached client could have written appears byte-identically to
upstream. Upstream never needs to encode, because its own parser refuses to
store a key with a space or a CRLF in it; this keyspace is shared with Redis and
VCP clients that can.

One place this server is *more* complete than upstream: `cachedump` there walks
only the COLD segment of a class's LRU, so a freshly written key does not appear
until the maintainer thread has moved it. There is no LRU here, so every live
key is dumped.

#### The two refusals

`stats reset` would leave `stats` and `/metrics` reporting different numbers for
the same counter, and `/metrics` over a time range answers the question better.
`stats detail` is the only subcommand that would put work on the retrieval hot
path, and upstream ships it disabled for that reason. Named rather than lumped
into `ERROR`, so "deliberately not built" stays distinguishable from "not
recognised".

### `lru_crawler`

The key listing of this dialect, and — like `LIST_KEYS`, `LIST_TAGS` and Redis
`SCAN` — behind `protocol.listing_enabled`, which is **off by default**. When
it is clear, both dumps answer `CLIENT_ERROR command disabled by
configuration`.

```text
lru_crawler metadump all
→ OK
  key=session%3A0001 exp=1786605851 cas=41 cls=1
  key=session%3A0002 exp=-1 cas=57 cls=1
  END

lru_crawler mgdump all
→ OK
  mg session%3A0001
  mg session%3A0002
  EN
```

One walk, two renderings. `mgdump` emits a ready-to-send `mg` command per key,
so a dump is its own replay script — which is also why it ends with the meta
protocol's `EN`.

**Framing, matched to upstream byte for byte** (verified against
`memcached:1.6-alpine`): an `OK` acknowledgement before any key, `metadump`
lines ending in a **trailing space and a bare `\n`**, `mgdump` lines ending in
CRLF, and the terminator last.

**Class ids.** There are no slab classes here, so everything is in class 1 and
`all`, `hash` and `1` are three spellings of one dump. Any other class holds
nothing and answers a bare terminator. A *missing* class is `ERROR`, which is
upstream's answer too.

**Keys are percent-encoded.** The keyspace is shared across dialects, so a Redis
or VCP client can store a key containing a space or a CRLF — which no memcached
client could have written, and which would otherwise end a dump early and let
the rest of the key be read as further lines. Encoding makes that inert with no
keys skipped. Upstream encodes `%` in `metadump` and not in `mgdump`, and that
asymmetry is reproduced: an `mg` line has to name the key it will look up.

**Fields.** Upstream guarantees only that a metadump line "will include at
least" `key`, `exp`, `la`, `cas` and `fetch`. Of those, `la` and `fetch` are
LRU bookkeeping and there is no LRU here (plan §6), so they are omitted rather
than zeroed — a `la=0` would claim every key was last touched at the epoch.
`size` and `flags` are not in the guaranteed set and are absent too; `mg <key>
s f` answers both per key.

**Paging and truncation.** The grammar has no cursor, so the server pages
internally, spending `protocol.listing_max_scan` across as many pages as it
needs. Each page opens and closes its own read transaction, so nothing pins a
snapshot for the length of the dump. A dump that exhausts the budget ends with

```text
SERVER_ERROR dump exceeded the scan budget; use SCAN or LIST_KEYS to page the keyspace
```

**in place of its terminator** — a reader consumes lines until `END`/`EN`, so a
truncated dump that ended in one would report a keyspace smaller than the real
one. Use `LIST_KEYS` or Redis `SCAN` for a keyspace larger than the budget.

The remaining `lru_crawler` subcommands — `enable`, `disable`, `sleep`,
`tocrawl`, `crawl` — steer a background LRU crawler and answer
`CLIENT_ERROR lru_crawler <name> is not implemented`. Upstream implements them,
so the bytes diverge whatever is sent; naming the refusal is worth more than a
shorter divergence.

## Meta commands

| Command | Purpose | Success | Miss |
|---|---|---|---|
| `mn` | no-op / batch marker | `MN` | — |
| `mg <key> <flags>*` | get | `HD <rflags>` or `VA <size> <rflags>\r\n<data>` | `EN` |
| `ms <key> <datalen> <flags>*` | set | `HD <rflags>` | `NS` / `EX` / `NF` |
| `md <key> <flags>*` | delete | `HD <rflags>` | `NF` |
| `ma <key> <flags>*` | arithmetic | `HD <rflags>` or `VA <size>\r\n<data>` | `NF` |
| `me <key>` | item debug | `ME <key> cas=<n> size=<n> fetch=yes` | `EN` |

`ms` is followed by exactly `<datalen>` bytes of data and `\r\n`.

### Supported flags

| Flag | On | Meaning |
|---|---|---|
| `v` | mg, ma | Return the value (`VA` instead of `HD`). |
| `f` | mg | Return client flags as `f<n>`. |
| `c` | mg, ms | Return the CAS token as `c<n>`. |
| `t` | mg | Return remaining TTL in seconds as `t<n>`; `t-1` means no expiry. |
| `s` | mg | Return the value size as `s<n>`. |
| `k` | mg, ms, md, ma | Echo the key as `k<key>`. |
| `O<token>` | all | Opaque token, echoed as `O<token>`. |
| `q` | all | Quiet: suppress the response **entirely**, including a hit on `mg`. See the divergence below. |
| `u` | mg | Do not bump the LRU. Inert here — there is no LRU. |
| `T<ttl>` | mg, ms, md | Set the TTL. On `mg` this makes it a get-and-touch. |
| `F<flags>` | ms | Set the client-flags field. |
| `C<cas>` | ms | Compare against this CAS token. Outranks `M`. |
| `M<mode>` | ms, ma | Mode; see below. |
| `D<delta>` | ma | Amount to add or subtract. Default 1. |
| `G<tags>` | ms | **Extension.** Comma-separated tag list. |

`ms` modes: `E` add, `A` append, `P` prepend, `R` replace, `S` set (default).
`ma` modes: `I`/`+` increment (default), `D`/`-` decrement.

Return flags appear in a **fixed order regardless of request order**: `f`, `s`,
`c`, `t`, `k`, `O`. A client must not assume they come back in the order it
asked for them.

```
ms doc:1 5 F9 T120 Gnews,sport
hello
HD
mg doc:1 v f s c t k Oop7
VA 5 f9 s5 c1 t120 kdoc:1 Oop7
hello
```

### Refused flags

These are defined by upstream but **not implemented here, and rejected with
`CLIENT_ERROR unsupported flag`** rather than ignored:

`b` (base64 key), `h` (hit-before), `l` (last-access), `x` (remove value only),
`I` (invalidate), `E` (set CAS), `R` (recache), `N` (vivify on miss).

Refusing is deliberate. Silently ignoring `b` would file the value under the
un-decoded key; ignoring `h` or `l` would return fewer tokens than the client is
parsing. Any other unrecognised flag gets `CLIENT_ERROR invalid flag`.

## Extensions

Two commands and one flag are **not part of the memcached protocol**. Clients
that never use them are unaffected; the `TAGS` capability bit in the VCP
handshake and the `vash_tags` stat both indicate support.

| | Form | Response |
|---|---|---|
| Attach tags | `ms <key> <datalen> G<tag>[,<tag>…]` | as `ms` |
| Invalidate (meta) | `mdt <tag> [q]` | `HD` / `NF` |
| Invalidate (classic) | `delete_by_tag <tag> [noreply]` | `DELETED` / `NOT_FOUND` |

`G` was chosen from the letters upstream leaves unassigned. It is a single
constant in the source (`memcached::meta::TAG_FLAG`), so it can be moved if
upstream ever claims it.

```
ms article:1 5 Gnews,sport
value
HD
mdt news
HD
mg article:1 v
EN
```

## Errors

| Line | Meaning |
|---|---|
| `ERROR` | Unknown command. |
| `CLIENT_ERROR <reason>` | The request was malformed. |
| `SERVER_ERROR <reason>` | The server could not comply. |

The wording matches upstream verbatim where upstream has one, because the
differential suite compares response bytes. Notably, every malformed command
line — missing key, over-long key, unparseable number, empty line — reports
`CLIENT_ERROR bad command line format`, not a more specific message.

### Stream resynchronisation

A rejected storage command still consumes its declared data block whenever the
`<bytes>` token is readable. This matters: if the block were left in the stream
it would be parsed as commands, and every request after it on that connection
would be misread. Client authors relying on pipelining should expect exactly one
error line for such a command, not one per stray line of the value.

If `<bytes>` itself is unreadable, only the command line is consumed — the
server has no way to know how much to skip.

## Deliberate divergences from memcached

Everything else is byte-identical in the differential suite; these are the
exceptions.

| Behaviour | memcached | vash | Why |
|---|---|---|---|
| Over-long key on `get` | error line, then a stray empty line | error line only | The extra line leaves a pipelining client counting one more response than it sent commands. |
| `flush_all` | always available | disabled unless enabled in config | It empties the cache for anyone who can reach the port. |
| `flush_all <delay>` | defers the flush | delay parsed and ignored; flush is immediate | A deferred wipe needs a scheduler; every client that sends one sends 0. |
| `decr` below the stored width | rewrites in place and pads the value with trailing spaces, so `100` decremented by `95` reads back as `5␠␠` | stores `5`, at its own length | The padding is an artefact of updating an item in place without resizing it. Both reply `5`, and both parse back to `5`; only a client comparing raw bytes can tell. |
| `mg … q` on a **hit** | returns the value; `q` suppresses only the `EN` of a miss | suppresses the whole reply, hit included | `q` is one `noreply` flag across every meta command here. It is not covered by the differential suite. **Do not use `q` with `mg` against vash** — the quiet-get-then-`mn` batching idiom returns nothing. |
| `me` output | full internal item dump | `cas`, `size`, `fetch` only | The rest describes internals vash does not have. |
| `stats` fields | full counter set | a subset, plus `vash_*` | Reporting an unmeasured counter as zero would mislead a dashboard. |
| `stats reset` / `detail` | implemented | `CLIENT_ERROR stats <name> is not implemented` | See [`stats` subcommands](#stats-subcommands). |
| `stats cachedump` item size | the item's real size | always `0` | The field cannot be dropped from a positional format, and carrying a length would put one on every listing entry the native protocol pays for and never reads. `mg <key> s` answers it per key. |
| `stats cachedump` coverage | the class's COLD LRU segment only | every live key in the class | There is no LRU here. Upstream's omits a key until its maintainer thread has moved it. |
| `stats items` / `slabs` classes | one per slab class | one synthetic class, `1` | There is no slab allocator. The id matches what the dumps print as `cls=`, so tooling's discover-then-dump loop still works. |
| `stats conns` identifiers | file descriptors, reused | monotonic connection ids | An fd is reused the moment one closes, so the same number can mean two clients a second apart. |
| `stats conns` `state` | ten event-loop states | absent; `vash_dialect` instead | A connection here is an async task, not a position in a state machine. |
| `lru_crawler` availability | always | behind `listing_enabled`, off by default | Keyspace enumeration is the same capability whichever dialect asks. |
| `lru_crawler` other subcommands | `enable`, `disable`, `sleep`, `tocrawl`, `crawl` | `CLIENT_ERROR lru_crawler <name> is not implemented` | They steer a background LRU crawler; plan §6 rejected an on-disk LRU, so there is nothing to crawl. |
| A dump pipelined behind other commands | `ERROR cannot pipeline other commands before metadump` | accepted | The dump is buffered and appended in order here, so pipelining is not a problem to refuse. Strictly more permissive. |
| Metadump `la=`, `fetch=`, `size=`, `flags=` | present | omitted | The first two are LRU bookkeeping and there is no LRU; the last two are outside upstream's guaranteed field set and `mg <key> s f` answers both. |
| Dump truncation | not reachable | `SERVER_ERROR …` **in place of** the terminator | A reader consumes lines until `END`; a truncated dump ending in one would report a keyspace smaller than the real one. |
| Meta flags `b h l x I E R N` | implemented | `CLIENT_ERROR unsupported flag` | See [Refused flags](#refused-flags). |
| Eviction under memory pressure | LRU | TTL-ordered | See [plan.md](plan.md) §6. |

---

# Redis compatibility

A subset of the Redis string and expiry commands, enough for a cache, plus
`SCAN` and `INFO` for introspection. There are no lists, hashes, sets, sorted
sets, streams, transactions, scripting, pub/sub, `SELECT` or replication
commands, and there never will be — see [plan.md](plan.md) §16.

**Tags reach this dialect through three commands that Redis does not have** —
`SETTAGS`, `MSETTAGS` and `DELBYTAG`, described [below](#tag-commands). They are
extensions, exactly as `ms … G<tag>` and `mdt` are in the memcached dialect, and
a client that never sends them is unaffected. Why they are new verbs rather than
options on `SET`, and what was rejected on the way there, is in
[resp-tags.md](resp-tags.md).

Served by default, and turned off with `protocol.resp_enabled = false` or
`--disable-resp`, in which case a connection opening with a RESP array is closed
unanswered — see [First-byte detection](#first-byte-detection).

## Framing

Requests are RESP arrays of bulk strings, exactly as the protocol specifies:

```text
*<argc>\r\n$<len>\r\n<arg>\r\n$<len>\r\n<arg>\r\n…
```

Command names and option tokens are ASCII and case-insensitive. Arguments are
binary-safe and length-delimited, so a value may contain CRLF.

- `*0\r\n` is accepted and skipped, as Redis does.
- **Inline commands are not supported.** See
  [First-byte detection](#first-byte-detection).
- An argument may be up to 64 MiB and a request may carry up to 8200 arguments.
  Both are checked before anything is allocated. The parser's ceiling is not the
  store's: an argument between `store.max_value_bytes` (1 MiB by default) and
  64 MiB parses fine and is then refused with `-ERR string exceeds maximum
  allowed size`.

## RESP2 and RESP3

Both, on the same connection, negotiated with `HELLO`. A connection starts at
RESP2 and moves to RESP3 when the client sends `HELLO 3`; there is no way back,
and no client asks for one.

Requests are identical in the two dialects, so only replies differ. For this
command set that is three things:

| | RESP2 | RESP3 |
|---|---|---|
| Null (a miss, a skipped `SET`) | `$-1\r\n` | `_\r\n` |
| `HELLO` reply | flat array of 14 items | map of 7 pairs |
| `INCREX` in `BYFLOAT` mode | bulk strings | doubles (`,1.75\r\n`) |

`INCRBYFLOAT` answers with a bulk string in **both**, which is Redis's own
inconsistency, not ours.

`HELLO` reports `server: redis` and `version: 7.4.0-vash`. The name is Redis's
because client libraries branch on it and an unfamiliar one sends some of them
down an error path; the suffix is what tells a human what they are talking to.
The reported `id` is always `0` — connections are not registered anywhere, and
there is no `CLIENT` command to use one with.

## Command reference

`numkeys`, options and their arguments follow the Redis documentation exactly
unless noted. Where a command takes options they may appear in any order, and
mutually exclusive ones (`NX`/`XX`, the expiry family) are a syntax error
together rather than last-one-wins.

| Command | Supported form |
|---|---|
| `GET` | `GET key` |
| `SET` | `SET key value [NX \| XX] [GET] [EX s \| PX ms \| EXAT ts \| PXAT ms \| KEEPTTL]` |
| `DEL` / `UNLINK` | `DEL key [key …]` — identical here; reclamation is always the background reclaimer's job |
| `MGET` | `MGET key [key …]` |
| `MSET` | `MSET key value [key value …]` |
| `MSETEX` | `MSETEX numkeys key value [key value …] [NX \| XX] [EX s \| PX ms \| EXAT ts \| PXAT ms \| KEEPTTL]` |
| `SETTAGS` | **Extension.** `SETTAGS key value numtags tag [tag …]`, then any option `SET` takes — see [Tags](#tag-commands) |
| `MSETTAGS` | **Extension.** `MSETTAGS numkeys key value [key value …] numtags tag [tag …]`, then any option `MSETEX` takes |
| `DELBYTAG` | **Extension.** `DELBYTAG tag [tag …]` |
| `EXISTS` | `EXISTS key [key …]` — counts a key once per mention |
| `TYPE` | `TYPE key` — `+string` or `+none`; every value here is a string |
| `EXPIRE` | `EXPIRE key seconds [NX \| XX \| GT \| LT]` |
| `EXPIREAT` | `EXPIREAT key unix-time-seconds [NX \| XX \| GT \| LT]` |
| `PERSIST` | `PERSIST key` |
| `TTL` | `TTL key` — `-2` absent, `-1` no expiry |
| `APPEND` | `APPEND key value` — creates the key, keeps an existing deadline |
| `INCR` / `DECR` | `INCR key` |
| `INCRBY` / `DECRBY` | `INCRBY key increment` |
| `INCRBYFLOAT` | `INCRBYFLOAT key increment` |
| `INCREX` | `INCREX key [BYFLOAT inc \| BYINT inc] [LBOUND lb] [UBOUND ub] [SATURATE] [EX s \| PX ms \| EXAT ts \| PXAT ms \| PERSIST] [ENX]` |
| `SCAN` | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` — see below |
| `INFO` | `INFO [section …]`, plus `all` and `everything` — see below |
| `HELLO` | `HELLO [protover [AUTH username password]]` |
| `AUTH` | `AUTH password` (the `default` identity) or `AUTH username password` |
| `PING` | `PING [message]` |
| `QUIT` | `QUIT` |

Anything else is answered `-ERR unknown command '…'` and the connection carries
on, which is how a client library discovers a feature is missing.

Numbers follow Redis's own `string2ll`, not Rust's `parse`: no `+`, no leading
zeros, no surrounding whitespace. The same rule judges command arguments and
stored counters, so a value `INCR` accepts is exactly a value it can write back.

### `SCAN`

Enumerating a keyspace is the same capability whichever dialect asks for it, so
`SCAN` sits behind `protocol.listing_enabled` alongside `LIST_KEYS` — which is
**off by default**. Until it is enabled, `SCAN` answers `-ERR command disabled
by configuration`.

**The guarantee is stronger than Redis's.** Redis promises that a key present
for the whole iteration is returned *at least* once; because resumption here is
by key rather than by hash bucket, it is returned **exactly** once. Keys created
behind the cursor are missed and keys created ahead of it are seen, as in Redis,
and a sequence of pages is still not a snapshot of anything.

| Option | Behaviour |
|---|---|
| `COUNT` | Defaults to 10, **clamped** to 1024. Redis specifies it as a hint, so clamping is invisible; VCP's `limit` is rejected instead, because a client that asked for 10000 and silently got 1024 would page incorrectly there. |
| `MATCH` | The [pattern](#patterns) syntax — `*`, `?`, `\`. Redis's character classes are **not** supported, and an unescaped `[` is refused by name rather than matched as a literal byte, which would silently match nothing. |
| `TYPE` | Every value here is a string. `TYPE string` scans; anything else answers `["0", []]`, which is true rather than empty — no key can be another type. |

**An empty page with a non-zero cursor is normal** and clients handle it: it is
what a scan budget spent entirely on dead or non-matching records produces.
Iterate until the cursor comes back `0`.

**The cursor is a token, and it lives in this process.** Redis's cursor is a
`u64` and every major client library parses it as one; this store's listing
position is `shard ‖ key`, which does not fit in eight bytes. So the server
holds the position and hands back an integer naming it. Consequences worth
knowing:

- Tokens are **server-wide**, so a pooled client whose next page lands on
  another connection is fine — which is how `scan_iter` and its equivalents
  work.
- At most `protocol.scan_cursors` are live at once and one expires after
  `protocol.scan_cursor_ttl_ms`. Only spent tokens are dropped: the one a live
  iteration needs is always the newest it was handed.
- A token that is gone answers `-ERR scan cursor expired; restart the iteration
  from 0`, **never a silent restart** — which would spin a pager forever without
  saying why.
- **A `SCAN` does not survive a server restart**, where a VCP `LIST_KEYS` cursor
  does: the position is still valid as bytes, but the mapping from the integer
  the client holds is gone with the process.

### `INFO`

`INFO [section …]`, where a section is `server`, `clients`, `memory`,
`persistence`, `stats`, `replication`, `cluster`, `keyspace` or `vash`, matched
case-insensitively. A bare `INFO` prints everything except `vash`; `all` and
`everything` add it. An unrecognised section contributes nothing, and naming
only unrecognised ones answers an empty string — Redis's behaviour, so a client
can probe for a section it may not have.

A bulk string in RESP2 and a **verbatim string** in RESP3, which is what real
Redis sends.

The counters come from the same snapshot memcached's `stats` prints, so the two
commands cannot disagree. Fields Redis reports and this server does not measure
are **absent, not zeroed**: `used_memory_rss`, `mem_fragmentation_ratio`,
`instantaneous_ops_per_sec`, `total_net_input_bytes`, `latest_fork_usec`,
`commandstats`, `latencystats` and the rest.

Three fields are worth calling out because clients act on them:

| Field | Value | Why |
|---|---|---|
| `cluster_enabled` | `0` | A `1` sends a client into Redis Cluster's protocol — `CLUSTER SLOTS`, `MOVED`/`ASK` redirection, hash-slot routing — none of which exists here. vash's clustering is tag invalidation between shared-nothing nodes and is not the same thing under the same name. |
| `role` | `master` | There is no replication, so this is true rather than aspirational. Sentinel-aware clients and health checks read it. |
| `maxmemory_policy` | `volatile-ttl` | The closest true statement in Redis's vocabulary for plan §6's "expired first, then soonest-to-expire". An approximation; see the divergences below. |

`db0` reports `keys=<n>,avg_ttl=0` and **omits `expires`**. Redis's `expires`
counts keys carrying a TTL; the nearest thing measured here is rows in the
expiry index, which has one per record whether or not it expires — a different
quantity, so the field is absent rather than wrong. `avg_ttl` stays at `0`
because that is Redis's own value for "not computed".

`vash_version` reports what this actually is; `redis_version` is a
compatibility claim, since client libraries gate features on it.

`vash_resp_tags:1` is in the `vash` section — a statement, not a counter, and
the way to learn this dialect has tags without sending a write to find out.

## Tag commands

Three commands Redis does not have. They are the only tag surface in this
dialect, and every rule about tags themselves — the limits, the generation
semantics, the ordering guarantee, the cluster behaviour — is the shared one in
[Tags](#tags), unchanged.

| Command | Reply |
|---|---|
| `SETTAGS key value numtags tag [tag …] [NX \| XX] [GET] [EX s \| PX ms \| EXAT ts \| PXAT ms \| KEEPTTL]` | Whatever `SET` would answer: `+OK`, a null when the condition skipped the write, the displaced value with `GET`. |
| `MSETTAGS numkeys key value [key value …] numtags tag [tag …] [NX \| XX] [EX s \| PX ms \| EXAT ts \| PXAT ms \| KEEPTTL]` | Whatever `MSETEX` would answer: `:1`, or `:0` when the guard skipped the batch. |
| `DELBYTAG tag [tag …]` | Integer: how many of the named tags were registered. A name this server has never seen counts zero, exactly as `DEL` counts a key that was not there. |

**`SETTAGS` is `SET` and `MSETTAGS` is `MSETEX`**, each with a counted tag list
between the body and the options. Every option behaves identically, the replies
are identical, and the writes are as atomic as their untagged twins — the tags
are part of the record the shard writer commits, not a second step.

**The tag list is counted, not delimited.** Tag names are binary-safe, so a
comma-separated list — which is what the memcached extension has to use, having
nowhere else to put them — cannot express a name containing a comma. `numtags`
says where the list ends and the options begin, exactly as `MSETEX`'s `numkeys`
says where the pairs do.

- **`numtags 0` is accepted**, unlike `numkeys 0`, and means a write with no
  tags. A batch of no keys is meaningless; a write with no tags is ordinary, and
  a client building a command from a list that turned out empty should not have
  to switch verbs to send it.
- A `numtags` that disagrees with the arguments given is an **arity error**,
  since the count is what says where the list stops.
- The parser refuses a list longer than **255** — the record format's own
  ceiling — before collecting it. `store.tags.max_per_record` is lower and is
  enforced by the store, which answers `-ERR too many tags` either way.

**`DELBYTAG` deletes records, not tags.** The registry keeps a name once it has
seen it, so a tag that has just been invalidated is still a registered name and
`DELBYTAG` on it answers `:1` again. Several names in one command are several
invalidations: each is a constant-time generation bump, they are counted
individually, and a failure part-way through leaves the earlier ones applied.
Retrying is safe — a generation bump is idempotent, which is the same property
that lets a cluster replay them freely.

There is no command to attach a tag to a record that already exists, and none to
read a record's tags. Both are designed in
[resp-tags.md](resp-tags.md#2-decision) and neither is built.

## Errors

| Reply | When |
|---|---|
| `-ERR unknown command '…'` | Outside the subset above. |
| `-ERR wrong number of arguments for '…' command` | Arity. |
| `-ERR syntax error` | Unknown or conflicting options. |
| `-ERR invalid expire time in '…' command` | A non-positive `EX`/`PX`/`EXAT`/`PXAT`, or one that does not fit. |
| `-ERR value is not an integer or out of range` | A stored value or argument that is not an integer. |
| `-ERR value is not a valid float` | The float equivalent. |
| `-ERR increment or decrement would overflow` | `INCR`-family arithmetic past `i64`. |
| `-ERR increment would produce NaN or Infinity` | `INCRBYFLOAT` past the float range. |
| `-ERR invalid key` | Empty, or past this server's 511-byte key limit. |
| `-ERR string exceeds maximum allowed size` | A value past `store.max_value_bytes`. |
| `-ERR numkeys should be greater than 0` / `-ERR too many keys` | `MSETEX` with a non-positive `numkeys`, or more than 4096 pairs. |
| `-ERR numtags should be greater than or equal to 0` | A negative `numtags`. Zero is legal; see [Tags](#tag-commands). |
| `-ERR invalid tag` | A tag name that is empty or longer than 255 bytes. |
| `-ERR too many tags` | More tags on one record than `store.tags.max_per_record`, or more than the format's 255. |
| `-ERR LBOUND must be less than or equal to UBOUND` | `INCREX` with an empty range. |
| `-ERR command disabled by configuration` | A command gated off server-side — `SCAN` with `listing_enabled` clear. |
| `-ERR invalid cursor` | A `SCAN` cursor that is not a non-negative integer. Redis's own wording. |
| `-ERR scan cursor expired; restart the iteration from 0` | A `SCAN` token the server no longer holds. |
| `-ERR character classes are not supported in MATCH` | An unescaped `[` in a `SCAN` pattern. |
| `-ERR unsupported operation` / `-ERR invalid argument` / `-ERR internal error` | The remaining status codes, rendered in Redis's shape. |
| `-ERR server is overloaded, try again` | Write queue full or shutting down. Retryable. |
| `-OOM command not allowed when used memory > 'maxmemory'` | The map is full, or the tag registry is (`store.tags.max_tags`) — both are `CAPACITY_FULL` and this dialect does not tell them apart. Clients treat `OOM` as "back off", which is right for the first; for the second nothing frees a name, so it needs an operator. |
| `-NOPROTO unsupported protocol version` | `HELLO` with anything but 2 or 3. |
| `-NOAUTH Authentication required.` | Any command before authenticating. |
| `-NOAUTH HELLO must be called with the client already authenticated, …` | A bare `HELLO` while unauthenticated. Redis's own wording, which explains the combined form. |
| `-WRONGPASS invalid username-password pair or user is disabled.` | A bad name or a bad secret. One message for both, as Redis does, so it does not confirm which names exist. |
| `-ERR AUTH <password> called without any password configured for the default user. …` | `AUTH` when the server has no credentials configured at all. |
| `-ERR Protocol error: …` | Framing that cannot be resynchronised. Sent, **then** the connection closes. |

A rejected command consumes exactly its own bytes, so one bad command in a
pipeline produces one error line in the position it occupied and everything
after it is still read correctly.

## Deliberate divergences from Redis

| Behaviour | Redis | vash | Why |
|---|---|---|---|
| Inline commands | accepted | connection is read as memcached | The first byte picks the dialect; see above. |
| Expiry precision | milliseconds | rounded to the nearest second, never to the past | The store's deadline field is whole seconds. `PX 100` therefore buys up to a full second — too long rather than too short, since a key that vanishes as it is written is the worse failure. |
| `SET … IFEQ/IFNE/IFDEQ/IFDNE` | supported | `-ERR … are not supported` | Value-conditional writes need a compare inside the write transaction, and the digest forms need a `DIGEST` command that does not exist here. Named explicitly rather than reported as a syntax error. |
| `HELLO … SETNAME` | supported | `-ERR … is not supported` | There is no client registry, and accepting it would report back a name nothing had stored. `HELLO … AUTH` **is** supported. |
| `RESET` | supported | `-ERR unknown command` | It exists partly to drop authentication state; a client that wants that can close the connection. |
| `SCAN` availability | always | behind `listing_enabled`, off by default | Keyspace enumeration is the same capability whichever dialect asks; see [`SCAN`](#scan). |
| `SCAN` cursor lifetime | stateless, survives anything | a process-local token, with a capacity and a TTL | Redis's cursor is a hash-bucket index that fits in a `u64`; a key does not. An expired one is refused rather than restarted. |
| `SCAN … MATCH` character classes | `[a-z]`, `[^x]` | `-ERR character classes are not supported in MATCH` | The pattern matcher is two tokens and an escape by design; matching `[` as a literal would silently return nothing. |
| `SCAN … COUNT` above 1024 | honoured as a hint | clamped to 1024 | Redis specifies `COUNT` as a hint, and the reply's cursor is what drives the loop. |
| `KEYS` | supported | `-ERR unknown command` | An unbounded scan with no cursor. `listing_max_scan` exists so no request can hold a read transaction across the whole keyspace, and `KEYS` cannot be expressed without breaking it. `SCAN` is the answer in Redis too. |
| `INFO db0:expires` | keys carrying a TTL | omitted | Nothing here measures that quantity; see [`INFO`](#info). |
| `INFO maxmemory_policy` | the configured policy | `volatile-ttl` | The closest true statement in Redis's vocabulary for TTL-ordered eviction (plan §6). |
| `ACL` command family | supported | `-ERR unknown command` | The credential table is config, loaded from a file and reloaded on `SIGHUP`. A runtime mutation command is a road that ends at `ACL SETUSER`; see [auth.md](auth.md#36-a-database-of-users). |
| Empty key (`SET "" v`) | allowed | `-ERR invalid key` | LMDB has no empty key. |
| Keys over 511 bytes | allowed | `-ERR invalid key` | LMDB's compile-time `MDB_MAXKEYSIZE`; see [storage.md](storage.md). |
| `INCRBYFLOAT` precision | 80-bit `long double` | 64-bit `f64` | Rust has no 80-bit float. The last digits of a long chain of increments can differ. |
| Arithmetic and `APPEND` atomicity | atomic (single-threaded) | atomic (single writer per shard) | See below. |
| `SET … GET/KEEPTTL`, conditional `EXPIRE`, `PERSIST` | atomic | atomic (single writer per shard) | See below. |
| `MSETEX NX/XX` spanning shards | atomic | the guard can be stale | Multi-key atomicity across shards is a standing non-goal (plan §16). Atomic within a shard, which is every single-shard deployment. |
| Eviction under memory pressure | configurable LRU/LFU | TTL-ordered | See [plan.md](plan.md) §6. |
| Databases (`SELECT`) | 16 | one | A cache does not need a namespace it cannot see into. |

### Atomicity

**The arithmetic commands are atomic.** `APPEND`, `INCR`, `INCRBY`, `DECR`,
`DECRBY`, `INCRBYFLOAT` and `INCREX` are each one storage primitive, evaluated
inside the transaction of the shard's single writer thread. Reading the current
value and writing the new one is one step, so concurrent clients cannot lose an
update: Redis gets that guarantee from being single-threaded, and this server
gets it from having exactly one writer per shard. `INCREX` is an exact rate
limiter, not a best-effort one.

Nor do plain `GET`, `SET`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `TTL` or
`PERSIST` have a seam — they were always single operations.

**The conditional writes are atomic as well.** `SET … KEEPTTL`, `SET … GET`,
`EXPIRE`/`EXPIREAT` with `NX`/`XX`/`GT`/`LT`, and `PERSIST` each used to read a
deadline or a value from the network tier and then write against what they had
read. Each is now a single storage primitive, with the guard evaluated and the
displaced value captured inside the transaction that writes:

| Command | What the guarantee now is |
|---|---|
| `SET … KEEPTTL` | Keeps the deadline the key holds **at the moment of the write**. No deadline is read at all — it travels as "keep" and is settled against the record being replaced. |
| `SET … GET` | Reports exactly the value this write displaced, never one a read a moment earlier happened to see. |
| `EXPIRE`/`EXPIREAT` with a condition | `GT`/`LT` compare against the deadline the record actually holds when the write lands. |
| `PERSIST` | Cannot clear a deadline set concurrently after it decided there was none. |

**The one exception is `MSETEX` with `NX`/`XX` across shards.** Its guard has to
see every key at once, and a batch spanning shards is several transactions —
plan §16's standing non-goal, not an oversight. It is atomic when the keys land
in one shard, which includes every single-shard deployment; across shards the
all-present/all-absent test can be stale by the time the later shards commit.

**Note the asymmetry M10 removed.** Before it, `INCR` was atomic over memcached
and not over Redis, on the same key, on the same server — because the memcached
adapter had always executed it in the writer and the Redis adapter composed it
from two calls. Both now use the same primitive.

---

# Shared semantics

These apply to all three protocols, identically except where noted.

## TTL semantics

`0` means "never expires" on all three protocols, and an expired item is
**never served**, whether or not its space has been reclaimed yet — reclamation
is a background process and lags by design.

How a *non-zero* TTL is read is the one thing that differs between them,
because memcached's `exptime` overloads the field:

| Value | memcached | VCP | Redis |
|---|---|---|---|
| `1` … `2592000` (30 days) | Relative offset in seconds. | Relative offset in seconds. | Relative offset, in the unit the option names. |
| `> 2592000` | **Absolute unix timestamp** in seconds, not an offset. | Relative offset in seconds, same as any other. | Relative offset, same as any other. |
| negative | Already expired. | Not representable: the field is a `u32`. | `SET … EX` refuses it; `EXPIRE` deletes the key. |

Only memcached flips to a timestamp past 30 days, and only because its clients
expect it to. On VCP a TTL is an offset at every magnitude, so `ttl_secs` of
`5184000` is sixty days from now rather than a date in March 1970. Redis has
separate options for the two forms — `EX`/`PX` for an offset, `EXAT`/`PXAT` for
a stamp — and neither one changes meaning with its magnitude.

The furthest deadline any of them can express is in 2106, the ceiling of the
`u32` of unix seconds the store keeps internally. A longer TTL is stored as that
ceiling rather than refused.

## CAS tokens

A `u64` that is **unique across the whole server** and never repeats — a restart
skips forward rather than reusing a range.

Ordering is guaranteed **per key**: every write to a given key gets a token
higher than the one before it, which is all compare-and-swap depends on. Tokens
are *not* globally ordered: each shard numbers independently and the values are
striped so they cannot collide, so two keys in different shards say nothing
about which was written first. A client must treat a token as an opaque version
for one key, never as a clock.

Zero is not a valid token. Compare-and-swap is available through the memcached
`cas` command and the `ms … C<cas>` flag.

## Tags

A tag is a 1–255 byte name, attached at write time.

Two limits apply, both configurable:

| Limit | Setting | Default | On breach |
|---|---|---|---|
| Tags on one record | `store.tags.max_per_record` | 32 | `BAD_REQUEST` |
| Distinct names registered | `store.tags.max_tags` | 100000 | `CAPACITY_FULL` |

The first is reported as `max_tags_per_record` in the
[handshake](#hello-0x01) — enforce it locally, because the refusal is a bare
`BAD_REQUEST` and under `NO_REPLY` there is no refusal at all. The second is
not advertised and cannot be: it bounds a shared registry, so whether the next
write fits depends on what every other client has registered, not on the
request.

`max_per_record` cannot exceed **255**: the record header counts tags in a single
byte, and a server configured past that refuses to start. The default is 32
because every tag costs 12 bytes in each copy of the record, a tag-index row on
every write, and one comparison on every read of the key — the liveness check is
O(tags) and runs before anything is served. Lowering it later is safe: records
already carrying more stay readable and touchable, and only new writes are
refused.

Names are registered on first use. The registry lives entirely in RAM, which is
what `max_tags` bounds.

Invalidation is a generation bump, so it costs the same for ten keys or a
million. The ordering guarantee a client can rely on:

> When an invalidation response is received, no subsequent read on any
> connection will return a record that carried that tag and was written before
> the invalidation.

Rewriting a key after an invalidation makes it live again — the new write
captures the new generation. Reclaiming the space of invalidated records happens
in the background and is not observable except through `stats`.

## Cluster

Nodes are otherwise independent — clients shard the keyspace, no data moves
between nodes, nothing is replicated. Tag invalidation is the one thing that
crosses a node boundary, because a tag's keys are spread across *every* node, so
an invalidation that stopped at the node the client happened to call would leave
most of the affected keys being served.

A node forwards invalidations according to `cluster.delete_by_tag`:

| Mode | `DELETE_BY_TAG` returns | Staleness elsewhere |
|---|---|---|
| `local` | after the local bump | unbounded — the client must call every node |
| `fanout` (default) | after the local bump; peers are told in the background | bounded by `cluster.gossip_interval_ms` |
| `fanout_sync` | after reachable peers have applied it | none for reachable peers; gossip interval for the rest |

Regardless of mode, every node also exchanges tag generations with each of its
peers every `cluster.gossip_interval_ms`. That is what closes the gap for a node
that was down, partitioned, restarted, or simply missed a message — fan-out is
an optimisation on top of it, not a replacement for it.

**Consistency statement.** Tag invalidation is *strongly consistent within a
node* and *eventually consistent across the cluster*. Concretely:

- Within one node, the ordering guarantee above holds exactly.
- Across nodes under `fanout`, there is a window — normally milliseconds, at
  worst one gossip interval — in which another node still serves records the
  invalidation covered.
- A record written on another node **during** that window is treated as
  pre-invalidation and dropped when the message lands, because that node had not
  yet learned the invalidation happened. The error is always in the direction of
  a miss, never a stale hit. `fanout_sync` closes this for reachable peers: once
  the response is in hand, a subsequent write anywhere is safe.

Invalidations merge by taking the higher generation, so they can be replayed,
reordered and retried freely, and two nodes invalidating the same tag at once
converge rather than conflict. A node that has never heard of a tag registers it
at the generation it was told, which is what stops a later local write there from
capturing a lower number and being killed by the next gossip round.

Tag *ids* never cross the wire — they are per-shard counters, and two nodes will
assign different ids to the same name. Names are the global identity.

The `CLUSTER` opcode reports a node's peer list and their reachability.

## Client flags

A 32-bit field stored verbatim alongside the value, for client libraries that
encode a type tag in it. Set it with memcached `set`'s `<flags>` argument or the
meta `F` flag. **A VCP `SET` always stores 0**, since the VCP body has no flags
field.

## Durability

A write is acknowledged once committed to the storage engine. Depending on the
server's `store.durability` setting, that commit may not yet be on stable
storage — in `lazy` (the default) an OS crash can lose the last few
transactions, and under `--ephemeral` the store starts empty anyway. This is a cache;
treat an acknowledged write as durable only if the deployment is configured for
it.
