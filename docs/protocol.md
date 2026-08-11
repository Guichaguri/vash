# Protocol reference

Normative specification of the three protocols vash speaks, written to be
sufficient for implementing a client without reading the server source.

- [VCP](#vcp--the-native-binary-protocol) — the native binary protocol.
- [Memcached](#memcached-compatibility) — what is supported, what is extended,
  and where behaviour deliberately differs from upstream.
- [Redis](#redis-compatibility) — the RESP subset, in RESP2 and RESP3.

Design rationale for these choices lives in [plan.md](plan.md) §3 and §7; this
document describes only what is on the wire.

**Protocol version:** 1. **Default port:** 11311.

---

## Choosing between them

All three protocols are served on the **same port**. Use VCP for anything new:
it is cheaper to parse, supports batch operations in one round trip, and exposes
tags natively. Use the memcached or Redis protocol when an existing client
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
4. Issue commands.
5. Close the socket. There is no `QUIT` opcode in VCP.

A `HELLO` carrying a version other than `1` is answered with
`UNSUPPORTED` (8) and the connection stays open, so a client can report a clear
error rather than hanging.

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
| `0x03` | `AUTH` | reserved — returns `UNSUPPORTED` |
| `0x04` | `STATS` | reserved — returns `UNSUPPORTED` |
| `0x05` | `CLUSTER` | yes |
| `0x10` | `GET` | yes |
| `0x11` | `SET` | yes |
| `0x12` | `DELETE` | yes |
| `0x13` | `TOUCH` | yes |
| `0x20` | `GET_MANY` | yes |
| `0x21` | `SET_MANY` | yes |
| `0x22` | `DELETE_MANY` | yes |
| `0x30` | `DELETE_BY_TAG` | yes |
| `0x31` | `FLUSH` | yes, if enabled server-side |
| `0x40` | `TAG_SYNC` | yes — peer-to-peer, see [Cluster](#cluster) |

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
| 5 | `UNAUTHORIZED` | Command disabled by configuration (`FLUSH`). |
| 6 | `OVERLOADED` | Write queue full, or the server is shutting down. Retryable. |
| 7 | `CAPACITY_FULL` | The store is out of space, or the tag registry is full. |
| 8 | `UNSUPPORTED` | Unknown or unimplemented opcode, or an unsupported protocol version. |
| 9 | `INTERNAL` | Server-side failure. Details are logged, not sent. |
| 10 | `NOT_STORED` | Reserved for a guarded write whose condition failed. Not emitted over VCP today. |
| 11 | `NOT_NUMERIC` | Reserved for arithmetic on a non-numeric value. Not emitted over VCP today. |

Codes 2, 10 and 11 are defined so that conditional writes and arithmetic can be
added to VCP without a wire change; today they arise only through the memcached
adapter. **Clients should handle unknown status codes as generic failures**
rather than rejecting the frame.

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

**Response body** (16 bytes), status `OK`:

| Offset | Field |
|---|---|
| 0 | `protocol_version` u16 |
| 2 | `shards` u16 |
| 4 | `max_key_len` u32 |
| 8 | `max_value_len` u32 |
| 12 | `capabilities` u32 |

`max_key_len` and `max_value_len` are the server's configured limits; a client
should enforce them locally rather than discovering them through errors.

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

`CLUSTER` is set only when this node has peers configured **and** is set to
forward — not merely because the build supports it. A client seeing it clear
must invalidate on every node itself.

Unlisted bits are reserved; ignore them.

### `PING` (0x02)

Empty request body. Response is `OK` with an empty body. Does not touch storage,
so it is a liveness check, not a health check.

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
| 6 | `tag_count` u8 — at most 32 |
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

### `GET_MANY` (0x20)

**Request body:**

| Offset | Field |
|---|---|
| 0 | `count` u32 — at most 4096 |
| 4 | `count` × (`key_len` u16, key `bytes[key_len]`) |

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
the request produce duplicates in the reply. All keys are resolved against a
single consistent snapshot.

### `SET_MANY` (0x21)

**Request body:** `count` u32, then `count` repetitions of the `SET` body layout
(header, key, value, tags) back to back.

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

## Worked example

Bytes on the wire for a handshake, a write and a read. `→` is client-to-server.
These are the actual bytes a freshly started server exchanges, so a client can
be checked against them directly — the CAS token is 1 because it is the first
write to an empty store.

```
→ HELLO, request_id 1
  01 00 00 00  01 00 00 00  04 00 00 00     header: opcode 0x01, flags 0, status 0, id 1, body 4
  01 00 00 00                               version 1, reserved 0

← 01 01 00 00  01 00 00 00  10 00 00 00     flags 0x01 = RESPONSE, status 0, body 16
  01 00                                     protocol_version 1
  01 00                                     shards 1
  ff 01 00 00                               max_key_len 511
  00 00 10 00                               max_value_len 1048576
  03 00 00 00                               capabilities TAGS|MEMCACHED

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

- Send `HELLO` first; nothing else is accepted as an opening frame.
- Buffer inbound bytes; a frame may arrive split across reads, and several may
  arrive in one. Read the 12-byte header, then wait for `body_len` more.
- Reject an inbound `body_len` above 64 MiB and close the connection.
- Correlate by `request_id`. Do not assume replies arrive in order.
- Treat unknown status codes as failures, not as protocol errors.
- Enforce `max_key_len` and `max_value_len` from the handshake locally.
- Expect no reply at all for `NO_REPLY` requests.

---

# Memcached compatibility

vash speaks the classic text protocol and the meta commands. The **legacy
binary protocol (magic `0x80`) is not implemented and will not be** — upstream
deprecated it in favour of the meta commands.

Compatibility is checked in CI two ways: a real client library
(`pymemcache`) driven against both vash and real memcached, and a byte-for-byte
differential that sends identical command sequences to both and compares raw
responses. See `tests/compat/`.

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
| `stats` | `STAT <name> <value>` lines, then `END` |
| `version` | `VERSION <string>` |
| `verbosity <level> [noreply]` | accepted and ignored |
| `quit` | connection closes, no response |

Storage commands are followed by exactly `<bytes>` of data and then `\r\n`. The
framing is **length-delimited, not line-delimited**: a value may contain `\r\n`.

`append` and `prepend` keep the existing item's client flags and TTL; the
`<flags>` and `<exptime>` on their command line are ignored, as upstream does.

`incr` wraps at 64 bits; `decr` clamps at zero.

### `stats`

A subset of memcached's counters — only what is actually measured — plus
vash's own under a `vash_` prefix. Nothing is reported as a plausible zero
just to fill the field out.

`pid`, `version`, `pointer_size`, `curr_items`, `bytes`, `limit_maxbytes`, and:
`vash_utilisation`, `vash_expiry_entries`, `vash_tags`,
`vash_tag_index_entries`, `vash_pending_reclaims`, `vash_commits`,
`vash_committed_ops`, `vash_mean_batch`, `vash_sweeps`,
`vash_reclaimed`, `vash_tag_reclaimed`, `vash_sweep_lag_ms`,
`vash_epoch`, `vash_readers_in_use`, `vash_cluster_mode`,
`vash_cluster_peers`, `vash_cluster_peers_reachable`.

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
| `q` | all | Quiet: suppress the response. |
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
| `me` output | full internal item dump | `cas`, `size`, `fetch` only | The rest describes internals vash does not have. |
| `stats` fields | full counter set | a subset, plus `vash_*` | Reporting an unmeasured counter as zero would mislead a dashboard. |
| Meta flags `b h l x I E R N` | implemented | `CLIENT_ERROR unsupported flag` | See [Refused flags](#refused-flags). |
| Eviction under memory pressure | LRU | TTL-ordered | See [plan.md](plan.md) §6. |

---

# Redis compatibility

A subset of the Redis string and expiry commands, enough for a cache. There are
no lists, hashes, sets, sorted sets, streams, transactions, scripting, pub/sub,
`SCAN`, `SELECT` or replication commands, and there never will be — see
[plan.md](plan.md) §16.

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
- An argument may be up to the server's `max_value_bytes`; a request may carry
  up to 8200 arguments. Both are checked before anything is allocated.

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
| `EXISTS` | `EXISTS key [key …]` — counts a key once per mention |
| `EXPIRE` | `EXPIRE key seconds [NX \| XX \| GT \| LT]` |
| `EXPIREAT` | `EXPIREAT key unix-time-seconds [NX \| XX \| GT \| LT]` |
| `PERSIST` | `PERSIST key` |
| `TTL` | `TTL key` — `-2` absent, `-1` no expiry |
| `APPEND` | `APPEND key value` — creates the key, keeps an existing deadline |
| `INCR` / `DECR` | `INCR key` |
| `INCRBY` / `DECRBY` | `INCRBY key increment` |
| `INCRBYFLOAT` | `INCRBYFLOAT key increment` |
| `INCREX` | `INCREX key [BYFLOAT inc \| BYINT inc] [LBOUND lb] [UBOUND ub] [SATURATE] [EX s \| PX ms \| EXAT ts \| PXAT ms \| PERSIST] [ENX]` |
| `HELLO` | `HELLO [protover]` |
| `PING` | `PING [message]` |
| `QUIT` | `QUIT` |

Anything else is answered `-ERR unknown command '…'` and the connection carries
on, which is how a client library discovers a feature is missing.

Numbers follow Redis's own `string2ll`, not Rust's `parse`: no `+`, no leading
zeros, no surrounding whitespace. The same rule judges command arguments and
stored counters, so a value `INCR` accepts is exactly a value it can write back.

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
| `-OOM command not allowed when used memory > 'maxmemory'` | The map is full. Clients treat `OOM` as "back off", which is right. |
| `-NOPROTO unsupported protocol version` | `HELLO` with anything but 2 or 3. |
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
| `HELLO … AUTH/SETNAME` | supported | `-ERR … are not supported` | There is no auth and no client registry. Accepting `AUTH` would tell a client it had authenticated. |
| Empty key (`SET "" v`) | allowed | `-ERR invalid key` | LMDB has no empty key. |
| Keys over 511 bytes | allowed | `-ERR invalid key` | LMDB's compile-time `MDB_MAXKEYSIZE`; see [storage.md](storage.md). |
| `INCRBYFLOAT` precision | 80-bit `long double` | 64-bit `f64` | Rust has no 80-bit float. The last digits of a long chain of increments can differ. |
| Arithmetic and `APPEND` atomicity | atomic (single-threaded) | **read-modify-write, not atomic** | See below. |
| Eviction under memory pressure | configurable LRU/LFU | TTL-ordered | See [plan.md](plan.md) §6. |
| Databases (`SELECT`) | 16 | one | A cache does not need a namespace it cannot see into. |

### Atomicity

The Redis adapter is a **protocol layer only**: it composes the storage
operations the engine already has rather than adding new ones. Commands that
need a read and a write therefore have a seam between them, and two clients
touching the same key at the same moment can lose an update where Redis, being
single-threaded, cannot.

This affects `APPEND`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`,
`INCREX`, `SET … GET`, `SET … KEEPTTL`, `EXPIRE`/`EXPIREAT` with a condition,
and `MSETEX` with `NX`/`XX`. It does **not** affect plain `GET`, `SET`, `MGET`,
`MSET`, `DEL`, `UNLINK`, `EXISTS`, `TTL` or `PERSIST`.

Closing the seam means new primitives inside the shard writer, which already
serialises everything on one thread — so the fix is cheap in principle and is a
storage-engine change, not a protocol one. Until then, treat `INCREX` as a
best-effort rate limiter rather than an exact one.

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

A tag is a 1–255 byte name. A record may carry up to **32** tags, attached at
write time. Tags are registered on first use; the registry is bounded
(`store.tags.max_tags`, default 100000) and a write that would exceed it fails
with `CAPACITY_FULL`.

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
storage — in `relaxed` (the default) an OS crash can lose the last few
transactions, and in `ephemeral` a crash discards everything. This is a cache;
treat an acknowledged write as durable only if the deployment is configured for
it.
