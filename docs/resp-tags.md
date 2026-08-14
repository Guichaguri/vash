# Tags over RESP — design

Status: **phase 1 built** — `SETTAGS`, `MSETTAGS` and `DELBYTAG` are on the
wire and specified in [protocol.md](protocol.md#tag-commands); phase 2 (`ADDTAGS`,
`REMTAGS`) and phase 3 (the reserved key prefix) are not. Same house style as
the rest of the docs: decision first, then the reasoning, then the rejected
alternatives. Sections marked *as built* record where implementing it changed
the design.

Tags are a first-class feature of the store ([plan.md](plan.md) §5) and reached
two of the three dialects. VCP carries a tag list in the `SET` body and has
[`DELETE_BY_TAG`](protocol.md#delete_by_tag-0x30); memcached gets them through
the two documented [extensions](protocol.md#extensions), `ms … G<tag>` and
`mdt`/`delete_by_tag`. **The Redis dialect had no tag surface at all** — a
client speaking RESP could neither attach a tag nor invalidate one, and since
the dialect answers no `FLUSHALL` or `FLUSHDB` either, it had *no* way to
invalidate more than one named key at a time.

That was the largest remaining hole in the RESP subset, and the one that matters
most in practice: the Redis dialect is what a framework cache backend speaks,
and tag-based invalidation is the feature those backends want. This document is
how the surface was chosen.

---

## 1. The constraint that decides everything

Redis has no tag concept, so anything here is an extension. The interesting
question is not *what to name it* but **what shapes an unmodified client library
can put on the wire**. There are only four, and they form a ladder:

| Rung | What the client can emit | What it costs the caller |
|---|---|---|
| **A** | The typed high-level API only — `get`, `set`, `del` | Nothing. Works from framework code nobody controls. |
| **B** | The raw escape hatch — `execute_command`, `sendCommand`, `rawCommand`. Present in every library worth naming. | Call sites change. |
| **C** | Connection configuration only — pool options, the URL | Config changes; granularity is the connection. |
| **D** | The key bytes | The key spelling changes. |

The load-bearing observation: **no mechanism reaches rung A for writes.** A
typed `set(key, value, ex=60)` has nowhere to put a tag, in any client, and no
server-side cleverness invents an argument the client never sent. Rung A is
reachable only for *invalidation* (`del`), and only by spending a key prefix on
it (§6.6).

So the write path is a rung-B decision, and once that is settled the question
narrows to: which rung-B spelling, and what else is worth adding around it.

---

## 2. Decision

**Three new commands, none of which collides with a Redis command name, all of
which map onto the `Command` boundary as it already exists.**

| Command | Form | Reply |
|---|---|---|
| `SETTAGS` | `SETTAGS key value numtags tag [tag …] [NX \| XX] [GET] [EX s \| PX ms \| EXAT ts \| PXAT ms \| KEEPTTL]` | As `SET`: `+OK`, null when the guard skips it, the previous value with `GET` |
| `MSETTAGS` | `MSETTAGS numkeys key value [key value …] numtags tag [tag …] [NX \| XX] [EX s \| PX ms \| EXAT ts \| PXAT ms \| KEEPTTL]` | As `MSETEX`: `+OK`, or null when the guard skips the batch |
| `DELBYTAG` | `DELBYTAG tag [tag …]` | Integer — how many of the named tags were registered, as `DEL` counts keys |

`SETTAGS` is `SET` plus a counted tag list; `MSETTAGS` is `MSETEX` plus one tag
list shared by every pair in the batch. The tags are attached at write time,
which is the only moment the store attaches them cheaply (§5).

**Deferred, designed here so the grammar does not have to move later:**

| Command | Form | Reply |
|---|---|---|
| `ADDTAGS` | `ADDTAGS key numtags tag [tag …]` | Integer — tags newly attached |
| `REMTAGS` | `REMTAGS key numtags tag [tag …]` | Integer — tags actually removed |

These attach to a record that already exists. They need a storage primitive that
does not exist and cost a full record rewrite; §5 says why they are phase 2
rather than phase 1.

**Optionally, and off by default:** a reserved key prefix that routes `DEL` to
`DELBYTAG`, so a caller with no raw escape hatch can still invalidate (§6.6).

### The naming rule

An earlier draft called these `SETTAG`, `MSETTAG` and `DELTAG`, and the shared
`…TAG` suffix was a bug in the design. `SETTAG` and `DELTAG` look like a matched
pair operating on the same thing, and they are not: two of them write an
**entry** that carries tags, and the third selects entries **by** a tag and
kills them. Read as a pair, `DELTAG` says "remove a tag from the entry
`SETTAG` put it on" — which is a real operation, and not that one.

So the two spaces are spelled differently, and the rule is one line:

> **A command whose name ends in plural `…TAGS` takes a key and is about that
> one entry's tags. The command that says `BY TAG` takes tag names and is about
> every entry carrying them.**

| Space | Commands | First argument |
|---|---|---|
| One entry's tags | `SETTAGS`, `MSETTAGS`, `ADDTAGS`, `REMTAGS`, `GETTAGS` | a key |
| Selection by tag | `DELBYTAG` | a tag |

`DELBYTAG` also carries the name the feature already has everywhere else: VCP's
[`DELETE_BY_TAG`](protocol.md#delete_by_tag-0x30) opcode, memcached's
`delete_by_tag`, and `vash-client`'s `delete_by_tag()`. One concept, one name,
four surfaces — a reader who knows any dialect recognises it in the others.

**And it is the only name that is honest about what happens.** `DELTAG` and
`TAGDEL` both say the tag is deleted. The tag is not deleted: the registry keeps
the name forever, since [nothing removes a tag today](protocol.md#recommendations)
— not a flush, not deleting every record that carried it. `DELBYTAG` promises
only what it does, which is delete records *by* tag and bump a generation.

The phase-2 pair is `ADDTAGS`/`REMTAGS` rather than `ADDTAGS`/`DELTAGS` for the
same reason in miniature: `REM` removes a name from one entry's list, `DEL`
destroys something, and Redis already uses `SREM` for exactly this distinction.

### Names considered and rejected

| Name | Why not |
|---|---|
| `SETTAG` / `MSETTAG` (singular) | Reads as "set the tags **of** a key", which is `ADDTAGS`'s job, not this one's. The plural pairs the name with the option family it adds, as `MSETEX` pairs with `EX`. |
| `DELTAG` | The suffix collision above, plus it claims to delete the tag. |
| `TAGDEL`, `TAGADD` | Redis's own convention is type-first (`HDEL`, `SREM`, `ZADD`), so this is the most Redis-shaped option. Rejected because a tag is not a type living at a key — `TAGDEL` takes no key at all — and the prefix would put `TAGADD key …` (an entry command) in the same family as `TAGDEL tag` (a tag command), recreating the confusion one letter further along. |
| `FLUSHTAG` | Reads well beside `FLUSHDB`/`FLUSHALL` and is unmistakably bulk. Rejected because it implies kinship with `FLUSH` that does not exist: `FLUSH` bumps the global epoch, takes the whole cache, and is gated by `protocol.flush_enabled`. Tag invalidation is none of those, and an operator who turned flushing off would reasonably expect a `FLUSHTAG` to be off with it. |
| `INVALIDATE` | Accurate about the mechanism — records die lazily on their next read — but silent about what is being invalidated, and no other dialect calls it that. |
| `TSET` / `MTSET` | Short, and nobody writing a client would guess it. |
| `SETX` | One letter from Redis's real `SETEX`, with a different argument order. A typo that silently means something else is the worst outcome on this list. |

None of `SETTAGS`, `MSETTAGS`, `ADDTAGS`, `REMTAGS`, `GETTAGS` or `DELBYTAG`
collides with a Redis command name, present or historical.

### Why the count and not a comma list

`numtags` is positional and counted, following `MSETEX numkeys`, rather than
memcached's comma-separated `G<tag>,<tag>`. Tag names are **binary-safe, 1–255
bytes** ([protocol.md §Tags](protocol.md#tags)) and a comma list cannot express
a name containing a comma. The memcached extension has that limitation because
memcached's meta flags are a text grammar with nowhere else to go; RESP is
length-delimited and has no such excuse.

`numtags 0` is **accepted** and means "no tags", which is a deliberate
divergence from `MSETEX`'s refusal of `numkeys 0`. A batch of zero keys is a
meaningless write; a write with zero tags is an ordinary one, and a client
library building a command from a possibly-empty tag list should not have to
branch to a different verb to send it.

---

## 3. Why new verbs rather than an option on `SET`

The obvious alternative is `SET key value EX 60 TAGS 2 news author:7`. It is one
parser instead of two and it composes with every future `SET` option for free.
It is rejected, for four reasons that all point the same way.

**The house style is already new verbs.** `MSETEX` and `INCREX` exist precisely
because vash wanted `MSET` and `INCR` with extras and would not modify the
originals. [protocol.md](protocol.md#command-reference-1) states the rule
outright: options follow the Redis documentation exactly unless noted. A `TAGS`
token on `SET` breaks that rule for the first time; `SETTAGS` extends the pattern
that is already there.

**An unknown verb fails loudly, and in the right place.** `-ERR unknown command
'SETTAGS'` is how [protocol.md:1510](protocol.md#command-reference-1) already says a
client discovers a missing feature, and it is unambiguous: the server does not
have tags. An unknown *option* on a known command comes back as `-ERR syntax
error`, which every client and every operator will first read as their own bug.

**Mirroring and migration.** A `SET` that a real Redis rejects is a trap for
dual-write migrations, proxies, and anything that records commands and replays
them elsewhere — the command looks like a `SET` those tools understand, and it
is not one. A `SETTAGS` is visibly not a Redis command anywhere it is seen.

**Two verbs need not mean two parsers.** `parse_set` and `parse_msetex` in
[command.rs](../crates/vash-proto/src/resp/command.rs) already share the
`Options` cursor; `SETTAGS` is `parse_set` with a tag list parsed between the
value and the options, and the option grammar stays in exactly one place. The
duplication the option form avoids is duplication that does not have to exist.

**The honest counterargument**, recorded so it is not re-litigated as if it were
new: two verbs is two arities, two entries in every compatibility table, and a
standing obligation to add each future `SET` option to both. That is real, and
it is the price of leaving Redis's `SET` byte-exact. It is worth paying because
`SET` is the single most-parsed command in the ecosystem and this server is not
the only thing that will read it.

---

## 4. Semantics, limits, errors

**Atomicity.** `SETTAGS` is one storage primitive, evaluated inside the shard
writer's transaction, so it is atomic exactly as `SET` is — the tags land with
the value or neither does. `MSETTAGS` inherits `MSETEX`'s guard behaviour
verbatim, including the standing caveat that `NX`/`XX` across shards can be
stale (plan §16).

**Generations.** A tag reference stores the tag's generation at the moment of
the write ([record.rs](../crates/vash-core/src/record.rs)). A `DELBYTAG` bumps the
generation, so records written *before* it die and records written *after* it
live — including a record written microseconds later with the same tag. This is
the same [read-your-writes caveat](protocol.md#tags) the other dialects carry,
and it is worth restating in the RESP docs because the Redis audience is the
one most likely to write-then-invalidate in the same request.

**Cluster.** `DELBYTAG` is `DELETE_BY_TAG` under another name, so fan-out and
anti-entropy gossip apply unchanged, governed by `cluster.delete_by_tag`. No new
cluster surface.

**Auth.** Unchanged: any authenticated client can already invalidate any tag
([auth.md](auth.md) §9), and `DELBYTAG` adds no new authority — only a new spelling
of an existing one.

**Limits**, all of them already enforced by the store and merely surfaced here:

| Limit | Setting | Default |
|---|---|---|
| Tag name length | fixed | 1–255 bytes |
| Tags per record | `store.tags.max_per_record` | 32 |
| Distinct registered names | `store.tags.max_tags` | 100 000 |

**Errors**, in the shape [the RESP error table](protocol.md#errors-1) already
uses:

| Reply | When |
|---|---|
| `-ERR invalid tag` | Empty, or over 255 bytes. Mirrors `-ERR invalid key`. |
| `-ERR too many tags` | Past `store.tags.max_per_record`, or past the format's 255. Named rather than reported as a bare `-ERR invalid argument`. |
| `-ERR numtags should be greater than or equal to 0` | A negative count. |
| `-ERR wrong number of arguments for '…' command` | The count and the argument list disagree, as `MSETEX` checks `numkeys`. |

*As built:* two of these moved. A `numtags` that does not match the arguments is
an **arity error** rather than a message of its own, because that is what
`MSETEX` already answers for the same mistake about `numkeys`, and one wording
for one class of error is worth more than a bespoke sentence.

The registry-full case does **not** get `-ERR tag registry is full`. The store
reports it as `CapacityFull` with no cause attached — the same status a full map
produces — and this dialect renders that as `-OOM`, which client libraries treat
as "back off". Telling the two apart would mean plumbing a new distinction
through `dispatch::to_failed` for all three dialects to serve a wording in one,
so the honest thing was to document the collision rather than build for it:
[protocol.md's error table](protocol.md#errors-1) now names both causes on the
`-OOM` row, and notes that backing off does not help the second one because
nothing frees a name.

### The warning this surface needs more than the others

**Nothing removes a tag today.** Names are registered on first use, the registry
is capped, and neither a flush nor deleting every record that carried a tag
frees its name ([protocol.md](protocol.md#recommendations)). The RESP audience is
exactly the audience that will emit one tag per entity — framework cache
backends generate `user:7`, `post:912`, `cart:88f1…` without a second thought,
and 100 000 of those arrive quickly. Whatever else the RESP docs say about tags,
they have to say this first: **tag vocabularies are small and bounded, one per
*kind* of thing, not one per thing.**

---

## 5. What it costs to build

| Phase | Work | Where | Status |
|---|---|---|---|
| 1 | `SETTAGS`, `MSETTAGS`, `DELBYTAG` | Parser only | **built** |
| 2 | `ADDTAGS`, `REMTAGS` | A new storage primitive | not built |
| 3 (optional) | Reserved-prefix `DEL` routing | Executor | not built |

**Phase 1 needed no storage change and no boundary change**, which is what M10
phase 2 bought by routing RESP through the shared `Command` boundary instead of
composing `Store` calls directly. `Set` already carried
`pub tags: Vec<&'a [u8]>` ([command.rs](../crates/vash-core/src/command.rs)),
`SetMany` is a `Vec<Set>`, and `Store::delete_by_tag` was already on the trait
([lib.rs](../crates/vash-store/src/lib.rs)).

*As built*, the shape of the change was:

- **`SETTAGS` and `MSETTAGS` are not new commands to the executor.** They are
  `Command::Set` and `Command::MSetEx` carrying a tag list and a `tagged` flag,
  so `translate` and `render` gained a field each rather than arms of their own.
  The flag is not redundant with an empty list: `SETTAGS key value 0` is a legal
  tagless write, and the flag is what names the right command in an expiry
  error.
- **One parser for both spellings.** `parse_set` and `parse_msetex` take a
  `Tagged` flag that decides whether a counted tag list is read and which
  command name every error is worded with. The option grammar exists once, which
  was §3's promise.
- **`DELBYTAG` is answered outside `translate`**, beside `SCAN`, because the
  boundary invalidates one tag at a time and the command names several. Each is
  a separate `dispatch::execute`, so a three-tag command is three invalidations
  in the metrics — which is what it is. A failure part-way leaves the earlier
  bumps applied; that is safe because a bump is idempotent, the same property
  the cluster already relies on.

**Phase 2 is where the real work is.** The tag table lives *inside* the record,
between the header and the value, 12 bytes per tag
([record.rs](../crates/vash-core/src/record.rs)). Attaching a tag to an existing
record therefore means rewriting the record: re-encode the header with the new
`tag_count`, write the widened tag table, and copy the value across. Done inside
the writer's transaction it copies from a borrowed mmap slice and never
materialises the value in the network tier — the same shape `apply_append`
took in M10 phase 1 — but it is still O(value) where a `SETTAGS` is O(1) extra.
Plus one `tagidx` row per newly attached tag.

That asymmetry is the argument for shipping phase 1 alone and seeing whether
anyone asks for phase 2. A client that knows the tags when it writes never needs
`ADDTAGS`; a client that does not can re-`SETTAGS` the record. `REMTAGS` has no
equivalent workaround, which is the one thing that might pull phase 2 forward.

---

## 6. Every option considered

| # | Mechanism | Rung | Store work | `SET` stays Redis-exact | Against a real Redis | Verdict |
|---|---|---|---|---|---|---|
| 1 | `SETTAGS` / `MSETTAGS` | B | none | yes | `unknown command` | **built** |
| 2 | `DELBYTAG` | B | none | yes | `unknown command` | **built** |
| 3 | `ADDTAGS` / `REMTAGS` post-hoc | B | new primitive, O(value) | yes | `unknown command` | deferred to phase 2 |
| 4 | `SET … TAGS n t…` option | B | none | **no** | `syntax error` | rejected — §3 |
| 5 | `GETTAGS key` introspection | B | none (registry read) | yes | `unknown command` | deferred — §6.5 |
| 6 | Reserved key prefix, `DEL` only | **A** | none | yes | deletes a real key | optional, off by default — §6.6 |
| 7 | Tags as virtual sets (`SADD`/`SMEMBERS`) | **A** | phase-2 primitive | yes | operates on a real set | rejected — §6.7 |
| 8 | Ambient tags via a connection name | **C** | none | yes | name is stored and ignored | rejected — §6.8 |
| 9 | `SELECT db` as a tag namespace | C | none | yes | selects a real db | rejected — §6.9 |
| 10 | Cluster `{…}` hash tags in the key | **D** | none | yes | routing hint, silently | rejected — §6.10 |
| 11 | `TAGNEXT t` sentinel before the write | B | none | yes | `unknown command` | rejected — §6.11 |
| 12 | Tags in a value prefix | A | none | yes | corrupt value | rejected — §6.12 |
| 13 | Key-name convention + `SCAN`/`DEL` | A | none | yes | works, slowly | the status quo — §6.13 |

### 6.5 `GETTAGS key` — reading a record's tags

Cheap to implement and genuinely useful for debugging, but it is a deliberate
omission elsewhere: `LIST_KEYS` leaves tag names out of its reply on purpose
([protocol.md](protocol.md#list_keys-0x50-and-list_tags-0x51)). Adding a
per-key tag reader to the *most* accessible dialect while the native one
withholds it is the wrong order to do things in. If it lands, it lands in VCP
first and behind `listing_enabled`.

### 6.6 A reserved key prefix, routing `DEL` to `DELBYTAG`

The only rung-A mechanism worth having. With `store.tags.resp_prefix` set — say
`vash:tag:`, empty by default, which disables the whole thing — the keyspace
under that prefix becomes a view of the tag registry:

| Command | Behaviour under the prefix |
|---|---|
| `DEL` / `UNLINK` | Invalidates the tag. Counts as `DELBYTAG` does. |
| `EXISTS` | 1 if the tag is registered. |
| `GET` / `TTL` / `TYPE` | Null, `-2`, `+none`. A tag is not a value. |
| `SET`, `APPEND`, arithmetic | `-ERR key namespace reserved for tags` |
| `SCAN` | Never lists them. |

This is worth it because invalidation is usually the part living in framework
code you cannot change, while writes usually are not — a Django or Laravel cache
backend calls `del()` on a key it computed, and if the key it computed is a tag
key, invalidation works with no raw commands anywhere.

It is off by default because it spends part of the keyspace, and silently: a
deployment already storing keys under the configured prefix would find writes
to them refused after an upgrade. Refusing the writes is nevertheless the right
failure — the alternative is two namespaces aliasing, where `SET` creates a key
that `DEL` will not delete.

### 6.7 Tags as virtual sets

`SADD vash:tag:news article:1`, `SMEMBERS`, `SREM`, `DEL` to invalidate. This is
seductive: every client library has `sadd` and `smembers` in its typed API, so
the whole feature reaches rung A, tag→key membership *is* a set, and `SCARD`
is a question the `tagidx` can answer.

Rejected on the standing non-goal. [plan.md §16](plan.md#16-non-goals) and
[protocol.md:1425](protocol.md#redis-compatibility) both say there are no sets
and there never will be, and while "a read-only view over the tag index is not a
set type" is a defensible line, it is a line that moves: the first request after
`SMEMBERS` is `SINTER` of two tags, which is the secondary-index query language
§16 also rules out. `SADD` would also inherit phase 2's record rewrite while
looking like an O(1) set insert, which is the kind of mismatch between a
familiar name and an unfamiliar cost that this server tries hard not to ship.

### 6.8 Ambient tags on the connection

`CLIENT SETNAME vash-tags:deploy-4711` or `HELLO 3 … SETNAME …`, with every
subsequent write on that connection inheriting the named tags. Genuinely rung C
— most clients expose a client name in pool configuration and send it
automatically, so the tag never appears at a call site at all — and it fits the
storage model perfectly, since the tags are known before the record is encoded.

Two things sink it. **Granularity:** the value is sticky per connection, and a
pooled connection is reused across unrelated requests, so it can express "this
deploy generation" or "this service" but never "this article", which is what
tag invalidation is mostly for. **It reverses a decision:** there is no `CLIENT`
command here and `HELLO … SETNAME` is refused by name, with a documented reason
— there is no client registry, so accepting a name would mean reporting back
something nothing stored ([protocol.md](protocol.md#deliberate-divergences-from-redis)).
Storing it as tags would make the reason obsolete, but it means introducing
connection-scoped write state to a dialect that has none, for a coarse subset of
what §2 already delivers. Reconsider only if a deployment turns up that cannot
change its call sites at all.

### 6.9 `SELECT db` as a tag namespace

Map database indices to tags, so `SELECT 3` tags everything written afterwards
and `FLUSHDB` invalidates. Rung C via the connection URL, and no new command.
Rejected: sixteen tags is not a tag system, `SELECT` is a documented divergence
that returns one database on purpose, and overloading it would make a client's
`db=3` mean something no other Redis-speaking tool would guess.

### 6.10 Hash tags in the key — `SET {news}article:1 v`

Redis Cluster's `{…}` syntax passes through every client untouched, so a server
that read the braces as tag names would need no client change of any kind — rung
D, and the only proposal here that works through a fully typed API for both
writes *and* invalidation.

Rejected because it welds the tag into the key: the key's spelling now encodes
its invalidation policy, one tag per key at most, and re-tagging means rewriting
every reader. It also hijacks a token that means *routing* to anyone who later
points a cluster-aware client at a vash node — the one interpretation this
server must not quietly contradict, given `cluster_enabled:0` is already
[explained at length](protocol.md#info) as meaning something different here.

### 6.11 A sentinel command before the write

`TAGNEXT news` followed by `SET article:1 v`, with the server holding the tags
for exactly one write. It keeps `SET` untouched and needs no per-command
grammar.

Rejected: it makes two commands one logical operation on a connection where
nothing else is. Automatic pipelining, transparent retry after a reconnect, and
any pool that can hand the second command to a different connection all break it
silently and *wrongly* — the write lands untagged, or worse, wearing the tags of
an unrelated request. A feature whose failure mode is "the wrong record is
invalidated later" is not a feature.

### 6.12 Tags inside the value

A magic prefix on the value that the server strips and interprets. Rung A, and
wrong: values are shared across all three dialects, so a record written this way
over RESP would come back to a memcached or VCP client with the prefix still on
it — or, if the server strips it everywhere, an ordinary value that happens to
start with the magic bytes gets silently truncated. Binary-safe values are not
negotiable.

### 6.13 What clients do today

Encode a namespace into the key, `SCAN MATCH ns:*`, `DEL` the results. It works,
which is why it is the baseline every proposal above is measured against — and
it is O(keyspace) per invalidation, needs `listing_enabled` (off by default),
and races anything written mid-scan. The whole point of the tag machinery is
that invalidation is O(1) and needs no enumeration; §2's job is to let a RESP
client reach it.

---

## 7. Discovery

A client finds out whether the server supports this the way
[protocol.md](protocol.md#command-reference-1) already prescribes: send the
command and read the error. `-ERR unknown command 'DELBYTAG'` is unambiguous and
needs no handshake.

For code that would rather ask than probe, `INFO` carries one line in the vash
section beside the existing `vash_tags` counter:

```text
vash_resp_tags:1
```

*As built*, it is emitted by the RESP `INFO` renderer rather than collected as a
counter, because it is a fact about *this dialect* and the memcached `stats`
payload — which shares the counter list — should not carry it. Like every vash
field it is out of the default `INFO` and reached by `INFO all` or `INFO vash`.
A second line, `vash_tag_prefix`, belongs with §6.6 if that is ever built.

This mirrors how the memcached dialect announces its tag extensions through
`stats`, so each dialect states its tag support where a client of that dialect
already looks.

---

## 8. What would change this decision

- **A client library that cannot send an unknown verb.** None of the major ones
  qualifies today; if a significant one does, §6.6 grows from optional to
  necessary and phase 1 alone stops being enough.
- **`REMTAGS` turning out to be load-bearing.** Re-`SETTAGS` covers attach; nothing
  covers detach. A real use case pulls phase 2 forward.
- **Per-key tag lists in `MSETTAGS`.** The `Command` boundary already supports
  them — `SetMany` holds a `Vec<Set>`, each with its own tags — and only the
  wire grammar shares one list. If batches with differing tags turn out common,
  the grammar can grow a per-pair form without the boundary moving.
