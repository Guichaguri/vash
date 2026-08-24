# Roles — a proposal

[auth.md](auth.md) §16 lists "no per-command permissions, no roles in M9" among
the non-goals, and §9 says why the omission was deliberate rather than an
oversight: *"the credential table has room for a `role` column, and the case for
`peer` is real … but any authenticated client can already invalidate any tag, so
the marginal authority a peer credential would fence off is small, and the check
is one enum comparison to add later. Not built; noted so the shape does not
preclude it."*

Three things have changed since that was written. `FLUSH` and the listings
arrived with server-wide enable flags but no way to give one client a listing
and not another. M12 made a client certificate an identity, so a deployment can
now have a dozen named services on one cache without a dozen shared secrets to
distribute — which is exactly the deployment where "every identity is equally
trusted" starts to grate. And the credential file already refuses an unknown
trailing `key=value` at startup (`auth.rs::parse_line`), which was put there
precisely so that *"a file written for a newer server must not load silently
into an older one with the field dropped … the day `role=` exists"*.

This document cashes that. Same house style as the plan: decision first, then
the reasoning, then what was rejected.

**The recommendation, up front:**

1. **Six roles — `read`, `write`, `invalidate`, `list`, `flush`, `peer` — as a
   bitset on the credential row**, carried by a `roles=` field in the
   credential file. Not per-key patterns, not Redis's category algebra:
   [§3](#3-choosing-the-granularity) argues that the useful axis here is *blast
   radius*, and that this cache has exactly six rungs on it.
2. **Absence of `roles=` means unrestricted**, so every credential file that
   exists today keeps working byte for byte. `auth.require_roles = true` turns
   the permissive default into a startup error for operators who want every row
   explicit. [§4](#4-where-the-roles-live).
3. **The identity carries its roles**, resolved once at authentication.
   The per-command check is one bit test against a `u8` already in `ConnAuth` —
   no table lookup, no lock, nothing on the shared cacheline.
   [§5](#5-server-side-shape).
4. **The gate goes exactly where the authentication gate already goes**, in all
   three dialects, and *not* inside `dispatch::execute`.
   [§6](#6-where-the-check-goes) is the load-bearing section: `execute` is not
   the choke point it looks like, and putting the check there would leave the
   two hottest commands ungated.
5. **`UNAUTHORIZED` (5) is reused on the wire, and `AUTH` grows a reply body
   naming the granted roles.** auth.md §6.2 already widened that status to
   "refused by policy", which is what a role denial is; the reply body is what
   keeps its three meanings distinguishable to a client that cares.
   [§7](#7-the-wire).

---

## 1. What authorization is for here

Stated up front, because as with authentication it decides most of what
follows.

**In scope.** Bounding what one *authenticated* identity can do, so that a
compromised or simply buggy client is limited to the damage its job requires. A
web tier that reads and writes its own keys should not be able to `FLUSH` the
cache, enumerate the keyspace, or advance a tag generation to `u64::MAX`. A
metrics exporter that lists keys should not be able to write. A cluster peer's
credential, if it leaks out of a node's config, should not be a general-purpose
cache credential.

**Out of scope, and this is the important one.** This is not multi-tenancy.
Roles do not partition the keyspace: an identity with `read` can read *every*
key on the server, and an identity with `write` can overwrite every key. Two
services that must not see each other's data need two caches, not two roles —
per-key scoping is [§3.4](#34-per-key-patterns), and it is rejected on grounds
that have nothing to do with effort.

What roles buy is the difference between "this credential leaked and someone is
reading our cache" and "this credential leaked and our cache is gone". Those are
different incidents.

**The threat model does not change.** auth.md §1 still holds: the network
boundary is the primary control, and the wire observer is TLS's problem. Roles
sit *inside* the authenticated set and reduce its blast radius. They defend
against nothing that authentication does not already stop.

---

## 2. Decision

Six roles. Every command needs one of them, and the mapping is fixed in the
server rather than configurable.

| Role | Bit | What it authorises | Why it is its own rung |
|---|---|---|---|
| `read` | `0x01` | `GET`, `GET_MANY`, deadline queries (`TTL`, `EXISTS`, `TYPE`) | Returns a stored **value**. |
| `write` | `0x02` | `SET`, `SET_MANY`, `DELETE`, `DELETE_MANY`, `TOUCH`, `EXPIRE`, `APPEND` | Changes one named key at a time. |
| `invalidate` | `0x04` | `DELETE_BY_TAG` | Reaches keys the caller cannot name, in O(1), across the whole cluster. |
| `list` | `0x08` | `LIST_KEYS`, `LIST_TAGS`, `lru_crawler metadump`, `stats cachedump`, `SCAN` | Enumerates. A reader must already know the key; a lister does not. |
| `flush` | `0x10` | `FLUSH`, `flush_all` | Empties the cache. |
| `peer` | `0x20` | `TAG_SYNC` | Sets a generation to any value, where `DELETE_BY_TAG` moves it by one. |

**Why `delete` is not a seventh role.** It would fence off nothing: an identity
with `write` deletes any key it can name by writing it with a one-second TTL.
A role that records an intent the server cannot enforce is worse than no role,
because the file then documents a boundary that does not exist. The same test
is what justifies each of the other five — `invalidate`, `flush`, `list` and
`peer` all reach something `write` cannot reach at all.

Three commands need **two** roles, because they genuinely do two things:

| Command | Roles | Why |
|---|---|---|
| `GET_AND_TOUCH` / `gat` | `read` + `write` | Returns the value and re-stamps the deadline. |
| `ARITHMETIC` / `INCR` / `incr` | `read` + `write` | Returns the resulting value, and `INCRBY 0` is a `GET` with extra steps. |
| `SET … GET` (`return_previous`) | `read` + `write` | The flag exists to report the displaced value. |

Without that, `roles=write` is a credential that can read any key it can name,
which is not what an operator writing `write` believes they wrote.

**Ungated for any authenticated identity:** `HELLO`, `PING`, `QUIT`, `VERSION`,
`STATS`, `CLUSTER`. These are connection maintenance and introspection with no
storage effect. `STATS` and `CLUSTER` disclose counters, settings and peer
addresses — the same disclosure `/stats` on the admin port makes to anyone who
can reach it (auth.md §10) — and gating them would break every real client
library, several of which call `stats` on connect. The trade is stated rather
than assumed: a `stats` role would fence off configuration disclosure and cost
compatibility with clients nobody controls.

**Where existence leaks, and why that line is where it is.** `read` gates
commands that return a stored *value*. It does not gate the existence and TTL
side channels inherent to writing: `SET … NX` answering `NOT_STORED`, `DELETE`
answering whether it deleted, `incr` missing. Fencing those off would mean
writes that cannot report their own outcome. A deadline query is on the other
side of the line — it returns stored state the caller did not supply, so it is
`read`.

---

## 3. Choosing the granularity

### 3.1 A single `readonly` flag

| | |
|---|---|
| **Shape** | One boolean per row. A read-only credential is refused every write. |
| **Pros** | Trivial: one bit, one comparison, one line of documentation. Covers the most-asked-for case (a dashboard, an exporter, a debugging shell). |
| **Cons** | Says nothing about the three commands with a blast radius larger than a write: `FLUSH`, `DELETE_BY_TAG` and `TAG_SYNC` are all "writes", and lumping them with `SET` is what makes the flag useless for the case that motivated this. It also cannot express `list` — the listings are reads, so a read-only credential gets keyspace enumeration whether the operator wanted it or not. |
| **Verdict** | Rejected, but note what it gets right: it is the 80% case, and §2's set contains it exactly (`roles=read`). |

### 3.2 A role per opcode

| | |
|---|---|
| **Shape** | The bitmask is over opcodes, not categories. `roles=get,set,delete,touch,…` |
| **Pros** | Maximum expressiveness; no judgement calls about which commands group together. |
| **Cons** | It pushes every judgement onto the operator, once per row, forever. Nobody has a use for "may `SET` but not `APPEND`", and the day someone writes that row, `SET` remains a strictly stronger way to do the same thing — so the file records an intent the server cannot enforce. It also makes every new command a config-compatibility event: a row written before `EXPIRE` existed does not grant it, so upgrading the server silently withholds authority. Twenty-one bits is not more precise than six, it is six meaningful ones and fifteen ways to be wrong. |
| **Verdict** | Rejected. The commands do not vary independently; their blast radii do. |

### 3.3 Redis's ACL model — categories, plus per-command and per-key patterns

| | |
|---|---|
| **Shape** | `+@read -@dangerous +get ~cache:*`, an ordered algebra of grants and revocations over categories, commands and key patterns, mutated at runtime through an `ACL` command family. |
| **Pros** | Genuinely more expressive, and familiar to anyone who has configured Redis 6+. |
| **Cons** | The expressiveness is inseparable from an evaluation order that has to be specified, tested and explained — `+@all -@dangerous +flushall` is a rule whose meaning depends on where you stop reading. It presumes a runtime mutation surface (`ACL SETUSER`), which auth.md §16 declined for the credential table itself and which would have to be built here first. And the per-key half of it is [§3.4](#34-per-key-patterns). |
| **Verdict** | Rejected. A cache with 21 commands does not need an algebra; it needs six names an operator can hold in their head. If a deployment genuinely needs Redis's ACLs, it needs Redis. |

### 3.4 Per-key patterns

| | |
|---|---|
| **Shape** | `roles=read:cache:*,write:session:*` — authority scoped to a key prefix or glob. |
| **Pros** | The thing people actually mean when they say "multi-tenancy". |
| **Cons** | It has to be evaluated **per key, not per command**, which puts a glob match on the hot path of `GET_MANY` and `SET_MANY` — a batch of up to `MAX_BATCH_ITEMS` keys becomes that many matches before any storage work happens, on the connection thread. Worse, it has no honest answer for the commands that do not name keys: `DELETE_BY_TAG` invalidates keys the request never mentions, `FLUSH` mentions nothing, and the tag registry is not partitioned by key prefix. A scoping scheme with three commands it cannot express is a scheme that gives operators a false boundary. The listing already has a `pattern` and it is a filter, not a fence. |
| **Verdict** | Rejected, and not "for now". Two tenants that must not see each other's keys should have two caches: the isolation is then a property of the address they connect to, which is checkable from outside the server. This is the same reasoning §1 uses to keep multi-tenancy out of scope. |

### 3.5 Roles as named sets defined in configuration

| | |
|---|---|
| **Shape** | `[roles.filler] allow = ["set", "touch"]` in the main config; rows reference a role by name. |
| **Pros** | An operator names the deployment's actual roles (`filler`, `exporter`, `peer`) rather than restating a capability list per row. Indirection means one edit changes every row that uses it. |
| **Cons** | Two files have to agree — the credential file (mode 600, not committed) and the main config (committed) — and a row referencing a role the config does not define is a startup failure in a *different* file from the one being edited. The indirection also inverts the property that makes the credential file auditable: today one line tells you everything about one identity. |
| **Verdict** | Rejected for v1, and it composes cleanly if it is ever wanted: named sets would expand to the same bitset, so nothing below changes. |

### Summary

| Option | Expressive enough | Hot-path cost | Auditable per row | Verdict |
|---|---|---|---|---|
| `readonly` flag | No | One bit test | Yes | Contained in the choice |
| Per-opcode bitmask | Too much | One bit test | Barely | Rejected — §3.2 |
| Redis ACL algebra | Yes | Ordered evaluation | No | Rejected — §3.3 |
| Per-key patterns | For a problem we do not have | Per key | No | Rejected — §3.4 |
| Named sets in config | Yes | One bit test | No — two files | Deferred — §3.5 |
| **Six roles, per row** | **Yes** | **One bit test** | **Yes** | **Chosen** |

---

## 4. Where the roles live

The credential file's reserved trailing field, which exists for this:

```text
<name>  <algorithm>:<value>  [roles=<list>]
```

```text
# Unrestricted, exactly as it loads today.
legacy-app      sha256:1f3a…

# The web tier: reads, writes, and nothing that empties anything.
web             sha256:8c21…                 roles=read,write

# The invalidation worker: it does not need to read a single key.
invalidator     sha256:44de…                 roles=invalidate

# A dashboard, over mTLS.
dashboard       mtls:dash.svc.internal       roles=read,list

# A cluster peer. See §8 — this row is checked at startup.
peer            sha256:9b07…                 roles=peer
```

**Rules, all of them enforced at load:**

- Names are the six in §2. An unknown name is a **hard error** naming the line,
  under the rule the loader already has: a credential file must never
  half-load. A typo that reads as a smaller grant is exactly as dangerous as
  one that reads as a larger one.
- Comma-separated, no spaces (the format is whitespace-delimited), no
  duplicates, order irrelevant.
- **No `all` keyword.** It would mean "and every role added in future
  versions", which is a grant an operator cannot have consented to. Omitting
  the field is how you say "unrestricted" and it carries that meaning
  explicitly (below).
- **`roles=` with an empty list is refused**, because it is indistinguishable
  at a glance from the field being absent while meaning the opposite. A row
  that should authorise nothing should be deleted, and deleting it is also what
  closes the connections holding it.

**Absence means unrestricted.** Every credential file in existence was written
without this field, and the whole point of the loader's reserved-field error is
that those files are still valid. A row with no `roles=` gets every role,
including roles added later — which is the *opposite* asymmetry from `all`, and
deliberately: absence is a legacy row that predates the question, presence is an
answer to it. `auth.require_roles = true` turns absence into a startup error for
operators who want that answer from every row.

**`VASH_AUTH_SECRET`** builds a one-row `default` table and therefore an
unrestricted identity. It is the deployment that has one credential; there is
nothing for it to be restricted relative to. `auth.require_roles = true` and
`VASH_AUTH_SECRET` together are refused at startup rather than silently
resolving one way or the other, matching how the two credential sources already
refuse to coexist.

**`mtls:` rows take `roles=` like any other row.** A certificate subject is an
identity, not a mechanism — M12's whole argument — so authorization attaches to
the row, not to how the row was satisfied.

---

## 5. Server-side shape

```rust
/// The authority a credential carries. A bitset because the check is on the
/// hot path and the set is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Roles(u8);

impl Roles {
    pub const READ: Self       = Self(0x01);
    pub const WRITE: Self      = Self(0x02);
    pub const INVALIDATE: Self = Self(0x04);
    pub const LIST: Self       = Self(0x08);
    pub const FLUSH: Self      = Self(0x10);
    pub const PEER: Self       = Self(0x20);

    /// A row with no `roles=`: everything, now and later. See §4.
    pub const UNRESTRICTED: Self = Self(u8::MAX);

    #[inline]
    pub fn allows(self, needed: Roles) -> bool {
        self.0 & needed.0 == needed.0
    }
}
```

`allows` takes a set, not a single role, so §2's three two-role commands are one
call and not a special case at each site.

**`Identity` carries the roles**, resolved once when the credential verifies:

```rust
pub struct Identity {
    name: Arc<str>,
    roles: Roles,
}
```

That is the design's load-bearing performance decision. The alternative — look
the row up per command — would put the `RwLock<Arc<Auth>>` and a `HashMap`
probe on every `GET`, on a cacheline every connection thread shares, to answer
a question that cannot change for the life of the identity. `ConnAuth` is
already cloned across the `spawn_blocking` hop on every batch; a `u8` beside
the `Arc<str>` is free in that clone.

**Roles are frozen at authentication.** Editing a row's `roles=` and sending
`SIGHUP` affects the *next* authentication, not connections already holding the
identity — identical to how a changed secret behaves today, and for the same
reason: `AuthState::replace` swaps the table, and *"connections that already
authenticated keep the identity they authenticated with"*. So the rotation
story for authority is the rotation story for credentials, which is already
written and already tested. Narrowing a role in an emergency means restarting,
or removing the row and letting the reconnect fail — the same escape hatch a
compromised secret has.

**Roles are checked whenever an identity is present, including when
`auth.required` is off.** This looks perverse for one moment — with enforcement
off, an authenticated client is more restricted than an unauthenticated one —
and it is the right rule, because it makes the rollout in §14 work: during
auth.md §15's step 2, a client that has been rolled to send its credential
immediately experiences its role set, in production, while everything not yet
rolled keeps working. The perversity is bounded to exactly the window where, by
auth.md §1, the server is not defending anything anyway.

---

## 6. Where the check goes

**This is the section to get right.** The obvious home is
`dispatch::execute` — "the single point every command passes through", as its
own comment says. That comment is true of *counting* and false of execution.
Three paths reach storage:

| Path | Reaches | Goes through `execute` |
|---|---|---|
| `dispatch::execute` | everything not below | — |
| `dispatch::execute_get_into` | `GET` (all three dialects) | **No.** The fused read renders straight out of the store, which is why it exists. |
| `SetBatch::flush` | pipelined `SET` runs (all three dialects) | **No** on the happy path — it calls `store.set_many` directly and only falls back to `execute` when the batch fails. |

So a role check inside `execute` would leave `GET` and pipelined `SET`
ungated: the two hottest commands on the server, and precisely the two an
attacker with a leaked `list`-only credential would reach for. The bypasses are
not accidents — one is M10's fused read, the other is the batching that makes
pipelining fast — and neither is going away.

The gate therefore goes where the **authentication** gate already goes, which is
ahead of both bypasses in all three dialects. Three call sites, all of which
exist today:

1. **VCP** — `dispatch::execute_frame_into`, in the same place the pre-auth
   check sits *ahead of opcode recognition*. The check is on the **opcode**, so
   a refused command never runs its body decoder — the same property the
   pre-auth gate was given, for the same reason (auth.md §6.2: an
   unauthenticated stranger must not drive `decode_set` or `decode_tag_sync`).
2. **memcached** — `dispatch::execute_memcached`, plus `conn_gated`, which is
   what keeps a refused `set` out of the batch. That pairing already exists for
   authentication and the note on `conn_gated` already says why both are needed:
   *"such a command must never reach the batch"*. It grows a `Command`
   parameter.
3. **Redis** — `resp::refusal`, which is already the per-command policy point
   and is already consulted by `batchable_set` before a `SET` can be held back.

**The mapping is an exhaustive `match` on `vash_core::Command`**, written in
`vash-server` beside the credential table rather than in `vash-core`: the
storage tier still never learns authorization exists, and exhaustiveness works
across the crate boundary regardless. A new `Command` variant fails to compile
until it is classified — the same discipline `Command::inline_safe` documents,
and for a sharper reason. `inline_safe` falling through to the wrong answer
stalls a runtime worker; this one hands out authority.

The VCP path checks an opcode and the other two check a parsed command, so
there are two mappings that must agree. They are kept honest by a test that
decodes every opcode and asserts the opcode's roles equal the decoded command's
— not by hoping. (The three two-role commands in §2 have no VCP opcode:
`GET_AND_TOUCH` and `ARITHMETIC`'s disclosing forms arrive over memcached and
Redis, and `SET … GET` is Redis-only. The opcode table is therefore
unambiguous.)

**Deferred to the same place as the enablement gates.** memcached's
`stats cachedump` and `lru_crawler metadump` are `ResponseStyle` variants that
reach the listing without a `vash_core::Command`; they carry `LIST` explicitly
at their two call sites, next to the `listing_gate` call that is already there.

---

## 7. The wire

**No new VCP status.** auth.md §6.2 widened `UNAUTHORIZED` (5) from "command
disabled by configuration" to **"refused by policy"**, explicitly so it could
cover more than one refusal, and a role denial is refused by policy. A new
status code would cost every existing client an unknown value to buy a
distinction the next paragraph makes better.

**`AUTH` grows a reply body: one byte, the granted role bitset.** Today `OK`
carries an empty body. VCP frames are length-prefixed, so a client that ignores
it is unaffected — and a client that reads it can log what it was granted, or
fail fast at startup instead of at 3am on the first `FLUSH`. This is also what
answers the objection to reusing status 5: a client knows whether it
authenticated (auth.md §6.2's argument) *and* now knows what that
authentication granted, so all three meanings of `UNAUTHORIZED` are
distinguishable client-side without a wire change.

An unrestricted identity reports `0xFF`. `vash-client` exposes it on the
`Client` and returns a distinguishable error when a call is refused for
authority rather than for authentication, so the cluster can tell a
misconfigured peer role from a bad peer secret (§8).

**Redis: `-NOPERM`.** Real clients already know it, and it is worth the
divergence from VCP here because the alternative is `-ERR command disabled by
configuration`, which sends an operator to the wrong config file.

```text
-NOPERM this user has no permissions to run the 'flushall' command
```

`-NOAUTH` continues to mean "authenticate", `-WRONGPASS` "that credential is
wrong", and `-NOPERM` "you are who you say you are and the answer is still no".

**memcached: `CLIENT_ERROR unauthorized`.** Upstream has no ACL vocabulary at
all, so anything here is an extension; `CLIENT_ERROR` is the class real clients
already treat as a non-fatal per-command error, which is what this is. It is
deliberately a different string from the `CLIENT_ERROR unauthenticated` the
pre-auth gate renders — the same distinction Redis draws with two codes.

**No new capability bit,** and the reason is a payoff from a decision already
made. A client cannot need to discover role support, because a credential file
using `roles=` **cannot load into a server that does not understand it**: the
unknown trailing field is a startup error, which is what that error was built
for. The mixed-version failure this bit would warn about is a server that
refuses to start.

---

## 8. Cluster peers

This is the case auth.md §9 named, and it is also the one that breaks in the
worst shape, so it comes with the feature rather than after it.

**`peer` is `TAG_SYNC` and nothing else.** Peers are ordinary VCP clients, and
reading `cluster.rs` settles what they actually send: `HELLO`, `AUTH`, and
`TAG_SYNC` — the fan-out path (`PeerMessage::Invalidate`) and the anti-entropy
path (`exchange`) both go through the same `TAG_SYNC` frame. A peer credential
therefore needs `roles=peer` and no more, and that is a real reduction: a peer
secret that leaks out of a node's config today is a full cache credential.

**The startup check.** §9's point 1 established the principle — a node whose
peer credential is missing refuses to start, because *"refusing to start is the
only failure mode an operator cannot miss"*. Roles reopen that hole in a new
shape: `cluster.auth_name` naming a row that exists but lacks `peer` gives a
cluster where writes work, `TAG_SYNC` is refused, invalidations quietly stop
converging, and every node reports itself healthy. So a node that has a
credential file, a `cluster.auth_name` in it, and peers configured **checks its
own row for `peer` at startup and refuses to start without it**. It is a
table lookup at boot against a hole that otherwise takes a day to find.

The check is one-sided by necessity — a node can inspect the credential it
presents, not the row a peer will match it against — which is why the runtime
half matters too. `cluster::is_unauthorized` already classifies a peer's
`UNAUTHORIZED` as a configuration error rather than unreachability, logs it at
`warn` on the transition only, and settles reachability from the round's
outcome. A role denial arrives as the same status on the same path and inherits
all of that for free.

---

## 9. Relationship to the enablement gates

`protocol.flush_enabled` and `protocol.listing_enabled` are **server-wide**, and
they stay exactly as they are. Roles are **per-identity**. The two are ANDed,
server gate first:

| `flush_enabled` | `roles` has `flush` | Result |
|---|---|---|
| false | either | `UNAUTHORIZED` — "not on this server" |
| true | no | `UNAUTHORIZED` / `-NOPERM` — "not for you" |
| true | yes | Flushed |

Server gate first, so a disabled command answers identically to every identity
and a client cannot use a role denial to probe what the server has switched on.
The `FLUSH` and `LISTING` capability bits keep reporting *server* enablement,
unchanged — they answer "is it here", and the `AUTH` reply body answers "is it
mine". `listing_gate`'s comment (*"that one decides who may connect, this one
decides what any connected client may do, and every authenticated client on this
server is equally trusted"*) is the sentence this proposal makes false, and it
needs rewriting to say the gates now compose.

---

## 10. What an operator sees

- **`vash_auth_denied_total{role}`** — a counter per role, fixed cardinality
  six, beside the existing `auth_ok` / `auth_failed` / `auth_refused`. Denials
  are *not* folded into `auth_refused`: that one counts commands refused for
  want of authentication, and an alert that cannot tell "nobody rolled their
  credential" from "someone's role set is wrong" is an alert that gets muted.
- **A `warn` log naming the identity, the command and the missing role**, on
  the transition rather than per command — the rate-limiting shape §8 already
  uses for a peer's refusal, and for the same reason: a wrong role set will not
  fix itself and will otherwise fill the log at line rate.
- **`vash_roles` in `stats conns`**, beside `vash_authenticated`,
  `vash_auth_method` and `vash_tls`. The reasoning already written for
  `vash_tls` — *"the rollout's only honest progress bar … no aggregate can say
  which client it is"* — applies verbatim to an operator about to add `roles=`
  to a row and wondering which live connections hold that identity.

---

## 11. What it costs

| | |
|---|---|
| **Hot path** | One `u8` AND and a compare, on a value already in the connection's `ConnAuth`. No lock, no map probe, no allocation, no atomic. Below measurement noise on a path where the `spawn_blocking` hop costs microseconds. |
| **Per connection** | One `u8` in `Identity`, inside an existing allocation's padding. |
| **Startup** | Parsing one more field per row, and one lookup for §8's peer check. |
| **Wire** | One byte on the `AUTH` reply. Nothing on any other frame — no request carries an identity, which is auth.md §5's decision and stays true. |
| **Code** | Three call sites that already exist, one exhaustive `match`, one loader field. |

The check is genuinely free; what this feature costs is the operational risk in
§18, not cycles.

---

## 12. Configuration surface

```toml
[auth]
require_roles = false   # a row without `roles=` is a startup error (§4)
```

That is the whole addition. There is no `auth.roles_required` runtime switch and
no per-role enable — **the unit of rollout is the row**, which is finer than a
global flag and needs no new state. An operator restricts one identity, watches
`vash_auth_denied_total`, and moves to the next.

`[cluster] auth_name` gains the startup check in §8 but no new key.

---

## 13. Testing

- **The property test extends.** M9's *"no command in any dialect executes
  unauthenticated"* becomes *"no command in any dialect executes without its
  roles"*, driven over the same three dialects. It is what catches the §6
  bypasses if someone routes a fourth path around the gate — and it must
  exercise **pipelined** batches specifically, because the batching path is one
  of the two bypasses.
- **The two mappings agree**: every VCP opcode decoded and compared against the
  roles of the command it produces.
- **Every role is denied and granted at least once per dialect**, including the
  three two-role commands, whose failure mode (`roles=write` reading values) is
  invisible in a test that only checks the happy path.
- **Loader tests** for each rule in §4, all asserting a *hard* error: unknown
  role, empty list, duplicate, `all`, `require_roles` with a bare row,
  `require_roles` with `VASH_AUTH_SECRET`.
- **A three-node cluster converges with `roles=peer`**, and a node whose row
  lacks `peer` refuses to start — the pair M9 already runs for a missing peer
  credential.
- **A file with `roles=` is refused by a build without this feature**, which is
  the §7 claim that no capability bit is needed. That test is a regression
  guard on `parse_line`'s reserved-field error.

---

## 14. Rollout

Roles have no global switch to flip, so the rollout is per identity and can stop
anywhere:

1. Add `roles=` to **one** row — the one whose job is narrowest and best
   understood. Everything else stays unrestricted.
2. `SIGHUP` (or restart on Windows, per M9's carried-forward limitation). The
   identity's *next* authentication picks it up; live connections keep what they
   have (§5).
3. Watch `vash_auth_denied_total` and the `warn` log. A wrong guess names the
   identity, the command and the missing role, so the fix is the same edit
   again.
4. Repeat. When every row is explicit, set `auth.require_roles = true` so the
   next row somebody adds cannot be unrestricted by omission.

**There is deliberately no audit-only mode.** auth.md §15's sentence applies
unchanged — *"a mode where unauthenticated clients still work is not
authentication, it is a log line"* — and a mode where denied commands still run
is not authorization for the same reason. The per-row granularity is what makes
the safe rollout possible without one: the blast radius of a wrong role set is
one identity, and step 1 chooses which.

---

## 15. Rejected

Beyond §3's granularity options:

- **A `role` column rather than a `roles` set** (one role per credential, as
  auth.md §9 sketched). A web tier needs `read` and `write`; forcing a
  `readwrite` compound role recreates the category algebra one name at a time.
  The plural was the right correction to make while the field is still
  hypothetical.
- **Roles resolved per command instead of at authentication.** It would make a
  `SIGHUP` narrow a live connection's authority, which sounds better than it is:
  it puts a lock and a map probe on the hot path, and it makes authority
  inconsistent with the identity's own secret, which already only changes at the
  next authentication. Emergency revocation is a restart, as it is today.
- **Denying `STATS` to a restricted identity.** §2 — it breaks real client
  libraries to fence off what the admin port already discloses.
- **A `ROLES` capability bit.** §7 — the credential file cannot load into a
  server that would need it.
- **`ACL`-style runtime mutation.** auth.md §16 declined runtime credential
  mutation, and authority is not a weaker case than the secret it attaches to.

---

## 16. Non-goals

No per-key or per-prefix scoping (§3.4) and therefore no multi-tenancy; no
runtime mutation and no `ACL` command family; no roles on the admin port, which
is still uncovered by design (auth.md §10); no `stats` or `cluster` role (§2);
no per-role rate limiting; no audit-only mode (§14); no named role sets in the
main config (§3.5, deferred rather than refused); and no change to what a
request carries on the wire — an identity is still a property of a connection,
established once.

---

## 17. Milestone

| # | Scope | Exit criteria |
|---|---|---|
| **M13** | `Roles` bitset and the `roles=` field; the mapping from `Command` and from `Opcode`; the gate at the three existing authentication call sites; the `AUTH` reply body and `vash-client` exposure; `-NOPERM` and `CLIENT_ERROR unauthorized`; `auth.require_roles`; the peer-role startup check; the denial counter, the `warn` log and `vash_roles` in `stats conns` | The property test proves no command in any dialect executes without its roles, **including pipelined batches and the fused `GET`**; a `roles=write` identity cannot read a value through `gat`, `incr`, or `SET … GET`; a three-node cluster converges with `roles=peer` and a node whose row lacks it refuses to start; every credential file that loads today loads unchanged; a `roles=` file is refused by a build without the feature |

Sequencing: the loader and the bitset first (they are testable with no server),
then the `Command` mapping and its exhaustive match, then VCP (the dialect we
own end to end, and `vash-client` is the test driver everything else uses), then
Redis and memcached, then the cluster — which cannot be done before the client.
The same order M9 used, for the same reasons.

---

## 18. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| A role set that is too narrow breaks a working client in production | An outage in the one place the operator was trying to make safer | The unit of rollout is one row (§14); the denial names the identity and the command within seconds (§10); the fix is one line and a `SIGHUP` |
| The gate is added at a fourth path into storage that bypasses all three call sites | Silent loss of authority checking on whichever command took that path — the §6 failure, regrown | The property test drives all three dialects including pipelined batches; the exhaustive `match` makes a new *command* a compile error, though not a new *path* |
| The opcode mapping and the command mapping drift | An opcode authorised as one role executes as another | They are compared in a test that decodes every opcode (§13) |
| A peer row without `peer` | A cluster that stops converging while reporting itself healthy | Startup refusal on the local row (§8), plus the existing transition-logged `UNAUTHORIZED` classification |
| `roles=write` believed to exclude reads | A credential quietly stronger than its name | §2's two-role rule, and a test per fused command (§13) |
| Operators read roles as tenant isolation | Data exposure believed to be prevented | §1 and §3.4 say so in the two places anyone will look, and the `AUTH` reply reports the roles rather than a scope |
