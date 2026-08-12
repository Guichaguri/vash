# Authentication — design

Status: **built (M9), off by default.** This document is to authentication what
[plan.md](plan.md) is to the rest of the server: decision first, then the
reasoning, then the rejected alternatives. Sections marked *as implemented*
record where building it changed the design.

`AUTH` (`0x03`) is answered over VCP, Redis takes `AUTH` and `HELLO … AUTH`,
memcached takes upstream's ASCII mechanism, and cluster peers authenticate like
any other client. Nothing is enforced until `auth.required` is set, and the
network boundary remains the primary control (§1).

---

## 1. What authentication is for here

Stated up front, because it decides most of what follows.

**In scope.** Stopping a party who can reach the port from using the cache: an
adjacent service in a shared cluster, a process on a co-tenant host, an
accidental bind to `0.0.0.0`, a container network that turned out flatter than
the diagram. The realistic attacker reaches the port but does not sit on the
wire between a legitimate client and the server.

**Out of scope, and this is the important one.** Authentication does not make
the connection private. Without TLS — a v1 non-goal (plan.md §16) — every key
and every value crosses the network in the clear. A party who can read the wire
already has the data; hiding the password from them while streaming the payload
past them protects the wrong thing. **The eavesdropper is TLS's problem, not
authentication's**, and that single observation removes most of the pressure
towards elaborate credential schemes (§3).

**Also out of scope:** protecting one authenticated client from another. There
is one keyspace, no namespaces (`SELECT` is a documented divergence), and
`FLUSH`/`DELETE_BY_TAG` are cache-wide by design. Every authenticated client is
equally trusted, and the existing `flush_enabled`/`listing_enabled` gates stay
exactly as they are — they limit what *anyone* may do, which is a different
control from who may connect.

---

## 2. Decision

**A connection is authenticated once, at the start, against a small credential
table. Nothing crosses the wire but the secret itself, compared in constant
time against a SHA-256 verifier. Enforcement is a single flag; the table is
loaded from a file at boot.**

Five parts, each argued below:

1. **Per connection, not per request** (§5). Verified once; steady-state cost is
   a branch on a bool already in a register.
2. **A credential table, not a single secret** (§3.6, §4) — `name → verifier`,
   where the single-secret deployment is a table with one row. Rotation and
   per-service credentials fall out; nothing about the wire format changes with
   the number of rows.
3. **Plaintext on the wire, hashed at rest** (§3.1–3.3). Not because it is the
   strongest option, but because memcached and Redis clients can send nothing
   else, and because §1 says the wire observer is TLS's problem. A VCP-only
   challenge–response mechanism is *specified* (§6.3) and deliberately not built.
4. **SHA-256, not a password KDF** (§3.2). The credential is machine-generated
   with ≥128 bits of entropy; a slow KDF buys nothing against that and turns
   connection setup into a self-inflicted denial of service.
5. **The gate lives in the executor, not the parser** (§5). A refused command
   must still consume its own bytes and emit its refusal in the position it
   occupied, which is the resynchronisation rule the three protocols already
   obey.

---

## 3. Choosing a credential scheme

This is the part with real alternatives. Each is judged on four axes: what
crosses the wire, what is stored on the server, what it costs per connection,
and whether existing memcached and Redis clients can speak it at all — because a
mechanism those clients cannot send is a mechanism that only protects VCP.

### 3.1 A plain shared secret, stored in plaintext

The Redis `requirepass` model. One string in the config; the client sends it;
the server compares.

| | |
|---|---|
| **Pros** | No dependencies at all — the comparison is a fold over two byte slices. Every memcached and Redis client already supports it. Nothing to explain to an operator. Zero measurable cost per connection. Trivially correct, which for security code is worth more than it sounds. |
| **Cons** | The secret is readable by anyone who can read the config file, a backup of it, or the process's environment — and config files end up in version control. A naive `==` leaks a timing oracle over the shared prefix; on a LAN, and with enough samples, that is a real attack rather than a theoretical one. One secret for everyone means rotation is a synchronised restart of every client. |
| **Verdict** | The comparison is right; storing it in the clear is the weak half. |

### 3.2 A shared secret, hashed at rest

The same wire behaviour, but the server stores `SHA-256(secret)` and compares
digests. This is what Redis ACL's `#<sha256hex>` form does.

| | |
|---|---|
| **Pros** | The config file no longer contains a usable credential — leaking it costs an attacker a preimage search over a 128-bit space, which is to say it costs them nothing they can afford. Digests are fixed-length, so constant-time comparison is easy to get right. Two small, boring dependencies (`sha2`, `subtle`). Still ~1 µs per connection, once. |
| **Cons** | The secret is still plaintext on the wire and still plaintext in whatever deploys it. Offers nothing at all against a wire observer. |
| **Verdict** | **Chosen.** It is strictly better than §3.1 for two dependencies and a microsecond, and it costs the client nothing. |

**On password KDFs (argon2, bcrypt, scrypt, PBKDF2).** The obvious next step —
and wrong here. A KDF exists to make *offline guessing of a low-entropy human
password* expensive, at a deliberate cost of 50–100 ms per verification. This
credential is not a human password: it is a machine-generated token with at
least 128 bits of entropy, where guessing is already infeasible against a fast
hash. Meanwhile the cost lands on **connection setup**, on a server that is
built to accept connections quickly, which hands an unauthenticated attacker a
CPU amplification factor of roughly a hundred thousand: one cheap connect burns
100 ms of a core. A cache server that took argon2 seriously would need a cache
in front of its authentication, which is where the idea collapses under its own
weight. Fast hash, high-entropy secret, minimum length enforced at startup.

### 3.3 Challenge–response (HMAC-SHA-256 over a server nonce)

Server sends a random nonce; client replies `HMAC(secret, nonce ‖ user)`. The
secret never crosses the wire. SASL CRAM-MD5 and SCRAM are this shape.

| | |
|---|---|
| **Pros** | A passive observer learns nothing reusable, and a captured exchange cannot be replayed (the nonce is per connection). Protects against the *most common* real-world credential leak, which is not packet capture but a secret landing in a log, a proxy trace, a shell history or a crash dump. Server-side storage can still be a digest-shaped verifier. |
| **Cons** | An extra round trip on every connect — irrelevant for pooled connections, painful for a client that opens one per request. Needs a CSPRNG and a nonce store. **No memcached or Redis client can do it**, so it protects only VCP, and only VCP clients we write. It does nothing against an active man-in-the-middle without channel binding, which needs TLS — so the threat model it covers is a narrow band between "passive observer" and "TLS". And §1's point stands: that observer is reading every value in the cache anyway. |
| **Verdict** | Specified (§6.3), not built. Worth having as an option for the log-leak case; not worth blocking M9 on, and never a reason to skip TLS. |

### 3.4 Asymmetric keys — a signed nonce, or client certificates

Client holds a private key; server holds only the public half. Ed25519 over a
challenge, or full X.509 client certificates.

| | |
|---|---|
| **Pros** | The server stores nothing secret at all, so a compromised server config yields no credential. Per-client identity and revocation without touching other clients. Certificates carry an expiry, which forces rotation to actually happen. |
| **Cons** | Ed25519 verification is ~50 µs — fine per connection, but it is now the most expensive thing in connection setup and an unauthenticated party controls how often it runs. Key distribution is a genuinely harder operational problem than secret distribution, and half-done PKI is worse than a good secret. No memcached or Redis client can speak it. Client certificates are *mTLS*, which means the TLS work has to happen first, at which point the interesting question is TLS's, not this document's. |
| **Verdict** | Rejected for v1. If the deployment justifies PKI it justifies mTLS (§3.7), and that is one mechanism instead of two. |

### 3.5 Bearer tokens — JWT, macaroons, signed capabilities

A token issued elsewhere, carrying an expiry and possibly a scope, verified by
signature rather than by lookup.

| | |
|---|---|
| **Pros** | Stateless verification — no credential table to distribute, which is attractive across a cluster. Built-in expiry limits the damage from a leak. Scopes could one day express "may read, may not `FLUSH`". Integrates with an existing identity provider if one is already deployed. |
| **Cons** | Enormously more machinery: a JWT parser (a new untrusted-input surface, on the pre-auth path, needing its own fuzz target), signature verification, clock skew handling, `alg` confusion pitfalls, key rotation, and an issuer that must exist. A bearer token on a plaintext wire is exactly as interceptable as a password. Expiry means re-authenticating a long-lived pooled connection, which needs a re-auth story the other mechanisms do not. All of this to serve a cache whose entire authorisation model is one bit. |
| **Verdict** | Rejected. The scoping it would buy is a problem this server does not have (§1). If it ever does, the credential table (§3.6) is where roles go, not the wire format. |

### 3.6 A database of users

Multiple identities, each with its own credential — Redis ACL's model, and
memcached's `-Y` auth file.

The question splits in two, and the two halves have opposite answers.

**A credential *table*: yes.** `name → verifier`, a handful of rows, loaded into
a `HashMap` at boot.

| | |
|---|---|
| **Pros** | Rotation without a flag day: add the new credential, roll the clients, remove the old one — impossible with a single secret. Per-service credentials mean one compromised app is revoked on its own. Logging and metrics can name *which* identity failed, which is the difference between an actionable alert and a number. A role column later separates a peer's authority from a client's (§9) without a wire change. The single-secret deployment is just a one-row table, so this costs nothing to those who want the simple thing. |
| **Cons** | One `HashMap` lookup before the compare — nanoseconds, once per connection. The operator has one more concept. Redis's 1-argument `AUTH` has no username, so it needs a designated default identity (§8). |
| **Verdict** | **Chosen.** The rotation argument alone pays for it. |

**A mutable user *database*: no.** Runtime `ACL SETUSER`, or credentials stored
in LMDB.

| | |
|---|---|
| **Pros** | Change credentials without a restart or a config push. |
| **Cons** | Storing them in LMDB puts authentication inside the thing that `ephemeral` mode, `wipe_on_start` and a failed integrity check are all licensed to erase — locking everyone out of a cache, or worse, opening it. It makes the cluster's shared-nothing model (plan.md §10) a lie: a user added on one node does not exist on another, and replicating them is the replication this project deliberately does not have. A runtime mutation command is a privilege-escalation target that has to be authorised by something. **The peer list is already static config for exactly these reasons**; credentials belong in the same place, distributed by whatever already distributes the peer list. |
| **Verdict** | Rejected. Reload from file on `SIGHUP` covers the real need (§4). |

### 3.7 Transport authentication — mTLS

Identity as a property of the connection, established by the TLS handshake
before a byte of protocol is read.

| | |
|---|---|
| **Pros** | The only option here that solves confidentiality and authentication together, which per §1 is the pairing that actually matters. Strong identity, revocable, expiring. Terminates in front of the protocol code, so all three dialects get it at once with no per-protocol work. `rustls` is mature. |
| **Cons** | Plan.md §16 makes TLS a v1 non-goal, and this document does not get to overturn that. The handshake is real per-connection cost (~1 ms and an allocation storm), which for a server measured in microseconds is a change in kind. It breaks every plain memcached and Redis client unless the deployment fronts them with a terminator. Certificate lifecycle is the largest operational burden of any option here. |
| **Verdict** | Out of scope, and **the right long-term answer** for anyone who needs the wire protected. §5's design keeps it a wrapper around the connection rather than a change to it. |

### 3.8 Not authenticating at all

The status quo, and it deserves to be on the list rather than assumed away.

| | |
|---|---|
| **Pros** | Zero cost, zero code, zero pre-auth attack surface, nothing to misconfigure. **A network boundary is a stronger control than a password**: a firewall rule or a network policy stops a party who never gets to send a byte, where a password only stops them after they have reached a parser. It is what memcached shipped for fifteen years, and the reason is not laziness. A Unix socket with file permissions is authentication by the operating system, already supported, and unforgeable. |
| **Cons** | Defence in depth of exactly one layer: one bad security-group edit and the cache is open. "Bind it to a private network" is advice an operator can silently fail to take, and there is no way for the server to notice. It cannot express "these three services, not that fourth one on the same subnet". Compliance regimes require an authentication control regardless of the network, and that requirement is not going away. |
| **Verdict** | Stays the default. Authentication is opt-in, and the documentation continues to say the network boundary is the primary control — this adds a layer, it does not replace one. |

### Summary

| Option | On the wire | At rest | Per connection | memcached / Redis clients | Verdict |
|---|---|---|---|---|---|
| Plain secret, plaintext at rest | secret | secret | ~0 | yes | Half right (§3.1) |
| Plain secret, digest at rest | secret | SHA-256 | ~1 µs | yes | **Chosen** |
| Password KDF at rest | secret | argon2 | 50–100 ms | yes | Rejected — a DoS you built yourself |
| Challenge–response (HMAC) | a MAC | verifier | ~2 µs + 1 RTT | **no** | Specified, not built (§6.3) |
| Signed nonce (Ed25519) | signature | public key | ~50 µs | **no** | Rejected — if PKI, then mTLS |
| Bearer token (JWT) | token | issuer key | ~100 µs | no (needs a password slot) | Rejected — machinery ≫ problem |
| Credential table | — | N verifiers | one lookup | yes | **Chosen** |
| Mutable user database | — | LMDB / runtime | one lookup | yes | Rejected — wipeable, and cluster-incoherent |
| mTLS | handshake | CA + certs | ~1 ms | needs a terminator | Out of scope; the right end state |
| Nothing | — | — | 0 | yes | **Stays the default** |

---

## 4. Where the credentials live

**A separate file, referenced by config, never inline in `vash.toml`.**

```toml
[auth]
required = false          # enforcement. Credentials may be configured while this is false — see §15
file = "/etc/vash/credentials"
```

```
# /etc/vash/credentials
default      sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
billing-api  sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae
peer         hmac-sha256-key:4f8a2c91e0b7d3465a8e1f02c7d94b3e6a05f81c2d7e94b306a1f8c25d7e0b43
```

Reasons, in order:

1. **The main config is not secret and gets committed.** A credential file has
   its own path, its own permissions, and its own place in a secret manager.
2. **It can be reloaded.** `SIGHUP` re-reads it and swaps an `Arc`; existing
   connections keep the identity they authenticated with, new ones see the new
   table. That is the whole of the rotation story, and it is why §3.6 does not
   need a runtime mutation command.
3. **Permissions are checkable.** Startup refuses a file that is group- or
   world-readable on Unix, the same way `ssh` refuses a loose private key. A
   check the server can make is worth more than a sentence in a manual.
4. **It matches the peer list.** Static config, distributed by the operator's
   existing tooling. One operational model, not two.

### The line format

**Whitespace-separated fields, one credential per line, with the algorithm named
explicitly in the value.** The shape is `~/.ssh/authorized_keys`, deliberately:
a flat list of credentials, one per line, each carrying an algorithm tag, `#`
comments, revoked by deleting a line and added by appending one. That is this
file's job exactly, and an operator already knows how to read, diff, review and
template it.

```
<name>  <algorithm>:<value>  [key=value …]
```

| Field | Rule |
|---|---|
| `name` | 1–64 bytes of `[A-Za-z0-9_.-]`. No colon and no whitespace, so both splits are unambiguous. Duplicates are a startup error, not last-one-wins. |
| `algorithm:value` | One of the two below. The algorithm is **always** present; there is no default and a bare value is refused. |
| trailing `key=value` | Reserved. None are defined; an unknown one is a startup error, so the field cannot be silently ignored the day `role=` exists (§9). |

Blank lines and lines whose **first non-whitespace character** is `#` are
skipped. `#` is a comment marker and nothing else — it never appears inside a
field, which is why it is not also a delimiter.

| Algorithm | Value | Mechanism | Meaning |
|---|---|---|---|
| `sha256:` | 64 lowercase hex characters | 0 `PLAIN` only | `SHA-256(secret)`. The file holds no usable credential. |
| `hmac-sha256-key:` | 64 lowercase hex characters — a 32-byte key | 1 `HMAC_SHA256` only | **The secret itself.** The server needs the key to recompute a MAC (§6.3). |

**As implemented:** the algorithm token `hmac-sha256-key` is recognised and then
**refused at startup**, naming §6.3, because the mechanism that would consume it
is not built. A row that parses but can never authenticate is worse than one
that is rejected — it reads as a working credential. The token is reserved here
so that building mechanism 1 is not a file-format change.

The two are **not interchangeable**, and a row is bound to its mechanism: a
`PLAIN` attempt against an `hmac-sha256-key:` row is refused even if the bytes
would match, and vice versa. Without that rule the raw key in an
`hmac-sha256-key:` row is a password that logs in over `PLAIN`, which would
defeat the entire point of storing a digest for everyone else.

Hex for both, rather than base64 for the key, because one encoding is one
parser and no new dependency — and because it makes the two forms visibly the
same width, so a row that is the wrong kind is obvious from the algorithm tag
rather than from the length.

**A value beginning with `$` is refused at startup**, with a message saying so.
That reserves modular crypt format — `$5$`, `$2y$`, `$argon2id$` — for the day a
real KDF row is wanted, at which point the identifier will mean what it means
everywhere else and be verifiable by existing tools. Taking the label without
implementing the algorithm is the one thing this format must not do; refusing it
loudly is cheaper than a silent mismatch against a hash someone pasted from
`mkpasswd`. An unrecognised algorithm is likewise a startup error rather than a
skipped line: a credential file that half-loads is how a server ends up
accepting fewer identities than its operator believes.

Nothing here is a standard, and no format for this is. What it does instead is
avoid resembling one it does not implement — the same reason `$5$`, CSV and
properties syntax were all rejected. A whitespace-separated line list has no
spec to violate.

### Generating credentials

`VASH_AUTH_SECRET` in the environment configures a single `default` credential
without a file, for containers where a one-row table does not justify a mount.
Startup refuses both at once rather than silently preferring one.

`vash-server auth-gen [name]` generates a 32-byte secret from the system CSPRNG,
prints the secret once on stderr and the finished file line on stdout, and never
writes either anywhere. Generating rather than accepting a secret is what keeps
§3.2's assumption true in practice: a fast unsalted hash is the right storage
for a high-entropy token and the wrong one for `1234`, and the tool is where
that gets decided. Startup still refuses a secret shorter than 16 bytes and
warns on a verifier matching a small built-in list of the usual suspects, for
credentials that arrive some other way.

---

## 5. Server-side shape

Where each piece goes, in the existing code.

**The table.** A new [`vash-server/src/auth.rs`](../crates/vash-server/src/auth.rs):
`Auth { required: bool, table: HashMap<Box<[u8]>, Credential> }`, built from
config and held in [`ServerState`](../crates/vash-server/src/state.rs) beside
`flush_enabled`. Verification is `table.get(name)` then a constant-time compare
of two 32-byte digests. It touches no store.

**As implemented:** `RwLock<Arc<Auth>>` rather than the `ArcSwap` this section
first proposed. The table is read once per *connection*, never per request, so
the lock is not on any path worth optimising and a dependency is saved.

**The connection's state.** A local in
[`conn.rs::handle`](../crates/vash-server/src/conn.rs), exactly like
`resp_version`: `let mut identity: Option<Identity> = None`. Per connection,
never shared, and dropped when the socket closes. Two consequences to get right:

- The `spawn_blocking` hops in `drain_memcached` and `drain_resp` must thread
  the identity through and copy it back on return — `drain_resp` already does
  this dance for `version` and is the template.
- A `HELLO` and an `AUTH` may arrive in the same pipelined read. Authentication
  therefore has to take effect *within* a block, not between reads, which is
  again what `version` already requires.

**The gate goes in the executor, not the parser.**
[`dispatch.rs`](../crates/vash-server/src/dispatch.rs) and
[`resp.rs`](../crates/vash-server/src/resp.rs) grow one check ahead of the
match: if `required` and the identity is absent and the command is not in the
pre-auth set, emit the refusal and return. It must be there and not in the
decoder for the reason the resynchronisation rules already state: `set k 0 0
5\r\nhello\r\n` must consume its data block even when refused, or the next
command on that connection is read out of the middle of a value. The parsers
stay pure functions of bytes and learn nothing about identity.

**As implemented, VCP refuses one step earlier than that — from the frame
header, before the body is decoded at all.** The two are not in tension: the
text dialects are length-delimited by their *parsers*, so the gate cannot
precede parsing there, while a VCP frame's boundary comes from a fixed twelve
byte header that is already validated before the frame is split off. Taking the
refusal there costs nothing and shrinks the pre-authentication attack surface
from every body decoder in the protocol down to the header plus `decode_auth`.
Those decoders are all fuzzed, so this is defence in depth rather than a hole
being closed — but a gate that runs after the parsing it protects is not much of
a gate.

This was found by the exhaustive test in §14 rather than by design: `STATS` is
refused by the decoder as unimplemented, so it answered `UNSUPPORTED` where
every other opcode answered `UNAUTHORIZED`. The narrow fix was a special case;
the real one was that the gate was in the wrong place. It has a second benefit
the original design did not have — an unknown or unimplemented opcode is now
`UNAUTHORIZED` like everything else, so an unauthenticated party cannot
enumerate which opcodes the build implements.

**No new domain concept.** `vash-core::Command` gains nothing. Authentication is
a property of a connection, not an operation on a cache, and the storage tier
never learns that it exists. VCP's `AUTH` is answered in `dispatch.rs` before
`execute` is reached, the way `HELLO` already is.

**Metrics** (`metrics.rs`): `auth_ok`, `auth_failed`, `auth_refused`,
`auth_timeouts` and `auth_capacity_rejected`. A failure rate that is not zero is
the alert worth having; a *sudden* zero on a required-auth server is worth one
too.

**As implemented:** failures are one counter rather than split by reason.
Distinguishing `unknown_name` from `bad_secret` in a metric would publish the
distinction §8 deliberately refuses to put in the error reply — an unauthorised
observer with access to a dashboard could confirm which names exist by watching
which counter moves.

---

## 6. VCP

### 6.1 `AUTH` (0x03)

The opcode is already reserved for this and the decoder already refuses it, so
adding it collides with nothing a client may have probed.

**Request body:**

| Offset | Field |
|---|---|
| 0 | `mechanism` u8 — 0 `PLAIN`, 1 `HMAC_SHA256` (§6.3) |
| 1 | `name_len` u8 — 0–64; 0 means the `default` identity |
| 2 | `secret_len` u16 — 0–512 |
| 4 | `name` bytes[`name_len`] |
| 4 + `name_len` | `secret` bytes[`secret_len`] |

**Response:** `OK` with an empty body, or `UNAUTHORIZED` (5) with an empty body.
An unknown `mechanism` is `UNSUPPORTED` (8), so a client can probe for §6.3
without a capability bit. A body that does not match the layout is
`BAD_REQUEST` (3) — counted as a failed attempt (§12), because a malformed body
is as good a brute-force vehicle as a well-formed one.

**`NO_REPLY` is ignored on `AUTH`; the response is always sent.** A client that
cannot learn whether it authenticated will pipeline a batch into a connection
that refuses all of it. This is the only opcode that overrides the flag, and it
is documented rather than silently handled.

Re-authenticating on an already-authenticated connection is allowed and replaces
the identity; a failed re-authentication leaves the existing one intact and
counts as a failure. Symmetry with the first attempt, and it makes a long-lived
pooled connection able to follow a rotation without reconnecting.

### 6.2 Ordering, `HELLO`, and the pre-auth set

**`HELLO` must stay legal before `AUTH`**, because first-byte detection
([`detect`](../crates/vash-proto/src/lib.rs)) requires the connection to open
with opcode `0x01` — there is no way to authenticate before announcing the
dialect. So the sequence is `HELLO` → `AUTH` → everything else.

`HELLO` therefore discloses, to an unauthenticated party: protocol version,
shard count, key and value limits, and the capability bits. That is a deliberate
disclosure, and it is the minimum that lets a client discover it must
authenticate rather than guess from a refusal.

A new capability bit:

| Bit | Name | Meaning |
|---|---|---|
| `0x10` | `AUTH_REQUIRED` | This connection must send `AUTH` before any other command. |

Set only when `auth.required` is on — the same contract as `CLUSTER` and
`LISTING`, which report what is enabled here rather than what the build knows.

**Pre-auth set: `HELLO` and `AUTH`. Nothing else, `PING` included.** `PING`
looks harmless, but everything it could tell an unauthenticated party, `HELLO`
already told them, and `/health` on the admin port is the health check an
operator should be using. Every other opcode is `UNAUTHORIZED` (5).

**`UNAUTHORIZED` (5) is reused rather than split.** Its documented meaning
widens from "command disabled by configuration" to "refused by policy", which
covers both. A client does not need the two distinguished, because it knows
whether it has authenticated: the same code means "authenticate and retry" to a
client that has not, and "this will never work" to one that has. A second status
code would buy nothing and cost every existing client an unknown value.

### 6.3 The challenge–response mechanism (specified, not built)

Reserved so that building it later is not a wire change. `mechanism = 1`:

1. Client sends `AUTH` with `mechanism = 1`, its name, and `secret_len = 0`.
2. Server replies `OK` with a 32-byte random nonce as the body, and remembers it
   for this connection only.
3. Client sends `AUTH` with `mechanism = 1`, its name, and
   `HMAC-SHA256(secret, nonce ‖ name)` as a 32-byte secret field.
4. Server recomputes and compares in constant time.

The verifier for a `HMAC_SHA256` identity is the secret itself, not a digest of
it — the server needs the key to recompute the MAC. That is what the
`hmac-sha256-key:` row in §4 holds, why it is a separate algorithm tag rather
than a flag, and why such a row can never satisfy a `PLAIN` attempt. **It is
also a real cost and must be stated where an operator will see it**: enabling
this mechanism trades
"the config file holds no usable credential" (§3.2) for "the wire carries no
usable credential" (§3.3). They are not both available at once without SCRAM,
and SCRAM is more machinery than §3.3's verdict supports.

A nonce is single-use and expires with the connection's auth deadline (§12).

### 6.4 The client

[`vash-client`](../crates/vash-client/src/lib.rs) grows
`Client::connect_with(addr, credential)`, which sends `HELLO` then `AUTH` before
returning, and returns a distinguishable error on refusal so a caller can tell a
bad credential from a dead server. `connect()` stays as it is and is what an
unauthenticated deployment keeps using. The cluster needs this (§9), and it is
the integration-test driver, so it needs it first.

---

## 7. Memcached

**Upstream's ASCII authentication, verbatim.** memcached 1.5.15 added `-Y
<authfile>`; the client authenticates by sending what looks like a `set` whose
key is the username and whose data block is `<user> <pass>`:

```
set billing-api 0 0 34\r\n
billing-api s3cr3t-token-goes-here-here\r\n
→ STORED\r\n          (or CLIENT_ERROR on failure)
```

It is an ugly mechanism — credentials tunnelled through a storage command,
because the text protocol had no room for a new verb that old clients would
tolerate. It is also the only thing memcached clients implement, and
compatibility is the entire reason the dialect exists here. Implementing our own
`auth` verb instead would be a command no client sends.

**Detection is unaffected**: the line starts with `s`, a lowercase letter, so
[`detect`](../crates/vash-proto/src/lib.rs) still routes it to the memcached
parser on the first byte.

**Where it hooks.** [`text.rs`](../crates/vash-proto/src/memcached/text.rs)
already routes `set` through the storage-command path that reads a
length-delimited block. The parser keeps doing exactly that and gains nothing;
the *executor* decides, when the connection is unauthenticated and the command
is a `set`, that the block is a credential rather than a value. Keeping the
decision out of the parser means the block is consumed correctly either way,
which is the resynchronisation rule again — and it means the fuzz targets do not
grow a mode.

**Pre-auth set: the authenticating `set`, and `quit`.** Everything else, meta
commands included, is refused with `CLIENT_ERROR unauthenticated`. The meta
protocol has no authentication command of its own upstream, so a meta-only
client must send the classic `set` first.

**Two things to pin against real memcached rather than assert here**, in the
differential suite that already compares raw bytes (`tests/compat/`):

1. The exact refusal line for an unauthenticated command. `CLIENT_ERROR
   unauthenticated` is what upstream is understood to emit; the suite decides.
2. Whether `version`, `stats` and `quit` are permitted before authentication.
   The design above allows only `quit`; if upstream differs, upstream wins —
   that is the standing rule for this dialect.

The **binary protocol and its SASL commands stay unimplemented** (plan.md §7).
SASL lives only in the binary protocol upstream, so supporting it means adding
the third parser this project decided not to have, to serve clients that have
moved to meta commands.

---

## 8. Redis

Redis has the most developed model of the three, and the clients are already
written for it.

| Form | Meaning here |
|---|---|
| `AUTH password` | The `default` identity. Redis's pre-6 form; still the most common. |
| `AUTH username password` | A named identity from the table. |
| `HELLO 3 AUTH username password` | Authenticate and negotiate RESP3 in one round trip. |

`HELLO … AUTH` is currently rejected on purpose — "accepting `AUTH` would tell a
client it had authenticated" ([resp/command.rs](../crates/vash-proto/src/resp/command.rs)).
That rejection is exactly right today and must be **lifted as part of this work**,
together with the divergence row in [protocol.md](protocol.md#deliberate-divergences-from-redis).
`SETNAME` stays rejected: there is still no client registry.

**Replies, matching Redis 7 wording** — the server reports `7.4.0-vash`, so
clients branch on 7's behaviour:

| Reply | When |
|---|---|
| `+OK` | Authentication succeeded. |
| `-WRONGPASS invalid username-password pair or user is disabled.` | Bad name or bad secret. **One message for both**, as Redis does, so the error does not confirm which names exist. |
| `-NOAUTH Authentication required.` | Any other command before authenticating. |
| `-ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?` | `AUTH` when no credential is configured at all. |
| `-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time` | Bare `HELLO` while unauthenticated. Redis's own wording, long as it is. |

**Pre-auth set: `AUTH`, `HELLO … AUTH`, `QUIT`.** Bare `HELLO` is refused with
the message above — which, note, is Redis choosing to be *more* restrictive than
VCP is (§6.2), where `HELLO` must be allowed because the dialect is not yet
known. Two protocols, two correct answers, for a reason worth writing down.

**`RESET` is not implemented and stays that way.** It exists in Redis partly to
drop authentication state; a client that wants that can close the connection,
which costs one round trip on a path nobody is optimising.

**The `ACL` command family is not implemented** (§3.6). `ACL WHOAMI` is
tempting and trivial, and would still be the first step onto a road that ends at
`ACL SETUSER`. Deliberately not started.

---

## 9. Cluster peers

**This is the part most likely to break in production, so it comes with the
feature rather than after it.** Peers are ordinary VCP clients on the ordinary
port ([cluster.rs](../crates/vash-server/src/cluster.rs) — "a peer is just
another VCP client"), which means turning `auth.required` on without giving
peers a credential silently breaks tag fan-out and gossip across the whole
cluster. It fails in the worst available shape: writes keep working, `TAG_SYNC`
starts being refused, and invalidations quietly stop converging while every
node reports itself healthy.

Three things follow:

1. `[cluster] auth_name` / `auth_secret` configure what the peer connections
   present. When unset and `auth.required` is on, **startup fails** rather than
   starting a node whose cluster is broken. Refusing to start is the only
   failure mode an operator cannot miss.
2. `Cluster::exchange` uses `Client::connect_with` (§6.4). An `UNAUTHORIZED`
   from a peer marks it unreachable, is logged distinctly from a connection
   failure, and — because a bad credential will not fix itself — is rate-limited
   in the log rather than repeated every gossip interval.
3. `vash_cluster_peers_reachable` already exists and covers this, provided the
   auth failure settles reachability from the round's outcome. That is the exact
   bug M5 fixed for connect timeouts; the same path must not regrow it.

**Peer authority is not separated from client authority in M9.** The credential
table has room for a `role` column, and the case for `peer` is real —
`TAG_SYNC` can advance a generation arbitrarily far where `DELETE_BY_TAG` moves
it by one, which is the distinction cluster.rs's Trust note already names. But
any authenticated client can already invalidate any tag, so the marginal
authority a peer credential would fence off is small, and the check is one enum
comparison to add later. Not built; noted so the shape does not preclude it.

---

## 10. The admin port

Out of scope, and stated so it is not assumed covered. `/metrics`, `/health` and
`/stats` are on a separate port that defaults to `127.0.0.1` and can be switched
off with `admin_listen = ""`. `/stats` discloses counters, not keys or values.
If it ever needs a control, a bearer token in an `Authorization` header is the
whole design — HTTP already has the mechanism, and it shares nothing with the
cache-port credential path.

---

## 11. Configuration surface

```toml
[auth]
required = false                    # enforcement; credentials may exist while this is false (§15)
file = ""                           # path to the credential file (§4)
# or VASH_AUTH_SECRET in the environment for a single `default` identity
timeout_ms = 5000                   # drop a connection that has not authenticated (§12)
max_attempts = 3                    # failures on one connection before it is closed
max_unauthenticated_connections = 0 # 0 = a tenth of server.max_connections

[cluster]
auth_name = "peer"                  # what peer connections present (§9)
auth_secret = ""
```

`protocol.auth_secret`, sketched in plan.md §11 and never read, is superseded by
this section and should be deleted rather than aliased — nothing depends on it,
and a config key that half-works is worse than one that does not exist.

---

## 12. Abuse budget for unauthenticated connections

An unauthenticated connection is the one thing on this server that a stranger
can create, so it gets a budget rather than the ordinary limits.

| Control | Default | Why |
|---|---|---|
| Authentication deadline | 5 s | A connection that authenticates nothing occupies a slot for free. Enforced in the `select!` that already handles shutdown, so it costs no new task. |
| Failed attempts per connection | 3, then close | Bounds guessing per connection without a lockout an attacker could use to lock out a legitimate client. |
| Concurrent unauthenticated connections | `max_connections / 10` | Otherwise the pre-auth budget is the whole connection budget, and a stranger can fill it. |
| Buffered bytes before authentication | 4 KiB, then close | A pre-auth connection must not be able to make the server hold, or *reserve*, arbitrary memory. Both halves are needed: a VCP header claiming a 64 MiB body reserves against a length that has not arrived, so checking what is buffered would not catch it. |
| Constant-time comparison | always | The timing oracle in §3.1 is the one attack a naive implementation hands over for free. |
| No delay on failure | — | Redis does not sleep on a bad `AUTH` and neither should this: a sleeping connection is a held resource, which converts a guessing attempt into a cheaper denial of service. The attempt cap is the control. |

Failures are logged at `warn` with the identity name and peer address but
**never the presented secret** — a near-miss credential in a log file is a
credential in a log file.

---

## 13. What it costs

The claim to be tested, not assumed (the standing rule from plan.md §13):

- **Per connection:** one `HashMap` lookup and one 32-byte constant-time
  compare, on the order of a microsecond, against a TCP handshake and a `HELLO`
  round trip already measured in tens of microseconds. Below the noise floor.
- **Per request:** one predictable branch on a bool in the executor's prologue.
  Expected to be unmeasurable against a 15.6 ns VCP decode.
- **The one number worth watching:** connections per second with authentication
  required versus without, on a workload that opens a connection per request —
  the pattern where per-connection cost stops amortising. If that number moves
  by more than the ±25% run-to-run variance the benchmark box already has, the
  cause is not the compare and should be found before shipping.

M6's methodological note applies: any result that survives only one run is not a
result.

---

## 14. Testing

| Layer | What |
|---|---|
| Unit | Constant-time compare, and the credential file reader against every way a line can be wrong: unknown algorithm, missing algorithm, `$…$` refusal, bad hex, wrong length, duplicate name, illegal name character, unknown trailing `key=value`, `#` as the first non-whitespace character versus `#` inside a field, blank lines, CRLF, permission refusal. **Each must be a startup error naming the line number**, not a skipped row — the property that matters is that a file never half-loads. |
| Unit | Mechanism binding: a `PLAIN` attempt against an `hmac-sha256-key:` row is refused even when the presented bytes equal the stored key, and an `HMAC_SHA256` attempt against a `sha256:` row is refused. Both are the tests that stop §4's two forms collapsing into one. |
| Exhaustive | **The invariant that matters: `no_vcp_opcode_executes_unauthenticated` walks all 256 opcode bytes** — not a list someone maintained by hand — and requires `UNAUTHORIZED` with an empty body from every one outside the pre-auth set. An opcode added later without a gate fails here rather than shipping. This is the test that found the ordering bug in §5. |
| Integration | Consumption is unchanged by authentication state: `a_refused_storage_command_still_consumes_its_block` pipelines a refused memcached `set` whose data block would parse as a command, and asserts exactly two replies. One more would mean the block had been read as commands. |
| Fuzz | `vcp_auth` runs `decode_auth` against a seeded corpus, joining the five existing targets in CI. It is pre-auth input by definition, so it is the highest-value surface in the system. |
| Integration | Per dialect: authenticate and succeed, wrong secret, unknown name, command before auth, re-auth on a live connection, `SIGHUP` reload, the attempt cap and the deadline. |
| Differential | The memcached error line and pre-auth command set against real memcached under `-Y` (§7); the Redis error strings and `HELLO … AUTH` against real Redis. Both are byte comparisons in the existing suite. **Still outstanding** — the strings shipped are the ones §7 and §8 name, pinned by this repo's own tests rather than against a real server. |
| Client compat | `pymemcache` with a username and password, and `redis-py` with `AUTH` and with `HELLO 3 AUTH`. The point is whether real clients drive it unchanged, which is the same bar M3 and M7 were held to. |
| Cluster | A three-node cluster with auth required, converging; and the negative case — a node started without a peer credential must fail startup, not run degraded (§9). |

---

## 15. Rollout

There is deliberately **no `optional` mode**. A mode where unauthenticated
clients still work is not authentication, it is a log line. The two-step rollout
comes from separating the credential table from enforcement:

1. Configure `auth.file` and leave `auth.required = false`. The server now
   *accepts* `AUTH` and answers it truthfully, but refuses nothing.
2. Roll every client — and every peer (§9) — to send its credential. Nothing
   breaks if one lags: it simply does not authenticate.
3. Flip `auth.required = true` and restart. Anything still unauthenticated now
   fails, and `vash_auth_failed_total` names it.

Step 1 is why `AUTH` is answered even when enforcement is off, and why sending
it against a credential-less server is an error (§8) rather than a cheerful
`+OK`: a client must never be told it authenticated against nothing.

---

## 16. Non-goals

Stated so they are not re-litigated: no TLS in this work (§3.7 — it is the
right answer to a different question, and plan.md §16 owns the decision), no
SASL and no memcached binary protocol, no runtime credential mutation and no
`ACL` command family, no per-key or per-command permissions, no roles in M9
(§9), no OIDC/JWT/external identity provider, no authentication on the admin
port (§10), no per-connection rate limiting beyond the pre-auth budget (§12),
and no lockout of an identity across connections — an attacker who can trigger
a lockout has a denial of service instead of a break-in, which is not a trade
worth making.

---

## 17. Milestone

| # | Scope | Exit criteria |
|---|---|---|
| **M9** | Credential table and file loader; `AUTH` in VCP, memcached ASCII auth, Redis `AUTH`/`HELLO … AUTH`; the pre-auth gate in all three executors; peer credentials; `SIGHUP` reload; the pre-auth abuse budget | Real memcached and Redis clients authenticate unchanged and are refused when they do not; the property test proves no command in any dialect executes unauthenticated; a three-node cluster converges with auth required, and a node with a missing peer credential refuses to start; the `AUTH` decoder is fuzzed in CI |

**Delivered**, with two exceptions carried forward:

- **`SIGHUP` reload is Unix only.** Windows has no `SIGHUP`; rotation there
  means a restart, which the two-step rollout already tolerates.
- **The differential suite has not been extended** to compare authentication
  against a real memcached and a real Redis (§14). The wire strings here are the
  ones §7 and §8 specify, pinned by this repo's tests; §7 names the two
  memcached behaviours that upstream, not this document, should settle.

Sequencing within it: the credential table and VCP first (it is the dialect we
own end to end, and `vash-client` is the test driver everything else uses), then
Redis (the best-specified of the two compatibility surfaces), then memcached
(the one whose behaviour has to be discovered from a real server), then the
cluster — which cannot be done before the client, and must not be left after
release.

---

## 18. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Enabling auth silently breaks cluster tag invalidation | **High** — converges nowhere while every node reports healthy | Startup refuses a clustered node with auth required and no peer credential (§9); a cluster test covers the negative case |
| A command added later without a gate | High | The property test in §14 enumerates every command in every dialect rather than listing the gated ones |
| Plaintext credential on an unencrypted wire read as stronger than it is | Medium | §1 states the boundary; the same statement goes in operations.md and the README, next to the existing network-boundary advice |
| Timing oracle in the comparison | Medium | Constant-time compare over fixed-length digests, unit-tested; never compare raw secrets |
| A slow KDF adopted later "for security" | Medium | §3.2 records why not, with the amplification factor, so the decision is not re-made from intuition |
| Credential file with loose permissions | Medium | Refused at startup on Unix, like `ssh` |
| Pre-auth connections exhaust the connection budget | Medium | Separate cap plus an authentication deadline (§12) |
| memcached's ASCII auth diverges from what is written here | Low | It is pinned by differential test against a real server, not by this document (§7) |
| Operators assume the admin port is covered | Low | §10 says it is not; it defaults to loopback |
