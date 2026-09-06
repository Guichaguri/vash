# Security review

A vulnerability analysis of the server and of the three protocols it speaks,
against `563dda0`. Written the way the rest of `docs/` is: the finding first,
then why it is what it is, then what closes it.

Scope: `vash-core`, `vash-proto`, `vash-store`, `vash-server`, `vash-client`,
the shipped `Dockerfile` and `vash.example.toml`. Everything reachable from a
socket was read; the storage engines' own C libraries (LMDB, libmdbx) were not
audited, only how this workspace calls them.

Three findings were reproduced against a running server rather than argued from
the source. Those are marked **measured** and carry their numbers.

---

## 1. Threat model

The one this server already states, in [auth.md](auth.md) §1, and it is the
right one: **a party who can reach the port but does not sit on the wire.** An
adjacent service in a shared cluster, a process on a co-tenant host, a bind to
`0.0.0.0` that was meant to be a bind to `127.0.0.1`, a container network
flatter than the diagram. The eavesdropper is TLS's problem; the operator with
a shell on the box is nobody's.

That gives three classes of attacker, and the findings below are ordered by
which one they hand something to:

| | Can reach the cache port | Holds a credential | Reads the wire |
|---|---|---|---|
| **A — stranger** | yes | no | no |
| **B — client** | yes | yes | no |
| **C — network** | yes | maybe | yes |

**In the default configuration every attacker is class B**, because
`auth.required` is false and the credential table is empty: there is no gate,
so reaching the port *is* holding a credential. That is a deliberate choice
(§4.3) and it decides the severity of most of what follows.

---

## 2. Findings

| # | Severity | Finding | Attacker |
|---|---|---|---|
| [V-01](#v-01) | **High** | A 12-byte VCP header commits 64 MiB of memory | A |
| [V-02](#v-02) | **High** | No per-connection read-buffer ceiling after the pre-auth gate | A |
| [V-03](#v-03) | **High** | Unauthenticated by default, and the shipped container binds `0.0.0.0` | A |
| [V-04](#v-04) | Medium | Log injection through the `AUTH` username | A |
| [V-05](#v-05) | Medium | No idle timeout except before authenticating | A |
| [V-06](#v-06) | Medium | mTLS identity is non-deterministic for a multi-SAN certificate | B |
| [V-07](#v-07) | Medium | The cluster peer secret sits in plaintext TOML, unchecked | local |
| [V-08](#v-08) | Low | `VASH_AUTH_SECRET` is readable from the process environment | local |
| [V-09](#v-09) | Low | Log-volume amplification from refused commands | A |
| [V-10](#v-10) | Low | Batch counts size allocations before the items arrive | A |
| [V-11](#v-11) | Low | `SCAN` cursors are server-wide with sequential tokens | B |
| [V-12](#v-12) | Info | Username-existence timing side channel in `verify` | A |
| [V-13](#v-13) | Info | `stats conns` and `CLUSTER` disclose peers at `Roles::NONE` | B |
| [V-14](#v-14) | Info | The admin port has no authentication of its own | A |
| [V-15](#v-15) | Info | The tag registry is fillable and has no per-client quota | B |
| [V-16](#v-16) | Info | No supply-chain scanning in CI | — |

---

### V-01

**A 12-byte VCP header commits 64 MiB of memory.** High. Measured.

`conn.rs:736` sets `measured.reserve` from a frame length the client declared,
and `conn.rs:695` hands it to `BytesMut::reserve`. The reserve exists so a
large frame is allocated once instead of grown repeatedly, and the length it
trusts comes from a header the peer wrote.

There is a guard, at `conn.rs:733`, and it is the right guard for the wrong
population:

```rust
if gated && needed > PRE_AUTH_MAX_BUFFERED {
    measured.fatal = Some("unauthenticated frame is too large");
} else {
    measured.reserve = needed;
}
```

`gated` is `!conn.is_authenticated() && state.auth.required()`. With
`auth.required` false — the default — `gated` is false for every connection
that ever exists, and the `else` branch takes any length up to `MAX_BODY_LEN`,
64 MiB.

Reproduced on `127.0.0.1:11399`, default configuration. Each connection sends a
16-byte `HELLO`, reads the reply, then sends one 12-byte header announcing a
67108864-byte body and stops. Twenty connections, 560 bytes of traffic in
total:

| | committed (`PrivateMemorySize64`) | working set |
|---|---|---|
| before | 27.6 MB | 10.6 MB |
| holding 20 headers | **1310.7 MB** | 11.4 MB |
| after the clients disconnect | 27.9 MB | 10.9 MB |

1.28 GiB from 560 bytes — 64 MiB per connection, exactly the ceiling, for 28
bytes each. At the default `server.max_connections` of 10000 that is **640 GiB
of commit charge from under 300 KB of traffic**. The working set stays flat
because nothing touches the pages, which is what makes this a commit-charge and
address-space attack rather than an RSS one: harmless under Linux's default
overcommit, fatal under `vm.overcommit_memory=2`, inside a cgroup with a
`memory.max`, or on Windows, where the commit charge is RAM plus pagefile and
1.3 GB of it is what the table above measures.

The fix is to bound the reserve rather than the population it applies to. The
frame ceiling a deployment actually needs is knowable — `store.max_value_bytes`
defaults to 1 MiB, and a `SET` frame is that plus a key and a tag table — so
`MAX_BODY_LEN` at 64 MiB is already 64× more than the store will accept.
Reserve `min(needed, ceiling)` and let the buffer grow from arriving bytes past
it; the amortised growth costs a few reallocations on a frame nobody sends, and
it removes the amplification entirely. Widening `gated` would not work — that
would break legitimate large frames on an authenticated connection — which is
why this needs a bound of its own.

---

### V-02

**No per-connection read-buffer ceiling after the pre-auth gate.** High. Measured.

The only bound on what one connection may make the server hold is
`conn.rs:130`:

```rust
if unauthenticated && read_buf.len() > PRE_AUTH_MAX_BUFFERED {
```

and `unauthenticated` is again `enforcing && !authenticated`. Past that gate —
or in the default configuration, where the gate never applies — `read_buf`
grows for as long as the peer keeps sending, because bytes are only released
when they form complete commands and `split_to` consumes them. A command that
never completes is never consumed.

RESP is where this is widest. `MAX_ARGS` is 8200 (`resp/mod.rs:44`) and
`MAX_BULK_LEN` is `ABSOLUTE_MAX_VALUE_LEN`, 64 MiB (`resp/mod.rs:36`), so **one
legal RESP command may declare roughly 512 GiB of arguments**, and every byte of
it is buffered before the parser is allowed to have an opinion. Both ceilings
are individually defensible; their product is the problem, and nothing checks
the product.

Reproduced: one connection, `*8200\r\n` followed by 300 repetitions of
`$1048576\r\n<1 MiB>\r\n` — a single command, still incomplete.

| | committed | working set |
|---|---|---|
| before | 27.6 MB | 10.6 MB |
| one connection, 300 MiB sent | **540.8 MB** | **310.9 MB** |

Unlike V-01 this is resident memory, because the attacker actually sends the
bytes. That makes it bandwidth-bound and therefore slower, and it makes it
*real* — no overcommit setting saves you, and 310 MB from one socket generalises
to whatever the attacker is willing to transmit. It also crosses two limits the
server does enforce elsewhere: the payload is 300× `store.max_value_bytes` and
nearly 5× the VCP frame ceiling, and neither applies here because neither is
checked until the command parses.

memcached is bounded better, and by construction rather than by accident:
`MAX_LINE_LEN` is 16 KiB (`memcached/mod.rs:29`) and a storage block is refused
above `ABSOLUTE_MAX_VALUE_LEN` (`memcached/text.rs:426`), so one connection tops
out near 64 MiB. VCP is bounded at 64 MiB per frame by `MAX_BODY_LEN`. RESP has
no equivalent because its ceiling is per *argument*.

The fix is one check in the read loop that does not consult the auth state: an
absolute per-connection buffered-bytes ceiling, defaulting to something derived
from `store.max_value_bytes` and `server.read_buffer`, enforced on every read
for every connection. That is a strictly stronger statement than
`PRE_AUTH_MAX_BUFFERED` makes and it subsumes it.

---

### V-03

**Unauthenticated by default, and the shipped container binds `0.0.0.0`.** High.

Two defaults that are each defensible and are dangerous together.

`config.rs:692` binds `127.0.0.1:11311`, which is the right default and is
worth saying so. `auth.required` defaults to false and the credential table
defaults to empty, which is memcached's and Redis's convention and is the
reason [auth.md](auth.md) §1 puts the network boundary first.

The `Dockerfile` then ships:

```dockerfile
CMD ["--listen", "0.0.0.0:11311", "--data", "/var/lib/vash"]
```

with no credential, no TLS, and `docker run --rm -p 11311:11311 …` in the
comment at the top of the file. That is the documented way to run this server,
and it produces a process that serves every key it holds to anything that can
route to the port. `--listen 0.0.0.0` is also the natural thing to type on a
host, because the loopback default is exactly what stops a container or a
sibling service from connecting.

Nothing warns. There is no check anywhere that a non-loopback listen address
meets an empty credential table, and V-01, V-02, V-05, V-09 and V-15 all take
their severity from this: every one of them is an unauthenticated attack
specifically because there is nothing to authenticate to.

The precedent is exact. Redis shipped this combination, open instances were
found and exploited at scale, and 3.2 added protected mode: bound to a
non-loopback address with no password, it accepts connections and answers every
command with a refusal that explains what to do. That is a better shape than
refusing to start, because it does not break anyone whose deployment is
genuinely on a private network — it just makes the operator say so.

The smallest version of it here is a startup `warn!` when
`server.listen.ip().is_loopback()` is false and `Auth::configured()` is false —
`bind_with` already logs two similar lines, at `lib.rs:163` and `lib.rs:168`.
The fuller version is a `server.protected_mode` defaulting on, refusing
commands from non-loopback peers on an unauthenticated server, which the
`Dockerfile` would then have to opt out of explicitly and visibly.

---

### V-04

**Log injection through the `AUTH` username.** Medium. Measured.

`resp.rs:546` and `memcached.rs:450` log a failed authentication with

```rust
warn!(name = %String::from_utf8_lossy(name), failures = conn.failures(), "authentication failed");
```

`%` is `Display`. The default `observability.log_format` is `pretty`
(`config.rs:729`), whose formatter writes field values verbatim — no escaping,
no quoting. `from_utf8_lossy` replaces invalid UTF-8 and preserves everything
else, newlines and ANSI escapes included.

Reproduced against a server with credentials configured. One RESP `AUTH` whose
username argument carried `\n` and a hand-built log line produced, in the
server's log:

```
2026-09-05T23:58:54.685243Z  WARN vash_server::resp: authentication failed name=attacker
2026-01-01T00:00:00.000000Z  INFO vash_server::auth: audit log cleared by operator rows=0 failures=1
```

The second line is the attacker's, with their timestamp, their module path and
their level, rendered in the same ANSI colours the real formatter uses. Only
the trailing `failures=1` gives it away, and an attacker who cares can pad the
line so that lands somewhere unread, or simply choose a forgery that plausibly
ends in `failures=1`.

There is a size dimension too. VCP bounds the name at `MAX_AUTH_NAME_LEN` = 64
(`vcp/decode.rs`), which is why `vcp.rs:462` is not part of this. RESP's
username is an ordinary bulk string, so up to 64 MiB, and memcached's arrives
inside a `set` value, so likewise: **3 failed attempts per connection × 64 MiB
of attacker-chosen text into the log stream**, with no connection limit in front
of it.

Two one-line fixes, both wanted: `?` instead of `%`, which routes through
`Debug` and escapes; and truncate the name to `MAX_AUTH_NAME_LEN` before
logging it, which VCP already does structurally. `cluster.rs:515` already gets
this right with `tag = ?String::from_utf8_lossy(&entry.name)` — the pattern
exists in the codebase, it is just not used in these two places.

---

### V-05

**No idle timeout except before authenticating.** Medium.

`conn.rs:100` computes `unauthenticated = enforcing && !conn_auth.is_authenticated()`,
and `conn.rs:107` makes the whole deadline arm conditional on it. So the
`auth.timeout_ms` deadline is the only timeout a connection ever faces, and
with `auth.required` false it never arms. A connection that opens and sends
nothing lives until the peer or the network gives up.

[protocol.md](protocol.md) states this — *"An authenticated connection is never
idle-reaped. There is no idle timeout and no server-side keepalive"* — and it
is a reasonable choice for pooled clients. What it costs is that
`server.max_connections`, 10000 by default, is exhaustible by 10000 sockets
that send zero bytes, after which every real client gets a connect that
succeeds and an immediate EOF (`lib.rs`, the `try_acquire_owned` arm). Class A
attacker, no credential, no bandwidth, no cleverness.

`auth.max_unauthenticated_connections` is the existing answer and it only
covers the enforcing case: it defaults to a tenth of the budget and is not
taken at all when `enforcing` is false (`lib.rs`, `pre_auth_permit`).

The fix that fits the design is not a general idle timeout — the document is
right that pooled connections should not be reaped — but a deadline on
connections that have sent nothing at all, which is a strictly smaller
population than "idle" and contains no legitimate client. The
`PRE_AUTH_MAX_BUFFERED` machinery is already shaped for this; it needs to stop
asking whether authentication is enforced.

---

### V-06

**mTLS identity is non-deterministic for a multi-SAN certificate.** Medium.

`auth.rs:537`:

```rust
pub fn identity_for_certificate(&self, matches: impl Fn(&str) -> bool) -> Option<Identity> {
    for (name, credential) in &self.table {
        ...
        if matches(subject) { return Some(Identity { ... }); }
    }
    None
}
```

`self.table` is a `HashMap` with the default `RandomState`, so iteration order
is randomised per process and unspecified within one. The first match wins.

For a certificate whose SANs satisfy exactly one `mtls:` row that is fine. For
one that satisfies two — a service certificate carrying both
`billing.svc.internal` and `billing.internal`, or a wildcard `*.svc.internal`
against rows for several names under it — **which identity the connection gets,
and therefore which role set, is decided by hash iteration order.** A holder of
a wildcard certificate under `client_auth = "required"` authenticates as
whichever row is found first, which may be the one with `roles=flush` rather
than the one with `roles=read`.

`tls.rs:identity_for` supplies the predicate via
`webpki::EndEntityCert::verify_is_valid_for_subject_name`, which applies the
RFC 6125 wildcard rules — correctly, and that is the point: the matcher is
right and the *selection among matches* is the gap.

Two changes close it. Collect every matching row rather than returning at the
first; if there is more than one, refuse the connection and log both names,
because an ambiguous certificate is a configuration error and picking one
silently is how it stays undiscovered. Second, validate `mtls:` subjects at
load time — `tls.rs` already notes that a subject which is not a valid
`ServerName` "would be better" refused at load and settles for never matching,
which is safe but leaves a row that reads as a working credential and is not
one, the same failure `parse_credential` refuses `hmac-sha256-key` to avoid.

---

### V-07

**The cluster peer secret sits in plaintext TOML, unchecked.** Medium.

`cluster.auth_secret` (`config.rs:475`) is a plain `String` read out of the
config file by `Config::load` (`config.rs:796`), and it is a live credential:
`credential()` hands it to `Client::connect_with`, and the row it names carries
`Roles::PEER`, which `TAG_SYNC` uses to set any tag generation to any value.

The asymmetry is the finding. The *credential file* gets a real check —
`auth.rs:check_permissions` refuses mode `0o077` on unix, the way `ssh` refuses
a loose private key, and the comment there is right that a check the server can
make is worth more than a sentence in a manual. The *config file*, which holds
a secret of equal power, gets none. There is also no environment alternative:
`VASH_AUTH_SECRET` exists for the server's own credential and there is no
`VASH_CLUSTER_SECRET` for this one, so a container deployment has no way to
supply it except through a file or a command line.

Either would fix it: apply the same mode check to the config file whenever
`cluster.auth_secret` is non-empty, or add the environment variable and let the
file hold only a name.

---

### V-08

**`VASH_AUTH_SECRET` is readable from the process environment.** Low.

`auth.rs:412` reads the secret from the environment when no credential file is
configured, which is the documented container path. A process environment is
not a secret store: `/proc/<pid>/environ` is readable by the same uid, `docker
inspect` prints it, orchestrators log it, and it lands in core dumps and crash
reports.

This is a known trade and the right default is already the other one — the file
is what `auth.file` points at, and it is permission-checked. Worth stating in
[operations.md](operations.md) beside the variable rather than changed: the
deployments that reach for it have usually decided this already.

---

### V-09

**Log-volume amplification from refused commands.** Low.

`dispatch.rs:679` logs `warn!("rejected flush: disabled by configuration")` per
refused `FLUSH`; `dispatch.rs:711` does the same for every refused listing. The
failed-authentication warnings in V-04 are the third. All are at `WARN`, which
survives the default `log_level = "info"`, and all are one line per small
request.

A `flush_all\r\n` is 11 bytes and produces a log line; the connection stays
open and can send another immediately. That is a disk-fill and
log-pipeline-cost attack rather than a memory one, and it is the sort of thing
that is only noticed when the log bill arrives.

Rate-limiting or sampling these three — first occurrence plus a periodic count,
which is what they are actually useful for — keeps the diagnostic and removes
the amplification.

---

### V-10

**Batch counts size allocations before the items arrive.** Low.

`vcp/decode.rs` `decode_key_list`, `decode_tag_sync` and the `SetMany` arm each
read a count and immediately `Vec::with_capacity(count)`, before any item has
been read. Both counts are bounded first — `MAX_BATCH_ITEMS` 4096,
`MAX_TAG_SYNC_ENTRIES` 8192 — and the bounds are checked in the right order,
which is why this is Low and not a repeat of V-01.

What remains is the ratio. An 8-byte truncated `GET_MANY` body declaring 4096
keys allocates ~64 KiB and then fails; a `TAG_SYNC` declaring 8192 entries
allocates ~200 KiB. Per frame, freed immediately, and a pipelined connection is
decoded one frame at a time — so the cost is bounded and transient. It is the
same pattern as V-01 at a scale that does not matter, noted because a future
raise of either constant would make it matter, and because reserving
incrementally (`with_capacity(count.min(64))`) costs nothing on a batch that is
genuinely large.

---

### V-11

**`SCAN` cursors are server-wide with sequential tokens.** Low.

`scan.rs` is explicit that the table is server-wide and explains why — a pooled
client returns its connection between iterations, so page two routinely arrives
on a different socket. The consequence is that any connection can resolve any
live token, and `scan.rs:76` issues them from `AtomicU64::fetch_add`, so they
are consecutive integers and trivially guessable.

The impact is confined by what a token *is*: a position in the keyspace, not an
authorisation. Anyone who can resolve one can also scan from the beginning,
because both need `Roles::LIST` and `protocol.listing_enabled`. So what leaks is
another client's *progress*, not access it did not have.

Worth a sentence in the module docs rather than a change. If it ever needs to
be per-identity, the entry already has room for the name.

---

### V-12

**Username-existence timing side channel in `verify`.** Informational.

`auth.rs:567` returns early on a `HashMap` miss, and on a hit computes
`sha256(secret)` and a constant-time compare. The compare is right —
`subtle::ConstantTimeEq` over fixed-length digests, and the comment explaining
why it is not a direct secret comparison is correct. The control flow around it
is not uniform: a miss is a hash probe, a hit is a probe plus a SHA-256 plus 32
bytes of constant-time compare, a few hundred nanoseconds apart.

Over a network that is far below the noise floor, and the names are not secret
anyway — `mtls:` subjects are public by construction and a credential file is
an operator's document. Recorded for completeness because the module reasons
carefully about exactly this class of thing elsewhere, and the asymmetry is
easy to read as an oversight when it is a non-issue.

---

### V-13

**`stats conns` and `CLUSTER` disclose peers at `Roles::NONE`.** Informational.

`auth.rs:roles_for_command` maps `Stats` and `Cluster` to `Roles::NONE`, and the
comment gives the reason: gating them would break client libraries that call
`stats` on connect. `stats conns` then renders one block per open connection
including `<id>:addr` as `tcp:<ip>:<port>` (`connections.rs:223`), and `CLUSTER`
returns the configured peer list.

So any client that gets past the gate — which in the default configuration is
any client at all — can enumerate every other client of the cache and every
peer node. That is memcached's own behaviour and matches the compatibility
promise. It is a lateral-movement aid rather than a vulnerability, and it is
worth an operator knowing it is there.

---

### V-14

**The admin port has no authentication of its own.** Informational.

By design, and the design is stated in three places — `admin.rs:35`, the
`--admin-listen` help text, and the `Dockerfile`'s note on port 9090 — all of
which say private interface. `observability.admin_listen` defaults to empty
(`config.rs:732`), so nothing listens unless asked.

What it serves when asked: `/metrics`, `/health`, `/stats` (counters, cluster
view, configuration), and with `--features introspect`, `/slowlog` and
`/hotkeys`. `/slowlog` renders the peer address of the connection that issued
each slow request and the key it named — as an `xxh3` digest by default, as the
bytes under `protocol.listing_enabled`, which is the gate
[slowlog-proposal.md](slowlog-proposal.md) §3.3 chose deliberately. `/hotkeys`
is behind that gate entirely, for the stated reason that a digest-only hot-key
ranking has nothing to act on.

The HTTP handling itself is sound: `MAX_REQUEST_HEAD` 8 KiB, `MAX_CONCURRENT`
32, a 10-second per-request timeout, `GET` only, exact path matching with no
filesystem access, and no request data reflected into a response header or body
— so no header injection and no path traversal. The finding is only that the
boundary is a config flag rather than a credential, and an operator who binds it
to `0.0.0.0` gets no warning, exactly as in V-03.

---

### V-15

**The tag registry is fillable and has no per-client quota.** Informational.

`store.tags.max_tags` defaults to 100000 (`vash.example.toml:258`) and
`tags.rs:181` refuses a new name past it with `StoreError::TagLimit`. The limit
is process-wide and the registry is shared, so any client that can write can
consume it with generated tag names, after which every *other* client's tagged
write fails until the registry is reclaimed.

The effect is confined to tagging — plain writes are unaffected — and 100000
names is a large budget to burn. It is listed because the limit is the only
thing standing between one client and a shared resource, and because a
per-identity quota is the kind of thing that is much easier to add before
anyone depends on the absence of one.

---

### V-16

**No supply-chain scanning in CI.** Informational.

`.github/workflows/ci.yml` runs fmt, clippy with `-D warnings`, tests and six
coverage-guided fuzz targets. It does not run `cargo audit` or `cargo deny`, so
nothing tells the project when a dependency picks up an advisory.

The lockfile is clean at review time and current: rustls 0.23.43, ring 0.17.14
(past RUSTSEC-2025-0009), rustls-webpki 0.103.15, sha2 0.10.9, subtle 2.6.1,
zerocopy 0.8.56, tokio 1.53.1, lmdb-master-sys 0.2.6. Nothing here is known
vulnerable. A `cargo audit` job is what keeps that sentence true without
someone having to re-check it.

---

## 3. The protocols

### 3.1 First-byte detection

`vash_proto::detect` settles the dialect on one byte and never revisits it:
`0x01` is VCP, `*` is RESP, `a`–`z` is memcached, anything else closes the
connection. The three sets are disjoint, so there is no ambiguity to resolve
and no state machine to confuse — and RESP inline commands are refused
precisely because `get foo\r\n` is legal in two dialects. This is the right
design and it removes a whole class of protocol-confusion bug before it exists.

A disabled dialect is closed at detection rather than refused by its own parser
(`conn.rs:141`), which means the parser an operator turned off never runs. That
is the correct reading of what disabling something means.

### 3.2 VCP

The binary framing is the strongest of the three. A 12-byte header with an
explicit `body_len`, bounded before it is trusted for anything, and
`peek_frame_len` kept separate so the connection can find a frame boundary
without decoding a body. `Cursor` is bounds-checked on every accessor and
returns `None` rather than panicking, so truncated input from a stranger is an
ordinary error path. The header/body error split is deliberate and correct: a
bad body is answerable and the connection survives, a bad header means the
stream cannot be resynchronised and the connection goes.

The gate ordering deserves specific credit. `vcp.rs:146` refuses a frame from
its **header alone**, before any body decoder runs, so an unauthenticated
stranger never drives `decode_set`, `decode_tag_sync` or the listing parser.
And `vcp.rs:388` makes the refusal for an unknown opcode identical to the
refusal for a known one while unauthenticated, so a stranger cannot enumerate
which opcodes this build implements. Both are the kind of thing that is
invisible when done right.

`AUTH` is `Decoded::Auth`, its own variant, parsed by a standalone public
function with both lengths checked before either slices and trailing bytes
refused — and it is a fuzz target, which is the correct place to spend the
budget, since it is by definition the one parser that runs on
pre-authentication input.

Weaknesses: V-01 (the reserve), and `MAX_BODY_LEN` at 64 MiB being 64× what
`store.max_value_bytes` will accept, which is what gives V-01 its size.

### 3.3 RESP

`frame` is shared between `parse` and `peek` so the measuring walk and the
executing walk cannot disagree about where a command ends — and `Fatal` is a
distinct type so "the framing walk only produces unrecoverable errors" lives in
the signature rather than in a comment. `read_header` bounds an unterminated
header at `MAX_HEADER_LEN` before the buffer can grow, and `read_bulk` rejects a
negative or oversized length *before* waiting for the bytes. `Args` is inline
for eight and heap past that, sized from the declared count, and its
unreachable-by-construction overflow arm promotes to the heap rather than
indexing past the end — with the reason given as "this parser reads bytes from
unauthenticated clients", which is exactly the right instinct.

Weakness: V-02. `MAX_ARGS` × `MAX_BULK_LEN` is the one place two individually
sound limits multiply into an unsound one.

### 3.4 memcached

Both dialects share `find_line`, which bounds an unterminated line at
`MAX_LINE_LEN` = 16 KiB. `parse_storage` reads the declared length *before*
validating anything else, deliberately, so that a malformed line still steps
over its data block — getting that wrong is how a value gets reinterpreted as
commands, and the comment says so. The length is refused above
`ABSOLUTE_MAX_VALUE_LEN` before the server waits for the bytes. Keys are held to
memcached's own 250, stricter than this store's 511.

The legacy binary protocol (magic `0x80`) is deliberately absent — one fewer
parser facing strangers, and upstream deprecated it anyway.

`flush_all` is behind `protocol.flush_enabled`, off by default; `stats
cachedump` and `lru_crawler metadump` are behind `protocol.listing_enabled`, off
by default, *and* behind `Roles::LIST`. Both gates are checked ahead of the
write batching rather than inside `execute`, which `memcached.rs:158` explains
is necessary because a batched write never passes through `execute` at all —
that is a real bypass that was found and closed, and the comment records it.

The authentication mechanism is upstream's: a `set` whose value is
`<user> <pass>`. It only engages when `auth.required` is set, which is correct,
and the refusal strings are byte-compatible with 1.6.45 including the
deliberate choice to answer an unparseable command from an unauthenticated
connection as `CLIENT_ERROR unauthenticated` rather than as a parse error, so a
stranger cannot probe which commands exist.

Weakness: V-04 logs the username unescaped and unbounded.

### 3.5 No UDP

`TcpListener` only; nothing in the workspace opens a `UdpSocket`, and `stats`
reports `udpport 0` as a permanent honest zero. Memcached's UDP protocol was the
vehicle for the 2018 reflection attacks at amplification factors past 50000×,
and not implementing it is the complete fix. Worth stating explicitly because
"memcached-compatible" invites the question.

### 3.6 Cluster

Peers are ordinary VCP clients on the ordinary port — no private shortcut, no
second parser, and the peer's authority is exactly `Roles::PEER`, which is
`TAG_SYNC` and nothing else. `check_peer_role` (`lib.rs:93`) refuses to start
when this node's own peer credential lacks the role, which closes a failure that
would otherwise present as a healthy cluster that silently stops converging.

The peer list is fixed configuration, so `DELETE_BY_TAG` fan-out is not a
request-forgery primitive: a client cannot influence where the server connects.
Peer TLS (`vash-client`) requires an explicit CA bundle, has no fallback to
platform roots, verifies against a configured or derived server name, is TLS 1.3
only, and contains no `dangerous()` verifier anywhere in the workspace.

Weakness: V-07, the peer secret's storage.

### 3.7 TLS

TLS 1.3 only, `rustls` with the `ring` provider, no cipher-suite knob — with the
correct reason given, that rustls has no bad suites to turn off so a knob could
only narrow a set that is already safe. The certificate and key are read
*before* the listener binds, so a bad certificate stops startup rather than
surfacing as a handshake failure on the first client, and `Config::validate`
refuses `tls.listen` in a build without the feature rather than quietly serving
that port in the clear — which is the single most important decision in the
whole TLS surface.

`client_auth = "required"` uses `WebPkiClientVerifier`, so rustls refuses a
handshake with no chain; a certificate that verifies but matches no `mtls:` row
is then closed by `peer_identity` (`lib.rs:560`), on the correct reasoning that
holding *a* certificate is not the same as being someone this cache serves. The
handshake runs inside the spawned task, after the connection and pre-auth
permits are taken, so the most expensive thing a stranger can ask for is inside
the budget built for exactly that, with its own timeout.

Reload on `SIGHUP` replaces the whole acceptor rather than a resolver's pieces,
keeps the old one when the new files do not parse, and leaves sessions in flight
untouched.

Weakness: V-06, the identity selection among multiple matching rows.

---

## 4. What is solid

Listed because a review that only lists problems misrepresents the thing it
reviewed, and because these are the parts that should not be traded away later.

**Cryptography and credentials.** `subtle::ConstantTimeEq` over fixed-length
digests, with the length-leak reasoning written down. Authentication failures
that do not distinguish an unknown name from a bad secret. `mtls:` rows that
cannot be used as passwords and `hmac-sha256-key` rows that are refused rather
than accepted as one — both closing the same trap, and both with the trap named
in a comment. The `$…` modular-crypt namespace reserved and refused rather than
redefined. A credential file that must never half-load, so a typo is a boot
failure rather than a different set of identities than the operator believes.
`chmod 600` enforced on unix, and honestly declined on Windows rather than
faked. `auth-gen` generating the secret rather than accepting one, which is what
keeps the fast-hash argument true in practice instead of only on paper.

**Authorization.** One decision function, `AuthState::refusal`, after five call
sites were found to agree by luck. `roles_for_command` exhaustive over
`Command`, so a new variant does not compile until someone classifies it — and
the reasoning for why that matters more here than for `inline_safe` is correct.
A second header-only mapping for VCP, kept from drifting by a test rather than
by hope. The two fused commands (`GetAndTouch`, `Arithmetic`) correctly
requiring `read` as well as `write`, with `INCRBY 0` named as the reason. No
`delete` role, with the honest explanation that `write` plus a one-second TTL
makes it unenforceable.

**Defaults.** Loopback bind, admin off, TLS off, `flush_all` off, listing off,
`introspect` off, `mdbx` off. Everything dangerous is opt-in. V-03 is that this
is undermined by one line in a Dockerfile, not that the defaults are wrong.

**Parsers.** Hand-written, borrowing rather than copying, bounds-checked, with
the header/body error split done consistently in all three dialects, and six
coverage-guided fuzz targets in CI including the pre-auth `AUTH` body and the
record header. The glob matcher is the linear two-pointer algorithm, chosen
explicitly over the recursive one, with `a*a*a*a*a*a*a*a*b` against 4096 `a`s as
a regression test — that is a denial-of-service class this code has already
thought about and closed.

**Memory safety.** `unsafe` confined to the default-off mdbx FFI backend and the
Linux prefault path, every block carrying a `SAFETY:` note, with
`undocumented_unsafe_blocks` as a workspace lint so the next one cannot be the
exception. Record parsing through `zerocopy` with real error paths rather than
casts. `tags_alive` failing closed on a tag the registry has never heard of, so
a rebuilt registry is a cache miss rather than a stale hit. Arithmetic through
`checked_*`/`saturating_*` with `NaN` rejected at parse.

**Absent attack surface.** No subprocess execution anywhere. No untrusted
deserialization — `serde` reads the operator's config file and writes JSON
responses, and never parses client input. No UDP. No filesystem access from the
admin routes. No client input reflected into an HTTP response.

---

## 5. Recommendations, in order

1. **Bound the VCP reserve** (V-01) — the largest amplification measured here,
   and a change confined to `measure_vcp`.
2. **Add an absolute per-connection buffered-bytes ceiling** (V-02), checked on
   every read regardless of auth state. This subsumes `PRE_AUTH_MAX_BUFFERED`
   rather than replacing it.
3. **Warn, or refuse, on a non-loopback bind with no credentials** (V-03), and
   revisit the `Dockerfile` CMD. A startup `warn!` is one line and removes most
   of the risk that the other findings are reachable at all.
4. **Escape and truncate the logged username** (V-04) — `?` instead of `%`,
   capped at `MAX_AUTH_NAME_LEN`, in `resp.rs:546` and `memcached.rs:450`.
5. **Deadline connections that have sent nothing**, independent of whether
   authentication is enforced (V-05).
6. **Refuse an ambiguous client certificate** rather than picking by hash order
   (V-06), and validate `mtls:` subjects at load.
7. **Protect the peer secret** (V-07) — extend the mode check to the config
   file, or add an environment variable for it.
8. **Add `cargo audit` to CI** (V-16).

Items 1, 2 and 4 are small and self-contained. Item 3 is the one that changes
the shape of the threat model, because it is what decides whether the rest of
this list describes an attack on a private service or an attack on the
internet.
