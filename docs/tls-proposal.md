# TLS — a proposal

[plan.md](plan.md) §16 makes TLS a v1 non-goal, with one parenthesis of escape
hatch: *"rustls behind a feature flag if a deployment needs it"*. [auth.md](auth.md)
§1 then leans on that non-goal hard enough that it decides the credential
scheme — the wire observer is TLS's problem, so authentication does not have to
be clever — and §3.7 calls mTLS **"the right long-term answer for anyone who
needs the wire protected"** while declining to build it.

This document cashes that parenthesis. It says where termination goes, what it
costs on this server's own measured numbers, what it does to the pre-auth
budget M9 built, and how a certificate becomes an identity the existing
credential table already has a slot for.

Same house style as the plan: decision first, then the reasoning, then what was
rejected.

**The recommendation, up front:**

1. **`rustls` in-process, terminated in the accept loop**, behind a `tls` cargo
   feature that is off for `cargo build` and on for the release artefacts. Not a
   sidecar terminator — [§3](#3-where-termination-goes) argues that at length,
   and the deciding fact is that a scratch image with one static binary has
   nowhere to put a sidecar, and that cluster peers dial *out*, so a terminator
   is two hops per node, not one.
2. **A second listener on its own port**, Redis's `tls-port` model, not an
   in-band upgrade on the cache port. [§3.2](#32-one-port-or-two) — and note that
   `0x16` is currently free in `detect()`, so the in-band form is *possible*;
   it is rejected on other grounds.
3. **The connection loop becomes generic over `AsyncRead + AsyncWrite`.** One
   type parameter on `conn::handle`, `set_nodelay` moves up into the accept
   loop, and *nothing else in the server changes*. Both dialects and all three
   protocols get TLS with no per-protocol work, exactly as auth.md §3.7
   predicted. [§5](#5-where-it-goes-in-the-code) has the diff shape.
4. **mTLS is an authentication mechanism, not a parallel one.** A verified
   client certificate resolves to an `Identity` and satisfies `auth.required`
   through the same `ConnAuth` the password path fills. The credential file
   already prefixes verifiers with an algorithm and already refuses a mechanism
   it cannot execute (`auth.rs:492`); `mtls:` is one more row type.
   [§7](#7-identity-mtls-and-the-credential-table).
5. **The handshake is not the number to watch — the bulk cipher is.** This
   server moves 1.4 GB/s of value bytes at 4 KiB (README). Framing and
   encrypting that is a whole core's worth of AES-GCM, where the handshake is a
   once-per-connection cost that pooled clients amortise to nothing.
   [§8](#8-what-it-costs) does the arithmetic and says what Phase 0 must measure
   before any of this is believed.

---

> **Phase 0 has run. Three of this document's cost claims did not survive it,
> and it found a deadlock that blocks phase 1.**
> Measured on both toolchains, five repeats, in
> [benchmarks.md](benchmarks.md#what-tls-costs):
>
> 1. **[§4.2](#42-the-crypto-provider-and-what-it-does-to-the-build) was wrong
>    about the build.** Neither provider needs `cmake` or NASM — both build
>    unattended on Windows MSVC and on `rust:1.92-alpine` with only `musl-dev`.
>    But `aws-lc-sys` reaches that outcome by *silently* falling back to a
>    no-assembly build (`Building with: CC`, `cargo:rustc-cfg=universal`) that
>    runs at roughly half speed. `ring` is still the recommendation; every
>    reason given for it was wrong.
> 2. **[§8.1](#81-the-handshake-is-the-number-everyone-quotes-and-the-wrong-one-to-watch)
>    was right to call "~1 ms" pessimistic, and wrong about what to watch.**
>    A full P-256 handshake is 308 µs of server CPU on Windows and 528 on musl;
>    RSA-2048 is 804 and 1,133. Resumption erases the gap between them, because
>    what it skips is the signature.
> 3. **[§8.2](#82-the-bulk-cipher-is-the-number-to-watch) got the arithmetic
>    right and the framing wrong.** 1.86 GiB/s per core at 4 KiB is the "whole
>    core of AES-GCM" it predicted — but at 64 bytes a TLS record costs 336 ns
>    regardless of its contents, so small values are charged per *reply*, not
>    per byte. End to end (re-measured after the fix below): 96% of plaintext
>    throughput closed-loop on both platforms, 75–95% pipelined on Windows,
>    58–68% on musl, and at 64 bytes on musl TLS is 62% *faster* than
>    plaintext.
> 4. **Phase 0 also found a hang, and phase 1 fixed it.** Over TLS, a single
>    `write_all` above ~256 KiB stopped the connection dead. Phase 0 diagnosed
>    a write-write deadlock and was wrong: it is a **missing flush**, because
>    `write_all` on a TLS stream only means the session accepted the bytes.
>    One line on each end. The correction, and why the wrong answer was
>    convincing, is in [§8.4](#84-the-hang-phase-0-found-and-what-it-actually-was)
>    — and it invalidated every TLS throughput number Phase 0 published.
>
> The rest of this document is kept as written, including the parts the
> measurements went on to contradict.

---

## 1. What TLS is for here

auth.md §1 drew the line and this document stays on its side of it.

**In scope.** A party who sits *on the wire* between a client and the server:
the case authentication explicitly does not cover. Concretely — a shared or
untrusted L2 segment, traffic crossing an availability zone or a VPC peering
link, a cloud provider's network the deployment does not own, an operator who
must state that cache traffic is encrypted in transit because a compliance
regime requires the statement rather than because they have identified an
attacker. And the active version of that party: one who can *modify* the
stream, not merely read it. Against a plaintext cache, an active attacker does
not need to be subtle — flipping a `GET` reply's bytes poisons whatever the
client caches downstream, and nothing in any of the three dialects would
notice.

**Also in scope, and it is the half that gets forgotten.** Authentication over
plaintext hands the secret to that same observer. M9's credential is a bearer
token: it crosses the wire in the clear in all three dialects (auth.md §3.2,
"still plaintext on the wire"), so on a network where anyone can read frames,
`auth.required = true` buys one replay. TLS is what makes the M9 credential
worth having on such a network, which is why these two features are more
coupled than the milestone ordering suggests.

**Out of scope.** Everything auth.md §1 already excluded stays excluded: TLS
does not separate one authenticated client from another, does not give the
keyspace namespaces, and does not change what `FLUSH` or `DELETE_BY_TAG` may
do. It also does not replace the network boundary. A firewall rule still stops a
party who never sends a byte; TLS only decides what happens to the ones who do.
The README's advice (`README.md:552`) gets *amended*, not deleted: bind the port
to a private network **and** encrypt it.

**Explicitly out of scope: making TLS the default.** Off by default, like
authentication, and for the same reason — a cache that breaks every existing
memcached and Redis client on upgrade is a cache nobody upgrades.

---

## 2. Decision

**TLS 1.3 (1.2 available by configuration), terminated by `rustls` inside the
server on a dedicated listener, behind a cargo feature. The connection loop
stops naming `TcpStream` and names a stream trait instead. A verified client
certificate is one more way to fill the `Identity` that authentication already
carries.**

Six parts, each argued below:

1. **In-process, not fronted** (§3.1). The deployment shapes this project
   actually ships — a static binary on a scratch image, and cluster peers that
   dial each other — both make an external terminator worse rather than
   simpler.
2. **A second port, not an in-band upgrade** (§3.2). One byte of sniffing would
   work; what it costs is a stranger-facing state machine in front of the
   stranger-facing parsers, and the ability to say "this port is encrypted,
   full stop".
3. **`rustls`, not OpenSSL** (§4). The parsers are the only code in this server
   that reads bytes from unauthenticated strangers, and the project treats that
   fact as load-bearing — continuous fuzzing, no `unsafe`, strict caps before
   allocation (plan §15). TLS termination *becomes the first code to read those
   bytes*. It has to be held to the same standard, and a memory-safe
   implementation is how.
4. **Generic over the stream, not boxed** (§5.1). `Box<dyn AsyncRead +
   AsyncWrite>` is one vtable dispatch per `read_buf` on the hot loop, for a
   type parameter's worth of convenience. Monomorphise it.
5. **mTLS folds into `ConnAuth`** (§7). Two authentication systems that each
   half-satisfy `auth.required` is the kind of drift m10.md exists to clean up.
   One gate, two ways to pass it.
6. **Feature-gated, and refused rather than downgraded** (§6). A config that
   asks for TLS in a binary built without it must fail startup, exactly as
   `store.backend = "mdbx"` does in a build without the `mdbx` feature
   (`config.rs`). Silently serving plaintext because the feature was off is the
   worst failure in this document.

---

## 3. Where termination goes

This is the section with real alternatives, and the one whose answer most
people assume rather than argue.

### 3.1 The four options

| | In-process `rustls` | Sidecar terminator (stunnel, nginx, envoy) | Service mesh (Istio, Linkerd) | Network-layer (WireGuard, IPsec) |
|---|---|---|---|---|
| **Client compat** | Every client that speaks TLS; plain clients keep the plain port | Same | Transparent to clients inside the mesh | Transparent to everything |
| **Cluster peers** | Works — peers are `vash_client` connections and get the client half (§5.3) | **Needs an outbound terminator too**, so two extra processes per node | Works | Works |
| **Deployment fit** | One static binary, unchanged | Breaks the scratch image: a second process needs a base image, a supervisor, and a shared network namespace | Only in Kubernetes, and only with the mesh already adopted | Needs kernel modules or privileged containers |
| **Latency** | One buffer copy, no hop | An extra loopback hop each way: two more syscalls and a scheduler wake per request, on a server whose p50 is 0.20 ms | Same as sidecar, plus the mesh proxy | ~0 |
| **Identity** | Certificate → `Identity` → the credential table (§7) | The terminator knows who the client is and the server does not; passing it on means trusting a header or a `PROXY` protocol line | Mesh identity, same handoff problem | None — encrypts the pipe, authenticates the host |
| **Ops burden** | Certs in the server's config, reloaded on SIGHUP | Certs plus a second config, a second process to monitor and restart | Large, but usually already paid | Large, and usually someone else's team |
| **Verdict** | **Chosen** | Rejected as the *only* option; stays fully supported and is the right answer for a deployment that already runs one | Not our decision to make; works today with no change | Complementary, not a substitute |

Two facts decide it, and neither is about cryptography.

**The peers dial out.** `cluster.rs:411` connects to each peer through
`vash_client::Client`. A fronting terminator only ever protects *inbound*
traffic, so a cluster with a sidecar in front of each node still gossips tag
generations in the clear unless each node also runs an outbound terminator
pointed at every peer. That is `n` more processes and `n` more configs per node
to protect a link that the client half of §5.3 protects with one config key.

**The artefact is a static binary on `scratch`.** The Dockerfile ships nothing
but `vash-server` — "nothing in the image but the binary means nothing in the
image to have a CVE. There is no shell to exec into, which is the point."
A sidecar model deletes that property, and the property was deliberate.

Nothing here argues that a fronted deployment is wrong. Somebody already
running envoy for every service should keep doing that, and this proposal does
not make it harder. It argues that fronting cannot be the *only* answer for a
project shipped this way.

### 3.2 One port, or two

**Two.** `tls.listen` is a separate bind; `server.listen` keeps serving
plaintext until an operator turns it off.

The in-band alternative deserves the paragraph, because it is closer to free
than it looks. `detect()` settles the dialect on the first byte and the three
sets are `0x01`, `*`, and `a`–`z` (`vash-proto/src/lib.rs:42`). A TLS
ClientHello starts `0x16`, which today takes the `UnknownProtocol` branch and
closes the connection. Sniffing it and handing the socket to a
`LazyConfigAcceptor` would give one port that speaks plaintext and TLS at once,
with no new configuration on the client side beyond "use TLS".

It is rejected for three reasons:

1. **It puts a new state machine in front of the stranger-facing parsers.** The
   pre-auth path is the one part of this server an unauthenticated party
   controls, and M9 gave it a deadline, a buffered-byte cap
   (`PRE_AUTH_MAX_BUFFERED`, `conn.rs:22`), and its own semaphore. An upgrade
   path adds a fourth pre-auth state to reason about, and the parsers' safety
   argument depends on that region staying small.
2. **It cannot express "this port is encrypted".** With two ports an operator
   empties `server.listen`, and then there is no way to reach the cache without
   TLS — a property a security review can verify by reading `ss -lntp`. With
   one port, "plaintext is refused" is a runtime policy flag, and a flag that
   must be right is weaker than a socket that does not exist.
3. **It is the shape of a downgrade bug.** Not a downgrade *attack* — the client
   chooses — but a client library that silently falls back to plaintext on
   handshake failure would work perfectly against a mixed port, and that
   library will be written.

Redis reached the same answer with `tls-port` beside `port`, and `port 0` to
disable plaintext, which is a precedent worth taking for free: operators
already know the shape.

---

## 4. Which TLS implementation

### 4.1 The library

| | `rustls` | `openssl` / `native-tls` |
|---|---|---|
| **Memory safety on the pre-auth path** | Safe Rust; the handshake parser is the same class of code as our own parsers, held to the same standard | A C parser reading attacker-controlled bytes before authentication. Heartbleed was exactly this position in exactly this code |
| **Bad configuration** | Not expressible: no RC4, no CBC, no renegotiation, no compression | Every one of those is a config key away, and the defaults have historically been wrong |
| **Static musl build** | Builds; no system dependency | Needs `openssl-dev` and either vendored OpenSSL (large, slow builds) or dynamic linking, which the scratch image cannot do |
| **Ambient config** | None — the server's policy is the server's config | Picks up system-wide `openssl.cnf`, which makes the same binary behave differently on two hosts |
| **FIPS** | Only via `aws-lc-rs` in FIPS mode | Available on distro builds |
| **Verdict** | **Chosen** | Rejected, unless a FIPS requirement appears — and then `aws-lc-rs` in FIPS mode is the smaller change |

### 4.2 The crypto provider, and what it does to the build

`rustls` needs a provider. This is the one decision here with a real, boring
cost, because this repo builds on two toolchains that both have to keep working:
`rust:1.92-alpine` with `musl-dev` for the release artefact, and native MSVC on
Windows, where the benchmarks are run.

| | `aws-lc-rs` (rustls default) | `ring` |
|---|---|---|
| **Build deps added** | ~~`cmake`, and NASM on Windows MSVC~~ **none — measured** | A C compiler, which `heed` already requires for LMDB |
| **Alpine image** | ~~Add `cmake`~~ **no Dockerfile change** | No Dockerfile change |
| **Windows dev** | ~~The friction is here~~ **no friction; builds unattended** | Nothing |
| **What actually happens** | Falls back to a no-assembly build, quietly: 749 µs per P-256 handshake against `ring`'s 308 | Its ordinary pre-generated assembly |
| **FIPS path** | Yes, later, without a rewrite | No |
| **Verdict** | Needs `cmake` to be *worth* having, not to build | **Chosen — measured** |

**Phase 0 correction.** Every "expected" above was wrong, and the conclusion
survived anyway. `aws-lc-sys` 0.44 does not fail without `cmake`; it prints
`Building with: CC`, sets `cargo:rustc-cfg=universal`, and produces a portable
C build with no AES-NI-tuned assembly. A build that is silently half-speed is a
worse failure mode than one that stops, and it is the reason to prefer the
provider whose fast path is its only path.

**Recommendation, now measured: `ring`.** The provider is selected by handing a
`CryptoProvider` to the config builder, so this stays a line rather than an
architecture — and a deployment that needs FIPS should add `cmake` to its build
and take `aws-lc-rs` properly, rather than getting the `universal` build by
accident.

The mdbx spike found that every Rust wrapper hardcoded a flag that cost 56× on
reads, and it found that by building it rather than by reading about it. The
same held here.

### 4.3 Protocol versions

TLS 1.3 only by default. `tls.min_version = "1.2"` opts back into 1.2 for a
deployment with a client stuck on an old runtime; 1.0 and 1.1 are not
expressible, because `rustls` does not implement them and that is a feature.

No cipher suite configuration. `rustls` has no bad suites to turn off, and a
knob whose only settings are "the good ones" and "a subset of the good ones" is
an invitation to misconfigure and a support burden with no upside.

---

## 5. Where it goes in the code

The pleasant surprise: this is a small diff in a small number of places,
because the connection loop touches its socket in exactly four spots.

### 5.1 The connection loop stops naming `TcpStream`

Today `conn::handle` takes `mut stream: TcpStream` (`conn.rs:30`) and uses it
for four things: `set_nodelay` (`conn.rs:44`), `read_buf` in the `select!`
(`conn.rs:81`), `write_all` (`conn.rs:229`), and nothing else. Three of those
four are `AsyncRead`/`AsyncWrite`. The fourth is the only TCP-specific call, and
it belongs in the accept loop anyway — it is a property of the socket, not of
the protocol served over it.

```rust
pub async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    ...
```

`set_nodelay` moves to `lib.rs`, before the spawn, where the `TcpStream` is
still concrete and before the handshake — Nagle would otherwise batch handshake
flights, which is the one place it hurts most.

Monomorphisation, not `Box<dyn ...>`: the read in the `select!` runs once per
syscall on every connection, and a vtable there is a cost paid forever to save a
type parameter. Two instantiations of the loop is a code-size number nobody will
notice on a binary that already links LMDB.

Cancel-safety is unchanged and this matters: the `select!` comment at
`conn.rs:70` depends on both branches being cancel-safe. `tokio_rustls`'s stream
is cancel-safe for reads in the same sense `TcpStream` is — a cancelled read has
consumed nothing from the *plaintext* stream, and buffered ciphertext stays
buffered inside the session. Phase 1 states this as a test, not as a paragraph.

### 5.2 The accept loop grows a second arm

`serve()`'s `select!` (`lib.rs:285`) gains a `tls_listener.accept()` branch that
is the existing one plus a handshake. The ordering inside it is the
security-relevant part:

1. Acquire the connection permit and the **pre-auth permit first**, before the
   handshake, not after. The handshake is the expensive thing a stranger can
   make the server do, so it must be inside the budget M9 built for exactly
   this, not in front of it.
2. `tokio::spawn`, then handshake **inside the spawned task** — never in the
   accept loop. A handshake in the loop is a client-controlled stall on every
   other pending accept.
3. `timeout(tls.handshake_timeout_ms, acceptor.accept(stream))`. A peer that
   opens a socket and sends half a ClientHello must not hold a slot until the
   auth deadline; the handshake gets its own, shorter one.
4. On success, drop into the same `conn::handle` the plaintext path uses, with
   the connection's row in `stats conns` marked first.

### 5.3 The client, and therefore the cluster

`vash_client::Client` holds `stream: TcpStream` (`vash-client/src/lib.rs:42`).
Here the choice goes the other way: an **enum**, not a type parameter.

```rust
enum Stream { Plain(TcpStream), Tls(Box<TlsStream<TcpStream>>) }
```

`Client` is a public type held in `Option<vash_client::Client>` by every peer
task (`cluster.rs:289`); giving it a type parameter would infect the peer
connection, the cluster's error type, and every caller, to save one branch per
syscall on a request-at-a-time client. The branch is free beside the syscall;
the API churn is not.

The cluster then needs only configuration: `cluster.tls`, a CA path, and — the
detail that will actually bite — a server name for verification.
`cluster.peers` is a list of `host:port` strings, so the name is there when
peers are named by DNS and absent when they are named by IP. An IP-only peer
list needs certificates with IP SANs, or a `cluster.tls_server_name` override
for the single-certificate case. Say it in the config comments, because the
failure is a handshake rejection that reads like a network fault — the same
class of confusion `cluster.rs:352` already records for connect timeouts.

### 5.4 What does not change

Dispatch, the store, the three parsers, group commit, sharding, tags, the
executor, the `Store` trait. TLS is a property of the socket, and this server's
layering means that is all it has to be. That is the claim auth.md §3.7 made
about mTLS ("terminates in front of the protocol code, so all three dialects get
it at once"), and reading the code, it holds.

### 5.5 The admin endpoint

`admin::serve` (`admin.rs:55`) gets the same treatment or none.
**Recommendation: none, in phase 1**, and say so explicitly in the docs —
`/metrics` carries no keys and no values, it is conventionally bound to loopback
or a private interface, and scraping over TLS brings its own certificate story.
The code is the same acceptor if a deployment later needs it; the argument for
waiting is that nothing on that port is secret.

---

## 6. Configuration surface

```toml
[tls]
# A second listener. The plaintext port keeps serving until you empty
# `server.listen`, which is how a rollout works: turn this on, roll every
# client and peer, then close the plain port.
listen = ""                      # e.g. "0.0.0.0:11312"; empty means no TLS listener

cert = ""                        # PEM chain: leaf first, then intermediates
key = ""                         # PEM private key (PKCS#8 or SEC1)

min_version = "1.3"              # "1.3" or "1.2". Older is not expressible.
handshake_timeout_ms = 3000      # a half-finished handshake holds a pre-auth slot

# mTLS. "none" asks for no certificate; "required" refuses a connection without
# a valid one. There is deliberately no "optional": a certificate that may be
# absent authenticates nobody. See §7.
client_auth = "none"
client_ca = ""                   # PEM bundle of CAs whose certificates are accepted
# Which field of a verified certificate becomes the credential name.
identity_from = "san_dns"        # or "cn"

# Resumption. Tickets are on because a connect-per-request client — which is
# most PHP memcached deployments — otherwise pays a full handshake per request.
session_tickets = true
ticket_rotation_secs = 3600      # keys are generated at boot and never written to disk

[cluster]
tls = false                      # peers are dialled over TLS
tls_ca = ""                      # CA for verifying peers; empty uses the platform roots
tls_server_name = ""             # override when peers are named by IP
```

Three notes on shape:

**`listen = ""` for off** matches `observability.admin_listen`, which already
uses the empty string for "do not bind". Consistency with the file the operator
is already reading beats a separate `enabled` boolean.

**No `[tls] enabled`.** A cert path with no listener, or a listener with no
cert, is a configuration error and `Config::validate` (`config.rs:619`) should
say so at startup. An `enabled` flag adds a third state that means "configured
but ignored", which is the state auth.md refused to add for authentication.

**Refused, not downgraded.** `tls.listen` set in a binary built without the
`tls` feature must fail startup with a message naming the feature — the
`store.backend = "mdbx"` precedent, which "is refused rather than quietly
downgraded when it is not" (`config.rs:50`). This is worth a test of its own,
because it is the one misconfiguration that would leave an operator believing
traffic is encrypted when it is not.

---

## 7. Identity: mTLS and the credential table

The temptation is to treat a client certificate as a separate access-control
system. Resist it. The server already has one gate — `ConnAuth` holds an
`Option<Identity>` (`auth.rs:102`), and `auth.required` refuses commands
without it — and a second gate that also satisfies `auth.required` is precisely
the divergence m10.md was written to undo.

**Design: a verified certificate produces an `Identity` and calls the same
`succeed()` path an `AUTH` command would.**

The name comes from the certificate's SAN DNS entry (or CN, by configuration)
and must match a row in the credential file:

```
billing-api  mtls:billing-api.svc.cluster.local
```

The `mtls:` verifier says "this row is satisfied by a certificate naming this
subject, and by nothing else" — a row that cannot be passed by presenting a
password, in the same way a `sha256:` row cannot be passed by presenting a
certificate. The file format already carries an algorithm prefix and already
refuses a row whose mechanism is not built (`auth.rs:492`, for
`hmac-sha256-key`), so this is one match arm and one `Mechanism` variant.

What this buys, beyond encryption:

- **`auth.required = true` with no secrets distributed at all.** The credential
  file becomes a list of names; the secrets are certificates that a PKI already
  rotates and expires.
- **Revocation that is not a restart.** Removing a row and sending `SIGHUP`
  already works — credential reload is built. Certificate revocation proper
  (CRL/OCSP) is *not* proposed; the credential table is the revocation list,
  which for a cache with a handful of client services is the right size of
  mechanism.
- **Cluster peers with no shared secret**, which is the deployment where
  distributing a peer credential to every node is most annoying.

**Why no `client_auth = "optional"`.** A mode where a client may present a
certificate or may not is a mode where the server cannot state who is on the
other end. It reads as flexibility and behaves as a hole. Two modes: ask for
nothing, or require and verify.

---

## 8. What it costs

### 8.1 The handshake is the number everyone quotes and the wrong one to watch

auth.md §3.7 costed mTLS at "~1 ms and an allocation storm". For a full TLS 1.3
handshake with an ECDSA P-256 leaf that is pessimistic — the server's work is
one signature and one key agreement — but the shape of the objection is right:
it is the most expensive thing in connection setup by orders of magnitude, and
an unauthenticated party controls how often it happens.

**Measured (Phase 0), server-side CPU per handshake, one thread:**

| leaf | resumed | Windows | musl |
|---|---|---:|---:|
| ECDSA P-256 | no | 308 µs | 528 µs |
| ECDSA P-256 | yes | 298 µs | 476 µs |
| RSA-2048 | no | 804 µs | 1,133 µs |
| RSA-2048 | yes | 297 µs | 448 µs |

So "~1 ms" was right for RSA and about 3× pessimistic for P-256, and point 2
below turns out to matter more than point 1: **resumption erases the difference
between the two key algorithms entirely**, because what it skips is the
signature. Twelve threads buy about 5×, not 12× — six physical cores, and this
arithmetic does not share one well.

Three things make it survivable, in descending order of importance:

1. **Connections are pooled.** A handshake amortised over a connection that
   serves millions of requests is not a cost. The clients that hurt are
   connect-per-request ones, which is why `session_tickets` defaults on.
2. **ECDSA, not RSA.** An RSA-2048 leaf costs the *server* substantially more
   per handshake than P-256, because the server signs. The operations runbook
   should say this, since it is a choice made when the certificate is issued and
   cannot be fixed later in config.
3. **It is inside the pre-auth budget** (§5.2), which already caps concurrent
   unauthenticated connections at a tenth of the connection limit by default
   (`lib.rs:125`).

### 8.2 The bulk cipher is the number to watch

This is the repo-specific finding, and it comes out of the README's own
measurements.

`GET` at 4 KiB runs at 337,000 ops/s, which the README describes as **1.4 GB/s
of value bytes leaving the process**. Every one of those bytes now has to be
framed into TLS records and encrypted. AES-GCM with AES-NI runs on the order of
1–4 GB/s per core depending on microarchitecture and record size, so at that
workload TLS is plausibly **a whole core of additional CPU**, on a four-core
measurement box.

**Measured (Phase 0).** The arithmetic holds: one core encrypts 1.86 GiB/s at
4 KiB records on Windows, 1.79 on musl, which is the "whole core" this section
predicted. The framing was wrong, and this is the finding worth keeping:

| record | Windows | ns per record | musl | ns per record |
|---:|---:|---:|---:|---:|
| 64 B | 0.18 GiB/s | 336 | 0.12 GiB/s | 504 |
| 1 KiB | 1.17 GiB/s | 818 | 1.16 GiB/s | 822 |
| 16 KiB | 2.28 GiB/s | 6,699 | 2.19 GiB/s | 6,961 |

**A 64-byte reply costs about what a 1 KiB reply costs**, because a record's
fixed cost — header, nonce, tag, and the call around them — dominates until the
payload is a kilobyte. A cache serving small values is charged per reply, not
per byte, which is the opposite of what this section assumed when it named
bandwidth as the thing to watch.

End to end, with the load generator pointed at a real TLS listener: **63–68% of
plaintext throughput at 1 KiB and 4 KiB on both platforms**, 91% at 64 bytes on
Windows, and — reproducibly, five repeats — **168% at 64 bytes on musl**, where
rustls' record buffering appears to hand the server larger read blocks than the
plaintext path does. Closed-loop latency does not move at all: p50 0.330 ms
against 0.329. The full tables are in
[benchmarks.md](benchmarks.md#what-tls-costs).

That is not an argument against doing it. It is an argument that:

- the headline throughput numbers must be **re-measured with TLS on** and
  published as their own row in `benchmarks.md`, not estimated;
- large-value workloads are where TLS will show, and small-value workloads
  (64 B at 2.04M ops/s) are where the *handshake and per-record overhead* will
  show instead, so both ends of the size range have to be measured;
- `SET` at 40,000 ops/s is bounded by the storage engine and should barely
  move, which is worth stating so nobody attributes a write regression to TLS.

### 8.3 What Phase 0 measured

Following the mdbx spike's discipline — build it, measure it, and let the
measurements contradict this document. All five ran; the answers are in
[benchmarks.md](benchmarks.md#what-tls-costs) and summarised at the top of this
document:

| # | Question | How |
|---|---|---|
| 1 | Does `ring` build unchanged on `rust:1.92-alpine` + `musl-dev`, and on native MSVC? | Add the dep, build both. This decides §4.2 |
| 2 | Handshakes per second per core, ECDSA P-256 vs RSA-2048, full vs resumed | A loop in `vash-bench` that connects and disconnects |
| 3 | Throughput delta at 64 B, 1 KiB, 4 KiB, pipelined and closed-loop | The existing `vash-bench` workloads, pointed at the TLS port |
| 4 | p50/p99 delta on the closed-loop latency table | Same harness, same table shape as README |
| 5 | Binary size and build time delta with the feature on | `ls -l`, `cargo build --timings` |

**Answers:** (1) both providers build unattended on both toolchains, and
`aws-lc-rs` does it by going no-assembly — §4.2. (2) 308 µs per full P-256
handshake on Windows, 804 for RSA-2048, resumption erasing the gap — §8.1.
(3) and (4) 63–68% of plaintext throughput at 1 KiB and above, no measurable
latency change, and one result nobody predicted — §8.2. (5) +1.05 MiB of
binary (2.84 → 3.95 MB) and ten seconds of crate rebuild.

Run it with the same discipline benchmarks.md already demands: five repeats,
both platforms, ranges reported — run-to-run variance on this harness is around
±25%, so a single pair of numbers proves nothing.

If (3) shows more than the arithmetic in §8.2 predicts, the record size and the
write path are the first suspects — `write_all` of a whole reply buffer
(`conn.rs:229`) is already the right shape for TLS, one record per reply batch,
but that should be confirmed rather than assumed.

### 8.4 The hang Phase 0 found, and what it actually was

**Phase 0 recorded this as a write-write deadlock. That diagnosis was wrong,
and it is worth keeping the correction rather than the conclusion.**

The symptom was real and deterministic: over TLS, a single `write_all` of more
than roughly 256 KiB stopped the connection dead, where plaintext at the same
sizes never did.

| one batch | plaintext | TLS |
|---|---:|---:|
| 32 × 4 KiB = 128 KiB | 2,911 ops/s | 2,163 ops/s |
| 64 × 4 KiB = 256 KiB | 2,483 ops/s | **hung** |
| 256 × 4 KiB = 1 MiB | 4,157 ops/s | **hung** |

The explanation offered — that both ends write a whole batch before reading any
of it, and that TLS spends the socket-buffer slack plaintext survives on — was
plausible, fit the threshold, and was not what was happening. Instrumenting
both loops showed the client had *finished* writing and was reading, the server
was reading, and 55 replies were missing. Nobody was blocked on a write at all.

**It was a missing flush.** `write_all` on a TLS stream means the session
*accepted* the bytes, not that they reached the socket: if the socket was not
writable for all of them, the remainder stays as ciphertext inside `rustls`, and
nothing sends it until something polls the write side again. Both ends then wait
on reads for data that was accepted and never transmitted. Over a plaintext
`TcpStream` `flush` is a no-op, which is why eleven milestones of this server
never needed one — the requirement arrives with the first buffered transport.

The fix is one line on each side, and both are load-bearing: removing the
server's flush hangs `GET` (large replies), removing the client's hangs
`SET` (large requests). `conn::handle` flushes after its reply buffer;
`vash-bench`'s load generator flushes after its batch.

Three things follow, and the middle one is the reason this section still
exists:

1. **The threshold was a symptom, not a cause.** 256 KiB is where a write stops
   fitting in the socket buffer on this platform, which is where a remainder
   first gets stranded. It is not a limit on anything.
2. **The published throughput numbers were measurements of the bug.** Every TLS
   figure Phase 0 reported was taken with writes intermittently stranded until
   some later write flushed them. Re-measured on both platforms after the fix,
   1 KiB pipelined `GET` went from 63% of plaintext to 75% on Windows, and
   4 KiB — the cell that could not be filled at all — is 95%.
   [benchmarks.md](benchmarks.md#what-tls-costs) carries the corrected tables
   and says what the retracted ones were measuring.
3. **The plaintext path was never at risk**, so there is no latent bug to fix
   there. The earlier claim that there was one followed from the wrong
   mechanism.

This is what Phase 0 is for, and the lesson is narrower than "measure": the
hang was reproducible in one command, and *reasoning* about it produced a
confident, well-argued, wrong answer that survived being written into two
documents. Twenty minutes of `eprintln` in both loops produced the right one.

---

## 9. Denial of service

TLS adds one genuinely new capability for an unauthenticated stranger: making
the server do public-key cryptography on demand. The existing pre-auth
machinery covers it, provided the ordering in §5.2 is respected — permits
first, handshake second, both inside a spawned task with its own timeout.

Three smaller things:

**No renegotiation.** TLS 1.3 does not have it. This is one of the reasons for
the version floor.

**Client-initiated key updates.** TLS 1.3's `KeyUpdate` is symmetric-only and
cheap, but not free, and a peer can send them in a loop. Phase 1 should confirm
what the `rustls` version in use bounds this at, and record the answer in the
security notes rather than discovering it later.

**A handshake that never completes** is the cheapest attack, and it is what
`handshake_timeout_ms` exists for. It defaults *below* `auth.timeout_ms`
(5000 ms), because a handshake is a fixed number of round trips and a client
that cannot finish it in three seconds is not going to.

---

## 10. What an operator sees

**`stats settings` must stop lying.** `stats.rs:322` hardcodes
`ssl_enabled = no` with the comment *"A client that checks this before sending a
credential must not be told otherwise"* — which is exactly right, and exactly
why it becomes a per-connection value the moment TLS exists. A memcached client
that checks this field before sending its password is doing the correct thing,
and after this change it gets the correct answer.

**`stats conns` gains a `tls` column.** `ConnInfo` already carries `dialect` and
`authenticated` as atomics written once by the owning task
(`connections.rs:29`); a third follows the same pattern. An operator answering
"is anything still connecting in the clear?" during a rollout needs exactly this
and has nothing else.

**Metrics**, in the shape M10 already established:

| Metric | Why |
|---|---|
| `tls_handshakes_total` | The denominator for everything else |
| `tls_handshake_failures_total{reason}` | Expired cert, unknown CA, no shared version, timeout — four different operator actions, and they must not be one counter |
| `tls_handshake_seconds` | A histogram, beside the M10 latency ones |
| `tls_connections`, beside plaintext connections | The rollout gauge: this is how an operator knows it is safe to close the plain port |
| `tls_cert_expiry_seconds` | The one metric that prevents the outage everybody eventually has. A gauge, alerted well before zero |

**Certificate reload on SIGHUP**, reusing the credential reload that already
exists (`spawn_credential_reload`, `lib.rs:424`). Certificates expire every 90
days under ACME; a cache that must be restarted to pick up a renewal will
eventually not be restarted in time. Mechanically this is a certificate
resolver reading an `Arc` that the existing reload task swaps, and a reload that
fails to parse must **keep the old certificate and log loudly** rather than
leave the listener with none — the same shape the credential reload already
uses.

---

## 11. Rejected

**Encrypting values at rest instead.** Different problem, and the wrong one: the
threat here is the wire, not the disk, and a cache whose contents are
reconstructible from the source of truth has little at rest worth protecting.

**Application-layer encryption of values only.** Keys still leak, and key names
in a cache are frequently more sensitive than values (`session:<user-id>`,
`cart:<email>`). It also breaks every existing client.

**A VCP capability bit advertising that a TLS port exists.** A plaintext
connection asking "do you support TLS?" is negotiation an active attacker can
edit, and answering it teaches clients a habit — connect plain, upgrade if
offered — that is worse than the problem. A client that needs TLS is configured
for TLS.

**Making the whole thing a runtime option with no cargo feature.** The feature
exists so that a deployment that does not want TLS does not link a TLS stack:
smaller binary, smaller audit surface, fewer CVE notifications for code that is
not reachable. The cost is the refuse-at-startup path in §6, which is cheap.

**`client_auth = "optional"`** (§7), **cipher suite configuration** (§4.3), and
**TLS below 1.2** (§4.3) — each argued in place.

**Waiting for a v2.** The parenthesis in plan §16 is nine words long and has
been read by at least three other documents as a promise. Either the escape
hatch is real, in which case this is what it costs, or it should be struck from
§16 so nobody plans around it.

---

## 12. Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| **0** ✅ | Spike: provider choice on both toolchains, handshake rate, throughput and latency deltas at three value sizes | **Done.** Numbers in [benchmarks.md](benchmarks.md#what-tls-costs), five repeats, both platforms; §4.2, §8.1 and §8.2 corrected against them; one deadlock found (§8.4) |
| **1** ✅ | The flush of §8.4, then server-side: `conn::handle` generic, `set_nodelay` moved, `[tls]` config, the second listener, handshake timeout inside the pre-auth budget, `ssl_enabled` per connection, `vash_tls` in `stats conns`, four metrics (the certificate-expiry gauge is phase 4, with the reload it belongs to) | **Done.** A 1 MiB pipelined batch completes over TLS in both directions (`tests/tls.rs`); plaintext and TLS ports serve one store concurrently; `ssl_enabled` answers per connection; a `tls.listen` in a non-`tls` build refuses to start. Third-party clients are *not* yet verified — see the note under the table |
| **2** | Client-side: `vash_client` `Stream` enum, `cluster.tls`, SNI and the IP-peer override | A three-node cluster converges over TLS, including a peer that was down; a bad CA is logged as a configuration error and not as unreachability |
| **3** | mTLS: `client_auth = "required"`, `mtls:` credential rows, `Identity` from the certificate, `stats conns` showing it | `auth.required = true` is satisfied by a certificate alone; a certificate whose name is not in the table is refused; removing a row and sending `SIGHUP` locks that client out without a restart |
| **4** | Operations: SIGHUP certificate reload, the expiry metric, the runbook — issuing, rotating, closing the plaintext port, and what each failure looks like from the client side | A certificate renewed under the running server is picked up with no dropped connection; `operations.md` covers the four handshake failure modes by symptom |

Phase 1 is the one with value on its own. Phases 2–4 are each worth doing and
none of them is worth blocking phase 1 on.

**One phase-1 exit criterion is not met.** "A real `redis-cli --tls` and a real
memcached client with TLS drive the server unchanged" has not been tested: no
such client is installed on the development machine, and the suite in
`tests/tls.rs` drives the server with a `rustls` client of our own instead.
That proves the termination works and proves nothing about interoperability —
`redis-cli --tls` needs a `tls-port`-shaped configuration and a CA path, and
the memcached ASCII clients that support TLS are a small subset. It is carried
into phase 2, where the client work makes a real third-party client necessary
anyway.

---

## 13. What else has to change

Not code — prose that currently states a non-goal as a fact, and would become
wrong the moment phase 1 lands:

| Where | What it says now |
|---|---|
| `plan.md` §16 | "no TLS in v1 (rustls behind a feature flag if a deployment needs it)" — becomes a milestone row in §14 |
| `plan.md` §15 | No row for "cache traffic is readable on the wire". There should have been one from the start |
| `auth.md` §1, §3.7, the §3 summary table | "TLS — a v1 non-goal"; mTLS "out of scope, and the right long-term answer". §3.7's verdict changes, and §1's threat model gains a companion sentence rather than a rewrite |
| `README.md:552` | "Without TLS every key and value crosses the wire in the clear" — becomes "unless you turn it on, and here is how" |
| `guide.md:324` | "there is no TLS, so authentication decides who may *use* the cache" |
| `operations.md:44` | The same claim, in the security section |
| `stats-subcommands.md:195` | `ssl_enabled` documented as permanently `no` |
| `vash.example.toml:367` | The `[auth]` preamble's "it does not make the connection private" paragraph, plus a new `[tls]` section |

---

## 14. Non-goals of this proposal

Stated so they are not re-litigated: no TLS on the admin endpoint in phase 1
(§5.5), no CRL or OCSP (§7 — the credential table is the revocation list), no
certificate issuance or ACME client in the server (that is `certbot`'s job, and
the SIGHUP reload is the whole integration), no encryption at rest (§11), no
per-client authorisation beyond the single `Identity` authentication already
carries (auth.md §1 keeps this out of scope), and **no change to the default**:
a server that is upgraded and not reconfigured serves exactly what it served
before, on exactly the port it served it on.
