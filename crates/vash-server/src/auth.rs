//! The credential table, the file it is loaded from, and per-connection
//! identity.
//!
//! Authentication here is a property of a **connection**, not an operation on a
//! cache: it is checked once, at the start, and the storage tier never learns
//! that it exists. Nothing in `vash_core::Command` changes, and no request
//! carries an identity.
//!
//! # What this defends against
//!
//! A party who can reach the port but does not sit on the wire — an adjacent
//! service in a shared cluster, a process on a co-tenant host, an accidental
//! bind to `0.0.0.0`. It does **not** make the connection private: without TLS
//! every key and value crosses the network in the clear, so an eavesdropper
//! already has the data and hiding the password from them protects the wrong
//! thing. See `docs/auth.md` §1; that boundary decides most of the design.
//!
//! # Why a fast hash
//!
//! The verifier is `SHA-256(secret)`, not a password KDF. A KDF exists to make
//! offline guessing of a low-entropy *human* password expensive, at a
//! deliberate 50–100ms per verification. This credential is machine-generated
//! with 256 bits of entropy, where guessing is already infeasible against a
//! fast hash — and the cost would land on connection setup, handing an
//! unauthenticated stranger a CPU amplification factor of roughly a hundred
//! thousand. `auth-gen` generating the secret rather than accepting one is what
//! keeps that assumption true.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::{Context, bail, ensure};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::{info, warn};

/// The identity a credential-less deployment, a bare `VASH_AUTH_SECRET`, and
/// Redis's one-argument `AUTH` all resolve to.
pub const DEFAULT_NAME: &str = "default";

/// Environment variable holding a single `default` secret, for containers where
/// a one-row table does not justify a file mount.
pub const SECRET_ENV: &str = "VASH_AUTH_SECRET";

/// Shortest secret accepted, in bytes.
///
/// Not a password-strength rule — it is what the storage choice above depends
/// on. A fast unsalted hash is correct for a high-entropy token and worthless
/// for `1234`, so the floor is part of the security argument rather than
/// hygiene advice.
pub const MIN_SECRET_LEN: usize = 16;

const MAX_NAME_LEN: usize = 64;

/// The authority a credential carries, as a bitset.
///
/// A bitset rather than a set of enums because the check runs once per command,
/// on a value the connection already holds: `allows` is an AND and a compare,
/// with no lock, no map probe and no allocation. See `docs/roles-proposal.md`
/// §5.
///
/// The roles are rungs of *blast radius*, not a classification of what a
/// command does to storage — `DELETE_BY_TAG` is a write in the storage sense
/// and is its own rung here, because it destroys keys the caller cannot name.
/// There is deliberately no `delete` role: an identity with [`Roles::WRITE`]
/// deletes any key it can name by writing it with a one-second TTL, so the role
/// would fence off nothing while documenting a boundary the server cannot
/// enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Roles(u8);

impl Roles {
    /// Authorises nothing. What a row inherits when `auth.default_roles` is set
    /// to the empty list.
    pub const NONE: Self = Self(0);
    /// `GET`, `GET_MANY`, and the deadline queries. Gates commands that return
    /// a stored **value**.
    pub const READ: Self = Self(0x01);
    /// Changes one named key at a time.
    pub const WRITE: Self = Self(0x02);
    /// `DELETE_BY_TAG`.
    pub const INVALIDATE: Self = Self(0x04);
    /// The listings, in every dialect that has one.
    pub const LIST: Self = Self(0x08);
    /// `FLUSH` and memcached's `flush_all`.
    pub const FLUSH: Self = Self(0x10);
    /// `TAG_SYNC` — a cluster peer's authority, and only that.
    pub const PEER: Self = Self(0x20);

    /// What the three fused commands need: they return a stored value *and*
    /// write. See [`roles_for_command`].
    pub const READ_WRITE: Self = Self(Self::READ.0 | Self::WRITE.0);

    /// What `auth.default_roles` is when unset: everything, now and in later
    /// versions.
    ///
    /// Every bit rather than the six defined ones, and that is the whole of the
    /// compatibility promise: a credential file written before roles existed
    /// grants a seventh role added in a future version, where an operator who
    /// enumerated all six today would not. Absence is permissive, presence is
    /// exact — the same asymmetry the `roles=` field itself has.
    pub const UNRESTRICTED: Self = Self(u8::MAX);

    /// How many roles this build defines. The width of the per-role counters.
    pub const COUNT: usize = 6;

    /// The roles in wire and rendering order, with their names.
    const NAMED: [(Self, &'static str); Self::COUNT] = [
        (Self::READ, "read"),
        (Self::WRITE, "write"),
        (Self::INVALIDATE, "invalidate"),
        (Self::LIST, "list"),
        (Self::FLUSH, "flush"),
        (Self::PEER, "peer"),
    ];

    /// The roles with their counter index and label, for the metrics family.
    pub const INDEXED: [(usize, &'static str, Self); Self::COUNT] = [
        (0, "read", Self::READ),
        (1, "write", Self::WRITE),
        (2, "invalidate", Self::INVALIDATE),
        (3, "list", Self::LIST),
        (4, "flush", Self::FLUSH),
        (5, "peer", Self::PEER),
    ];

    /// Whether this set satisfies everything `needed` asks for.
    ///
    /// Takes a set rather than one role so the commands that need two are one
    /// call at each gate instead of a special case at each of them.
    #[inline]
    pub fn allows(self, needed: Roles) -> bool {
        self.0 & needed.0 == needed.0
    }

    /// What `needed` asks for and this set does not have. For the denial log,
    /// which has to name the role an operator should add.
    #[inline]
    pub fn missing(self, needed: Roles) -> Roles {
        Roles(needed.0 & !self.0)
    }

    #[inline]
    pub fn union(self, other: Roles) -> Roles {
        Roles(self.0 | other.0)
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The bitset as it travels in the `AUTH` reply body.
    #[inline]
    pub fn bits(self) -> u8 {
        self.0
    }

    pub fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// The names of the roles present, comma-separated.
    ///
    /// An unrestricted identity renders as the six this build knows, which is
    /// an honest answer to "what can this connection do here" even though the
    /// stored value also covers roles that do not exist yet.
    pub fn to_names(self) -> String {
        if self.is_empty() {
            return "none".into();
        }
        let mut out = String::new();
        for (role, name) in Self::NAMED {
            if self.allows(role) {
                if !out.is_empty() {
                    out.push(',');
                }
                out.push_str(name);
            }
        }
        out
    }

    /// Parses one role name.
    fn from_name(name: &str) -> Option<Self> {
        Self::NAMED
            .iter()
            .find(|(_, known)| *known == name)
            .map(|(role, _)| *role)
    }

    /// Parses a comma-separated list, as it appears in `roles=` and in
    /// `auth.default_roles`.
    ///
    /// Every failure is an error rather than a skipped name, under the rule the
    /// loader already has: a credential file must never half-load. A typo that
    /// reads as a *smaller* grant is exactly as dangerous as one that reads as
    /// a larger one — it silently breaks a working client instead.
    pub fn parse_list(list: &str) -> anyhow::Result<Self> {
        ensure!(
            !list.is_empty(),
            "the role list is empty. Omit `roles=` to inherit auth.default_roles, or delete \
             the row if it should authorise nothing — an empty list reads at a glance like the \
             field being absent while meaning the opposite"
        );

        let mut roles = Self::NONE;
        for name in list.split(',') {
            let role = Self::from_name(name).with_context(|| {
                format!(
                    "unknown role {name:?}. Known: {}",
                    Self::NAMED
                        .iter()
                        .map(|(_, n)| *n)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;
            ensure!(!roles.allows(role), "role {name:?} is listed twice");
            roles = roles.union(role);
        }
        Ok(roles)
    }

    /// Parses `auth.default_roles`, where the empty list is meaningful.
    ///
    /// `None` — the key unset — is [`Roles::UNRESTRICTED`]; `Some([])` is
    /// [`Roles::NONE`], the fail-safe form of "every row must state its own
    /// authority".
    pub fn parse_default(configured: Option<&[String]>) -> anyhow::Result<Self> {
        let Some(names) = configured else {
            return Ok(Self::UNRESTRICTED);
        };
        if names.is_empty() {
            return Ok(Self::NONE);
        }
        Self::parse_list(&names.join(",")).context("in auth.default_roles")
    }
}

impl std::fmt::Display for Roles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_names())
    }
}

/// Which VCP mechanism a credential can satisfy.
///
/// Rows are **bound** to their mechanism. Without that rule the raw key stored
/// for an `hmac-sha256-key` identity is a password that logs in as that
/// identity over `PLAIN`, which would defeat storing a digest for everyone
/// else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mechanism {
    /// The secret crosses the wire; the table holds `SHA-256(secret)`.
    Plain = 0,
    /// Challenge–response. Specified in `docs/auth.md` §6.3 and **not built**;
    /// the value exists so adding it later is not a wire change.
    HmacSha256 = 1,
}

impl Mechanism {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Plain,
            1 => Self::HmacSha256,
            _ => return None,
        })
    }
}

/// Who a connection authenticated as.
///
/// Cheap to clone because it crosses a `spawn_blocking` boundary with the rest
/// of the connection's state on every batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    name: Arc<str>,
    /// Resolved once, here, and never looked up again.
    ///
    /// Carrying the set rather than the row's name is what keeps the per-command
    /// check off the credential table: looking the row up per command would put
    /// the `RwLock<Arc<Auth>>` and a map probe on every `GET`, on a cacheline
    /// every connection thread shares, to answer a question that cannot change
    /// for the life of the identity.
    ///
    /// The consequence, and it is the same one a rotated secret has: editing a
    /// row's `roles=` and reloading affects the *next* authentication, not a
    /// connection already holding this.
    roles: Roles,
}

impl Identity {
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn roles(&self) -> Roles {
        self.roles
    }
}

/// One connection's authentication state.
///
/// Per connection and never shared, like the negotiated RESP version beside it.
/// It is `Clone` because it crosses the `spawn_blocking` hop with every batch
/// and is copied back — an `AUTH` and the commands after it can arrive in one
/// pipelined read, so authentication has to take effect *within* a block rather
/// than between reads.
#[derive(Debug, Clone, Default)]
pub struct ConnAuth {
    identity: Option<Identity>,
    failures: u32,
}

impl ConnAuth {
    #[inline]
    pub fn is_authenticated(&self) -> bool {
        self.identity.is_some()
    }

    pub fn identity(&self) -> Option<&Identity> {
        self.identity.as_ref()
    }

    #[inline]
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// Records a success, replacing any previous identity.
    ///
    /// Re-authenticating on a live connection is allowed so a pooled connection
    /// can follow a credential rotation without reconnecting.
    pub fn succeed(&mut self, identity: Identity) {
        self.identity = Some(identity);
    }

    /// Records a failure. A failed re-authentication leaves an existing
    /// identity intact — it is an attempt that did not land, not a logout.
    pub fn fail(&mut self) {
        self.failures = self.failures.saturating_add(1);
    }
}

/// One row of the credential table.
#[derive(Debug, Clone)]
struct Credential {
    mechanism: Mechanism,
    /// A digest for [`Mechanism::Plain`]; the key itself for
    /// [`Mechanism::HmacSha256`].
    value: [u8; 32],
    /// The certificate subject this row accepts, for an `mtls:` row.
    ///
    /// `Some` exactly when the row is satisfied by the TLS handshake instead
    /// of by an `AUTH` command. Such a row holds no secret at all: the subject
    /// is public, and what proves the client owns it is a certificate chain
    /// the configured CA signed. Presenting the subject as a password gets
    /// nowhere, because [`Auth::verify`] only ever looks at `value`.
    subject: Option<Box<str>>,
    /// What this identity may do, resolved at load.
    ///
    /// Held as a concrete set rather than an `Option` the check has to unwrap:
    /// by the time the table is live, an inherited set and a written one are
    /// indistinguishable, which is what keeps `auth.default_roles` confined to
    /// the loader.
    roles: Roles,
    /// Whether `roles` came from `auth.default_roles` rather than from a
    /// `roles=` field. Kept only so startup can report which rows took the
    /// default — see [`Auth::load`].
    inherited: bool,
}

/// The credential table, plus whether it is being enforced.
///
/// Enforcement is separate from the table on purpose: configuring credentials
/// while `required` is false is the first step of the rollout in
/// `docs/auth.md` §15, and it is why `AUTH` is answered truthfully even when
/// nothing is being refused.
#[derive(Debug, Default)]
pub struct Auth {
    required: bool,
    table: HashMap<Box<[u8]>, Credential>,
}

/// Resolves `auth.default_roles` into every row that omitted `roles=`, and
/// reports which rows those were.
///
/// Split out from [`Auth::load`] so the resolution is testable without a
/// config, and because it is the one place the default is read: below this
/// line nothing knows whether a set was inherited or written.
fn resolve_defaults(table: &mut HashMap<Box<[u8]>, Credential>, default: Roles) -> Vec<String> {
    let mut inherited = Vec::new();
    for (name, credential) in table.iter_mut() {
        if credential.inherited {
            credential.roles = default;
            inherited.push(String::from_utf8_lossy(name).into_owned());
        }
    }
    inherited.sort();
    inherited
}

impl Auth {
    /// Builds the table from a credential file, the environment, or neither.
    ///
    /// Refuses both sources at once rather than silently preferring one.
    pub fn load(config: &crate::config::AuthConfig) -> anyhow::Result<Self> {
        let from_env = std::env::var_os(SECRET_ENV);
        let has_file = !config.file.as_os_str().is_empty();
        let default_roles = Roles::parse_default(config.default_roles.as_deref())?;

        let mut table = match (has_file, from_env) {
            (true, Some(_)) => bail!(
                "both auth.file and {SECRET_ENV} are set; use one or the other so it is \
                 unambiguous which credentials the server is running with"
            ),
            (true, None) => load_file(&config.file)?,
            (false, Some(secret)) => {
                let secret = secret.as_encoded_bytes();
                ensure!(
                    secret.len() >= MIN_SECRET_LEN,
                    "{SECRET_ENV} is {} bytes; the minimum is {MIN_SECRET_LEN}. A fast unsalted \
                     hash is the right storage for a high-entropy token and the wrong one for a \
                     guessable string — see docs/auth.md §3.2",
                    secret.len()
                );
                let mut table = HashMap::with_capacity(1);
                table.insert(
                    DEFAULT_NAME.as_bytes().into(),
                    Credential {
                        mechanism: Mechanism::Plain,
                        subject: None,
                        value: sha256(secret),
                        // A bare row like any other, so it takes
                        // `auth.default_roles` too. This is the only way the
                        // single-secret deployment can restrict its one
                        // identity, and it costs a special case rather than
                        // adding one.
                        roles: Roles::UNRESTRICTED,
                        inherited: true,
                    },
                );
                table
            }
            (false, None) => HashMap::new(),
        };

        let inherited = resolve_defaults(&mut table, default_roles);
        if !inherited.is_empty() {
            if default_roles.is_empty() {
                // A row that authenticates and is then refused everything is
                // confusing to debug from the client side, so the rows are
                // named here rather than left to be discovered one denial at a
                // time. This is what `auth.require_roles` was for, without the
                // boot failure — see docs/roles-proposal.md §15.
                warn!(
                    rows = %inherited.join(", "),
                    "auth.default_roles is empty and these credentials have no `roles=`; \
                     they can authenticate and will then be refused every command"
                );
            } else {
                info!(
                    rows = inherited.len(),
                    roles = %default_roles,
                    "credentials without `roles=` inherit auth.default_roles"
                );
            }
        }

        // Enforcement with an empty table locks everyone out, including peers.
        // Refusing to start is the only failure mode an operator cannot miss.
        ensure!(
            !config.required || !table.is_empty(),
            "auth.required is set but no credentials are configured; set auth.file or \
             {SECRET_ENV}, or the server would refuse every client including its own peers"
        );

        Ok(Self {
            required: config.required,
            table,
        })
    }

    /// A table with one generated-looking credential, for tests.
    #[cfg(test)]
    pub fn for_test(required: bool, name: &str, secret: &[u8]) -> Self {
        Self::for_test_with_roles(required, name, secret, Roles::UNRESTRICTED)
    }

    /// As [`Auth::for_test`], for the role gates.
    #[cfg(test)]
    pub fn for_test_with_roles(required: bool, name: &str, secret: &[u8], roles: Roles) -> Self {
        let mut table = HashMap::new();
        table.insert(
            name.as_bytes().into(),
            Credential {
                mechanism: Mechanism::Plain,
                value: sha256(secret),
                subject: None,
                roles,
                inherited: false,
            },
        );
        Self { required, table }
    }

    #[inline]
    pub fn required(&self) -> bool {
        self.required
    }

    /// Whether any credential exists, regardless of enforcement.
    ///
    /// This is what separates "authenticate and retry" from "there is nothing
    /// here to authenticate against", which Redis reports differently and a
    /// client must never have confused.
    #[inline]
    pub fn configured(&self) -> bool {
        !self.table.is_empty()
    }

    /// Finds the identity whose `mtls:` subject `matches` accepts.
    ///
    /// The predicate is supplied by the caller rather than evaluated here, and
    /// that is the seam: matching a name against a certificate is X.509 work —
    /// SAN entries, wildcard rules, encodings — and this module has no
    /// business knowing any of it. `crate::tls` hands in a closure backed by
    /// `webpki`; this walks the rows.
    ///
    /// Linear in the number of `mtls:` rows, once per connection. The table is
    /// a handful of services, and each candidate costs a certificate parse, so
    /// an index would trade a real cost for an imaginary one.
    pub fn identity_for_certificate(&self, matches: impl Fn(&str) -> bool) -> Option<Identity> {
        for (name, credential) in &self.table {
            let Some(subject) = &credential.subject else {
                continue;
            };
            if matches(subject) {
                return Some(Identity {
                    name: String::from_utf8_lossy(name)
                        .into_owned()
                        .into_boxed_str()
                        .into(),
                    roles: credential.roles,
                });
            }
        }
        None
    }

    /// The roles a named row carries, for the startup checks that have to look
    /// one up without authenticating as it — see `crate::check_peer_role`.
    ///
    /// `None` when the row does not exist, which the caller reports differently
    /// from a row that exists and lacks a role.
    pub fn roles_of(&self, name: &[u8]) -> Option<Roles> {
        self.table.get(name).map(|credential| credential.roles)
    }

    /// Verifies a `PLAIN` attempt. `None` on any failure, deliberately without
    /// saying which — an error that distinguishes an unknown name from a bad
    /// secret confirms which names exist.
    pub fn verify(&self, name: &[u8], secret: &[u8]) -> Option<Identity> {
        let credential = self.table.get(name)?;
        if credential.subject.is_some() {
            // An `mtls:` row is satisfied by a handshake, never by a password.
            // Its subject is public, so without this a client could
            // authenticate as that identity by typing its own certificate's
            // name — the same trap `hmac-sha256-key` rows are guarded against
            // below, for the same reason.
            return None;
        }
        if credential.mechanism != Mechanism::Plain {
            // The stored value is a raw HMAC key. Accepting it here would make
            // it a password.
            return None;
        }

        let presented = sha256(secret);
        // Digests are fixed-length, so this leaks nothing through its duration.
        // Comparing the secrets directly would leak the length of the shared
        // prefix, which on a LAN is a real attack rather than a theoretical one.
        if presented.ct_eq(&credential.value).into() {
            Some(Identity {
                name: String::from_utf8_lossy(name)
                    .into_owned()
                    .into_boxed_str()
                    .into(),
                roles: credential.roles,
            })
        } else {
            None
        }
    }
}

/// The live table plus the pre-auth budget, shared by every connection.
///
/// `RwLock<Arc<..>>` rather than a lock-free swap: the table itself is only
/// reached for an actual verification, which happens once per connection, so
/// the lock is not on any path worth optimising and it saves a dependency.
///
/// Whether authentication is *enforced* is a different question, and one every
/// command asks — the gates in `dispatch` and `resp` check it before each one.
/// It is mirrored into an atomic beside the lock so that asking costs a relaxed
/// load rather than a lock acquisition and an `Arc` refcount round trip on a
/// cacheline every connection thread shares. With enforcement off the
/// `is_authenticated()` short-circuit in front of those gates never fires, so
/// the mirror is what keeps the default configuration off the lock entirely.
pub struct AuthState {
    current: RwLock<Arc<Auth>>,
    /// Mirrors `current.required()`. Written only under the write lock, so it
    /// cannot disagree with the table for longer than a reload takes.
    required: AtomicBool,
    pub limits: Limits,
}

/// Bounds on what an unauthenticated connection may consume.
///
/// An unauthenticated connection is the one thing on this server a stranger can
/// create, so it gets a budget rather than the ordinary limits.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// How long a connection may stay unauthenticated before it is dropped.
    pub timeout: std::time::Duration,
    /// Failed attempts on one connection before it is closed. Bounds guessing
    /// without a cross-connection lockout, which would turn a guessing attempt
    /// into a denial of service against the legitimate holder.
    pub max_attempts: u32,
    /// Concurrent unauthenticated connections. Without it the pre-auth budget
    /// is the whole connection budget.
    pub max_connections: usize,
}

impl AuthState {
    pub fn new(auth: Auth, limits: Limits) -> Self {
        Self {
            required: AtomicBool::new(auth.required()),
            current: RwLock::new(Arc::new(auth)),
            limits,
        }
    }

    /// Whether this server enforces authentication.
    ///
    /// The per-command gate, and the reason it does not go through
    /// [`Self::current`] — see the note on the struct.
    #[inline]
    pub fn required(&self) -> bool {
        self.required.load(Ordering::Relaxed)
    }

    /// The table as it stands. Cloned out so a reload cannot swap underneath a
    /// verification in progress.
    pub fn current(&self) -> Arc<Auth> {
        Arc::clone(&self.current.read().expect("auth table lock poisoned"))
    }

    /// Swaps in a freshly loaded table.
    ///
    /// Connections that already authenticated keep the identity they
    /// authenticated with; only new attempts see the new table. That is the
    /// whole of the rotation story — add the new credential, roll the clients,
    /// remove the old one — and it is why there is no runtime mutation command.
    pub fn replace(&self, auth: Auth) {
        let mut current = self.current.write().expect("auth table lock poisoned");
        // Both under the write lock, so the mirror and the table it describes
        // are swapped together. A gate reading the mirror without the lock can
        // still catch the instant between them, which is the same window a
        // reload already has between one `current()` and the next — a command
        // in flight is judged by one side or the other, never by half of each.
        self.required.store(auth.required(), Ordering::Relaxed);
        *current = Arc::new(auth);
    }

    /// **The one decision behind every gate, in every dialect.**
    ///
    /// `None` means the connection may proceed. The two refusals are kept
    /// apart because they are different problems with different fixes and
    /// different counters — "nobody rolled their credential" against
    /// "someone's role set is wrong" — and because Redis words them
    /// differently on the wire (`NOAUTH` against `NOPERM`).
    ///
    /// This used to be written out at five call sites: the memcached batch
    /// gate, the memcached executor, the VCP header gate, the RESP pair, and
    /// the awaited-write check in `conn`. All five agreed, which was lucky
    /// rather than enforced — each one independently combined
    /// `is_authenticated`, `required()`, and the identity's roles, and a new
    /// bypass path meant finding all five. Now they differ only in what they
    /// exempt beforehand and how they word the answer, which is the part that
    /// genuinely is per-dialect.
    ///
    /// **Roles are checked whether or not `auth.required` is set.** An identity
    /// carries its roles from the moment it is established, which is what lets
    /// a client rolled during auth.md §15's step 2 experience its role set
    /// before enforcement is turned on.
    #[inline]
    pub fn refusal(&self, conn: &ConnAuth, needed: Roles) -> Option<Refused> {
        let Some(identity) = conn.identity() else {
            // Nothing enforced and nobody authenticated: there is no identity
            // to restrict, because the server is not defending anything.
            return self.required().then_some(Refused::Unauthenticated);
        };
        let roles = identity.roles();
        if roles.allows(needed) {
            return None;
        }
        Some(Refused::Missing(roles.missing(needed)))
    }

    /// Whether a connection may run something needing `needed`.
    ///
    /// [`Self::refusal`] without the reason, for the gates that only have to
    /// pick a code path rather than word an answer.
    #[inline]
    pub fn allows(&self, conn: &ConnAuth, needed: Roles) -> bool {
        self.refusal(conn, needed).is_none()
    }
}

/// Why a connection may not run a command.
///
/// Both answer `UNAUTHORIZED` on the VCP wire — auth.md §6.2 widened that
/// status to "refused by policy", which covers both — so this exists to keep
/// the *counters* and the log apart, not the status. An alert that cannot tell
/// the two apart is an alert that gets muted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// Has presented no credential, and this server requires one.
    Unauthenticated,
    /// Authenticated, but the identity is short these roles.
    Missing(Roles),
}

/// The roles a command needs, and the authority every gate defers to.
///
/// **Exhaustive on purpose.** A new [`vash_core::Command`] variant fails to
/// compile until it is classified here — the same discipline
/// `Command::inline_safe` documents, and for a sharper reason. Falling through
/// to the wrong answer there stalls a runtime worker; falling through here
/// hands out authority.
///
/// Lives in the server rather than in `vash-core` so the storage tier still
/// never learns that authorization exists. Exhaustiveness works across the
/// crate boundary regardless.
pub fn roles_for_command(command: &vash_core::Command<'_>) -> Roles {
    use vash_core::Command as C;

    match command {
        // Connection maintenance and introspection, with no storage effect.
        // `STATS` and `CLUSTER` disclose counters, settings and peer addresses
        // — the same disclosure `/stats` on the admin port makes — and gating
        // them would break client libraries that call `stats` on connect. See
        // docs/roles-proposal.md §2.
        C::Hello { .. } | C::Ping | C::Stats | C::Version | C::Quit | C::Cluster => Roles::NONE,

        // Returns a stored value, or stored metadata the caller did not supply.
        C::Get { .. } | C::GetMany(_) | C::Deadline { .. } | C::Deadlines(_) => Roles::READ,

        // `SET … GET` is the disclosing form: the flag exists to report the
        // value that was displaced, so it needs `read` as well. A plain `SET`
        // does not, and the existence side channel of a guarded write — a
        // refused `NX` — is not fenced off, because a write that cannot report
        // its own outcome is not a write.
        C::Set(set) => {
            if set.return_previous {
                Roles::READ_WRITE
            } else {
                Roles::WRITE
            }
        }

        C::SetMany { .. }
        | C::Delete { .. }
        | C::DeleteMany(_)
        | C::Touch { .. }
        | C::Expire { .. }
        | C::Append { .. } => Roles::WRITE,

        // The fused commands, which hand back a stored value *and* write.
        // Without `read` here, `roles=write` is a credential that reads every
        // key it can name — `INCRBY 0` is a `GET` with extra steps.
        C::GetAndTouch { .. } | C::Arithmetic(_) => Roles::READ_WRITE,

        // Reaches keys the caller cannot name, resolving them from a registry
        // it has no way to enumerate without `list`, and fans the result out to
        // every node. Not reachable from `write`, which is why it is not folded
        // into it.
        C::DeleteByTag { .. } => Roles::INVALIDATE,

        C::Flush => Roles::FLUSH,

        // A peer's whole authority. `TAG_SYNC` sets a generation to any value,
        // where `DELETE_BY_TAG` moves it by one.
        C::TagSync { .. } => Roles::PEER,

        C::ListKeys(_) | C::ListTags(_) => Roles::LIST,
    }
}

/// The roles a VCP opcode needs, decided from the frame header alone.
///
/// A second mapping, and it exists because the VCP gate runs *before* the body
/// is decoded — which is what keeps a refused command from driving a body
/// parser, exactly as the pre-auth gate does. The two are kept from drifting by
/// `the_opcode_and_command_mappings_agree`, not by hoping.
pub fn roles_for_opcode(opcode: vash_proto::vcp::Opcode) -> Roles {
    use vash_proto::vcp::Opcode as O;

    match opcode {
        O::Hello | O::Ping | O::Auth | O::Stats | O::Cluster => Roles::NONE,
        O::Get | O::GetMany => Roles::READ,
        O::Set | O::SetMany | O::Delete | O::DeleteMany | O::Touch => Roles::WRITE,
        // VCP's arithmetic answers with the resulting value, like every other
        // dialect's.
        O::Arithmetic => Roles::READ_WRITE,
        O::DeleteByTag => Roles::INVALIDATE,
        O::Flush => Roles::FLUSH,
        O::TagSync => Roles::PEER,
        O::ListKeys | O::ListTags => Roles::LIST,
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// Reads and parses a credential file.
///
/// Every failure is fatal and names the line. **A credential file must never
/// half-load**: a skipped row is a server accepting a different set of
/// identities than its operator believes, in whichever direction.
fn load_file(path: &Path) -> anyhow::Result<HashMap<Box<[u8]>, Credential>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading the credential file {}", path.display()))?;
    check_permissions(path)?;
    parse(&text).with_context(|| format!("in the credential file {}", path.display()))
}

/// Refuses a credential file that is group- or world-readable, the way `ssh`
/// refuses a loose private key. A check the server can make is worth more than
/// a sentence in a manual.
#[cfg(unix)]
fn check_permissions(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path)
        .with_context(|| format!("reading permissions of {}", path.display()))?
        .permissions()
        .mode();
    ensure!(
        mode & 0o077 == 0,
        "the credential file {} is mode {:o}; it must not be readable by group or other. \
         Run: chmod 600 {}",
        path.display(),
        mode & 0o777,
        path.display()
    );
    Ok(())
}

/// Windows ACLs do not map onto the mode bits this check reads, and a wrong
/// answer either way is worse than none: refusing a correctly-locked file would
/// block startup, and passing a readable one would claim a check that did not
/// happen.
#[cfg(not(unix))]
fn check_permissions(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

/// Parses the credential file format documented in `docs/auth.md` §4.
///
/// ```text
/// <name>  <algorithm>:<value>  [key=value …]
/// ```
///
/// Whitespace-separated, one credential per line, `#` comments — the shape of
/// `~/.ssh/authorized_keys`, which is the same job. The algorithm is always
/// present: a bare value would have no way to say whether it is a digest or a
/// key, which is exactly the hole this format exists to close.
fn parse(text: &str) -> anyhow::Result<HashMap<Box<[u8]>, Credential>> {
    let mut table: HashMap<Box<[u8]>, Credential> = HashMap::new();

    for (index, raw) in text.lines().enumerate() {
        let line_number = index + 1;
        let line = raw.trim();
        // `#` is a comment marker and nothing else. It never appears inside a
        // field, which is why it is not also a delimiter.
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        parse_line(line, &mut table).with_context(|| format!("on line {line_number}: {line:?}"))?;
    }

    Ok(table)
}

fn parse_line(line: &str, table: &mut HashMap<Box<[u8]>, Credential>) -> anyhow::Result<()> {
    let mut fields = line.split_whitespace();
    let name = fields.next().expect("the line is not blank");
    let credential = fields
        .next()
        .context("expected `<name> <algorithm>:<value>`, but the line has only one field")?;

    // `roles=` is the first field to occupy the slot this loader reserved.
    // Everything else stays refused rather than ignored, which is what stops a
    // file written for a newer server loading silently into an older one with
    // the field dropped — and it is why roles need no capability bit.
    let mut roles = None;
    for field in fields {
        let Some(list) = field.strip_prefix("roles=") else {
            bail!(
                "unknown trailing field {field:?}. Defined: `roles=<list>`. \
                 Others are reserved"
            );
        };
        ensure!(roles.is_none(), "`roles=` is given twice");
        roles = Some(Roles::parse_list(list)?);
    }

    validate_name(name)?;
    let mut credential = parse_credential(credential)?;
    if let Some(roles) = roles {
        credential.roles = roles;
        credential.inherited = false;
    }

    // Last-one-wins on a duplicate would make which credential is live depend
    // on file order, which is not a thing anyone should have to know.
    if table.insert(name.as_bytes().into(), credential).is_some() {
        bail!("duplicate name {name:?}");
    }
    Ok(())
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        (1..=MAX_NAME_LEN).contains(&name.len()),
        "name is {} bytes; it must be 1 to {MAX_NAME_LEN}",
        name.len()
    );
    // No colon and no whitespace, so both splits are unambiguous.
    if let Some(bad) = name
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && !matches!(c, '_' | '.' | '-'))
    {
        bail!("name contains {bad:?}; only [A-Za-z0-9_.-] is allowed");
    }
    Ok(())
}

fn parse_credential(field: &str) -> anyhow::Result<Credential> {
    // Modular crypt format is the right encoding for a KDF row, and `$5$`
    // already means SHA-256-crypt — a salted, 5000-round KDF this design
    // rejected. Writing a bare digest behind that label would silently disagree
    // with libcrypt, PAM, and anything pasted out of `mkpasswd`, so the whole
    // namespace is reserved and refused loudly.
    if field.starts_with('$') {
        bail!(
            "{field:?} looks like a modular crypt hash. That format is reserved for a real \
             key-derivation function, which this build does not implement — see docs/auth.md §4"
        );
    }

    let (algorithm, value) = field.split_once(':').with_context(|| {
        format!("{field:?} has no algorithm. Expected `sha256:<64 hex>` — there is no default")
    })?;

    match algorithm {
        "sha256" => Ok(Credential {
            mechanism: Mechanism::Plain,
            value: parse_hex32(value)?,
            subject: None,
            roles: Roles::UNRESTRICTED,
            inherited: true,
        }),

        // A certificate subject rather than a secret. The row says "whoever
        // presents a certificate for this name, issued by tls.client_ca, is
        // this identity" — so there is nothing here to keep secret and nothing
        // to hash.
        "mtls" => {
            ensure!(
                !value.is_empty(),
                "`mtls:` needs a certificate subject, e.g. `mtls:billing.svc.internal`"
            );
            ensure!(
                !value.contains(char::is_whitespace),
                "an `mtls:` subject cannot contain whitespace: {value:?}"
            );
            Ok(Credential {
                mechanism: Mechanism::Plain,
                value: [0u8; 32],
                subject: Some(value.into()),
                roles: Roles::UNRESTRICTED,
                inherited: true,
            })
        }

        // The format defines it, the mechanism that would consume it does not
        // exist yet, and a row that can never authenticate is worse than one
        // that is refused: it reads as a working credential.
        "hmac-sha256-key" => {
            parse_hex32(value)?;
            bail!(
                "`hmac-sha256-key` needs the HMAC_SHA256 mechanism, which is specified but not \
                 built in this version — see docs/auth.md §6.3. Use `sha256:` for now"
            )
        }

        other => bail!(
            "unknown algorithm {other:?}. Known: `sha256`. \
             A misspelt algorithm is refused rather than skipped, so the table never half-loads"
        ),
    }
}

fn parse_hex32(value: &str) -> anyhow::Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "expected 64 hex characters, found {}",
        value.len()
    );
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let pair = &value[i * 2..i * 2 + 2];
        // Lowercase only: one canonical rendering means a file diffs cleanly
        // and two rows for the same digest cannot look different.
        ensure!(
            !pair.bytes().any(|b| b.is_ascii_uppercase()),
            "hex must be lowercase"
        );
        *byte = u8::from_str_radix(pair, 16).with_context(|| format!("{pair:?} is not hex"))?;
    }
    Ok(out)
}

/// Renders a credential line for a freshly generated secret.
///
/// Returns `(secret, line)`. Generating rather than accepting a secret is what
/// keeps the fast-hash argument in this module's header true in practice.
pub fn generate(name: &str, roles: Option<&str>) -> anyhow::Result<(String, String)> {
    validate_name(name)?;
    // Validated here rather than left for the server to reject on the next
    // start: a generated row is pasted into a file unread, and a typo in it
    // would surface as a boot failure at the least convenient moment.
    let roles = roles.map(Roles::parse_list).transpose()?;

    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).context("reading from the system random source")?;

    let secret = hex(&secret);
    let mut line = format!("{name}  sha256:{}", hex(&sha256(secret.as_bytes())));
    if let Some(roles) = roles {
        line.push_str("  roles=");
        line.push_str(&roles.to_names());
    }
    Ok((secret, line))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `sha256("hunter2hunter2hu")`, long enough to pass the length floor.
    const SECRET: &str = "hunter2hunter2hu";

    fn digest_line(name: &str, secret: &str) -> String {
        format!("{name}  sha256:{}", hex(&sha256(secret.as_bytes())))
    }

    fn state(required: bool, roles: Roles) -> AuthState {
        AuthState::new(
            Auth::for_test_with_roles(required, "default", SECRET.as_bytes(), roles),
            Limits {
                timeout: std::time::Duration::from_secs(1),
                max_attempts: 3,
                max_connections: 1,
            },
        )
    }

    fn connected(state: &AuthState) -> ConnAuth {
        let mut conn = ConnAuth::default();
        conn.succeed(
            state
                .current()
                .verify(b"default", SECRET.as_bytes())
                .expect("the test credential verifies"),
        );
        conn
    }

    /// **The whole decision table of the one gate every dialect defers to.**
    ///
    /// This used to be written out at five call sites — the memcached batch
    /// gate, the memcached executor, the VCP header gate, the RESP pair and
    /// the awaited-write check in `conn` — each independently combining
    /// `is_authenticated`, `required()` and the identity's roles. They agreed,
    /// but nothing made them: a sixth path could have got any cell of this
    /// wrong on its own. Pinning the table here is what that consolidation
    /// bought.
    #[test]
    fn the_gate_answers_the_same_four_questions_for_every_dialect() {
        let anonymous = ConnAuth::default();

        // Nothing enforced and nobody authenticated: there is no identity to
        // restrict, because the server is not defending anything.
        let open = state(false, Roles::READ);
        assert_eq!(open.refusal(&anonymous, Roles::FLUSH), None);
        assert!(open.allows(&anonymous, Roles::FLUSH));

        // Enforced and nobody authenticated: refused, and for that reason
        // rather than for a role — the two carry different counters.
        let closed = state(true, Roles::READ);
        assert_eq!(
            closed.refusal(&anonymous, Roles::READ),
            Some(Refused::Unauthenticated)
        );
        assert!(!closed.allows(&anonymous, Roles::NONE));

        // Authenticated and holding the role: through.
        let reader = connected(&closed);
        assert_eq!(closed.refusal(&reader, Roles::READ), None);

        // Authenticated and short of it: refused with what is missing, so the
        // denial log can name the role an operator should add.
        assert_eq!(
            closed.refusal(&reader, Roles::READ_WRITE),
            Some(Refused::Missing(Roles::WRITE))
        );
    }

    /// Roles bind whether or not enforcement is on.
    ///
    /// An identity carries its roles from the moment it is established, which
    /// is what lets a client rolled during auth.md §15's step 2 experience its
    /// role set before `auth.required` is turned on — the step that would
    /// otherwise break at 3am on the first `FLUSH`.
    #[test]
    fn an_identity_is_held_to_its_roles_before_enforcement_begins() {
        let open = state(false, Roles::READ);
        let reader = connected(&open);
        assert_eq!(
            open.refusal(&reader, Roles::FLUSH),
            Some(Refused::Missing(Roles::FLUSH))
        );
    }

    #[test]
    fn a_digest_row_verifies_its_secret() {
        let table = parse(&digest_line("default", SECRET)).unwrap();
        let auth = Auth {
            required: true,
            table,
        };

        let identity = auth.verify(b"default", SECRET.as_bytes()).unwrap();
        assert_eq!(identity.name(), "default");
        assert!(auth.verify(b"default", b"wrong").is_none());
        assert!(auth.verify(b"nobody", SECRET.as_bytes()).is_none());
    }

    #[test]
    fn comments_and_blank_lines_are_skipped() {
        let text = format!(
            "# a comment\n\n   \n{}\n   # indented comment\n{}\n",
            digest_line("one", SECRET),
            digest_line("two", SECRET)
        );
        assert_eq!(parse(&text).unwrap().len(), 2);
    }

    #[test]
    fn columns_may_be_aligned_with_any_run_of_whitespace() {
        let text = format!("padded\t\t  sha256:{}", hex(&sha256(SECRET.as_bytes())));
        assert!(parse(&text).unwrap().contains_key(b"padded".as_slice()));
    }

    /// The property that matters more than any individual message: a file never
    /// half-loads. Every one of these is a hard failure, not a skipped row.
    #[test]
    fn every_malformed_line_is_a_hard_error() {
        let digest = hex(&sha256(SECRET.as_bytes()));
        let cases: &[(&str, &str)] = &[
            ("lonely", "only one field"),
            ("name sha256", "no algorithm"),
            ("name deadbeef", "no algorithm"),
            ("name md5:0123", "unknown algorithm"),
            ("name sha256:xyz", "not 64 characters"),
            ("name sha256:0123", "too short"),
            ("bad name here sha256:x", "space in the name"),
            ("name! sha256:x", "illegal name character"),
            ("", "handled as blank, never reaches here"),
        ];
        for (line, why) in cases {
            if line.is_empty() {
                continue;
            }
            assert!(
                parse(line).is_err(),
                "expected {line:?} to be refused ({why})"
            );
        }

        // Uppercase hex, the right length.
        assert!(parse(&format!("name sha256:{}", digest.to_uppercase())).is_err());
        // A trailing field nobody defined.
        assert!(parse(&format!("name sha256:{digest} role=peer")).is_err());
        // Two rows for one name.
        assert!(parse(&format!("n sha256:{digest}\nn sha256:{digest}")).is_err());
    }

    #[test]
    fn modular_crypt_is_refused_by_name() {
        // The message has to say *why*, or the next person tries $6$.
        let chain = format!("{:#}", parse("admin $5$rounds=5000$salt$hash").unwrap_err());
        assert!(chain.contains("modular crypt"), "{chain}");
    }

    #[test]
    fn an_hmac_key_row_is_refused_while_the_mechanism_is_unbuilt() {
        let line = format!("peer hmac-sha256-key:{}", hex(&[7u8; 32]));
        let error = format!("{:#}", parse(&line).unwrap_err());
        assert!(error.contains("not built"), "{error}");
    }

    /// An `hmac-sha256-key` row holds the secret itself. If it could satisfy a
    /// `PLAIN` attempt, that stored key would be a working password — which is
    /// the whole reason rows are bound to a mechanism.
    #[test]
    fn a_key_row_can_never_satisfy_plain() {
        let key = [7u8; 32];
        let mut table = HashMap::new();
        table.insert(
            b"peer".to_vec().into_boxed_slice(),
            Credential {
                mechanism: Mechanism::HmacSha256,
                value: key,
                subject: None,
                roles: Roles::UNRESTRICTED,
                inherited: true,
            },
        );
        let auth = Auth {
            required: true,
            table,
        };

        assert!(auth.verify(b"peer", &key).is_none());
        assert!(auth.verify(b"peer", hex(&key).as_bytes()).is_none());
    }

    #[test]
    fn error_messages_name_the_line() {
        let good = digest_line("ok", SECRET);
        let error = format!("{:#}", parse(&format!("{good}\nbroken\n")).unwrap_err());
        assert!(error.contains("line 2"), "{error}");
    }

    #[test]
    fn a_generated_line_verifies_its_own_secret() {
        let (secret, line) = generate("billing-api", None).unwrap();
        assert!(secret.len() >= MIN_SECRET_LEN);

        let auth = Auth {
            required: true,
            table: parse(&line).unwrap(),
        };
        assert!(auth.verify(b"billing-api", secret.as_bytes()).is_some());
    }

    #[test]
    fn a_row_carries_the_roles_it_names() {
        let table = parse(&format!("{}  roles=read,list", digest_line("dash", SECRET))).unwrap();
        let roles = table[b"dash".as_slice()].roles;
        assert!(roles.allows(Roles::READ));
        assert!(roles.allows(Roles::LIST));
        assert!(!roles.allows(Roles::WRITE));
        assert!(!roles.allows(Roles::FLUSH));
        // A row that named its roles does not take the default.
        assert!(!table[b"dash".as_slice()].inherited);
    }

    #[test]
    fn a_bare_row_takes_the_default() {
        let mut table = parse(&digest_line("legacy", SECRET)).unwrap();
        assert!(table[b"legacy".as_slice()].inherited);

        let inherited = resolve_defaults(&mut table, Roles::READ);
        assert_eq!(inherited, vec!["legacy".to_string()]);
        assert_eq!(table[b"legacy".as_slice()].roles, Roles::READ);
    }

    /// The compatibility promise, and the reason the unset default is every bit
    /// rather than the six this build defines: a file written before roles
    /// existed must not lose authority when the server gains a role.
    #[test]
    fn the_unset_default_covers_roles_that_do_not_exist_yet() {
        let future = Roles::from_bits(0b0100_0000);

        let unset = Roles::parse_default(None).unwrap();
        assert!(unset.allows(future));

        // An operator who enumerated today's six has said "exactly these".
        let names: Vec<String> = Roles::NAMED.iter().map(|(_, n)| (*n).to_string()).collect();
        let spelled_out = Roles::parse_default(Some(&names)).unwrap();
        assert!(!spelled_out.allows(future));
    }

    #[test]
    fn an_empty_default_is_distinct_from_an_absent_one() {
        assert_eq!(Roles::parse_default(Some(&[])).unwrap(), Roles::NONE);
        assert_eq!(Roles::parse_default(None).unwrap(), Roles::UNRESTRICTED);
    }

    #[test]
    fn every_malformed_role_list_is_a_hard_error() {
        let digest = hex(&sha256(SECRET.as_bytes()));
        let cases: &[(&str, &str)] = &[
            (
                "n sha256:{d} roles=",
                "an empty list reads like the field being absent",
            ),
            ("n sha256:{d} roles=admin", "no such role"),
            (
                "n sha256:{d} roles=read,admin",
                "one bad name poisons the row",
            ),
            (
                "n sha256:{d} roles=all",
                "no `all` keyword: it would grant roles that do not exist yet",
            ),
            (
                "n sha256:{d} roles=read,read",
                "a duplicate is a typo, not an intent",
            ),
            (
                "n sha256:{d} roles=read, write",
                "the format is whitespace-delimited",
            ),
            ("n sha256:{d} roles=read roles=write", "two role fields"),
            ("n sha256:{d} role=read", "the singular is not the field"),
            ("n sha256:{d} scope=read", "still refused, still reserved"),
        ];

        for (line, why) in cases {
            let line = line.replace("{d}", &digest);
            assert!(
                parse(&line).is_err(),
                "expected {line:?} to be refused ({why})"
            );
        }
    }

    /// The three commands that hand back a stored value while writing. Without
    /// `read` here, `roles=write` is a credential that reads every key it can
    /// name.
    #[test]
    fn the_fused_commands_need_read_as_well_as_write() {
        use vash_core::Command;

        let key = vash_core::Key::new(b"k").unwrap();
        let fused = [
            Command::GetAndTouch {
                keys: key.into(),
                ttl_secs: 60,
            },
            Command::Arithmetic(vash_core::Arithmetic::counter(key, 1, false)),
            Command::Set(vash_core::Set {
                return_previous: true,
                ..vash_core::Set::plain(key, b"v", 0)
            }),
        ];

        for command in fused {
            let needed = roles_for_command(&command);
            assert!(
                !Roles::WRITE.allows(needed),
                "{command:?} must not be reachable with `write` alone"
            );
            assert!(Roles::READ_WRITE.allows(needed), "{command:?}");
        }

        // The plain form of the same write does not need `read`.
        let plain = Command::Set(vash_core::Set::plain(key, b"v", 0));
        assert!(Roles::WRITE.allows(roles_for_command(&plain)));
    }

    /// The VCP gate decides from the frame header, before the body is decoded,
    /// so it reads a different mapping from every other gate. This is what
    /// stops the two drifting.
    #[test]
    fn the_opcode_and_command_mappings_agree() {
        use vash_core::Command;
        use vash_proto::vcp::Opcode;

        let key = vash_core::Key::new(b"k").unwrap();
        let equivalents: Vec<(Opcode, Command<'_>)> = vec![
            (
                Opcode::Hello,
                Command::Hello {
                    protocol_version: 1,
                },
            ),
            (Opcode::Ping, Command::Ping),
            (Opcode::Cluster, Command::Cluster),
            (Opcode::Get, Command::Get { key }),
            (Opcode::GetMany, Command::GetMany(key.into())),
            (
                Opcode::Set,
                Command::Set(vash_core::Set::plain(key, b"v", 0)),
            ),
            (
                Opcode::SetMany,
                Command::SetMany {
                    sets: vec![vash_core::Set::plain(key, b"v", 0)],
                    guard: vash_core::BatchGuard::Always,
                },
            ),
            (Opcode::Delete, Command::Delete { key }),
            (Opcode::DeleteMany, Command::DeleteMany(vec![key])),
            (Opcode::Touch, Command::Touch { key, ttl_secs: 60 }),
            (
                Opcode::Arithmetic,
                Command::Arithmetic(vash_core::Arithmetic::counter(key, 1, false)),
            ),
            (Opcode::DeleteByTag, Command::DeleteByTag { tag: b"t" }),
            (Opcode::Flush, Command::Flush),
            (
                Opcode::TagSync,
                Command::TagSync {
                    full: true,
                    entries: Vec::new(),
                },
            ),
            (
                Opcode::ListKeys,
                Command::ListKeys(vash_core::ListRequest {
                    limit: 10,
                    cursor: b"",
                    pattern: b"",
                }),
            ),
            (
                Opcode::ListTags,
                Command::ListTags(vash_core::ListRequest {
                    limit: 10,
                    cursor: b"",
                    pattern: b"",
                }),
            ),
        ];

        for (opcode, command) in &equivalents {
            assert_eq!(
                roles_for_opcode(*opcode),
                roles_for_command(command),
                "{opcode:?} and the command it decodes to disagree"
            );
        }
    }

    /// Every role gates something, and every command is reachable from some
    /// role. A role nothing needs is one an operator can withhold with no
    /// effect, which is worse than not having it at all.
    #[test]
    fn every_role_gates_something() {
        use vash_core::Command;

        let key = vash_core::Key::new(b"k").unwrap();
        let commands = [
            Command::Get { key },
            Command::Set(vash_core::Set::plain(key, b"v", 0)),
            Command::DeleteByTag { tag: b"t" },
            Command::ListKeys(vash_core::ListRequest {
                limit: 10,
                cursor: b"",
                pattern: b"",
            }),
            Command::Flush,
            Command::TagSync {
                full: true,
                entries: Vec::new(),
            },
        ];

        let reachable = commands.iter().fold(Roles::NONE, |acc, command| {
            acc.union(roles_for_command(command))
        });

        for (_, name, role) in Roles::INDEXED {
            assert!(reachable.allows(role), "no command needs `{name}`");
        }
    }

    #[test]
    fn a_generated_line_can_carry_roles() {
        let (secret, line) = generate("worker", Some("invalidate")).unwrap();
        let mut table = parse(&line).unwrap();
        resolve_defaults(&mut table, Roles::NONE);

        let auth = Auth {
            required: true,
            table,
        };
        let identity = auth.verify(b"worker", secret.as_bytes()).unwrap();
        assert_eq!(identity.roles(), Roles::INVALIDATE);

        // A typo is caught where the line is written, not at the next boot.
        assert!(generate("worker", Some("invalidat")).is_err());
    }

    #[test]
    fn two_generated_secrets_differ() {
        let (a, _) = generate("x", None).unwrap();
        let (b, _) = generate("x", None).unwrap();
        assert_ne!(a, b);
    }
}
