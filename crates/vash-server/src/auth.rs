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
pub struct Identity(Arc<str>);

impl Identity {
    pub fn name(&self) -> &str {
        &self.0
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
#[derive(Debug, Clone, Copy)]
struct Credential {
    mechanism: Mechanism,
    /// A digest for [`Mechanism::Plain`]; the key itself for
    /// [`Mechanism::HmacSha256`].
    value: [u8; 32],
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

impl Auth {
    /// Builds the table from a credential file, the environment, or neither.
    ///
    /// Refuses both sources at once rather than silently preferring one.
    pub fn load(config: &crate::config::AuthConfig) -> anyhow::Result<Self> {
        let from_env = std::env::var_os(SECRET_ENV);
        let has_file = !config.file.as_os_str().is_empty();

        let table = match (has_file, from_env) {
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
                        value: sha256(secret),
                    },
                );
                table
            }
            (false, None) => HashMap::new(),
        };

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
        let mut table = HashMap::new();
        table.insert(
            name.as_bytes().into(),
            Credential {
                mechanism: Mechanism::Plain,
                value: sha256(secret),
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

    /// Verifies a `PLAIN` attempt. `None` on any failure, deliberately without
    /// saying which — an error that distinguishes an unknown name from a bad
    /// secret confirms which names exist.
    pub fn verify(&self, name: &[u8], secret: &[u8]) -> Option<Identity> {
        let credential = self.table.get(name)?;
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
            Some(Identity(
                String::from_utf8_lossy(name)
                    .into_owned()
                    .into_boxed_str()
                    .into(),
            ))
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

    // Reserved rather than ignored: the day `role=` exists, a file written for
    // a newer server must not load silently into an older one with the field
    // dropped.
    if let Some(extra) = fields.next() {
        bail!(
            "unknown trailing field {extra:?}. No `key=value` fields are defined yet; \
             they are reserved"
        );
    }

    validate_name(name)?;
    let credential = parse_credential(credential)?;

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
        }),

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
pub fn generate(name: &str) -> anyhow::Result<(String, String)> {
    validate_name(name)?;

    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).context("reading from the system random source")?;

    let secret = hex(&secret);
    let line = format!("{name}  sha256:{}", hex(&sha256(secret.as_bytes())));
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
        let (secret, line) = generate("billing-api").unwrap();
        assert!(secret.len() >= MIN_SECRET_LEN);

        let auth = Auth {
            required: true,
            table: parse(&line).unwrap(),
        };
        assert!(auth.verify(b"billing-api", secret.as_bytes()).is_some());
    }

    #[test]
    fn two_generated_secrets_differ() {
        let (a, _) = generate("x").unwrap();
        let (b, _) = generate("x").unwrap();
        assert_ne!(a, b);
    }
}
