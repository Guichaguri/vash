//! TLS termination, compiled in only under the `tls` feature.
//!
//! Phase 1 of [`docs/tls-proposal.md`], in the shape Phase 0 measured: server
//! certificates only, TLS 1.3 only, `ring` as the provider. Client
//! certificates and the identity mapping onto the credential table are phase 3
//! and deliberately absent — a half-built mTLS path that authenticates nobody
//! would be worse than none.
//!
//! Everything here happens once, at startup. The per-connection cost is the
//! handshake in the accept loop and the record framing under
//! [`crate::conn::handle`], neither of which is in this file.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use rustls::pki_types::UnixTime;
use tokio_rustls::TlsAcceptor;

use crate::auth::{Auth, Identity};
use crate::config::{ClientAuth, TlsConfig};

/// Reads the certificate chain and key, and builds the acceptor every TLS
/// connection is handed to.
///
/// Called before the listener binds, so a certificate that will not parse
/// stops startup rather than surfacing as a handshake failure on the first
/// client — the same rule `Auth::load` follows for the credential file.
pub fn acceptor(config: &TlsConfig) -> anyhow::Result<TlsAcceptor> {
    let chain = {
        let file = File::open(&config.cert)
            .with_context(|| format!("opening the certificate {}", config.cert.display()))?;
        rustls_pemfile::certs(&mut BufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("reading the certificate {}", config.cert.display()))?
    };
    if chain.is_empty() {
        bail!(
            "{} contains no certificate. Expected PEM, leaf first",
            config.cert.display()
        );
    }

    let key = {
        let file = File::open(&config.key)
            .with_context(|| format!("opening the private key {}", config.key.display()))?;
        rustls_pemfile::private_key(&mut BufReader::new(file))
            .with_context(|| format!("reading the private key {}", config.key.display()))?
            .with_context(|| format!("{} contains no private key", config.key.display()))?
    };

    // TLS 1.3 only, and no cipher-suite configuration: rustls has no bad
    // suites to turn off, so a knob here could only ever narrow a set that is
    // already safe. `min_version = "1.2"` is proposal §4.3 and not built.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("building the TLS configuration")?;

    let builder = match config.client_auth {
        ClientAuth::None => builder.with_no_client_auth(),
        ClientAuth::Required => {
            let mut roots = rustls::RootCertStore::empty();
            let file = File::open(&config.client_ca)
                .with_context(|| format!("opening the client CA {}", config.client_ca.display()))?;
            let mut added = 0;
            for cert in rustls_pemfile::certs(&mut BufReader::new(file)) {
                roots.add(cert.with_context(|| {
                    format!("reading the client CA {}", config.client_ca.display())
                })?)?;
                added += 1;
            }
            if added == 0 {
                bail!(
                    "{} contains no certificate, so no client certificate could ever be                      verified against it",
                    config.client_ca.display()
                );
            }

            // Mandatory, not optional: rustls refuses the handshake outright
            // when a client presents nothing. A connection that reaches the
            // protocol has therefore already proved it holds a key the
            // configured CA vouched for — all that is left is deciding *who*
            // that is, which `identity_for` does.
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .context("building the client certificate verifier")?;
            builder.with_client_cert_verifier(verifier)
        }
    };

    let server = builder
        .with_single_cert(chain, key)
        .context("the certificate and private key do not match")?;

    Ok(TlsAcceptor::from(Arc::new(server)))
}

/// The acceptor in use, swappable while the server is running.
///
/// A whole acceptor behind a lock rather than a `ResolvesServerCert` reading a
/// swapped key: rebuilding is a file read and a parse, it happens on a signal
/// rather than per handshake, and this way *every* part of the configuration a
/// reload could change — the chain, the key, the client CA — is replaced
/// together instead of only the pieces a resolver can reach.
///
/// Read once per accepted connection, which is a read lock against a signal
/// that arrives approximately never. Existing connections are untouched: a
/// session holds the parameters it handshook with, so a reload cannot
/// interrupt traffic in flight.
#[derive(Clone)]
pub struct Reloadable {
    acceptor: Arc<std::sync::RwLock<TlsAcceptor>>,
}

impl Reloadable {
    pub fn new(acceptor: TlsAcceptor) -> Self {
        Self {
            acceptor: Arc::new(std::sync::RwLock::new(acceptor)),
        }
    }

    /// The acceptor to hand the next connection.
    pub fn current(&self) -> TlsAcceptor {
        // Cheap: `TlsAcceptor` is an `Arc` inside, so this clones a pointer
        // and drops the lock before the handshake starts.
        self.acceptor
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn replace(&self, acceptor: TlsAcceptor) {
        *self
            .acceptor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = acceptor;
    }
}

/// Reads the chain and reports when it expires, for the gauge.
///
/// Failure here is never fatal — a certificate the server is already serving
/// with is not made unusable by our inability to date it — so this logs and
/// reports `0` rather than refusing to start.
pub fn record_expiry(config: &TlsConfig, metrics: &crate::metrics::ServerMetrics) {
    let expiry = read_chain(&config.cert)
        .ok()
        .and_then(|chain| expiry(&chain));
    match expiry {
        Some(at) => {
            metrics.tls_cert_expires_at(at.as_secs());
            tracing::info!(
                unix_seconds = at.as_secs(),
                "the TLS certificate chain is valid until"
            );
        }
        None => {
            metrics.tls_cert_expires_at(0);
            tracing::warn!(
                cert = %config.cert.display(),
                "could not determine when the TLS certificate expires; it may already have.                  vash_tls_cert_expiry_timestamp_seconds reports 0"
            );
        }
    }
}

/// Resolves a verified client certificate to the identity that holds it.
///
/// The chain is already verified by the time this runs — `rustls` refused the
/// handshake otherwise — so this answers only "which row is this?". Matching a
/// name against a certificate is X.509 work, which is why it lives here rather
/// than in `auth`: `webpki` reads the Subject Alternative Names and applies
/// the wildcard rules, and the credential table supplies the candidates.
///
/// **Subject Alternative Names only.** The proposal offered `identity_from =
/// "cn"` as an alternative and it is not built: matching on the Common Name
/// was deprecated by RFC 6125 and abandoned by the browsers a decade ago,
/// every tool that issues certificates puts the name in a SAN, and supporting
/// it would mean adding an X.509 parser to read a field nothing should be
/// using. A certificate whose name is only in its CN does not authenticate
/// here, and that is the intended answer.
pub fn identity_for(
    auth: &Auth,
    certificate: &rustls::pki_types::CertificateDer<'_>,
) -> Option<Identity> {
    let parsed = webpki::EndEntityCert::try_from(certificate).ok()?;
    auth.identity_for_certificate(|subject| {
        let Ok(name) = rustls::pki_types::ServerName::try_from(subject) else {
            // A row whose subject is not a name a certificate could carry.
            // Refusing it at load time would be better; refusing to match it
            // is what keeps that from being a security hole in the meantime.
            return false;
        };
        parsed.verify_is_valid_for_subject_name(&name).is_ok()
    })
}

/// Ten years. The upper bound of the search below, and a ceiling on what the
/// gauge will report: nothing issues a certificate longer than this, and a
/// deployment that somehow has one does not need an expiry alert.
const MAX_LIFETIME: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);

/// When this chain stops verifying, as a Unix timestamp.
///
/// `None` when it does not verify *now* — an expired certificate, one not yet
/// valid, or a chain that does not lead to its own last element. The caller
/// logs that; there is no sensible number for it.
///
/// # Why this is a search and not a field read
///
/// The obvious implementation reads `notAfter` off the leaf, and the obvious
/// way to do that is `x509-parser` — which would add eight crates to a binary
/// that exists to serve a cache, on the one code path that faces strangers,
/// to read a single field. `webpki` is already here verifying the chain, and
/// it takes the time as a parameter, so asking it *when* the answer changes
/// costs no dependency at all.
///
/// It also answers a slightly better question. `notAfter` on the leaf is not
/// when the deployment breaks if an intermediate expires first; the boundary
/// found here is the whole chain's, which is what an operator wants to be
/// alerted about.
///
/// Roughly thirty verifications, at a few tens of microseconds each, once at
/// startup and once per reload. Never on a request path.
pub fn expiry(chain: &[rustls::pki_types::CertificateDer<'static>]) -> Option<UnixTime> {
    let (leaf, intermediates) = chain.split_first()?;
    let parsed = webpki::EndEntityCert::try_from(leaf).ok()?;

    // The last certificate in the chain is the anchor. For a self-contained
    // chain that is the CA; for one that stops at an intermediate it is that
    // intermediate, and the boundary found is still the first expiry along the
    // path we can see.
    let anchor_der = chain.last()?;
    let anchor = webpki::anchor_from_trusted_cert(anchor_der).ok()?;
    let anchors = [anchor];
    // An anchor that is also the leaf must not appear as its own intermediate.
    let intermediates = match intermediates.is_empty() {
        true => &[][..],
        false => &intermediates[..intermediates.len() - 1],
    };

    let verifies_at = |at: UnixTime| {
        parsed
            .verify_for_usage(
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .all,
                &anchors,
                intermediates,
                at,
                webpki::KeyUsage::server_auth(),
                None,
                None,
            )
            .is_ok()
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    if !verifies_at(UnixTime::since_unix_epoch(now)) {
        return None;
    }

    // Invariant: `good` verifies and `bad` does not. Halve until they are a
    // second apart, and `good` is the last second the chain is usable.
    let mut good = now.as_secs();
    let mut bad = now.as_secs() + MAX_LIFETIME.as_secs();
    if verifies_at(UnixTime::since_unix_epoch(Duration::from_secs(bad))) {
        // Longer-lived than the ceiling. Report the ceiling rather than
        // searching further: the difference does not change any decision.
        return Some(UnixTime::since_unix_epoch(Duration::from_secs(bad)));
    }
    while bad - good > 1 {
        let middle = good + (bad - good) / 2;
        if verifies_at(UnixTime::since_unix_epoch(Duration::from_secs(middle))) {
            good = middle;
        } else {
            bad = middle;
        }
    }
    Some(UnixTime::since_unix_epoch(Duration::from_secs(good)))
}

/// Reads the certificate chain alone, for [`expiry`].
pub fn read_chain(
    path: &std::path::Path,
) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("reading {}", path.display()))
}
