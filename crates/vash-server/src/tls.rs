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

use anyhow::{Context, bail};
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
