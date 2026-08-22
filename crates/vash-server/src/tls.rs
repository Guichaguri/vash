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

use crate::config::TlsConfig;

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
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .context("building the TLS configuration")?
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .context("the certificate and private key do not match")?;

    Ok(TlsAcceptor::from(Arc::new(server)))
}
