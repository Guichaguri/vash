//! Issues a throwaway CA and server certificate, for trying TLS out.
//!
//! ```text
//! cargo run -p vash-server --example gen_cert -- ./certs
//! ```
//!
//! Writes `ca.pem`, `cert.pem` and `key.pem`, with SANs for `localhost`,
//! `host.docker.internal` and `127.0.0.1` — the three names something is
//! likely to dial this server by on a development machine, the middle one
//! being how a container reaches its host.
//!
//! **This is not how a deployment should get a certificate.** There is no
//! revocation, no rotation and no chain of trust to anything: the CA lives for
//! as long as it takes to run an interoperability test. A real deployment
//! issues from whatever already issues its internal certificates and reloads
//! them as they renew — see `docs/tls-proposal.md` §10.

use std::path::PathBuf;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, PKCS_ECDSA_P256_SHA256, SanType,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir: PathBuf = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "certs".into())
        .into();
    std::fs::create_dir_all(&dir)?;

    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let mut ca_params = CertificateParams::new(Vec::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "vash development CA");
    let ca = ca_params.clone().self_signed(&ca_key)?;

    // P-256 rather than RSA, and the reason is measured: the server signs once
    // per full handshake, 308µs against 804µs. See
    // `docs/benchmarks.md#what-tls-costs`.
    let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let mut leaf = CertificateParams::new(vec!["localhost".to_string()])?;
    leaf.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into()?),
        // How a container reaches the host it is running on, which is what an
        // interoperability test against `redis-cli` or a memcached client
        // needs.
        SanType::DnsName("host.docker.internal".try_into()?),
        SanType::IpAddress(std::net::IpAddr::from([127, 0, 0, 1])),
    ];
    leaf.distinguished_name
        .push(DnType::CommonName, "localhost");
    let cert = leaf.signed_by(&leaf_key, &rcgen::Issuer::new(ca_params, ca_key))?;

    let ca_path = dir.join("ca.pem");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&ca_path, ca.pem())?;
    // Leaf first, then the issuer: the order a chain is read in.
    std::fs::write(&cert_path, format!("{}{}", cert.pem(), ca.pem()))?;
    std::fs::write(&key_path, leaf_key.serialize_pem())?;

    println!("ca:   {}", ca_path.display());
    println!("cert: {}", cert_path.display());
    println!("key:  {}", key_path.display());
    println!("SANs: localhost, host.docker.internal, 127.0.0.1");
    Ok(())
}
