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

    // A second CA for client certificates, and one client certificate issued
    // from it. Separate from the server's CA on purpose: `tls.client_ca` says
    // which authority may vouch for clients, and pointing it at the same CA
    // that issued the server would let anything holding a server certificate
    // authenticate as a client.
    let client_ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let mut client_ca_params = CertificateParams::new(Vec::new())?;
    client_ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    client_ca_params
        .distinguished_name
        .push(DnType::CommonName, "vash development client CA");
    let client_ca = client_ca_params.clone().self_signed(&client_ca_key)?;

    let client_subject = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "client.internal".into());
    let client_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let client = CertificateParams::new(vec![client_subject.clone()])?.signed_by(
        &client_key,
        &rcgen::Issuer::new(client_ca_params, client_ca_key),
    )?;

    let client_ca_path = dir.join("client-ca.pem");
    let client_cert_path = dir.join("client.pem");
    let client_key_path = dir.join("client-key.pem");
    std::fs::write(&client_ca_path, client_ca.pem())?;
    std::fs::write(&client_cert_path, client.pem())?;
    std::fs::write(&client_key_path, client_key.serialize_pem())?;

    println!("ca:   {}", ca_path.display());
    println!("cert: {}", cert_path.display());
    println!("key:  {}", key_path.display());
    println!("SANs: localhost, host.docker.internal, 127.0.0.1");
    println!();
    println!("client CA:   {}", client_ca_path.display());
    println!("client cert: {}", client_cert_path.display());
    println!("client key:  {}", client_key_path.display());
    println!("client SAN:  {client_subject}");
    println!();
    println!("For mTLS, point tls.client_ca at the client CA and add a row:");
    println!("  some-service  mtls:{client_subject}");
    Ok(())
}
