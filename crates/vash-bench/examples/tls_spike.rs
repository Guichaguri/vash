//! Phase 0 of [`docs/tls-proposal.md`]: what TLS costs, measured, before any of
//! it is built into the server.
//!
//! ```text
//! cargo run --release -p vash-bench --example tls_spike
//! cargo run --release -p vash-bench --example tls_spike -- handshake
//! cargo run --release -p vash-bench --example tls_spike -- bulk
//! ```
//!
//! The proposal makes two cost claims and marks both as unverified. This
//! answers them in isolation, with nothing of vash in the path — the same
//! discipline `vash-store/examples/txn_bench.rs` used for the reader-slot
//! question, and for the same reason: a number measured through the whole
//! server leaves room to blame the server.
//!
//! **Handshakes** (§8.1). auth.md §3.7 costed mTLS at "~1 ms and an allocation
//! storm" and the proposal called that pessimistic without saying by how much.
//! The leaf's key algorithm is the variable that matters, because the server
//! signs once per full handshake: RSA-2048 signing is arithmetic on a 2048-bit
//! modulus, P-256 signing is not.
//!
//! **Bulk** (§8.2). The proposal's arithmetic says a 4 KiB `GET` workload at
//! 1.4 GB/s is plausibly a whole core of AES-GCM. That rests on a
//! GB/s-per-core figure taken from the literature rather than from this box.
//!
//! Both providers are linked and selected at run time, because Phase 0 found
//! that neither needs extra build tooling — so the choice between them is a
//! measurement, not a packaging constraint.
//!
//! # Why the handshake table has no sockets in it
//!
//! The first version of this file connected a real socket per handshake and
//! measured nonsense: a few hundred per second, falling to nearly zero, and
//! then `os error 10048` when the bulk section tried to bind. Thousands of
//! connect-and-close cycles a second exhaust the ephemeral port range —
//! Windows leaves each one in `TIME_WAIT` for two minutes — so what it was
//! measuring was the port table, not the cryptography.
//!
//! The handshake sections therefore drive `rustls` connection objects against
//! each other through memory buffers, which is what rustls' own benchmarks do.
//! No sockets, no port table, no scheduler: the number is the CPU cost of a
//! handshake and nothing else, which is exactly the quantity §8.1 needs and
//! §9 reasons about. It is also, usefully, an upper bound — a real handshake
//! adds two round trips that this cannot see.
//!
//! The bulk section keeps its sockets, because there the question is what the
//! *write path* delivers, syscalls included. It opens one connection per
//! configuration rather than thousands.
//!
//! # What this cannot tell you
//!
//! Loopback moves bytes at memory bandwidth and both ends share these cores,
//! so the plaintext column in the bulk table is unreachable on real hardware.
//! The TLS/plaintext ratio and the encrypt-only column are the transferable
//! numbers.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// How long each individual configuration runs.
const DURATION: Duration = Duration::from_secs(3);

/// Reply-sized writes: the README's `GET` value sizes, because a reply is what
/// the server actually encrypts, plus the sizes above them where the
/// per-record cost stops mattering.
///
/// 16 KiB is the last row because that is TLS's maximum record size. A larger
/// write is several records, and measures the same thing twice.
const CHUNKS: [usize; 5] = [64, 256, 1024, 4096, 16384];

/// The write size for the socket comparison.
///
/// Fixed, and large, because that table is about the transport rather than the
/// record size: at 64 bytes a write per reply is one syscall per 64 bytes, and
/// the plaintext column collapses to a syscall benchmark that TLS then "wins"
/// by buffering. 64 KiB is the batched-reply case, where the comparison is
/// between two paths that are both moving bytes rather than both trapping.
const SOCKET_WRITE: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Provider {
    Ring,
    AwsLc,
}

impl Provider {
    fn name(self) -> &'static str {
        match self {
            Self::Ring => "ring",
            Self::AwsLc => "aws-lc-rs",
        }
    }

    fn build(self) -> Arc<CryptoProvider> {
        Arc::new(match self {
            Self::Ring => rustls::crypto::ring::default_provider(),
            Self::AwsLc => rustls::crypto::aws_lc_rs::default_provider(),
        })
    }

    /// The stateless ticketer from the same provider, for the resumed column.
    fn ticketer(self) -> Arc<dyn rustls::server::ProducesTickets> {
        match self {
            Self::Ring => rustls::crypto::ring::Ticketer::new(),
            Self::AwsLc => rustls::crypto::aws_lc_rs::Ticketer::new(),
        }
        .expect("ticketer")
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Leaf {
    EcdsaP256,
    Rsa2048,
}

impl Leaf {
    fn name(self) -> &'static str {
        match self {
            Self::EcdsaP256 => "ECDSA P-256",
            Self::Rsa2048 => "RSA-2048",
        }
    }
}

/// A CA and a leaf signed by it, which is the shape a real deployment has —
/// verifying a self-signed leaf would skip a signature check the client
/// actually performs.
struct Pki {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    roots: RootCertStore,
}

fn issue(leaf: Leaf) -> anyhow::Result<Pki> {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, PKCS_ECDSA_P256_SHA256,
        PKCS_RSA_SHA256,
    };

    // The CA is P-256 in both cases. It signs once, at issue time, and never
    // during a handshake — so its algorithm is not part of what is being
    // measured, and holding it constant keeps the two rows comparable.
    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let mut ca_params = CertificateParams::new(Vec::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "vash tls spike CA");
    let ca = ca_params.clone().self_signed(&ca_key)?;

    let leaf_key = match leaf {
        Leaf::EcdsaP256 => KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?,
        Leaf::Rsa2048 => KeyPair::generate_for(&PKCS_RSA_SHA256)?,
    };
    let leaf_params = CertificateParams::new(vec!["localhost".to_string()])?;
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer)?;

    let mut roots = RootCertStore::empty();
    roots.add(ca.der().clone())?;

    Ok(Pki {
        chain: vec![leaf_cert.der().clone(), ca.der().clone()],
        key: PrivateKeyDer::try_from(leaf_key.serialize_der()).map_err(anyhow::Error::msg)?,
        roots,
    })
}

/// A client that checks nothing, used to bound the *server's* share of a
/// handshake.
///
/// An in-memory handshake pays for both ends on one thread, but only one of
/// those ends is the server being sized. Switching the client's certificate
/// work off leaves it with a key exchange and some parsing, so the difference
/// between the two columns is what the client was spending on certificates —
/// and the remainder is the number §9 cares about, since the server is the
/// side a stranger can make do work.
///
/// Never anywhere near the server: this is a benchmark client talking to a
/// certificate this process generated eleven microseconds ago.
#[derive(Debug)]
struct NoVerify(Arc<CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn configs(
    provider: Provider,
    pki: &Pki,
    resume: bool,
    verify: bool,
) -> anyhow::Result<(Arc<ServerConfig>, Arc<ClientConfig>)> {
    let crypto = provider.build();

    let mut server = ServerConfig::builder_with_provider(Arc::clone(&crypto))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(pki.chain.clone(), pki.key.clone_key())?;

    let builder = ClientConfig::builder_with_provider(Arc::clone(&crypto))
        .with_protocol_versions(&[&rustls::version::TLS13])?;
    let mut client = if verify {
        builder
            .with_root_certificates(pki.roots.clone())
            .with_no_client_auth()
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(crypto)))
            .with_no_client_auth()
    };

    if resume {
        server.ticketer = provider.ticketer();
    } else {
        // Both halves, because either one alone still leaves the other side
        // offering: a resumed handshake must be impossible, not merely
        // unlikely.
        server.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        client.resumption = rustls::client::Resumption::disabled();
    }

    Ok((Arc::new(server), Arc::new(client)))
}

// ---------------------------------------------------------------------------
// Handshakes, in memory
// ---------------------------------------------------------------------------

/// Moves whatever one side wants to write into the other, until neither has
/// anything left to say.
///
/// Returns once both sides are idle, which after a completed handshake means
/// the session ticket has been delivered too — without that the "resumed"
/// column would measure a client that never received one.
fn pump(client: &mut ClientConnection, server: &mut ServerConnection) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(16 * 1024);
    for _ in 0..16 {
        let mut moved = false;

        buf.clear();
        while client.wants_write() {
            client.write_tls(&mut buf)?;
            moved = true;
        }
        if !buf.is_empty() {
            let mut cursor = std::io::Cursor::new(&buf[..]);
            while cursor.position() < buf.len() as u64 {
                server.read_tls(&mut cursor)?;
                server.process_new_packets()?;
            }
        }

        buf.clear();
        while server.wants_write() {
            server.write_tls(&mut buf)?;
            moved = true;
        }
        if !buf.is_empty() {
            let mut cursor = std::io::Cursor::new(&buf[..]);
            while cursor.position() < buf.len() as u64 {
                client.read_tls(&mut cursor)?;
                client.process_new_packets()?;
            }
        }

        if !moved {
            return Ok(());
        }
    }
    Ok(())
}

/// Complete handshakes per second, on `threads` threads.
fn handshake_rate(
    provider: Provider,
    pki: &Pki,
    resume: bool,
    verify: bool,
    threads: usize,
) -> anyhow::Result<f64> {
    let (server_config, client_config) = configs(provider, pki, resume, verify)?;
    let name = ServerName::try_from("localhost")?.to_owned();

    // A resumed run has to acquire a ticket before it can measure using one,
    // and the ticket lives in the client config's session cache. Priming it
    // here rather than inside the timed loop keeps one full handshake out of
    // the numbers.
    if resume {
        let mut client = ClientConnection::new(Arc::clone(&client_config), name.clone())?;
        let mut server = ServerConnection::new(Arc::clone(&server_config))?;
        pump(&mut client, &mut server)?;
    }

    let count = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + DURATION;
    let started = Instant::now();

    std::thread::scope(|scope| -> anyhow::Result<()> {
        for _ in 0..threads {
            let server_config = Arc::clone(&server_config);
            let client_config = Arc::clone(&client_config);
            let name = name.clone();
            let count = Arc::clone(&count);
            scope.spawn(move || {
                let mut local = 0u64;
                while Instant::now() < deadline {
                    // 32 between clock reads: `Instant::now` is a syscall on
                    // some platforms and a handshake is microseconds.
                    for _ in 0..32 {
                        let mut client =
                            ClientConnection::new(Arc::clone(&client_config), name.clone())
                                .expect("client");
                        let mut server =
                            ServerConnection::new(Arc::clone(&server_config)).expect("server");
                        pump(&mut client, &mut server).expect("handshake");
                        assert!(!client.is_handshaking() && !server.is_handshaking());
                        local += 1;
                    }
                }
                count.fetch_add(local, Ordering::Relaxed);
            });
        }
        Ok(())
    })?;

    Ok(count.load(Ordering::Relaxed) as f64 / started.elapsed().as_secs_f64())
}

// ---------------------------------------------------------------------------
// Bulk
// ---------------------------------------------------------------------------

/// Bytes per second a single core can turn into TLS records: the plaintext is
/// written into an established session and the ciphertext is thrown away.
///
/// This is the server's half of §8.2 with nothing else in it — no socket, no
/// reader, no loopback. It is the figure the proposal's "a whole core of
/// AES-GCM" arithmetic actually needs.
fn encrypt_rate(provider: Provider, pki: &Pki, chunk: usize) -> anyhow::Result<f64> {
    let (server_config, client_config) = configs(provider, pki, false, true)?;
    let name = ServerName::try_from("localhost")?.to_owned();
    let mut client = ClientConnection::new(client_config, name)?;
    let mut server = ServerConnection::new(server_config)?;
    pump(&mut client, &mut server)?;
    anyhow::ensure!(!server.is_handshaking(), "handshake did not complete");

    let payload = vec![b'v'; chunk];
    let mut sink = Vec::with_capacity(chunk + 4096);
    let deadline = Instant::now() + DURATION;
    let started = Instant::now();
    let mut total = 0u64;

    while Instant::now() < deadline {
        for _ in 0..64 {
            server.writer().write_all(&payload)?;
            sink.clear();
            while server.wants_write() {
                server.write_tls(&mut sink)?;
            }
            total += payload.len() as u64;
        }
    }

    Ok(total as f64 / started.elapsed().as_secs_f64())
}

/// Bytes per second, server → client over loopback, in `chunk`-sized writes.
///
/// The direction matters: a cache server's bulk traffic is replies, so the
/// expensive half — encryption — belongs on the server side, where this puts
/// it.
async fn socket_rate(
    provider: Provider,
    pki: &Pki,
    tls: bool,
    chunk: usize,
) -> anyhow::Result<f64> {
    let (server_config, client_config) = configs(provider, pki, false, true)?;
    let acceptor = TlsAcceptor::from(server_config);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let payload = vec![b'v'; chunk];
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        stream.set_nodelay(true).expect("nodelay");
        let deadline = Instant::now() + DURATION;
        if tls {
            let mut stream = acceptor.accept(stream).await.expect("handshake");
            while Instant::now() < deadline && stream.write_all(&payload).await.is_ok() {}
            let _ = stream.shutdown().await;
        } else {
            let mut stream = stream;
            while Instant::now() < deadline && stream.write_all(&payload).await.is_ok() {}
            let _ = stream.shutdown().await;
        }
    });

    let connector = TlsConnector::from(client_config);
    let name = ServerName::try_from("localhost")?.to_owned();
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    // 64 KiB, so the reader is not the bottleneck at small chunk sizes: the
    // question is what the writer can encrypt, not how often the reader wakes.
    let mut sink = vec![0u8; 64 * 1024];
    let started = Instant::now();
    let mut total = 0u64;

    if tls {
        let mut stream = connector.connect(name, stream).await?;
        while let Ok(n) = stream.read(&mut sink).await {
            if n == 0 {
                break;
            }
            total += n as u64;
        }
    } else {
        let mut stream = stream;
        while let Ok(n) = stream.read(&mut sink).await {
            if n == 0 {
                break;
            }
            total += n as u64;
        }
    }

    let elapsed = started.elapsed();
    let _ = server.await;
    Ok(total as f64 / elapsed.as_secs_f64())
}

// ---------------------------------------------------------------------------

fn gib(bytes_per_second: f64) -> f64 {
    bytes_per_second / (1024.0 * 1024.0 * 1024.0)
}

fn run_handshakes(cores: usize) -> anyhow::Result<()> {
    println!(
        "## Full and resumed TLS 1.3 handshakes per second, in memory
"
    );
    println!(
        "Both ends run on the same thread. The \"server\" column switches the client's
         certificate verification off, so the gap between the columns is what a client
         spends checking certificates, and what is left is close to the server's own cost.
"
    );
    println!(
        "{:>10}  {:>12}  {:>9}  {:>8}  {:>12}  {:>12}  {:>12}",
        "provider", "leaf", "resumed", "threads", "both /s", "server /s", "server us"
    );

    for provider in [Provider::Ring, Provider::AwsLc] {
        for leaf in [Leaf::EcdsaP256, Leaf::Rsa2048] {
            let pki = issue(leaf)?;
            for resume in [false, true] {
                for threads in [1, cores] {
                    let both = handshake_rate(provider, &pki, resume, true, threads)?;
                    let server = handshake_rate(provider, &pki, resume, false, threads)?;
                    println!(
                        "{:>10}  {:>12}  {:>9}  {:>8}  {:>12.0}  {:>12.0}  {:>12.0}",
                        provider.name(),
                        leaf.name(),
                        if resume { "yes" } else { "no" },
                        threads,
                        both,
                        server,
                        1_000_000.0 * threads as f64 / server,
                    );
                }
            }
        }
    }
    println!();
    Ok(())
}

async fn run_bulk() -> anyhow::Result<()> {
    println!("## What a core can encrypt, by record size (no sockets)\n");
    println!(
        "One record per write, which is what a reply becomes. The fall at small sizes is\n\
         the fixed per-record cost — a header, a nonce and a 16-byte tag — not the cipher.\n"
    );
    println!(
        "{:>10}  {:>10}  {:>12}  {:>14}  {:>16}",
        "provider", "record", "GiB/s", "records/s", "ns per record"
    );

    for provider in [Provider::Ring, Provider::AwsLc] {
        let pki = issue(Leaf::EcdsaP256)?;
        for chunk in CHUNKS {
            let rate = encrypt_rate(provider, &pki, chunk)?;
            let records = rate / chunk as f64;
            println!(
                "{:>10}  {:>10}  {:>12.2}  {:>14.0}  {:>16.0}",
                provider.name(),
                chunk,
                gib(rate),
                records,
                1e9 / records,
            );
        }
    }

    println!("\n## Over a loopback socket, {SOCKET_WRITE} B writes, server to client\n");
    println!(
        "{:>10}  {:>16}  {:>12}  {:>11}",
        "provider", "plaintext GiB/s", "TLS GiB/s", "TLS/plain"
    );
    for provider in [Provider::Ring, Provider::AwsLc] {
        let pki = issue(Leaf::EcdsaP256)?;
        let plain = socket_rate(provider, &pki, false, SOCKET_WRITE).await?;
        let tls = socket_rate(provider, &pki, true, SOCKET_WRITE).await?;
        println!(
            "{:>10}  {:>16.2}  {:>12.2}  {:>10.0}%",
            provider.name(),
            gib(plain),
            gib(tls),
            100.0 * tls / plain
        );
    }
    println!();
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    println!("{cores} logical cores | {DURATION:?} per configuration\n");

    match mode.as_str() {
        "handshake" => run_handshakes(cores)?,
        "bulk" => run_bulk().await?,
        "all" => {
            run_handshakes(cores)?;
            run_bulk().await?;
        }
        other => anyhow::bail!("unknown mode {other:?}; expected handshake, bulk or all"),
    }
    Ok(())
}
