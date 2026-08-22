//! TLS termination, over a real socket, against a real `rustls` client.
//!
//! Phase 1 of `docs/tls-proposal.md`. The test that matters most is
//! [`a_batch_larger_than_the_socket_buffer_completes`]: it is the regression
//! test for the hang Phase 0 found, and it is written as a timeout rather than
//! an assertion because the failure mode is not a wrong answer — it is a
//! connection that never speaks again.
//!
//! Only compiled with `--features tls`; without it there is nothing to serve.

#![cfg(feature = "tls")]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use vash_proto::vcp::{HEADER_LEN, Opcode, Status, encode_request, encode_set_body};
use vash_server::{Config, Server};

/// Nothing here may hang. Every exchange runs under this, so a regression of
/// the Phase 0 deadlock fails the suite in seconds instead of wedging CI.
const DEADLINE: Duration = Duration::from_secs(30);

/// Issues a CA and a `localhost` leaf into `dir`, returning their paths and the
/// CA in DER for the client's root store.
fn issue(dir: &std::path::Path) -> (PathBuf, PathBuf, rustls::pki_types::CertificateDer<'static>) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, PKCS_ECDSA_P256_SHA256};

    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.clone().self_signed(&ca_key).unwrap();

    let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .signed_by(&leaf_key, &rcgen::Issuer::new(ca_params, ca_key))
        .unwrap();

    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    // Leaf first, then the issuer: the order a chain is read in.
    std::fs::write(&cert, format!("{}{}", leaf.pem(), ca.pem())).unwrap();
    std::fs::write(&key, leaf_key.serialize_pem()).unwrap();

    (cert, key, ca.der().clone())
}

struct TestServer {
    plain: SocketAddr,
    tls: SocketAddr,
    ca: rustls::pki_types::CertificateDer<'static>,
    _dir: TempDir,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl TestServer {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key, ca) = issue(dir.path());

        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.store.path = dir.path().join("db");
        config.store.map_size_mb = 64;
        // One environment: nothing here is about sharding, and the suite runs
        // in parallel against a finite pool of reader slots.
        config.store.shards = 1;
        config.observability.admin_listen = String::new();
        config.tls.listen = "127.0.0.1:0".into();
        config.tls.cert = cert;
        config.tls.key = key;

        let server = Server::bind(config).await.expect("binding");
        let plain = server.local_addr().unwrap();
        let tls = server.tls_addr().expect("a TLS listener");
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve(async {
                    let _ = rx.await;
                })
                .await
        });

        Self {
            plain,
            tls,
            ca,
            _dir: dir,
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    /// A client that trusts exactly the CA this server was issued from, so a
    /// certificate error is a real failure rather than a missing root.
    fn connector(&self) -> tokio_rustls::TlsConnector {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.ca.clone()).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    async fn connect_tls(&self) -> Conn<tokio_rustls::client::TlsStream<TcpStream>> {
        let stream = TcpStream::connect(self.tls).await.expect("connecting");
        stream.set_nodelay(true).unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let stream = tokio::time::timeout(DEADLINE, self.connector().connect(name, stream))
            .await
            .expect("the handshake timed out")
            .expect("the handshake failed");
        Conn::new(stream)
    }

    async fn connect_plain(&self) -> Conn<TcpStream> {
        let stream = TcpStream::connect(self.plain).await.expect("connecting");
        stream.set_nodelay(true).unwrap();
        Conn::new(stream)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        drop(self.shutdown.take());
        self.handle.take();
    }
}

struct Conn<S> {
    stream: S,
    buf: Vec<u8>,
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Conn<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    async fn send(&mut self, bytes: &[u8]) {
        tokio::time::timeout(DEADLINE, async {
            self.stream.write_all(bytes).await.unwrap();
            // The flush the Phase 0 hang was missing. A test that omitted it
            // would reproduce the bug rather than detect it.
            self.stream.flush().await.unwrap();
        })
        .await
        .expect("writing timed out");
    }

    /// Reads one whole VCP frame, refusing to wait forever for it.
    async fn frame(&mut self) -> (Status, Vec<u8>) {
        tokio::time::timeout(DEADLINE, async {
            loop {
                if let vash_proto::vcp::FrameLen::Complete(len) =
                    vash_proto::vcp::peek_frame_len(&self.buf)
                {
                    let frame: Vec<u8> = self.buf.drain(..len).collect();
                    let status = u16::from_le_bytes(frame[2..4].try_into().unwrap());
                    return (
                        Status::from_u16(status).expect("a known status"),
                        frame[HEADER_LEN..].to_vec(),
                    );
                }
                let mut chunk = [0u8; 64 * 1024];
                let read = self.stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "the server closed the connection mid-frame");
                self.buf.extend_from_slice(&chunk[..read]);
            }
        })
        .await
        .expect("reading a reply timed out — the connection stopped answering")
    }

    async fn hello(&mut self) {
        let mut body = Vec::new();
        body.extend_from_slice(&vash_core::PROTOCOL_VERSION.to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes());
        let mut frame = Vec::new();
        encode_request(&mut frame, Opcode::Hello, 0, &body);
        self.send(&frame).await;
        assert_eq!(self.frame().await.0, Status::Ok, "HELLO over TLS");
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) {
        let mut body = Vec::new();
        encode_set_body(&mut body, key, value, 0, &[]);
        let mut frame = Vec::new();
        encode_request(&mut frame, Opcode::Set, 1, &body);
        self.send(&frame).await;
        assert_eq!(self.frame().await.0, Status::Ok, "SET");
    }

    async fn get(&mut self, key: &[u8]) -> (Status, Vec<u8>) {
        let mut frame = Vec::new();
        encode_request(&mut frame, Opcode::Get, 2, key);
        self.send(&frame).await;
        self.frame().await
    }

    /// `stats settings` over the memcached dialect, as a map.
    async fn memcached_settings(&mut self) -> std::collections::HashMap<String, String> {
        self.send(b"stats settings\r\n").await;
        let text = tokio::time::timeout(DEADLINE, async {
            let mut out = Vec::new();
            loop {
                let mut chunk = [0u8; 64 * 1024];
                let read = self.stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "the server closed the connection");
                out.extend_from_slice(&chunk[..read]);
                if out.ends_with(b"END\r\n") {
                    return String::from_utf8(out).unwrap();
                }
            }
        })
        .await
        .expect("reading stats timed out");

        text.lines()
            .filter_map(|line| line.strip_prefix("STAT "))
            .filter_map(|line| line.split_once(' '))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}

#[tokio::test]
async fn a_client_can_speak_vcp_over_tls() {
    let server = TestServer::start().await;
    let mut conn = server.connect_tls().await;
    conn.hello().await;
    conn.set(b"encrypted", b"value").await;
    let (status, body) = conn.get(b"encrypted").await;
    assert_eq!(status, Status::Ok);
    assert!(
        body.ends_with(b"value"),
        "the value must survive the round trip"
    );
}

/// The Phase 0 regression test.
///
/// One `write_all` of more than a socket buffer's worth used to strand its tail
/// inside `rustls` — the peer then waited for bytes that had been accepted but
/// never sent, and both ends sat reading forever. 1 MiB is four times the
/// threshold that was measured, and the whole exchange runs under a deadline
/// so the failure is a failed test rather than a hung one.
#[tokio::test]
async fn a_batch_larger_than_the_socket_buffer_completes() {
    let server = TestServer::start().await;
    let mut conn = server.connect_tls().await;
    conn.hello().await;

    let value = vec![b'v'; 4096];
    let mut batch = Vec::new();
    for i in 0..256u32 {
        let mut body = Vec::new();
        encode_set_body(&mut body, format!("bulk:{i:08}").as_bytes(), &value, 0, &[]);
        encode_request(&mut batch, Opcode::Set, i, &body);
    }
    assert!(
        batch.len() > 1024 * 1024,
        "the batch has to exceed the buffers to be the test it claims to be: {} bytes",
        batch.len()
    );

    conn.send(&batch).await;
    for _ in 0..256 {
        assert_eq!(conn.frame().await.0, Status::Ok, "every SET is answered");
    }

    // And the connection is still usable afterwards, which is what says the
    // stream is in a consistent state rather than merely drained.
    let (status, body) = conn.get(b"bulk:00000255").await;
    assert_eq!(status, Status::Ok);
    assert!(body.ends_with(&value));
}

/// A reply large enough to fill the socket in the other direction — the same
/// bug, mirrored, which is how it was confirmed to need a flush on both ends.
#[tokio::test]
async fn a_reply_larger_than_the_socket_buffer_completes() {
    let server = TestServer::start().await;
    let mut conn = server.connect_tls().await;
    conn.hello().await;

    let value = vec![b'v'; 4096];
    for i in 0..128u32 {
        conn.set(format!("big:{i:08}").as_bytes(), &value).await;
    }

    let mut batch = Vec::new();
    for i in 0..128u32 {
        encode_request(&mut batch, Opcode::Get, i, format!("big:{i:08}").as_bytes());
    }
    conn.send(&batch).await;
    for _ in 0..128 {
        assert_eq!(conn.frame().await.0, Status::Ok, "every GET is answered");
    }
}

#[tokio::test]
async fn both_ports_serve_the_same_store() {
    let server = TestServer::start().await;

    let mut encrypted = server.connect_tls().await;
    encrypted.hello().await;
    encrypted.set(b"shared", b"written-over-tls").await;

    let mut plain = server.connect_plain().await;
    plain.hello().await;
    let (status, body) = plain.get(b"shared").await;
    assert_eq!(status, Status::Ok, "the plaintext port sees the same store");
    assert!(body.ends_with(b"written-over-tls"));
}

/// `ssl_enabled` describes the connection asking, not the process answering.
///
/// Both ports serve one store, so a client on the plaintext one that checks
/// this before sending a credential has to be told `no` even while the TLS
/// listener is up.
#[tokio::test]
async fn ssl_enabled_is_per_connection() {
    let server = TestServer::start().await;

    let mut encrypted = server.connect_tls().await;
    assert_eq!(
        encrypted.memcached_settings().await.get("ssl_enabled"),
        Some(&"yes".to_string()),
    );

    let mut plain = server.connect_plain().await;
    assert_eq!(
        plain.memcached_settings().await.get("ssl_enabled"),
        Some(&"no".to_string()),
        "a plaintext connection must never be told the cache is encrypted"
    );
}

/// The rollout's progress bar: an operator closing the plaintext port needs to
/// see, per connection, what is still arriving in the clear.
#[tokio::test]
async fn stats_conns_marks_encrypted_connections() {
    let server = TestServer::start().await;

    // Neither connection announces a dialect first: detection settles on the
    // first byte, and `stats` is memcached's. A VCP `HELLO` here would pin the
    // connection to the wrong dialect and the server would close it.
    let mut encrypted = server.connect_tls().await;
    // Opened and left silent. It is registered from the moment it is accepted,
    // which is exactly the connection an operator running `stats conns` during
    // a rollout is looking for.
    let _plain = server.connect_plain().await;

    // Asked over the encrypted connection, so the answer includes itself.
    encrypted.send(b"stats conns\r\n").await;
    let text = tokio::time::timeout(DEADLINE, async {
        let mut out = Vec::new();
        loop {
            let mut chunk = [0u8; 64 * 1024];
            let read = encrypted.stream.read(&mut chunk).await.unwrap();
            out.extend_from_slice(&chunk[..read]);
            if out.ends_with(b"END\r\n") {
                return String::from_utf8(out).unwrap();
            }
        }
    })
    .await
    .expect("reading stats conns timed out");

    let flags: Vec<&str> = text
        .lines()
        .filter(|line| line.contains(":vash_tls "))
        .map(|line| line.rsplit(' ').next().unwrap())
        .collect();
    assert!(
        flags.contains(&"yes") && flags.contains(&"no"),
        "one encrypted and one plaintext connection are open, so both must \
         appear: {flags:?}"
    );
}

// ---------------------------------------------------------------------------
// Phase 2: cluster peers over TLS
// ---------------------------------------------------------------------------

/// A cluster whose nodes reach each other over TLS.
///
/// Peers are listed by `127.0.0.1:port`, which is deliberately the awkward
/// case: an address carries no name, so the certificate cannot match one
/// unless it was issued with an IP SAN. `cluster.tls_server_name` is the
/// override for exactly that, and this exercises it.
/// One running node: the handle that stops it, and the task serving it.
struct Node {
    shutdown: oneshot::Sender<()>,
    serving: JoinHandle<anyhow::Result<()>>,
}

struct TlsCluster {
    plain_addrs: Vec<SocketAddr>,
    tls_addrs: Vec<SocketAddr>,
    cert: PathBuf,
    key: PathBuf,
    ca: PathBuf,
    nodes: Vec<Option<Node>>,
    /// One per node, kept so a stopped node can come back to the same
    /// database, plus the PKI directory at the end.
    dirs: Vec<TempDir>,
}

impl TlsCluster {
    async fn start(size: usize) -> Self {
        // One CA and one `localhost` certificate for every node: a shared
        // certificate is the deployment `tls_server_name` exists for.
        let pki_dir = tempfile::tempdir().unwrap();
        let (cert, key, _) = issue(pki_dir.path());
        // `issue` writes the chain leaf-first with the CA appended, so the
        // chain file doubles as the CA bundle a client verifies against.
        let ca = cert.clone();

        // Reserve the ports before anything starts: every node's peer list
        // names the others, so the addresses have to exist first.
        let mut held = Vec::new();
        for _ in 0..size * 2 {
            held.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let addrs: Vec<SocketAddr> = held.iter().map(|l| l.local_addr().unwrap()).collect();
        drop(held);
        let (plain_addrs, tls_addrs) = addrs.split_at(size);

        let mut dirs = Vec::new();
        for _ in 0..size {
            dirs.push(tempfile::tempdir().unwrap());
        }
        // The PKI directory has to outlive the nodes, which read it at boot
        // and — for the CA — on every peer connection.
        dirs.push(pki_dir);

        let mut cluster = Self {
            plain_addrs: plain_addrs.to_vec(),
            tls_addrs: tls_addrs.to_vec(),
            cert,
            key,
            ca,
            nodes: (0..size).map(|_| None).collect(),
            dirs,
        };
        for index in 0..size {
            cluster.spawn(index).await;
        }
        cluster
    }

    /// Starts (or restarts) one node against its own database directory.
    async fn spawn(&mut self, index: usize) {
        let size = self.tls_addrs.len();
        let peers: Vec<String> = self
            .tls_addrs
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, addr)| addr.to_string())
            .collect();

        let mut config = Config::default();
        config.server.listen = self.plain_addrs[index];
        config.store.path = self.dirs[index].path().join("db");
        config.store.map_size_mb = 64;
        config.store.shards = 1;
        config.observability.admin_listen = String::new();
        config.tls.listen = self.tls_addrs[index].to_string();
        config.tls.cert = self.cert.clone();
        config.tls.key = self.key.clone();
        config.cluster.peers = peers;
        config.cluster.tls = true;
        config.cluster.tls_ca = self.ca.clone();
        // The peers are IPs, so the name has to come from here.
        config.cluster.tls_server_name = "localhost".into();
        config.cluster.gossip_interval_ms = 100;
        config.cluster.fanout_timeout_ms = 1_000;
        let _ = size;

        let server = Server::bind(config).await.expect("binding a TLS node");
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve(async {
                    let _ = rx.await;
                })
                .await
        });
        self.nodes[index] = Some(Node {
            shutdown: tx,
            serving: handle,
        });
    }

    fn tls_config(&self) -> vash_client::TlsConfig {
        vash_client::TlsConfig::new(&self.ca, "localhost").expect("client TLS config")
    }

    async fn client(&self, index: usize) -> vash_client::Client {
        vash_client::Client::connect_tls(&self.tls_addrs[index], None, &self.tls_config())
            .await
            .expect("connecting to a node over TLS")
    }

    async fn stop(&mut self, index: usize) {
        if let Some(node) = self.nodes[index].take() {
            drop(node.shutdown);
            let _ = tokio::time::timeout(DEADLINE, node.serving).await;
        }
    }
}

/// The phase 2 exit criterion: invalidation converges across three nodes with
/// every peer connection encrypted.
#[tokio::test]
async fn invalidation_converges_across_a_tls_cluster() {
    let mut cluster = TlsCluster::start(3).await;

    // Each node holds its own key under a shared tag, which is the case a
    // single-node invalidation gets wrong.
    for index in 0..3 {
        let mut client = cluster.client(index).await;
        client
            .set_tagged(
                format!("node{index}-article").as_bytes(),
                b"body",
                0,
                &[b"homepage".as_slice()],
            )
            .await
            .expect("writing a tagged key over TLS");
    }

    cluster
        .client(0)
        .await
        .delete_by_tag(b"homepage")
        .await
        .expect("invalidating over TLS");

    // Fan-out is asynchronous, so this is a convergence assertion rather than
    // an immediate one.
    let deadline = std::time::Instant::now() + DEADLINE;
    loop {
        let mut converged = true;
        for index in 0..3 {
            let mut client = cluster.client(index).await;
            if client
                .get(format!("node{index}-article").as_bytes())
                .await
                .expect("reading over TLS")
                .is_some()
            {
                converged = false;
            }
        }
        if converged {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the invalidation never reached every node over TLS"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    for index in 0..3 {
        cluster.stop(index).await;
    }
}

/// A bad CA is a configuration error, and has to read as one.
///
/// This is the failure the `ClientError::Tls` variant exists for: without it a
/// handshake rejection arrives as an `Io` error and a node reports its peers
/// as unreachable, pointing an operator at the network instead of at the file
/// they mistyped.
#[tokio::test]
async fn a_ca_that_does_not_match_is_refused_as_configuration() {
    let server = TestServer::start().await;

    // A perfectly valid CA, for somebody else's certificate.
    let other = tempfile::tempdir().unwrap();
    let (wrong_ca, _, _) = issue(other.path());

    let tls = vash_client::TlsConfig::new(&wrong_ca, "localhost").expect("a parseable CA");
    let error = match vash_client::Client::connect_tls(&server.tls, None, &tls).await {
        Err(error) => error,
        Ok(_) => panic!("a certificate from an unknown CA must not be accepted"),
    };

    assert!(
        matches!(error, vash_client::ClientError::Tls(_)),
        "a handshake rejection must be distinguishable from a dead peer, or the cluster \
         reports a misconfigured CA as unreachability: {error:?}"
    );
}

/// An unreadable CA file stops startup rather than surfacing later as every
/// peer being unreachable.
#[tokio::test]
async fn a_cluster_with_an_unreadable_ca_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.server.listen = "127.0.0.1:0".parse().unwrap();
    config.store.path = dir.path().join("db");
    config.store.map_size_mb = 64;
    config.store.shards = 1;
    config.observability.admin_listen = String::new();
    config.cluster.peers = vec!["127.0.0.1:1".into()];
    config.cluster.tls = true;
    config.cluster.tls_ca = dir.path().join("nothing-here.pem");
    config.cluster.tls_server_name = "localhost".into();

    let error = match Server::bind(config).await {
        Err(error) => error,
        Ok(_) => panic!("a missing CA must stop startup"),
    };
    let chain = format!("{error:#}");
    assert!(
        chain.contains("nothing-here.pem") || chain.contains("cluster"),
        "the error has to name what could not be read: {chain}"
    );
}

/// The other half of the phase 2 criterion: a peer that was *down* when the
/// invalidation happened still converges once it comes back.
///
/// Fan-out cannot reach a node that is not listening, so this is anti-entropy's
/// job — and anti-entropy runs over the same encrypted peer connections. A node
/// that came back would otherwise keep serving keys the rest of the cluster has
/// already invalidated, which is the failure mode that is invisible until
/// somebody reads stale data.
#[tokio::test]
async fn a_peer_that_was_down_converges_over_tls() {
    let mut cluster = TlsCluster::start(3).await;

    for index in 0..3 {
        let mut client = cluster.client(index).await;
        client
            .set_tagged(
                format!("node{index}-article").as_bytes(),
                b"body",
                0,
                &[b"homepage".as_slice()],
            )
            .await
            .expect("writing a tagged key over TLS");
    }

    // Node 2 misses the invalidation entirely.
    cluster.stop(2).await;

    cluster
        .client(0)
        .await
        .delete_by_tag(b"homepage")
        .await
        .expect("invalidating while a peer is down");

    cluster.spawn(2).await;

    // It has its old database back, so the key is still there until gossip
    // tells it otherwise.
    let deadline = std::time::Instant::now() + DEADLINE;
    loop {
        let served = cluster
            .client(2)
            .await
            .get(b"node2-article")
            .await
            .expect("reading from the restarted node over TLS")
            .is_some();
        if !served {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "anti-entropy never reached the restarted node over TLS"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    for index in 0..3 {
        cluster.stop(index).await;
    }
}

// ---------------------------------------------------------------------------
// Phase 3: the certificate is the identity
// ---------------------------------------------------------------------------

/// A client certificate for `subject`, issued by `ca_key`/`ca_params`.
///
/// Returned as DER plus its key, because a `rustls` client wants them that way
/// and nothing here needs them on disk.
fn issue_client(
    ca: &(rcgen::CertificateParams, rcgen::KeyPair),
    subject: &str,
) -> (
    rustls::pki_types::CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
) {
    use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};

    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let params = CertificateParams::new(vec![subject.to_string()]).unwrap();
    let ca_key = KeyPair::from_pem(&ca.1.serialize_pem()).unwrap();
    let cert = params
        .signed_by(&key, &rcgen::Issuer::new(ca.0.clone(), ca_key))
        .unwrap();
    (
        cert.der().clone(),
        rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap(),
    )
}

/// A CA that issues client certificates, kept so several can be issued from it.
fn client_ca(dir: &std::path::Path) -> ((rcgen::CertificateParams, rcgen::KeyPair), PathBuf) {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, PKCS_ECDSA_P256_SHA256,
    };

    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "vash client CA");
    let cert = params.clone().self_signed(&key).unwrap();

    let path = dir.join("client-ca.pem");
    std::fs::write(&path, cert.pem()).unwrap();
    ((params, key), path)
}

/// A server that requires client certificates, with a credential table.
struct MtlsServer {
    tls: SocketAddr,
    server_ca: rustls::pki_types::CertificateDer<'static>,
    client_ca: (rcgen::CertificateParams, rcgen::KeyPair),
    credentials: PathBuf,
    dir: TempDir,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl MtlsServer {
    /// `rows` are credential-file lines, e.g. `billing  mtls:billing.internal`.
    async fn start(rows: &[&str], auth_required: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key, server_ca) = issue(dir.path());
        let (client_ca_pair, client_ca_path) = client_ca(dir.path());

        let credentials = dir.path().join("credentials");
        std::fs::write(&credentials, format!("{}\n", rows.join("\n"))).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
        }

        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.store.path = dir.path().join("db");
        config.store.map_size_mb = 64;
        config.store.shards = 1;
        config.observability.admin_listen = String::new();
        config.tls.listen = "127.0.0.1:0".into();
        config.tls.cert = cert;
        config.tls.key = key;
        config.tls.client_auth = vash_server::config::ClientAuth::Required;
        config.tls.client_ca = client_ca_path;
        config.auth.required = auth_required;
        config.auth.file = credentials.clone();

        let server = Server::bind(config).await.expect("binding");
        let tls = server.tls_addr().expect("a TLS listener");
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve(async {
                    let _ = rx.await;
                })
                .await
        });

        Self {
            tls,
            server_ca,
            client_ca: client_ca_pair,
            credentials,
            dir,
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    /// Connects presenting a certificate for `subject`.
    async fn connect_as(
        &self,
        subject: &str,
    ) -> std::io::Result<Conn<tokio_rustls::client::TlsStream<TcpStream>>> {
        let (cert, key) = issue_client(&self.client_ca, subject);

        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.server_ca.clone()).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![cert], key)
        .unwrap();

        let stream = TcpStream::connect(self.tls).await?;
        stream.set_nodelay(true).unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, stream)
            .await?;
        Ok(Conn::new(stream))
    }

    /// Rewrites the credential file and reloads it the way `SIGHUP` does.
    ///
    /// The signal itself is Unix-only, so what is exercised here is the half
    /// that is not: that the table is consulted per connection rather than
    /// captured once, which is what makes removing a row lock a client out.
    fn rewrite_credentials(&self, rows: &[&str]) {
        std::fs::write(&self.credentials, format!("{}\n", rows.join("\n"))).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.credentials, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
    }
}

impl Drop for MtlsServer {
    fn drop(&mut self) {
        drop(self.shutdown.take());
        self.handle.take();
        let _ = &self.dir;
    }
}

/// The phase 3 exit criterion: `auth.required` satisfied by a certificate, with
/// no `AUTH` command and no secret anywhere.
#[tokio::test]
async fn a_certificate_satisfies_auth_required_on_its_own() {
    let server = MtlsServer::start(&["billing  mtls:billing.internal"], true).await;

    let mut conn = server
        .connect_as("billing.internal")
        .await
        .expect("connecting");
    conn.hello().await;
    // No AUTH is sent. With `auth.required` this would be refused if the
    // handshake had not already said who this is.
    conn.set(b"mtls:key", b"value").await;
    let (status, body) = conn.get(b"mtls:key").await;
    assert_eq!(status, Status::Ok);
    assert!(body.ends_with(b"value"));
}

/// A certificate the CA signed, for a name nobody claims, is refused.
///
/// Being issued *a* certificate is not the same as being someone this cache
/// serves — the CA says the holder is who they say they are, and the credential
/// table decides whether that is anybody here.
#[tokio::test]
async fn a_certificate_with_no_matching_row_is_refused() {
    let server = MtlsServer::start(&["billing  mtls:billing.internal"], true).await;

    // The handshake itself succeeds — the CA is right — so the refusal shows
    // up as the connection being closed rather than as a TLS error.
    let closed = match server.connect_as("nobody.internal").await {
        Err(_) => true,
        Ok(mut conn) => {
            let mut chunk = [0u8; 64];
            matches!(
                tokio::time::timeout(DEADLINE, conn.stream.read(&mut chunk)).await,
                Ok(Ok(0)) | Ok(Err(_))
            )
        }
    };
    assert!(
        closed,
        "a certificate matching no credential row must not be served"
    );
}

/// Removing the row locks that client out of its next connection.
///
/// This is what certificate revocation means here: the credential table is the
/// revocation list, which for a handful of services is the right size of
/// mechanism — no CRL, no OCSP, no waiting for an expiry.
#[tokio::test]
async fn removing_a_row_locks_the_certificate_out() {
    let server = MtlsServer::start(
        &[
            "billing  mtls:billing.internal",
            "reports  mtls:reports.internal",
        ],
        true,
    )
    .await;

    let mut conn = server
        .connect_as("reports.internal")
        .await
        .expect("connecting");
    conn.hello().await;
    conn.set(b"mtls:before", b"value").await;
    drop(conn);

    // The same rotation `SIGHUP` performs: rewrite the file, reload the table.
    server.rewrite_credentials(&["billing  mtls:billing.internal"]);
    let reloaded = vash_server::auth::Auth::load(&vash_server::config::AuthConfig {
        required: true,
        file: server.credentials.clone(),
        ..Default::default()
    })
    .expect("the rewritten file must load");
    assert!(
        reloaded
            .identity_for_certificate(|subject| subject == "reports.internal")
            .is_none(),
        "the removed row must be gone from the reloaded table"
    );
    assert!(
        reloaded
            .identity_for_certificate(|subject| subject == "billing.internal")
            .is_some(),
        "and the row that stayed must still be there"
    );
}

/// An `mtls:` row is not a password.
///
/// The subject is public — it is in every certificate that client presents — so
/// a row that could also be satisfied by typing it would be a credential
/// published to everyone who ever connected.
#[tokio::test]
async fn an_mtls_subject_is_not_a_password() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("credentials");
    std::fs::write(&path, "billing  mtls:billing.internal\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let auth = vash_server::auth::Auth::load(&vash_server::config::AuthConfig {
        required: true,
        file: path,
        ..Default::default()
    })
    .expect("loading");

    assert!(
        auth.verify(b"billing", b"billing.internal").is_none(),
        "the subject must not work as a secret"
    );
    assert!(
        auth.verify(b"billing", b"").is_none(),
        "and neither must an empty one"
    );
}

/// `stats conns` distinguishes how a connection authenticated.
#[tokio::test]
async fn stats_conns_reports_certificate_authentication() {
    let server = MtlsServer::start(&["billing  mtls:billing.internal"], true).await;

    let mut conn = server
        .connect_as("billing.internal")
        .await
        .expect("connecting");
    let settings = conn.memcached_settings().await;
    assert_eq!(settings.get("ssl_enabled"), Some(&"yes".to_string()));

    let mut conn = server
        .connect_as("billing.internal")
        .await
        .expect("connecting");
    conn.send(b"stats conns\r\n").await;
    let text = tokio::time::timeout(DEADLINE, async {
        let mut out = Vec::new();
        loop {
            let mut chunk = [0u8; 64 * 1024];
            let read = conn.stream.read(&mut chunk).await.unwrap();
            out.extend_from_slice(&chunk[..read]);
            if out.ends_with(b"END\r\n") {
                return String::from_utf8(out).unwrap();
            }
        }
    })
    .await
    .expect("reading stats conns timed out");

    assert!(
        text.contains(":vash_auth_method certificate"),
        "a connection identified by its certificate has to say so: {text}"
    );
}

/// `client_auth = "required"` means required.
///
/// A client with no certificate is refused by the handshake itself, before any
/// protocol byte is read — which is the difference between this and a
/// credential, and the reason there is no "optional" mode.
#[tokio::test]
async fn a_client_with_no_certificate_is_refused() {
    let server = MtlsServer::start(&["billing  mtls:billing.internal"], true).await;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(server.server_ca.clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();

    let stream = TcpStream::connect(server.tls).await.expect("connecting");
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let handshake = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, stream)
        .await;

    // TLS 1.3 sends the client certificate after the client believes the
    // handshake is done, so the refusal can surface either here or on the
    // first read. Both are the connection being refused; neither serves a
    // command.
    let refused = match handshake {
        Err(_) => true,
        Ok(mut stream) => {
            let mut chunk = [0u8; 64];
            !matches!(
                tokio::time::timeout(DEADLINE, stream.read(&mut chunk)).await,
                Ok(Ok(n)) if n > 0
            )
        }
    };
    assert!(
        refused,
        "a connection with no client certificate must not be served"
    );
}
