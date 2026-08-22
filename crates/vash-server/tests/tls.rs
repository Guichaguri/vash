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
