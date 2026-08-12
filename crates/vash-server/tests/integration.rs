//! End-to-end tests: a real server on a real socket, driven by the real client.
//!
//! Nothing here stubs the storage engine or the transport. A test that passes
//! against a fake proves the fake works.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use vash_client::{Client, ClientError};
use vash_proto::vcp::Status;
use vash_server::{Config, Server};

struct TestServer {
    addr: SocketAddr,
    admin: Option<SocketAddr>,
    dir: TempDir,
    /// Held so tests can inspect on-disk state directly â€” `entries()` is the
    /// only way to tell the sweeper apart from the lazy read-path check.
    ///
    /// An `Option` because it must be released before the server shuts down:
    /// the environment cannot close while another handle is outstanding.
    store: Option<Arc<dyn vash_store::Store>>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl TestServer {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("creating a temp dir");
        Self::start_in(dir).await
    }

    async fn start_in(dir: TempDir) -> Self {
        Self::start_with(dir, |_| {}).await
    }

    async fn start_with(dir: TempDir, tweak: impl FnOnce(&mut Config)) -> Self {
        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.store.path = dir.path().join("db");
        // Small enough to be cheap in CI, large enough for these tests.
        config.store.map_size_mb = 64;
        // Port 0: these run in parallel and would otherwise fight over 9090.
        config.observability.admin_listen = "127.0.0.1:0".into();
        tweak(&mut config);

        let server = Server::bind(config).await.expect("binding the server");
        let addr = server.local_addr().expect("reading the bound address");
        let admin = server.admin_addr();
        let store = Arc::clone(server.store());

        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve(async {
                    let _ = rx.await;
                })
                .await
        });

        Self {
            addr,
            admin,
            dir,
            store: Some(store),
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    /// Fetches an admin endpoint, returning `(status code, body)`.
    async fn admin(&self, path: &str) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = self.admin.expect("admin endpoint not bound");
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connecting");
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
            .await
            .expect("writing");

        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("reading");
        let text = String::from_utf8(raw).expect("responses are utf-8");

        let code = text
            .split(' ')
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or_else(|| panic!("no status code in {text:?}"));
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (code, body)
    }

    async fn client(&self) -> Client {
        Client::connect(self.addr).await.expect("connecting")
    }

    fn db_path(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    /// A live key's deadline in unix milliseconds. `None` if it is absent,
    /// `Some(NEVER)` if it has no expiry.
    ///
    /// Read from the store because VCP does not report a remaining lifetime on
    /// the wire — the memcached `t` flag is the only way to ask over a socket.
    fn deadline_ms(&self, key: &[u8]) -> Option<u64> {
        self.store
            .as_ref()
            .expect("store handle released")
            .get(vash_core::Key::new(key).expect("a valid key"))
            .expect("reading the key")
            .and_then(|value| value.expires_at_ms)
    }

    /// Records currently on disk, including any the sweeper has not reached.
    fn entries(&self) -> u64 {
        self.store
            .as_ref()
            .expect("store handle released")
            .stats()
            .expect("reading store stats")
            .entries
    }

    /// Stops the server and waits for it to release the database.
    async fn stop(mut self) -> TempDir {
        // Must go first: the server closes the LMDB environment on shutdown,
        // and cannot while this handle is alive.
        drop(self.store.take());
        drop(self.shutdown.take());
        self.handle
            .take()
            .unwrap()
            .await
            .expect("server task panicked")
            .expect("server returned an error");
        self.dir
    }
}

/// A set of nodes that know about each other.
///
/// Peers are configured before a node binds, so the addresses have to be known
/// in advance. Reserving them by binding and immediately releasing leaves a
/// window in which something else could take one — but the alternative is
/// hard-coded ports that collide with whatever else is running on the machine,
/// which is worse and fails more often.
struct TestCluster {
    addrs: Vec<SocketAddr>,
    nodes: Vec<Option<TestServer>>,
}

impl TestCluster {
    async fn start(size: usize) -> Self {
        Self::start_with(size, |_| {}).await
    }

    async fn start_with(size: usize, tweak: impl Fn(&mut Config) + Copy) -> Self {
        let mut held = Vec::new();
        for _ in 0..size {
            held.push(
                tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("reserving a port"),
            );
        }
        let addrs: Vec<SocketAddr> = held
            .iter()
            .map(|l| l.local_addr().expect("reading a reserved port"))
            .collect();
        drop(held);

        let mut cluster = Self {
            addrs,
            nodes: Vec::new(),
        };
        for index in 0..size {
            let dir = tempfile::tempdir().unwrap();
            let node = cluster.spawn(index, dir, tweak).await;
            cluster.nodes.push(Some(node));
        }
        cluster
    }

    async fn spawn(&self, index: usize, dir: TempDir, tweak: impl Fn(&mut Config)) -> TestServer {
        let listen = self.addrs[index];
        let peers: Vec<String> = self
            .addrs
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, addr)| addr.to_string())
            .collect();

        TestServer::start_with(dir, |config| {
            config.server.listen = listen;
            config.cluster.peers = peers;
            // Far tighter than the 5s default so a test can watch anti-entropy
            // work rather than wait for it.
            config.cluster.gossip_interval_ms = 100;
            config.cluster.fanout_timeout_ms = 1_000;
            tweak(config);
        })
        .await
    }

    fn node(&self, index: usize) -> &TestServer {
        self.nodes[index].as_ref().expect("node is stopped")
    }

    async fn client(&self, index: usize) -> Client {
        self.node(index).client().await
    }

    /// A client that authenticates, for the clustered-with-auth test.
    async fn client_with(&self, index: usize, credential: &vash_client::Credential) -> Client {
        Client::connect_with(self.node(index).addr, credential)
            .await
            .expect("connecting")
    }

    /// Stops one node, keeping its database so it can come back.
    async fn stop(&mut self, index: usize) -> TempDir {
        self.nodes[index]
            .take()
            .expect("node already stopped")
            .stop()
            .await
    }

    async fn restart(&mut self, index: usize, dir: TempDir) {
        self.nodes[index] = Some(self.spawn(index, dir, |_| {}).await);
    }

    /// Waits for `check` to hold on every running node.
    async fn wait_for(&self, what: &str, mut check: impl AsyncFnMut(&TestServer) -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let mut all = true;
            for node in self.nodes.iter().flatten() {
                if !check(node).await {
                    all = false;
                    break;
                }
            }
            if all {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    async fn shutdown(mut self) {
        for index in 0..self.nodes.len() {
            if self.nodes[index].is_some() {
                self.stop(index).await;
            }
        }
    }
}

/// The key node `index` owns in a cluster test.
///
/// Clients shard the keyspace themselves, so in a real deployment a given key
/// lives on exactly one node. These tests imitate that: each node holds its own
/// key under a shared tag, which is precisely the case a single-node
/// invalidation would get wrong.
fn cluster_key(index: usize) -> String {
    format!("node{index}-article")
}

#[tokio::test]
async fn invalidation_converges_across_a_three_node_cluster() {
    // The M5 exit criterion. A tag's keys are spread across every node, so an
    // invalidation sent to one node has to reach the rest or most of the
    // affected keys keep being served.
    let cluster = TestCluster::start(3).await;

    for index in 0..3 {
        let mut client = cluster.client(index).await;
        client
            .set_tagged(cluster_key(index).as_bytes(), b"v", 0, &[b"news"])
            .await
            .unwrap();
    }

    // Written on every node, and every node is serving its own key.
    for index in 0..3 {
        let mut client = cluster.client(index).await;
        assert!(
            client
                .get(cluster_key(index).as_bytes())
                .await
                .unwrap()
                .is_some()
        );
    }

    // One client, one node, one invalidation.
    let mut client = cluster.client(0).await;
    assert!(client.delete_by_tag(b"news").await.unwrap());

    cluster
        .wait_for(
            "every node to stop serving the invalidated tag",
            async |node| {
                let mut client = node.client().await;
                let mut gone = true;
                for index in 0..3 {
                    gone &= client
                        .get(cluster_key(index).as_bytes())
                        .await
                        .unwrap()
                        .is_none();
                }
                gone
            },
        )
        .await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn fanout_sync_invalidates_peers_before_it_answers() {
    // What the mode buys over `fanout`: no polling here, because the
    // acknowledgement is what the reply waited for.
    let cluster = TestCluster::start_with(2, |config| {
        config.cluster.delete_by_tag = vash_server::config::FanoutMode::FanoutSync;
        // Long enough that gossip cannot be what makes this pass.
        config.cluster.gossip_interval_ms = 60_000;
    })
    .await;

    let mut peer = cluster.client(1).await;
    peer.set_tagged(b"k", b"v", 0, &[b"news"]).await.unwrap();

    // Give the peer connection a moment to exist; a first fan-out that has to
    // connect is still synchronous, this just keeps the assertion about the
    // mode rather than about connection setup.
    let mut origin = cluster.client(0).await;
    origin
        .set_tagged(b"local", b"v", 0, &[b"news"])
        .await
        .unwrap();
    assert!(origin.delete_by_tag(b"news").await.unwrap());

    assert!(
        peer.get(b"k").await.unwrap().is_none(),
        "fanout_sync must not answer before reachable peers have applied it"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_restarted_node_catches_up_on_invalidations_it_missed() {
    // The other half of the exit criterion. Fan-out cannot reach a node that is
    // down, so anti-entropy has to — and it has to work from the restarted
    // node's own persisted state, which knows nothing of what it missed.
    let mut cluster = TestCluster::start(3).await;

    for index in 0..3 {
        let mut client = cluster.client(index).await;
        client
            .set_tagged(cluster_key(index).as_bytes(), b"v", 0, &[b"news"])
            .await
            .unwrap();
    }

    // Node 2 goes away, so the invalidation below cannot reach it.
    let dir = cluster.stop(2).await;

    let mut client = cluster.client(0).await;
    assert!(client.delete_by_tag(b"news").await.unwrap());

    cluster
        .wait_for("the reachable nodes to converge", async |node| {
            let mut client = node.client().await;
            client
                .get(cluster_key(0).as_bytes())
                .await
                .unwrap()
                .is_none()
        })
        .await;

    cluster.restart(2, dir).await;

    // It comes back still serving stale data, and must shed it without anyone
    // touching the key.
    cluster
        .wait_for("the restarted node to catch up", async |node| {
            let mut client = node.client().await;
            client
                .get(cluster_key(2).as_bytes())
                .await
                .unwrap()
                .is_none()
        })
        .await;

    // And it is a full member again: a new invalidation reaches it directly.
    let mut restarted = cluster.client(2).await;
    restarted
        .set_tagged(b"fresh", b"v", 0, &[b"sport"])
        .await
        .unwrap();

    let mut origin = cluster.client(0).await;
    origin
        .set_tagged(b"other", b"v", 0, &[b"sport"])
        .await
        .unwrap();
    origin.delete_by_tag(b"sport").await.unwrap();

    cluster
        .wait_for(
            "the restarted node to receive a new invalidation",
            async |node| {
                let mut client = node.client().await;
                client.get(b"fresh").await.unwrap().is_none()
            },
        )
        .await;

    cluster.shutdown().await;
}

/// The other half of the M9 exit criterion: a cluster that authenticates still
/// converges.
///
/// Peers reach each other over the cache port as ordinary VCP clients, so this
/// is the test that would catch fan-out and gossip being silently refused —
/// the failure mode where writes keep working, invalidations stop propagating,
/// and every node goes on reporting itself healthy.
#[tokio::test]
async fn invalidation_converges_across_a_cluster_that_authenticates() {
    let shared = tempfile::tempdir().unwrap();
    let (secret, line) = vash_server::auth::generate("peer").unwrap();
    let credentials = shared.path().join("credentials");
    std::fs::write(&credentials, format!("{line}\n")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let credentials = &credentials;
    let secret = &secret;
    let cluster = TestCluster::start_with(3, |config| {
        config.auth.required = true;
        config.auth.file = credentials.clone();
        config.cluster.auth_name = "peer".into();
        config.cluster.auth_secret = secret.clone();
        // One environment per node rather than one per shard: this test is
        // about the peer credential, and the suite runs in parallel against a
        // finite pool of LMDB thread-local slots.
        config.store.shards = 1;
    })
    .await;

    let credential = vash_client::Credential::new("peer", secret.clone());

    for index in 0..3 {
        cluster
            .client_with(index, &credential)
            .await
            .set_tagged(cluster_key(index).as_bytes(), b"v", 0, &[b"news"])
            .await
            .unwrap();
    }

    assert!(
        cluster
            .client_with(0, &credential)
            .await
            .delete_by_tag(b"news")
            .await
            .unwrap()
    );

    cluster
        .wait_for(
            "every authenticating node to stop serving the invalidated tag",
            async |node| {
                let mut client = Client::connect_with(node.addr, &credential).await.unwrap();
                let mut gone = true;
                for index in 0..3 {
                    gone &= client
                        .get(cluster_key(index).as_bytes())
                        .await
                        .unwrap()
                        .is_none();
                }
                gone
            },
        )
        .await;

    cluster.shutdown().await;
}

/// A plain `connect` against a server that requires authentication fails at the
/// handshake rather than on the first command, so a misconfiguration reads as
/// one clear error instead of every request coming back `UNAUTHORIZED`.
#[tokio::test]
async fn an_unauthenticated_client_is_told_at_connect_time() {
    let dir = tempfile::tempdir().unwrap();
    let (secret, line) = vash_server::auth::generate("app").unwrap();
    let credentials = dir.path().join("credentials");
    std::fs::write(&credentials, format!("{line}\n")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let server = TestServer::start_with(dir, |config| {
        config.auth.required = true;
        config.auth.file = credentials.clone();
        config.store.shards = 1;
    })
    .await;

    let Err(error) = Client::connect(server.addr).await else {
        panic!("connecting without a credential must fail at the handshake");
    };
    assert!(
        matches!(error, ClientError::Protocol(detail) if detail.contains("requires authentication")),
        "unexpected error: {error}"
    );

    // And the same address works with a credential.
    let credential = vash_client::Credential::new("app", secret);
    let mut client = Client::connect_with(server.addr, &credential)
        .await
        .unwrap();
    client.set(b"k", b"v", 0).await.unwrap();
}

#[tokio::test]
async fn a_write_after_an_acknowledged_invalidation_survives_convergence() {
    // The dangerous failure mode of a max-merge counter: a node learning a
    // higher generation must not take down records written *after* the
    // invalidation that produced it.
    //
    // `fanout_sync` is what makes this checkable across nodes. Under `fanout`
    // the invalidation may still be in flight when the write lands on another
    // node, and that write is then treated as pre-invalidation and dropped when
    // the message arrives — the documented staleness window, which errs towards
    // a miss.
    let cluster = TestCluster::start_with(2, |config| {
        config.cluster.delete_by_tag = vash_server::config::FanoutMode::FanoutSync;
    })
    .await;

    let mut origin = cluster.client(0).await;
    origin.set_tagged(b"a", b"1", 0, &[b"news"]).await.unwrap();
    origin.delete_by_tag(b"news").await.unwrap();

    // The reply is back, so every reachable peer has applied it — including the
    // one that had never heard of the tag, which registered it at the
    // invalidated generation rather than at zero.
    let mut peer = cluster.client(1).await;
    peer.set_tagged(b"b", b"2", 0, &[b"news"]).await.unwrap();

    // Several gossip intervals: whatever the nodes go on to exchange, this
    // stands.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        peer.get(b"b").await.unwrap().map(|v| v.data.to_vec()),
        Some(b"2".to_vec()),
        "a write after the invalidation captured the new generation and is live"
    );
    assert!(
        origin.get(b"a").await.unwrap().is_none(),
        "and the invalidation itself still holds"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn the_cluster_opcode_reports_the_peer_list() {
    let cluster = TestCluster::start(2).await;
    let mut client = cluster.client(0).await;

    let view = client.cluster().await.unwrap();
    assert_eq!(view.mode, vash_core::ClusterMode::Fanout);
    assert_eq!(view.peers.len(), 1);
    assert_eq!(view.peers[0].addr, cluster.addrs[1].to_string());

    // The capability is claimed only because peers are actually configured.
    assert_eq!(
        client.server_info().capabilities & vash_core::capability::CLUSTER,
        vash_core::capability::CLUSTER
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn a_standalone_node_reports_no_cluster_and_claims_no_capability() {
    // Claiming the capability would make a client trust a cluster-wide
    // invalidation that in fact stops here.
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let view = client.cluster().await.unwrap();
    assert!(view.peers.is_empty());
    assert_eq!(
        client.server_info().capabilities & vash_core::capability::CLUSTER,
        0
    );
}

#[tokio::test]
async fn a_node_accepts_invalidations_without_listing_any_peers_itself() {
    // Membership is one-sided: peers are configured on the sending node, so a
    // node that lists nobody can still be somebody else's fan-out target.
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set_tagged(b"k", b"v", 0, &[b"news"]).await.unwrap();
    let learned = client.tag_sync(false, &[(b"news", 5)]).await.unwrap();

    assert!(
        learned.is_empty(),
        "the sender was ahead, so there is nothing to send back: {learned:?}"
    );
    assert!(client.get(b"k").await.unwrap().is_none());
}

#[tokio::test]
async fn a_digest_exchange_reports_what_the_sender_is_behind_on() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set_tagged(b"k", b"v", 0, &[b"news"]).await.unwrap();
    client.delete_by_tag(b"news").await.unwrap();

    // A partial push answers only about the names it named...
    let behind = client.tag_sync(false, &[(b"news", 0)]).await.unwrap();
    assert_eq!(behind.len(), 1);
    assert_eq!(&*behind[0].name, b"news");
    assert_eq!(behind[0].generation, 1);

    // ...while a full digest may also volunteer tags the sender never mentioned,
    // which is what lets a node that knows nothing catch up in one round.
    let behind = client.tag_sync(true, &[]).await.unwrap();
    assert_eq!(behind.len(), 1);
    assert_eq!(&*behind[0].name, b"news");
}

#[tokio::test]
async fn handshake_reports_server_limits() {
    let server = TestServer::start().await;
    let client = server.client().await;

    let info = client.server_info();
    assert_eq!(info.protocol_version, vash_core::PROTOCOL_VERSION);
    assert_eq!(info.max_key_len, vash_core::MAX_KEY_LEN as u32);
    assert_eq!(info.max_value_len, vash_core::DEFAULT_MAX_VALUE_LEN as u32);
    // Capabilities are advertised only as each milestone lands: cluster
    // invalidation (M5) is not claimed yet.
    assert_eq!(
        info.capabilities,
        vash_core::capability::TAGS | vash_core::capability::MEMCACHED
    );
}

#[tokio::test]
async fn ping_round_trips() {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    client.ping().await.expect("ping");
}

#[tokio::test]
async fn set_then_get_returns_the_value() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let cas = client.set(b"greeting", b"hello world", 0).await.unwrap();
    assert!(cas > 0, "cas tokens start at 1");

    let value = client.get(b"greeting").await.unwrap().expect("a hit");
    assert_eq!(&value.data[..], b"hello world");
    assert_eq!(value.cas, cas);
}

#[tokio::test]
async fn get_of_an_absent_key_is_a_miss() {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    assert!(client.get(b"never-written").await.unwrap().is_none());
}

#[tokio::test]
async fn overwrite_replaces_the_value_and_advances_cas() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let first = client.set(b"k", b"one", 0).await.unwrap();
    let second = client.set(b"k", b"two", 0).await.unwrap();
    assert!(second > first, "cas must advance on overwrite");

    let value = client.get(b"k").await.unwrap().unwrap();
    assert_eq!(&value.data[..], b"two");
    assert_eq!(value.cas, second);
}

#[tokio::test]
async fn delete_reports_whether_the_key_was_live() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"k", b"v", 0).await.unwrap();
    assert!(client.delete(b"k").await.unwrap(), "first delete hits");
    assert!(!client.delete(b"k").await.unwrap(), "second delete misses");
    assert!(client.get(b"k").await.unwrap().is_none());
}

#[tokio::test]
async fn values_and_keys_are_binary_safe() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let key = [0x00u8, 0xff, b' ', b'\r', b'\n', 0x80];
    let value = (0..=255u8).collect::<Vec<_>>();

    client.set(&key, &value, 0).await.unwrap();
    let got = client.get(&key).await.unwrap().unwrap();
    assert_eq!(&got.data[..], &value[..]);
}

#[tokio::test]
async fn empty_value_round_trips() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"empty", b"", 0).await.unwrap();
    let got = client
        .get(b"empty")
        .await
        .unwrap()
        .expect("a hit, not a miss");
    assert!(got.data.is_empty());
}

#[tokio::test]
async fn maximum_length_key_is_accepted() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let key = vec![b'k'; vash_core::MAX_KEY_LEN];
    client.set(&key, b"v", 0).await.unwrap();
    assert!(client.get(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn oversized_key_is_rejected_without_closing_the_connection() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let key = vec![b'k'; vash_core::MAX_KEY_LEN + 1];
    assert!(matches!(
        client.get(&key).await,
        Err(ClientError::Status(Status::TooLarge))
    ));

    // The connection must survive a rejected request.
    client.ping().await.expect("connection still usable");
}

#[tokio::test]
async fn oversized_value_is_rejected() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let value = vec![0u8; vash_core::DEFAULT_MAX_VALUE_LEN + 1];
    assert!(matches!(
        client.set(b"big", &value, 0).await,
        Err(ClientError::Status(Status::TooLarge))
    ));
    client.ping().await.expect("connection still usable");
}

#[tokio::test]
async fn empty_key_is_rejected() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    assert!(matches!(
        client.get(b"").await,
        Err(ClientError::Status(Status::BadRequest))
    ));
    client.ping().await.expect("connection still usable");
}

#[tokio::test]
async fn tagged_writes_round_trip() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .set_tagged(b"k", b"v", 0, &[b"alpha", b"beta"])
        .await
        .unwrap();

    let got = client.get(b"k").await.unwrap().expect("a hit");
    assert_eq!(&got.data[..], b"v");
}

#[tokio::test]
async fn delete_by_tag_invalidates_over_the_wire() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set_tagged(b"a", b"1", 0, &[b"news"]).await.unwrap();
    client
        .set_tagged(b"b", b"2", 0, &[b"news", b"sport"])
        .await
        .unwrap();
    client.set_tagged(b"c", b"3", 0, &[b"sport"]).await.unwrap();
    client.set(b"plain", b"4", 0).await.unwrap();

    assert!(client.delete_by_tag(b"news").await.unwrap());

    assert!(client.get(b"a").await.unwrap().is_none());
    assert!(
        client.get(b"b").await.unwrap().is_none(),
        "one dead tag is enough to kill a record"
    );
    assert!(client.get(b"c").await.unwrap().is_some());
    assert!(client.get(b"plain").await.unwrap().is_some());
}

#[tokio::test]
async fn delete_by_tag_reports_an_unknown_tag_as_a_miss() {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    assert!(!client.delete_by_tag(b"never-used").await.unwrap());
}

#[tokio::test]
async fn an_empty_or_oversized_tag_is_rejected() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    assert!(matches!(
        client.delete_by_tag(b"").await,
        Err(ClientError::Status(Status::BadRequest))
    ));

    let long = vec![b't'; vash_core::MAX_TAG_LEN + 1];
    assert!(matches!(
        client.delete_by_tag(&long).await,
        Err(ClientError::Status(Status::TooLarge))
    ));

    client.ping().await.expect("connection still usable");
}

#[tokio::test]
async fn the_handshake_advertises_tag_support() {
    let server = TestServer::start().await;
    let client = server.client().await;
    assert_eq!(
        client.server_info().capabilities & vash_core::capability::TAGS,
        vash_core::capability::TAGS
    );
}

#[tokio::test]
async fn flush_is_refused_unless_enabled() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"k", b"v", 0).await.unwrap();

    // A remote cache-wipe primitive must not be available by default.
    assert!(matches!(
        client.flush().await,
        Err(ClientError::Status(Status::Unauthorized))
    ));
    assert!(
        client.get(b"k").await.unwrap().is_some(),
        "data must be untouched"
    );
}

#[tokio::test]
async fn flush_empties_the_cache_when_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(dir, |config| config.protocol.flush_enabled = true).await;
    let mut client = server.client().await;

    client.set(b"a", b"1", 0).await.unwrap();
    client.set_tagged(b"b", b"2", 0, &[b"t"]).await.unwrap();

    let epoch = client.flush().await.unwrap();
    assert!(epoch > 0);

    assert!(client.get(b"a").await.unwrap().is_none());
    assert!(client.get(b"b").await.unwrap().is_none());
    assert_eq!(
        server.entries(),
        0,
        "flush must free the space, not just hide it"
    );

    client.set(b"c", b"3", 0).await.unwrap();
    assert!(client.get(b"c").await.unwrap().is_some());
}

#[tokio::test]
async fn the_server_reclaims_invalidated_records_in_the_background() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    for i in 0..100u32 {
        client
            .set_tagged(format!("k{i}").as_bytes(), b"v", 0, &[b"bulk"])
            .await
            .unwrap();
    }
    client.delete_by_tag(b"bulk").await.unwrap();

    // Nothing reads these and none has a TTL, so only the tag reclaimer can
    // free them.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while server.entries() > 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(server.entries(), 0);
}

#[tokio::test]
async fn many_keys_round_trip_on_one_connection() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    for i in 0..500u32 {
        client
            .set(
                format!("key{i}").as_bytes(),
                format!("value{i}").as_bytes(),
                0,
            )
            .await
            .unwrap();
    }
    for i in 0..500u32 {
        let got = client
            .get(format!("key{i}").as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&got.data[..], format!("value{i}").as_bytes());
    }
}

#[tokio::test]
async fn concurrent_clients_do_not_interfere() {
    let server = TestServer::start().await;

    let mut tasks = Vec::new();
    for worker in 0..8u32 {
        let addr = server.addr;
        tasks.push(tokio::spawn(async move {
            let mut client = Client::connect(addr).await.unwrap();
            for i in 0..50u32 {
                let key = format!("w{worker}-k{i}");
                let value = format!("w{worker}-v{i}");
                client
                    .set(key.as_bytes(), value.as_bytes(), 0)
                    .await
                    .unwrap();
                let got = client.get(key.as_bytes()).await.unwrap().unwrap();
                assert_eq!(&got.data[..], value.as_bytes());
            }
        }));
    }

    for task in tasks {
        task.await.expect("worker panicked");
    }
}

#[tokio::test]
async fn data_survives_a_restart() {
    let server = TestServer::start().await;
    let db_path = server.db_path();

    let mut client = server.client().await;
    client.set(b"durable", b"value", 0).await.unwrap();
    let cas = client.get(b"durable").await.unwrap().unwrap().cas;
    drop(client);

    let dir = server.stop().await;

    let restarted = TestServer::start_in(dir).await;
    assert_eq!(
        restarted.db_path(),
        db_path,
        "must reopen the same database"
    );

    let mut client = restarted.client().await;
    let got = client
        .get(b"durable")
        .await
        .unwrap()
        .expect("value survived");
    assert_eq!(&got.data[..], b"value");

    // CAS must never go backwards across a restart, or a client's compare-and-swap
    // could succeed against a stale token.
    let new_cas = client.set(b"other", b"v", 0).await.unwrap();
    assert!(new_cas > cas, "cas {new_cas} must exceed pre-restart {cas}");
}

#[tokio::test]
async fn a_value_with_a_ttl_expires() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"short", b"v", 1).await.unwrap();
    assert!(
        client.get(b"short").await.unwrap().is_some(),
        "live before the ttl"
    );

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    // The sweeper does not exist until M1; this must be the lazy read-path check
    // refusing to serve an expired record.
    assert!(
        client.get(b"short").await.unwrap().is_none(),
        "expired after the ttl"
    );
}

#[tokio::test]
async fn a_zero_ttl_never_expires() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"forever", b"v", 0).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(client.get(b"forever").await.unwrap().is_some());
}

#[tokio::test]
async fn deleting_an_expired_key_reports_a_miss() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"gone", b"v", 1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    // Present on disk but logically absent, so the delete is a miss even though
    // it reclaims the row.
    assert!(!client.delete(b"gone").await.unwrap());
}

#[tokio::test]
async fn touch_extends_a_lifetime_over_the_wire() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"k", b"payload", 1).await.unwrap();
    assert!(client.touch(b"k", 3600).await.unwrap());

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let got = client
        .get(b"k")
        .await
        .unwrap()
        .expect("survived its original ttl");
    assert_eq!(&got.data[..], b"payload");
}

/// memcached reads an `exptime` past 30 days as an absolute unix timestamp.
/// VCP does not: its `ttl_secs` is an offset at every magnitude, so a TTL a
/// client would plausibly ask for — a quarter, a year — must not be read back
/// as a date in 1970 and expire the value on arrival.
#[tokio::test]
async fn a_vcp_ttl_past_thirty_days_is_still_an_offset() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    const NINETY_DAYS: u32 = 90 * 24 * 60 * 60;
    let before = vash_core::Clock::new().now_ms();
    client.set(b"quarter", b"v", NINETY_DAYS).await.unwrap();

    assert!(
        client.get(b"quarter").await.unwrap().is_some(),
        "a 90-day value must survive being written"
    );

    let deadline = server.deadline_ms(b"quarter").expect("a live key");
    let expected = before + NINETY_DAYS as u64 * 1_000;
    assert!(
        deadline >= expected && deadline <= expected + 2_000,
        "expected roughly 90 days out ({expected}), got {deadline}"
    );

    // And `TOUCH` reads its TTL the same way, or a long-lived key could not be
    // extended without resending the value.
    client.touch(b"quarter", NINETY_DAYS * 2).await.unwrap();
    let extended = server.deadline_ms(b"quarter").expect("still live");
    assert!(
        extended > deadline,
        "touch should have pushed the deadline out, got {extended} from {deadline}"
    );
}

#[tokio::test]
async fn touch_misses_on_an_absent_key() {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    assert!(!client.touch(b"absent", 60).await.unwrap());
}

#[tokio::test]
async fn get_many_returns_one_slot_per_key_in_order() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"a", b"1", 0).await.unwrap();
    client.set(b"c", b"3", 0).await.unwrap();

    let values = client
        .get_many(&[b"a".as_slice(), b"missing", b"c"])
        .await
        .unwrap();

    assert_eq!(values.len(), 3);
    assert_eq!(&values[0].as_ref().unwrap().data[..], b"1");
    assert!(values[1].is_none(), "a miss must keep its position");
    assert_eq!(&values[2].as_ref().unwrap().data[..], b"3");
}

#[tokio::test]
async fn get_many_of_an_empty_list_is_an_empty_result() {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    assert!(client.get_many(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn set_many_then_get_many_round_trips() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let items: Vec<(&[u8], &[u8], u32)> = vec![
        (b"k1", b"v1", 0),
        (b"k2", b"v2", 3600),
        (b"k3", b"", 0), // empty values must survive a batch too
    ];
    let cas = client.set_many(&items).await.unwrap();
    assert_eq!(cas.len(), 3);
    // Tokens are unique server-wide, but not ordered across a batch: the keys
    // land in different shards and each shard counts independently. Ordering
    // holds per key, which is all compare-and-swap depends on.
    let unique: std::collections::HashSet<_> = cas.iter().collect();
    assert_eq!(
        unique.len(),
        cas.len(),
        "cas tokens must be unique: {cas:?}"
    );

    let values = client
        .get_many(&[b"k1".as_slice(), b"k2", b"k3"])
        .await
        .unwrap();
    assert_eq!(&values[0].as_ref().unwrap().data[..], b"v1");
    assert_eq!(&values[1].as_ref().unwrap().data[..], b"v2");
    assert!(values[2].as_ref().unwrap().data.is_empty());
}

#[tokio::test]
async fn delete_many_reports_each_key() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"live", b"v", 0).await.unwrap();
    let hits = client
        .delete_many(&[b"live".as_slice(), b"absent"])
        .await
        .unwrap();

    assert_eq!(hits, vec![true, false]);
    assert!(client.get(b"live").await.unwrap().is_none());
}

#[tokio::test]
async fn an_oversized_batch_is_rejected_without_closing_the_connection() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let keys: Vec<&[u8]> = vec![b"k"; vash_core::MAX_BATCH_ITEMS + 1];
    assert!(matches!(
        client.get_many(&keys).await,
        Err(ClientError::Status(Status::BadRequest))
    ));

    client.ping().await.expect("connection still usable");
}

#[tokio::test]
async fn a_batch_containing_a_bad_key_is_rejected_whole() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"good", b"v", 0).await.unwrap();

    let oversized = vec![b'k'; vash_core::MAX_KEY_LEN + 1];
    assert!(matches!(
        client.get_many(&[b"good".as_slice(), &oversized]).await,
        Err(ClientError::Status(Status::TooLarge))
    ));

    client.ping().await.expect("connection still usable");
}

#[tokio::test]
async fn the_server_reclaims_expired_keys_in_the_background() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    for i in 0..50u32 {
        client
            .set(format!("k{i}").as_bytes(), b"v", 1)
            .await
            .unwrap();
    }
    assert_eq!(server.entries(), 50);

    // Nothing here reads the expired keys, so only the sweeper can remove them.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while server.entries() > 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        server.entries(),
        0,
        "the sweeper must reclaim without any read touching the keys"
    );
}

#[tokio::test]
async fn metrics_report_what_actually_happened() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    client.set(b"hit-me", b"v", 0).await.unwrap();
    client.get(b"hit-me").await.unwrap();
    client.get(b"absent").await.unwrap();

    let (code, body) = server.admin("/metrics").await;
    assert_eq!(code, 200);

    // Prometheus rejects a sample whose family has no TYPE line. A histogram
    // declares its type once, on the family — `_bucket`, `_sum` and `_count`
    // belong to it and carry none of their own — so the suffix is stripped
    // rather than the series exempted, which would lose the check for them.
    for line in body
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let series = line.split(['{', ' ']).next().unwrap();
        let family = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suffix| series.strip_suffix(suffix))
            .filter(|family| body.contains(&format!("# TYPE {family} histogram")))
            .unwrap_or(series);
        assert!(
            body.contains(&format!("# TYPE {family} ")),
            "{series} has no TYPE line"
        );
    }

    assert!(body.contains("vash_hits_total 1"), "{body}");
    assert!(body.contains("vash_misses_total 1"), "{body}");
    assert!(body.contains("vash_writes_total 1"), "{body}");
    assert!(body.contains("vash_connections_active 1"), "{body}");
    assert!(body.contains("vash_shards "), "{body}");
}

#[tokio::test]
async fn health_reports_ok_while_serving() {
    let server = TestServer::start().await;
    let (code, body) = server.admin("/health").await;
    assert_eq!(code, 200);
    assert_eq!(body, "ok\n");
}

#[tokio::test]
async fn stats_are_json_and_carry_the_shard_count() {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    client.set(b"k", b"v", 0).await.unwrap();

    let (code, body) = server.admin("/stats").await;
    assert_eq!(code, 200);
    assert!(body.trim_start().starts_with('{'), "{body}");
    assert!(body.contains("\"shards\":"), "{body}");
    assert!(body.contains("\"pressure\": \"normal\""), "{body}");
    assert!(body.contains("\"items\": 1"), "{body}");
}

#[tokio::test]
async fn unknown_admin_routes_and_methods_are_refused() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestServer::start().await;
    assert_eq!(server.admin("/nope").await.0, 404);

    let mut stream = tokio::net::TcpStream::connect(server.admin.unwrap())
        .await
        .unwrap();
    stream
        .write_all(b"POST /metrics HTTP/1.1\r\nHost: t\r\n\r\n")
        .await
        .unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    assert!(raw.starts_with("HTTP/1.1 405"), "{raw}");
}

#[tokio::test]
async fn keys_spread_across_shards_and_all_remain_readable() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(dir, |config| config.store.shards = 4).await;
    let mut client = server.client().await;

    assert_eq!(
        client.server_info().shards,
        4,
        "the handshake advertises the real count"
    );

    for i in 0..300u32 {
        client
            .set(format!("k{i}").as_bytes(), format!("v{i}").as_bytes(), 0)
            .await
            .unwrap();
    }

    // Through a multi-get, which is the path that fans out and reassembles.
    let keys: Vec<String> = (0..300).map(|i| format!("k{i}")).collect();
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
    let values = client.get_many(&refs).await.unwrap();

    for (i, value) in values.iter().enumerate() {
        assert_eq!(
            value.as_ref().map(|v| v.data.to_vec()).as_deref(),
            Some(format!("v{i}").as_bytes()),
            "k{i} came back wrong"
        );
    }
}

#[tokio::test]
async fn garbage_input_closes_the_connection_without_killing_the_server() {
    use tokio::io::AsyncWriteExt;

    let server = TestServer::start().await;

    // A frame header claiming an absurd body length.
    let mut stream = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let mut frame = vec![0x10u8, 0, 0, 0, 1, 0, 0, 0];
    frame.extend_from_slice(&u32::MAX.to_le_bytes());
    let _ = stream.write_all(&frame).await;
    drop(stream);

    // The server must still be serving everyone else.
    let mut client = server.client().await;
    client
        .ping()
        .await
        .expect("server survived a hostile client");
}

// ---- listing ---------------------------------------------------------------

/// Pages a listing to exhaustion over the wire at a **caller-chosen page size**.
///
/// Deliberately not `Client::list_all_keys`, which always asks for the largest
/// page the server allows: at that size these keyspaces come back in one reply
/// and the cursor is never exercised. Small limits are the whole point here.
/// The iteration bound turns "the cursor stopped advancing" into a failing test
/// rather than a hung one.
async fn page_all(
    client: &mut Client,
    keys: bool,
    limit: u32,
    pattern: &[u8],
) -> Vec<(Vec<u8>, u64)> {
    let mut out = Vec::new();
    let mut cursor: Vec<u8> = Vec::new();
    for _ in 0..1_000 {
        let page = if keys {
            client.list_keys(limit, &cursor, pattern).await.unwrap()
        } else {
            client.list_tags(limit, &cursor, pattern).await.unwrap()
        };
        out.extend(page.entries.iter().map(|e| (e.name.to_vec(), e.version)));
        match page.cursor {
            Some(next) => cursor = next.to_vec(),
            None => return out,
        }
    }
    panic!("listing did not terminate");
}

#[tokio::test]
async fn listing_is_refused_unless_enabled() {
    let server = TestServer::start().await;
    let mut client = server.client().await;
    client.set(b"k", b"v", 0).await.unwrap();

    // Enumerating the cache is not available by default, and the capability
    // bit says so rather than making a client discover it by being refused.
    assert_eq!(
        client.server_info().capabilities & vash_core::capability::LISTING,
        0
    );
    assert!(matches!(
        client.list_keys(10, b"", b"").await,
        Err(ClientError::Status(Status::Unauthorized))
    ));
    assert!(matches!(
        client.list_tags(10, b"", b"").await,
        Err(ClientError::Status(Status::Unauthorized))
    ));
}

#[tokio::test]
async fn listing_pages_the_keyspace_over_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(dir, |config| {
        config.protocol.listing_enabled = true;
        config.store.shards = 4;
    })
    .await;
    let mut client = server.client().await;

    assert_ne!(
        client.server_info().capabilities & vash_core::capability::LISTING,
        0,
        "the capability bit reports enablement"
    );

    for i in 0..100 {
        client
            .set(format!("key:{i:03}").as_bytes(), b"v", 0)
            .await
            .unwrap();
    }

    // A page size that does not divide the keyspace, across four shards, so a
    // cursor mistake at a boundary has somewhere to hide.
    let listed = page_all(&mut client, true, 7, b"").await;
    let mut names: Vec<Vec<u8>> = listed.iter().map(|(n, _)| n.clone()).collect();
    assert_eq!(names.len(), 100, "no key returned twice");
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 100, "no key missed");

    // The version is the CAS token, which is what makes two listings diffable.
    let (name, version) = &listed[0];
    let value = client.get(name).await.unwrap().expect("live");
    assert_eq!(value.cas, *version);
}

#[tokio::test]
async fn a_listing_reflects_writes_and_invalidations() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(dir, |config| config.protocol.listing_enabled = true).await;
    let mut client = server.client().await;

    client.set_tagged(b"a", b"1", 0, &[b"news"]).await.unwrap();
    client.set(b"b", b"2", 0).await.unwrap();

    let names = |listed: Vec<(Vec<u8>, u64)>| {
        let mut n: Vec<Vec<u8>> = listed.into_iter().map(|(name, _)| name).collect();
        n.sort();
        n
    };

    assert_eq!(
        names(page_all(&mut client, true, 100, b"").await),
        vec![b"a".to_vec(), b"b".to_vec()]
    );

    // Invalidated keys stop being listed at the same moment they stop being
    // served, because the listing applies the same liveness rule.
    client.delete_by_tag(b"news").await.unwrap();
    assert!(client.get(b"a").await.unwrap().is_none());
    assert_eq!(
        names(page_all(&mut client, true, 100, b"").await),
        vec![b"b".to_vec()]
    );

    // The tag survives the invalidation, carrying its bumped generation.
    let tags = page_all(&mut client, false, 100, b"").await;
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].0, b"news".to_vec());
    assert!(tags[0].1 > 0, "generation bumped by the invalidation");
}

#[tokio::test]
async fn a_pattern_selects_a_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(dir, |config| config.protocol.listing_enabled = true).await;
    let mut client = server.client().await;

    for i in 0..10 {
        client
            .set(format!("session:{i}").as_bytes(), b"v", 0)
            .await
            .unwrap();
        client
            .set(format!("user:{i}").as_bytes(), b"v", 0)
            .await
            .unwrap();
    }

    let sessions = page_all(&mut client, true, 4, b"session:*").await;
    assert_eq!(sessions.len(), 10);
    assert!(sessions.iter().all(|(n, _)| n.starts_with(b"session:")));

    let single = page_all(&mut client, true, 4, b"user:?").await;
    assert_eq!(single.len(), 10, "one byte each");
}

#[tokio::test]
async fn malformed_listing_requests_are_refused_without_closing_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(dir, |config| config.protocol.listing_enabled = true).await;
    let mut client = server.client().await;
    client.set(b"k", b"v", 0).await.unwrap();

    // A limit outside the range is refused rather than clamped: a client that
    // asked for 10000 and silently got 1024 would page incorrectly.
    assert!(matches!(
        client.list_keys(0, b"", b"").await,
        Err(ClientError::Status(Status::BadRequest))
    ));
    assert!(matches!(
        client
            .list_keys(vash_core::MAX_LIST_LIMIT + 1, b"", b"")
            .await,
        Err(ClientError::Status(Status::BadRequest))
    ));

    // An unterminated escape names a byte that is not there.
    assert!(matches!(
        client.list_keys(10, b"", b"bad\\").await,
        Err(ClientError::Status(Status::BadRequest))
    ));

    // A fabricated cursor is refused rather than silently restarting the
    // listing, which would loop a pager forever.
    assert!(matches!(
        client.list_keys(10, &[0xff, 0xff, b'k'], b"").await,
        Err(ClientError::Status(Status::BadRequest))
    ));

    // Every one of those is recoverable: the connection carries on.
    assert!(client.get(b"k").await.unwrap().is_some());
    let listed = page_all(&mut client, true, 10, b"").await;
    assert_eq!(listed.len(), 1);
}

#[tokio::test]
async fn a_scan_budget_still_lets_a_listing_finish() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_with(dir, |config| {
        config.protocol.listing_enabled = true;
        // Absurdly small on purpose: nearly every page will stop on the budget
        // rather than on the limit, which is the case that must still converge.
        config.protocol.listing_max_scan = 3;
    })
    .await;
    let mut client = server.client().await;

    for i in 0..60 {
        client
            .set(format!("k{i:02}").as_bytes(), b"v", 0)
            .await
            .unwrap();
    }

    let listed = page_all(&mut client, true, 100, b"").await;
    assert_eq!(listed.len(), 60, "a truncated page still advances");

    let page = client.list_keys(100, b"", b"").await.unwrap();
    assert!(page.truncated, "and says that it was truncated");
    assert!(page.scanned <= 3);
}

/// The native protocol can increment a counter without falling back to a
/// compatibility dialect.
///
/// Until M10 phase 7 it could not: VCP had no arithmetic opcode at all, so a
/// first-party client had to speak memcached or Redis to move a number — the
/// wrong way round for the protocol the plan calls primary.
#[tokio::test]
async fn vcp_arithmetic_round_trips() {
    use vash_core::{Arithmetic, Delta, Missing, Number, OnBound, TtlChange};

    let server = TestServer::start().await;
    let mut client = server.client().await;

    // memcached's domain: never creates, so an absent key is a miss.
    let key = vash_core::Key::new(b"n").unwrap();
    assert!(
        client
            .arithmetic(&Arithmetic::counter(key, 1, false))
            .await
            .unwrap()
            .is_none(),
        "a counter must not create the key it did not find"
    );

    client.set(b"n", b"10", 0).await.unwrap();
    let applied = client
        .arithmetic(&Arithmetic::counter(key, 5, false))
        .await
        .unwrap()
        .expect("the key is live");
    assert_eq!(applied.value, Number::Counter(15));
    assert!(applied.wrote);

    // Redis's domain, over the native protocol: signed, and it creates.
    let fresh = vash_core::Key::new(b"signed").unwrap();
    let applied = client
        .arithmetic(&Arithmetic::redis(fresh, Delta::int(-7)))
        .await
        .unwrap()
        .expect("creates at zero");
    assert_eq!(applied.value, Number::Int(-7));

    // Floats survive the round trip as bits, not as text.
    let f = vash_core::Key::new(b"float").unwrap();
    let applied = client
        .arithmetic(&Arithmetic::redis(f, Delta::float(0.5)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.value, Number::Float(0.5));

    // Bounds and the skip policy, which only the native protocol and INCREX can
    // express.
    let bounded = Arithmetic {
        key: vash_core::Key::new(b"bounded").unwrap(),
        delta: Delta::Int {
            delta: 100,
            lower: 0,
            upper: 10,
        },
        on_bound: OnBound::Skip,
        missing: Missing::CreateAtZero,
        ttl: TtlChange::Keep,
    };
    let applied = client.arithmetic(&bounded).await.unwrap().unwrap();
    assert_eq!(applied.value, Number::Int(0), "the bound held it at zero");
    assert_eq!(applied.applied, Number::Int(0));
    assert!(!applied.wrote, "a skipped step writes nothing");
}
