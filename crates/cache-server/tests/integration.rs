//! End-to-end tests: a real server on a real socket, driven by the real client.
//!
//! Nothing here stubs the storage engine or the transport. A test that passes
//! against a fake proves the fake works.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use cache_client::{Client, ClientError};
use cache_proto::kcp::Status;
use cache_server::{Config, Server};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct TestServer {
    addr: SocketAddr,
    dir: TempDir,
    /// Held so tests can inspect on-disk state directly — `entries()` is the
    /// only way to tell the sweeper apart from the lazy read-path check.
    ///
    /// An `Option` because it must be released before the server shuts down:
    /// the environment cannot close while another handle is outstanding.
    store: Option<Arc<cache_store::LmdbStore>>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl TestServer {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("creating a temp dir");
        Self::start_in(dir).await
    }

    async fn start_in(dir: TempDir) -> Self {
        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.store.path = dir.path().join("db");
        // Small enough to be cheap in CI, large enough for these tests.
        config.store.map_size_mb = 64;

        let server = Server::bind(config).await.expect("binding the server");
        let addr = server.local_addr().expect("reading the bound address");
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
            dir,
            store: Some(store),
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    async fn client(&self) -> Client {
        Client::connect(self.addr).await.expect("connecting")
    }

    fn db_path(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    /// Records currently on disk, including any the sweeper has not reached.
    fn entries(&self) -> u64 {
        use cache_store::Store;
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

#[tokio::test]
async fn handshake_reports_server_limits() {
    let server = TestServer::start().await;
    let client = server.client().await;

    let info = client.server_info();
    assert_eq!(info.protocol_version, cache_core::PROTOCOL_VERSION);
    assert_eq!(info.max_key_len, cache_core::MAX_KEY_LEN as u32);
    assert_eq!(info.max_value_len, cache_core::DEFAULT_MAX_VALUE_LEN as u32);
    // M0 implements no optional features, and must not claim otherwise.
    assert_eq!(info.capabilities, 0);
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

    let key = vec![b'k'; cache_core::MAX_KEY_LEN];
    client.set(&key, b"v", 0).await.unwrap();
    assert!(client.get(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn oversized_key_is_rejected_without_closing_the_connection() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    let key = vec![b'k'; cache_core::MAX_KEY_LEN + 1];
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

    let value = vec![0u8; cache_core::DEFAULT_MAX_VALUE_LEN + 1];
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
async fn tagged_writes_are_refused_rather_than_silently_dropped() {
    let server = TestServer::start().await;
    let mut client = server.client().await;

    // Tags land in M2. Until then the server must say so, because silently
    // ignoring them would let a client believe invalidation is working.
    assert!(matches!(
        client.set_tagged(b"k", b"v", 0, &[b"tag"]).await,
        Err(ClientError::Status(Status::Unsupported))
    ));
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
    assert!(
        cas.windows(2).all(|w| w[1] > w[0]),
        "cas must advance in order"
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

    let keys: Vec<&[u8]> = vec![b"k"; cache_core::MAX_BATCH_ITEMS + 1];
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

    let oversized = vec![b'k'; cache_core::MAX_KEY_LEN + 1];
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
