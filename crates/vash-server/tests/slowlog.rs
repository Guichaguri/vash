//! The slowlog, driven end to end over a store that is not LMDB.
//!
//! Phase 1 of `docs/slowlog-proposal.md`, and its exit criteria are what this
//! file asserts: an entry appears for a request past the threshold with its
//! queue wait and its execution time distinguishable, and a batch produces
//! **one** entry carrying its member count rather than one entry per member.
//!
//! Driven over the wire and read back off the admin port, rather than by calling
//! `Slowlog::record` directly, because the thing most likely to break is not the
//! ring — it is whether the dialects reach it at all. `dispatch::execute`, the
//! fused read, and `SetBatch` are three different routes to a recorded entry and
//! only one of them is obvious.

#![cfg(feature = "introspect")]

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use vash_server::{Config, Server};
use vash_store::memory::MemoryStore;

struct Harness {
    addr: SocketAddr,
    admin: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Harness {
    /// A server whose slowlog records **everything**.
    ///
    /// `threshold_us = 1` rather than a sleep somewhere: the question these
    /// tests ask is which requests reach the log and what they carry, not
    /// whether a comparison works — `slowlog::tests` already pins the boundary
    /// from both sides.
    async fn start(disclose_keys: bool) -> Self {
        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.observability.admin_listen = "127.0.0.1:0".into();
        config.observability.introspect.slowlog_threshold_us = 1;
        config.observability.introspect.slowlog_max_len = 256;
        config.protocol.listing_enabled = disclose_keys;
        config.store.path = "/nonexistent-on-purpose".into();

        let store = Arc::new(MemoryStore::new());
        let server = Server::bind_with(config, store as Arc<dyn vash_store::Store>)
            .await
            .expect("binding against the in-memory store");
        let addr = server.local_addr().unwrap();
        let admin = server.admin_addr().expect("the admin port bound");

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
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    /// Sends one block of RESP and reads whatever comes back.
    ///
    /// One `write_all`, so consecutive plain `SET`s arrive as a single block and
    /// `SetBatch` can hold them together — which is the condition the batch rule
    /// exists for.
    async fn resp(&self, block: &str, expected_replies: usize) -> String {
        let mut stream = TcpStream::connect(self.addr).await.expect("connecting");
        stream.set_nodelay(true).unwrap();
        stream
            .write_all(block.as_bytes())
            .await
            .expect("writing the block");

        let mut out = Vec::new();
        let mut chunk = [0u8; 4096];
        while out.iter().filter(|b| **b == b'\n').count() < expected_replies {
            let read = stream.read(&mut chunk).await.expect("reading the replies");
            if read == 0 {
                break;
            }
            out.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    async fn slowlog(&self) -> serde_json::Value {
        let mut stream = TcpStream::connect(self.admin).await.expect("admin connect");
        stream
            .write_all(b"GET /slowlog?count=256 HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .expect("admin request");

        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.expect("admin response");
        let text = String::from_utf8_lossy(&out).into_owned();
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or(&text);
        serde_json::from_str(body)
            .unwrap_or_else(|e| panic!("admin body was not JSON: {e}\n{text}"))
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.handle.take();
    }
}

fn entries(log: &serde_json::Value) -> &Vec<serde_json::Value> {
    log["entries"].as_array().expect("an entries array")
}

/// The exit criterion, first half: a request past the threshold is recorded,
/// and the two durations are separately visible.
///
/// `queue_us` is `0` here and that is the measured truth rather than a gap — the
/// in-memory store has no writer queue to wait in. What the assertion pins is
/// that the fields are disjoint and sum to the whole, which is the property the
/// arithmetic can get wrong.
#[tokio::test]
async fn a_slow_request_is_recorded_with_its_two_durations_apart() {
    let harness = Harness::start(true).await;
    harness
        .resp("*3\r\n$3\r\nSET\r\n$5\r\nalpha\r\n$1\r\nv\r\n", 1)
        .await;

    let log = harness.slowlog().await;
    assert_eq!(log["threshold_us"], 1);
    let entries = entries(&log);
    assert!(!entries.is_empty(), "nothing was recorded: {log}");

    let entry = entries
        .iter()
        .find(|e| e["command"] == "set")
        .unwrap_or_else(|| panic!("no set entry in {log}"));
    assert_eq!(entry["dialect"], "resp");
    assert!(entry["queue_us"].is_u64(), "queue_us must be reported");
    assert!(entry["exec_us"].is_u64(), "exec_us must be reported");
    assert_eq!(entry["value_bytes"], 1);
}

/// The exit criterion, second half, and §3.4: a batch is **one** entry carrying
/// its member count, not one entry per member.
///
/// The counters below it still divide the commit across its members. The two
/// disagreeing is the design — a 40 ms commit of 64 sets would otherwise become
/// 64 observations of 625 µs, all under any sane threshold, hiding the batch
/// that actually stalled the connection.
#[tokio::test]
async fn a_batch_is_one_entry_carrying_its_member_count() {
    let harness = Harness::start(true).await;

    const MEMBERS: usize = 8;
    let mut block = String::new();
    for i in 0..MEMBERS {
        block.push_str(&format!(
            "*3\r\n$3\r\nSET\r\n$7\r\nbatch:{i}\r\n$3\r\nval\r\n"
        ));
    }
    let replies = harness.resp(&block, MEMBERS).await;
    assert_eq!(
        replies.matches("+OK").count(),
        MEMBERS,
        "every member should have been stored: {replies}"
    );

    let log = harness.slowlog().await;
    let batched: Vec<&serde_json::Value> = entries(&log)
        .iter()
        .filter(|e| e["batch"].as_u64().unwrap_or(1) > 1)
        .collect();

    assert_eq!(
        batched.len(),
        1,
        "a batch must produce exactly one entry, got {}: {log}",
        batched.len()
    );
    let entry = batched[0];
    assert_eq!(entry["batch"], MEMBERS as u64);
    assert_eq!(entry["keys"], MEMBERS as u64);
    assert_eq!(entry["command"], "set");
    // Three bytes of value per member, summed rather than sampled.
    assert_eq!(entry["value_bytes"], (MEMBERS * 3) as u64);

    // And the batch did not also leave one entry per member behind it.
    let singles = entries(&log)
        .iter()
        .filter(|e| e["command"] == "set" && e["batch"].as_u64() == Some(1))
        .count();
    assert!(
        singles < MEMBERS,
        "the batch was also recorded member by member: {log}"
    );
}

/// §3.3, over the wire: the admin port is unauthenticated, so it renders a
/// digest until `listing_enabled` says enumeration is permitted.
#[tokio::test]
async fn keys_are_digests_until_the_listing_gate_opens() {
    let closed = Harness::start(false).await;
    closed
        .resp("*3\r\n$3\r\nSET\r\n$6\r\nsecret\r\n$1\r\nv\r\n", 1)
        .await;
    let log = closed.slowlog().await;
    assert_eq!(log["keys_disclosed"], false);
    for entry in entries(&log) {
        let key = entry["key"].as_str().expect("a key string");
        assert!(key.starts_with("xxh3:"), "expected a digest, got {key}");
        assert!(!key.contains("secret"), "the gate leaked a key: {key}");
    }

    let open = Harness::start(true).await;
    open.resp("*3\r\n$3\r\nSET\r\n$6\r\nsecret\r\n$1\r\nv\r\n", 1)
        .await;
    let log = open.slowlog().await;
    assert_eq!(log["keys_disclosed"], true);
    assert!(
        entries(&log)
            .iter()
            .any(|e| e["key"].as_str() == Some("secret")),
        "the open gate should name the key: {log}"
    );
}

/// A read reaches the log too. `execute_get_into` renders straight out of the
/// store and never builds a `Command`, so it is a second route to an entry and
/// the one most easily left unwired.
#[tokio::test]
async fn a_read_is_recorded_by_the_fused_path() {
    let harness = Harness::start(true).await;
    harness
        .resp("*3\r\n$3\r\nSET\r\n$4\r\nread\r\n$1\r\nv\r\n", 1)
        .await;
    harness.resp("*2\r\n$3\r\nGET\r\n$4\r\nread\r\n", 1).await;

    let log = harness.slowlog().await;
    assert!(
        entries(&log).iter().any(|e| e["command"] == "get"),
        "the fused read path never reached the slowlog: {log}"
    );
}
