//! Memcached protocol tests over a real socket.
//!
//! Deliberately raw: these speak the wire format byte for byte rather than
//! going through a client library, so they pin the exact bytes a real client
//! will receive. The client-library compatibility suite lives in
//! `tests/compat/` and runs against both this server and a real memcached.

use std::net::SocketAddr;

use cache_server::{Config, Server};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct TestServer {
    addr: SocketAddr,
    _dir: TempDir,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    async fn start_with(tweak: impl FnOnce(&mut Config)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.store.path = dir.path().join("db");
        config.store.map_size_mb = 64;
        // Port 0: these run in parallel and would otherwise fight over 9090.
        config.observability.admin_listen = "127.0.0.1:0".into();
        tweak(&mut config);

        let server = Server::bind(config).await.expect("binding");
        let addr = server.local_addr().unwrap();
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
            _dir: dir,
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    async fn connect(&self) -> Conn {
        let stream = TcpStream::connect(self.addr).await.expect("connecting");
        stream.set_nodelay(true).unwrap();
        Conn {
            stream,
            buf: Vec::new(),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        drop(self.shutdown.take());
        self.handle.take();
    }
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("writing");
    }

    /// Reads until the accumulated response ends with `terminator`.
    async fn read_until(&mut self, terminator: &str) -> String {
        let mut chunk = [0u8; 8192];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

        loop {
            if let Ok(text) = std::str::from_utf8(&self.buf)
                && text.ends_with(terminator)
            {
                let out = text.to_string();
                self.buf.clear();
                return out;
            }

            let read = tokio::time::timeout_at(deadline, self.stream.read(&mut chunk))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "timed out waiting for {terminator:?}; have {:?}",
                        String::from_utf8_lossy(&self.buf)
                    )
                })
                .expect("reading");

            assert_ne!(read, 0, "server closed while waiting for {terminator:?}");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    /// Sends a command and reads one line back.
    async fn line(&mut self, command: &str) -> String {
        self.send(command.as_bytes()).await;
        self.read_until("\r\n").await
    }

    async fn get(&mut self, command: &str) -> String {
        self.send(command.as_bytes()).await;
        self.read_until("END\r\n").await
    }
}

#[tokio::test]
async fn set_and_get_round_trip() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("set foo 7 0 5\r\nhello\r\n").await, "STORED\r\n");
    assert_eq!(
        c.get("get foo\r\n").await,
        "VALUE foo 7 5\r\nhello\r\nEND\r\n",
        "client flags and length must round-trip exactly"
    );
}

#[tokio::test]
async fn a_miss_is_just_end() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    assert_eq!(c.get("get nothing\r\n").await, "END\r\n");
}

#[tokio::test]
async fn multi_get_returns_only_the_hits() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set a 0 0 1\r\n1\r\n").await;
    c.line("set c 0 0 1\r\n3\r\n").await;

    assert_eq!(
        c.get("get a b c\r\n").await,
        "VALUE a 0 1\r\n1\r\nVALUE c 0 1\r\n3\r\nEND\r\n"
    );
}

#[tokio::test]
async fn gets_reports_a_cas_token_that_cas_accepts() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set k 0 0 3\r\nold\r\n").await;
    let response = c.get("gets k\r\n").await;

    let cas: u64 = response
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(4))
        .expect("gets must report a cas token")
        .parse()
        .expect("cas token must be numeric");

    assert_eq!(
        c.line(&format!("cas k 0 0 3 {cas}\r\nnew\r\n")).await,
        "STORED\r\n"
    );
    // The token has moved on, so replaying it must fail.
    assert_eq!(
        c.line(&format!("cas k 0 0 3 {cas}\r\nbad\r\n")).await,
        "EXISTS\r\n"
    );
    assert_eq!(c.get("get k\r\n").await, "VALUE k 0 3\r\nnew\r\nEND\r\n");
}

#[tokio::test]
async fn cas_on_an_absent_key_is_not_found() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    assert_eq!(c.line("cas gone 0 0 1 5\r\nx\r\n").await, "NOT_FOUND\r\n");
}

#[tokio::test]
async fn add_only_stores_when_absent() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("add k 0 0 1\r\na\r\n").await, "STORED\r\n");
    assert_eq!(c.line("add k 0 0 1\r\nb\r\n").await, "NOT_STORED\r\n");
    assert_eq!(c.get("get k\r\n").await, "VALUE k 0 1\r\na\r\nEND\r\n");
}

#[tokio::test]
async fn replace_only_stores_when_present() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("replace k 0 0 1\r\na\r\n").await, "NOT_STORED\r\n");
    c.line("set k 0 0 1\r\na\r\n").await;
    assert_eq!(c.line("replace k 0 0 1\r\nb\r\n").await, "STORED\r\n");
    assert_eq!(c.get("get k\r\n").await, "VALUE k 0 1\r\nb\r\nEND\r\n");
}

#[tokio::test]
async fn append_and_prepend_concatenate_and_keep_the_flags() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("append k 0 0 1\r\nx\r\n").await, "NOT_STORED\r\n");

    c.line("set k 9 0 3\r\nmid\r\n").await;
    assert_eq!(c.line("append k 0 0 5\r\n-tail\r\n").await, "STORED\r\n");
    assert_eq!(c.line("prepend k 0 0 5\r\nhead-\r\n").await, "STORED\r\n");

    assert_eq!(
        c.get("get k\r\n").await,
        "VALUE k 9 13\r\nhead-mid-tail\r\nEND\r\n",
        "concatenation must preserve the original client flags"
    );
}

#[tokio::test]
async fn delete_reports_hit_and_miss() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set k 0 0 1\r\nx\r\n").await;
    assert_eq!(c.line("delete k\r\n").await, "DELETED\r\n");
    assert_eq!(c.line("delete k\r\n").await, "NOT_FOUND\r\n");
}

#[tokio::test]
async fn incr_and_decr_operate_on_decimal_text() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set n 0 0 2\r\n10\r\n").await;
    assert_eq!(c.line("incr n 5\r\n").await, "15\r\n");
    assert_eq!(c.line("decr n 3\r\n").await, "12\r\n");

    // The stored value stays plain text a get returns unchanged.
    assert_eq!(c.get("get n\r\n").await, "VALUE n 0 2\r\n12\r\nEND\r\n");
}

#[tokio::test]
async fn decr_clamps_at_zero_rather_than_wrapping() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set n 0 0 1\r\n5\r\n").await;
    assert_eq!(c.line("decr n 100\r\n").await, "0\r\n");
}

#[tokio::test]
async fn incr_on_a_non_numeric_value_is_a_client_error() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set s 0 0 3\r\nabc\r\n").await;
    assert!(
        c.line("incr s 1\r\n").await.starts_with("CLIENT_ERROR"),
        "a non-numeric value must not read as a miss"
    );
    assert_eq!(c.line("incr absent 1\r\n").await, "NOT_FOUND\r\n");
}

#[tokio::test]
async fn touch_and_gat_extend_a_lifetime() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set k 0 1 1\r\nx\r\n").await;
    assert_eq!(c.line("touch k 3600\r\n").await, "TOUCHED\r\n");
    assert_eq!(c.line("touch absent 10\r\n").await, "NOT_FOUND\r\n");

    assert_eq!(c.get("gat 3600 k\r\n").await, "VALUE k 0 1\r\nx\r\nEND\r\n");

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    assert_eq!(
        c.get("get k\r\n").await,
        "VALUE k 0 1\r\nx\r\nEND\r\n",
        "the original one-second ttl should have been replaced"
    );
}

#[tokio::test]
async fn a_value_may_contain_crlf() {
    // The framing is length-delimited; a line-based parser would corrupt this.
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("set k 0 0 7\r\na\r\nb\r\nc\r\n").await, "STORED\r\n");
    assert_eq!(
        c.get("get k\r\n").await,
        "VALUE k 0 7\r\na\r\nb\r\nc\r\nEND\r\n"
    );
}

#[tokio::test]
async fn noreply_suppresses_the_response_but_not_the_work() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.send(b"set k 0 0 1 noreply\r\nx\r\n").await;
    // No response is expected, so the next command's reply is what comes back.
    assert_eq!(c.get("get k\r\n").await, "VALUE k 0 1\r\nx\r\nEND\r\n");

    c.send(b"delete k noreply\r\n").await;
    assert_eq!(c.get("get k\r\n").await, "END\r\n");
}

#[tokio::test]
async fn pipelined_commands_are_answered_in_order() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.send(b"set a 0 0 1\r\n1\r\nset b 0 0 1\r\n2\r\nget a\r\nget b\r\n")
        .await;

    let response = c.read_until("END\r\n").await;
    assert_eq!(
        response,
        "STORED\r\nSTORED\r\nVALUE a 0 1\r\n1\r\nEND\r\nVALUE b 0 1\r\n2\r\nEND\r\n"
    );
}

#[tokio::test]
async fn an_unknown_command_is_an_error_and_the_connection_survives() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("frobnicate\r\n").await, "ERROR\r\n");
    assert_eq!(c.line("set k 0 0 1\r\nx\r\n").await, "STORED\r\n");
}

#[tokio::test]
async fn a_mismatched_data_block_resynchronises() {
    // The declared length must be consumed even though the block is rejected,
    // or every later command on the connection would be misread.
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.send(b"set k 0 0 3\r\nhello\r\n").await;
    assert!(c.read_until("\r\n").await.starts_with("CLIENT_ERROR"));

    assert_eq!(c.line("set k 0 0 1\r\nx\r\n").await, "STORED\r\n");
}

#[tokio::test]
async fn an_oversized_key_is_rejected() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let key = "k".repeat(251); // memcached's limit is 250
    assert!(
        c.line(&format!("get {key}\r\n"))
            .await
            .starts_with("CLIENT_ERROR")
    );
    assert_eq!(c.line("set ok 0 0 1\r\nx\r\n").await, "STORED\r\n");
}

#[tokio::test]
async fn version_and_stats_answer() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert!(c.line("version\r\n").await.starts_with("VERSION "));

    let stats = c.get("stats\r\n").await;
    assert!(stats.contains("STAT pid "), "{stats}");
    assert!(stats.contains("STAT curr_items "), "{stats}");
    assert!(stats.ends_with("END\r\n"));
}

#[tokio::test]
async fn quit_closes_the_connection() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.send(b"quit\r\n").await;
    let mut chunk = [0u8; 64];
    assert_eq!(
        c.stream.read(&mut chunk).await.unwrap(),
        0,
        "quit must close rather than reply"
    );
}

#[tokio::test]
async fn flush_all_is_refused_unless_enabled() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set k 0 0 1\r\nx\r\n").await;
    assert!(c.line("flush_all\r\n").await.starts_with("CLIENT_ERROR"));
    assert_eq!(c.get("get k\r\n").await, "VALUE k 0 1\r\nx\r\nEND\r\n");
}

#[tokio::test]
async fn flush_all_empties_the_cache_when_enabled() {
    let server = TestServer::start_with(|c| c.protocol.flush_enabled = true).await;
    let mut c = server.connect().await;

    c.line("set k 0 0 1\r\nx\r\n").await;
    assert_eq!(c.line("flush_all\r\n").await, "OK\r\n");
    assert_eq!(c.get("get k\r\n").await, "END\r\n");
}

// ---- meta protocol ---------------------------------------------------------

#[tokio::test]
async fn mn_is_a_no_op_marker() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    assert_eq!(c.line("mn\r\n").await, "MN\r\n");
}

#[tokio::test]
async fn meta_set_and_get_round_trip() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("ms k 5 F7\r\nhello\r\n").await, "HD\r\n");
    assert_eq!(c.line("mg k v f\r\n").await, "VA 5 f7\r\nhello\r\n");
    assert_eq!(
        c.line("mg k\r\n").await,
        "HD\r\n",
        "no v flag means no value"
    );
}

#[tokio::test]
async fn a_meta_miss_is_en() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    assert_eq!(c.line("mg absent v\r\n").await, "EN\r\n");
}

#[tokio::test]
async fn meta_set_modes_enforce_their_guards() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(
        c.line("ms k 1 ME\r\na\r\n").await,
        "HD\r\n",
        "add on absent"
    );
    assert_eq!(
        c.line("ms k 1 ME\r\nb\r\n").await,
        "NS\r\n",
        "add on present"
    );
    assert_eq!(c.line("ms k 1 MA\r\nc\r\n").await, "HD\r\n", "append");
    assert_eq!(c.line("mg k v\r\n").await, "VA 2\r\nac\r\n");

    assert_eq!(
        c.line("ms absent 1 MR\r\nz\r\n").await,
        "NS\r\n",
        "replace on absent"
    );
}

#[tokio::test]
async fn meta_delete_and_opaque_echo() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("ms k 1\r\nx\r\n").await;
    assert_eq!(c.line("md k Oabc k\r\n").await, "HD kk Oabc\r\n");
    assert_eq!(c.line("md k\r\n").await, "NF\r\n");
}

#[tokio::test]
async fn meta_arithmetic() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("ms n 2\r\n10\r\n").await;
    // A bare `ma` increments by one, so this is 11...
    assert_eq!(c.line("ma n\r\n").await, "HD\r\n");
    // ...and `MD D4` subtracts four, leaving a single-byte "7".
    assert_eq!(c.line("ma n MD D4 v\r\n").await, "VA 1\r\n7\r\n");
}

#[tokio::test]
async fn the_meta_ttl_flag_reports_a_real_remaining_lifetime() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("ms forever 1\r\nx\r\n").await;
    assert_eq!(
        c.line("mg forever t\r\n").await,
        "HD t-1\r\n",
        "-1 is memcached's 'never expires'"
    );

    c.line("ms expiring 1 T120\r\nx\r\n").await;
    let response = c.line("mg expiring t\r\n").await;
    let ttl: i64 = response
        .trim_end()
        .rsplit(" t")
        .next()
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("expected a ttl token, got {response:?}"));

    // Must be the actual remaining lifetime, not a placeholder.
    assert!(
        (110..=120).contains(&ttl),
        "expected roughly 120s remaining, got {ttl}"
    );
}

#[tokio::test]
async fn meta_flags_that_are_not_implemented_are_refused() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // Accepting these silently would be worse than refusing: `b` would file the
    // value under the un-decoded key, and `h`/`l` are return flags whose
    // absence leaves the client parsing a shorter reply than it expects.
    for flag in ["b", "h", "l", "N30", "E5", "I", "R10", "x"] {
        let response = c.line(&format!("mg k {flag}\r\n")).await;
        assert!(
            response.starts_with("CLIENT_ERROR"),
            "flag {flag} should be refused, got {response:?}"
        );
    }

    // `u` asks the server not to bump the LRU, and there is no LRU here, so it
    // is genuinely inert and accepted.
    c.line("ms k 1\r\nx\r\n").await;
    assert_eq!(c.line("mg k v u\r\n").await, "VA 1\r\nx\r\n");
}

#[tokio::test]
async fn an_unknown_meta_flag_is_rejected() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert!(c.line("mg k Z\r\n").await.starts_with("CLIENT_ERROR"));
    assert_eq!(c.line("mn\r\n").await, "MN\r\n", "connection survives");
}

// ---- tag extension ---------------------------------------------------------

#[tokio::test]
async fn tags_can_be_attached_over_meta_and_invalidated() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(c.line("ms a 1 Gnews\r\n1\r\n").await, "HD\r\n");
    assert_eq!(c.line("ms b 1 Gnews,sport\r\n2\r\n").await, "HD\r\n");
    assert_eq!(c.line("ms c 1 Gsport\r\n3\r\n").await, "HD\r\n");
    c.line("set plain 0 0 1\r\n4\r\n").await;

    assert_eq!(c.line("mdt news\r\n").await, "HD\r\n");

    assert_eq!(c.line("mg a v\r\n").await, "EN\r\n");
    assert_eq!(c.line("mg b v\r\n").await, "EN\r\n");
    assert_eq!(c.line("mg c v\r\n").await, "VA 1\r\n3\r\n");
    assert_eq!(
        c.get("get plain\r\n").await,
        "VALUE plain 0 1\r\n4\r\nEND\r\n"
    );
}

#[tokio::test]
async fn the_classic_tag_command_works_too() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("ms k 1 Gdemo\r\nx\r\n").await;
    assert_eq!(c.line("delete_by_tag demo\r\n").await, "DELETED\r\n");
    assert_eq!(c.line("delete_by_tag demo\r\n").await, "DELETED\r\n");
    assert_eq!(
        c.line("delete_by_tag never-used\r\n").await,
        "NOT_FOUND\r\n",
        "an unregistered tag is a miss"
    );
    assert_eq!(c.get("get k\r\n").await, "END\r\n");
}

// ---- protocol detection ----------------------------------------------------

#[tokio::test]
async fn both_protocols_are_served_on_one_port() {
    let server = TestServer::start().await;

    // A memcached client writes the value...
    let mut mc = server.connect().await;
    mc.line("set shared 0 0 5\r\nvalue\r\n").await;

    // ...and a KCP client reads it back from the same store, same port.
    let mut kcp = cache_client::Client::connect(server.addr).await.unwrap();
    let got = kcp.get(b"shared").await.unwrap().expect("a hit");
    assert_eq!(&got.data[..], b"value");

    // And the reverse direction, including the client flags.
    kcp.set(b"from-kcp", b"other", 0).await.unwrap();
    assert_eq!(
        mc.get("get from-kcp\r\n").await,
        "VALUE from-kcp 0 5\r\nother\r\nEND\r\n"
    );
}

#[tokio::test]
async fn an_unrecognised_opening_byte_closes_the_connection() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.send(&[0xff, 0xfe, 0xfd]).await;
    let mut chunk = [0u8; 64];
    assert_eq!(
        c.stream.read(&mut chunk).await.unwrap(),
        0,
        "junk must be hung up on, not guessed at"
    );

    // Other clients are unaffected.
    let mut other = server.connect().await;
    assert!(other.line("version\r\n").await.starts_with("VERSION "));
}
