//! Memcached protocol tests over a real socket.
//!
//! Deliberately raw: these speak the wire format byte for byte rather than
//! going through a client library, so they pin the exact bytes a real client
//! will receive. The client-library compatibility suite lives in
//! `tests/compat/` and runs against both this server and a real memcached.

use std::net::SocketAddr;

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use vash_server::{Config, Server};

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
        // The whole protocol suite runs against whichever engine this build
        // carries, so `--features mdbx` re-runs it on the second one. See
        // `crates/vash-store/tests/store.rs` for the same arrangement a layer
        // down, and `docs/mdbx-proposal.md` for why the choice is per build.
        #[cfg(feature = "mdbx")]
        {
            config.store.backend = vash_server::config::Backend::Mdbx;
        }
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

    /// Reads until the server hangs up, returning whatever it said first.
    async fn read_to_close(&mut self) -> String {
        let mut chunk = [0u8; 8192];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

        loop {
            let read = tokio::time::timeout_at(deadline, self.stream.read(&mut chunk))
                .await
                .expect("timed out waiting for the server to close")
                .expect("reading");
            if read == 0 {
                return String::from_utf8_lossy(&self.buf).into_owned();
            }
            self.buf.extend_from_slice(&chunk[..read]);
        }
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

/// Turning the dialect off closes the connection at first-byte detection,
/// before the memcached parser sees anything. Refusing in memcached's own words
/// would mean running the parser we were told not to serve.
#[tokio::test]
async fn a_disabled_dialect_closes_the_connection_without_parsing() {
    let server = TestServer::start_with(|c| c.protocol.memcached_enabled = false).await;
    let mut c = server.connect().await;

    c.send(b"set foo 0 0 5\r\nhello\r\n").await;
    assert_eq!(
        c.read_to_close().await,
        "",
        "a disabled dialect answers nothing at all"
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

/// This dialect's counter has always been atomic — it has always been evaluated
/// inside the writer's transaction — and both dialects now share the primitive
/// that makes it so. The guard is here because "shared" is exactly the condition
/// under which one dialect's change can quietly cost the other its guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_do_not_lose_an_update() {
    const CONNECTIONS: usize = 8;
    const EACH: usize = 64;

    let server = TestServer::start().await;
    server.connect().await.line("set n 0 0 1\r\n0\r\n").await;

    let mut clients = Vec::new();
    for _ in 0..CONNECTIONS {
        let mut conn = server.connect().await;
        clients.push(tokio::spawn(async move {
            for _ in 0..EACH {
                conn.line("incr n 1\r\n").await;
            }
        }));
    }
    for client in clients {
        client.await.expect("increment task");
    }

    let total = CONNECTIONS * EACH;
    assert_eq!(
        server.connect().await.get("get n\r\n").await,
        format!(
            "VALUE n 0 {}\r\n{total}\r\nEND\r\n",
            total.to_string().len()
        )
    );
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
async fn verbosity_is_accepted_and_answers_ok() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // Upstream answers `OK`. Answering `VERSION …` here made a client that
    // checks the reply believe the command had failed.
    assert_eq!(c.line("verbosity 1\r\n").await, "OK\r\n");
    assert_eq!(c.line("verbosity 0\r\n").await, "OK\r\n");

    // The level is not optional, and it has to be a number.
    assert_eq!(c.line("verbosity\r\n").await, "ERROR\r\n");
    assert_eq!(
        c.line("verbosity loud\r\n").await,
        "CLIENT_ERROR bad command line format\r\n"
    );

    // `version` still reports the version, which is the command that should.
    assert!(c.line("version\r\n").await.starts_with("VERSION "));
}

/// The one place the 30-day overloading survives. VCP and Redis read a long
/// TTL as an offset; memcached's clients have expected a timestamp since 2003,
/// and the differential suite compares this against a real server.
#[tokio::test]
async fn an_exptime_past_thirty_days_is_an_absolute_timestamp() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Well past the threshold, so it can only be read as a stamp — and an hour
    // out, so reading it as an offset instead would be a lifetime of decades.
    let stamp = now + 3_600;
    assert_eq!(
        c.line(&format!("set k 0 {stamp} 1\r\nx\r\n")).await,
        "STORED\r\n"
    );

    let response = c.line("mg k t\r\n").await;
    let ttl: i64 = response
        .trim_end()
        .rsplit(" t")
        .next()
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("expected a ttl token, got {response:?}"));

    assert!(
        (3_590..=3_600).contains(&ttl),
        "expected roughly an hour remaining, got {ttl}"
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

    // ...and a VCP client reads it back from the same store, same port.
    let mut vcp = vash_client::Client::connect(server.addr).await.unwrap();
    let got = vcp.get(b"shared").await.unwrap().expect("a hit");
    assert_eq!(&got.data[..], b"value");

    // And the reverse direction, including the client flags.
    vcp.set(b"from-vcp", b"other", 0).await.unwrap();
    assert_eq!(
        mc.get("get from-vcp\r\n").await,
        "VALUE from-vcp 0 5\r\nother\r\nEND\r\n"
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

// ---- stats --------------------------------------------------------------

/// Every upstream field name this server is allowed to claim.
///
/// A name is on this list because its **meaning** matches upstream's, not
/// because the word fits. `reclaimed` is the cautionary one and is deliberately
/// absent: upstream's counts entries stored into the memory of an expired one —
/// slab reuse — where this server's sweeper reclaim count is a different
/// quantity that happens to share the word.
const UPSTREAM_FIELDS: &[&str] = &[
    // general
    "pid",
    "uptime",
    "time",
    "version",
    "pointer_size",
    "max_connections",
    "curr_connections",
    "total_connections",
    "rejected_connections",
    "accepting_conns",
    "cmd_get",
    "cmd_set",
    "cmd_touch",
    "cmd_flush",
    "cmd_meta",
    "get_hits",
    "get_misses",
    "delete_hits",
    "delete_misses",
    "incr_hits",
    "incr_misses",
    "decr_hits",
    "decr_misses",
    "cas_hits",
    "cas_misses",
    "cas_badval",
    "touch_hits",
    "touch_misses",
    "total_items",
    "store_too_large",
    "store_no_memory",
    "auth_cmds",
    "auth_errors",
    "bytes_read",
    "bytes_written",
    "curr_items",
    "bytes",
    "limit_maxbytes",
    "evictions",
    // settings
    "maxbytes",
    "maxconns",
    "tcpport",
    "udpport",
    "inter",
    "verbosity",
    "domain_socket",
    "shutdown_command",
    "cas_enabled",
    "auth_enabled_sasl",
    "auth_enabled_ascii",
    "item_size_max",
    "maxconns_fast",
    "flush_enabled",
    "dump_enabled",
    "lru_crawler",
    "lru_crawler_tocrawl",
    "lru_maintainer_thread",
    "temp_lru",
    "track_sizes",
    "detail_enabled",
    "ssl_enabled",
    "proxy_enabled",
    "client_flags_size",
    // items / slabs / conns / sizes
    "number",
    // Upstream's `items:<class>:evicted` counts items dropped from that class
    // to make room, which is what this counts. Its sibling `evictions` in the
    // general set is the same event seen server-wide.
    "evicted",
    "outofmemory",
    "get_hits",
    "cmd_set",
    // Upstream allocates one chunk per item, so `used_chunks` tracks the live
    // item count — measured against 1.6.45. There is no chunking here at all,
    // so one record is one unit in use and the meaning carries over exactly.
    "used_chunks",
    "active_slabs",
    "total_malloced",
    "addr",
    "listen_addr",
    "state",
    "secs_since_last_cmd",
    "sizes_status",
];

/// **The rule, pinned.** Every field name every subcommand emits is either a
/// reviewed upstream name or carries the `vash_` prefix.
///
/// Without this, a counter added later could quietly claim an upstream name
/// whose meaning it does not share — which is exactly the mistake `reclaimed`
/// would have been, and the kind a reader of a dashboard never catches.
#[tokio::test]
async fn no_field_claims_an_upstream_name_it_was_not_given() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    c.line("set k 0 0 1\r\nv\r\n").await;

    for section in ["", "settings", "items", "slabs", "conns", "sizes"] {
        let command = format!("stats {section}\r\n");
        let command = format!("{}\r\n", command.trim_end());
        for name in stat_fields(&mut c, &command).await.keys() {
            // `items:1:number` and `1:get_hits` and `7:addr` are namespaced by
            // class or connection id; the field is the last segment.
            let field = name.rsplit(':').next().expect("a field name");
            assert!(
                field.starts_with("vash_") || UPSTREAM_FIELDS.contains(&field),
                "`{name}` (from `stats {section}`) claims the upstream name \
                 `{field}` without being on the reviewed list. Either its \
                 meaning matches upstream's — add it there — or it does not, \
                 and it needs a vash_ prefix."
            );
        }
    }
}

/// Reads a `STAT`-line reply into its fields.
async fn stat_fields(conn: &mut Conn, command: &str) -> std::collections::HashMap<String, String> {
    conn.send(command.as_bytes()).await;
    let body = conn.read_until("END\r\n").await;
    body.lines()
        .filter_map(|line| line.strip_prefix("STAT "))
        .filter_map(|line| line.split_once(' '))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

/// Three subcommands upstream implements and this server does not, refused **by
/// name** — the call `lru_crawler enable` already makes. Upstream implements
/// them, so the bytes diverge whatever is sent, and saying which command was
/// refused is worth more than a shorter divergence.
#[tokio::test]
async fn the_unimplemented_subcommands_are_refused_by_name() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    assert_eq!(
        c.line("stats reset\r\n").await,
        "CLIENT_ERROR stats reset is not implemented\r\n"
    );
    assert_eq!(
        c.line("stats detail on\r\n").await,
        "CLIENT_ERROR stats detail is not implemented\r\n"
    );

    // An unrecognised subcommand still gets upstream's bare `ERROR`, so the two
    // cases stay distinguishable — "we do not have that" against "we chose not
    // to build that".
    assert_eq!(c.line("stats bogus\r\n").await, "ERROR\r\n");
    // 1.6.45 removed both verbs and answers the same.
    assert_eq!(c.line("stats sizes_enable\r\n").await, "ERROR\r\n");
    assert_eq!(c.line("stats sizes_disable\r\n").await, "ERROR\r\n");

    // None of the implemented sections takes an argument.
    assert_eq!(
        c.line("stats settings extra\r\n").await,
        "CLIENT_ERROR bad command line format\r\n"
    );
}

/// `sizes`, `extstore` and `proxy` are answered **byte-identically to a stock
/// memcached**: upstream tracks item sizes only under `-o track_sizes`, and
/// answers an empty reply for subsystems that were not compiled in. There is no
/// size tracking, no external storage and no proxy here, so each reply is exact
/// rather than an approximation.
#[tokio::test]
async fn the_sections_that_a_stock_memcached_also_leaves_empty() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.send(b"stats sizes\r\n").await;
    assert_eq!(
        c.read_until("END\r\n").await,
        "STAT sizes_status disabled\r\nEND\r\n"
    );

    for section in ["extstore", "proxy"] {
        c.send(format!("stats {section}\r\n").as_bytes()).await;
        assert_eq!(c.read_until("END\r\n").await, "END\r\n", "stats {section}");
    }
}

#[tokio::test]
async fn stats_settings_reports_the_configuration_in_force() {
    let server = TestServer::start_with(|config| {
        config.protocol.flush_enabled = true;
        config.protocol.listing_enabled = true;
    })
    .await;
    let mut c = server.connect().await;

    let fields = stat_fields(&mut c, "stats settings\r\n").await;

    // The two that match upstream exactly in meaning, not merely in name.
    assert_eq!(fields["flush_enabled"], "yes");
    assert_eq!(fields["dump_enabled"], "yes");
    assert_eq!(fields["lru_crawler"], "yes");

    // Measurements of a decision, not placeholders.
    assert_eq!(fields["udpport"], "0");
    assert_eq!(fields["ssl_enabled"], "no");
    assert_eq!(fields["proxy_enabled"], "no");
    assert_eq!(fields["auth_enabled_sasl"], "no");
    assert_eq!(fields["lru_maintainer_thread"], "no");
    assert_eq!(fields["track_sizes"], "no");
    assert_eq!(fields["detail_enabled"], "no");
    assert_eq!(fields["cas_enabled"], "yes");
    assert_eq!(fields["client_flags_size"], "4");

    // The bound port, not the configured one — the test server asks for 0.
    assert_eq!(fields["tcpport"], server.addr.port().to_string());
    assert_ne!(fields["tcpport"], "0", "port 0 is a request, not an answer");

    assert!(fields.contains_key("maxbytes"));
    assert!(fields.contains_key("item_size_max"));
    assert!(fields.contains_key("vash_shards"));

    // Slab, LRU and extstore geometry: absent, not zeroed.
    for absent in [
        "growth_factor",
        "chunk_size",
        "hot_lru_pct",
        "ext_item_size",
    ] {
        assert!(!fields.contains_key(absent), "{absent} has no meaning here");
    }
}

#[tokio::test]
async fn stats_settings_follows_the_gates_it_reports() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let fields = stat_fields(&mut c, "stats settings\r\n").await;
    assert_eq!(fields["flush_enabled"], "no", "off by default");
    assert_eq!(fields["dump_enabled"], "no");
    assert_eq!(fields["lru_crawler"], "no");
}

#[tokio::test]
async fn stats_items_reports_the_class_the_dumps_accept() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    for i in 0..3 {
        c.line(&format!("set k{i} 0 0 1\r\nv\r\n")).await;
    }

    let fields = stat_fields(&mut c, "stats items\r\n").await;
    assert_eq!(fields["items:1:number"], "3");
    assert_eq!(fields["items:1:evicted"], "0");
    assert!(fields.contains_key("items:1:outofmemory"));

    // The class id a tool reads here must be one the dumps answer to, or the
    // discover-then-dump loop every memcached tool runs would go nowhere.
    c.send(b"lru_crawler metadump 1\r\n").await;
    let dump = c.read_until("END\r\n").await;
    assert_eq!(dumped_keys(&dump).len(), 3);

    // LRU segmentation and item age: absent, not zeroed.
    for absent in ["items:1:number_hot", "items:1:age", "items:1:evicted_time"] {
        assert!(!fields.contains_key(absent), "{absent} needs an LRU");
    }
}

#[tokio::test]
async fn stats_slabs_reports_the_counters_a_slab_class_would_carry() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    c.line("set k 0 0 1\r\nv\r\n").await;
    c.get("get k\r\n").await;

    let fields = stat_fields(&mut c, "stats slabs\r\n").await;
    assert_eq!(fields["1:get_hits"], "1");
    assert_eq!(fields["1:cmd_set"], "1");
    assert_eq!(fields["active_slabs"], "1");
    assert!(fields.contains_key("total_malloced"));

    // The rest of the geometry: a page here holds records of many sizes, so
    // reporting one would let a tool compute a slab efficiency that means
    // nothing. **`used_chunks` has no denominator to be divided by**, which is
    // what keeps it honest while these stay out.
    for absent in [
        "1:chunk_size",
        "1:chunks_per_page",
        "1:total_pages",
        "1:total_chunks",
        "1:free_chunks",
        "1:free_chunks_end",
    ] {
        assert!(
            !fields.contains_key(absent),
            "{absent} describes a slab allocator"
        );
    }
}

/// `stats cachedump` — upstream's older key dump, in its positional bracket
/// format. Superseded by `lru_crawler metadump`, and implemented because older
/// tooling calls it.
#[tokio::test]
async fn cachedump_lists_keys_in_upstreams_format() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    c.line("set teste 0 0 6\r\nabcdef\r\n").await;
    c.line("set teste2 0 900 9\r\n123456789\r\n").await;

    c.send(b"stats cachedump 1 10\r\n").await;
    let body = c.read_until("END\r\n").await;

    let items: Vec<&str> = body
        .lines()
        .filter(|line| line.starts_with("ITEM "))
        .collect();
    assert_eq!(items.len(), 2, "{body:?}");

    // `size` is always 0 and must not be read: the field cannot be dropped from
    // a positional format, and carrying a real length would put a `value_len`
    // on every `ListEntry` that every VCP listing pays for and never reads.
    // `mg <key> s` answers the size of one key without that.
    let never = items
        .iter()
        .find(|line| line.starts_with("ITEM teste "))
        .expect("the key with no deadline");
    assert_eq!(
        *never, "ITEM teste [0 b; 0 s]",
        "0 means never expires here"
    );

    // …and note that `metadump` spells the same thing `-1`. Upstream's own
    // asymmetry, reproduced rather than tidied.
    c.send(b"lru_crawler metadump all\r\n").await;
    let meta = c.read_until("END\r\n").await;
    assert!(
        meta.contains("key=teste exp=-1 "),
        "metadump uses -1 where cachedump uses 0: {meta:?}"
    );

    let expiring = items
        .iter()
        .find(|line| line.starts_with("ITEM teste2 "))
        .expect("the key with a deadline");
    let stamp: i64 = expiring
        .split(' ')
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("an absolute unix stamp: {expiring:?}"));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(
        (890..=901).contains(&(stamp - now)),
        "expected ~900s of life, got {}",
        stamp - now
    );
}

#[tokio::test]
async fn cachedump_arguments_follow_upstream() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    c.line("set k 0 0 1\r\nv\r\n").await;

    // A class this server does not have holds nothing.
    c.send(b"stats cachedump 2 10\r\n").await;
    assert_eq!(c.read_until("END\r\n").await, "END\r\n");

    // **A limit of 0 means no limit**, not "nothing" — upstream's reading,
    // verified against 1.6.45 and caught by the differential when this was
    // implemented the other way round.
    c.send(b"stats cachedump 1 0\r\n").await;
    let body = c.read_until("END\r\n").await;
    assert_eq!(
        body.lines().filter(|l| l.starts_with("ITEM ")).count(),
        1,
        "a limit of 0 dumps the class: {body:?}"
    );

    // Upstream distinguishes a short line from an unreadable one, down to the
    // word "format". Verified against 1.6.45.
    assert_eq!(
        c.line("stats cachedump 1\r\n").await,
        "CLIENT_ERROR bad command line\r\n"
    );
    assert_eq!(
        c.line("stats cachedump\r\n").await,
        "CLIENT_ERROR bad command line\r\n"
    );
    assert_eq!(
        c.line("stats cachedump 1 abc\r\n").await,
        "CLIENT_ERROR bad command line format\r\n"
    );
    assert_eq!(
        c.line("stats cachedump abc 10\r\n").await,
        "CLIENT_ERROR bad command line format\r\n"
    );
}

#[tokio::test]
async fn cachedump_honours_its_limit_and_the_listing_gate() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    for i in 0..20 {
        c.line(&format!("set k{i:02} 0 0 1\r\nv\r\n")).await;
    }

    c.send(b"stats cachedump 1 5\r\n").await;
    let body = c.read_until("END\r\n").await;
    assert_eq!(body.lines().filter(|l| l.starts_with("ITEM ")).count(), 5);

    // Enumerating a keyspace is the same capability whichever command asks.
    let closed = TestServer::start().await;
    let mut d = closed.connect().await;
    assert_eq!(
        d.line("stats cachedump 1 10\r\n").await,
        "CLIENT_ERROR command disabled by configuration\r\n"
    );
}

/// The cross-dialect hazard, in the format least able to cope with it: `ITEM`
/// lines are positional and upstream does not encode the key, because its own
/// parser refuses to store one that would need it. This keyspace is shared.
#[tokio::test]
async fn a_key_no_memcached_client_could_write_cannot_break_a_cachedump() {
    let server = TestServer::start_with(listing_on).await;

    let mut redis = server.connect().await;
    let hostile = "danger key\r\nITEM injected [0 b; 0 s]";
    redis
        .send(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{hostile}\r\n$1\r\nv\r\n",
                hostile.len()
            )
            .as_bytes(),
        )
        .await;
    assert_eq!(redis.read_until("\r\n").await, "+OK\r\n");

    let mut c = server.connect().await;
    c.send(b"stats cachedump 1 10\r\n").await;
    let body = c.read_until("END\r\n").await;

    assert_eq!(
        body.matches("END\r\n").count(),
        1,
        "one terminator: {body:?}"
    );
    assert_eq!(
        body.lines().filter(|l| l.starts_with("ITEM ")).count(),
        1,
        "one item: {body:?}"
    );
    assert!(
        body.contains("ITEM danger%20key%0D%0AITEM%20injected%20[0%20b;%200%20s] [0 b; 0 s]"),
        "encoded rather than able to inject a line: {body:?}"
    );
}

/// Upstream allocates one chunk per item, so `used_chunks` tracks the live item
/// count and falls when one is removed — measured against 1.6.45. There is no
/// chunking here, so the mapping is exact, and the two numbers a tool can read
/// for "how many items" must agree.
#[tokio::test]
async fn used_chunks_tracks_the_live_item_count() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let used = |fields: &std::collections::HashMap<String, String>| fields["1:used_chunks"].clone();

    assert_eq!(used(&stat_fields(&mut c, "stats slabs\r\n").await), "0");

    for i in 0..3 {
        c.line(&format!("set k{i} 0 0 1\r\nv\r\n")).await;
    }
    let after_writes = stat_fields(&mut c, "stats slabs\r\n").await;
    assert_eq!(used(&after_writes), "3");

    // The same quantity `stats items` reports, as upstream's two also are.
    let items = stat_fields(&mut c, "stats items\r\n").await;
    assert_eq!(used(&after_writes), items["items:1:number"]);

    c.line("delete k0\r\n").await;
    assert_eq!(used(&stat_fields(&mut c, "stats slabs\r\n").await), "2");
}

#[tokio::test]
async fn stats_conns_lists_connections_while_they_are_open() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    c.line("version\r\n").await;

    let fields = stat_fields(&mut c, "stats conns\r\n").await;

    // The listener itself, which is the one state that is unambiguous.
    assert_eq!(fields["0:state"], "conn_listening");
    assert!(fields["0:addr"].starts_with("tcp:"));

    // The asking connection is in there, having just run a memcached command.
    let dialects: Vec<&String> = fields
        .iter()
        .filter(|(name, _)| name.ends_with(":vash_dialect"))
        .map(|(_, value)| value)
        .collect();
    assert!(
        dialects.iter().any(|d| *d == "memcached"),
        "the asking connection should be listed: {fields:?}"
    );

    // A second connection appears, and is gone once it closes.
    let count = |fields: &std::collections::HashMap<String, String>| {
        fields.keys().filter(|k| k.ends_with(":addr")).count()
    };
    let before = count(&fields);
    {
        let mut other = server.connect().await;
        other.line("version\r\n").await;
        let with_both = stat_fields(&mut c, "stats conns\r\n").await;
        assert_eq!(count(&with_both), before + 1);
    }

    // Upstream keys this table by file descriptor, which is reused; a
    // monotonic id is what makes two calls comparable.
    assert!(!fields.contains_key("1:state"), "no invented conn state");
}

#[tokio::test]
async fn stats_reports_the_counters_it_measures() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set k 0 0 1\r\nv\r\n").await;
    c.get("get k\r\n").await;
    c.get("get missing\r\n").await;

    c.send(b"stats\r\n").await;
    let body = c.read_until("END\r\n").await;
    let fields: std::collections::HashMap<&str, &str> = body
        .lines()
        .filter_map(|line| line.strip_prefix("STAT "))
        .filter_map(|line| line.split_once(' '))
        .collect();

    assert_eq!(fields["curr_items"], "1");
    assert_eq!(fields["cmd_set"], "1");
    assert_eq!(fields["cmd_get"], "2");
    assert_eq!(fields["get_hits"], "1");
    assert_eq!(fields["get_misses"], "1");
    assert_eq!(fields["total_items"], "1");
    assert_eq!(fields["accepting_conns"], "1");
    assert!(fields.contains_key("uptime"));
    assert!(fields.contains_key("max_connections"));
    assert!(fields.contains_key("evictions"));
    assert!(fields["version"].ends_with("-vash"));
    // Bytes crossed the socket to get here, so neither can be zero.
    assert_ne!(fields["bytes_read"], "0");
    assert_ne!(fields["bytes_written"], "0");

    // Absent rather than zeroed: nothing measures them.
    //
    // `reclaimed` is the interesting one. Upstream's counts entries stored into
    // the memory of an expired one — slab reuse. This server's sweeper reclaim
    // count is a different quantity that happens to share the word, so it is
    // reported as `vash_reclaimed` and upstream's name is not claimed.
    for unmeasured in [
        "reclaimed",
        "get_expired",
        "get_flushed",
        "expired_unfetched",
        "rusage_user",
        "hash_power_level",
    ] {
        assert!(
            !fields.contains_key(unmeasured),
            "{unmeasured} is not measured and must not be reported"
        );
    }
    assert!(fields.contains_key("vash_reclaimed"));
}

/// The per-command splits, which is what `stats items`, `stats slabs` and half
/// of upstream's general set are built from.
#[tokio::test]
async fn stats_reports_what_each_command_found() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set counter 0 0 1\r\n5\r\n").await;
    c.line("incr counter 1\r\n").await; // hit
    c.line("incr missing 1\r\n").await; // miss
    c.line("decr counter 1\r\n").await; // hit
    c.line("touch counter 100\r\n").await; // hit
    c.line("touch missing 100\r\n").await; // miss
    c.line("delete counter\r\n").await; // hit
    c.line("delete missing\r\n").await; // miss

    let fields = stat_fields(&mut c, "stats\r\n").await;
    assert_eq!(fields["incr_hits"], "1");
    assert_eq!(fields["incr_misses"], "1");
    assert_eq!(fields["decr_hits"], "1");
    assert_eq!(fields["decr_misses"], "0");
    assert_eq!(fields["touch_hits"], "1");
    assert_eq!(fields["touch_misses"], "1");
    assert_eq!(fields["delete_hits"], "1");
    assert_eq!(fields["delete_misses"], "1");
}

/// CAS is the one write whose three outcomes are all distinct, and telling
/// `badval` from a miss is the whole point of it.
#[tokio::test]
async fn stats_separates_a_lost_cas_race_from_a_miss() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set k 0 0 1\r\nv\r\n").await;
    let token: u64 = c
        .get("gets k\r\n")
        .await
        .lines()
        .next()
        .and_then(|line| line.split(' ').nth(4))
        .and_then(|cas| cas.parse().ok())
        .expect("gets reports a cas token");

    assert_eq!(
        c.line(&format!("cas k 0 0 1 {token}\r\nw\r\n")).await,
        "STORED\r\n"
    );
    // The token is spent, so the same one now names a value that has moved on.
    assert_eq!(
        c.line(&format!("cas k 0 0 1 {token}\r\nx\r\n")).await,
        "EXISTS\r\n"
    );
    assert_eq!(
        c.line(&format!("cas gone 0 0 1 {token}\r\ny\r\n")).await,
        "NOT_FOUND\r\n"
    );

    let fields = stat_fields(&mut c, "stats\r\n").await;
    assert_eq!(fields["cas_hits"], "1");
    assert_eq!(fields["cas_badval"], "1");
    assert_eq!(fields["cas_misses"], "1");
}

/// Meta commands are counted apart from the classic ones — the single thing
/// that distinguishes the two grammars once a command has been parsed.
#[tokio::test]
async fn stats_counts_the_meta_dialect_separately() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.line("set k 0 0 1\r\nv\r\n").await;
    c.line("mg k v\r\n").await;
    c.line("mn\r\n").await;

    let fields = stat_fields(&mut c, "stats\r\n").await;
    assert_eq!(fields["cmd_meta"], "2");
}

// ---- lru_crawler --------------------------------------------------------
//
// The key listing of this dialect. The grammar has no cursor, so the server
// pages the listing internally and writes as it goes — which is why these
// assert on whole dumps rather than on single replies.

fn listing_on(config: &mut Config) {
    config.protocol.listing_enabled = true;
}

/// Every `key=` from a metadump, in the order it arrived.
fn dumped_keys(body: &str) -> Vec<&str> {
    body.lines()
        .filter_map(|line| line.strip_prefix("key="))
        .filter_map(|line| line.split(' ').next())
        .collect()
}

/// The framing, byte for byte, against what `memcached:1.6-alpine` sends: an
/// `OK` acknowledgement first, data lines ending in a **bare `\n` with a
/// trailing space**, and `END\r\n` last. Every one of those is a detail a
/// parser keyed on line endings would notice.
#[tokio::test]
async fn metadump_matches_upstream_framing() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    c.line("set solo 0 0 1\r\nv\r\n").await;

    c.send(b"lru_crawler metadump all\r\n").await;
    let body = c.read_until("END\r\n").await;

    let mut lines = body.split_inclusive('\n');
    assert_eq!(lines.next().unwrap(), "OK\r\n");

    let entry = lines.next().unwrap();
    assert!(entry.ends_with(" \n"), "trailing space, bare LF: {entry:?}");
    assert!(!entry.contains('\r'), "no CR on a data line: {entry:?}");
    assert!(entry.starts_with("key=solo exp=-1 cas="), "{entry:?}");
    assert!(entry.contains(" cls=1 "), "{entry:?}");

    assert_eq!(lines.next().unwrap(), "END\r\n");
    assert_eq!(lines.next(), None);
}

/// `mgdump` emits a ready-to-send `mg` command per key and ends with the meta
/// protocol's `EN`, which is what lets a dump be piped back in.
#[tokio::test]
async fn mgdump_emits_replayable_commands() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    c.line("set solo 0 0 1\r\nv\r\n").await;

    c.send(b"lru_crawler mgdump all\r\n").await;
    assert_eq!(c.read_until("EN\r\n").await, "OK\r\nmg solo\r\nEN\r\n");

    // And it really is a command: sent back, it hits.
    assert_eq!(c.line("mg solo v\r\n").await, "VA 1\r\nv\r\n");
}

/// There are no slab classes here, so everything is in class 1 and `all`,
/// `hash` and `1` are three spellings of one dump. Any other class is genuinely
/// empty, and a missing one is upstream's bare `ERROR`.
#[tokio::test]
async fn the_dump_class_argument_follows_upstreams_vocabulary() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    for i in 0..3 {
        c.line(&format!("set k{i} 0 0 1\r\nv\r\n")).await;
    }

    let mut dumps = Vec::new();
    for class in ["all", "hash", "1"] {
        c.send(format!("lru_crawler metadump {class}\r\n").as_bytes())
            .await;
        let body = c.read_until("END\r\n").await;
        let mut keys = dumped_keys(&body);
        keys.sort_unstable();
        dumps.push(keys.join(","));
    }
    assert_eq!(dumps[0], "k0,k1,k2");
    assert_eq!(dumps[0], dumps[1], "`hash` is the same dump");
    assert_eq!(
        dumps[0], dumps[2],
        "and so is the class this server reports"
    );

    // A class this server does not have holds nothing — and says so with a
    // terminator, not an error.
    c.send(b"lru_crawler metadump 7\r\n").await;
    assert_eq!(c.read_until("END\r\n").await, "OK\r\nEND\r\n");
    c.send(b"lru_crawler mgdump 42\r\n").await;
    assert_eq!(c.read_until("EN\r\n").await, "OK\r\nEN\r\n");

    // Verified against upstream: a missing class is `ERROR`, not a client error.
    assert_eq!(c.line("lru_crawler metadump\r\n").await, "ERROR\r\n");
    assert_eq!(c.line("lru_crawler bogus\r\n").await, "ERROR\r\n");
}

/// The rest of `lru_crawler` steers a background LRU crawler, and plan §6
/// rejected an on-disk LRU — so there is nothing to enable, sleep or crawl.
/// Refused by name, since upstream implements these and the bytes diverge
/// either way.
#[tokio::test]
async fn the_other_lru_crawler_subcommands_are_refused_by_name() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;

    for (command, expected) in [
        ("lru_crawler enable", "enable"),
        ("lru_crawler disable", "disable"),
        ("lru_crawler sleep 1000", "sleep"),
        ("lru_crawler tocrawl 100", "tocrawl"),
        ("lru_crawler crawl 1", "crawl"),
    ] {
        assert_eq!(
            c.line(&format!("{command}\r\n")).await,
            format!("CLIENT_ERROR lru_crawler {expected} is not implemented\r\n")
        );
    }
}

#[tokio::test]
async fn a_dump_returns_every_key_across_its_internal_pages() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    // Past `MAX_LIST_LIMIT`, so the dump has to page more than once.
    for i in 0..2_500 {
        c.line(&format!("set k{i:05} 0 0 1\r\nv\r\n")).await;
    }

    c.send(b"lru_crawler metadump all\r\n").await;
    let body = c.read_until("END\r\n").await;

    let mut keys = dumped_keys(&body);
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), 2_500, "every key, exactly once");
}

/// **The error replaces the terminator rather than following it.** A tool reads
/// lines until `END`, so a truncated dump ending in one would report a keyspace
/// smaller than the real one.
#[tokio::test]
async fn a_dump_cut_short_by_its_budget_ends_in_an_error_not_a_terminator() {
    let server = TestServer::start_with(|config| {
        config.protocol.listing_enabled = true;
        config.protocol.listing_max_scan = 10;
    })
    .await;
    let mut c = server.connect().await;
    for i in 0..2_000 {
        c.line(&format!("set k{i:05} 0 0 1\r\nv\r\n")).await;
    }

    c.send(b"lru_crawler metadump all\r\n").await;
    let body = c.read_until("\r\n").await;
    assert!(
        body.ends_with("SERVER_ERROR dump exceeded the scan budget; use SCAN or LIST_KEYS to page the keyspace\r\n"),
        "{body:?}"
    );
    assert!(
        !body.contains("END\r\n"),
        "a cut-short dump must never look complete"
    );
}

/// Enumerating a keyspace is the same capability whichever dialect asks, so the
/// dumps sit behind the same gate as `LIST_KEYS` — including for a class that
/// would have been empty, because "disabled" is true regardless of which class
/// was named.
#[tokio::test]
async fn the_dumps_are_refused_when_listing_is_disabled() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    c.line("set k 0 0 1\r\nv\r\n").await;

    for command in [
        "lru_crawler metadump all",
        "lru_crawler mgdump all",
        "lru_crawler metadump 7",
    ] {
        assert_eq!(
            c.line(&format!("{command}\r\n")).await,
            "CLIENT_ERROR command disabled by configuration\r\n",
            "{command}"
        );
    }
}

/// **The cross-dialect hazard.** The keyspace is shared, so a Redis or VCP
/// client can store a key holding a space and a CRLF — which no memcached client
/// could have written, and which would otherwise close the dump early and inject
/// a line of the attacker's choosing.
#[tokio::test]
async fn a_key_no_memcached_client_could_write_cannot_break_the_dump() {
    let server = TestServer::start_with(listing_on).await;

    // Written over Redis, into the same keyspace.
    let mut redis = server.connect().await;
    let hostile = "danger key\r\nEND\r\nkey=injected exp=-1 cas=1 cls=1 ";
    redis
        .send(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{hostile}\r\n$1\r\nv\r\n",
                hostile.len()
            )
            .as_bytes(),
        )
        .await;
    assert_eq!(redis.read_until("\r\n").await, "+OK\r\n");

    let mut c = server.connect().await;
    c.send(b"lru_crawler metadump all\r\n").await;
    let body = c.read_until("END\r\n").await;

    assert_eq!(
        body.matches("END\r\n").count(),
        1,
        "one terminator: {body:?}"
    );
    assert_eq!(dumped_keys(&body).len(), 1, "one key: {body:?}");
    // The payload survives as *text inside the encoded key*, which is the
    // point: it is no longer a line of its own, so it cannot be read as a
    // record or as the end of the dump.
    assert!(
        !body.lines().any(|line| line.starts_with("key=injected")),
        "the payload must not become a line: {body:?}"
    );
    assert!(
        body.contains("key=danger%20key%0D%0AEND%0D%0Akey=injected%20exp=-1%20cas=1%20cls=1%20 "),
        "percent-encoded rather than skipped: {body:?}"
    );

    // Every data line still parses as `field=value` pairs.
    for line in body.lines().filter(|line| line.starts_with("key=")) {
        assert!(
            line.split(' ')
                .all(|field| field.is_empty() || field.contains('='))
        );
    }
}

/// The expiry a dump reports is the record's own deadline, in absolute unix
/// seconds — and `-1` for a key that never expires, which is upstream's
/// convention in this line.
#[tokio::test]
async fn a_dump_reports_the_deadline_each_record_holds() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    c.line("set forever 0 0 1\r\nv\r\n").await;
    c.line("set soon 0 900 1\r\nv\r\n").await;

    c.send(b"lru_crawler metadump all\r\n").await;
    let body = c.read_until("END\r\n").await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for line in body.lines().filter(|line| line.starts_with("key=")) {
        let exp: i64 = line
            .split(' ')
            .find_map(|field| field.strip_prefix("exp="))
            .expect("every line carries an exp")
            .parse()
            .expect("exp is a number");

        if line.starts_with("key=forever ") {
            assert_eq!(exp, -1, "never expires");
        } else {
            let remaining = exp - now as i64;
            assert!(
                (890..=901).contains(&remaining),
                "expected ~900s of life, got {remaining}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Coalesced writes. A run of consecutive plain `set`s crosses the writer queue
// as one submission (`dispatch::SetBatch`); these pin the semantics that must
// not change because of it, which are all about a pipelined client seeing
// exactly what it would have seen one command at a time.
// ---------------------------------------------------------------------------

/// The reply stream is one `STORED` per `set`, in request order, however many
/// of them arrived in a single read.
#[tokio::test]
async fn a_pipelined_run_of_sets_answers_each_one() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    let mut block = String::new();
    for i in 0..32 {
        block.push_str(&format!("set k{i} 0 0 3\r\nv{i:02}\r\n"));
    }
    conn.send(block.as_bytes()).await;

    let replies = conn.read_until("STORED\r\n").await;
    assert_eq!(replies, "STORED\r\n".repeat(32));

    // And every one of them actually landed.
    for i in 0..32 {
        assert_eq!(
            conn.get(&format!("get k{i}\r\n")).await,
            format!("VALUE k{i} 0 3\r\nv{i:02}\r\nEND\r\n")
        );
    }
}

/// **A `get` in the same block sees the `set` before it.** The run has to be
/// submitted before the read is answered, or a pipelined client reads a value
/// the server has already told it was stored.
#[tokio::test]
async fn a_read_in_the_same_block_sees_the_writes_before_it() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    conn.send(b"set a 0 0 1\r\nA\r\nset b 0 0 1\r\nB\r\nget a\r\n")
        .await;

    assert_eq!(
        conn.read_until("END\r\n").await,
        "STORED\r\nSTORED\r\nVALUE a 0 1\r\nA\r\nEND\r\n"
    );
}

/// **A block that alternates reads and writes is served as runs, in order.**
///
/// Each class goes to the tier that suits it rather than the whole block taking
/// the slowest route any one command needs, and the only thing that makes the
/// answer right is that the runs execute in sequence: every `get` sees the `set`
/// before it and none of the ones after.
#[tokio::test]
async fn an_alternating_block_is_answered_in_order() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    conn.send(b"get k\r\nset k 0 0 3\r\none\r\nget k\r\nset k 0 0 3\r\ntwo\r\nget k\r\n")
        .await;

    assert_eq!(
        conn.read_until("two\r\nEND\r\n").await,
        "END\r\nSTORED\r\nVALUE k 0 3\r\none\r\nEND\r\nSTORED\r\nVALUE k 0 3\r\ntwo\r\nEND\r\n"
    );
}

/// Two writes to one key in a single block keep last-write-wins, and each still
/// gets its own `STORED`.
#[tokio::test]
async fn a_repeated_key_in_one_block_keeps_the_last_write() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    conn.send(b"set k 0 0 5\r\nfirst\r\nset k 0 0 6\r\nsecond\r\nget k\r\n")
        .await;

    assert_eq!(
        conn.read_until("END\r\n").await,
        "STORED\r\nSTORED\r\nVALUE k 0 6\r\nsecond\r\nEND\r\n"
    );
}

/// A guarded write cannot be deferred — its reply is a verdict — so it flushes
/// the run and is answered against what the run wrote.
#[tokio::test]
async fn a_guarded_write_sees_the_run_before_it() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    // `add k` must fail, because the `set k` ahead of it in the same block
    // created the key.
    conn.send(b"set k 0 0 1\r\nA\r\nadd k 0 0 1\r\nB\r\nget k\r\n")
        .await;

    assert_eq!(
        conn.read_until("END\r\n").await,
        "STORED\r\nNOT_STORED\r\nVALUE k 0 1\r\nA\r\nEND\r\n"
    );
}

/// **One rejected write does not take its neighbours down with it.** A batch is
/// one transaction per shard, so a record the store refuses fails the whole
/// submission; the run is then retried one command at a time so each gets the
/// verdict it would have had unbatched.
#[tokio::test]
async fn a_refused_write_does_not_fail_the_run_around_it() {
    // Small enough that one value in the run is over it, and nothing else is.
    let server = TestServer::start_with(|config| config.store.max_value_bytes = 1024).await;
    let mut conn = server.connect().await;

    let oversized = "x".repeat(4096);
    let mut block = String::from("set before 0 0 1\r\nA\r\n");
    block.push_str(&format!(
        "set toobig 0 0 {}\r\n{oversized}\r\n",
        oversized.len()
    ));
    block.push_str("set after 0 0 1\r\nB\r\n");
    conn.send(block.as_bytes()).await;

    let replies = conn.read_until("STORED\r\n").await;
    assert_eq!(
        replies,
        "STORED\r\nSERVER_ERROR object too large for cache\r\nSTORED\r\n"
    );

    // The two good writes are there, and the refused one is not.
    assert_eq!(
        conn.get("get before\r\n").await,
        "VALUE before 0 1\r\nA\r\nEND\r\n"
    );
    assert_eq!(
        conn.get("get after\r\n").await,
        "VALUE after 0 1\r\nB\r\nEND\r\n"
    );
    assert_eq!(conn.get("get toobig\r\n").await, "END\r\n");
}

/// `noreply` suppresses its own response and nothing else's, including inside a
/// coalesced run.
#[tokio::test]
async fn noreply_inside_a_run_suppresses_only_itself() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    conn.send(
        b"set a 0 0 1 noreply\r\nA\r\nset b 0 0 1\r\nB\r\nset c 0 0 1 noreply\r\nC\r\nget c\r\n",
    )
    .await;

    assert_eq!(
        conn.read_until("END\r\n").await,
        "STORED\r\nVALUE c 0 1\r\nC\r\nEND\r\n"
    );
}

/// **`resident_mode` earns inline reads; it does not assume them.** The whole
/// point of the setting is that the unsafe half is conditional on the safe half
/// having worked, so the two must agree however the lock went — locked
/// everywhere it can be (Linux with memlock headroom), locked nowhere it cannot
/// (every other platform, where LMDB's mapping cannot even be located).
#[tokio::test]
async fn resident_mode_serves_reads_inline_only_when_the_map_is_locked() {
    let server = TestServer::start_with(|config| config.store.resident_mode = true).await;
    let mut c = server.connect().await;
    let fields = stat_fields(&mut c, "stats settings\r\n").await;

    let locked = fields.get("vash_map_locked").expect("vash_map_locked");
    let inline = fields.get("vash_inline_reads").expect("vash_inline_reads");
    assert_eq!(
        inline, locked,
        "resident mode must serve reads inline exactly when it locked the map"
    );
}

/// Asking for `inline_reads` directly still means it, whatever the lock did:
/// the check is a service to an operator who wants one, not a veto on one who
/// knows their deployment.
#[tokio::test]
async fn inline_reads_stays_an_explicit_choice() {
    let server = TestServer::start_with(|config| config.store.inline_reads = true).await;
    let mut c = server.connect().await;
    let fields = stat_fields(&mut c, "stats settings\r\n").await;

    assert_eq!(
        fields.get("vash_inline_reads").map(String::as_str),
        Some("yes")
    );
}

/// **The multi-threaded runtime takes a different hand-off**, and it is the one
/// production runs on while `#[tokio::test]` defaults to the other. `run_block`
/// uses `block_in_place` there and `spawn_blocking` on a current-thread runtime,
/// because `block_in_place` panics rather than degrading when there is no other
/// worker to hand the queue to. A test on the default flavour therefore cannot
/// see the path that actually ships.
///
/// This drives writes, reads and a guarded write over the real runtime, so a
/// mistake in that branch is a failed test rather than a panicking server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_multi_threaded_runtime_serves_the_whole_command_surface() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    assert_eq!(conn.line("set k 0 0 1\r\nA\r\n").await, "STORED\r\n");
    assert_eq!(conn.get("get k\r\n").await, "VALUE k 0 1\r\nA\r\nEND\r\n");
    assert_eq!(conn.line("add k 0 0 1\r\nB\r\n").await, "NOT_STORED\r\n");
    assert_eq!(conn.line("delete k\r\n").await, "DELETED\r\n");
    assert_eq!(conn.get("get k\r\n").await, "END\r\n");

    // And a pipelined run of writes, which is the shape the hand-off change was
    // made for: one block, one submission, one reply each.
    let mut block = String::new();
    for i in 0..16 {
        block.push_str(&format!("set p{i} 0 0 1\r\nv\r\n"));
    }
    conn.send(block.as_bytes()).await;
    assert_eq!(conn.read_until("STORED\r\n").await, "STORED\r\n".repeat(16));
}

/// **Admission control must bound writes, not deadlock them.** An awaited write
/// holds a permit across its submit *and* its wait, so a server with one permit
/// serialises every writer in the process. If the permit were ever dropped late
/// — or not at all — this is where it would show, as a hang rather than a wrong
/// answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_share_a_single_write_permit() {
    const CONNECTIONS: usize = 8;
    const EACH: usize = 12;

    // One permit for the whole server: `write_permits` is sized from this.
    let server = TestServer::start_with(|config| config.server.max_blocking_threads = 1).await;

    let mut clients = Vec::new();
    for id in 0..CONNECTIONS {
        let mut conn = server.connect().await;
        clients.push(tokio::spawn(async move {
            for n in 0..EACH {
                let key = format!("c{id}-{n}");
                let line = format!("set {key} 0 0 1\r\nv\r\n");
                assert_eq!(conn.line(&line).await, "STORED\r\n");
            }
        }));
    }
    for client in clients {
        client.await.expect("a writer panicked or hung");
    }

    // Every write landed, so nothing was lost to the permit hand-off.
    let mut conn = server.connect().await;
    for id in 0..CONNECTIONS {
        for n in 0..EACH {
            assert_eq!(
                conn.get(&format!("get c{id}-{n}\r\n")).await,
                format!("VALUE c{id}-{n} 0 1\r\nv\r\nEND\r\n")
            );
        }
    }
}
