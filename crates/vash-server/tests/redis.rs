//! Redis protocol tests over a real socket.
//!
//! Deliberately raw, like the memcached suite: these build RESP arrays by hand
//! and assert on the exact reply bytes, so they pin what a real client library
//! will actually receive rather than what an abstraction claims it received.

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

/// Builds a RESP request array. Every client library sends this form; the
/// inline form is not accepted, and [`inline_commands_are_not_resp`] pins that.
fn request(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("writing");
    }

    async fn fill(&mut self, wanted: usize) {
        let mut chunk = [0u8; 8192];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

        while self.buf.len() < wanted {
            let read = tokio::time::timeout_at(deadline, self.stream.read(&mut chunk))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "timed out waiting for {wanted} bytes; have {:?}",
                        String::from_utf8_lossy(&self.buf)
                    )
                })
                .expect("reading");
            assert_ne!(read, 0, "server closed while a reply was outstanding");
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    /// Sends a command and asserts on the reply byte for byte.
    async fn call(&mut self, args: &[&str], expected: &str) {
        self.send(&request(args)).await;
        self.fill(expected.len()).await;
        let actual = String::from_utf8_lossy(&self.buf[..expected.len()]).into_owned();
        assert_eq!(actual, expected, "reply to {args:?}");
        self.buf.drain(..expected.len());
    }

    /// Sends a command and returns one line, for replies whose exact value
    /// depends on the clock.
    async fn line(&mut self, args: &[&str]) -> String {
        self.send(&request(args)).await;
        self.read_line().await
    }

    /// Reads one more line of a reply already in flight.
    ///
    /// A bulk string is two lines — a length header and then the payload — so a
    /// caller that wants the payload reads the header with [`Conn::line`] and the
    /// rest with this.
    async fn read_line(&mut self) -> String {
        loop {
            if let Some(end) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line = String::from_utf8_lossy(&self.buf[..end + 2]).into_owned();
                self.buf.drain(..end + 2);
                return line;
            }
            self.fill(self.buf.len() + 1).await;
        }
    }
}

/// Turning the dialect off closes the connection at first-byte detection,
/// before the RESP parser sees anything. Answering `-ERR` would mean running
/// the parser we were told not to serve.
#[tokio::test]
async fn a_disabled_dialect_closes_the_connection_without_parsing() {
    let server = TestServer::start_with(|c| c.protocol.resp_enabled = false).await;
    let mut c = server.connect().await;

    c.send(&request(&["SET", "foo", "hello"])).await;

    let mut chunk = [0u8; 64];
    let read = tokio::time::timeout(std::time::Duration::from_secs(5), c.stream.read(&mut chunk))
        .await
        .expect("timed out waiting for the server to close")
        .expect("reading");
    assert_eq!(read, 0, "a disabled dialect answers nothing at all");
}

// ---- strings ------------------------------------------------------------

#[tokio::test]
async fn set_and_get_round_trip() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "foo", "hello"], "+OK\r\n").await;
    c.call(&["GET", "foo"], "$5\r\nhello\r\n").await;
    c.call(&["GET", "absent"], "$-1\r\n").await;
}

#[tokio::test]
async fn values_are_binary_safe() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // A CRLF inside a value is data. A line-oriented parser gets this wrong.
    c.send(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n")
        .await;
    c.fill(5).await;
    assert_eq!(&c.buf[..5], b"+OK\r\n");
    c.buf.drain(..5);

    c.call(&["GET", "k"], "$4\r\na\r\nb\r\n").await;
}

#[tokio::test]
async fn set_conditions_report_a_skipped_write_as_null() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "first", "NX"], "+OK\r\n").await;
    c.call(&["SET", "k", "second", "NX"], "$-1\r\n").await;
    c.call(&["GET", "k"], "$5\r\nfirst\r\n").await;

    c.call(&["SET", "k", "third", "XX"], "+OK\r\n").await;
    c.call(&["SET", "absent", "v", "XX"], "$-1\r\n").await;
    c.call(&["EXISTS", "absent"], ":0\r\n").await;
}

#[tokio::test]
async fn set_with_get_answers_the_old_value_and_never_the_verdict() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "old"], "+OK\r\n").await;
    c.call(&["SET", "k", "new", "GET"], "$3\r\nold\r\n").await;
    c.call(&["GET", "k"], "$3\r\nnew\r\n").await;

    // A key that was not there reads as null whether or not the write applied.
    c.call(&["SET", "fresh", "v", "GET"], "$-1\r\n").await;
    // And the skipped write still reports only what was there.
    c.call(&["SET", "k", "ignored", "NX", "GET"], "$3\r\nnew\r\n")
        .await;
    c.call(&["GET", "k"], "$3\r\nnew\r\n").await;
}

#[tokio::test]
async fn set_clears_an_existing_ttl_unless_told_to_keep_it() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "v", "EX", "1000"], "+OK\r\n").await;
    c.call(&["SET", "k", "v2"], "+OK\r\n").await;
    c.call(&["TTL", "k"], ":-1\r\n").await;

    c.call(&["SET", "k", "v", "EX", "1000"], "+OK\r\n").await;
    c.call(&["SET", "k", "v3", "KEEPTTL"], "+OK\r\n").await;
    let ttl = c.line(&["TTL", "k"]).await;
    assert!(
        ttl == ":1000\r\n" || ttl == ":999\r\n",
        "KEEPTTL should have carried the deadline over, got {ttl:?}"
    );
}

#[tokio::test]
async fn del_and_unlink_count_the_keys_they_removed() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["MSET", "a", "1", "b", "2", "c", "3"], "+OK\r\n")
        .await;
    c.call(&["DEL", "a", "missing", "b"], ":2\r\n").await;
    c.call(&["UNLINK", "c", "missing"], ":1\r\n").await;
    c.call(&["EXISTS", "a", "b", "c"], ":0\r\n").await;
}

#[tokio::test]
async fn mget_keeps_request_order_and_marks_misses() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["MSET", "a", "1", "b", "2"], "+OK\r\n").await;
    c.call(
        &["MGET", "a", "missing", "b"],
        "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n",
    )
    .await;
}

#[tokio::test]
async fn exists_counts_a_key_once_per_mention() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "v"], "+OK\r\n").await;
    c.call(&["EXISTS", "k", "k", "k"], ":3\r\n").await;
    c.call(&["EXISTS", "k", "missing"], ":1\r\n").await;
}

#[tokio::test]
async fn type_reports_string_or_none() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "v"], "+OK\r\n").await;
    // A simple string in both RESP2 and RESP3, and never a null: `none` is the
    // answer for a key that is not there.
    c.call(&["TYPE", "k"], "+string\r\n").await;
    c.call(&["TYPE", "missing"], "+none\r\n").await;

    // Everything this server stores is a string, whatever wrote it.
    c.call(&["INCR", "n"], ":1\r\n").await;
    c.call(&["TYPE", "n"], "+string\r\n").await;

    // An expired key is not there, so it is `none` rather than `string`.
    c.call(&["SET", "gone", "v", "PX", "1"], "+OK\r\n").await;
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    c.call(&["TYPE", "gone"], "+none\r\n").await;

    c.call(
        &["TYPE"],
        "-ERR wrong number of arguments for 'type' command\r\n",
    )
    .await;
    c.call(
        &["TYPE", "a", "b"],
        "-ERR wrong number of arguments for 'type' command\r\n",
    )
    .await;
}

/// `now + millis` overflows `i64` for these, which panicked the connection's
/// task in debug and wrapped to a deadline in the past in release — storing the
/// key pre-expired, the opposite of what was asked. Redis refuses a deadline it
/// cannot represent, so this does too.
#[tokio::test]
async fn an_expiry_too_far_out_to_represent_is_refused() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    const MAX: &str = "9223372036854775807";

    c.call(
        &["SET", "k", "v", "PX", MAX],
        "-ERR invalid expire time in 'set' command\r\n",
    )
    .await;
    c.call(&["EXISTS", "k"], ":0\r\n").await;

    c.call(
        &["MSETEX", "1", "a", "1", "PX", MAX],
        "-ERR invalid expire time in 'msetex' command\r\n",
    )
    .await;
    c.call(&["EXISTS", "a"], ":0\r\n").await;

    c.call(
        &["INCREX", "n", "PX", MAX],
        "-ERR invalid expire time in 'increx' command\r\n",
    )
    .await;

    // The connection is still usable, which is the other half of the bug.
    c.call(&["SET", "k", "v", "EX", "60"], "+OK\r\n").await;
    c.call(&["TTL", "k"], ":60\r\n").await;
}

#[tokio::test]
async fn msetex_applies_all_or_nothing() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["MSETEX", "2", "a", "1", "b", "2", "EX", "1000"], ":1\r\n")
        .await;
    c.call(&["GET", "a"], "$1\r\n1\r\n").await;
    let ttl = c.line(&["TTL", "b"]).await;
    assert!(ttl == ":1000\r\n" || ttl == ":999\r\n", "got {ttl:?}");

    // NX needs *every* key absent, and one of these is not.
    c.call(&["MSETEX", "2", "a", "9", "fresh", "9", "NX"], ":0\r\n")
        .await;
    c.call(&["GET", "a"], "$1\r\n1\r\n").await;
    c.call(&["EXISTS", "fresh"], ":0\r\n").await;

    // XX needs every key present.
    c.call(&["MSETEX", "2", "a", "9", "b", "9", "XX"], ":1\r\n")
        .await;
    c.call(&["GET", "b"], "$1\r\n9\r\n").await;
}

#[tokio::test]
async fn append_creates_the_key_and_reports_the_new_length() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["APPEND", "k", "Hello"], ":5\r\n").await;
    c.call(&["APPEND", "k", " World"], ":11\r\n").await;
    c.call(&["GET", "k"], "$11\r\nHello World\r\n").await;

    // Appending alters the value in place, so the lifetime survives it.
    c.call(&["SET", "t", "a", "EX", "1000"], "+OK\r\n").await;
    c.call(&["APPEND", "t", "b"], ":2\r\n").await;
    let ttl = c.line(&["TTL", "t"]).await;
    assert!(ttl == ":1000\r\n" || ttl == ":999\r\n", "got {ttl:?}");
}

// ---- expiry -------------------------------------------------------------

#[tokio::test]
async fn ttl_distinguishes_absent_from_persistent() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["TTL", "absent"], ":-2\r\n").await;
    c.call(&["SET", "k", "v"], "+OK\r\n").await;
    c.call(&["TTL", "k"], ":-1\r\n").await;
    c.call(&["EXPIRE", "k", "1000"], ":1\r\n").await;
    c.call(&["TTL", "k"], ":1000\r\n").await;
}

/// Redis has separate options for an offset and a stamp, so neither form
/// changes meaning with its magnitude — unlike memcached's `exptime`, whose
/// 30-day threshold must not leak into this dialect.
#[tokio::test]
async fn a_redis_ttl_past_thirty_days_is_still_an_offset() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // 90 days, comfortably past memcached's threshold and an entirely ordinary
    // thing for a Redis client to ask for.
    const NINETY_DAYS: &str = "7776000";

    c.call(&["SET", "k", "v", "EX", NINETY_DAYS], "+OK\r\n")
        .await;
    c.call(&["GET", "k"], "$1\r\nv\r\n").await;
    let ttl = c.line(&["TTL", "k"]).await;
    assert!(
        ttl == ":7776000\r\n" || ttl == ":7775999\r\n",
        "expected 90 days remaining, got {ttl:?}"
    );

    // `EXPIRE` reads the same field and must agree.
    c.call(&["EXPIRE", "k", NINETY_DAYS], ":1\r\n").await;
    let ttl = c.line(&["TTL", "k"]).await;
    assert!(
        ttl == ":7776000\r\n" || ttl == ":7775999\r\n",
        "expected 90 days remaining, got {ttl:?}"
    );
}

#[tokio::test]
async fn expire_conditions_follow_the_current_deadline() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "v"], "+OK\r\n").await;

    // XX needs an existing deadline; NX needs the absence of one.
    c.call(&["EXPIRE", "k", "100", "XX"], ":0\r\n").await;
    c.call(&["TTL", "k"], ":-1\r\n").await;
    c.call(&["EXPIRE", "k", "100", "NX"], ":1\r\n").await;
    c.call(&["EXPIRE", "k", "100", "NX"], ":0\r\n").await;

    // GT only moves a deadline further out, LT only closer in.
    c.call(&["EXPIRE", "k", "50", "GT"], ":0\r\n").await;
    c.call(&["EXPIRE", "k", "200", "GT"], ":1\r\n").await;
    c.call(&["EXPIRE", "k", "500", "LT"], ":0\r\n").await;
    c.call(&["EXPIRE", "k", "100", "LT"], ":1\r\n").await;
    c.call(&["TTL", "k"], ":100\r\n").await;

    // A key with no deadline is infinitely far off: GT never applies to one,
    // LT always does.
    c.call(&["SET", "p", "v"], "+OK\r\n").await;
    c.call(&["EXPIRE", "p", "100", "GT"], ":0\r\n").await;
    c.call(&["EXPIRE", "p", "100", "LT"], ":1\r\n").await;
}

#[tokio::test]
async fn a_deadline_in_the_past_deletes_the_key() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "v"], "+OK\r\n").await;
    c.call(&["EXPIRE", "k", "-1"], ":1\r\n").await;
    c.call(&["EXISTS", "k"], ":0\r\n").await;

    c.call(&["SET", "k", "v"], "+OK\r\n").await;
    c.call(&["EXPIREAT", "k", "1"], ":1\r\n").await;
    c.call(&["GET", "k"], "$-1\r\n").await;

    // And a key that was never there reports nothing to do.
    c.call(&["EXPIRE", "absent", "-1"], ":0\r\n").await;
}

#[tokio::test]
async fn expireat_takes_an_absolute_stamp() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 1000;

    c.call(&["SET", "k", "v"], "+OK\r\n").await;
    c.call(&["EXPIREAT", "k", &deadline.to_string()], ":1\r\n")
        .await;
    let ttl = c.line(&["TTL", "k"]).await;
    assert!(ttl == ":1000\r\n" || ttl == ":999\r\n", "got {ttl:?}");
}

#[tokio::test]
async fn persist_clears_a_deadline_once() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "v", "EX", "1000"], "+OK\r\n").await;
    c.call(&["PERSIST", "k"], ":1\r\n").await;
    c.call(&["TTL", "k"], ":-1\r\n").await;
    // Nothing left to clear, and nothing to clear on a missing key.
    c.call(&["PERSIST", "k"], ":0\r\n").await;
    c.call(&["PERSIST", "absent"], ":0\r\n").await;
}

// ---- arithmetic ---------------------------------------------------------

#[tokio::test]
async fn counters_start_at_zero_and_go_negative() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["INCR", "n"], ":1\r\n").await;
    c.call(&["INCR", "n"], ":2\r\n").await;
    c.call(&["INCRBY", "n", "10"], ":12\r\n").await;
    c.call(&["DECR", "n"], ":11\r\n").await;
    c.call(&["DECRBY", "n", "20"], ":-9\r\n").await;
    // Stored as its decimal text, so a plain GET reads it back.
    c.call(&["GET", "n"], "$2\r\n-9\r\n").await;
}

#[tokio::test]
async fn arithmetic_refuses_a_value_that_is_not_a_number() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "hello"], "+OK\r\n").await;
    c.call(
        &["INCR", "k"],
        "-ERR value is not an integer or out of range\r\n",
    )
    .await;

    // Redis's own strictness: no padding, and no leading zeros.
    c.call(&["SET", "k", "007"], "+OK\r\n").await;
    c.call(
        &["INCR", "k"],
        "-ERR value is not an integer or out of range\r\n",
    )
    .await;
}

#[tokio::test]
async fn counters_overflow_rather_than_wrap() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "n", "9223372036854775807"], "+OK\r\n")
        .await;
    c.call(
        &["INCR", "n"],
        "-ERR increment or decrement would overflow\r\n",
    )
    .await;
    // And the key is left exactly as it was.
    c.call(&["GET", "n"], "$19\r\n9223372036854775807\r\n")
        .await;
}

// ---- atomicity -----------------------------------------------------------
//
// The property the storage engine's arithmetic primitive exists for. `INCR` was
// once a `get` and then a `set` issued from the network tier, so two connections
// could read the same value and both write back one more than it. These fail
// against that shape and pass only when the read and the write are one step
// inside the shard writer's transaction.
//
// Multi-threaded on purpose: the whole point is real overlap.

/// Concurrent `INCR` on one key must sum exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_do_not_lose_an_update() {
    const CONNECTIONS: usize = 8;
    const EACH: usize = 64;

    let server = TestServer::start().await;

    let mut clients = Vec::new();
    for _ in 0..CONNECTIONS {
        let mut conn = server.connect().await;
        clients.push(tokio::spawn(async move {
            for _ in 0..EACH {
                // Every connection sees a different number, so only the total
                // is worth asserting on.
                conn.line(&["INCR", "counter"]).await;
            }
        }));
    }
    for client in clients {
        client.await.expect("increment task");
    }

    let total = (CONNECTIONS * EACH).to_string();
    server
        .connect()
        .await
        .call(
            &["GET", "counter"],
            &format!("${}\r\n{total}\r\n", total.len()),
        )
        .await;
}

/// `SET … GET` must report exactly what it displaced.
///
/// The invariant is a conservation law, and it is what makes this worth testing
/// concurrently: every value written is either handed back to exactly one client
/// as the value it displaced, or is the one still in the key at the end. If the
/// read and the write can interleave, two clients see the same predecessor and
/// one written value vanishes from the accounting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_with_get_reports_exactly_what_it_displaced() {
    const CONNECTIONS: usize = 8;
    const EACH: usize = 16;

    let server = TestServer::start().await;

    let mut clients = Vec::new();
    for client in 0..CONNECTIONS {
        let mut conn = server.connect().await;
        clients.push(tokio::spawn(async move {
            let mut displaced = Vec::new();
            for round in 0..EACH {
                let value = format!("{client}-{round}");
                // `$-1\r\n` for the very first write, which displaced nothing;
                // otherwise a bulk header followed by the previous value.
                let header = conn.line(&["SET", "k", &value, "GET"]).await;
                if header != "$-1\r\n" {
                    displaced.push(conn.read_line().await.trim_end().to_string());
                }
            }
            displaced
        }));
    }

    let mut seen: Vec<String> = Vec::new();
    for client in clients {
        seen.extend(client.await.expect("set task"));
    }

    let survivor = {
        let mut conn = server.connect().await;
        conn.line(&["GET", "k"]).await;
        conn.read_line().await.trim_end().to_string()
    };
    seen.push(survivor);
    seen.sort();
    seen.dedup();

    assert_eq!(
        seen.len(),
        CONNECTIONS * EACH,
        "every value written must be displaced exactly once, or be the survivor"
    );
}

/// Concurrent `APPEND` to one key must not lose a fragment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_do_not_lose_a_fragment() {
    const CONNECTIONS: usize = 8;
    const EACH: usize = 32;

    let server = TestServer::start().await;

    let mut clients = Vec::new();
    for _ in 0..CONNECTIONS {
        let mut conn = server.connect().await;
        clients.push(tokio::spawn(async move {
            for _ in 0..EACH {
                conn.line(&["APPEND", "log", "x"]).await;
            }
        }));
    }
    for client in clients {
        client.await.expect("append task");
    }

    // The bulk header is the assertion: every fragment is the same byte, so a
    // lost concatenation can only show up as a short value.
    let expected = CONNECTIONS * EACH;
    let header = server.connect().await.line(&["GET", "log"]).await;
    assert_eq!(
        header,
        format!("${expected}\r\n"),
        "every append must have contributed a byte"
    );
}

#[tokio::test]
async fn incrbyfloat_answers_with_a_bulk_string() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "f", "10.5"], "+OK\r\n").await;
    c.call(&["INCRBYFLOAT", "f", "0.1"], "$4\r\n10.6\r\n").await;
    // A whole result loses its trailing zero, as Redis does.
    c.call(&["INCRBYFLOAT", "f", "-0.6"], "$2\r\n10\r\n").await;
    c.call(&["INCRBYFLOAT", "fresh", "5.0e3"], "$4\r\n5000\r\n")
        .await;
}

#[tokio::test]
async fn incr_keeps_the_keys_lifetime() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "n", "1", "EX", "1000"], "+OK\r\n").await;
    c.call(&["INCR", "n"], ":2\r\n").await;
    let ttl = c.line(&["TTL", "n"]).await;
    assert!(
        ttl == ":1000\r\n" || ttl == ":999\r\n",
        "INCR alters the value in place, so the deadline stands; got {ttl:?}"
    );
}

// ---- INCREX -------------------------------------------------------------

#[tokio::test]
async fn increx_reports_the_new_value_and_the_increment_applied() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["INCREX", "k"], "*2\r\n:1\r\n:1\r\n").await;
    c.call(&["INCREX", "k", "BYINT", "5"], "*2\r\n:6\r\n:5\r\n")
        .await;
    c.call(&["INCREX", "k", "BYINT", "-10"], "*2\r\n:-4\r\n:-10\r\n")
        .await;
}

#[tokio::test]
async fn increx_skips_an_out_of_bounds_result_and_says_so() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "k", "99"], "+OK\r\n").await;
    // Rejected: the reply reports the unchanged value and a zero increment.
    c.call(
        &["INCREX", "k", "BYINT", "5", "UBOUND", "100"],
        "*2\r\n:99\r\n:0\r\n",
    )
    .await;
    c.call(&["GET", "k"], "$2\r\n99\r\n").await;

    // SATURATE caps it instead, and the reply reflects the smaller increment.
    c.call(
        &["INCREX", "k", "BYINT", "5", "UBOUND", "100", "SATURATE"],
        "*2\r\n:100\r\n:1\r\n",
    )
    .await;
    c.call(&["GET", "k"], "$3\r\n100\r\n").await;
}

#[tokio::test]
async fn increx_saturates_towards_the_bound_that_was_breached() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // The value already sits above the ceiling, so a zero increment still
    // breaches it. Reading the direction off the sign of the increment would
    // clamp this to the floor instead.
    c.call(&["SET", "k", "10"], "+OK\r\n").await;
    c.call(
        &["INCREX", "k", "BYINT", "0", "UBOUND", "5", "SATURATE"],
        "*2\r\n:5\r\n:-5\r\n",
    )
    .await;

    c.call(&["SET", "k", "-10"], "+OK\r\n").await;
    c.call(
        &["INCREX", "k", "BYINT", "0", "LBOUND", "-5", "SATURATE"],
        "*2\r\n:-5\r\n:5\r\n",
    )
    .await;
}

#[tokio::test]
async fn increx_treats_type_overflow_as_a_bound_violation() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "n", "9223372036854775807"], "+OK\r\n")
        .await;
    c.call(&["INCREX", "n"], "*2\r\n:9223372036854775807\r\n:0\r\n")
        .await;
}

#[tokio::test]
async fn increx_enx_sets_a_deadline_only_when_there_is_none() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // The window-counter rate limiter: a fresh key gets the window, and a
    // later hit inside it does not extend it.
    c.call(
        &["INCREX", "hits", "BYINT", "1", "EX", "1000", "ENX"],
        "*2\r\n:1\r\n:1\r\n",
    )
    .await;
    c.call(&["TTL", "hits"], ":1000\r\n").await;

    c.call(
        &["INCREX", "hits", "BYINT", "1", "EX", "10", "ENX"],
        "*2\r\n:2\r\n:1\r\n",
    )
    .await;
    let ttl = c.line(&["TTL", "hits"]).await;
    assert!(
        ttl == ":1000\r\n" || ttl == ":999\r\n",
        "ENX must not reset a window that is already running; got {ttl:?}"
    );

    // Without ENX the deadline is replaced outright.
    c.call(
        &["INCREX", "hits", "BYINT", "1", "EX", "10"],
        "*2\r\n:3\r\n:1\r\n",
    )
    .await;
    c.call(&["TTL", "hits"], ":10\r\n").await;
}

#[tokio::test]
async fn increx_persist_clears_the_deadline() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "n", "5", "EX", "1000"], "+OK\r\n").await;
    c.call(
        &["INCREX", "n", "BYINT", "1", "PERSIST"],
        "*2\r\n:6\r\n:1\r\n",
    )
    .await;
    c.call(&["TTL", "n"], ":-1\r\n").await;
}

#[tokio::test]
async fn increx_byfloat_renders_as_bulk_strings_in_resp2() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SET", "f", "1.5"], "+OK\r\n").await;
    c.call(
        &["INCREX", "f", "BYFLOAT", "0.25"],
        "*2\r\n$4\r\n1.75\r\n$4\r\n0.25\r\n",
    )
    .await;
}

// ---- connection and dialect --------------------------------------------

#[tokio::test]
async fn ping_and_quit() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["PING"], "+PONG\r\n").await;
    c.call(&["PING", "hello"], "$5\r\nhello\r\n").await;
    c.call(&["QUIT"], "+OK\r\n").await;

    // The connection closes after QUIT.
    let mut chunk = [0u8; 16];
    let read = c.stream.read(&mut chunk).await.expect("reading");
    assert_eq!(read, 0, "QUIT should close the connection");
}

#[tokio::test]
async fn hello_negotiates_resp3_and_changes_how_nulls_are_rendered() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // RESP2 until asked otherwise.
    c.call(&["GET", "absent"], "$-1\r\n").await;

    c.send(&request(&["HELLO", "3"])).await;
    c.fill(4).await;
    assert_eq!(&c.buf[..4], b"%7\r\n", "RESP3 answers HELLO with a map");
    // Drain the rest of the handshake before the next assertion.
    c.buf.clear();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut chunk = [0u8; 4096];
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        c.stream.read(&mut chunk),
    )
    .await;

    c.call(&["GET", "absent"], "_\r\n").await;
    c.call(&["MGET", "absent"], "*1\r\n_\r\n").await;

    // INCREX in float mode uses the RESP3 double type; INCRBYFLOAT does not.
    c.call(&["SET", "f", "1.5"], "+OK\r\n").await;
    c.call(
        &["INCREX", "f", "BYFLOAT", "0.25"],
        "*2\r\n,1.75\r\n,0.25\r\n",
    )
    .await;
    c.call(&["INCRBYFLOAT", "f", "0.25"], "$1\r\n2\r\n").await;
}

#[tokio::test]
async fn an_unsupported_protocol_version_is_refused_by_name() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["HELLO", "4"], "-NOPROTO unsupported protocol version\r\n")
        .await;
    // The connection survives it, still speaking RESP2.
    c.call(&["PING"], "+PONG\r\n").await;
}

#[tokio::test]
async fn commands_outside_the_subset_are_rejected_by_name() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["LPUSH", "k", "v"], "-ERR unknown command 'LPUSH'\r\n")
        .await;
    c.call(
        &["SUBSCRIBE", "chan"],
        "-ERR unknown command 'SUBSCRIBE'\r\n",
    )
    .await;
    // And the connection carries on.
    c.call(&["PING"], "+PONG\r\n").await;
}

#[tokio::test]
async fn a_rejected_command_does_not_desynchronise_a_pipeline() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let mut pipeline = request(&["SET", "k", "v"]);
    pipeline.extend_from_slice(&request(&["BOGUS"]));
    pipeline.extend_from_slice(&request(&["GET", "k"]));
    c.send(&pipeline).await;

    let expected = "+OK\r\n-ERR unknown command 'BOGUS'\r\n$1\r\nv\r\n";
    c.fill(expected.len()).await;
    assert_eq!(String::from_utf8_lossy(&c.buf[..expected.len()]), expected);
}

#[tokio::test]
async fn a_protocol_error_is_answered_before_the_connection_closes() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    // A valid opening byte, then framing that cannot be resynchronised.
    c.send(b"*1\r\n+OK\r\n").await;
    let expected = "-ERR Protocol error: invalid bulk length\r\n";
    c.fill(expected.len()).await;
    assert_eq!(String::from_utf8_lossy(&c.buf[..expected.len()]), expected);

    let mut chunk = [0u8; 16];
    let read = c.stream.read(&mut chunk).await.expect("reading");
    assert_eq!(read, 0, "a protocol error closes the connection");
}

#[tokio::test]
async fn inline_commands_are_not_resp() {
    // `get foo\r\n` is a legal inline Redis command and a legal memcached one.
    // The first byte picks the dialect, so this connection is memcached's.
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.send(b"get foo\r\n").await;
    c.fill(5).await;
    assert_eq!(
        &c.buf[..5],
        b"END\r\n",
        "an inline command must be read as memcached, not as RESP"
    );
}

#[tokio::test]
async fn redis_and_memcached_share_the_same_keyspace() {
    let server = TestServer::start().await;

    let mut redis = server.connect().await;
    redis.call(&["SET", "shared", "hello"], "+OK\r\n").await;

    let mut memcached = server.connect().await;
    memcached.send(b"get shared\r\n").await;
    let expected = "VALUE shared 0 5\r\nhello\r\nEND\r\n";
    memcached.fill(expected.len()).await;
    assert_eq!(
        String::from_utf8_lossy(&memcached.buf[..expected.len()]),
        expected,
        "one store, three protocols"
    );
}

#[tokio::test]
async fn arity_and_syntax_errors_name_the_command() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(
        &["GET"],
        "-ERR wrong number of arguments for 'get' command\r\n",
    )
    .await;
    c.call(&["SET", "k", "v", "NX", "XX"], "-ERR syntax error\r\n")
        .await;
    c.call(
        &["SET", "k", "v", "EX", "0"],
        "-ERR invalid expire time in 'set' command\r\n",
    )
    .await;
}

// ---- SCAN ---------------------------------------------------------------
//
// The command carrying the most design risk in this suite: its cursor is an
// integer standing in for a position the server holds, and its guarantee is
// about what a *sequence* of calls returns rather than any single one. Neither
// is observable except end to end.

fn listing_on(config: &mut Config) {
    config.protocol.listing_enabled = true;
}

/// One page: `[cursor, [key …]]`.
async fn scan_page(conn: &mut Conn, args: &[&str]) -> (String, Vec<String>) {
    conn.send(&request(args)).await;

    assert_eq!(conn.read_line().await, "*2\r\n");
    conn.read_line().await; // the cursor's length header
    let next = conn.read_line().await.trim_end().to_string();

    let header = conn.read_line().await;
    let len: usize = header
        .trim_start_matches('*')
        .trim_end()
        .parse()
        .unwrap_or_else(|_| panic!("expected an array of keys, got {header:?}"));

    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        conn.read_line().await; // length header
        keys.push(conn.read_line().await.trim_end().to_string());
    }
    (next, keys)
}

/// Walks a whole keyspace, the way a client library's `scan_iter` does.
async fn scan_all(conn: &mut Conn, extra: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    // Bounded, so a cursor that fails to advance fails the test instead of
    // hanging it.
    for _ in 0..10_000 {
        let mut args = vec!["SCAN", &cursor];
        args.extend_from_slice(extra);
        let (next, keys) = scan_page(conn, &args).await;
        seen.extend(keys);
        if next == "0" {
            seen.sort();
            seen.dedup();
            return seen;
        }
        cursor = next;
    }
    panic!("SCAN did not terminate; a cursor is not advancing");
}

/// The guarantee `SCAN` exists to provide, and the reason its cursor is a key
/// rather than an offset: a key present for the whole walk comes back **exactly**
/// once. Redis promises at least once; resuming by key is what makes this the
/// stronger statement.
#[tokio::test]
async fn scan_returns_every_key_exactly_once() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    for i in 0..500 {
        c.call(&["SET", &format!("k{i:04}"), "v"], "+OK\r\n").await;
    }

    // A small COUNT is the interesting one: it is Redis's own order of
    // magnitude, and it means many pages and many cursors.
    let seen = scan_all(&mut c, &["COUNT", "7"]).await;
    assert_eq!(seen.len(), 500, "every key, and none of them twice");
    assert_eq!(seen[0], "k0000");
    assert_eq!(seen[499], "k0499");
}

/// A pooled client returns its connection between pages, so page two routinely
/// lands on a different socket. This is the reason the cursor table is
/// server-wide rather than per connection.
#[tokio::test]
async fn an_iteration_survives_moving_between_connections() {
    let server = TestServer::start_with(listing_on).await;
    let mut writer = server.connect().await;
    for i in 0..200 {
        writer
            .call(&["SET", &format!("k{i:04}"), "v"], "+OK\r\n")
            .await;
    }

    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    for _ in 0..10_000 {
        // A fresh connection for every single page.
        let mut c = server.connect().await;
        let (next, keys) = scan_page(&mut c, &["SCAN", &cursor, "COUNT", "10"]).await;
        seen.extend(keys);
        if next == "0" {
            break;
        }
        cursor = next;
    }

    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 200);
}

#[tokio::test]
async fn scan_filters_on_match() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    for i in 0..50 {
        c.call(&["SET", &format!("user:{i}"), "v"], "+OK\r\n").await;
    }
    c.call(&["SET", "other", "v"], "+OK\r\n").await;

    assert_eq!(scan_all(&mut c, &["COUNT", "10"]).await.len(), 51);

    let matched = scan_all(&mut c, &["MATCH", "user:*", "COUNT", "10"]).await;
    assert_eq!(matched.len(), 50);
    assert!(matched.iter().all(|key| key.starts_with("user:")));
}

/// `COUNT` above the listing ceiling is **clamped, not refused** — Redis
/// specifies it as a hint and clients pass large values freely. VCP's `limit`
/// is rejected instead, because a client that asked for 10000 and silently got
/// 1024 would page incorrectly there.
#[tokio::test]
async fn a_huge_count_is_clamped_rather_than_refused() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    for i in 0..20 {
        c.call(&["SET", &format!("k{i}"), "v"], "+OK\r\n").await;
    }

    let (cursor, keys) = scan_page(&mut c, &["SCAN", "0", "COUNT", "1000000"]).await;
    assert_eq!(cursor, "0", "one page covered the keyspace");
    assert_eq!(keys.len(), 20);
}

#[tokio::test]
async fn scan_argument_errors_match_redis() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;

    // Redis's own wording, verified against 7.4.
    c.call(&["SCAN", "abc"], "-ERR invalid cursor\r\n").await;
    c.call(&["SCAN", "-1"], "-ERR invalid cursor\r\n").await;
    c.call(&["SCAN", "0", "COUNT", "0"], "-ERR syntax error\r\n")
        .await;
    c.call(&["SCAN", "0", "BOGUS", "x"], "-ERR syntax error\r\n")
        .await;
    c.call(
        &["SCAN"],
        "-ERR wrong number of arguments for 'scan' command\r\n",
    )
    .await;

    // This server's glob has no character classes, and matching `[` as a
    // literal byte would make the pattern silently match nothing.
    c.call(
        &["SCAN", "0", "MATCH", "k[0-9]*"],
        "-ERR character classes are not supported in MATCH\r\n",
    )
    .await;
    // Escaped, it is an ordinary byte and the pattern is accepted.
    c.call(&["SCAN", "0", "MATCH", "k\\[0"], "*2\r\n$1\r\n0\r\n*0\r\n")
        .await;
}

/// Every value here is a string, so any other type genuinely has no keys —
/// which is a completed iteration, not an empty page.
#[tokio::test]
async fn scan_by_type_answers_only_for_strings() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    c.call(&["SET", "k", "v"], "+OK\r\n").await;

    c.call(
        &["SCAN", "0", "TYPE", "string"],
        "*2\r\n$1\r\n0\r\n*1\r\n$1\r\nk\r\n",
    )
    .await;
    c.call(&["SCAN", "0", "TYPE", "hash"], "*2\r\n$1\r\n0\r\n*0\r\n")
        .await;
}

/// Enumerating a keyspace is the same capability whichever dialect asks, so
/// `SCAN` sits behind the same gate as `LIST_KEYS` and is off by default.
#[tokio::test]
async fn scan_is_refused_when_listing_is_disabled() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    c.call(&["SET", "k", "v"], "+OK\r\n").await;
    c.call(&["SCAN", "0"], "-ERR command disabled by configuration\r\n")
        .await;
}

/// A token the table no longer holds is an error, **never a silent restart from
/// the beginning** — which would spin a client's pager forever without ever
/// saying why.
#[tokio::test]
async fn an_unknown_cursor_is_refused_rather_than_restarted() {
    let server = TestServer::start_with(listing_on).await;
    let mut c = server.connect().await;
    for i in 0..50 {
        c.call(&["SET", &format!("k{i}"), "v"], "+OK\r\n").await;
    }

    c.call(
        &["SCAN", "999999"],
        "-ERR scan cursor expired; restart the iteration from 0\r\n",
    )
    .await;
}

/// Capacity eviction takes the *oldest* token, and a live iteration's token is
/// always the newest it was handed — so a pager outlives a table far smaller
/// than the number of pages it walks.
#[tokio::test]
async fn an_iteration_outlives_a_table_smaller_than_its_page_count() {
    let server = TestServer::start_with(|config| {
        config.protocol.listing_enabled = true;
        config.protocol.scan_cursors = 2;
    })
    .await;
    let mut c = server.connect().await;
    for i in 0..100 {
        c.call(&["SET", &format!("k{i:03}"), "v"], "+OK\r\n").await;
    }

    // Twenty pages against a two-slot table.
    assert_eq!(scan_all(&mut c, &["COUNT", "5"]).await.len(), 100);
}

/// A page may come back empty with a live cursor — which Redis permits and
/// clients handle — when the scan budget lands entirely on records that do not
/// match. The walk must still finish, and still return everything.
#[tokio::test]
async fn an_empty_page_with_a_live_cursor_is_not_the_end() {
    let server = TestServer::start_with(|config| {
        config.protocol.listing_enabled = true;
        // Two records examined per call. With a pattern that most of them fail,
        // a page routinely spends its whole budget and collects nothing.
        config.protocol.listing_max_scan = 2;
    })
    .await;
    let mut c = server.connect().await;
    for i in 0..30 {
        c.call(&["SET", &format!("k{i:03}"), "v"], "+OK\r\n").await;
    }
    c.call(&["SET", "wanted", "v"], "+OK\r\n").await;

    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    let mut empty_pages = 0;
    for _ in 0..10_000 {
        let (next, keys) =
            scan_page(&mut c, &["SCAN", &cursor, "MATCH", "wanted", "COUNT", "10"]).await;
        if keys.is_empty() && next != "0" {
            empty_pages += 1;
        }
        seen.extend(keys);
        if next == "0" {
            break;
        }
        cursor = next;
    }

    assert_eq!(seen, ["wanted"], "the walk still found the one match");
    assert!(
        empty_pages > 0,
        "a budget spent on non-matching records should have produced empty pages"
    );
}

// ---- INFO ---------------------------------------------------------------

/// Reads an `INFO` reply into its `field:value` pairs, the way a client
/// library's `parse_info` does.
async fn info(conn: &mut Conn, args: &[&str]) -> std::collections::HashMap<String, String> {
    conn.send(&request(args)).await;
    let header = conn.read_line().await;
    let len: usize = header
        .trim_start_matches(['$', '='])
        .trim_end()
        .parse()
        .unwrap_or_else(|_| panic!("expected a bulk or verbatim header, got {header:?}"));

    conn.fill(len + 2).await;
    let body = String::from_utf8_lossy(&conn.buf[..len]).into_owned();
    conn.buf.drain(..len + 2);

    body.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_string(), value.trim_end().to_string()))
        .collect()
}

#[tokio::test]
async fn info_reports_the_fields_clients_branch_on() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    c.call(&["SET", "k", "v"], "+OK\r\n").await;

    let fields = info(&mut c, &["INFO"]).await;

    // **The load-bearing one.** A client that reads `1` here starts speaking
    // Redis Cluster — `CLUSTER SLOTS`, `MOVED` redirection, hash-slot routing —
    // none of which exists here.
    assert_eq!(fields.get("cluster_enabled").map(String::as_str), Some("0"));

    assert_eq!(
        fields.get("redis_mode").map(String::as_str),
        Some("standalone")
    );
    assert_eq!(fields.get("role").map(String::as_str), Some("master"));
    assert_eq!(fields.get("loading").map(String::as_str), Some("0"));
    assert!(fields["redis_version"].ends_with("-vash"));
    assert_eq!(fields["vash_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(fields["db0"], "keys=1,avg_ttl=0");

    // Counters out of the same snapshot memcached's `stats` prints.
    assert!(fields.contains_key("used_memory"));
    assert!(fields.contains_key("connected_clients"));
    assert!(fields.contains_key("total_commands_processed"));

    // Absent rather than zeroed, because nothing measures them.
    for unmeasured in [
        "used_memory_rss",
        "mem_fragmentation_ratio",
        "latest_fork_usec",
    ] {
        assert!(
            !fields.contains_key(unmeasured),
            "{unmeasured} is not measured and must not be reported"
        );
    }
}

/// Redis's `expires` counts keys carrying a TTL. The nearest counter here is
/// rows in the expiry index, which has one per record whether or not it
/// expires — a different quantity, so the field is absent rather than wrong.
#[tokio::test]
async fn the_keyspace_line_does_not_claim_an_expiry_count() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;
    c.call(&["SET", "forever", "v"], "+OK\r\n").await;
    c.call(&["SET", "soon", "v", "EX", "900"], "+OK\r\n").await;

    let fields = info(&mut c, &["INFO", "keyspace"]).await;
    assert_eq!(fields["db0"], "keys=2,avg_ttl=0");
}

#[tokio::test]
async fn info_selects_sections_and_tolerates_unknown_ones() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let clients = info(&mut c, &["INFO", "clients"]).await;
    assert!(clients.contains_key("connected_clients"));
    assert!(!clients.contains_key("redis_version"), "only that section");

    // Case-insensitive, and several at once.
    let two = info(&mut c, &["INFO", "SERVER", "clients"]).await;
    assert!(two.contains_key("redis_version") && two.contains_key("connected_clients"));

    // `vash` is out of the default set and in `all`, exactly as Redis keeps its
    // optional sections out of a bare `INFO`.
    assert!(!info(&mut c, &["INFO"]).await.contains_key("vash_shards"));
    assert!(
        info(&mut c, &["INFO", "all"])
            .await
            .contains_key("vash_shards")
    );
    assert!(
        info(&mut c, &["INFO", "everything"])
            .await
            .contains_key("vash_shards")
    );

    // Redis answers an unrecognised section with an empty string, not an error.
    c.call(&["INFO", "bogus"], "$0\r\n\r\n").await;
}

/// RESP3 has a verbatim string for text meant to be displayed, and `INFO` is
/// what real Redis sends it for.
#[tokio::test]
async fn info_is_a_verbatim_string_under_resp3() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    let plain = "# Cluster\r\ncluster_enabled:0\r\n";
    c.call(
        &["INFO", "cluster"],
        &format!("${}\r\n{plain}\r\n", plain.len()),
    )
    .await;

    // Negotiate RESP3, then drain the handshake so the next reply starts at a
    // known position. Drained by reading until the socket goes quiet rather
    // than by counting lines, because the handshake's line count depends on
    // which fields are bulk strings and which are integers.
    c.send(&request(&["HELLO", "3"])).await;
    c.fill(4).await;
    assert_eq!(&c.buf[..4], b"%7\r\n", "RESP3 answers HELLO with a map");
    c.buf.clear();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut chunk = [0u8; 4096];
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        c.stream.read(&mut chunk),
    )
    .await;

    c.call(
        &["INFO", "cluster"],
        &format!("={}\r\ntxt:{plain}\r\n", plain.len() + 4),
    )
    .await;
}

// ---- tags ------------------------------------------------------------------
//
// The tag surface this dialect gained: `SETTAGS` and `MSETTAGS` attach names at
// write time, `DELBYTAG` invalidates every record carrying one. Designed in
// `docs/resp-tags.md`, and these pin the wire behaviour a client sees.

#[tokio::test]
async fn settags_attaches_tags_and_delbytag_invalidates_them() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SETTAGS", "article:1", "one", "1", "news"], "+OK\r\n")
        .await;
    c.call(
        &["SETTAGS", "article:2", "two", "2", "news", "author:7"],
        "+OK\r\n",
    )
    .await;
    // Untagged, and therefore not the tag's business.
    c.call(&["SET", "unrelated", "keep"], "+OK\r\n").await;

    c.call(&["GET", "article:1"], "$3\r\none\r\n").await;
    c.call(&["DELBYTAG", "news"], ":1\r\n").await;

    // Both records carried the tag, so both are gone — in one constant-time
    // operation, whatever the count.
    c.call(&["GET", "article:1"], "$-1\r\n").await;
    c.call(&["GET", "article:2"], "$-1\r\n").await;
    c.call(&["GET", "unrelated"], "$4\r\nkeep\r\n").await;

    // The second tag the invalidated record carried is still a live name: the
    // registry keeps it, and nothing that carried it is left.
    c.call(&["DELBYTAG", "author:7"], ":1\r\n").await;
}

/// A record written *after* an invalidation carries the tag's new generation,
/// so it survives — including one written immediately afterwards, which is the
/// case a framework hits when it invalidates and then repopulates.
#[tokio::test]
async fn a_write_after_the_invalidation_survives_it() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SETTAGS", "k", "old", "1", "news"], "+OK\r\n")
        .await;
    c.call(&["DELBYTAG", "news"], ":1\r\n").await;
    c.call(&["SETTAGS", "k", "new", "1", "news"], "+OK\r\n")
        .await;
    c.call(&["GET", "k"], "$3\r\nnew\r\n").await;

    // And the tag still works on it.
    c.call(&["DELBYTAG", "news"], ":1\r\n").await;
    c.call(&["GET", "k"], "$-1\r\n").await;
}

#[tokio::test]
async fn delbytag_counts_the_names_it_knew() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SETTAGS", "k", "v", "2", "a", "b"], "+OK\r\n")
        .await;

    // Two of the three are registered; `never-used` was never seen, so nothing
    // could have carried it. Counted the way `DEL` counts absent keys.
    c.call(&["DELBYTAG", "a", "b", "never-used"], ":2\r\n")
        .await;
    c.call(&["DELBYTAG", "never-used"], ":0\r\n").await;
    c.call(&["GET", "k"], "$-1\r\n").await;
}

#[tokio::test]
async fn msettags_tags_every_pair_in_the_batch() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(
        &[
            "MSETTAGS", "2", "a", "1", "b", "2", "1", "batch", "EX", "1000",
        ],
        ":1\r\n",
    )
    .await;
    c.call(&["GET", "a"], "$1\r\n1\r\n").await;
    let ttl = c.line(&["TTL", "b"]).await;
    assert!(ttl == ":1000\r\n" || ttl == ":999\r\n", "got {ttl:?}");

    // The guard still governs the batch, and a skipped batch tags nothing.
    c.call(&["MSETTAGS", "1", "a", "9", "1", "other", "NX"], ":0\r\n")
        .await;
    c.call(&["DELBYTAG", "other"], ":0\r\n").await;
    c.call(&["GET", "a"], "$1\r\n1\r\n").await;

    c.call(&["DELBYTAG", "batch"], ":1\r\n").await;
    c.call(&["GET", "a"], "$-1\r\n").await;
    c.call(&["GET", "b"], "$-1\r\n").await;
}

/// `SETTAGS` is `SET` with a tag list, and the options have to keep behaving
/// exactly as they do on `SET` — including the ones that decide whether the
/// write happens at all.
#[tokio::test]
async fn settags_keeps_every_set_option() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(
        &["SETTAGS", "k", "first", "1", "t", "EX", "1000"],
        "+OK\r\n",
    )
    .await;
    let ttl = c.line(&["TTL", "k"]).await;
    assert!(ttl == ":1000\r\n" || ttl == ":999\r\n", "got {ttl:?}");

    // NX on a key that exists: skipped, and reported as a null.
    c.call(&["SETTAGS", "k", "second", "1", "t", "NX"], "$-1\r\n")
        .await;
    c.call(&["GET", "k"], "$5\r\nfirst\r\n").await;

    // GET reports what was displaced, and KEEPTTL leaves the deadline alone.
    c.call(
        &["SETTAGS", "k", "second", "1", "t", "GET", "KEEPTTL"],
        "$5\r\nfirst\r\n",
    )
    .await;
    let ttl = c.line(&["TTL", "k"]).await;
    assert!(ttl == ":1000\r\n" || ttl == ":999\r\n", "got {ttl:?}");

    // A tagless `SETTAGS` is a plain write, not an error.
    c.call(&["SETTAGS", "k", "third", "0"], "+OK\r\n").await;
    c.call(&["GET", "k"], "$5\r\nthird\r\n").await;
    c.call(&["DELBYTAG", "t"], ":1\r\n").await;
    // Which is why it survived: the last write carried no tag at all.
    c.call(&["GET", "k"], "$5\r\nthird\r\n").await;
}

/// Every refusal lands before the write, so a rejected command leaves the key
/// exactly as it was — and the connection carries on.
#[tokio::test]
async fn a_bad_tag_list_is_refused_without_writing() {
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(&["SETTAGS", "k", "v", "1", ""], "-ERR invalid tag\r\n")
        .await;
    c.call(&["EXISTS", "k"], ":0\r\n").await;

    let long = "x".repeat(256);
    c.call(&["SETTAGS", "k", "v", "1", &long], "-ERR invalid tag\r\n")
        .await;

    // Past the record format's ceiling: refused by the parser.
    c.call(&["SETTAGS", "k", "v", "256", "t"], "-ERR too many tags\r\n")
        .await;

    // Past the *configured* per-record limit, which only the store knows. The
    // parser lets these through and the store names the refusal.
    let mut over: Vec<String> = vec!["SETTAGS".into(), "k".into(), "v".into(), "33".into()];
    over.extend((0..33).map(|i| format!("t{i}")));
    let over: Vec<&str> = over.iter().map(String::as_str).collect();
    c.call(&over, "-ERR too many tags\r\n").await;

    c.call(
        &["SETTAGS", "k", "v", "3", "a"],
        "-ERR wrong number of arguments for 'settags' command\r\n",
    )
    .await;
    c.call(
        &["DELBYTAG"],
        "-ERR wrong number of arguments for 'delbytag' command\r\n",
    )
    .await;

    c.call(&["EXISTS", "k"], ":0\r\n").await;
    c.call(&["SETTAGS", "k", "v", "1", "t"], "+OK\r\n").await;
}

/// The expiry error names the verb the client actually sent, which is what
/// Redis does and what makes the message actionable.
#[tokio::test]
async fn the_tagged_writes_name_themselves_in_an_expiry_error() {
    const MAX: &str = "9223372036854775807";
    let server = TestServer::start().await;
    let mut c = server.connect().await;

    c.call(
        &["SETTAGS", "k", "v", "1", "t", "PX", MAX],
        "-ERR invalid expire time in 'settags' command\r\n",
    )
    .await;
    c.call(
        &["MSETTAGS", "1", "a", "1", "1", "t", "PX", MAX],
        "-ERR invalid expire time in 'msettags' command\r\n",
    )
    .await;
    c.call(&["EXISTS", "k"], ":0\r\n").await;
}

/// One tag space, three dialects. A tag attached over RESP is the same
/// registered name a memcached client invalidates, and the record it kills is
/// the same record — which is the whole point of the tag surface being a
/// property of the store rather than of a protocol.
#[tokio::test]
async fn a_tag_attached_over_resp_is_invalidated_over_memcached() {
    let server = TestServer::start().await;

    let mut redis = server.connect().await;
    redis
        .call(&["SETTAGS", "shared", "hello", "1", "news"], "+OK\r\n")
        .await;

    let mut memcached = server.connect().await;
    memcached.send(b"delete_by_tag news\r\n").await;
    let expected = "DELETED\r\n";
    memcached.fill(expected.len()).await;
    assert_eq!(
        String::from_utf8_lossy(&memcached.buf[..expected.len()]),
        expected,
        "the tag was registered by the RESP write"
    );

    redis.call(&["GET", "shared"], "$-1\r\n").await;

    // And the reverse: a tag the memcached dialect registered is one `DELBYTAG`
    // knows about.
    memcached.buf.clear();
    memcached.send(b"ms tagged 5 Gsport\r\nvalue\r\n").await;
    let stored = "HD\r\n";
    memcached.fill(stored.len()).await;
    assert_eq!(
        String::from_utf8_lossy(&memcached.buf[..stored.len()]),
        stored
    );
    redis.call(&["DELBYTAG", "sport"], ":1\r\n").await;
    redis.call(&["GET", "tagged"], "$-1\r\n").await;
}

// ---------------------------------------------------------------------------
// Coalesced writes. A run of consecutive plain `SET`s crosses the writer queue
// as one submission (`dispatch::SetBatch`); these pin what a pipelined client
// must still observe.
// ---------------------------------------------------------------------------

/// Sends everything as one block and asserts on the whole reply stream, which
/// is the only way to see that the replies came back in request order.
async fn pipeline(conn: &mut Conn, commands: &[&[&str]], expected: &str) {
    let mut block = Vec::new();
    for args in commands {
        block.extend_from_slice(&request(args));
    }
    conn.send(&block).await;
    conn.fill(expected.len()).await;
    let actual = String::from_utf8_lossy(&conn.buf[..expected.len()]).into_owned();
    assert_eq!(actual, expected);
    conn.buf.drain(..expected.len());
}

/// One `+OK` per `SET`, in order, however many arrived in a single read.
#[tokio::test]
async fn a_pipelined_run_of_sets_answers_each_one() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    let commands: Vec<Vec<&str>> = (0..16).map(|_| vec!["SET", "k", "v"]).collect::<Vec<_>>();
    let refs: Vec<&[&str]> = commands.iter().map(|c| c.as_slice()).collect();
    pipeline(&mut conn, &refs, &"+OK\r\n".repeat(16)).await;
}

/// **A `GET` in the same block sees the `SET` before it.**
#[tokio::test]
async fn a_read_in_the_same_block_sees_the_writes_before_it() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    pipeline(
        &mut conn,
        &[&["SET", "a", "A"], &["SET", "b", "B"], &["GET", "a"]],
        "+OK\r\n+OK\r\n$1\r\nA\r\n",
    )
    .await;
}

/// A conditional write is answered against what the run ahead of it wrote, so
/// `SET … NX` on a key the block just created must decline.
#[tokio::test]
async fn a_conditional_write_sees_the_run_before_it() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    pipeline(
        &mut conn,
        &[&["SET", "k", "A"], &["SET", "k", "B", "NX"], &["GET", "k"]],
        "+OK\r\n$-1\r\n$1\r\nA\r\n",
    )
    .await;
}

/// An error in the middle of a block keeps its place in the reply stream: the
/// held writes ahead of it must be answered before it, not after.
#[tokio::test]
async fn an_error_keeps_its_position_in_a_pipelined_block() {
    let server = TestServer::start().await;
    let mut conn = server.connect().await;

    pipeline(
        &mut conn,
        &[
            &["SET", "a", "A"],
            &["GET"],
            &["SET", "c", "C"],
            &["GET", "a"],
        ],
        "+OK\r\n-ERR wrong number of arguments for 'get' command\r\n+OK\r\n$1\r\nA\r\n",
    )
    .await;
}

/// One rejected write does not take its neighbours down with it: a failed batch
/// is retried a command at a time so each gets its own verdict.
#[tokio::test]
async fn a_refused_write_does_not_fail_the_run_around_it() {
    let server = TestServer::start_with(|config| config.store.max_value_bytes = 1024).await;
    let mut conn = server.connect().await;

    let oversized = "x".repeat(4096);
    pipeline(
        &mut conn,
        &[
            &["SET", "before", "A"],
            &["SET", "toobig", &oversized],
            &["SET", "after", "B"],
            &["GET", "before"],
            &["GET", "toobig"],
        ],
        "+OK\r\n-ERR string exceeds maximum allowed size\r\n+OK\r\n$1\r\nA\r\n$-1\r\n",
    )
    .await;
}
