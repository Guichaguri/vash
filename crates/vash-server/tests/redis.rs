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
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.store.path = dir.path().join("db");
        config.store.map_size_mb = 64;
        // Port 0: these run in parallel and would otherwise fight over 9090.
        config.observability.admin_listen = "127.0.0.1:0".into();

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
