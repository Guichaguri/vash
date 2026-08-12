//! The server, driven over a store that is not LMDB.
//!
//! This is the test that makes the [`Store`] trait mean something. `vash-store`
//! documents the trait as keeping the LMDB decision reversible and letting the
//! server be tested against a fake; until M10 phase 3 neither was true, because
//! there was one implementation and every consumer named it by its concrete
//! type. A swap would have touched every use site, and no fake could be
//! substituted.
//!
//! What is exercised here is the whole stack above storage — listener,
//! first-byte protocol detection, all three codecs, dispatch, metrics — with no
//! environment opened and no file on disk. If any of that reaches past the trait
//! again, this stops compiling, which is the point: the boundary is checked by
//! the compiler rather than asserted in a doc comment.
//!
//! [`Store`]: vash_store::Store

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use vash_server::{Config, Server};
use vash_store::Store;
use vash_store::memory::MemoryStore;

struct FakeBackedServer {
    addr: SocketAddr,
    /// Held so a test can look at the store the server is actually using, which
    /// is the other half of what a seam buys.
    store: Arc<MemoryStore>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl FakeBackedServer {
    async fn start() -> Self {
        let mut config = Config::default();
        config.server.listen = "127.0.0.1:0".parse().unwrap();
        config.observability.admin_listen = "127.0.0.1:0".into();
        // Never read: nothing opens a path. Leaving it at the default is itself
        // part of the assertion.
        config.store.path = "/nonexistent-on-purpose".into();

        let store = Arc::new(MemoryStore::new());
        let server = Server::bind_with(config, Arc::clone(&store) as Arc<dyn vash_store::Store>)
            .await
            .expect("binding against the in-memory store");
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
            store,
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

impl Drop for FakeBackedServer {
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
    async fn call(&mut self, request: &[u8], expected: &str) {
        self.stream.write_all(request).await.expect("writing");

        let mut chunk = [0u8; 8192];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while self.buf.len() < expected.len() {
            let read = tokio::time::timeout_at(deadline, self.stream.read(&mut chunk))
                .await
                .unwrap_or_else(|_| {
                    panic!("timed out; have {:?}", String::from_utf8_lossy(&self.buf))
                })
                .expect("reading");
            assert_ne!(read, 0, "server closed while a reply was outstanding");
            self.buf.extend_from_slice(&chunk[..read]);
        }

        let actual = String::from_utf8_lossy(&self.buf[..expected.len()]).into_owned();
        assert_eq!(actual, expected);
        self.buf.drain(..expected.len());
    }
}

/// Builds a RESP request array.
fn resp(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

#[tokio::test]
async fn the_redis_dialect_runs_against_a_store_that_is_not_lmdb() {
    let server = FakeBackedServer::start().await;
    let mut c = server.connect().await;

    c.call(&resp(&["SET", "k", "hello"]), "+OK\r\n").await;
    c.call(&resp(&["GET", "k"]), "$5\r\nhello\r\n").await;
    c.call(&resp(&["EXISTS", "k"]), ":1\r\n").await;
    c.call(&resp(&["TTL", "k"]), ":-1\r\n").await;

    // The phase 1 and 2a primitives, through the fake: it implements the same
    // trait, so it owes the same guarantees.
    c.call(&resp(&["INCR", "n"]), ":1\r\n").await;
    c.call(&resp(&["INCRBY", "n", "41"]), ":42\r\n").await;
    c.call(&resp(&["APPEND", "k", " world"]), ":11\r\n").await;
    c.call(&resp(&["GET", "k"]), "$11\r\nhello world\r\n").await;
    c.call(
        &resp(&["SET", "k", "next", "GET"]),
        "$11\r\nhello world\r\n",
    )
    .await;

    c.call(&resp(&["EXPIRE", "k", "1000"]), ":1\r\n").await;
    c.call(&resp(&["PERSIST", "k"]), ":1\r\n").await;
    // Nothing left to clear, which is what `IfVolatile` is for.
    c.call(&resp(&["PERSIST", "k"]), ":0\r\n").await;

    c.call(&resp(&["DEL", "k"]), ":1\r\n").await;
    c.call(&resp(&["GET", "k"]), "$-1\r\n").await;
}

#[tokio::test]
async fn the_memcached_dialect_runs_against_the_same_fake() {
    // Two dialects over one store is the property that matters: they share the
    // boundary, so a store that satisfies one satisfies both.
    let server = FakeBackedServer::start().await;
    let mut c = server.connect().await;

    c.call(b"set greeting 7 0 5\r\nhello\r\n", "STORED\r\n")
        .await;
    c.call(
        b"get greeting\r\n",
        "VALUE greeting 7 5\r\nhello\r\nEND\r\n",
    )
    .await;

    // `add` on a key that is there is refused; `replace` on one that is not.
    c.call(b"add greeting 0 0 2\r\nno\r\n", "NOT_STORED\r\n")
        .await;
    c.call(b"replace absent 0 0 2\r\nno\r\n", "NOT_STORED\r\n")
        .await;

    c.call(b"set n 0 0 2\r\n10\r\n", "STORED\r\n").await;
    c.call(b"incr n 5\r\n", "15\r\n").await;
    c.call(b"decr n 20\r\n", "0\r\n").await;
    c.call(b"incr absent 1\r\n", "NOT_FOUND\r\n").await;

    c.call(b"delete greeting\r\n", "DELETED\r\n").await;
    c.call(b"get greeting\r\n", "END\r\n").await;
}

#[tokio::test]
async fn tag_invalidation_works_through_the_trait() {
    let server = FakeBackedServer::start().await;
    let mut c = server.connect().await;

    // The tag extension on the classic dialect.
    c.call(b"ms tagged 3 Gnews\r\nabc\r\n", "HD\r\n").await;
    c.call(b"mg tagged v\r\n", "VA 3\r\nabc\r\n").await;

    c.call(b"delete_by_tag news\r\n", "DELETED\r\n").await;
    // Invalidated by generation, exactly as the on-disk implementation does it:
    // the record is still in the map and reads as absent.
    c.call(b"mg tagged v\r\n", "EN\r\n").await;

    assert_eq!(
        server.store.stats().unwrap().tags,
        1,
        "the tag stays registered after invalidation; only its generation moved"
    );
}
