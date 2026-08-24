//! What every integration suite needs before it can say anything about a
//! dialect: a configuration that is safe to run in parallel, a bound server,
//! and a shutdown that releases the database.
//!
//! **This was copied into five files.** `memcached.rs` and `redis.rs` held it
//! byte for byte, comments included, and `auth.rs`, `integration.rs` and
//! `tls.rs` each held a variation. Five copies of a harness is five places to
//! fix when the server's startup changes — and they had already drifted:
//! `tls.rs` was missing the `mdbx` block the others carry, so the whole TLS
//! suite ran on LMDB even under `--all-features`, which is exactly the
//! coverage that block exists to provide.
//!
//! Each suite is its own crate, so this is a `mod common;` rather than a
//! library. What stays in the suites is what genuinely differs: their `Conn`
//! helpers, which speak one dialect each.

// Every suite uses a subset of this, and each is compiled as its own crate.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use vash_server::{Config, Server};

/// The configuration every suite starts from.
///
/// Callers tweak it before handing it to [`start`]; what is here is only what
/// they all need. In particular `store.shards` is deliberately left at the
/// default — the suites that want a single environment say so, because for the
/// rest the shard count is part of what is under test.
pub fn config(dir: &Path) -> Config {
    let mut config = Config::default();
    // Port 0: the suites run in parallel and would otherwise fight over 9090.
    config.server.listen = "127.0.0.1:0".parse().expect("a valid loopback address");
    config.store.path = dir.join("db");
    // The whole protocol suite runs against whichever engine this build
    // carries, so `--features mdbx` re-runs it on the second one. See
    // `crates/vash-store/tests/store.rs` for the same arrangement a layer
    // down, and `docs/mdbx-proposal.md` for why the choice is per build.
    #[cfg(feature = "mdbx")]
    {
        config.store.backend = vash_server::config::Backend::Mdbx;
    }
    // Small enough to be cheap in CI, large enough for these tests.
    config.store.map_size_mb = 64;
    // Port 0 again, for the same reason.
    config.observability.admin_listen = "127.0.0.1:0".into();
    config
}

/// A bound, serving node and the handles needed to stop it.
pub struct Running {
    pub addr: SocketAddr,
    /// The TLS port, when the configuration asked for one.
    pub tls_addr: Option<SocketAddr>,
    /// The admin port, when the configuration asked for one.
    pub admin_addr: Option<SocketAddr>,
    /// The swappable TLS acceptor, for the suite that drives a reload without
    /// a signal.
    #[cfg(feature = "tls")]
    pub tls_reloadable: Option<vash_server::tls::Reloadable>,
    /// A handle on the store, for the assertions that read on-disk state
    /// directly rather than through a socket.
    ///
    /// An `Option` because it has to be released before the server shuts down:
    /// the environment cannot close while another handle is outstanding, which
    /// is what [`Running::stop`] takes care of.
    store: Option<Arc<dyn vash_store::Store>>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

/// Binds and starts serving. Panics on failure, which in a test is what you
/// want: a server that will not bind has nothing to report.
pub async fn start(config: Config) -> Running {
    let server = Server::bind(config).await.expect("binding the server");
    let addr = server.local_addr().expect("reading the bound address");
    let tls_addr = server.tls_addr();
    let admin_addr = server.admin_addr();
    let store = Arc::clone(server.store());
    #[cfg(feature = "tls")]
    let tls_reloadable = server.tls_reloadable();

    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        server
            .serve(async {
                let _ = rx.await;
            })
            .await
    });

    Running {
        addr,
        tls_addr,
        admin_addr,
        #[cfg(feature = "tls")]
        tls_reloadable,
        store: Some(store),
        shutdown: Some(tx),
        handle: Some(handle),
    }
}

impl Running {
    pub fn store(&self) -> &Arc<dyn vash_store::Store> {
        self.store.as_ref().expect("store handle released")
    }

    /// Stops the server and waits for it to release the database.
    ///
    /// Needed by any test that reopens the same directory afterwards — under
    /// LMDB, dropping the environment only *schedules* the close, and a reopen
    /// in the same process is refused until it lands.
    pub async fn stop(&mut self) {
        // Must go first: the server closes the environment on shutdown, and
        // cannot while this handle is alive.
        drop(self.store.take());
        drop(self.shutdown.take());
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .expect("server task panicked")
                .expect("server returned an error");
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        drop(self.store.take());
        drop(self.shutdown.take());
        self.handle.take();
    }
}

/// Fetches an admin endpoint, returning `(status code, body)`.
pub async fn admin_get(addr: SocketAddr, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connecting to the admin port");
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
        .await
        .expect("writing the request");

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
