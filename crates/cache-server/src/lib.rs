//! The cache server.
//!
//! Exposed as a library as well as a binary so integration tests can start a
//! real server on an ephemeral port and drive it over a real socket, rather
//! than testing a stubbed-out approximation of it.

pub mod config;
pub mod conn;
pub mod dispatch;
pub mod state;

use std::sync::Arc;

use anyhow::Context;
use cache_store::{LmdbStore, Store};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

pub use config::Config;
pub use state::ServerState;

/// A bound, running server.
pub struct Server {
    listener: TcpListener,
    state: Arc<ServerState>,
    config: Config,
    connections: Arc<Semaphore>,
}

impl Server {
    /// Opens the store and binds the listener. Returns before accepting, so the
    /// caller can read [`Server::local_addr`] — which is what makes port 0 work
    /// for tests.
    pub async fn bind(config: Config) -> anyhow::Result<Self> {
        config.validate()?;

        let store = LmdbStore::open(&config.store_config())
            .with_context(|| format!("opening store at {}", config.store.path.display()))?;
        let store = Arc::new(store);

        let info = dispatch::server_info(1, config.store.max_value_bytes);
        let state = ServerState::new(Arc::clone(&store), info, config.protocol.flush_enabled);

        let listener = TcpListener::bind(config.server.listen)
            .await
            .with_context(|| format!("binding {}", config.server.listen))?;

        info!(addr = %listener.local_addr()?, "listening");

        Ok(Self {
            listener,
            state,
            connections: Arc::new(Semaphore::new(config.server.max_connections)),
            config,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub fn store(&self) -> &Arc<LmdbStore> {
        &self.state.store
    }

    /// Accepts connections until `shutdown` resolves, then drains in-flight
    /// connections and closes the store.
    pub async fn serve(
        self,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let Self {
            listener,
            state,
            config,
            connections,
        } = self;

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer) = match result {
                        Ok(pair) => pair,
                        Err(e) => {
                            // Per-connection accept errors (a peer that
                            // vanished, a transient fd shortage) must not take
                            // the listener down.
                            error!(error = %e, "accept failed");
                            continue;
                        }
                    };

                    // Refuse past the limit instead of accepting into a backlog
                    // nobody can serve: a client that is told no can fall back,
                    // a client left waiting cannot.
                    let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
                        debug!(%peer, "refusing connection: at the connection limit");
                        drop(stream);
                        continue;
                    };

                    let conn_state = Arc::clone(&state);
                    let read_buffer = config.server.read_buffer;
                    tokio::spawn(async move {
                        if let Err(e) = conn::handle(stream, conn_state, read_buffer).await {
                            debug!(%peer, error = %e, "connection ended with an error");
                        }
                        drop(permit);
                    });
                }

                _ = &mut shutdown => {
                    info!("shutting down");
                    break;
                }
            }
        }

        // Stop accepting, then wait for in-flight connections to finish. Every
        // permit being back means every connection task has ended and dropped
        // its handle on the store.
        drop(listener);
        let outstanding = config.server.max_connections as u32;
        match tokio::time::timeout(DRAIN_TIMEOUT, connections.acquire_many(outstanding)).await {
            Ok(Ok(_permits)) => debug!("all connections drained"),
            Ok(Err(e)) => error!(error = %e, "connection semaphore closed during drain"),
            Err(_) => warn!(
                timeout = ?DRAIN_TIMEOUT,
                "drain timed out; closing with connections still open"
            ),
        }

        // Get everything buffered onto disk before exiting. In `relaxed`
        // durability this is the difference between a clean restart and losing
        // the last few seconds of writes.
        if let Err(e) = state.store.sync() {
            error!(error = %e, "final sync failed");
        }

        // Release the LMDB environment. Dropping it only schedules the close,
        // and LMDB refuses to reopen a path that is still registered in this
        // process, so anything that restarts in-process needs the wait.
        match Arc::try_unwrap(state).map(|state| Arc::try_unwrap(state.store)) {
            Ok(Ok(store)) => store.close(),
            _ => warn!("store still referenced at shutdown; environment left open"),
        }

        Ok(())
    }
}

/// How long shutdown waits for in-flight connections before giving up on them.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
