//! The cache server.
//!
//! Exposed as a library as well as a binary so integration tests can start a
//! real server on an ephemeral port and drive it over a real socket, rather
//! than testing a stubbed-out approximation of it.

pub mod admin;
pub mod auth;
pub mod cluster;
pub mod config;
pub mod conn;
pub mod dispatch;
pub mod metrics;
pub mod resp;
pub mod state;

use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use vash_store::{LmdbStore, Store};

pub use config::Config;
pub use state::ServerState;

/// A bound, running server.
pub struct Server {
    listener: TcpListener,
    admin: Option<TcpListener>,
    state: Arc<ServerState>,
    config: Config,
    connections: Arc<Semaphore>,
    /// A separate, smaller budget for connections that have not authenticated
    /// yet. Without it the pre-auth budget *is* the connection budget, and a
    /// stranger can fill it without presenting anything.
    pre_auth: Arc<Semaphore>,
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

        // Started before the listener so a peer that answers instantly on the
        // very first invalidation finds the tasks already running.
        let cluster = cluster::Cluster::start(
            &config.cluster,
            Arc::clone(&store),
            Arc::new(metrics::ClusterMetrics::default()),
        );

        // Loaded before the listener binds: a credential file that will not
        // parse must stop startup, not surface as a refusal on the first
        // client.
        let credentials = auth::Auth::load(&config.auth)?;
        if credentials.required() {
            info!("authentication is required on the cache port");
        } else if credentials.configured() {
            // The rollout's middle step, and worth saying out loud: an operator
            // who thinks they turned it on should be told they have not.
            warn!("credentials are configured but auth.required is false; nothing is refused");
        }

        let auth_state = auth::AuthState::new(
            credentials,
            auth::Limits {
                timeout: std::time::Duration::from_millis(config.auth.timeout_ms),
                max_attempts: config.auth.max_attempts,
                max_connections: match config.auth.max_unauthenticated_connections {
                    // A tenth of the budget, so pre-auth connections cannot
                    // crowd out the authenticated ones.
                    0 => (config.server.max_connections / 10).max(1),
                    explicit => explicit,
                },
            },
        );

        let info = dispatch::server_info(
            store.shard_count() as u16,
            config.store.max_value_bytes,
            cluster.active(),
            config.auth.required,
            config.protocol.listing_enabled,
        );
        let state = ServerState::new(
            Arc::clone(&store),
            info,
            config.protocol,
            auth_state,
            cluster,
            config.store.inline_reads,
        );

        let listener = TcpListener::bind(config.server.listen)
            .await
            .with_context(|| format!("binding {}", config.server.listen))?;

        info!(
            addr = %listener.local_addr()?,
            shards = store.shard_count(),
            "listening"
        );

        // Bound here rather than in `serve` so a port clash fails startup, and
        // so tests can read the assigned port before anything is served.
        let admin = match config.observability.admin_listen.as_str() {
            "" => None,
            addr => Some(
                TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("binding the admin endpoint on {addr}"))?,
            ),
        };

        Ok(Self {
            listener,
            admin,
            pre_auth: Arc::new(Semaphore::new(state.auth.limits.max_connections)),
            state,
            connections: Arc::new(Semaphore::new(config.server.max_connections)),
            config,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub fn admin_addr(&self) -> Option<std::net::SocketAddr> {
        self.admin.as_ref().and_then(|l| l.local_addr().ok())
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
            admin,
            state,
            config,
            connections,
            pre_auth,
        } = self;
        let enforcing = state.auth.current().required();

        // Signalled once the listener is gone, so connections sitting idle
        // between requests let go instead of holding the drain open until it
        // times out.
        let (conn_stop, conn_stopped) = tokio::sync::watch::channel(false);

        // The admin endpoints get their own listener and their own shutdown
        // signal, so a scrape in flight cannot hold up the drain.
        let (admin_stop, admin_stopped) = tokio::sync::oneshot::channel();
        let admin_task = admin.map(|listener| {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                admin::serve(listener, state, async {
                    let _ = admin_stopped.await;
                })
                .await
            })
        });

        // Joined before the store is closed below: a detached task holding an
        // `Arc<ServerState>` would keep the LMDB environment open past
        // shutdown.
        let reload_task = spawn_credential_reload(
            Arc::clone(&state),
            config.auth.clone(),
            conn_stopped.clone(),
        );

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
                        state.metrics.connection_rejected();
                        drop(stream);
                        continue;
                    };

                    // Held until the connection authenticates, and released the
                    // moment it does — so the cap counts connections that have
                    // presented nothing, not connections in total.
                    let pre_auth_permit = if enforcing {
                        match Arc::clone(&pre_auth).try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                debug!(%peer, "refusing connection: too many unauthenticated connections");
                                state.metrics.auth_capacity_rejected();
                                state.metrics.connection_rejected();
                                drop(stream);
                                continue;
                            }
                        }
                    } else {
                        None
                    };

                    let conn_state = Arc::clone(&state);
                    let read_buffer = config.server.read_buffer;
                    let stopping = conn_stopped.clone();
                    conn_state.metrics.connection_opened();
                    tokio::spawn(async move {
                        if let Err(e) = conn::handle(stream, Arc::clone(&conn_state), read_buffer, stopping, pre_auth_permit).await {
                            debug!(%peer, error = %e, "connection ended with an error");
                        }
                        conn_state.metrics.connection_closed();
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
        let _ = conn_stop.send(true);
        let _ = admin_stop.send(());
        if let Some(task) = admin_task {
            let _ = task.await;
        }
        if let Some(task) = reload_task {
            let _ = task.await;
        }
        let outstanding = config.server.max_connections as u32;
        match tokio::time::timeout(DRAIN_TIMEOUT, connections.acquire_many(outstanding)).await {
            Ok(Ok(_permits)) => debug!("all connections drained"),
            Ok(Err(e)) => error!(error = %e, "connection semaphore closed during drain"),
            Err(_) => warn!(
                timeout = ?DRAIN_TIMEOUT,
                "drain timed out; closing with connections still open"
            ),
        }

        // Peer tasks hold their own handle on the store, so they have to be
        // stopped and joined before it can be closed below.
        state.cluster.shutdown().await;

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

/// Reloads the credential table on `SIGHUP`.
///
/// This is the whole of the rotation story — add the new credential, roll the
/// clients, remove the old one — and it is why there is no runtime mutation
/// command and no credential storage inside LMDB. Connections that already
/// authenticated keep the identity they authenticated with; only new attempts
/// see the new table.
///
/// A reload that fails leaves the running table in place and logs. Refusing to
/// start on a bad file is right, because nothing is serving yet; swapping in an
/// empty table because someone truncated the file mid-edit is not.
#[cfg(unix)]
fn spawn_credential_reload(
    state: Arc<ServerState>,
    config: config::AuthConfig,
    mut stopping: tokio::sync::watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut hangup = match signal(SignalKind::hangup()) {
        Ok(stream) => stream,
        Err(e) => {
            // Not fatal: the server works, it just cannot be told to reload.
            error!(error = %e, "could not listen for SIGHUP; credential reload is disabled");
            return None;
        }
    };

    Some(tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = hangup.recv() => match auth::Auth::load(&config) {
                    Ok(reloaded) => {
                        state.auth.replace(reloaded);
                        info!("reloaded the credential table");
                    }
                    Err(e) => error!(error = %format!("{e:#}"), "credential reload failed; keeping the table in use"),
                },
                _ = stopping.changed() => return,
            }
        }
    }))
}

/// Windows has no `SIGHUP`. Rotation there means a restart, which is what the
/// two-step rollout already tolerates.
#[cfg(not(unix))]
fn spawn_credential_reload(
    _state: Arc<ServerState>,
    _config: config::AuthConfig,
    _stopping: tokio::sync::watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    None
}

/// How long shutdown waits for in-flight connections before giving up on them.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
