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
pub mod connections;
pub mod dispatch;
pub mod metrics;
pub mod resp;
pub mod scan;
pub mod state;
pub mod stats;
#[cfg(feature = "tls")]
pub mod tls;

use std::sync::Arc;

use anyhow::Context;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use vash_store::{Store, StoreHandle};

pub use config::Config;
pub use state::ServerState;

/// A bound, running server.
pub struct Server {
    listener: TcpListener,
    /// The TLS listener, when `tls.listen` asked for one.
    ///
    /// Not gated on the feature: without it, `Config::validate` refuses a
    /// configuration that would fill this, so it is permanently `None` and the
    /// accept arm below waits forever on nothing.
    tls: Option<TcpListener>,
    #[cfg(feature = "tls")]
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    admin: Option<TcpListener>,
    state: Arc<ServerState>,
    config: Config,
    connections: Arc<Semaphore>,
    /// A separate, smaller budget for connections that have not authenticated
    /// yet. Without it the pre-auth budget *is* the connection budget, and a
    /// stranger can fill it without presenting anything.
    pre_auth: Arc<Semaphore>,
    /// The concrete store, when this server opened one itself.
    ///
    /// The only place the implementation is named, and it earns it: LMDB's
    /// environment is *scheduled* for closing when the handle drops, and it
    /// refuses to reopen a path still registered in the process — so an
    /// in-process restart has to block until it is really gone. That is an
    /// LMDB-specific lifecycle, so it lives with the code that chose LMDB
    /// rather than on the trait, where every other implementation would have to
    /// carry a method it has no use for.
    ///
    /// `None` when the store came from [`Server::bind_with`], where releasing it
    /// is the caller's business.
    ///
    /// A handle rather than a concrete store since M11: which engine is
    /// underneath is a configuration question, and `vash-server` is the one
    /// crate that should not have to answer it.
    store: Option<StoreHandle>,
}

impl Server {
    /// Opens the store and binds the listener. Returns before accepting, so the
    /// caller can read [`Server::local_addr`] — which is what makes port 0 work
    /// for tests.
    pub async fn bind(config: Config) -> anyhow::Result<Self> {
        config.validate()?;

        let handle = vash_store::open(&config.store_config()).with_context(|| {
            format!(
                "opening the {} store at {}",
                config.store.backend.as_str(),
                config.store.path.display()
            )
        })?;

        // The handle is kept alongside the trait object for exactly one reason,
        // and it is spelled out on the field: an engine may need an explicit,
        // blocking release that `Drop` does not give.
        let store = Arc::clone(handle.store());
        let mut server = Self::bind_with(config, store).await?;
        server.store = Some(handle);
        Ok(server)
    }

    /// Binds against a store the caller supplies.
    ///
    /// This is what makes the [`Store`] seam real rather than decorative: the
    /// server is built against the trait, so a test can run the whole stack —
    /// listener, protocol adapters, dispatch — over an in-memory implementation
    /// and never open an environment. It is also the shape a `libmdbx` swap
    /// would take, which is the reversibility the trait claims.
    ///
    /// Note what is *not* here: no `close`. Releasing the store is the business
    /// of whoever opened it, which for [`Server::bind`] is the server itself and
    /// for a caller of this function is the caller.
    pub async fn bind_with(config: Config, store: Arc<dyn Store>) -> anyhow::Result<Self> {
        config.validate()?;

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

        // Said once at startup because the per-connection close is logged at
        // `debug`, where an operator at the default level will not see it — and
        // the symptom on the client side is a bare disconnect that looks like a
        // network fault rather than a decision someone made.
        if !config.protocol.memcached_enabled {
            info!("the memcached protocol is disabled; those connections will be closed");
        }
        if !config.protocol.resp_enabled {
            info!("the Redis protocol is disabled; those connections will be closed");
        }

        let info = dispatch::server_info(
            config.protocol,
            store.shard_count() as u16,
            config.store.max_value_bytes,
            config.store.tags.max_per_record,
            cluster.active(),
            config.auth.required,
        );
        // Bound before the state is built, because the state reports the
        // address it is *actually* serving on: a configured port of 0 is a
        // request for one, not an answer, and `stats settings` has to tell a
        // client which port that turned out to be.
        let listener = TcpListener::bind(config.server.listen)
            .await
            .with_context(|| format!("binding {}", config.server.listen))?;

        // `resident_mode` is a request, not a setting: it earns inline reads
        // only by having locked every shard's map. Reported either way, because
        // "I asked for this and did not get it" is exactly the thing an
        // operator must not have to infer from a throughput graph.
        let inline_reads = if config.store.resident_mode && !config.store.inline_reads {
            let locked = store.map_locked();
            if locked {
                info!("resident mode: every shard's map is locked; serving reads inline");
            } else {
                warn!(
                    "resident mode: the map could not be locked, so reads keep the \
                     storage-pool hand-off. Raise RLIMIT_MEMLOCK above the store's \
                     size, or set store.inline_reads to assert residency yourself"
                );
            }
            locked
        } else {
            config.store.inline_reads
        };

        let state = ServerState::new(
            Arc::clone(&store),
            info,
            config.protocol,
            auth_state,
            cluster,
            inline_reads,
            state::Binding {
                addr: listener.local_addr()?,
                max_connections: config.server.max_connections as u64,
                max_blocking_threads: config.server.max_blocking_threads as u64,
                read_buffer: config.server.read_buffer as u64,
            },
        );

        info!(
            addr = %listener.local_addr()?,
            shards = store.shard_count(),
            "listening"
        );

        // Built before either listener binds, so a certificate that will not
        // parse stops startup rather than surfacing as a handshake failure on
        // the first client.
        #[cfg(feature = "tls")]
        let tls_acceptor = match config.tls.enabled() {
            false => None,
            true => Some(tls::acceptor(&config.tls).with_context(|| {
                format!("loading the TLS certificate for {}", config.tls.listen)
            })?),
        };

        let tls = match config.tls.enabled() {
            false => None,
            true => {
                let listener = TcpListener::bind(&config.tls.listen)
                    .await
                    .with_context(|| format!("binding the TLS port on {}", config.tls.listen))?;
                info!(addr = %listener.local_addr()?, "listening for TLS");
                Some(listener)
            }
        };

        // Bound here rather than in `serve` so a port clash fails startup, and
        // so tests can read the assigned port before anything is served.
        // Nothing is announced here: `admin::serve` already logs the bound
        // address, which is the operator's confirmation that the endpoints they
        // asked for are the ones being served.
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
            tls,
            #[cfg(feature = "tls")]
            tls_acceptor,
            admin,
            pre_auth: Arc::new(Semaphore::new(state.auth.limits.max_connections)),
            state,
            connections: Arc::new(Semaphore::new(config.server.max_connections)),
            config,
            store: None,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// The TLS port, when one was configured. `None` otherwise.
    ///
    /// Separate from [`Server::local_addr`] for the same reason that one
    /// exists: a configured port of 0 is a request, not an answer, and a test
    /// or a benchmark has to be told which port it got.
    pub fn tls_addr(&self) -> Option<std::net::SocketAddr> {
        self.tls.as_ref().and_then(|l| l.local_addr().ok())
    }

    pub fn admin_addr(&self) -> Option<std::net::SocketAddr> {
        self.admin.as_ref().and_then(|l| l.local_addr().ok())
    }

    pub fn store(&self) -> &Arc<dyn Store> {
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
            tls,
            #[cfg(feature = "tls")]
            tls_acceptor,
            admin,
            state,
            config,
            connections,
            pre_auth,
            store: store_handle,
        } = self;
        let enforcing = state.auth.required();

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
                result = accept_any(&listener, &tls) => {
                    let (stream, peer, encrypted) = match result {
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
                    // Registered here rather than inside the handler so that a
                    // connection is listed from the moment it is accepted —
                    // including one that never sends a byte, which is exactly
                    // the connection an operator running `stats conns` is
                    // usually looking for.
                    let registered = conn_state.connections.open(peer);
                    // Cloned per connection, which is an `Arc` bump: the
                    // acceptor is shared configuration, not per-session state.
                    #[cfg(feature = "tls")]
                    let acceptor = tls_acceptor.clone();
                    #[cfg(feature = "tls")]
                    let handshake_timeout =
                        std::time::Duration::from_millis(config.tls.handshake_timeout_ms);
                    tokio::spawn(async move {
                        let id = registered.id;
                        // Cache traffic is small and latency-sensitive; Nagle
                        // would batch a reply against the next one and add up
                        // to 40ms for nothing — and on a TLS connection it
                        // would batch handshake flights too, which is where it
                        // hurts most. Set here rather than in `conn::handle`,
                        // which no longer knows it holds a socket.
                        let _ = stream.set_nodelay(true);
                        let result = if encrypted {
                            serve_encrypted(
                                stream,
                                #[cfg(feature = "tls")]
                                acceptor,
                                #[cfg(feature = "tls")]
                                handshake_timeout,
                                Arc::clone(&conn_state),
                                read_buffer,
                                stopping,
                                pre_auth_permit,
                                registered,
                            )
                            .await
                        } else {
                            conn::handle(stream, Arc::clone(&conn_state), read_buffer, stopping, pre_auth_permit, registered).await
                        };
                        if let Err(e) = result {
                            debug!(%peer, error = %e, "connection ended with an error");
                        }
                        conn_state.connections.close(id);
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
        // On the blocking pool, not here: a `Sync` crosses the writer queue like
        // any other operation and waits for it, and that wait is not one a
        // runtime worker may take.
        let syncing = Arc::clone(&state.store);
        if let Err(e) = tokio::task::spawn_blocking(move || syncing.sync())
            .await
            .map_err(std::io::Error::other)
            .and_then(|result| result.map_err(std::io::Error::other))
        {
            error!(error = %e, "final sync failed");
        }

        // Release the storage environment, if this server is the one that
        // opened it. Under LMDB, dropping only *schedules* the close and a
        // reopen of the same path in this process is refused until it lands, so
        // an in-process restart needs the wait; other engines close on drop and
        // this costs nothing.
        //
        // The state has to go first: it holds the trait object, which shares a
        // reference count with the handle below.
        if let Some(handle) = store_handle {
            drop(state);
            handle.close();
        }

        Ok(())
    }
}

/// Accepts from the plaintext listener, the TLS one, or whichever answers
/// first.
///
/// Both arms are cancel-safe — `accept` is, and `pending` trivially is — which
/// is what lets this sit inside the `select!` that also watches for shutdown.
async fn accept_any(
    plain: &TcpListener,
    tls: &Option<TcpListener>,
) -> std::io::Result<(TcpStream, std::net::SocketAddr, bool)> {
    match tls {
        None => plain.accept().await.map(|(s, peer)| (s, peer, false)),
        Some(tls) => tokio::select! {
            result = plain.accept() => result.map(|(s, peer)| (s, peer, false)),
            result = tls.accept() => result.map(|(s, peer)| (s, peer, true)),
        },
    }
}

/// Completes the handshake, then serves the session with the same loop a
/// plaintext connection gets.
///
/// The handshake runs here, inside the spawned task and *after* the connection
/// and pre-auth permits have been taken, rather than in the accept loop: it is
/// the most expensive thing an unauthenticated stranger can ask this server to
/// do, so it belongs inside the budget M9 built for exactly that, and a slow
/// one must not stall every other pending accept.
// Eight, because it hands `conn::handle` everything a plaintext connection
// gets plus the two it needs to finish a handshake first. Bundling them into a
// struct would move the argument list rather than shorten it.
#[expect(clippy::too_many_arguments)]
// `stream` and the state are unused in a build without the feature, where this
// function is unreachable but still has to compile.
#[cfg_attr(not(feature = "tls"), allow(unused_variables))]
async fn serve_encrypted(
    stream: TcpStream,
    #[cfg(feature = "tls")] acceptor: Option<tokio_rustls::TlsAcceptor>,
    #[cfg(feature = "tls")] handshake_timeout: std::time::Duration,
    state: Arc<ServerState>,
    read_buffer: usize,
    shutdown: tokio::sync::watch::Receiver<bool>,
    pre_auth: Option<tokio::sync::OwnedSemaphorePermit>,
    registered: Arc<connections::ConnInfo>,
) -> std::io::Result<()> {
    #[cfg(feature = "tls")]
    {
        let acceptor = acceptor.expect("a TLS listener without an acceptor");
        match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
            Ok(Ok(stream)) => {
                conn::handle(stream, state, read_buffer, shutdown, pre_auth, registered).await
            }
            // A failed handshake is a client-side configuration problem far
            // more often than an attack — the wrong CA, an expired
            // certificate, a plaintext client on the wrong port — and it is
            // invisible from the client side, which sees only a closed socket.
            Ok(Err(e)) => {
                debug!(error = %e, "TLS handshake failed");
                Ok(())
            }
            Err(_) => {
                debug!(timeout = ?handshake_timeout, "TLS handshake timed out");
                Ok(())
            }
        }
    }
    // Unreachable: `Config::validate` refuses `tls.listen` in a build without
    // the feature, so nothing ever binds the listener that leads here.
    #[cfg(not(feature = "tls"))]
    Ok(())
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
