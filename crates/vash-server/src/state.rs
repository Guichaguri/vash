use std::sync::Arc;

use vash_core::ServerInfo;
use vash_store::Store;

use crate::auth::AuthState;
use crate::cluster::Cluster;

/// Everything a connection needs, shared by `Arc` across all of them.
pub struct ServerState {
    pub store: Arc<dyn Store>,
    pub info: ServerInfo,
    /// Which commands are available and on what budget.
    ///
    /// Held whole rather than unpacked into a field per setting: it is `Copy`,
    /// every field is read by dispatch and by nothing else, and a new toggle
    /// should not have to be declared and threaded twice to reach the one place
    /// that acts on it.
    pub protocol: crate::config::ProtocolConfig,
    /// The credential table and the pre-auth budget. Present even when
    /// authentication is off, so the gate has no special case and `AUTH` can be
    /// answered truthfully during a rollout.
    pub auth: AuthState,
    pub metrics: crate::metrics::ServerMetrics,
    /// Present even with no peers configured, so `CLUSTER` and `/stats` have
    /// something truthful to report and dispatch has no special case.
    pub cluster: Arc<Cluster>,
    /// Read-only requests skip the hop to the storage tier. See
    /// [`crate::config::StoreConfig::inline_reads`].
    pub inline_reads: bool,
    /// How many awaited writes may be in flight at once.
    ///
    /// **Admission control that used to be accidental.** A write submitted from
    /// the blocking pool was bounded by the size of that pool, so at most
    /// `server.max_blocking_threads` of them could be queued at the shard
    /// writers. Awaiting the reply removes the thread and with it the bound —
    /// and measured, an unbounded version flooded the queue: pipelined `SET` on
    /// the default durability fell from 19,047 to 10,822 ops/s with the writers'
    /// queue wait climbing from 5.2 ms to 17.1 ms. Same ceiling, made explicit.
    pub write_permits: Arc<tokio::sync::Semaphore>,
    /// Live `SCAN` cursors. See [`crate::scan`].
    pub scan_cursors: crate::scan::ScanCursors,
    /// When this process began serving, for `stats uptime` and `INFO
    /// uptime_in_seconds`.
    ///
    /// An `Instant` rather than a unix stamp because it is only ever read as an
    /// elapsed duration, and a monotonic clock cannot be walked backwards by
    /// NTP into reporting a negative uptime.
    pub started: std::time::Instant,
    /// How this node is bound and bounded. See [`Binding`].
    pub binding: Binding,
    /// What one connection may consume. See [`ConnLimits`].
    pub limits: ConnLimits,
    /// Who is connected, for `stats conns`. See [`crate::connections`].
    pub connections: crate::connections::Registry,
    /// What was slow and which key is hot. See [`crate::introspect`].
    ///
    /// Present whether or not the `introspect` feature is compiled in, so
    /// dispatch has no `cfg` on its hot path — without the feature the type is
    /// empty and its methods have no bodies.
    pub introspect: crate::introspect::Introspect,
}

/// Where this node is listening, and the ceilings it is running under.
///
/// Held whole for the same reason [`ServerState::protocol`] is: it is `Copy`,
/// every field is read by the stats renderers and by nothing else, and a new
/// limit should not have to be declared and threaded twice to reach the one
/// place that prints it.
#[derive(Debug, Clone, Copy)]
pub struct Binding {
    /// The address the cache port is **actually** bound to.
    ///
    /// The bound address rather than the configured one, because a configured
    /// port of 0 is a request and not an answer — and `stats settings` has to
    /// tell a client which port that turned out to be.
    pub addr: std::net::SocketAddr,
    /// `server.max_connections`, reported as memcached's `max_connections` and
    /// Redis's `maxclients`.
    pub max_connections: u64,
    /// `server.max_blocking_threads`.
    ///
    /// Reported under a `vash_` name rather than as memcached's `threads` or
    /// `num_threads`: those count the workers that serve *connections*, and this
    /// pool does storage work while connections are served by the async runtime.
    /// Two pools, neither of them memcached's — and a name matching is not a
    /// meaning matching.
    pub max_blocking_threads: u64,
    pub read_buffer: u64,
}

/// What one connection may consume, whoever it turns out to be.
///
/// Separate from [`crate::auth::Limits`], which bounds a connection that has
/// **not** authenticated and is therefore conditional on `auth.required`.
/// These two are not: they apply to every connection on the server, which is
/// the whole reason they exist — the pre-auth budget never armed at all in the
/// default configuration, so until these there was no ceiling on a connection's
/// memory and no deadline on one that sent nothing.
#[derive(Debug, Clone, Copy)]
pub struct ConnLimits {
    /// `server.max_buffered_bytes`.
    pub max_buffered_bytes: usize,
    /// `server.first_byte_timeout_ms`. `0` disables the deadline.
    pub first_byte_timeout_ms: u64,
}

impl From<&crate::config::ServerConfig> for ConnLimits {
    fn from(config: &crate::config::ServerConfig) -> Self {
        Self {
            max_buffered_bytes: config.max_buffered_bytes,
            first_byte_timeout_ms: config.first_byte_timeout_ms,
        }
    }
}

impl ServerState {
    // Nine, and they are nine independent things a server is assembled from
    // rather than one thing with nine parts. A parameter struct here would
    // exist only to satisfy the lint, and every caller would fill it in field
    // by field at the call site this signature already is.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<dyn Store>,
        info: ServerInfo,
        protocol: crate::config::ProtocolConfig,
        auth: AuthState,
        cluster: Arc<Cluster>,
        inline_reads: bool,
        binding: Binding,
        limits: ConnLimits,
        introspect: &crate::config::IntrospectConfig,
    ) -> Arc<Self> {
        let write_permits = Arc::new(tokio::sync::Semaphore::new(
            binding.max_blocking_threads.max(1) as usize,
        ));
        Arc::new(Self {
            store,
            info,
            protocol,
            auth,
            metrics: crate::metrics::ServerMetrics::default(),
            cluster,
            inline_reads,
            write_permits,
            scan_cursors: crate::scan::ScanCursors::new(
                protocol.scan_cursors,
                std::time::Duration::from_millis(protocol.scan_cursor_ttl_ms),
            ),
            // Started here rather than passed in: this is the moment the server
            // became able to answer anything.
            started: std::time::Instant::now(),
            binding,
            limits,
            connections: crate::connections::Registry::default(),
            introspect: crate::introspect::Introspect::new(introspect),
        })
    }
}
