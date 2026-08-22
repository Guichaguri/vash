use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

/// Only the settings M0 actually honours are present.
///
/// The surface grows one milestone at a time on purpose: a config key that is
/// accepted but ignored is worse than one that does not exist, because it
/// reads as a working feature.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub store: StoreConfig,
    pub protocol: ProtocolConfig,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub cluster: ClusterConfig,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    /// Rejected beyond this many concurrent connections, rather than accepted
    /// and starved.
    pub max_connections: usize,
    /// Bytes reserved per connection read buffer.
    pub read_buffer: usize,
    /// Threads available for store operations.
    ///
    /// This is the ceiling on concurrent reads, and therefore on LMDB reader
    /// slots in use — `store.max_readers` has to cover it or reads start
    /// failing with `MDB_READERS_FULL` under load. Validation enforces that.
    pub max_blocking_threads: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StoreConfig {
    pub path: PathBuf,
    /// Which storage engine to open.
    ///
    /// **Not interchangeable on an existing database.** The engines write
    /// different file formats, so switching empties the cache — startup refuses
    /// a directory the other one wrote rather than opening it and missing on
    /// every key. `mdbx` also needs a binary built with the `mdbx` feature, and
    /// is refused rather than quietly downgraded when it is not.
    pub backend: Backend,
    /// Independent LMDB environments, and therefore the ceiling on concurrent
    /// writers. `0` picks `min(num_cpus, 2)` — see [`Config::shard_count`] for
    /// why it is capped there, and for the measurement that lowered it from 4.
    ///
    /// Fixed once a database exists: changing it would route every key to a
    /// different environment, so startup refuses to open a mismatched store
    /// rather than silently losing the whole cache.
    pub shards: usize,
    /// Map size **per shard**, not in total.
    pub map_size_mb: usize,
    /// How much of the file to allocate at creation, **per shard**. `0` grows
    /// it on demand.
    ///
    /// Only `backend = "mdbx"` does anything with this: LMDB sizes its file to
    /// `map_size_mb` at creation and leaves it sparse, so it never grows.
    /// Preallocating costs that much disk immediately and buys back what
    /// growing costs the write path — see `docs/benchmarks.md`.
    pub preallocate_mb: usize,
    pub max_readers: u32,
    pub durability: Durability,
    pub max_value_bytes: usize,
    /// Start from an empty database. What `--ephemeral` sets, together with
    /// `lazy` durability.
    pub wipe_on_start: bool,
    /// Let LMDB write dirty pages straight into the map (`MDB_WRITEMAP`).
    ///
    /// Lower peak memory in a large transaction. **Whether it is faster is a
    /// platform question**: on Linux it measured nothing three times, and
    /// natively on Windows it measured 1.08–1.26× under `lazy` on the same
    /// disk — see `docs/performance-proposals.md` §6.
    ///
    /// It also removes `lazy`'s integrity guarantee, because pages are written
    /// in place with no ordering: a crash can leave the database corrupt rather
    /// than merely stale. Pair it with `wipe_on_start`, or run it under
    /// `durable`, or accept rebuilding the cache after a crash.
    pub write_map: bool,
    /// Read every shard's data file at startup, so the map is resident before
    /// the first request instead of faulting in under one.
    ///
    /// This is the other half of `inline_reads`. That flag asks the operator to
    /// assert a resident working set and then removes the protection against
    /// its not being one; this makes the assertion true at the moment the
    /// server starts serving. The cost is startup time proportional to the
    /// data, at sequential-read bandwidth.
    pub prefault: bool,
    /// Run read-only requests on the network worker instead of handing them to
    /// the storage tier.
    ///
    /// The hand-off exists so that a read which page-faults blocks a thread
    /// that is allowed to block, rather than an async worker serving other
    /// connections. That is the right default and stays the default.
    ///
    /// It is also, measurably, the most expensive thing in a request. A hop
    /// costs two thread wake-ups, which the plan estimated at ~200ns and which
    /// measure far higher on Windows — enough that a `PING`, which does no work
    /// at all, spends tens of microseconds of CPU. Turning this on removes the
    /// hop for reads only; writes always go to the storage tier, because they
    /// wait on the writer queue by design.
    ///
    /// Turn it on when the working set is resident — when it is not, a cold
    /// read stalls every connection sharing that worker.
    ///
    /// Prefer `resident_mode`, which asks the server to *make* that true and
    /// enables this only if it succeeded. This flag remains for an operator who
    /// knows their deployment better than the check does.
    pub inline_reads: bool,
    /// Make the working set resident, keep it resident, and serve reads inline
    /// **only if that worked**.
    ///
    /// The two flags above are the halves of a bargain nobody was enforcing.
    /// `prefault` makes the map resident at startup; `inline_reads` bets that it
    /// still is on every subsequent read. Nothing connected them, so the bet was
    /// the operator's to lose — and losing it does not look like a slow read, it
    /// looks like every connection on that worker stalling together.
    ///
    /// This turns the bargain into one the server keeps:
    ///
    /// 1. Prefault every shard, as `prefault` does.
    /// 2. Lock each map into memory, so the kernel cannot reclaim it again.
    /// 3. Serve reads inline **only if every shard came back locked**.
    ///
    /// When the lock is refused — no `RLIMIT_MEMLOCK` headroom, or a platform
    /// where LMDB's mapping cannot be located — the server logs why, leaves
    /// reads on the storage pool, and carries on. The failure mode is the
    /// current default, never a stall.
    ///
    /// **What it does not cover**: pages written after startup. The lock spans
    /// the map's high-water mark at open, so a database that grows while
    /// serving has unlocked pages above it. This is a read-mostly setting for a
    /// working set that is loaded before traffic arrives, which is what a cache
    /// in front of an origin usually is — it is not a promise about a store that
    /// doubles in size at runtime.
    ///
    /// Setting `inline_reads` directly still works and still skips the check.
    pub resident_mode: bool,
    pub write: WriteConfig,
    pub ttl: TtlConfig,
    pub tags: TagConfig,
    pub eviction: EvictionConfig,
}

/// Capacity watermarks, as fractions of a shard's map in use.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EvictionConfig {
    /// Reclamation stops waiting for its interval and runs continuously.
    pub soft: f64,
    /// Live records start being evicted, soonest-to-expire first.
    pub hard: f64,
    /// Writes are refused with `CAPACITY_FULL`; reads and deletes still work.
    pub critical: f64,
    /// Records evicted per pass, bounding how long a pass holds the write
    /// transaction.
    pub batch: usize,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        let defaults = vash_store::EvictionConfig::default();
        Self {
            soft: defaults.soft,
            hard: defaults.hard,
            critical: defaults.critical,
            batch: defaults.batch,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TagConfig {
    /// Ceiling on registered tag names. The registry is held entirely in RAM,
    /// so without a limit a client inventing tag names is a memory leak.
    pub max_tags: usize,
    /// Ceiling on the tags one record may carry. Every tag costs bytes in the
    /// record, a tag-index row per write, and a comparison on every read of the
    /// key, so this bounds what a single client can charge the read path.
    pub max_per_record: usize,
    /// Tag-index entries examined per reclamation pass. While a job is
    /// outstanding these run back to back, so this bounds transaction length
    /// rather than drain rate.
    pub reclaim_batch: usize,
}

impl Default for TagConfig {
    fn default() -> Self {
        let defaults = vash_store::StoreConfig::default();
        Self {
            max_tags: defaults.max_tags,
            max_per_record: defaults.max_tags_per_record,
            reclaim_batch: defaults.write.reclaim_batch,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WriteConfig {
    /// Most operations packed into a single commit.
    pub max_batch: usize,
    /// Queued writes before further ones are refused with `OVERLOADED`.
    pub queue_depth: usize,
    /// Artificial delay before sealing a batch. Zero — the default — batches
    /// whatever queued during the previous commit, which adds no latency when
    /// idle and still forms large batches under load.
    pub linger_us: u64,
    /// How often the writer forces committed data onto the device. `0` never
    /// does.
    ///
    /// Under `lazy` this is the loss window: an OS crash costs the writes newer
    /// than the last one. Under `relaxed` and `durable` the data is already on
    /// the device and this only pushes the meta page, which is what those modes
    /// have always documented and nothing was doing.
    pub sync_interval_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TtlConfig {
    /// Granularity that expiry-index buckets round up to. Coarser means fewer
    /// distinct index keys and less write amplification. It never delays a read
    /// from seeing a key as expired, because reads check the record's exact
    /// timestamp.
    pub bucket_granularity_ms: u64,
    pub sweep_interval_ms: u64,
    /// Most index entries examined per sweep, bounding how long reclamation can
    /// hold the write transaction.
    pub sweep_batch: usize,
}

impl Default for WriteConfig {
    fn default() -> Self {
        let defaults = vash_store::WriteConfig::default();
        Self {
            max_batch: defaults.max_batch,
            queue_depth: defaults.queue_depth,
            linger_us: defaults.linger_us,
            sync_interval_ms: defaults.sync_interval_ms,
        }
    }
}

impl Default for TtlConfig {
    fn default() -> Self {
        let defaults = vash_store::WriteConfig::default();
        Self {
            bucket_granularity_ms: 1000,
            sweep_interval_ms: defaults.sweep_interval_ms,
            sweep_batch: defaults.sweep_batch,
        }
    }
}

/// Mirrors `vash_store::BackendKind`, kept separate for the same reason as
/// [`Durability`]: the store crate takes no serde dependency for the config
/// file's sake.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    #[default]
    Lmdb,
    Mdbx,
}

impl Backend {
    /// The name this engine goes by in the config file and in log lines.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lmdb => "lmdb",
            Self::Mdbx => "mdbx",
        }
    }
}

impl From<Backend> for vash_store::BackendKind {
    fn from(b: Backend) -> Self {
        match b {
            Backend::Lmdb => Self::Lmdb,
            Backend::Mdbx => Self::Mdbx,
        }
    }
}

/// Mirrors `vash_store::Durability`, kept separate so the store crate does not
/// take a serde dependency for the sake of the config file.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    Durable,
    Relaxed,
    #[default]
    Lazy,
}

impl From<Durability> for vash_store::Durability {
    fn from(d: Durability) -> Self {
        match d {
            Durability::Durable => Self::Durable,
            Durability::Relaxed => Self::Relaxed,
            Durability::Lazy => Self::Lazy,
        }
    }
}

/// Who may use the cache port.
///
/// Off by default. The network boundary stays the primary control — a firewall
/// rule stops a party who never sends a byte, where a credential only stops
/// them after they have reached a parser — and this adds a layer rather than
/// replacing one.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Whether unauthenticated connections are refused.
    ///
    /// Separate from the credential table so a rollout can configure
    /// credentials, roll every client and peer, and only then start enforcing.
    /// There is deliberately no third "optional" mode: one where
    /// unauthenticated clients still work is not authentication.
    pub required: bool,
    /// Path to the credential file. Empty uses `VASH_AUTH_SECRET`, or nothing.
    ///
    /// A separate file rather than a key here, because the main config is not
    /// secret and gets committed. See `docs/auth.md` §4 for the format.
    pub file: PathBuf,
    /// How long a connection may stay unauthenticated before it is dropped.
    pub timeout_ms: u64,
    /// Failed attempts on one connection before it is closed.
    pub max_attempts: u32,
    /// Concurrent unauthenticated connections. `0` picks a tenth of
    /// `server.max_connections`, so the pre-auth budget is never the whole
    /// connection budget.
    pub max_unauthenticated_connections: usize,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            required: false,
            file: PathBuf::new(),
            timeout_ms: 5_000,
            max_attempts: 3,
            max_unauthenticated_connections: 0,
        }
    }
}

/// TLS termination on a second listener.
///
/// A separate port rather than an upgrade on the cache port, so that closing
/// `server.listen` is what makes a deployment encrypted-only — a socket that
/// does not exist is a stronger statement than a policy flag. See
/// `docs/tls-proposal.md` §3.2.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TlsConfig {
    /// Where the TLS listener binds. Empty means there is not one, which is
    /// the default — the empty string for "do not bind" is what
    /// `observability.admin_listen` already uses.
    pub listen: String,
    /// PEM certificate chain, leaf first.
    pub cert: PathBuf,
    /// PEM private key, PKCS#8 or SEC1.
    pub key: PathBuf,
    /// How long a handshake may take before the connection is dropped.
    ///
    /// Below `auth.timeout_ms` on purpose: a handshake is a fixed number of
    /// round trips, so a peer that cannot finish one in three seconds is not
    /// going to, and it is holding a pre-auth slot while it tries.
    pub handshake_timeout_ms: u64,
    /// Whether clients must present a certificate, and be identified by it.
    ///
    /// There is deliberately no "optional": a certificate that may be absent
    /// identifies nobody, and a mode where half the connections have an
    /// identity is one an operator cannot reason about. Ask for nothing, or
    /// require and verify.
    pub client_auth: ClientAuth,
    /// PEM bundle of the CA that issued the client certificates. Required when
    /// `client_auth` is `required`.
    pub client_ca: PathBuf,
}

/// Whether the server asks clients for a certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientAuth {
    /// Ask for nothing. Clients authenticate with a credential, or not at all.
    #[default]
    None,
    /// Refuse any connection that does not present a certificate this server's
    /// `client_ca` issued, and whose subject is not in the credential table.
    Required,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            listen: String::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            handshake_timeout_ms: 3_000,
            client_auth: ClientAuth::None,
            client_ca: PathBuf::new(),
        }
    }
}

impl TlsConfig {
    /// Whether a TLS listener was asked for.
    pub fn enabled(&self) -> bool {
        !self.listen.is_empty()
    }
}

/// Peers, and how tag invalidation reaches them.
///
/// Nodes are otherwise shared-nothing: no replication, no consensus, no
/// server-side data movement. This is the only thing that crosses a node
/// boundary, and it crosses as a name and a counter.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    /// Addresses of the other nodes' cache ports. Membership is static: there
    /// is nothing to agree on, and a node simply reports what it was told.
    pub peers: Vec<String>,
    pub delete_by_tag: FanoutMode,
    /// How often a node exchanges tag generations with each of its peers.
    ///
    /// This is the staleness bound in `fanout` mode when a message is lost, and
    /// the only thing that closes the gap for a node that was down or
    /// unreachable.
    pub gossip_interval_ms: u64,
    /// How long a single exchange with a peer may take, including connecting.
    /// Also how long `fanout_sync` waits for an acknowledgement.
    pub fanout_timeout_ms: u64,
    /// Invalidations queued per peer before further ones are dropped.
    ///
    /// Dropping is safe rather than merely tolerable: generations max-merge, so
    /// anti-entropy delivers whatever fan-out lost. An unbounded queue against
    /// a peer that is down would not be.
    pub queue_depth: usize,
    /// Credential the peer connections present. A peer is an ordinary VCP
    /// client on the ordinary port, so with `auth.required` set it has to
    /// authenticate like any other — and a cluster whose peers cannot is one
    /// where invalidation silently stops converging while every node reports
    /// itself healthy. Startup refuses that configuration.
    pub auth_name: String,
    pub auth_secret: String,
    /// Dial peers over TLS rather than in the clear.
    ///
    /// Peers are ordinary VCP clients on the cache port, so this is the client
    /// half of the same feature `[tls]` serves — and it is why an in-process
    /// terminator was chosen over a sidecar: peers dial *out*, and a fronting
    /// proxy would leave this traffic unprotected. It needs the `tls` feature
    /// and a peer list pointed at the other nodes' TLS ports.
    pub tls: bool,
    /// PEM bundle of the CA that issued the peers' certificates.
    ///
    /// Required when `tls` is set. There is deliberately no fallback to the
    /// platform roots: these are internal names with an internal CA, and a
    /// silent fallback would be a way to trust the wrong issuer.
    pub tls_ca: PathBuf,
    /// The name peers' certificates must carry, when it is not the host in
    /// `peers`.
    ///
    /// `peers` holds `host:port` strings, so the name is already there when
    /// nodes are named by DNS. It is *not* there when they are named by IP: an
    /// address has no name in it, and a certificate can only match one via an
    /// IP SAN. Set this to the name one shared certificate carries, or issue
    /// certificates with IP SANs and leave it empty.
    pub tls_server_name: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            delete_by_tag: FanoutMode::default(),
            gossip_interval_ms: 5_000,
            fanout_timeout_ms: 2_000,
            queue_depth: 1_024,
            auth_name: String::new(),
            auth_secret: String::new(),
            tls: false,
            tls_ca: PathBuf::new(),
            tls_server_name: String::new(),
        }
    }
}

impl ClusterConfig {
    /// The name to verify a peer's certificate against.
    ///
    /// The explicit override when there is one, otherwise the host out of the
    /// peer address — which is the right answer whenever peers are named by
    /// DNS, and the reason the override exists whenever they are not.
    pub fn tls_name_for(&self, addr: &str) -> String {
        if !self.tls_server_name.is_empty() {
            return self.tls_server_name.clone();
        }
        // Rsplit, so an IPv6 literal's colons do not confuse the port split.
        match addr.rsplit_once(':') {
            Some((host, _port)) => host.trim_matches(['[', ']']).to_string(),
            None => addr.to_string(),
        }
    }

    /// What peer connections should present, if anything.
    pub fn credential(&self) -> Option<(&str, &str)> {
        if self.auth_secret.is_empty() {
            return None;
        }
        let name = if self.auth_name.is_empty() {
            crate::auth::DEFAULT_NAME
        } else {
            &self.auth_name
        };
        Some((name, &self.auth_secret))
    }
}

/// Mirrors [`vash_core::ClusterMode`], kept separate so the domain crate does
/// not take a serde dependency for the sake of the config file.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FanoutMode {
    /// No fan-out. The client calls every node itself.
    Local,
    /// Reply immediately, forward in the background.
    #[default]
    Fanout,
    /// Reply once reachable peers have acknowledged.
    FanoutSync,
}

impl From<FanoutMode> for vash_core::ClusterMode {
    fn from(mode: FanoutMode) -> Self {
        match mode {
            FanoutMode::Local => Self::Local,
            FanoutMode::Fanout => Self::Fanout,
            FanoutMode::FanoutSync => Self::FanoutSync,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObservabilityConfig {
    /// `json` or `pretty`.
    pub log_format: String,
    pub log_level: String,
    /// Address for `/metrics`, `/health` and `/stats`. Empty — the default —
    /// serves none of them.
    ///
    /// Off unless asked for, because it has no authentication of its own: it
    /// reports the store's size, its hit rate and the cluster's membership to
    /// anyone who can reach it, and a default-on port is one an operator has to
    /// know about in order to close. Enable it with an address, and give it a
    /// private interface — a separate port from cache traffic is what lets a
    /// flood of scrapes not crowd out real requests.
    pub admin_listen: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ProtocolConfig {
    /// `FLUSH` empties the whole cache on request from any client that can
    /// reach the port, so it is off unless deliberately enabled.
    pub flush_enabled: bool,

    /// `LIST_KEYS` and `LIST_TAGS`, off unless deliberately enabled.
    ///
    /// Two separately sufficient reasons, and authentication retires neither.
    /// **Disclosure**: cache keys routinely embed user and session identifiers,
    /// so enumeration turns a reachable port into a dump of who is in the cache,
    /// and every authenticated client here is equally trusted. **Cost**: these
    /// are the only reads whose work is not bounded by the request — every other
    /// one touches as many records as the client named.
    pub listing_enabled: bool,

    /// Records one `LIST_KEYS` call may examine before it stops early.
    ///
    /// This is what bounds how long a single request holds a read transaction
    /// open, which is the thing that blocks LMDB from reusing freed pages. A
    /// page that stops on the budget still advances its cursor, so paging makes
    /// progress through a region of dead or non-matching records rather than
    /// stalling on it.
    ///
    /// It also caps a whole memcached `lru_crawler` dump, which has no cursor in
    /// its grammar and so pages internally: one dump gets one `LIST_KEYS` call's
    /// budget, spent across as many pages as it needs.
    pub listing_max_scan: usize,

    /// Live Redis `SCAN` cursors held at once.
    ///
    /// A `SCAN` cursor must reach the client as an integer — the major client
    /// libraries parse it as one — and this store's listing cursor is a key,
    /// which does not fit in a `u64`. So the server holds the position and hands
    /// out a token. Oldest are dropped first once this many are live; the token
    /// a live iteration needs is always the newest, so only spent ones go. See
    /// [`crate::scan`].
    pub scan_cursors: usize,

    /// How long a `SCAN` cursor stays resumable.
    ///
    /// Enforced when a token is looked up, not only when the table is swept, so
    /// how long a cursor lasts does not depend on how busy the server is.
    pub scan_cursor_ttl_ms: u64,

    /// Whether the memcached text and meta protocols are served.
    ///
    /// On by default: the dialects share the port with VCP and cost nothing
    /// until someone speaks one. Turning a dialect off is for a deployment that
    /// wants one parser reachable rather than three — the adapters are the only
    /// code that reads bytes from unauthenticated clients, so an unused one is
    /// attack surface with no compensating use.
    pub memcached_enabled: bool,

    /// Whether the Redis protocol (RESP2 and RESP3) is served. See
    /// [`memcached_enabled`](Self::memcached_enabled).
    pub resp_enabled: bool,
}

impl ProtocolConfig {
    /// Whether a connection that opened with this dialect may proceed.
    ///
    /// VCP is not disableable: it is the native protocol, and it is the only
    /// one that can report what a server does and does not serve.
    pub fn dialect_enabled(&self, dialect: vash_proto::Protocol) -> bool {
        match dialect {
            vash_proto::Protocol::Vcp => true,
            vash_proto::Protocol::Memcached => self.memcached_enabled,
            vash_proto::Protocol::Resp => self.resp_enabled,
        }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            flush_enabled: false,
            listing_enabled: false,
            // ~10-20ms of walking at the per-record cost measured in M6, which
            // is a reasonable ceiling on one held read transaction.
            listing_max_scan: 100_000,
            // Worst case ~0.5 MB, and only while that many iterations are
            // genuinely in flight. `SCAN` is administrative; this is not a
            // number a healthy server approaches.
            scan_cursors: 1_024,
            scan_cursor_ttl_ms: 60_000,
            // On, unlike the command gates above: these are compatibility with
            // clients someone already has, and defaulting them off would make
            // the drop-in replacement not one.
            memcached_enabled: true,
            resp_enabled: true,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:11311".parse().expect("valid default address"),
            max_connections: 10_000,
            read_buffer: 16 * 1024,
            max_blocking_threads: 128,
        }
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
            backend: Backend::default(),
            map_size_mb: 4096,
            preallocate_mb: 0,
            // Comfortably above the default blocking pool, which is the ceiling
            // on concurrent readers; validation enforces the relationship.
            max_readers: 256,
            durability: Durability::default(),
            max_value_bytes: vash_core::DEFAULT_MAX_VALUE_LEN,
            wipe_on_start: false,
            write_map: false,
            prefault: false,
            inline_reads: false,
            resident_mode: false,
            shards: 0,
            write: WriteConfig::default(),
            ttl: TtlConfig::default(),
            tags: TagConfig::default(),
            eviction: EvictionConfig::default(),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: "pretty".into(),
            log_level: "info".into(),
            admin_listen: String::new(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let config: Self = toml::from_str(&text)
            .with_context(|| format!("parsing config file {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let map_size = self.store.map_size_mb * 1024 * 1024;
        anyhow::ensure!(
            map_size >= vash_store::config::MIN_MAP_SIZE,
            "store.map_size_mb is {} MiB, but the minimum is {} MiB: below that, LMDB can \
             report a full map permanently even after everything has been deleted",
            self.store.map_size_mb,
            vash_store::config::MIN_MAP_SIZE / (1024 * 1024)
        );
        anyhow::ensure!(
            self.store.preallocate_mb <= self.store.map_size_mb,
            "store.preallocate_mb is {} MiB, which is larger than store.map_size_mb ({} MiB);              a file cannot start bigger than its own ceiling",
            self.store.preallocate_mb,
            self.store.map_size_mb
        );
        anyhow::ensure!(self.store.max_readers > 0, "store.max_readers must be > 0");
        // Bounded by u32 because the drain on shutdown reacquires every permit
        // at once, and `Semaphore::acquire_many` counts in u32.
        anyhow::ensure!(
            self.server.max_connections > 0 && self.server.max_connections <= u32::MAX as usize,
            "server.max_connections must be between 1 and {}",
            u32::MAX
        );
        anyhow::ensure!(
            self.store.max_value_bytes <= vash_core::ABSOLUTE_MAX_VALUE_LEN,
            "store.max_value_bytes exceeds the absolute limit of {} bytes",
            vash_core::ABSOLUTE_MAX_VALUE_LEN
        );
        anyhow::ensure!(
            self.server.read_buffer >= vash_proto::vcp::HEADER_LEN,
            "server.read_buffer must hold at least one frame header"
        );
        anyhow::ensure!(
            self.server.max_blocking_threads > 0,
            "server.max_blocking_threads must be > 0"
        );

        // Each concurrent read holds an LMDB reader slot for the life of its
        // transaction, so the slot table has to cover every thread that can be
        // reading at once, plus the writer. Getting this wrong shows up only
        // under load, as MDB_READERS_FULL.
        anyhow::ensure!(
            self.store.max_readers as usize > self.server.max_blocking_threads,
            "store.max_readers ({}) must exceed server.max_blocking_threads ({}), \
             or reads will fail with MDB_READERS_FULL under load",
            self.store.max_readers,
            self.server.max_blocking_threads
        );

        // A configuration that asks for TLS in a binary that cannot serve it
        // must stop startup. Downgrading to plaintext would leave an operator
        // believing traffic is encrypted when it is not, which is the worst
        // failure available here — the same reason `store.backend = "mdbx"` is
        // refused rather than quietly swapped for LMDB.
        #[cfg(not(feature = "tls"))]
        anyhow::ensure!(
            !self.tls.enabled(),
            "tls.listen is set, but this binary was built without the `tls` feature and \
             cannot terminate TLS. Rebuild with `--features tls`, or clear tls.listen — \
             it will not fall back to serving that port in the clear"
        );
        anyhow::ensure!(
            !self.tls.enabled()
                || !(self.tls.cert.as_os_str().is_empty() || self.tls.key.as_os_str().is_empty()),
            "tls.listen is set but tls.cert or tls.key is empty; a TLS listener with no \
             certificate has nothing to present"
        );
        anyhow::ensure!(
            self.tls.enabled()
                || (self.tls.cert.as_os_str().is_empty() && self.tls.key.as_os_str().is_empty()),
            "tls.cert or tls.key is set but tls.listen is empty, so nothing will serve them. \
             Set tls.listen, or clear both"
        );

        // Same rule as `tls.listen`, for the same reason: a node told to reach
        // its peers over TLS by a binary that cannot must stop, not fall back
        // to gossiping tag generations in the clear.
        #[cfg(not(feature = "tls"))]
        anyhow::ensure!(
            !self.cluster.tls,
            "cluster.tls is set, but this binary was built without the `tls` feature.              Rebuild with `--features tls`, or clear cluster.tls — it will not fall back              to dialling peers in the clear"
        );
        anyhow::ensure!(
            !self.cluster.tls || !self.cluster.tls_ca.as_os_str().is_empty(),
            "cluster.tls is set but cluster.tls_ca is empty. Peers are verified against an              explicit CA; there is no fallback to the platform roots, because these are              internal names issued by an internal authority"
        );

        // Peers are ordinary VCP clients on the ordinary port, so enabling auth
        // without giving them a credential breaks tag fan-out and gossip across
        // the whole cluster — and it breaks in the worst available shape:
        // writes keep working, TAG_SYNC starts being refused, and invalidations
        // quietly stop converging while every node reports itself healthy.
        // Refusing to start is the only failure mode an operator cannot miss.
        anyhow::ensure!(
            !self.auth.required
                || self.cluster.peers.is_empty()
                || self.cluster.credential().is_some(),
            "auth.required is set and {} peers are configured, but cluster.auth_secret is \
             empty. Peer traffic uses the cache port like any other client, so the peers \
             would be refused and tag invalidation would stop converging across the cluster \
             while every node still reported itself healthy",
            self.cluster.peers.len()
        );
        anyhow::ensure!(
            self.auth.max_attempts > 0,
            "auth.max_attempts must be > 0, or no credential could ever be presented"
        );
        anyhow::ensure!(
            self.auth.timeout_ms > 0,
            "auth.timeout_ms must be > 0, or a connection would be dropped before it could \
             authenticate"
        );

        anyhow::ensure!(
            self.store.write.max_batch > 0,
            "store.write.max_batch must be > 0"
        );
        anyhow::ensure!(
            self.store.write.queue_depth > 0,
            "store.write.queue_depth must be > 0"
        );
        anyhow::ensure!(
            self.store.ttl.sweep_interval_ms > 0,
            "store.ttl.sweep_interval_ms must be > 0"
        );
        anyhow::ensure!(
            self.store.ttl.sweep_batch > 0,
            "store.ttl.sweep_batch must be > 0"
        );
        anyhow::ensure!(
            self.store.ttl.bucket_granularity_ms > 0,
            "store.ttl.bucket_granularity_ms must be > 0"
        );
        anyhow::ensure!(
            self.store.tags.max_tags > 0,
            "store.tags.max_tags must be > 0"
        );
        anyhow::ensure!(
            self.store.tags.max_per_record > 0,
            "store.tags.max_per_record must be > 0"
        );
        anyhow::ensure!(
            self.store.tags.max_per_record <= vash_core::ABSOLUTE_MAX_TAGS,
            "store.tags.max_per_record exceeds the absolute limit of {}, \
             which the record header cannot describe",
            vash_core::ABSOLUTE_MAX_TAGS
        );
        anyhow::ensure!(
            self.store.tags.reclaim_batch > 0,
            "store.tags.reclaim_batch must be > 0"
        );
        // A budget of zero would let a listing examine one record per request,
        // so a client would page forever and never finish. Refused rather than
        // treated as "unlimited", which is the other thing someone might mean
        // by it and the more dangerous one.
        anyhow::ensure!(
            self.protocol.listing_max_scan > 0,
            "protocol.listing_max_scan must be > 0"
        );
        // Zero of either would make every `SCAN` past the first page answer
        // "cursor expired", which is a working server that cannot complete an
        // iteration — a failure better refused at startup than discovered by a
        // client's pager.
        anyhow::ensure!(
            self.protocol.scan_cursors > 0,
            "protocol.scan_cursors must be > 0"
        );
        anyhow::ensure!(
            self.protocol.scan_cursor_ttl_ms > 0,
            "protocol.scan_cursor_ttl_ms must be > 0"
        );

        let evict = &self.store.eviction;
        anyhow::ensure!(
            (0.0..1.0).contains(&evict.soft)
                && (0.0..1.0).contains(&evict.hard)
                && (0.0..=1.0).contains(&evict.critical),
            "store.eviction watermarks must be fractions between 0 and 1"
        );
        // Out of order, the levels would fight: eviction would trigger before
        // reclamation, or writes would be refused before anything was freed.
        anyhow::ensure!(
            evict.soft < evict.hard && evict.hard < evict.critical,
            "store.eviction watermarks must increase: soft ({}) < hard ({}) < critical ({})",
            evict.soft,
            evict.hard,
            evict.critical
        );
        anyhow::ensure!(evict.batch > 0, "store.eviction.batch must be > 0");

        anyhow::ensure!(
            self.cluster.gossip_interval_ms > 0,
            "cluster.gossip_interval_ms must be > 0"
        );
        anyhow::ensure!(
            self.cluster.fanout_timeout_ms > 0,
            "cluster.fanout_timeout_ms must be > 0"
        );
        anyhow::ensure!(
            self.cluster.queue_depth > 0,
            "cluster.queue_depth must be > 0"
        );
        for peer in &self.cluster.peers {
            // Resolved lazily on each connection so a peer that is down at
            // startup is not fatal, but an address that can never parse is a
            // typo worth failing on rather than retrying forever.
            anyhow::ensure!(
                peer.contains(':') && !peer.starts_with(':'),
                "cluster.peers entry {peer:?} is not a host:port address"
            );
        }
        Ok(())
    }

    /// Shards actually used, resolving the `0` default.
    ///
    /// **Capped at 2**, lowered from 4 when `lazy` became the default.
    ///
    /// Sharding buys concurrent *writers* and nothing else — LMDB reads are
    /// already lock-free and concurrent within one environment — so it only pays
    /// when a writer thread is the bottleneck. It costs something on every
    /// write: splitting the offered load across N queues divides the mean batch
    /// by roughly N, so each commit amortises over fewer records.
    ///
    /// The cap was 4 because the writer *was* the bottleneck: commits waited for
    /// the device, so a second writer had real work to take. `lazy` stops a
    /// commit waiting for the device, which removes the benefit and leaves the
    /// cost. Re-measured on a four-core container, `SET`-only, medians of three
    /// alternating rounds:
    ///
    /// | shards | pipeline 16 | pipeline 1 | mean batch at pipeline 1 |
    /// |---:|---:|---:|---:|
    /// | 1 | 83,925 | **66,605** | 42.3 |
    /// | 2 | **140,767** | 42,357 | 5.5 |
    /// | 4 | 126,292 | 24,901 | 1.9 |
    /// | 8 | 47,554 | 19,922 | 1.5 |
    ///
    /// Two is the only count that is not beaten by 4 in either shape, and the
    /// batch column is the mechanism: at pipeline 1 four shards see a batch of
    /// 1.9, which is group commit doing nothing at all.
    ///
    /// **This is one box and one workload.** A machine with many more cores under
    /// sustained write load may well want more writers than two, and the setting
    /// is there for that. What the measurement rules out is the old default
    /// being right for the new one.
    pub fn shard_count(&self) -> usize {
        match self.store.shards {
            0 => std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .min(2),
            n => n,
        }
    }

    pub fn store_config(&self) -> vash_store::StoreConfig {
        vash_store::StoreConfig {
            path: self.store.path.clone(),
            backend: self.store.backend.into(),
            shards: self.shard_count(),
            map_size: self.store.map_size_mb * 1024 * 1024,
            preallocate: self.store.preallocate_mb * 1024 * 1024,
            max_readers: self.store.max_readers,
            durability: self.store.durability.into(),
            max_value_len: self.store.max_value_bytes,
            wipe_on_start: self.store.wipe_on_start,
            write_map: self.store.write_map,
            // `resident_mode` implies prefaulting: locking pages that were
            // never read in would pin an empty map and answer `true` to a
            // question about data that is not there.
            prefault: self.store.prefault || self.store.resident_mode,
            lock_map: self.store.resident_mode,
            bucket_granularity_ms: self.store.ttl.bucket_granularity_ms,
            max_tags: self.store.tags.max_tags,
            max_tags_per_record: self.store.tags.max_per_record,
            write: vash_store::WriteConfig {
                max_batch: self.store.write.max_batch,
                queue_depth: self.store.write.queue_depth,
                linger_us: self.store.write.linger_us,
                sync_interval_ms: self.store.write.sync_interval_ms,
                sweep_interval_ms: self.store.ttl.sweep_interval_ms,
                sweep_batch: self.store.ttl.sweep_batch,
                reclaim_batch: self.store.tags.reclaim_batch,
                eviction: vash_store::EvictionConfig {
                    soft: self.store.eviction.soft,
                    hard: self.store.eviction.hard,
                    critical: self.store.eviction.critical,
                    batch: self.store.eviction.batch,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Config::default().validate().unwrap();
    }

    /// The endpoints report the store's contents and the cluster's shape with
    /// no authentication in front of them, so nothing is served until an
    /// operator names an address.
    #[test]
    fn the_admin_endpoint_is_off_until_given_an_address() {
        assert_eq!(Config::default().observability.admin_listen, "");
    }

    /// The shipped example is the documentation for every setting, and
    /// `deny_unknown_fields` means a key renamed in one place and not the other
    /// makes it unusable. Cheaper to catch here than in someone's deployment.
    #[test]
    fn the_example_file_parses_and_validates() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vash.example.toml");
        Config::load(&path).expect("vash.example.toml must be a usable config");
    }

    /// The one misconfiguration that could leave an operator believing traffic
    /// is encrypted when it is not, so it must stop startup rather than warn.
    #[test]
    #[cfg(not(feature = "tls"))]
    fn a_tls_listener_is_refused_in_a_build_that_cannot_serve_it() {
        let mut config = Config::default();
        config.tls.listen = "0.0.0.0:11312".into();
        config.tls.cert = PathBuf::from("cert.pem");
        config.tls.key = PathBuf::from("key.pem");

        let error = config.validate().expect_err("must not start");
        let message = format!("{error}");
        assert!(
            message.contains("`tls` feature"),
            "the message has to name the feature, since the fix is a rebuild: {message}"
        );
        assert!(
            message.contains("will not fall back"),
            "and it has to say what it is *not* doing: {message}"
        );
    }

    /// A certificate with nothing serving it, and a listener with no
    /// certificate, are both configuration errors rather than defaults.
    #[test]
    fn a_half_configured_tls_section_is_refused() {
        let mut config = Config::default();
        config.tls.cert = PathBuf::from("cert.pem");
        config.tls.key = PathBuf::from("key.pem");
        assert!(
            config.validate().is_err(),
            "certificates with no listener serve nobody"
        );

        let mut config = Config::default();
        config.tls.listen = "0.0.0.0:11312".into();
        assert!(
            config.validate().is_err(),
            "a listener with no certificate has nothing to present"
        );
    }

    #[test]
    fn parses_a_partial_file_and_fills_the_rest_from_defaults() {
        let config: Config = toml::from_str(
            r#"
            [server]
            listen = "0.0.0.0:1234"

            [store]
            durability = "durable"
            "#,
        )
        .unwrap();

        assert_eq!(config.server.listen.port(), 1234);
        assert_eq!(config.store.durability, Durability::Durable);
        // Untouched keys keep their defaults.
        assert_eq!(config.store.map_size_mb, 4096);
    }

    /// The compatibility dialects are on unless turned off, and the native one
    /// has no switch at all — it is what reports the other two.
    #[test]
    fn dialects_are_served_unless_disabled_and_vcp_always_is() {
        use vash_proto::Protocol;

        let mut protocol = ProtocolConfig::default();
        assert!(protocol.dialect_enabled(Protocol::Memcached));
        assert!(protocol.dialect_enabled(Protocol::Resp));

        protocol.memcached_enabled = false;
        protocol.resp_enabled = false;
        assert!(!protocol.dialect_enabled(Protocol::Memcached));
        assert!(!protocol.dialect_enabled(Protocol::Resp));
        assert!(
            protocol.dialect_enabled(Protocol::Vcp),
            "a server with no reachable dialect would be unconfigurable"
        );
    }

    #[test]
    fn rejects_unknown_keys_rather_than_ignoring_them() {
        let err = toml::from_str::<Config>("[store]\nmap_sise_mb = 10\n").unwrap_err();
        assert!(err.to_string().contains("map_sise_mb"), "{err}");
    }

    #[test]
    fn rejects_a_reader_table_too_small_for_the_blocking_pool() {
        // Getting this wrong surfaces only under load, as MDB_READERS_FULL, so
        // it has to be caught at startup.
        let mut config = Config::default();
        config.server.max_blocking_threads = config.store.max_readers as usize;

        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("MDB_READERS_FULL"), "{err}");
    }

    #[test]
    fn rejects_an_oversized_value_limit() {
        let mut config = Config::default();
        config.store.max_value_bytes = vash_core::ABSOLUTE_MAX_VALUE_LEN + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn the_per_record_tag_limit_stays_within_what_the_header_can_describe() {
        let mut config = Config::default();
        assert_eq!(
            config.store.tags.max_per_record, 32,
            "the documented default"
        );

        config.store.tags.max_per_record = vash_core::ABSOLUTE_MAX_TAGS;
        assert!(config.validate().is_ok(), "the ceiling itself is allowed");

        // `tag_count` is a `u8`, so anything past this would be truncated on
        // its way to disk and read back as a different record.
        config.store.tags.max_per_record = vash_core::ABSOLUTE_MAX_TAGS + 1;
        assert!(config.validate().is_err());

        config.store.tags.max_per_record = 0;
        assert!(config.validate().is_err(), "a store that refuses every tag");
    }
}
