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
    /// Independent LMDB environments, and therefore the ceiling on concurrent
    /// writers. `0` picks `min(num_cpus, 8)`.
    ///
    /// Fixed once a database exists: changing it would route every key to a
    /// different environment, so startup refuses to open a mismatched store
    /// rather than silently losing the whole cache.
    pub shards: usize,
    /// Map size **per shard**, not in total.
    pub map_size_mb: usize,
    pub max_readers: u32,
    pub durability: Durability,
    pub max_value_bytes: usize,
    pub wipe_on_start: bool,
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
        let defaults = cache_store::EvictionConfig::default();
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
    /// Tag-index entries examined per reclamation pass. While a job is
    /// outstanding these run back to back, so this bounds transaction length
    /// rather than drain rate.
    pub reclaim_batch: usize,
}

impl Default for TagConfig {
    fn default() -> Self {
        let defaults = cache_store::StoreConfig::default();
        Self {
            max_tags: defaults.max_tags,
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
        let defaults = cache_store::WriteConfig::default();
        Self {
            max_batch: defaults.max_batch,
            queue_depth: defaults.queue_depth,
            linger_us: defaults.linger_us,
        }
    }
}

impl Default for TtlConfig {
    fn default() -> Self {
        let defaults = cache_store::WriteConfig::default();
        Self {
            bucket_granularity_ms: 1000,
            sweep_interval_ms: defaults.sweep_interval_ms,
            sweep_batch: defaults.sweep_batch,
        }
    }
}

/// Mirrors `cache_store::Durability`, kept separate so the store crate does not
/// take a serde dependency for the sake of the config file.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    Durable,
    #[default]
    Relaxed,
    Ephemeral,
}

impl From<Durability> for cache_store::Durability {
    fn from(d: Durability) -> Self {
        match d {
            Durability::Durable => Self::Durable,
            Durability::Relaxed => Self::Relaxed,
            Durability::Ephemeral => Self::Ephemeral,
        }
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
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            delete_by_tag: FanoutMode::default(),
            gossip_interval_ms: 5_000,
            fanout_timeout_ms: 2_000,
            queue_depth: 1_024,
        }
    }
}

/// Mirrors [`cache_core::ClusterMode`], kept separate so the domain crate does
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

impl From<FanoutMode> for cache_core::ClusterMode {
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
    /// Address for `/metrics`, `/health` and `/stats`. Empty disables them.
    ///
    /// A separate port from cache traffic, so it can be bound to a private
    /// interface and a flood of scrapes cannot crowd out real requests.
    pub admin_listen: String,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ProtocolConfig {
    /// `FLUSH` empties the whole cache on request from any client that can
    /// reach the port, so it is off unless deliberately enabled.
    pub flush_enabled: bool,
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
            map_size_mb: 4096,
            // Comfortably above the default blocking pool, which is the ceiling
            // on concurrent readers; validation enforces the relationship.
            max_readers: 256,
            durability: Durability::default(),
            max_value_bytes: cache_core::DEFAULT_MAX_VALUE_LEN,
            wipe_on_start: false,
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
            admin_listen: "127.0.0.1:9090".into(),
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
            map_size >= cache_store::config::MIN_MAP_SIZE,
            "store.map_size_mb is {} MiB, but the minimum is {} MiB: below that, LMDB can \
             report a full map permanently even after everything has been deleted",
            self.store.map_size_mb,
            cache_store::config::MIN_MAP_SIZE / (1024 * 1024)
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
            self.store.max_value_bytes <= cache_core::ABSOLUTE_MAX_VALUE_LEN,
            "store.max_value_bytes exceeds the absolute limit of {} bytes",
            cache_core::ABSOLUTE_MAX_VALUE_LEN
        );
        anyhow::ensure!(
            self.server.read_buffer >= cache_proto::kcp::HEADER_LEN,
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
            self.store.tags.reclaim_batch > 0,
            "store.tags.reclaim_batch must be > 0"
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
    /// Capped at 4 rather than 8. Sharding buys concurrent *writers* and
    /// nothing else — LMDB reads are already lock-free and concurrent within
    /// one environment — so it only pays when the writer thread is the
    /// bottleneck. When commits are disk-bound, which the default `relaxed`
    /// durability makes likely, more environments fragment I/O across the same
    /// device and measured throughput *falls*. Four is a compromise that helps
    /// where sharding helps without punishing the disk-bound case; see the
    /// benchmark in the README before raising it.
    pub fn shard_count(&self) -> usize {
        match self.store.shards {
            0 => std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .min(4),
            n => n,
        }
    }

    pub fn store_config(&self) -> cache_store::StoreConfig {
        cache_store::StoreConfig {
            path: self.store.path.clone(),
            shards: self.shard_count(),
            map_size: self.store.map_size_mb * 1024 * 1024,
            max_readers: self.store.max_readers,
            durability: self.store.durability.into(),
            max_value_len: self.store.max_value_bytes,
            wipe_on_start: self.store.wipe_on_start,
            bucket_granularity_ms: self.store.ttl.bucket_granularity_ms,
            max_tags: self.store.tags.max_tags,
            write: cache_store::WriteConfig {
                max_batch: self.store.write.max_batch,
                queue_depth: self.store.write.queue_depth,
                linger_us: self.store.write.linger_us,
                sweep_interval_ms: self.store.ttl.sweep_interval_ms,
                sweep_batch: self.store.ttl.sweep_batch,
                reclaim_batch: self.store.tags.reclaim_batch,
                eviction: cache_store::EvictionConfig {
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

    #[test]
    fn parses_a_partial_file_and_fills_the_rest_from_defaults() {
        let config: Config = toml::from_str(
            r#"
            [server]
            listen = "0.0.0.0:1234"

            [store]
            durability = "ephemeral"
            "#,
        )
        .unwrap();

        assert_eq!(config.server.listen.port(), 1234);
        assert_eq!(config.store.durability, Durability::Ephemeral);
        // Untouched keys keep their defaults.
        assert_eq!(config.store.map_size_mb, 4096);
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
        config.store.max_value_bytes = cache_core::ABSOLUTE_MAX_VALUE_LEN + 1;
        assert!(config.validate().is_err());
    }
}
