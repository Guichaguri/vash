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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StoreConfig {
    pub path: PathBuf,
    pub map_size_mb: usize,
    pub max_readers: u32,
    pub durability: Durability,
    pub max_value_bytes: usize,
    pub wipe_on_start: bool,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObservabilityConfig {
    /// `json` or `pretty`.
    pub log_format: String,
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:11311".parse().expect("valid default address"),
            max_connections: 10_000,
            read_buffer: 16 * 1024,
        }
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
            map_size_mb: 4096,
            max_readers: 128,
            durability: Durability::default(),
            max_value_bytes: cache_core::DEFAULT_MAX_VALUE_LEN,
            wipe_on_start: false,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: "pretty".into(),
            log_level: "info".into(),
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
        anyhow::ensure!(self.store.map_size_mb > 0, "store.map_size_mb must be > 0");
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
        Ok(())
    }

    pub fn store_config(&self) -> cache_store::StoreConfig {
        cache_store::StoreConfig {
            path: self.store.path.clone(),
            map_size: self.store.map_size_mb * 1024 * 1024,
            max_readers: self.store.max_readers,
            durability: self.store.durability.into(),
            max_value_len: self.store.max_value_bytes,
            wipe_on_start: self.store.wipe_on_start,
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
    fn rejects_an_oversized_value_limit() {
        let mut config = Config::default();
        config.store.max_value_bytes = cache_core::ABSOLUTE_MAX_VALUE_LEN + 1;
        assert!(config.validate().is_err());
    }
}
