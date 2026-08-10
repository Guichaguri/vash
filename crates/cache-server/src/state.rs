use std::sync::Arc;

use cache_core::ServerInfo;
use cache_store::LmdbStore;

/// Everything a connection needs, shared by `Arc` across all of them.
pub struct ServerState {
    pub store: Arc<LmdbStore>,
    pub info: ServerInfo,
    /// `FLUSH` is a remote cache-wipe primitive, so it is off unless
    /// deliberately enabled.
    pub flush_enabled: bool,
    pub metrics: crate::metrics::ServerMetrics,
}

impl ServerState {
    pub fn new(store: Arc<LmdbStore>, info: ServerInfo, flush_enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            store,
            info,
            flush_enabled,
            metrics: crate::metrics::ServerMetrics::default(),
        })
    }
}
