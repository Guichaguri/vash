use std::sync::Arc;

use vash_core::ServerInfo;
use vash_store::LmdbStore;

use crate::cluster::Cluster;

/// Everything a connection needs, shared by `Arc` across all of them.
pub struct ServerState {
    pub store: Arc<LmdbStore>,
    pub info: ServerInfo,
    /// `FLUSH` is a remote cache-wipe primitive, so it is off unless
    /// deliberately enabled.
    pub flush_enabled: bool,
    pub metrics: crate::metrics::ServerMetrics,
    /// Present even with no peers configured, so `CLUSTER` and `/stats` have
    /// something truthful to report and dispatch has no special case.
    pub cluster: Arc<Cluster>,
    /// Read-only requests skip the hop to the storage tier. See
    /// [`crate::config::StoreConfig::inline_reads`].
    pub inline_reads: bool,
}

impl ServerState {
    pub fn new(
        store: Arc<LmdbStore>,
        info: ServerInfo,
        flush_enabled: bool,
        cluster: Arc<Cluster>,
        inline_reads: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            info,
            flush_enabled,
            metrics: crate::metrics::ServerMetrics::default(),
            cluster,
            inline_reads,
        })
    }
}
