use std::sync::Arc;

use vash_core::ServerInfo;
use vash_store::LmdbStore;

use crate::auth::AuthState;
use crate::cluster::Cluster;

/// Everything a connection needs, shared by `Arc` across all of them.
pub struct ServerState {
    pub store: Arc<LmdbStore>,
    pub info: ServerInfo,
    /// `FLUSH` is a remote cache-wipe primitive, so it is off unless
    /// deliberately enabled.
    pub flush_enabled: bool,
    /// `LIST_KEYS`/`LIST_TAGS` enumerate the cache, so they are off unless
    /// deliberately enabled. See [`crate::config::ProtocolConfig`].
    pub listing_enabled: bool,
    /// Records one `LIST_KEYS` call may examine before stopping early.
    pub listing_max_scan: usize,
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
}

impl ServerState {
    pub fn new(
        store: Arc<LmdbStore>,
        info: ServerInfo,
        protocol: crate::config::ProtocolConfig,
        auth: AuthState,
        cluster: Arc<Cluster>,
        inline_reads: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            info,
            flush_enabled: protocol.flush_enabled,
            listing_enabled: protocol.listing_enabled,
            listing_max_scan: protocol.listing_max_scan,
            auth,
            metrics: crate::metrics::ServerMetrics::default(),
            cluster,
            inline_reads,
        })
    }
}
