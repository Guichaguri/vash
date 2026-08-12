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
}

impl ServerState {
    pub fn new(
        store: Arc<dyn Store>,
        info: ServerInfo,
        protocol: crate::config::ProtocolConfig,
        auth: AuthState,
        cluster: Arc<Cluster>,
        inline_reads: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            info,
            protocol,
            auth,
            metrics: crate::metrics::ServerMetrics::default(),
            cluster,
            inline_reads,
        })
    }
}
