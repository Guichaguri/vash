use std::sync::Arc;

use cache_core::ServerInfo;
use cache_store::LmdbStore;

/// Everything a connection needs, shared by `Arc` across all of them.
pub struct ServerState {
    pub store: Arc<LmdbStore>,
    pub info: ServerInfo,
}

impl ServerState {
    pub fn new(store: Arc<LmdbStore>, info: ServerInfo) -> Arc<Self> {
        Arc::new(Self { store, info })
    }
}
