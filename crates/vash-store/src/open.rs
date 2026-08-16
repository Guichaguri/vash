//! Choosing an engine at startup.
//!
//! The one place in the tree that names more than one backend. Everything above
//! it takes an `Arc<dyn Store>` and never learns which engine answered.

use std::sync::Arc;

use crate::config::{BackendKind, StoreConfig};
#[cfg_attr(feature = "mdbx", allow(unused_imports))]
use crate::error::{Result, StoreError};
use crate::{LmdbStore, Store};

/// An opened store, and the means to release it.
///
/// The release is a closure rather than a method on [`Store`] because it is an
/// engine lifecycle rather than a storage contract: LMDB only *schedules* a
/// close when its handle drops and refuses to reopen a path still registered in
/// the process, so anything restarting in-process has to block on it, while
/// mdbx closes synchronously and needs nothing. Putting it on the trait would
/// make every implementation — including the in-memory fake — carry a method
/// with nothing to do. See [`m10.md`] phase 3, which decided this before there
/// was a second engine to test it against.
///
/// [`m10.md`]: https://github.com/guichaguri/vash/blob/main/docs/m10.md
pub struct StoreHandle {
    store: Arc<dyn Store>,
    /// Captures the concrete handle, so releasing it needs no downcast.
    close: Box<dyn FnOnce() + Send>,
}

impl StoreHandle {
    pub fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    /// Releases the environment, blocking if the engine needs it.
    ///
    /// **Every other reference has to be gone first.** The trait object handed
    /// out by [`Self::store`] shares a reference count with the handle captured
    /// here, so a caller drops its own state before calling this — which is
    /// exactly what the server's shutdown path does.
    pub fn close(self) {
        drop(self.store);
        (self.close)();
    }
}

/// Opens the store the configuration asks for.
pub fn open(config: &StoreConfig) -> Result<StoreHandle> {
    match config.backend {
        BackendKind::Lmdb => handle(LmdbStore::open(config)?),
        #[cfg(feature = "mdbx")]
        BackendKind::Mdbx => handle(crate::VashStore::<crate::MdbxBackend>::open(config)?),
        // Refused rather than quietly served by the other engine. An operator
        // who asked for one engine and got another would read the resulting
        // benchmark as if it were the engine they configured — and the same
        // silent-substitution argument the shard-count check makes applies
        // here, with the added twist that the two write different files.
        #[cfg(not(feature = "mdbx"))]
        BackendKind::Mdbx => Err(StoreError::Corrupt(
            "store.backend is \"mdbx\", but this binary was built without the `mdbx` \
             feature; rebuild with --features vash-store/mdbx or choose \"lmdb\""
                .into(),
        )),
    }
}

/// Wraps a concrete store as a trait object plus its closer.
fn handle<S: Store + Closes>(store: S) -> Result<StoreHandle> {
    let concrete = Arc::new(store);
    let store = Arc::clone(&concrete) as Arc<dyn Store>;
    Ok(StoreHandle {
        store,
        close: Box::new(move || match Arc::try_unwrap(concrete) {
            Ok(store) => store.close(),
            Err(_) => {
                tracing::warn!("store still referenced at shutdown; environment left open")
            }
        }),
    })
}

/// The release half of a concrete store, which [`Store`] deliberately omits.
pub trait Closes: Send + Sync + 'static {
    fn close(self);
}

impl<B: crate::Backend> Closes for crate::VashStore<B> {
    fn close(self) {
        crate::VashStore::close(self);
    }
}
