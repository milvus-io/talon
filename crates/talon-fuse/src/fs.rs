//! Filesystem adapter mapping FUSE operations onto the Talon object store.

use std::sync::Arc;
use talon_core::ObjectStore;

/// A FUSE filesystem backed by a Talon [`ObjectStore`].
///
/// This is a skeleton: wire up a concrete FUSE implementation (e.g. the
/// `fuser` crate) to translate inode/path operations into object store calls.
pub struct TalonFs {
    store: Arc<dyn ObjectStore>,
}

impl TalonFs {
    /// Create a new filesystem over the given object store.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Access the underlying object store.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    // TODO: implement fuser::Filesystem (lookup, read, write, getattr, ...).
}
