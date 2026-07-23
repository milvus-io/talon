//! Kernel FUSE mount adapter (`mount` feature).
//!
//! This module is compiled only with `--features mount`; it pulls in `fuser`
//! and defines [`TalonFuse`], the type that implements [`fuser::Filesystem`] by
//! delegating to the runtime-independent read-path logic ([`ReadOnlyFs`],
//! [`BlockReader`]). Keeping it behind a feature means `cargo build`/`test
//! --workspace` — including CI without `/dev/fuse` or libfuse — never needs the
//! kernel bindings, while the mount binary opts in explicitly.
//!
//! This step ([#100]) is the **scaffold**: it wires the struct, its
//! construction, and a `fuser::Filesystem` impl with the six read ops present
//! but not yet dispatching (metadata callbacks land in #101, data in #102).
//! Every method compiles and is ready to be filled in without touching the
//! feature plumbing again.
//!
//! [#100]: https://github.com/milvus-io/talon/issues/100

use std::sync::Arc;

use crate::block_reader::BlockReader;
use crate::ops::ReadOnlyFs;

/// A mountable Talon filesystem: the `fuser` adapter over the read path.
///
/// Holds the namespace tree ([`ReadOnlyFs`]) that answers metadata ops and the
/// [`BlockReader`] that serves data ops. A Tokio [`Handle`](tokio::runtime::Handle)
/// is retained so the synchronous `fuser` callbacks can drive the async read
/// path (via the bridge) once the data callbacks are implemented.
pub struct TalonFuse {
    /// Synthesized namespace tree for lookup/getattr/readdir.
    fs: Arc<ReadOnlyFs>,
    /// Read-path orchestrator for open/read.
    reader: BlockReader,
    /// Handle to the async runtime the callbacks dispatch onto.
    runtime: tokio::runtime::Handle,
}

impl TalonFuse {
    /// Build the adapter over a populated namespace and a read-path reader.
    ///
    /// `runtime` is the handle the synchronous FUSE callbacks use to run async
    /// work; typically `tokio::runtime::Handle::current()` on the mounting
    /// thread.
    pub fn new(fs: Arc<ReadOnlyFs>, reader: BlockReader, runtime: tokio::runtime::Handle) -> Self {
        Self {
            fs,
            reader,
            runtime,
        }
    }

    /// The namespace tree backing metadata ops.
    pub fn namespace(&self) -> &Arc<ReadOnlyFs> {
        &self.fs
    }

    /// The read-path orchestrator backing data ops.
    pub fn reader(&self) -> &BlockReader {
        &self.reader
    }

    /// The runtime handle the callbacks dispatch async work onto.
    pub fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }
}

impl fuser::Filesystem for TalonFuse {
    // Metadata ops (lookup, getattr, readdir) are implemented in #101; data ops
    // (open, read, release) in #102. The trait's default methods return ENOSYS,
    // so an unimplemented op fails cleanly rather than misbehaving; the scaffold
    // deliberately does not override them yet.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator_client::CoordinatorClient;
    use crate::placement_cache::PlacementCache;

    #[tokio::test]
    async fn constructs_over_read_path_components() {
        let fs = Arc::new(ReadOnlyFs::new());
        fs.insert_object("s3/b/o.bin", 123);
        let reader = BlockReader::new(
            CoordinatorClient::new("127.0.0.1:7000"),
            Arc::new(PlacementCache::new(1000)),
            1,
        );
        let mounted = TalonFuse::new(Arc::clone(&fs), reader, tokio::runtime::Handle::current());
        // The adapter exposes its components for the callbacks to use.
        assert_eq!(mounted.namespace().getattr(1).unwrap().ino, 1);
        assert_eq!(mounted.reader().coordinator_addr(), "127.0.0.1:7000");
    }
}
