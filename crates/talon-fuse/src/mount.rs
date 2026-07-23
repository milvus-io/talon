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
use std::time::Duration;

use crate::block_reader::BlockReader;
use crate::ops::{Attr, FileKind, FsError, ReadOnlyFs};

/// How long the kernel may cache a metadata reply before re-asking.
///
/// The namespace is populated from coordinator listings and is effectively
/// immutable for a mount session (read-only v1), so a modest TTL avoids a
/// callback per stat without risking staleness.
const ATTR_TTL: Duration = Duration::from_secs(1);

/// Map a read-op [`FsError`] to a POSIX errno for a `fuser` reply.
pub(crate) fn errno(err: FsError) -> i32 {
    match err {
        FsError::NotFound => libc::ENOENT,
        FsError::ReadOnly => libc::EROFS,
        FsError::Unsupported => libc::ENOSYS,
        FsError::BadHandle => libc::EBADF,
    }
}

/// Convert a synthesized [`Attr`] into a `fuser::FileAttr`.
///
/// Times are fixed to the UNIX epoch (the namespace is synthetic and read-only,
/// so there is no meaningful mtime); links are 1, ownership is left to the
/// mounting user via `uid`/`gid`. `blocks` is a 512-byte-unit count as POSIX
/// expects.
pub(crate) fn to_file_attr(attr: Attr, uid: u32, gid: u32) -> fuser::FileAttr {
    let kind = match attr.kind {
        FileKind::Directory => fuser::FileType::Directory,
        FileKind::File => fuser::FileType::RegularFile,
    };
    let epoch = std::time::UNIX_EPOCH;
    fuser::FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        atime: epoch,
        mtime: epoch,
        ctime: epoch,
        crtime: epoch,
        kind,
        perm: attr.perm,
        nlink: 1,
        uid,
        gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

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
    /// Resolve a child `name` under directory `parent` to its attributes.
    fn lookup(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let name = match name.to_str() {
            Some(s) => s,
            None => return reply.error(libc::EINVAL),
        };
        match self.fs.lookup(parent, name) {
            Ok(attr) => {
                let fa = to_file_attr(attr, req.uid(), req.gid());
                reply.entry(&ATTR_TTL, &fa, 0);
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    /// Report attributes for an inode.
    fn getattr(&mut self, req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match self.fs.getattr(ino) {
            Ok(attr) => {
                let fa = to_file_attr(attr, req.uid(), req.gid());
                reply.attr(&ATTR_TTL, &fa);
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    /// List the children of a directory inode.
    ///
    /// The kernel drives pagination via `offset`: entries with an index `<=`
    /// offset were already returned, so we start after it. `add` returns true
    /// when the reply buffer is full; we stop there and let the kernel re-call
    /// with a higher offset. The synthetic `.` and `..` entries are emitted
    /// first (self and parent both map back to `ino` for the read-only tree).
    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let children = match self.fs.readdir(ino) {
            Ok(c) => c,
            Err(e) => return reply.error(errno(e)),
        };
        // Prepend `.` and `..` so tools like `ls -a` behave; both point at `ino`
        // (parent tracking is unnecessary for a read-only synthetic namespace).
        let mut all: Vec<(u64, fuser::FileType, String)> = vec![
            (ino, fuser::FileType::Directory, ".".to_string()),
            (ino, fuser::FileType::Directory, "..".to_string()),
        ];
        all.extend(children.into_iter().map(|e| {
            let kind = match e.kind {
                FileKind::Directory => fuser::FileType::Directory,
                FileKind::File => fuser::FileType::RegularFile,
            };
            (e.ino, kind, e.name)
        }));

        for (i, (child_ino, kind, name)) in all.into_iter().enumerate().skip(offset as usize) {
            // The offset stored per entry is "next index to fetch" = i + 1.
            if reply.add(child_ino, (i + 1) as i64, kind, name) {
                break; // buffer full; kernel will re-call from here.
            }
        }
        reply.ok();
    }
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

    #[test]
    fn errno_maps_each_fs_error() {
        assert_eq!(errno(FsError::NotFound), libc::ENOENT);
        assert_eq!(errno(FsError::ReadOnly), libc::EROFS);
        assert_eq!(errno(FsError::Unsupported), libc::ENOSYS);
        assert_eq!(errno(FsError::BadHandle), libc::EBADF);
    }

    #[test]
    fn to_file_attr_maps_kind_size_and_perm() {
        let dir = Attr {
            ino: 1,
            kind: FileKind::Directory,
            size: 0,
            perm: 0o555,
        };
        let fa = to_file_attr(dir, 1000, 1000);
        assert_eq!(fa.kind, fuser::FileType::Directory);
        assert_eq!(fa.perm, 0o555);
        assert_eq!(fa.uid, 1000);
        assert_eq!(fa.nlink, 1);

        let file = Attr {
            ino: 7,
            kind: FileKind::File,
            size: 1000, // 1000 bytes → ceil(1000/512) = 2 blocks
            perm: 0o444,
        };
        let fa = to_file_attr(file, 0, 0);
        assert_eq!(fa.kind, fuser::FileType::RegularFile);
        assert_eq!(fa.size, 1000);
        assert_eq!(fa.blocks, 2);
        assert_eq!(fa.blksize, 512);
    }
}
