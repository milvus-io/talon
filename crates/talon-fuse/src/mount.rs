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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::block_reader::{BlockReader, FileView};
use crate::lock::MutexExt;
use crate::mapping::path_to_object;
use crate::ops::{Attr, FileKind, FsError, ReadOnlyFs};
use crate::prefetch::Prefetcher;
use crate::readahead::ReadaheadConfig;
use talon_core::ObjectId;

/// How long the kernel may cache a metadata reply before re-asking.
///
/// The namespace is populated from coordinator listings and is effectively
/// immutable for a mount session (read-only v1), so a generous TTL avoids a
/// callback per stat/lookup without risking staleness.
const ATTR_TTL: Duration = Duration::from_secs(60);

/// Target maximum size of a single kernel read/write, in bytes.
///
/// The kernel default caps a FUSE read at 128 KiB; raising it to 1 MiB (via
/// `FUSE_CAP_MAX_PAGES`, kernel ≥4.20) lets one `read` callback carry 8× the
/// data, amortizing the per-request `/dev/fuse` round-trip on sequential reads
/// (issue #180). The kernel/`fuser` clamps to what it supports.
const TARGET_MAX_IO: u32 = 1 << 20;

/// Max concurrent speculative prefetch fetches per open file handle.
///
/// Prefetch is fire-and-forget and bounded (see [`Prefetcher`]); this caps how
/// many upcoming blocks can be warming at once so readahead never floods a
/// worker or the client's task pool. Excess prefetches are dropped, not queued.
const PREFETCH_MAX_INFLIGHT: usize = 4;

/// Map a read-op [`FsError`] to a POSIX errno for a `fuser` reply.
pub(crate) fn errno(err: FsError) -> i32 {
    match err {
        FsError::NotFound => libc::ENOENT,
        FsError::ReadOnly => libc::EROFS,
        FsError::Unsupported => libc::ENOSYS,
        FsError::BadHandle => libc::EBADF,
        FsError::TooLarge => libc::EFBIG,
    }
}

/// A monotonic-ish millisecond timestamp for the placement cache TTL.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

/// The canonical placeholder version the mount path uses to address blocks.
///
/// **Placement is version-independent on the mount path in v1, by design.** The
/// coordinator locates a block by hashing the [`BlockId`](talon_core::BlockId) the client sends
/// (rendezvous/HRW), and the worker is the sole authority on *freshness*: it
/// resolves each object's real ETag itself and refuses to serve stale bytes
/// (issues #119, #163). The client never transmits a version to the worker —
/// [`RangeRequest`](talon_transport::RangeRequest) carries only `object` +
/// `[offset, len)` — so the version in a client-side [`BlockId`](talon_core::BlockId) affects only the
/// client's own placement-cache key, never the bytes returned.
///
/// Rather than fabricate a per-object `"v1"` that looks meaningful but isn't,
/// every mount-path block is addressed under this single canonical token. This
/// keeps client and coordinator trivially consistent (both hash the same value)
/// and makes the "no per-object version negotiation here" contract explicit. A
/// future revision that wants version-aware placement would resolve the real
/// version on both sides and revisit the mount's page-cache retention
/// (`FOPEN_KEEP_CACHE`), which currently relies on session immutability.
pub const CANONICAL_MOUNT_VERSION: &str = "talon-mount-v1";

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
    /// Logical block size used to split reads into per-block fetches.
    block_size: u32,
    /// Canonical placeholder version used to address blocks.
    ///
    /// The mount path is version-independent by design (see
    /// [`CANONICAL_MOUNT_VERSION`]): this value only keys the client's placement
    /// cache and feeds the HRW hash, never the bytes the worker returns. It is
    /// normally [`CANONICAL_MOUNT_VERSION`].
    version: talon_core::Version,
    /// Readahead tuning (sequential-run trigger + prefetch window). The window
    /// comes from `FuseConfig::readahead_blocks`.
    readahead: ReadaheadConfig,
    /// Per-open-handle prefetch drivers, keyed by FUSE file handle. Created
    /// lazily on the first `read` of a handle (when the object/size are known)
    /// and removed on `release`. Each detects a sequential run and warms the
    /// next blocks on the owning worker ahead of the cursor (issue #206).
    prefetchers: Mutex<HashMap<u64, Prefetcher>>,
    /// When `true`, the mount is read-write: the write callbacks
    /// (create/write/setattr/unlink/flush) are active. When `false` (the safe
    /// default), writes are rejected with `EROFS`. Set via
    /// [`with_read_write`](Self::with_read_write) (#226/#232).
    read_write: bool,
    /// Shared pool for write/delete connections to workers (mirrors the read
    /// pool). Reused across write handles.
    write_pool: Arc<crate::pool::ConnectionPool>,
}

impl TalonFuse {
    /// Build the adapter over a populated namespace and a read-path reader.
    ///
    /// `runtime` is the handle the synchronous FUSE callbacks use to run async
    /// work; typically `tokio::runtime::Handle::current()` on the mounting
    /// thread. `block_size` and `version` address the objects' blocks (see the
    /// field docs on `version`). Uses the default [`ReadaheadConfig`]; call
    /// [`with_readahead`](Self::with_readahead) to set the prefetch window from
    /// config.
    pub fn new(
        fs: Arc<ReadOnlyFs>,
        reader: BlockReader,
        runtime: tokio::runtime::Handle,
        block_size: u32,
        version: talon_core::Version,
    ) -> Self {
        Self {
            fs,
            reader,
            runtime,
            block_size,
            version,
            readahead: ReadaheadConfig::default(),
            prefetchers: Mutex::new(HashMap::new()),
            read_write: false,
            write_pool: Arc::new(crate::pool::ConnectionPool::new()),
        }
    }

    /// Enable the write path (create/write/setattr/unlink/flush). When disabled
    /// (the default), those callbacks reject with `EROFS` (#232).
    pub fn with_read_write(mut self, enabled: bool) -> Self {
        self.read_write = enabled;
        self
    }

    /// Set the readahead prefetch window (number of blocks to prefetch ahead of
    /// the cursor once a sequential run is detected), typically from
    /// `FuseConfig::readahead_blocks`. A window of `0` disables prefetch.
    pub fn with_readahead(mut self, window_blocks: u32) -> Self {
        self.readahead.window = window_blocks;
        self
    }

    /// Feed a read at `offset` on handle `fh` to that handle's prefetcher,
    /// lazily creating it, and return the block indices a prefetch was spawned
    /// for (empty until a sequential run is detected, or if readahead is off).
    ///
    /// Split out of the `read` callback so the wiring is unit-testable without a
    /// kernel mount: the prefetcher only fires after `trigger_run` consecutive
    /// in-order reads and never for random access (issue #206).
    fn drive_readahead(
        &self,
        fh: u64,
        offset: u64,
        file_size: u64,
        object: Option<ObjectId>,
        now_ms: u64,
    ) -> Vec<u64> {
        if self.readahead.window == 0 {
            return Vec::new();
        }
        let object = match object {
            Some(o) => o,
            None => return Vec::new(),
        };
        let block_index = offset / self.block_size as u64;
        let mut prefetchers = self.prefetchers.lock_recover();
        let prefetcher = prefetchers.entry(fh).or_insert_with(|| {
            Prefetcher::new(
                self.reader.clone(),
                self.readahead,
                PREFETCH_MAX_INFLIGHT,
                object,
                self.block_size,
                self.version.clone(),
                file_size,
            )
        });
        // The prefetcher spawns fetches on the runtime; enter it so the spawns
        // have a reactor even though we may be on a sync FUSE thread.
        let _guard = self.runtime.enter();
        prefetcher.on_read(block_index, now_ms)
    }

    /// Write `bytes` back to the object at mount-relative `path` through its
    /// owning worker (resolve owner → `WriteClient::put_object`). Returns `Ok` on
    /// a committed write, `Err` for any resolution/transport/backend failure.
    ///
    /// Runs the async write path on the runtime; called from the synchronous
    /// FUSE `flush`/`fsync` callback so a write error surfaces to the app's
    /// `close(2)`/`fsync(2)` (#232).
    fn writeback_object(&self, path: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        let object = path_to_object(path)?;
        let reader = self.reader.clone();
        let pool = Arc::clone(&self.write_pool);
        let block_size = self.block_size;
        let version = self.version.clone();
        let now_ms = now_ms();
        self.runtime.block_on(async move {
            let addr = reader
                .resolve_owner(&object, block_size, &version, now_ms)
                .await?;
            let client = crate::worker_client::WriteClient::with_pool(addr, pool);
            client.put_object(&object, &bytes).await?;
            Ok::<(), anyhow::Error>(())
        })
    }

    /// Delete the object at mount-relative `path` through its owning worker.
    fn delete_backend_object(&self, path: &str) -> anyhow::Result<()> {
        let object = path_to_object(path)?;
        let reader = self.reader.clone();
        let pool = Arc::clone(&self.write_pool);
        let block_size = self.block_size;
        let version = self.version.clone();
        let now_ms = now_ms();
        self.runtime.block_on(async move {
            let addr = reader
                .resolve_owner(&object, block_size, &version, now_ms)
                .await?;
            let client = crate::worker_client::WriteClient::with_pool(addr, pool);
            client.delete_object(&object).await?;
            Ok::<(), anyhow::Error>(())
        })
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
    /// Negotiate kernel FUSE parameters at mount time.
    ///
    /// Raises the maximum read size to 1 MiB (`TARGET_MAX_IO`) so a single
    /// `read` callback carries up to 8× the 128 KiB default, and matches
    /// readahead to it — both pure throughput wins for a read-only mount serving
    /// large objects (issue #180). `fuser`/the kernel clamp each request to what
    /// they support; the granted values are logged.
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        match config.set_max_write(TARGET_MAX_IO) {
            Ok(granted) => tracing::info!(max_write = granted, "negotiated FUSE max_write"),
            Err(cap) => {
                let granted = config.set_max_write(cap).unwrap_or(cap);
                tracing::info!(max_write = granted, "FUSE max_write clamped by kernel");
            }
        }
        match config.set_max_readahead(TARGET_MAX_IO) {
            Ok(granted) => tracing::info!(max_readahead = granted, "negotiated FUSE max_readahead"),
            Err(cap) => {
                let granted = config.set_max_readahead(cap).unwrap_or(cap);
                tracing::info!(
                    max_readahead = granted,
                    "FUSE max_readahead clamped by kernel"
                );
            }
        }
        Ok(())
    }

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

    /// Open a file inode, returning a handle for subsequent reads.
    ///
    /// Directories are rejected with `EISDIR`-equivalent `ENOSYS` semantics via
    /// the op layer (`FsError::Unsupported`). The handle indexes into the
    /// namespace so the `read` callback can recover the object.
    ///
    /// Replies with `FOPEN_KEEP_CACHE` so the kernel retains this file's page
    /// cache across opens: the namespace is read-only and immutable for a mount
    /// session, so repeated reads and `mmap` can serve from RAM without a FUSE
    /// round-trip (issue #180).
    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        // A write/read-write open (O_WRONLY / O_RDWR) starts a dirty buffer for
        // whole-object rewrite (#232); a read-only open uses the read handle.
        let accmode = flags & libc::O_ACCMODE;
        let wants_write = accmode == libc::O_WRONLY || accmode == libc::O_RDWR;
        if wants_write {
            if !self.read_write {
                return reply.error(libc::EROFS);
            }
            match self.fs.open_write(ino) {
                // No FOPEN_KEEP_CACHE for a write handle: contents are changing.
                Ok(fh) => reply.opened(fh, 0),
                Err(e) => reply.error(errno(e)),
            }
        } else {
            match self.fs.open(ino) {
                Ok(fh) => reply.opened(fh, fuser::consts::FOPEN_KEEP_CACHE),
                Err(e) => reply.error(errno(e)),
            }
        }
    }

    /// Serve `size` bytes at `offset` for an open handle.
    ///
    /// Recovers the object from the handle, then dispatches the (possibly
    /// multi-block) fetch onto the async runtime via
    /// [`BlockReader::read`], which splits across block boundaries, serves each
    /// block through the placement cache, and stitches the result. The
    /// synchronous FUSE thread blocks on the runtime — never the reverse — so
    /// the kernel side stays responsive.
    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let (path, file_size) = match self.fs.file_meta(fh) {
            Ok(m) => m,
            Err(e) => return reply.error(errno(e)),
        };
        let object = match path_to_object(&path) {
            Ok(o) => o,
            Err(_) => return reply.error(libc::EINVAL),
        };
        let reader = self.reader.clone();
        let block_size = self.block_size;
        let version = self.version.clone();
        let offset = offset.max(0) as u64;
        // Clone the object id for the prefetcher before `object` is moved into
        // the foreground read future below (only needed when readahead is on).
        let object_for_prefetch = if self.readahead.window > 0 {
            Some(object.clone())
        } else {
            None
        };
        // A monotonic-ish millisecond stamp for the placement cache TTL.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let result = self.runtime.block_on(async move {
            let view = FileView {
                object: &object,
                block_size,
                version: &version,
                size: file_size,
            };
            reader.read(&view, offset, size as u64, now_ms).await
        });

        // Drive client-side readahead: feed this read's starting block index to
        // the per-handle prefetcher, which warms the next blocks on the owning
        // worker only once a sequential run is detected (issue #206). Prefetch is
        // fire-and-forget and bounded, so this never delays the reply.
        self.drive_readahead(fh, offset, file_size, object_for_prefetch, now_ms);

        match result {
            Ok(bytes) => reply.data(&bytes),
            Err(_) => reply.error(libc::EIO),
        }
    }

    /// Create and open a new file for writing (`O_CREAT`).
    fn create(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        if !self.read_write {
            return reply.error(libc::EROFS);
        }
        let name = match name.to_str() {
            Some(s) => s,
            None => return reply.error(libc::EINVAL),
        };
        match self.fs.create(parent, name) {
            Ok((attr, fh)) => {
                let fa = to_file_attr(attr, req.uid(), req.gid());
                reply.created(&ATTR_TTL, &fa, 0, fh, 0);
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    /// Write `data` at `offset` into an open write handle's dirty buffer.
    #[allow(clippy::too_many_arguments)]
    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        if !self.read_write {
            return reply.error(libc::EROFS);
        }
        match self.fs.write(fh, offset.max(0) as u64, data) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(errno(e)),
        }
    }

    /// Set attributes; only a size change (truncate) is meaningful for a write
    /// handle. Other attribute sets are accepted as a no-op so `chmod`/`touch`
    /// don't fail the write flow.
    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        if let Some(new_size) = size {
            if !self.read_write {
                return reply.error(libc::EROFS);
            }
            if let Some(fh) = fh {
                if let Err(e) = self.fs.truncate(fh, new_size) {
                    return reply.error(errno(e));
                }
            }
        }
        // Reply with the current attributes.
        match self.fs.getattr(ino) {
            Ok(attr) => reply.attr(&ATTR_TTL, &to_file_attr(attr, req.uid(), req.gid())),
            Err(e) => reply.error(errno(e)),
        }
    }

    /// Flush a write handle: write its assembled object through to the backend.
    ///
    /// Object stores commit on a whole-object PUT, so the write is reported here
    /// (called on `close(2)`) rather than per `write`. A backend failure surfaces
    /// as the `close`/`flush` error. A read handle (no dirty buffer) is a no-op.
    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        let (path, bytes) = match (self.fs.dirty_path(fh), self.fs.dirty_bytes(fh)) {
            (Some(p), Some(b)) => (p, b),
            // Not a write handle → nothing to flush.
            _ => return reply.ok(),
        };
        match self.writeback_object(&path, bytes) {
            Ok(()) => reply.ok(),
            Err(error) => {
                tracing::warn!(%path, %error, "flush writeback failed");
                reply.error(libc::EIO);
            }
        }
    }

    /// `fsync`: same durability point as flush — PUT the dirty buffer through.
    fn fsync(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let (path, bytes) = match (self.fs.dirty_path(fh), self.fs.dirty_bytes(fh)) {
            (Some(p), Some(b)) => (p, b),
            _ => return reply.ok(),
        };
        match self.writeback_object(&path, bytes) {
            Ok(()) => reply.ok(),
            Err(_) => reply.error(libc::EIO),
        }
    }

    /// Remove a file: delete the backend object, then drop the namespace node.
    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        if !self.read_write {
            return reply.error(libc::EROFS);
        }
        let name = match name.to_str() {
            Some(s) => s,
            None => return reply.error(libc::EINVAL),
        };
        // Look up the path first (without removing) so a backend delete failure
        // leaves the namespace entry intact.
        let path = match self.fs.lookup(parent, name) {
            Ok(_) => match self.fs.file_path(parent, name) {
                Some(p) => p,
                None => return reply.error(libc::ENOENT),
            },
            Err(e) => return reply.error(errno(e)),
        };
        if let Err(error) = self.delete_backend_object(&path) {
            tracing::warn!(%path, %error, "unlink backend delete failed");
            return reply.error(libc::EIO);
        }
        match self.fs.unlink(parent, name) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
    }

    /// Release a previously opened handle.
    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        // Drop this handle's prefetch state (and its readahead cursor); any
        // in-flight speculative fetches finish on their own detached tasks.
        self.prefetchers.lock_recover().remove(&fh);
        match self.fs.release(fh) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
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
        let mounted = TalonFuse::new(
            Arc::clone(&fs),
            reader,
            tokio::runtime::Handle::current(),
            256 << 20,
            talon_core::Version::new(CANONICAL_MOUNT_VERSION),
        );
        // The adapter exposes its components for the callbacks to use.
        assert_eq!(mounted.namespace().getattr(1).unwrap().ino, 1);
        assert_eq!(mounted.reader().coordinator_addr(), "127.0.0.1:7000");
    }

    #[test]
    fn canonical_mount_version_is_a_stable_nonempty_token() {
        // The mount path is version-independent (issue #182): placement hashes
        // this token on both client and coordinator, and the worker owns
        // freshness. It must be non-empty (an empty version is refused by the
        // worker) and stable, so this guards against an accidental change.
        assert!(!CANONICAL_MOUNT_VERSION.is_empty());
        assert_eq!(CANONICAL_MOUNT_VERSION, "talon-mount-v1");
    }

    /// Build a `TalonFuse` over a `BlockReader` pointed at an unused address. The
    /// readahead *decision* (which block indices to prefetch) is independent of
    /// whether the speculative fetches succeed — they are fire-and-forget — so
    /// this exercises the mount→prefetcher wiring without any live server.
    fn adapter_with_readahead(window: u32) -> TalonFuse {
        let fs = Arc::new(ReadOnlyFs::new());
        let reader = BlockReader::new(
            CoordinatorClient::new("127.0.0.1:9"), // unused; prefetch fetches just fail
            Arc::new(PlacementCache::new(1000)),
            1,
        );
        TalonFuse::new(
            fs,
            reader,
            tokio::runtime::Handle::current(),
            8, // block_size
            talon_core::Version::new(CANONICAL_MOUNT_VERSION),
        )
        .with_readahead(window)
    }

    fn obj() -> ObjectId {
        ObjectId::new(talon_core::Backend::S3, "b", "o.bin")
    }

    #[tokio::test]
    async fn readahead_fires_only_after_a_sequential_run() {
        // Default trigger_run is 3 (issue #206): the first two in-order reads
        // establish the run but prefetch nothing; the third and onward prefetch
        // the window ahead of the cursor.
        let fuse = adapter_with_readahead(4);
        let size = 64 * 8; // 64 blocks of 8 bytes
        let o = Some(obj());
        // Reads at block 0,1 (offsets 0,8): run building, no prefetch yet.
        assert!(fuse.drive_readahead(1, 0, size, o.clone(), 0).is_empty());
        assert!(fuse.drive_readahead(1, 8, size, o.clone(), 0).is_empty());
        // Block 2: run reaches trigger_run=3 → prefetch the next blocks.
        let spawned = fuse.drive_readahead(1, 16, size, o.clone(), 0);
        assert!(!spawned.is_empty(), "sequential run must prefetch");
        assert_eq!(spawned, vec![3, 4, 5, 6], "window of 4 ahead of block 2");
    }

    #[tokio::test]
    async fn readahead_never_fires_for_random_access() {
        let fuse = adapter_with_readahead(4);
        let size = 64 * 8;
        let o = Some(obj());
        // Jumping around (0, 5, 2, 9) is never a sequential run → no prefetch.
        for off in [0u64, 40, 16, 72] {
            assert!(
                fuse.drive_readahead(1, off, size, o.clone(), 0).is_empty(),
                "random access must not prefetch (offset {off})"
            );
        }
    }

    #[tokio::test]
    async fn readahead_window_zero_disables_prefetch() {
        // A window of 0 (readahead_blocks = 0) turns prefetch off entirely, even
        // for a clean sequential run.
        let fuse = adapter_with_readahead(0);
        let size = 64 * 8;
        let o = Some(obj());
        for i in 0..6u64 {
            assert!(
                fuse.drive_readahead(1, i * 8, size, o.clone(), 0)
                    .is_empty(),
                "window 0 must never prefetch"
            );
        }
        // No per-handle state was even created.
        assert!(fuse.prefetchers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn release_drops_the_handle_prefetch_state() {
        let fuse = adapter_with_readahead(4);
        let size = 64 * 8;
        let o = Some(obj());
        let _ = fuse.drive_readahead(7, 0, size, o.clone(), 0);
        assert!(fuse.prefetchers.lock().unwrap().contains_key(&7));
        // Simulate the release callback's cleanup.
        fuse.prefetchers.lock().unwrap().remove(&7);
        assert!(!fuse.prefetchers.lock().unwrap().contains_key(&7));
    }

    #[test]
    fn errno_maps_each_fs_error() {
        assert_eq!(errno(FsError::NotFound), libc::ENOENT);
        assert_eq!(errno(FsError::ReadOnly), libc::EROFS);
        assert_eq!(errno(FsError::Unsupported), libc::ENOSYS);
        assert_eq!(errno(FsError::BadHandle), libc::EBADF);
        assert_eq!(errno(FsError::TooLarge), libc::EFBIG);
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

    #[tokio::test]
    async fn read_write_defaults_off_and_builder_enables_it() {
        // Write support is opt-in: a plain adapter is read-only (so mounts stay
        // safe unless a caller — the mount binary — explicitly enables writes),
        // and `with_read_write(true)` flips it on (#232).
        let ro = adapter_with_readahead(4);
        assert!(!ro.read_write, "adapter must default to read-only");
        let rw = adapter_with_readahead(4).with_read_write(true);
        assert!(rw.read_write, "with_read_write(true) enables writes");
    }
}
