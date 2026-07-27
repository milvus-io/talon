//! FUSE operation logic (read + write).
//!
//! This module implements the request-handling logic behind the filesystem ops
//! — `lookup`, `getattr`, `readdir`, `open`, `read`, `release` for the read path,
//! and `create`, `write`, `truncate`, `unlink` for the write path (#226/#231) —
//! decoupled from the `fuser` mount callbacks so it is unit-testable without a
//! kernel mount. The mount layer is a thin adapter that translates `fuser` calls
//! into these methods and back.
//!
//! Paths under the mount mirror the backend namespace (`/s3/<bucket>/<key…>`,
//! see [`crate::mapping`]). Directories are synthesized from the path hierarchy;
//! files correspond to objects. Writes accumulate into a per-handle whole-object
//! buffer (object stores replace whole objects, not byte ranges); the assembled
//! object is written through to the backend at flush.

use crate::lock::MutexExt;
use crate::mapping::path_to_object;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// The root inode number (fixed by FUSE convention).
pub const ROOT_INO: u64 = 1;

/// A file-system object kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    /// A directory (namespace prefix).
    Directory,
    /// A regular file (a backend object).
    File,
    /// A symbolic link whose target bytes are stored in a backend object.
    Symlink,
    /// A mount-local named pipe.
    NamedPipe,
    /// A mount-local block device node.
    BlockDevice,
    /// A mount-local character device node.
    CharDevice,
    /// A mount-local Unix socket node.
    Socket,
}

impl FileKind {
    pub(crate) fn has_backend_object(self) -> bool {
        matches!(self, Self::File | Self::Symlink)
    }
}

/// Synthesized attributes for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attr {
    /// Inode number.
    pub ino: u64,
    /// File vs directory.
    pub kind: FileKind,
    /// Size in bytes (0 for directories).
    pub size: u64,
    /// Read-only permission bits (dirs `0o555`, files `0o444`).
    pub perm: u16,
    /// Number of namespace links. An open file retained after unlink has zero.
    pub nlink: u32,
    /// Last access time.
    pub atime: SystemTime,
    /// Last content modification time.
    pub mtime: SystemTime,
    /// Last inode metadata change time.
    pub ctime: SystemTime,
    /// Device identifier for block and character devices.
    pub rdev: u32,
    /// Owning user.
    pub uid: u32,
    /// Owning group.
    pub gid: u32,
}

/// Errors returned by the read-only op layer (mapped to errno by the adapter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsError {
    /// No such file or directory (`ENOENT`).
    NotFound,
    /// A mutating op was attempted on the read-only FS (`EROFS`).
    ReadOnly,
    /// Operation not supported (`ENOSYS`).
    Unsupported,
    /// A read used a bad handle (`EBADF`).
    BadHandle,
    /// The write would grow a buffered object past the allowed maximum, or the
    /// requested offset is not representable (`EFBIG`).
    TooLarge,
    /// A file-only operation targeted a directory (`EISDIR`).
    IsDir,
    /// A directory was required but another node type was found (`ENOTDIR`).
    NotDir,
    /// The target name already exists (`EEXIST`).
    Exists,
    /// An invalid flag, name, path, or argument was supplied (`EINVAL`).
    Invalid,
    /// A directory is not empty (`ENOTEMPTY`).
    NotEmpty,
    /// A pathname component exceeds the filesystem name limit (`ENAMETOOLONG`).
    NameTooLong,
    /// Too many symbolic links were encountered (`ELOOP`).
    Loop,
    /// The operation is not permitted for this inode type (`EPERM`).
    OperationNotPermitted,
    /// Source and destination are on different backend filesystems (`EXDEV`).
    CrossDevice,
    /// Access is denied by inode mode bits (`EACCES`).
    PermissionDenied,
}

/// Access and mutation behavior for an open file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    /// Permit reads through the handle.
    pub read: bool,
    /// Permit writes through the handle.
    pub write: bool,
    /// Force each write to the current end of the file.
    pub append: bool,
}

/// Data source for a read through an open handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSource {
    /// Read from an in-progress whole-object write buffer.
    Buffered(Vec<u8>),
    /// Read the committed object from the backend.
    Backend {
        /// Mount-relative object path.
        path: String,
        /// Current object size.
        size: u64,
    },
}

/// Default cap on a single in-memory write buffer (1 GiB).
///
/// v1 buffers a whole object in RAM, so the buffer length is attacker-reachable
/// from an unprivileged `pwrite`/`ftruncate` at an arbitrary offset. Without a
/// cap, `pwrite(fd, buf, 1, 1<<40)` asks for a 1 TiB zero-filled allocation and
/// takes the mount (and likely the host) down. See
/// [`ReadOnlyFs::with_max_object_bytes`] to tune.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 1 << 30;

const INTERNAL_OBJECT_PREFIX: &str = ".__talon_internal";
const MAX_SYMLINK_TARGET_BYTES: usize = 4095;
const MODE_TYPE_MASK: u32 = 0o170000;
const MODE_SOCKET: u32 = 0o140000;
const MODE_SYMLINK: u32 = 0o120000;
const MODE_REGULAR: u32 = 0o100000;
const MODE_BLOCK_DEVICE: u32 = 0o060000;
const MODE_DIRECTORY: u32 = 0o040000;
const MODE_CHAR_DEVICE: u32 = 0o020000;
const MODE_NAMED_PIPE: u32 = 0o010000;

/// A directory entry yielded by `readdir`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    /// Child inode.
    pub ino: u64,
    /// Child kind.
    pub kind: FileKind,
    /// Child name (single path component).
    pub name: String,
}

/// Immutable backend and namespace facts for a regular-file rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRenamePlan {
    pub(crate) source_ino: u64,
    pub(crate) source_parent: u64,
    pub(crate) source_name: String,
    pub(crate) source_path: String,
    pub(crate) source_size: u64,
    pub(crate) source_backend_object: bool,
    pub(crate) target_parent: u64,
    pub(crate) target_name: String,
    pub(crate) target_path: String,
    pub(crate) target: Option<(u64, u64)>,
    pub(crate) target_backend_object: bool,
}

/// One backend object moved as part of a directory-tree rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryRenameEntry {
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) size: u64,
    pub(crate) marker: bool,
}

/// Immutable backend and namespace facts for a directory-tree rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryRenamePlan {
    pub(crate) source_ino: u64,
    pub(crate) source_parent: u64,
    pub(crate) source_name: String,
    pub(crate) source_path: String,
    pub(crate) target_parent: u64,
    pub(crate) target_name: String,
    pub(crate) target_path: String,
    pub(crate) target: Option<(u64, bool)>,
    pub(crate) entries: Vec<DirectoryRenameEntry>,
}

/// Immutable backend and namespace facts for an unlink operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlinkPlan {
    pub(crate) ino: u64,
    pub(crate) parent: u64,
    pub(crate) name: String,
    pub(crate) source_path: String,
    pub(crate) source_size: u64,
    pub(crate) orphan_path: Option<String>,
    pub(crate) buffered_contents: Option<Vec<u8>>,
    pub(crate) backend_object: bool,
}

/// Immutable backend and namespace facts for hard-link creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardLinkPlan {
    pub(crate) source_ino: u64,
    pub(crate) source_path: String,
    pub(crate) source_size: u64,
    pub(crate) buffered_contents: Option<Vec<u8>>,
    pub(crate) target_parent: u64,
    pub(crate) target_name: String,
    pub(crate) target_path: String,
    pub(crate) backend_object: bool,
}

/// Immutable namespace and backend facts for `mknod`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MknodPlan {
    pub(crate) parent: u64,
    pub(crate) name: String,
    pub(crate) path: String,
    pub(crate) kind: FileKind,
    pub(crate) perm: u16,
    pub(crate) rdev: u32,
    pub(crate) uid: u32,
    pub(crate) gid: u32,
}

#[derive(Debug, Clone)]
struct Node {
    ino: u64,
    parent: Option<u64>,
    name: String,
    kind: FileKind,
    size: u64,
    children: Vec<u64>,
    /// Full mount-relative path, e.g. `s3/bkt/dir` or `s3/bkt/o.bin`.
    path: String,
    /// Whether this directory is persisted by a trailing-slash blob marker.
    directory_marker: bool,
    /// Raw target bytes for symbolic links.
    symlink_target: Option<Vec<u8>>,
    /// Whether the inode still has a visible namespace entry.
    linked: bool,
    perm: u16,
    rdev: u32,
    uid: u32,
    gid: u32,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
}

/// A read-only view over the backend namespace, addressed by inode.
///
/// Nodes are registered up front (e.g. from a coordinator listing) via
/// [`insert_object`](ReadOnlyFs::insert_object); the tree of synthetic
/// directories is created on demand.
pub struct ReadOnlyFs {
    inner: Mutex<Inner>,
    /// Owner assigned to nodes synthesized from backend listings.
    listing_uid: u32,
    /// Group assigned to nodes synthesized from backend listings.
    listing_gid: u32,
    /// Cap on the length of any single write handle's in-memory buffer.
    max_object_bytes: u64,
    /// Mount-scoped backend namespace for open-but-unlinked objects.
    orphan_namespace: String,
}

struct Inner {
    nodes: HashMap<u64, Node>,
    // (parent_ino, name) -> child_ino for O(1) lookup.
    index: HashMap<(u64, String), u64>,
    next_ino: u64,
    // Open file handles -> inode and access mode.
    handles: HashMap<u64, Handle>,
    next_fh: u64,
    // Write handles -> their in-progress whole-object buffer. A handle opened for
    // write accumulates bytes here (random-offset writes land by position); the
    // assembled object is taken at flush/release and written through to the
    // backend (#226/#231). v1 buffers in memory (objects are single-block/small);
    // a temp-file-backed buffer for large objects is future work.
    dirty: HashMap<u64, DirtyFile>,
}

#[derive(Debug, Clone, Copy)]
struct Handle {
    ino: u64,
    read: bool,
    write: bool,
    append: bool,
}

/// A per-write-handle whole-object buffer.
struct DirtyFile {
    /// The file inode this handle writes.
    ino: u64,
    /// The object contents assembled so far (extended with zeros for sparse
    /// writes past the current end).
    buf: Vec<u8>,
}

static NEXT_FS_INSTANCE: AtomicU64 = AtomicU64::new(1);

impl Default for ReadOnlyFs {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadOnlyFs {
    /// Create a root-owned filesystem with just the root directory.
    pub fn new() -> Self {
        Self::new_with_owner(0, 0)
    }

    /// Create a filesystem whose listed objects belong to `uid` and `gid`.
    pub fn new_with_owner(uid: u32, gid: u32) -> Self {
        let mut nodes = HashMap::new();
        let now = SystemTime::now();
        nodes.insert(
            ROOT_INO,
            Node {
                ino: ROOT_INO,
                parent: None,
                name: "/".to_string(),
                kind: FileKind::Directory,
                size: 0,
                children: Vec::new(),
                path: String::new(),
                directory_marker: false,
                symlink_target: None,
                linked: true,
                perm: 0o555,
                rdev: 0,
                uid,
                gid,
                atime: now,
                mtime: now,
                ctime: now,
            },
        );
        let instance = NEXT_FS_INSTANCE.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            inner: Mutex::new(Inner {
                nodes,
                index: HashMap::new(),
                next_ino: ROOT_INO + 1,
                handles: HashMap::new(),
                next_fh: 1,
                dirty: HashMap::new(),
            }),
            listing_uid: uid,
            listing_gid: gid,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            orphan_namespace: format!("{:x}-{timestamp:x}-{instance:x}", std::process::id()),
        }
    }

    /// Override the per-object write-buffer cap (see
    /// [`DEFAULT_MAX_OBJECT_BYTES`]). Writes or truncates that would push a
    /// buffer past this fail with [`FsError::TooLarge`] (`EFBIG`).
    pub fn with_max_object_bytes(mut self, max: u64) -> Self {
        self.max_object_bytes = max;
        self
    }

    /// The configured per-object write-buffer cap.
    pub fn max_object_bytes(&self) -> u64 {
        self.max_object_bytes
    }

    /// Register an object at `path` (e.g. `/s3/bucket/a/b/file.bin`) with `size`,
    /// creating intermediate directories. Returns the file's inode.
    pub fn insert_object(&self, path: &str, size: u64) -> u64 {
        let comps: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
        let mut g = self.inner.lock_recover();
        let mut parent = ROOT_INO;
        let mut current_path = String::new();
        let now = SystemTime::now();
        for (i, comp) in comps.iter().enumerate() {
            let is_leaf = i == comps.len() - 1;
            if !current_path.is_empty() {
                current_path.push('/');
            }
            current_path.push_str(comp);
            let key = (parent, comp.to_string());
            if let Some(&existing) = g.index.get(&key) {
                parent = existing;
                continue;
            }
            let ino = g.next_ino;
            g.next_ino += 1;
            let kind = if is_leaf {
                FileKind::File
            } else {
                FileKind::Directory
            };
            let node = Node {
                ino,
                parent: Some(parent),
                name: comp.to_string(),
                kind,
                size: if is_leaf { size } else { 0 },
                children: Vec::new(),
                path: current_path.clone(),
                directory_marker: false,
                symlink_target: None,
                linked: true,
                perm: if is_leaf { 0o644 } else { 0o755 },
                rdev: 0,
                uid: self.listing_uid,
                gid: self.listing_gid,
                atime: now,
                mtime: now,
                ctime: now,
            };
            g.nodes.insert(ino, node);
            g.index.insert(key, ino);
            g.nodes.get_mut(&parent).unwrap().children.push(ino);
            parent = ino;
        }
        parent
    }

    /// Bulk-register `(path, size)` listing entries, synthesizing directories.
    ///
    /// Convenience over [`insert_object`](Self::insert_object) for populating the
    /// namespace from a coordinator `ObjectList`. Idempotent: re-inserting an
    /// existing object is a no-op for the tree shape (the path already resolves).
    /// Returns the number of entries processed.
    pub fn populate_from_listing<'a, I>(&self, entries: I) -> usize
    where
        I: IntoIterator<Item = (&'a str, u64)>,
    {
        let mut n = 0;
        for (path, size) in entries {
            if Self::is_internal_object_path(path) {
                continue;
            }
            if path.ends_with('/') {
                if self.insert_directory_marker(path).is_ok() {
                    n += 1;
                }
            } else {
                self.insert_object(path, size);
                n += 1;
            }
        }
        n
    }

    /// Register a directory represented by a trailing-slash blob marker.
    pub fn insert_directory_marker(&self, marker_path: &str) -> Result<u64, FsError> {
        let path = marker_path
            .strip_suffix('/')
            .filter(|path| !path.is_empty())
            .ok_or(FsError::Invalid)?;
        Self::validate_visible_object_path(path)?;
        let comps: Vec<&str> = path
            .split('/')
            .filter(|component| !component.is_empty())
            .collect();
        let mut g = self.inner.lock_recover();
        let mut parent = ROOT_INO;
        let mut current_path = String::new();
        let now = SystemTime::now();
        for (index, component) in comps.iter().enumerate() {
            if !current_path.is_empty() {
                current_path.push('/');
            }
            current_path.push_str(component);
            let key = (parent, component.to_string());
            if let Some(&existing) = g.index.get(&key) {
                let node = g.nodes.get_mut(&existing).ok_or(FsError::NotFound)?;
                if node.kind != FileKind::Directory {
                    return Err(FsError::NotDir);
                }
                if index == comps.len() - 1 {
                    node.directory_marker = true;
                }
                parent = existing;
                continue;
            }
            let ino = g.next_ino;
            g.next_ino += 1;
            let node = Node {
                ino,
                parent: Some(parent),
                name: component.to_string(),
                kind: FileKind::Directory,
                size: 0,
                children: Vec::new(),
                path: current_path.clone(),
                directory_marker: index == comps.len() - 1,
                symlink_target: None,
                linked: true,
                perm: 0o755,
                rdev: 0,
                uid: self.listing_uid,
                gid: self.listing_gid,
                atime: now,
                mtime: now,
                ctime: now,
            };
            g.nodes.insert(ino, node);
            g.index.insert(key, ino);
            g.nodes.get_mut(&parent).unwrap().children.push(ino);
            parent = ino;
        }
        Ok(parent)
    }

    fn attr_of(g: &Inner, node: &Node) -> Attr {
        let nlink = if node.kind == FileKind::Directory {
            2 + g
                .index
                .iter()
                .filter(|((parent, _), child_ino)| {
                    *parent == node.ino
                        && g.nodes
                            .get(child_ino)
                            .is_some_and(|child| child.kind == FileKind::Directory)
                })
                .count() as u32
        } else {
            g.index
                .values()
                .filter(|candidate| **candidate == node.ino)
                .count() as u32
        };
        Attr {
            ino: node.ino,
            kind: node.kind,
            size: node.size,
            perm: node.perm,
            nlink,
            atime: node.atime,
            mtime: node.mtime,
            ctime: node.ctime,
            rdev: node.rdev,
            uid: node.uid,
            gid: node.gid,
        }
    }

    /// `lookup`: resolve a child `name` under directory `parent_ino`.
    pub fn lookup(&self, parent_ino: u64, name: &str) -> Result<Attr, FsError> {
        Self::validate_name_length(name)?;
        let g = self.inner.lock_recover();
        let ino = *g
            .index
            .get(&(parent_ino, name.to_string()))
            .ok_or(FsError::NotFound)?;
        Ok(Self::attr_of(
            &g,
            g.nodes.get(&ino).ok_or(FsError::NotFound)?,
        ))
    }

    /// `getattr`: attributes for an inode.
    pub fn getattr(&self, ino: u64) -> Result<Attr, FsError> {
        let g = self.inner.lock_recover();
        Ok(Self::attr_of(
            &g,
            g.nodes.get(&ino).ok_or(FsError::NotFound)?,
        ))
    }

    /// Apply explicit atime/mtime updates and advance ctime when either changes.
    pub fn set_times(
        &self,
        ino: u64,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> Result<Attr, FsError> {
        let mut g = self.inner.lock_recover();
        let node = g.nodes.get_mut(&ino).ok_or(FsError::NotFound)?;
        let changed = atime.is_some() || mtime.is_some();
        if let Some(atime) = atime {
            node.atime = atime;
        }
        if let Some(mtime) = mtime {
            node.mtime = mtime;
        }
        if changed {
            node.ctime = SystemTime::now();
        }
        Ok(Self::attr_of(
            &g,
            g.nodes.get(&ino).ok_or(FsError::NotFound)?,
        ))
    }

    /// Apply chmod, chown, and timestamp changes with POSIX ownership checks.
    #[allow(clippy::too_many_arguments)]
    pub fn set_metadata(
        &self,
        ino: u64,
        caller_uid: u32,
        caller_gid: u32,
        caller_groups: &[u32],
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        times_are_now: bool,
    ) -> Result<Attr, FsError> {
        let mut g = self.inner.lock_recover();
        let node = g.nodes.get_mut(&ino).ok_or(FsError::NotFound)?;
        let is_root = caller_uid == 0;
        let is_owner = caller_uid == node.uid;

        if let Some(mode) = mode {
            let requested = (mode & 0o7777) as u16;
            let automatic_write_clear = requested == node.perm & !0o6000
                && Self::access_allowed_with_groups(
                    node,
                    caller_uid,
                    caller_gid,
                    caller_groups,
                    0o2,
                );
            if !is_root && !is_owner && !automatic_write_clear {
                return Err(FsError::OperationNotPermitted);
            }
        }
        if (uid.is_some() || gid.is_some()) && !is_root && !is_owner {
            return Err(FsError::OperationNotPermitted);
        }
        if let Some(new_uid) = uid {
            if !is_root && new_uid != node.uid {
                return Err(FsError::OperationNotPermitted);
            }
        }
        if let Some(new_gid) = gid {
            if !is_root
                && new_gid != node.gid
                && new_gid != caller_gid
                && !caller_groups.contains(&new_gid)
            {
                return Err(FsError::OperationNotPermitted);
            }
        }
        if atime.is_some() || mtime.is_some() {
            let can_write =
                Self::access_allowed_with_groups(node, caller_uid, caller_gid, caller_groups, 0o2);
            if !(is_root || is_owner || times_are_now && can_write) {
                return Err(FsError::OperationNotPermitted);
            }
        }

        let mut changed = false;
        if let Some(mode) = mode {
            let mut perm = (mode & 0o7777) as u16;
            if !is_root && caller_gid != node.gid && !caller_groups.contains(&node.gid) {
                perm &= !0o2000;
            }
            node.perm = perm;
            changed = true;
        }
        let ownership_changed = uid.is_some_and(|new_uid| new_uid != node.uid)
            || gid.is_some_and(|new_gid| new_gid != node.gid);
        if let Some(uid) = uid {
            node.uid = uid;
            changed = true;
        }
        if let Some(gid) = gid {
            node.gid = gid;
            changed = true;
        }
        if ownership_changed && node.kind == FileKind::File {
            node.perm &= !0o6000;
        }
        if let Some(atime) = atime {
            node.atime = atime;
            changed = true;
        }
        if let Some(mtime) = mtime {
            node.mtime = mtime;
            changed = true;
        }
        if changed {
            node.ctime = SystemTime::now();
        }
        Ok(Self::attr_of(
            &g,
            g.nodes.get(&ino).ok_or(FsError::NotFound)?,
        ))
    }

    /// Check read, write, and execute access for one primary uid/gid.
    pub fn check_access(&self, ino: u64, uid: u32, gid: u32, mask: i32) -> Result<(), FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if Self::access_allowed(node, uid, gid, mask as u16) {
            Ok(())
        } else {
            Err(FsError::PermissionDenied)
        }
    }

    /// `readdir`: list children of a directory inode (excluding `.`/`..`).
    pub fn readdir(&self, ino: u64) -> Result<Vec<DirEntry>, FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        let mut entries: Vec<DirEntry> = g
            .index
            .iter()
            .filter(|((parent, _), _)| *parent == ino)
            .filter_map(|((_, name), child_ino)| {
                g.nodes.get(child_ino).map(|child| DirEntry {
                    ino: child.ino,
                    kind: child.kind,
                    name: name.clone(),
                })
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    /// Return a directory inode's parent, with root parented to itself.
    pub fn parent_ino(&self, ino: u64) -> Result<u64, FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        Ok(node.parent.unwrap_or(ROOT_INO))
    }

    /// Open an existing file with the requested access mode.
    ///
    /// Writable handles use `initial_contents` as their whole-object working
    /// copy. A truncating open passes an empty vector; a non-truncating open
    /// passes bytes read from the committed backend object.
    pub fn open_with_options(
        &self,
        ino: u64,
        options: OpenOptions,
        initial_contents: Option<Vec<u8>>,
    ) -> Result<u64, FsError> {
        self.open_with_options_and_truncate(ino, options, initial_contents, false)
    }

    /// Open an existing file and record whether `O_TRUNC` was requested.
    pub fn open_with_options_and_truncate(
        &self,
        ino: u64,
        options: OpenOptions,
        initial_contents: Option<Vec<u8>>,
        truncate: bool,
    ) -> Result<u64, FsError> {
        if !options.read && !options.write {
            return Err(FsError::Invalid);
        }
        let mut g = self.inner.lock_recover();
        let kind = g.nodes.get(&ino).ok_or(FsError::NotFound)?.kind;
        match kind {
            FileKind::File => {}
            FileKind::Directory => return Err(FsError::IsDir),
            FileKind::Symlink => return Err(FsError::Loop),
            FileKind::NamedPipe
            | FileKind::BlockDevice
            | FileKind::CharDevice
            | FileKind::Socket => return Err(FsError::Unsupported),
        }
        if options.write && initial_contents.is_none() {
            return Err(FsError::Invalid);
        }
        let fh = g.next_fh;
        g.next_fh += 1;
        g.handles.insert(
            fh,
            Handle {
                ino,
                read: options.read,
                write: options.write,
                append: options.append,
            },
        );
        if let Some(buf) = initial_contents {
            g.dirty.insert(fh, DirtyFile { ino, buf });
            let size = g.dirty.get(&fh).unwrap().buf.len() as u64;
            let node = g.nodes.get_mut(&ino).unwrap();
            node.size = size;
            if truncate {
                let now = SystemTime::now();
                node.mtime = now;
                node.ctime = now;
            }
        }
        Ok(fh)
    }

    /// Open an existing file read-only.
    pub fn open(&self, ino: u64) -> Result<u64, FsError> {
        self.open_with_options(
            ino,
            OpenOptions {
                read: true,
                write: false,
                append: false,
            },
            None,
        )
    }

    /// Return the orphan backend path that the final release must delete.
    pub fn release_cleanup_path(&self, fh: u64) -> Result<Option<String>, FsError> {
        let g = self.inner.lock_recover();
        let handle = g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        let is_last_handle = g
            .handles
            .values()
            .filter(|candidate| candidate.ino == handle.ino)
            .count()
            == 1;
        let Some(node) = g.nodes.get(&handle.ino) else {
            return Ok(None);
        };
        Ok((is_last_handle && !node.linked).then(|| node.path.clone()))
    }

    /// `release`: drop a previously opened handle (read or write), discarding any
    /// write buffer. The caller is expected to have already flushed a dirty write
    /// handle's contents and deleted a final orphan object when requested by
    /// [`release_cleanup_path`](Self::release_cleanup_path).
    pub fn release(&self, fh: u64) -> Result<(), FsError> {
        let mut g = self.inner.lock_recover();
        let had_dirty = g.dirty.remove(&fh).is_some();
        let handle = g.handles.remove(&fh);
        let Some(handle) = handle else {
            return if had_dirty {
                Ok(())
            } else {
                Err(FsError::BadHandle)
            };
        };
        let has_remaining_handles = g
            .handles
            .values()
            .any(|candidate| candidate.ino == handle.ino);
        if !has_remaining_handles && g.nodes.get(&handle.ino).is_some_and(|node| !node.linked) {
            g.nodes.remove(&handle.ino);
        }
        Ok(())
    }

    /// The mount-relative object path + size for an open file handle.
    ///
    /// Lets a data callback recover the backend object (via
    /// [`crate::mapping::path_to_object`]) and clamp reads to the file size.
    /// Errors with [`FsError::BadHandle`] for an unknown handle or
    /// [`FsError::Unsupported`] if the handle somehow references a directory.
    pub fn file_meta(&self, fh: u64) -> Result<(String, u64), FsError> {
        let g = self.inner.lock_recover();
        let handle = g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        if !handle.read {
            return Err(FsError::BadHandle);
        }
        let node = g.nodes.get(&handle.ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::File {
            return Err(FsError::IsDir);
        }
        Ok((node.path.clone(), node.size))
    }

    /// Return a handle's read source, preferring its uncommitted write buffer.
    pub fn read_source(&self, fh: u64, offset: u64, size: u32) -> Result<ReadSource, FsError> {
        let mut g = self.inner.lock_recover();
        let handle = g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        if !handle.read {
            return Err(FsError::BadHandle);
        }
        let ino = handle.ino;
        let source = if let Some(dirty) = g.dirty.get(&fh) {
            let start = usize::try_from(offset).map_err(|_| FsError::Invalid)?;
            if start >= dirty.buf.len() {
                ReadSource::Buffered(Vec::new())
            } else {
                let end = start.saturating_add(size as usize).min(dirty.buf.len());
                ReadSource::Buffered(dirty.buf[start..end].to_vec())
            }
        } else {
            let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
            ReadSource::Backend {
                path: node.path.clone(),
                size: node.size,
            }
        };
        g.nodes.get_mut(&ino).ok_or(FsError::NotFound)?.atime = SystemTime::now();
        Ok(source)
    }

    /// Return the committed object path and size for an inode before opening it.
    pub fn inode_file_meta(&self, ino: u64) -> Result<(String, u64), FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::File {
            return Err(FsError::IsDir);
        }
        Ok((node.path.clone(), node.size))
    }

    // ----- write path (#231) -----------------------------------------------

    /// `create`: make a new empty file `name` under directory `parent_ino` and
    /// open it for writing.
    ///
    /// Inserts a `File` node (size 0) into the namespace and returns
    /// `(attr, fh)` where `fh` is a write handle backed by an empty dirty buffer.
    /// Fails with [`FsError::NotDir`] if `parent_ino` is not a directory or
    /// [`FsError::Exists`] if the name already exists.
    pub fn create_with_options(
        &self,
        parent_ino: u64,
        name: &str,
        options: OpenOptions,
    ) -> Result<(Attr, u64), FsError> {
        self.create_with_metadata(parent_ino, name, options, 0o666, 0, 0, 0)
    }

    /// Create a file with request ownership, mode, and umask metadata.
    #[allow(clippy::too_many_arguments)]
    pub fn create_with_metadata(
        &self,
        parent_ino: u64,
        name: &str,
        options: OpenOptions,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
    ) -> Result<(Attr, u64), FsError> {
        if !options.read && !options.write {
            return Err(FsError::Invalid);
        }
        if name.is_empty() || name == "." || name == ".." || name.contains('/') {
            return Err(FsError::Invalid);
        }
        let mut g = self.inner.lock_recover();
        let (parent_perm, parent_gid) = {
            let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
            if parent.kind != FileKind::Directory {
                return Err(FsError::NotDir);
            }
            (parent.perm, parent.gid)
        };
        if g.index.contains_key(&(parent_ino, name.to_string())) {
            return Err(FsError::Exists);
        }
        // Build the child's mount-relative path from its ancestors.
        let path = {
            let mut parts = Self::ancestry(&g, parent_ino);
            parts.push(name.to_string());
            parts.join("/")
        };
        Self::validate_visible_object_path(&path)?;
        let ino = g.next_ino;
        g.next_ino += 1;
        let now = SystemTime::now();
        let child_gid = if parent_perm & 0o2000 != 0 {
            parent_gid
        } else {
            gid
        };
        let node = Node {
            ino,
            parent: Some(parent_ino),
            name: name.to_string(),
            kind: FileKind::File,
            size: 0,
            children: Vec::new(),
            path,
            directory_marker: false,
            symlink_target: None,
            linked: true,
            perm: ((mode & !umask) & 0o7777) as u16,
            rdev: 0,
            uid,
            gid: child_gid,
            atime: now,
            mtime: now,
            ctime: now,
        };
        g.nodes.insert(ino, node);
        g.index.insert((parent_ino, name.to_string()), ino);
        g.nodes.get_mut(&parent_ino).unwrap().children.push(ino);
        Self::mark_directory_changed(&mut g, parent_ino, now);
        let fh = g.next_fh;
        g.next_fh += 1;
        g.handles.insert(
            fh,
            Handle {
                ino,
                read: options.read,
                write: options.write,
                append: options.append,
            },
        );
        // A newly created object must be written through even when opened
        // read-only and never explicitly written.
        g.dirty.insert(
            fh,
            DirtyFile {
                ino,
                buf: Vec::new(),
            },
        );
        let attr = Self::attr_of(&g, g.nodes.get(&ino).unwrap());
        Ok((attr, fh))
    }

    /// Create a new file opened write-only with an empty whole-object buffer.
    pub fn create(&self, parent_ino: u64, name: &str) -> Result<(Attr, u64), FsError> {
        self.create_with_options(
            parent_ino,
            name,
            OpenOptions {
                read: false,
                write: true,
                append: false,
            },
        )
    }

    /// Validate and describe a regular or mount-local special node creation.
    #[allow(clippy::too_many_arguments)]
    pub fn mknod_plan(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u32,
        umask: u32,
        rdev: u32,
        uid: u32,
        gid: u32,
    ) -> Result<MknodPlan, FsError> {
        Self::validate_component(name)?;
        let kind = match mode & MODE_TYPE_MASK {
            MODE_REGULAR => FileKind::File,
            MODE_NAMED_PIPE => FileKind::NamedPipe,
            MODE_BLOCK_DEVICE => FileKind::BlockDevice,
            MODE_CHAR_DEVICE => FileKind::CharDevice,
            MODE_SOCKET => FileKind::Socket,
            MODE_DIRECTORY | MODE_SYMLINK => return Err(FsError::OperationNotPermitted),
            _ => return Err(FsError::Invalid),
        };
        let g = self.inner.lock_recover();
        let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
        if parent.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        if g.index.contains_key(&(parent_ino, name.to_string())) {
            return Err(FsError::Exists);
        }
        let path = Self::child_path(parent, name);
        Self::validate_visible_object_path(&path)?;
        let child_gid = if parent.perm & 0o2000 != 0 {
            parent.gid
        } else {
            gid
        };
        Ok(MknodPlan {
            parent: parent_ino,
            name: name.to_string(),
            path,
            kind,
            perm: ((mode & !umask) & 0o7777) as u16,
            rdev: if matches!(kind, FileKind::BlockDevice | FileKind::CharDevice) {
                rdev
            } else {
                0
            },
            uid,
            gid: child_gid,
        })
    }

    /// Publish a node after any required regular-file backend PUT succeeds.
    pub fn commit_mknod(&self, plan: &MknodPlan) -> Result<Attr, FsError> {
        let mut g = self.inner.lock_recover();
        if g.index.contains_key(&(plan.parent, plan.name.clone())) {
            return Err(FsError::Exists);
        }
        let parent = g.nodes.get(&plan.parent).ok_or(FsError::NotFound)?;
        if parent.kind != FileKind::Directory || Self::child_path(parent, &plan.name) != plan.path {
            return Err(FsError::NotDir);
        }
        let ino = g.next_ino;
        g.next_ino += 1;
        let now = SystemTime::now();
        g.nodes.insert(
            ino,
            Node {
                ino,
                parent: Some(plan.parent),
                name: plan.name.clone(),
                kind: plan.kind,
                size: 0,
                children: Vec::new(),
                path: plan.path.clone(),
                directory_marker: false,
                symlink_target: None,
                linked: true,
                perm: plan.perm,
                rdev: plan.rdev,
                uid: plan.uid,
                gid: plan.gid,
                atime: now,
                mtime: now,
                ctime: now,
            },
        );
        g.index.insert((plan.parent, plan.name.clone()), ino);
        g.nodes
            .get_mut(&plan.parent)
            .ok_or(FsError::NotFound)?
            .children
            .push(ino);
        Self::mark_directory_changed(&mut g, plan.parent, now);
        Ok(Self::attr_of(
            &g,
            g.nodes.get(&ino).ok_or(FsError::NotFound)?,
        ))
    }

    /// `write`: write `data` at `offset` into the write handle's buffer.
    ///
    /// Random-offset writes land by position; a write past the current end
    /// extends the buffer with zeros (sparse). Updates the node's visible size.
    /// Returns the number of bytes written. Fails with [`FsError::BadHandle`] for
    /// a handle not opened for write, or [`FsError::TooLarge`] (`EFBIG`) if the
    /// resulting buffer would exceed [`ReadOnlyFs::max_object_bytes`].
    ///
    /// The end offset is computed with a checked add: `offset` comes straight
    /// from the kernel and `offset + len` would otherwise overflow `usize` (a
    /// debug panic under the global lock, a wrap in release).
    pub fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32, FsError> {
        let mut g = self.inner.lock_recover();
        let handle = g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        if !handle.write {
            return Err(FsError::BadHandle);
        }
        let append = handle.append;
        let start = if append {
            g.dirty.get(&fh).ok_or(FsError::BadHandle)?.buf.len() as u64
        } else {
            offset
        };
        let end = start
            .checked_add(data.len() as u64)
            .ok_or(FsError::TooLarge)?;
        if end > self.max_object_bytes {
            return Err(FsError::TooLarge);
        }
        let end: usize = end.try_into().map_err(|_| FsError::TooLarge)?;
        let start: usize = start.try_into().map_err(|_| FsError::TooLarge)?;
        let dirty = g.dirty.get_mut(&fh).ok_or(FsError::BadHandle)?;
        if dirty.buf.len() < end {
            dirty.buf.resize(end, 0);
        }
        dirty.buf[start..end].copy_from_slice(data);
        let ino = dirty.ino;
        let new_size = dirty.buf.len() as u64;
        if let Some(node) = g.nodes.get_mut(&ino) {
            node.size = node.size.max(new_size);
            let now = SystemTime::now();
            node.mtime = now;
            node.ctime = now;
        }
        Ok(data.len() as u32)
    }

    /// Build a resized whole-object buffer for `ftruncate` without committing
    /// the visible size. The caller can write the returned bytes through to the
    /// backend before calling [`commit_handle_contents`](Self::commit_handle_contents).
    pub fn truncate_handle_plan(&self, fh: u64, size: u64) -> Result<(String, Vec<u8>), FsError> {
        let g = self.inner.lock_recover();
        let handle = g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        if !handle.write {
            return Err(FsError::BadHandle);
        }
        if size > self.max_object_bytes {
            return Err(FsError::TooLarge);
        }
        let new_len: usize = size.try_into().map_err(|_| FsError::TooLarge)?;
        let dirty = g.dirty.get(&fh).ok_or(FsError::BadHandle)?;
        let node = g.nodes.get(&dirty.ino).ok_or(FsError::NotFound)?;
        let mut contents = dirty.buf.clone();
        contents.resize(new_len, 0);
        Ok((node.path.clone(), contents))
    }

    /// Build a resized whole-object buffer for path-based `truncate`.
    ///
    /// `contents` must contain the committed prefix that should survive the
    /// resize. A shrink may pass only the retained prefix; an extension passes
    /// the complete existing object.
    pub fn truncate_inode_plan(
        &self,
        ino: u64,
        size: u64,
        mut contents: Vec<u8>,
    ) -> Result<(String, Vec<u8>), FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::File {
            return Err(FsError::IsDir);
        }
        if size > self.max_object_bytes {
            return Err(FsError::TooLarge);
        }
        let new_len: usize = size.try_into().map_err(|_| FsError::TooLarge)?;
        contents.resize(new_len, 0);
        Ok((node.path.clone(), contents))
    }

    /// Commit bytes after a successful handle-based truncate write-through.
    pub fn commit_handle_contents(&self, fh: u64, contents: Vec<u8>) -> Result<Attr, FsError> {
        let mut g = self.inner.lock_recover();
        let handle = g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        if !handle.write {
            return Err(FsError::BadHandle);
        }
        let ino = handle.ino;
        Self::commit_inode_contents_locked(&mut g, ino, contents)
    }

    /// Commit bytes after a successful path-based truncate write-through.
    pub fn commit_inode_contents(&self, ino: u64, contents: Vec<u8>) -> Result<Attr, FsError> {
        let mut g = self.inner.lock_recover();
        Self::commit_inode_contents_locked(&mut g, ino, contents)
    }

    /// Resize a handle in memory. The mount adapter uses the plan/commit methods
    /// above so backend write-through happens before this state change.
    pub fn truncate(&self, fh: u64, size: u64) -> Result<(), FsError> {
        let (_, contents) = self.truncate_handle_plan(fh, size)?;
        self.commit_handle_contents(fh, contents)?;
        Ok(())
    }

    fn commit_inode_contents_locked(
        g: &mut Inner,
        ino: u64,
        contents: Vec<u8>,
    ) -> Result<Attr, FsError> {
        let node = g.nodes.get_mut(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::File {
            return Err(FsError::IsDir);
        }
        node.size = contents.len() as u64;
        let now = SystemTime::now();
        node.mtime = now;
        node.ctime = now;
        for dirty in g.dirty.values_mut().filter(|dirty| dirty.ino == ino) {
            dirty.buf.clone_from(&contents);
        }
        Ok(Self::attr_of(g, g.nodes.get(&ino).unwrap()))
    }

    /// Return the assembled object bytes for a write handle (for writeback at
    /// flush/release), leaving the handle's buffer in place so a later flush is a
    /// no-op unless more is written. Returns `None` for a non-write handle.
    pub fn dirty_bytes(&self, fh: u64) -> Option<Vec<u8>> {
        let g = self.inner.lock_recover();
        g.dirty.get(&fh).map(|d| d.buf.clone())
    }

    /// The mount-relative object path a write handle targets.
    pub fn dirty_path(&self, fh: u64) -> Option<String> {
        let g = self.inner.lock_recover();
        let ino = g.dirty.get(&fh)?.ino;
        g.nodes.get(&ino).map(|n| n.path.clone())
    }

    /// All backend object paths linked to a write handle's inode.
    pub fn dirty_paths(&self, fh: u64) -> Result<Vec<String>, FsError> {
        let g = self.inner.lock_recover();
        let ino = g.dirty.get(&fh).ok_or(FsError::BadHandle)?.ino;
        Self::inode_paths_locked(&g, ino)
    }

    /// All visible backend object paths linked to an inode.
    pub fn inode_paths(&self, ino: u64) -> Result<Vec<String>, FsError> {
        let g = self.inner.lock_recover();
        Self::inode_paths_locked(&g, ino)
    }

    /// The mount-relative object path of file `name` under `parent_ino`, without
    /// removing it. `None` if the name doesn't resolve to a file.
    pub fn file_path(&self, parent_ino: u64, name: &str) -> Option<String> {
        let g = self.inner.lock_recover();
        let ino = *g.index.get(&(parent_ino, name.to_string()))?;
        let node = g.nodes.get(&ino)?;
        if node.kind == FileKind::Directory {
            return None;
        }
        Some(node.path.clone())
    }

    /// Validate and describe hard-link creation without mutating namespace.
    pub fn hard_link_plan(
        &self,
        source_ino: u64,
        target_parent: u64,
        target_name: &str,
    ) -> Result<HardLinkPlan, FsError> {
        Self::validate_component(target_name)?;
        let g = self.inner.lock_recover();
        let source = g.nodes.get(&source_ino).ok_or(FsError::NotFound)?;
        if source.kind == FileKind::Directory {
            return Err(FsError::OperationNotPermitted);
        }
        let target_parent_node = g.nodes.get(&target_parent).ok_or(FsError::NotFound)?;
        if target_parent_node.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        if g.index
            .contains_key(&(target_parent, target_name.to_string()))
        {
            return Err(FsError::Exists);
        }
        let target_path = Self::child_path(target_parent_node, target_name);
        Self::validate_visible_object_path(&target_path)?;
        let source_object = path_to_object(&source.path).map_err(|_| FsError::Invalid)?;
        let target_object = path_to_object(&target_path).map_err(|_| FsError::Invalid)?;
        if source_object.backend != target_object.backend
            || source_object.bucket != target_object.bucket
        {
            return Err(FsError::CrossDevice);
        }
        Ok(HardLinkPlan {
            source_ino,
            source_path: source.path.clone(),
            source_size: source.size,
            buffered_contents: g
                .dirty
                .iter()
                .filter(|(_, dirty)| dirty.ino == source_ino)
                .max_by_key(|(fh, _)| *fh)
                .map(|(_, dirty)| dirty.buf.clone()),
            target_parent,
            target_name: target_name.to_string(),
            target_path,
            backend_object: source.kind.has_backend_object(),
        })
    }

    /// Add a hard-link dentry after the destination object is written through.
    pub fn commit_hard_link(&self, plan: &HardLinkPlan) -> Result<Attr, FsError> {
        let mut g = self.inner.lock_recover();
        let source = g.nodes.get(&plan.source_ino).ok_or(FsError::NotFound)?;
        if source.kind == FileKind::Directory || source.path != plan.source_path {
            return Err(FsError::Invalid);
        }
        if g.index
            .contains_key(&(plan.target_parent, plan.target_name.clone()))
        {
            return Err(FsError::Exists);
        }
        let target_parent = g.nodes.get(&plan.target_parent).ok_or(FsError::NotFound)?;
        if target_parent.kind != FileKind::Directory
            || Self::child_path(target_parent, &plan.target_name) != plan.target_path
        {
            return Err(FsError::NotDir);
        }
        g.index.insert(
            (plan.target_parent, plan.target_name.clone()),
            plan.source_ino,
        );
        g.nodes
            .get_mut(&plan.target_parent)
            .ok_or(FsError::NotFound)?
            .children
            .push(plan.source_ino);
        let now = SystemTime::now();
        Self::mark_directory_changed(&mut g, plan.target_parent, now);
        g.nodes
            .get_mut(&plan.source_ino)
            .ok_or(FsError::NotFound)?
            .ctime = now;
        Ok(Self::attr_of(
            &g,
            g.nodes.get(&plan.source_ino).ok_or(FsError::NotFound)?,
        ))
    }

    /// Validate and describe a regular-file rename without mutating namespace.
    pub fn file_rename_plan(
        &self,
        source_parent: u64,
        source_name: &str,
        target_parent: u64,
        target_name: &str,
    ) -> Result<FileRenamePlan, FsError> {
        Self::validate_component(source_name)?;
        Self::validate_component(target_name)?;
        let g = self.inner.lock_recover();
        let source_parent_node = g.nodes.get(&source_parent).ok_or(FsError::NotFound)?;
        let target_parent_node = g.nodes.get(&target_parent).ok_or(FsError::NotFound)?;
        if source_parent_node.kind != FileKind::Directory
            || target_parent_node.kind != FileKind::Directory
        {
            return Err(FsError::NotDir);
        }
        let source_ino = *g
            .index
            .get(&(source_parent, source_name.to_string()))
            .ok_or(FsError::NotFound)?;
        let source = g.nodes.get(&source_ino).ok_or(FsError::NotFound)?;
        if source.kind == FileKind::Directory {
            return Err(FsError::IsDir);
        }
        let source_path = Self::child_path(source_parent_node, source_name);
        let mut target_path = Self::child_path(target_parent_node, target_name);
        Self::validate_visible_object_path(&target_path)?;
        let target = match g.index.get(&(target_parent, target_name.to_string())) {
            Some(&target_ino) if target_ino == source_ino => {
                target_path.clone_from(&source_path);
                None
            }
            Some(&target_ino) => {
                let node = g.nodes.get(&target_ino).ok_or(FsError::NotFound)?;
                if node.kind == FileKind::Directory {
                    return Err(FsError::IsDir);
                }
                Some((target_ino, node.size))
            }
            None => None,
        };
        let target_backend_object = target
            .and_then(|(target_ino, _)| g.nodes.get(&target_ino))
            .is_some_and(|node| node.kind.has_backend_object());
        Ok(FileRenamePlan {
            source_ino,
            source_parent,
            source_name: source_name.to_string(),
            source_path,
            source_size: source.size,
            source_backend_object: source.kind.has_backend_object(),
            target_parent,
            target_name: target_name.to_string(),
            target_path,
            target,
            target_backend_object,
        })
    }

    /// Commit a regular-file rename after backend copy/delete succeeds.
    pub fn commit_file_rename(&self, plan: &FileRenamePlan) -> Result<(), FsError> {
        if plan.source_path == plan.target_path {
            return Ok(());
        }
        let mut g = self.inner.lock_recover();
        let current_source = g
            .index
            .get(&(plan.source_parent, plan.source_name.clone()))
            .copied();
        if current_source != Some(plan.source_ino) {
            return Err(FsError::NotFound);
        }
        let now = SystemTime::now();
        if let Some((target_ino, _)) = plan.target {
            let current_target = g
                .index
                .get(&(plan.target_parent, plan.target_name.clone()))
                .copied();
            if current_target != Some(target_ino) {
                return Err(FsError::Exists);
            }
            g.index
                .remove(&(plan.target_parent, plan.target_name.clone()));
            Self::remove_child_once(&mut g, plan.target_parent, target_ino);
            let remaining_target = Self::inode_dentries_locked(&g, target_ino);
            if let Some((parent, name, path)) = remaining_target.first() {
                let target = g.nodes.get_mut(&target_ino).ok_or(FsError::NotFound)?;
                if target.path == plan.target_path {
                    target.parent = Some(*parent);
                    target.name.clone_from(name);
                    target.path.clone_from(path);
                }
                target.ctime = now;
            } else {
                g.nodes.remove(&target_ino);
            }
        } else if g
            .index
            .contains_key(&(plan.target_parent, plan.target_name.clone()))
        {
            return Err(FsError::Exists);
        }

        g.index
            .remove(&(plan.source_parent, plan.source_name.clone()));
        Self::remove_child_once(&mut g, plan.source_parent, plan.source_ino);
        g.index.insert(
            (plan.target_parent, plan.target_name.clone()),
            plan.source_ino,
        );
        g.nodes
            .get_mut(&plan.target_parent)
            .ok_or(FsError::NotFound)?
            .children
            .push(plan.source_ino);
        let source = g.nodes.get_mut(&plan.source_ino).ok_or(FsError::NotFound)?;
        if source.path == plan.source_path {
            source.parent = Some(plan.target_parent);
            source.name.clone_from(&plan.target_name);
            source.path.clone_from(&plan.target_path);
        }
        source.ctime = now;
        Self::mark_directory_changed(&mut g, plan.source_parent, now);
        Self::mark_directory_changed(&mut g, plan.target_parent, now);
        Ok(())
    }

    /// Validate and describe a directory-tree rename without mutating namespace.
    pub fn directory_rename_plan(
        &self,
        source_parent: u64,
        source_name: &str,
        target_parent: u64,
        target_name: &str,
    ) -> Result<DirectoryRenamePlan, FsError> {
        Self::validate_component(source_name)?;
        Self::validate_component(target_name)?;
        let g = self.inner.lock_recover();
        let source_parent_node = g.nodes.get(&source_parent).ok_or(FsError::NotFound)?;
        let target_parent_node = g.nodes.get(&target_parent).ok_or(FsError::NotFound)?;
        if source_parent_node.kind != FileKind::Directory
            || target_parent_node.kind != FileKind::Directory
        {
            return Err(FsError::NotDir);
        }
        let source_ino = *g
            .index
            .get(&(source_parent, source_name.to_string()))
            .ok_or(FsError::NotFound)?;
        let source = g.nodes.get(&source_ino).ok_or(FsError::NotFound)?;
        if source.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        let target_path = Self::child_path(target_parent_node, target_name);
        if source.path == target_path {
            return Ok(DirectoryRenamePlan {
                source_ino,
                source_parent,
                source_name: source_name.to_string(),
                source_path: source.path.clone(),
                target_parent,
                target_name: target_name.to_string(),
                target_path,
                target: None,
                entries: Vec::new(),
            });
        }
        let mut ancestor = Some(target_parent);
        while let Some(ino) = ancestor {
            if ino == source_ino {
                return Err(FsError::Invalid);
            }
            ancestor = g.nodes.get(&ino).and_then(|node| node.parent);
        }
        Self::validate_visible_object_path(&target_path)?;
        let target = match g.index.get(&(target_parent, target_name.to_string())) {
            Some(&target_ino) => {
                let node = g.nodes.get(&target_ino).ok_or(FsError::NotFound)?;
                if node.kind != FileKind::Directory {
                    return Err(FsError::NotDir);
                }
                if !node.children.is_empty() {
                    return Err(FsError::NotEmpty);
                }
                Some((target_ino, node.directory_marker))
            }
            None => None,
        };

        let source_path = Self::child_path(source_parent_node, source_name);
        let mut stack = vec![(source_ino, source_path.clone(), target_path.clone())];
        let mut entries = Vec::new();
        while let Some((ino, current_source_path, current_target_path)) = stack.pop() {
            let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
            if node.kind != FileKind::Directory {
                return Err(FsError::Invalid);
            }
            if node.directory_marker {
                entries.push(DirectoryRenameEntry {
                    source_path: format!("{current_source_path}/"),
                    target_path: format!("{current_target_path}/"),
                    size: 0,
                    marker: true,
                });
            }
            let mut children: Vec<(String, u64)> = g
                .index
                .iter()
                .filter(|((parent, _), _)| *parent == ino)
                .map(|((_, name), child_ino)| (name.clone(), *child_ino))
                .collect();
            children.sort_by(|left, right| left.0.cmp(&right.0));
            for (name, child_ino) in children.into_iter().rev() {
                let child = g.nodes.get(&child_ino).ok_or(FsError::NotFound)?;
                let child_source_path = format!("{current_source_path}/{name}");
                let child_target_path = format!("{current_target_path}/{name}");
                if child.kind == FileKind::Directory {
                    stack.push((child_ino, child_source_path, child_target_path));
                } else if child.kind.has_backend_object() {
                    entries.push(DirectoryRenameEntry {
                        source_path: child_source_path,
                        target_path: child_target_path,
                        size: child.size,
                        marker: false,
                    });
                }
            }
        }
        entries.sort_by(|left, right| left.source_path.cmp(&right.source_path));
        Ok(DirectoryRenamePlan {
            source_ino,
            source_parent,
            source_name: source_name.to_string(),
            source_path: source.path.clone(),
            target_parent,
            target_name: target_name.to_string(),
            target_path,
            target,
            entries,
        })
    }

    /// Commit a directory-tree rename after all backend moves succeed.
    pub fn commit_directory_rename(&self, plan: &DirectoryRenamePlan) -> Result<(), FsError> {
        if plan.source_path == plan.target_path {
            return Ok(());
        }
        let mut g = self.inner.lock_recover();
        if g.index
            .get(&(plan.source_parent, plan.source_name.clone()))
            .copied()
            != Some(plan.source_ino)
        {
            return Err(FsError::NotFound);
        }
        if let Some((target_ino, _)) = plan.target {
            if g.index
                .get(&(plan.target_parent, plan.target_name.clone()))
                .copied()
                != Some(target_ino)
            {
                return Err(FsError::Exists);
            }
            let target = g.nodes.get(&target_ino).ok_or(FsError::NotFound)?;
            if target.kind != FileKind::Directory || !target.children.is_empty() {
                return Err(FsError::NotEmpty);
            }
            g.index
                .remove(&(plan.target_parent, plan.target_name.clone()));
            if let Some(parent) = g.nodes.get_mut(&plan.target_parent) {
                parent.children.retain(|child| *child != target_ino);
            }
            g.nodes.remove(&target_ino);
        } else if g
            .index
            .contains_key(&(plan.target_parent, plan.target_name.clone()))
        {
            return Err(FsError::Exists);
        }

        g.index
            .remove(&(plan.source_parent, plan.source_name.clone()));
        if let Some(parent) = g.nodes.get_mut(&plan.source_parent) {
            parent.children.retain(|child| *child != plan.source_ino);
        }
        let source_prefix = format!("{}/", plan.source_path);
        for node in g.nodes.values_mut() {
            if node.path == plan.source_path {
                node.path.clone_from(&plan.target_path);
            } else if let Some(suffix) = node.path.strip_prefix(&source_prefix) {
                node.path = format!("{}/{suffix}", plan.target_path);
            }
        }
        let source = g.nodes.get_mut(&plan.source_ino).ok_or(FsError::NotFound)?;
        source.parent = Some(plan.target_parent);
        source.name.clone_from(&plan.target_name);
        let now = SystemTime::now();
        source.ctime = now;
        g.index.insert(
            (plan.target_parent, plan.target_name.clone()),
            plan.source_ino,
        );
        g.nodes
            .get_mut(&plan.target_parent)
            .ok_or(FsError::NotFound)?
            .children
            .push(plan.source_ino);
        Self::mark_directory_changed(&mut g, plan.source_parent, now);
        Self::mark_directory_changed(&mut g, plan.target_parent, now);
        Ok(())
    }

    /// Validate a new symbolic link and return its backend object path.
    pub fn new_symlink_path(
        &self,
        parent_ino: u64,
        name: &str,
        target: &[u8],
    ) -> Result<String, FsError> {
        Self::validate_component(name)?;
        if target.len() > MAX_SYMLINK_TARGET_BYTES {
            return Err(FsError::NameTooLong);
        }
        let g = self.inner.lock_recover();
        let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
        if parent.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        if g.index.contains_key(&(parent_ino, name.to_string())) {
            return Err(FsError::Exists);
        }
        let path = Self::child_path(parent, name);
        Self::validate_visible_object_path(&path)?;
        Ok(path)
    }

    /// Insert a symbolic-link inode after its target bytes are written through.
    pub fn symlink(&self, parent_ino: u64, name: &str, target: Vec<u8>) -> Result<Attr, FsError> {
        self.symlink_with_owner(parent_ino, name, target, 0, 0)
    }

    /// Insert a symbolic link owned by the requesting credentials.
    pub fn symlink_with_owner(
        &self,
        parent_ino: u64,
        name: &str,
        target: Vec<u8>,
        uid: u32,
        gid: u32,
    ) -> Result<Attr, FsError> {
        let path = self.new_symlink_path(parent_ino, name, &target)?;
        let mut g = self.inner.lock_recover();
        if g.index.contains_key(&(parent_ino, name.to_string())) {
            return Err(FsError::Exists);
        }
        let ino = g.next_ino;
        g.next_ino += 1;
        let now = SystemTime::now();
        let (parent_perm, parent_gid) = {
            let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
            (parent.perm, parent.gid)
        };
        let child_gid = if parent_perm & 0o2000 != 0 {
            parent_gid
        } else {
            gid
        };
        let node = Node {
            ino,
            parent: Some(parent_ino),
            name: name.to_string(),
            kind: FileKind::Symlink,
            size: target.len() as u64,
            children: Vec::new(),
            path,
            directory_marker: false,
            symlink_target: Some(target),
            linked: true,
            perm: 0o777,
            rdev: 0,
            uid,
            gid: child_gid,
            atime: now,
            mtime: now,
            ctime: now,
        };
        g.nodes.insert(ino, node);
        g.index.insert((parent_ino, name.to_string()), ino);
        g.nodes.get_mut(&parent_ino).unwrap().children.push(ino);
        Self::mark_directory_changed(&mut g, parent_ino, now);
        Ok(Self::attr_of(&g, g.nodes.get(&ino).unwrap()))
    }

    /// Return the raw target bytes for a symbolic-link inode.
    pub fn readlink(&self, ino: u64) -> Result<Vec<u8>, FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        match &node.symlink_target {
            Some(target) if node.kind == FileKind::Symlink => Ok(target.clone()),
            _ => Err(FsError::Invalid),
        }
    }

    /// Validate a new directory and return its trailing-slash marker path.
    pub fn new_directory_marker_path(
        &self,
        parent_ino: u64,
        name: &str,
    ) -> Result<String, FsError> {
        Self::validate_component(name)?;
        let g = self.inner.lock_recover();
        let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
        if parent.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        if g.index.contains_key(&(parent_ino, name.to_string())) {
            return Err(FsError::Exists);
        }
        let path = Self::child_path(parent, name);
        Self::validate_visible_object_path(&path)?;
        Ok(format!("{path}/"))
    }

    /// Insert a new explicit directory after its marker has been committed.
    pub fn mkdir(&self, parent_ino: u64, name: &str) -> Result<Attr, FsError> {
        self.mkdir_with_metadata(parent_ino, name, 0o777, 0, 0, 0)
    }

    /// Insert a directory with request ownership, mode, and umask metadata.
    pub fn mkdir_with_metadata(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Attr, FsError> {
        Self::validate_component(name)?;
        let mut g = self.inner.lock_recover();
        let (path, parent_perm, parent_gid) = {
            let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
            if parent.kind != FileKind::Directory {
                return Err(FsError::NotDir);
            }
            (Self::child_path(parent, name), parent.perm, parent.gid)
        };
        if g.index.contains_key(&(parent_ino, name.to_string())) {
            return Err(FsError::Exists);
        }
        Self::validate_visible_object_path(&path)?;
        let ino = g.next_ino;
        g.next_ino += 1;
        let now = SystemTime::now();
        let child_gid = if parent_perm & 0o2000 != 0 {
            parent_gid
        } else {
            gid
        };
        let inherited_setgid = parent_perm & 0o2000;
        let node = Node {
            ino,
            parent: Some(parent_ino),
            name: name.to_string(),
            kind: FileKind::Directory,
            size: 0,
            children: Vec::new(),
            path,
            directory_marker: true,
            symlink_target: None,
            linked: true,
            perm: (((mode & !umask) & 0o7777) as u16) | inherited_setgid,
            rdev: 0,
            uid,
            gid: child_gid,
            atime: now,
            mtime: now,
            ctime: now,
        };
        g.nodes.insert(ino, node);
        g.index.insert((parent_ino, name.to_string()), ino);
        g.nodes.get_mut(&parent_ino).unwrap().children.push(ino);
        Self::mark_directory_changed(&mut g, parent_ino, now);
        Ok(Self::attr_of(&g, g.nodes.get(&ino).unwrap()))
    }

    /// Return the marker to delete before removing an empty directory.
    pub fn rmdir_marker_path(
        &self,
        parent_ino: u64,
        name: &str,
    ) -> Result<Option<String>, FsError> {
        Self::validate_component(name)?;
        let g = self.inner.lock_recover();
        let ino = *g
            .index
            .get(&(parent_ino, name.to_string()))
            .ok_or(FsError::NotFound)?;
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        if !node.children.is_empty() {
            return Err(FsError::NotEmpty);
        }
        Ok(node.directory_marker.then(|| format!("{}/", node.path)))
    }

    /// Remove an empty directory from the namespace.
    pub fn rmdir(&self, parent_ino: u64, name: &str) -> Result<(), FsError> {
        Self::validate_component(name)?;
        let mut g = self.inner.lock_recover();
        let ino = *g
            .index
            .get(&(parent_ino, name.to_string()))
            .ok_or(FsError::NotFound)?;
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        if !node.children.is_empty() {
            return Err(FsError::NotEmpty);
        }
        g.nodes.remove(&ino);
        g.index.remove(&(parent_ino, name.to_string()));
        if let Some(parent) = g.nodes.get_mut(&parent_ino) {
            parent.children.retain(|child| *child != ino);
        }
        Self::mark_directory_changed(&mut g, parent_ino, SystemTime::now());
        Ok(())
    }

    /// Validate and describe an unlink without mutating the namespace.
    ///
    /// Files with open handles move to a mount-scoped internal backend key so
    /// descriptors can continue to read, write, and fsync after the visible name
    /// is removed. The final release deletes that orphan object.
    pub fn unlink_plan(&self, parent_ino: u64, name: &str) -> Result<UnlinkPlan, FsError> {
        Self::validate_component(name)?;
        let g = self.inner.lock_recover();
        let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
        if parent.kind != FileKind::Directory {
            return Err(FsError::NotDir);
        }
        let ino = *g
            .index
            .get(&(parent_ino, name.to_string()))
            .ok_or(FsError::NotFound)?;
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind == FileKind::Directory {
            return Err(FsError::IsDir);
        }
        let source_path = Self::child_path(parent, name);
        let link_count = g
            .index
            .values()
            .filter(|candidate| **candidate == ino)
            .count();
        let has_open_handles = g.handles.values().any(|handle| handle.ino == ino);
        let orphan_path = (link_count == 1 && has_open_handles)
            .then(|| self.orphan_path(&source_path, ino))
            .transpose()?;
        let buffered_contents = g
            .dirty
            .iter()
            .filter(|(_, dirty)| dirty.ino == ino)
            .max_by_key(|(fh, _)| *fh)
            .map(|(_, dirty)| dirty.buf.clone());
        Ok(UnlinkPlan {
            ino,
            parent: parent_ino,
            name: name.to_string(),
            source_path,
            source_size: node.size,
            orphan_path,
            buffered_contents,
            backend_object: node.kind.has_backend_object(),
        })
    }

    /// Commit an unlink after the backend delete or orphan move succeeds.
    pub fn commit_unlink(&self, plan: &UnlinkPlan) -> Result<(), FsError> {
        let mut g = self.inner.lock_recover();
        if g.index.get(&(plan.parent, plan.name.clone())).copied() != Some(plan.ino) {
            return Err(FsError::NotFound);
        }
        let node = g.nodes.get(&plan.ino).ok_or(FsError::NotFound)?;
        if node.kind == FileKind::Directory {
            return Err(FsError::Invalid);
        }
        let parent = g.nodes.get(&plan.parent).ok_or(FsError::NotFound)?;
        if Self::child_path(parent, &plan.name) != plan.source_path {
            return Err(FsError::Invalid);
        }
        let link_count = g
            .index
            .values()
            .filter(|candidate| **candidate == plan.ino)
            .count();
        let has_open_handles = g.handles.values().any(|handle| handle.ino == plan.ino);
        if (link_count == 1 && has_open_handles) != plan.orphan_path.is_some() {
            return Err(FsError::Invalid);
        }

        g.index.remove(&(plan.parent, plan.name.clone()));
        Self::remove_child_once(&mut g, plan.parent, plan.ino);
        let now = SystemTime::now();
        Self::mark_directory_changed(&mut g, plan.parent, now);
        let remaining = Self::inode_dentries_locked(&g, plan.ino);
        if let Some((parent, name, path)) = remaining.first() {
            let node = g.nodes.get_mut(&plan.ino).ok_or(FsError::NotFound)?;
            if node.path == plan.source_path {
                node.parent = Some(*parent);
                node.name.clone_from(name);
                node.path.clone_from(path);
            }
            node.linked = true;
            node.ctime = now;
        } else if let Some(orphan_path) = &plan.orphan_path {
            let node = g.nodes.get_mut(&plan.ino).ok_or(FsError::NotFound)?;
            node.parent = None;
            node.path.clone_from(orphan_path);
            node.linked = false;
            node.ctime = now;
        } else {
            g.nodes.remove(&plan.ino);
        }
        Ok(())
    }

    /// Remove a file from the in-memory namespace without backend I/O.
    ///
    /// The mount adapter uses [`unlink_plan`](Self::unlink_plan) and
    /// [`commit_unlink`](Self::commit_unlink) around synchronous backend work.
    pub fn unlink(&self, parent_ino: u64, name: &str) -> Result<String, FsError> {
        let plan = self.unlink_plan(parent_ino, name)?;
        let path = plan.source_path.clone();
        self.commit_unlink(&plan)?;
        Ok(path)
    }

    /// Build the ancestry name chain (root-excluded) of `ino`, e.g.
    /// `["s3", "bucket", "data"]`, so a child path can be formed on `create`.
    fn ancestry(g: &Inner, ino: u64) -> Vec<String> {
        let mut names = Vec::new();
        let mut cur = ino;
        while cur != ROOT_INO {
            let node = match g.nodes.get(&cur) {
                Some(n) => n,
                None => break,
            };
            names.push(node.name.clone());
            match node.parent {
                Some(p) => cur = p,
                None => break,
            }
        }
        names.reverse();
        names
    }

    fn validate_component(name: &str) -> Result<(), FsError> {
        Self::validate_name_length(name)?;
        if name.is_empty() || name == "." || name == ".." || name.contains('/') {
            Err(FsError::Invalid)
        } else {
            Ok(())
        }
    }

    fn validate_name_length(name: &str) -> Result<(), FsError> {
        if name.len() > 255 {
            Err(FsError::NameTooLong)
        } else {
            Ok(())
        }
    }

    fn child_path(parent: &Node, name: &str) -> String {
        if parent.path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent.path, name)
        }
    }

    fn inode_paths_locked(g: &Inner, ino: u64) -> Result<Vec<String>, FsError> {
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind == FileKind::Directory {
            return Err(FsError::IsDir);
        }
        let mut paths: Vec<String> = Self::inode_dentries_locked(g, ino)
            .into_iter()
            .map(|(_, _, path)| path)
            .collect();
        paths.sort();
        paths.dedup();
        if paths.is_empty() && !node.linked {
            paths.push(node.path.clone());
        }
        Ok(paths)
    }

    fn inode_dentries_locked(g: &Inner, ino: u64) -> Vec<(u64, String, String)> {
        let mut entries: Vec<(u64, String, String)> = g
            .index
            .iter()
            .filter(|(_, child_ino)| **child_ino == ino)
            .filter_map(|((parent_ino, name), _)| {
                g.nodes
                    .get(parent_ino)
                    .map(|parent| (*parent_ino, name.clone(), Self::child_path(parent, name)))
            })
            .collect();
        entries.sort_by(|left, right| left.2.cmp(&right.2));
        entries
    }

    fn remove_child_once(g: &mut Inner, parent_ino: u64, child_ino: u64) {
        if let Some(parent) = g.nodes.get_mut(&parent_ino) {
            if let Some(position) = parent
                .children
                .iter()
                .position(|candidate| *candidate == child_ino)
            {
                parent.children.remove(position);
            }
        }
    }

    fn mark_directory_changed(g: &mut Inner, ino: u64, now: SystemTime) {
        if let Some(node) = g.nodes.get_mut(&ino) {
            node.mtime = now;
            node.ctime = now;
        }
    }

    fn access_allowed(node: &Node, uid: u32, gid: u32, mask: u16) -> bool {
        Self::access_allowed_with_groups(node, uid, gid, &[], mask)
    }

    fn access_allowed_with_groups(
        node: &Node,
        uid: u32,
        gid: u32,
        groups: &[u32],
        mask: u16,
    ) -> bool {
        if mask == 0 {
            return true;
        }
        if uid == 0 {
            return mask & 0o1 == 0 || node.kind == FileKind::Directory || node.perm & 0o111 != 0;
        }
        let bits = if uid == node.uid {
            (node.perm >> 6) & 0o7
        } else if gid == node.gid || groups.contains(&node.gid) {
            (node.perm >> 3) & 0o7
        } else {
            node.perm & 0o7
        };
        bits & mask == mask
    }

    fn orphan_path(&self, source_path: &str, ino: u64) -> Result<String, FsError> {
        let object = path_to_object(source_path).map_err(|_| FsError::Invalid)?;
        Ok(format!(
            "{}/{}/{INTERNAL_OBJECT_PREFIX}/unlinked/{}/{ino}",
            object.backend.prefix(),
            object.bucket,
            self.orphan_namespace
        ))
    }

    fn validate_visible_object_path(path: &str) -> Result<(), FsError> {
        let object = path_to_object(path).map_err(|_| FsError::Invalid)?;
        if object.object_path.split('/').next() == Some(INTERNAL_OBJECT_PREFIX) {
            return Err(FsError::Invalid);
        }
        Ok(())
    }

    fn is_internal_object_path(path: &str) -> bool {
        path_to_object(path).is_ok_and(|object| {
            object.object_path.split('/').next() == Some(INTERNAL_OBJECT_PREFIX)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fs() -> ReadOnlyFs {
        let fs = ReadOnlyFs::new();
        fs.insert_object("/s3/bucket/data/a.bin", 1000);
        fs.insert_object("/s3/bucket/data/b.bin", 500);
        fs.insert_object("/gcs/other/c.bin", 42);
        fs
    }

    fn data_dir(fs: &ReadOnlyFs) -> Attr {
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        fs.lookup(bucket.ino, "data").unwrap()
    }

    #[test]
    fn lookup_and_getattr_walk_the_tree() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        assert_eq!(s3.kind, FileKind::Directory);
        assert_eq!(s3.perm, 0o755);
        assert_eq!(fs.getattr(ROOT_INO).unwrap().nlink, 4);
        assert_eq!(s3.nlink, 3);
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let data = fs.lookup(bucket.ino, "data").unwrap();
        assert_eq!(bucket.nlink, 3);
        assert_eq!(data.nlink, 2);
        let a = fs.lookup(data.ino, "a.bin").unwrap();
        assert_eq!(a.kind, FileKind::File);
        assert_eq!(a.size, 1000);
        assert_eq!(a.perm, 0o644);
        assert_eq!(fs.getattr(a.ino).unwrap(), a);

        assert_eq!(fs.lookup(ROOT_INO, "nope"), Err(FsError::NotFound));
    }

    #[test]
    fn explicit_timestamp_updates_preserve_omitted_fields_and_advance_ctime() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        let original = fs.getattr(file.ino).unwrap();
        let atime = UNIX_EPOCH + std::time::Duration::new(123, 456);
        let mtime = UNIX_EPOCH + std::time::Duration::new(789, 123);

        let updated = fs.set_times(file.ino, Some(atime), Some(mtime)).unwrap();
        assert_eq!(updated.atime, atime);
        assert_eq!(updated.mtime, mtime);
        assert!(updated.ctime >= original.ctime);

        let later_mtime = UNIX_EPOCH + std::time::Duration::new(999, 321);
        let omitted = fs.set_times(file.ino, None, Some(later_mtime)).unwrap();
        assert_eq!(omitted.atime, atime);
        assert_eq!(omitted.mtime, later_mtime);
        assert!(omitted.ctime >= updated.ctime);

        let unchanged = fs.set_times(file.ino, None, None).unwrap();
        assert_eq!(unchanged, omitted);
    }

    #[test]
    fn readdir_lists_sorted_children() {
        let fs = fs();
        let root = fs.readdir(ROOT_INO).unwrap();
        let names: Vec<&str> = root.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["gcs", "s3"]);

        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let data = fs.lookup(bucket.ino, "data").unwrap();
        let data_entries = fs.readdir(data.ino).unwrap();
        let files: Vec<&str> = data_entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(files, vec!["a.bin", "b.bin"]);
    }

    #[test]
    fn open_file_meta_release_flow() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let data = fs.lookup(bucket.ino, "data").unwrap();
        let a = fs.lookup(data.ino, "a.bin").unwrap();

        // Cannot open a directory.
        assert_eq!(fs.open(data.ino), Err(FsError::IsDir));

        let fh = fs.open(a.ino).unwrap();
        // file_meta yields the object path + size for the open handle.
        let (path, size) = fs.file_meta(fh).unwrap();
        assert_eq!(path, "s3/bucket/data/a.bin");
        assert_eq!(size, 1000);

        fs.release(fh).unwrap();
        // Handle no longer valid.
        assert_eq!(fs.file_meta(fh), Err(FsError::BadHandle));
        assert_eq!(fs.release(fh), Err(FsError::BadHandle));
    }

    #[test]
    fn truncating_open_updates_size_mtime_and_ctime() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        let before = fs.getattr(file.ino).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1));

        let fh = fs
            .open_with_options_and_truncate(
                file.ino,
                OpenOptions {
                    read: false,
                    write: true,
                    append: false,
                },
                Some(Vec::new()),
                true,
            )
            .unwrap();
        let after = fs.getattr(file.ino).unwrap();
        assert_eq!(after.size, 0);
        assert!(after.mtime > before.mtime);
        assert!(after.ctime > before.ctime);
        fs.release(fh).unwrap();
    }

    #[test]
    fn populate_from_listing_builds_tree_and_readdir() {
        let fs = ReadOnlyFs::new();
        let n = fs.populate_from_listing([
            ("s3/bkt/dir/a.bin", 10u64),
            ("s3/bkt/dir/b.bin", 20u64),
            ("s3/bkt/other.bin", 5u64),
        ]);
        assert_eq!(n, 3);

        // Root shows the single backend dir.
        let root: Vec<String> = fs
            .readdir(ROOT_INO)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(root, vec!["s3"]);

        // Walk into the synthesized directories.
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bkt = fs.lookup(s3.ino, "bkt").unwrap();
        let bkt_children: Vec<String> = fs
            .readdir(bkt.ino)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(bkt_children, vec!["dir", "other.bin"]);

        let dir = fs.lookup(bkt.ino, "dir").unwrap();
        let a = fs.lookup(dir.ino, "a.bin").unwrap();
        assert_eq!(a.kind, FileKind::File);
        assert_eq!(a.size, 10);

        // Idempotent: re-inserting keeps the same inode / shape.
        let a_ino = a.ino;
        fs.populate_from_listing([("s3/bkt/dir/a.bin", 10u64)]);
        assert_eq!(fs.lookup(dir.ino, "a.bin").unwrap().ino, a_ino);
    }

    #[test]
    fn listed_nodes_use_the_configured_mount_owner() {
        let fs = ReadOnlyFs::new_with_owner(1000, 100);
        fs.populate_from_listing([("s3/bucket/file.bin", 4), ("s3/bucket/empty/", 0)]);

        let root = fs.getattr(ROOT_INO).unwrap();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let file = fs.lookup(bucket.ino, "file.bin").unwrap();
        let empty = fs.lookup(bucket.ino, "empty").unwrap();

        for attr in [root, s3, bucket, file, empty] {
            assert_eq!(attr.uid, 1000);
            assert_eq!(attr.gid, 100);
        }
    }

    #[test]
    fn directory_markers_populate_hidden_empty_directories() {
        let fs = ReadOnlyFs::new();
        let count =
            fs.populate_from_listing([("s3/bkt/empty/", 0u64), ("s3/bkt/nonempty/file.bin", 5u64)]);
        assert_eq!(count, 2);

        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bkt").unwrap();
        let entries = fs.readdir(bucket.ino).unwrap();
        let names: Vec<&str> = entries.iter().map(|entry| entry.name.as_str()).collect();
        assert_eq!(names, vec!["empty", "nonempty"]);

        let empty = fs.lookup(bucket.ino, "empty").unwrap();
        assert_eq!(empty.kind, FileKind::Directory);
        assert!(fs.readdir(empty.ino).unwrap().is_empty());
        assert_eq!(fs.parent_ino(empty.ino).unwrap(), bucket.ino);
        assert_eq!(
            fs.rmdir_marker_path(bucket.ino, "empty").unwrap(),
            Some("s3/bkt/empty/".to_string())
        );
    }

    #[test]
    fn internal_orphan_objects_are_hidden_and_reserved() {
        let fs = ReadOnlyFs::new();
        let count = fs.populate_from_listing([
            ("s3/bkt/visible.bin", 1u64),
            ("s3/bkt/.__talon_internal/unlinked/stale/7", 5u64),
        ]);
        assert_eq!(count, 1);

        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bkt").unwrap();
        assert_eq!(
            fs.lookup(bucket.ino, INTERNAL_OBJECT_PREFIX),
            Err(FsError::NotFound)
        );
        assert_eq!(
            fs.create(bucket.ino, INTERNAL_OBJECT_PREFIX),
            Err(FsError::Invalid)
        );
        assert_eq!(
            fs.mkdir(bucket.ino, INTERNAL_OBJECT_PREFIX),
            Err(FsError::Invalid)
        );
    }

    #[test]
    fn symlink_tracks_target_bytes_and_moves_as_a_nondirectory_object() {
        let fs = fs();
        let parent = data_dir(&fs);
        let target = b"a.bin".to_vec();
        assert_eq!(
            fs.new_symlink_path(parent.ino, "alias", &target).unwrap(),
            "s3/bucket/data/alias"
        );

        let link = fs.symlink(parent.ino, "alias", target.clone()).unwrap();
        assert_eq!(link.kind, FileKind::Symlink);
        assert_eq!(link.size, target.len() as u64);
        assert_eq!(link.perm, 0o777);
        assert_eq!(fs.readlink(link.ino).unwrap(), target);
        assert_eq!(
            fs.symlink(parent.ino, "alias", b"other".to_vec()),
            Err(FsError::Exists)
        );

        let plan = fs
            .file_rename_plan(parent.ino, "alias", parent.ino, "moved")
            .unwrap();
        fs.commit_file_rename(&plan).unwrap();
        assert_eq!(fs.lookup(parent.ino, "alias"), Err(FsError::NotFound));
        assert_eq!(fs.lookup(parent.ino, "moved").unwrap().ino, link.ino);
        assert_eq!(fs.readlink(link.ino).unwrap(), b"a.bin");

        fs.unlink(parent.ino, "moved").unwrap();
        assert_eq!(fs.getattr(link.ino), Err(FsError::NotFound));
    }

    #[test]
    fn symlink_rejects_long_targets_and_names() {
        let fs = fs();
        let parent = data_dir(&fs);
        assert_eq!(
            fs.new_symlink_path(parent.ino, "alias", &vec![b'x'; 4096]),
            Err(FsError::NameTooLong)
        );
        assert_eq!(
            fs.new_symlink_path(parent.ino, &"x".repeat(256), b"target"),
            Err(FsError::NameTooLong)
        );
    }

    #[test]
    fn hard_links_share_inode_link_count_and_backend_paths() {
        let fs = fs();
        let parent = data_dir(&fs);
        let source = fs.lookup(parent.ino, "a.bin").unwrap();
        let plan = fs
            .hard_link_plan(source.ino, parent.ino, "linked.bin")
            .unwrap();
        assert_eq!(plan.source_path, "s3/bucket/data/a.bin");
        assert_eq!(plan.target_path, "s3/bucket/data/linked.bin");

        let linked = fs.commit_hard_link(&plan).unwrap();
        assert_eq!(linked.ino, source.ino);
        assert_eq!(linked.nlink, 2);
        assert_eq!(fs.lookup(parent.ino, "linked.bin").unwrap().ino, source.ino);
        assert_eq!(
            fs.inode_paths(source.ino).unwrap(),
            vec![
                "s3/bucket/data/a.bin".to_string(),
                "s3/bucket/data/linked.bin".to_string(),
            ]
        );
        let names: Vec<String> = fs
            .readdir(parent.ino)
            .unwrap()
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        assert!(names.contains(&"a.bin".to_string()));
        assert!(names.contains(&"linked.bin".to_string()));

        let same_inode = fs
            .file_rename_plan(parent.ino, "a.bin", parent.ino, "linked.bin")
            .unwrap();
        assert_eq!(same_inode.source_path, same_inode.target_path);
        fs.commit_file_rename(&same_inode).unwrap();
        assert_eq!(fs.getattr(source.ino).unwrap().nlink, 2);

        let move_link = fs
            .file_rename_plan(parent.ino, "linked.bin", parent.ino, "moved.bin")
            .unwrap();
        fs.commit_file_rename(&move_link).unwrap();
        assert_eq!(
            fs.inode_paths(source.ino).unwrap(),
            vec![
                "s3/bucket/data/a.bin".to_string(),
                "s3/bucket/data/moved.bin".to_string(),
            ]
        );

        fs.unlink(parent.ino, "a.bin").unwrap();
        assert_eq!(fs.lookup(parent.ino, "a.bin"), Err(FsError::NotFound));
        assert_eq!(fs.lookup(parent.ino, "moved.bin").unwrap().nlink, 1);
        assert_eq!(
            fs.inode_paths(source.ino).unwrap(),
            vec!["s3/bucket/data/moved.bin".to_string()]
        );
    }

    #[test]
    fn hard_links_reject_directories_conflicts_and_cross_bucket_targets() {
        let fs = fs();
        let source_parent = data_dir(&fs);
        let source = fs.lookup(source_parent.ino, "a.bin").unwrap();
        assert_eq!(
            fs.hard_link_plan(source.ino, source_parent.ino, "b.bin"),
            Err(FsError::Exists)
        );
        assert_eq!(
            fs.hard_link_plan(source_parent.ino, source_parent.ino, "dir-link"),
            Err(FsError::OperationNotPermitted)
        );

        let gcs = fs.lookup(ROOT_INO, "gcs").unwrap();
        let other = fs.lookup(gcs.ino, "other").unwrap();
        assert_eq!(
            fs.hard_link_plan(source.ino, other.ino, "linked.bin"),
            Err(FsError::CrossDevice)
        );
    }

    #[test]
    fn mknod_creates_regular_and_mount_local_special_nodes() {
        let fs = fs();
        let parent = data_dir(&fs);
        let cases = [
            (MODE_REGULAR, FileKind::File, 0),
            (MODE_NAMED_PIPE, FileKind::NamedPipe, 0),
            (MODE_BLOCK_DEVICE, FileKind::BlockDevice, 0x0102),
            (MODE_CHAR_DEVICE, FileKind::CharDevice, 0x0304),
            (MODE_SOCKET, FileKind::Socket, 0),
        ];

        for (index, (mode, kind, rdev)) in cases.into_iter().enumerate() {
            let name = format!("node-{index}");
            let plan = fs
                .mknod_plan(parent.ino, &name, mode | 0o666, 0o022, rdev, 1000, 100)
                .unwrap();
            assert_eq!(plan.kind, kind);
            assert_eq!(plan.perm, 0o644);
            assert_eq!(plan.kind.has_backend_object(), kind == FileKind::File);
            let attr = fs.commit_mknod(&plan).unwrap();
            assert_eq!(attr.kind, kind);
            assert_eq!(attr.perm, 0o644);
            assert_eq!(
                attr.rdev,
                if matches!(kind, FileKind::BlockDevice | FileKind::CharDevice) {
                    rdev
                } else {
                    0
                }
            );
            assert_eq!(fs.lookup(parent.ino, &name).unwrap(), attr);
        }
    }

    #[test]
    fn create_metadata_applies_credentials_umask_and_setgid_inheritance() {
        let fs = fs();
        let parent = data_dir(&fs);
        fs.set_metadata(
            parent.ino,
            0,
            0,
            &[],
            Some(0o2775),
            None,
            Some(42),
            None,
            None,
            false,
        )
        .unwrap();
        let (file, _) = fs
            .create_with_metadata(
                parent.ino,
                "owned.bin",
                OpenOptions {
                    read: true,
                    write: true,
                    append: false,
                },
                0o666,
                0o027,
                1000,
                100,
            )
            .unwrap();
        assert_eq!(file.perm, 0o640);
        assert_eq!(file.uid, 1000);
        assert_eq!(file.gid, 42);

        let directory = fs
            .mkdir_with_metadata(parent.ino, "owned-dir", 0o777, 0o027, 1000, 100)
            .unwrap();
        assert_eq!(directory.perm, 0o2750);
        assert_eq!(directory.uid, 1000);
        assert_eq!(directory.gid, 42);
    }

    #[test]
    fn chmod_chown_and_access_enforce_owner_rules() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        let owned = fs
            .set_metadata(
                file.ino,
                0,
                0,
                &[],
                Some(0o6754),
                Some(1000),
                Some(100),
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(owned.uid, 1000);
        assert_eq!(owned.gid, 100);
        assert_eq!(owned.perm, 0o754);

        let chmod = fs
            .set_metadata(
                file.ino,
                1000,
                100,
                &[],
                Some(0o6750),
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(chmod.perm, 0o6750);
        assert_eq!(
            fs.set_metadata(
                file.ino,
                2000,
                200,
                &[],
                Some(0o600),
                None,
                None,
                None,
                None,
                false,
            ),
            Err(FsError::OperationNotPermitted)
        );

        let chgrp = fs
            .set_metadata(
                file.ino,
                1000,
                200,
                &[],
                None,
                None,
                Some(200),
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(chgrp.gid, 200);
        assert_eq!(chgrp.perm & 0o6000, 0);
        assert_eq!(
            fs.set_metadata(
                file.ino,
                1000,
                200,
                &[],
                None,
                Some(2000),
                None,
                None,
                None,
                false,
            ),
            Err(FsError::OperationNotPermitted)
        );

        assert_eq!(fs.check_access(file.ino, 1000, 200, 0o6), Ok(()));
        assert_eq!(
            fs.check_access(file.ino, 3000, 300, 0o2),
            Err(FsError::PermissionDenied)
        );
    }

    #[test]
    fn supplementary_groups_allow_chgrp_and_preserve_setgid() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        fs.set_metadata(
            file.ino,
            0,
            0,
            &[],
            Some(0o750),
            Some(1000),
            Some(100),
            None,
            None,
            false,
        )
        .unwrap();

        let chgrp = fs
            .set_metadata(
                file.ino,
                1000,
                200,
                &[100, 300],
                None,
                None,
                Some(300),
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(chgrp.gid, 300);

        let chmod = fs
            .set_metadata(
                file.ino,
                1000,
                200,
                &[300],
                Some(0o2750),
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(chmod.perm, 0o2750);

        fs.set_metadata(
            file.ino,
            1000,
            200,
            &[300],
            Some(0o2670),
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
        let cleared = fs
            .set_metadata(
                file.ino,
                2000,
                200,
                &[300],
                Some(0o670),
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(cleared.perm, 0o670);
    }

    #[test]
    fn mount_local_special_nodes_link_rename_and_unlink_without_backend_paths() {
        let fs = fs();
        let parent = data_dir(&fs);
        let plan = fs
            .mknod_plan(parent.ino, "fifo", MODE_NAMED_PIPE | 0o600, 0, 0, 1000, 100)
            .unwrap();
        let fifo = fs.commit_mknod(&plan).unwrap();

        let link = fs
            .hard_link_plan(fifo.ino, parent.ino, "fifo-link")
            .unwrap();
        assert!(!link.backend_object);
        assert_eq!(fs.commit_hard_link(&link).unwrap().nlink, 2);

        let rename = fs
            .file_rename_plan(parent.ino, "fifo-link", parent.ino, "fifo-moved")
            .unwrap();
        fs.commit_file_rename(&rename).unwrap();
        assert_eq!(
            fs.lookup(parent.ino, "fifo-moved").unwrap().kind,
            FileKind::NamedPipe
        );

        let unlink = fs.unlink_plan(parent.ino, "fifo").unwrap();
        assert!(!unlink.backend_object);
        fs.commit_unlink(&unlink).unwrap();
        assert_eq!(fs.lookup(parent.ino, "fifo"), Err(FsError::NotFound));
        assert_eq!(fs.lookup(parent.ino, "fifo-moved").unwrap().nlink, 1);
    }

    #[test]
    fn mkdir_and_rmdir_enforce_empty_directory_invariants() {
        let fs = fs();
        let parent = data_dir(&fs);
        assert_eq!(
            fs.new_directory_marker_path(parent.ino, "new").unwrap(),
            "s3/bucket/data/new/"
        );
        let directory = fs.mkdir(parent.ino, "new").unwrap();
        assert_eq!(directory.kind, FileKind::Directory);
        assert_eq!(fs.parent_ino(directory.ino).unwrap(), parent.ino);
        assert_eq!(fs.mkdir(parent.ino, "new"), Err(FsError::Exists));

        let (_, fh) = fs.create(directory.ino, "child.bin").unwrap();
        fs.release(fh).unwrap();
        assert_eq!(
            fs.rmdir_marker_path(parent.ino, "new"),
            Err(FsError::NotEmpty)
        );
        assert_eq!(fs.rmdir(parent.ino, "new"), Err(FsError::NotEmpty));

        fs.unlink(directory.ino, "child.bin").unwrap();
        assert_eq!(
            fs.rmdir_marker_path(parent.ino, "new").unwrap(),
            Some("s3/bucket/data/new/".to_string())
        );
        fs.rmdir(parent.ino, "new").unwrap();
        assert_eq!(fs.lookup(parent.ino, "new"), Err(FsError::NotFound));
    }

    #[test]
    fn directory_operations_reject_names_over_name_max() {
        let fs = fs();
        let parent = data_dir(&fs);
        let name = "x".repeat(256);

        assert_eq!(
            fs.new_directory_marker_path(parent.ino, &name),
            Err(FsError::NameTooLong)
        );
        assert_eq!(fs.lookup(parent.ino, &name), Err(FsError::NameTooLong));
        assert_eq!(fs.mkdir(parent.ino, &name), Err(FsError::NameTooLong));
        assert_eq!(
            fs.rmdir_marker_path(parent.ino, &name),
            Err(FsError::NameTooLong)
        );
        assert_eq!(fs.rmdir(parent.ino, &name), Err(FsError::NameTooLong));
    }

    #[test]
    fn file_meta_recovers_path_and_size() {
        let fs = ReadOnlyFs::new();
        fs.insert_object("s3/bkt/dir/a.bin", 4096);
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bkt = fs.lookup(s3.ino, "bkt").unwrap();
        let dir = fs.lookup(bkt.ino, "dir").unwrap();
        let a = fs.lookup(dir.ino, "a.bin").unwrap();

        let fh = fs.open(a.ino).unwrap();
        let (path, size) = fs.file_meta(fh).unwrap();
        assert_eq!(path, "s3/bkt/dir/a.bin");
        assert_eq!(size, 4096);

        // Unknown handle → BadHandle; directory open is rejected upstream.
        assert_eq!(fs.file_meta(9999), Err(FsError::BadHandle));
    }

    #[test]
    fn create_write_assembles_object_and_updates_size() {
        let fs = fs();
        let data = fs
            .lookup(fs.lookup(ROOT_INO, "s3").unwrap().ino, "bucket")
            .unwrap();
        let dir = fs.lookup(data.ino, "data").unwrap();
        let (attr, fh) = fs.create(dir.ino, "new.bin").unwrap();
        assert_eq!(attr.kind, FileKind::File);
        assert_eq!(attr.size, 0);
        // The new file is visible in the namespace at its full path.
        assert_eq!(fs.dirty_path(fh).as_deref(), Some("s3/bucket/data/new.bin"));

        // Sequential then random-offset writes assemble the whole object.
        assert_eq!(fs.write(fh, 0, b"hello ").unwrap(), 6);
        assert_eq!(fs.write(fh, 6, b"world").unwrap(), 5);
        // Overwrite a slice in the middle.
        assert_eq!(fs.write(fh, 0, b"HELLO").unwrap(), 5);
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"HELLO world");
        // Node size reflects the assembled length.
        assert_eq!(fs.getattr(attr.ino).unwrap().size, 11);
    }

    #[test]
    fn write_past_end_extends_with_zeros() {
        let fs = fs();
        let (_, fh) = fs.create(data_dir(&fs).ino, "sparse.bin").unwrap();
        // Write at offset 5 with nothing before → bytes 0..5 are zero-filled.
        fs.write(fh, 5, b"XY").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), vec![0, 0, 0, 0, 0, b'X', b'Y']);
    }

    #[test]
    fn truncate_resizes_buffer_and_size() {
        let fs = fs();
        let (attr, fh) = fs.create(data_dir(&fs).ino, "t.bin").unwrap();
        fs.write(fh, 0, b"0123456789").unwrap();
        fs.truncate(fh, 4).unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"0123");
        assert_eq!(fs.getattr(attr.ino).unwrap().size, 4);
        // Extend.
        fs.truncate(fh, 6).unwrap();
        assert_eq!(
            fs.dirty_bytes(fh).unwrap(),
            vec![b'0', b'1', b'2', b'3', 0, 0]
        );
    }

    #[test]
    fn truncate_plans_do_not_change_visible_state_before_commit() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        let write_fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: true,
                    write: true,
                    append: false,
                },
                Some(b"abcde".to_vec()),
            )
            .unwrap();

        let (path, planned) = fs.truncate_handle_plan(write_fh, 3).unwrap();
        assert_eq!(path, "s3/bucket/data/a.bin");
        assert_eq!(planned, b"abc");
        assert_eq!(fs.getattr(file.ino).unwrap().size, 5);
        assert_eq!(fs.dirty_bytes(write_fh).unwrap(), b"abcde");

        let committed = fs.commit_handle_contents(write_fh, planned).unwrap();
        assert_eq!(committed.size, 3);
        assert_eq!(fs.dirty_bytes(write_fh).unwrap(), b"abc");

        let (path, planned) = fs
            .truncate_inode_plan(file.ino, 6, b"abc".to_vec())
            .unwrap();
        assert_eq!(path, "s3/bucket/data/a.bin");
        assert_eq!(planned, vec![b'a', b'b', b'c', 0, 0, 0]);
        assert_eq!(fs.getattr(file.ino).unwrap().size, 3);

        fs.commit_inode_contents(file.ino, planned).unwrap();
        assert_eq!(fs.getattr(file.ino).unwrap().size, 6);
        assert_eq!(
            fs.dirty_bytes(write_fh).unwrap(),
            vec![b'a', b'b', b'c', 0, 0, 0]
        );
    }

    #[test]
    fn file_rename_preserves_inode_and_updates_open_handles() {
        let fs = fs();
        let parent = data_dir(&fs);
        let source = fs.lookup(parent.ino, "a.bin").unwrap();
        let fh = fs
            .open_with_options(
                source.ino,
                OpenOptions {
                    read: true,
                    write: true,
                    append: false,
                },
                Some(b"contents".to_vec()),
            )
            .unwrap();

        let plan = fs
            .file_rename_plan(parent.ino, "a.bin", parent.ino, "renamed.bin")
            .unwrap();
        assert_eq!(plan.source_path, "s3/bucket/data/a.bin");
        assert_eq!(plan.target_path, "s3/bucket/data/renamed.bin");
        assert_eq!(fs.lookup(parent.ino, "a.bin").unwrap().ino, source.ino);

        fs.commit_file_rename(&plan).unwrap();
        assert_eq!(fs.lookup(parent.ino, "a.bin"), Err(FsError::NotFound));
        assert_eq!(
            fs.lookup(parent.ino, "renamed.bin").unwrap().ino,
            source.ino
        );
        assert_eq!(
            fs.dirty_path(fh).as_deref(),
            Some("s3/bucket/data/renamed.bin")
        );
    }

    #[test]
    fn file_rename_replaces_a_regular_file_and_rejects_directories() {
        let fs = fs();
        let parent = data_dir(&fs);
        let source = fs.lookup(parent.ino, "a.bin").unwrap();
        let replaced = fs.lookup(parent.ino, "b.bin").unwrap();

        let plan = fs
            .file_rename_plan(parent.ino, "a.bin", parent.ino, "b.bin")
            .unwrap();
        assert_eq!(plan.target, Some((replaced.ino, 500)));
        fs.commit_file_rename(&plan).unwrap();
        assert_eq!(fs.lookup(parent.ino, "b.bin").unwrap().ino, source.ino);
        assert_eq!(fs.getattr(replaced.ino), Err(FsError::NotFound));

        assert_eq!(
            fs.file_rename_plan(
                fs.lookup(ROOT_INO, "s3").unwrap().ino,
                "bucket",
                parent.ino,
                "bucket"
            ),
            Err(FsError::IsDir)
        );
    }

    #[test]
    fn file_rename_updates_replaced_inode_ctime_when_links_remain() {
        let fs = fs();
        let parent = data_dir(&fs);
        let source = fs.lookup(parent.ino, "a.bin").unwrap();
        let target = fs.lookup(parent.ino, "b.bin").unwrap();
        let link = fs
            .hard_link_plan(target.ino, parent.ino, "b-link.bin")
            .unwrap();
        fs.commit_hard_link(&link).unwrap();
        let before = fs.getattr(target.ino).unwrap().ctime;
        std::thread::sleep(std::time::Duration::from_millis(1));

        let rename = fs
            .file_rename_plan(parent.ino, "a.bin", parent.ino, "b.bin")
            .unwrap();
        fs.commit_file_rename(&rename).unwrap();

        let remaining = fs.lookup(parent.ino, "b-link.bin").unwrap();
        assert_eq!(remaining.ino, target.ino);
        assert_eq!(remaining.nlink, 1);
        assert!(remaining.ctime > before);
        assert_eq!(fs.lookup(parent.ino, "b.bin").unwrap().ino, source.ino);
    }

    #[test]
    fn directory_rename_rewrites_subtree_paths_and_open_handles() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let source = fs.lookup(bucket.ino, "data").unwrap();
        let file = fs.lookup(source.ino, "a.bin").unwrap();
        let fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: true,
                    write: true,
                    append: false,
                },
                Some(b"abc".to_vec()),
            )
            .unwrap();

        let plan = fs
            .directory_rename_plan(bucket.ino, "data", bucket.ino, "moved")
            .unwrap();
        assert_eq!(plan.entries.len(), 2);
        assert!(plan.entries.iter().any(|entry| {
            entry.source_path == "s3/bucket/data/a.bin"
                && entry.target_path == "s3/bucket/moved/a.bin"
        }));
        fs.commit_directory_rename(&plan).unwrap();

        assert_eq!(fs.lookup(bucket.ino, "data"), Err(FsError::NotFound));
        let moved = fs.lookup(bucket.ino, "moved").unwrap();
        assert_eq!(moved.ino, source.ino);
        assert_eq!(fs.lookup(moved.ino, "a.bin").unwrap().ino, file.ino);
        assert_eq!(fs.dirty_path(fh).as_deref(), Some("s3/bucket/moved/a.bin"));
    }

    #[test]
    fn directory_rename_moves_only_hard_link_dentries_inside_the_subtree() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let source = fs.lookup(bucket.ino, "data").unwrap();
        let file = fs.lookup(source.ino, "a.bin").unwrap();
        let external = fs
            .hard_link_plan(file.ino, bucket.ino, "external.bin")
            .unwrap();
        fs.commit_hard_link(&external).unwrap();

        let plan = fs
            .directory_rename_plan(bucket.ino, "data", bucket.ino, "moved")
            .unwrap();
        assert!(plan.entries.iter().any(|entry| {
            entry.source_path == "s3/bucket/data/a.bin"
                && entry.target_path == "s3/bucket/moved/a.bin"
        }));
        assert!(!plan
            .entries
            .iter()
            .any(|entry| entry.source_path == "s3/bucket/external.bin"));
        fs.commit_directory_rename(&plan).unwrap();

        let moved = fs.lookup(bucket.ino, "moved").unwrap();
        assert_eq!(fs.lookup(moved.ino, "a.bin").unwrap().ino, file.ino);
        assert_eq!(fs.lookup(bucket.ino, "external.bin").unwrap().ino, file.ino);
        assert_eq!(
            fs.inode_paths(file.ino).unwrap(),
            vec![
                "s3/bucket/external.bin".to_string(),
                "s3/bucket/moved/a.bin".to_string(),
            ]
        );
    }

    #[test]
    fn directory_rename_rejects_cycles_and_nonempty_targets() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let source = fs.lookup(bucket.ino, "data").unwrap();
        assert_eq!(
            fs.directory_rename_plan(bucket.ino, "data", source.ino, "nested"),
            Err(FsError::Invalid)
        );

        fs.insert_object("s3/bucket/target/file.bin", 1);
        assert_eq!(
            fs.directory_rename_plan(bucket.ino, "data", bucket.ino, "target"),
            Err(FsError::NotEmpty)
        );
    }

    #[test]
    fn unlink_removes_node_and_returns_path() {
        let fs = fs();
        let bucket = fs
            .lookup(fs.lookup(ROOT_INO, "s3").unwrap().ino, "bucket")
            .unwrap();
        let dir = fs.lookup(bucket.ino, "data").unwrap();
        // a.bin exists.
        assert!(fs.lookup(dir.ino, "a.bin").is_ok());
        let path = fs.unlink(dir.ino, "a.bin").unwrap();
        assert_eq!(path, "s3/bucket/data/a.bin");
        // Now gone from the namespace.
        assert_eq!(fs.lookup(dir.ino, "a.bin"), Err(FsError::NotFound));
        // Unlinking a missing name errors.
        assert_eq!(fs.unlink(dir.ino, "a.bin"), Err(FsError::NotFound));
    }

    #[test]
    fn unlink_retains_an_open_inode_under_an_orphan_path() {
        let fs = fs();
        let dir = data_dir(&fs);
        let file = fs.lookup(dir.ino, "a.bin").unwrap();
        let fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: true,
                    write: true,
                    append: false,
                },
                Some(b"contents".to_vec()),
            )
            .unwrap();

        let plan = fs.unlink_plan(dir.ino, "a.bin").unwrap();
        let orphan_path = plan.orphan_path.clone().unwrap();
        assert!(orphan_path.contains("/.__talon_internal/unlinked/"));
        assert_eq!(
            plan.buffered_contents.as_deref(),
            Some(b"contents".as_slice())
        );

        fs.commit_unlink(&plan).unwrap();
        assert_eq!(fs.lookup(dir.ino, "a.bin"), Err(FsError::NotFound));
        assert_eq!(fs.getattr(file.ino).unwrap().nlink, 0);
        assert_eq!(fs.dirty_path(fh).as_deref(), Some(orphan_path.as_str()));

        fs.write(fh, 8, b"-tail").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"contents-tail");
        assert_eq!(
            fs.release_cleanup_path(fh).unwrap().as_deref(),
            Some(orphan_path.as_str())
        );
        fs.release(fh).unwrap();
        assert_eq!(fs.getattr(file.ino), Err(FsError::NotFound));
    }

    #[test]
    fn write_and_release_handle_lifecycle() {
        let fs = fs();
        let (_, fh) = fs.create(data_dir(&fs).ino, "x.bin").unwrap();
        assert!(fs.dirty_bytes(fh).is_some());
        fs.release(fh).unwrap();
        // After release the write buffer is gone.
        assert!(fs.dirty_bytes(fh).is_none());
        assert_eq!(fs.write(fh, 0, b"z"), Err(FsError::BadHandle));
    }

    #[test]
    fn create_rejects_existing_name() {
        let fs = fs();
        let bucket = fs
            .lookup(fs.lookup(ROOT_INO, "s3").unwrap().ino, "bucket")
            .unwrap();
        let dir = fs.lookup(bucket.ino, "data").unwrap();
        assert_eq!(fs.create(dir.ino, "a.bin"), Err(FsError::Exists));
    }

    #[test]
    fn non_truncating_write_open_preserves_existing_contents() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        let fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: true,
                    write: true,
                    append: false,
                },
                Some(b"existing".to_vec()),
            )
            .unwrap();

        fs.write(fh, 3, b"XYZ").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"exiXYZng");
        assert_eq!(
            fs.read_source(fh, 0, 8).unwrap(),
            ReadSource::Buffered(b"exiXYZng".to_vec())
        );
    }

    #[test]
    fn append_handle_ignores_the_supplied_offset() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        let fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: false,
                    write: true,
                    append: true,
                },
                Some(b"base".to_vec()),
            )
            .unwrap();

        fs.write(fh, 0, b"-tail").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"base-tail");
        assert_eq!(fs.read_source(fh, 0, 32), Err(FsError::BadHandle));
    }

    #[test]
    fn read_only_truncating_open_mutates_but_does_not_allow_write() {
        let fs = fs();
        let file = fs.lookup(data_dir(&fs).ino, "a.bin").unwrap();
        let fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: true,
                    write: false,
                    append: false,
                },
                Some(Vec::new()),
            )
            .unwrap();

        assert_eq!(
            fs.read_source(fh, 0, 32).unwrap(),
            ReadSource::Buffered(Vec::new())
        );
        assert_eq!(fs.write(fh, 0, b"x"), Err(FsError::BadHandle));
        assert_eq!(fs.dirty_bytes(fh).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn create_rejects_paths_without_backend_and_bucket() {
        let fs = ReadOnlyFs::new();
        assert_eq!(fs.create(ROOT_INO, "orphan.bin"), Err(FsError::Invalid));
    }

    /// The namespace lives behind a single Mutex, so with `lock().unwrap()` one
    /// panic while holding it poisoned the lock and made *every* subsequent
    /// FUSE op panic — the mount hung rather than erroring, and could not be
    /// unmounted cleanly. Poison recovery degrades that to a normal error.
    #[test]
    fn namespace_survives_a_panic_under_its_lock() {
        use std::sync::Arc;

        let fs = Arc::new(fs());
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();

        // Panic while the namespace lock is actually held, exactly as a bug on
        // the FUSE thread mid-operation would (e.g. an arithmetic overflow in
        // an op that has already taken the guard).
        let fs2 = Arc::clone(&fs);
        let panicked = std::thread::spawn(move || {
            let mut g = fs2.inner.lock_recover();
            // A mutation lands before the panic, so we can also check the data
            // is still there afterwards rather than silently reset.
            g.next_fh += 100;
            panic!("bug on the FUSE thread while holding the namespace lock");
        })
        .join();
        assert!(panicked.is_err(), "the thread must actually have panicked");
        assert!(
            fs.inner.is_poisoned(),
            "the namespace mutex must actually be poisoned"
        );

        // Every read op still works, and the pre-panic mutation is visible.
        assert_eq!(fs.lookup(ROOT_INO, "s3").unwrap().ino, s3.ino);
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let dir = fs.lookup(bucket.ino, "data").unwrap();
        assert!(fs.lookup(dir.ino, "a.bin").is_ok());
        assert!(fs.getattr(ROOT_INO).is_ok());
        assert!(fs.readdir(dir.ino).is_ok());

        // And write ops: a fresh handle still opens, writes and releases.
        let (_, fh) = fs.create(dir.ino, "after-panic.bin").unwrap();
        assert_eq!(fs.write(fh, 0, b"ok").unwrap(), 2);
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"ok");
        fs.release(fh).unwrap();
    }

    /// Regression: `offset` comes from the kernel unvalidated, so a single
    /// `pwrite` at a huge offset used to ask for a proportionally huge
    /// zero-filled allocation (`buf.resize(offset + len)`) — a one-syscall DoS
    /// of the whole mount. It is now bounded by `max_object_bytes`.
    #[test]
    fn write_past_the_cap_is_efbig_and_allocates_nothing() {
        let fs = fs().with_max_object_bytes(1024);
        let (attr, fh) = fs.create(data_dir(&fs).ino, "big.bin").unwrap();

        // 1 TiB offset: would have been a 1 TiB resize.
        assert_eq!(fs.write(fh, 1 << 40, b"x"), Err(FsError::TooLarge));
        // Just past the cap.
        assert_eq!(fs.write(fh, 1024, b"x"), Err(FsError::TooLarge));
        // Straddling the cap.
        assert_eq!(fs.write(fh, 1020, b"12345"), Err(FsError::TooLarge));

        // Nothing was buffered and the visible size never moved.
        assert!(fs.dirty_bytes(fh).unwrap().is_empty());
        assert_eq!(fs.getattr(attr.ino).unwrap().size, 0);

        // Exactly at the cap still succeeds.
        assert_eq!(fs.write(fh, 1023, b"x").unwrap(), 1);
        assert_eq!(fs.dirty_bytes(fh).unwrap().len(), 1024);
    }

    /// `offset + data.len()` overflowed `usize`: a debug panic while holding the
    /// global mutex (poisoning it and bricking every later op), a silent wrap in
    /// release. It is now a checked add.
    #[test]
    fn write_offset_overflow_is_efbig_not_a_panic() {
        let fs = fs();
        let (_, fh) = fs.create(data_dir(&fs).ino, "ovf.bin").unwrap();
        assert_eq!(fs.write(fh, u64::MAX, b"xyz"), Err(FsError::TooLarge));
        assert_eq!(fs.write(fh, u64::MAX - 1, b"xyz"), Err(FsError::TooLarge));
        // The filesystem is still usable afterwards (no poisoned lock).
        assert_eq!(fs.write(fh, 0, b"ok").unwrap(), 2);
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"ok");
    }

    /// `truncate -s 1P file` reached `buf.resize(size)` directly.
    #[test]
    fn truncate_past_the_cap_is_efbig() {
        let fs = fs().with_max_object_bytes(1024);
        let (attr, fh) = fs.create(data_dir(&fs).ino, "t.bin").unwrap();
        fs.write(fh, 0, b"seed").unwrap();

        assert_eq!(fs.truncate(fh, 1 << 50), Err(FsError::TooLarge));
        assert_eq!(fs.truncate(fh, 1025), Err(FsError::TooLarge));
        // Untouched by the rejected calls.
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"seed");
        assert_eq!(fs.getattr(attr.ino).unwrap().size, 4);

        // At the cap is fine.
        fs.truncate(fh, 1024).unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap().len(), 1024);
    }

    /// A bad handle is EBADF regardless of how absurd the size argument is.
    #[test]
    fn bad_handle_outranks_the_size_check() {
        let fs = ReadOnlyFs::new().with_max_object_bytes(16);
        assert_eq!(fs.write(9999, 1 << 40, b"x"), Err(FsError::BadHandle));
        assert_eq!(fs.truncate(9999, 1 << 40), Err(FsError::BadHandle));
    }

    #[test]
    fn default_cap_is_applied_and_overridable() {
        assert_eq!(
            ReadOnlyFs::new().max_object_bytes(),
            DEFAULT_MAX_OBJECT_BYTES
        );
        assert_eq!(
            ReadOnlyFs::new()
                .with_max_object_bytes(7)
                .max_object_bytes(),
            7
        );
    }

    /// Regression: a non-truncating write open must NOT start an empty dirty
    /// buffer. Doing so meant `open(O_RDWR)` + `close()` PUT zero bytes over a
    /// live object — silent remote data loss with no write ever issued.
    #[test]
    fn open_write_preserves_existing_contents() {
        let fs = fs();
        let bucket = fs
            .lookup(fs.lookup(ROOT_INO, "s3").unwrap().ino, "bucket")
            .unwrap();
        let dir = fs.lookup(bucket.ino, "data").unwrap();
        let file = fs.lookup(dir.ino, "a.bin").unwrap();
        let fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: true,
                    write: true,
                    append: false,
                },
                Some(b"existing".to_vec()),
            )
            .unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"existing");
        assert_eq!(fs.getattr(file.ino).unwrap().size, 8);
        fs.write(fh, 0, b"new").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"newsting");
    }

    #[test]
    fn open_write_with_truncate_resets_size_and_opens_a_write_handle() {
        let fs = fs();
        let bucket = fs
            .lookup(fs.lookup(ROOT_INO, "s3").unwrap().ino, "bucket")
            .unwrap();
        let dir = fs.lookup(bucket.ino, "data").unwrap();
        let file = fs.lookup(dir.ino, "a.bin").unwrap();

        let fh = fs
            .open_with_options(
                file.ino,
                OpenOptions {
                    read: false,
                    write: true,
                    append: false,
                },
                Some(Vec::new()),
            )
            .unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), Vec::<u8>::new());
        assert_eq!(fs.getattr(file.ino).unwrap().size, 0);
        fs.write(fh, 0, b"new contents").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"new contents");
    }

    #[test]
    fn open_write_on_a_directory_is_isdir() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        assert_eq!(
            fs.open_with_options(
                s3.ino,
                OpenOptions {
                    read: false,
                    write: true,
                    append: false,
                },
                Some(Vec::new()),
            ),
            Err(FsError::IsDir)
        );
    }
}
