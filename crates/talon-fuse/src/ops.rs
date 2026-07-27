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
use std::collections::HashMap;
use std::sync::Mutex;

/// The root inode number (fixed by FUSE convention).
pub const ROOT_INO: u64 = 1;

/// A file-system object kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    /// A directory (namespace prefix).
    Directory,
    /// A regular file (a backend object).
    File,
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
}

/// Default cap on a single in-memory write buffer (1 GiB).
///
/// v1 buffers a whole object in RAM, so the buffer length is attacker-reachable
/// from an unprivileged `pwrite`/`ftruncate` at an arbitrary offset. Without a
/// cap, `pwrite(fd, buf, 1, 1<<40)` asks for a 1 TiB zero-filled allocation and
/// takes the mount (and likely the host) down. See
/// [`ReadOnlyFs::with_max_object_bytes`] to tune.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 1 << 30;

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

#[derive(Debug, Clone)]
struct Node {
    ino: u64,
    name: String,
    kind: FileKind,
    size: u64,
    children: Vec<u64>,
    /// Full mount-relative path for a file leaf (e.g. `s3/bkt/o.bin`); empty for
    /// directories. Lets a data callback recover the object from an inode.
    path: String,
}

/// A read-only view over the backend namespace, addressed by inode.
///
/// Nodes are registered up front (e.g. from a coordinator listing) via
/// [`insert_object`](ReadOnlyFs::insert_object); the tree of synthetic
/// directories is created on demand.
pub struct ReadOnlyFs {
    inner: Mutex<Inner>,
    /// Cap on the length of any single write handle's in-memory buffer.
    max_object_bytes: u64,
}

struct Inner {
    nodes: HashMap<u64, Node>,
    // (parent_ino, name) -> child_ino for O(1) lookup.
    index: HashMap<(u64, String), u64>,
    next_ino: u64,
    // Open file handles -> the inode they reference.
    handles: HashMap<u64, u64>,
    next_fh: u64,
    // Write handles -> their in-progress whole-object buffer. A handle opened for
    // write accumulates bytes here (random-offset writes land by position); the
    // assembled object is taken at flush/release and written through to the
    // backend (#226/#231). v1 buffers in memory (objects are single-block/small);
    // a temp-file-backed buffer for large objects is future work.
    dirty: HashMap<u64, DirtyFile>,
}

/// A per-write-handle whole-object buffer.
struct DirtyFile {
    /// The file inode this handle writes.
    ino: u64,
    /// The object contents assembled so far (extended with zeros for sparse
    /// writes past the current end).
    buf: Vec<u8>,
}

impl Default for ReadOnlyFs {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadOnlyFs {
    /// Create a filesystem with just the root directory.
    pub fn new() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            ROOT_INO,
            Node {
                ino: ROOT_INO,
                name: "/".to_string(),
                kind: FileKind::Directory,
                size: 0,
                children: Vec::new(),
                path: String::new(),
            },
        );
        Self {
            inner: Mutex::new(Inner {
                nodes,
                index: HashMap::new(),
                next_ino: ROOT_INO + 1,
                handles: HashMap::new(),
                next_fh: 1,
                dirty: HashMap::new(),
            }),
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
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
        for (i, comp) in comps.iter().enumerate() {
            let is_leaf = i == comps.len() - 1;
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
                name: comp.to_string(),
                kind,
                size: if is_leaf { size } else { 0 },
                children: Vec::new(),
                path: if is_leaf {
                    path.trim_start_matches('/').to_string()
                } else {
                    String::new()
                },
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
            self.insert_object(path, size);
            n += 1;
        }
        n
    }

    fn attr_of(node: &Node) -> Attr {
        let perm = match node.kind {
            FileKind::Directory => 0o555,
            FileKind::File => 0o444,
        };
        Attr {
            ino: node.ino,
            kind: node.kind,
            size: node.size,
            perm,
        }
    }

    /// `lookup`: resolve a child `name` under directory `parent_ino`.
    pub fn lookup(&self, parent_ino: u64, name: &str) -> Result<Attr, FsError> {
        let g = self.inner.lock_recover();
        let ino = *g
            .index
            .get(&(parent_ino, name.to_string()))
            .ok_or(FsError::NotFound)?;
        Ok(Self::attr_of(g.nodes.get(&ino).ok_or(FsError::NotFound)?))
    }

    /// `getattr`: attributes for an inode.
    pub fn getattr(&self, ino: u64) -> Result<Attr, FsError> {
        let g = self.inner.lock_recover();
        Ok(Self::attr_of(g.nodes.get(&ino).ok_or(FsError::NotFound)?))
    }

    /// `readdir`: list children of a directory inode (excluding `.`/`..`).
    pub fn readdir(&self, ino: u64) -> Result<Vec<DirEntry>, FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::Directory {
            return Err(FsError::NotFound);
        }
        let mut entries: Vec<DirEntry> = node
            .children
            .iter()
            .filter_map(|c| g.nodes.get(c))
            .map(|c| DirEntry {
                ino: c.ino,
                kind: c.kind,
                name: c.name.clone(),
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    /// `open`: obtain a read handle for a file inode. Directories are rejected.
    pub fn open(&self, ino: u64) -> Result<u64, FsError> {
        let mut g = self.inner.lock_recover();
        let kind = g.nodes.get(&ino).ok_or(FsError::NotFound)?.kind;
        if kind != FileKind::File {
            return Err(FsError::Unsupported);
        }
        let fh = g.next_fh;
        g.next_fh += 1;
        g.handles.insert(fh, ino);
        Ok(fh)
    }

    /// `release`: drop a previously opened handle (read or write), discarding any
    /// write buffer. The caller is expected to have already flushed a dirty write
    /// handle's contents (writeback happens on flush, #232).
    pub fn release(&self, fh: u64) -> Result<(), FsError> {
        let mut g = self.inner.lock_recover();
        let had_dirty = g.dirty.remove(&fh).is_some();
        let had_handle = g.handles.remove(&fh).is_some();
        if had_handle || had_dirty {
            Ok(())
        } else {
            Err(FsError::BadHandle)
        }
    }

    /// The mount-relative object path + size for an open file handle.
    ///
    /// Lets a data callback recover the backend object (via
    /// [`crate::mapping::path_to_object`]) and clamp reads to the file size.
    /// Errors with [`FsError::BadHandle`] for an unknown handle or
    /// [`FsError::Unsupported`] if the handle somehow references a directory.
    pub fn file_meta(&self, fh: u64) -> Result<(String, u64), FsError> {
        let g = self.inner.lock_recover();
        let ino = *g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::File {
            return Err(FsError::Unsupported);
        }
        Ok((node.path.clone(), node.size))
    }

    /// Return the committed object path and size before opening a writable copy.
    pub fn inode_file_meta(&self, ino: u64) -> Result<(String, u64), FsError> {
        let g = self.inner.lock_recover();
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::File {
            return Err(FsError::Unsupported);
        }
        Ok((node.path.clone(), node.size))
    }

    // ----- write path (#231) -----------------------------------------------

    /// `create`: make a new empty file `name` under directory `parent_ino` and
    /// open it for writing.
    ///
    /// Inserts a `File` node (size 0) into the namespace and returns
    /// `(attr, fh)` where `fh` is a write handle backed by an empty dirty buffer.
    /// Fails with [`FsError::NotFound`] if `parent_ino` is not a directory, or
    /// [`FsError::ReadOnly`] if the name already exists (v1 uses O_TRUNC-style
    /// whole-object creation; opening an existing file for write is
    /// [`open_write`](Self::open_write)).
    pub fn create(&self, parent_ino: u64, name: &str) -> Result<(Attr, u64), FsError> {
        let mut g = self.inner.lock_recover();
        let parent = g.nodes.get(&parent_ino).ok_or(FsError::NotFound)?;
        if parent.kind != FileKind::Directory {
            return Err(FsError::NotFound);
        }
        if g.index.contains_key(&(parent_ino, name.to_string())) {
            return Err(FsError::ReadOnly);
        }
        // Build the child's mount-relative path from its ancestors.
        let path = {
            let mut parts = Self::ancestry(&g, parent_ino);
            parts.push(name.to_string());
            parts.join("/")
        };
        let ino = g.next_ino;
        g.next_ino += 1;
        let node = Node {
            ino,
            name: name.to_string(),
            kind: FileKind::File,
            size: 0,
            children: Vec::new(),
            path,
        };
        g.nodes.insert(ino, node);
        g.index.insert((parent_ino, name.to_string()), ino);
        g.nodes.get_mut(&parent_ino).unwrap().children.push(ino);
        let fh = g.next_fh;
        g.next_fh += 1;
        g.handles.insert(fh, ino);
        g.dirty.insert(
            fh,
            DirtyFile {
                ino,
                buf: Vec::new(),
            },
        );
        let attr = Self::attr_of(g.nodes.get(&ino).unwrap());
        Ok((attr, fh))
    }

    /// Open an existing file `ino` for writing from a whole-object working copy.
    pub fn open_write(&self, ino: u64, initial_contents: Vec<u8>) -> Result<u64, FsError> {
        let mut g = self.inner.lock_recover();
        let kind = g.nodes.get(&ino).ok_or(FsError::NotFound)?.kind;
        if kind != FileKind::File {
            return Err(FsError::Unsupported);
        }
        let fh = g.next_fh;
        g.next_fh += 1;
        g.handles.insert(fh, ino);
        let size = initial_contents.len() as u64;
        g.dirty.insert(
            fh,
            DirtyFile {
                ino,
                buf: initial_contents,
            },
        );
        if let Some(node) = g.nodes.get_mut(&ino) {
            node.size = size;
        }
        Ok(fh)
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
        // EBADF outranks EFBIG, so validate the handle before the size checks.
        if !g.dirty.contains_key(&fh) {
            return Err(FsError::BadHandle);
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or(FsError::TooLarge)?;
        if end > self.max_object_bytes {
            return Err(FsError::TooLarge);
        }
        // Past the cap check, `end` fits in u64 and is bounded; on a 32-bit host
        // it may still exceed usize, which is a legitimate EFBIG.
        let end: usize = end.try_into().map_err(|_| FsError::TooLarge)?;
        let start: usize = offset.try_into().map_err(|_| FsError::TooLarge)?;
        let dirty = g.dirty.get_mut(&fh).ok_or(FsError::BadHandle)?;
        if dirty.buf.len() < end {
            dirty.buf.resize(end, 0);
        }
        dirty.buf[start..end].copy_from_slice(data);
        let ino = dirty.ino;
        let new_size = dirty.buf.len() as u64;
        if let Some(node) = g.nodes.get_mut(&ino) {
            node.size = node.size.max(new_size);
        }
        Ok(data.len() as u32)
    }

    /// `setattr(size)`: truncate/extend the write handle's buffer to `size`.
    ///
    /// Fails with [`FsError::TooLarge`] (`EFBIG`) if `size` exceeds
    /// [`ReadOnlyFs::max_object_bytes`] — `ftruncate` takes an arbitrary
    /// user-supplied length and the buffer is zero-filled eagerly.
    pub fn truncate(&self, fh: u64, size: u64) -> Result<(), FsError> {
        let mut g = self.inner.lock_recover();
        if !g.dirty.contains_key(&fh) {
            return Err(FsError::BadHandle);
        }
        if size > self.max_object_bytes {
            return Err(FsError::TooLarge);
        }
        let new_len: usize = size.try_into().map_err(|_| FsError::TooLarge)?;
        let dirty = g.dirty.get_mut(&fh).ok_or(FsError::BadHandle)?;
        dirty.buf.resize(new_len, 0);
        let ino = dirty.ino;
        if let Some(node) = g.nodes.get_mut(&ino) {
            node.size = size;
        }
        Ok(())
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

    /// The mount-relative object path of file `name` under `parent_ino`, without
    /// removing it. `None` if the name doesn't resolve to a file.
    pub fn file_path(&self, parent_ino: u64, name: &str) -> Option<String> {
        let g = self.inner.lock_recover();
        let ino = *g.index.get(&(parent_ino, name.to_string()))?;
        let node = g.nodes.get(&ino)?;
        if node.kind != FileKind::File {
            return None;
        }
        Some(node.path.clone())
    }

    /// `unlink`: remove file `name` under directory `parent_ino` from the
    /// namespace. Returns the removed file's mount-relative path so the caller
    /// can delete the backend object. Directories are not removed here.
    pub fn unlink(&self, parent_ino: u64, name: &str) -> Result<String, FsError> {
        let mut g = self.inner.lock_recover();
        let ino = *g
            .index
            .get(&(parent_ino, name.to_string()))
            .ok_or(FsError::NotFound)?;
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        if node.kind != FileKind::File {
            return Err(FsError::Unsupported);
        }
        let path = node.path.clone();
        g.nodes.remove(&ino);
        g.index.remove(&(parent_ino, name.to_string()));
        if let Some(parent) = g.nodes.get_mut(&parent_ino) {
            parent.children.retain(|c| *c != ino);
        }
        Ok(path)
    }

    /// Build the ancestry name chain (root-excluded) of `ino`, e.g.
    /// `["s3", "bucket", "data"]`, so a child path can be formed on `create`.
    fn ancestry(g: &Inner, ino: u64) -> Vec<String> {
        // Walk children from root is O(n); instead find each node's parent by
        // scanning the index. For the shallow synthetic tree this is fine.
        let mut names = Vec::new();
        let mut cur = ino;
        while cur != ROOT_INO {
            let node = match g.nodes.get(&cur) {
                Some(n) => n,
                None => break,
            };
            names.push(node.name.clone());
            // Find the parent whose children contain `cur`.
            let parent = g
                .nodes
                .iter()
                .find(|(_, n)| n.children.contains(&cur))
                .map(|(p, _)| *p);
            match parent {
                Some(p) => cur = p,
                None => break,
            }
        }
        names.reverse();
        names
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

    #[test]
    fn lookup_and_getattr_walk_the_tree() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        assert_eq!(s3.kind, FileKind::Directory);
        assert_eq!(s3.perm, 0o555);
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let data = fs.lookup(bucket.ino, "data").unwrap();
        let a = fs.lookup(data.ino, "a.bin").unwrap();
        assert_eq!(a.kind, FileKind::File);
        assert_eq!(a.size, 1000);
        assert_eq!(a.perm, 0o444);
        assert_eq!(fs.getattr(a.ino).unwrap(), a);

        assert_eq!(fs.lookup(ROOT_INO, "nope"), Err(FsError::NotFound));
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
        assert_eq!(fs.open(data.ino), Err(FsError::Unsupported));

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
        let fs = ReadOnlyFs::new();
        let (_, fh) = fs.create(ROOT_INO, "sparse.bin").unwrap();
        // Write at offset 5 with nothing before → bytes 0..5 are zero-filled.
        fs.write(fh, 5, b"XY").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), vec![0, 0, 0, 0, 0, b'X', b'Y']);
    }

    #[test]
    fn truncate_resizes_buffer_and_size() {
        let fs = ReadOnlyFs::new();
        let (attr, fh) = fs.create(ROOT_INO, "t.bin").unwrap();
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
    fn write_and_release_handle_lifecycle() {
        let fs = ReadOnlyFs::new();
        let (_, fh) = fs.create(ROOT_INO, "x.bin").unwrap();
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
        assert_eq!(fs.create(dir.ino, "a.bin"), Err(FsError::ReadOnly));
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
        let fs = ReadOnlyFs::new().with_max_object_bytes(1024);
        let (attr, fh) = fs.create(ROOT_INO, "big.bin").unwrap();

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
        let fs = ReadOnlyFs::new();
        let (_, fh) = fs.create(ROOT_INO, "ovf.bin").unwrap();
        assert_eq!(fs.write(fh, u64::MAX, b"xyz"), Err(FsError::TooLarge));
        assert_eq!(fs.write(fh, u64::MAX - 1, b"xyz"), Err(FsError::TooLarge));
        // The filesystem is still usable afterwards (no poisoned lock).
        assert_eq!(fs.write(fh, 0, b"ok").unwrap(), 2);
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"ok");
    }

    /// `truncate -s 1P file` reached `buf.resize(size)` directly.
    #[test]
    fn truncate_past_the_cap_is_efbig() {
        let fs = ReadOnlyFs::new().with_max_object_bytes(1024);
        let (attr, fh) = fs.create(ROOT_INO, "t.bin").unwrap();
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
        let fh = fs.open_write(file.ino, b"existing".to_vec()).unwrap();
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

        let fh = fs.open_write(file.ino, Vec::new()).unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), Vec::<u8>::new());
        assert_eq!(fs.getattr(file.ino).unwrap().size, 0);
        fs.write(fh, 0, b"new contents").unwrap();
        assert_eq!(fs.dirty_bytes(fh).unwrap(), b"new contents");
    }

    #[test]
    fn open_write_on_a_directory_is_unsupported() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        assert_eq!(fs.open_write(s3.ino, Vec::new()), Err(FsError::Unsupported));
    }
}
