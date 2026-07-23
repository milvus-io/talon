//! Read-only FUSE operation logic.
//!
//! v1 exposes the cache as a **read-only** filesystem. This module implements
//! the request-handling logic behind the six read ops — `lookup`, `getattr`,
//! `readdir`, `open`, `read`, `release` — decoupled from the `fuser` mount
//! callbacks so it is unit-testable without a kernel mount. The mount layer is a
//! thin adapter that translates `fuser` calls into these methods and back.
//!
//! Paths under the mount mirror the backend namespace (`/s3/<bucket>/<key…>`,
//! see [`crate::mapping`]). Directories are synthesized from the path
//! hierarchy; files correspond to objects. Every mutating operation is rejected
//! with [`FsError::ReadOnly`] so the filesystem is enforced read-only.

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
}

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
}

/// A read-only view over the backend namespace, addressed by inode.
///
/// Nodes are registered up front (e.g. from a coordinator listing) via
/// [`insert_object`](ReadOnlyFs::insert_object); the tree of synthetic
/// directories is created on demand.
pub struct ReadOnlyFs {
    inner: Mutex<Inner>,
}

struct Inner {
    nodes: HashMap<u64, Node>,
    // (parent_ino, name) -> child_ino for O(1) lookup.
    index: HashMap<(u64, String), u64>,
    next_ino: u64,
    // Open file handles -> the inode they reference.
    handles: HashMap<u64, u64>,
    next_fh: u64,
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
            },
        );
        Self {
            inner: Mutex::new(Inner {
                nodes,
                index: HashMap::new(),
                next_ino: ROOT_INO + 1,
                handles: HashMap::new(),
                next_fh: 1,
            }),
        }
    }

    /// Register an object at `path` (e.g. `/s3/bucket/a/b/file.bin`) with `size`,
    /// creating intermediate directories. Returns the file's inode.
    pub fn insert_object(&self, path: &str, size: u64) -> u64 {
        let comps: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
        let mut g = self.inner.lock().unwrap();
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
        let g = self.inner.lock().unwrap();
        let ino = *g
            .index
            .get(&(parent_ino, name.to_string()))
            .ok_or(FsError::NotFound)?;
        Ok(Self::attr_of(g.nodes.get(&ino).ok_or(FsError::NotFound)?))
    }

    /// `getattr`: attributes for an inode.
    pub fn getattr(&self, ino: u64) -> Result<Attr, FsError> {
        let g = self.inner.lock().unwrap();
        Ok(Self::attr_of(g.nodes.get(&ino).ok_or(FsError::NotFound)?))
    }

    /// `readdir`: list children of a directory inode (excluding `.`/`..`).
    pub fn readdir(&self, ino: u64) -> Result<Vec<DirEntry>, FsError> {
        let g = self.inner.lock().unwrap();
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
        let mut g = self.inner.lock().unwrap();
        let kind = g.nodes.get(&ino).ok_or(FsError::NotFound)?.kind;
        if kind != FileKind::File {
            return Err(FsError::Unsupported);
        }
        let fh = g.next_fh;
        g.next_fh += 1;
        g.handles.insert(fh, ino);
        Ok(fh)
    }

    /// Resolve which inode + size a `read` on handle `fh` targets, clamping
    /// `[offset, offset+size)` to the file length.
    ///
    /// Returns `(ino, clamped_len)`; the caller performs the actual GET/GET_RANGE
    /// on the owning worker for that inode's object. A zero `clamped_len` means
    /// the read is entirely past EOF.
    pub fn read_plan(&self, fh: u64, offset: u64, size: u32) -> Result<(u64, u64), FsError> {
        let g = self.inner.lock().unwrap();
        let ino = *g.handles.get(&fh).ok_or(FsError::BadHandle)?;
        let node = g.nodes.get(&ino).ok_or(FsError::NotFound)?;
        let end = offset.saturating_add(size as u64).min(node.size);
        let clamped = end.saturating_sub(offset);
        Ok((ino, clamped))
    }

    /// `release`: drop a previously opened handle.
    pub fn release(&self, fh: u64) -> Result<(), FsError> {
        let mut g = self.inner.lock().unwrap();
        g.handles.remove(&fh).map(|_| ()).ok_or(FsError::BadHandle)
    }

    /// Any mutating operation (write/create/unlink/rename/chmod/…): always
    /// rejected on the read-only filesystem.
    pub fn mutate(&self) -> FsError {
        FsError::ReadOnly
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
    fn open_read_plan_release_flow() {
        let fs = fs();
        let s3 = fs.lookup(ROOT_INO, "s3").unwrap();
        let bucket = fs.lookup(s3.ino, "bucket").unwrap();
        let data = fs.lookup(bucket.ino, "data").unwrap();
        let a = fs.lookup(data.ino, "a.bin").unwrap();

        // Cannot open a directory.
        assert_eq!(fs.open(data.ino), Err(FsError::Unsupported));

        let fh = fs.open(a.ino).unwrap();
        // Full read within bounds.
        assert_eq!(fs.read_plan(fh, 0, 256).unwrap(), (a.ino, 256));
        // Partial read near EOF (footer): clamp to file size.
        assert_eq!(fs.read_plan(fh, 900, 256).unwrap(), (a.ino, 100));
        // Entirely past EOF.
        assert_eq!(fs.read_plan(fh, 2000, 10).unwrap(), (a.ino, 0));

        fs.release(fh).unwrap();
        // Handle no longer valid.
        assert_eq!(fs.read_plan(fh, 0, 1), Err(FsError::BadHandle));
        assert_eq!(fs.release(fh), Err(FsError::BadHandle));
    }

    #[test]
    fn mutating_ops_are_read_only() {
        let fs = fs();
        assert_eq!(fs.mutate(), FsError::ReadOnly);
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
}
