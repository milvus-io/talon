//! NVMe-backed whole-block store.
//!
//! A *whole* block is stored as a single `.blk` file on local disk (NVMe in
//! production). Reads return a [`BlockHandle`] over the open file descriptor so
//! the transport layer can serve bytes with `sendfile`, never copying them
//! through userspace.
//!
//! # Layout
//!
//! Each block maps to `<root>/<shard>/<digest>.blk`, where `digest` is a stable
//! hash of the block's identity ([`BlockId`]'s `Display`, which includes object
//! path, version, offset, and block size) and `shard` is the first two hex
//! digits of that digest. Sharding keeps any single directory from growing
//! unbounded.
//!
//! Paging is not handled here — [`get_page`](WholeBlockStore::get_page) returns
//! [`Error::NotFound`]; the per-page store lands separately (see #15).

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use talon_core::{BlockHandle, BlockId, Error, ObjectStore, PageIndex, Result};

/// A local, file-backed store for whole blocks.
pub struct WholeBlockStore {
    root: PathBuf,
}

impl WholeBlockStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// The cache root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Stable filesystem path for a block: `<root>/<shard>/<digest>.blk`.
    fn path_for(&self, id: &BlockId) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let digest = hasher.finish();
        let hex = format!("{digest:016x}");
        self.root.join(&hex[0..2]).join(format!("{hex}.blk"))
    }

    /// Open a present block file read-only, returning its fd and byte length.
    fn open_ro(&self, id: &BlockId) -> Result<(OwnedFd, u64)> {
        let path = self.path_for(id);
        match std::fs::File::open(&path) {
            Ok(f) => {
                let len = f.metadata()?.len();
                Ok((OwnedFd::from(f), len))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(id.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl ObjectStore for WholeBlockStore {
    async fn get_block(&self, id: &BlockId) -> Result<BlockHandle> {
        let (fd, len) = self.open_ro(id)?;
        Ok(BlockHandle::new(fd, 0, len))
    }

    async fn get_page(&self, id: &BlockId, _page: PageIndex) -> Result<BlockHandle> {
        // Whole-block store has no per-page granularity; paged store is #15.
        Err(Error::NotFound(id.to_string()))
    }

    async fn get_range(&self, id: &BlockId, offset: u64, len: u64) -> Result<Vec<BlockHandle>> {
        let (fd, file_len) = self.open_ro(id)?;
        if offset.checked_add(len).map_or(true, |end| end > file_len) {
            return Err(Error::Other(format!(
                "range {offset}+{len} out of bounds for block of {file_len} bytes"
            )));
        }
        Ok(vec![BlockHandle::new(fd, offset, len)])
    }

    async fn get_bytes(&self, id: &BlockId) -> Result<Bytes> {
        let path = self.path_for(id);
        match std::fs::File::open(&path) {
            Ok(mut f) => {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                Ok(Bytes::from(buf))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(id.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, id: &BlockId, value: Bytes) -> Result<()> {
        let path = self.path_for(id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Write to a temp file then rename so a present `.blk` is always
        // complete (crash-atomic commit).
        let tmp = path.with_extension("blk.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&value)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    async fn delete(&self, id: &BlockId) -> Result<()> {
        let path = self.path_for(id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn contains(&self, id: &BlockId) -> Result<bool> {
        Ok(self.path_for(id).exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use talon_core::{Backend, ObjectId, Version};

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", format!("obj/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn tmp_root() -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "talon-blkstore-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        p
    }

    fn rand_suffix() -> u64 {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        h.finish()
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let id = block(1);
        let data = Bytes::from_static(b"hello block");

        assert!(!store.contains(&id).await.unwrap());
        assert!(matches!(
            store.get_bytes(&id).await,
            Err(Error::NotFound(_))
        ));

        store.put(&id, data.clone()).await.unwrap();
        assert!(store.contains(&id).await.unwrap());
        assert_eq!(store.get_bytes(&id).await.unwrap(), data);

        store.delete(&id).await.unwrap();
        assert!(!store.contains(&id).await.unwrap());
        // Deleting again is a no-op.
        store.delete(&id).await.unwrap();

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn get_block_returns_valid_fd() {
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let id = block(2);
        let data = Bytes::from_static(b"0123456789");
        store.put(&id, data.clone()).await.unwrap();

        let handle = store.get_block(&id).await.unwrap();
        assert_eq!(handle.offset, 0);
        assert_eq!(handle.len, data.len() as u64);
        assert!(handle.fd.as_raw_fd() >= 0);

        // The fd is readable and yields the stored bytes.
        let mut f = std::fs::File::from(handle.fd);
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, data);

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn get_range_bounds() {
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let id = block(3);
        store
            .put(&id, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();

        let handles = store.get_range(&id, 2, 3).await.unwrap();
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0].offset, 2);
        assert_eq!(handles[0].len, 3);

        assert!(store.get_range(&id, 6, 5).await.is_err());
        assert!(matches!(
            store.get_range(&block(999), 0, 1).await,
            Err(Error::NotFound(_))
        ));

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn get_page_is_not_supported() {
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let id = block(4);
        store.put(&id, Bytes::from_static(b"x")).await.unwrap();
        assert!(matches!(
            store.get_page(&id, PageIndex(0)).await,
            Err(Error::NotFound(_))
        ));
        std::fs::remove_dir_all(&root).ok();
    }
}
