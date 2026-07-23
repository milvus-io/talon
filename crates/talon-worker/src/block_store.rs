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
use std::sync::atomic::{AtomicU64, Ordering};
use talon_core::{
    BlockForm, BlockHandle, BlockId, BlockMeta, Error, ObjectStore, PageIndex, Result,
};

/// Process-wide monotonic counter making staging temp filenames unique, so two
/// concurrent writers of the same block never share a `.tmp` path (issue #113).
static STAGING_SEQ: AtomicU64 = AtomicU64::new(0);

/// Run blocking filesystem work on Tokio's blocking pool, so a large read or
/// write-plus-fsync never stalls the async reactor thread and the other
/// connections multiplexed on it (issue #115). A panic in the closure is mapped
/// to a backend error.
async fn spawn_blocking_io<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(join_error) => Err(Error::Backend(format!(
            "blocking store task failed: {join_error}"
        ))),
    }
}

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

    /// Sidecar metadata path for a block: `<root>/<shard>/<digest>.meta`.
    ///
    /// The `.blk` filename is a one-way digest of the [`BlockId`], so the id
    /// cannot be recovered from the block file alone. This sidecar stores the
    /// serialized [`BlockMeta`] (id, form, len) next to each committed block so
    /// the index can be rebuilt on startup from on-disk cache (issue #114).
    fn meta_path_for(&self, id: &BlockId) -> PathBuf {
        self.path_for(id).with_extension("meta")
    }

    /// Scan the cache directory and return the [`BlockMeta`] of every committed
    /// block, reconstructed from the on-disk `.meta` sidecars.
    ///
    /// Used at worker startup to repopulate the in-memory [`crate::BlockIndex`]
    /// so a restart does not re-download blocks already resident on local disk. A
    /// sidecar without a matching `.blk` (or vice versa) is skipped, and a
    /// malformed sidecar is ignored rather than failing the whole scan.
    pub fn scan(&self) -> Result<Vec<BlockMeta>> {
        let mut out = Vec::new();
        let shards = match std::fs::read_dir(&self.root) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        for shard in shards.flatten() {
            if !shard.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(shard.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("meta") {
                    continue;
                }
                // Require the matching block file to still be present.
                if !path.with_extension("blk").exists() {
                    continue;
                }
                match std::fs::read(&path) {
                    Ok(bytes) => match serde_json::from_slice::<BlockMeta>(&bytes) {
                        Ok(meta) => out.push(meta),
                        Err(error) => {
                            tracing::warn!(path = %path.display(), %error, "skipping malformed block sidecar");
                        }
                    },
                    Err(error) => {
                        tracing::warn!(path = %path.display(), %error, "skipping unreadable block sidecar");
                    }
                }
            }
        }
        Ok(out)
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
        let id = id.clone();
        // A whole-block read is up to block_size (256 MiB default) of blocking
        // disk I/O; run it on the blocking pool so it never stalls the async
        // reactor thread and its other multiplexed connections (issue #115).
        spawn_blocking_io(move || match std::fs::File::open(&path) {
            Ok(mut f) => {
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                Ok(Bytes::from(buf))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(id.to_string()))
            }
            Err(e) => Err(e.into()),
        })
        .await
    }

    async fn put(&self, id: &BlockId, value: Bytes) -> Result<()> {
        let path = self.path_for(id);
        let meta_path = self.meta_path_for(id);
        let meta = BlockMeta {
            id: id.clone(),
            form: BlockForm::Whole,
            len: value.len() as u64,
        };
        // The write + fsync of a whole block is blocking disk I/O; keep it off
        // the reactor thread (issue #115).
        spawn_blocking_io(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let pid = std::process::id();
            // Write to a per-writer-unique temp file then rename, so a present
            // `.blk` is always complete (crash-atomic commit) AND two concurrent
            // writers of the same block never truncate each other's staging file
            // (issue #113). Both writers stage identical content; whichever
            // renames last wins with a complete file, and the loser's rename is
            // a harmless overwrite of the same bytes.
            let seq = STAGING_SEQ.fetch_add(1, Ordering::Relaxed);
            let tmp = path.with_extension(format!("blk.tmp.{pid}.{seq}"));
            {
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(&value)?;
                f.sync_all()?;
            }
            // Rename our own unique temp into place. If a concurrent writer
            // already renamed theirs, this atomically replaces it with identical
            // bytes.
            if let Err(e) = std::fs::rename(&tmp, &path) {
                // Best-effort cleanup of our staging file on failure so a failed
                // commit never leaks a `.tmp`.
                let _ = std::fs::remove_file(&tmp);
                return Err(e.into());
            }
            // Write the sidecar (id + form + len) so the index can be rebuilt on
            // startup (issue #114). Same unique-temp-then-rename discipline. The
            // block file is already committed; a missing sidecar only costs a
            // re-fetch of that block after a restart, so a sidecar failure is
            // logged but does not fail the commit.
            let encoded = serde_json::to_vec(&meta)
                .map_err(|e| Error::Other(format!("encode block sidecar: {e}")))?;
            let meta_seq = STAGING_SEQ.fetch_add(1, Ordering::Relaxed);
            let meta_tmp = meta_path.with_extension(format!("meta.tmp.{pid}.{meta_seq}"));
            if let Err(error) = (|| -> std::io::Result<()> {
                let mut f = std::fs::File::create(&meta_tmp)?;
                f.write_all(&encoded)?;
                f.sync_all()?;
                std::fs::rename(&meta_tmp, &meta_path)
            })() {
                let _ = std::fs::remove_file(&meta_tmp);
                tracing::warn!(%error, "failed to write block sidecar; block will re-fetch after restart");
            }
            Ok(())
        })
        .await
    }

    async fn delete(&self, id: &BlockId) -> Result<()> {
        let path = self.path_for(id);
        let meta_path = self.meta_path_for(id);
        spawn_blocking_io(move || {
            // Remove the sidecar too so a deleted block is not resurrected by a
            // startup scan.
            let _ = std::fs::remove_file(&meta_path);
            match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }

    async fn contains(&self, id: &BlockId) -> Result<bool> {
        let path = self.path_for(id);
        spawn_blocking_io(move || Ok(path.exists())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
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
    async fn concurrent_puts_of_same_block_commit_complete_bytes() {
        // Two writers staging the same block must not share a temp file and
        // truncate each other; the committed `.blk` is always the full content
        // and no `.tmp` is leaked (issue #113).
        let root = tmp_root();
        let store = Arc::new(WholeBlockStore::open(&root).unwrap());
        let id = block(7);
        let data = Bytes::from(vec![0xABu8; 4096]);

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let id = id.clone();
            let data = data.clone();
            tasks.push(tokio::spawn(
                async move { store.put(&id, data).await.unwrap() },
            ));
        }
        for t in tasks {
            t.await.unwrap();
        }

        // The committed block is complete.
        assert_eq!(store.get_bytes(&id).await.unwrap(), data);
        // No staging temp files leaked.
        let shard = store.path_for(&id).parent().unwrap().to_path_buf();
        let leaked: Vec<_> = std::fs::read_dir(&shard)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(leaked.is_empty(), "leaked staging temp files: {leaked:?}");

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn scan_rebuilds_metadata_for_committed_blocks() {
        // Commit several blocks, then scan reconstructs each BlockMeta (id +
        // Whole + len) from the on-disk sidecars — the basis for rebuilding the
        // index on startup (issue #114).
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let committed = [
            (block(1), Bytes::from_static(b"one")),
            (block(2), Bytes::from(vec![0u8; 100])),
            (block(3), Bytes::from_static(b"three-block")),
        ];
        for (id, data) in &committed {
            store.put(id, data.clone()).await.unwrap();
        }

        let mut metas = store.scan().unwrap();
        metas.sort_by_key(|m| m.len);
        assert_eq!(metas.len(), 3);
        // Every committed block is present with the right id, form, and length.
        for (id, data) in &committed {
            let found = metas
                .iter()
                .find(|m| &m.id == id)
                .unwrap_or_else(|| panic!("scan missing block {id}"));
            assert_eq!(found.form, BlockForm::Whole);
            assert_eq!(found.len, data.len() as u64);
        }

        // A fresh store over the same root rebuilds the same set (restart).
        let reopened = WholeBlockStore::open(&root).unwrap();
        assert_eq!(reopened.scan().unwrap().len(), 3);

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn scan_skips_deleted_blocks_and_handles_empty_root() {
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        // Empty cache -> empty scan.
        assert!(store.scan().unwrap().is_empty());

        store
            .put(&block(1), Bytes::from_static(b"x"))
            .await
            .unwrap();
        store
            .put(&block(2), Bytes::from_static(b"y"))
            .await
            .unwrap();
        assert_eq!(store.scan().unwrap().len(), 2);

        // Deleting a block removes it (and its sidecar) from the scan.
        store.delete(&block(1)).await.unwrap();
        let metas = store.scan().unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].id, block(2));

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
