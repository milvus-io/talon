//! NVMe-backed whole-block store.
//!
//! A *whole* block is stored as a single `.blk` file on local disk (NVMe in
//! production). Reads return a [`BlockHandle`] over the open file descriptor so
//! the transport layer can serve bytes with `sendfile`, never copying them
//! through userspace.
//!
//! # Layout
//!
//! Each block maps to `<root>/whole/<shard>/<digest>.blk`, where `digest` is a stable
//! hash of the block's identity ([`BlockId`]'s `Display`, which includes object
//! path, version, offset, and block size) and `shard` is the first two hex
//! digits of that digest. Sharding keeps any single directory from growing
//! unbounded.
//!
//! Paging is not handled here — [`get_page`](WholeBlockStore::get_page) returns
//! [`Error::NotFound`]. Per-page caching lives in
//! [`PagedBlockStore`](crate::PagedBlockStore), which the worker uses instead of
//! this store when `l2_page_size_bytes` is set.

#[cfg(test)]
mod journal_tests;

use crate::fd_cache::{CachedFd, FdCache};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use talon_core::{BlockHandle, BlockId, BlockMeta, Error, ObjectStore, PageIndex, Result};

pub(crate) fn block_path(root: &Path, id: &BlockId) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    let hex = format!("{:016x}", hasher.finish());
    root.join(&hex[..2]).join(format!("{hex}.blk"))
}

fn catalog_error(error: anyhow::Error) -> Error {
    Error::Other(format!("whole block catalog: {error:#}"))
}

/// Run blocking filesystem work on Tokio's blocking pool, so a large read or
/// write-plus-fsync never stalls the async reactor thread and the other
/// connections multiplexed on it (issue #115). A panic in the closure is mapped
/// to a backend error.
async fn spawn_blocking_io<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let task = if talon_telemetry::diagnostic() {
        let operation = talon_telemetry::Operation::new(
            "talon.cache.io",
            "internal",
            talon_telemetry::TraceParent::Inherit,
        );
        let submitted = std::time::Instant::now();
        tokio::task::spawn_blocking(move || {
            operation.record(
                "talon.blocking.queue_wait_us",
                submitted.elapsed().as_micros() as u64,
            );
            let started = std::time::Instant::now();
            let result = operation.in_scope(f);
            operation.record(
                "talon.blocking.execution_us",
                started.elapsed().as_micros() as u64,
            );
            operation.outcome(if result.is_ok() { "success" } else { "error" });
            result
        })
    } else {
        tokio::task::spawn_blocking(f)
    };
    match task.await {
        Ok(result) => result,
        Err(join_error) => Err(Error::Backend(format!(
            "blocking store task failed: {join_error}"
        ))),
    }
}

/// A local, file-backed store for whole blocks.
#[derive(Clone)]
pub struct WholeBlockStore {
    uring: bool,
    direct_alignment: Option<talon_uring::DirectIoAlignment>,
    root: PathBuf,
    fd_cache: Arc<FdCache>,
    catalog: Arc<crate::catalog::Catalog>,
    catalog_path: String,
    mutations: Arc<crate::page_gc::Mutations>,
    gates: Arc<[tokio::sync::Mutex<()>; 64]>,
}

impl WholeBlockStore {
    /// Select native I/O and probe this cache directory before accepting writes.
    pub fn with_uring(mut self, enabled: bool) -> std::io::Result<Self> {
        self.direct_alignment = enabled
            .then(|| talon_uring::DirectIoAlignment::probe(&self.root))
            .transpose()?;
        self.uring = enabled;
        if let Some(alignment) = self.direct_alignment {
            tracing::info!(root = %self.root.display(), memory_alignment = alignment.memory(), write_alignment = alignment.offset(), "cache direct I/O alignment");
        }
        Ok(self)
    }
    pub(crate) fn direct_alignment(&self) -> Option<talon_uring::DirectIoAlignment> {
        self.direct_alignment
    }
    pub(crate) async fn put_payload(
        &self,
        id: &BlockId,
        value: talon_uring::Payload,
    ) -> Result<u64> {
        if self.uring {
            talon_uring::require_ring()?;
        }
        let store = self.clone();
        let id = id.clone();
        self.mutations
            .run(async move {
                let _gate = store.gate(&id).lock().await;
                let transaction = store
                    .catalog
                    .begin(
                        &store.catalog_path,
                        &id,
                        crate::catalog::Operation::PutWhole {
                            len: value.len() as u64,
                        },
                    )
                    .await
                    .map_err(catalog_error)?;
                #[cfg(test)]
                crate::page_access_store::crash_barrier("whole_begin")?;
                let path = store.path_for(&id);
                let staged = path.with_extension(format!("blk.catalog.{}", transaction.id));
                let result: Result<u64> = if store.uring {
                    let alignment = store.direct_alignment.expect("native store alignment");
                    talon_uring::spawn_local(async move {
                        talon_uring::atomic_write_payload_staged(&path, &staged, value, alignment)
                            .await
                            .map_err(Error::from)
                    })
                    .await
                    .map_err(|error| Error::Other(format!("whole block task stopped: {error}")))
                    .and_then(|result| result)
                } else {
                    spawn_blocking_io(move || {
                        let dir = path.parent().unwrap();
                        std::fs::create_dir_all(dir)?;
                        std::fs::File::open(dir.parent().unwrap())?.sync_all()?;
                        let mut file = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&staged)?;
                        talon_telemetry::sync_io("talon.cache.write", || {
                            file.write_all(&value.into_bytes())
                        })?;
                        talon_telemetry::sync_io("talon.cache.fsync", || file.sync_all())?;
                        let allocated = file.metadata()?.blocks() * 512;
                        #[cfg(test)]
                        crate::page_access_store::crash_barrier("whole_staged")?;
                        talon_telemetry::sync_io("talon.cache.rename", || {
                            std::fs::rename(&staged, &path)
                        })?;
                        std::fs::File::open(dir)?.sync_all()?;
                        Ok(allocated)
                    })
                    .await
                };
                store.fd_cache.invalidate(&store.path_for(&id));
                match result {
                    Ok(allocated) => {
                        #[cfg(test)]
                        crate::page_access_store::crash_barrier("whole_published")?;
                        store
                            .catalog
                            .complete(transaction, allocated)
                            .await
                            .map_err(catalog_error)?;
                        #[cfg(test)]
                        crate::page_access_store::crash_barrier("whole_complete")?;
                        Ok(allocated)
                    }
                    Err(error) => {
                        store.catalog.poison();
                        Err(error)
                    }
                }
            })
            .await
    }

    fn gate(&self, id: &BlockId) -> &tokio::sync::Mutex<()> {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        &self.gates[hasher.finish() as usize % self.gates.len()]
    }

    pub(crate) async fn drain(&self) {
        self.mutations.drain().await;
    }

    /// Open a standalone journaled store, retaining an exclusive root lease.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let lock = Arc::new(
            crate::page_access_store::CacheRootLock::acquire(&root).map_err(catalog_error)?,
        );
        Self::open_locked(root, lock)
    }

    pub fn open_locked(
        root: impl Into<PathBuf>,
        lock: Arc<crate::page_access_store::CacheRootLock>,
    ) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        let service = crate::catalog::service(lock).map_err(catalog_error)?;
        let catalog = crate::catalog::Catalog::with_service(
            &service,
            &root,
            crate::catalog::transaction_budget(),
            None,
        )
        .map_err(catalog_error)?;
        Self::open_with_catalog(
            root,
            Arc::new(FdCache::new(
                talon_core::WorkerConfig::default().whole_fd_cache_capacity,
            )),
            catalog,
        )
    }

    pub(crate) fn open_with_catalog(
        root: impl Into<PathBuf>,
        fd_cache: Arc<FdCache>,
        catalog: Arc<crate::catalog::Catalog>,
    ) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        let data = root.join("whole");
        let catalog_path = catalog.store_path(&data).map_err(catalog_error)?;
        if !catalog
            .registered(&catalog_path, 0)
            .map_err(catalog_error)?
        {
            // Only first registration inspects the scope. Never adopt legacy
            // sidecars or unregistered data, even when paged mode is enabled.
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name == "whole"
                    || (name.len() == 2 && name.bytes().all(|c| c.is_ascii_hexdigit()))
                {
                    return Err(Error::Other("whole cache has no journal; use a new empty cache directory (legacy migration is not supported)".into()));
                }
            }
            catalog.register(&catalog_path, 0).map_err(catalog_error)?;
        }
        std::fs::create_dir_all(&data)?;
        if std::fs::symlink_metadata(&data)?.file_type().is_symlink() {
            return Err(Error::Other(
                "whole cache directory must not be a symlink".into(),
            ));
        }
        std::fs::File::open(&root)?.sync_all()?;
        Ok(Self {
            uring: false,
            direct_alignment: None,
            root,
            fd_cache,
            catalog,
            catalog_path,
            mutations: Arc::new(crate::page_gc::Mutations::new(true)),
            gates: Arc::new(std::array::from_fn(|_| tokio::sync::Mutex::new(()))),
        })
    }

    /// The cache root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) async fn checkpoint_catalog(&self) -> anyhow::Result<()> {
        self.catalog.checkpoint().await
    }

    fn path_for(&self, id: &BlockId) -> PathBuf {
        block_path(&self.root.join("whole"), id)
    }

    /// Return residency from the journal without enumerating or statting data files.
    pub fn scan(&self) -> Result<Vec<BlockMeta>> {
        Ok(self
            .scan_allocated()?
            .into_iter()
            .map(|entry| entry.meta.into())
            .collect())
    }

    pub fn restore_index(&self, index: &crate::BlockIndex) -> Result<()> {
        for entry in self.scan_allocated()? {
            index.commit_stored(entry);
        }
        Ok(())
    }

    pub(crate) fn restore_index_exclusive(&self, index: &mut crate::BlockIndex) -> Result<()> {
        index.restore_allocated(self.scan_allocated()?, crate::recovery::workers());
        Ok(())
    }

    pub(crate) fn scan_allocated(&self) -> Result<Vec<crate::index::StoredBlock>> {
        self.catalog
            .recover(&self.catalog_path)
            .map_err(catalog_error)?
            .ok_or_else(|| Error::Other("whole cache is not registered".into()))
    }
    /// Open a present block file read-only, returning a shared fd and its byte
    /// length.
    ///
    /// Backed by the shared open-fd cache: a repeat read of a resident block
    /// reuses the
    /// already-open descriptor instead of issuing `openat` + `statx` + `close`
    /// per request. Under load that trio dominated the serving path — it does
    /// not scale with threads (all callers serialize on the same inode and
    /// dentry), and measured ~50x the cost of the data access it guards.
    async fn open_ro(&self, id: &BlockId) -> Result<(Arc<OwnedFd>, u64)> {
        let path = self.path_for(id);
        if self.uring {
            talon_uring::require_ring()?;
            return self
                .fd_cache
                .open_native(path)
                .await
                .map(|hit| (hit.fd, hit.len))
                .map_err(|error| cache_read_error(error, id.to_string()));
        }
        if let Some(hit) = self.fd_cache.get(&path) {
            return Ok((hit.fd, hit.len));
        }
        match std::fs::File::open(&path) {
            Ok(f) => {
                let len = f.metadata()?.len();
                let fd = Arc::new(OwnedFd::from(f));
                self.fd_cache.insert(
                    path,
                    CachedFd {
                        fd: Arc::clone(&fd),
                        len,
                    },
                );
                Ok((fd, len))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(id.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Read exactly one block-relative byte window into userspace.
    ///
    /// This is used to promote touched L2 regions into the finer-grained L1
    /// page cache without reading the entire (256 MiB by default) block.
    ///
    /// Unlike [`get_block`](ObjectStore::get_block) and
    /// [`get_range`](ObjectStore::get_range) this still opens the file per
    /// call. Promotion happens once per region rather than once per request, so
    /// it is not on the hot serving path that motivated the fd cache; routing it
    /// through the cache is a follow-up, not a correctness matter.
    pub async fn get_range_bytes(&self, id: &BlockId, offset: u64, len: u64) -> Result<Bytes> {
        let path = self.path_for(id);
        let id = id.clone();
        if self.uring {
            talon_uring::require_ring()?;
            return talon_uring::spawn_cancellable(async move {
                talon_uring::read_file(&path, Some((offset, len)))
                    .await
                    .map_err(|error| cache_read_error(error, id.to_string()))
            })
            .await
            .map_err(|e| std::io::Error::other(format!("ring read task stopped: {e}")))?;
        }
        spawn_blocking_io(move || {
            let file = match std::fs::File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(Error::NotFound(id.to_string()));
                }
                Err(error) => return Err(error.into()),
            };
            let file_len = file.metadata()?.len();
            if offset.checked_add(len).map_or(true, |end| end > file_len) {
                return Err(Error::Other(format!(
                    "range {offset}+{len} out of bounds for block of {file_len} bytes"
                )));
            }
            let size = usize::try_from(len)
                .map_err(|_| Error::Other(format!("range length {len} exceeds usize")))?;
            let mut buffer = vec![0_u8; size];
            file.read_exact_at(&mut buffer, offset)?;
            Ok(Bytes::from(buffer))
        })
        .await
    }
}

#[async_trait]
impl ObjectStore for WholeBlockStore {
    async fn get_block(&self, id: &BlockId) -> Result<BlockHandle> {
        let (fd, len) = self.open_ro(id).await?;
        Ok(BlockHandle::from_shared(fd, 0, len))
    }

    async fn get_page(&self, id: &BlockId, _page: PageIndex) -> Result<BlockHandle> {
        // Whole-block store has no per-page granularity; a paged worker uses
        // PagedBlockStore for page reads instead.
        Err(Error::NotFound(id.to_string()))
    }

    async fn get_range(&self, id: &BlockId, offset: u64, len: u64) -> Result<Vec<BlockHandle>> {
        let (fd, file_len) = self.open_ro(id).await?;
        if offset.checked_add(len).map_or(true, |end| end > file_len) {
            return Err(Error::Other(format!(
                "range {offset}+{len} out of bounds for block of {file_len} bytes"
            )));
        }
        Ok(vec![BlockHandle::from_shared(fd, offset, len)])
    }

    async fn get_bytes(&self, id: &BlockId) -> Result<Bytes> {
        let path = self.path_for(id);
        let id = id.clone();
        if self.uring {
            talon_uring::require_ring()?;
            return talon_uring::spawn_cancellable(async move {
                talon_uring::read_file(&path, None)
                    .await
                    .map_err(|error| cache_read_error(error, id.to_string()))
            })
            .await
            .map_err(|e| std::io::Error::other(format!("ring read task stopped: {e}")))?;
        }
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
        self.put_payload(id, value.into()).await.map(|_| ())
    }

    async fn delete(&self, id: &BlockId) -> Result<()> {
        let store = self.clone();
        let id = id.clone();
        self.mutations
            .run(async move {
                let _gate = store.gate(&id).lock().await;
                let transaction = store
                    .catalog
                    .begin(
                        &store.catalog_path,
                        &id,
                        crate::catalog::Operation::DeleteBlock,
                    )
                    .await
                    .map_err(catalog_error)?;
                #[cfg(test)]
                crate::page_access_store::crash_barrier("whole_delete_begin")?;
                let path = store.path_for(&id);
                let result = spawn_blocking_io(move || {
                    match std::fs::remove_file(&path) {
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        result => result?,
                    }
                    match std::fs::File::open(path.parent().unwrap()) {
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        file => file?.sync_all()?,
                    }
                    Ok(())
                })
                .await;
                store.fd_cache.invalidate(&store.path_for(&id));
                match result {
                    Ok(()) => {
                        #[cfg(test)]
                        crate::page_access_store::crash_barrier("whole_deleted")?;
                        store
                            .catalog
                            .complete(transaction, 0)
                            .await
                            .map_err(catalog_error)
                    }
                    Err(error) => {
                        store.catalog.poison();
                        Err(error)
                    }
                }
            })
            .await
    }
    async fn contains(&self, id: &BlockId) -> Result<bool> {
        let path = self.path_for(id);
        spawn_blocking_io(move || Ok(path.exists())).await
    }
}

/// Preserve cache-miss semantics across the native and portable file engines.
pub(crate) fn cache_read_error(error: std::io::Error, label: String) -> Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        Error::NotFound(label)
    } else {
        error.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use talon_core::{Backend, BlockForm, ObjectId, Version};

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
    async fn recovery_uses_committed_allocation_without_statting_data() {
        let root = tempfile::tempdir().unwrap();
        let store = WholeBlockStore::open(root.path()).unwrap();
        let id = block(0);
        store.put(&id, Bytes::from(vec![0; 6000])).await.unwrap();
        let allocated = std::fs::metadata(store.path_for(&id)).unwrap().blocks() * 512;
        let shard = store.path_for(&id).parent().unwrap().to_owned();
        store.catalog.checkpoint().await.unwrap();
        drop(store);
        // Recovery must neither enumerate the shard nor stat its data files.
        std::fs::rename(&shard, root.path().join("hidden")).unwrap();
        std::fs::write(&shard, b"not a directory").unwrap();
        let store = WholeBlockStore::open(root.path()).unwrap();
        let index = crate::BlockIndex::new();
        store.restore_index(&index).unwrap();
        assert_eq!(index.get(&id).unwrap().len, 6000);
        assert_eq!(index.resident_bytes(), allocated);
        assert_eq!(index.snapshot_units(), vec![(id, None, allocated)]);
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
            .filter(|e| e.file_name().to_string_lossy().contains(".catalog."))
            .collect();
        assert!(leaked.is_empty(), "leaked staging temp files: {leaked:?}");

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn journal_rebuilds_metadata_for_committed_blocks() {
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

        for (id, _) in &committed {
            assert!(!store.path_for(id).with_extension("meta").exists());
        }
        store.catalog.checkpoint().await.unwrap();
        drop(store);
        let reopened = WholeBlockStore::open(&root).unwrap();
        assert_eq!(reopened.scan().unwrap().len(), 3);
        let index = crate::BlockIndex::new();
        reopened.restore_index(&index).unwrap();
        assert_eq!(index.snapshot_units().len(), 3);

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

        // Deleting a block removes its journal residency.
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
        // The handle shares its descriptor with the store's fd cache, so dup
        // it rather than taking ownership (which would close the cached fd).
        let mut f = std::fs::File::from(handle.fd.try_clone().unwrap());
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
    async fn get_range_bytes_reads_only_the_requested_window() {
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let id = block(42);
        store
            .put(&id, Bytes::from_static(b"0123456789abcdef"))
            .await
            .unwrap();

        assert_eq!(
            store.get_range_bytes(&id, 5, 6).await.unwrap(),
            Bytes::from_static(b"56789a")
        );
        assert!(store.get_range_bytes(&id, 15, 2).await.is_err());
        std::fs::remove_dir_all(root).ok();
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

    /// The open-fd cache must not let a stale descriptor outlive the file it
    /// was opened for. A rewritten block renames a new inode over the path, and
    /// a deleted block unlinks it; both must be reflected by subsequent reads.
    #[tokio::test]
    async fn fd_cache_does_not_serve_stale_bytes_after_rewrite_or_delete() {
        use std::io::Read;
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let id = block(42);

        let read_handle = |h: BlockHandle| {
            let mut f = std::fs::File::from(h.fd.try_clone().unwrap());
            let mut buf = vec![0u8; h.len as usize];
            use std::io::{Seek, SeekFrom};
            f.seek(SeekFrom::Start(h.offset)).unwrap();
            f.read_exact(&mut buf).unwrap();
            buf
        };

        store.put(&id, Bytes::from_static(b"aaaa")).await.unwrap();
        // Populate the cache, then read again to exercise the hit path.
        assert_eq!(read_handle(store.get_block(&id).await.unwrap()), b"aaaa");
        assert_eq!(read_handle(store.get_block(&id).await.unwrap()), b"aaaa");

        // Rewrite with different content and length: the cached fd points at
        // the superseded inode and must have been invalidated.
        store
            .put(&id, Bytes::from_static(b"bbbbbbbb"))
            .await
            .unwrap();
        let h = store.get_block(&id).await.unwrap();
        assert_eq!(h.len, 8, "cached len must refresh after rewrite");
        assert_eq!(read_handle(h), b"bbbbbbbb");

        // A range read goes through the same cache.
        let ranged = store.get_range(&id, 2, 3).await.unwrap();
        assert_eq!(read_handle(ranged.into_iter().next().unwrap()), b"bbb");

        // After delete the block is gone, not served from a pinned fd.
        store.delete(&id).await.unwrap();
        assert!(matches!(
            store.get_block(&id).await,
            Err(Error::NotFound(_))
        ));

        std::fs::remove_dir_all(&root).ok();
    }

    /// An in-flight handle keeps the bytes readable even if the block is
    /// deleted mid-request, matching the pre-cache ownership semantics.
    #[tokio::test]
    async fn handle_outlives_delete() {
        use std::io::Read;
        let root = tmp_root();
        let store = WholeBlockStore::open(&root).unwrap();
        let id = block(43);
        store
            .put(&id, Bytes::from_static(b"payload"))
            .await
            .unwrap();

        let handle = store.get_block(&id).await.unwrap();
        store.delete(&id).await.unwrap();

        let mut f = std::fs::File::from(handle.fd.try_clone().unwrap());
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"payload");

        std::fs::remove_dir_all(&root).ok();
    }
}
