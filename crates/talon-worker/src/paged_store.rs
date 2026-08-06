//! Paged block store: per-page files under a block directory.
//!
//! A *paged* block is materialized as a directory containing one `.page` file
//! per resident page — **not** a sparse file. This lets point queries fetch and
//! evict individual pages while the block as a whole stays addressable.
//!
//! # Layout
//!
//! ```text
//! <root>/<shard>/<digest>.pages/
//!     <page_index>.page      # one file per resident page
//! ```
//!
//! `digest`/`shard` mirror the whole-block store ([`WholeBlockStore`]).
//!
//! [`get_page`](PagedBlockStore::get_page) opens a page's fd;
//! [`get_range`](PagedBlockStore::get_range) returns one [`BlockHandle`] per
//! present page covering the range, coalescing contiguous present pages into a
//! single handle. Any absent covered page yields [`Error::NotFound`] carrying
//! the `(block, page)` context so the caller can trigger a page-level miss.
//!
//! [`WholeBlockStore`]: crate::WholeBlockStore

use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use talon_core::{
    BlockForm, BlockHandle, BlockId, BlockMeta, Error, PageIndex, PresentBitmap, Result,
};

/// Process-wide counter making staging temp filenames unique, so two concurrent
/// writers of the same page never share a `.tmp` path (mirrors the whole-block
/// store's discipline, issue #113).
static STAGING_SEQ: AtomicU64 = AtomicU64::new(0);

/// Run blocking filesystem work on Tokio's blocking pool, so page I/O never
/// stalls the async reactor thread and the connections multiplexed on it
/// (issue #115).
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

/// A local, file-backed store for paged blocks (per-page files).
pub struct PagedBlockStore {
    root: PathBuf,
    page_size: u32,
}

impl PagedBlockStore {
    /// Open (creating if needed) a paged store rooted at `root`, using
    /// `page_size`-byte pages.
    pub fn open(root: impl Into<PathBuf>, page_size: u32) -> Result<Self> {
        if page_size == 0 {
            return Err(Error::Other("page_size must be > 0".into()));
        }
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, page_size })
    }

    /// The configured page size in bytes.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// The cache root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Directory holding a block's page files: `<root>/<shard>/<digest>.pages`.
    fn dir_for(&self, id: &BlockId) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let digest = hasher.finish();
        let hex = format!("{digest:016x}");
        self.root.join(&hex[0..2]).join(format!("{hex}.pages"))
    }

    /// Path of a single page file within a block directory.
    fn page_path(&self, id: &BlockId, page: PageIndex) -> PathBuf {
        self.dir_for(id).join(format!("{}.page", page.0))
    }

    /// Sidecar metadata path for a paged block: `<block dir>/block.meta`.
    ///
    /// The directory name is a one-way digest of the [`BlockId`], so the id
    /// cannot be recovered from the page files alone. This sidecar records the
    /// block's id, page size, and logical length so [`scan`](Self::scan) can
    /// rebuild the index (and the present bitmap, from the page files actually
    /// on disk) after a restart — the paged analogue of issue #114.
    fn meta_path_for(&self, id: &BlockId) -> PathBuf {
        self.dir_for(id).join("block.meta")
    }

    /// Record a paged block's identity so a restart can rebuild its index entry.
    ///
    /// Called once when a paged block is first touched. Best-effort: a missing
    /// sidecar only costs a re-fetch of that block's pages after a restart.
    pub fn write_sidecar(&self, id: &BlockId, len: u64) -> Result<()> {
        let dir = self.dir_for(id);
        std::fs::create_dir_all(&dir)?;
        let meta_path = self.meta_path_for(id);
        if meta_path.exists() {
            return Ok(());
        }
        // The bitmap is not persisted: page files on disk are the source of
        // truth, so a crash between a page rename and a bitmap write cannot
        // desynchronize them. `scan` reconstructs presence by listing `.page`s.
        let meta = BlockMeta {
            id: id.clone(),
            form: BlockForm::Paged {
                page_size: self.page_size,
                present: PresentBitmap::new(id.page_count(self.page_size)),
            },
            len,
        };
        let encoded = serde_json::to_vec(&meta)
            .map_err(|e| Error::Other(format!("encode paged block sidecar: {e}")))?;
        let pid = std::process::id();
        let seq = STAGING_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!("block.meta.tmp.{pid}.{seq}"));
        match (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&encoded)?;
            f.sync_all()?;
            std::fs::rename(&tmp, &meta_path)
        })() {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e.into())
            }
        }
    }

    /// Scan the cache root and return the [`BlockMeta`] of every paged block,
    /// with each present bitmap reconstructed from the `.page` files on disk.
    ///
    /// A block directory without a readable sidecar is skipped (its pages are
    /// unaddressable), as is one with no page files at all.
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
                let dir = entry.path();
                if dir.extension().and_then(|e| e.to_str()) != Some("pages") {
                    continue;
                }
                let Ok(bytes) = std::fs::read(dir.join("block.meta")) else {
                    continue;
                };
                let mut meta = match serde_json::from_slice::<BlockMeta>(&bytes) {
                    Ok(meta) => meta,
                    Err(error) => {
                        tracing::warn!(path = %dir.display(), %error, "skipping malformed paged sidecar");
                        continue;
                    }
                };
                let BlockForm::Paged { page_size, present } = &mut meta.form else {
                    continue;
                };
                // A sidecar written under a different page size cannot be
                // reinterpreted at the current one; drop it rather than serve
                // misaligned pages.
                if *page_size != self.page_size {
                    tracing::warn!(
                        path = %dir.display(),
                        sidecar_page_size = *page_size,
                        configured_page_size = self.page_size,
                        "skipping paged block written under a different page size"
                    );
                    continue;
                }
                let Ok(pages) = std::fs::read_dir(&dir) else {
                    continue;
                };
                for page_file in pages.flatten() {
                    let path = page_file.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("page") {
                        continue;
                    }
                    let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                        continue;
                    };
                    if let Ok(index) = stem.parse::<u32>() {
                        present.set(PageIndex(index));
                    }
                }
                if present.count() > 0 {
                    out.push(meta);
                }
            }
        }
        Ok(out)
    }

    /// Whether a specific page is resident.
    pub fn has_page(&self, id: &BlockId, page: PageIndex) -> bool {
        self.page_path(id, page).exists()
    }

    /// Materialize (commit) one page's bytes via temp-file + rename.
    ///
    /// The temp name carries pid + a process-wide sequence so two concurrent
    /// writers of the same page cannot clobber each other's staging file and
    /// commit a torn page (issue #113).
    pub fn put_page(&self, id: &BlockId, page: PageIndex, value: Bytes) -> Result<()> {
        let dir = self.dir_for(id);
        std::fs::create_dir_all(&dir)?;
        let path = self.page_path(id, page);
        let pid = std::process::id();
        let seq = STAGING_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!("{}.page.tmp.{pid}.{seq}", page.0));
        match (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&value)?;
            f.sync_all()?;
            std::fs::rename(&tmp, &path)
        })() {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e.into())
            }
        }
    }

    /// Commit one page's bytes off the async reactor thread.
    pub async fn put_page_async(&self, id: &BlockId, page: PageIndex, value: Bytes) -> Result<()> {
        let dir = self.dir_for(id);
        let path = self.page_path(id, page);
        spawn_blocking_io(move || {
            std::fs::create_dir_all(&dir)?;
            let pid = std::process::id();
            let seq = STAGING_SEQ.fetch_add(1, Ordering::Relaxed);
            let tmp = path.with_extension(format!("page.tmp.{pid}.{seq}"));
            match (|| -> std::io::Result<()> {
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(&value)?;
                f.sync_all()?;
                std::fs::rename(&tmp, &path)
            })() {
                Ok(()) => Ok(()),
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp);
                    Err(e.into())
                }
            }
        })
        .await
    }

    /// Read one page's bytes into userspace, off the async reactor thread.
    ///
    /// Returns [`Error::NotFound`] if the page is absent, so the caller can
    /// trigger a page-level miss.
    pub async fn get_page_bytes(&self, id: &BlockId, page: PageIndex) -> Result<Bytes> {
        let path = self.page_path(id, page);
        let label = format!("{id} page {}", page.0);
        spawn_blocking_io(move || match std::fs::File::open(&path) {
            Ok(f) => {
                let len = f.metadata()?.len();
                let size = usize::try_from(len)
                    .map_err(|_| Error::Other(format!("page length {len} exceeds usize")))?;
                let mut buffer = vec![0_u8; size];
                f.read_exact_at(&mut buffer, 0)?;
                Ok(Bytes::from(buffer))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::NotFound(label)),
            Err(e) => Err(e.into()),
        })
        .await
    }

    /// Remove a single page off the async reactor thread. Idempotent.
    pub async fn evict_page_async(&self, id: &BlockId, page: PageIndex) -> Result<()> {
        let path = self.page_path(id, page);
        spawn_blocking_io(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        })
        .await
    }

    /// Remove an entire paged block off the async reactor thread. Idempotent.
    pub async fn delete_block_async(&self, id: &BlockId) -> Result<()> {
        let dir = self.dir_for(id);
        spawn_blocking_io(move || match std::fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        })
        .await
    }

    /// Open a resident page as a zero-copy handle over its whole file.
    pub fn get_page(&self, id: &BlockId, page: PageIndex) -> Result<BlockHandle> {
        let path = self.page_path(id, page);
        match std::fs::File::open(&path) {
            Ok(f) => {
                let len = f.metadata()?.len();
                Ok(BlockHandle::new(OwnedFd::from(f), 0, len))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(format!("{id} page {}", page.0)))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Return handles covering `[offset, offset + len)` across present pages.
    ///
    /// One handle per present page (sub-ranged at the ends), with contiguous
    /// present pages coalesced into a single handle where their files are
    /// adjacent on disk — since pages are separate files, coalescing here means
    /// returning one handle per page but merging the intra-page ranges. Any
    /// absent covered page yields [`Error::NotFound`].
    pub fn get_range(&self, id: &BlockId, offset: u64, len: u64) -> Result<Vec<BlockHandle>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let ps = self.page_size as u64;
        let start_page = (offset / ps) as u32;
        let end_byte = offset + len - 1;
        let end_page = (end_byte / ps) as u32;

        let mut handles = Vec::new();
        for p in start_page..=end_page {
            let page = PageIndex(p);
            let page_start = p as u64 * ps;
            // Intersection of the requested range with this page's byte span.
            let from = offset.max(page_start);
            let to = (offset + len).min(page_start + ps);
            let in_page_off = from - page_start;
            let in_page_len = to - from;

            let handle = self.get_page(id, page)?; // NotFound propagates
                                                   // Clamp to the actually-present bytes of the page file.
            let avail = handle.len.saturating_sub(in_page_off);
            let serve = in_page_len.min(avail);
            handles.push(BlockHandle::new(handle.fd, in_page_off, serve));
        }
        Ok(handles)
    }

    /// Remove a single page (e.g. page-level eviction), leaving the block dir
    /// and other pages intact. Idempotent.
    pub fn evict_page(&self, id: &BlockId, page: PageIndex) -> Result<()> {
        match std::fs::remove_file(self.page_path(id, page)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Remove an entire paged block (all pages + directory). Idempotent.
    pub fn delete_block(&self, id: &BlockId) -> Result<()> {
        match std::fs::remove_dir_all(self.dir_for(id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;
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
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        let mut p = std::env::temp_dir();
        p.push(format!("talon-paged-{}-{}", std::process::id(), h.finish()));
        p
    }

    fn read_all(h: BlockHandle) -> Vec<u8> {
        let mut f = std::fs::File::from(h.fd);
        use std::io::Seek;
        f.seek(std::io::SeekFrom::Start(h.offset)).unwrap();
        let mut buf = vec![0u8; h.len as usize];
        f.read_exact(&mut buf).unwrap();
        buf
    }

    #[test]
    fn put_get_and_absent_page() {
        let root = tmp_root();
        let store = PagedBlockStore::open(&root, 4).unwrap();
        let id = block(1);

        assert!(!store.has_page(&id, PageIndex(0)));
        assert!(matches!(
            store.get_page(&id, PageIndex(0)),
            Err(Error::NotFound(_))
        ));

        store
            .put_page(&id, PageIndex(0), Bytes::from_static(b"abcd"))
            .unwrap();
        assert!(store.has_page(&id, PageIndex(0)));
        let h = store.get_page(&id, PageIndex(0)).unwrap();
        assert_eq!(read_all(h), b"abcd");

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn range_spanning_multiple_present_pages() {
        let root = tmp_root();
        let store = PagedBlockStore::open(&root, 4).unwrap();
        let id = block(2);
        // pages: 0=[abcd] 1=[efgh] 2=[ijkl]
        store
            .put_page(&id, PageIndex(0), Bytes::from_static(b"abcd"))
            .unwrap();
        store
            .put_page(&id, PageIndex(1), Bytes::from_static(b"efgh"))
            .unwrap();
        store
            .put_page(&id, PageIndex(2), Bytes::from_static(b"ijkl"))
            .unwrap();

        // Read bytes [2, 10): tail of p0 "cd", all p1 "efgh", head of p2 "ij".
        let handles = store.get_range(&id, 2, 8).unwrap();
        assert_eq!(handles.len(), 3);
        let bytes: Vec<u8> = handles.into_iter().flat_map(read_all).collect();
        assert_eq!(bytes, b"cdefghij");

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn range_with_absent_page_is_notfound() {
        let root = tmp_root();
        let store = PagedBlockStore::open(&root, 4).unwrap();
        let id = block(3);
        store
            .put_page(&id, PageIndex(0), Bytes::from_static(b"abcd"))
            .unwrap();
        // page 1 missing
        let err = store.get_range(&id, 0, 8).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
        assert!(err.to_string().contains("page 1"));

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn evict_page_leaves_block_intact() {
        let root = tmp_root();
        let store = PagedBlockStore::open(&root, 4).unwrap();
        let id = block(4);
        store
            .put_page(&id, PageIndex(0), Bytes::from_static(b"abcd"))
            .unwrap();
        store
            .put_page(&id, PageIndex(1), Bytes::from_static(b"efgh"))
            .unwrap();

        store.evict_page(&id, PageIndex(0)).unwrap();
        assert!(!store.has_page(&id, PageIndex(0)));
        assert!(store.has_page(&id, PageIndex(1))); // sibling intact
        store.evict_page(&id, PageIndex(0)).unwrap(); // idempotent

        store.delete_block(&id).unwrap();
        assert!(!store.has_page(&id, PageIndex(1)));
        store.delete_block(&id).unwrap(); // idempotent

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn scan_rebuilds_presence_from_the_page_files_on_disk() {
        let root = tmp_root();
        let store = PagedBlockStore::open(&root, 4).unwrap();
        let id = block(5);
        store.write_sidecar(&id, 20).unwrap();
        store
            .put_page(&id, PageIndex(1), Bytes::from_static(b"efgh"))
            .unwrap();
        store
            .put_page(&id, PageIndex(3), Bytes::from_static(b"mnop"))
            .unwrap();

        let metas = store.scan().unwrap();
        assert_eq!(metas.len(), 1);
        let meta = &metas[0];
        assert_eq!(meta.id, id);
        assert_eq!(meta.len, 20);
        let BlockForm::Paged { page_size, present } = &meta.form else {
            panic!("expected a paged form");
        };
        assert_eq!(*page_size, 4);
        // Exactly the pages with files on disk are marked present.
        assert!(!present.is_present(PageIndex(0)));
        assert!(present.is_present(PageIndex(1)));
        assert!(!present.is_present(PageIndex(2)));
        assert!(present.is_present(PageIndex(3)));
        assert_eq!(present.count(), 2);
        // Only present pages are charged, not the block's full length.
        assert_eq!(meta.resident_bytes(), 8);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn scan_skips_a_block_written_under_a_different_page_size() {
        let root = tmp_root();
        {
            let store = PagedBlockStore::open(&root, 4).unwrap();
            let id = block(6);
            store.write_sidecar(&id, 20).unwrap();
            store
                .put_page(&id, PageIndex(0), Bytes::from_static(b"abcd"))
                .unwrap();
            assert_eq!(store.scan().unwrap().len(), 1);
        }
        // Reopening at a different page size must not reinterpret those pages:
        // page 1 at 4 bytes is not page 1 at 8 bytes.
        let store = PagedBlockStore::open(&root, 8).unwrap();
        assert!(store.scan().unwrap().is_empty());

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn scan_ignores_a_block_with_a_sidecar_but_no_pages() {
        let root = tmp_root();
        let store = PagedBlockStore::open(&root, 4).unwrap();
        let id = block(7);
        store.write_sidecar(&id, 20).unwrap();

        // A sidecar alone contributes nothing resident.
        assert!(store.scan().unwrap().is_empty());

        std::fs::remove_dir_all(&root).ok();
    }
}
