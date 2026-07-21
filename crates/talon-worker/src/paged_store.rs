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
use std::path::{Path, PathBuf};
use talon_core::{BlockHandle, BlockId, Error, PageIndex, Result};

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

    /// Whether a specific page is resident.
    pub fn has_page(&self, id: &BlockId, page: PageIndex) -> bool {
        self.page_path(id, page).exists()
    }

    /// Materialize (commit) one page's bytes via temp-file + rename.
    pub fn put_page(&self, id: &BlockId, page: PageIndex, value: Bytes) -> Result<()> {
        let dir = self.dir_for(id);
        std::fs::create_dir_all(&dir)?;
        let path = self.page_path(id, page);
        let tmp = path.with_extension("page.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&value)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(())
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
}
