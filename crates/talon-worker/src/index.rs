//! In-memory block index.
//!
//! The [`BlockIndex`] maps each resident [`BlockId`] to its [`BlockMeta`]
//! (physical [`BlockForm`] + length) and tracks total resident bytes for
//! eviction. It is the central structure shared by the data path (hit/miss
//! decisions), the miss/loader path (commit on completion), and eviction
//! (byte accounting).
//!
//! Access is synchronized with a single [`RwLock`]; reads (presence/lookup)
//! take a shared lock, mutations (commit/remove/page updates) take an exclusive
//! one. This is deliberately simple for v1; a sharded map can replace it later
//! if the lock becomes hot.

use std::collections::HashMap;
use std::sync::RwLock;

use talon_core::{BlockForm, BlockId, BlockMeta, PageIndex, PresentBitmap};

/// The result of a presence query — how a read should be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// The whole block is resident; serve directly.
    Whole,
    /// The block is paged and every requested page is resident.
    PageHit,
    /// The block is paged but at least one requested page is absent.
    PageMiss,
    /// The block is not in the index at all.
    Miss,
}

/// A thread-safe index of resident blocks.
#[derive(Default)]
pub struct BlockIndex {
    inner: RwLock<Inner>,
}

#[derive(Default)]
struct Inner {
    map: HashMap<BlockId, BlockMeta>,
    resident_bytes: u64,
}

impl BlockIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of blocks currently tracked.
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().map.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().map.is_empty()
    }

    /// Total resident bytes across all tracked blocks (for eviction).
    pub fn resident_bytes(&self) -> u64 {
        self.inner.read().unwrap().resident_bytes
    }

    /// Number of materialized pages across all paged blocks.
    pub fn page_count(&self) -> u64 {
        self.inner
            .read()
            .unwrap()
            .map
            .values()
            .map(|meta| match &meta.form {
                BlockForm::Whole => 0,
                BlockForm::Paged { present, .. } => u64::from(present.count()),
            })
            .sum()
    }

    /// Look up a copy of a block's metadata.
    pub fn get(&self, id: &BlockId) -> Option<BlockMeta> {
        self.inner.read().unwrap().map.get(id).cloned()
    }

    /// Snapshot the `(BlockId, len)` of every currently-tracked block.
    ///
    /// Used to seed the eviction tracker at startup from the index rebuilt off
    /// on-disk cache, so already-resident blocks count against capacity from the
    /// first request (issue #159).
    pub fn snapshot_lens(&self) -> Vec<(BlockId, u64)> {
        self.inner
            .read()
            .unwrap()
            .map
            .values()
            .map(|meta| (meta.id.clone(), meta.len))
            .collect()
    }

    /// Commit a fully-materialized block (whole or a complete paged set).
    ///
    /// Called by PUT and by loader completion. Replaces any existing entry;
    /// byte accounting is adjusted by the delta between old and new `len`.
    pub fn commit(&self, meta: BlockMeta) {
        let mut g = self.inner.write().unwrap();
        if let Some(prev) = g.map.insert(meta.id.clone(), meta.clone()) {
            g.resident_bytes = g.resident_bytes.saturating_sub(prev.len);
        }
        g.resident_bytes = g.resident_bytes.saturating_add(meta.len);
    }

    /// Insert (or reset) a paged block with an empty presence bitmap.
    ///
    /// Used when a paged load begins: pages are then filled in with
    /// [`mark_page`](Self::mark_page). `len` is the block's logical length.
    pub fn init_paged(&self, id: BlockId, page_size: u32, len: u64) {
        let page_count = id.page_count(page_size);
        let meta = BlockMeta {
            id,
            form: BlockForm::Paged {
                page_size,
                present: PresentBitmap::new(page_count),
            },
            len,
        };
        self.commit(meta);
    }

    /// Mark a page present on an existing paged block.
    ///
    /// Returns `true` if the block exists and is paged; `false` otherwise
    /// (whole blocks and unknown blocks are ignored).
    pub fn mark_page(&self, id: &BlockId, page: PageIndex) -> bool {
        let mut g = self.inner.write().unwrap();
        match g.map.get_mut(id) {
            Some(meta) => match &mut meta.form {
                BlockForm::Paged { present, .. } => {
                    present.set(page);
                    true
                }
                BlockForm::Whole => false,
            },
            None => false,
        }
    }

    /// Decide how a read over `[start_page, end_page)` should be served.
    ///
    /// `start_page`/`end_page` are only consulted for paged blocks; for a whole
    /// block the answer is always [`Presence::Whole`].
    pub fn presence(&self, id: &BlockId, start_page: PageIndex, end_page: PageIndex) -> Presence {
        let g = self.inner.read().unwrap();
        match g.map.get(id) {
            None => Presence::Miss,
            Some(meta) => match &meta.form {
                BlockForm::Whole => Presence::Whole,
                BlockForm::Paged { present, .. } => {
                    if present.range_present(start_page, end_page) {
                        Presence::PageHit
                    } else {
                        Presence::PageMiss
                    }
                }
            },
        }
    }

    /// Remove a block from the index, returning its metadata if present.
    ///
    /// Byte accounting is decremented by the removed block's `len`.
    pub fn remove(&self, id: &BlockId) -> Option<BlockMeta> {
        let mut g = self.inner.write().unwrap();
        let removed = g.map.remove(id);
        if let Some(meta) = &removed {
            g.resident_bytes = g.resident_bytes.saturating_sub(meta.len);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use talon_core::{Backend, ObjectId, Version};

    fn block(n: u64) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", format!("o/{n}")),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn whole_meta(id: BlockId, len: u64) -> BlockMeta {
        BlockMeta {
            id,
            form: BlockForm::Whole,
            len,
        }
    }

    #[test]
    fn commit_lookup_and_byte_accounting() {
        let idx = BlockIndex::new();
        assert!(idx.is_empty());

        idx.commit(whole_meta(block(1), 1000));
        idx.commit(whole_meta(block(2), 2000));
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.resident_bytes(), 3000);

        // Re-commit with a different length adjusts accounting by the delta.
        idx.commit(whole_meta(block(1), 1500));
        assert_eq!(idx.resident_bytes(), 3500);

        let m = idx.get(&block(2)).unwrap();
        assert_eq!(m.len, 2000);

        let removed = idx.remove(&block(1)).unwrap();
        assert_eq!(removed.len, 1500);
        assert_eq!(idx.resident_bytes(), 2000);
        assert!(idx.remove(&block(1)).is_none());
    }

    #[test]
    fn presence_drives_hit_vs_miss() {
        let idx = BlockIndex::new();
        let id = block(7);

        // Unknown block.
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::Miss
        );

        // Whole block always a whole hit.
        idx.commit(whole_meta(id.clone(), 10));
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(1)),
            Presence::Whole
        );

        // Paged block: pages absent -> miss, then present -> hit.
        let page_size = 256 * 1024;
        idx.init_paged(id.clone(), page_size, 256 << 20);
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(2)),
            Presence::PageMiss
        );

        assert!(idx.mark_page(&id, PageIndex(0)));
        assert!(idx.mark_page(&id, PageIndex(1)));
        assert_eq!(idx.page_count(), 2);
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(2)),
            Presence::PageHit
        );
        assert_eq!(
            idx.presence(&id, PageIndex(0), PageIndex(3)),
            Presence::PageMiss
        );

        // mark_page on a whole block is a no-op returning false.
        idx.commit(whole_meta(id.clone(), 10));
        assert!(!idx.mark_page(&id, PageIndex(0)));
    }

    #[test]
    fn concurrent_update_and_read() {
        let idx = Arc::new(BlockIndex::new());
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let idx = Arc::clone(&idx);
            handles.push(std::thread::spawn(move || {
                for n in 0..500u64 {
                    let id = block(t * 1000 + n);
                    idx.commit(whole_meta(id.clone(), 8));
                    let _ = idx.presence(&id, PageIndex(0), PageIndex(1));
                    let _ = idx.get(&id);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(idx.len(), 8 * 500);
        assert_eq!(idx.resident_bytes(), 8 * 500 * 8);
    }
}
