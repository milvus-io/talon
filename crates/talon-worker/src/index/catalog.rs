//! Runtime reads over the journal's file geometry.
use super::*;
use crate::catalog::{Geometry, StoreKind};

pub(super) fn metadata(key: &BlockKey, geometry: &Geometry, page_size: u32) -> BlockMeta {
    let form = if geometry.kind() == Some(StoreKind::Whole) {
        BlockForm::Whole
    } else {
        let mut present = PresentBitmap::new(key.view().page_count(page_size));
        for (page, _) in geometry.pages_after(None) {
            present.set(PageIndex(page));
        }
        BlockForm::Paged { page_size, present }
    };
    BlockMeta {
        id: key.materialize(),
        form,
        len: geometry.len(),
    }
}

impl BlockIndex {
    pub(crate) fn from_catalog(entries: Arc<RwLock<Directory>>) -> Self {
        let (page_work, compact) = {
            let directory = entries.read().unwrap();
            (directory.page_work.clone(), directory.compact.clone())
        };
        Self {
            entries,
            page_work,
            compact,
            catalog: true,
            ..Self::default()
        }
    }

    pub(crate) fn uses_catalog(&self) -> bool {
        self.catalog
    }

    pub(crate) fn shares_directory(&self, directory: &Arc<RwLock<Directory>>) -> bool {
        Arc::ptr_eq(&self.entries, directory)
    }

    pub(crate) fn has_journaled_pages(&self, id: &BlockId) -> bool {
        self.catalog
            && self
                .entries
                .read()
                .unwrap()
                .store_geometry(StoreKind::Paged, id)
                .is_some()
    }

    pub(crate) fn publish_page(
        &self,
        id: &BlockId,
        page: PageIndex,
        page_size: u32,
        len: u64,
        allocated: u64,
        disk_id: Option<DiskId>,
    ) {
        if self.catalog {
            // The public attachment is a standalone sidecar. Journal-backed
            // namespace stores publish into their own directory after sync.
            debug_assert!(disk_id.is_none());
            self.entries
                .write()
                .unwrap()
                .publish_sidecar_page(id, page, page_size, len, allocated);
        } else {
            self.init_paged(id.clone(), page_size, len);
            self.mark_page_allocated_at(id, page, allocated, disk_id);
        }
    }

    pub(super) fn catalog_read_pages(
        self: &Arc<Self>,
        id: &BlockId,
        start: PageIndex,
        end: PageIndex,
    ) -> Option<(Option<DiskId>, PageReadGuard)> {
        // Drop any partially acquired range after releasing the directory lock.
        let (disk_id, read) = {
            let entries = self.entries.read().unwrap();
            let slot = entries.slot(id)?;
            let (_, geometry, _) = entries.resident_at(slot)?;
            let touch = |state: &crate::page_lifecycle::PageWord, ()| {
                state.touch(entries.clock(slot));
            };
            if start.0 + 1 == end.0 {
                let row = geometry.page_row(start.0, None)?;
                let read = PageReadGuard::acquire(
                    self,
                    slot,
                    start.0,
                    std::iter::once((row.state, ())),
                    touch,
                )?;
                return Some((geometry.disk_id, read));
            }
            let rows = (start.0..end.0)
                .zip(geometry.page_rows_from(start.0, entries.inline_age(slot)))
                .map_while(|(want, (found, row))| (want == found).then_some((row.state, ())));
            let read = PageReadGuard::acquire(self, slot, start.0, rows, touch)?;
            (geometry.disk_id, read)
        };
        read.covers(end.0 - start.0).then_some((disk_id, read))
    }

    /// Under the block mutation gate, sample recency and claim the complete
    /// range with one exclusive directory borrow. A busy journal publisher
    /// leaves this candidate eligible for the next ownership attempt.
    pub(crate) fn try_claim_catalog_ownership(
        &self,
        candidate: &crate::eviction::StaleBlockCandidate,
        claim: impl FnOnce(usize, usize) -> Option<()>,
    ) -> Option<u64> {
        let entries = crate::runtime::uncontended(self.entries.try_write())?;
        let slot = entries.slot(&candidate.block)?;
        let (_, geometry, entry) = entries.resident_at(slot)?;
        let pins = self.compact.try_block_pins(slot)?;
        let activity = self.block_activity_with_pins(&entries, slot, || pins)?;
        if !candidate.matches(&activity) {
            return None;
        }
        if geometry.kind() == Some(StoreKind::Whole) {
            return Some(geometry.len());
        }
        claim(slot, geometry.page_count())?;
        let page_size = entries.page_size(entry);
        Some(
            geometry
                .pages_after(None)
                .map(|(page, _)| talon_core::page_len(geometry.len(), page_size, PageIndex(page)))
                .sum(),
        )
    }
}
