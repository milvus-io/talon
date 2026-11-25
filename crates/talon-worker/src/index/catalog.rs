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

    pub(super) fn catalog_read_pages(
        self: &Arc<Self>,
        id: &BlockId,
        start: PageIndex,
        end: PageIndex,
    ) -> Option<(Option<DiskId>, PageReadGuard, Vec<PageReadGuard>)> {
        // Drop any partially acquired range after releasing the directory lock.
        let first;
        let mut tail = Vec::new();
        let disk_id = {
            let entries = self.entries.read().unwrap();
            let slot = entries.slot(id)?;
            let (_, geometry, _) = entries.resident_at(slot)?;
            let mut rows = geometry.page_rows_from(start.0, entries.inline_age(slot));
            let mut acquire = |page| {
                let (found, row) = rows.next()?;
                if found != page {
                    return None;
                }
                let guard = PageReadGuard::acquire(self, slot, page, row.state)?;
                row.state.touch(entries.clock(slot));
                Some(guard)
            };
            first = acquire(start.0)?;
            for page in start.0 + 1..end.0 {
                tail.push(acquire(page)?);
            }
            geometry.disk_id
        };
        Some((disk_id, first, tail))
    }
}
