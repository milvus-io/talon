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
}
