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
        let page_work = entries.read().unwrap().page_work.clone();
        Self {
            entries,
            page_work,
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
            let (_, geometry, runtime) = entries.resident_at(slot)?;
            let mut rows = geometry.page_rows_from(start.0);
            let mut acquire = |page| {
                let (found, row) = rows.next()?;
                if found != page {
                    return None;
                }
                let guard = PageReadGuard::acquire(self, slot, page, row.state)?;
                runtime.policy.touch_catalog_page(page);
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

    pub(super) fn catalog_recover_policy(
        &self,
        track_clock: bool,
    ) -> Vec<crate::eviction::RecoveredUnit> {
        let entries = self.entries.read().unwrap();
        let (blocks, pages, _) = entries.totals();
        let mut units = Vec::with_capacity((blocks + pages) as usize);
        for (key, geometry) in entries.residents() {
            let clock =
                track_clock.then(|| Arc::new(AtomicU64::new(crate::eviction::access_millis())));
            for (page, bytes) in geometry.pages_after(None) {
                let kind = if geometry.kind() == Some(StoreKind::Whole) {
                    UnitKind::Whole
                } else {
                    UnitKind::Page(PageIndex(page))
                };
                units.push(crate::eviction::RecoveredUnit {
                    independent: false,
                    unit: SharedUnit {
                        block: key.clone(),
                        kind,
                    },
                    bytes,
                    access: AccessHandle::with_state(Arc::default(), clock.clone()),
                });
            }
        }
        units
    }
}

pub(super) fn touch_whole(runtime: &DirectoryEntry) {
    if let Some(slot) = &runtime.policy.whole {
        if let Some(access) = &slot.read().unwrap().access {
            access.touch();
        }
    }
}
