use super::*;
use crate::{
    eviction::{Lru, SharedUnit, UnitKind},
    index::BlockIndex,
    page_lifecycle::{PageEntry, PageLifecycle},
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicIsize, Ordering::Relaxed},
};
use talon_core::{Backend, ObjectId, Version};

struct Allocator;
static REQUESTED: AtomicIsize = AtomicIsize::new(0);
static USABLE: AtomicIsize = AtomicIsize::new(0);
static SIZES: [AtomicIsize; 4096] = [const { AtomicIsize::new(0) }; 4096];

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

unsafe fn account(ptr: *mut u8, size: usize, direction: isize) {
    if !ptr.is_null() {
        REQUESTED.fetch_add(direction * size as isize, Relaxed);
        USABLE.fetch_add(
            direction * libc::malloc_usable_size(ptr.cast()) as isize,
            Relaxed,
        );
        if size < SIZES.len() {
            SIZES[size].fetch_add(direction, Relaxed);
        }
    }
}

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        account(ptr, layout.size(), 1);
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        account(ptr, layout.size(), 1);
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        account(ptr, layout.size(), -1);
        System.dealloc(ptr, layout);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        account(ptr, layout.size(), -1);
        let new_ptr = System.realloc(ptr, layout, new_size);
        if new_ptr.is_null() {
            account(ptr, layout.size(), 1);
        } else {
            account(new_ptr, new_size, 1);
        }
        new_ptr
    }
}

#[derive(Clone, Copy)]
struct Snapshot {
    requested: isize,
    usable: isize,
    sizes: [isize; 4096],
}
impl Snapshot {
    fn now() -> Self {
        Self {
            requested: REQUESTED.load(Relaxed),
            usable: USABLE.load(Relaxed),
            sizes: std::array::from_fn(|i| SIZES[i].load(Relaxed)),
        }
    }
    fn report(self, label: &str, count: usize, before: Self) {
        let requested = self.requested - before.requested;
        let usable = self.usable - before.usable;
        eprintln!(
            "HEAP|{label}|{count}|{requested}|{usable}|{:.3}|{:.3}",
            requested as f64 / count as f64,
            usable as f64 / count as f64
        );
        for (size, (after, before)) in self.sizes.iter().zip(before.sizes).enumerate() {
            if *after != before {
                eprintln!("ALLOCS|{label}|{size}|{}", after - before);
            }
        }
    }
}

fn measure<T>(label: &str, count: usize, run: impl FnOnce() -> T) -> T {
    let before = Snapshot::now();
    let value = run();
    Snapshot::now().report(label, count, before);
    value
}

fn identity(n: usize) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "b".repeat(16), format!("{n:096}")),
        0,
        256 << 20,
        Version::new("v".repeat(32)),
    )
}

fn scenario(count: usize, pages: u32, tracked: bool, tti: bool) {
    let label = format!(
        "{}-p{pages}-tracked{tracked}-tti{tti}",
        if pages == 0 { "whole" } else { "paged" }
    );
    eprintln!("SCENARIO|{label}|blocks={count}|pages={pages}|text=144");
    let mut index = BlockIndex::new();
    let baseline = Snapshot::now();
    let keys: Vec<_> = measure("identity_with_temporary_vec", count, || {
        (0..count).map(|n| BlockKey::new(&identity(n))).collect()
    });
    let catalog: HashMap<_, _> = measure("catalog_excluding_identity", count, || {
        keys.iter()
            .map(|key| {
                let block = Block {
                    id: key.clone(),
                    len: u64::from(pages.max(1)) << 20,
                    pages: (0..pages.max(1)).map(|p| (p, 1 << 20)).collect(),
                };
                (key.clone(), Arc::new(block))
            })
            .collect()
    });
    measure("resident_index_excluding_identity", count, || {
        let blocks = keys
            .iter()
            .map(|key| StoredBlock {
                meta: StoredMeta {
                    id: key.clone(),
                    form: if pages == 0 {
                        BlockForm::Whole
                    } else {
                        BlockForm::Paged {
                            page_size: 1 << 20,
                            present: (0..pages).fold(PresentBitmap::new(256), |mut b, p| {
                                b.set(PageIndex(p));
                                b
                            }),
                        }
                    },
                    len: u64::from(pages.max(1)) << 20,
                },
                allocated: (0..pages.max(1))
                    .map(|p| ((pages != 0).then_some(PageIndex(p)), 1 << 20))
                    .collect(),
            })
            .collect();
        index.restore_allocated(blocks, 1);
    });
    let before_owner = Snapshot::now();
    let index = Arc::new(index);
    let after_owner = Snapshot::now();
    after_owner.report("fixed_index_owner_per_scope", 1, before_owner);
    let lru = measure("capacity_policy", count, || {
        let units = index.restore_policy_units(tracked);
        Lru::from_recovered(index.clone(), units, count, tracked, tracked)
    });
    let before_lifecycle = Snapshot::now();
    let lifecycle = measure("fixed_lifecycle_per_scope", 1, || {
        PageLifecycle::with_index(index.clone(), tti)
    });
    let after_lifecycle = Snapshot::now();
    if pages != 0 && tti {
        measure("lifecycle_binding", count, || {
            index.restore_page_states(&lifecycle);
        });
    }
    drop(keys);
    let end = Snapshot::now();
    end.report("total_including_fixed_lifecycle", count, baseline);
    let requested = end.requested
        - baseline.requested
        - (after_lifecycle.requested - before_lifecycle.requested)
        - (after_owner.requested - before_owner.requested);
    let usable = end.usable
        - baseline.usable
        - (after_lifecycle.usable - before_lifecycle.usable)
        - (after_owner.usable - before_owner.usable);
    eprintln!(
        "TOTAL|{label}|{count}|{pages}|{requested}|{usable}|{:.3}|{:.3}",
        requested as f64 / count as f64,
        usable as f64 / count as f64
    );
    assert_eq!(index.len(), count);
    assert_eq!(index.page_count(), count as u64 * u64::from(pages));
    assert_eq!(
        lru.total_bytes(),
        count as u64 * u64::from(pages.max(1)) * (1 << 20)
    );
    if !tti {
        measure("on_demand_lifecycle_binding", count, || {
            if pages == 0 {
                for key in catalog.keys() {
                    lifecycle.block(&key.materialize());
                }
            } else {
                index.restore_page_states(&lifecycle);
            }
        });
    }
    std::hint::black_box((&catalog, &index, &lru, &lifecycle));
}

#[test]
#[ignore = "manual retained-memory and type-layout analysis"]
fn store_memory_breakdown() {
    crate::block_key::memory_probe_layouts();
    crate::index::memory_probe_layouts();
    crate::eviction::memory_probe_layouts();
    crate::page_lifecycle::memory_probe_layouts();
    crate::fd_cache::memory_probe_layouts();
    crate::runtime::memory_probe_layouts();
    for (name, size) in [
        ("CatalogBlock", std::mem::size_of::<Block>()),
        ("CatalogPageMap", std::mem::size_of::<PageMap<u64>>()),
        ("Store", std::mem::size_of::<Store>()),
        ("State", std::mem::size_of::<State>()),
    ] {
        eprintln!("LAYOUT|catalog|{name}|{size}|8");
    }
    // Initialize process-wide timing and thread bookkeeping before the large fixtures.
    scenario(16, 1, true, false);
    for tracked in [false, true] {
        scenario(1, 0, tracked, false);
        scenario(1, 1, tracked, false);
    }
    for (tracked, tti) in [(false, false), (true, false), (true, true)] {
        for pages in [0, 1, 2, 4, 16, 64, 256] {
            if pages == 0 && tti {
                continue;
            }
            let count = if pages <= 4 { 16_384 } else { 1024 };
            scenario(count, pages, tracked, tti);
        }
    }
    let key = BlockKey::new(&identity(0));
    for n in [1_usize, 2, 4, 16, 64, 256, 16_384] {
        let map: BTreeMap<_, _> = measure("capacity_queue_bulk", n, || {
            (0..n)
                .map(|p| {
                    (
                        p as u64,
                        SharedUnit {
                            block: key.clone(),
                            kind: UnitKind::Page(PageIndex(p as u32)),
                        },
                    )
                })
                .collect()
        });
        drop(map);
        let map = measure("capacity_queue_incremental", n, || {
            let mut map = BTreeMap::new();
            for p in 0..n {
                map.insert(
                    p as u64,
                    SharedUnit {
                        block: key.clone(),
                        kind: UnitKind::Page(PageIndex(p as u32)),
                    },
                );
            }
            map
        });
        drop(map);
        let map: BTreeMap<_, _> = measure("block_queue_bulk", n, || {
            (0..n).map(|p| (p as u64, key.clone())).collect()
        });
        drop(map);
        let map: PageMap = measure("page_table_nodes_and_states", n, || {
            (0..n).map(|p| (p as u32, PageEntry::unbound())).collect()
        });
        drop(map);
        let map: PageMap<u64> = measure("catalog_page_nodes", n, || {
            (0..n).map(|p| (p as u32, 4096)).collect()
        });
        drop(map);
    }
}
