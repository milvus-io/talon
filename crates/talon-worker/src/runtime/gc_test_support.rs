//! Shared cache fixtures for independent maintenance tests.
use super::WorkerRuntime;
use crate::eviction::access_millis;
use crate::{BlockIndex, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics};
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use talon_core::{Backend, BackendStore, BlockId, Error, ObjectId, ObjectStat, PageIndex, Version};
const DAY_MS: u64 = 24 * 60 * 60 * 1000;
#[derive(Default)]
pub(super) struct Origin {
    pub(super) calls: AtomicUsize,
    pub(super) mode: AtomicUsize, // 0 missing, 1 exists, 2 forbidden, 3 timeout
    pub(super) hold: AtomicBool,
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl BackendStore for Origin {
    async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> talon_core::Result<Bytes> {
        panic!("GC must not fetch data")
    }
    async fn head(&self, object: &ObjectId) -> talon_core::Result<ObjectStat> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.entered.notify_one();
        if self.hold.load(Ordering::Relaxed) {
            self.release.notified().await;
        }
        match self.mode.load(Ordering::Relaxed) {
            0 => Err(Error::NotFound(object.to_path())),
            1 => Ok(ObjectStat {
                len: 32,
                version: Version::new("new-version"),
            }),
            2 => Err(Error::Backend("403 Forbidden".into())),
            _ => std::future::pending().await,
        }
    }
}
pub(super) fn block(n: u64) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", "key"),
        n * 32,
        32,
        Version::new("v1"),
    )
}
pub(super) fn runtime(root: &std::path::Path, paged: bool, origin: Arc<Origin>) -> WorkerRuntime {
    let store = WholeBlockStore::open(root.join("whole")).unwrap();
    let index = Arc::new(BlockIndex::new());
    for meta in store.scan().unwrap() {
        index.commit(meta);
    }
    let r = WorkerRuntime::new(
        store,
        index,
        Arc::new(InFlightLoads::new()),
        origin,
        32,
        0,
        WorkerMetrics::new(0),
    );
    if paged {
        r.with_paged_store(PagedBlockStore::open(root.join("paged"), 16).unwrap())
    } else {
        r
    }
}
pub(super) async fn fill(r: &WorkerRuntime, id: &BlockId, paged: bool) {
    if paged {
        for page in 0..2 {
            r.commit_fetched_page(id, PageIndex(page), 32, Bytes::from_static(&[1; 16]))
                .await
                .unwrap();
        }
    } else {
        r.commit_cached_block(id, Bytes::from_static(&[1; 32]))
            .await
            .unwrap();
    }
}
pub(super) fn cold_now() -> u64 {
    access_millis() + DAY_MS + 1
}
