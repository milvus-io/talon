use std::sync::Arc;

use bytes::Bytes;
use talon_core::{BackendStore, NodeId, NodeInfo, NodeRole, ObjectId, ObjectStat, Result};
use talon_worker::dynamic_config::{reload_once, DynamicConfig};
use talon_worker::{
    BlockIndex, InFlightLoads, NamespaceRateLimiter, WholeBlockStore, WorkerObservability,
    WorkerRuntime,
};

struct UnusedOrigin;

#[async_trait::async_trait]
impl BackendStore for UnusedOrigin {
    async fn fetch_range(&self, _: &ObjectId, _: u64, _: u64) -> Result<Bytes> {
        unreachable!("config reload does not fetch objects")
    }

    async fn head(&self, _: &ObjectId) -> Result<ObjectStat> {
        unreachable!("config reload does not inspect objects")
    }
}

#[tokio::test]
async fn standalone_runtime_supports_limiter_installation_and_reload() {
    let dir = tempfile::tempdir().unwrap();
    let index = Arc::new(BlockIndex::new());
    let inflight = Arc::new(InFlightLoads::new());
    let observability = WorkerObservability::new(
        "cluster".into(),
        NodeInfo {
            id: NodeId::new("worker"),
            address: "127.0.0.1:1".into(),
            role: NodeRole::Worker,
        },
        "127.0.0.1:2".into(),
        1024,
        index.clone(),
        inflight.clone(),
    )
    .unwrap();
    let limiter = Arc::new(NamespaceRateLimiter::disabled());
    let runtime: WorkerRuntime = WorkerRuntime::new(
        WholeBlockStore::open(dir.path()).unwrap(),
        index,
        inflight,
        Arc::new(UnusedOrigin),
        1024,
        0,
        observability.metrics().clone(),
    )
    .with_rate_limiter(limiter.clone());
    assert!(std::ptr::eq(runtime.rate_limiter(), limiter.as_ref()));

    let mut config = DynamicConfig::default();
    assert!(config.apply_runtime("[rate_limits]\nenabled = true", &runtime));
    assert!(limiter.is_enabled());

    let path = dir.path().join("worker.toml");
    std::fs::write(&path, "[rate_limits]\nenabled = false").unwrap();
    reload_once(&path, &mut config, &runtime, &observability).await;
    assert!(!limiter.is_enabled());
    assert!(observability.config_reload_status().applied_hash.is_some());
}
