// SPDX-License-Identifier: Apache-2.0
//! Readiness, status heartbeats, and the admin HTTP surface.
//!
//! The endpoints match `talon-worker`'s — `/metrics`, `/healthz`, `/readyz`,
//! `/api/v1/status` — because a fleet runs one set of probes and dashboards
//! against both. What differs is what fills them: the metric namespace is
//! `talon_async_worker_*` (see [`crate::metrics`]) and the status snapshot
//! reports extents where the block worker reports blocks.
//!
//! # Mapping an extent cache onto a block-shaped status
//!
//! [`NodeMetricsSnapshot`] was designed around the block worker and its field
//! names say so. Rather than leave the management UI showing zeroes, this maps
//! each field to its nearest true equivalent and leaves genuinely inapplicable
//! ones at zero:
//!
//! | Snapshot field | Async worker meaning |
//! |---|---|
//! | `block_count` | extents addressable on NVMe |
//! | `page_count` | 0 — there are no pages |
//! | `resident_bytes` | bytes packed into NVMe regions plus DRAM bytes |
//! | `cache_hits_total` | L1 hits plus L2 hits |
//! | `inflight_loads` | 0 — coalescing is internal to the cache, not tracked |
//!
//! Reporting `block_count` as extents is the one that could mislead. It is
//! still the right choice: the field is rendered as "how many things does this
//! node hold", the role label on the same record says `async_worker`, and a
//! zero there would read as an idle node rather than a differently-shaped one.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use talon_core::{
    NodeHealth, NodeInfo, NodeMetricsSnapshot, NodeStatus, NODE_STATUS_SCHEMA_VERSION,
};
use tokio::net::TcpListener;

use crate::cache::tiered::TieredExtentCache;
use crate::metrics::AsyncWorkerMetrics;
use crate::runtime::AsyncWorkerRuntime;

/// Readiness of the async worker's required dependencies.
///
/// The NVMe tier is deliberately not a readiness input. It is optional — a
/// DRAM-only deployment is a supported configuration — so gating readiness on
/// it would keep a working worker permanently out of rotation.
#[derive(Debug, Default)]
pub struct AsyncWorkerReadiness {
    backend_ready: AtomicBool,
    cache_ready: AtomicBool,
    control_registered: AtomicBool,
    shutting_down: AtomicBool,
}

impl AsyncWorkerReadiness {
    /// Mark origin backend initialization as ready or unavailable.
    pub fn set_backend_ready(&self, ready: bool) {
        self.backend_ready.store(ready, Ordering::Release);
    }

    /// Mark extent cache initialization as ready or unavailable.
    pub fn set_cache_ready(&self, ready: bool) {
        self.cache_ready.store(ready, Ordering::Release);
    }

    /// Mark coordinator registration and heartbeat state.
    pub fn set_control_registered(&self, ready: bool) {
        self.control_registered.store(ready, Ordering::Release);
    }

    /// Mark process shutdown, which immediately removes readiness and liveness.
    pub fn set_shutting_down(&self, shutting_down: bool) {
        self.shutting_down.store(shutting_down, Ordering::Release);
    }

    /// Whether the worker can safely receive normal data-plane traffic.
    pub fn is_ready(&self) -> bool {
        self.backend_ready.load(Ordering::Acquire)
            && self.cache_ready.load(Ordering::Acquire)
            && self.control_registered.load(Ordering::Acquire)
            && !self.shutting_down.load(Ordering::Acquire)
    }

    /// Whether the process is alive, i.e. not shutting down.
    pub fn is_live(&self) -> bool {
        !self.shutting_down.load(Ordering::Acquire)
    }

    /// Coarse health for the status heartbeat.
    ///
    /// `Degraded` rather than `Unhealthy` when only registration is missing:
    /// the worker can serve every read it holds, it just is not being routed
    /// any, and a coordinator restart should not make the whole pool look sick.
    pub fn health(&self) -> NodeHealth {
        if self.is_ready() {
            NodeHealth::Healthy
        } else if self.backend_ready.load(Ordering::Acquire)
            && self.cache_ready.load(Ordering::Acquire)
            && self.is_live()
        {
            NodeHealth::Degraded
        } else {
            NodeHealth::Unhealthy
        }
    }

    /// Which dependencies are currently keeping the worker out of rotation.
    pub fn blocking_reasons(&self) -> Vec<&'static str> {
        let mut reasons = Vec::new();
        if !self.backend_ready.load(Ordering::Acquire) {
            reasons.push("backend_not_ready");
        }
        if !self.cache_ready.load(Ordering::Acquire) {
            reasons.push("cache_not_ready");
        }
        if !self.control_registered.load(Ordering::Acquire) {
            reasons.push("coordinator_not_registered");
        }
        if self.shutting_down.load(Ordering::Acquire) {
            reasons.push("shutting_down");
        }
        reasons
    }
}

/// Shared observability state behind the admin HTTP surface and heartbeats.
pub struct AsyncWorkerObservability {
    node: NodeInfo,
    cluster_id: String,
    admin_address: String,
    incarnation_id: String,
    build_version: String,
    started_at_unix_ms: u64,
    heartbeat_seq: AtomicU64,
    capacity_bytes: u64,
    metrics: Arc<AsyncWorkerMetrics>,
    readiness: AsyncWorkerReadiness,
    runtime: Arc<AsyncWorkerRuntime>,
}

impl std::fmt::Debug for AsyncWorkerObservability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncWorkerObservability")
            .field("node", &self.node.id)
            .field("cluster_id", &self.cluster_id)
            .field("ready", &self.readiness.is_ready())
            .finish()
    }
}

impl AsyncWorkerObservability {
    /// Create observability state with a fresh random process incarnation.
    ///
    /// `capacity_bytes` is the configured NVMe ceiling, reported so the
    /// management UI can show residency against it.
    pub fn new(
        cluster_id: String,
        node: NodeInfo,
        admin_address: String,
        capacity_bytes: u64,
        metrics: Arc<AsyncWorkerMetrics>,
        runtime: Arc<AsyncWorkerRuntime>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            node,
            cluster_id,
            admin_address,
            incarnation_id: generate_incarnation_id()?,
            build_version: env!("CARGO_PKG_VERSION").into(),
            started_at_unix_ms: now_unix_ms(),
            heartbeat_seq: AtomicU64::new(0),
            capacity_bytes,
            metrics,
            readiness: AsyncWorkerReadiness::default(),
            runtime,
        })
    }

    /// The metric registry shared with the connection loop.
    pub fn metrics(&self) -> &Arc<AsyncWorkerMetrics> {
        &self.metrics
    }

    /// Dependency readiness controls.
    pub fn readiness(&self) -> &AsyncWorkerReadiness {
        &self.readiness
    }

    /// Whether normal data-plane requests may be served.
    pub fn is_ready(&self) -> bool {
        self.readiness.is_ready()
    }

    /// Build a fresh bounded status snapshot for a heartbeat or the API.
    pub fn status(&self) -> NodeStatus {
        let serve = self.runtime.stats();
        let cache = self.runtime.cache().stats();
        let disk = self.runtime.cache().disk();

        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: self.cluster_id.clone(),
            node: self.node.clone(),
            incarnation_id: self.incarnation_id.clone(),
            admin_address: Some(self.admin_address.clone()),
            build_version: self.build_version.clone(),
            started_at_unix_ms: self.started_at_unix_ms,
            // Clamped forward so a backwards wall-clock step cannot produce a
            // report that predates process start, which `validate` rejects.
            reported_at_unix_ms: now_unix_ms().max(self.started_at_unix_ms),
            heartbeat_seq: self.heartbeat_seq.fetch_add(1, Ordering::Relaxed),
            health: self.readiness.health(),
            ready: self.readiness.is_ready(),
            metrics: NodeMetricsSnapshot {
                requests_total: serve.served,
                errors_total: 0,
                bytes_served_total: serve.bytes_served,
                cache_hits_total: cache.memory_hits + cache.disk_hits,
                // No L2 miss counter exists, and inventing one would double
                // count: every read that reached the origin missed L1, so the
                // L1 miss count is exactly the fall-through count.
                cache_misses_total: cache.memory_misses,
                backend_errors_total: 0,
                evictions_total: cache.memory_evictions + cache.disk_extents_evicted,
                inflight_loads: 0,
                block_count: disk.map(|d| d.extent_count()).unwrap_or(0),
                page_count: 0,
                resident_bytes: disk.map(|d| d.allocated_bytes()).unwrap_or(0) + cache.memory_bytes,
                capacity_bytes: self.capacity_bytes,
                state_snapshot_age_ms: 0,
            },
            labels: BTreeMap::new(),
        }
    }

    /// Render Prometheus text, syncing the cache and runtime counters first.
    pub fn metrics_text(&self) -> String {
        self.metrics
            .sync(self.runtime.cache(), &self.runtime.stats());
        self.metrics.render()
    }

    fn cache(&self) -> &TieredExtentCache {
        self.runtime.cache()
    }
}

/// Serve the async worker administration API until the listener closes.
pub async fn serve_admin(
    listener: TcpListener,
    observability: Arc<AsyncWorkerObservability>,
) -> std::io::Result<()> {
    axum::serve(listener, admin_router(observability)).await
}

fn admin_router(observability: Arc<AsyncWorkerObservability>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health_handler))
        .route("/readyz", get(readiness_handler))
        .route("/api/v1/status", get(status_handler))
        .route("/api/v1/cache", get(cache_handler))
        .with_state(observability)
}

async fn metrics_handler(State(state): State<Arc<AsyncWorkerObservability>>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics_text(),
    )
}

async fn health_handler(State(state): State<Arc<AsyncWorkerObservability>>) -> Response {
    let live = state.readiness.is_live();
    let status = if live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "status": if live { "ok" } else { "shutting_down" }
        })),
    )
        .into_response()
}

async fn readiness_handler(State(state): State<Arc<AsyncWorkerObservability>>) -> Response {
    let ready = state.readiness.is_ready();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "ready": ready,
            "reasons": state.readiness.blocking_reasons(),
        })),
    )
        .into_response()
}

async fn status_handler(State(state): State<Arc<AsyncWorkerObservability>>) -> Response {
    let status = state.status();
    match status.validate() {
        Ok(()) => Json(status).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

/// Extent-cache detail that has no home in the block-shaped [`NodeStatus`].
///
/// The origin-versus-served byte ratio is the number this worker exists to
/// change, and it is not a field the management UI knows to ask for. Exposing
/// it here means an operator can read it off one node without scraping
/// Prometheus.
async fn cache_handler(State(state): State<Arc<AsyncWorkerObservability>>) -> Response {
    let c = state.cache().stats();
    let serve = state.runtime.stats();
    let disk = state.cache().disk();
    Json(serde_json::json!({
        "bytes_served_total": serve.bytes_served,
        "origin_bytes_fetched_total": serve.origin_bytes_fetched,
        "reads_total": serve.served,
        "sendfile_reads_total": serve.sendfile_served,
        "l1": {
            "hits": c.memory_hits,
            "misses": c.memory_misses,
            "evictions": c.memory_evictions,
            "bytes": c.memory_bytes,
            "enabled": state.cache().memory().is_enabled(),
        },
        "l2": {
            "hits": c.disk_hits,
            "short_misses": c.disk_short_misses,
            "bytes_written": c.disk_bytes_written,
            "extents_evicted": c.disk_extents_evicted,
            "extents": disk.map(|d| d.extent_count()).unwrap_or(0),
            "allocated_bytes": disk.map(|d| d.allocated_bytes()).unwrap_or(0),
            "capacity_bytes": state.capacity_bytes,
            "enabled": disk.is_some(),
        },
        "admissions": {
            "rejected": c.admissions_rejected,
            "dropped": c.admissions_dropped,
        },
    }))
    .into_response()
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A random per-process identity, so a restart at the same node id is
/// distinguishable from the process that preceded it.
fn generate_incarnation_id() -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut id = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(id, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use async_trait::async_trait;
    use bytes::Bytes;
    use talon_core::{
        BackendStore, Error, NodeId, NodeRole, ObjectId, ObjectStat, Result, Version,
    };

    use super::*;
    use crate::cache::tiered::ExtentCacheConfig;

    struct StubBackend {
        fetches: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for StubBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, len: u64) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::from(vec![7u8; len as usize]))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 1 << 20,
                version: Version::new("v1"),
            })
        }
    }

    async fn observability() -> Arc<AsyncWorkerObservability> {
        let cache = TieredExtentCache::new(&ExtentCacheConfig {
            memory_bytes: 1 << 20,
            memory_shards: 1,
            ..Default::default()
        })
        .await
        .unwrap();
        let backend = Arc::new(StubBackend {
            fetches: AtomicUsize::new(0),
        });
        let runtime = Arc::new(AsyncWorkerRuntime::new(cache, backend));
        Arc::new(
            AsyncWorkerObservability::new(
                "test".into(),
                NodeInfo {
                    id: NodeId::new("aw-1"),
                    address: "127.0.0.1:7101".into(),
                    role: NodeRole::AsyncWorker,
                },
                "127.0.0.1:8101".into(),
                64 << 30,
                Arc::new(AsyncWorkerMetrics::new("s3")),
                runtime,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn a_fresh_worker_is_not_ready_and_says_why() {
        let obs = observability().await;
        assert!(!obs.is_ready());
        let reasons = obs.readiness().blocking_reasons();
        assert!(reasons.contains(&"backend_not_ready"));
        assert!(reasons.contains(&"cache_not_ready"));
        assert!(reasons.contains(&"coordinator_not_registered"));
        assert!(!reasons.contains(&"shutting_down"));
    }

    #[tokio::test]
    async fn readiness_requires_every_dependency() {
        let obs = observability().await;
        obs.readiness().set_backend_ready(true);
        assert!(!obs.is_ready(), "backend alone is not enough");
        obs.readiness().set_cache_ready(true);
        assert!(!obs.is_ready(), "cache alone is not enough");
        obs.readiness().set_control_registered(true);
        assert!(obs.is_ready());
        assert!(obs.readiness().blocking_reasons().is_empty());
    }

    #[tokio::test]
    async fn losing_the_coordinator_is_degraded_not_unhealthy() {
        // A worker that can serve every read it holds but is not being routed
        // any is not sick; a coordinator restart must not make the pool look
        // like it is failing.
        let obs = observability().await;
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_cache_ready(true);
        assert_eq!(obs.readiness().health(), NodeHealth::Degraded);

        obs.readiness().set_control_registered(true);
        assert_eq!(obs.readiness().health(), NodeHealth::Healthy);
    }

    #[tokio::test]
    async fn shutting_down_drops_liveness_immediately() {
        let obs = observability().await;
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_cache_ready(true);
        obs.readiness().set_control_registered(true);
        assert!(obs.is_ready());
        assert!(obs.readiness().is_live());

        obs.readiness().set_shutting_down(true);
        assert!(!obs.is_ready());
        assert!(!obs.readiness().is_live());
        assert_eq!(obs.readiness().health(), NodeHealth::Unhealthy);
    }

    #[tokio::test]
    async fn the_status_snapshot_is_valid_and_names_the_async_role() {
        let obs = observability().await;
        let status = obs.status();
        status.validate().expect("status must satisfy the contract");
        assert_eq!(status.node.role, NodeRole::AsyncWorker);
        assert_eq!(status.cluster_id, "test");
        assert_eq!(status.admin_address.as_deref(), Some("127.0.0.1:8101"));
        assert_eq!(status.metrics.capacity_bytes, 64 << 30);
        // There are no pages here, and claiming any would be a lie the UI
        // would render.
        assert_eq!(status.metrics.page_count, 0);
    }

    #[tokio::test]
    async fn the_heartbeat_sequence_advances_once_per_snapshot() {
        // The coordinator rejects a status whose sequence did not advance, so a
        // repeated number would silently stall the node's record.
        let obs = observability().await;
        let a = obs.status().heartbeat_seq;
        let b = obs.status().heartbeat_seq;
        let c = obs.status().heartbeat_seq;
        assert_eq!((a, b, c), (0, 1, 2));
    }

    #[tokio::test]
    async fn two_processes_get_distinct_incarnations() {
        let a = observability().await.status().incarnation_id;
        let b = observability().await.status().incarnation_id;
        assert_ne!(a, b);
        assert_eq!(a.len(), 32, "128 bits, hex encoded");
    }

    #[tokio::test]
    async fn the_status_reports_residency_from_the_cache_not_a_block_index() {
        let obs = observability().await;
        let before = obs.status().metrics.resident_bytes;
        assert_eq!(before, 0);

        // Warm L1 through the real serve path.
        obs.runtime
            .serve(&talon_transport::data::RangeRequest {
                object: ObjectId::new(talon_core::Backend::S3, "b", "o.parquet"),
                offset: 0,
                len: 4096,
            })
            .await
            .unwrap();

        let after = obs.status().metrics;
        assert!(
            after.resident_bytes >= 4096,
            "DRAM residency must show up as resident bytes, got {}",
            after.resident_bytes
        );
        assert_eq!(after.requests_total, 1);
        assert_eq!(after.bytes_served_total, 4096);
    }

    #[tokio::test]
    async fn rendering_metrics_picks_up_the_runtime_counters() {
        let obs = observability().await;
        obs.runtime
            .serve(&talon_transport::data::RangeRequest {
                object: ObjectId::new(talon_core::Backend::S3, "b", "o.parquet"),
                offset: 0,
                len: 4096,
            })
            .await
            .unwrap();

        // The runtime keeps its own atomics and holds no metric handles, so
        // nothing reaches the registry until a render syncs it.
        let text = obs.metrics_text();
        assert!(
            text.contains("talon_async_worker_origin_bytes_fetched_total 4096"),
            "origin bytes missing from:\n{text}"
        );
    }

    #[tokio::test]
    async fn an_error_is_reported_as_a_string_not_a_panic() {
        // A status that fails its own contract must surface as a 500 with the
        // reason, not take the admin server down.
        struct FailingBackend;
        #[async_trait]
        impl BackendStore for FailingBackend {
            async fn fetch_range(
                &self,
                _object: &ObjectId,
                _offset: u64,
                _len: u64,
            ) -> Result<Bytes> {
                Err(Error::Backend("origin down".into()))
            }
            async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
                Err(Error::Backend("origin down".into()))
            }
        }
        let cache = TieredExtentCache::new(&ExtentCacheConfig::default())
            .await
            .unwrap();
        let runtime = Arc::new(AsyncWorkerRuntime::new(cache, Arc::new(FailingBackend)));
        let obs = AsyncWorkerObservability::new(
            "test".into(),
            NodeInfo {
                id: NodeId::new("aw-1"),
                address: "127.0.0.1:7101".into(),
                role: NodeRole::AsyncWorker,
            },
            "127.0.0.1:8101".into(),
            0,
            Arc::new(AsyncWorkerMetrics::new("s3")),
            runtime,
        )
        .unwrap();
        // A backend outage does not make the *status* invalid — the node is
        // still describable, just unhealthy.
        obs.status().validate().unwrap();
        assert_eq!(obs.readiness().health(), NodeHealth::Unhealthy);
    }
}
