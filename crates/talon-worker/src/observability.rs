//! Worker metrics, readiness, status snapshots, and HTTP administration API.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use talon_core::metrics::labels;
use talon_core::{
    Counter, Gauge, Histogram, Metrics, NodeHealth, NodeInfo, NodeMetricsSnapshot, NodeStatus,
    NODE_STATUS_SCHEMA_VERSION,
};
use tokio::net::TcpListener;

use crate::{BlockIndex, InFlightLoads};

const BACKEND_LABELS: &[(&str, &str)] = &[("backend", "azure")];

/// Pre-registered metric handles used on worker hot paths.
#[derive(Clone)]
pub struct WorkerMetrics {
    registry: Metrics,
    configured_capacity_bytes: u64,
    active_connection_count: Arc<AtomicU64>,
    requests_total: Counter,
    request_errors_total: Counter,
    bytes_served_total: Counter,
    cache_hits_total: Counter,
    cache_misses_total: Counter,
    backend_fetch_bytes_total: Counter,
    backend_fetch_errors_total: Counter,
    evictions_total: Counter,
    heartbeat_success_total: Counter,
    heartbeat_failure_total: Counter,
    request_duration_seconds: Histogram,
    backend_fetch_duration_seconds: Histogram,
    active_connections: Gauge,
    inflight_loads: Gauge,
    block_count: Gauge,
    page_count: Gauge,
    resident_bytes: Gauge,
    ready: Gauge,
    process_uptime_seconds: Gauge,
}

/// RAII guard that keeps the active-connection gauge accurate on every exit.
pub struct ActiveConnectionGuard {
    count: Arc<AtomicU64>,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl WorkerMetrics {
    /// Create all worker metric families and initialize configured capacity.
    pub fn new(configured_capacity_bytes: u64) -> Self {
        let registry = Metrics::new();
        registry
            .gauge(
                "talon_worker_build_info",
                "Worker build information.",
                labels(&[("version", env!("CARGO_PKG_VERSION"))]),
            )
            .set(1.0);
        let active_connection_count = Arc::new(AtomicU64::new(0));
        let requests_total = registry.counter(
            "talon_worker_requests_total",
            "Data-plane requests completed by the worker.",
            BTreeMap::new(),
        );
        let request_errors_total = registry.counter(
            "talon_worker_request_errors_total",
            "Data-plane requests completed with an error.",
            BTreeMap::new(),
        );
        let bytes_served_total = registry.counter(
            "talon_worker_bytes_served_total",
            "Bytes returned to data-plane clients.",
            BTreeMap::new(),
        );
        let cache_hits_total = registry.counter(
            "talon_worker_cache_hits_total",
            "Worker cache hits.",
            labels(&[("form", "whole")]),
        );
        let cache_misses_total = registry.counter(
            "talon_worker_cache_misses_total",
            "Worker cache misses.",
            labels(&[("form", "whole")]),
        );
        let backend_fetch_bytes_total = registry.counter(
            "talon_worker_backend_fetch_bytes_total",
            "Bytes fetched from the origin backend.",
            labels(BACKEND_LABELS),
        );
        let backend_fetch_errors_total = registry.counter(
            "talon_worker_backend_fetch_errors_total",
            "Origin backend range fetch failures.",
            labels(BACKEND_LABELS),
        );
        let evictions_total = registry.counter(
            "talon_worker_evictions_total",
            "Cache units evicted by the worker.",
            BTreeMap::new(),
        );
        let heartbeat_success_total = registry.counter(
            "talon_worker_control_heartbeat_total",
            "Control-plane heartbeat attempts by result.",
            labels(&[("result", "success")]),
        );
        let heartbeat_failure_total = registry.counter(
            "talon_worker_control_heartbeat_total",
            "Control-plane heartbeat attempts by result.",
            labels(&[("result", "failure")]),
        );
        let request_duration_seconds = registry.histogram(
            "talon_worker_request_duration_seconds",
            "Data-plane request latency in seconds.",
            BTreeMap::new(),
        );
        let backend_fetch_duration_seconds = registry.histogram(
            "talon_worker_backend_fetch_duration_seconds",
            "Origin backend range fetch latency in seconds.",
            labels(BACKEND_LABELS),
        );
        let active_connections = registry.gauge(
            "talon_worker_active_connections",
            "Data-plane connections currently open.",
            BTreeMap::new(),
        );
        let inflight_loads = registry.gauge(
            "talon_worker_inflight_loads",
            "Backend loads currently in flight.",
            BTreeMap::new(),
        );
        let block_count = registry.gauge(
            "talon_worker_blocks",
            "Blocks currently indexed by the worker.",
            BTreeMap::new(),
        );
        let page_count = registry.gauge(
            "talon_worker_pages",
            "Materialized pages currently indexed by the worker.",
            BTreeMap::new(),
        );
        let resident_bytes = registry.gauge(
            "talon_worker_resident_bytes",
            "Bytes currently resident in the worker cache.",
            BTreeMap::new(),
        );
        let capacity_bytes = registry.gauge(
            "talon_worker_capacity_bytes",
            "Configured worker cache capacity in bytes.",
            BTreeMap::new(),
        );
        capacity_bytes.set(configured_capacity_bytes as f64);
        let ready = registry.gauge(
            "talon_worker_ready",
            "Whether the worker is ready to serve normal traffic.",
            BTreeMap::new(),
        );
        let process_uptime_seconds = registry.gauge(
            "talon_worker_process_uptime_seconds",
            "Worker process uptime in seconds.",
            BTreeMap::new(),
        );

        Self {
            registry,
            configured_capacity_bytes,
            active_connection_count,
            requests_total,
            request_errors_total,
            bytes_served_total,
            cache_hits_total,
            cache_misses_total,
            backend_fetch_bytes_total,
            backend_fetch_errors_total,
            evictions_total,
            heartbeat_success_total,
            heartbeat_failure_total,
            request_duration_seconds,
            backend_fetch_duration_seconds,
            active_connections,
            inflight_loads,
            block_count,
            page_count,
            resident_bytes,
            ready,
            process_uptime_seconds,
        }
    }

    /// Record a successful data-plane request.
    pub fn record_request_success(&self, bytes: u64, elapsed: Duration) {
        self.requests_total.inc();
        self.bytes_served_total.add(bytes);
        self.request_duration_seconds.observe(elapsed.as_secs_f64());
    }

    /// Record a failed data-plane request.
    pub fn record_request_error(&self, elapsed: Duration) {
        self.requests_total.inc();
        self.request_errors_total.inc();
        self.request_duration_seconds.observe(elapsed.as_secs_f64());
    }

    /// Record a whole-block cache hit.
    pub fn record_cache_hit(&self) {
        self.cache_hits_total.inc();
    }

    /// Record a whole-block cache miss.
    pub fn record_cache_miss(&self) {
        self.cache_misses_total.inc();
    }

    /// Record a successful backend fetch.
    pub fn record_backend_fetch_success(&self, bytes: u64, elapsed: Duration) {
        self.backend_fetch_bytes_total.add(bytes);
        self.backend_fetch_duration_seconds
            .observe(elapsed.as_secs_f64());
    }

    /// Record a failed backend fetch.
    pub fn record_backend_fetch_error(&self, elapsed: Duration) {
        self.backend_fetch_errors_total.inc();
        self.backend_fetch_duration_seconds
            .observe(elapsed.as_secs_f64());
    }

    /// Record a successful control-plane heartbeat cycle.
    pub fn record_heartbeat_success(&self) {
        self.heartbeat_success_total.inc();
    }

    /// Record a failed control-plane heartbeat or registration cycle.
    pub fn record_heartbeat_failure(&self) {
        self.heartbeat_failure_total.inc();
    }

    /// Record a completed cache eviction.
    pub fn record_eviction(&self) {
        self.evictions_total.inc();
    }

    /// Increment active connections until the returned guard is dropped.
    pub fn track_connection(&self) -> ActiveConnectionGuard {
        self.active_connection_count.fetch_add(1, Ordering::Relaxed);
        ActiveConnectionGuard {
            count: Arc::clone(&self.active_connection_count),
        }
    }

    fn refresh_runtime(
        &self,
        index: &BlockIndex,
        inflight: &InFlightLoads,
        started: Instant,
        ready: bool,
    ) {
        self.refresh_values(
            inflight.len() as u64,
            index.len() as u64,
            index.page_count(),
            index.resident_bytes(),
            started,
            ready,
        );
    }

    fn refresh_values(
        &self,
        inflight_loads: u64,
        block_count: u64,
        page_count: u64,
        resident_bytes: u64,
        started: Instant,
        ready: bool,
    ) {
        self.active_connections
            .set(self.active_connection_count.load(Ordering::Relaxed) as f64);
        self.inflight_loads.set(inflight_loads as f64);
        self.block_count.set(block_count as f64);
        self.page_count.set(page_count as f64);
        self.resident_bytes.set(resident_bytes as f64);
        self.ready.set(u8::from(ready) as f64);
        self.process_uptime_seconds
            .set(started.elapsed().as_secs_f64());
    }

    fn snapshot(
        &self,
        inflight_loads: u64,
        block_count: u64,
        page_count: u64,
        resident_bytes: u64,
    ) -> NodeMetricsSnapshot {
        NodeMetricsSnapshot {
            requests_total: self.requests_total.get(),
            errors_total: self.request_errors_total.get(),
            bytes_served_total: self.bytes_served_total.get(),
            cache_hits_total: self.cache_hits_total.get(),
            cache_misses_total: self.cache_misses_total.get(),
            backend_errors_total: self.backend_fetch_errors_total.get(),
            evictions_total: self.evictions_total.get(),
            inflight_loads,
            block_count,
            page_count,
            resident_bytes,
            capacity_bytes: self.configured_capacity_bytes,
            state_snapshot_age_ms: 0,
        }
    }

    /// Render the worker registry in Prometheus text exposition format.
    pub fn render(&self) -> String {
        self.registry.render()
    }
}

/// Readiness of required worker dependencies.
#[derive(Default)]
pub struct WorkerReadiness {
    backend_ready: AtomicBool,
    store_ready: AtomicBool,
    control_registered: AtomicBool,
    shutting_down: AtomicBool,
}

impl WorkerReadiness {
    /// Mark origin backend initialization as ready or unavailable.
    pub fn set_backend_ready(&self, ready: bool) {
        self.backend_ready.store(ready, Ordering::Release);
    }

    /// Mark local cache-store initialization as ready or unavailable.
    pub fn set_store_ready(&self, ready: bool) {
        self.store_ready.store(ready, Ordering::Release);
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
            && self.store_ready.load(Ordering::Acquire)
            && self.control_registered.load(Ordering::Acquire)
            && !self.shutting_down.load(Ordering::Acquire)
    }

    fn is_live(&self) -> bool {
        !self.shutting_down.load(Ordering::Acquire)
    }

    fn health(&self) -> NodeHealth {
        if self.is_ready() {
            NodeHealth::Healthy
        } else if self.backend_ready.load(Ordering::Acquire)
            && self.store_ready.load(Ordering::Acquire)
            && self.is_live()
        {
            NodeHealth::Degraded
        } else {
            NodeHealth::Unhealthy
        }
    }

    fn blocking_reasons(&self) -> Vec<&'static str> {
        let mut reasons = Vec::new();
        if !self.backend_ready.load(Ordering::Acquire) {
            reasons.push("backend_not_ready");
        }
        if !self.store_ready.load(Ordering::Acquire) {
            reasons.push("store_not_ready");
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

/// Shared worker observability state exposed to HTTP and heartbeats.
pub struct WorkerObservability {
    node: NodeInfo,
    cluster_id: String,
    admin_address: String,
    incarnation_id: String,
    build_version: String,
    started_at_unix_ms: u64,
    started: Instant,
    heartbeat_seq: AtomicU64,
    metrics: WorkerMetrics,
    readiness: WorkerReadiness,
    index: Arc<BlockIndex>,
    inflight: Arc<InFlightLoads>,
}

impl WorkerObservability {
    /// Create worker observability state with a random process incarnation.
    pub fn new(
        cluster_id: String,
        node: NodeInfo,
        admin_address: String,
        capacity_bytes: u64,
        index: Arc<BlockIndex>,
        inflight: Arc<InFlightLoads>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            node,
            cluster_id,
            admin_address,
            incarnation_id: generate_incarnation_id()?,
            build_version: env!("CARGO_PKG_VERSION").into(),
            started_at_unix_ms: now_unix_ms(),
            started: Instant::now(),
            heartbeat_seq: AtomicU64::new(0),
            metrics: WorkerMetrics::new(capacity_bytes),
            readiness: WorkerReadiness::default(),
            index,
            inflight,
        })
    }

    /// Worker metric handles for data and control paths.
    pub fn metrics(&self) -> &WorkerMetrics {
        &self.metrics
    }

    /// Worker dependency readiness controls.
    pub fn readiness(&self) -> &WorkerReadiness {
        &self.readiness
    }

    /// Whether normal data-plane requests may be served.
    pub fn is_ready(&self) -> bool {
        self.readiness.is_ready()
    }

    /// Build a fresh bounded status snapshot.
    pub fn status(&self) -> NodeStatus {
        let ready = self.readiness.is_ready();
        let inflight_loads = self.inflight.len() as u64;
        let block_count = self.index.len() as u64;
        let page_count = self.index.page_count();
        let resident_bytes = self.index.resident_bytes();
        self.metrics.refresh_values(
            inflight_loads,
            block_count,
            page_count,
            resident_bytes,
            self.started,
            ready,
        );
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: self.cluster_id.clone(),
            node: self.node.clone(),
            incarnation_id: self.incarnation_id.clone(),
            admin_address: Some(self.admin_address.clone()),
            build_version: self.build_version.clone(),
            started_at_unix_ms: self.started_at_unix_ms,
            reported_at_unix_ms: now_unix_ms().max(self.started_at_unix_ms),
            heartbeat_seq: self.heartbeat_seq.fetch_add(1, Ordering::Relaxed),
            health: self.readiness.health(),
            ready,
            metrics: self
                .metrics
                .snapshot(inflight_loads, block_count, page_count, resident_bytes),
            labels: BTreeMap::new(),
        }
    }

    fn metrics_text(&self) -> String {
        self.metrics.refresh_runtime(
            &self.index,
            &self.inflight,
            self.started,
            self.readiness.is_ready(),
        );
        self.metrics.render()
    }
}

/// Serve the worker administration API until the listener closes.
pub async fn serve_admin(
    listener: TcpListener,
    observability: Arc<WorkerObservability>,
) -> std::io::Result<()> {
    axum::serve(listener, admin_router(observability)).await
}

fn admin_router(observability: Arc<WorkerObservability>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health_handler))
        .route("/readyz", get(readiness_handler))
        .route("/api/v1/status", get(status_handler))
        .with_state(observability)
}

async fn metrics_handler(State(state): State<Arc<WorkerObservability>>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics_text(),
    )
}

async fn health_handler(State(state): State<Arc<WorkerObservability>>) -> Response {
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

async fn readiness_handler(State(state): State<Arc<WorkerObservability>>) -> Response {
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

async fn status_handler(State(state): State<Arc<WorkerObservability>>) -> Response {
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

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

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
    use std::sync::Arc;

    use talon_core::{NodeId, NodeRole};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn observability() -> Arc<WorkerObservability> {
        Arc::new(
            WorkerObservability::new(
                "cluster-a".into(),
                NodeInfo {
                    id: NodeId::new("worker-1"),
                    address: "127.0.0.1:7001".into(),
                    role: NodeRole::Worker,
                },
                "127.0.0.1:8001".into(),
                4096,
                Arc::new(BlockIndex::new()),
                Arc::new(InFlightLoads::new()),
            )
            .unwrap(),
        )
    }

    #[test]
    fn status_and_readiness_reflect_dependencies() {
        let observability = observability();
        let initial = observability.status();
        assert_eq!(initial.health, NodeHealth::Unhealthy);
        assert!(!initial.ready);
        assert_eq!(initial.metrics.capacity_bytes, 4096);

        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        assert_eq!(observability.status().health, NodeHealth::Degraded);

        observability.readiness().set_control_registered(true);
        let ready = observability.status();
        assert!(ready.ready);
        assert_eq!(ready.health, NodeHealth::Healthy);
        ready.validate().unwrap();
    }

    #[test]
    fn heartbeat_failure_updates_metrics_and_readiness() {
        let observability = observability();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        observability.readiness().set_control_registered(true);
        observability.metrics().record_heartbeat_success();
        observability.metrics().record_heartbeat_failure();
        observability.readiness().set_control_registered(false);

        let rendered = observability.metrics_text();
        assert!(rendered.contains("talon_worker_control_heartbeat_total{result=\"success\"} 1"));
        assert!(rendered.contains("talon_worker_control_heartbeat_total{result=\"failure\"} 1"));
        assert!(!observability.status().ready);
    }

    #[test]
    fn request_metrics_feed_prometheus_and_status_snapshot() {
        let observability = observability();
        observability
            .metrics()
            .record_request_success(4096, Duration::from_millis(2));
        observability
            .metrics()
            .record_request_error(Duration::from_millis(3));

        let status = observability.status();
        assert_eq!(status.metrics.requests_total, 2);
        assert_eq!(status.metrics.errors_total, 1);
        assert_eq!(status.metrics.bytes_served_total, 4096);
        let rendered = observability.metrics_text();
        assert!(rendered.contains("talon_worker_requests_total 2"));
        assert!(rendered.contains("talon_worker_request_errors_total 1"));
        assert!(rendered.contains("talon_worker_bytes_served_total 4096"));
        assert!(rendered.contains("talon_worker_request_duration_seconds_count 2"));
    }

    #[tokio::test]
    async fn admin_endpoints_expose_metrics_health_readiness_and_status() {
        let observability = observability();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_admin(listener, Arc::clone(&observability)));

        let readiness = request(address, "/readyz").await;
        assert!(readiness.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(readiness.contains("coordinator_not_registered"));

        observability.readiness().set_control_registered(true);
        let health = request(address, "/healthz").await;
        assert!(health.starts_with("HTTP/1.1 200 OK"));
        let readiness = request(address, "/readyz").await;
        assert!(readiness.starts_with("HTTP/1.1 200 OK"));

        let metrics = request(address, "/metrics").await;
        assert!(metrics.contains("talon_worker_requests_total"));
        assert!(metrics.contains("talon_worker_capacity_bytes 4096"));
        assert!(metrics.contains("talon_worker_build_info{version=\"0.1.0\"} 1"));
        assert!(metrics.contains("talon_worker_active_connections 0"));
        assert!(metrics.contains("talon_worker_ready 1"));

        let status = request(address, "/api/v1/status").await;
        assert!(status.starts_with("HTTP/1.1 200 OK"));
        assert!(status.contains("\"cluster_id\":\"cluster-a\""));
        assert!(status.contains("\"ready\":true"));

        server.abort();
    }

    async fn request(address: std::net::SocketAddr, path: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }
}
