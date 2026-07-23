//! Coordinator metrics, shared-state readiness, and administration HTTP API.

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
    Counter, Gauge, Histogram, Metrics, NodeHealth, NodeInfo, NodeMetricsSnapshot, NodeRole,
    NodeStatus, NODE_STATUS_SCHEMA_VERSION,
};
use talon_transport::ControlMessage;
use tokio::net::TcpListener;

use crate::{ClusterSnapshot, ClusterStateStore, StateStoreError, StateStoreResult, WriteResult};

/// Bounded control operation label.
#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub enum ControlOperation {
    /// Worker registration.
    Register,
    /// Legacy worker heartbeat.
    Heartbeat,
    /// Versioned node status heartbeat.
    StatusHeartbeat,
    /// Placement lookup.
    Placement,
    /// Membership query.
    Membership,
    /// Other control message.
    Other,
}

impl ControlOperation {
    /// Classify a control message before dispatch.
    pub fn from_message(message: &ControlMessage) -> Self {
        match message {
            ControlMessage::Register { .. } => Self::Register,
            ControlMessage::Heartbeat { .. } => Self::Heartbeat,
            ControlMessage::NodeStatusHeartbeat { .. } => Self::StatusHeartbeat,
            ControlMessage::PlacementLookup { .. } => Self::Placement,
            ControlMessage::MembershipQuery {} => Self::Membership,
            _ => Self::Other,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Heartbeat => "heartbeat",
            Self::StatusHeartbeat => "status_heartbeat",
            Self::Placement => "placement_lookup",
            Self::Membership => "membership_query",
            Self::Other => "other",
        }
    }
}

#[derive(Clone)]
struct OperationMetric {
    requests: Counter,
    errors: Counter,
    duration: Histogram,
}

/// Pre-registered coordinator metric handles.
#[derive(Clone)]
pub struct CoordinatorMetrics {
    registry: Metrics,
    operations: Arc<Vec<OperationMetric>>,
    protocol_errors: Counter,
    registration_accepted: Counter,
    registration_rejected: Counter,
    heartbeat_legacy_accepted: Counter,
    heartbeat_legacy_rejected: Counter,
    heartbeat_status_accepted: Counter,
    heartbeat_status_rejected: Counter,
    placement_duration: Histogram,
    placement_errors: Counter,
    state_readiness_duration: Histogram,
    state_snapshot_duration: Histogram,
    state_upsert_duration: Histogram,
    active_count: Arc<AtomicU64>,
    active_connections: Gauge,
    ready: Gauge,
    uptime: Gauge,
    snapshot_age_seconds: Gauge,
    snapshot_age_value: Arc<AtomicU64>,
}

/// RAII guard for coordinator control connections.
pub struct CoordinatorConnectionGuard {
    count: Arc<AtomicU64>,
}

impl Drop for CoordinatorConnectionGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl CoordinatorMetrics {
    /// Create all coordinator metric families.
    pub fn new() -> Self {
        let registry = Metrics::new();
        registry
            .gauge(
                "talon_coordinator_build_info",
                "Coordinator build information.",
                labels(&[("version", env!("CARGO_PKG_VERSION"))]),
            )
            .set(1.0);
        let operations = [
            ControlOperation::Register,
            ControlOperation::Heartbeat,
            ControlOperation::StatusHeartbeat,
            ControlOperation::Placement,
            ControlOperation::Membership,
            ControlOperation::Other,
        ]
        .into_iter()
        .map(|operation| {
            let operation_labels = labels(&[("operation", operation.label())]);
            OperationMetric {
                requests: registry.counter(
                    "talon_coordinator_control_requests_total",
                    "Decoded control requests by operation.",
                    operation_labels.clone(),
                ),
                errors: registry.counter(
                    "talon_coordinator_control_errors_total",
                    "Control requests returning an error by operation.",
                    operation_labels.clone(),
                ),
                duration: registry.histogram(
                    "talon_coordinator_control_duration_seconds",
                    "Control request latency in seconds by operation.",
                    operation_labels,
                ),
            }
        })
        .collect();
        let result_counter =
            |name: &str, help: &str, pairs| registry.counter(name, help, labels(pairs));
        let active_connections = registry.gauge(
            "talon_coordinator_active_connections",
            "Control-plane connections currently open.",
            BTreeMap::new(),
        );
        let ready = registry.gauge(
            "talon_coordinator_ready",
            "Whether authoritative shared state is ready.",
            BTreeMap::new(),
        );
        let uptime = registry.gauge(
            "talon_coordinator_process_uptime_seconds",
            "Coordinator process uptime in seconds.",
            BTreeMap::new(),
        );
        let snapshot_age_seconds = registry.gauge(
            "talon_coordinator_state_snapshot_age_seconds",
            "Age of the latest successful shared-state snapshot in seconds.",
            BTreeMap::new(),
        );
        Self {
            protocol_errors: registry.counter(
                "talon_coordinator_protocol_errors_total",
                "Control frames rejected before dispatch.",
                BTreeMap::new(),
            ),
            registration_accepted: result_counter(
                "talon_coordinator_registration_total",
                "Worker registrations by outcome.",
                &[("result", "accepted")],
            ),
            registration_rejected: result_counter(
                "talon_coordinator_registration_total",
                "Worker registrations by outcome.",
                &[("result", "rejected")],
            ),
            heartbeat_legacy_accepted: registry.counter(
                "talon_coordinator_heartbeat_total",
                "Node heartbeats by kind and outcome.",
                labels(&[("kind", "legacy"), ("result", "accepted")]),
            ),
            heartbeat_legacy_rejected: registry.counter(
                "talon_coordinator_heartbeat_total",
                "Node heartbeats by kind and outcome.",
                labels(&[("kind", "legacy"), ("result", "rejected")]),
            ),
            heartbeat_status_accepted: registry.counter(
                "talon_coordinator_heartbeat_total",
                "Node heartbeats by kind and outcome.",
                labels(&[("kind", "status"), ("result", "accepted")]),
            ),
            heartbeat_status_rejected: registry.counter(
                "talon_coordinator_heartbeat_total",
                "Node heartbeats by kind and outcome.",
                labels(&[("kind", "status"), ("result", "rejected")]),
            ),
            placement_duration: registry.histogram(
                "talon_coordinator_placement_duration_seconds",
                "Placement lookup latency in seconds.",
                BTreeMap::new(),
            ),
            placement_errors: registry.counter(
                "talon_coordinator_placement_errors_total",
                "Placement lookup failures.",
                BTreeMap::new(),
            ),
            state_readiness_duration: registry.histogram(
                "talon_coordinator_state_store_duration_seconds",
                "Shared-state operation latency in seconds.",
                labels(&[("operation", "readiness")]),
            ),
            state_snapshot_duration: registry.histogram(
                "talon_coordinator_state_store_duration_seconds",
                "Shared-state operation latency in seconds.",
                labels(&[("operation", "snapshot")]),
            ),
            state_upsert_duration: registry.histogram(
                "talon_coordinator_state_store_duration_seconds",
                "Shared-state operation latency in seconds.",
                labels(&[("operation", "upsert")]),
            ),
            registry,
            operations: Arc::new(operations),
            active_count: Arc::new(AtomicU64::new(0)),
            active_connections,
            ready,
            uptime,
            snapshot_age_seconds,
            snapshot_age_value: Arc::new(AtomicU64::new(0)),
        }
    }

    fn operation(&self, operation: ControlOperation) -> &OperationMetric {
        &self.operations[operation as usize]
    }

    /// Record one decoded control request.
    pub fn record_control(&self, operation: ControlOperation, error: bool, elapsed: Duration) {
        let metric = self.operation(operation);
        metric.requests.inc();
        if error {
            metric.errors.inc();
        }
        metric.duration.observe(elapsed.as_secs_f64());
    }

    /// Record a protocol decode failure.
    pub fn record_protocol_error(&self) {
        self.protocol_errors.inc();
    }

    /// Record a registration outcome.
    pub fn record_registration(&self, accepted: bool) {
        if accepted {
            self.registration_accepted.inc();
        } else {
            self.registration_rejected.inc();
        }
    }

    /// Record a legacy or status heartbeat outcome.
    pub fn record_heartbeat(&self, status: bool, accepted: bool) {
        match (status, accepted) {
            (false, true) => self.heartbeat_legacy_accepted.inc(),
            (false, false) => self.heartbeat_legacy_rejected.inc(),
            (true, true) => self.heartbeat_status_accepted.inc(),
            (true, false) => self.heartbeat_status_rejected.inc(),
        }
    }

    /// Record placement-specific latency and failure.
    pub fn record_placement(&self, error: bool, elapsed: Duration) {
        if error {
            self.placement_errors.inc();
        }
        self.placement_duration.observe(elapsed.as_secs_f64());
    }

    /// Track one active control connection.
    pub fn track_connection(&self) -> CoordinatorConnectionGuard {
        self.active_count.fetch_add(1, Ordering::Relaxed);
        CoordinatorConnectionGuard {
            count: Arc::clone(&self.active_count),
        }
    }

    fn record_state<T>(
        &self,
        operation: &'static str,
        result: &StateStoreResult<T>,
        elapsed: Duration,
    ) {
        match operation {
            "readiness" => &self.state_readiness_duration,
            "snapshot" => &self.state_snapshot_duration,
            "upsert" => &self.state_upsert_duration,
            _ => unreachable!("state operation labels are fixed"),
        }
        .observe(elapsed.as_secs_f64());
        if let Err(error) = result {
            self.registry
                .counter(
                    "talon_coordinator_state_store_errors_total",
                    "Shared-state operation failures by operation and kind.",
                    labels(&[("operation", operation), ("kind", state_error_kind(error))]),
                )
                .inc();
        }
    }

    fn refresh(&self, started: Instant, ready: bool) {
        self.active_connections
            .set(self.active_count.load(Ordering::Relaxed) as f64);
        self.ready.set(u8::from(ready) as f64);
        self.uptime.set(started.elapsed().as_secs_f64());
        self.snapshot_age_seconds
            .set(self.snapshot_age_value.load(Ordering::Relaxed) as f64 / 1000.0);
    }

    fn update_snapshot(&self, snapshot: &ClusterSnapshot) {
        let now = now_unix_ms();
        self.snapshot_age_value.store(
            now.saturating_sub(snapshot.observed_at_unix_ms),
            Ordering::Relaxed,
        );
        for role in [NodeRole::Coordinator, NodeRole::Worker] {
            for health in [
                NodeHealth::Healthy,
                NodeHealth::Degraded,
                NodeHealth::Unhealthy,
                NodeHealth::Unknown,
            ] {
                let count = snapshot
                    .nodes
                    .iter()
                    .filter(|node| node.node.role == role && node.health == health)
                    .count();
                self.registry
                    .gauge(
                        "talon_coordinator_live_nodes",
                        "Live leased nodes by role and reported health.",
                        labels(&[("role", role_label(role)), ("health", health_label(health))]),
                    )
                    .set(count as f64);
            }
        }
    }

    fn totals(&self) -> (u64, u64) {
        self.operations
            .iter()
            .fold((0, 0), |(requests, errors), op| {
                (requests + op.requests.get(), errors + op.errors.get())
            })
    }

    /// Render the Prometheus registry.
    pub fn render(&self) -> String {
        self.registry.render()
    }
}

impl Default for CoordinatorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared coordinator observability and state-store access.
pub struct CoordinatorObservability {
    cluster_id: String,
    node: NodeInfo,
    admin_address: String,
    incarnation_id: String,
    started_at_unix_ms: u64,
    started: Instant,
    sequence: AtomicU64,
    ready: AtomicBool,
    shutting_down: AtomicBool,
    request_timeout: Duration,
    metrics: CoordinatorMetrics,
    store: Arc<dyn ClusterStateStore>,
}

impl CoordinatorObservability {
    /// Create observability state over the selected shared-state backend.
    pub fn new(
        cluster_id: String,
        node: NodeInfo,
        admin_address: String,
        request_timeout: Duration,
        store: Arc<dyn ClusterStateStore>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            cluster_id,
            node,
            admin_address,
            incarnation_id: generate_incarnation_id()?,
            started_at_unix_ms: now_unix_ms(),
            started: Instant::now(),
            sequence: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            request_timeout,
            metrics: CoordinatorMetrics::new(),
            store,
        })
    }

    /// Coordinator metric handles.
    pub fn metrics(&self) -> &CoordinatorMetrics {
        &self.metrics
    }

    /// Selected state store.
    pub fn store(&self) -> &Arc<dyn ClusterStateStore> {
        &self.store
    }

    /// Logical cluster accepted by status heartbeats.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Whether authoritative shared state is currently ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) && !self.shutting_down.load(Ordering::Acquire)
    }

    /// Read-only readiness check with deadline.
    pub async fn check_ready(&self) -> StateStoreResult<()> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.check_ready()).await {
                Ok(result) => result.map(|_| ()),
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("readiness", &result, started.elapsed());
        self.ready.store(result.is_ok(), Ordering::Release);
        result
    }

    /// Upsert a leased node status with metrics and deadline.
    pub async fn upsert_status(
        &self,
        status: NodeStatus,
        lease_ttl: Duration,
    ) -> StateStoreResult<WriteResult> {
        let started = Instant::now();
        let result = match tokio::time::timeout(
            self.request_timeout,
            self.store.upsert_node(status, lease_ttl),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(StateStoreError::Timeout {
                backend: self.store.backend(),
            }),
        };
        self.metrics
            .record_state("upsert", &result, started.elapsed());
        if result.is_err() {
            self.ready.store(false, Ordering::Release);
        }
        result
    }

    /// Refresh live-node and snapshot-age gauges.
    pub async fn refresh_snapshot(&self) -> StateStoreResult<()> {
        let started = Instant::now();
        let result =
            match tokio::time::timeout(self.request_timeout, self.store.snapshot(&self.cluster_id))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(StateStoreError::Timeout {
                    backend: self.store.backend(),
                }),
            };
        self.metrics
            .record_state("snapshot", &result, started.elapsed());
        match result {
            Ok(snapshot) => {
                self.metrics.update_snapshot(&snapshot);
                self.ready.store(true, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                self.ready.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Build the coordinator's own leased status record.
    pub fn status(&self) -> NodeStatus {
        let (requests_total, errors_total) = self.metrics.totals();
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: self.cluster_id.clone(),
            node: self.node.clone(),
            incarnation_id: self.incarnation_id.clone(),
            admin_address: Some(self.admin_address.clone()),
            build_version: env!("CARGO_PKG_VERSION").into(),
            started_at_unix_ms: self.started_at_unix_ms,
            reported_at_unix_ms: now_unix_ms().max(self.started_at_unix_ms),
            heartbeat_seq: self.sequence.fetch_add(1, Ordering::Relaxed),
            health: if self.is_ready() {
                NodeHealth::Healthy
            } else {
                NodeHealth::Degraded
            },
            ready: self.is_ready(),
            metrics: NodeMetricsSnapshot {
                requests_total,
                errors_total,
                state_snapshot_age_ms: self.metrics.snapshot_age_value.load(Ordering::Relaxed),
                ..Default::default()
            },
            labels: BTreeMap::new(),
        }
    }
}

/// Serve coordinator health, readiness, and metrics endpoints.
pub async fn serve_admin(
    listener: TcpListener,
    state: Arc<CoordinatorObservability>,
) -> std::io::Result<()> {
    axum::serve(
        listener,
        Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/healthz", get(health_handler))
            .route("/readyz", get(readiness_handler))
            .with_state(state),
    )
    .await
}

async fn metrics_handler(State(state): State<Arc<CoordinatorObservability>>) -> impl IntoResponse {
    let _ = state.refresh_snapshot().await;
    state.metrics.refresh(state.started, state.is_ready());
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
}

async fn health_handler(State(state): State<Arc<CoordinatorObservability>>) -> Response {
    let live = !state.shutting_down.load(Ordering::Acquire);
    (
        if live {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({"status": if live { "ok" } else { "shutting_down" }})),
    )
        .into_response()
}

async fn readiness_handler(State(state): State<Arc<CoordinatorObservability>>) -> Response {
    match state.check_ready().await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"ready": true}))).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ready": false,
                "reason": state_error_kind(&error),
            })),
        )
            .into_response(),
    }
}

fn state_error_kind(error: &StateStoreError) -> &'static str {
    match error {
        StateStoreError::Authentication { .. } => "authentication",
        StateStoreError::PermissionDenied { .. } => "permission_denied",
        StateStoreError::Timeout { .. } => "timeout",
        StateStoreError::Unavailable { .. } => "unavailable",
        StateStoreError::Compacted { .. } => "compacted",
        StateStoreError::WatchLagged { .. } => "watch_lagged",
        StateStoreError::InvalidRecord(_)
        | StateStoreError::InvalidLeaseTtl(_)
        | StateStoreError::InvalidRevision { .. } => "invalid",
    }
}

fn role_label(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Coordinator => "coordinator",
        NodeRole::Worker => "worker",
    }
}

fn health_label(health: NodeHealth) -> &'static str {
    match health {
        NodeHealth::Healthy => "healthy",
        NodeHealth::Degraded => "degraded",
        NodeHealth::Unhealthy => "unhealthy",
        NodeHealth::Unknown => "unknown",
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
    let mut id = String::with_capacity(32);
    for byte in bytes {
        write!(id, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use talon_core::{NodeId, NodeMetricsSnapshot};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::MemoryStateStore;

    fn observability() -> (Arc<CoordinatorObservability>, Arc<MemoryStateStore>) {
        let store = Arc::new(MemoryStateStore::new());
        let state_store: Arc<dyn ClusterStateStore> = store.clone();
        let observability = Arc::new(
            CoordinatorObservability::new(
                "cluster-a".into(),
                NodeInfo {
                    id: NodeId::new("coordinator-1"),
                    address: "127.0.0.1:7000".into(),
                    role: NodeRole::Coordinator,
                },
                "127.0.0.1:8000".into(),
                Duration::from_millis(100),
                state_store,
            )
            .unwrap(),
        );
        (observability, store)
    }

    fn worker_status() -> NodeStatus {
        let now = now_unix_ms();
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: "cluster-a".into(),
            node: NodeInfo {
                id: NodeId::new("worker-1"),
                address: "127.0.0.1:7001".into(),
                role: NodeRole::Worker,
            },
            incarnation_id: "worker-incarnation".into(),
            admin_address: Some("127.0.0.1:8001".into()),
            build_version: "test".into(),
            started_at_unix_ms: now,
            reported_at_unix_ms: now,
            heartbeat_seq: 0,
            health: NodeHealth::Healthy,
            ready: true,
            metrics: NodeMetricsSnapshot::default(),
            labels: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn readiness_and_status_use_shared_state() {
        let (observability, store) = observability();
        observability.check_ready().await.unwrap();
        assert!(observability.is_ready());

        let status = observability.status();
        status.validate().unwrap();
        assert!(status.ready);
        observability
            .upsert_status(status, Duration::from_secs(30))
            .await
            .unwrap();
        observability
            .upsert_status(worker_status(), Duration::from_secs(30))
            .await
            .unwrap();
        observability.refresh_snapshot().await.unwrap();

        let rendered = observability.metrics.render();
        assert!(rendered
            .contains("talon_coordinator_live_nodes{health=\"healthy\",role=\"coordinator\"} 1"));
        assert!(
            rendered.contains("talon_coordinator_live_nodes{health=\"healthy\",role=\"worker\"} 1")
        );

        store.set_available(false);
        assert!(observability.check_ready().await.is_err());
        assert!(!observability.is_ready());
        assert!(observability.metrics.render().contains(
            "talon_coordinator_state_store_errors_total{kind=\"unavailable\",operation=\"readiness\"} 1"
        ));
    }

    #[test]
    fn control_instrumentation_is_atomic_and_bounded() {
        let (observability, _) = observability();
        let guard = observability.metrics.track_connection();
        observability.metrics.record_control(
            ControlOperation::Placement,
            true,
            Duration::from_millis(2),
        );
        observability
            .metrics
            .record_placement(true, Duration::from_millis(2));
        observability.metrics.refresh(observability.started, false);
        let rendered = observability.metrics.render();
        assert!(rendered.contains("talon_coordinator_active_connections 1"));
        assert!(rendered.contains(
            "talon_coordinator_control_requests_total{operation=\"placement_lookup\"} 1"
        ));
        assert!(rendered.contains("talon_coordinator_placement_errors_total 1"));
        drop(guard);
    }

    #[tokio::test]
    async fn admin_endpoints_report_health_readiness_metrics_and_failure() {
        let (observability, store) = observability();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_admin(listener, Arc::clone(&observability)));

        let health = request(address, "/healthz").await;
        assert!(health.starts_with("HTTP/1.1 200 OK"));
        let ready = request(address, "/readyz").await;
        assert!(ready.starts_with("HTTP/1.1 200 OK"));
        let metrics = request(address, "/metrics").await;
        assert!(metrics.contains("talon_coordinator_build_info{version=\"0.1.0\"} 1"));
        assert!(metrics.contains("talon_coordinator_ready 1"));
        assert!(metrics.contains("talon_coordinator_state_snapshot_age_seconds"));

        store.set_available(false);
        let ready = request(address, "/readyz").await;
        assert!(ready.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(ready.contains("\"reason\":\"unavailable\""));

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
