//! Pre-registered coordinator metric handles: the control-plane registry,
//! derived series refreshed from shared state, and their recording API.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use talon_core::metrics::labels;
use talon_core::{Counter, Gauge, Histogram, Metrics, NodeHealth, NodeRole};
use talon_transport::ControlMessage;

use super::{now_unix_ms, state_error_kind};
use crate::{ClusterSnapshot, StateStoreResult};

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
    api_requests: Counter,
    api_errors: Counter,
    api_duration: Histogram,
    active_count: Arc<AtomicU64>,
    active_connections: Gauge,
    ready: Gauge,
    uptime: Gauge,
    snapshot_age_seconds: Gauge,
    pub(crate) snapshot_age_value: Arc<AtomicU64>,
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
            api_requests: registry.counter(
                "talon_coordinator_api_requests_total",
                "Management API requests served.",
                BTreeMap::new(),
            ),
            api_errors: registry.counter(
                "talon_coordinator_api_errors_total",
                "Management API requests returning an error.",
                BTreeMap::new(),
            ),
            api_duration: registry.histogram(
                "talon_coordinator_api_duration_seconds",
                "Management API request latency in seconds.",
                BTreeMap::new(),
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

    /// Record a management-API request's latency and outcome.
    pub fn record_api(&self, error: bool, elapsed: Duration) {
        self.api_requests.inc();
        if error {
            self.api_errors.inc();
        }
        self.api_duration.observe(elapsed.as_secs_f64());
    }

    /// Track one active control connection.
    pub fn track_connection(&self) -> CoordinatorConnectionGuard {
        self.active_count.fetch_add(1, Ordering::Relaxed);
        CoordinatorConnectionGuard {
            count: Arc::clone(&self.active_count),
        }
    }

    pub(crate) fn record_state<T>(
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

    pub(crate) fn refresh(&self, started: Instant, ready: bool) {
        self.active_connections
            .set(self.active_count.load(Ordering::Relaxed) as f64);
        self.ready.set(u8::from(ready) as f64);
        self.uptime.set(started.elapsed().as_secs_f64());
        self.snapshot_age_seconds
            .set(self.snapshot_age_value.load(Ordering::Relaxed) as f64 / 1000.0);
    }

    pub(crate) fn update_snapshot(&self, snapshot: &ClusterSnapshot) {
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

    pub(crate) fn totals(&self) -> (u64, u64) {
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

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn control_instrumentation_is_atomic_and_bounded() {
        let metrics = CoordinatorMetrics::new();
        let started = Instant::now();
        let guard = metrics.track_connection();
        metrics.record_control(ControlOperation::Placement, true, Duration::from_millis(2));
        metrics.record_placement(true, Duration::from_millis(2));
        metrics.refresh(started, false);
        let rendered = metrics.render();
        assert!(rendered.contains("talon_coordinator_active_connections 1"));
        assert!(rendered.contains(
            "talon_coordinator_control_requests_total{operation=\"placement_lookup\"} 1"
        ));
        assert!(rendered.contains("talon_coordinator_placement_errors_total 1"));
        drop(guard);
    }
}
