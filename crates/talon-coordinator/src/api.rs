//! Read-only versioned cluster management API (`/api/v1`).
//!
//! A small, bounded JSON surface over the shared [`ClusterSnapshot`] so
//! operators and the management UI can read the live cluster view from any
//! coordinator. Every coordinator answers from the same
//! [`ClusterStateStore`](crate::ClusterStateStore) snapshot, so responses are
//! equivalent for the same backend revision (issue #82).
//!
//! Design constraints (per the issue):
//! - **Read-only.** No mutation routes in v1.
//! - **Bounded.** List endpoints page with a hard `limit` cap and stable
//!   ordering; there is no arbitrary-query or PromQL proxy.
//! - **Self-describing.** Every response carries the generation time, the
//!   snapshot's opaque revision, and its age in milliseconds, so a client can
//!   reason about staleness. Units are explicit in field names (`_ms`,
//!   `_bytes`, `_total`).
//! - **Fail-closed.** When the backend cannot produce a snapshot within the
//!   coordinator's timeout, the API returns `503` with a stable error body and
//!   does not serve stale data unmarked (ADR 0001 §8).
//!
//! The machine-readable contract is served at `/api/v1/openapi.json`.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use talon_core::{NodeHealth, NodeRole, NodeStatus};

use crate::observability::CoordinatorObservability;
use crate::{ClusterSnapshot, StateStoreError};

/// Hard cap on how many nodes a single list response may return.
const MAX_PAGE_LIMIT: usize = 500;
/// Default page size when the caller does not specify one.
const DEFAULT_PAGE_LIMIT: usize = 100;
/// API schema version, surfaced in every envelope.
pub const API_VERSION: &str = "v1";

/// Metadata attached to every API response so clients can reason about
/// freshness and consistency without a second call.
#[derive(Debug, Clone, Serialize)]
pub struct ResponseMeta {
    /// API schema version.
    pub api_version: &'static str,
    /// Wall-clock time this response was generated, Unix milliseconds.
    pub generated_at_unix_ms: u64,
    /// Opaque store revision the underlying snapshot was observed at.
    pub snapshot_revision: String,
    /// Age of the snapshot when this response was built, milliseconds.
    pub snapshot_age_ms: u64,
    /// Backend implementation serving the data.
    pub backend: String,
}

/// Cluster-level summary counts.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterSummary {
    /// Logical cluster id.
    pub cluster_id: String,
    /// Total non-expired nodes.
    pub node_count: usize,
    /// Non-expired coordinators.
    pub coordinator_count: usize,
    /// Non-expired workers.
    pub worker_count: usize,
    /// Workers reporting healthy.
    pub healthy_worker_count: usize,
    /// Sum of worker cache capacity, bytes.
    pub total_capacity_bytes: u64,
    /// Sum of worker resident bytes.
    pub total_resident_bytes: u64,
    /// Sum of worker-held blocks.
    pub total_block_count: u64,
    /// Response metadata.
    pub meta: ResponseMeta,
}

/// One node's bounded management view.
#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    /// Stable node id.
    pub node_id: String,
    /// `coordinator` or `worker`.
    pub role: String,
    /// Service address.
    pub address: String,
    /// Admin/HTTP address, if advertised.
    pub admin_address: Option<String>,
    /// Health: `healthy` | `degraded` | `unhealthy` | `unknown`.
    pub health: String,
    /// Whether the node is ready for service.
    pub ready: bool,
    /// Build/package version.
    pub build_version: String,
    /// Process start time, Unix milliseconds.
    pub started_at_unix_ms: u64,
    /// Last accepted heartbeat time, Unix milliseconds.
    pub reported_at_unix_ms: u64,
    /// Requests accepted since start.
    pub requests_total: u64,
    /// Errored requests since start.
    pub errors_total: u64,
    /// Bytes served since start.
    pub bytes_served_total: u64,
    /// Cache hits since start (workers).
    pub cache_hits_total: u64,
    /// Cache misses since start (workers).
    pub cache_misses_total: u64,
    /// Blocks currently held (workers).
    pub block_count: u64,
    /// Resident bytes (workers).
    pub resident_bytes: u64,
    /// Configured capacity bytes (workers).
    pub capacity_bytes: u64,
    /// Deployment labels (region/zone/etc), bounded.
    pub labels: std::collections::BTreeMap<String, String>,
}

impl NodeView {
    fn from_status(status: &NodeStatus) -> Self {
        Self {
            node_id: status.node.id.0.clone(),
            role: role_str(status.node.role).to_string(),
            address: status.node.address.clone(),
            admin_address: status.admin_address.clone(),
            health: health_str(status.health).to_string(),
            ready: status.ready,
            build_version: status.build_version.clone(),
            started_at_unix_ms: status.started_at_unix_ms,
            reported_at_unix_ms: status.reported_at_unix_ms,
            requests_total: status.metrics.requests_total,
            errors_total: status.metrics.errors_total,
            bytes_served_total: status.metrics.bytes_served_total,
            cache_hits_total: status.metrics.cache_hits_total,
            cache_misses_total: status.metrics.cache_misses_total,
            block_count: status.metrics.block_count,
            resident_bytes: status.metrics.resident_bytes,
            capacity_bytes: status.metrics.capacity_bytes,
            labels: status.labels.clone(),
        }
    }
}

/// A bounded, ordered page of nodes.
#[derive(Debug, Clone, Serialize)]
pub struct NodeList {
    /// Nodes in this page, ordered by node id.
    pub nodes: Vec<NodeView>,
    /// Total nodes matching the filter (before pagination).
    pub total: usize,
    /// Offset this page started at.
    pub offset: usize,
    /// Maximum nodes returned in this page.
    pub limit: usize,
    /// Response metadata.
    pub meta: ResponseMeta,
}

/// Backend status and current revision.
#[derive(Debug, Clone, Serialize)]
pub struct BackendStatus {
    /// Backend implementation.
    pub backend: String,
    /// Whether the coordinator can currently serve authoritative reads.
    pub ready: bool,
    /// Current snapshot revision.
    pub revision: String,
    /// Snapshot age, milliseconds.
    pub snapshot_age_ms: u64,
    /// Response metadata.
    pub meta: ResponseMeta,
}

/// Stable error body returned for every non-2xx API response.
#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    /// Stable machine-readable error code.
    pub error: String,
    /// Human-readable, credential-free detail.
    pub message: String,
}

/// Query parameters for the node list.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeQuery {
    /// Filter by role: `coordinator` | `worker`.
    #[serde(default)]
    pub role: Option<String>,
    /// Filter by health state.
    #[serde(default)]
    pub health: Option<String>,
    /// Page offset (default 0).
    #[serde(default)]
    pub offset: Option<usize>,
    /// Page size (clamped to a hard maximum of 500).
    #[serde(default)]
    pub limit: Option<usize>,
}

fn role_str(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Coordinator => "coordinator",
        NodeRole::Worker => "worker",
    }
}

fn health_str(health: NodeHealth) -> &'static str {
    match health {
        NodeHealth::Healthy => "healthy",
        NodeHealth::Degraded => "degraded",
        NodeHealth::Unhealthy => "unhealthy",
        NodeHealth::Unknown => "unknown",
    }
}

/// Build the `/api/v1` router over the coordinator's shared state.
pub fn router(state: Arc<CoordinatorObservability>) -> Router {
    Router::new()
        .route("/api/v1/cluster", get(cluster_handler))
        .route("/api/v1/nodes", get(nodes_handler))
        .route("/api/v1/nodes/{node_id}", get(node_detail_handler))
        .route("/api/v1/backend", get(backend_handler))
        .route("/api/v1/openapi.json", get(openapi_handler))
        .with_state(state)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn build_meta(state: &CoordinatorObservability, snapshot: &ClusterSnapshot) -> ResponseMeta {
    let now = now_unix_ms();
    ResponseMeta {
        api_version: API_VERSION,
        generated_at_unix_ms: now,
        snapshot_revision: snapshot.revision.to_string(),
        snapshot_age_ms: now.saturating_sub(snapshot.observed_at_unix_ms),
        backend: state.store().backend().to_string(),
    }
}

/// Short-lived caching headers: management data is a live snapshot, so it may be
/// cached only briefly. The revision doubles as a weak ETag.
fn cache_headers(revision: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=2"),
    );
    if let Ok(etag) = HeaderValue::from_str(&format!("W/\"{revision}\"")) {
        headers.insert(header::ETAG, etag);
    }
    headers
}

fn map_state_error(error: &StateStoreError) -> (StatusCode, ApiError) {
    let (status, code) = match error {
        StateStoreError::Authentication { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "backend_authentication")
        }
        StateStoreError::PermissionDenied { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "backend_permission_denied")
        }
        StateStoreError::Timeout { .. } => (StatusCode::SERVICE_UNAVAILABLE, "backend_timeout"),
        StateStoreError::Unavailable { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "backend_unavailable")
        }
        _ => (StatusCode::SERVICE_UNAVAILABLE, "backend_error"),
    };
    (
        status,
        ApiError {
            error: code.to_string(),
            // The Display impls are credential-free by construction.
            message: error.to_string(),
        },
    )
}

/// Fetch a snapshot, recording API latency and mapping failures to a stable
/// 503 body. Returns the snapshot on success.
async fn snapshot_or_error(state: &CoordinatorObservability) -> Result<ClusterSnapshot, Response> {
    let started = Instant::now();
    let result = state.snapshot_for_api().await;
    state
        .metrics()
        .record_api(result.is_err(), started.elapsed());
    result.map_err(|error| {
        let (status, body) = map_state_error(&error);
        (status, Json(body)).into_response()
    })
}

async fn cluster_handler(State(state): State<Arc<CoordinatorObservability>>) -> Response {
    let snapshot = match snapshot_or_error(&state).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let meta = build_meta(&state, &snapshot);
    let mut summary = ClusterSummary {
        cluster_id: state.cluster_id().to_string(),
        node_count: snapshot.nodes.len(),
        coordinator_count: 0,
        worker_count: 0,
        healthy_worker_count: 0,
        total_capacity_bytes: 0,
        total_resident_bytes: 0,
        total_block_count: 0,
        meta,
    };
    for status in &snapshot.nodes {
        match status.node.role {
            NodeRole::Coordinator => summary.coordinator_count += 1,
            NodeRole::Worker => {
                summary.worker_count += 1;
                if status.health == NodeHealth::Healthy {
                    summary.healthy_worker_count += 1;
                }
                summary.total_capacity_bytes = summary
                    .total_capacity_bytes
                    .saturating_add(status.metrics.capacity_bytes);
                summary.total_resident_bytes = summary
                    .total_resident_bytes
                    .saturating_add(status.metrics.resident_bytes);
                summary.total_block_count = summary
                    .total_block_count
                    .saturating_add(status.metrics.block_count);
            }
        }
    }
    let headers = cache_headers(&snapshot.revision.to_string());
    (headers, Json(summary)).into_response()
}

async fn nodes_handler(
    State(state): State<Arc<CoordinatorObservability>>,
    Query(query): Query<NodeQuery>,
) -> Response {
    let snapshot = match snapshot_or_error(&state).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let meta = build_meta(&state, &snapshot);

    // Filter, then order by node id for stable pagination.
    let mut filtered: Vec<&NodeStatus> = snapshot
        .nodes
        .iter()
        .filter(|s| match &query.role {
            Some(role) => role_str(s.node.role) == role.as_str(),
            None => true,
        })
        .filter(|s| match &query.health {
            Some(health) => health_str(s.health) == health.as_str(),
            None => true,
        })
        .collect();
    filtered.sort_by(|a, b| a.node.id.0.cmp(&b.node.id.0));

    let total = filtered.len();
    let offset = query.offset.unwrap_or(0);
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT);
    let nodes: Vec<NodeView> = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(NodeView::from_status)
        .collect();

    let headers = cache_headers(&snapshot.revision.to_string());
    (
        headers,
        Json(NodeList {
            nodes,
            total,
            offset,
            limit,
            meta,
        }),
    )
        .into_response()
}

async fn node_detail_handler(
    State(state): State<Arc<CoordinatorObservability>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
) -> Response {
    let snapshot = match snapshot_or_error(&state).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match snapshot.nodes.iter().find(|s| s.node.id.0 == node_id) {
        Some(status) => {
            let headers = cache_headers(&snapshot.revision.to_string());
            (headers, Json(NodeView::from_status(status))).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: "node_not_found".into(),
                message: format!("no node with id {node_id:?} in current snapshot"),
            }),
        )
            .into_response(),
    }
}

async fn backend_handler(State(state): State<Arc<CoordinatorObservability>>) -> Response {
    let snapshot = match snapshot_or_error(&state).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let meta = build_meta(&state, &snapshot);
    let body = BackendStatus {
        backend: state.store().backend().to_string(),
        ready: state.is_ready(),
        revision: snapshot.revision.to_string(),
        snapshot_age_ms: meta.snapshot_age_ms,
        meta,
    };
    (cache_headers(&snapshot.revision.to_string()), Json(body)).into_response()
}

async fn openapi_handler() -> Response {
    ([(header::CONTENT_TYPE, "application/json")], OPENAPI_JSON).into_response()
}

/// Machine-readable OpenAPI 3.0 contract for the management API.
pub const OPENAPI_JSON: &str = include_str!("openapi.json");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStateStore;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use talon_core::{
        NodeId, NodeInfo, NodeMetricsSnapshot, NodeStatus, NODE_STATUS_SCHEMA_VERSION,
    };
    use tower::ServiceExt;

    fn worker(id: &str, cap: u64, resident: u64, blocks: u64) -> NodeStatus {
        let now = now_unix_ms();
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: "c".into(),
            node: NodeInfo {
                id: NodeId::new(id),
                address: format!("{id}:7001"),
                role: NodeRole::Worker,
            },
            incarnation_id: format!("inc-{id}"),
            admin_address: Some(format!("{id}:8001")),
            build_version: "test".into(),
            started_at_unix_ms: now,
            reported_at_unix_ms: now,
            heartbeat_seq: 0,
            health: NodeHealth::Healthy,
            ready: true,
            metrics: NodeMetricsSnapshot {
                capacity_bytes: cap,
                resident_bytes: resident,
                block_count: blocks,
                ..Default::default()
            },
            labels: BTreeMap::new(),
        }
    }

    async fn state_with_workers(n: usize) -> Arc<CoordinatorObservability> {
        let store: Arc<dyn crate::ClusterStateStore> = Arc::new(MemoryStateStore::new());
        for i in 0..n {
            store
                .upsert_node(
                    worker(&format!("w{i:02}"), 1000, 100, 10),
                    Duration::from_secs(30),
                )
                .await
                .unwrap();
        }
        let obs = Arc::new(
            CoordinatorObservability::new(
                "c".into(),
                NodeInfo {
                    id: NodeId::new("coord"),
                    address: "coord:7000".into(),
                    role: NodeRole::Coordinator,
                },
                "coord:8000".into(),
                Duration::from_secs(1),
                store,
            )
            .unwrap(),
        );
        obs.check_ready().await.unwrap();
        obs
    }

    async fn get(
        state: Arc<CoordinatorObservability>,
        uri: &str,
    ) -> (StatusCode, serde_json::Value) {
        let app = router(state);
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    #[tokio::test]
    async fn cluster_summary_counts_nodes() {
        let state = state_with_workers(3).await;
        let (status, body) = get(state, "/api/v1/cluster").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["worker_count"], 3);
        assert_eq!(body["healthy_worker_count"], 3);
        assert_eq!(body["total_capacity_bytes"], 3000);
        assert_eq!(body["meta"]["api_version"], "v1");
        assert!(body["meta"]["snapshot_revision"].is_string());
    }

    #[tokio::test]
    async fn nodes_list_paginates_with_stable_order() {
        let state = state_with_workers(5).await;
        let (status, body) = get(state.clone(), "/api/v1/nodes?limit=2&offset=0").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 5);
        assert_eq!(body["limit"], 2);
        let ids: Vec<String> = body["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["node_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["w00", "w01"]);
        // Next page continues in the same stable order.
        let (_, body2) = get(state, "/api/v1/nodes?limit=2&offset=2").await;
        let ids2: Vec<String> = body2["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["node_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids2, vec!["w02", "w03"]);
    }

    #[tokio::test]
    async fn limit_is_capped() {
        let state = state_with_workers(1).await;
        let (_, body) = get(state, "/api/v1/nodes?limit=100000").await;
        assert_eq!(body["limit"], MAX_PAGE_LIMIT);
    }

    #[tokio::test]
    async fn role_filter_selects_workers() {
        let state = state_with_workers(2).await;
        let (_, body) = get(state, "/api/v1/nodes?role=worker").await;
        assert_eq!(body["total"], 2);
        let (_, body) = get(
            state_with_workers(2).await,
            "/api/v1/nodes?role=coordinator",
        )
        .await;
        assert_eq!(body["total"], 0);
    }

    #[tokio::test]
    async fn node_detail_found_and_missing() {
        let state = state_with_workers(1).await;
        let (status, body) = get(state.clone(), "/api/v1/nodes/w00").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["node_id"], "w00");
        let (status, body) = get(state, "/api/v1/nodes/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "node_not_found");
    }

    #[tokio::test]
    async fn backend_unavailable_maps_to_503() {
        let store = Arc::new(MemoryStateStore::new());
        let obs = Arc::new(
            CoordinatorObservability::new(
                "c".into(),
                NodeInfo {
                    id: NodeId::new("coord"),
                    address: "coord:7000".into(),
                    role: NodeRole::Coordinator,
                },
                "coord:8000".into(),
                Duration::from_secs(1),
                Arc::clone(&store) as Arc<dyn crate::ClusterStateStore>,
            )
            .unwrap(),
        );
        store.set_available(false);
        let (status, body) = get(obs, "/api/v1/cluster").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "backend_unavailable");
    }

    #[tokio::test]
    async fn openapi_document_is_served_and_valid_json() {
        let state = state_with_workers(0).await;
        let (status, body) = get(state, "/api/v1/openapi.json").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["openapi"], "3.0.3");
        assert!(body["paths"]["/api/v1/cluster"].is_object());
    }

    #[test]
    fn openapi_constant_parses() {
        let doc: serde_json::Value = serde_json::from_str(OPENAPI_JSON).unwrap();
        assert!(doc["paths"].is_object());
    }
}
