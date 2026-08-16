//! Coordinator admin HTTP surface: the /metrics, /healthz and /readyz
//! endpoints, the router that merges the management API and embedded UI, and
//! the security wrapping around both.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tokio::net::TcpListener;

use super::state::CoordinatorObservability;
use super::state_error_kind;

/// Serve coordinator health, readiness, and metrics endpoints.
pub async fn serve_admin(
    listener: TcpListener,
    state: Arc<CoordinatorObservability>,
) -> std::io::Result<()> {
    serve_admin_secured(
        listener,
        state,
        Arc::new(crate::security::SecurityConfig::default()),
    )
    .await
}

/// Serve the admin surface with an explicit security configuration (auth,
/// security headers, request-size cap). See [`crate::security`].
pub async fn serve_admin_secured(
    listener: TcpListener,
    state: Arc<CoordinatorObservability>,
    security: Arc<crate::security::SecurityConfig>,
) -> std::io::Result<()> {
    axum::serve(listener, secured_admin_router(state, security)).await
}

/// Build the coordinator administration router: metrics/health/readiness, the
/// versioned management API (#82), and the embedded UI (#83). Split out from
/// [`serve_admin`] so route coexistence is unit-testable without binding a port.
pub fn admin_router(state: Arc<CoordinatorObservability>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health_handler))
        .route("/readyz", get(readiness_handler))
        .with_state(Arc::clone(&state))
        // Read-only versioned management API under /api/v1 (issue #82).
        .merge(crate::api::router(state))
        // Embedded management UI under / and /ui (issue #83).
        .merge(crate::ui::router())
}

/// Build the admin router wrapped with the management-security layer (#85):
/// authentication on protected routes, security headers on every response, and
/// a bounded request-body limit.
pub fn secured_admin_router(
    state: Arc<CoordinatorObservability>,
    security: Arc<crate::security::SecurityConfig>,
) -> Router {
    admin_router(state)
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&security),
            |axum::extract::State(sec): axum::extract::State<
                Arc<crate::security::SecurityConfig>,
            >,
             req: axum::extract::Request,
             next: axum::middleware::Next| async move {
                crate::security::guard(sec, req, next).await
            },
        ))
        .layer(axum::extract::DefaultBodyLimit::max(
            crate::security::MAX_REQUEST_BODY_BYTES,
        ))
}

async fn metrics_handler(State(state): State<Arc<CoordinatorObservability>>) -> impl IntoResponse {
    // Do NOT hit the state store here. The snapshot-age and live-node gauges are
    // refreshed on the reconcile timer (see reconcile_membership -> update_snapshot),
    // so /metrics can render the last reconciled values without its own backend
    // RPC. This keeps the public, unauthenticated /metrics endpoint from
    // amplifying scrape traffic into authoritative etcd/Kubernetes load (#164).
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::observability::state::observability;

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

    #[tokio::test]
    async fn metrics_does_not_hit_the_state_store() {
        // /metrics must render from the reconcile-timer-maintained gauges without
        // its own store snapshot RPC, so a public scrape cannot amplify into
        // backend load (#164). With the store injected-unavailable from the
        // start, a handler that called snapshot() would surface an error; here
        // /metrics must still respond 200 with the static series.
        let (observability, store) = observability();
        store.set_available(false);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_admin(listener, Arc::clone(&observability)));

        let metrics = request(address, "/metrics").await;
        assert!(metrics.starts_with("HTTP/1.1 200 OK"));
        assert!(metrics.contains("talon_coordinator_build_info{version=\"0.1.0\"} 1"));
        // Crucially, scraping /metrics recorded no state-store operation: a
        // snapshot RPC would have incremented the state-store error counter
        // (the store is unavailable), so its absence proves no store call.
        assert!(!metrics.contains("talon_coordinator_state_store_errors_total"));

        server.abort();
    }

    #[tokio::test]
    async fn ui_and_api_coexist_without_shadowing_admin_routes() {
        // The embedded UI (#83) and the management API (#82) share the admin
        // server with /metrics, /healthz, /readyz. None must shadow another.
        let (observability, _store) = observability();
        observability.check_ready().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_admin(listener, Arc::clone(&observability)));

        // UI shell at / and /ui, with the strict CSP.
        let root = request(address, "/").await;
        assert!(root.starts_with("HTTP/1.1 200 OK"));
        assert!(root.contains("content-security-policy"));
        assert!(root.contains("id=\"app\""));
        let asset = request(address, "/ui/assets/app.js").await;
        assert!(asset.contains("text/javascript"));

        // API still answers under /api/v1.
        let cluster = request(address, "/api/v1/cluster").await;
        assert!(cluster.starts_with("HTTP/1.1 200 OK"));
        assert!(cluster.contains("\"cluster_id\""));

        // Operational routes are unaffected.
        assert!(request(address, "/healthz")
            .await
            .starts_with("HTTP/1.1 200 OK"));
        assert!(request(address, "/metrics")
            .await
            .contains("talon_coordinator_build_info"));

        server.abort();
    }

    async fn request(address: std::net::SocketAddr, path: &str) -> String {
        request_with(address, path, "").await
    }

    async fn request_with(address: std::net::SocketAddr, path: &str, extra: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!(
                    "GET {path} HTTP/1.1\r\nHost: localhost\r\n{extra}Connection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    #[tokio::test]
    async fn security_layer_enforces_auth_and_headers() {
        use crate::security::{AuthMode, SecurityConfig};
        let (observability, _store) = observability();
        observability.check_ready().await.unwrap();
        let security = Arc::new(SecurityConfig {
            auth: AuthMode::BearerToken {
                token: "an-adequately-long-token".into(),
            },
            trust_forwarded_headers: false,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_admin_secured(
            listener,
            Arc::clone(&observability),
            security,
        ));

        // Public operational endpoints require no auth.
        assert!(request(address, "/healthz")
            .await
            .starts_with("HTTP/1.1 200 OK"));
        assert!(request(address, "/metrics")
            .await
            .starts_with("HTTP/1.1 200 OK"));

        // Protected API without a token fails closed with a challenge.
        let unauth = request(address, "/api/v1/cluster").await;
        assert!(unauth.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(unauth.to_lowercase().contains("www-authenticate"));

        // A wrong token is still rejected.
        let bad = request_with(
            address,
            "/api/v1/cluster",
            "Authorization: Bearer wrong-token\r\n",
        )
        .await;
        assert!(bad.starts_with("HTTP/1.1 401 Unauthorized"));

        // The correct token is accepted and the response carries hardening
        // headers.
        let ok = request_with(
            address,
            "/api/v1/cluster",
            "Authorization: Bearer an-adequately-long-token\r\n",
        )
        .await;
        assert!(ok.starts_with("HTTP/1.1 200 OK"));
        let lower = ok.to_lowercase();
        assert!(lower.contains("x-content-type-options: nosniff"));
        assert!(lower.contains("x-frame-options: deny"));
        assert!(lower.contains("referrer-policy: no-referrer"));

        server.abort();
    }

    #[tokio::test]
    async fn disabled_auth_allows_management_but_still_stamps_headers() {
        let (observability, _store) = observability();
        observability.check_ready().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Default security config = auth disabled.
        let server = tokio::spawn(serve_admin_secured(
            listener,
            Arc::clone(&observability),
            Arc::new(crate::security::SecurityConfig::default()),
        ));

        let resp = request(address, "/api/v1/cluster").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.to_lowercase().contains("x-frame-options: deny"));
        server.abort();
    }
}
