//! Axum server, lifecycle middleware, operational endpoints, and limits.

use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures::StreamExt;
use http_body_util::{BodyStream, Limited, StreamBody};
use serde_json::json;
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutBody;

use crate::config::{GatewayConfig, GatewayConfigError, GatewayMode, GatewaySecurity};
use crate::metrics::{GatewayMetrics, RequestObservation, ResponseObserver};
use crate::model::{
    FailureReason, GatewayAdapter, GatewayOperation, GatewayOutcome, GatewayRequestContext,
    GatewayResponse, GatewayRoute,
};

/// Opaque request ID emitted by the shared core and translated by adapters.
pub const REQUEST_ID_HEADER: &str = "x-talon-request-id";

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
struct MetricsRecorded;

/// Mutable dependency and security readiness shared with adapter binaries.
pub struct GatewayReadiness {
    mode: GatewayMode,
    dependencies_ready: AtomicBool,
    shutting_down: AtomicBool,
    security: RwLock<GatewaySecurity>,
}

impl GatewayReadiness {
    fn new(mode: GatewayMode, security: GatewaySecurity) -> Self {
        Self {
            mode,
            dependencies_ready: AtomicBool::new(true),
            shutting_down: AtomicBool::new(false),
            security: RwLock::new(security),
        }
    }

    /// Update whether coordinator/origin dependencies can serve requests.
    pub fn set_dependencies_ready(&self, ready: bool) {
        self.dependencies_ready.store(ready, Ordering::Release);
    }

    /// Update installed production security components.
    pub fn set_security(&self, security: GatewaySecurity) {
        *self.security.write().unwrap() = security;
    }

    /// Whether the process should receive provider traffic.
    pub fn is_ready(&self) -> bool {
        !self.shutting_down.load(Ordering::Acquire)
            && self.dependencies_ready.load(Ordering::Acquire)
            && (self.mode == GatewayMode::Development
                || self.security.read().unwrap().production_ready())
    }

    fn is_live(&self) -> bool {
        !self.shutting_down.load(Ordering::Acquire)
    }

    fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
    }

    fn blocking_reasons(&self) -> Vec<&'static str> {
        let mut reasons = Vec::new();
        if self.shutting_down.load(Ordering::Acquire) {
            reasons.push("shutting_down");
        }
        if !self.dependencies_ready.load(Ordering::Acquire) {
            reasons.push("dependencies_not_ready");
        }
        if self.mode == GatewayMode::Production {
            let security = *self.security.read().unwrap();
            if !security.tls {
                reasons.push("tls_not_configured");
            }
            if !security.authentication {
                reasons.push("authentication_not_configured");
            }
            if !security.authorization {
                reasons.push("authorization_not_configured");
            }
        }
        reasons
    }
}

/// Shared runtime state used by a protocol-specific binary.
pub struct GatewayRuntime {
    config: GatewayConfig,
    adapter: Arc<dyn GatewayAdapter>,
    readiness: Arc<GatewayReadiness>,
    metrics: GatewayMetrics,
}

impl GatewayRuntime {
    /// Validate configuration and construct a shared runtime.
    pub fn new(
        config: GatewayConfig,
        adapter: Arc<dyn GatewayAdapter>,
        security: GatewaySecurity,
    ) -> Result<Self, GatewayConfigError> {
        config.validate()?;
        if config.mode == GatewayMode::Development {
            tracing::warn!(
                bind = %config.bind,
                "gateway development mode is unauthenticated and loopback-only"
            );
        }
        Ok(Self {
            readiness: Arc::new(GatewayReadiness::new(config.mode, security)),
            config,
            adapter,
            metrics: GatewayMetrics::new(),
        })
    }

    /// Live readiness controls for dependency and security initialization.
    pub fn readiness(&self) -> &Arc<GatewayReadiness> {
        &self.readiness
    }

    /// Shared metrics registry.
    pub fn metrics(&self) -> &GatewayMetrics {
        &self.metrics
    }
}

/// Build the complete provider and operational router.
pub fn gateway_router(runtime: Arc<GatewayRuntime>) -> Router {
    let data = Router::new()
        .fallback(adapter_handler)
        .layer(ConcurrencyLimitLayer::new(runtime.config.max_concurrency))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&runtime),
            enforce_limits,
        ));

    Router::new()
        .route("/healthz", get(health_handler))
        .route("/readyz", get(readiness_handler))
        .route("/metrics", get(metrics_handler))
        .merge(data)
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&runtime),
            request_lifecycle,
        ))
        .with_state(runtime)
}

/// Serve until shutdown, then wait a bounded time for active responses.
pub async fn serve(
    listener: TcpListener,
    runtime: Arc<GatewayRuntime>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let local = listener.local_addr()?;
    if runtime.config.mode == GatewayMode::Development && !local.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "development gateway listener is not loopback",
        ));
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(
        listener,
        gateway_router(Arc::clone(&runtime)).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        let _ = shutdown_rx.await;
    })
    .into_future();
    tokio::pin!(server);
    tokio::pin!(shutdown);

    tokio::select! {
        result = &mut server => result,
        () = &mut shutdown => {
            runtime.readiness.begin_shutdown();
            let _ = shutdown_tx.send(());
            match tokio::time::timeout(runtime.config.graceful_shutdown_timeout, &mut server).await {
                Ok(result) => result,
                Err(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "gateway graceful shutdown timed out",
                )),
            }
        }
    }
}

async fn adapter_handler(State(runtime): State<Arc<GatewayRuntime>>, request: Request) -> Response {
    let context = request
        .extensions()
        .get::<GatewayRequestContext>()
        .cloned()
        .expect("lifecycle middleware installs request context");
    let elapsed = context.started.elapsed();
    let remaining = runtime.config.request_deadline.saturating_sub(elapsed);
    let protocol = runtime.adapter.protocol();
    let result =
        match tokio::time::timeout(remaining, runtime.adapter.handle(request, context.clone()))
            .await
        {
            Ok(result) => result,
            Err(_) => GatewayResponse {
                response: (
                    StatusCode::GATEWAY_TIMEOUT,
                    "gateway request deadline exceeded",
                )
                    .into_response(),
                operation: GatewayOperation::Unsupported,
                target: None,
                route: GatewayRoute::None,
                outcome: GatewayOutcome::Failed,
                failure: Some(FailureReason::Timeout),
                requested_bytes: 0,
                cache_bytes: 0,
                origin_bytes: 0,
            },
        };

    let mut response = result.response;
    let status = response.status();
    runtime.metrics.record_headers(RequestObservation {
        protocol,
        operation: result.operation,
        route: result.route,
        outcome: result.outcome,
        failure: result.failure,
        status: status.as_u16(),
        requested_bytes: result.requested_bytes,
        cache_bytes: result.cache_bytes,
        origin_bytes: result.origin_bytes,
        headers_latency: context.started.elapsed(),
    });
    let observer = runtime
        .metrics
        .response_observer(protocol, result.operation);
    response.extensions_mut().insert(MetricsRecorded);
    observe_response_body(
        response,
        observer,
        context.started,
        runtime.config.request_deadline,
    )
}

fn observe_response_body(
    response: Response,
    observer: ResponseObserver,
    started: std::time::Instant,
    request_deadline: std::time::Duration,
) -> Response {
    let (parts, body) = response.into_parts();
    let frames = Box::pin(BodyStream::new(body));
    let completion = ResponseCompletion {
        observer: Some(observer),
        bytes: 0,
        started,
    };
    let stream = futures::stream::unfold(
        (frames, completion, false),
        move |(mut frames, mut completion, deadline_expired)| async move {
            if deadline_expired {
                return None;
            }
            let deadline = tokio::time::Instant::from_std(started + request_deadline);
            let frame = if tokio::time::Instant::now() >= deadline {
                None
            } else {
                tokio::select! {
                    frame = frames.next() => Some(frame),
                    () = tokio::time::sleep_until(deadline) => None,
                }
            };
            match frame {
                None => {
                    completion.finish();
                    let error = axum::Error::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "gateway response deadline exceeded",
                    ));
                    Some((Err(error), (frames, completion, true)))
                }
                Some(Some(Ok(frame))) => {
                    if let Some(data) = frame.data_ref() {
                        completion.bytes = completion.bytes.saturating_add(data.len() as u64);
                    }
                    Some((Ok::<_, axum::Error>(frame), (frames, completion, false)))
                }
                Some(Some(Err(error))) => {
                    completion.finish();
                    Some((Err(error), (frames, completion, true)))
                }
                Some(None) => {
                    completion.finish();
                    None
                }
            }
        },
    );
    Response::from_parts(parts, Body::new(StreamBody::new(stream)))
}

struct ResponseCompletion {
    observer: Option<ResponseObserver>,
    bytes: u64,
    started: std::time::Instant,
}

impl ResponseCompletion {
    fn finish(&mut self) {
        if let Some(observer) = self.observer.take() {
            observer.complete(self.bytes, self.started.elapsed());
        }
    }
}

impl Drop for ResponseCompletion {
    fn drop(&mut self) {
        self.finish();
    }
}

async fn enforce_limits(
    State(runtime): State<Arc<GatewayRuntime>>,
    request: Request,
    next: Next,
) -> Response {
    let target_len = request
        .uri()
        .path_and_query()
        .map_or(0, |value| value.as_str().len());
    if target_len > runtime.config.max_request_target_bytes {
        return (
            StatusCode::URI_TOO_LONG,
            "request target exceeds gateway limit",
        )
            .into_response();
    }
    if request.headers().len() > runtime.config.max_header_count {
        return (
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "request has too many headers",
        )
            .into_response();
    }
    let header_bytes = request
        .headers()
        .iter()
        .try_fold(0usize, |total, (name, value)| {
            total
                .checked_add(name.as_str().len())?
                .checked_add(value.as_bytes().len())
        });
    if match header_bytes {
        Some(size) => size > runtime.config.max_header_bytes,
        None => true,
    } {
        return (
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "request headers exceed gateway limit",
        )
            .into_response();
    }
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > runtime.config.max_body_bytes as u64)
    {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds gateway limit",
        )
            .into_response();
    }
    if !runtime.readiness.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway is not ready to serve provider traffic",
        )
            .into_response();
    }

    let (parts, body) = request.into_parts();
    let body = TimeoutBody::new(runtime.config.body_idle_timeout, body);
    let body = Limited::new(body, runtime.config.max_body_bytes);
    let response = next.run(Request::from_parts(parts, Body::new(body))).await;
    let (parts, body) = response.into_parts();
    let body = TimeoutBody::new(runtime.config.body_idle_timeout, body);
    Response::from_parts(parts, Body::new(body))
}

async fn request_lifecycle(
    State(runtime): State<Arc<GatewayRuntime>>,
    mut request: Request,
    next: Next,
) -> Response {
    let started = std::time::Instant::now();
    let sequence = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed) & 0x0000_ffff_ffff_ffff;
    let request_id = format!("00000000-0000-4000-8000-{sequence:012x}");
    let method = request.method().clone();
    let operational = matches!(request.uri().path(), "/healthz" | "/readyz" | "/metrics");
    let context = GatewayRequestContext {
        request_id: request_id.clone(),
        started,
    };
    request.extensions_mut().insert(context);
    let mut response = next.run(request).await;
    if !operational && response.extensions().get::<MetricsRecorded>().is_none() {
        let status = response.status();
        let failure = match status {
            StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => FailureReason::Timeout,
            StatusCode::SERVICE_UNAVAILABLE => FailureReason::CacheUnavailable,
            _ => FailureReason::InvalidRequest,
        };
        let protocol = runtime.adapter.protocol();
        runtime.metrics.record_headers(RequestObservation {
            protocol,
            operation: GatewayOperation::Unsupported,
            route: GatewayRoute::None,
            outcome: GatewayOutcome::Failed,
            failure: Some(failure),
            status: status.as_u16(),
            requested_bytes: 0,
            cache_bytes: 0,
            origin_bytes: 0,
            headers_latency: started.elapsed(),
        });
        let observer = runtime
            .metrics
            .response_observer(protocol, GatewayOperation::Unsupported);
        response =
            observe_response_body(response, observer, started, runtime.config.request_deadline);
    }
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    tracing::info!(
        request_id,
        method = %method,
        status = response.status().as_u16(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        protocol = runtime.adapter.protocol().label(),
        "gateway request headers completed"
    );
    response
}

async fn health_handler(State(runtime): State<Arc<GatewayRuntime>>) -> Response {
    let live = runtime.readiness.is_live();
    (
        if live {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({"live": live})),
    )
        .into_response()
}

async fn readiness_handler(State(runtime): State<Arc<GatewayRuntime>>) -> Response {
    let ready = runtime.readiness.is_ready();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "ready": ready,
            "reasons": runtime.readiness.blocking_reasons(),
        })),
    )
        .into_response()
}

async fn metrics_handler(State(runtime): State<Arc<GatewayRuntime>>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        runtime.metrics.render(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use axum::body::to_bytes;
    use axum::http::Request as HttpRequest;
    use bytes::Bytes;
    use tower::ServiceExt;

    #[derive(Clone, Copy)]
    enum AdapterBehavior {
        Immediate,
        Pending,
    }

    struct TestAdapter {
        calls: AtomicUsize,
        behavior: AdapterBehavior,
    }

    impl TestAdapter {
        fn immediate() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                behavior: AdapterBehavior::Immediate,
            })
        }
    }

    #[async_trait]
    impl GatewayAdapter for TestAdapter {
        fn protocol(&self) -> crate::ProviderProtocol {
            crate::ProviderProtocol::S3
        }

        async fn handle(
            &self,
            _request: Request,
            _context: GatewayRequestContext,
        ) -> GatewayResponse {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                AdapterBehavior::Immediate => {
                    GatewayResponse::new(Response::new(Body::from("ok")), GatewayOperation::Read)
                }
                AdapterBehavior::Pending => std::future::pending().await,
            }
        }
    }

    fn runtime_with(
        mut config: GatewayConfig,
        adapter: Arc<dyn GatewayAdapter>,
        security: GatewaySecurity,
    ) -> Arc<GatewayRuntime> {
        config.bind = "127.0.0.1:0".parse().unwrap();
        Arc::new(GatewayRuntime::new(config, adapter, security).unwrap())
    }

    #[tokio::test]
    async fn production_readiness_fails_closed_until_every_security_layer_is_ready() {
        let config = GatewayConfig {
            mode: GatewayMode::Production,
            ..GatewayConfig::default()
        };
        let adapter = TestAdapter::immediate();
        let runtime = runtime_with(config, adapter.clone(), GatewaySecurity::default());
        let app = gateway_router(Arc::clone(&runtime));

        let response = app
            .clone()
            .oneshot(HttpRequest::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert!(body.windows(18).any(|part| part == b"tls_not_configured"));
        let response = app
            .clone()
            .oneshot(HttpRequest::get("/object").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);

        runtime.readiness().set_security(GatewaySecurity {
            tls: true,
            authentication: true,
            authorization: true,
        });
        let response = app
            .clone()
            .oneshot(HttpRequest::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .oneshot(HttpRequest::get("/object").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn oversized_metadata_and_bodies_fail_before_adapter_dispatch() {
        let adapter = TestAdapter::immediate();
        let config = GatewayConfig {
            max_request_target_bytes: 8,
            max_header_count: 1,
            max_body_bytes: 4,
            ..GatewayConfig::default()
        };
        let runtime = runtime_with(config, adapter.clone(), GatewaySecurity::default());
        let app = gateway_router(runtime);

        let response = app
            .clone()
            .oneshot(
                HttpRequest::get("/too-long-target")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::URI_TOO_LONG);

        let request = HttpRequest::get("/object")
            .header("x-one", "1")
            .header("x-two", "2")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );

        let request = HttpRequest::post("/object")
            .header(header::CONTENT_LENGTH, "5")
            .body(Body::from("12345"))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    }

    struct BodyReadingAdapter;

    #[async_trait]
    impl GatewayAdapter for BodyReadingAdapter {
        fn protocol(&self) -> crate::ProviderProtocol {
            crate::ProviderProtocol::Azure
        }

        async fn handle(
            &self,
            request: Request,
            _context: GatewayRequestContext,
        ) -> GatewayResponse {
            let status = if to_bytes(request.into_body(), 1024).await.is_ok() {
                StatusCode::OK
            } else {
                StatusCode::REQUEST_TIMEOUT
            };
            let mut response =
                GatewayResponse::new(Response::new(Body::empty()), GatewayOperation::Write);
            *response.response.status_mut() = status;
            response
        }
    }

    #[tokio::test]
    async fn stalled_request_body_hits_the_idle_timeout() {
        let config = GatewayConfig {
            request_deadline: std::time::Duration::from_secs(1),
            body_idle_timeout: std::time::Duration::from_millis(10),
            ..GatewayConfig::default()
        };
        let app = gateway_router(runtime_with(
            config,
            Arc::new(BodyReadingAdapter),
            GatewaySecurity::default(),
        ));
        let body = Body::from_stream(futures::stream::pending::<
            Result<Bytes, std::convert::Infallible>,
        >());
        let response = app
            .oneshot(HttpRequest::post("/object").body(body).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn request_deadline_cancels_adapter_work() {
        let adapter = Arc::new(TestAdapter {
            calls: AtomicUsize::new(0),
            behavior: AdapterBehavior::Pending,
        });
        let config = GatewayConfig {
            request_deadline: std::time::Duration::from_millis(10),
            ..GatewayConfig::default()
        };
        let app = gateway_router(runtime_with(
            config,
            adapter.clone(),
            GatewaySecurity::default(),
        ));
        let response = app
            .oneshot(HttpRequest::get("/object").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    }

    struct BlockingAdapter {
        calls: AtomicUsize,
        release: tokio::sync::Semaphore,
    }

    #[async_trait]
    impl GatewayAdapter for BlockingAdapter {
        fn protocol(&self) -> crate::ProviderProtocol {
            crate::ProviderProtocol::S3
        }

        async fn handle(
            &self,
            _request: Request,
            _context: GatewayRequestContext,
        ) -> GatewayResponse {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let permit = self.release.acquire().await.unwrap();
            permit.forget();
            GatewayResponse::new(Response::new(Body::empty()), GatewayOperation::Read)
        }
    }

    #[tokio::test]
    async fn provider_concurrency_is_bounded_without_blocking_health_routes() {
        let adapter = Arc::new(BlockingAdapter {
            calls: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let config = GatewayConfig {
            max_concurrency: 1,
            ..GatewayConfig::default()
        };
        let app = gateway_router(runtime_with(
            config,
            adapter.clone(),
            GatewaySecurity::default(),
        ));
        let first = tokio::spawn(
            app.clone()
                .oneshot(HttpRequest::get("/first").body(Body::empty()).unwrap()),
        );
        while adapter.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let second = tokio::spawn(
            app.clone()
                .oneshot(HttpRequest::get("/second").body(Body::empty()).unwrap()),
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);

        let health = app
            .oneshot(HttpRequest::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        adapter.release.add_permits(2);
        assert!(first.await.unwrap().is_ok());
        assert!(second.await.unwrap().is_ok());
    }

    struct StreamingAdapter {
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl GatewayAdapter for StreamingAdapter {
        fn protocol(&self) -> crate::ProviderProtocol {
            crate::ProviderProtocol::Azure
        }

        async fn handle(
            &self,
            _request: Request,
            _context: GatewayRequestContext,
        ) -> GatewayResponse {
            let polls = Arc::clone(&self.polls);
            let guard = DropSignal(Arc::clone(&self.dropped));
            let stream = futures::stream::unfold((0u8, guard), move |(index, guard)| {
                let polls = Arc::clone(&polls);
                async move {
                    polls.fetch_add(1, Ordering::SeqCst);
                    Some((
                        Ok::<_, std::convert::Infallible>(Bytes::from(vec![index])),
                        (index + 1, guard),
                    ))
                }
            });
            GatewayResponse::new(
                Response::new(Body::from_stream(stream)),
                GatewayOperation::Read,
            )
        }
    }

    #[tokio::test]
    async fn response_body_is_demand_driven_and_drop_cancels_it() {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let adapter = Arc::new(StreamingAdapter {
            polls: Arc::clone(&polls),
            dropped: Arc::clone(&dropped),
        });
        let app = gateway_router(runtime_with(
            GatewayConfig::default(),
            adapter,
            GatewaySecurity::default(),
        ));
        let response = app
            .oneshot(HttpRequest::get("/object").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let mut body = response.into_body().into_data_stream();
        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(&[0])
        );
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        drop(body);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn total_deadline_also_bounds_a_streaming_response() {
        let adapter = Arc::new(StreamingAdapter {
            polls: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicBool::new(false)),
        });
        let config = GatewayConfig {
            request_deadline: std::time::Duration::from_millis(10),
            ..GatewayConfig::default()
        };
        let response = gateway_router(runtime_with(config, adapter, GatewaySecurity::default()))
            .oneshot(HttpRequest::get("/object").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let mut body = response.into_body().into_data_stream();
        assert!(body.next().await.unwrap().is_ok());
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        assert!(body.next().await.unwrap().is_err());
        assert!(body.next().await.is_none());
    }

    #[tokio::test]
    async fn every_response_has_an_opaque_request_id_and_bounded_metrics() {
        let runtime = runtime_with(
            GatewayConfig::default(),
            TestAdapter::immediate(),
            GatewaySecurity::default(),
        );
        let response = gateway_router(Arc::clone(&runtime))
            .oneshot(
                HttpRequest::get("/secret-bucket/credential-like-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()[REQUEST_ID_HEADER].as_bytes().len(), 36);
        let _ = to_bytes(response.into_body(), 4096).await.unwrap();
        let metrics = runtime.metrics().render();
        assert!(metrics.contains("protocol=\"s3\""));
        assert!(!metrics.contains("secret-bucket"));
        assert!(!metrics.contains("credential-like-key"));
    }

    #[tokio::test]
    async fn serve_stops_cleanly_after_shutdown_signal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runtime = runtime_with(
            GatewayConfig::default(),
            TestAdapter::immediate(),
            GatewaySecurity::default(),
        );
        let readiness = Arc::clone(runtime.readiness());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve(listener, runtime, async move {
            let _ = shutdown_rx.await;
        }));
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!readiness.is_live());
        assert!(!readiness.is_ready());
    }

    #[tokio::test]
    async fn maintained_http_parser_rejects_malformed_requests() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime = runtime_with(
            GatewayConfig::default(),
            TestAdapter::immediate(),
            GatewaySecurity::default(),
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve(listener, runtime, async move {
            let _ = shutdown_rx.await;
        }));

        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"not an HTTP request\r\n\r\n")
            .await
            .unwrap();
        let mut response = [0; 512];
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response[..read].starts_with(b"HTTP/1.1 400"));

        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
}
