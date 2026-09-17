use super::{telemetry_request, TelemetryBody};
use axum::{body::Body, response::Response, routing::get, Router};
use futures::StreamExt;
use http_body_util::BodyExt;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing_subscriber::layer::SubscriberExt;
#[derive(Clone, Debug, Default)]
struct Capture(Arc<Mutex<Vec<SpanData>>>);
impl SpanExporter for Capture {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send>,
    > {
        self.0.lock().unwrap().extend(batch);
        Box::pin(async { Ok(()) })
    }
}
fn attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|a| a.key.as_str() == key)
        .map(|a| a.value.to_string())
}
#[tokio::test]
async fn http_framing_completes_success_and_preserves_cancellation() {
    talon_telemetry::configure(talon_telemetry::Config {
        mode: talon_telemetry::Mode::Standard,
        root_sample_ratio: 1.0,
        ..Default::default()
    })
    .unwrap();
    let capture = Capture::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(capture.clone())
        .with_max_attributes_per_span(48)
        .build();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("repro"))),
    )
    .unwrap();
    let router = Router::new()
        .route(
            "/fixed",
            get(|| async {
                Response::builder()
                    .header("content-length", "3")
                    .body(Body::from("abc"))
                    .unwrap()
            }),
        )
        .route(
            "/chunked",
            get(|| async {
                Body::from_stream(futures::stream::iter([Ok::<_, std::io::Error>("abc")]))
            }),
        )
        .route(
            "/empty",
            get(|| async { axum::http::StatusCode::NO_CONTENT }),
        )
        .route(
            "/not-modified",
            get(|| async { axum::http::StatusCode::NOT_MODIFIED }),
        )
        .route(
            "/missing",
            get(|| async {
                Response::builder()
                    .status(404)
                    .header("content-length", "3")
                    .body(Body::from("err"))
                    .unwrap()
            }),
        )
        .layer(axum::middleware::from_fn(telemetry_request));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });
    for (method, path, expected_outcome) in [
        ("GET", "/fixed", "success"),
        ("HEAD", "/fixed", "success"),
        ("GET", "/chunked", "success"),
        ("GET", "/empty", "success"),
        ("GET", "/not-modified", "success"),
        ("GET", "/missing", "http_error"),
    ] {
        let before = capture.0.lock().unwrap().len();
        let mut conn = tokio::net::TcpStream::connect(address).await.unwrap();
        conn.write_all(
            format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\ntraceparent: 00-99999999999999999999999999999999-1111111111111111-01\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
        let mut response = Vec::new();
        conn.read_to_end(&mut response).await.unwrap();
        provider.force_flush().unwrap();
        let spans = capture.0.lock().unwrap();
        let span = spans[before..]
            .iter()
            .find(|s| {
                s.name == "talon.gateway"
                    && s.span_context.trace_id().to_string() == "99999999999999999999999999999999"
            })
            .unwrap();
        println!(
            "{method} {path}: HTTP={}, outcome={}, complete={}, bytes={:?}",
            String::from_utf8_lossy(&response).lines().next().unwrap(),
            attr(span, "talon.outcome").unwrap(),
            attr(span, "talon.details.complete").unwrap(),
            attr(span, "talon.response.bytes")
        );
        assert_eq!(
            attr(span, "talon.outcome").as_deref(),
            Some(expected_outcome)
        );
        assert_eq!(
            attr(span, "talon.details.complete").as_deref(),
            Some("true")
        );
    }
    // A body dropped before its declared length remains cancelled with partial bytes.
    let op = talon_telemetry::Operation::new(
        "early-drop",
        "internal",
        talon_telemetry::TraceParent::Root,
    );
    let body = Body::from_stream(
        futures::stream::once(async { Ok::<_, std::io::Error>("abc") })
            .chain(futures::stream::pending()),
    );
    let mut observed = TelemetryBody {
        body,
        operation: op,
        bytes: 0,
        length: Some(6),
        http_error: false,
    };
    assert_eq!(
        observed
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .len(),
        3
    );
    drop(observed);
    provider.force_flush().unwrap();
    {
        let spans = capture.0.lock().unwrap();
        let span = spans.iter().find(|s| s.name == "early-drop").unwrap();
        assert_eq!(attr(span, "talon.outcome").as_deref(), Some("cancelled"));
        assert_eq!(attr(span, "talon.response.bytes").as_deref(), Some("3"));
    }
    shutdown_tx.send(()).unwrap();
    server.await.unwrap();
}
