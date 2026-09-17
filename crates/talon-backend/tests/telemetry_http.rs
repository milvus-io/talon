#![cfg(feature = "telemetry")]
use futures::StreamExt;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::sync::{Arc, Mutex};
use talon_backend::{HttpClient, HttpRequest, Method, ReqwestClient};
use talon_telemetry::{Config, Mode, Operation, TraceParent};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::instrument::WithSubscriber;
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
fn attribute(span: &SpanData, key: &str) -> String {
    span.attributes
        .iter()
        .find(|a| a.key.as_str() == key)
        .unwrap_or_else(|| panic!("missing {key} on {}", span.name))
        .value
        .to_string()
}

#[tokio::test]
async fn attempts_own_body_until_eof_error_or_drop_and_preserve_partial_bytes() {
    talon_telemetry::configure(Config {
        mode: Mode::Standard,
        root_sample_ratio: 1.0,
        ..Default::default()
    })
    .unwrap();
    let capture = Capture::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(capture.clone())
        .build();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("http-test"))),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let held = release.clone();
    let server = tokio::spawn(async move {
        for (i, status) in [
            "200 OK",
            "503 Unavailable",
            "200 OK",
            "200 OK",
            "200 OK",
            "200 OK",
        ]
        .into_iter()
        .enumerate()
        {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
                assert!(request.len() < 4096);
            }
            let length = if i == 4 { 3 } else { 6 };
            // Deliberately advertise more bytes than delivered. Streaming callers
            // either observe the EOF error or abandon the body after this chunk.
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\nabc"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            if i == 5 {
                held.notified().await;
            }
            tokio::task::yield_now().await;
        }
    });
    async {
        let root = Operation::new("read", "internal", TraceParent::Root);
        root.scope(async {
            let client =
                ReqwestClient::with_client(reqwest::Client::builder().no_proxy().build().unwrap());
            for i in 0..6 {
                let request = HttpRequest::new(
                    Method::Get,
                    format!("http://{address}/?secret=never-record"),
                    vec![],
                );
                if i == 0 {
                    assert!(client.execute(request).await.is_err());
                } else if i == 5 {
                    let retry = talon_backend::RetryingHttpClient::new(
                        Arc::new(ReqwestClient::with_client(
                            reqwest::Client::builder().no_proxy().build().unwrap(),
                        )),
                        talon_backend::RetryConfig {
                            timeout_floor: std::time::Duration::from_millis(100),
                            ..talon_backend::RetryConfig::NONE
                        },
                        1,
                    );
                    assert!(retry.execute(request).await.is_err());
                    release.notify_one();
                } else {
                    let mut response = client.execute_stream(request).await.unwrap();
                    assert_eq!(&response.body.next().await.unwrap().unwrap()[..], b"abc");
                    if i == 4 {
                        assert!(response.body.next().await.is_none());
                    } else if i != 3 {
                        assert!(response.body.next().await.unwrap().is_err());
                    }
                }
            }
        })
        .await;
        root.outcome("success");
    }
    .with_subscriber(dispatch)
    .await;
    server.await.unwrap();
    provider.force_flush().unwrap();
    let spans = capture.0.lock().unwrap();
    let attempts: Vec<_> = spans.iter().filter(|s| s.name == "HTTP attempt").collect();
    assert_eq!(attempts.len(), 6);
    assert_eq!(
        attempts
            .iter()
            .filter(|s| attribute(s, "talon.outcome") == "timeout")
            .count(),
        1
    );
    assert_eq!(
        attempts
            .iter()
            .filter(|s| attribute(s, "talon.outcome") == "success")
            .count(),
        1
    );
    assert_eq!(
        attempts
            .iter()
            .filter(|s| attribute(s, "talon.outcome") == "cancelled")
            .count(),
        1
    );
    assert_eq!(
        attempts
            .iter()
            .filter(|s| attribute(s, "talon.outcome") == "error")
            .count(),
        3
    );
    for attempt in attempts {
        assert_eq!(attribute(attempt, "talon.origin.body_bytes"), "3");
        assert_eq!(
            attribute(attempt, "talon.http.attempt_boundary"),
            "reqwest.execute"
        );
        assert!(!format!("{:?}", attempt.attributes).contains("secret"));
    }
    let root = spans.iter().find(|s| s.name == "read").unwrap();
    assert_eq!(attribute(root, "talon.origin.get.attempts"), "6");
    assert_eq!(attribute(root, "talon.origin.body_bytes"), "18");
}
