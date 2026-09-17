#![cfg(feature = "telemetry")]
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::sync::{Arc, Mutex};
use talon_transport::{codec, FrameHeader, HEADER_LEN};
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

#[tokio::test]
async fn negative_ack_marks_fresh_and_reused_rpc_as_errors() {
    talon_telemetry::configure(talon_telemetry::Config {
        mode: talon_telemetry::Mode::Standard,
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
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("negative-ack"))),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut payload = vec![0; header.length as usize];
            socket.read_exact(&mut payload).await.unwrap();
            socket
                .write_all(
                    &codec::encode(
                        header.request_id,
                        &codec::ControlMessage::Ack {
                            ok: false,
                            detail: Some("object not found".into()),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    async {
        let client = talon_cache_client::CoordinatorClient::new(address.to_string());
        let object = talon_core::ObjectId::new(talon_core::Backend::S3, "bucket", "object");
        for _ in 0..2 {
            assert!(client.stat_object(&object).await.is_err());
        }
    }
    .with_subscriber(dispatch)
    .await;
    server.await.unwrap();
    provider.force_flush().unwrap();
    let spans = capture.0.lock().unwrap();
    let rpcs: Vec<_> = spans.iter().filter(|s| s.name == "talon.rpc").collect();
    assert_eq!(rpcs.len(), 2);
    for rpc in rpcs {
        assert!(matches!(
            rpc.status,
            opentelemetry::trace::Status::Error { .. }
        ));
        assert!(rpc
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "talon.outcome" && a.value.to_string() == "error"));
    }
}
