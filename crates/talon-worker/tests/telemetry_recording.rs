#![cfg(feature = "telemetry")]
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use talon_core::{
    Backend, BackendStore, NodeId, NodeInfo, NodeRole, ObjectId, ObjectStat, Result, Version,
};
use talon_transport::{codec, data, envelope, FrameHeader, HEADER_LEN};
use talon_worker::{
    BlockIndex, InFlightLoads, WholeBlockStore, WorkerObservability, WorkerRuntime,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Origin(Mutex<Vec<Option<talon_telemetry::TraceContext>>>);
#[async_trait]
impl BackendStore for Origin {
    async fn fetch_range(&self, _: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
        self.0
            .lock()
            .unwrap()
            .push(talon_telemetry::current_carrier());
        Ok((offset..offset + len)
            .map(|i| i as u8)
            .collect::<Vec<_>>()
            .into())
    }
    async fn fetch_range_if_match(
        &self,
        object: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> Result<Bytes> {
        if let Some(expected) = if_match {
            if expected.as_str() != "v1" {
                return Err(talon_core::Error::VersionMismatch {
                    expected: expected.0.clone(),
                    found: "v1".into(),
                });
            }
        }
        self.fetch_range(object, offset, len).await
    }
    async fn head(&self, _: &ObjectId) -> Result<ObjectStat> {
        self.0
            .lock()
            .unwrap()
            .push(talon_telemetry::current_carrier());
        Ok(ObjectStat {
            len: 64,
            version: Version::new("v1"),
        })
    }
}

#[tokio::test]
async fn sampled_worker_counts_actual_refill_and_keeps_hit_zero() {
    talon_telemetry::configure(talon_telemetry::Config {
        mode: talon_telemetry::Mode::Standard,
        ..Default::default()
    })
    .unwrap();
    use opentelemetry::trace::TracerProvider;
    use tracing_subscriber::layer::SubscriberExt;
    let capture = Capture::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(capture.clone())
        .build();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("worker-test"))),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let index = Arc::new(BlockIndex::new());
    let inflight = Arc::new(InFlightLoads::new());
    let backend = Arc::new(Origin(Mutex::new(Vec::new())));
    let obs = Arc::new(
        WorkerObservability::new(
            "c".into(),
            NodeInfo {
                id: NodeId::new("w"),
                address: "127.0.0.1:1".into(),
                role: NodeRole::Worker,
            },
            "127.0.0.1:2".into(),
            1024,
            index.clone(),
            inflight.clone(),
        )
        .unwrap(),
    );
    obs.readiness().set_backend_ready(true);
    obs.readiness().set_store_ready(true);
    obs.readiness().set_control_registered(true);
    let worker = Arc::new(WorkerRuntime::new(
        WholeBlockStore::open(root.path()).unwrap(),
        index,
        inflight,
        backend.clone(),
        16,
        0,
        obs.metrics().clone(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        talon_worker::tokio_conn::handle_conn(stream, worker, obs)
            .await
            .unwrap();
    });
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let parent = talon_telemetry::TraceContext::from_w3c(
        "00-11111111111111111111111111111111-2222222222222222-01",
        Some("test=value"),
    )
    .unwrap();
    let object = ObjectId::new(Backend::S3, "bucket", "object");
    let mut stat = codec::encode(
        1,
        &codec::ControlMessage::StatObject {
            object: object.clone(),
        },
    )
    .unwrap();
    envelope::encode(&mut stat, Some(&parent), None).unwrap();
    let (header, response) = exchange(&mut stream, stat).await;
    assert_eq!(header.version, 2);
    assert!(matches!(
        codec::decode(&response).unwrap().1,
        codec::ControlMessage::ObjectStat { size: 64, .. }
    ));
    for version in [2, 2, 1] {
        let mut request = data::encode_request(
            2,
            &data::RangeRequest {
                object: object.clone(),
                offset: 0,
                len: 4,
            },
        )
        .unwrap();
        if version == 2 {
            envelope::encode(&mut request, Some(&parent), None).unwrap();
        }
        let (header, response) = exchange(&mut stream, request).await;
        assert_eq!(header.version, version);
        assert_eq!(&response[HEADER_LEN..], &[0, 1, 2, 3]);
    }
    let calls_before = backend.0.lock().unwrap().len();
    let mut request = data::encode_cached_request(
        3,
        &data::CachedRangeRequest {
            object,
            version: Version::new("missing"),
            offset: 0,
            len: 4,
        },
    )
    .unwrap();
    envelope::encode(&mut request, Some(&parent), None).unwrap();
    let (header, response) = exchange(&mut stream, request).await;
    assert_eq!(header.version, 2);
    assert_eq!(
        data::decode_error_payload(&response[HEADER_LEN..]).code,
        talon_transport::DataErrorCode::CacheMiss
    );
    assert_eq!(backend.0.lock().unwrap().len(), calls_before);
    assert!(backend.0.lock().unwrap().iter().all(|c| c
        .as_ref()
        .is_some_and(|c| c.traceparent()[3..35] == parent.traceparent()[3..35])));
    drop(stream);
    server.await.unwrap();
    // Captured follower attribution must survive replacement of the map entry.
    let flights = Arc::new(InFlightLoads::new());
    let key = talon_worker::LoadKey::Whole(talon_core::BlockId::new(
        ObjectId::new(Backend::S3, "b", "flight"),
        0,
        16,
        Version::new("v"),
    ));
    let leader = talon_telemetry::Operation::new(
        "old-leader",
        "internal",
        talon_telemetry::TraceParent::Explicit(&parent),
    );
    let (guard, _) = leader.in_scope(|| flights.admit_traced(key.clone()));
    let (_, snapshot) = leader.in_scope(|| flights.admit_traced(key.clone()));
    leader.in_scope(|| flights.bind_trace(&key));
    let old_span_id = leader.carrier().unwrap().traceparent()[36..52].to_string();
    drop(guard);
    let replacement = talon_telemetry::Operation::new(
        "replacement",
        "internal",
        talon_telemetry::TraceParent::Explicit(&parent),
    );
    let (guard, _) = replacement.in_scope(|| flights.admit_traced(key.clone()));
    replacement.in_scope(|| flights.bind_trace(&key));
    let follower = talon_telemetry::Operation::new(
        "follower",
        "internal",
        talon_telemetry::TraceParent::Explicit(&parent),
    );
    snapshot.link(&follower);
    follower.outcome("success");
    drop(follower);
    drop(guard);
    leader.outcome("success");
    replacement.outcome("success");
    drop(leader);
    drop(replacement);
    provider.force_flush().unwrap();
    let spans = capture.0.lock().unwrap();
    assert_eq!(spans.iter().filter(|s| s.name == "talon.refill").count(), 1);
    let follower = spans.iter().find(|s| s.name == "follower").unwrap();
    assert_eq!(
        follower.links.links[0].span_context.span_id().to_string(),
        old_span_id
    );
    assert_eq!(
        attr(follower, "talon.refill.shared_dependencies"),
        Some("1".into())
    );
    assert_eq!(
        attr(follower, "talon.dependencies.complete"),
        Some("false".into())
    );
    assert!(spans.iter().any(|s| s.name == "talon.rpc"
        && attr(s, "talon.cache.tier") == Some("l2".into())
        && attr(s, "talon.refill.started") == Some("0".into())));
    let cold = spans
        .iter()
        .find(|s| s.name == "talon.rpc" && attr(s, "talon.refill.started") == Some("1".into()))
        .unwrap();
    assert_eq!(attr(cold, "talon.refill.completed"), Some("1".into()));
    assert_eq!(
        attr(cold, "talon.origin.validated_bytes"),
        Some("16".into())
    );
    assert_eq!(attr(cold, "talon.cache.committed_bytes"), Some("16".into()));
    assert_eq!(attr(cold, "talon.details.complete"), Some("true".into()));
    assert_eq!(cold.parent_span_id.to_string(), "2222222222222222");
    assert!(spans
        .iter()
        .any(|s| s.name == "talon.rpc" && attr(s, "talon.outcome") == Some("cache_miss".into())));
}

async fn exchange(stream: &mut tokio::net::TcpStream, request: Vec<u8>) -> (FrameHeader, Vec<u8>) {
    stream.write_all(&request).await.unwrap();
    let mut header = [0; HEADER_LEN];
    stream.read_exact(&mut header).await.unwrap();
    let decoded = FrameHeader::decode(&header).unwrap();
    let mut response = header.to_vec();
    response.resize(HEADER_LEN + decoded.length as usize, 0);
    stream
        .read_exact(&mut response[HEADER_LEN..])
        .await
        .unwrap();
    (decoded, response)
}

#[derive(Clone, Debug, Default)]
struct Capture(Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);
impl opentelemetry_sdk::trace::SpanExporter for Capture {
    fn export(
        &mut self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send>,
    > {
        self.0.lock().unwrap().extend(batch);
        Box::pin(async { Ok(()) })
    }
}
fn attr(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|a| a.key.as_str() == key)
        .map(|a| a.value.to_string())
}
