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
async fn v2_stat_cold_hit_cache_only_and_v1_on_same_connection() {
    talon_telemetry::configure(talon_telemetry::Config {
        mode: talon_telemetry::Mode::Propagate,
        ..Default::default()
    })
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
        "00-11111111111111111111111111111111-2222222222222222-00",
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
    for (index, version) in [2, 2, 1].into_iter().enumerate() {
        let range = data::RangeRequest {
            object: object.clone(),
            offset: 0,
            len: 4,
        };
        let versioned = data::VersionedRangeRequest {
            request: range.clone(),
            version: Version::new("v1"),
        };
        let mut request = match index {
            0 => data::encode_versioned_request(2, &versioned).unwrap(),
            1 => data::encode_versioned_tenant_request(
                2,
                &data::TenantScopedVersionedRange {
                    tenant: talon_core::TenantId::named("acme"),
                    request: versioned,
                },
            )
            .unwrap(),
            _ => data::encode_request(2, &range).unwrap(),
        };
        if version == 2 {
            envelope::encode(&mut request, Some(&parent), None).unwrap();
        }
        let (header, response) = exchange(&mut stream, request).await;
        assert_eq!(header.version, version);
        assert_eq!(&response[HEADER_LEN..], &[0, 1, 2, 3]);
    }
    for (source_version, code) in [
        ("stale", talon_transport::DataErrorCode::VersionMismatch),
        (" ", talon_transport::DataErrorCode::InvalidRequest),
    ] {
        let mut request = data::encode_versioned_request(
            4,
            &data::VersionedRangeRequest {
                request: data::RangeRequest {
                    object: object.clone(),
                    offset: 32,
                    len: 4,
                },
                version: Version::new(source_version),
            },
        )
        .unwrap();
        envelope::encode(&mut request, Some(&parent), None).unwrap();
        let (header, response) = exchange(&mut stream, request).await;
        assert_eq!(header.version, 2);
        assert_eq!(
            data::decode_error_payload(&response[HEADER_LEN..]).code,
            code
        );
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
    assert!(backend
        .0
        .lock()
        .unwrap()
        .iter()
        .all(|c| c.as_ref() == Some(&parent)));
    drop(stream);
    server.await.unwrap();
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
