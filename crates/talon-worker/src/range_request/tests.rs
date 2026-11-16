use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use talon_core::{
    Backend, BackendStore, MetricLimits, NodeId, NodeInfo, NodeRole, ObjectId, ObjectStat,
    RateLimit, RateLimitPolicy, WorkerConfig,
};
use talon_transport::{data::VersionedRangeRequest, envelope};

#[derive(Default)]
struct Origin {
    heads: AtomicUsize,
    ranges: Mutex<Vec<(u64, u64)>>,
}

#[async_trait]
impl BackendStore for Origin {
    async fn head(&self, _: &ObjectId) -> talon_core::Result<ObjectStat> {
        self.heads.fetch_add(1, Ordering::Relaxed);
        Ok(ObjectStat {
            len: 2 << 20,
            version: Version::new("v1"),
        })
    }

    async fn fetch_range(&self, _: &ObjectId, offset: u64, len: u64) -> talon_core::Result<Bytes> {
        self.ranges.lock().unwrap().push((offset, len));
        Ok(Bytes::from(
            (offset..offset + len)
                .map(|n| (n % 251) as u8)
                .collect::<Vec<_>>(),
        ))
    }

    async fn fetch_range_if_match(
        &self,
        object: &ObjectId,
        offset: u64,
        len: u64,
        expected: Option<&Version>,
    ) -> talon_core::Result<Bytes> {
        if expected.is_some_and(|version| version.as_str() != "v1") {
            return Err(talon_core::Error::VersionMismatch {
                expected: expected.unwrap().0.clone(),
                found: "v1".into(),
            });
        }
        self.fetch_range(object, offset, len).await
    }
}

struct Fixture {
    worker: Arc<WorkerRuntime>,
    observability: WorkerObservability,
    origin: Arc<Origin>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new(limited: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let origin = Arc::new(Origin::default());
        let observability = WorkerObservability::new(
            "cluster".into(),
            NodeInfo {
                id: NodeId::new("worker"),
                address: "127.0.0.1:7001".into(),
                role: NodeRole::Worker,
            },
            "127.0.0.1:8001".into(),
            32 << 20,
            Arc::new(crate::BlockIndex::new()),
            Arc::new(crate::InFlightLoads::new()),
        )
        .unwrap();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        observability.readiness().set_control_registered(true);
        let config = WorkerConfig {
            backend: Some("s3".into()),
            block_size: 4096,
            capacity_bytes: 32 << 20,
            l2_page_size_bytes: 0,
            l1_capacity_bytes: 0,
            page_tti_hours: 0,
            small_object_threshold_bytes: 128 << 10,
            small_object_segment_bytes: 2 << 20,
            rate_limits: RateLimitPolicy {
                enabled: limited,
                default: MetricLimits {
                    read_iops: Some(RateLimit::new(1, 1.0).unwrap()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        let lock = Arc::new(crate::page_access_store::CacheRootLock::acquire(dir.path()).unwrap());
        let worker = Arc::new(
            WorkerRuntime::open_namespaces(
                &config,
                lock,
                origin.clone(),
                observability.metrics().clone(),
            )
            .unwrap(),
        );
        Self {
            worker,
            observability,
            origin,
            _dir: dir,
        }
    }

    async fn execute(&self, frame: &[u8]) -> Result<ServeOutcome, RangeError> {
        let header = FrameHeader::decode(frame).unwrap();
        let payload = &frame[HEADER_LEN..];
        let worker = self
            .worker
            .for_request(&header, payload)
            .await
            .map_err(RangeError::Request)?;
        serve(&header, payload, &worker, &self.observability).await
    }
}

fn request() -> VersionedRangeRequest {
    VersionedRangeRequest {
        request: data::RangeRequest {
            object: ObjectId::new(Backend::S3, "bucket", "key"),
            offset: 17,
            len: 23,
        },
        version: Version::new("v1"),
        object_len: 2 << 20,
    }
}

fn frame(kind: MsgType, version: u8, request: &VersionedRangeRequest) -> Vec<u8> {
    let mut frame = match kind {
        MsgType::GetRange => data::encode_request(7, &request.request).unwrap(),
        MsgType::GetRangeNamespace => data::encode_namespace_request(
            7,
            &data::NamespaceScopedRange {
                namespace: NamespaceId::named("team"),
                request: request.request.clone(),
            },
        )
        .unwrap(),
        MsgType::GetVersionedRange => data::encode_versioned_request(7, request).unwrap(),
        MsgType::GetVersionedRangeNamespace => data::encode_versioned_namespace_request(
            7,
            &data::NamespaceScopedVersionedRange {
                namespace: NamespaceId::named("team"),
                request: request.clone(),
            },
        )
        .unwrap(),
        MsgType::GetExactRange => data::encode_exact_request(7, request).unwrap(),
        MsgType::GetExactRangeNamespace => data::encode_exact_namespace_request(
            7,
            &data::NamespaceScopedVersionedRange {
                namespace: NamespaceId::named("team"),
                request: request.clone(),
            },
        )
        .unwrap(),
        _ => unreachable!(),
    };
    match version {
        1 => {}
        2 => envelope::encode(&mut frame, None, None).unwrap(),
        3 => envelope::encode_namespace(&mut frame, &NamespaceId::named("team")).unwrap(),
        _ => unreachable!(),
    }
    frame
}

fn error_code(error: &RangeError, frame: &[u8]) -> DataErrorCode {
    let request = FrameHeader::decode(frame).unwrap();
    let reply = error.response(&request);
    let header = FrameHeader::decode(&reply).unwrap();
    assert_eq!(header.version, request.version);
    assert_eq!(header.request_id, request.request_id);
    assert!(header.flags.contains(talon_transport::Flags::ERROR));
    data::decode_error_payload(&reply[HEADER_LEN..]).code
}

async fn rejected(fixture: &Fixture, frame: &[u8], code: DataErrorCode) -> RangeError {
    let error = fixture
        .execute(frame)
        .await
        .err()
        .expect("request must fail");
    assert_eq!(error_code(&error, frame), code);
    error
}

#[tokio::test]
async fn all_range_forms_dispatch_with_the_selected_namespace() {
    for kind in [
        MsgType::GetRange,
        MsgType::GetRangeNamespace,
        MsgType::GetVersionedRange,
        MsgType::GetVersionedRangeNamespace,
        MsgType::GetExactRange,
        MsgType::GetExactRangeNamespace,
    ] {
        for version in [1, 2, 3] {
            let fixture = Fixture::new(false);
            let frame = frame(kind, version, &request());
            let header = FrameHeader::decode(&frame).unwrap();
            let selected = fixture
                .worker
                .for_request(&header, &frame[HEADER_LEN..])
                .await
                .unwrap();
            let scoped = version == 3
                || matches!(
                    kind,
                    MsgType::GetRangeNamespace
                        | MsgType::GetVersionedRangeNamespace
                        | MsgType::GetExactRangeNamespace
                );
            assert_eq!(
                selected.namespace().effective_name(),
                if scoped { "team" } else { "default" }
            );
            let result = serve(
                &header,
                &frame[HEADER_LEN..],
                &selected,
                &fixture.observability,
            )
            .await
            .unwrap();
            let len = match result {
                ServeOutcome::Bytes(bytes) => {
                    assert_eq!(
                        bytes.as_ref(),
                        &(17..40).map(|n| (n % 251) as u8).collect::<Vec<_>>()
                    );
                    bytes.len() as u64
                }
                ServeOutcome::Sendfile(handle) => handle.len,
                ServeOutcome::SendfileMany(handles) => handles.iter().map(|h| h.len).sum(),
            };
            assert_eq!(len, 23);
            let ordinary = matches!(kind, MsgType::GetRange | MsgType::GetRangeNamespace);
            assert_eq!(
                fixture.origin.heads.load(Ordering::Relaxed),
                usize::from(ordinary)
            );
            let exact = matches!(
                kind,
                MsgType::GetExactRange | MsgType::GetExactRangeNamespace
            );
            assert_eq!(
                *fixture.origin.ranges.lock().unwrap(),
                if exact {
                    vec![(17, 23)]
                } else {
                    vec![(0, 4096)]
                }
            );
        }
    }
}

#[tokio::test]
async fn invalid_coordinates_versions_and_exact_limit_preserve_wire_errors() {
    let fixture = Fixture::new(false);
    for version in [1, 2, 3] {
        for kind in [
            MsgType::GetVersionedRange,
            MsgType::GetVersionedRangeNamespace,
            MsgType::GetExactRange,
            MsgType::GetExactRangeNamespace,
        ] {
            for (offset, len, source_version, size) in [
                (u64::MAX, 1, "v1", u64::MAX),
                (17, 23, "v1", 39),
                (17, 23, " ", 2 << 20),
            ] {
                let mut request = request();
                request.request.offset = offset;
                request.request.len = len;
                request.version = Version::new(source_version);
                request.object_len = size;
                rejected(
                    &fixture,
                    &frame(kind, version, &request),
                    DataErrorCode::InvalidRequest,
                )
                .await;
            }
        }
        for kind in [MsgType::GetExactRange, MsgType::GetExactRangeNamespace] {
            let mut request = request();
            request.request.len = talon_core::MAX_EXACT_READ_LEN + 1;
            rejected(
                &fixture,
                &frame(kind, version, &request),
                DataErrorCode::InvalidRequest,
            )
            .await;
        }
    }
    assert_eq!(fixture.origin.heads.load(Ordering::Relaxed), 0);
    assert!(fixture.origin.ranges.lock().unwrap().is_empty());
}

#[tokio::test]
async fn decode_and_namespace_checks_precede_readiness() {
    let fixture = Fixture::new(false);
    fixture.observability.readiness().set_backend_ready(false);
    let bad_payload = [1];
    let header = FrameHeader::new(MsgType::GetRange, 7, 1);
    let error = serve(
        &header,
        &bad_payload,
        &fixture.worker,
        &fixture.observability,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(
        error_code(&error, &[header.encode().as_slice(), &bad_payload].concat()),
        DataErrorCode::InvalidRequest
    );
    let scoped = data::NamespaceScopedRange {
        namespace: NamespaceId::named("../bad"),
        request: request().request,
    };
    let frame = data::encode_namespace_request(7, &scoped).unwrap();
    let header = FrameHeader::decode(&frame).unwrap();
    let error = serve(
        &header,
        &frame[HEADER_LEN..],
        &fixture.worker,
        &fixture.observability,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error_code(&error, &frame), DataErrorCode::InvalidRequest);
    assert_eq!(fixture.origin.heads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn readiness_and_range_checks_do_not_consume_rate_limit() {
    let fixture = Fixture::new(true);
    let mut invalid = request();
    invalid.request.offset = u64::MAX;
    invalid.version = Version::new("");
    let invalid_frame = frame(MsgType::GetVersionedRange, 1, &invalid);
    fixture.observability.readiness().set_backend_ready(false);
    rejected(&fixture, &invalid_frame, DataErrorCode::Unavailable).await;
    fixture.observability.readiness().set_backend_ready(true);
    let error = rejected(&fixture, &invalid_frame, DataErrorCode::InvalidRequest).await;
    let message = data::decode_error_payload(
        &error.response(&FrameHeader::decode(&invalid_frame).unwrap())[HEADER_LEN..],
    )
    .message;
    assert!(message.starts_with("range exceeds"));
    let mut valid = request();
    valid.request.len = 0;
    let valid_frame = frame(MsgType::GetVersionedRange, 1, &valid);
    assert!(fixture.execute(&valid_frame).await.is_ok());
    rejected(&fixture, &valid_frame, DataErrorCode::RateLimited).await;
    rejected(&fixture, &invalid_frame, DataErrorCode::InvalidRequest).await;
    assert!(fixture.origin.ranges.lock().unwrap().is_empty());
    assert_eq!(fixture.origin.heads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn runtime_failures_keep_typed_codes_and_post_send_accounting() {
    let fixture = Fixture::new(false);
    let mut request = request();
    request.version = Version::new("stale");
    let frame = frame(MsgType::GetVersionedRange, 2, &request);
    let error = rejected(&fixture, &frame, DataErrorCode::VersionMismatch).await;
    assert_eq!(fixture.observability.status().metrics.errors_total, 0);
    error.record(fixture.observability.metrics(), Duration::ZERO);
    assert_eq!(fixture.observability.status().metrics.errors_total, 1);
    assert_eq!(fixture.observability.status().metrics.requests_total, 1);
}

#[tokio::test]
async fn throttling_is_separate_from_request_error_accounting() {
    let fixture = Fixture::new(true);
    let mut request = request();
    request.request.len = 0;
    let frame = frame(MsgType::GetVersionedRange, 3, &request);
    assert!(fixture.execute(&frame).await.is_ok());
    let error = rejected(&fixture, &frame, DataErrorCode::RateLimited).await;
    error.record(fixture.observability.metrics(), Duration::ZERO);
    assert_eq!(fixture.observability.status().metrics.errors_total, 0);
    assert_eq!(fixture.observability.status().metrics.requests_total, 0);
    assert!(fixture
        .observability
        .metrics()
        .render()
        .contains("talon_worker_rate_limited_total{metric=\"read_iops\"} 1"));
}

#[tokio::test]
async fn exact_limit_is_inclusive_and_does_not_round_the_origin_read() {
    let fixture = Fixture::new(false);
    let mut request = request();
    request.request.len = talon_core::MAX_EXACT_READ_LEN;
    let frame = frame(MsgType::GetExactRange, 3, &request);
    assert!(fixture.execute(&frame).await.is_ok());
    assert_eq!(
        *fixture.origin.ranges.lock().unwrap(),
        [(17, talon_core::MAX_EXACT_READ_LEN)]
    );
    assert_eq!(fixture.origin.heads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn ordinary_range_overflow_and_unsupported_type_fail_without_origin_io() {
    let fixture = Fixture::new(false);
    let mut request = request();
    request.request.offset = u64::MAX;
    for kind in [MsgType::GetRange, MsgType::GetRangeNamespace] {
        rejected(
            &fixture,
            &frame(kind, 2, &request),
            DataErrorCode::InvalidRequest,
        )
        .await;
    }
    let header = FrameHeader::new(MsgType::Control, 7, 0);
    let error = serve(&header, &[], &fixture.worker, &fixture.observability)
        .await
        .err()
        .unwrap();
    assert_eq!(
        error_code(&error, &header.encode()),
        DataErrorCode::InvalidRequest
    );
    assert_eq!(fixture.origin.heads.load(Ordering::Relaxed), 0);
    assert!(fixture.origin.ranges.lock().unwrap().is_empty());
}
