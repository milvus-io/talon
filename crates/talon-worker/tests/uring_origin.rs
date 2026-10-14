//! Native ring acceptance tests. Run explicitly on an io_uring-capable host.
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use talon_backend::{
    http::{HttpClient, HttpRequest, Method},
    monoio_client::MonoioClient,
    S3Backend, S3Config, S3Credentials,
};
use talon_core::{Backend, BlockId, NamespaceId, ObjectId, Version, WorkerConfig};
use talon_transport::data::CachedRangeRequest;
use talon_worker::{page_access_store::CacheRootLock, WorkerMetrics, WorkerRuntime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// Tokio drives the fixture server, DNS and legacy cache maintenance only.
// The tested request future runs on this thread's native ring.
fn on_native_ring(work: impl std::future::Future<Output = ()>) {
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _entered = tokio.enter();
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap()
        .block_on(async {
            let ring = talon_uring::LocalRing::enter();
            work.await;
            ring.drain().await;
        });
}

struct Origin {
    address: std::net::SocketAddr,
    tls: Arc<rustls::ClientConfig>,
    requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Origin {
    async fn start(tls: bool) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                let acceptor = acceptor.clone();
                let count = count.clone();
                tokio::spawn(async move {
                    if tls {
                        if let Ok(stream) = acceptor.accept(socket).await {
                            respond(stream, count).await;
                        }
                    } else {
                        respond(socket, count).await;
                    }
                });
            }
        });
        Self {
            address,
            tls: Arc::new(client),
            requests,
            connections,
            task,
        }
    }
    fn client(&self) -> Arc<MonoioClient> {
        Arc::new(MonoioClient::with_tls_config(self.tls.clone()))
    }
    fn backend(&self, tls: bool) -> Arc<S3Backend> {
        Arc::new(S3Backend::new(
            S3Config {
                region: "test".into(),
                endpoint: format!("localhost:{}", self.address.port()),
                path_style: true,
                tls,
            },
            S3Credentials {
                access_key_id: "test".into(),
                secret_access_key: "test".into(),
                session_token: None,
            },
            self.client(),
        ))
    }
}

async fn respond<S: AsyncRead + AsyncWrite + Unpin>(mut stream: S, count: Arc<AtomicUsize>) {
    loop {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let Ok(byte) = stream.read_u8().await else {
                return;
            };
            request.push(byte);
            assert!(request.len() < 32768);
        }
        let attempt = count.fetch_add(1, Ordering::SeqCst);
        let request = String::from_utf8(request).unwrap();
        let target = request.split_whitespace().nth(1).unwrap();
        if target.starts_with("/stall") {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read_u8()).await;
            return;
        }
        if target.starts_with("/concurrent") {
            // No request may finish until more than the old 128 slots are active.
            while count.load(Ordering::SeqCst) < 129 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
        if target.contains("/large-") {
            let object_len: usize = target.rsplit("/large-").next().unwrap().parse().unwrap();
            let range = request
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("range: bytes=")
                        .map(str::to_owned)
                })
                .unwrap();
            let (start, end) = range.split_once('-').unwrap();
            let start: usize = start.parse().unwrap();
            let end: usize = end.parse::<usize>().unwrap().min(object_len - 1);
            let body: Vec<u8> = (start..=end).map(|i| (i % 251) as u8).collect();
            let chunked = target.contains("chunked");
            let framing = if chunked {
                "Transfer-Encoding: chunked".to_owned()
            } else {
                format!("Content-Length: {}", body.len())
            };
            let mut response = format!("HTTP/1.1 206 Partial Content\r\n{framing}\r\nContent-Range: bytes {start}-{end}/{object_len}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n").into_bytes();
            if chunked {
                for piece in body.chunks(777) {
                    response.extend_from_slice(format!("{:x};test=1\r\n", piece.len()).as_bytes());
                    response.extend_from_slice(piece);
                    response.extend_from_slice(b"\r\n");
                }
                response.extend_from_slice(b"0\r\nX-Test: trailer\r\n\r\n");
            } else {
                response.extend_from_slice(&body);
            }
            stream.write_all(&response).await.unwrap();
            let _ = stream.shutdown().await;
            return;
        }
        let keepalive = target.starts_with("/keepalive");
        let response = if target.contains("/retry") && attempt == 0 {
            "HTTP/1.1 503 Slow Down\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned()
        } else if keepalive || target.starts_with("/concurrent") {
            "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nabcdefgh".to_owned()
        } else if target.starts_with("/chunked") {
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n5\r\ndefgh\r\n0\r\n\r\n".to_owned()
        } else if target.contains("/truncated") {
            "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nabc".to_owned()
        } else if target.starts_with("/error") {
            "HTTP/1.1 503 Slow Down\r\nContent-Length: 5\r\nConnection: close\r\n\r\nretry"
                .to_owned()
        } else if request.starts_with("HEAD ") {
            "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n"
                .to_owned()
        } else {
            let range = request
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("range: bytes=")
                        .map(str::to_owned)
                })
                .unwrap();
            assert!(request.to_ascii_lowercase().contains("if-match: \"v1\""));
            let (start, end) = range.split_once('-').unwrap();
            let start: usize = start.parse().unwrap();
            let end: usize = end.parse::<usize>().unwrap().min(7);
            let body = &"abcdefgh"[start..=end];
            format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/8\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n{body}", body.len())
        };
        let _ = stream.write_all(response.as_bytes()).await;
        if keepalive {
            continue;
        }
        let _ = stream.shutdown().await;
        break;
    }
}

#[test]
#[ignore = "requires native io_uring; includes verified local HTTPS"]
fn uring_http_tls_framing_and_certificate_validation() {
    on_native_ring(async {
        for tls in [false, true] {
            let origin = Origin::start(tls).await;
            let client = origin.client();
            let url = format!(
                "{}://localhost:{}",
                if tls { "https" } else { "http" },
                origin.address.port()
            );
            let before = origin.connections.load(Ordering::SeqCst);
            for _ in 0..2 {
                let response = client
                    .execute(HttpRequest::new(
                        Method::Get,
                        format!("{url}/keepalive"),
                        vec![],
                    ))
                    .await
                    .unwrap();
                assert_eq!(response.body, "abcdefgh");
            }
            assert_eq!(
                origin.connections.load(Ordering::SeqCst),
                before + 1,
                "must reuse the TCP/TLS connection"
            );
            for (path, status, body) in [("/chunked", 200, "abcdefgh"), ("/error", 503, "retry")] {
                let response = client
                    .execute(HttpRequest {
                        method: Method::Get,
                        url: format!("{url}{path}"),
                        headers: vec![],
                        body: Bytes::new(),
                    })
                    .await
                    .unwrap();
                assert_eq!(response.status, status);
                assert_eq!(response.body, body);
            }
            let head = client
                .execute(HttpRequest {
                    method: Method::Head,
                    url: format!("{url}/head"),
                    headers: vec![],
                    body: Bytes::new(),
                })
                .await
                .unwrap();
            assert!(head.body.is_empty());
            assert_eq!(head.header("content-length"), Some("8"));
            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(100),
                client.execute(HttpRequest::new(
                    Method::Get,
                    format!("{url}/stall"),
                    vec![]
                ))
            )
            .await
            .is_err());
            let response = client
                .execute(HttpRequest::new(
                    Method::Get,
                    format!("{url}/keepalive"),
                    vec![],
                ))
                .await
                .unwrap();
            assert_eq!(
                response.body, "abcdefgh",
                "cancelled socket must never be pooled"
            );
            assert!(client
                .execute(HttpRequest {
                    method: Method::Get,
                    url: format!("{url}/truncated?secret=do-not-leak"),
                    headers: vec![],
                    body: Bytes::new()
                })
                .await
                .unwrap_err()
                .contains("truncated"));
            if tls {
                let untrusted = MonoioClient::new();
                let error = untrusted
                    .execute(HttpRequest {
                        method: Method::Get,
                        url: format!("{url}/chunked?secret=do-not-leak"),
                        headers: vec![],
                        body: Bytes::new(),
                    })
                    .await
                    .unwrap_err();
                assert!(error.contains("certificate"));
                assert!(!error.contains("do-not-leak"));
            }
        }
    });
}

#[test]
#[ignore = "requires native io_uring; exercises S3 HTTPS LOAD and restart"]
fn uring_https_load_whole_paged_namespace_and_recovery() {
    on_native_ring(async {
        let origin = Origin::start(true).await;
        let backend = origin.backend(true);
        for page_size in [0, 4] {
            let root = tempfile::tempdir().unwrap();
            let config = WorkerConfig {
                block_size: 8,
                capacity_bytes: 1024,
                l1_capacity_bytes: 0,
                l2_page_size_bytes: page_size,
                backend: Some("s3".into()),
                page_tti_hours: 0,
                page_access_checkpoint_interval_minutes: 1,
                ..Default::default()
            };
            let open = || {
                Arc::new(
                    WorkerRuntime::open_namespaces_with_uring(
                        &config,
                        Arc::new(CacheRootLock::acquire(root.path()).unwrap()),
                        backend.clone(),
                        WorkerMetrics::new(1024),
                        true,
                    )
                    .unwrap(),
                )
            };
            let runtime = open();
            let named = runtime
                .for_namespace(&NamespaceId::named("experiment"))
                .await
                .unwrap();
            let block = BlockId::new(
                ObjectId::new(Backend::S3, "bucket", "file"),
                0,
                8,
                Version::new("v1"),
            );
            named.load_block(&block, 8).await.unwrap();
            let cached = CachedRangeRequest {
                object: block.object.clone(),
                version: block.version.clone(),
                offset: 0,
                len: 8,
            };
            assert_eq!(
                named.serve_cached(&cached).await.unwrap(),
                Bytes::from_static(b"abcdefgh")
            );
            let requests = origin.requests.load(Ordering::SeqCst);
            named.load_block(&block, 8).await.unwrap();
            assert_eq!(origin.requests.load(Ordering::SeqCst), requests);
            let bad = BlockId::new(
                ObjectId::new(Backend::S3, "bucket", "truncated"),
                0,
                8,
                Version::new("v1"),
            );
            assert!(named.load_block(&bad, 8).await.is_err());
            assert!(named
                .serve_cached(&CachedRangeRequest {
                    object: bad.object,
                    version: bad.version,
                    offset: 0,
                    len: 8
                })
                .await
                .is_err());
            let requests = origin.requests.load(Ordering::SeqCst);
            drop(named);
            drop(runtime);
            let recovered = open();
            let named = recovered
                .for_namespace(&NamespaceId::named("experiment"))
                .await
                .unwrap();
            assert_eq!(
                named.serve_cached(&cached).await.unwrap(),
                Bytes::from_static(b"abcdefgh")
            );
            assert_eq!(origin.requests.load(Ordering::SeqCst), requests);
        }
    });
}

/// The worker's Tokio runtime is entered but NEVER driven. Only the existing
/// data-plane rings can complete LOAD; no fallback origin runtime exists.
#[test]
#[ignore = "requires native io_uring; proves complete wire LOAD stays on its ring"]
fn wire_load_runs_on_existing_rings_without_tokio_scheduler() {
    use std::{
        io::{Read, Write},
        time::Duration,
    };
    use talon_core::{NodeId, NodeInfo, NodeRole};
    use talon_transport::{codec, ControlMessage, FrameHeader, HEADER_LEN};
    use talon_worker::{BlockIndex, InFlightLoads, WorkerObservability};

    struct CheckedHttp(Arc<MonoioClient>);
    #[async_trait::async_trait]
    impl HttpClient for CheckedHttp {
        async fn execute(
            &self,
            request: HttpRequest,
        ) -> Result<talon_backend::HttpResponse, String> {
            let thread = std::thread::current();
            assert!(thread.name().unwrap().starts_with("talon-ring-"));
            assert!(talon_uring::on_ring());
            assert!(request
                .headers
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case("authorization")));
            let response = self.0.execute(request).await;
            assert_eq!(thread.id(), std::thread::current().id());
            response
        }
    }
    struct StopWorker {
        stop: Arc<std::sync::atomic::AtomicBool>,
        obs: Arc<WorkerObservability>,
    }
    impl Drop for StopWorker {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            self.obs.begin_shutdown();
        }
    }
    // Keep the independent origin server running, but do not drive the Worker
    // Tokio runtime below. No DNS is required for this IP-literal TLS endpoint.
    let origin_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dormant_tokio = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let _entered = dormant_tokio.enter();
    // Saturate the only blocking worker too: an undriven Tokio scheduler alone
    // does not prevent spawn_blocking from making progress.
    let (release_pool, wait_pool) = std::sync::mpsc::channel::<()>();
    let (pool_ready, wait_pool_ready) = std::sync::mpsc::channel();
    let pool_task = dormant_tokio.spawn_blocking(move || {
        pool_ready.send(()).unwrap();
        let _ = wait_pool.recv();
    });
    wait_pool_ready
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    for (page_size, l1_capacity) in [(0, 0), (4, 0), (0, 16), (4, 16)] {
        let origin = origin_rt.block_on(Origin::start(true));
        let client = Arc::new(CheckedHttp(Arc::new(MonoioClient::with_tls_config(
            origin.tls.clone(),
        ))));
        let http = Arc::new(talon_backend::RetryingHttpClient::new(
            client,
            talon_backend::retry::RetryConfig {
                max_retries: 1,
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
                timeout_floor: Duration::from_secs(2),
                min_throughput_bytes_per_sec: 0,
            },
            7,
        ));
        let backend = Arc::new(S3Backend::new(
            S3Config {
                region: "test".into(),
                endpoint: origin.address.to_string(),
                path_style: true,
                tls: true,
            },
            S3Credentials {
                access_key_id: "test".into(),
                secret_access_key: "test".into(),
                session_token: None,
            },
            http,
        ));
        let root = tempfile::tempdir().unwrap();
        let config = WorkerConfig {
            block_size: 8,
            capacity_bytes: 1024,
            l1_capacity_bytes: l1_capacity,
            l1_page_size_bytes: 4,
            l2_page_size_bytes: page_size,
            backend: Some("s3".into()),
            page_tti_hours: 0,
            ..Default::default()
        };
        let worker = Arc::new(
            WorkerRuntime::open_namespaces_with_uring(
                &config,
                Arc::new(CacheRootLock::acquire(root.path()).unwrap()),
                backend,
                WorkerMetrics::new(1024),
                true,
            )
            .unwrap(),
        );
        let obs = Arc::new(
            WorkerObservability::new(
                "test".into(),
                NodeInfo {
                    id: NodeId::new("ring-worker"),
                    address: "127.0.0.1:1".into(),
                    role: NodeRole::Worker,
                },
                "127.0.0.1:2".into(),
                1024,
                Arc::new(BlockIndex::new()),
                Arc::new(InFlightLoads::new()),
            )
            .unwrap(),
        );
        obs.attach_namespace_runtime(&worker);
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_store_ready(true);
        obs.readiness().set_control_registered(true);
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = StopWorker {
            stop: stop.clone(),
            obs: obs.clone(),
        };
        let handler = talon_worker::uring_conn::RingConnHandler::new(worker.clone(), obs.clone());
        let admission = talon_worker::ConnectionAdmission::new(8, obs.metrics().clone());
        let handle = dormant_tokio.handle().clone();
        let (finished, wait_finished) = std::sync::mpsc::channel();
        let serving = std::thread::spawn(move || {
            let result = talon_worker::uring_serve::serve_with_shutdown(
                address.to_string(),
                2,
                admission,
                handler,
                handle,
                stop,
            );
            let _ = finished.send(result);
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut socket = loop {
            if let Ok(socket) = std::net::TcpStream::connect(address) {
                break socket;
            }
            assert!(std::time::Instant::now() < deadline, "rings never bound");
            std::thread::sleep(Duration::from_millis(10));
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let block = BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "retry"),
            0,
            8,
            Version::new("v1"),
        );
        for batch in [false, true] {
            let request = if batch {
                ControlMessage::BatchLoad {
                    blocks: vec![talon_transport::LoadBlockRequest {
                        block: BlockId::new(
                            ObjectId::new(Backend::S3, "bucket", "batch"),
                            0,
                            8,
                            Version::new("v1"),
                        ),
                        len: 8,
                    }],
                }
            } else {
                ControlMessage::LoadBlock {
                    block: block.clone(),
                    len: 8,
                }
            };
            socket
                .write_all(&codec::encode(7, &request).unwrap())
                .unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).unwrap();
            let parsed = FrameHeader::decode(&header).unwrap();
            let mut response = header.to_vec();
            response.resize(HEADER_LEN + parsed.length as usize, 0);
            socket.read_exact(&mut response[HEADER_LEN..]).unwrap();
            match codec::decode(&response).unwrap().1 {
                ControlMessage::Ack { ok, detail } if !batch => assert!(ok, "{detail:?}"),
                ControlMessage::BatchLoadResult { failures } if batch => {
                    assert!(failures.is_empty(), "{failures:?}")
                }
                other => panic!("unexpected LOAD result: {other:?}"),
            }
        }
        assert_eq!(worker.resident_bytes(), 16);
        assert_eq!(
            origin.requests.load(Ordering::SeqCst),
            3,
            "one 503 retry plus two successful ranges"
        );
        // Cold/warm sendfile descriptors, cache-only byte reads, L1 promotion,
        // origin miss, and a paged partial hit all run with the pools unavailable.
        use talon_transport::data::{self, RangeRequest, VersionedRangeRequest};
        let mut read = |object: &str, len: u64, cached: bool, versioned: bool| {
            let object = ObjectId::new(Backend::S3, "bucket", object);
            let range = RangeRequest {
                object: object.clone(),
                offset: 0,
                len,
            };
            let frame = if cached {
                data::encode_cached_request(
                    9,
                    &CachedRangeRequest {
                        object,
                        version: Version::new("v1"),
                        offset: 0,
                        len,
                    },
                )
                .unwrap()
            } else if versioned {
                data::encode_versioned_request(
                    9,
                    &VersionedRangeRequest {
                        request: range,
                        version: Version::new("v1"),
                        object_len: 8,
                    },
                )
                .unwrap()
            } else {
                data::encode_request(9, &range).unwrap()
            };
            socket.write_all(&frame).unwrap();
            let mut header = [0; HEADER_LEN];
            socket.read_exact(&mut header).unwrap();
            let parsed = FrameHeader::decode(&header).unwrap();
            assert_eq!(parsed.msg_type, talon_transport::MsgType::GetRange);
            let mut bytes = vec![0; parsed.length as usize];
            socket.read_exact(&mut bytes).unwrap();
            assert_eq!(bytes, b"abcdefgh"[..len as usize]);
        };
        read("retry", 8, false, true);
        read("retry", 8, false, true);
        read("retry", 8, true, true);
        read("read-miss", 4, false, true);
        read("read-miss", 8, false, true);
        read("read-miss", 8, true, true);
        read("regular-read", 8, false, false);
        drop(socket);
        drop(guard);
        wait_finished
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        serving.join().unwrap();
    }
    drop(release_pool);
    dormant_tokio.block_on(pool_task).unwrap();
}

#[test]
#[ignore = "requires native io_uring; regression for the removed executor limit"]
fn origin_requests_are_not_capped_at_128() {
    on_native_ring(async {
        let origin = Origin::start(true).await;
        let client = origin.client();
        let request = HttpRequest::new(
            Method::Get,
            format!("https://127.0.0.1:{}/concurrent", origin.address.port()),
            vec![],
        );
        let requests = (0..129).map(|_| client.execute(request.clone()));
        let responses = monoio::time::timeout(
            std::time::Duration::from_secs(10),
            futures::future::join_all(requests),
        )
        .await
        .unwrap();
        for response in responses {
            assert_eq!(response.unwrap().body, "abcdefgh");
        }
        assert_eq!(origin.requests.load(Ordering::SeqCst), 129);
    });
}

#[tokio::test]
async fn native_origin_rejects_non_ring_callers() {
    let error = MonoioClient::new()
        .execute(HttpRequest::new(
            Method::Get,
            "https://127.0.0.1:1/".into(),
            vec![],
        ))
        .await
        .unwrap_err();
    assert!(
        error.contains("requires an existing Monoio ring"),
        "{error}"
    );
}

#[test]
#[ignore = "requires native io_uring and O_DIRECT; checks fixed/chunked plaintext/TLS allocations"]
fn aligned_http_bodies_and_paged_commits_preserve_payload() {
    on_native_ring(async {
        for tls in [false, true] {
            let origin = Origin::start(tls).await;
            let client = origin.client();
            let scheme = if tls { "https" } else { "http" };
            for chunked in [false, true] {
                let kind = if chunked { "chunked" } else { "fixed" };
                for len in [4095, 4096, 8192, 12289] {
                    let response = client
                        .execute(HttpRequest::new(
                            Method::Get,
                            format!("{scheme}://{}/{kind}/large-{len}", origin.address),
                            vec![("Range".into(), format!("bytes=0-{}", len - 1))],
                        ))
                        .await
                        .unwrap();
                    assert_eq!(response.body.as_ptr() as usize % 4096, 0);
                    let expected: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
                    assert_eq!(response.body, expected);
                }
                for page_size in [0, 4096] {
                    let root = tempfile::tempdir().unwrap();
                    let config = WorkerConfig {
                        block_size: 16384,
                        capacity_bytes: 131072,
                        l1_capacity_bytes: 0,
                        l2_page_size_bytes: page_size,
                        backend: Some("s3".into()),
                        page_tti_hours: 0,
                        ..Default::default()
                    };
                    let backend = origin.backend(tls);
                    let open = || {
                        WorkerRuntime::open_namespaces_with_uring(
                            &config,
                            Arc::new(CacheRootLock::acquire(root.path()).unwrap()),
                            backend.clone(),
                            WorkerMetrics::new(131072),
                            true,
                        )
                        .unwrap()
                    };
                    let runtime = open();
                    for len in [8192, 12289] {
                        let block = BlockId::new(
                            ObjectId::new(Backend::S3, kind, format!("large-{len}")),
                            0,
                            16384,
                            Version::new("v1"),
                        );
                        runtime.load_block(&block, len).await.unwrap();
                        let request = CachedRangeRequest {
                            object: block.object,
                            version: block.version,
                            offset: 0,
                            len,
                        };
                        let expected: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
                        assert_eq!(runtime.serve_cached(&request).await.unwrap(), expected);
                    }
                    drop(runtime);
                    let recovered = open();
                    let count = origin.requests.load(Ordering::SeqCst);
                    let request = CachedRangeRequest {
                        object: ObjectId::new(Backend::S3, kind, "large-12289"),
                        version: Version::new("v1"),
                        offset: 0,
                        len: 12289,
                    };
                    assert_eq!(recovered.serve_cached(&request).await.unwrap().len(), 12289);
                    assert_eq!(origin.requests.load(Ordering::SeqCst), count);
                }
            }
        }
    });
}
