//! Opt-in read-only worker validation against an existing real cloud object.
//! The broker descriptor is read from the environment and never written to disk.

use std::panic::AssertUnwindSafe;
use std::process::Stdio;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use futures::FutureExt;
use talon_backend::{
    BackendResolver, DefaultBackendResolver, HttpClient, HttpRequest, HttpResponse, Method,
    NoopObserver, ReqwestClient, RouteRequest,
};
use talon_core::{Backend, BrokerConfig, NodeId, NodeInfo, NodeRole, ObjectId};
use talon_rust_client::{ClientBuilder, ObjectStat, RequestOptions};
use talon_transport::{codec, ControlMessage};
use tokio::{
    io::AsyncWriteExt,
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};

#[path = "../../talon-transport/tests/support/membership.rs"]
mod membership;

const BLOCK_SIZE: u32 = 1 << 20;

// Never print a backend error, request, response, object identity, or object bytes.
fn checked<T, E>(result: Result<T, E>, stage: &'static str) -> T {
    result.unwrap_or_else(|_| panic!("{stage} (details suppressed)"))
}

#[derive(Default)]
struct Background(Vec<JoinHandle<()>>);

impl Drop for Background {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

struct BrokerProxy {
    url: String,
    http: ReqwestClient,
    calls: AtomicUsize,
}

struct DiscoveryHttp(ReqwestClient);

#[async_trait::async_trait]
impl HttpClient for DiscoveryHttp {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let method = request.method;
        let response = self
            .0
            .execute(request)
            .await
            .map_err(|_| "live discovery HTTP transport failed")?;
        eprintln!("live discovery {method:?} HTTP {}", response.status);
        Ok(response)
    }
}

async fn forward_broker(State(state): State<Arc<BrokerProxy>>, body: Bytes) -> Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    if body.len() > talon_core::MAX_CREDENTIAL_BROKER_BYTES {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let response = state
        .http
        .execute(HttpRequest::with_body(
            Method::Post,
            state.url.clone(),
            vec![("content-type".into(), "application/json".into())],
            body,
        ))
        .await;
    match response {
        Ok(response) => {
            eprintln!("live worker broker POST HTTP {}", response.status);
            (
                StatusCode::from_u16(response.status).unwrap_or(StatusCode::BAD_GATEWAY),
                [("content-type", "application/json")],
                response.body,
            )
                .into_response()
        }
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

#[tokio::test]
#[ignore = "requires TALON_LIVE_CREDENTIAL_BROKER JSON and readable cloud data; read-only"]
async fn live_dynamic_broker_worker_reads() {
    let descriptor = checked(
        std::env::var("TALON_LIVE_CREDENTIAL_BROKER"),
        "set TALON_LIVE_CREDENTIAL_BROKER",
    );
    let mut broker = checked(
        BrokerConfig::from_json(&descriptor),
        "invalid live descriptor",
    );
    let backend = match broker.request.get("csp").and_then(|value| value.as_str()) {
        Some("aws") => Backend::S3,
        Some("gcp") => Backend::Gcs,
        Some("azure") => Backend::Azure,
        _ => panic!("live descriptor requires a supported csp"),
    };
    let bucket = std::env::var("TALON_LIVE_BUCKET")
        .ok()
        .or_else(|| {
            broker
                .request
                .get("bucket")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .filter(|value| !value.is_empty())
        .expect("descriptor request.bucket or TALON_LIVE_BUCKET is required");
    let mut background = Background::default();
    let root = checked(tempfile::tempdir(), "create live test directory");
    let mut child = None;
    let exercise = async {
        let http: Arc<dyn HttpClient> = Arc::new(DiscoveryHttp(ReqwestClient::new()));
        let resolver = DefaultBackendResolver::new(
            backend,
            http.clone(),
            http.clone(),
            Arc::new(NoopObserver),
        );
        let origin = checked(
            resolver
                .resolve(&RouteRequest {
                    backend,
                    bucket: &bucket,
                    broker: Some(&broker),
                })
                .await,
            "live discovery credential exchange failed",
        )
        .store;
        let key = if let Ok(key) = std::env::var("TALON_LIVE_OBJECT_KEY") {
            assert!(!key.is_empty(), "TALON_LIVE_OBJECT_KEY must be nonempty");
            key
        } else {
            let (mut cursor, mut found) = (None, None);
            for _ in 0..3 {
                let page = checked(
                    origin
                        .list_objects(&bucket, "", cursor.as_deref(), 100)
                        .await,
                    "live discovery LIST failed",
                );
                found = page
                    .objects
                    .into_iter()
                    .find(|object| object.size > 0)
                    .map(|object| object.key);
                if found.is_some() || page.next.is_none() {
                    break;
                }
                cursor = page.next;
            }
            eprintln!("live discovery LIST passed");
            found.expect("no nonempty object in the first 300 listing entries")
        };
        let object = ObjectId::new(backend, &bucket, key);
        let source = checked(origin.head(&object).await, "live discovery HEAD failed");
        assert!(
            source.len > 0 && !source.version.as_str().is_empty(),
            "live object must have bytes and a version"
        );
        let length = source.len.min(1024);
        let expected = checked(
            origin
                .fetch_range_if_match(&object, 0, length, Some(&source.version))
                .await,
            "live discovery conditional GET failed",
        );
        assert!(
            expected.len() as u64 == length,
            "live discovery returned a short range"
        );
        eprintln!("live discovery HEAD and conditional GET passed");

        // Discovery uses the real endpoint directly. Count only the worker's
        // acquisitions through this forwarding proxy, without inspecting tokens.
        let listener = checked(TcpListener::bind("127.0.0.1:0").await, "bind broker proxy");
        let proxy_address = checked(listener.local_addr(), "broker proxy address");
        let proxy = Arc::new(BrokerProxy {
            url: broker.url.clone(),
            http: ReqwestClient::new(),
            calls: AtomicUsize::new(0),
        });
        let app = Router::new()
            .route("/broker", post(forward_broker))
            .with_state(proxy.clone());
        background.0.push(tokio::spawn(async move {
            checked(axum::serve(listener, app).await, "broker proxy stopped");
        }));
        broker.url = format!("http://{proxy_address}/broker");
        let worker_json = checked(
            serde_json::to_string(&broker),
            "encode live worker descriptor",
        );

        let listener = checked(
            TcpListener::bind("127.0.0.1:0").await,
            "bind control fixture",
        );
        let control_address = checked(listener.local_addr(), "control fixture address");
        let reservation = checked(
            std::net::TcpListener::bind("127.0.0.1:0"),
            "reserve worker port",
        );
        let worker_address = checked(reservation.local_addr(), "worker address");
        let admin_reservation = checked(
            std::net::TcpListener::bind("127.0.0.1:0"),
            "reserve admin port",
        );
        let admin_address = checked(admin_reservation.local_addr(), "admin address");
        background.0.push(tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                connections.spawn(async move {
                    while let Ok((header, payload)) =
                        talon_transport::read_frame(&mut stream, Duration::from_secs(5)).await
                    {
                        let mut frame = header.encode().to_vec();
                        frame.extend_from_slice(&payload);
                        let (_, message) = checked(
                            codec::decode_request(&frame),
                            "decode control fixture request",
                        );
                        let reply = match message {
                            ControlMessage::NodeStatusHeartbeat { .. } => {
                                ControlMessage::NodeStatusAck {
                                    accepted: true,
                                    serving: true,
                                    detail: None,
                                }
                            }
                            ControlMessage::MembershipQuery {} => {
                                membership::plain(vec![NodeInfo {
                                    id: NodeId::new("live-dynamic-worker"),
                                    address: worker_address.to_string(),
                                    role: NodeRole::Worker,
                                }])
                            }
                            _ => panic!("unexpected live control fixture request"),
                        };
                        let reply = checked(
                            codec::encode(header.request_id, &reply),
                            "encode control fixture response",
                        );
                        if stream.write_all(&reply).await.is_err() {
                            break;
                        }
                    }
                });
                while connections.try_join_next().is_some() {}
            }
        }));

        // A static allowlist with an unrelated dummy bucket prevents absent
        // properties from triggering implicit IAM or accessing warmed dynamic data.
        let dummy_bucket = if bucket == "talon-live-static-denied" {
            "talon-live-static-other"
        } else {
            "talon-live-static-denied"
        };
        let storage = match backend {
            Backend::S3 => "backend = \"s3\"",
            Backend::Gcs => "backend = \"gcs\"",
            Backend::Azure => "backend = \"azure\"",
        };
        let keys = match backend {
            Backend::S3 => "s3_region = \"us-east-1\"\ns3_access_key_id = \"STATIC_TEST\"\ns3_secret_access_key = \"fixture-only\"",
            Backend::Gcs => "gcs_access_key_id = \"STATIC_TEST\"\ngcs_secret_access_key = \"fixture-only\"",
            Backend::Azure => "azure_account = \"fixtureaccount\"\nazure_shared_key = \"ZmFrZQ==\"",
        };
        let config = root.path().join("worker.toml");
        let cache = checked(
            serde_json::to_string(&root.path().join("cache")),
            "encode cache path",
        );
        checked(std::fs::write(&config, format!(
            "{storage}\nlisten = \"{worker_address}\"\nadmin_listen = \"{admin_address}\"\ncoordinator = \"{control_address}\"\nnode_id = \"live-dynamic-worker\"\nheartbeat_interval_ms = 100\nownership_gc_enabled = false\nstale_gc_enabled = false\nblock_size = {BLOCK_SIZE}\nl2_page_size_bytes = 65536\ncache_dirs = [{cache}]\n[[buckets]]\nname = \"{dummy_bucket}\"\ncredentials_source = \"aksk\"\n{keys}\n"
        )), "write nonsecret worker configuration");
        drop(reservation);
        drop(admin_reservation);
        child = Some(checked(
            tokio::process::Command::new(env!("CARGO_BIN_EXE_talon-worker"))
                .arg("--config")
                .arg(&config)
                .env_clear()
                .env("TALON_ZONE", "live-test-zone")
                .env("RUST_LOG", "off")
                .env("TALON_WORKER_FORCE_TOKIO_DATA_PLANE", "1")
                .env("TALON_WORKER_ORIGIN_IO", "tokio")
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn(),
            "launch live worker",
        ));
        let readiness_http = ReqwestClient::new();
        checked(
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    assert!(
                        checked(child.as_mut().unwrap().try_wait(), "check live worker").is_none(),
                        "live worker exited before readiness"
                    );
                    if let Ok(response) = readiness_http
                        .execute(HttpRequest::new(
                            Method::Get,
                            format!("http://{admin_address}/readyz"),
                            vec![],
                        ))
                        .await
                    {
                        if response.status == 200 {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await,
            "live worker readiness deadline exceeded",
        );

        let client = checked(
            ClientBuilder::default()
                .with_coordinator(control_address.to_string())
                .with_block_size(BLOCK_SIZE)
                .build(),
            "build live SDK client",
        );
        let known = ObjectStat {
            size: source.len,
            version: source.version.as_str().to_owned(),
        };
        let options = RequestOptions::default().with_credential_broker(worker_json.as_str());
        for _ in 0..2 {
            let bytes = checked(
                client
                    .read(&object, 0, Some(length), Some(&known), &options)
                    .await,
                "live dynamic worker read failed",
            );
            assert!(
                bytes.as_slice() == expected.as_ref(),
                "live worker bytes differ from origin"
            );
        }
        assert!(
            proxy.calls.load(Ordering::SeqCst) == 1,
            "repeated dynamic reads must reuse one worker credential exchange"
        );
        assert!(
            client
                .read(&object, 0, Some(length), Some(&known), &Default::default())
                .await
                .is_err(),
            "absent properties accessed warmed dynamic cache"
        );
        assert!(
            proxy.calls.load(Ordering::SeqCst) == 1,
            "absent properties unexpectedly called broker"
        );
        eprintln!(
            "live worker repeated reads, credential reuse, and absent-property isolation passed"
        );

        if backend == Backend::S3
            && std::env::var("TALON_LIVE_REJECT_EXTERNAL_ID").as_deref() == Ok("1")
        {
            broker.request["externalId"] = format!(
                "talon-invalid-{}-{}",
                std::process::id(),
                checked(
                    SystemTime::now().duration_since(UNIX_EPOCH),
                    "read test clock"
                )
                .as_nanos()
            )
            .into();
            let invalid = checked(
                serde_json::to_string(&broker),
                "encode negative live descriptor",
            );
            let options = RequestOptions::default().with_credential_broker(invalid.as_str());
            assert!(
                client
                    .read(&object, 0, Some(length), Some(&known), &options)
                    .await
                    .is_err(),
                "invalid externalId accessed warmed dynamic cache or was accepted by broker"
            );
            assert!(
                proxy.calls.load(Ordering::SeqCst) == 2,
                "negative case must issue exactly one extra worker credential exchange"
            );
            eprintln!("live worker invalid-externalId isolation passed");
        }
    };
    let result = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(300), exercise))
        .catch_unwind()
        .await;
    if let Some(mut child) = child {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    drop(background);
    match result {
        Ok(result) => checked(result, "live dynamic worker test deadline exceeded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
