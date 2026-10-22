//! Worker processes using independent AK/SK, IAM and Broker bucket routes.
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use talon_core::{Backend, ObjectId, Version};
use talon_transport::{codec, data, ControlMessage, DataErrorCode, Flags, FrameHeader, HEADER_LEN};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Clone)]
struct Fixture {
    backend: Backend,
    calls: Arc<Mutex<Vec<String>>>,
}

async fn origin(State(state): State<Fixture>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    state.calls.lock().unwrap().push(path.clone());
    if path == "/latest/api/token" {
        return (
            [("x-aws-ec2-metadata-token-ttl-seconds", "21600")],
            "imds-token",
        )
            .into_response();
    }
    if path == "/latest/meta-data/iam/security-credentials/" {
        return "fixture-role".into_response();
    }
    if path == "/latest/meta-data/iam/security-credentials/fixture-role" {
        return axum::Json(serde_json::json!({"Code":"Success","AccessKeyId":"IAM_C","SecretAccessKey":"secret-c","Token":"iam-session","Expiration":"2099-01-01T00:00:00Z"})).into_response();
    }
    if path == "/computeMetadata/v1/instance/service-accounts/default/token" {
        return ([("metadata-flavor", "Google")], axum::Json(serde_json::json!({"access_token":"iam-token","expires_in":3600,"token_type":"Bearer"}))).into_response();
    }
    if path == "/metadata/identity/oauth2/token" {
        return axum::Json(serde_json::json!({"access_token":"iam-token","expires_on":"4102444800","token_type":"Bearer","resource":"https://storage.azure.com/"})).into_response();
    }
    if path == "/broker" && request.method() == Method::POST {
        let body = axum::body::to_bytes(request.into_body(), 4096)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["bucket"], "bucket-b");
        let (csp, ak, sk, token) = match state.backend {
            Backend::S3 => ("aws", "BROKER_B", "secret-b", "broker-session"),
            Backend::Gcs => ("gcp", "", "", "broker-token"),
            Backend::Azure => ("azure", "fixtureaccount", "", "sv=2026&sig=broker-token"),
        };
        assert_eq!(body["csp"], csp);
        return axum::Json(serde_json::json!({"success":true,"credentials":{"tempAk":ak,"tempSk":sk,"sessionToken":token,"expiredAt":"2099-01-01T00:00:00Z"}})).into_response();
    }
    let prefix = if state.backend == Backend::Azure {
        "/fixtureaccount/"
    } else {
        "/"
    };
    let Some(bucket) = path
        .strip_prefix(prefix)
        .and_then(|p| p.strip_suffix("/same-key"))
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let auth = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = request
        .headers()
        .get("x-amz-security-token")
        .and_then(|v| v.to_str().ok());
    let valid = match (state.backend, bucket) {
        (Backend::S3, "bucket-a") => auth.contains("Credential=STATIC_A/") && token.is_none(),
        (Backend::S3, "bucket-b") => {
            auth.contains("Credential=BROKER_B/") && token == Some("broker-session")
        }
        (Backend::S3, "bucket-c") => {
            auth.contains("Credential=IAM_C/") && token == Some("iam-session")
        }
        (Backend::Gcs, "bucket-a") => auth.starts_with("GOOG4-HMAC-SHA256 Credential=HMAC_A/"),
        (Backend::Gcs, "bucket-b") => auth == "Bearer broker-token",
        (Backend::Azure, "bucket-a") => {
            auth.starts_with("SharedKey fixtureaccount:") && request.uri().query().is_none()
        }
        (Backend::Azure, "bucket-b") => {
            auth.is_empty() && request.uri().query() == Some("sv=2026&sig=broker-token")
        }
        (Backend::Gcs | Backend::Azure, "bucket-c") => auth == "Bearer iam-token",
        _ => false,
    };
    if !valid {
        return StatusCode::FORBIDDEN.into_response();
    }
    if request.method() == Method::HEAD {
        return (
            [("content-length", "8"), ("etag", "\"same-version\"")],
            Body::empty(),
        )
            .into_response();
    }
    if request.method() == Method::GET
        && request
            .headers()
            .get("if-match")
            .is_some_and(|v| v == "\"same-version\"")
    {
        return (
            StatusCode::PARTIAL_CONTENT,
            [
                ("content-range", "bytes 0-7/8"),
                ("etag", "\"same-version\""),
            ],
            bucket.to_owned(),
        )
            .into_response();
    }
    StatusCode::BAD_REQUEST.into_response()
}

async fn exchange(stream: &mut TcpStream, request: &[u8]) -> (FrameHeader, Vec<u8>) {
    stream.write_all(request).await.unwrap();
    let mut header = [0; HEADER_LEN];
    stream.read_exact(&mut header).await.unwrap();
    let header = FrameHeader::decode(&header).unwrap();
    let mut body = vec![0; header.length as usize];
    stream.read_exact(&mut body).await.unwrap();
    (header, body)
}

async fn three_bucket_worker(backend: Backend) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Fixture {
        backend,
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new().fallback(origin).with_state(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let coordinator = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord_addr = coordinator.local_addr().unwrap();
    let control = tokio::spawn(async move {
        loop {
            let (mut stream, _) = coordinator.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            stream.read_exact(&mut body).await.unwrap();
            stream
                .write_all(
                    &codec::encode(
                        header.request_id,
                        &ControlMessage::NodeStatusAck {
                            accepted: true,
                            serving: true,
                            detail: None,
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    let root = tempfile::tempdir().unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = reservation.local_addr().unwrap();
    let admin_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let admin = admin_reservation.local_addr().unwrap();
    let (storage, keys, broker_fields) = match backend {
        Backend::S3 => (format!("backend = \"s3\"\ns3_region = \"us-west-2\"\ns3_endpoint = \"http://{address}\"\ns3_path_style = true"), "s3_access_key_id = \"STATIC_A\"\ns3_secret_access_key = \"secret-a\"", "csp = \"aws\"\nregion = \"us-west-2\"\nroleArn = \"fixture-role-b\""),
        Backend::Gcs => (format!("backend = \"gcs\"\ngcs_endpoint = \"http://{address}\""), "gcs_access_key_id = \"HMAC_A\"\ngcs_secret_access_key = \"secret-a\"", "csp = \"gcp\"\ngcpServiceAccountEmail = \"b@example.iam.gserviceaccount.com\""),
        Backend::Azure => (format!("backend = \"azure\"\nazure_account = \"fixtureaccount\"\nazure_endpoint = \"http://{address}\""), "azure_shared_key = \"c2VjcmV0LWE=\"", "csp = \"azure\"\nregion = \"westus3\"\nazureClientId = \"client-b\"\nazureTenantId = \"tenant\"\nazureAccountName = \"fixtureaccount\""),
    };
    let broker_fields = broker_fields
        .lines()
        .map(|line| format!("credential_broker_request.{line}\n"))
        .collect::<String>();
    let config = root.path().join("worker.toml");
    std::fs::write(
        &config,
        format!(
            r#"
{storage}
listen = "{listen}"
admin_listen = "{admin}"
coordinator = "{coord_addr}"
heartbeat_interval_ms = 100
ownership_gc_enabled = false
cache_dirs = [{}]

[[buckets]]
name = "bucket-a"
credentials_source = "aksk"
{keys}

[[buckets]]
name = "bucket-b"
credentials_source = "broker"
credential_broker_url = "http://{address}/broker"
{broker_fields}
credential_broker_request.bucket = "bucket-b"
credential_broker_request.durationSeconds = 3600

[[buckets]]
name = "bucket-c"
credentials_source = "iam"
"#,
            serde_json::to_string(&root.path().join("cache")).unwrap()
        ),
    )
    .unwrap();
    drop(reservation);
    drop(admin_reservation);
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_talon-worker"))
        .arg("--config")
        .arg(&config)
        .env_clear()
        .env("TALON_ZONE", "test-zone")
        .env("RUST_LOG", "off")
        .env("TALON_WORKER_FORCE_TOKIO_DATA_PLANE", "1")
        .env("TALON_WORKER_ORIGIN_IO", "tokio")
        .env("AWS_REGION", "us-west-2")
        .env("AWS_CONFIG_FILE", root.path().join("missing-config"))
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            root.path().join("missing-credentials"),
        )
        .env(
            "AWS_EC2_METADATA_SERVICE_ENDPOINT",
            format!("http://{address}"),
        )
        .env("GCE_METADATA_HOST", address.to_string())
        .env("AZURE_CLIENT_ID", "11111111-1111-1111-1111-111111111111")
        .env("TALON_AZURE_IMDS_ENDPOINT", format!("http://{address}"))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut stream = loop {
            if child.try_wait().unwrap().is_some() {
                let mut error = String::new();
                child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut error)
                    .await
                    .unwrap();
                panic!("worker failed to start: {error}");
            }
            if let Ok(stream) = TcpStream::connect(listen).await {
                break stream;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        for bucket in ["bucket-a", "bucket-b", "bucket-c"] {
            let object = ObjectId::new(backend, bucket, "same-key");
            let request = data::encode_request(
                1,
                &data::RangeRequest {
                    object: object.clone(),
                    offset: 0,
                    len: 8,
                },
            )
            .unwrap();
            loop {
                let (header, body) = exchange(&mut stream, &request).await;
                if header.flags.contains(Flags::ERROR) {
                    let error = data::decode_error_payload(&body);
                    if error.code == DataErrorCode::Unavailable {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                    panic!("read failed: {error:?}");
                }
                assert_eq!(body, bucket.as_bytes());
                break;
            }
            let cached = data::encode_cached_request(
                2,
                &data::CachedRangeRequest {
                    object,
                    offset: 0,
                    len: 8,
                    version: Version::new("same-version"),
                },
            )
            .unwrap();
            let (header, body) = exchange(&mut stream, &cached).await;
            assert!(!header.flags.contains(Flags::ERROR));
            assert_eq!(body, bucket.as_bytes());
        }
        let calls_before = state.calls.lock().unwrap().len();
        for key in [
            "../unconfigured/same-key",
            "%2e%2e/unconfigured/same-key",
            r"..\unconfigured\same-key",
        ] {
            let invalid = data::encode_request(
                3,
                &data::RangeRequest {
                    object: ObjectId::new(backend, "bucket-a", key),
                    offset: 0,
                    len: 8,
                },
            )
            .unwrap();
            assert!(exchange(&mut stream, &invalid)
                .await
                .0
                .flags
                .contains(Flags::ERROR));
        }
        assert_eq!(
            state.calls.lock().unwrap().len(),
            calls_before,
            "unsafe object target reached origin HTTP"
        );
        let unknown = data::encode_cached_request(
            3,
            &data::CachedRangeRequest {
                object: ObjectId::new(backend, "unknown-bucket", "same-key"),
                offset: 0,
                len: 0,
                version: Version::new("same-version"),
            },
        )
        .unwrap();
        assert!(exchange(&mut stream, &unknown)
            .await
            .0
            .flags
            .contains(Flags::ERROR));
        let mut metrics_stream = TcpStream::connect(admin).await.unwrap();
        metrics_stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut metrics = String::new();
        metrics_stream.read_to_string(&mut metrics).await.unwrap();
        let expiry = metrics
            .lines()
            .find(|line| line.starts_with("talon_worker_origin_credentials_expiry_seconds"))
            .expect("multi-bucket expiry metric must be present");
        assert!(expiry.ends_with(" 0"), "unexpected expiry: {expiry}");
        let successes = metrics
            .lines()
            .find(|line| {
                line.starts_with("talon_worker_origin_credentials_total")
                    && line.contains("result=\"refresh_success\"")
            })
            .expect("credential acquisitions must still be counted")
            .split_whitespace()
            .last()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(successes >= 2, "Broker and IAM must both report success");
    })
    .await
    .expect("multi-bucket worker read deadline");
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    server.abort();
    control.abort();
    let calls = state.calls.lock().unwrap();
    for bucket in ["bucket-a", "bucket-b", "bucket-c"] {
        assert_eq!(
            calls
                .iter()
                .filter(|path| path.ends_with(&format!("/{bucket}/same-key")))
                .count(),
            2,
            "one HEAD+GET, then cache-only hit"
        );
    }
    assert_eq!(calls.iter().filter(|p| p.as_str() == "/broker").count(), 1);
    assert!(!calls.iter().any(|p| p.contains("unknown-bucket")));
}

#[tokio::test]
async fn s3_worker_routes_mixed_bucket_credentials() {
    three_bucket_worker(Backend::S3).await;
}
#[tokio::test]
async fn gcs_worker_routes_mixed_bucket_credentials() {
    three_bucket_worker(Backend::Gcs).await;
}
#[tokio::test]
async fn azure_worker_routes_mixed_bucket_credentials() {
    three_bucket_worker(Backend::Azure).await;
}
