//! Real worker processes/HTTP with local EC2 IMDSv2 and Azure IMDS fixtures.
use std::net::SocketAddr;
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
use talon_core::{Backend, ObjectId};
use talon_transport::{
    codec::{self, ControlMessage},
    data, DataErrorCode, Flags, FrameHeader, HEADER_LEN,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Clone, Default)]
struct Fixture {
    paths: Arc<Mutex<Vec<String>>>,
    redirect_metadata: bool,
    aksk: Option<Backend>,
    legacy_token: Option<Backend>,
}

#[derive(Clone, Copy, PartialEq)]
enum IdentityMode {
    Explicit,
    Iam,
    Workload,
    AkSk,
    AutoAkSk,
    LegacyToken(Option<&'static str>),
}

async fn reply(State(fixture): State<Fixture>, request: Request) -> Response {
    let path = request.uri().path();
    fixture.paths.lock().unwrap().push(path.into());
    if path == "/latest/api/token" && request.method() == Method::PUT {
        return (
            [("x-aws-ec2-metadata-token-ttl-seconds", "21600")],
            "fixture-metadata-token",
        )
            .into_response();
    }
    if path.starts_with("/latest/meta-data/") {
        if request
            .headers()
            .get("x-aws-ec2-metadata-token")
            .and_then(|v| v.to_str().ok())
            != Some("fixture-metadata-token")
        {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if path.ends_with("/security-credentials/") {
            if fixture.redirect_metadata {
                return (StatusCode::FOUND, [("location", "/redirect-target")]).into_response();
            }
            return "fixture-role".into_response();
        }
        if path.ends_with("/security-credentials/fixture-role") {
            return r#"{"Code":"Success","AccessKeyId":"INSTANCEAK","SecretAccessKey":"fixture-secret","Token":"fixture-session","Expiration":"2099-01-01T00:00:00Z"}"#.into_response();
        }
    }
    if path == "/metadata/identity/oauth2/token" {
        if request
            .headers()
            .get("metadata")
            .and_then(|v| v.to_str().ok())
            != Some("true")
        {
            return StatusCode::BAD_REQUEST.into_response();
        }
        if fixture.redirect_metadata {
            return (StatusCode::FOUND, [("location", "/redirect-target")]).into_response();
        }
        let query = request.uri().query().unwrap_or("");
        for required in [
            "api-version=2018-02-01",
            "resource=https%3A%2F%2Fstorage.azure.com%2F",
            "client_id=11111111-1111-1111-1111-111111111111",
        ] {
            if !query.split('&').any(|part| part == required) {
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
        return r#"{"access_token":"managed-token","expires_on":"4102444800","expires_in":"3600","token_type":"Bearer","resource":"https://storage.azure.com/"}"#.into_response();
    }
    if path == "/fixture-tenant/oauth2/v2.0/token" && request.method() == Method::POST {
        return r#"{"access_token":"managed-token","expires_in":3600,"token_type":"Bearer"}"#
            .into_response();
    }
    let azure = path == "/fixtureaccount/example-bucket/key";
    if path == "/example-bucket/key" || azure {
        let authorization = request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let token = request
            .headers()
            .get("x-amz-security-token")
            .and_then(|v| v.to_str().ok());
        if let Some(backend) = fixture.legacy_token {
            let valid = match backend {
                Backend::Azure => {
                    authorization.is_empty()
                        && request.uri().query() == Some("sv=fixture&sig=fixed-sas")
                }
                Backend::Gcs => authorization == "Bearer fixed-token",
                Backend::S3 => unreachable!(),
            };
            if !valid {
                return StatusCode::FORBIDDEN.into_response();
            }
        } else if let Some(backend) = fixture.aksk {
            let valid = match backend {
                Backend::S3 => {
                    authorization.starts_with("AWS4-HMAC-SHA256 Credential=STATICAK/")
                        && authorization.contains("/us-west-2/s3/aws4_request")
                        && token.is_none()
                }
                Backend::Azure => {
                    authorization.starts_with("SharedKey fixtureaccount:")
                        && request.headers().contains_key("x-ms-date")
                        && request.uri().query().is_none()
                }
                Backend::Gcs => {
                    authorization.starts_with("GOOG4-HMAC-SHA256 Credential=HMACAK/")
                        && authorization.contains("/auto/storage/goog4_request")
                        && request.headers().contains_key("x-goog-date")
                        && request.uri().query().is_none()
                }
            };
            if !valid {
                return StatusCode::FORBIDDEN.into_response();
            }
        } else if azure {
            if authorization != "Bearer managed-token" || request.uri().query().is_some() {
                return StatusCode::FORBIDDEN.into_response();
            }
        } else if !authorization.contains("Credential=INSTANCEAK/")
            || !authorization.contains("/us-west-2/s3/aws4_request")
            || token != Some("fixture-session")
        {
            return StatusCode::FORBIDDEN.into_response();
        }
        if request.method() == Method::HEAD {
            return ([("content-length", "9"), ("etag", "\"v1\"")], Body::empty()).into_response();
        }
        if request.method() == Method::GET
            && request
                .headers()
                .get("if-match")
                .is_some_and(|v| v == "\"v1\"")
        {
            return (
                StatusCode::PARTIAL_CONTENT,
                [("content-range", "bytes 0-8/9"), ("etag", "\"v1\"")],
                "role-data",
            )
                .into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

fn start_worker(
    origin: SocketAddr,
    coordinator: SocketAddr,
    root: &tempfile::TempDir,
    backend: Backend,
    mode: IdentityMode,
) -> (tokio::process::Child, SocketAddr) {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = reservation.local_addr().unwrap();
    let config = root.path().join("worker.toml");
    let (storage, source, metadata_env) = match backend {
        Backend::S3 => (format!("backend = \"s3\"\ns3_region = \"us-west-2\"\ns3_endpoint = \"http://{origin}\"\ns3_path_style = true"), "aws-instance-profile", "AWS_EC2_METADATA_SERVICE_ENDPOINT"),
        Backend::Azure => (format!("backend = \"azure\"\nazure_account = \"fixtureaccount\"\nazure_endpoint = \"http://{origin}\""), "azure-managed-identity", "TALON_AZURE_IMDS_ENDPOINT"),
        Backend::Gcs => (format!("backend = \"gcs\"\ngcs_endpoint = \"http://{origin}\""), "gcp-metadata", "GCE_METADATA_HOST"),
    };
    std::fs::write(
        &config,
        format!(
            r#"
{storage}
listen = "{listen}"
admin_listen = "127.0.0.1:0"
coordinator = "{coordinator}"
heartbeat_interval_ms = 100
ownership_gc_enabled = false
cache_dirs = [{}]
"#,
            serde_json::to_string(&root.path().join("cache")).unwrap()
        ),
    )
    .unwrap();
    drop(reservation);
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_talon-worker"));
    command
        .arg("--config")
        .arg(config)
        .env_clear()
        .env("RUST_LOG", "off")
        .env("TALON_ZONE", "test-zone")
        .env("TALON_WORKER_FORCE_TOKIO_DATA_PLANE", "1")
        .env("TALON_ORIGIN_CREDENTIALS_SOURCE", source)
        .env(metadata_env, format!("http://localhost:{}", origin.port()))
        // Explicit IAM must not try validating or using this partial static key.
        .env("TALON_WORKER_S3_ACCESS_KEY_ID", "incomplete-static")
        .env("AWS_ROLE_ARN", "unused-web-identity-role")
        .env("AWS_WEB_IDENTITY_TOKEN_FILE", "/nonexistent-fixture-token")
        .env("TALON_WORKER_AZURE_SAS", "sv=unused&sig=unused")
        .env("TALON_WORKER_AZURE_SHARED_KEY", "not-a-valid-shared-key")
        .env("TALON_WORKER_GCS_BEARER_TOKEN", "unused-fixed-token")
        .env("TALON_WORKER_GCS_ACCESS_KEY_ID", "incomplete-hmac")
        .env("AZURE_CLIENT_ID", "11111111-1111-1111-1111-111111111111")
        .env("AZURE_TENANT_ID", "unused-tenant")
        .env("AZURE_FEDERATED_TOKEN_FILE", "/nonexistent-fixture-token")
        // Only the origin endpoint bypasses this broken proxy. IMDS must bypass
        // proxies itself even though its hostname is not in NO_PROXY.
        .env("HTTP_PROXY", "http://127.0.0.1:1")
        .env("HTTPS_PROXY", "http://127.0.0.1:1")
        .env("ALL_PROXY", "http://127.0.0.1:1")
        .env("NO_PROXY", "127.0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if matches!(mode, IdentityMode::Iam | IdentityMode::Workload) {
        command
            .env("TALON_ORIGIN_CREDENTIALS_SOURCE", "iam")
            .env("AWS_REGION", "us-west-2")
            .env("AWS_CONFIG_FILE", root.path().join("missing-config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                root.path().join("missing-credentials"),
            )
            .env_remove("AWS_ROLE_ARN")
            .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
            .env_remove("AZURE_FEDERATED_TOKEN_FILE");
    }
    if mode == IdentityMode::Workload {
        let token = root.path().join("federated-token");
        std::fs::write(&token, "pod-assertion").unwrap();
        command
            .env("AZURE_FEDERATED_TOKEN_FILE", token)
            .env("AZURE_TENANT_ID", "fixture-tenant")
            .env("AZURE_AUTHORITY_HOST", format!("http://{origin}"));
    }
    if matches!(mode, IdentityMode::AkSk | IdentityMode::AutoAkSk) {
        command
            .env("TALON_ORIGIN_CREDENTIALS_SOURCE", "aksk")
            .env_remove("TALON_WORKER_AZURE_SAS")
            .env_remove("TALON_WORKER_GCS_BEARER_TOKEN");
        if mode == IdentityMode::AutoAkSk {
            command.env_remove("TALON_ORIGIN_CREDENTIALS_SOURCE");
        }
        match backend {
            Backend::S3 => {
                command
                    .env("TALON_WORKER_S3_ACCESS_KEY_ID", "STATICAK")
                    .env("TALON_WORKER_S3_SECRET_ACCESS_KEY", "fixture-static-secret");
            }
            Backend::Azure => {
                command.env("TALON_WORKER_AZURE_SHARED_KEY", "Zml4dHVyZS1zaGFyZWQta2V5");
            }
            Backend::Gcs => {
                command
                    .env("TALON_WORKER_GCS_ACCESS_KEY_ID", "HMACAK")
                    .env("TALON_WORKER_GCS_SECRET_ACCESS_KEY", "fixture-hmac-secret");
            }
        }
    }
    if let IdentityMode::LegacyToken(selector) = mode {
        command
            .env_remove("TALON_ORIGIN_CREDENTIALS_SOURCE")
            .env_remove("TALON_WORKER_AZURE_SHARED_KEY")
            .env_remove("TALON_WORKER_GCS_ACCESS_KEY_ID")
            .env_remove("TALON_WORKER_GCS_SECRET_ACCESS_KEY")
            .env("TALON_WORKER_AZURE_SAS", "sv=fixture&sig=fixed-sas")
            .env("TALON_WORKER_GCS_BEARER_TOKEN", "fixed-token");
        if let Some(selector) = selector {
            command.env("TALON_ORIGIN_CREDENTIALS_SOURCE", selector);
        }
    }
    let child = command.spawn().unwrap();
    (child, listen)
}

#[tokio::test]
async fn instance_profile_worker_config_reads_one_bucket_and_hits_cache() {
    worker_reads_one_bucket(Backend::S3, IdentityMode::Explicit).await;
}

#[tokio::test]
async fn azure_managed_identity_worker_config_reads_one_bucket_and_hits_cache() {
    worker_reads_one_bucket(Backend::Azure, IdentityMode::Explicit).await;
}

#[tokio::test]
async fn iam_worker_discovers_ec2_credentials() {
    worker_reads_one_bucket(Backend::S3, IdentityMode::Iam).await;
}

#[tokio::test]
async fn iam_worker_discovers_azure_managed_identity() {
    worker_reads_one_bucket(Backend::Azure, IdentityMode::Iam).await;
}

#[tokio::test]
async fn iam_worker_prefers_azure_workload_identity() {
    worker_reads_one_bucket(Backend::Azure, IdentityMode::Workload).await;
}

#[tokio::test]
async fn aksk_worker_signs_s3_requests() {
    worker_reads_one_bucket(Backend::S3, IdentityMode::AkSk).await;
}

#[tokio::test]
async fn aksk_worker_signs_azure_shared_key_requests() {
    worker_reads_one_bucket(Backend::Azure, IdentityMode::AkSk).await;
}

#[tokio::test]
async fn aksk_worker_signs_gcs_hmac_requests() {
    worker_reads_one_bucket(Backend::Gcs, IdentityMode::AkSk).await;
}

#[tokio::test]
async fn worker_infers_aksk_from_configured_keys() {
    for backend in [Backend::S3, Backend::Azure, Backend::Gcs] {
        worker_reads_one_bucket(backend, IdentityMode::AutoAkSk).await;
    }
}

#[tokio::test]
async fn explicit_aksk_rejects_fixed_bearer_and_sas() {
    for (backend, legacy_env) in [
        ("gcs", "TALON_WORKER_GCS_BEARER_TOKEN"),
        ("azure", "TALON_WORKER_AZURE_SAS"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("worker.toml");
        std::fs::write(&config, format!(
            "backend = {backend:?}\nazure_account = \"fixtureaccount\"\nlisten = \"127.0.0.1:0\"\nadmin_listen = \"127.0.0.1:0\"\ncoordinator = \"127.0.0.1:1\"\nownership_gc_enabled = false\ncache_dirs = [{}]\n",
            serde_json::to_string(&root.path().join("cache")).unwrap()
        )).unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_talon-worker"));
        command
            .arg("--config")
            .arg(config)
            .env_clear()
            .env("RUST_LOG", "off")
            .env("TALON_ZONE", "test-zone")
            .env("TALON_WORKER_FORCE_TOKIO_DATA_PLANE", "1")
            .env(legacy_env, "fixed-secret-must-not-appear")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command.env("TALON_ORIGIN_CREDENTIALS_SOURCE", "aksk");
        let output = tokio::time::timeout(
            Duration::from_secs(3),
            command.spawn().unwrap().wait_with_output(),
        )
        .await
        .expect("worker accepted legacy token instead of rejecting its mode")
        .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains(legacy_env), "wrong startup failure: {error}");
        assert!(
            error.contains("iam") && error.contains("broker"),
            "missing migration guidance: {error}"
        );
        assert!(!error.contains("fixed-secret-must-not-appear"));
    }
}

#[tokio::test]
async fn legacy_fixed_credentials_still_read_and_hit_cache() {
    for backend in [Backend::Azure, Backend::Gcs] {
        for selector in [None, Some("auto"), Some("static")] {
            worker_reads_one_bucket(backend, IdentityMode::LegacyToken(selector)).await;
        }
    }
}

async fn worker_reads_one_bucket(backend: Backend, mode: IdentityMode) {
    let fixture = Fixture {
        aksk: matches!(mode, IdentityMode::AkSk | IdentityMode::AutoAkSk).then_some(backend),
        legacy_token: matches!(mode, IdentityMode::LegacyToken(_)).then_some(backend),
        ..Fixture::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = listener.local_addr().unwrap();
    let app = Router::new().fallback(reply).with_state(fixture.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let root = tempfile::tempdir().unwrap();
    // Admission is an independent control-plane requirement. Acknowledge the
    // real worker heartbeat; an absent coordinator must not be bypassed in production.
    let coordinator = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator_addr = coordinator.local_addr().unwrap();
    let control = tokio::spawn(async move {
        loop {
            let (mut stream, _) = coordinator.accept().await.unwrap();
            let mut header = [0; HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let header_value = FrameHeader::decode(&header).unwrap();
            let mut frame = header.to_vec();
            frame.resize(HEADER_LEN + header_value.length as usize, 0);
            stream.read_exact(&mut frame[HEADER_LEN..]).await.unwrap();
            assert!(matches!(
                codec::decode(&frame).unwrap().1,
                ControlMessage::NodeStatusHeartbeat { .. }
            ));
            stream
                .write_all(
                    &codec::encode(
                        header_value.request_id,
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
    let (mut child, listen) = start_worker(origin, coordinator_addr, &root, backend, mode);
    tokio::time::timeout(Duration::from_secs(15), async {
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
                panic!(
                    "worker exited before serving: {error}; requests: {:?}",
                    fixture.paths.lock().unwrap()
                );
            }
            if let Ok(stream) = TcpStream::connect(listen).await {
                break stream;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let request = data::encode_request(
            1,
            &data::RangeRequest {
                object: ObjectId::new(backend, "example-bucket", "key"),
                offset: 0,
                len: 9,
            },
        )
        .unwrap();
        let mut successes = 0;
        while successes < 2 {
            stream.write_all(&request).await.unwrap();
            let mut header = [0; HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0; header.length as usize];
            stream.read_exact(&mut body).await.unwrap();
            if header.flags.contains(Flags::ERROR) && successes == 0 {
                let error = data::decode_error_payload(&body);
                if error.code == DataErrorCode::Unavailable {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
            }
            assert!(
                !header.flags.contains(Flags::ERROR),
                "worker returned {:?}",
                data::decode_error_payload(&body)
            );
            assert_eq!(body, b"role-data");
            successes += 1;
        }
    })
    .await
    .expect("worker startup/read deadline exceeded");
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    server.abort();
    control.abort();
    let paths = fixture.paths.lock().unwrap();
    if matches!(
        mode,
        IdentityMode::AkSk | IdentityMode::AutoAkSk | IdentityMode::LegacyToken(_)
    ) {
        assert_eq!(
            paths.len(),
            2,
            "direct credentials must not exchange IAM credentials or refetch cached data"
        );
        return;
    }
    let (prefix, exchanges, object_path) = match backend {
        Backend::S3 => ("/latest/", 3, "/example-bucket/key"),
        Backend::Azure => (
            if mode == IdentityMode::Workload {
                "/fixture-tenant/"
            } else {
                "/metadata/"
            },
            1,
            "/fixtureaccount/example-bucket/key",
        ),
        _ => unreachable!(),
    };
    assert_eq!(
        paths.iter().filter(|p| p.starts_with(prefix)).count(),
        exchanges
    );
    assert_eq!(
        paths.iter().filter(|p| *p == object_path).count(),
        2,
        "cached read performed origin I/O"
    );
}

#[tokio::test]
async fn instance_profile_worker_does_not_forward_metadata_token_on_redirect() {
    worker_rejects_metadata_redirect(Backend::S3).await;
}

#[tokio::test]
async fn azure_managed_identity_worker_does_not_follow_metadata_redirect() {
    worker_rejects_metadata_redirect(Backend::Azure).await;
}

async fn worker_rejects_metadata_redirect(backend: Backend) {
    let fixture = Fixture {
        redirect_metadata: true,
        ..Fixture::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = listener.local_addr().unwrap();
    let app = Router::new().fallback(reply).with_state(fixture.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let root = tempfile::tempdir().unwrap();
    let (child, _) = start_worker(
        origin,
        "127.0.0.1:1".parse().unwrap(),
        &root,
        backend,
        IdentityMode::Explicit,
    );
    let output = tokio::time::timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    server.abort();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("302"), "wrong startup failure: {error}");
    assert!(!error.contains("fixture-metadata-token"));
    let paths = fixture.paths.lock().unwrap();
    assert_eq!(paths.len(), if backend == Backend::S3 { 2 } else { 1 });
    assert!(!paths.iter().any(|p| p == "/redirect-target"));
}
