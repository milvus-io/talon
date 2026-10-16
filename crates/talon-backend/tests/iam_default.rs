//! Default IAM discovery runs in isolated subprocesses, never against the
//! developer's real credential files or process-wide environment.
use std::sync::{Arc, Mutex};

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use talon_backend::{
    GcsBackend, GcsConfig, NoopObserver, ReqwestClient, S3Backend, S3Config, S3Credentials,
};
use talon_core::{Backend, BackendStore, ObjectId};

#[derive(Clone, Default)]
struct Requests(Arc<Mutex<Vec<String>>>, bool);

async fn reply(State(requests): State<Requests>, request: Request) -> Response {
    let path = request.uri().path();
    requests.0.lock().unwrap().push(path.into());
    match path {
        "/latest/api/token" if request.method() == Method::PUT => (
            [("x-aws-ec2-metadata-token-ttl-seconds", "21600")], "imds-token",
        ).into_response(),
        "/latest/meta-data/iam/security-credentials/" => "fixture-role".into_response(),
        "/latest/meta-data/iam/security-credentials/fixture-role" | "/ecs" =>
            r#"{"Code":"Success","AccessKeyId":"IAMAK","SecretAccessKey":"iam-secret","Token":"iam-session","Expiration":"2099-01-01T00:00:00Z"}"#.into_response(),
        "/sts/" if request.method() == Method::POST =>
            r#"<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>IAMAK</AccessKeyId><SecretAccessKey>iam-secret</SecretAccessKey><SessionToken>iam-session</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><SubjectFromWebIdentityToken>fixture</SubjectFromWebIdentityToken></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"#.into_response(),
        "/oauth/token" if request.method() == Method::POST => {
            let count = requests.0.lock().unwrap().iter().filter(|p| *p == "/oauth/token").count();
            let (token, expires) = if requests.1 && count == 1 { ("iam-token", 2) } else if requests.1 { ("rotated-token", 3600) } else { ("iam-token", 3600) };
            axum::Json(serde_json::json!({"access_token": token, "expires_in": expires, "token_type":"Bearer"})).into_response()
        }
        "/computeMetadata/v1/instance/service-accounts/default/token" => (
            [("metadata-flavor", "Google"), ("content-type", "application/json")],
            r#"{"access_token":"iam-token","expires_in":3600,"token_type":"Bearer"}"#,
        ).into_response(),
        "/bucket/key" if request.method() == Method::HEAD => {
            let authorization = request.headers().get("authorization").and_then(|h| h.to_str().ok()).unwrap_or("");
            if authorization != "Bearer iam-token" && authorization != "Bearer rotated-token" && !authorization.contains("Credential=IAMAK/") {
                return StatusCode::FORBIDDEN.into_response();
            }
            ([("content-length", "9"), ("etag", "\"version\"")], "").into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

#[tokio::test]
#[ignore = "internal subprocess entry point; invoked by the isolated tests below"]
async fn resolve_in_child() {
    let mode = std::env::var("TEST_IAM_MODE").unwrap();
    let endpoint = std::env::var("TEST_IAM_ENDPOINT").unwrap();
    let http = Arc::new(ReqwestClient::new());
    if mode == "gcp-invalid-adc" {
        let error = match talon_backend::resolve_gcs_credentials(
            None,
            Some("iam"),
            http,
            Arc::new(NoopObserver),
        )
        .await
        {
            Ok(_) => panic!("invalid explicit ADC must not fall back to metadata"),
            Err(error) => error,
        };
        assert!(error.contains("ADC"), "{error}");
        assert!(!error.contains("private-fixture-marker"));
    } else if mode.starts_with("gcp") {
        let resolved = talon_backend::resolve_gcs_credentials(
            Some(talon_backend::GcsHmacCredentials {
                access_key_id: "unused-key".into(),
                secret_access_key: "unused-secret".into(),
            }),
            Some("iam"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(resolved.source, "gcp-adc");
        let talon_backend::GcsCredentials::Bearer(provider) = &resolved.provider else {
            panic!("expected ADC bearer")
        };
        let provider = provider.clone();
        let backend = GcsBackend::with_credentials(
            GcsConfig::emulator(endpoint, false),
            resolved.provider,
            http,
        );
        assert_eq!(
            backend
                .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
                .await
                .unwrap()
                .len,
            9
        );
        if mode == "gcp-refresh" {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            assert_eq!(
                backend
                    .head(&ObjectId::new(Backend::Gcs, "bucket", "key"))
                    .await
                    .unwrap()
                    .len,
                9
            );
            assert_eq!(provider.current().unwrap().as_str(), "rotated-token");
        }
    } else {
        let resolved = talon_backend::resolve_s3_credentials(
            Some(S3Credentials {
                access_key_id: "unused-static".into(),
                secret_access_key: "unused".into(),
                session_token: None,
            }),
            Some("iam"),
            http.clone(),
            Arc::new(NoopObserver),
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(resolved.source, "aws-default");
        assert_eq!(resolved.provider.current().access_key_id, "IAMAK");
        let mut config = S3Config::aws("us-west-2");
        config.endpoint = endpoint;
        config.tls = false;
        config.path_style = true;
        let backend = S3Backend::with_credentials_provider(config, resolved.provider, http);
        assert_eq!(
            backend
                .head(&ObjectId::new(Backend::S3, "bucket", "key"))
                .await
                .unwrap()
                .len,
            9
        );
    }
}

#[tokio::test]
async fn iam_default_discovery_uses_environment_profile_container_and_metadata() {
    for mode in [
        "aws-env",
        "aws-profile",
        "aws-web-identity",
        "aws-container",
        "aws-metadata",
        "gcp-adc-file",
        "gcp-adc-local",
        "gcp-metadata",
        "gcp-invalid-adc",
        "gcp-refresh",
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("config"), "").unwrap();
        let credentials = root.path().join("credentials");
        std::fs::write(&credentials, if mode == "aws-profile" {
            "[default]\naws_access_key_id=IAMAK\naws_secret_access_key=iam-secret\naws_session_token=iam-session\n"
        } else { "" }).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Requests(Arc::default(), mode == "gcp-refresh");
        let router = Router::new().fallback(reply).with_state(requests.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "resolve_in_child", "--ignored", "--nocapture"])
            .env_clear()
            .env("HOME", root.path())
            .env("TEST_IAM_MODE", mode)
            .env("TEST_IAM_ENDPOINT", addr.to_string())
            .env("AWS_REGION", "us-west-2")
            .env("AWS_CONFIG_FILE", root.path().join("config"))
            .env("AWS_SHARED_CREDENTIALS_FILE", credentials)
            .env(
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                format!("http://{addr}"),
            )
            .env("GCE_METADATA_HOST", addr.to_string())
            .kill_on_drop(true);
        if mode == "aws-env" {
            child
                .env("AWS_ACCESS_KEY_ID", "IAMAK")
                .env("AWS_SECRET_ACCESS_KEY", "iam-secret")
                .env("AWS_SESSION_TOKEN", "iam-session");
        }
        if matches!(mode, "aws-env" | "aws-profile" | "aws-web-identity") {
            let token = root.path().join("web-identity-token");
            std::fs::write(&token, "pod-assertion").unwrap();
            child
                .env("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
                .env("AWS_WEB_IDENTITY_TOKEN_FILE", token)
                .env("AWS_ENDPOINT_URL_STS", format!("http://{addr}/sts"));
        }
        if mode == "aws-container" {
            child.env(
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                format!("http://{addr}/ecs"),
            );
        }
        if mode == "gcp-invalid-adc" {
            let adc = root.path().join("adc.json");
            std::fs::write(&adc, "private-fixture-marker").unwrap();
            child.env("GOOGLE_APPLICATION_CREDENTIALS", adc);
        }
        if matches!(mode, "gcp-adc-file" | "gcp-adc-local" | "gcp-refresh") {
            let adc = if mode == "gcp-adc-local" {
                let directory = root.path().join(".config/gcloud");
                std::fs::create_dir_all(&directory).unwrap();
                directory.join("application_default_credentials.json")
            } else {
                root.path().join("adc.json")
            };
            std::fs::write(&adc, serde_json::json!({"type":"authorized_user", "client_id":"fixture-client", "client_secret":"fixture-secret", "refresh_token":"fixture-refresh", "token_uri":format!("http://{addr}/oauth/token")}).to_string()).unwrap();
            if mode != "gcp-adc-local" {
                child.env("GOOGLE_APPLICATION_CREDENTIALS", adc);
            }
        }
        let output = tokio::time::timeout(std::time::Duration::from_secs(45), child.output())
            .await
            .unwrap()
            .unwrap();
        server.abort();
        assert!(
            output.status.success(),
            "{mode}, requests {:?}: {}{}",
            requests.0.lock().unwrap(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let paths = requests.0.lock().unwrap();
        match mode {
            "aws-env" | "aws-profile" => assert_eq!(*paths, ["/bucket/key"]),
            "aws-container" => assert_eq!(*paths, ["/ecs", "/bucket/key"]),
            "aws-web-identity" => assert_eq!(*paths, ["/sts/", "/bucket/key"]),
            "aws-metadata" => assert!(paths
                .iter()
                .any(|p| p.ends_with("/security-credentials/fixture-role"))),
            "gcp-metadata" => assert!(paths.iter().any(|p| p.ends_with("/default/token"))),
            "gcp-invalid-adc" => assert!(paths.is_empty()),
            "gcp-adc-file" | "gcp-adc-local" | "gcp-refresh" => {
                assert!(paths.iter().any(|p| p == "/oauth/token"));
                assert!(!paths.iter().any(|p| p.contains("Metadata")));
            }
            _ => unreachable!(),
        }
    }
}
