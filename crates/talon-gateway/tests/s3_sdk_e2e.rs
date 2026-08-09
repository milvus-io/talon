//! S3 gateway conformance against LocalStack and real client libraries.
//!
//! The test is skipped unless `TALON_S3_TEST_ENDPOINT` is set. CI supplies a
//! real LocalStack service; the gateway listens on an ephemeral loopback port.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use talon_backend::{ReqwestClient, S3Backend, S3Config, S3Credentials};
use talon_cache_client::CacheReadError;
use talon_core::{ObjectId, Version};
use talon_gateway::s3::{S3Adapter, S3AdapterConfig, S3Cache, S3CacheRequest};
use talon_gateway::{serve, GatewayConfig, GatewayRuntime, GatewaySecurity};
use tokio::net::TcpListener;

type CacheKey = (ObjectId, Version, u64, u64);
type CacheBody = Pin<Box<dyn Stream<Item = Result<Bytes, CacheReadError>> + Send>>;

struct ReadThroughCache {
    origin: Arc<S3Backend>,
    entries: Arc<Mutex<HashMap<CacheKey, Bytes>>>,
    hits: Arc<AtomicUsize>,
    misses: Arc<AtomicUsize>,
}

impl ReadThroughCache {
    fn new(origin: Arc<S3Backend>) -> Arc<Self> {
        Arc::new(Self {
            origin,
            entries: Arc::new(Mutex::new(HashMap::new())),
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
        })
    }
}

impl S3Cache for ReadThroughCache {
    fn stream(&self, request: S3CacheRequest<'_>) -> Result<CacheBody, CacheReadError> {
        let key = (
            request.object.clone(),
            request.version.clone(),
            request.offset,
            request.len,
        );
        if let Some(body) = self.entries.lock().unwrap().get(&key).cloned() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Box::pin(futures::stream::once(async move { Ok(body) })));
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let origin = Arc::clone(&self.origin);
        let entries = Arc::clone(&self.entries);
        let object = request.object.clone();
        let version = request.version.to_string();
        let offset = request.offset;
        let len = request.len;
        Ok(Box::pin(futures::stream::once(async move {
            let end = offset
                .checked_add(len)
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| CacheReadError::InvalidRequest("invalid cache range".into()))?;
            let conditions = vec![("if-match".to_string(), format!("\"{version}\""))];
            let mut response = origin
                .execute_get_stream_raw(&object, Some((offset, end)), &conditions)
                .await
                .map_err(CacheReadError::Origin)?;
            if response.status == 412 {
                return Err(CacheReadError::VersionMismatch(
                    "origin version changed".into(),
                ));
            }
            if response.status != 206 {
                return Err(CacheReadError::Origin(format!(
                    "origin returned HTTP {}",
                    response.status
                )));
            }
            let mut body = Vec::with_capacity(len as usize);
            while let Some(chunk) = response.body.next().await {
                body.extend_from_slice(&chunk.map_err(CacheReadError::Origin)?);
                if body.len() as u64 > len {
                    return Err(CacheReadError::Protocol(
                        "origin returned more bytes than requested".into(),
                    ));
                }
            }
            if body.len() as u64 != len {
                return Err(CacheReadError::Protocol(format!(
                    "origin returned {} bytes, expected {len}",
                    body.len()
                )));
            }
            let body = Bytes::from(body);
            entries.lock().unwrap().insert(key, body.clone());
            Ok(body)
        })))
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn endpoint_host(endpoint: &str) -> (String, bool) {
    if let Some(host) = endpoint.strip_prefix("http://") {
        (host.to_string(), false)
    } else {
        (
            endpoint
                .strip_prefix("https://")
                .unwrap_or(endpoint)
                .to_string(),
            true,
        )
    }
}

fn expected_object() -> Vec<u8> {
    (0..4096).map(|index| (index % 251) as u8).collect()
}

#[tokio::test]
async fn s3_sdks_and_localstack_gateway_conformance() {
    let Some(origin_endpoint) = env("TALON_S3_TEST_ENDPOINT") else {
        eprintln!("skipping S3 gateway e2e: TALON_S3_TEST_ENDPOINT is not set");
        return;
    };
    let bucket = env("TALON_S3_TEST_BUCKET").expect("TALON_S3_TEST_BUCKET must be set");
    let object_key = env("TALON_S3_TEST_KEY").unwrap_or_else(|| "e2e/object.bin".to_string());
    let region = env("TALON_S3_TEST_REGION").unwrap_or_else(|| "us-east-1".to_string());
    let access_key = env("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret_key = env("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
    let (host, tls) = endpoint_host(&origin_endpoint);
    let origin = Arc::new(S3Backend::new(
        S3Config {
            region: region.clone(),
            endpoint: host,
            path_style: true,
            tls,
        },
        S3Credentials {
            access_key_id: access_key,
            secret_access_key: secret_key,
            session_token: None,
        },
        Arc::new(ReqwestClient::new()),
    ));
    let cache = ReadThroughCache::new(Arc::clone(&origin));
    let adapter = Arc::new(
        S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::clone(&cache) as Arc<dyn S3Cache>,
            origin,
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let runtime = Arc::new(
        GatewayRuntime::new(
            GatewayConfig {
                bind: address,
                ..GatewayConfig::default()
            },
            adapter,
            GatewaySecurity::default(),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(listener, runtime, async move {
        let _ = shutdown_rx.await;
    }));
    let gateway_endpoint = format!("http://{address}");

    if let Some(python) = env("TALON_S3_SDK_PYTHON") {
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/s3_gateway_sdk_conformance.py");
        let status = tokio::process::Command::new(python)
            .arg(script)
            .env("TALON_S3_GATEWAY_ENDPOINT", &gateway_endpoint)
            .env("TALON_S3_TEST_BUCKET", &bucket)
            .env("TALON_S3_TEST_KEY", &object_key)
            .env("TALON_S3_TEST_REGION", &region)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "S3 SDK conformance process failed");
    } else {
        eprintln!("skipping Python S3 SDK assertions: TALON_S3_SDK_PYTHON is not set");
    }

    // Arrow's S3 filesystem uses signed HEAD and exact ranged GET requests.
    // Authentication is intentionally deferred to #446, but these fixtures
    // prove that its headers do not alter parsing or leak to the origin signer.
    let raw = reqwest::Client::new();
    let object_url = format!("{gateway_endpoint}/{bucket}/{object_key}");
    let properties = raw
        .head(&object_url)
        .header("x-amz-date", "20260809T000000Z")
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
        .header(
            reqwest::header::AUTHORIZATION,
            "AWS4-HMAC-SHA256 Credential=arrow/20260809/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fixture",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(properties.status(), reqwest::StatusCode::OK);
    assert_eq!(
        properties.headers()[reqwest::header::CONTENT_LENGTH],
        "4096"
    );

    let range = raw
        .get(&object_url)
        .header(reqwest::header::RANGE, "bytes=7-1030")
        .header("x-amz-checksum-mode", "ENABLED")
        .send()
        .await
        .unwrap();
    assert_eq!(range.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.bytes().await.unwrap(), &expected_object()[7..1031]);
    assert!(cache.misses.load(Ordering::Relaxed) >= 1);
    assert!(cache.hits.load(Ordering::Relaxed) >= 1);

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}
