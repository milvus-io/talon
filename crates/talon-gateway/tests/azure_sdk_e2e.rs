//! Azure gateway conformance against Azurite and the official Azure SDK.
//!
//! The test is skipped unless `TALON_AZURE_TEST_ENDPOINT` is set. CI supplies
//! a real Azurite service; the gateway itself listens on an ephemeral port.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use talon_backend::{AzureBackend, AzureConfig, ReqwestClient};
use talon_cache_client::CacheReadError;
use talon_core::{ObjectId, Version};
use talon_gateway::azure::{AzureAdapterConfig, AzureBlobAdapter, AzureCache, AzureCacheRequest};
use talon_gateway::{serve, GatewayConfig, GatewayRuntime, GatewaySecurity};
use tokio::net::TcpListener;

const DEV_ACCOUNT: &str = "devstoreaccount1";
const DEV_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

type CacheKey = (ObjectId, Version, u64, u64);
type CacheBody = Pin<Box<dyn Stream<Item = Result<Bytes, CacheReadError>> + Send>>;

struct ReadThroughCache {
    origin: Arc<AzureBackend>,
    entries: Arc<Mutex<HashMap<CacheKey, Bytes>>>,
    hits: Arc<AtomicUsize>,
    misses: Arc<AtomicUsize>,
}

impl ReadThroughCache {
    fn new(origin: Arc<AzureBackend>) -> Arc<Self> {
        Arc::new(Self {
            origin,
            entries: Arc::new(Mutex::new(HashMap::new())),
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
        })
    }
}

impl AzureCache for ReadThroughCache {
    fn stream(&self, request: AzureCacheRequest<'_>) -> Result<CacheBody, CacheReadError> {
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
async fn azure_sdk_and_azurite_gateway_conformance() {
    let Some(origin_endpoint) = env("TALON_AZURE_TEST_ENDPOINT") else {
        eprintln!("skipping Azure gateway e2e: TALON_AZURE_TEST_ENDPOINT is not set");
        return;
    };
    let container =
        env("TALON_AZURE_TEST_CONTAINER").expect("TALON_AZURE_TEST_CONTAINER must be set");
    let object_key = env("TALON_AZURE_TEST_KEY").unwrap_or_else(|| "e2e/object.bin".to_string());
    let (host, tls) = endpoint_host(&origin_endpoint);
    let origin = Arc::new(AzureBackend::with_shared_key(
        AzureConfig::emulator(DEV_ACCOUNT, host, tls),
        DEV_KEY,
        Arc::new(ReqwestClient::new()),
    ));
    let cache = ReadThroughCache::new(Arc::clone(&origin));
    let adapter = Arc::new(
        AzureBlobAdapter::new(
            AzureAdapterConfig::path_style(DEV_ACCOUNT),
            Arc::clone(&cache) as Arc<dyn AzureCache>,
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

    if let Some(python) = env("TALON_AZURE_SDK_PYTHON") {
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/azure_gateway_sdk_conformance.py");
        let status = tokio::process::Command::new(python)
            .arg(script)
            .env("TALON_AZURE_GATEWAY_ENDPOINT", &gateway_endpoint)
            .env("TALON_AZURE_TEST_CONTAINER", &container)
            .env("TALON_AZURE_TEST_KEY", &object_key)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "Azure SDK conformance process failed");
    } else {
        eprintln!("skipping Python Azure SDK assertions: TALON_AZURE_SDK_PYTHON is not set");
    }

    let raw = reqwest::Client::new();
    let object_url = format!("{gateway_endpoint}/{DEV_ACCOUNT}/{container}/{object_key}");
    let properties = raw.head(&object_url).send().await.unwrap();
    assert_eq!(properties.status(), reqwest::StatusCode::OK);
    assert_eq!(
        properties.headers()[reqwest::header::CONTENT_LENGTH],
        "4096"
    );
    let etag = properties.headers()[reqwest::header::ETAG]
        .to_str()
        .unwrap()
        .to_string();
    let whole = raw.get(&object_url).send().await.unwrap();
    assert_eq!(whole.status(), reqwest::StatusCode::OK);
    assert_eq!(whole.bytes().await.unwrap(), expected_object());
    let warm = raw.get(&object_url).send().await.unwrap();
    assert_eq!(warm.status(), reqwest::StatusCode::OK);
    assert_eq!(warm.bytes().await.unwrap(), expected_object());
    assert!(cache.misses.load(Ordering::Relaxed) >= 1);
    assert!(cache.hits.load(Ordering::Relaxed) >= 1);

    for range in [0_usize..1024, 7_usize..1031, 4000_usize..4096] {
        let response = raw
            .get(&object_url)
            .header(
                reqwest::header::RANGE,
                format!("bytes={}-{}", range.start, range.end - 1),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        let bytes = response.bytes().await.unwrap();
        assert_eq!(&bytes[..], &expected_object()[range.start..range.end]);
    }

    let matching = raw
        .get(&object_url)
        .header(reqwest::header::RANGE, "bytes=0-15")
        .header(reqwest::header::IF_MATCH, &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(matching.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let stale = raw
        .get(&object_url)
        .header(reqwest::header::RANGE, "bytes=0-15")
        .header(reqwest::header::IF_MATCH, "\"not-the-etag\"")
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), reqwest::StatusCode::PRECONDITION_FAILED);

    let missing = raw
        .get(format!(
            "{gateway_endpoint}/{DEV_ACCOUNT}/{container}/e2e/missing.bin"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(missing.headers()["x-ms-error-code"], "BlobNotFound");

    let invalid_range = raw
        .get(&object_url)
        .header(reqwest::header::RANGE, "bytes=5000-6000")
        .send()
        .await
        .unwrap();
    assert_eq!(
        invalid_range.status(),
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    );
    assert_eq!(invalid_range.headers()["x-ms-error-code"], "InvalidRange");
    assert!(invalid_range
        .text()
        .await
        .unwrap()
        .contains("<Code>InvalidRange</Code>"));

    let multiple_ranges = raw
        .get(&object_url)
        .header(reqwest::header::RANGE, "bytes=0-1,4-5")
        .send()
        .await
        .unwrap();
    assert_eq!(multiple_ranges.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        multiple_ranges.headers()["x-ms-error-code"],
        "MultipleConditionHeadersNotSupported"
    );

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}
