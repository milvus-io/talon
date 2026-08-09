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
use talon_gateway::azure_auth::{AzureClientIdentity, AzureStorageAuthenticator};
use talon_gateway::{
    serve, AuthenticatedPrincipal, AuthorizationGrant, AuthorizationPolicy, GatewayConfig,
    GatewayOperation, GatewayRuntime, GatewaySecurity, ProviderProtocol,
};
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
    invalidations: Arc<AtomicUsize>,
}

impl ReadThroughCache {
    fn new(origin: Arc<AzureBackend>) -> Arc<Self> {
        Arc::new(Self {
            origin,
            entries: Arc::new(Mutex::new(HashMap::new())),
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
            invalidations: Arc::new(AtomicUsize::new(0)),
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

    fn invalidate_object(&self, object: &ObjectId) -> usize {
        let mut entries = self.entries.lock().unwrap();
        let before = entries.len();
        entries.retain(|(cached, _, _, _), _| cached != object);
        let removed = before - entries.len();
        self.invalidations.fetch_add(1, Ordering::Relaxed);
        removed
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
    runtime.install_authentication(Arc::new(
        AzureStorageAuthenticator::new(
            DEV_ACCOUNT,
            vec![AzureClientIdentity {
                account_key: DEV_KEY.into(),
                principal: AuthenticatedPrincipal::new("azure-sdk", DEV_ACCOUNT),
            }],
            std::time::Duration::from_secs(15 * 60),
            false,
        )
        .unwrap(),
    ));
    runtime.install_authorization(
        AuthorizationPolicy::new(vec![AuthorizationGrant {
            id: "azure-sdk-conformance".into(),
            principal: "azure-sdk".into(),
            protocol: ProviderProtocol::Azure,
            provider_account: DEV_ACCOUNT.into(),
            namespace: container.clone(),
            prefix: None,
            operations: vec![
                GatewayOperation::Stat,
                GatewayOperation::Read,
                GatewayOperation::List,
                GatewayOperation::Write,
                GatewayOperation::Delete,
            ],
        }])
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

    assert!(cache.misses.load(Ordering::Relaxed) >= 1);
    assert!(cache.hits.load(Ordering::Relaxed) >= 1);
    assert!(cache.invalidations.load(Ordering::Relaxed) >= 6);

    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}
