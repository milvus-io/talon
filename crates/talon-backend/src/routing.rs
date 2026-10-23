//! Backend resolution and routing over shared store instances.
//!
//! Loading TOML or obtaining future configuration updates belongs to the caller.
//! Each entry owns its credential provider while sharing the supplied transports.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use sha2::{Digest, Sha256};
use talon_core::backend::AccessScope;
use talon_core::config::BucketConfig;
use talon_core::{
    Backend, BackendStore, Error, ListPage, ObjectId, ObjectStat, OriginOptions, ResolvedBackend,
    Result, Version,
};
use tokio::sync::{Notify, OnceCell};

use crate::broker;
use crate::origin::{
    AzureCredentialInput, AzureOrigin, AzureOriginInput, BackendBuildConfig, GcsCredentialInput,
    GcsOrigin, GcsOriginInput, OriginValidation, S3Origin, S3OriginInput,
};
use crate::{
    BrokerConfig, CredentialOptions, CredentialsObserver, GcsHmacCredentials, HttpClient,
    S3Credentials,
};

const MAX_DYNAMIC_BACKENDS: usize = 4096;

/// The backend family, bucket, and optional request-owned credential configuration.
#[derive(Debug, Clone, Copy)]
pub struct RouteRequest<'a> {
    pub backend: Backend,
    pub bucket: &'a str,
    pub broker: Option<&'a BrokerConfig>,
}

/// Select a shared store without exposing its construction to the caller.
#[async_trait]
pub trait BackendResolver: Send + Sync {
    fn ready_for_background(&self) -> bool {
        true
    }

    /// Check local route eligibility before cache access, without performing I/O.
    fn validate(&self, request: &RouteRequest<'_>) -> Result<()>;

    fn validate_object(&self, object: &ObjectId) -> Result<()> {
        self.validate(&RouteRequest {
            backend: object.backend,
            bucket: &object.bucket,
            broker: None,
        })?;
        validate_object_key(object)
    }

    async fn resolve(&self, request: &RouteRequest<'_>) -> Result<ResolvedBackend>;
}

/// Addressing defaults only: dynamic requests supply their own endpoint and credentials.
#[derive(Clone, Default)]
pub struct DynamicBackendDefaults {
    pub s3_path_style: Option<bool>,
}

/// Retains legacy implicit IAM discovery without contacting it at dynamic-only startup.
pub type LegacyBackendFactory = Arc<
    dyn Fn() -> BoxFuture<'static, std::result::Result<Arc<dyn BackendStore>, String>>
        + Send
        + Sync,
>;

enum LegacyBackend {
    Ready(Arc<dyn BackendStore>),
    Lazy {
        factory: LegacyBackendFactory,
        store: OnceCell<Arc<dyn BackendStore>>,
    },
}

// The full normalized configuration is the equality key. It contains request
// secrets and deliberately has no Debug implementation; only its digest leaves
// this resolver as the persistent cache scope.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ConfigKey(String);

impl ConfigKey {
    /// Derive the stable cache scope from the normalized configuration.
    fn access_scope(&self) -> Result<AccessScope> {
        format!("v1-{:x}", Sha256::digest(self.0.as_bytes())).parse()
    }
}

type CreationResult = std::result::Result<Arc<dyn BackendStore>, String>;

#[derive(Default)]
struct PendingBackend {
    result: OnceCell<CreationResult>,
    ready: Notify,
}

#[derive(Default)]
struct DynamicBackends {
    stores: HashMap<ConfigKey, Arc<dyn BackendStore>>,
    pending: HashMap<ConfigKey, Arc<PendingBackend>>,
}

/// Static routes and normalized request configurations for one cloud family.
pub struct DefaultBackendResolver {
    backend: Backend,
    static_routes: HashMap<String, Arc<dyn BackendStore>>,
    dynamic: Arc<Mutex<DynamicBackends>>,
    defaults: DynamicBackendDefaults,
    legacy: Option<LegacyBackend>,
    origin_http: Arc<dyn HttpClient>,
    exchange_http: Arc<dyn HttpClient>,
    observer: Arc<dyn CredentialsObserver>,
}

impl DefaultBackendResolver {
    /// Start with no static routes. No credential discovery or network I/O occurs.
    pub fn new(
        backend: Backend,
        origin_http: Arc<dyn HttpClient>,
        exchange_http: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> Self {
        Self {
            backend,
            static_routes: HashMap::new(),
            dynamic: Arc::new(Mutex::new(DynamicBackends::default())),
            defaults: DynamicBackendDefaults::default(),
            legacy: None,
            origin_http,
            exchange_http,
            observer,
        }
    }

    pub fn with_dynamic_defaults(mut self, defaults: DynamicBackendDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    pub fn with_legacy_backend(mut self, store: Arc<dyn BackendStore>) -> Self {
        self.legacy = Some(LegacyBackend::Ready(store));
        self
    }

    pub fn with_legacy_factory(mut self, factory: LegacyBackendFactory) -> Self {
        self.legacy = Some(LegacyBackend::Lazy {
            factory,
            store: OnceCell::new(),
        });
        self
    }

    /// Validate every explicit route before the first credential acquisition.
    pub async fn from_configs(
        backend: Backend,
        configs: Vec<BucketConfig>,
        origin_http: Arc<dyn HttpClient>,
        exchange_http: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> std::result::Result<Self, String> {
        if configs.is_empty() {
            return Err("backend router requires at least one bucket".into());
        }
        let mut names = HashSet::with_capacity(configs.len());
        let mut prepared = Vec::with_capacity(configs.len());
        for config in configs {
            if !names.insert(config.name.clone()) {
                return Err(format!("duplicate bucket {:?}", config.name));
            }
            prepared.push(RouteBuildConfig::new(backend, config)?);
        }
        let mut resolver = Self::new(backend, origin_http, exchange_http, observer);
        for config in prepared {
            let name = config.bucket.clone();
            let store = config
                .build(
                    resolver.origin_http.clone(),
                    resolver.exchange_http.clone(),
                    resolver.observer.clone(),
                )
                .await?;
            resolver.static_routes.insert(name, store);
        }
        Ok(resolver)
    }

    fn prepare_dynamic(&self, request: &RouteRequest<'_>) -> Result<RouteBuildConfig> {
        if request.backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        let mut broker = request.broker.expect("dynamic request").clone();
        if !broker.request.is_object() {
            return Err(Error::Backend(
                "broker request must be a JSON object".into(),
            ));
        }
        if let Some(bucket) = broker.request.get(broker::FIELD_BUCKET) {
            if bucket.as_str() != Some(request.bucket) {
                return Err(Error::Backend(
                    "broker request bucket does not match requested bucket".into(),
                ));
            }
        } else if self.backend != Backend::Azure {
            broker.request[broker::FIELD_BUCKET] = request.bucket.into();
        }
        validate_bucket_name(request.bucket).map_err(Error::Backend)?;
        let (origin, reserved_origin) =
            BackendBuildConfig::dynamic(self.backend, broker, &self.defaults)
                .map_err(Error::Backend)?;
        Ok(RouteBuildConfig {
            bucket: request.bucket.to_owned(),
            origin,
            reserved_origin,
        })
    }

    async fn resolve_dynamic(&self, request: &RouteRequest<'_>) -> Result<ResolvedBackend> {
        let prepared = self.prepare_dynamic(request)?;
        let key = prepared.key();
        let scope = key.access_scope()?;
        let pending = {
            let mut dynamic = self.dynamic.lock().unwrap();
            if let Some(store) = dynamic.stores.get(&key) {
                return Ok(ResolvedBackend {
                    scope: Some(scope),
                    store: store.clone(),
                });
            }
            if let Some(pending) = dynamic.pending.get(&key) {
                pending.clone()
            } else {
                if dynamic.stores.len() + dynamic.pending.len() >= MAX_DYNAMIC_BACKENDS {
                    return Err(Error::Backend(
                        "dynamic backend configuration limit reached".into(),
                    ));
                }
                let pending = Arc::new(PendingBackend::default());
                dynamic.pending.insert(key.clone(), pending.clone());
                let registry = self.dynamic.clone();
                let slot = pending.clone();
                let origin_http = self.origin_http.clone();
                let exchange_http = self.exchange_http.clone();
                let observer = self.observer.clone();
                let backend = self.backend;
                let bucket = request.bucket.to_owned();
                // The registry owns bounded construction, independent of the
                // initiating RPC. Cancellation cannot strand an occupied slot.
                tokio::spawn(async move {
                    let result = std::panic::AssertUnwindSafe(prepared.build(
                        origin_http,
                        exchange_http,
                        observer,
                    ))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err("dynamic backend initialization failed".into()))
                    .map(|store| {
                        // A cached binding must retain local target checks;
                        // raw cloud backends intentionally accept any bucket.
                        Arc::new(BackendRouter::new(
                            backend,
                            Arc::new(BoundBackend {
                                backend,
                                bucket,
                                store,
                            }),
                        )) as Arc<dyn BackendStore>
                    });
                    {
                        let mut registry = registry.lock().unwrap();
                        if let Ok(store) = &result {
                            registry.stores.insert(key.clone(), store.clone());
                        }
                        registry.pending.remove(&key);
                    }
                    let _ = slot.result.set(result);
                    slot.ready.notify_waiters();
                });
                pending
            }
        };
        loop {
            let notified = pending.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = pending.result.get() {
                return result
                    .clone()
                    .map(|store| ResolvedBackend {
                        scope: Some(scope),
                        store,
                    })
                    .map_err(Error::Backend);
            }
            notified.await;
        }
    }
}

#[async_trait]
impl BackendResolver for DefaultBackendResolver {
    fn ready_for_background(&self) -> bool {
        !self.static_routes.is_empty()
            || match &self.legacy {
                Some(LegacyBackend::Ready(_)) => true,
                Some(LegacyBackend::Lazy { store, .. }) => store.get().is_some(),
                None => false,
            }
    }

    fn validate(&self, request: &RouteRequest<'_>) -> Result<()> {
        if request.backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        if request.broker.is_some() {
            return self.prepare_dynamic(request).map(|_| ());
        }
        if self.static_routes.contains_key(request.bucket) || self.legacy.is_some() {
            Ok(())
        } else {
            Err(Error::Backend(
                "bucket is not configured in this router".into(),
            ))
        }
    }

    fn validate_object(&self, object: &ObjectId) -> Result<()> {
        self.validate(&RouteRequest {
            backend: object.backend,
            bucket: &object.bucket,
            broker: None,
        })?;
        if self.static_routes.contains_key(&object.bucket) || self.legacy.is_none() {
            validate_object_key(object)?;
        }
        Ok(())
    }

    async fn resolve(&self, request: &RouteRequest<'_>) -> Result<ResolvedBackend> {
        if request.broker.is_some() {
            return self.resolve_dynamic(request).await;
        }
        self.validate(request)?;
        let store = if let Some(store) = self.static_routes.get(request.bucket) {
            store.clone()
        } else {
            match self.legacy.as_ref().expect("validated legacy route") {
                LegacyBackend::Ready(store) => store.clone(),
                LegacyBackend::Lazy { factory, store } => store
                    .get_or_try_init(|| factory())
                    .await
                    .map_err(|error| Error::Backend(error.to_owned()))?
                    .clone(),
            }
        };
        Ok(ResolvedBackend { scope: None, store })
    }
}

struct BoundBackend {
    backend: Backend,
    bucket: String,
    store: Arc<dyn BackendStore>,
}

#[async_trait]
impl BackendResolver for BoundBackend {
    fn validate(&self, request: &RouteRequest<'_>) -> Result<()> {
        if request.backend != self.backend
            || request.bucket != self.bucket
            || request.broker.is_some()
        {
            return Err(Error::Backend(
                "request does not match bound backend target".into(),
            ));
        }
        Ok(())
    }

    async fn resolve(&self, request: &RouteRequest<'_>) -> Result<ResolvedBackend> {
        self.validate(request)?;
        Ok(ResolvedBackend {
            scope: None,
            store: self.store.clone(),
        })
    }
}

/// Bucket route configuration used to identify and build a reusable backend.
struct RouteBuildConfig {
    bucket: String,
    origin: BackendBuildConfig,
    reserved_origin: Option<OriginOptions>,
}

impl RouteBuildConfig {
    fn new(backend: Backend, config: BucketConfig) -> std::result::Result<Self, String> {
        config
            .validate(backend)
            .map_err(|error| error.to_string())?;
        let broker = if config.credentials_source == "broker" {
            let broker = BrokerConfig {
                origin: config
                    .credential_broker_timeout_ms
                    .map(|timeout| OriginOptions {
                        request_timeout_ms: Some(timeout),
                        ..Default::default()
                    }),
                url: config
                    .credential_broker_url
                    .as_ref()
                    .expect("validated Broker URL")
                    .expose()
                    .clone(),
                request: config
                    .credential_broker_request
                    .as_ref()
                    .expect("validated Broker request")
                    .expose()
                    .clone(),
            };
            if broker
                .request
                .get(broker::FIELD_BUCKET)
                .is_some_and(|bucket| bucket.as_str() != Some(config.name.as_str()))
            {
                return Err("broker request bucket does not match configured bucket".into());
            }
            Some(broker)
        } else {
            None
        };
        let options = CredentialOptions {
            source: Some(config.credentials_source),
            broker,
            s3_region: config.s3_region.clone(),
        };
        let validation = OriginValidation::Routed;
        let origin = match backend {
            Backend::S3 => BackendBuildConfig::S3(S3Origin::prepare(S3OriginInput {
                region: config.s3_region.expect("validated S3 region"),
                endpoint: config.s3_endpoint,
                path_style: config.s3_path_style.unwrap_or(false),
                use_ssl: None,
                validation,
                keys: config
                    .s3_access_key_id
                    .zip(config.s3_secret_access_key)
                    .map(|(access_key_id, secret)| S3Credentials {
                        access_key_id,
                        secret_access_key: secret.expose().clone(),
                        session_token: config.s3_session_token.map(|v| v.expose().clone()),
                    }),
                credentials: options,
            })?),
            Backend::Gcs => BackendBuildConfig::Gcs(GcsOrigin::prepare(GcsOriginInput {
                endpoint: config.gcs_endpoint,
                use_ssl: None,
                validation,
                credentials: GcsCredentialInput::Resolve {
                    keys: config
                        .gcs_access_key_id
                        .zip(config.gcs_secret_access_key)
                        .map(|(access_key_id, secret)| GcsHmacCredentials {
                            access_key_id,
                            secret_access_key: secret.expose().clone(),
                        }),
                    options: Box::new(options),
                },
            })?),
            Backend::Azure => BackendBuildConfig::Azure(AzureOrigin::prepare(AzureOriginInput {
                account: config.azure_account.expect("validated Azure account"),
                endpoint: config.azure_endpoint,
                endpoint_suffix: None,
                path_style: None,
                use_ssl: None,
                validation,
                credentials: AzureCredentialInput::Resolve {
                    shared_key: config.azure_shared_key.map(|v| v.expose().clone()),
                    options: Box::new(options),
                },
            })?),
        };
        Ok(Self {
            bucket: config.name,
            origin,
            reserved_origin: None,
        })
    }

    fn key(&self) -> ConfigKey {
        let broker = self.origin.broker().expect("dynamic Broker configuration");
        let mut key = vec![
            self.origin.key(),
            serde_json::json!(self.bucket),
            serde_json::json!(broker.url()),
            broker.request().clone(),
        ];
        if let Some(reserved) = &self.reserved_origin {
            key.push(serde_json::json!(reserved));
        }
        ConfigKey(serde_json::Value::Array(key).to_string())
    }

    async fn build(
        self,
        origin_http: Arc<dyn HttpClient>,
        exchange_http: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> CreationResult {
        self.origin
            .build(origin_http, exchange_http, observer)
            .await
    }
}

fn validate_bucket_name(name: &str) -> std::result::Result<(), String> {
    if name.is_empty()
        || matches!(name, "." | "..")
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Err("bucket name must be a nonempty, safe bucket/container component".into());
    }
    Ok(())
}

/// Forward object operations through one resolver.
///
/// Object keys must be nonempty relative paths with nonempty components other
/// than `.` or `..`. Controls, trailing spaces, and URL syntax (`%`, `?`, `#`,
/// and backslash) are rejected before cache/origin access. These restrictions
/// prevent the existing URL builders from normalizing a key into another target;
/// ordinary Unicode and spaces within a key remain supported.
pub struct BackendRouter {
    backend: Backend,
    resolver: Arc<dyn BackendResolver>,
}

impl BackendRouter {
    pub fn new(backend: Backend, resolver: Arc<dyn BackendResolver>) -> Self {
        Self { backend, resolver }
    }

    /// Build static routes through the default resolver.
    pub async fn from_configs(
        backend: Backend,
        configs: Vec<BucketConfig>,
        origin_http: Arc<dyn HttpClient>,
        exchange_http: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> std::result::Result<Self, String> {
        let resolver = DefaultBackendResolver::from_configs(
            backend,
            configs,
            origin_http,
            exchange_http,
            observer,
        )
        .await?;
        Ok(Self::new(backend, Arc::new(resolver)))
    }

    async fn object_store(&self, object: &ObjectId) -> Result<Arc<dyn BackendStore>> {
        self.validate_object(object)?;
        self.resolver
            .resolve(&RouteRequest {
                backend: object.backend,
                bucket: &object.bucket,
                broker: None,
            })
            .await
            .map(|resolved| resolved.store)
    }
}

fn validate_object_key(object: &ObjectId) -> Result<()> {
    let key = &object.object_path;
    // The origin URL builders interpolate raw keys. URL parsing removes dot
    // segments (including percent-encoded ones), treats backslash as a
    // separator, and strips trailing spaces. Reject URI syntax outright so
    // nested escapes cannot alias another object or escape the allowed bucket.
    if key
        .split('/')
        .any(|component| matches!(component, "" | "." | ".."))
        || key.ends_with(' ')
        || key.chars().any(char::is_control)
        || key
            .bytes()
            .any(|byte| matches!(byte, b'%' | b'?' | b'#' | b'\\'))
    {
        return Err(Error::Backend(
            "backend router requires a nonempty relative object path without empty/dot components, trailing spaces, controls, or URL syntax (%, ?, #, backslash)"
                .into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl BackendStore for BackendRouter {
    fn ready_for_background(&self) -> bool {
        self.resolver.ready_for_background()
    }

    async fn bind_request(
        &self,
        backend: Backend,
        bucket: &str,
        broker: &BrokerConfig,
    ) -> Result<ResolvedBackend> {
        if backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        self.resolver
            .resolve(&RouteRequest {
                backend,
                bucket,
                broker: Some(broker),
            })
            .await
    }

    fn validate_bucket(&self, backend: Backend, bucket: &str) -> Result<()> {
        if backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        self.resolver.validate(&RouteRequest {
            backend,
            bucket,
            broker: None,
        })
    }

    fn validate_object(&self, object: &ObjectId) -> Result<()> {
        self.validate_bucket(object.backend, &object.bucket)?;
        self.resolver.validate_object(object)
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        cursor: Option<&str>,
        max_keys: u32,
    ) -> Result<ListPage> {
        self.validate_bucket(self.backend, bucket)?;
        self.resolver
            .resolve(&RouteRequest {
                backend: self.backend,
                bucket,
                broker: None,
            })
            .await?
            .store
            .list_objects(bucket, prefix, cursor, max_keys)
            .await
    }

    async fn fetch_range(&self, obj: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
        self.object_store(obj)
            .await?
            .fetch_range(obj, offset, len)
            .await
    }

    async fn fetch_range_if_match(
        &self,
        obj: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> Result<Bytes> {
        self.object_store(obj)
            .await?
            .fetch_range_if_match(obj, offset, len, if_match)
            .await
    }

    async fn head(&self, obj: &ObjectId) -> Result<ObjectStat> {
        self.object_store(obj).await?.head(obj).await
    }

    async fn put(&self, obj: &ObjectId, body: Bytes) -> Result<Version> {
        self.object_store(obj).await?.put(obj, body).await
    }

    async fn put_if_match(
        &self,
        obj: &ObjectId,
        body: Bytes,
        if_match: Option<&Version>,
    ) -> Result<Version> {
        self.object_store(obj)
            .await?
            .put_if_match(obj, body, if_match)
            .await
    }

    async fn put_file(&self, obj: &ObjectId, path: &Path, len: u64) -> Result<Version> {
        self.object_store(obj).await?.put_file(obj, path, len).await
    }

    async fn delete(&self, obj: &ObjectId) -> Result<()> {
        self.object_store(obj).await?.delete(obj).await
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    // Pin persisted key bytes for explicit endpoints. Changing them migrates the
    // cache scope even when pairwise normalization tests still pass.
    #[test]
    fn dynamic_configuration_key_bytes_are_stable() {
        let cases = [
            (
                Backend::S3,
                serde_json::json!({"csp":"aws","region":"us-west-2","roleArn":"role","nested":{"z":2,"a":1}}),
                r#"[["s3","us-west-2","s3.us-west-2.amazonaws.com",false,true],"bucket","https://broker.example/exchange",{"bucket":"bucket","csp":"aws","durationSeconds":3600,"nested":{"a":1,"z":2},"region":"us-west-2","roleArn":"role"}]"#,
            ),
            (
                Backend::Gcs,
                serde_json::json!({"csp":"gcp","gcpServiceAccountEmail":"target@example"}),
                r#"[["gcs","storage.googleapis.com",true],"bucket","https://broker.example/exchange",{"bucket":"bucket","csp":"gcp","durationSeconds":3600,"gcpServiceAccountEmail":"target@example"}]"#,
            ),
            (
                Backend::Azure,
                serde_json::json!({"csp":"azure","credentialType":"iam","bucket":"bucket","azureAccountName":"account","azureClientId":"ignored"}),
                r#"[["azure","account","blob.core.windows.net","account.blob.core.windows.net",false,true],"bucket","https://broker.example/exchange",{"azureAccountName":"account","bucket":"bucket","credentialType":"iam","csp":"azure","durationSeconds":3600}]"#,
            ),
        ];
        for (backend, request, expected) in cases {
            let http = Arc::new(crate::ReqwestClient::new());
            let resolver = DefaultBackendResolver::new(
                backend,
                http.clone(),
                http,
                Arc::new(crate::NoopObserver),
            );
            let endpoint = match backend {
                Backend::S3 => "s3.us-west-2.amazonaws.com",
                Backend::Gcs => "storage.googleapis.com",
                Backend::Azure => "account.blob.core.windows.net",
            };
            let mut broker = BrokerConfig {
                url: "https://BROKER.example:443/exchange".into(),
                request,
                origin: Some(OriginOptions {
                    endpoint: Some(endpoint.into()),
                    region: (backend == Backend::S3).then(|| "us-west-2".into()),
                    ..Default::default()
                }),
            };
            let key = |broker: &BrokerConfig| {
                resolver
                    .prepare_dynamic(&RouteRequest {
                        backend,
                        bucket: "bucket",
                        broker: Some(broker),
                    })
                    .unwrap()
                    .key()
                    .0
            };
            assert_eq!(key(&broker), expected);
            broker.origin = Some(OriginOptions {
                endpoint: Some(format!("https://{endpoint}/")),
                region: (backend == Backend::S3).then(|| "us-west-2".into()),
                ..Default::default()
            });
            assert_eq!(key(&broker), expected);
            broker.origin = Some(OriginOptions {
                endpoint: Some(endpoint.into()),
                region: (backend == Backend::S3).then(|| "us-west-2".into()),
                tls_ca_pem: Some("reserved".into()),
                request_timeout_ms: Some(123),
                ..Default::default()
            });
            let reserved = format!(
                "{},{{\"request_timeout_ms\":123,\"tls_ca_pem\":\"reserved\"}}]",
                expected.strip_suffix(']').unwrap()
            );
            assert_eq!(key(&broker), reserved);
        }
    }
}
