//! Backend resolution and routing over shared store instances.
//!
//! Loading TOML or obtaining future configuration updates belongs to the caller.
//! Each entry owns its credential provider while sharing the supplied transports.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use sha2::{Digest, Sha256};
use talon_core::backend::{AccessScope, BackgroundRoute};
use talon_core::config::{BucketConfig, Sensitive};
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

const MAX_DYNAMIC_BACKENDS: usize = 65_535;

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

    /// Select a route using cached bindings only. Lazy resolvers override this.
    fn background_route(&self, _backend: Backend, _bucket: &str) -> BackgroundRoute {
        if self.ready_for_background() {
            BackgroundRoute::Current
        } else {
            BackgroundRoute::Skip
        }
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

/// Startup configuration for a worker's backend resolver.
#[derive(Default)]
pub struct BackendResolverConfig {
    pub buckets: Vec<BucketConfig>,
    pub defaults: BucketConfig,
    pub credential_endpoint: Option<Sensitive<String>>,
    pub legacy_factory: Option<LegacyBackendFactory>,
    pub initialize_legacy_on_startup: bool,
}

impl From<Vec<BucketConfig>> for BackendResolverConfig {
    fn from(buckets: Vec<BucketConfig>) -> Self {
        Self {
            buckets,
            ..Self::default()
        }
    }
}

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

struct DynamicBackends {
    stores: HashMap<ConfigKey, Arc<dyn BackendStore>>,
    pending: HashMap<ConfigKey, Arc<PendingBackend>>,
    capacity: usize,
    evict_idle_on_full: bool,
}

impl Default for DynamicBackends {
    fn default() -> Self {
        Self {
            stores: HashMap::new(),
            pending: HashMap::new(),
            capacity: MAX_DYNAMIC_BACKENDS,
            evict_idle_on_full: false,
        }
    }
}

/// The active URL for a default Broker route enabled at startup.
pub type SharedBrokerEndpoint = Arc<ArcSwap<Sensitive<String>>>;

struct IamBrokerRoutes {
    endpoint: SharedBrokerEndpoint,
    defaults: BucketConfig,
    buckets: Option<HashMap<String, BucketConfig>>,
    backends: Arc<RwLock<DynamicBackends>>,
}

impl IamBrokerRoutes {
    fn prepare(&self, backend: Backend, bucket: &str) -> Result<RouteBuildConfig> {
        // Bucket entries supply the origin settings. The Broker's Pod IAM
        // identity supplies the credentials for this default route.
        let origin = self
            .buckets
            .as_ref()
            .and_then(|buckets| buckets.get(bucket))
            .unwrap_or(&self.defaults);
        let mut config = BucketConfig {
            name: bucket.to_owned(),
            credentials_source: "broker".into(),
            s3_region: origin.s3_region.clone(),
            s3_endpoint: origin.s3_endpoint.clone(),
            s3_path_style: origin.s3_path_style,
            gcs_endpoint: origin.gcs_endpoint.clone(),
            azure_account: origin.azure_account.clone(),
            azure_endpoint: origin.azure_endpoint.clone(),
            credential_broker_url: Some(self.endpoint.load().as_ref().clone()),
            credential_broker_timeout_ms: origin
                .credential_broker_timeout_ms
                .or(self.defaults.credential_broker_timeout_ms),
            ..Default::default()
        };
        config.credential_broker_request =
            Some(broker::build_iam_request_body(backend, &config).into());
        RouteBuildConfig::new(backend, config).map_err(Error::Backend)
    }
}

/// Static routes and normalized request configurations for one cloud family.
pub struct DefaultBackendResolver {
    backend: Backend,
    static_routes: HashMap<String, Arc<dyn BackendStore>>,
    dynamic: Arc<RwLock<DynamicBackends>>,
    defaults: DynamicBackendDefaults,
    legacy: Option<LegacyBackend>,
    iam_broker: Option<IamBrokerRoutes>,
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
            dynamic: Arc::new(RwLock::new(DynamicBackends::default())),
            defaults: DynamicBackendDefaults::default(),
            legacy: None,
            iam_broker: None,
            origin_http,
            exchange_http,
            observer,
        }
    }

    pub fn with_dynamic_defaults(mut self, defaults: DynamicBackendDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// Share the active URL with the Worker configuration reloader.
    pub fn broker_endpoint(&self) -> Option<SharedBrokerEndpoint> {
        self.iam_broker.as_ref().map(|iam| iam.endpoint.clone())
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

    /// Resolve the startup configuration and initialize the selected static backends.
    pub async fn from_configs(
        backend: Backend,
        configs: impl Into<BackendResolverConfig>,
        origin_http: Arc<dyn HttpClient>,
        exchange_http: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> std::result::Result<Self, String> {
        let configs = configs.into();
        let mut names = HashSet::with_capacity(configs.buckets.len());
        for config in &configs.buckets {
            if !names.insert(config.name.clone()) {
                return Err(format!("duplicate bucket {:?}", config.name));
            }
        }
        let mut resolver = Self::new(backend, origin_http, exchange_http, observer)
            .with_dynamic_defaults(DynamicBackendDefaults {
                s3_path_style: configs.defaults.s3_path_style,
            });
        if let Some(endpoint) = configs.credential_endpoint {
            for config in &configs.buckets {
                config
                    .validate_addressing(backend)
                    .map_err(|error| error.to_string())?;
            }
            resolver.iam_broker = Some(IamBrokerRoutes {
                endpoint: Arc::new(ArcSwap::from_pointee(endpoint)),
                defaults: configs.defaults,
                buckets: (!configs.buckets.is_empty()).then(|| {
                    configs
                        .buckets
                        .into_iter()
                        .map(|bucket| (bucket.name.clone(), bucket))
                        .collect()
                }),
                backends: Arc::new(RwLock::new(DynamicBackends {
                    evict_idle_on_full: true,
                    ..Default::default()
                })),
            });
            return Ok(resolver);
        }
        if configs.buckets.is_empty() {
            if let Some(factory) = configs.legacy_factory {
                resolver = if configs.initialize_legacy_on_startup {
                    resolver.with_legacy_backend(factory().await?)
                } else {
                    resolver.with_legacy_factory(factory)
                };
            }
            return Ok(resolver);
        }

        // Validate every static route before the first credential acquisition.
        let prepared = configs
            .buckets
            .into_iter()
            .map(|config| RouteBuildConfig::new(backend, config))
            .collect::<std::result::Result<Vec<_>, _>>()?;
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

    fn prepare_dynamic(&self, request: &RouteRequest<'_>) -> Result<Option<RouteBuildConfig>> {
        let Some(broker) = request.broker else {
            return Ok(None);
        };
        let worker_broker = self.iam_broker.as_ref().filter(|_| broker.url.is_empty());
        let worker_endpoint = worker_broker.map(|iam| iam.endpoint.load_full());
        let worker_url = worker_endpoint
            .as_deref()
            .map(|endpoint| endpoint.expose().as_str());
        let Some(url) = broker.url_with_default(worker_url) else {
            return Ok(None);
        };
        if request.backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        let mut broker = BrokerConfig {
            url: url.to_owned(),
            request: broker.request.clone(),
            origin: broker.origin.clone(),
        };
        if let (Some(iam), Some(origin)) = (worker_broker, &mut broker.origin) {
            origin.request_timeout_ms = origin
                .request_timeout_ms
                .or(iam.defaults.credential_broker_timeout_ms);
        }
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
        Ok(Some(RouteBuildConfig {
            bucket: request.bucket.to_owned(),
            origin,
            reserved_origin,
        }))
    }

    async fn resolve_broker(
        &self,
        prepared: RouteBuildConfig,
        backends: &Arc<RwLock<DynamicBackends>>,
    ) -> Result<ResolvedBackend> {
        let key = prepared.key();
        let scope = key.access_scope()?;
        let pending = {
            let dynamic = backends.read().unwrap();
            if let Some(store) = dynamic.stores.get(&key) {
                return Ok(ResolvedBackend {
                    scope: Some(scope),
                    store: store.clone(),
                });
            }
            dynamic.pending.get(&key).cloned()
        };
        let pending = if let Some(pending) = pending {
            pending
        } else {
            // Another caller may have registered or completed this key since
            // the read lock was released. Recheck before reserving capacity.
            let mut dynamic = backends.write().unwrap();
            if let Some(store) = dynamic.stores.get(&key) {
                return Ok(ResolvedBackend {
                    scope: Some(scope),
                    store: store.clone(),
                });
            }
            if let Some(pending) = dynamic.pending.get(&key) {
                pending.clone()
            } else {
                let full = dynamic.stores.len() + dynamic.pending.len() >= dynamic.capacity;
                let evicted = if full && dynamic.evict_idle_on_full {
                    // Check and remove under the same lock used to clone bindings.
                    let idle = dynamic.stores.iter().find_map(|(key, store)| {
                        (Arc::strong_count(store) == 1).then(|| key.clone())
                    });
                    idle.and_then(|key| dynamic.stores.remove(&key))
                } else {
                    None
                };
                if full && evicted.is_none() {
                    return Err(Error::Backend(
                        "dynamic backend configuration limit reached".into(),
                    ));
                }
                let pending = Arc::new(PendingBackend::default());
                dynamic.pending.insert(key.clone(), pending.clone());
                drop(dynamic);
                drop(evicted);
                let registry = backends.clone();
                let slot = pending.clone();
                let origin_http = self.origin_http.clone();
                let exchange_http = self.exchange_http.clone();
                let observer = self.observer.clone();
                let backend = self.backend;
                let bucket = prepared.bucket.clone();
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
                        let mut registry = registry.write().unwrap();
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
            || self
                .iam_broker
                .as_ref()
                .is_some_and(|iam| !iam.backends.read().unwrap().stores.is_empty())
            || match &self.legacy {
                Some(LegacyBackend::Ready(_)) => true,
                Some(LegacyBackend::Lazy { store, .. }) => store.get().is_some(),
                None => false,
            }
    }

    fn background_route(&self, backend: Backend, bucket: &str) -> BackgroundRoute {
        if let Some(iam) = &self.iam_broker {
            let Ok(prepared) = iam.prepare(backend, bucket) else {
                return BackgroundRoute::Skip;
            };
            return iam
                .backends
                .read()
                .unwrap()
                .stores
                .get(&prepared.key())
                .cloned()
                .map_or(BackgroundRoute::Skip, BackgroundRoute::Bound);
        }
        let store = self
            .static_routes
            .get(bucket)
            .or_else(|| match &self.legacy {
                Some(LegacyBackend::Ready(store)) => Some(store),
                Some(LegacyBackend::Lazy { store, .. }) => store.get(),
                None => None,
            });
        if store.is_some() {
            BackgroundRoute::Current
        } else {
            BackgroundRoute::Skip
        }
    }

    fn validate(&self, request: &RouteRequest<'_>) -> Result<()> {
        if request.backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        if self.prepare_dynamic(request)?.is_some() {
            return Ok(());
        }
        if let Some(iam) = &self.iam_broker {
            if iam
                .buckets
                .as_ref()
                .is_some_and(|buckets| !buckets.contains_key(request.bucket))
            {
                return Err(Error::Backend(
                    "bucket is not configured in this router".into(),
                ));
            }
            return validate_bucket_name(request.bucket).map_err(Error::Backend);
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

    // Request Broker URL?
    // +-- yes -> Request URL + request Broker parameters
    // `-- no  -> Worker credential_endpoint?
    //            +-- no  -> Existing Worker credentials
    //            `-- yes -> Request Broker parameters?
    //                       +-- yes -> Worker URL + request parameters (credentialType)
    //                       `-- no  -> Worker URL + generated Pod IAM parameters (csp from backend)
    async fn resolve(&self, request: &RouteRequest<'_>) -> Result<ResolvedBackend> {
        // Bind request parameters to a cache scope using the effective Broker URL.
        if let Some(prepared) = self.prepare_dynamic(request)? {
            return self.resolve_broker(prepared, &self.dynamic).await;
        }
        self.validate(request)?;
        if let Some(iam) = &self.iam_broker {
            let prepared = iam.prepare(self.backend, request.bucket)?;
            return self.resolve_broker(prepared, &iam.backends).await;
        }
        // Bucket stores are ready at startup. Implicit global IAM discovery
        // starts on the first request that needs the legacy backend.
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
            Backend::S3 | Backend::Ali | Backend::Tc => {
                BackendBuildConfig::S3(S3Origin::prepare(S3OriginInput {
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
                })?)
            }
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

    fn background_route(&self, backend: Backend, bucket: &str) -> BackgroundRoute {
        if backend == self.backend {
            self.resolver.background_route(backend, bucket)
        } else {
            BackgroundRoute::Skip
        }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn full_default_iam_registry_rebinds_evicted_buckets() {
        #[derive(Default)]
        struct BrokerHttp(AtomicUsize);

        #[async_trait]
        impl HttpClient for BrokerHttp {
            async fn execute(
                &self,
                request: crate::HttpRequest,
            ) -> std::result::Result<crate::HttpResponse, String> {
                assert_eq!(request.method, crate::Method::Post);
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(crate::HttpResponse {
                    status: 200,
                    headers: vec![],
                    body: Bytes::from_static(br#"{"success":true,"credentials":{"tempAk":"access","tempSk":"secret","sessionToken":"token","expiredAt":"2099-01-01T00:00:00Z"}}"#),
                })
            }
        }

        let http = Arc::new(BrokerHttp::default());
        let backend = Backend::S3;
        let resolver = DefaultBackendResolver::from_configs(
            backend,
            BackendResolverConfig {
                credential_endpoint: Some("https://broker.example/exchange".to_owned().into()),
                defaults: BucketConfig {
                    s3_region: Some("us-west-2".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            http.clone(),
            http.clone(),
            Arc::new(crate::NoopObserver),
        )
        .await
        .unwrap();
        resolver
            .iam_broker
            .as_ref()
            .unwrap()
            .backends
            .write()
            .unwrap()
            .capacity = 2;
        let request = |bucket| RouteRequest {
            backend,
            bucket,
            broker: None,
        };
        let held = resolver.resolve(&request("held")).await.unwrap();
        let idle_scope = resolver.resolve(&request("idle")).await.unwrap().scope;
        let new = resolver.resolve(&request("new")).await.unwrap();
        let reused = resolver.resolve(&request("held")).await.unwrap();
        assert!(Arc::ptr_eq(&held.store, &reused.store));
        assert!(matches!(
            resolver.background_route(backend, "idle"),
            BackgroundRoute::Skip
        ));
        assert_eq!(http.0.load(Ordering::Relaxed), 3);

        let error = resolver.resolve(&request("idle")).await.err().unwrap();
        assert!(error
            .to_string()
            .contains("dynamic backend configuration limit reached"));
        assert_eq!(http.0.load(Ordering::Relaxed), 3);

        drop(new);
        let rebound = resolver.resolve(&request("idle")).await.unwrap();
        assert_eq!(rebound.scope, idle_scope);
        assert_eq!(http.0.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn cached_and_pending_bindings_do_not_block_other_readers() {
        let http = Arc::new(crate::ReqwestClient::new());
        let resolver = DefaultBackendResolver::new(
            Backend::S3,
            http.clone(),
            http.clone(),
            Arc::new(crate::NoopObserver),
        );
        let store: Arc<dyn BackendStore> = Arc::new(crate::S3Backend::new(
            crate::S3Config::aws("us-west-2"),
            S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: "secret".into(),
                session_token: None,
            },
            http,
        ));
        let broker = BrokerConfig {
            url: "https://broker.example/exchange".into(),
            request: serde_json::json!({"csp":"aws","region":"us-west-2","roleArn":"role"}),
            origin: Some(OriginOptions {
                endpoint: Some("s3.us-west-2.amazonaws.com".into()),
                region: Some("us-west-2".into()),
                ..Default::default()
            }),
        };
        let request = RouteRequest {
            backend: Backend::S3,
            bucket: "bucket",
            broker: Some(&broker),
        };
        let key = resolver.prepare_dynamic(&request).unwrap().unwrap().key();
        for cached in [true, false] {
            {
                let mut dynamic = resolver.dynamic.write().unwrap();
                if cached {
                    dynamic.stores.insert(key.clone(), store.clone());
                } else {
                    dynamic.stores.clear();
                    dynamic
                        .pending
                        .insert(key.clone(), Arc::new(PendingBackend::default()));
                }
            }
            std::thread::scope(|threads| {
                let reader = resolver.dynamic.read().unwrap();
                let (sent, received) = std::sync::mpsc::sync_channel(1);
                let lookup = resolver.resolve(&request);
                threads.spawn(move || sent.send(lookup.now_or_never()).unwrap());
                let result = received.recv_timeout(std::time::Duration::from_secs(2));
                // Release the reader before checking the result so a blocked
                // lookup can finish and the scoped thread always joins.
                drop(reader);
                let result = result.expect("binding lookup must not wait for other readers");
                if cached {
                    assert!(Arc::ptr_eq(&store, &result.unwrap().unwrap().store));
                } else {
                    assert!(result.is_none(), "reuse the pending initializer");
                }
            });
        }
    }

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
                Backend::S3 | Backend::Ali | Backend::Tc => "s3.us-west-2.amazonaws.com",
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
