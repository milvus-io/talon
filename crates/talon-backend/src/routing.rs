//! Backend resolution and routing over shared store instances.
//!
//! Loading TOML or obtaining future configuration updates belongs to the caller.
//! Each entry owns its credential provider while sharing the supplied transports.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use talon_core::config::BucketConfig;
use talon_core::{Backend, BackendStore, Error, ListPage, ObjectId, ObjectStat, Result, Version};

use crate::broker::BrokerFetch;
use crate::{
    endpoint_host, resolve_azure_credentials, resolve_gcs_credentials, resolve_s3_credentials,
    AzureBackend, AzureConfig, BrokerConfig, CredentialOptions, CredentialsObserver, GcsBackend,
    GcsConfig, GcsHmacCredentials, HttpClient, S3Backend, S3Config, S3Credentials,
};

/// The backend family and bucket selected by a request.
#[derive(Debug, Clone, Copy)]
pub struct RouteRequest<'a> {
    pub backend: Backend,
    pub bucket: &'a str,
}

/// Select a shared store without exposing its construction to the caller.
///
/// Implementations own instance reuse and must reject unsupported targets.
#[async_trait]
pub trait BackendResolver: Send + Sync {
    /// Check local route eligibility before cache access, without performing I/O.
    fn validate(&self, request: &RouteRequest<'_>) -> Result<()>;

    /// Return a shared instance, rejecting targets disallowed by `validate`.
    async fn resolve(&self, request: &RouteRequest<'_>) -> Result<Arc<dyn BackendStore>>;
}

/// Static bucket routes for one backend family, with independent credentials.
pub struct DefaultBackendResolver {
    backend: Backend,
    static_routes: HashMap<String, Arc<dyn BackendStore>>,
}

impl DefaultBackendResolver {
    /// Build all backends from resolved records without reading configuration files.
    ///
    /// AK/SK and Broker settings are local to each record. IAM uses the current
    /// process identity through the existing cloud resolvers. All records are
    /// validated before the first credential exchange.
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
            config
                .validate(backend)
                .map_err(|error| error.to_string())?;
            if !names.insert(config.name.clone()) {
                return Err(format!("duplicate bucket {:?}", config.name));
            }
            let endpoint = match backend {
                Backend::S3 => {
                    let region = config.s3_region.as_deref().expect("validated S3 region");
                    if !region
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                    {
                        return Err("S3 region contains invalid characters".into());
                    }
                    config.s3_endpoint.as_deref()
                }
                Backend::Gcs => config.gcs_endpoint.as_deref(),
                Backend::Azure => {
                    let account = config
                        .azure_account
                        .as_deref()
                        .expect("validated Azure account");
                    if !account.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
                        return Err("Azure account contains invalid characters".into());
                    }
                    config.azure_endpoint.as_deref()
                }
            }
            .map(validated_endpoint)
            .transpose()?;
            let broker = if config.credentials_source == "broker" {
                let broker = BrokerConfig {
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
                    .get("bucket")
                    .is_some_and(|bucket| bucket.as_str() != Some(config.name.as_str()))
                {
                    return Err("broker request bucket does not match configured bucket".into());
                }
                let (csp, account) = match backend {
                    Backend::S3 => ("aws", None),
                    Backend::Gcs => ("gcp", None),
                    Backend::Azure => ("azure", config.azure_account.as_deref()),
                };
                // Reuse the protocol's complete validation without making an
                // exchange, including Azure's required bucket/account scope.
                BrokerFetch::<()>::from_config(&broker, csp, account, exchange_http.clone())?;
                Some(broker)
            } else {
                None
            };
            let options = CredentialOptions {
                source: Some(config.credentials_source.clone()),
                broker,
                s3_region: config.s3_region.clone(),
            };
            prepared.push((config, options, endpoint));
        }
        let mut static_routes: HashMap<String, Arc<dyn BackendStore>> =
            HashMap::with_capacity(prepared.len());
        for (config, options, endpoint) in prepared {
            let store: Arc<dyn BackendStore> = match backend {
                Backend::S3 => {
                    let mut origin = S3Config::aws(config.s3_region.expect("validated S3 region"));
                    origin.path_style = config.s3_path_style.unwrap_or(false);
                    if let Some((host, tls)) = endpoint {
                        origin.endpoint = host;
                        origin.tls = tls;
                    }
                    let keys = config
                        .s3_access_key_id
                        .zip(config.s3_secret_access_key)
                        .map(|(access_key_id, secret)| S3Credentials {
                            access_key_id,
                            secret_access_key: secret.expose().clone(),
                            session_token: config
                                .s3_session_token
                                .map(|value| value.expose().clone()),
                        });
                    let resolved = resolve_s3_credentials(
                        keys,
                        options,
                        exchange_http.clone(),
                        observer.clone(),
                    )
                    .await?;
                    Arc::new(S3Backend::with_credentials_provider(
                        origin,
                        resolved.provider,
                        origin_http.clone(),
                    ))
                }
                Backend::Gcs => {
                    let origin = match endpoint {
                        Some((endpoint, tls)) => GcsConfig { endpoint, tls },
                        None => GcsConfig::default(),
                    };
                    let keys = config
                        .gcs_access_key_id
                        .zip(config.gcs_secret_access_key)
                        .map(|(access_key_id, secret)| GcsHmacCredentials {
                            access_key_id,
                            secret_access_key: secret.expose().clone(),
                        });
                    let resolved = resolve_gcs_credentials(
                        keys,
                        options,
                        exchange_http.clone(),
                        observer.clone(),
                    )
                    .await?;
                    Arc::new(GcsBackend::with_credentials(
                        origin,
                        resolved.provider,
                        origin_http.clone(),
                    ))
                }
                Backend::Azure => {
                    let account = config.azure_account.expect("validated Azure account");
                    let origin = match endpoint {
                        Some((host, tls)) => AzureConfig::emulator(&account, host, tls),
                        None => AzureConfig::new(&account),
                    };
                    let resolved = resolve_azure_credentials(
                        &account,
                        config.azure_shared_key.map(|value| value.expose().clone()),
                        options,
                        exchange_http.clone(),
                        observer.clone(),
                    )
                    .await?;
                    Arc::new(AzureBackend::with_credentials(
                        origin,
                        resolved.provider,
                        origin_http.clone(),
                    ))
                }
            };
            static_routes.insert(config.name, store);
        }
        Ok(Self {
            backend,
            static_routes,
        })
    }

    fn store(&self, request: &RouteRequest<'_>) -> Result<&Arc<dyn BackendStore>> {
        if request.backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        self.static_routes
            .get(request.bucket)
            .ok_or_else(|| Error::Backend("bucket is not configured in this router".into()))
    }
}

#[async_trait]
impl BackendResolver for DefaultBackendResolver {
    fn validate(&self, request: &RouteRequest<'_>) -> Result<()> {
        self.store(request).map(|_| ())
    }

    async fn resolve(&self, request: &RouteRequest<'_>) -> Result<Arc<dyn BackendStore>> {
        self.store(request).cloned()
    }
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
            })
            .await
    }
}

fn validated_endpoint(endpoint: &str) -> std::result::Result<(String, bool), String> {
    let invalid = || {
        "bucket endpoint must be an HTTP(S) URL or host without userinfo, query, fragment or whitespace"
            .to_string()
    };
    if endpoint.is_empty()
        || endpoint
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control() || byte == b'\\')
    {
        return Err(invalid());
    }
    let full = if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("https://{endpoint}")
    };
    let url = url::Url::parse(&full).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(endpoint_host(url.as_str()))
}

#[async_trait]
impl BackendStore for BackendRouter {
    fn validate_bucket(&self, backend: Backend, bucket: &str) -> Result<()> {
        if backend != self.backend {
            return Err(Error::Backend(
                "backend is not configured in this router".into(),
            ));
        }
        self.resolver.validate(&RouteRequest { backend, bucket })
    }

    fn validate_object(&self, object: &ObjectId) -> Result<()> {
        self.validate_bucket(object.backend, &object.bucket)?;
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
            })
            .await?
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
