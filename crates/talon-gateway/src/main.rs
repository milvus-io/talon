//! Environment-configured Talon object-store gateway process.

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use talon_backend::{
    resolve_azure_bearer, resolve_s3_credentials, AzureBackend, AzureConfig, CredentialsObserver,
    ProvideBearerToken, ProvideS3Credentials, ReqwestClient, S3Backend, S3Config, S3Credentials,
};
use talon_cache_client::{BlockReader, CoordinatorClient, PlacementCache};
use talon_gateway::azure::{AzureAdapterConfig, AzureBlobAdapter, AzureCache};
use talon_gateway::azure_auth::{AzureClientIdentity, AzureStorageAuthenticator};
use talon_gateway::s3::{S3Adapter, S3AdapterConfig, S3Cache};
use talon_gateway::s3_auth::{S3ClientIdentity, S3SigV4Authenticator};
use talon_gateway::{
    serve, serve_tls, AuthenticatedPrincipal, AuthorizationGrant, AuthorizationPolicy,
    GatewayAdapter, GatewayClientAuthConfig, GatewayClientAuthMode, GatewayConfig, GatewayMetrics,
    GatewayMode, GatewayMtlsIdentity, GatewayOperation, GatewayRoute, GatewayRuntime,
    GatewaySecurity, GatewayTlsConfig, OriginAuthMode, ProviderProtocol,
};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

type MainResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Settings {
    protocol: String,
    bind: SocketAddr,
    mode: GatewayMode,
    origin_auth: OriginAuthMode,
    coordinator: String,
    block_size: u32,
    transfer_chunk_bytes: u32,
    placement_ttl_ms: u64,
    replicas: u8,
    max_body_bytes: usize,
    request_deadline_ms: u64,
    route: GatewayRoute,
    incoming_path_style: bool,
    endpoint_suffix: Option<String>,
    azure_account: Option<String>,
    azure_endpoint: Option<String>,
    azure_shared_key: Option<String>,
    azure_sas: Option<String>,
    azure_client_identities_path: Option<PathBuf>,
    azure_max_clock_skew_ms: u64,
    azure_max_block_bindings: usize,
    azure_block_binding_ttl_ms: u64,
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    s3_session_token: Option<String>,
    s3_client_identities_path: Option<PathBuf>,
    s3_origin_path_style: bool,
    s3_max_clock_skew_ms: u64,
    s3_max_multipart_uploads: usize,
    s3_multipart_ttl_ms: u64,
    authorization_path: Option<PathBuf>,
    auth_reload_ms: u64,
    origin_credentials_source: Option<String>,
    tls: Option<GatewayTlsConfig>,
}

impl Settings {
    fn from_env() -> MainResult<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> MainResult<Self> {
        let value = |get: &mut dyn FnMut(&str) -> Option<String>, name: &str| {
            get(name).filter(|value| !value.is_empty())
        };
        let protocol = value(&mut get, "TALON_GATEWAY_PROTOCOL")
            .unwrap_or_else(|| "s3".to_string())
            .to_ascii_lowercase();
        if !matches!(protocol.as_str(), "s3" | "azure") {
            return Err(invalid("TALON_GATEWAY_PROTOCOL must be s3 or azure"));
        }
        let bind: SocketAddr = parse_or(
            value(&mut get, "TALON_GATEWAY_BIND"),
            "127.0.0.1:8080",
            "TALON_GATEWAY_BIND",
        )?;
        let mode = match value(&mut get, "TALON_GATEWAY_MODE").as_deref() {
            None | Some("development") => GatewayMode::Development,
            Some("production") => GatewayMode::Production,
            Some(_) => {
                return Err(invalid(
                    "TALON_GATEWAY_MODE must be development or production",
                ))
            }
        };
        let origin_auth = match value(&mut get, "TALON_GATEWAY_ORIGIN_AUTH").as_deref() {
            None | Some("service") => OriginAuthMode::Service,
            Some("trusted-passthrough") => OriginAuthMode::TrustedPassthrough,
            Some(_) => {
                return Err(invalid(
                    "TALON_GATEWAY_ORIGIN_AUTH must be service or trusted-passthrough",
                ))
            }
        };
        if origin_auth == OriginAuthMode::TrustedPassthrough {
            if mode != GatewayMode::Development {
                return Err(invalid(
                    "trusted-passthrough origin auth requires TALON_GATEWAY_MODE=development",
                ));
            }
            if !bind.ip().is_loopback() {
                return Err(invalid(
                    "trusted-passthrough origin auth requires a loopback bind address",
                ));
            }
        }
        let coordinator = required(&mut get, "TALON_COORDINATOR_ADDR")?;
        let block_size = parse_or(
            value(&mut get, "TALON_GATEWAY_BLOCK_SIZE"),
            "268435456",
            "TALON_GATEWAY_BLOCK_SIZE",
        )?;
        let transfer_chunk_bytes = parse_or(
            value(&mut get, "TALON_GATEWAY_TRANSFER_CHUNK_BYTES"),
            "1048576",
            "TALON_GATEWAY_TRANSFER_CHUNK_BYTES",
        )?;
        let placement_ttl_ms = parse_or(
            value(&mut get, "TALON_GATEWAY_PLACEMENT_TTL_MS"),
            "5000",
            "TALON_GATEWAY_PLACEMENT_TTL_MS",
        )?;
        let replicas = parse_or(
            value(&mut get, "TALON_GATEWAY_REPLICAS"),
            "1",
            "TALON_GATEWAY_REPLICAS",
        )?;
        let max_body_bytes = parse_or(
            value(&mut get, "TALON_GATEWAY_MAX_BODY_BYTES"),
            "16777216",
            "TALON_GATEWAY_MAX_BODY_BYTES",
        )?;
        let request_deadline_ms = parse_or(
            value(&mut get, "TALON_GATEWAY_REQUEST_DEADLINE_MS"),
            "30000",
            "TALON_GATEWAY_REQUEST_DEADLINE_MS",
        )?;
        let route = match value(&mut get, "TALON_GATEWAY_ROUTE").as_deref() {
            None | Some("cache") => GatewayRoute::Cache,
            Some("cache-only") => GatewayRoute::CacheOnly,
            Some("origin") => GatewayRoute::Origin,
            Some(_) => {
                return Err(invalid(
                    "TALON_GATEWAY_ROUTE must be cache, cache-only, or origin",
                ))
            }
        };
        let incoming_path_style = parse_bool(
            value(&mut get, "TALON_GATEWAY_PATH_STYLE").as_deref(),
            false,
            "TALON_GATEWAY_PATH_STYLE",
        )?;
        let auth_reload_ms: u64 = parse_or(
            value(&mut get, "TALON_GATEWAY_AUTH_RELOAD_MS"),
            "5000",
            "TALON_GATEWAY_AUTH_RELOAD_MS",
        )?;
        if auth_reload_ms == 0 {
            return Err(invalid(
                "TALON_GATEWAY_AUTH_RELOAD_MS must be greater than zero",
            ));
        }
        let tls_certificate = value(&mut get, "TALON_GATEWAY_TLS_CERT_PATH");
        let tls_private_key = value(&mut get, "TALON_GATEWAY_TLS_KEY_PATH");
        let client_auth_mode =
            value(&mut get, "TALON_GATEWAY_TLS_CLIENT_AUTH").unwrap_or_else(|| "off".into());
        let client_ca = value(&mut get, "TALON_GATEWAY_TLS_CLIENT_CA_PATH");
        let trust_domain = value(&mut get, "TALON_GATEWAY_TLS_TRUST_DOMAIN");
        let mtls_identities = value(&mut get, "TALON_GATEWAY_MTLS_IDENTITIES_PATH");
        let client_auth = match client_auth_mode.as_str() {
            "off" if client_ca.is_none() && trust_domain.is_none() && mtls_identities.is_none() => {
                None
            }
            "off" => return Err(invalid(
                "mTLS client settings require TALON_GATEWAY_TLS_CLIENT_AUTH=optional or required",
            )),
            "optional" | "required" => {
                let mode = if client_auth_mode == "optional" {
                    GatewayClientAuthMode::Optional
                } else {
                    GatewayClientAuthMode::Required
                };
                let ca = client_ca.ok_or_else(|| {
                    invalid("TALON_GATEWAY_TLS_CLIENT_CA_PATH is required for mTLS")
                })?;
                let trust_domain = trust_domain.ok_or_else(|| {
                    invalid("TALON_GATEWAY_TLS_TRUST_DOMAIN is required for mTLS")
                })?;
                let identities = mtls_identities.ok_or_else(|| {
                    invalid("TALON_GATEWAY_MTLS_IDENTITIES_PATH is required for mTLS")
                })?;
                Some(load_mtls_identities(
                    Path::new(&identities),
                    PathBuf::from(ca),
                    mode,
                    trust_domain,
                )?)
            }
            _ => {
                return Err(invalid(
                    "TALON_GATEWAY_TLS_CLIENT_AUTH must be off, optional, or required",
                ))
            }
        };
        let tls = match (tls_certificate, tls_private_key) {
            (None, None) if client_auth.is_none() => None,
            (None, None) => return Err(invalid("mTLS requires gateway TLS certificate and key")),
            (Some(certificate_path), Some(private_key_path)) => Some(GatewayTlsConfig {
                certificate_path: certificate_path.into(),
                private_key_path: private_key_path.into(),
                reload_interval: std::time::Duration::from_millis(parse_or(
                    value(&mut get, "TALON_GATEWAY_TLS_RELOAD_MS"),
                    "5000",
                    "TALON_GATEWAY_TLS_RELOAD_MS",
                )?),
                handshake_timeout: std::time::Duration::from_millis(parse_or(
                    value(&mut get, "TALON_GATEWAY_TLS_HANDSHAKE_TIMEOUT_MS"),
                    "10000",
                    "TALON_GATEWAY_TLS_HANDSHAKE_TIMEOUT_MS",
                )?),
                max_concurrent_handshakes: parse_or(
                    value(&mut get, "TALON_GATEWAY_TLS_MAX_HANDSHAKES"),
                    "256",
                    "TALON_GATEWAY_TLS_MAX_HANDSHAKES",
                )?,
                client_auth,
            }),
            _ => return Err(invalid(
                "TALON_GATEWAY_TLS_CERT_PATH and TALON_GATEWAY_TLS_KEY_PATH must be set together",
            )),
        };

        let settings = Self {
            protocol,
            bind,
            mode,
            origin_auth,
            coordinator,
            block_size,
            transfer_chunk_bytes,
            placement_ttl_ms,
            replicas,
            max_body_bytes,
            request_deadline_ms,
            route,
            incoming_path_style,
            endpoint_suffix: value(&mut get, "TALON_GATEWAY_ENDPOINT_SUFFIX"),
            azure_account: value(&mut get, "TALON_GATEWAY_AZURE_ACCOUNT"),
            azure_endpoint: value(&mut get, "TALON_GATEWAY_AZURE_ENDPOINT"),
            azure_shared_key: value(&mut get, "TALON_GATEWAY_AZURE_SHARED_KEY"),
            azure_sas: value(&mut get, "TALON_GATEWAY_AZURE_SAS"),
            azure_client_identities_path: value(
                &mut get,
                "TALON_GATEWAY_AZURE_CLIENT_IDENTITIES_PATH",
            )
            .map(PathBuf::from),
            azure_max_clock_skew_ms: parse_or(
                value(&mut get, "TALON_GATEWAY_AZURE_MAX_CLOCK_SKEW_MS"),
                "900000",
                "TALON_GATEWAY_AZURE_MAX_CLOCK_SKEW_MS",
            )?,
            azure_max_block_bindings: parse_or(
                value(&mut get, "TALON_GATEWAY_AZURE_MAX_BLOCK_BINDINGS"),
                "1024",
                "TALON_GATEWAY_AZURE_MAX_BLOCK_BINDINGS",
            )?,
            azure_block_binding_ttl_ms: parse_or(
                value(&mut get, "TALON_GATEWAY_AZURE_BLOCK_BINDING_TTL_MS"),
                "86400000",
                "TALON_GATEWAY_AZURE_BLOCK_BINDING_TTL_MS",
            )?,
            s3_region: value(&mut get, "TALON_GATEWAY_S3_REGION"),
            s3_endpoint: value(&mut get, "TALON_GATEWAY_S3_ENDPOINT"),
            s3_access_key: value(&mut get, "AWS_ACCESS_KEY_ID"),
            s3_secret_key: value(&mut get, "AWS_SECRET_ACCESS_KEY"),
            s3_session_token: value(&mut get, "AWS_SESSION_TOKEN"),
            s3_client_identities_path: value(&mut get, "TALON_GATEWAY_S3_CLIENT_IDENTITIES_PATH")
                .map(PathBuf::from),
            // OSS and COS S3-compatible endpoints require virtual-host
            // addressing; MinIO/Ceph-style stores require path style.
            s3_origin_path_style: parse_bool(
                value(&mut get, "TALON_GATEWAY_S3_ORIGIN_PATH_STYLE").as_deref(),
                true,
                "TALON_GATEWAY_S3_ORIGIN_PATH_STYLE",
            )?,
            s3_max_clock_skew_ms: parse_or(
                value(&mut get, "TALON_GATEWAY_S3_MAX_CLOCK_SKEW_MS"),
                "900000",
                "TALON_GATEWAY_S3_MAX_CLOCK_SKEW_MS",
            )?,
            s3_max_multipart_uploads: parse_or(
                value(&mut get, "TALON_GATEWAY_S3_MAX_MULTIPART_UPLOADS"),
                "1024",
                "TALON_GATEWAY_S3_MAX_MULTIPART_UPLOADS",
            )?,
            s3_multipart_ttl_ms: parse_or(
                value(&mut get, "TALON_GATEWAY_S3_MULTIPART_TTL_MS"),
                "86400000",
                "TALON_GATEWAY_S3_MULTIPART_TTL_MS",
            )?,
            authorization_path: value(&mut get, "TALON_GATEWAY_AUTHORIZATION_PATH")
                .map(PathBuf::from),
            auth_reload_ms,
            origin_credentials_source: value(&mut get, "TALON_ORIGIN_CREDENTIALS_SOURCE"),
            tls,
        };
        settings.validate_origin_auth()?;
        Ok(settings)
    }

    fn validate_origin_auth(&self) -> MainResult<()> {
        if self.origin_auth != OriginAuthMode::TrustedPassthrough {
            return Ok(());
        }
        if self.s3_client_identities_path.is_some()
            || self.azure_client_identities_path.is_some()
            || self.authorization_path.is_some()
        {
            return Err(invalid(
                "trusted-passthrough origin auth cannot be combined with gateway client identity or authorization policy configuration",
            ));
        }
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(Error::new(ErrorKind::InvalidInput, message.into()))
}

fn required(get: &mut dyn FnMut(&str) -> Option<String>, name: &str) -> MainResult<String> {
    get(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid(format!("{name} is required")))
}

fn parse_or<T: std::str::FromStr>(
    value: Option<String>,
    default: &str,
    name: &str,
) -> MainResult<T> {
    value
        .as_deref()
        .unwrap_or(default)
        .parse()
        .map_err(|_| invalid(format!("{name} is invalid")))
}

fn parse_bool(value: Option<&str>, default: bool, name: &str) -> MainResult<bool> {
    match value {
        None => Ok(default),
        Some("1" | "true" | "yes") => Ok(true),
        Some("0" | "false" | "no") => Ok(false),
        Some(_) => Err(invalid(format!("{name} must be true or false"))),
    }
}

fn split_endpoint(endpoint: &str) -> (String, bool) {
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

fn cache_reader(settings: &Settings) -> Arc<BlockReader> {
    Arc::new(BlockReader::new(
        CoordinatorClient::new(&settings.coordinator),
        Arc::new(PlacementCache::new(settings.placement_ttl_ms)),
        settings.replicas,
    ))
}

/// The origin credential a gateway Azure adapter signs with.
enum AzureOriginAuth {
    /// Base64 account key (`Authorization: SharedKey`).
    SharedKey(String),
    /// SAS token appended to every URL.
    Sas(String),
    /// Azure AD bearer tokens from AKS workload identity.
    Bearer(Arc<dyn ProvideBearerToken>),
}

/// The statically configured Azure credential, `None` when the environment
/// configures neither form and workload identity should be resolved.
fn azure_static_auth(settings: &Settings) -> MainResult<Option<AzureOriginAuth>> {
    match (&settings.azure_shared_key, &settings.azure_sas) {
        (Some(_), Some(_)) => Err(invalid(
            "set only one of TALON_GATEWAY_AZURE_SHARED_KEY or TALON_GATEWAY_AZURE_SAS",
        )),
        (Some(key), None) => Ok(Some(AzureOriginAuth::SharedKey(key.clone()))),
        (None, Some(sas)) => Ok(Some(AzureOriginAuth::Sas(
            sas.trim_start_matches('?').to_string(),
        ))),
        (None, None) => Ok(None),
    }
}

fn azure_adapter(
    settings: &Settings,
    auth: AzureOriginAuth,
) -> MainResult<Arc<dyn GatewayAdapter>> {
    if settings.origin_auth == OriginAuthMode::TrustedPassthrough {
        return Err(invalid(
            "trusted-passthrough Azure routing is not available in this build",
        ));
    }
    let account = settings
        .azure_account
        .clone()
        .ok_or_else(|| invalid("TALON_GATEWAY_AZURE_ACCOUNT is required"))?;
    let mut origin_config = match settings.azure_endpoint.as_deref() {
        Some(endpoint) => {
            let (host, tls) = split_endpoint(endpoint);
            AzureConfig::emulator(&account, host, tls)
        }
        None => AzureConfig::new(&account),
    };
    if settings.azure_endpoint.is_none() {
        if let Some(suffix) = &settings.endpoint_suffix {
            origin_config.endpoint_suffix.clone_from(suffix);
        }
    }
    let http = Arc::new(ReqwestClient::new());
    let origin = match auth {
        AzureOriginAuth::SharedKey(key) => {
            Arc::new(AzureBackend::with_shared_key(origin_config, key, http))
        }
        AzureOriginAuth::Sas(sas) => Arc::new(AzureBackend::new(origin_config, Some(sas), http)),
        AzureOriginAuth::Bearer(bearer) => Arc::new(AzureBackend::with_bearer_provider(
            origin_config,
            bearer,
            http,
        )),
    };
    let mut config = if settings.incoming_path_style {
        AzureAdapterConfig::path_style(&account)
    } else {
        AzureAdapterConfig::public_cloud(&account)
    };
    if let Some(suffix) = &settings.endpoint_suffix {
        config.endpoint_suffix.clone_from(suffix);
    }
    config.block_size = settings.block_size;
    config.transfer_chunk_bytes = settings.transfer_chunk_bytes;
    config.default_route = settings.route;
    config.max_block_bindings = settings.azure_max_block_bindings;
    config.block_binding_ttl =
        std::time::Duration::from_millis(settings.azure_block_binding_ttl_ms);
    let cache = cache_reader(settings);
    Ok(Arc::new(AzureBlobAdapter::new(
        config,
        cache as Arc<dyn AzureCache>,
        origin,
    )?))
}

/// The statically configured S3 origin keys, `None` when the environment
/// sets none and workload identity should be resolved.
fn static_s3_credentials(settings: &Settings) -> MainResult<Option<S3Credentials>> {
    match (&settings.s3_access_key, &settings.s3_secret_key) {
        (Some(access_key), Some(secret_key)) => Ok(Some(S3Credentials {
            access_key_id: access_key.clone(),
            secret_access_key: secret_key.clone(),
            session_token: settings.s3_session_token.clone(),
        })),
        (None, None) => Ok(None),
        _ => Err(invalid(
            "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together",
        )),
    }
}

fn s3_adapter(
    settings: &Settings,
    credentials: Arc<dyn ProvideS3Credentials>,
) -> MainResult<Arc<dyn GatewayAdapter>> {
    if settings.origin_auth == OriginAuthMode::TrustedPassthrough {
        return Err(invalid(
            "trusted-passthrough S3 routing is not available in this build",
        ));
    }
    let region = settings
        .s3_region
        .clone()
        .unwrap_or_else(|| "us-east-1".to_string());
    let origin_config = match settings.s3_endpoint.as_deref() {
        Some(endpoint) => {
            let (host, tls) = split_endpoint(endpoint);
            S3Config {
                region: region.clone(),
                endpoint: host,
                path_style: settings.s3_origin_path_style,
                tls,
            }
        }
        None => S3Config::aws(&region),
    };
    let origin = Arc::new(S3Backend::with_credentials_provider(
        origin_config,
        credentials,
        Arc::new(ReqwestClient::new()),
    ));
    let mut config = if settings.incoming_path_style {
        S3AdapterConfig::path_style(
            settings
                .endpoint_suffix
                .clone()
                .unwrap_or_else(|| "localhost".to_string()),
        )
    } else {
        S3AdapterConfig::aws(&region)
    };
    if let Some(suffix) = &settings.endpoint_suffix {
        config.endpoint_suffix.clone_from(suffix);
    }
    config.region.clone_from(&region);
    config.block_size = settings.block_size;
    config.transfer_chunk_bytes = settings.transfer_chunk_bytes;
    config.default_route = settings.route;
    config.max_multipart_uploads = settings.s3_max_multipart_uploads;
    config.multipart_state_ttl = std::time::Duration::from_millis(settings.s3_multipart_ttl_ms);
    let cache = cache_reader(settings);
    Ok(Arc::new(S3Adapter::new(
        config,
        cache as Arc<dyn S3Cache>,
        origin,
    )?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct S3IdentityFile {
    identities: Vec<S3IdentityConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct S3IdentityConfig {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    principal: String,
    provider_account: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AzureIdentityFile {
    identities: Vec<AzureIdentityConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AzureIdentityConfig {
    account_key: String,
    principal: String,
    provider_account: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MtlsIdentityFile {
    identities: Vec<MtlsIdentityConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MtlsIdentityConfig {
    uri_san: String,
    principal: String,
    provider_account: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationFile {
    grants: Vec<AuthorizationGrantConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationGrantConfig {
    id: String,
    principal: String,
    protocol: String,
    provider_account: String,
    namespace: String,
    prefix: Option<String>,
    operations: Vec<String>,
}

/// Parse one auth configuration file, replacing serde's error — which can
/// echo file contents, and identity files hold secrets — with a stable
/// location-only message safe for logs and startup stderr.
fn parse_json<T: serde::de::DeserializeOwned>(bytes: &[u8], file: &'static str) -> MainResult<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        invalid(format!(
            "{file} is invalid at line {} column {}",
            error.line(),
            error.column()
        ))
    })
}

fn build_s3_authenticator(
    bytes: &[u8],
    region: &str,
    max_clock_skew_ms: u64,
) -> MainResult<S3SigV4Authenticator> {
    let configured: S3IdentityFile = parse_json(bytes, "gateway S3 client identity file")?;
    let identities = configured
        .identities
        .into_iter()
        .map(|identity| S3ClientIdentity {
            access_key_id: identity.access_key_id,
            secret_access_key: identity.secret_access_key,
            session_token: identity.session_token,
            principal: AuthenticatedPrincipal::new(identity.principal, identity.provider_account),
        })
        .collect();
    Ok(S3SigV4Authenticator::new(
        region,
        identities,
        std::time::Duration::from_millis(max_clock_skew_ms),
    )?)
}

fn build_azure_authenticator(
    bytes: &[u8],
    account: &str,
    max_clock_skew_ms: u64,
    transport_https: bool,
) -> MainResult<AzureStorageAuthenticator> {
    let configured: AzureIdentityFile = parse_json(bytes, "gateway Azure client identity file")?;
    let identities = configured
        .identities
        .into_iter()
        .map(|identity| AzureClientIdentity {
            account_key: identity.account_key,
            principal: AuthenticatedPrincipal::new(identity.principal, identity.provider_account),
        })
        .collect();
    Ok(AzureStorageAuthenticator::new(
        account,
        identities,
        std::time::Duration::from_millis(max_clock_skew_ms),
        transport_https,
    )?)
}

fn build_authorization(bytes: &[u8]) -> MainResult<AuthorizationPolicy> {
    let configured: AuthorizationFile = parse_json(bytes, "gateway authorization file")?;
    let grants = configured
        .grants
        .into_iter()
        .map(|grant| {
            let protocol = match grant.protocol.as_str() {
                "s3" => ProviderProtocol::S3,
                "azure" => ProviderProtocol::Azure,
                _ => return Err(invalid("authorization grant protocol must be s3 or azure")),
            };
            let operations = grant
                .operations
                .into_iter()
                .map(|operation| match operation.as_str() {
                    "stat" => Ok(GatewayOperation::Stat),
                    "probe" => Ok(GatewayOperation::Probe),
                    "read" => Ok(GatewayOperation::Read),
                    "list" => Ok(GatewayOperation::List),
                    "write" => Ok(GatewayOperation::Write),
                    "delete" => Ok(GatewayOperation::Delete),
                    _ => Err(invalid("authorization grant operation is invalid")),
                })
                .collect::<MainResult<Vec<_>>>()?;
            Ok(AuthorizationGrant {
                id: grant.id,
                principal: grant.principal,
                protocol,
                provider_account: grant.provider_account,
                namespace: grant.namespace,
                prefix: grant.prefix,
                operations,
            })
        })
        .collect::<MainResult<Vec<_>>>()?;
    Ok(AuthorizationPolicy::new(grants)?)
}

type ApplyFn = Box<dyn FnMut(&[u8]) -> MainResult<()> + Send>;

/// One polled configuration file and the bytes behind the active state.
/// `file` is the bounded `file` label on the reload metric.
struct ReloadTarget {
    file: &'static str,
    path: PathBuf,
    last_applied: Vec<u8>,
    apply: ApplyFn,
}

impl ReloadTarget {
    /// Re-read the file and atomically swap the running configuration when
    /// its bytes changed. Unreadable or invalid contents retain the active
    /// configuration and keep counting as failures until the file is fixed.
    fn poll(&mut self, metrics: &GatewayMetrics) {
        let outcome = match std::fs::read(&self.path) {
            Ok(bytes) if bytes == self.last_applied => Ok("unchanged"),
            Ok(bytes) => (self.apply)(&bytes).map(|()| {
                self.last_applied = bytes;
                tracing::info!(file = self.file, "gateway auth configuration reloaded");
                "success"
            }),
            Err(error) => Err(error.into()),
        };
        let result = match outcome {
            Ok(result) => result,
            Err(error) => {
                // Safe to include: `parse_json` strips serde's content echoes
                // and the constructor errors are stable, so no file contents
                // (secrets) can reach this line.
                tracing::warn!(
                    file = self.file,
                    %error,
                    "gateway auth reload failed; retaining last valid configuration"
                );
                "failure"
            }
        };
        metrics.record_auth_reload(self.file, result);
    }
}

/// Install the configured identity and authorization files and return one
/// reload target per file so updates apply without a restart.
fn configure_auth(
    runtime: &Arc<GatewayRuntime>,
    settings: &Settings,
) -> MainResult<Vec<ReloadTarget>> {
    let mut targets = Vec::new();
    if let Some(path) = &settings.s3_client_identities_path {
        if settings.protocol != "s3" {
            return Err(invalid(
                "TALON_GATEWAY_S3_CLIENT_IDENTITIES_PATH requires the s3 protocol",
            ));
        }
        let region = settings
            .s3_region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string());
        let max_clock_skew_ms = settings.s3_max_clock_skew_ms;
        let bytes = std::fs::read(path)?;
        runtime.install_authentication(Arc::new(build_s3_authenticator(
            &bytes,
            &region,
            max_clock_skew_ms,
        )?));
        let reload_runtime = Arc::clone(runtime);
        targets.push(ReloadTarget {
            file: "s3_identities",
            path: path.clone(),
            last_applied: bytes,
            apply: Box::new(move |bytes| {
                let authenticator = build_s3_authenticator(bytes, &region, max_clock_skew_ms)?;
                reload_runtime.install_authentication(Arc::new(authenticator));
                Ok(())
            }),
        });
    }
    if let Some(path) = &settings.azure_client_identities_path {
        if settings.protocol != "azure" {
            return Err(invalid(
                "TALON_GATEWAY_AZURE_CLIENT_IDENTITIES_PATH requires the azure protocol",
            ));
        }
        let account = settings
            .azure_account
            .clone()
            .ok_or_else(|| invalid("TALON_GATEWAY_AZURE_ACCOUNT is required"))?;
        let max_clock_skew_ms = settings.azure_max_clock_skew_ms;
        let transport_https = settings.tls.is_some();
        let bytes = std::fs::read(path)?;
        runtime.install_authentication(Arc::new(build_azure_authenticator(
            &bytes,
            &account,
            max_clock_skew_ms,
            transport_https,
        )?));
        let reload_runtime = Arc::clone(runtime);
        targets.push(ReloadTarget {
            file: "azure_identities",
            path: path.clone(),
            last_applied: bytes,
            apply: Box::new(move |bytes| {
                let authenticator =
                    build_azure_authenticator(bytes, &account, max_clock_skew_ms, transport_https)?;
                reload_runtime.install_authentication(Arc::new(authenticator));
                Ok(())
            }),
        });
    }
    if let Some(path) = &settings.authorization_path {
        let bytes = std::fs::read(path)?;
        runtime.install_authorization(build_authorization(&bytes)?);
        let reload_runtime = Arc::clone(runtime);
        targets.push(ReloadTarget {
            file: "authorization",
            path: path.clone(),
            last_applied: bytes,
            apply: Box::new(move |bytes| {
                reload_runtime.install_authorization(build_authorization(bytes)?);
                Ok(())
            }),
        });
    }
    Ok(targets)
}

/// Poll every reload target on one shared interval for the process lifetime.
fn spawn_auth_reload(
    interval: std::time::Duration,
    metrics: GatewayMetrics,
    mut targets: Vec<ReloadTarget>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Initial configuration was installed synchronously.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            for target in &mut targets {
                target.poll(&metrics);
            }
        }
    });
}

/// Bridges origin credential refresh outcomes into the gateway registry.
struct OriginCredentialsObserver(GatewayMetrics);

impl CredentialsObserver for OriginCredentialsObserver {
    fn refresh_succeeded(&self, expires_at: Option<std::time::SystemTime>) {
        self.0.record_origin_credentials("refresh_success");
        let unix_seconds = expires_at
            .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0.0, |since_epoch| since_epoch.as_secs_f64());
        self.0.set_origin_credentials_expiry(unix_seconds);
    }

    fn refresh_failed(&self) {
        self.0.record_origin_credentials("refresh_failure");
    }
}

fn load_mtls_identities(
    path: &Path,
    ca_certificate_path: PathBuf,
    mode: GatewayClientAuthMode,
    trust_domain: String,
) -> MainResult<GatewayClientAuthConfig> {
    let configured: MtlsIdentityFile = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    Ok(GatewayClientAuthConfig {
        ca_certificate_path,
        mode,
        trust_domain,
        identities: configured
            .identities
            .into_iter()
            .map(|identity| GatewayMtlsIdentity {
                uri_san: identity.uri_san,
                principal: AuthenticatedPrincipal::new(
                    identity.principal,
                    identity.provider_account,
                ),
            })
            .collect(),
    })
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> MainResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let settings = Settings::from_env()?;
    let metrics = GatewayMetrics::new();
    let credentials_observer: Arc<dyn CredentialsObserver> =
        Arc::new(OriginCredentialsObserver(metrics.clone()));
    let exchange_http: Arc<dyn talon_backend::HttpClient> = Arc::new(ReqwestClient::new());
    let adapter = match settings.protocol.as_str() {
        "azure" => {
            if settings.origin_auth == OriginAuthMode::TrustedPassthrough {
                return Err(invalid(
                    "trusted-passthrough Azure routing is not available in this build",
                ));
            }
            let auth = match azure_static_auth(&settings)? {
                Some(auth) => {
                    info!(source = "static", "origin Azure credentials resolved");
                    auth
                }
                None => match settings.origin_credentials_source.as_deref() {
                    None | Some("auto") | Some("azure-workload-identity") => {
                        let resolved = resolve_azure_bearer(
                            Arc::clone(&exchange_http),
                            Arc::clone(&credentials_observer),
                        )
                        .await
                        .map_err(|error| {
                            invalid(format!(
                                "an Azure shared key or SAS credential is required, \
                                 or AKS workload identity must be available: {error}"
                            ))
                        })?;
                        info!(
                            source = resolved.source,
                            "origin Azure credentials resolved"
                        );
                        AzureOriginAuth::Bearer(resolved.provider)
                    }
                    Some(other) => {
                        return Err(invalid(format!(
                            "an Azure shared key or SAS credential is required \
                             (origin credentials source {other:?} does not apply to azure)"
                        )))
                    }
                },
            };
            azure_adapter(&settings, auth)?
        }
        "s3" => {
            if settings.origin_auth == OriginAuthMode::TrustedPassthrough {
                return Err(invalid(
                    "trusted-passthrough S3 routing is not available in this build",
                ));
            }
            let resolved = resolve_s3_credentials(
                static_s3_credentials(&settings)?,
                settings.origin_credentials_source.as_deref(),
                Arc::clone(&exchange_http),
                Arc::clone(&credentials_observer),
            )
            .await
            .map_err(invalid)?;
            info!(source = resolved.source, "origin S3 credentials resolved");
            s3_adapter(&settings, resolved.provider)?
        }
        _ => unreachable!("validated protocol"),
    };
    let runtime = Arc::new(GatewayRuntime::new_with_metrics(
        GatewayConfig {
            bind: settings.bind,
            mode: settings.mode,
            origin_auth: settings.origin_auth,
            max_body_bytes: settings.max_body_bytes,
            request_deadline: std::time::Duration::from_millis(settings.request_deadline_ms),
            ..GatewayConfig::default()
        },
        adapter,
        GatewaySecurity::default(),
        metrics,
    )?);
    let reload_targets = configure_auth(&runtime, &settings)?;
    if !reload_targets.is_empty() {
        spawn_auth_reload(
            std::time::Duration::from_millis(settings.auth_reload_ms),
            runtime.metrics().clone(),
            reload_targets,
        );
    }
    let listener = TcpListener::bind(settings.bind).await?;
    info!(
        bind = %settings.bind,
        protocol = %settings.protocol,
        mode = ?settings.mode,
        route = ?settings.route,
        "starting object-store gateway"
    );
    match settings.tls {
        Some(tls) => serve_tls(listener, runtime, tls, shutdown_signal()).await?,
        None => serve(listener, runtime, shutdown_signal()).await?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::extract::Request;
    use reqwest::StatusCode;
    use talon_backend::sigv4::sign_request;
    use talon_backend::{AmzDate, HttpRequest, Method};
    use talon_core::{Backend, ObjectId};
    use talon_gateway::{GatewayAccess, GatewayRequestContext, GatewayResponse, GatewayTarget};
    use tempfile::TempDir;

    fn settings(values: &[(&str, &str)]) -> MainResult<Settings> {
        let values = values
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>();
        Settings::from_lookup(|name| values.get(name).cloned())
    }

    #[test]
    fn defaults_are_loopback_bounded_and_cache_routed() {
        let settings = settings(&[("TALON_COORDINATOR_ADDR", "coordinator:7411")]).unwrap();
        assert_eq!(settings.protocol, "s3");
        assert!(settings.bind.ip().is_loopback());
        assert_eq!(settings.mode, GatewayMode::Development);
        assert_eq!(settings.origin_auth, OriginAuthMode::Service);
        assert_eq!(settings.route, GatewayRoute::Cache);
        assert!(!settings.incoming_path_style);
        assert!(settings.tls.is_none());
        assert!(settings.s3_client_identities_path.is_none());
        assert!(settings.s3_origin_path_style);
        assert_eq!(settings.s3_max_clock_skew_ms, 900_000);
        assert_eq!(settings.s3_max_multipart_uploads, 1024);
        assert_eq!(settings.s3_multipart_ttl_ms, 86_400_000);
        assert!(settings.authorization_path.is_none());
        assert_eq!(settings.auth_reload_ms, 5000);
        assert!(settings.azure_client_identities_path.is_none());
        assert_eq!(settings.azure_max_clock_skew_ms, 900_000);
        assert_eq!(settings.azure_max_block_bindings, 1024);
        assert_eq!(settings.azure_block_binding_ttl_ms, 86_400_000);
        assert_eq!(settings.max_body_bytes, 16 * 1024 * 1024);
        assert_eq!(settings.request_deadline_ms, 30_000);
    }

    #[test]
    fn body_and_deadline_limits_are_configurable() {
        let configured = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_MAX_BODY_BYTES", "536870912"),
            ("TALON_GATEWAY_REQUEST_DEADLINE_MS", "300000"),
        ])
        .unwrap();
        assert_eq!(configured.max_body_bytes, 512 * 1024 * 1024);
        assert_eq!(configured.request_deadline_ms, 300_000);
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_MAX_BODY_BYTES", "not-a-number"),
        ])
        .is_err());
    }

    #[test]
    fn rejects_bad_modes_protocols_and_booleans() {
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "gcs"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_ORIGIN_AUTH", "forward"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_MODE", "unsafe"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PATH_STYLE", "maybe"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_S3_ORIGIN_PATH_STYLE", "maybe"),
        ])
        .is_err());
    }

    #[test]
    fn trusted_passthrough_configuration_is_bounded_and_fail_closed() {
        let trusted = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_ORIGIN_AUTH", "trusted-passthrough"),
        ])
        .unwrap();
        assert_eq!(trusted.origin_auth, OriginAuthMode::TrustedPassthrough);
        assert!(s3_adapter(&trusted, test_s3_provider()).is_err());

        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_ORIGIN_AUTH", "trusted-passthrough"),
            ("TALON_GATEWAY_MODE", "production"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_ORIGIN_AUTH", "trusted-passthrough"),
            ("TALON_GATEWAY_BIND", "0.0.0.0:8080"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_ORIGIN_AUTH", "trusted-passthrough"),
            ("TALON_GATEWAY_AUTHORIZATION_PATH", "/policy.json"),
        ])
        .is_err());
    }

    #[test]
    fn endpoint_scheme_selects_transport_without_retaining_the_scheme() {
        assert_eq!(
            split_endpoint("http://minio:9000"),
            ("minio:9000".into(), false)
        );
        assert_eq!(
            split_endpoint("https://blob.example"),
            ("blob.example".into(), true)
        );
        assert_eq!(split_endpoint("s3.example"), ("s3.example".into(), true));
    }

    fn test_s3_provider() -> Arc<dyn ProvideS3Credentials> {
        Arc::new(talon_backend::StaticS3Credentials::new(S3Credentials {
            access_key_id: "AKIDTEST".into(),
            secret_access_key: "secret".into(),
            session_token: None,
        }))
    }

    #[test]
    fn provider_credentials_are_required_and_mutually_exclusive() {
        // No static keys means workload identity must resolve at startup.
        let s3 = settings(&[("TALON_COORDINATOR_ADDR", "coordinator:7411")]).unwrap();
        assert!(static_s3_credentials(&s3).unwrap().is_none());
        // Half a static key pair is a configuration error, not auto-detection.
        let half = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("AWS_ACCESS_KEY_ID", "AKIDONLY"),
        ])
        .unwrap();
        assert!(static_s3_credentials(&half).is_err());

        let azure = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "azure"),
            ("TALON_GATEWAY_AZURE_ACCOUNT", "account"),
        ])
        .unwrap();
        assert!(azure_static_auth(&azure).unwrap().is_none());

        let no_account = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "azure"),
        ])
        .unwrap();
        assert!(azure_adapter(&no_account, AzureOriginAuth::Sas("sig=x".into())).is_err());

        let both = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "azure"),
            ("TALON_GATEWAY_AZURE_ACCOUNT", "account"),
            ("TALON_GATEWAY_AZURE_SHARED_KEY", "key"),
            ("TALON_GATEWAY_AZURE_SAS", "sas"),
        ])
        .unwrap();
        assert!(azure_static_auth(&both).is_err());
    }

    #[test]
    fn tls_paths_are_all_or_none_and_bounds_are_parsed() {
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_TLS_CERT_PATH", "/tls/cert.pem"),
        ])
        .is_err());

        let settings = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_TLS_CERT_PATH", "/tls/cert.pem"),
            ("TALON_GATEWAY_TLS_KEY_PATH", "/tls/key.pem"),
            ("TALON_GATEWAY_TLS_RELOAD_MS", "250"),
            ("TALON_GATEWAY_TLS_HANDSHAKE_TIMEOUT_MS", "900"),
            ("TALON_GATEWAY_TLS_MAX_HANDSHAKES", "32"),
        ])
        .unwrap();
        let tls = settings.tls.unwrap();
        assert_eq!(tls.certificate_path, std::path::Path::new("/tls/cert.pem"));
        assert_eq!(tls.reload_interval, std::time::Duration::from_millis(250));
        assert_eq!(tls.handshake_timeout, std::time::Duration::from_millis(900));
        assert_eq!(tls.max_concurrent_handshakes, 32);
        assert!(tls.client_auth.is_none());
        assert!(!format!("{tls:?}").contains("/tls/key.pem"));
    }

    #[test]
    fn mtls_settings_are_grouped_and_load_explicit_identity_mappings() {
        let temp = TempDir::new().unwrap();
        let identities = temp.path().join("mtls-identities.json");
        std::fs::write(
            &identities,
            r#"{
                "identities": [{
                    "uri_san": "spiffe://cluster.example/talon/cluster-a/worker/worker-1",
                    "principal": "worker-reader",
                    "provider_account": "account-a"
                }]
            }"#,
        )
        .unwrap();
        let identity_path = identities.to_str().unwrap();
        let configured = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_TLS_CERT_PATH", "/tls/cert.pem"),
            ("TALON_GATEWAY_TLS_KEY_PATH", "/tls/key.pem"),
            ("TALON_GATEWAY_TLS_CLIENT_AUTH", "required"),
            ("TALON_GATEWAY_TLS_CLIENT_CA_PATH", "/tls/client-ca.pem"),
            ("TALON_GATEWAY_TLS_TRUST_DOMAIN", "cluster.example"),
            ("TALON_GATEWAY_MTLS_IDENTITIES_PATH", identity_path),
        ])
        .unwrap();
        let client_auth = configured.tls.unwrap().client_auth.unwrap();
        assert_eq!(client_auth.mode, GatewayClientAuthMode::Required);
        assert_eq!(client_auth.trust_domain, "cluster.example");
        assert_eq!(client_auth.identities.len(), 1);
        assert_eq!(client_auth.identities[0].principal.id, "worker-reader");

        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_TLS_CLIENT_AUTH", "optional"),
            ("TALON_GATEWAY_TLS_CLIENT_CA_PATH", "/tls/client-ca.pem"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_TLS_CLIENT_CA_PATH", "/tls/client-ca.pem"),
        ])
        .is_err());
    }

    #[test]
    fn loads_separate_client_identity_and_authorization_files() {
        let temp = TempDir::new().unwrap();
        let identities = temp.path().join("identities.json");
        let azure_identities = temp.path().join("azure-identities.json");
        let authorization = temp.path().join("authorization.json");
        std::fs::write(
            &identities,
            r#"{
                "identities": [{
                    "access_key_id": "client-key",
                    "secret_access_key": "client-secret",
                    "principal": "reader",
                    "provider_account": "tenant-a"
                }]
            }"#,
        )
        .unwrap();
        std::fs::write(
            &azure_identities,
            r#"{
                "identities": [{
                    "account_key": "MDEyMzQ1Njc4OWFiY2RlZg==",
                    "principal": "azure-reader",
                    "provider_account": "account-a"
                }]
            }"#,
        )
        .unwrap();
        std::fs::write(
            &authorization,
            r#"{
                "grants": [{
                    "id": "tenant-read",
                    "principal": "reader",
                    "protocol": "s3",
                    "provider_account": "tenant-a",
                    "namespace": "bucket-a",
                    "prefix": "datasets/",
                    "operations": ["stat", "read", "list"]
                }]
            }"#,
        )
        .unwrap();

        let read = |path: &std::path::Path| std::fs::read(path).unwrap();
        assert!(build_s3_authenticator(&read(&identities), "us-east-1", 900_000).is_ok());
        assert!(
            build_azure_authenticator(&read(&azure_identities), "account-a", 900_000, true).is_ok()
        );
        assert!(build_authorization(&read(&authorization)).is_ok());
    }

    #[test]
    fn invalid_auth_files_never_echo_contents_in_errors() {
        let leaked = br#"{"identities": ["hunter2-super-secret"]}"#;
        // The authenticators deliberately lack Debug, so unwrap the Err side.
        let error = build_s3_authenticator(leaked, "us-east-1", 900_000)
            .err()
            .unwrap()
            .to_string();
        assert!(!error.contains("hunter2-super-secret"));
        assert!(error.contains("line 1"));
        let error = build_azure_authenticator(leaked, "account-a", 900_000, true)
            .err()
            .unwrap()
            .to_string();
        assert!(!error.contains("hunter2-super-secret"));
        let error = build_authorization(br#"{"grants": ["grant-blob"]}"#)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("grant-blob"));
    }

    #[test]
    fn empty_identity_lists_are_rejected_and_empty_grants_deny_all() {
        assert!(build_s3_authenticator(br#"{"identities": []}"#, "us-east-1", 900_000).is_err());
        assert!(
            build_azure_authenticator(br#"{"identities": []}"#, "account-a", 900_000, true)
                .is_err()
        );
        assert!(build_authorization(br#"{"grants": []}"#).is_ok());
    }

    #[test]
    fn auth_reload_interval_is_configurable_and_never_zero() {
        let configured = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_AUTH_RELOAD_MS", "250"),
        ])
        .unwrap();
        assert_eq!(configured.auth_reload_ms, 250);
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_AUTH_RELOAD_MS", "0"),
        ])
        .is_err());
        assert!(settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_AUTH_RELOAD_MS", "soon"),
        ])
        .is_err());
    }

    struct StubAdapter;

    #[async_trait]
    impl GatewayAdapter for StubAdapter {
        fn protocol(&self) -> ProviderProtocol {
            ProviderProtocol::S3
        }

        fn access(
            &self,
            _request: &Request,
            _context: &GatewayRequestContext,
        ) -> Result<GatewayAccess, Box<GatewayResponse>> {
            Ok(GatewayAccess {
                operation: GatewayOperation::Read,
                provider_account: Some("account-a".into()),
                target: GatewayTarget::Object(ObjectId::new(
                    Backend::S3,
                    "bucket-a",
                    "tenant/object",
                )),
                additional: Vec::new(),
            })
        }

        async fn handle(
            &self,
            _request: Request,
            _context: GatewayRequestContext,
        ) -> GatewayResponse {
            GatewayResponse::new(
                axum::response::Response::new(Body::empty()),
                GatewayOperation::Read,
            )
        }
    }

    fn identity_json(access_key: &str) -> String {
        format!(
            r#"{{"identities": [{{
                "access_key_id": "{access_key}",
                "secret_access_key": "secret-of-{access_key}",
                "principal": "reader",
                "provider_account": "account-a"
            }}]}}"#
        )
    }

    const READER_GRANTS: &str = r#"{"grants": [{
        "id": "reader-tenant",
        "principal": "reader",
        "protocol": "s3",
        "provider_account": "account-a",
        "namespace": "bucket-a",
        "prefix": "tenant/",
        "operations": ["read"]
    }]}"#;

    fn dev_runtime(bind: SocketAddr) -> Arc<GatewayRuntime> {
        Arc::new(
            GatewayRuntime::new(
                GatewayConfig {
                    bind,
                    mode: GatewayMode::Development,
                    ..GatewayConfig::default()
                },
                Arc::new(StubAdapter),
                GatewaySecurity::default(),
            )
            .unwrap(),
        )
    }

    /// Gateway served over loopback with reload targets built exactly like
    /// production `main`, plus deterministic manual polling.
    struct LiveGateway {
        temp: TempDir,
        runtime: Arc<GatewayRuntime>,
        targets: Vec<ReloadTarget>,
        address: SocketAddr,
        shutdown: tokio::sync::oneshot::Sender<()>,
        server: tokio::task::JoinHandle<std::io::Result<()>>,
    }

    impl LiveGateway {
        async fn start() -> Self {
            let temp = TempDir::new().unwrap();
            let identities = temp.path().join("identities.json");
            let authorization = temp.path().join("authorization.json");
            std::fs::write(&identities, identity_json("key-one")).unwrap();
            std::fs::write(&authorization, READER_GRANTS).unwrap();
            let configured = settings(&[
                ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
                (
                    "TALON_GATEWAY_S3_CLIENT_IDENTITIES_PATH",
                    identities.to_str().unwrap(),
                ),
                (
                    "TALON_GATEWAY_AUTHORIZATION_PATH",
                    authorization.to_str().unwrap(),
                ),
            ])
            .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let runtime = dev_runtime(address);
            let targets = configure_auth(&runtime, &configured).unwrap();
            let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
            let server_runtime = Arc::clone(&runtime);
            let server = tokio::spawn(async move {
                serve(listener, server_runtime, async move {
                    let _ = shutdown_rx.await;
                })
                .await
            });
            Self {
                temp,
                runtime,
                targets,
                address,
                shutdown,
                server,
            }
        }

        fn identities_path(&self) -> PathBuf {
            self.temp.path().join("identities.json")
        }

        fn authorization_path(&self) -> PathBuf {
            self.temp.path().join("authorization.json")
        }

        fn poll(&mut self) {
            let metrics = self.runtime.metrics().clone();
            for target in &mut self.targets {
                target.poll(&metrics);
            }
        }

        async fn stop(self) {
            self.shutdown.send(()).unwrap();
            self.server.await.unwrap().unwrap();
        }
    }

    async fn signed_status(address: SocketAddr, access_key: &str, secret_key: &str) -> StatusCode {
        let url = format!("http://{address}/tenant/object");
        let mut outgoing = HttpRequest::new(Method::Get, url.clone(), Vec::new());
        sign_request(
            &mut outgoing,
            &S3Credentials {
                access_key_id: access_key.into(),
                secret_access_key: secret_key.into(),
                session_token: None,
            },
            "us-east-1",
            "s3",
            &AmzDate::from_system_time(std::time::SystemTime::now()),
        );
        let mut request = reqwest::Client::new().get(&url);
        for (name, value) in outgoing.headers {
            if name.eq_ignore_ascii_case("host") {
                continue; // reqwest derives the identical value from the URL
            }
            request = request.header(&name, &value);
        }
        request.send().await.unwrap().status()
    }

    fn reload_count(metrics: &GatewayMetrics, file: &str, result: &str) -> u64 {
        let file = format!("file=\"{file}\"");
        let result = format!("result=\"{result}\"");
        metrics
            .render()
            .lines()
            .find(|line| {
                line.starts_with("talon_gateway_auth_reload_polls_total")
                    && line.contains(&file)
                    && line.contains(&result)
            })
            .and_then(|line| line.rsplit(' ').next()?.parse().ok())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn reload_applies_new_identities_and_rejects_removed_ones() {
        let mut gateway = LiveGateway::start().await;
        spawn_auth_reload(
            Duration::from_millis(20),
            gateway.runtime.metrics().clone(),
            std::mem::take(&mut gateway.targets),
        );
        assert_eq!(
            signed_status(gateway.address, "key-one", "secret-of-key-one").await,
            StatusCode::OK
        );

        std::fs::write(gateway.identities_path(), identity_json("key-two")).unwrap();
        let metrics = gateway.runtime.metrics().clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            while reload_count(&metrics, "s3_identities", "success") == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("identity reload must be applied and counted");

        assert_eq!(
            signed_status(gateway.address, "key-two", "secret-of-key-two").await,
            StatusCode::OK
        );
        assert_eq!(
            signed_status(gateway.address, "key-one", "secret-of-key-one").await,
            StatusCode::FORBIDDEN
        );
        gateway.stop().await;
    }

    #[tokio::test]
    async fn invalid_reload_retains_last_good_and_counts_failures() {
        let mut gateway = LiveGateway::start().await;
        std::fs::write(gateway.identities_path(), "not json").unwrap();
        gateway.poll();
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "s3_identities", "failure"),
            1
        );
        assert_eq!(
            signed_status(gateway.address, "key-one", "secret-of-key-one").await,
            StatusCode::OK
        );

        // An empty identity list is invalid configuration, not deny-all.
        std::fs::write(gateway.identities_path(), r#"{"identities": []}"#).unwrap();
        gateway.poll();
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "s3_identities", "failure"),
            2
        );
        assert_eq!(
            signed_status(gateway.address, "key-one", "secret-of-key-one").await,
            StatusCode::OK
        );

        // A vanished file is a failure that keeps the last-good configuration.
        std::fs::remove_file(gateway.identities_path()).unwrap();
        gateway.poll();
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "s3_identities", "failure"),
            3
        );

        // Restoring the previously applied bytes reads as unchanged.
        std::fs::write(gateway.identities_path(), identity_json("key-one")).unwrap();
        gateway.poll();
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "s3_identities", "unchanged"),
            1
        );
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "s3_identities", "failure"),
            3
        );
        assert_eq!(
            signed_status(gateway.address, "key-one", "secret-of-key-one").await,
            StatusCode::OK
        );
        gateway.stop().await;
    }

    #[test]
    fn azure_identity_reload_target_swaps_and_retains() {
        let temp = TempDir::new().unwrap();
        let identities = temp.path().join("azure-identities.json");
        let azure_identity = |account_key: &str| {
            format!(
                r#"{{"identities": [{{
                    "account_key": "{account_key}",
                    "principal": "azure-reader",
                    "provider_account": "account-a"
                }}]}}"#
            )
        };
        std::fs::write(&identities, azure_identity("MDEyMzQ1Njc4OWFiY2RlZg==")).unwrap();
        let configured = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "azure"),
            ("TALON_GATEWAY_AZURE_ACCOUNT", "account-a"),
            (
                "TALON_GATEWAY_AZURE_CLIENT_IDENTITIES_PATH",
                identities.to_str().unwrap(),
            ),
        ])
        .unwrap();
        let runtime = dev_runtime("127.0.0.1:0".parse().unwrap());
        let mut targets = configure_auth(&runtime, &configured).unwrap();
        assert_eq!(targets.len(), 1);
        let metrics = runtime.metrics().clone();

        targets[0].poll(&metrics);
        assert_eq!(reload_count(&metrics, "azure_identities", "unchanged"), 1);

        std::fs::write(&identities, azure_identity("YWJjZGVmMDEyMzQ1Njc4OQ==")).unwrap();
        targets[0].poll(&metrics);
        assert_eq!(reload_count(&metrics, "azure_identities", "success"), 1);

        std::fs::write(&identities, "not json").unwrap();
        targets[0].poll(&metrics);
        assert_eq!(reload_count(&metrics, "azure_identities", "failure"), 1);
    }

    #[test]
    fn configure_auth_rejects_cross_protocol_identity_paths() {
        let runtime = dev_runtime("127.0.0.1:0".parse().unwrap());
        let azure_with_s3_path = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "azure"),
            (
                "TALON_GATEWAY_S3_CLIENT_IDENTITIES_PATH",
                "/identities.json",
            ),
        ])
        .unwrap();
        assert!(configure_auth(&runtime, &azure_with_s3_path).is_err());

        let s3_with_azure_path = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            (
                "TALON_GATEWAY_AZURE_CLIENT_IDENTITIES_PATH",
                "/identities.json",
            ),
        ])
        .unwrap();
        assert!(configure_auth(&runtime, &s3_with_azure_path).is_err());
    }

    #[tokio::test]
    async fn unchanged_files_still_count_reload_heartbeats() {
        let mut gateway = LiveGateway::start().await;
        gateway.poll();
        gateway.poll();
        let metrics = gateway.runtime.metrics();
        assert_eq!(reload_count(metrics, "s3_identities", "unchanged"), 2);
        assert_eq!(reload_count(metrics, "authorization", "unchanged"), 2);
        assert_eq!(reload_count(metrics, "s3_identities", "success"), 0);
        assert_eq!(reload_count(metrics, "s3_identities", "failure"), 0);
        gateway.stop().await;
    }

    #[tokio::test]
    async fn files_reload_independently_and_empty_grants_deny_all() {
        let mut gateway = LiveGateway::start().await;
        std::fs::write(gateway.identities_path(), identity_json("key-two")).unwrap();
        std::fs::write(gateway.authorization_path(), "{").unwrap();
        gateway.poll();
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "s3_identities", "success"),
            1
        );
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "authorization", "failure"),
            1
        );
        // The new identity is live while the last-good policy still grants read.
        assert_eq!(
            signed_status(gateway.address, "key-two", "secret-of-key-two").await,
            StatusCode::OK
        );

        // An empty grant list is a valid policy and denies every request.
        std::fs::write(gateway.authorization_path(), r#"{"grants": []}"#).unwrap();
        gateway.poll();
        assert_eq!(
            reload_count(gateway.runtime.metrics(), "authorization", "success"),
            1
        );
        assert_eq!(
            signed_status(gateway.address, "key-two", "secret-of-key-two").await,
            StatusCode::FORBIDDEN
        );
        gateway.stop().await;
    }
}
