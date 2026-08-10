//! Environment-configured Talon object-store gateway process.

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use talon_backend::{AzureBackend, AzureConfig, ReqwestClient, S3Backend, S3Config, S3Credentials};
use talon_cache_client::{BlockReader, CoordinatorClient, PlacementCache};
use talon_gateway::azure::{AzureAdapterConfig, AzureBlobAdapter, AzureCache};
use talon_gateway::azure_auth::{AzureClientIdentity, AzureStorageAuthenticator};
use talon_gateway::s3::{S3Adapter, S3AdapterConfig, S3Cache};
use talon_gateway::s3_auth::{S3ClientIdentity, S3SigV4Authenticator};
use talon_gateway::{
    serve, serve_tls, AuthenticatedPrincipal, AuthorizationGrant, AuthorizationPolicy,
    GatewayAdapter, GatewayClientAuthConfig, GatewayClientAuthMode, GatewayConfig, GatewayMode,
    GatewayMtlsIdentity, GatewayOperation, GatewayRoute, GatewayRuntime, GatewaySecurity,
    GatewayTlsConfig, OriginAuthMode, ProviderProtocol,
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
    s3_max_clock_skew_ms: u64,
    s3_max_multipart_uploads: usize,
    s3_multipart_ttl_ms: u64,
    authorization_path: Option<PathBuf>,
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

fn azure_adapter(settings: &Settings) -> MainResult<Arc<dyn GatewayAdapter>> {
    if settings.origin_auth == OriginAuthMode::TrustedPassthrough {
        return Err(invalid(
            "trusted-passthrough Azure routing is not available in this build",
        ));
    }
    let account = settings
        .azure_account
        .clone()
        .ok_or_else(|| invalid("TALON_GATEWAY_AZURE_ACCOUNT is required"))?;
    if settings.azure_shared_key.is_some() && settings.azure_sas.is_some() {
        return Err(invalid(
            "set only one of TALON_GATEWAY_AZURE_SHARED_KEY or TALON_GATEWAY_AZURE_SAS",
        ));
    }
    if settings.azure_shared_key.is_none() && settings.azure_sas.is_none() {
        return Err(invalid("an Azure shared key or SAS credential is required"));
    }
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
    let origin = match settings.azure_shared_key.as_deref() {
        Some(key) => Arc::new(AzureBackend::with_shared_key(origin_config, key, http)),
        None => Arc::new(AzureBackend::new(
            origin_config,
            settings
                .azure_sas
                .as_deref()
                .map(|token| token.trim_start_matches('?').to_string()),
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

fn s3_adapter(settings: &Settings) -> MainResult<Arc<dyn GatewayAdapter>> {
    if settings.origin_auth == OriginAuthMode::TrustedPassthrough {
        return Err(invalid(
            "trusted-passthrough S3 routing is not available in this build",
        ));
    }
    let region = settings
        .s3_region
        .clone()
        .unwrap_or_else(|| "us-east-1".to_string());
    let access_key = settings
        .s3_access_key
        .clone()
        .ok_or_else(|| invalid("AWS_ACCESS_KEY_ID is required"))?;
    let secret_key = settings
        .s3_secret_key
        .clone()
        .ok_or_else(|| invalid("AWS_SECRET_ACCESS_KEY is required"))?;
    let origin_config = match settings.s3_endpoint.as_deref() {
        Some(endpoint) => {
            let (host, tls) = split_endpoint(endpoint);
            S3Config {
                region: region.clone(),
                endpoint: host,
                path_style: true,
                tls,
            }
        }
        None => S3Config::aws(&region),
    };
    let origin = Arc::new(S3Backend::new(
        origin_config,
        S3Credentials {
            access_key_id: access_key,
            secret_access_key: secret_key,
            session_token: settings.s3_session_token.clone(),
        },
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

fn load_s3_authenticator(
    path: &Path,
    region: &str,
    max_clock_skew_ms: u64,
) -> MainResult<S3SigV4Authenticator> {
    let configured: S3IdentityFile = serde_json::from_str(&std::fs::read_to_string(path)?)?;
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

fn load_azure_authenticator(
    path: &Path,
    account: &str,
    max_clock_skew_ms: u64,
    transport_https: bool,
) -> MainResult<AzureStorageAuthenticator> {
    let configured: AzureIdentityFile = serde_json::from_str(&std::fs::read_to_string(path)?)?;
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

fn load_authorization(path: &Path) -> MainResult<AuthorizationPolicy> {
    let configured: AuthorizationFile = serde_json::from_str(&std::fs::read_to_string(path)?)?;
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
    let adapter = match settings.protocol.as_str() {
        "azure" => azure_adapter(&settings)?,
        "s3" => s3_adapter(&settings)?,
        _ => unreachable!("validated protocol"),
    };
    let runtime = Arc::new(GatewayRuntime::new(
        GatewayConfig {
            bind: settings.bind,
            mode: settings.mode,
            origin_auth: settings.origin_auth,
            ..GatewayConfig::default()
        },
        adapter,
        GatewaySecurity::default(),
    )?);
    if let Some(path) = &settings.s3_client_identities_path {
        if settings.protocol != "s3" {
            return Err(invalid(
                "TALON_GATEWAY_S3_CLIENT_IDENTITIES_PATH requires the s3 protocol",
            ));
        }
        let region = settings.s3_region.as_deref().unwrap_or("us-east-1");
        runtime.install_authentication(Arc::new(load_s3_authenticator(
            path,
            region,
            settings.s3_max_clock_skew_ms,
        )?));
    }
    if let Some(path) = &settings.azure_client_identities_path {
        if settings.protocol != "azure" {
            return Err(invalid(
                "TALON_GATEWAY_AZURE_CLIENT_IDENTITIES_PATH requires the azure protocol",
            ));
        }
        let account = settings
            .azure_account
            .as_deref()
            .ok_or_else(|| invalid("TALON_GATEWAY_AZURE_ACCOUNT is required"))?;
        runtime.install_authentication(Arc::new(load_azure_authenticator(
            path,
            account,
            settings.azure_max_clock_skew_ms,
            settings.tls.is_some(),
        )?));
    }
    if let Some(path) = &settings.authorization_path {
        runtime.install_authorization(load_authorization(path)?);
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
        assert_eq!(settings.s3_max_clock_skew_ms, 900_000);
        assert_eq!(settings.s3_max_multipart_uploads, 1024);
        assert_eq!(settings.s3_multipart_ttl_ms, 86_400_000);
        assert!(settings.authorization_path.is_none());
        assert!(settings.azure_client_identities_path.is_none());
        assert_eq!(settings.azure_max_clock_skew_ms, 900_000);
        assert_eq!(settings.azure_max_block_bindings, 1024);
        assert_eq!(settings.azure_block_binding_ttl_ms, 86_400_000);
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
    }

    #[test]
    fn trusted_passthrough_configuration_is_bounded_and_fail_closed() {
        let trusted = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_ORIGIN_AUTH", "trusted-passthrough"),
        ])
        .unwrap();
        assert_eq!(trusted.origin_auth, OriginAuthMode::TrustedPassthrough);
        assert!(s3_adapter(&trusted).is_err());

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

    #[test]
    fn provider_credentials_are_required_and_mutually_exclusive() {
        let s3 = settings(&[("TALON_COORDINATOR_ADDR", "coordinator:7411")]).unwrap();
        assert!(s3_adapter(&s3).is_err());

        let azure = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "azure"),
            ("TALON_GATEWAY_AZURE_ACCOUNT", "account"),
        ])
        .unwrap();
        assert!(azure_adapter(&azure).is_err());

        let both = settings(&[
            ("TALON_COORDINATOR_ADDR", "coordinator:7411"),
            ("TALON_GATEWAY_PROTOCOL", "azure"),
            ("TALON_GATEWAY_AZURE_ACCOUNT", "account"),
            ("TALON_GATEWAY_AZURE_SHARED_KEY", "key"),
            ("TALON_GATEWAY_AZURE_SAS", "sas"),
        ])
        .unwrap();
        assert!(azure_adapter(&both).is_err());
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

        assert!(load_s3_authenticator(&identities, "us-east-1", 900_000).is_ok());
        assert!(load_azure_authenticator(&azure_identities, "account-a", 900_000, true).is_ok());
        assert!(load_authorization(&authorization).is_ok());
    }
}
