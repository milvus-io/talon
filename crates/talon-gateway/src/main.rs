//! Environment-configured Talon object-store gateway process.

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;

use talon_backend::{AzureBackend, AzureConfig, ReqwestClient, S3Backend, S3Config, S3Credentials};
use talon_cache_client::{BlockReader, CoordinatorClient, PlacementCache};
use talon_gateway::azure::{AzureAdapterConfig, AzureBlobAdapter, AzureCache};
use talon_gateway::s3::{S3Adapter, S3AdapterConfig, S3Cache};
use talon_gateway::{
    serve, serve_tls, GatewayAdapter, GatewayConfig, GatewayMode, GatewayRoute, GatewayRuntime,
    GatewaySecurity, GatewayTlsConfig,
};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

type MainResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Settings {
    protocol: String,
    bind: SocketAddr,
    mode: GatewayMode,
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
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    s3_session_token: Option<String>,
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
        let bind = parse_or(
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
        let tls = match (tls_certificate, tls_private_key) {
            (None, None) => None,
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
            }),
            _ => return Err(invalid(
                "TALON_GATEWAY_TLS_CERT_PATH and TALON_GATEWAY_TLS_KEY_PATH must be set together",
            )),
        };

        Ok(Self {
            protocol,
            bind,
            mode,
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
            s3_region: value(&mut get, "TALON_GATEWAY_S3_REGION"),
            s3_endpoint: value(&mut get, "TALON_GATEWAY_S3_ENDPOINT"),
            s3_access_key: value(&mut get, "AWS_ACCESS_KEY_ID"),
            s3_secret_key: value(&mut get, "AWS_SECRET_ACCESS_KEY"),
            s3_session_token: value(&mut get, "AWS_SESSION_TOKEN"),
            tls,
        })
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
    let cache = cache_reader(settings);
    Ok(Arc::new(AzureBlobAdapter::new(
        config,
        cache as Arc<dyn AzureCache>,
        origin,
    )?))
}

fn s3_adapter(settings: &Settings) -> MainResult<Arc<dyn GatewayAdapter>> {
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
    config.block_size = settings.block_size;
    config.transfer_chunk_bytes = settings.transfer_chunk_bytes;
    config.default_route = settings.route;
    let cache = cache_reader(settings);
    Ok(Arc::new(S3Adapter::new(
        config,
        cache as Arc<dyn S3Cache>,
        origin,
    )?))
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
            ..GatewayConfig::default()
        },
        adapter,
        GatewaySecurity::default(),
    )?);
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
        assert_eq!(settings.route, GatewayRoute::Cache);
        assert!(!settings.incoming_path_style);
        assert!(settings.tls.is_none());
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
        assert!(!format!("{tls:?}").contains("/tls/key.pem"));
    }
}
