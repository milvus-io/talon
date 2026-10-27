//! Talon worker entry point.
//!
//! Registers with the coordinator, then serves data-plane range requests. On a
//! miss it fetches the block-aligned range from the configured Azure backend
//! over real HTTPS, commits it durably to the local block store, and serves the
//! requested sub-range. A subsequent request for the same block is a local hit.
//!
//! # Wiring
//!
//! - Control plane (register/heartbeat) reuses [`talon_transport::codec`].
//! - Data plane uses [`talon_transport::data`]: a
//!   [`talon_transport::data::RangeRequest`] in, raw bytes (or an
//!   `ERROR`-flagged frame) out.
//! - Backend fetch uses [`ReqwestClient`]; origin credentials are resolved by
//!   the backend's AK/SK, IAM, or Broker mode and secrets are never logged.
//!
//! The hot path serves cached blocks **zero-copy** via `sendfile(2)`
//! ([`send_file_range`](talon_worker::send_file_range)) straight from the
//! committed block file's descriptor into the client socket — the bytes never
//! enter user space and no per-request block buffer is allocated (issue #179). A
//! cache miss (bytes just fetched into memory) or a boundary-spanning read falls
//! back to writing the in-memory bytes inline.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use talon_backend::origin::{
    AzureCredentialInput, AzureOrigin, AzureOriginInput, GcsCredentialInput, GcsOrigin,
    GcsOriginInput, OriginValidation, S3Origin, S3OriginInput,
};
use talon_backend::{
    AzureBackend, BrokerConfig, CredentialMode, CredentialOptions, GcsBackend, GcsHmacCredentials,
    ReqwestClient, S3Backend, S3Credentials,
};
use talon_core::{
    azure_sas_from_env, gcs_bearer_from_env, Backend, BackendStore, NodeId, NodeInfo, NodeRole,
    ObjectNamespace, ObjectNamespacePolicy, WorkerConfig, WorkerConfigPatch, WorkloadIdentity,
    WorkloadRole,
};
use talon_metadata::MappingRevision;
use talon_transport::control_tls::ControlTlsChannel;
use talon_transport::{codec, ControlMessage};
use talon_worker::mapping_guard::MappingGuard;
use talon_worker::tokio_conn::{handle_conn, read_control};
#[cfg(target_os = "linux")]
use talon_worker::uring_conn;
#[cfg(test)]
use talon_worker::WholeBlockStore;
use talon_worker::{
    serve_admin, BlockIndex, ConnectionAdmission, InFlightLoads, WorkerObservability, WorkerRuntime,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CONTROL_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const CONTROL_TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(5);

/// Bridges the backend retry decorator to the worker's metrics registry.
///
/// `talon-backend` deliberately knows nothing about the worker's registry, so it
/// emits retry and timeout events through the [`RetryObserver`] trait and this
/// adapter turns them into counters.
struct MetricsRetryObserver {
    observability: Arc<WorkerObservability>,
}

/// Bridges origin credential refresh outcomes to the worker's registry.
struct CredentialsMetricsObserver {
    observability: Arc<WorkerObservability>,
    report_expiry: bool,
}

impl talon_backend::CredentialsObserver for CredentialsMetricsObserver {
    fn refresh_succeeded(&self, expires_at: Option<std::time::SystemTime>) {
        self.observability
            .metrics()
            .record_origin_credentials("refresh_success");
        let unix_seconds = expires_at
            .filter(|_| self.report_expiry)
            .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0.0, |since_epoch| since_epoch.as_secs_f64());
        self.observability
            .metrics()
            .set_origin_credentials_expiry(unix_seconds);
    }

    fn refresh_failed(&self) {
        self.observability
            .metrics()
            .record_origin_credentials("refresh_failure");
    }
}

impl talon_backend::RetryObserver for MetricsRetryObserver {
    fn on_retry(&self, _attempt: u32, _status: Option<u16>) {
        self.observability.metrics().record_backend_retry();
    }

    fn on_timeout(&self) {
        self.observability.metrics().record_backend_timeout();
    }
}

/// Set to `1` to pin the legacy Tokio data plane regardless of io_uring
/// availability. An escape hatch for operators who hit a regression; the
/// io_uring path is the default because it wins decisively under the
/// connection counts a cache fleet actually produces (see #285).
const FORCE_TOKIO_ENV: &str = "TALON_WORKER_FORCE_TOKIO_DATA_PLANE";

/// Command-line arguments for a Talon worker.
#[derive(Debug, Parser)]
#[command(name = "talon-worker", version, about)]
struct Args {
    /// Path to a TOML config file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Address to bind the worker RPC service to.
    #[arg(long)]
    listen: Option<String>,
    /// Routable address advertised to clients (defaults to `listen`).
    #[arg(long)]
    advertise_addr: Option<String>,
    /// Address to bind the worker HTTP administration service to.
    #[arg(long)]
    admin_listen: Option<String>,
    /// Address of the coordinator to register with.
    #[arg(long)]
    coordinator: Option<String>,
    /// Dedicated coordinator-initiated mTLS control bind address.
    #[arg(long)]
    control_listen: Option<String>,
    /// Logical cluster advertised by worker status.
    #[arg(long)]
    cluster_id: Option<String>,
    /// Import a stable node identity; thereafter must match worker_identity.
    #[arg(long)]
    node_id: Option<String>,
    /// Heartbeat interval in milliseconds; detected failures retain readiness
    /// for at most three intervals since the last accepted heartbeat, capped at 15s.
    #[arg(long)]
    heartbeat_interval_ms: Option<u64>,
    /// Logical block size in bytes.
    #[arg(long)]
    block_size: Option<u32>,
    /// Serve the data plane on N io_uring rings (thread-per-core).
    ///
    /// `0` (the default) means one ring per available core. With
    /// TALON_WORKER_ORIGIN_IO=tokio, the worker can fall back to the Tokio
    /// data plane if io_uring is unavailable; also set
    /// TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1 to pin it explicitly.
    #[arg(long)]
    data_plane_rings: Option<usize>,
    /// Connections per data-plane thread (default 8192). The worker-global
    /// budget scales with the resolved ring count, or Tokio worker threads.
    #[arg(long)]
    data_plane_connections_per_thread: Option<usize>,
}

impl Args {
    fn into_patch(self) -> WorkerConfigPatch {
        WorkerConfigPatch {
            listen: self.listen,
            advertise_addr: self.advertise_addr,
            admin_listen: self.admin_listen,
            coordinator: self.coordinator,
            control_listen: self.control_listen,
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            control_tls: None,
            namespace_policy_path: None,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            block_size: self.block_size,
            data_plane_rings: self.data_plane_rings,
            data_plane_connections_per_thread: self.data_plane_connections_per_thread,
            cache_dirs: None,
            capacity_bytes: None,
            whole_fd_cache_capacity: None,
            page_fd_cache_capacity: None,
            background_task_concurrency: None,
            background_io_concurrency: None,
            background_scan_batch_size: None,
            background_delete_batch_size: None,
            background_io_max_mb_per_sec: None,
            background_delete_max_per_sec: None,
            async_eviction_enabled: None,
            stale_gc_enabled: None,
            stale_gc_head_max_per_sec: None,
            stale_gc_head_concurrency: None,
            async_eviction_high_watermark: None,
            async_eviction_low_watermark: None,
            async_eviction_check_interval_minutes: None,
            l1_capacity_bytes: None,
            l1_page_size_bytes: None,
            l2_page_size_bytes: None,
            paged_miss_run_concurrency: None,
            ownership_gc_enabled: None,
            ownership_gc_replicas: None,
            load_inflight_bytes: None,
            page_tti_hours: None,
            page_access_checkpoint_interval_minutes: None,
            page_gc_interval_minutes: None,
            file_cleanup_interval_hours: None,
            page_gc_scan_batch_size: None,
            page_gc_delete_batch_size: None,
            page_gc_io_concurrency: None,
            backend: None,
            azure_account: None,
            azure_endpoint: None,
            backend_delay_ms: None,
            backend_jitter_ms: None,
            backend_throughput_bytes: None,
            // Retry/timeout knobs are env- and file-only (`cli: false` in the
            // ConfigVar schema), so the CLI patch leaves them unset.
            backend_max_retries: None,
            backend_retry_base_ms: None,
            backend_retry_max_delay_ms: None,
            backend_timeout_floor_ms: None,
            backend_min_throughput_bytes: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            credentials_source: None,
            buckets: None,
            s3_secret_access_key: None,
            s3_session_token: None,
            gcs_access_key_id: None,
            gcs_secret_access_key: None,
            azure_shared_key: None,
            credential_broker_url: None,
            credential_broker_request: None,
            credential_broker_timeout_ms: None,
            s3_path_style: None,
            gcs_endpoint: None,
            // The [rate_limits] table is file-only; the CLI leaves it unset.
            rate_limits: None,
            namespaces: None,
        }
    }
}

/// Use the resolved ENV-over-TOML configuration without changing process env.
fn origin_credential_options(cfg: &WorkerConfig) -> anyhow::Result<CredentialOptions> {
    let source = cfg
        .credentials_source
        .clone()
        .filter(|value| !value.is_empty());
    let broker = if source.as_deref() == Some("broker") {
        Some(BrokerConfig {
            origin: cfg.credential_broker_timeout_ms.map(|timeout| talon_core::OriginOptions {
                request_timeout_ms: Some(timeout),
                ..Default::default()
            }),
            url: cfg.credential_broker_url.as_ref()
                .ok_or_else(|| anyhow::anyhow!("broker requires credential_broker_url or TALON_CREDENTIAL_BROKER_URL"))?
                .expose().clone(),
            request: cfg.credential_broker_request.as_ref()
                .ok_or_else(|| anyhow::anyhow!("broker requires credential_broker_request or TALON_CREDENTIAL_BROKER_REQUEST"))?
                .expose().clone(),
        })
    } else {
        None
    };
    Ok(CredentialOptions {
        source,
        broker,
        s3_region: cfg.s3_region.clone(),
    })
}

/// Build Azure from its account/endpoint and the common credential resolver.
/// IAM and Broker ignore leftover direct secrets in the resolved configuration.
async fn build_azure_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
    exchange_http: Arc<dyn talon_backend::http::HttpClient>,
    observer: Arc<dyn talon_backend::CredentialsObserver>,
) -> anyhow::Result<AzureBackend> {
    let account = cfg.azure_account.clone().ok_or_else(|| {
        anyhow::anyhow!("azure_account is required (set TALON_WORKER_AZURE_ACCOUNT)")
    })?;
    let options = origin_credential_options(cfg)?;
    let source = options.source.as_deref();
    let shared_key = cfg
        .azure_shared_key
        .as_ref()
        .map(|value| value.expose().clone())
        .filter(|value| !value.is_empty());
    let legacy_sas = azure_sas_from_env();
    // Preserve existing fixed-SAS configurations. Explicit modes still use
    // the common resolver, so IAM/Broker cannot fall back to this credential.
    if shared_key.is_none() && matches!(source, None | Some("auto" | "static")) {
        if let Some(sas) = &legacy_sas {
            return AzureOrigin::prepare(AzureOriginInput {
                account,
                endpoint: cfg.azure_endpoint.clone(),
                endpoint_suffix: None,
                path_style: None,
                use_ssl: None,
                validation: OriginValidation::Legacy,
                credentials: AzureCredentialInput::FixedSas(sas.clone()),
            })
            .map_err(anyhow::Error::msg)?
            .build(http, exchange_http, observer)
            .await
            .map(|(backend, _)| backend)
            .map_err(anyhow::Error::msg);
        }
    }
    let mode = CredentialMode::select(source, shared_key.is_some() || legacy_sas.is_some())
        .map_err(anyhow::Error::msg)?;
    let shared_key = if mode == CredentialMode::AkSk {
        if legacy_sas.is_some() {
            anyhow::bail!(
                "TALON_WORKER_AZURE_SAS is not an AK/SK account key; use \
                 TALON_ORIGIN_CREDENTIALS_SOURCE=iam or broker, or configure \
                 TALON_WORKER_AZURE_SHARED_KEY for aksk"
            );
        }
        shared_key
    } else {
        None
    };
    let (backend, source) = AzureOrigin::prepare(AzureOriginInput {
        account,
        endpoint: cfg.azure_endpoint.clone(),
        endpoint_suffix: None,
        path_style: None,
        use_ssl: None,
        validation: OriginValidation::Legacy,
        credentials: AzureCredentialInput::Resolve {
            shared_key,
            options: Box::new(options),
        },
    })
    .map_err(anyhow::Error::msg)?
    .build(http, exchange_http, observer)
    .await
    .map_err(anyhow::Error::msg)?;
    tracing::info!(source, "origin Azure credentials resolved");
    Ok(backend)
}

/// Build S3 from its region/endpoint and the common credential resolver.
async fn build_s3_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
    exchange_http: Arc<dyn talon_backend::http::HttpClient>,
    observer: Arc<dyn talon_backend::CredentialsObserver>,
) -> anyhow::Result<S3Backend> {
    let region = cfg
        .s3_region
        .clone()
        .ok_or_else(|| anyhow::anyhow!("s3_region is required (set TALON_WORKER_S3_REGION)"))?;
    let options = origin_credential_options(cfg)?;
    let source = options.source.as_deref();
    let secret_access_key = cfg
        .s3_secret_access_key
        .as_ref()
        .map(|value| value.expose().clone())
        .filter(|value| !value.is_empty());
    let mode = CredentialMode::select(
        source,
        cfg.s3_access_key_id.is_some() || secret_access_key.is_some(),
    )
    .map_err(anyhow::Error::msg)?;
    let static_credentials = if mode == CredentialMode::AkSk {
        match (cfg.s3_access_key_id.clone(), secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Some(S3Credentials {
                access_key_id,
                secret_access_key,
                session_token: cfg
                    .s3_session_token
                    .as_ref()
                    .map(|value| value.expose().clone())
                    .filter(|value| !value.is_empty()),
            }),
            (None, None) => None,
            (Some(_), None) => {
                anyhow::bail!(
                    "s3_secret_access_key or TALON_WORKER_S3_SECRET_ACCESS_KEY must be set"
                )
            }
            (None, Some(_)) => {
                anyhow::bail!("s3_access_key_id is required (set TALON_WORKER_S3_ACCESS_KEY_ID)")
            }
        }
    } else {
        None
    };
    let (backend, source) = S3Origin::prepare(S3OriginInput {
        region,
        endpoint: cfg.s3_endpoint.clone(),
        path_style: cfg.s3_path_style.unwrap_or(false),
        use_ssl: None,
        validation: OriginValidation::Legacy,
        keys: static_credentials,
        credentials: options,
    })
    .map_err(anyhow::Error::msg)?
    .build(http, exchange_http, observer)
    .await
    .map_err(anyhow::Error::msg)?;
    tracing::info!(source, "origin S3 credentials resolved");
    Ok(backend)
}

/// Build GCS from its endpoint and the common credential resolver. HMAC keys
/// come from the resolved configuration; legacy fixed bearer tokens remain supported.
async fn build_gcs_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
    exchange_http: Arc<dyn talon_backend::http::HttpClient>,
    observer: Arc<dyn talon_backend::CredentialsObserver>,
) -> anyhow::Result<GcsBackend> {
    let options = origin_credential_options(cfg)?;
    let source = options.source.as_deref();
    let access_key_id = cfg
        .gcs_access_key_id
        .clone()
        .filter(|value| !value.is_empty());
    let secret_access_key = cfg
        .gcs_secret_access_key
        .as_ref()
        .map(|value| value.expose().clone())
        .filter(|value| !value.is_empty());
    let legacy_token = gcs_bearer_from_env();
    if access_key_id.is_none()
        && secret_access_key.is_none()
        && matches!(source, None | Some("auto" | "static"))
    {
        // Explicit legacy static mode also permits anonymous emulator access.
        if source == Some("static") || legacy_token.is_some() {
            return GcsOrigin::prepare(GcsOriginInput {
                endpoint: cfg.gcs_endpoint.clone(),
                use_ssl: None,
                validation: OriginValidation::Legacy,
                credentials: GcsCredentialInput::Fixed(legacy_token),
            })
            .map_err(anyhow::Error::msg)?
            .build(http, exchange_http, observer)
            .await
            .map(|(backend, _)| backend)
            .map_err(anyhow::Error::msg);
        }
    }
    let mode = CredentialMode::select(
        source,
        access_key_id.is_some() || secret_access_key.is_some() || legacy_token.is_some(),
    )
    .map_err(anyhow::Error::msg)?;
    let credentials = if mode == CredentialMode::AkSk {
        if legacy_token.is_some() {
            anyhow::bail!(
                "TALON_WORKER_GCS_BEARER_TOKEN is not an AK/SK HMAC key; use \
                 TALON_ORIGIN_CREDENTIALS_SOURCE=iam or broker, or configure \
                 TALON_WORKER_GCS_ACCESS_KEY_ID and TALON_WORKER_GCS_SECRET_ACCESS_KEY for aksk"
            );
        }
        match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Some(GcsHmacCredentials {
                access_key_id,
                secret_access_key,
            }),
            (None, None) => None,
            (Some(_), None) => anyhow::bail!(
                "gcs_secret_access_key or TALON_WORKER_GCS_SECRET_ACCESS_KEY must be set"
            ),
            (None, Some(_)) => {
                anyhow::bail!("gcs_access_key_id or TALON_WORKER_GCS_ACCESS_KEY_ID must be set")
            }
        }
    } else {
        None
    };
    let (backend, source) = GcsOrigin::prepare(GcsOriginInput {
        endpoint: cfg.gcs_endpoint.clone(),
        use_ssl: None,
        validation: OriginValidation::Legacy,
        credentials: GcsCredentialInput::Resolve {
            keys: credentials,
            options: Box::new(options),
        },
    })
    .map_err(anyhow::Error::msg)?
    .build(http, exchange_http, observer)
    .await
    .map_err(anyhow::Error::msg)?;
    tracing::info!(source, "origin GCS credentials resolved");
    Ok(backend)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "telemetry")]
    let _telemetry = talon_telemetry::export::init("talon-worker")
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    #[cfg(not(feature = "telemetry"))]
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    #[cfg(not(feature = "telemetry"))]
    talon_telemetry::Config::from_env()
        .and_then(talon_telemetry::configure)
        .map_err(std::io::Error::other)?;
    let result = run().await;
    #[cfg(feature = "telemetry")]
    let _ = tokio::task::spawn_blocking(move || _telemetry.shutdown()).await;
    result
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    let file = match &args.config {
        Some(path) => WorkerConfigPatch::from_file(path)?,
        None => WorkerConfigPatch::default(),
    };
    let env = WorkerConfigPatch::from_env()?;
    let cli = args.into_patch();
    let mut cfg = WorkerConfig::resolve(file, env, cli)?;

    tracing::info!(
        listen = %cfg.listen,
        admin_listen = %cfg.admin_listen,
        coordinator = %cfg.coordinator,
        cluster_id = %cfg.cluster_id,
        node_id = ?cfg.node_id,
        heartbeat_interval_ms = cfg.heartbeat_interval_ms,
        block_size = cfg.block_size,
        cache_dirs = ?cfg.cache_dirs,
        capacity_bytes = cfg.capacity_bytes,
        l1_capacity_bytes = cfg.l1_capacity_bytes,
        l1_page_size_bytes = cfg.l1_page_size_bytes,
        l2_page_size_bytes = cfg.l2_page_size_bytes,
        paged_miss_run_concurrency = cfg.paged_miss_run_concurrency,
        load_inflight_bytes = cfg.load_inflight_bytes,
        azure_account = ?cfg.azure_account,
        azure_endpoint = ?cfg.azure_endpoint,
        "starting talon-worker"
    );

    // Local store stack rooted at the first cache dir.
    let root = cfg
        .cache_dirs
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("/tmp/talon-cache"));
    std::fs::create_dir_all(&root)?;
    // The running store uses this root, not the sum of all configured paths.
    // Fail before initializing identity, recovering cache data, or registering.
    talon_worker::capacity::validate_filesystem_capacity(&root, cfg.capacity_bytes)?;
    let cache_root_lock = Arc::new(talon_worker::page_access_store::CacheRootLock::acquire(
        &root,
    )?);
    let identity = cache_root_lock.load_identity(
        &cfg.cluster_id,
        cfg.node_id.as_deref(),
        cfg.block_size,
        cfg.l2_page_size_bytes,
    )?;
    cfg.node_id = Some(identity.worker_id);
    let index = Arc::new(BlockIndex::new());
    let inflight = Arc::new(InFlightLoads::new());
    let backend_kind = cfg.backend.as_deref().unwrap_or("azure");
    let _configured_backend: Backend = backend_kind.parse().map_err(|error| {
        anyhow::anyhow!(
            "unknown TALON_WORKER_BACKEND {backend_kind:?}; expected azure, s3, or gcs: {error}"
        )
    })?;
    let node = NodeInfo {
        id: NodeId::new(
            cfg.node_id
                .clone()
                .unwrap_or_else(|| cfg.advertise_addr.clone()),
        ),
        // Advertise the routable address, not the (possibly wildcard) bind
        // address, so clients receive a connectable owner (issue #118).
        address: cfg.advertise_addr.clone(),
        role: NodeRole::Worker,
    };
    let resolved_zone = talon_backend::resolve_zone().await;
    tracing::info!(
        zone = ?resolved_zone.zone,
        source = resolved_zone.source,
        "deployment zone resolved"
    );
    let observability = Arc::new(
        WorkerObservability::new_with_backend(
            cfg.cluster_id.clone(),
            node.clone(),
            cfg.admin_listen.clone(),
            cfg.capacity_bytes,
            backend_kind,
            Arc::clone(&index),
            Arc::clone(&inflight),
        )?
        .with_zone(resolved_zone.zone),
    );
    observability.readiness().set_backend_ready(true);
    // Listener readiness is published only after binding the data plane.
    observability.readiness().set_store_ready(false);
    observability
        .metrics()
        .set_l1_capacity(cfg.l1_capacity_bytes);

    let control_tls = if let Some(tls) = &cfg.control_tls {
        let identity = WorkloadIdentity::new(
            tls.trust_domain.clone(),
            cfg.cluster_id.clone(),
            WorkloadRole::Worker,
            node.id.0.clone(),
        )?;
        Some(ControlTlsChannel::load(
            tls.clone(),
            identity,
            WorkloadRole::Coordinator,
            CONTROL_TLS_RELOAD_INTERVAL,
        )?)
    } else {
        None
    };

    // The networked HTTP client, shared by whichever backend is selected. Two
    // decorators may wrap it, innermost first: retry (always on — a cache with
    // no retry turns a routine 503 into a failed read) and, optionally, the
    // latency injector for the test/latency lab.
    //
    // Retry is innermost so each attempt gets its own injected latency, which is
    // what the lab is modeling; wrapping the other way would make the whole
    // retry ladder share a single delay and understate the cost of a retry.
    let origin_uring = match std::env::var("TALON_WORKER_ORIGIN_IO")
        .as_deref()
        .unwrap_or("uring")
    {
        "tokio" => false,
        "uring" => true,
        _ => anyhow::bail!("TALON_WORKER_ORIGIN_IO must be tokio or uring"),
    };
    if origin_uring {
        #[cfg(target_os = "linux")]
        let available = talon_worker::uring_serve::io_uring_available();
        #[cfg(not(target_os = "linux"))]
        let available = false;
        validate_origin_runtime(force_tokio_data_plane(), available)?;
    }
    use talon_backend::monoio_client::{KernelTlsCapabilities, TlsMode, TlsVersion};
    let origin_tls = match std::env::var("TALON_WORKER_ORIGIN_TLS")
        .as_deref()
        .unwrap_or("auto")
    {
        "auto" => TlsMode::Auto,
        "rustls" => TlsMode::Rustls,
        "ktls" if origin_uring => TlsMode::Ktls,
        "ktls" => anyhow::bail!("kTLS requires TALON_WORKER_ORIGIN_IO=uring"),
        _ => anyhow::bail!("TALON_WORKER_ORIGIN_TLS must be auto, rustls, or ktls"),
    };
    let minimum_tls: TlsVersion = std::env::var("TALON_WORKER_ORIGIN_TLS_MIN_VERSION")
        .unwrap_or_else(|_| "1.2".into())
        .parse()
        .map_err(anyhow::Error::msg)?;
    let endpoint_minimums: std::collections::BTreeMap<String, String> = serde_json::from_str(
        &std::env::var("TALON_WORKER_ORIGIN_TLS_ENDPOINT_MIN_VERSIONS")
            .unwrap_or_else(|_| "{}".into()),
    )?;
    if !origin_uring && (minimum_tls != TlsVersion::Tls12 || !endpoint_minimums.is_empty()) {
        anyhow::bail!("origin TLS minimum version policy requires TALON_WORKER_ORIGIN_IO=uring");
    }
    // Complete the machine probe before publishing the client or serving traffic.
    let capabilities = match origin_uring {
        true if origin_tls != TlsMode::Rustls => KernelTlsCapabilities::probe().await?,
        _ => KernelTlsCapabilities::default(),
    };
    if origin_tls == TlsMode::Ktls && capabilities.is_empty() {
        anyhow::bail!("kernel TLS TX/RX capability probe failed; strict kTLS is unavailable");
    }
    tracing::info!(
        ?capabilities,
        ?minimum_tls,
        "origin TLS startup capabilities"
    );
    tracing::info!(
        origin_tls = ?origin_tls,
        origin_io = if origin_uring {
            "uring"
        } else {
            "tokio"
        },
        "origin download and cache commit engine"
    );
    let http: Arc<dyn talon_backend::http::HttpClient> = {
        let base: Arc<dyn talon_backend::http::HttpClient> = match origin_uring {
            true => {
                let mut client = talon_backend::monoio_client::MonoioClient::new()
                    .with_tls_mode(origin_tls)
                    .with_kernel_tls_capabilities(capabilities)
                    .with_tls_min_version(minimum_tls);
                for (endpoint, minimum) in endpoint_minimums {
                    client = client
                        .with_endpoint_tls_min_version(
                            &endpoint,
                            minimum.parse().map_err(anyhow::Error::msg)?,
                        )
                        .map_err(anyhow::Error::msg)?;
                }
                Arc::new(client)
            }
            false => Arc::new(ReqwestClient::new()),
        };

        // Seed from the node identity so each worker's jitter is distinct yet
        // reproducible across restarts. Shared by both decorators.
        let seed = cfg
            .node_id
            .as_deref()
            .unwrap_or(&cfg.advertise_addr)
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));

        let defaults = talon_backend::RetryConfig::default();
        let retry = talon_backend::RetryConfig {
            max_retries: cfg.backend_max_retries.unwrap_or(defaults.max_retries),
            base_delay: cfg
                .backend_retry_base_ms
                .map(Duration::from_millis)
                .unwrap_or(defaults.base_delay),
            max_delay: cfg
                .backend_retry_max_delay_ms
                .map(Duration::from_millis)
                .unwrap_or(defaults.max_delay),
            timeout_floor: cfg
                .backend_timeout_floor_ms
                .map(Duration::from_millis)
                .unwrap_or(defaults.timeout_floor),
            min_throughput_bytes_per_sec: cfg
                .backend_min_throughput_bytes
                .unwrap_or(defaults.min_throughput_bytes_per_sec),
        };
        tracing::info!(
            max_retries = retry.max_retries,
            base_delay_ms = retry.base_delay.as_millis(),
            max_delay_ms = retry.max_delay.as_millis(),
            timeout_floor_ms = retry.timeout_floor.as_millis(),
            min_throughput_bytes = retry.min_throughput_bytes_per_sec,
            "backend retry policy"
        );
        let base: Arc<dyn talon_backend::http::HttpClient> = Arc::new(
            talon_backend::RetryingHttpClient::new(base, retry, seed).with_observer(Arc::new(
                MetricsRetryObserver {
                    observability: Arc::clone(&observability),
                },
            )),
        );

        let delay = talon_backend::DelayConfig {
            base: Duration::from_millis(cfg.backend_delay_ms.unwrap_or(0)),
            jitter: Duration::from_millis(cfg.backend_jitter_ms.unwrap_or(0)),
            throughput_bytes_per_sec: cfg.backend_throughput_bytes.filter(|&b| b > 0),
        };
        if delay.is_active() {
            tracing::info!(
                base_ms = cfg.backend_delay_ms.unwrap_or(0),
                jitter_ms = cfg.backend_jitter_ms.unwrap_or(0),
                throughput_bytes = ?cfg.backend_throughput_bytes,
                "synthetic backend latency enabled"
            );
            Arc::new(talon_backend::DelayingHttpClient::new(base, delay, seed))
        } else {
            base
        }
    };

    // Select the object-store backend from config (default: azure). Each backend
    // reads its endpoint and credentials from resolved configuration. IAM and
    // Broker credentials refresh through their providers and update metrics.
    let buckets = cfg.resolved_buckets()?;
    // FIXME: Expose expiry per credential instance. Every worker can now own
    // several request-specific providers. A single
    // expiry cannot describe them; outcome counters remain meaningful.
    observability.metrics().set_origin_credentials_expiry(0.0);
    let credentials_observer: Arc<dyn talon_backend::CredentialsObserver> =
        Arc::new(CredentialsMetricsObserver {
            observability: Arc::clone(&observability),
            report_expiry: false,
        });
    if buckets.is_some() && (azure_sas_from_env().is_some() || gcs_bearer_from_env().is_some()) {
        anyhow::bail!("buckets cannot be combined with global fixed SAS or bearer credentials");
    }
    // Request-owned Broker URLs are unknown at startup. Always keep acquisition
    // separate from origin-transfer retries and their shorter deadlines.
    // Discovery and refresh run on Tokio, outside the native origin ring.
    let exchange_http: Arc<dyn talon_backend::HttpClient> = Arc::new(ReqwestClient::new());
    let backend_type: Backend = backend_kind.parse()?;
    let resolver = if let Some(buckets) = buckets {
        talon_backend::DefaultBackendResolver::from_configs(
            backend_type,
            buckets,
            http.clone(),
            exchange_http.clone(),
            credentials_observer.clone(),
        )
        .await
        .map_err(anyhow::Error::msg)?
    } else {
        let explicitly_configured = cfg.credentials_source.is_some()
            || cfg.s3_access_key_id.is_some()
            || cfg.s3_secret_access_key.is_some()
            || cfg.s3_session_token.is_some()
            || cfg.gcs_access_key_id.is_some()
            || cfg.gcs_secret_access_key.is_some()
            || cfg.azure_shared_key.is_some()
            || cfg.credential_broker_url.is_some()
            || cfg.credential_broker_request.is_some()
            || cfg.credential_broker_timeout_ms.is_some()
            || azure_sas_from_env().is_some()
            || gcs_bearer_from_env().is_some();
        let legacy_config = Arc::new(cfg.clone());
        let legacy_http = http.clone();
        let legacy_exchange = exchange_http.clone();
        let legacy_observer = credentials_observer.clone();
        let factory: talon_backend::LegacyBackendFactory = Arc::new(move || {
            let config = legacy_config.clone();
            let http = legacy_http.clone();
            let exchange = legacy_exchange.clone();
            let observer = legacy_observer.clone();
            Box::pin(async move {
                let result: anyhow::Result<Arc<dyn BackendStore>> = async {
                    Ok(match backend_type {
                        Backend::Azure => {
                            Arc::new(build_azure_backend(&config, http, exchange, observer).await?)
                                as Arc<dyn BackendStore>
                        }
                        Backend::S3 => {
                            Arc::new(build_s3_backend(&config, http, exchange, observer).await?)
                        }
                        Backend::Gcs => {
                            Arc::new(build_gcs_backend(&config, http, exchange, observer).await?)
                        }
                    })
                }
                .await;
                result.map_err(|error| error.to_string())
            })
        });
        let resolver = talon_backend::DefaultBackendResolver::new(
            backend_type,
            http,
            exchange_http,
            credentials_observer,
        );
        if explicitly_configured {
            resolver.with_legacy_backend(factory().await.map_err(anyhow::Error::msg)?)
        } else {
            // Preserve implicit IAM for legacy requests, while dynamic-only
            // workers start without IMDS, a global region, or an Azure account.
            resolver.with_legacy_factory(factory)
        }
    }
    .with_dynamic_defaults(talon_backend::DynamicBackendDefaults {
        s3_path_style: cfg.s3_path_style,
    });
    let backend: Arc<dyn BackendStore> = Arc::new(talon_backend::BackendRouter::new(
        backend_type,
        Arc::new(resolver),
    ));
    // The router owns initialized providers. Namespace cache runtimes only
    // need cache policy; avoid cloning all bucket secrets into each namespace.
    cfg.buckets = None;
    tracing::info!(backend = backend_kind, "object-store backend ready");

    let runtime = WorkerRuntime::open_namespaces_with_uring(
        &cfg,
        cache_root_lock,
        backend,
        observability.metrics().clone(),
        origin_uring,
    )?;
    let worker = Arc::new(runtime);
    observability.attach_namespace_runtime(&worker);
    for namespace in worker.namespace_runtimes() {
        namespace.recover_page_file_cleanup().await;
    }

    let admin_listener = TcpListener::bind(&cfg.admin_listen).await?;
    tracing::info!(listen = %cfg.admin_listen, "worker serving administration API");
    let admin_observability = Arc::clone(&observability);
    tokio::spawn(async move {
        if let Err(error) = serve_admin(admin_listener, admin_observability).await {
            tracing::error!(%error, "worker administration server stopped");
        }
    });

    let mut control_server = None;
    if let (Some(control_listen), Some(channel)) = (&cfg.control_listen, &control_tls) {
        let listener = TcpListener::bind(control_listen).await?;
        tracing::info!(listen = %control_listen, "worker serving coordinator mTLS control plane");
        let channel = channel.clone();
        let cluster_id = cfg.cluster_id.clone();
        let worker_id = node.id.0.clone();
        let worker_incarnation = observability.incarnation_id().to_owned();
        let policy = cfg.namespace_policy.clone();
        let guard = Arc::new(MappingGuard::new(Duration::from_millis(
            cfg.heartbeat_interval_ms.saturating_mul(3),
        )));
        let shutdown = observability.shutdown().clone();
        control_server = Some(tokio::spawn(async move {
            if let Err(error) = serve_control(
                listener,
                channel,
                cluster_id,
                worker_id,
                worker_incarnation,
                policy,
                guard,
                shutdown,
            )
            .await
            {
                tracing::error!(%error, "worker coordinator mTLS control plane stopped");
            }
        }));
    }

    let _control_plane = spawn_control_plane(
        cfg.coordinator.clone(),
        control_tls.clone(),
        Arc::clone(&observability),
        Duration::from_millis(cfg.heartbeat_interval_ms),
        cfg.ownership_gc_enabled.then(|| worker.clone()),
    );

    let background = talon_worker::runtime::WorkerBackground::start(worker.clone(), &cfg);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let data = serve_data_plane(cfg.clone(), worker, observability.clone(), stop.clone());
    tokio::pin!(data);
    let (result, data_done) = tokio::select! {
        result = &mut data => (result, true),
        result = shutdown_signal() => (result, false),
        _ = observability.shutdown().stopped() => (Ok(()), false),
    };
    let drain_started = Instant::now();
    observability.begin_shutdown();
    tracing::info!("worker draining");
    background.begin_shutdown();
    stop.store(true, std::sync::atomic::Ordering::Release);
    // Join the producer before publishing not-ready: an older ready heartbeat
    // must never follow the final report from this process.
    _control_plane.abort();
    let _ = _control_plane.await;
    let drain = async {
        let report = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(observability.status_for_heartbeat()),
        };
        let _ = tokio::time::timeout(
            CONTROL_OPERATION_TIMEOUT,
            instance_report(&cfg.coordinator, control_tls.as_ref(), &report),
        )
        .await;
        let data_result = if !data_done { data.await } else { Ok(()) };
        // Even a failed accept/ring must drain disk mutations and checkpoint
        // within the same deadline before returning the original error.
        let control_result = match control_server {
            Some(task) => task.await,
            None => Ok(()),
        };
        background.shutdown().await;
        control_result?;
        data_result
    };
    match tokio::time::timeout(Duration::from_secs(20), drain).await {
        Ok(drained) => drained?,
        Err(_) => {
            // Dropping a spawn_blocking future does not stop disk writes. Exit
            // the entire process before the cache-directory flock can escape.
            tracing::error!("worker drain deadline exceeded; terminating with cache lock held");
            std::process::exit(1);
        }
    }
    tracing::info!(
        elapsed_ms = drain_started.elapsed().as_millis() as u64,
        "worker drain complete"
    );
    result
}

async fn shutdown_signal() -> anyhow::Result<()> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {r = tokio::signal::ctrl_c() => r?, _ = term.recv() => {}}
    Ok(())
}

fn force_tokio_data_plane() -> bool {
    std::env::var(FORCE_TOKIO_ENV)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn validate_origin_runtime(force_tokio: bool, uring_available: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        !force_tokio && uring_available,
        "TALON_WORKER_ORIGIN_IO=uring requires the native Monoio data plane; disable {FORCE_TOKIO_ENV} and allow io_uring, or set TALON_WORKER_ORIGIN_IO=tokio"
    );
    Ok(())
}

async fn serve_data_plane(
    cfg: WorkerConfig,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    // Serve the data plane, on io_uring rings if configured (#285) or on the
    // portable Tokio path otherwise.
    // The io_uring data plane is the default. Fall back to the portable Tokio
    // path when the host cannot run it (older kernel, restrictive seccomp, some
    // container runtimes) or when an operator pins the legacy path explicitly.
    let force_tokio = force_tokio_data_plane();
    #[cfg(target_os = "linux")]
    let uring_available = talon_worker::uring_serve::io_uring_available();
    #[cfg(not(target_os = "linux"))]
    let uring_available = false;
    if force_tokio {
        tracing::info!("{FORCE_TOKIO_ENV} is set; serving the data plane on the Tokio path");
    } else if !uring_available {
        tracing::warn!(
            "io_uring is unavailable on this host; falling back to the Tokio \
             data plane. Performance will be lower under high connection counts."
        );
    }
    #[cfg(target_os = "linux")]
    if !force_tokio && uring_available {
        let rings = talon_worker::uring_serve::resolve_ring_count(cfg.data_plane_rings);
        let connection_admission = ConnectionAdmission::for_threads(
            rings,
            cfg.data_plane_connections_per_thread,
            observability.metrics().clone(),
        )?;
        tracing::info!(
            listen = %cfg.listen,
            rings,
            connections_per_thread = cfg.data_plane_connections_per_thread,
            "worker serving data plane on io_uring rings"
        );
        // `serve` blocks until the ring threads exit, and it must run off the
        // Tokio worker threads it is handing out — so drive it on a dedicated
        // thread and park this task on the join.
        let addr = cfg.listen.clone();
        // Every SO_REUSEPORT listener shares this worker-global budget. `serve`
        // acquires it before accept, matching the Tokio fallback and ensuring the
        // scaled cap covers accepted socket FDs and their tasks (#568).
        let handler =
            uring_conn::RingConnHandler::new(Arc::clone(&worker), Arc::clone(&observability));
        let tokio_handle = tokio::runtime::Handle::current();
        let joined = tokio::task::spawn_blocking(move || {
            talon_worker::uring_serve::serve_with_shutdown(
                addr,
                rings,
                connection_admission,
                handler,
                tokio_handle,
                stop,
            )
        })
        .await?;
        return joined;
    }

    let threads = tokio::runtime::Handle::current().metrics().num_workers();
    let connection_admission = ConnectionAdmission::for_threads(
        threads,
        cfg.data_plane_connections_per_thread,
        observability.metrics().clone(),
    )?;
    let listener = TcpListener::bind(&cfg.listen).await?;
    tracing::info!(
        listen = %cfg.listen,
        threads,
        connections_per_thread = cfg.data_plane_connections_per_thread,
        "worker serving data plane on Tokio"
    );
    serve_tokio_data_plane(listener, worker, observability, connection_admission).await
}

async fn serve_tokio_data_plane(
    listener: TcpListener,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
    connection_admission: ConnectionAdmission,
) -> anyhow::Result<()> {
    observability.readiness().set_store_ready(true);
    let mut connections = tokio::task::JoinSet::new();
    let mut failure = None;
    let stopped = observability.shutdown().stopped();
    tokio::pin!(stopped);
    loop {
        let accepted = tokio::select! {
            biased;
            _ = &mut stopped => break,
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed { tracing::warn!(%error, "connection task failed"); }
                continue;
            }
            accepted = async {
                let permit = connection_admission.acquire().await;
                listener.accept().await.map(|accepted| (permit, accepted))
            } => accepted,
        };
        let (permit, (stream, peer)) = match accepted {
            Ok(v) => v,
            Err(error) => {
                failure = Some(error);
                observability.begin_shutdown();
                break;
            }
        };
        let worker = Arc::clone(&worker);
        let observability = Arc::clone(&observability);
        connections.spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_conn(stream, worker, observability).await {
                tracing::debug!(%peer, error = %e, "worker: connection ended");
            }
        });
    }
    drop(listener);
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "connection task failed during drain");
        }
    }
    match failure {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

const CONTROL_FAILURE_GRACE_INTERVALS: u32 = 3;
const MAX_CONTROL_FAILURE_GRACE: Duration = Duration::from_secs(15);

/// Preserve readiness only within the last accepted heartbeat grace.
fn retain_readiness_after_control_failure(
    observability: &WorkerObservability,
    last_successful_heartbeat: Option<Instant>,
    grace: Duration,
) {
    let remaining = last_successful_heartbeat
        .and_then(|last| grace.checked_sub(last.elapsed()))
        .filter(|remaining| !remaining.is_zero());
    match remaining {
        Some(remaining) => observability
            .readiness()
            .limit_control_registration_to(remaining),
        None => observability.readiness().set_control_registered(false),
    }
}

/// Publish instance status and serve only after sole-instance acceptance.
fn spawn_control_plane(
    coordinator: String,
    channel: Option<ControlTlsChannel>,
    observability: Arc<WorkerObservability>,
    heartbeat_interval: Duration,
    membership: Option<Arc<WorkerRuntime>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let heartbeat = async {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_successful_heartbeat = None;
            let failure_grace = heartbeat_interval
                .saturating_mul(CONTROL_FAILURE_GRACE_INTERVALS)
                .min(MAX_CONTROL_FAILURE_GRACE);
            loop {
                ticker.tick().await;
                let proposed = ControlMessage::NodeStatusHeartbeat {
                    status: Box::new(observability.status_for_heartbeat()),
                };
                let report = tokio::time::timeout(
                    CONTROL_OPERATION_TIMEOUT,
                    instance_report(&coordinator, channel.as_ref(), &proposed),
                )
                .await;
                match report {
                    Ok(Ok((accepted, serving))) => {
                        observability
                            .readiness()
                            .set_control_registered(accepted && serving);
                        if accepted && serving {
                            last_successful_heartbeat = Some(Instant::now());
                            observability.metrics().record_heartbeat_success();
                        } else {
                            observability.metrics().record_heartbeat_failure();
                        }
                    }
                    result => {
                        observability.metrics().record_heartbeat_failure();
                        retain_readiness_after_control_failure(
                            &observability,
                            last_successful_heartbeat,
                            failure_grace,
                        );
                        tracing::warn!(?result, "worker instance heartbeat failed; retrying");
                    }
                }
            }
        };
        let refresh = async {
            if let Some(worker) = &membership {
                let mut ticker = tokio::time::interval(heartbeat_interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let observed = Instant::now();
                    let result = tokio::time::timeout(CONTROL_OPERATION_TIMEOUT, async {
                        if let Some(channel) = &channel {
                            query_membership(channel.connect(&coordinator).await?.stream).await
                        } else {
                            query_membership(TcpStream::connect(&coordinator).await?).await
                        }
                    })
                    .await;
                    match result {
                        Ok(Ok(view)) => {
                            if let Err(error) = worker.update_membership(Some(view), observed) {
                                tracing::warn!(%error, "invalid worker membership; deferring ownership cleanup");
                            }
                        }
                        error => {
                            worker
                                .update_membership(None, observed)
                                .expect("clear membership");
                            tracing::warn!(
                                ?error,
                                "worker membership refresh failed; deferring ownership cleanup"
                            );
                        }
                    }
                }
            }
        };
        // Independent loops in the existing control task: a stalled membership
        // request must not delay heartbeats or change their readiness result.
        tokio::join!(heartbeat, refresh);
    })
}

async fn query_membership<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
) -> anyhow::Result<talon_core::worker_membership::WorkerDiscovery> {
    stream
        .write_all(&codec::encode(0, &ControlMessage::MembershipQuery {})?)
        .await?;
    stream.flush().await?;
    match read_control(&mut stream).await? {
        Some(ControlMessage::MembershipList { view }) => Ok(view),
        other => anyhow::bail!("expected retained worker membership, got {other:?}"),
    }
}

async fn instance_report(
    addr: &str,
    channel: Option<&ControlTlsChannel>,
    msg: &ControlMessage,
) -> anyhow::Result<(bool, bool)> {
    match channel {
        Some(channel) => instance_report_on_stream(channel.connect(addr).await?.stream, msg).await,
        None => instance_report_on_stream(TcpStream::connect(addr).await?, msg).await,
    }
}

async fn instance_report_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    msg: &ControlMessage,
) -> anyhow::Result<(bool, bool)> {
    stream.write_all(&codec::encode(0, msg)?).await?;
    stream.flush().await?;
    match read_control(&mut stream).await? {
        Some(ControlMessage::NodeStatusAck {
            accepted,
            serving,
            detail,
        }) => {
            if !accepted {
                tracing::warn!(?detail, "worker instance rejected");
            }
            Ok((accepted, serving))
        }
        other => anyhow::bail!("expected worker instance acknowledgement, got {other:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_control(
    listener: TcpListener,
    channel: ControlTlsChannel,
    cluster_id: String,
    worker_id: String,
    worker_incarnation: String,
    policy: Option<ObjectNamespacePolicy>,
    guard: Arc<MappingGuard>,
    shutdown: Arc<talon_transport::shutdown::Shutdown>,
) -> anyhow::Result<()> {
    let mut connections = tokio::task::JoinSet::new();
    let stopped = shutdown.stopped();
    tokio::pin!(stopped);
    let result = loop {
        let accepted = tokio::select! {
            biased;
            _ = &mut stopped => break Ok(()),
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed { tracing::warn!(%error, "control connection task failed"); }
                continue;
            }
            accepted = listener.accept() => accepted,
        };
        let (stream, peer) = match accepted {
            Ok(value) => value,
            Err(error) => {
                shutdown.begin();
                break Err(error.into());
            }
        };
        let shutdown = shutdown.clone();
        let channel = channel.clone();
        let cluster_id = cluster_id.clone();
        let worker_id = worker_id.clone();
        let worker_incarnation = worker_incarnation.clone();
        let policy = policy.clone();
        let guard = Arc::clone(&guard);
        connections.spawn(async move {
            let stopped = shutdown.stopped();
            tokio::pin!(stopped);
            let result = async {
                let mut authenticated = tokio::select! {
                    biased;
                    _ = &mut stopped => return anyhow::Ok(()),
                    accepted = channel.accept(stream) => accepted?,
                };
                tracing::debug!(identity = %authenticated.identity, %peer, "accepted coordinator mTLS connection");
                let message = tokio::select! {
                    biased;
                    _ = &mut stopped => return anyhow::Ok(()),
                    message = read_control(&mut authenticated.stream) => message?,
                };
                if let Some(message) = message {
                    if shutdown.is_stopped() { return anyhow::Ok(()); }
                    let reply = handle_revision_update(
                        message,
                        &authenticated.identity,
                        &cluster_id,
                        &worker_id,
                        &worker_incarnation,
                        policy.as_ref(),
                        &guard,
                    )
                    .unwrap_or_else(|detail| ControlMessage::Ack {
                        ok: false,
                        detail: Some(detail),
                    });
                    authenticated
                        .stream
                        .write_all(&codec::encode(0, &reply)?)
                        .await?;
                    authenticated.stream.flush().await?;
                }
                anyhow::Ok(())
            }
            .await;
            if let Err(error) = result {
                tracing::debug!(%peer, %error, "worker coordinator mTLS connection ended");
            }
        });
    };
    drop(listener);
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "control connection task failed during shutdown");
        }
    }
    result
}

fn handle_revision_update(
    message: ControlMessage,
    peer: &WorkloadIdentity,
    cluster_id: &str,
    worker_id: &str,
    worker_incarnation: &str,
    policy: Option<&ObjectNamespacePolicy>,
    guard: &MappingGuard,
) -> Result<ControlMessage, String> {
    let ControlMessage::MappingRevisionUpdate {
        cluster_id: message_cluster,
        namespace,
        revision,
        coordinator_id,
        coordinator_incarnation,
    } = message
    else {
        return Err("expected mapping revision update".into());
    };
    if message_cluster != cluster_id || peer.cluster_id() != cluster_id {
        return Err("revision update cluster does not match authenticated session".into());
    }
    if coordinator_id != peer.node_id() {
        return Err("revision update coordinator ID does not match authenticated peer".into());
    }
    if coordinator_incarnation.is_empty() {
        return Err("revision update coordinator incarnation must not be empty".into());
    }
    let target = namespace
        .parse::<ObjectNamespace>()
        .map_err(|error| format!("invalid revision namespace: {error}"))?;
    if !policy.is_some_and(|policy| policy.authorizes(worker_id, &target)) {
        return Err("worker is not authorized for revision namespace".into());
    }

    guard.observe(&namespace, MappingRevision::new(revision));
    let held = guard
        .held(&namespace)
        .unwrap_or(MappingRevision::INITIAL)
        .get();
    Ok(ControlMessage::MappingRevisionAck {
        cluster_id: cluster_id.to_owned(),
        namespace,
        revision: held,
        worker_id: worker_id.to_owned(),
        worker_incarnation: worker_incarnation.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn native_origin_requires_native_data_plane() {
        assert!(super::validate_origin_runtime(false, true).is_ok());
        for (forced, available) in [(true, true), (true, false), (false, false)] {
            assert!(super::validate_origin_runtime(forced, available).is_err());
        }
    }

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::time::SystemTime;

    use async_trait::async_trait;
    use bytes::Bytes;
    use talon_core::{Error, ObjectId, ObjectStat, Result, Version};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn pod_iam_s3_compatible_legacy_origin_requires_endpoint_before_exchange() {
        struct NoHttp;
        #[async_trait::async_trait]
        impl talon_backend::HttpClient for NoHttp {
            async fn execute(
                &self,
                _: talon_backend::HttpRequest,
            ) -> std::result::Result<talon_backend::HttpResponse, String> {
                panic!("missing S3-compatible endpoint must fail before HTTP");
            }
        }
        for csp in ["ali", "tc", "hwc"] {
            let config = WorkerConfig {
                backend: Some("s3".into()),
                s3_region: Some("cn-hangzhou".into()),
                credentials_source: Some("broker".into()),
                credential_broker_url: Some("https://broker.example/exchange".to_owned().into()),
                credential_broker_request: Some(
                    serde_json::json!({"credentialType":"iam","csp":csp,"region":"cn-hangzhou"})
                        .into(),
                ),
                ..Default::default()
            };
            let http = Arc::new(NoHttp);
            let error = build_s3_backend(
                &config,
                http.clone(),
                http,
                Arc::new(talon_backend::NoopObserver),
            )
            .await
            .err()
            .expect("missing endpoint must fail");
            assert!(error.to_string().contains("s3_endpoint"));
        }
    }

    #[test]
    fn credential_metrics_keep_counts_and_only_report_single_backend_expiry() {
        use talon_backend::CredentialsObserver;

        for report_expiry in [false, true] {
            let (_worker, observability, _node, root) = test_worker();
            let observer = CredentialsMetricsObserver {
                observability: observability.clone(),
                report_expiry,
            };
            for timestamp in [1000, 2000] {
                observer.refresh_succeeded(Some(
                    std::time::UNIX_EPOCH + Duration::from_secs(timestamp),
                ));
                let expected = if report_expiry { timestamp } else { 0 };
                assert!(observability.metrics().render().lines().any(|line| {
                    line == format!("talon_worker_origin_credentials_expiry_seconds {expected}")
                }));
            }
            observer.refresh_failed();
            let metrics = observability.metrics().render();
            assert!(metrics
                .contains("talon_worker_origin_credentials_total{result=\"refresh_success\"} 2"));
            assert!(metrics
                .contains("talon_worker_origin_credentials_total{result=\"refresh_failure\"} 1"));
            let expected = if report_expiry { 2000 } else { 0 };
            assert!(metrics.lines().any(|line| {
                line == format!("talon_worker_origin_credentials_expiry_seconds {expected}")
            }));
            observer.refresh_succeeded(None);
            assert!(observability
                .metrics()
                .render()
                .lines()
                .any(|line| line == "talon_worker_origin_credentials_expiry_seconds 0"));
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn connection_budget_cli_overrides_environment() {
        let args = Args::try_parse_from([
            "talon-worker",
            "--data-plane-connections-per-thread",
            "16384",
        ])
        .unwrap();
        let env = WorkerConfigPatch {
            data_plane_connections_per_thread: Some(4096),
            ..Default::default()
        };
        let cfg = WorkerConfig::resolve(Default::default(), env, args.into_patch()).unwrap();
        assert_eq!(cfg.data_plane_connections_per_thread, 16384);
    }

    fn coordinator_identity(cluster_id: &str, node_id: &str) -> WorkloadIdentity {
        WorkloadIdentity::new(
            "cluster.example",
            cluster_id,
            WorkloadRole::Coordinator,
            node_id,
        )
        .unwrap()
    }

    fn revision_policy() -> ObjectNamespacePolicy {
        ObjectNamespacePolicy::from_toml(
            "version = 1\n\
             [[workers]]\n\
             node_id = \"worker-1\"\n\
             grants = [\"s3/data/models\"]\n",
        )
        .unwrap()
    }

    fn revision_update(cluster_id: &str, coordinator_id: &str, revision: u64) -> ControlMessage {
        ControlMessage::MappingRevisionUpdate {
            cluster_id: cluster_id.into(),
            namespace: "s3/data/models".into(),
            revision,
            coordinator_id: coordinator_id.into(),
            coordinator_incarnation: "coordinator-incarnation-1".into(),
        }
    }

    #[test]
    fn authorized_revision_update_returns_worker_ack() {
        let guard = MappingGuard::new(Duration::from_secs(30));
        let reply = handle_revision_update(
            revision_update("cluster-a", "coordinator-1", 7),
            &coordinator_identity("cluster-a", "coordinator-1"),
            "cluster-a",
            "worker-1",
            "worker-incarnation-1",
            Some(&revision_policy()),
            &guard,
        )
        .unwrap();

        assert_eq!(
            reply,
            ControlMessage::MappingRevisionAck {
                cluster_id: "cluster-a".into(),
                namespace: "s3/data/models".into(),
                revision: 7,
                worker_id: "worker-1".into(),
                worker_incarnation: "worker-incarnation-1".into(),
            }
        );
    }

    #[test]
    fn stale_revision_update_acks_the_higher_held_revision() {
        let guard = MappingGuard::new(Duration::from_secs(30));
        guard.observe("s3/data/models", MappingRevision::new(9));
        let reply = handle_revision_update(
            revision_update("cluster-a", "coordinator-1", 7),
            &coordinator_identity("cluster-a", "coordinator-1"),
            "cluster-a",
            "worker-1",
            "worker-incarnation-1",
            Some(&revision_policy()),
            &guard,
        )
        .unwrap();

        assert!(matches!(
            reply,
            ControlMessage::MappingRevisionAck { revision: 9, .. }
        ));
        assert_eq!(guard.held("s3/data/models"), Some(MappingRevision::new(9)));
    }

    #[test]
    fn revision_update_rejects_unauthorized_namespace_and_spoofed_identity() {
        let policy = revision_policy();
        let guard = MappingGuard::new(Duration::from_secs(30));
        let peer = coordinator_identity("cluster-a", "coordinator-1");
        let mut unauthorized = revision_update("cluster-a", "coordinator-1", 7);
        if let ControlMessage::MappingRevisionUpdate { namespace, .. } = &mut unauthorized {
            *namespace = "s3/private/models".into();
        }
        assert!(handle_revision_update(
            unauthorized,
            &peer,
            "cluster-a",
            "worker-1",
            "worker-incarnation-1",
            Some(&policy),
            &guard,
        )
        .is_err());
        assert!(handle_revision_update(
            revision_update("cluster-b", "coordinator-1", 7),
            &peer,
            "cluster-a",
            "worker-1",
            "worker-incarnation-1",
            Some(&policy),
            &guard,
        )
        .is_err());
        assert!(handle_revision_update(
            revision_update("cluster-a", "coordinator-2", 7),
            &peer,
            "cluster-a",
            "worker-1",
            "worker-incarnation-1",
            Some(&policy),
            &guard,
        )
        .is_err());
    }

    struct MockBackend {
        _calls: AtomicUsize,
    }

    #[async_trait]
    impl BackendStore for MockBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, _len: u64) -> Result<Bytes> {
            Err(Error::Backend("not used".into()))
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 0,
                version: Version::new("v1"),
            })
        }
    }

    #[tokio::test]
    async fn query_membership_rejects_errors_and_unexpected_replies() {
        for reply in [
            ControlMessage::Ack {
                ok: false,
                detail: Some("not ready".into()),
            },
            ControlMessage::MembershipQuery {},
        ] {
            let (client, mut server) = tokio::io::duplex(4096);
            let responder = tokio::spawn(async move {
                assert_eq!(
                    read_control(&mut server).await.unwrap(),
                    Some(ControlMessage::MembershipQuery {})
                );
                server
                    .write_all(&codec::encode(0, &reply).unwrap())
                    .await
                    .unwrap();
            });
            assert!(query_membership(client).await.is_err());
            responder.await.unwrap();
        }
    }

    #[tokio::test]
    async fn stalled_membership_does_not_delay_heartbeats() {
        use std::sync::atomic::Ordering;
        let (worker, observability, _, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap().to_string();
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let queries = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn({
            let heartbeats = heartbeats.clone();
            let queries = queries.clone();
            async move {
                let mut withheld = Vec::new();
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    match read_control(&mut stream).await.unwrap().unwrap() {
                        ControlMessage::MembershipQuery {} => {
                            queries.fetch_add(1, Ordering::Relaxed);
                            withheld.push(stream);
                        }
                        ControlMessage::NodeStatusHeartbeat { .. } => {
                            stream
                                .write_all(
                                    &codec::encode(
                                        0,
                                        &ControlMessage::NodeStatusAck {
                                            accepted: true,
                                            serving: true,
                                            detail: None,
                                        },
                                    )
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                            heartbeats.fetch_add(1, Ordering::Relaxed);
                        }
                        other => panic!("unexpected control request: {other:?}"),
                    }
                }
            }
        });
        let control = spawn_control_plane(
            coordinator,
            None,
            observability.clone(),
            Duration::from_millis(20),
            Some(worker),
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while heartbeats.load(Ordering::Relaxed) < 4 || queries.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            queries.load(Ordering::Relaxed),
            1,
            "only one refresh in flight"
        );
        assert!(observability.is_ready());
        control.abort();
        server.abort();
        let _ = control.await;
        let _ = server.await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn heartbeat_task_refreshes_membership_independently_of_gc() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use talon_core::worker_membership::{MemberRegistry, WorkerDiscovery, WorkerMember};
        let (worker, observability, node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let nodes = [
            node.clone(),
            NodeInfo {
                id: NodeId::new("other"),
                address: String::new(),
                role: NodeRole::Worker,
            },
        ];
        let view = WorkerDiscovery::retained(
            &MemberRegistry {
                members: nodes
                    .iter()
                    .map(|n| WorkerMember {
                        worker_id: n.id.0.clone(),
                        zone: None,
                        retired: false,
                    })
                    .collect(),
                ..Default::default()
            },
            &[],
        );
        let placement = talon_core::CachePlacementTable::new(&nodes);
        let block = (0..1000)
            .map(|i| {
                talon_core::BlockId::new(
                    ObjectId::new(Backend::S3, "bucket", format!("key-{i}")),
                    0,
                    8,
                    Version::new("v1"),
                )
            })
            .find(|id| placement.primary(id).unwrap().id != node.id)
            .unwrap();
        worker
            .admit_cached_block(
                &talon_transport::data::CachedBlockPutRequest {
                    block,
                    object_len: 8,
                    body_len: 8,
                },
                Bytes::from_static(&[1; 8]),
            )
            .await
            .unwrap();
        let allocated_before_refresh = worker.resident_bytes();
        assert!(allocated_before_refresh > 0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap().to_string();
        let queries = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let server = tokio::spawn({
            let queries = queries.clone();
            let fail = fail.clone();
            async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let reply = match read_control(&mut stream).await.unwrap().unwrap() {
                        ControlMessage::NodeStatusHeartbeat { .. } => {
                            ControlMessage::NodeStatusAck {
                                accepted: true,
                                serving: true,
                                detail: None,
                            }
                        }
                        ControlMessage::MembershipQuery {} => {
                            queries.fetch_add(1, Ordering::Relaxed);
                            if fail.load(Ordering::Relaxed) {
                                ControlMessage::Ack {
                                    ok: false,
                                    detail: None,
                                }
                            } else {
                                ControlMessage::MembershipList { view: view.clone() }
                            }
                        }
                        other => panic!("unexpected control request: {other:?}"),
                    };
                    stream
                        .write_all(&codec::encode(0, &reply).unwrap())
                        .await
                        .unwrap();
                }
            }
        });
        let control = spawn_control_plane(
            coordinator,
            None,
            observability.clone(),
            Duration::from_millis(10),
            Some(worker.clone()),
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while queries.load(Ordering::Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            worker.resident_bytes(),
            allocated_before_refresh,
            "refresh runs without GC"
        );
        let background = talon_worker::runtime::WorkerBackground::start(
            worker.clone(),
            &WorkerConfig {
                node_id: Some(node.id.0),
                ownership_gc_enabled: true,
                stale_gc_enabled: false,
                ..Default::default()
            },
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while worker.resident_bytes() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        fail.store(true, Ordering::Relaxed);
        let next = queries.load(Ordering::Relaxed) + 2;
        tokio::time::timeout(Duration::from_secs(2), async {
            while queries.load(Ordering::Relaxed) < next {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            observability.is_ready(),
            "membership failures do not reject accepted heartbeats"
        );
        assert!(!observability
            .metrics()
            .render()
            .contains("talon_worker_control_heartbeat_total{result=\"failure\"} 1"));
        background.shutdown().await;
        control.abort();
        server.abort();
        let _ = control.await;
        let _ = server.await;
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn control_plane_waits_for_instance_acceptance() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap();
        let (status_sent, status_received) = oneshot::channel();
        let (allow_ack, wait_for_ack) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let message = read_control(&mut stream).await.unwrap().unwrap();
            status_sent.send(message).unwrap();
            wait_for_ack.await.unwrap();
            stream
                .write_all(
                    &codec::encode(
                        0,
                        &ControlMessage::NodeStatusAck {
                            accepted: true,
                            serving: true,
                            detail: None,
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        });
        let (_worker, observability, node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let control = spawn_control_plane(
            coordinator.to_string(),
            None,
            observability.clone(),
            Duration::from_secs(60),
            None,
        );
        let message = tokio::time::timeout(Duration::from_secs(2), status_received)
            .await
            .unwrap()
            .unwrap();
        match message {
            ControlMessage::NodeStatusHeartbeat { status } => {
                status.validate().unwrap();
                assert_eq!(status.node, node);
                assert!(status.ready);
            }
            other => panic!("unexpected heartbeat: {other:?}"),
        }
        assert!(
            !observability.is_ready(),
            "unaccepted status must not enable reads"
        );
        allow_ack.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !observability.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        control.abort();
        server.await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn instance_ack_distinguishes_acceptance_from_serving() {
        let (_worker, observability, _node, root) = test_worker();
        let message = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(observability.status_for_heartbeat()),
        };
        for (accepted, serving) in [(false, false), (true, false), (true, true)] {
            let (client, mut server) = tokio::io::duplex(4096);
            let responder = tokio::spawn(async move {
                let message = read_control(&mut server).await.unwrap().unwrap();
                assert!(matches!(
                    message,
                    ControlMessage::NodeStatusHeartbeat { .. }
                ));
                server
                    .write_all(
                        &codec::encode(
                            0,
                            &ControlMessage::NodeStatusAck {
                                accepted,
                                serving,
                                detail: None,
                            },
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            });
            assert_eq!(
                instance_report_on_stream(client, &message).await.unwrap(),
                (accepted, serving)
            );
            responder.await.unwrap();
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn instance_heartbeat_rejects_missing_or_old_ack() {
        let (_worker, observability, _node, root) = test_worker();
        let message = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(observability.status_for_heartbeat()),
        };
        for reply in [
            None,
            Some(ControlMessage::Ack {
                ok: true,
                detail: None,
            }),
        ] {
            let (client, mut server) = tokio::io::duplex(4096);
            let responder = tokio::spawn(async move {
                read_control(&mut server).await.unwrap().unwrap();
                if let Some(reply) = reply {
                    server
                        .write_all(&codec::encode(0, &reply).unwrap())
                        .await
                        .unwrap();
                }
            });
            assert!(instance_report_on_stream(client, &message).await.is_err());
            responder.await.unwrap();
        }
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn recent_control_heartbeat_bridges_only_a_bounded_failure_window() {
        let (_worker, observability, _node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        observability.readiness().set_control_registered(true);
        let grace = Duration::from_secs(15);

        retain_readiness_after_control_failure(&observability, Some(Instant::now()), grace);
        assert!(
            observability.is_ready(),
            "one failure after a recent heartbeat must not stop the data plane"
        );

        let stale = Instant::now()
            .checked_sub(grace + Duration::from_millis(1))
            .unwrap();
        retain_readiness_after_control_failure(&observability, Some(stale), grace);
        assert!(
            !observability.is_ready(),
            "a worker must become unready after the heartbeat grace expires"
        );

        observability.readiness().set_control_registered(true);
        retain_readiness_after_control_failure(&observability, None, grace);
        assert!(
            !observability.is_ready(),
            "a worker without any successful heartbeat has no grace"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn registration_failure_keeps_worker_unready_and_is_counted() {
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = unused.local_addr().unwrap();
        drop(unused);

        let (_worker, observability, _node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let control = spawn_control_plane(
            coordinator.to_string(),
            None,
            Arc::clone(&observability),
            Duration::from_secs(60),
            None,
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if observability
                    .metrics()
                    .render()
                    .contains("talon_worker_control_heartbeat_total{result=\"failure\"} 1")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!observability.is_ready());

        control.abort();
        std::fs::remove_dir_all(root).ok();
    }

    /// A backend that serves deterministic bytes so a block can be committed and
    /// then re-served on a hit — used to exercise the end-to-end sendfile path.
    struct RampBackend;

    #[async_trait]
    impl BackendStore for RampBackend {
        async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            Ok(Bytes::from(
                (0..len)
                    .map(|i| ((offset + i) % 251) as u8)
                    .collect::<Vec<u8>>(),
            ))
        }

        async fn fetch_range_if_match(
            &self,
            object: &ObjectId,
            offset: u64,
            len: u64,
            if_match: Option<&Version>,
        ) -> Result<Bytes> {
            if let Some(expected) = if_match {
                if expected.as_str() != "v1" {
                    return Err(Error::VersionMismatch {
                        expected: expected.0.clone(),
                        found: "v1".into(),
                    });
                }
            }
            self.fetch_range(object, offset, len).await
        }

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: u64::MAX,
                version: Version::new("v1"),
            })
        }
    }

    #[tokio::test]
    async fn draining_finishes_admitted_read_and_holds_directory_lock() {
        struct PausedOrigin {
            entered: tokio::sync::Notify,
            release: tokio::sync::Notify,
        }
        #[async_trait]
        impl BackendStore for PausedOrigin {
            async fn fetch_range(&self, object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
                self.entered.notify_one();
                self.release.notified().await;
                RampBackend.fetch_range(object, offset, len).await
            }
            async fn fetch_range_if_match(
                &self,
                object: &ObjectId,
                offset: u64,
                len: u64,
                _: Option<&Version>,
            ) -> Result<Bytes> {
                self.fetch_range(object, offset, len).await
            }
            async fn head(&self, object: &ObjectId) -> Result<ObjectStat> {
                RampBackend.head(object).await
            }
        }
        use talon_worker::page_access_store::CacheRootLock;
        let root = tmp_root();
        std::fs::create_dir_all(&root).unwrap();
        let lock = Arc::new(CacheRootLock::acquire(&root).unwrap());
        let index = Arc::new(BlockIndex::new());
        let inflight = Arc::new(InFlightLoads::new());
        let obs = Arc::new(
            WorkerObservability::new(
                "c".into(),
                NodeInfo {
                    id: NodeId::new("w"),
                    address: "127.0.0.1:7".into(),
                    role: NodeRole::Worker,
                },
                "127.0.0.1:8".into(),
                1024,
                index.clone(),
                inflight.clone(),
            )
            .unwrap(),
        );
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_store_ready(true);
        obs.readiness().set_control_registered(true);
        let origin = Arc::new(PausedOrigin {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let worker = Arc::new(WorkerRuntime::new(
            WholeBlockStore::open(&root)
                .unwrap()
                .with_root_lock(lock.clone()),
            index,
            inflight,
            origin.clone(),
            16,
            0,
            obs.metrics().clone(),
        ));
        drop(lock);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let mut idle = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        // A partial frame must not hold up shutdown while another request runs.
        idle.write_all(&[0]).await.unwrap();
        let serving = obs.clone();
        let admission = ConnectionAdmission::new(2, obs.metrics().clone());
        let mut task = tokio::spawn(async move {
            serve_tokio_data_plane(listener, worker, serving, admission).await
        });
        let request = talon_transport::encode_request(
            1,
            &talon_transport::RangeRequest {
                object: ObjectId::new(talon_core::Backend::Azure, "c", "obj"),
                offset: 0,
                len: 8,
            },
        )
        .unwrap();
        client.write_all(&request).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), origin.entered.notified())
            .await
            .unwrap();
        // Queue a second frame before shutdown, while the first request is held.
        // It must never start even though the socket is readable on the next loop.
        client.write_all(&request).await.unwrap();
        obs.begin_shutdown();
        assert!(!obs.is_ready());
        assert!(CacheRootLock::acquire(&root).is_err());
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut task)
            .await
            .is_err());
        origin.release.notify_one();
        let (_, body) = talon_transport::read_frame(&mut client, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(body.as_slice(), &[0, 1, 2, 3, 4, 5, 6, 7]);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _lock = CacheRootLock::acquire(&root).unwrap();
        // A persistent socket is closed after its admitted response; it cannot
        // trigger a second origin operation during shutdown.
        let mut byte = [0];
        for socket in [&mut client, &mut idle] {
            match tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
            {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                other => panic!("connection must close without another response: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn handle_conn_serves_a_hit_via_sendfile_byte_exact() {
        use talon_transport::data::{
            encode_request, encode_versioned_request, RangeRequest, VersionedRangeRequest,
        };

        // Build a worker over a ramp backend so the first request commits a block
        // and the second is a resident hit served with sendfile.
        let root = tmp_root();
        let index = Arc::new(BlockIndex::new());
        let inflight = Arc::new(InFlightLoads::new());
        let node = NodeInfo {
            id: NodeId::new("w"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let observability = Arc::new(
            WorkerObservability::new(
                "c".into(),
                node,
                "127.0.0.1:8001".into(),
                1024,
                Arc::clone(&index),
                Arc::clone(&inflight),
            )
            .unwrap(),
        );
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        observability.readiness().set_control_registered(true);
        let backend: Arc<dyn BackendStore> = Arc::new(RampBackend);
        let worker = Arc::new(WorkerRuntime::new(
            WholeBlockStore::open(&root).unwrap(),
            index,
            inflight,
            backend,
            16, // block_size
            0,
            observability.metrics().clone(),
        ));

        // Serve the connection loop in the background.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_worker = Arc::clone(&worker);
        let srv_obs = Arc::clone(&observability);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_conn(stream, srv_worker, srv_obs).await;
        });

        // A client that issues two identical range requests on one connection:
        // the first is a miss (Bytes path), the second a hit (sendfile path).
        let obj = ObjectId::new(talon_core::Backend::Azure, "c", "obj");
        let req = RangeRequest {
            object: obj,
            offset: 3,
            len: 8,
        };
        let mut client = TcpStream::connect(addr).await.unwrap();
        let expected: Vec<u8> = (0..8u64).map(|i| ((3 + i) % 251) as u8).collect();

        for pass in 0..2 {
            let out = if pass == 0 {
                encode_request(0, &req).unwrap()
            } else {
                encode_versioned_request(
                    0,
                    &VersionedRangeRequest {
                        request: req.clone(),
                        version: Version::new("v1"),
                        object_len: u64::MAX,
                    },
                )
                .unwrap()
            };
            client.write_all(&out).await.unwrap();
            client.flush().await.unwrap();

            let mut hdr = [0u8; HEADER_LEN];
            client.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            assert!(
                !header.flags.contains(talon_transport::Flags::ERROR),
                "worker returned an error frame"
            );
            // The response body is the raw range bytes (no envelope), so a worker
            // can sendfile them straight from the block file.
            let mut body = vec![0u8; header.length as usize];
            client.read_exact(&mut body).await.unwrap();
            assert_eq!(body, expected, "range bytes must match on miss and hit");
        }

        drop(client);
        server.await.unwrap();
        // A hit was recorded (the sendfile path bumps the cache-hit counter).
        assert!(observability
            .metrics()
            .render()
            .contains("talon_worker_cache_hits_total{form=\"whole\"} 1"));
        std::fs::remove_dir_all(root).ok();
    }

    /// The Tokio data plane's cross-page guarantee, mirroring the io_uring test
    /// in `uring_conn`. The two planes must not diverge in what they answer, so
    /// a multi-`sendfile` bug in either shows up as wrong bytes here.
    #[tokio::test]
    async fn handle_conn_serves_a_cross_page_hit_byte_exact() {
        use talon_transport::data::{encode_request, RangeRequest};

        let root = tmp_root();
        let index = Arc::new(BlockIndex::new());
        let inflight = Arc::new(InFlightLoads::new());
        let node = NodeInfo {
            id: NodeId::new("w"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let observability = Arc::new(
            WorkerObservability::new(
                "c".into(),
                node,
                "127.0.0.1:8001".into(),
                1024,
                Arc::clone(&index),
                Arc::clone(&inflight),
            )
            .unwrap(),
        );
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        observability.readiness().set_control_registered(true);
        let backend: Arc<dyn BackendStore> = Arc::new(RampBackend);
        // 16-byte pages in a 64-byte block: a 40-byte read from offset 5 spans
        // three pages, partial at both ends.
        let worker = Arc::new(
            WorkerRuntime::new(
                WholeBlockStore::open(root.join("whole")).unwrap(),
                index,
                inflight,
                backend,
                64,
                0,
                observability.metrics().clone(),
            )
            .with_paged_store(talon_worker::PagedBlockStore::open(root.join("paged"), 16).unwrap())
            .with_page_gc(talon_worker::page_gc::PageGcConfig {
                tti_ms: 1,
                checkpoint_interval_ms: 1,
                ..Default::default()
            })
            .unwrap(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_worker = Arc::clone(&worker);
        let srv_obs = Arc::clone(&observability);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_conn(stream, srv_worker, srv_obs).await;
        });

        let obj = ObjectId::new(talon_core::Backend::Azure, "c", "obj");
        let req = RangeRequest {
            object: obj,
            offset: 5,
            len: 40,
        };
        let mut client = TcpStream::connect(addr).await.unwrap();
        let expected: Vec<u8> = (0..40u64).map(|i| ((5 + i) % 251) as u8).collect();

        for pass in 0..3 {
            if pass == 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert_eq!(worker.gc_once().await.bytes, 48);
                assert_eq!(worker.resident_bytes(), 0);
            }
            let out = encode_request(0, &req).unwrap();
            client.write_all(&out).await.unwrap();
            client.flush().await.unwrap();

            let mut hdr = [0u8; HEADER_LEN];
            client.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            assert!(
                !header.flags.contains(talon_transport::Flags::ERROR),
                "pass {pass}: worker returned an error frame"
            );
            assert_eq!(
                header.length as usize,
                expected.len(),
                "pass {pass}: advertised length must equal the request"
            );
            let mut body = vec![0u8; header.length as usize];
            client.read_exact(&mut body).await.unwrap();
            assert_eq!(body, expected, "pass {pass}: cross-page bytes must match");
        }

        drop(client);
        server.await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    /// A backend that stores whole-object PUTs in memory so a write-through can be
    /// read back, and records deletes.
    #[derive(Default)]
    struct StoringBackend {
        objects: std::sync::Mutex<std::collections::HashMap<String, Bytes>>,
    }

    #[async_trait]
    impl BackendStore for StoringBackend {
        async fn fetch_range(&self, object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
            let objs = self.objects.lock().unwrap();
            let full = objs
                .get(&object.to_path())
                .ok_or_else(|| Error::NotFound(object.to_path()))?;
            let start = offset as usize;
            let end = (start + len as usize).min(full.len());
            Ok(full.slice(start..end))
        }
        async fn fetch_range_if_match(
            &self,
            object: &ObjectId,
            offset: u64,
            len: u64,
            if_match: Option<&Version>,
        ) -> Result<Bytes> {
            if let Some(expected) = if_match {
                if expected.as_str() != "stored-v1" {
                    return Err(Error::VersionMismatch {
                        expected: expected.0.clone(),
                        found: "stored-v1".into(),
                    });
                }
            }
            self.fetch_range(object, offset, len).await
        }
        async fn head(&self, object: &ObjectId) -> Result<ObjectStat> {
            let objs = self.objects.lock().unwrap();
            let full = objs
                .get(&object.to_path())
                .ok_or_else(|| Error::NotFound(object.to_path()))?;
            Ok(ObjectStat {
                len: full.len() as u64,
                version: Version::new("stored-v1"),
            })
        }
        async fn put(&self, object: &ObjectId, body: Bytes) -> Result<Version> {
            self.objects.lock().unwrap().insert(object.to_path(), body);
            Ok(Version::new("stored-v1"))
        }
        async fn delete(&self, object: &ObjectId) -> Result<()> {
            self.objects.lock().unwrap().remove(&object.to_path());
            Ok(())
        }
    }

    #[tokio::test]
    async fn handle_conn_write_through_then_read_back() {
        use talon_transport::data::{encode_put_header, encode_request, PutRequest, RangeRequest};

        let root = tmp_root();
        let index = Arc::new(BlockIndex::new());
        let inflight = Arc::new(InFlightLoads::new());
        let node = NodeInfo {
            id: NodeId::new("w"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let observability = Arc::new(
            WorkerObservability::new(
                "c".into(),
                node,
                "127.0.0.1:8001".into(),
                1024,
                Arc::clone(&index),
                Arc::clone(&inflight),
            )
            .unwrap(),
        );
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        observability.readiness().set_control_registered(true);
        let backend = Arc::new(StoringBackend::default());
        let worker = Arc::new(WorkerRuntime::new(
            WholeBlockStore::open(&root).unwrap(),
            index,
            inflight,
            Arc::clone(&backend) as Arc<dyn BackendStore>,
            256 << 20, // block_size big enough for the object
            0,
            observability.metrics().clone(),
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_worker = Arc::clone(&worker);
        let srv_obs = Arc::clone(&observability);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_conn(stream, srv_worker, srv_obs).await;
        });

        let obj = ObjectId::new(talon_core::Backend::Azure, "c", "written.bin");
        let object_bytes = bytes::Bytes::from_static(b"hello written object");
        let mut client = TcpStream::connect(addr).await.unwrap();

        // 1) PUT: header frame (object + body_len) then the raw object bytes.
        let put = PutRequest {
            object: obj.clone(),
            body_len: object_bytes.len() as u64,
        };
        let hdr = encode_put_header(0, &put).unwrap();
        client.write_all(&hdr).await.unwrap();
        client.write_all(&object_bytes).await.unwrap();
        client.flush().await.unwrap();
        // Reply OK carrying the committed version.
        let mut rhdr = [0u8; HEADER_LEN];
        client.read_exact(&mut rhdr).await.unwrap();
        let rheader = FrameHeader::decode(&rhdr).unwrap();
        assert!(!rheader.flags.contains(talon_transport::Flags::ERROR));
        let mut vbody = vec![0u8; rheader.length as usize];
        client.read_exact(&mut vbody).await.unwrap();
        assert_eq!(&vbody, b"stored-v1");
        // The backend received the exact object bytes (write-through).
        assert_eq!(
            backend.objects.lock().unwrap().get(&obj.to_path()).unwrap(),
            &object_bytes
        );

        // 2) GetRange the same object: served from cache (read-after-write).
        let req = RangeRequest {
            object: obj.clone(),
            offset: 0,
            len: object_bytes.len() as u64,
        };
        let out = encode_request(0, &req).unwrap();
        client.write_all(&out).await.unwrap();
        client.flush().await.unwrap();
        let mut ghdr = [0u8; HEADER_LEN];
        client.read_exact(&mut ghdr).await.unwrap();
        let gheader = FrameHeader::decode(&ghdr).unwrap();
        assert!(!gheader.flags.contains(talon_transport::Flags::ERROR));
        let mut gbody = vec![0u8; gheader.length as usize];
        client.read_exact(&mut gbody).await.unwrap();
        assert_eq!(
            gbody, object_bytes,
            "read-after-write returns written bytes"
        );

        drop(client);
        server.await.unwrap();

        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn tokio_conn_admits_a_complete_block_without_origin_access() {
        use talon_core::BlockId;
        use talon_transport::data::{
            encode_cached_block_put_header, CachedBlockPutRequest, CachedRangeRequest,
        };

        let (worker, observability, _, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        observability.readiness().set_control_registered(true);
        let object = ObjectId::new(talon_core::Backend::Azure, "c", "admitted");
        let block = BlockId::new(object.clone(), 0, 8, Version::new("origin-v2"));
        let request = CachedBlockPutRequest {
            block,
            object_len: 8,
            body_len: 8,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_worker = Arc::clone(&worker);
        let server_observability = Arc::clone(&observability);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_conn(stream, server_worker, server_observability)
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(&encode_cached_block_put_header(7, &request).unwrap())
            .await
            .unwrap();
        client.write_all(b"gateway!").await.unwrap();
        let mut reply = [0_u8; HEADER_LEN];
        client.read_exact(&mut reply).await.unwrap();
        let reply = FrameHeader::decode(&reply).unwrap();
        assert_eq!(reply.length, 0);
        assert!(!reply.flags.contains(talon_transport::Flags::ERROR));
        assert_eq!(
            worker
                .serve_cached(&CachedRangeRequest {
                    object: object.clone(),
                    version: Version::new("origin-v2"),
                    offset: 0,
                    len: 8,
                })
                .await
                .unwrap(),
            Bytes::from_static(b"gateway!")
        );
        drop(client);
        server.await.unwrap();

        let truncated = CachedBlockPutRequest {
            block: BlockId::new(object.clone(), 0, 8, Version::new("truncated")),
            object_len: 8,
            body_len: 8,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_worker = Arc::clone(&worker);
        let server_observability = Arc::clone(&observability);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_conn(stream, server_worker, server_observability)
                .await
                .is_err()
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(&encode_cached_block_put_header(8, &truncated).unwrap())
            .await
            .unwrap();
        client.write_all(b"short").await.unwrap();
        client.shutdown().await.unwrap();
        assert!(server.await.unwrap());
        assert!(worker
            .serve_cached(&CachedRangeRequest {
                object,
                version: Version::new("truncated"),
                offset: 0,
                len: 1,
            })
            .await
            .is_err());
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn metadata_errors_preserve_typed_unavailability() {
        let (worker, obs, _, _) = test_worker(); // Deliberately not ready.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let task = tokio::spawn(async move { handle_conn(stream, worker, obs).await });
        let request = ControlMessage::StatObject {
            object: ObjectId::new(talon_core::Backend::Azure, "c", "o"),
        };
        client
            .write_all(&codec::encode(1, &request).unwrap())
            .await
            .unwrap();
        let reply = read_control(&mut client).await.unwrap().unwrap();
        assert!(matches!(
            reply,
            ControlMessage::ControlFailure {
                code: talon_transport::DataErrorCode::Unavailable,
                ..
            }
        ));
        drop(client);
        task.await.unwrap().unwrap();
    }

    fn test_worker() -> (
        Arc<WorkerRuntime>,
        Arc<WorkerObservability>,
        NodeInfo,
        PathBuf,
    ) {
        let root = tmp_root();
        let index = Arc::new(BlockIndex::new());
        let inflight = Arc::new(InFlightLoads::new());
        let node = NodeInfo {
            id: NodeId::new("worker-test"),
            address: "127.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let observability = Arc::new(
            WorkerObservability::new(
                "test-cluster".into(),
                node.clone(),
                "127.0.0.1:8001".into(),
                1024,
                Arc::clone(&index),
                Arc::clone(&inflight),
            )
            .unwrap(),
        );
        let backend: Arc<dyn BackendStore> = Arc::new(MockBackend {
            _calls: AtomicUsize::new(0),
        });
        let worker = Arc::new(WorkerRuntime::new(
            WholeBlockStore::open(&root).unwrap(),
            index,
            inflight,
            backend,
            8,
            0,
            observability.metrics().clone(),
        ));
        (worker, observability, node, root)
    }

    fn tmp_root() -> PathBuf {
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        std::thread::current().id().hash(&mut hasher);
        std::env::temp_dir().join(format!(
            "talon-control-{}-{}",
            std::process::id(),
            hasher.finish()
        ))
    }
}
