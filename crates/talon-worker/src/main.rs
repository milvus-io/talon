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
//! - Backend fetch is the real [`AzureBackend`] over [`ReqwestClient`]; the SAS
//!   token is read from the environment and **never logged**.
//!
//! The hot path serves cached blocks **zero-copy** via `sendfile(2)`
//! ([`send_file_range`](talon_worker::send_file_range)) straight from the
//! committed block file's descriptor into the client socket — the bytes never
//! enter user space and no per-request block buffer is allocated (issue #179). A
//! cache miss (bytes just fetched into memory) or a boundary-spanning read falls
//! back to writing the in-memory bytes inline.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use talon_backend::{
    endpoint_host, AzureBackend, AzureConfig, GcsBackend, GcsConfig, ReqwestClient, S3Backend,
    S3Config, S3Credentials,
};
use talon_core::{
    azure_sas_from_env, gcs_bearer_from_env, s3_secret_key_from_env, s3_session_token_from_env,
    Backend, BackendStore, NamespacePolicy, NodeId, NodeInfo, NodeRole, ObjectNamespace,
    WorkerConfig, WorkerConfigPatch, WorkloadIdentity, WorkloadRole,
};
use talon_metadata::MappingRevision;
use talon_transport::control_tls::ControlTlsChannel;
use talon_transport::{codec, ControlMessage};
use talon_worker::mapping_guard::MappingGuard;
use talon_worker::tokio_conn::{handle_conn, read_control};
use talon_worker::uring_conn;
use talon_worker::{
    serve_admin, BlockIndex, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerObservability,
    WorkerRuntime,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CONTROL_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const CONTROL_TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(5);

/// Upper bound on concurrent data-plane connections. Beyond this, new peers wait
/// for an in-flight connection to finish rather than each spawning an unbounded
/// task that could pin a payload buffer (issue #111).
const MAX_DATA_PLANE_CONNECTIONS: usize = 1024;

/// Blocking helper threads per io_uring ring, for the zero-copy `sendfile`
/// path. Kept small because every ring has its own pool and the rings are
/// pinned: a large pool per ring would oversubscribe the cores they sit on.
const URING_BLOCKING_THREADS_PER_RING: usize = 4;

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
}

impl talon_backend::CredentialsObserver for CredentialsMetricsObserver {
    fn refresh_succeeded(&self, expires_at: Option<std::time::SystemTime>) {
        self.observability
            .metrics()
            .record_origin_credentials("refresh_success");
        let unix_seconds = expires_at
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
    /// Stable node identity; defaults to the RPC listen address.
    #[arg(long)]
    node_id: Option<String>,
    /// Control-plane heartbeat interval in milliseconds.
    #[arg(long)]
    heartbeat_interval_ms: Option<u64>,
    /// Logical block size in bytes.
    #[arg(long)]
    block_size: Option<u32>,
    /// Serve the data plane on N io_uring rings (thread-per-core).
    ///
    /// `0` (the default) means one ring per available core. The worker falls
    /// back to the Tokio data plane automatically if io_uring is unavailable;
    /// set TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1 to pin it explicitly.
    #[arg(long)]
    data_plane_rings: Option<usize>,
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
            cache_dirs: None,
            capacity_bytes: None,
            l1_capacity_bytes: None,
            l1_page_size_bytes: None,
            l2_page_size_bytes: None,
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
            s3_path_style: None,
            gcs_endpoint: None,
        }
    }
}

/// Explicit origin credential mechanism (`static`, `aws-web-identity`,
/// `aliyun-oidc`, `tencent-oidc`, `huawei-agency`, `gcp-metadata`); unset
/// means static material first, then auto-detected workload identity. Read
/// from the environment like the secrets themselves.
fn origin_credentials_source() -> Option<String> {
    std::env::var("TALON_ORIGIN_CREDENTIALS_SOURCE")
        .ok()
        .filter(|value| !value.is_empty())
}

/// Build the Azure backend from account (config/env) plus a SAS token (env)
/// or, when no SAS is set, AKS workload identity, honoring an optional
/// endpoint override (Azurite/proxy).
async fn build_azure_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
    observer: Arc<dyn talon_backend::CredentialsObserver>,
) -> anyhow::Result<AzureBackend> {
    let account = cfg.azure_account.clone().ok_or_else(|| {
        anyhow::anyhow!("azure_account is required (set TALON_WORKER_AZURE_ACCOUNT)")
    })?;
    let azure_config = match cfg.azure_endpoint.clone() {
        Some(endpoint) => {
            let (host, tls) = endpoint_host(&endpoint);
            AzureConfig::emulator(account, host, tls)
        }
        None => AzureConfig::new(account),
    };
    match azure_sas_from_env() {
        Some(sas) => {
            tracing::info!(source = "sas", "origin Azure credentials resolved");
            Ok(AzureBackend::new(azure_config, Some(sas), http))
        }
        None => {
            let resolved = talon_backend::resolve_azure_bearer(Arc::clone(&http), observer)
                .await
                .map_err(|error| {
                    anyhow::anyhow!(
                        "TALON_WORKER_AZURE_SAS must be set (SAS token), or AKS \
                         workload identity must be available: {error}"
                    )
                })?;
            tracing::info!(
                source = resolved.source,
                "origin Azure credentials resolved"
            );
            Ok(AzureBackend::with_bearer_provider(
                azure_config,
                resolved.provider,
                http,
            ))
        }
    }
}

/// Build the S3 backend from region/endpoint (config) plus either static keys
/// (config + env) or a cloud workload identity (EKS IRSA, ACK RRSA, TKE OIDC
/// auto-detected; Huawei agency via `TALON_ORIGIN_CREDENTIALS_SOURCE`).
async fn build_s3_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
    observer: Arc<dyn talon_backend::CredentialsObserver>,
) -> anyhow::Result<S3Backend> {
    let region = cfg
        .s3_region
        .clone()
        .ok_or_else(|| anyhow::anyhow!("s3_region is required (set TALON_WORKER_S3_REGION)"))?;
    let static_credentials = match (cfg.s3_access_key_id.clone(), s3_secret_key_from_env()) {
        (Some(access_key_id), Some(secret_access_key)) => Some(S3Credentials {
            access_key_id,
            secret_access_key,
            session_token: s3_session_token_from_env(),
        }),
        (None, None) => None,
        (Some(_), None) => {
            anyhow::bail!("TALON_WORKER_S3_SECRET_ACCESS_KEY must be set (secret key)")
        }
        (None, Some(_)) => {
            anyhow::bail!("s3_access_key_id is required (set TALON_WORKER_S3_ACCESS_KEY_ID)")
        }
    };
    let mut config = S3Config::aws(&region);
    if let Some(endpoint) = cfg.s3_endpoint.clone() {
        let (host, tls) = endpoint_host(&endpoint);
        config.endpoint = host;
        config.tls = tls;
    }
    if let Some(path_style) = cfg.s3_path_style {
        config.path_style = path_style;
    }
    let resolved = talon_backend::resolve_s3_credentials(
        static_credentials,
        origin_credentials_source().as_deref(),
        Arc::clone(&http),
        observer,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(source = resolved.source, "origin S3 credentials resolved");
    Ok(S3Backend::with_credentials_provider(
        config,
        resolved.provider,
        http,
    ))
}

/// Build the GCS backend from an optional endpoint override (fake-gcs-server)
/// plus a bearer token (env) or the GKE metadata service
/// (`TALON_ORIGIN_CREDENTIALS_SOURCE=gcp-metadata`).
async fn build_gcs_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
    observer: Arc<dyn talon_backend::CredentialsObserver>,
) -> anyhow::Result<GcsBackend> {
    let config = match cfg.gcs_endpoint.clone() {
        Some(endpoint) => {
            let (host, tls) = endpoint_host(&endpoint);
            GcsConfig::emulator(host, tls)
        }
        None => GcsConfig::default(),
    };
    let resolved = talon_backend::resolve_gcs_bearer(
        gcs_bearer_from_env(),
        origin_credentials_source().as_deref(),
        Arc::clone(&http),
        observer,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(source = resolved.source, "origin GCS credentials resolved");
    Ok(GcsBackend::with_bearer_provider(
        config,
        resolved.provider,
        http,
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let file = match &args.config {
        Some(path) => WorkerConfigPatch::from_file(path)?,
        None => WorkerConfigPatch::default(),
    };
    let env = WorkerConfigPatch::from_env()?;
    let cli = args.into_patch();
    let cfg = WorkerConfig::resolve(file, env, cli)?;

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
    let store = WholeBlockStore::open(&root)?;
    // Paged L2 is opt-in: with `l2_page_size_bytes` set, a miss materializes only
    // the pages a read touches, under `<root>/paged`, instead of whole blocks.
    let paged = match u32::try_from(cfg.l2_page_size_bytes) {
        Ok(page_size) if page_size > 0 => {
            Some(PagedBlockStore::open(root.join("paged"), page_size)?)
        }
        _ => None,
    };

    // Rebuild the in-memory index from blocks already on local disk so a restart
    // does not re-download the resident working set (issue #114).
    let index = Arc::new(BlockIndex::new());
    if let Some(paged) = &paged {
        // Rebuild paged entries first; their present bitmaps come from the page
        // files actually on disk.
        match paged.scan() {
            Ok(metas) => {
                let count = metas.len();
                for meta in metas {
                    index.commit(meta);
                }
                if count > 0 {
                    tracing::info!(
                        blocks = count,
                        pages = index.page_count(),
                        "rebuilt paged block index from on-disk cache"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to scan on-disk paged cache");
            }
        }
    }
    match store.scan() {
        Ok(metas) => {
            let count = metas.len();
            for meta in metas {
                index.commit(meta);
            }
            if count > 0 {
                tracing::info!(
                    blocks = count,
                    resident_bytes = index.resident_bytes(),
                    "rebuilt block index from on-disk cache"
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to scan on-disk cache; starting with an empty index");
        }
    }
    let inflight = Arc::new(InFlightLoads::new());
    let backend_kind = cfg.backend.as_deref().unwrap_or("azure");
    let configured_backend: Backend = backend_kind.parse().map_err(|error| {
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
    observability.readiness().set_store_ready(true);
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
    let http: Arc<dyn talon_backend::http::HttpClient> = {
        let base: Arc<dyn talon_backend::http::HttpClient> = Arc::new(ReqwestClient::new());

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
    // reads its endpoint from config and its credentials from the environment
    // only — static secrets, or a cloud workload identity refreshed in the
    // background and counted through the worker registry.
    let credentials_observer: Arc<dyn talon_backend::CredentialsObserver> =
        Arc::new(CredentialsMetricsObserver {
            observability: Arc::clone(&observability),
        });
    let backend: Arc<dyn BackendStore> = match backend_kind {
        "azure" => Arc::new(build_azure_backend(&cfg, http, credentials_observer).await?),
        "s3" => Arc::new(build_s3_backend(&cfg, http, credentials_observer).await?),
        "gcs" => Arc::new(build_gcs_backend(&cfg, http, credentials_observer).await?),
        other => {
            anyhow::bail!("unknown TALON_WORKER_BACKEND {other:?}; expected azure, s3, or gcs")
        }
    };
    tracing::info!(backend = backend_kind, "object-store backend ready");

    let mut runtime = WorkerRuntime::new_with_l1(
        store,
        index,
        inflight,
        backend,
        cfg.block_size,
        cfg.capacity_bytes,
        cfg.l1_capacity_bytes,
        cfg.l1_page_size_bytes,
        observability.metrics().clone(),
    )
    .with_backend_kind(configured_backend);
    if let Some(paged) = paged {
        runtime = runtime.with_paged_store(paged);
    }
    let worker = Arc::new(runtime);

    let admin_listener = TcpListener::bind(&cfg.admin_listen).await?;
    tracing::info!(listen = %cfg.admin_listen, "worker serving administration API");
    let admin_observability = Arc::clone(&observability);
    tokio::spawn(async move {
        if let Err(error) = serve_admin(admin_listener, admin_observability).await {
            tracing::error!(%error, "worker administration server stopped");
        }
    });

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
        tokio::spawn(async move {
            if let Err(error) = serve_control(
                listener,
                channel,
                cluster_id,
                worker_id,
                worker_incarnation,
                policy,
                guard,
            )
            .await
            {
                tracing::error!(%error, "worker coordinator mTLS control plane stopped");
            }
        });
    }

    let _control_plane = spawn_control_plane(
        cfg.coordinator.clone(),
        control_tls,
        node,
        Arc::clone(&worker),
        Arc::clone(&observability),
        Duration::from_millis(cfg.heartbeat_interval_ms),
    );

    // Serve the data plane, on io_uring rings if configured (#285) or on the
    // portable Tokio path otherwise.
    // The io_uring data plane is the default. Fall back to the portable Tokio
    // path when the host cannot run it (older kernel, restrictive seccomp, some
    // container runtimes) or when an operator pins the legacy path explicitly.
    let force_tokio = std::env::var(FORCE_TOKIO_ENV)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let uring_available = talon_worker::uring_serve::io_uring_available();
    if force_tokio {
        tracing::info!("{FORCE_TOKIO_ENV} is set; serving the data plane on the Tokio path");
    } else if !uring_available {
        tracing::warn!(
            "io_uring is unavailable on this host; falling back to the Tokio \
             data plane. Performance will be lower under high connection counts."
        );
    }
    if !force_tokio && uring_available {
        let rings = talon_worker::uring_serve::resolve_ring_count(cfg.data_plane_rings);
        tracing::info!(
            listen = %cfg.listen,
            rings,
            "worker serving data plane on io_uring rings"
        );
        // `serve` blocks until the ring threads exit, and it must run off the
        // Tokio worker threads it is handing out — so drive it on a dedicated
        // thread and park this task on the join.
        let addr = cfg.listen.clone();
        // The cap is per ring, so divide the global budget across them; the
        // total admitted stays MAX_DATA_PLANE_CONNECTIONS regardless of how many
        // rings are configured (#111).
        let per_ring_connections = MAX_DATA_PLANE_CONNECTIONS.div_ceil(rings).max(1);
        let handler = uring_conn::RingConnHandler::new(
            Arc::clone(&worker),
            Arc::clone(&observability),
            per_ring_connections,
        );
        let tokio_handle = tokio::runtime::Handle::current();
        let joined = tokio::task::spawn_blocking(move || {
            talon_worker::uring_serve::serve(
                addr,
                rings,
                URING_BLOCKING_THREADS_PER_RING,
                handler,
                tokio_handle,
            )
        })
        .await?;
        return joined;
    }

    let listener = TcpListener::bind(&cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, "worker serving data plane");
    // Bound concurrent connections so a flood of idle peers cannot exhaust
    // memory/FDs (issue #111).
    let conn_limit = talon_transport::ConnectionLimit::new(MAX_DATA_PLANE_CONNECTIONS);
    loop {
        let permit = conn_limit.acquire().await;
        let (stream, peer) = listener.accept().await?;
        let worker = Arc::clone(&worker);
        let observability = Arc::clone(&observability);
        tokio::spawn(async move {
            // Hold the permit for the connection's lifetime.
            let _permit = permit;
            if let Err(e) = handle_conn(stream, worker, observability).await {
                tracing::debug!(%peer, error = %e, "worker: connection ended");
            }
        });
    }
}

/// Open a control connection to the coordinator and send `Register`.
async fn register_with_coordinator(
    coordinator: &str,
    channel: Option<&ControlTlsChannel>,
    node: &NodeInfo,
) -> anyhow::Result<()> {
    if let Some(channel) = channel {
        let authenticated = channel.connect(coordinator).await?;
        tracing::debug!(identity = %authenticated.identity, "connected to coordinator mTLS control plane");
        return register_on_stream(authenticated.stream, coordinator, node).await;
    }
    register_on_stream(TcpStream::connect(coordinator).await?, coordinator, node).await
}

async fn register_on_stream<S>(
    mut stream: S,
    coordinator: &str,
    node: &NodeInfo,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let buf = codec::encode(0, &ControlMessage::Register { node: node.clone() })?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    match read_control(&mut stream).await? {
        Some(ControlMessage::Ack {
            ok: true,
            detail: _,
        }) => {}
        Some(ControlMessage::Ack { ok: false, detail }) => {
            anyhow::bail!("coordinator rejected registration: {detail:?}")
        }
        Some(other) => anyhow::bail!("unexpected coordinator registration reply: {other:?}"),
        None => anyhow::bail!("coordinator closed registration connection without an Ack"),
    }
    tracing::info!(%coordinator, "registered with coordinator");
    Ok(())
}

/// Maintain registration and send legacy plus versioned status heartbeats.
fn spawn_control_plane(
    coordinator: String,
    channel: Option<ControlTlsChannel>,
    node: NodeInfo,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
    heartbeat_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut registered = false;
        loop {
            ticker.tick().await;

            if !registered {
                match tokio::time::timeout(
                    CONTROL_OPERATION_TIMEOUT,
                    register_with_coordinator(&coordinator, channel.as_ref(), &node),
                )
                .await
                {
                    Ok(Ok(())) => {
                        registered = true;
                        observability.readiness().set_control_registered(true);
                    }
                    Ok(Err(error)) => {
                        observability.metrics().record_heartbeat_failure();
                        observability.readiness().set_control_registered(false);
                        tracing::warn!(%error, "worker registration failed; retrying");
                        continue;
                    }
                    Err(_) => {
                        observability.metrics().record_heartbeat_failure();
                        observability.readiness().set_control_registered(false);
                        tracing::warn!("worker registration timed out; retrying");
                        continue;
                    }
                }
            }

            let legacy = ControlMessage::Heartbeat {
                node: node.id.clone(),
                block_count: worker.block_count(),
            };
            let status = ControlMessage::NodeStatusHeartbeat {
                status: Box::new(observability.status()),
            };
            let heartbeat = tokio::time::timeout(CONTROL_OPERATION_TIMEOUT, async {
                send_oneshot(&coordinator, channel.as_ref(), &legacy).await?;
                send_oneshot(&coordinator, channel.as_ref(), &status).await
            })
            .await;
            match heartbeat {
                Ok(Ok(())) => observability.metrics().record_heartbeat_success(),
                Ok(Err(error)) => {
                    registered = false;
                    observability.metrics().record_heartbeat_failure();
                    observability.readiness().set_control_registered(false);
                    tracing::warn!(%error, "control heartbeat failed; registration will retry");
                }
                Err(_) => {
                    registered = false;
                    observability.metrics().record_heartbeat_failure();
                    observability.readiness().set_control_registered(false);
                    tracing::warn!("control heartbeat timed out; registration will retry");
                }
            }
        }
    })
}

/// Connect, send one control message, and drop (fire-and-forget over TCP).
async fn send_oneshot(
    addr: &str,
    channel: Option<&ControlTlsChannel>,
    msg: &ControlMessage,
) -> anyhow::Result<()> {
    if let Some(channel) = channel {
        return send_on_stream(channel.connect(addr).await?.stream, msg).await;
    }
    send_on_stream(TcpStream::connect(addr).await?, msg).await
}

async fn send_on_stream<S>(mut stream: S, msg: &ControlMessage) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let buf = codec::encode(0, msg)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

async fn serve_control(
    listener: TcpListener,
    channel: ControlTlsChannel,
    cluster_id: String,
    worker_id: String,
    worker_incarnation: String,
    policy: Option<NamespacePolicy>,
    guard: Arc<MappingGuard>,
) -> anyhow::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let channel = channel.clone();
        let cluster_id = cluster_id.clone();
        let worker_id = worker_id.clone();
        let worker_incarnation = worker_incarnation.clone();
        let policy = policy.clone();
        let guard = Arc::clone(&guard);
        tokio::spawn(async move {
            let result = async {
                let mut authenticated = channel.accept(stream).await?;
                tracing::debug!(identity = %authenticated.identity, %peer, "accepted coordinator mTLS connection");
                if let Some(message) = read_control(&mut authenticated.stream).await? {
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
    }
}

fn handle_revision_update(
    message: ControlMessage,
    peer: &WorkloadIdentity,
    cluster_id: &str,
    worker_id: &str,
    worker_incarnation: &str,
    policy: Option<&NamespacePolicy>,
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

    fn coordinator_identity(cluster_id: &str, node_id: &str) -> WorkloadIdentity {
        WorkloadIdentity::new(
            "cluster.example",
            cluster_id,
            WorkloadRole::Coordinator,
            node_id,
        )
        .unwrap()
    }

    fn revision_policy() -> NamespacePolicy {
        NamespacePolicy::from_toml(
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
    async fn control_plane_sends_legacy_and_versioned_heartbeats() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap();
        let (messages_tx, messages_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut messages = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let message = read_control(&mut stream).await.unwrap().unwrap();
                if matches!(message, ControlMessage::Register { .. }) {
                    let ack = codec::encode(
                        0,
                        &ControlMessage::Ack {
                            ok: true,
                            detail: None,
                        },
                    )
                    .unwrap();
                    stream.write_all(&ack).await.unwrap();
                    stream.flush().await.unwrap();
                }
                messages.push(message);
            }
            messages_tx.send(messages).unwrap();
        });

        let (worker, observability, node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let control = spawn_control_plane(
            coordinator.to_string(),
            None,
            node.clone(),
            worker,
            Arc::clone(&observability),
            Duration::from_secs(60),
        );

        let messages = tokio::time::timeout(Duration::from_secs(2), messages_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &messages[0],
            ControlMessage::Register { node: registered } if registered == &node
        ));
        assert!(matches!(
            &messages[1],
            ControlMessage::Heartbeat {
                node: heartbeat_node,
                block_count: 0
            } if heartbeat_node == &node.id
        ));
        match &messages[2] {
            ControlMessage::NodeStatusHeartbeat { status } => {
                status.validate().unwrap();
                assert_eq!(status.node, node);
                assert!(status.ready);
            }
            other => panic!("unexpected status heartbeat: {other:?}"),
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(observability.is_ready());
        assert!(observability
            .metrics()
            .render()
            .contains("talon_worker_control_heartbeat_total{result=\"success\"} 1"));

        control.abort();
        server.await.unwrap();
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn registration_failure_keeps_worker_unready_and_is_counted() {
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = unused.local_addr().unwrap();
        drop(unused);

        let (worker, observability, node, root) = test_worker();
        observability.readiness().set_backend_ready(true);
        observability.readiness().set_store_ready(true);
        let control = spawn_control_plane(
            coordinator.to_string(),
            None,
            node,
            worker,
            Arc::clone(&observability),
            Duration::from_secs(60),
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

        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: u64::MAX,
                version: Version::new("v1"),
            })
        }
    }

    #[tokio::test]
    async fn handle_conn_serves_a_hit_via_sendfile_byte_exact() {
        use talon_transport::data::{encode_request, RangeRequest};

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

        for _ in 0..2 {
            let out = encode_request(0, &req).unwrap();
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
