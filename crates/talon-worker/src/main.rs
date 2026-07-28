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
use std::time::{Duration, Instant};

use clap::Parser;
use talon_backend::{
    AzureBackend, AzureConfig, GcsBackend, GcsConfig, ReqwestClient, S3Backend, S3Config,
    S3Credentials,
};
use talon_core::{
    azure_sas_from_env, gcs_bearer_from_env, s3_secret_key_from_env, s3_session_token_from_env,
    BackendStore, NodeId, NodeInfo, NodeRole, WorkerConfig, WorkerConfigPatch,
};
use talon_transport::data;
use talon_transport::frame::{MsgType, HEADER_LEN};
use talon_transport::{codec, ControlMessage, FrameHeader};
use talon_worker::uring_conn;
use talon_worker::{
    send_file_range, serve_admin, BlockIndex, InFlightLoads, ServeOutcome, WholeBlockStore,
    WorkerObservability, WorkerRuntime, DEFAULT_CHUNK,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CONTROL_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);

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
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            block_size: self.block_size,
            data_plane_rings: self.data_plane_rings,
            cache_dirs: None,
            capacity_bytes: None,
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

/// Split an endpoint URL into `(host, tls)`: an `http://` scheme selects
/// plaintext, `https://` or a bare host keeps TLS.
fn split_scheme(endpoint: &str) -> (String, bool) {
    match endpoint.strip_prefix("http://") {
        Some(rest) => (rest.to_string(), false),
        None => (
            endpoint
                .strip_prefix("https://")
                .unwrap_or(endpoint)
                .to_string(),
            true,
        ),
    }
}

/// Build the Azure backend from account (config/env) + SAS (env only), honoring
/// an optional endpoint override (Azurite/proxy).
fn build_azure_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
) -> anyhow::Result<AzureBackend> {
    let account = cfg.azure_account.clone().ok_or_else(|| {
        anyhow::anyhow!("azure_account is required (set TALON_WORKER_AZURE_ACCOUNT)")
    })?;
    let sas = azure_sas_from_env()
        .ok_or_else(|| anyhow::anyhow!("TALON_WORKER_AZURE_SAS must be set (SAS token)"))?;
    let azure_config = match cfg.azure_endpoint.clone() {
        Some(endpoint) => {
            let (host, tls) = split_scheme(&endpoint);
            AzureConfig::emulator(account, host, tls)
        }
        None => AzureConfig::new(account),
    };
    Ok(AzureBackend::new(azure_config, Some(sas), http))
}

/// Build the S3 backend from region/endpoint/access-key (config) + secret key
/// and optional session token (env only).
fn build_s3_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
) -> anyhow::Result<S3Backend> {
    let region = cfg
        .s3_region
        .clone()
        .ok_or_else(|| anyhow::anyhow!("s3_region is required (set TALON_WORKER_S3_REGION)"))?;
    let access_key_id = cfg.s3_access_key_id.clone().ok_or_else(|| {
        anyhow::anyhow!("s3_access_key_id is required (set TALON_WORKER_S3_ACCESS_KEY_ID)")
    })?;
    let secret_access_key = s3_secret_key_from_env().ok_or_else(|| {
        anyhow::anyhow!("TALON_WORKER_S3_SECRET_ACCESS_KEY must be set (secret key)")
    })?;
    let mut config = S3Config::aws(&region);
    if let Some(endpoint) = cfg.s3_endpoint.clone() {
        let (host, tls) = split_scheme(&endpoint);
        config.endpoint = host;
        config.tls = tls;
    }
    if let Some(path_style) = cfg.s3_path_style {
        config.path_style = path_style;
    }
    let creds = S3Credentials {
        access_key_id,
        secret_access_key,
        session_token: s3_session_token_from_env(),
    };
    Ok(S3Backend::new(config, creds, http))
}

/// Build the GCS backend from an optional endpoint override (fake-gcs-server) +
/// bearer token (env only).
fn build_gcs_backend(
    cfg: &WorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
) -> anyhow::Result<GcsBackend> {
    let config = match cfg.gcs_endpoint.clone() {
        Some(endpoint) => {
            let (host, tls) = split_scheme(&endpoint);
            GcsConfig::emulator(host, tls)
        }
        None => GcsConfig::default(),
    };
    Ok(GcsBackend::new(config, gcs_bearer_from_env(), http))
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

    // Rebuild the in-memory index from blocks already on local disk so a restart
    // does not re-download the resident working set (issue #114).
    let index = Arc::new(BlockIndex::new());
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
    let observability = Arc::new(WorkerObservability::new(
        cfg.cluster_id.clone(),
        node.clone(),
        cfg.admin_listen.clone(),
        cfg.capacity_bytes,
        Arc::clone(&index),
        Arc::clone(&inflight),
    )?);
    observability.readiness().set_backend_ready(true);
    observability.readiness().set_store_ready(true);

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
    // reads its endpoint from config and its secret from the environment only.
    let backend_kind = cfg.backend.as_deref().unwrap_or("azure");
    let backend: Arc<dyn BackendStore> = match backend_kind {
        "azure" => Arc::new(build_azure_backend(&cfg, http)?),
        "s3" => Arc::new(build_s3_backend(&cfg, http)?),
        "gcs" => Arc::new(build_gcs_backend(&cfg, http)?),
        other => {
            anyhow::bail!("unknown TALON_WORKER_BACKEND {other:?}; expected azure, s3, or gcs")
        }
    };
    tracing::info!(backend = backend_kind, "object-store backend ready");

    let worker = Arc::new(WorkerRuntime::new(
        store,
        index,
        inflight,
        backend,
        cfg.block_size,
        cfg.capacity_bytes,
        observability.metrics().clone(),
    ));

    let admin_listener = TcpListener::bind(&cfg.admin_listen).await?;
    tracing::info!(listen = %cfg.admin_listen, "worker serving administration API");
    let admin_observability = Arc::clone(&observability);
    tokio::spawn(async move {
        if let Err(error) = serve_admin(admin_listener, admin_observability).await {
            tracing::error!(%error, "worker administration server stopped");
        }
    });

    let _control_plane = spawn_control_plane(
        cfg.coordinator.clone(),
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
async fn register_with_coordinator(coordinator: &str, node: &NodeInfo) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(coordinator).await?;
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
                    register_with_coordinator(&coordinator, &node),
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
                send_oneshot(&coordinator, &legacy).await?;
                send_oneshot(&coordinator, &status).await
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
async fn send_oneshot(addr: &str, msg: &ControlMessage) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let buf = codec::encode(0, msg)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

/// Serve data-plane range requests on one connection until EOF.
async fn handle_conn(
    mut stream: TcpStream,
    worker: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
) -> anyhow::Result<()> {
    let _active_connection = observability.metrics().track_connection();
    loop {
        let request_started = Instant::now();
        // Read one frame with a per-type size cap enforced BEFORE allocation and
        // a read timeout, so a peer cannot pin a 320 MiB buffer by advertising a
        // huge length and stalling (issue #111).
        let (header, payload) =
            match talon_transport::read_frame(&mut stream, talon_transport::DEFAULT_READ_TIMEOUT)
                .await
            {
                Ok(frame) => frame,
                Err(talon_transport::ReadFrameError::Eof) => return Ok(()),
                Err(talon_transport::ReadFrameError::Timeout) => {
                    tracing::debug!("worker: connection read timed out");
                    return Ok(());
                }
                Err(e) => return Err(anyhow::anyhow!(e)),
            };

        // A write (Put) or delete (Delete) is handled here and loops; only a
        // GetRange falls through to the read-serve path below.
        if header.msg_type == MsgType::Put {
            handle_put(
                &mut stream,
                &header,
                &payload,
                &worker,
                &observability,
                request_started,
            )
            .await?;
            continue;
        }
        if header.msg_type == MsgType::Delete {
            handle_delete(
                &mut stream,
                &header,
                &payload,
                &worker,
                &observability,
                request_started,
            )
            .await?;
            continue;
        }

        // A Control frame on the data plane carries StatObject (#318). Clients
        // already hold a data-plane connection, and only a worker has backend
        // credentials, so answering here avoids a second connection and avoids
        // giving the coordinator backend access.
        if header.msg_type == MsgType::Control {
            handle_control_frame(
                &mut stream,
                &header,
                &payload,
                &worker,
                &observability,
                request_started,
            )
            .await?;
            continue;
        }

        // Type check BEFORE any per-request work; a data listener only serves
        // GetRange (plus the Put/Delete/Control handled above); other frames are
        // capped tightly by read_frame.
        if header.msg_type != MsgType::GetRange {
            let err = data::encode_error(
                header.request_id,
                "worker only serves GetRange/Put/Delete/StatObject",
            );
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
        full.extend_from_slice(&header.encode());
        full.extend_from_slice(&payload);
        let (h, req) = match data::decode_request(&full) {
            Ok(v) => v,
            Err(e) => {
                let err = data::encode_error(header.request_id, &format!("bad request: {e}"));
                stream.write_all(&err).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
                continue;
            }
        };

        if !observability.is_ready() {
            let err = data::encode_error(h.request_id, "worker is not ready");
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            continue;
        }

        match worker.serve(&req).await {
            Ok(ServeOutcome::Sendfile(handle)) => {
                // Zero-copy hit: write the frame header async, then stream the
                // block file's fd straight into the socket with sendfile(2) on
                // the blocking pool. The header's advertised length equals the
                // handle length, so a short read can never desync the client.
                let len = handle.len;
                let hdr = data::response_header_ok(h.request_id, len as u32);
                stream.write_all(&hdr).await?;
                stream.flush().await?;
                match sendfile_payload(stream, handle).await {
                    Ok(returned) => {
                        stream = returned;
                        observability
                            .metrics()
                            .record_request_success(len, request_started.elapsed());
                    }
                    Err(error) => {
                        // The header is already on the wire, so we cannot send an
                        // error frame; the connection is desynced. Drop it.
                        observability
                            .metrics()
                            .record_request_error(request_started.elapsed());
                        return Err(error);
                    }
                }
            }
            Ok(ServeOutcome::Bytes(bytes)) => {
                let hdr = data::response_header_ok(h.request_id, bytes.len() as u32);
                stream.write_all(&hdr).await?;
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_success(bytes.len() as u64, request_started.elapsed());
            }
            Err(e) => {
                let err = data::encode_error(h.request_id, &e.to_string());
                stream.write_all(&err).await?;
                stream.flush().await?;
                observability
                    .metrics()
                    .record_request_error(request_started.elapsed());
            }
        }
    }
}

/// Handle a `Control` frame on the data plane.
///
/// Only `StatObject` is served here (#318): a client must know an object's
/// version before it can address any block, and only a worker holds the backend
/// credentials needed to resolve it. Anything else gets an `Ack` naming what
/// was rejected, so a client sees the reason rather than a closed connection.
async fn handle_control_frame(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);

    let (h, message) = match codec::decode(&full) {
        Ok(v) => v,
        Err(e) => {
            let reply = codec::encode(
                header.request_id,
                &ControlMessage::Ack {
                    ok: false,
                    detail: Some(format!("bad control message: {e}")),
                },
            )?;
            stream.write_all(&reply).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };

    let reply = match message {
        ControlMessage::StatObject { object } => {
            if !observability.is_ready() {
                ControlMessage::Ack {
                    ok: false,
                    detail: Some("worker is not ready".into()),
                }
            } else {
                match worker.stat_object(&object).await {
                    Ok(stat) => ControlMessage::ObjectStat {
                        size: stat.len,
                        version: stat.version.as_str().to_string(),
                    },
                    Err(error) => ControlMessage::Ack {
                        ok: false,
                        detail: Some(error.to_string()),
                    },
                }
            }
        }
        other => ControlMessage::Ack {
            ok: false,
            detail: Some(format!(
                "worker serves only StatObject on the data plane, got {other:?}"
            )),
        },
    };

    let is_error = matches!(reply, ControlMessage::Ack { ok: false, .. });
    let buf = codec::encode(h.request_id, &reply)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    if is_error {
        observability
            .metrics()
            .record_request_error(request_started.elapsed());
    } else {
        observability
            .metrics()
            .record_request_success(0, request_started.elapsed());
    }
    Ok(())
}

/// Handle a `Put` frame: read the whole object body, write it through to the
/// backend, and cache it (#229).
///
/// The frame `payload` is the small bincode [`PutRequest`] header; the raw object
/// bytes (`body_len` of them) follow on the stream, so we read exactly that many
/// into memory (v1 objects are single-block) and hand them to
/// [`WorkerRuntime::write_object`], which PUTs to the origin then caches the
/// bytes. Replies OK with the committed version, or an `ERROR` frame.
async fn handle_put(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);
    let (h, req) = match data::decode_put_header(&full) {
        Ok(v) => v,
        Err(e) => {
            let err = data::encode_error(header.request_id, &format!("bad put: {e}"));
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };
    if !observability.is_ready() {
        let err = data::encode_error(h.request_id, "worker is not ready");
        stream.write_all(&err).await?;
        stream.flush().await?;
        return Ok(());
    }
    // Read exactly body_len raw object bytes that follow the header on the wire.
    let mut body = vec![0u8; req.body_len as usize];
    stream.read_exact(&mut body).await?;
    match worker
        .write_object(&req.object, bytes::Bytes::from(body))
        .await
    {
        Ok(version) => {
            // Reply OK; the body carries the committed version so the client can
            // record read-after-write consistency.
            let vbytes = version.as_str().as_bytes();
            let hdr = data::response_header_ok(h.request_id, vbytes.len() as u32);
            stream.write_all(&hdr).await?;
            stream.write_all(vbytes).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_success(req.body_len, request_started.elapsed());
        }
        Err(error) => {
            let err = data::encode_error(h.request_id, &error.to_string());
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(())
}

/// Handle a `Delete` frame: delete the object at the backend and evict locally.
async fn handle_delete(
    stream: &mut TcpStream,
    header: &FrameHeader,
    payload: &[u8],
    worker: &Arc<WorkerRuntime>,
    observability: &Arc<WorkerObservability>,
    request_started: Instant,
) -> anyhow::Result<()> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);
    let (h, req) = match data::decode_delete(&full) {
        Ok(v) => v,
        Err(e) => {
            let err = data::encode_error(header.request_id, &format!("bad delete: {e}"));
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
            return Ok(());
        }
    };
    if !observability.is_ready() {
        let err = data::encode_error(h.request_id, "worker is not ready");
        stream.write_all(&err).await?;
        stream.flush().await?;
        return Ok(());
    }
    match worker.delete_object(&req.object).await {
        Ok(()) => {
            let hdr = data::response_header_ok(h.request_id, 0);
            stream.write_all(&hdr).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_success(0, request_started.elapsed());
        }
        Err(error) => {
            let err = data::encode_error(h.request_id, &error.to_string());
            stream.write_all(&err).await?;
            stream.flush().await?;
            observability
                .metrics()
                .record_request_error(request_started.elapsed());
        }
    }
    Ok(())
}

/// Stream a resident block's bytes to the client with `sendfile(2)`.
///
/// `sendfile` is blocking and Linux-specific, and the tokio [`TcpStream`] is
/// non-blocking (a blocking `sendfile` on it would spuriously `EAGAIN`). So we
/// take the stream out of tokio ([`into_std`]), put the socket in blocking mode,
/// run the chunked [`send_file_range`] loop on the blocking helper pool
/// ([`spawn_blocking`]) — never on the async reactor, per DESIGN.md — then
/// restore non-blocking mode and hand the stream back for the next request.
///
/// [`into_std`]: tokio::net::TcpStream::into_std
/// [`spawn_blocking`]: tokio::task::spawn_blocking
async fn sendfile_payload(
    stream: TcpStream,
    handle: talon_core::BlockHandle,
) -> anyhow::Result<TcpStream> {
    let std_stream = stream.into_std()?;
    std_stream.set_nonblocking(false)?;
    let (std_stream, result) = tokio::task::spawn_blocking(move || {
        let res = send_file_range(
            &std_stream,
            &handle.fd,
            handle.offset,
            handle.len,
            DEFAULT_CHUNK,
        );
        (std_stream, res)
    })
    .await?;
    let sent = result?;
    if sent != handle.len {
        // sendfile hit EOF before the advertised length: the block file is
        // shorter than the index claimed. The header already promised `len`
        // bytes, so the connection is desynced — surface an error to drop it.
        anyhow::bail!(
            "sendfile short read: sent {sent} of {} bytes; block file truncated",
            handle.len
        );
    }
    std_stream.set_nonblocking(true)?;
    Ok(TcpStream::from_std(std_stream)?)
}

/// Read one framed control message (header + payload). `Ok(None)` on clean EOF.
async fn read_control(stream: &mut TcpStream) -> anyhow::Result<Option<ControlMessage>> {
    let mut header_buf = [0u8; HEADER_LEN];
    match stream.read_exact(&mut header_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let header = FrameHeader::decode(&header_buf)?;
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&payload);
    let (_h, msg) = codec::decode(&full)?;
    Ok(Some(msg))
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
    use tokio::sync::oneshot;

    use super::*;

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
