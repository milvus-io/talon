// SPDX-License-Identifier: Apache-2.0
//! Talon async worker entry point.
//!
//! Registers with the coordinator as [`NodeRole::AsyncWorker`], then serves
//! data-plane range requests from the extent cache. On a miss it fetches
//! **exactly the requested range** from the origin — not a block-aligned
//! superset — which is the whole point: a 4KB Parquet footer read costs 4KB.
//!
//! # How this differs from `talon-worker`
//!
//! - Placement resolves on a **separate rendezvous ring** keyed on the object
//!   identity, so every range of one object lands on one worker. The
//!   coordinator selects it by node role; see ADR 0005 §6.
//! - The NVMe tier survives a restart by **checkpointing** its extent map, so
//!   the cache directory is recovered rather than wiped (ADR 0005 §7). Setting
//!   `checkpoint_interval_bytes = 0` turns that off and restores the wipe.
//! - **Reads only.** A write or delete is refused with an error frame, after
//!   the body is drained so the connection stays in sync (ADR 0005 §8).
//! - The data plane runs on Tokio only. There is no io_uring path here: the
//!   zero-copy win comes from `sendfile` off a pinned region descriptor, which
//!   the Tokio path already does.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use talon_async_worker::cache::tiered::{ExtentCacheConfig, TieredExtentCache};
use talon_async_worker::{
    handle_conn, serve_admin, AsyncWorkerMetrics, AsyncWorkerObservability, AsyncWorkerRuntime,
};
use talon_backend::{
    AzureBackend, AzureConfig, GcsBackend, GcsConfig, ReqwestClient, S3Backend, S3Config,
    S3Credentials,
};
use talon_core::{
    async_azure_sas_from_env, async_gcs_bearer_from_env, async_s3_secret_key_from_env,
    async_s3_session_token_from_env, AsyncWorkerConfig, AsyncWorkerConfigPatch, Backend,
    BackendStore, NodeId, NodeInfo, NodeRole, WorkloadIdentity, WorkloadRole,
};
use talon_transport::control_tls::ControlTlsChannel;
use talon_transport::{codec, ControlMessage};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CONTROL_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const CONTROL_TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(5);

/// Upper bound on concurrent data-plane connections, so a flood of idle peers
/// cannot exhaust memory or file descriptors.
const MAX_DATA_PLANE_CONNECTIONS: usize = 1024;

/// Command-line arguments for a Talon async worker.
#[derive(Debug, Parser)]
#[command(name = "talon-async-worker", version, about)]
struct Args {
    /// Path to a TOML config file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Address to bind the data plane to.
    #[arg(long)]
    listen: Option<String>,
    /// Routable address advertised to clients (defaults to `listen`).
    #[arg(long)]
    advertise_addr: Option<String>,
    /// Address to bind the HTTP administration service to.
    #[arg(long)]
    admin_listen: Option<String>,
    /// Address of the coordinator to register with.
    #[arg(long)]
    coordinator: Option<String>,
    /// Dedicated coordinator-initiated mTLS control bind address.
    #[arg(long)]
    control_listen: Option<String>,
    /// Logical cluster advertised in node status.
    #[arg(long)]
    cluster_id: Option<String>,
    /// Stable node identity; defaults to the advertised address.
    #[arg(long)]
    node_id: Option<String>,
    /// Directory for the region-packed shard files. Wiped at startup.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// NVMe ceiling in bytes.
    #[arg(long)]
    capacity_bytes: Option<u64>,
    /// DRAM tier ceiling in bytes. Zero disables L1.
    #[arg(long)]
    l1_capacity_bytes: Option<u64>,
    /// Object-store backend: azure, s3, or gcs.
    #[arg(long)]
    backend: Option<String>,
    /// Azure Blob storage account.
    #[arg(long)]
    azure_account: Option<String>,
    /// Azure endpoint host override.
    #[arg(long)]
    azure_endpoint: Option<String>,
    /// S3 region.
    #[arg(long)]
    s3_region: Option<String>,
    /// S3 endpoint host override.
    #[arg(long)]
    s3_endpoint: Option<String>,
    /// S3 access key id. The secret key is environment-only.
    #[arg(long)]
    s3_access_key_id: Option<String>,
    /// GCS endpoint host override.
    #[arg(long)]
    gcs_endpoint: Option<String>,
}

impl Args {
    fn into_patch(self) -> AsyncWorkerConfigPatch {
        AsyncWorkerConfigPatch {
            listen: self.listen,
            advertise_addr: self.advertise_addr,
            admin_listen: self.admin_listen,
            coordinator: self.coordinator,
            control_listen: self.control_listen,
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            cache_dir: self.cache_dir,
            capacity_bytes: self.capacity_bytes,
            l1_capacity_bytes: self.l1_capacity_bytes,
            backend: self.backend,
            azure_account: self.azure_account,
            azure_endpoint: self.azure_endpoint,
            s3_region: self.s3_region,
            s3_endpoint: self.s3_endpoint,
            s3_access_key_id: self.s3_access_key_id,
            gcs_endpoint: self.gcs_endpoint,
            // The rest are file- and environment-only (`cli: false` in the
            // ConfigVar schema), so the CLI patch leaves them unset. Listing
            // them rather than using `..Default::default()` is deliberate: a
            // new field then fails to compile here instead of silently
            // becoming unreachable from the command line.
            control_tls: None,
            heartbeat_interval_ms: None,
            disk_shards: None,
            checksums_enabled: None,
            l1_shards: None,
            checkpoint_interval_bytes: None,
            s3_path_style: None,
            backend_max_retries: None,
            backend_retry_base_ms: None,
            backend_retry_max_delay_ms: None,
            backend_timeout_floor_ms: None,
            backend_min_throughput_bytes: None,
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

fn build_azure_backend(
    cfg: &AsyncWorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
) -> anyhow::Result<AzureBackend> {
    let account = cfg.azure_account.clone().ok_or_else(|| {
        anyhow::anyhow!("azure_account is required (set TALON_ASYNC_WORKER_AZURE_ACCOUNT)")
    })?;
    let sas = async_azure_sas_from_env()
        .ok_or_else(|| anyhow::anyhow!("TALON_ASYNC_WORKER_AZURE_SAS must be set (SAS token)"))?;
    let azure_config = match cfg.azure_endpoint.clone() {
        Some(endpoint) => {
            let (host, tls) = split_scheme(&endpoint);
            AzureConfig::emulator(account, host, tls)
        }
        None => AzureConfig::new(account),
    };
    Ok(AzureBackend::new(azure_config, Some(sas), http))
}

fn build_s3_backend(
    cfg: &AsyncWorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
) -> anyhow::Result<S3Backend> {
    let region = cfg.s3_region.clone().ok_or_else(|| {
        anyhow::anyhow!("s3_region is required (set TALON_ASYNC_WORKER_S3_REGION)")
    })?;
    let access_key_id = cfg.s3_access_key_id.clone().ok_or_else(|| {
        anyhow::anyhow!("s3_access_key_id is required (set TALON_ASYNC_WORKER_S3_ACCESS_KEY_ID)")
    })?;
    let secret_access_key = async_s3_secret_key_from_env().ok_or_else(|| {
        anyhow::anyhow!("TALON_ASYNC_WORKER_S3_SECRET_ACCESS_KEY must be set (secret key)")
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
        session_token: async_s3_session_token_from_env(),
    };
    Ok(S3Backend::new(config, creds, http))
}

fn build_gcs_backend(
    cfg: &AsyncWorkerConfig,
    http: Arc<dyn talon_backend::http::HttpClient>,
) -> anyhow::Result<GcsBackend> {
    let config = match cfg.gcs_endpoint.clone() {
        Some(endpoint) => {
            let (host, tls) = split_scheme(&endpoint);
            GcsConfig::emulator(host, tls)
        }
        None => GcsConfig::default(),
    };
    Ok(GcsBackend::new(config, async_gcs_bearer_from_env(), http))
}

/// Build the retrying HTTP client the selected backend will use.
///
/// Retry is always on: a cache with no retry turns a routine 503 into a failed
/// read. The jitter seed is derived from the node identity so each worker's
/// backoff is distinct yet reproducible across restarts.
fn build_http_client(cfg: &AsyncWorkerConfig) -> Arc<dyn talon_backend::http::HttpClient> {
    let base: Arc<dyn talon_backend::http::HttpClient> = Arc::new(ReqwestClient::new());
    let seed = cfg
        .effective_node_id()
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
    Arc::new(talon_backend::RetryingHttpClient::new(base, retry, seed))
}

/// Prepare the cache directory.
///
/// With warm restart on, the directory is created but never cleared: the shard
/// files and their checkpoints are exactly what recovery reads, and each shard
/// truncates itself if its own checkpoint turns out to be unusable. Wiping here
/// would defeat the feature before it ran.
///
/// With `checkpoint_interval_bytes = 0` nothing was ever written down, so
/// surviving regions are unaddressable and keeping them would consume the whole
/// capacity budget with bytes nothing can read (ADR 0005 §7).
fn prepare_cache_dir(dir: &std::path::Path, warm_restart: bool) -> std::io::Result<()> {
    if warm_restart {
        tracing::info!(
            dir = %dir.display(),
            "warm restart enabled; keeping the extent cache directory"
        );
        return std::fs::create_dir_all(dir);
    }
    match std::fs::remove_dir_all(dir) {
        Ok(()) => tracing::info!(dir = %dir.display(), "cleared the extent cache directory"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    std::fs::create_dir_all(dir)
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
        Some(path) => AsyncWorkerConfigPatch::from_file(path)?,
        None => AsyncWorkerConfigPatch::default(),
    };
    let env = AsyncWorkerConfigPatch::from_env()?;
    let cfg = AsyncWorkerConfig::resolve(file, env, args.into_patch())?;

    tracing::info!(
        listen = %cfg.listen,
        advertise_addr = %cfg.advertise_addr,
        admin_listen = %cfg.admin_listen,
        coordinator = %cfg.coordinator,
        cluster_id = %cfg.cluster_id,
        node_id = %cfg.effective_node_id(),
        cache_dir = %cfg.cache_dir.display(),
        capacity_bytes = cfg.capacity_bytes,
        disk_shards = cfg.disk_shards,
        checksums_enabled = cfg.checksums_enabled,
        l1_capacity_bytes = cfg.l1_capacity_bytes,
        l1_shards = cfg.l1_shards,
        "starting talon-async-worker"
    );

    let backend_kind = cfg.backend.as_deref().unwrap_or("azure");
    let configured_backend: Backend = backend_kind.parse().map_err(|error| {
        anyhow::anyhow!(
            "unknown TALON_ASYNC_WORKER_BACKEND {backend_kind:?}; \
             expected azure, s3, or gcs: {error}"
        )
    })?;

    let node = NodeInfo {
        id: NodeId::new(cfg.effective_node_id()),
        // Advertise the routable address, not the possibly-wildcard bind
        // address, so the coordinator hands clients something connectable.
        address: cfg.advertise_addr.clone(),
        role: NodeRole::AsyncWorker,
    };

    let metrics = Arc::new(AsyncWorkerMetrics::new(backend_kind));

    prepare_cache_dir(&cfg.cache_dir, cfg.checkpoint_interval_bytes > 0)?;
    let cache = TieredExtentCache::new(&ExtentCacheConfig {
        memory_bytes: cfg.l1_capacity_bytes,
        memory_shards: cfg.l1_shards,
        disk_dir: Some(cfg.cache_dir.clone()),
        disk_bytes: cfg.capacity_bytes,
        disk_shards: cfg.disk_shards,
        disk_checksums: cfg.checksums_enabled,
        checkpoint_interval_bytes: cfg.checkpoint_interval_bytes,
    })
    .await?;
    tracing::info!(
        l1_enabled = cache.memory().is_enabled(),
        l2_enabled = cache.disk().is_some(),
        extents_recovered = cache.stats().extents_recovered,
        "extent cache ready"
    );

    let http = build_http_client(&cfg);
    let backend: Arc<dyn BackendStore> = match backend_kind {
        "azure" => Arc::new(build_azure_backend(&cfg, http)?),
        "s3" => Arc::new(build_s3_backend(&cfg, http)?),
        "gcs" => Arc::new(build_gcs_backend(&cfg, http)?),
        other => anyhow::bail!(
            "unknown TALON_ASYNC_WORKER_BACKEND {other:?}; expected azure, s3, or gcs"
        ),
    };
    tracing::info!(backend = backend_kind, "object-store backend ready");

    let runtime = Arc::new(
        AsyncWorkerRuntime::new(cache, backend).with_configured_backend(configured_backend),
    );

    let observability = Arc::new(AsyncWorkerObservability::new(
        cfg.cluster_id.clone(),
        node.clone(),
        cfg.admin_listen.clone(),
        cfg.capacity_bytes,
        Arc::clone(&metrics),
        Arc::clone(&runtime),
    )?);
    observability.readiness().set_backend_ready(true);
    observability.readiness().set_cache_ready(true);

    let control_tls = match &cfg.control_tls {
        Some(tls) => {
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
        }
        None => None,
    };

    let admin_listener = TcpListener::bind(&cfg.admin_listen).await?;
    tracing::info!(listen = %cfg.admin_listen, "async worker serving administration API");
    let admin_observability = Arc::clone(&observability);
    tokio::spawn(async move {
        if let Err(error) = serve_admin(admin_listener, admin_observability).await {
            tracing::error!(%error, "async worker administration server stopped");
        }
    });

    if let (Some(control_listen), Some(channel)) = (&cfg.control_listen, &control_tls) {
        let listener = TcpListener::bind(control_listen).await?;
        tracing::info!(listen = %control_listen, "async worker serving coordinator mTLS control plane");
        let channel = channel.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_control(listener, channel).await {
                tracing::error!(%error, "async worker coordinator mTLS control plane stopped");
            }
        });
    }

    let _control_plane = spawn_control_plane(
        cfg.coordinator.clone(),
        control_tls,
        node,
        Arc::clone(&observability),
        Duration::from_millis(cfg.heartbeat_interval_ms),
    );

    let listener = TcpListener::bind(&cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, "async worker serving data plane");
    let conn_limit = talon_transport::ConnectionLimit::new(MAX_DATA_PLANE_CONNECTIONS);
    loop {
        let permit = conn_limit.acquire().await;
        let (stream, peer) = listener.accept().await?;
        let runtime = Arc::clone(&runtime);
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            // Hold the permit for the connection's lifetime.
            let _permit = permit;
            if let Err(error) = handle_conn(stream, runtime, metrics).await {
                tracing::debug!(%peer, %error, "async worker: connection ended");
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
        Some(ControlMessage::Ack { ok: true, .. }) => {}
        Some(ControlMessage::Ack { ok: false, detail }) => {
            anyhow::bail!("coordinator rejected registration: {detail:?}")
        }
        Some(other) => anyhow::bail!("unexpected coordinator registration reply: {other:?}"),
        None => anyhow::bail!("coordinator closed registration connection without an Ack"),
    }
    tracing::info!(%coordinator, role = %NodeRole::AsyncWorker, "registered with coordinator");
    Ok(())
}

/// Maintain registration and send legacy plus versioned status heartbeats.
///
/// The legacy `Heartbeat` reports `block_count`, which this worker does not
/// have. It sends the extent count instead — the same substitution the status
/// snapshot makes, for the same reason: a hard zero would read as an idle node
/// on every dashboard built before this worker existed.
fn spawn_control_plane(
    coordinator: String,
    channel: Option<ControlTlsChannel>,
    node: NodeInfo,
    observability: Arc<AsyncWorkerObservability>,
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
                        observability.readiness().set_control_registered(false);
                        tracing::warn!(%error, "async worker registration failed; retrying");
                        continue;
                    }
                    Err(_) => {
                        observability.readiness().set_control_registered(false);
                        tracing::warn!("async worker registration timed out; retrying");
                        continue;
                    }
                }
            }

            let status = observability.status();
            let legacy = ControlMessage::Heartbeat {
                node: node.id.clone(),
                block_count: status.metrics.block_count,
            };
            let status = ControlMessage::NodeStatusHeartbeat {
                status: Box::new(status),
            };
            let heartbeat = tokio::time::timeout(CONTROL_OPERATION_TIMEOUT, async {
                send_oneshot(&coordinator, channel.as_ref(), &legacy).await?;
                send_oneshot(&coordinator, channel.as_ref(), &status).await
            })
            .await;
            match heartbeat {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    registered = false;
                    observability.readiness().set_control_registered(false);
                    tracing::warn!(%error, "control heartbeat failed; registration will retry");
                }
                Err(_) => {
                    registered = false;
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

/// Read one framed control message, or `None` at EOF.
async fn read_control<S>(stream: &mut S) -> anyhow::Result<Option<ControlMessage>>
where
    S: AsyncRead + Unpin,
{
    use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
    use tokio::io::AsyncReadExt;

    // `codec::decode` wants the whole frame, header included, so the buffer is
    // grown in place rather than split into two.
    let mut frame = vec![0u8; HEADER_LEN];
    match stream.read_exact(&mut frame).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let header = FrameHeader::decode(&frame)?;
    anyhow::ensure!(
        header.msg_type == MsgType::Control,
        "expected a control frame, got {:?}",
        header.msg_type
    );
    frame.resize(HEADER_LEN + header.length as usize, 0);
    stream.read_exact(&mut frame[HEADER_LEN..]).await?;
    Ok(Some(codec::decode(&frame)?.1))
}

/// Accept coordinator-initiated mTLS connections.
///
/// No privileged coordinator-to-worker message is implemented for this worker
/// yet, so the listener authenticates the peer and refuses the request rather
/// than leaving the connection hanging.
async fn serve_control(listener: TcpListener, channel: ControlTlsChannel) -> anyhow::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let channel = channel.clone();
        tokio::spawn(async move {
            let result = async {
                let mut authenticated = channel.accept(stream).await?;
                tracing::debug!(identity = %authenticated.identity, %peer, "accepted coordinator mTLS connection");
                if read_control(&mut authenticated.stream).await?.is_some() {
                    let reply = ControlMessage::Ack {
                        ok: false,
                        detail: Some(
                            "no coordinator-to-async-worker privileged messages are implemented"
                                .into(),
                        ),
                    };
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
                tracing::debug!(%peer, %error, "async worker coordinator mTLS connection ended");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use talon_core::{ObjectId, ObjectStat, Result, Version};
    use tokio::sync::oneshot;

    use super::*;

    struct StubBackend;

    #[async_trait]
    impl BackendStore for StubBackend {
        async fn fetch_range(&self, _object: &ObjectId, _offset: u64, len: u64) -> Result<Bytes> {
            Ok(Bytes::from(vec![0u8; len as usize]))
        }
        async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
            Ok(ObjectStat {
                len: 1 << 20,
                version: Version::new("v1"),
            })
        }
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "talon-async-main-{tag}-{}-{:p}",
            std::process::id(),
            &tag
        ));
        path
    }

    async fn observability(node: NodeInfo) -> Arc<AsyncWorkerObservability> {
        let cache = TieredExtentCache::new(&ExtentCacheConfig {
            memory_bytes: 1 << 20,
            memory_shards: 1,
            ..Default::default()
        })
        .await
        .unwrap();
        let runtime = Arc::new(AsyncWorkerRuntime::new(cache, Arc::new(StubBackend)));
        let obs = Arc::new(
            AsyncWorkerObservability::new(
                "test".into(),
                node,
                "127.0.0.1:8101".into(),
                1 << 30,
                Arc::new(AsyncWorkerMetrics::new("s3")),
                runtime,
            )
            .unwrap(),
        );
        obs.readiness().set_backend_ready(true);
        obs.readiness().set_cache_ready(true);
        obs
    }

    fn node() -> NodeInfo {
        NodeInfo {
            id: NodeId::new("aw-1"),
            address: "127.0.0.1:7101".into(),
            role: NodeRole::AsyncWorker,
        }
    }

    /// The registration must carry `AsyncWorker`, or the coordinator files this
    /// node on the block ring and hands it blocks it cannot serve.
    #[tokio::test]
    async fn registration_advertises_the_async_worker_role() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
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
            tx.send(messages).unwrap();
        });

        let obs = observability(node()).await;
        let _control = spawn_control_plane(
            coordinator.to_string(),
            None,
            node(),
            Arc::clone(&obs),
            Duration::from_secs(60),
        );

        let messages = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("control plane must register promptly")
            .unwrap();

        match &messages[0] {
            ControlMessage::Register { node: registered } => {
                assert_eq!(registered.role, NodeRole::AsyncWorker);
                assert_eq!(registered.address, "127.0.0.1:7101");
            }
            other => panic!("expected Register, got {other:?}"),
        }
        assert!(matches!(
            &messages[1],
            ControlMessage::Heartbeat { node: id, .. } if id == &node().id
        ));
        match &messages[2] {
            ControlMessage::NodeStatusHeartbeat { status } => {
                status.validate().unwrap();
                assert_eq!(status.node.role, NodeRole::AsyncWorker);
                assert!(status.ready, "registration completes readiness");
            }
            other => panic!("expected NodeStatusHeartbeat, got {other:?}"),
        }
    }

    /// A rejected registration must not be reported as success, and must not
    /// mark the worker ready — an unroutable node claiming readiness is worse
    /// than one that admits it.
    #[tokio::test]
    async fn a_rejected_registration_leaves_the_worker_unready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = read_control(&mut stream).await;
                seen.fetch_add(1, Ordering::SeqCst);
                let nack = codec::encode(
                    0,
                    &ControlMessage::Ack {
                        ok: false,
                        detail: Some("cluster is full".into()),
                    },
                )
                .unwrap();
                let _ = stream.write_all(&nack).await;
                let _ = stream.flush().await;
            }
        });

        let obs = observability(node()).await;
        let _control = spawn_control_plane(
            coordinator.to_string(),
            None,
            node(),
            Arc::clone(&obs),
            Duration::from_millis(20),
        );

        // Let a few attempts fail.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(accepted.load(Ordering::SeqCst) >= 2, "must keep retrying");
        assert!(!obs.is_ready());
        assert!(obs
            .readiness()
            .blocking_reasons()
            .contains(&"coordinator_not_registered"));
    }

    /// With warm restart off the cache directory is wiped, not scanned: nothing
    /// wrote the run descriptors down, so surviving regions are unaddressable
    /// bytes that would consume the capacity budget forever.
    #[test]
    fn the_cache_directory_is_cleared_at_startup_without_warm_restart() {
        let dir = tmp_dir("clear");
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("extents_0.bin"), b"stale region data").unwrap();
        std::fs::write(dir.join("nested/other"), b"x").unwrap();

        prepare_cache_dir(&dir, false).unwrap();

        assert!(dir.is_dir(), "the directory must exist afterwards");
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "every stale file must be gone"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// With warm restart on the opposite is required: the shard files and their
    /// checkpoints *are* the recovery input, and each shard truncates itself if
    /// its own checkpoint turns out to be unusable.
    #[test]
    fn the_cache_directory_survives_when_warm_restart_is_on() {
        let dir = tmp_dir("keep");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("extents_0.bin"), b"region data").unwrap();
        std::fs::write(dir.join("extents_0.bin.cpt"), b"checkpoint").unwrap();

        prepare_cache_dir(&dir, true).unwrap();

        assert!(dir.join("extents_0.bin").exists());
        assert!(dir.join("extents_0.bin.cpt").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn preparing_a_missing_cache_directory_creates_it() {
        let dir = tmp_dir("create").join("deep/er");
        std::fs::remove_dir_all(&dir).ok();
        prepare_cache_dir(&dir, false).unwrap();
        assert!(dir.is_dir());
        std::fs::remove_dir_all(tmp_dir("create")).ok();
    }

    #[test]
    fn an_http_endpoint_scheme_selects_plaintext_or_tls() {
        assert_eq!(
            split_scheme("http://localhost:9000"),
            ("localhost:9000".into(), false)
        );
        assert_eq!(
            split_scheme("https://s3.example.com"),
            ("s3.example.com".into(), true)
        );
        // A bare host keeps TLS: defaulting to plaintext would silently
        // downgrade a production endpoint.
        assert_eq!(
            split_scheme("s3.example.com"),
            ("s3.example.com".into(), true)
        );
    }

    #[test]
    fn cli_arguments_reach_the_config() {
        let args = Args::parse_from([
            "talon-async-worker",
            "--listen",
            "0.0.0.0:7101",
            "--cache-dir",
            "/mnt/nvme/extents",
            "--capacity-bytes",
            "1073741824",
            "--backend",
            "s3",
            "--s3-region",
            "us-east-1",
        ]);
        let cfg =
            AsyncWorkerConfig::resolve(Default::default(), Default::default(), args.into_patch())
                .unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:7101");
        assert_eq!(cfg.cache_dir, PathBuf::from("/mnt/nvme/extents"));
        assert_eq!(cfg.capacity_bytes, 1 << 30);
        assert_eq!(cfg.backend.as_deref(), Some("s3"));
        assert_eq!(cfg.s3_region.as_deref(), Some("us-east-1"));
    }

    /// A secret must never be settable on the command line, where it would land
    /// in shell history and every `ps` listing.
    #[test]
    fn secrets_are_not_command_line_arguments() {
        for flag in [
            "--s3-secret-access-key",
            "--azure-sas",
            "--gcs-bearer-token",
        ] {
            assert!(
                Args::try_parse_from(["talon-async-worker", flag, "hunter2"]).is_err(),
                "{flag} must not be accepted"
            );
        }
    }

    #[test]
    fn the_s3_backend_refuses_to_start_without_its_secret() {
        // Env-only secrets mean a misconfiguration must fail at startup with a
        // message naming the variable, not at the first read.
        let cfg = AsyncWorkerConfig {
            backend: Some("s3".into()),
            s3_region: Some("us-east-1".into()),
            s3_access_key_id: Some("AKIA".into()),
            ..Default::default()
        };
        // Only meaningful when the ambient environment has not set it.
        if async_s3_secret_key_from_env().is_none() {
            let error = build_s3_backend(&cfg, Arc::new(ReqwestClient::new()))
                .err()
                .expect("must fail without the secret")
                .to_string();
            assert!(
                error.contains("TALON_ASYNC_WORKER_S3_SECRET_ACCESS_KEY"),
                "the error must name the variable, got {error}"
            );
        }
    }

    #[test]
    fn the_s3_backend_requires_a_region_and_says_which_variable_sets_it() {
        let cfg = AsyncWorkerConfig {
            backend: Some("s3".into()),
            ..Default::default()
        };
        let error = build_s3_backend(&cfg, Arc::new(ReqwestClient::new()))
            .err()
            .expect("must fail without a region")
            .to_string();
        assert!(error.contains("TALON_ASYNC_WORKER_S3_REGION"), "{error}");
    }

    #[test]
    fn the_azure_backend_requires_an_account() {
        let cfg = AsyncWorkerConfig {
            backend: Some("azure".into()),
            ..Default::default()
        };
        let error = build_azure_backend(&cfg, Arc::new(ReqwestClient::new()))
            .err()
            .expect("must fail without an account")
            .to_string();
        assert!(
            error.contains("TALON_ASYNC_WORKER_AZURE_ACCOUNT"),
            "{error}"
        );
    }
}
