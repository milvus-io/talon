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
//! - Data plane uses [`talon_transport::data`]: a [`RangeRequest`] in, raw bytes
//!   (or an `ERROR`-flagged frame) out.
//! - Backend fetch is the real [`AzureBackend`] over [`ReqwestClient`]; the SAS
//!   token is read from the environment and **never logged**.
//!
//! The response returns bytes inline for simplicity; the production hot path
//! serves them zero-copy via `sendfile` from the committed block file
//! ([`send_file_range`](talon_worker::send_file_range)).

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use talon_backend::{AzureBackend, AzureConfig, ReqwestClient};
use talon_core::{
    azure_sas_from_env, BackendStore, BlockForm, BlockId, BlockMeta, NodeId, NodeInfo, NodeRole,
    ObjectId, ObjectStore, PageIndex, Version, WorkerConfig, WorkerConfigPatch,
};
use talon_transport::data::{self, RangeRequest};
use talon_transport::frame::{MsgType, HEADER_LEN};
use talon_transport::{codec, ControlMessage, FrameHeader};
use talon_worker::{BlockIndex, InFlightLoads, LoadKey, Presence, WholeBlockStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Fixed placeholder version used for block identity in this single-worker e2e.
///
/// A production worker would carry the object's real etag (from a coordinator
/// `HEAD`) so a source overwrite invalidates the key. Here client and worker
/// agree on a constant so the client need not hold Azure credentials.
const PLACEHOLDER_VERSION: &str = "e2e-v1";

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
    /// Address of the coordinator to register with.
    #[arg(long)]
    coordinator: Option<String>,
    /// Logical block size in bytes.
    #[arg(long)]
    block_size: Option<u32>,
}

impl Args {
    fn into_patch(self) -> WorkerConfigPatch {
        WorkerConfigPatch {
            listen: self.listen,
            coordinator: self.coordinator,
            block_size: self.block_size,
            cache_dirs: None,
            capacity_bytes: None,
            azure_account: None,
        }
    }
}

/// Everything a request handler needs, shared across connections.
struct Worker {
    store: WholeBlockStore,
    index: BlockIndex,
    inflight: InFlightLoads,
    backend: Arc<dyn BackendStore>,
    block_size: u32,
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
        coordinator = %cfg.coordinator,
        block_size = cfg.block_size,
        cache_dirs = ?cfg.cache_dirs,
        capacity_bytes = cfg.capacity_bytes,
        azure_account = ?cfg.azure_account,
        "starting talon-worker"
    );

    // Build the Azure backend from account (config/env) + SAS (env only).
    let account = cfg.azure_account.clone().ok_or_else(|| {
        anyhow::anyhow!("azure_account is required (set TALON_WORKER_AZURE_ACCOUNT)")
    })?;
    let sas = azure_sas_from_env()
        .ok_or_else(|| anyhow::anyhow!("TALON_WORKER_AZURE_SAS must be set (SAS token)"))?;
    let http = Arc::new(ReqwestClient::new());
    let backend: Arc<dyn BackendStore> = Arc::new(AzureBackend::new(
        AzureConfig::new(account),
        Some(sas),
        http,
    ));

    // Local store stack rooted at the first cache dir.
    let root = cfg
        .cache_dirs
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("/tmp/talon-cache"));
    std::fs::create_dir_all(&root)?;
    let store = WholeBlockStore::open(&root)?;

    let worker = Arc::new(Worker {
        store,
        index: BlockIndex::new(),
        inflight: InFlightLoads::new(),
        backend,
        block_size: cfg.block_size,
    });

    // Register with the coordinator and start a heartbeat task.
    let node = NodeInfo {
        id: NodeId::new(cfg.listen.clone()),
        address: cfg.listen.clone(),
        role: NodeRole::Worker,
    };
    register_with_coordinator(&cfg.coordinator, &node).await?;
    spawn_heartbeat(
        cfg.coordinator.clone(),
        node.id.clone(),
        Arc::clone(&worker),
    );

    // Serve the data plane.
    let listener = TcpListener::bind(&cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, "worker serving data plane");
    loop {
        let (stream, peer) = listener.accept().await?;
        let worker = Arc::clone(&worker);
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, worker).await {
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
    // Read the Ack (best-effort).
    if let Some(ControlMessage::Ack { ok, detail }) = read_control(&mut stream).await? {
        if !ok {
            anyhow::bail!("coordinator rejected registration: {detail:?}");
        }
    }
    tracing::info!(%coordinator, "registered with coordinator");
    Ok(())
}

/// Periodically send a `Heartbeat` with the current resident block count.
fn spawn_heartbeat(coordinator: String, node: NodeId, worker: Arc<Worker>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            ticker.tick().await;
            let block_count = worker.index.len() as u64;
            let msg = ControlMessage::Heartbeat {
                node: node.clone(),
                block_count,
            };
            if let Err(e) = send_oneshot(&coordinator, &msg).await {
                tracing::debug!(error = %e, "heartbeat send failed");
            }
        }
    });
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
async fn handle_conn(mut stream: TcpStream, worker: Arc<Worker>) -> anyhow::Result<()> {
    loop {
        let mut header_buf = [0u8; HEADER_LEN];
        match stream.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        let header = FrameHeader::decode(&header_buf)?;
        let mut payload = vec![0u8; header.length as usize];
        stream.read_exact(&mut payload).await?;

        if header.msg_type != MsgType::GetRange {
            let err = data::encode_error(header.request_id, "worker only serves GetRange");
            stream.write_all(&err).await?;
            stream.flush().await?;
            continue;
        }

        let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
        full.extend_from_slice(&header_buf);
        full.extend_from_slice(&payload);
        let (h, req) = match data::decode_request(&full) {
            Ok(v) => v,
            Err(e) => {
                let err = data::encode_error(header.request_id, &format!("bad request: {e}"));
                stream.write_all(&err).await?;
                stream.flush().await?;
                continue;
            }
        };

        match worker.serve_range(&req).await {
            Ok(bytes) => {
                let hdr = data::response_header_ok(h.request_id, bytes.len() as u32);
                stream.write_all(&hdr).await?;
                stream.write_all(&bytes).await?;
                stream.flush().await?;
            }
            Err(e) => {
                let err = data::encode_error(h.request_id, &e.to_string());
                stream.write_all(&err).await?;
                stream.flush().await?;
            }
        }
    }
}

impl Worker {
    /// The block-aligned [`BlockId`] that contains `offset` of `object`.
    fn block_for(&self, object: &ObjectId, offset: u64) -> BlockId {
        let bs = self.block_size as u64;
        let block_start = (offset / bs) * bs;
        BlockId::new(
            object.clone(),
            block_start,
            self.block_size,
            Version::new(PLACEHOLDER_VERSION),
        )
    }

    /// Serve `[offset, offset+len)` of `object`: local hit, or miss→Azure→commit.
    async fn serve_range(&self, req: &RangeRequest) -> anyhow::Result<bytes::Bytes> {
        let block = self.block_for(&req.object, req.offset);
        let offset_in_block = req.offset - block.offset;

        // Hit path: whole block already resident.
        if matches!(
            self.index.presence(&block, PageIndex(0), PageIndex(1)),
            Presence::Whole
        ) {
            tracing::info!(block = %block, "HIT");
            let bytes = self
                .store
                .get_bytes(&block)
                .await
                .map_err(|e| anyhow::anyhow!("read committed block: {e}"))?;
            return slice(&bytes, offset_in_block, req.len);
        }

        // Miss path: fetch the block-aligned range once, commit, then serve.
        tracing::info!(block = %block, "MISS -> Azure fetch");
        let key = LoadKey::Whole(block.clone());
        // Dedup marker for observability; a real herd would coordinate on this.
        let _ = self.inflight.admit(key.clone());

        let fetch_len = self.block_size as u64; // whole block-aligned range
        let fetched = self
            .backend
            .fetch_range(&req.object, block.offset, fetch_len)
            .await;
        self.inflight.complete(&key);
        let bytes = fetched?;

        // Commit durably via the store's atomic write (temp file + fsync +
        // rename), then register the block so future reads are hits.
        self.store
            .put(&block, bytes.clone())
            .await
            .map_err(|e| anyhow::anyhow!("commit block failed: {e}"))?;
        self.index.commit(BlockMeta {
            id: block.clone(),
            form: BlockForm::Whole,
            len: bytes.len() as u64,
        });
        tracing::info!(block = %block, bytes = bytes.len(), "committed block");

        // Serve the requested sub-range from the freshly fetched bytes.
        slice(&bytes, offset_in_block, req.len)
    }
}

/// Slice `[offset, offset+len)` out of `buf`, clamping `len` to what is present.
fn slice(buf: &[u8], offset: u64, len: u64) -> anyhow::Result<bytes::Bytes> {
    let start = offset as usize;
    if start > buf.len() {
        anyhow::bail!("offset {offset} beyond block length {} bytes", buf.len());
    }
    let end = (start + len as usize).min(buf.len());
    Ok(bytes::Bytes::copy_from_slice(&buf[start..end]))
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
