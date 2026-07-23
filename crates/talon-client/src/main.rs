//! Talon CLI client.
//!
//! Runs the same read path a FUSE mount would, without a kernel mount (this
//! sandbox has no `/dev/fuse`):
//!
//! 1. Parse `/az/<container>/<blob>` into an [`ObjectId`].
//! 2. Ask the coordinator (`PlacementLookup`) which worker owns the block.
//! 3. Resolve that owner id to an address (`MembershipQuery`).
//! 4. Send a data-plane [`RangeRequest`] to the worker and read the raw bytes.
//!
//! Prints byte count + elapsed time; writes the bytes to `--out` when given so
//! the caller can `cmp` two reads for byte-exactness.

use std::time::Instant;

use clap::Parser;
use talon_core::{BlockId, ObjectId, Version};
use talon_transport::data::{self, RangeRequest};
use talon_transport::frame::{Flags, HEADER_LEN};
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Block size used only to compute the placement key (must match the worker's
/// so HRW selects the same owner; with a single worker any value works).
const PLACEMENT_BLOCK_SIZE: u32 = 256 << 20;
/// Placeholder version matching the worker's block identity.
const PLACEHOLDER_VERSION: &str = "e2e-v1";

/// Command-line arguments for the Talon client.
#[derive(Debug, Parser)]
#[command(name = "talon-client", version, about)]
struct Args {
    /// Address of the coordinator to query for placement.
    #[arg(long, default_value = "127.0.0.1:7000")]
    coordinator: String,
    /// Object path, e.g. `/az/<container>/<blob>`.
    #[arg(long)]
    path: String,
    /// Byte offset to start reading at.
    #[arg(long, default_value_t = 0)]
    offset: u64,
    /// Number of bytes to read.
    #[arg(long)]
    len: u64,
    /// Optional output file for the fetched bytes.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let object = ObjectId::from_path(&args.path)?;
    let block = BlockId::new(
        object.clone(),
        (args.offset / PLACEMENT_BLOCK_SIZE as u64) * PLACEMENT_BLOCK_SIZE as u64,
        PLACEMENT_BLOCK_SIZE,
        Version::new(PLACEHOLDER_VERSION),
    );

    // 1. Placement lookup: which worker owns this block?
    let owners = placement_lookup(&args.coordinator, &block).await?;
    let owner = owners
        .first()
        .ok_or_else(|| anyhow::anyhow!("no worker owns this block (empty cluster?)"))?;
    tracing::info!(owner = %owner, "resolved owner");

    // 2. Resolve the owner id to a worker address.
    let worker_addr = resolve_address(&args.coordinator, owner).await?;
    tracing::info!(%worker_addr, "resolved worker address");

    // 3. Fetch the range from the worker.
    let start = Instant::now();
    let bytes = fetch_range(&worker_addr, &object, args.offset, args.len).await?;
    let elapsed = start.elapsed();

    // Verify the worker returned the full requested range. A short read means
    // truncation (or the object ended inside the range); either way, silently
    // reporting it as success would hide corruption (issue #112).
    if (bytes.len() as u64) < args.len {
        anyhow::bail!(
            "short read: requested {} bytes at offset {}, got {} (truncated or past EOF)",
            args.len,
            args.offset,
            bytes.len()
        );
    }

    println!(
        "read {} bytes from {} in {:.1?}",
        bytes.len(),
        worker_addr,
        elapsed
    );
    if let Some(out) = &args.out {
        tokio::fs::write(out, &bytes).await?;
        println!("wrote {} bytes to {}", bytes.len(), out.display());
    } else {
        let n = bytes.len().min(64);
        println!("first {n} bytes (hex): {}", hex_prefix(&bytes[..n]));
    }
    Ok(())
}

/// Send a `PlacementLookup` and return the ordered owner ids.
async fn placement_lookup(coordinator: &str, block: &BlockId) -> anyhow::Result<Vec<String>> {
    let req = ControlMessage::PlacementLookup {
        block: block.clone(),
        k: 1,
    };
    match request_control(coordinator, &req).await? {
        ControlMessage::PlacementResponse { owners, .. } => {
            Ok(owners.into_iter().map(|n| n.0).collect())
        }
        other => anyhow::bail!("unexpected placement reply: {other:?}"),
    }
}

/// Send a `MembershipQuery` and resolve `owner_id` to its worker address.
async fn resolve_address(coordinator: &str, owner_id: &str) -> anyhow::Result<String> {
    match request_control(coordinator, &ControlMessage::MembershipQuery {}).await? {
        ControlMessage::MembershipList { nodes } => nodes
            .into_iter()
            .find(|n| n.id.0 == owner_id)
            .map(|n| n.address)
            .ok_or_else(|| anyhow::anyhow!("owner {owner_id} not in membership list")),
        other => anyhow::bail!("unexpected membership reply: {other:?}"),
    }
}

/// Send a control request over a fresh connection and read one reply.
async fn request_control(addr: &str, msg: &ControlMessage) -> anyhow::Result<ControlMessage> {
    let mut stream = TcpStream::connect(addr).await?;
    let buf = codec::encode(0, msg)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&payload);
    let (_h, reply) = codec::decode(&full)?;
    Ok(reply)
}

/// Send a `RangeRequest` to a worker and read the raw response bytes.
async fn fetch_range(
    worker_addr: &str,
    object: &ObjectId,
    offset: u64,
    len: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(worker_addr).await?;
    let req = RangeRequest {
        object: object.clone(),
        offset,
        len,
    };
    let buf = data::encode_request(0, &req)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)?;
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;

    if header.flags.contains(Flags::ERROR) {
        anyhow::bail!("worker error: {}", String::from_utf8_lossy(&payload));
    }
    Ok(payload)
}

/// Render bytes as a space-free hex string.
fn hex_prefix(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}
