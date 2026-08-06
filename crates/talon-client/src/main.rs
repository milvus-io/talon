//! Talon CLI client.
//!
//! Runs the same read path a FUSE mount would, without a kernel mount (this
//! sandbox has no `/dev/fuse`):
//!
//! 1. Parse `/az/<container>/<blob>` into an [`ObjectId`].
//! 2. Resolve the owning worker — locally on the block ring, via the
//!    coordinator on the async ring (see *Choosing a ring* below).
//! 3. Send a data-plane [`RangeRequest`] to that worker and read the raw bytes.
//!
//! Prints byte count + elapsed time; writes the bytes to `--out` when given so
//! the caller can `cmp` two reads for byte-exactness.
//!
//! # Choosing a ring
//!
//! `--ring` selects which pool answers step 2. `block` (the default) sends
//! `PlacementLookup` and reaches a `talon-worker`; `async` sends
//! `AsyncPlacementLookup` and reaches a `talon-async-worker`, which caches the
//! exact range rather than a 256MB block.
//!
//! The caller picks, not the coordinator. The coordinator sees only a byte
//! range and would have to guess from a file extension or a read size; the
//! caller knows whether it is about to read a Parquet footer or stream a whole
//! partition. See ADR 0005 section 6.
//!
//! The data plane is identical either way — both workers answer the same
//! `RangeRequest` — so only step 2 changes.

use std::time::Instant;

use clap::{Parser, ValueEnum};
use talon_core::{BlockId, CachePlacementTable, NodeRole, ObjectId, Version};
use talon_transport::data::{self, RangeRequest};
use talon_transport::frame::{Flags, HEADER_LEN};
use talon_transport::{codec, ControlMessage, FrameHeader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Block size used only to compute the placement key (must match the worker's
/// so Maglev selects the same owner; with a single worker any value works).
const PLACEMENT_BLOCK_SIZE: u32 = 256 << 20;
/// Placeholder version matching the worker's block identity.
const PLACEHOLDER_VERSION: &str = "e2e-v1";

/// Which rendezvous ring answers the placement lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Ring {
    /// Block workers, hashed on the whole `BlockId`. The default, and the only
    /// ring that existed before ADR 0005.
    Block,
    /// Async workers, hashed on the object identity alone, so every range of
    /// one object resolves to the same worker.
    Async,
}

impl Ring {
    /// The node role this ring is drawn from.
    fn role(self) -> NodeRole {
        match self {
            Ring::Block => NodeRole::Worker,
            Ring::Async => NodeRole::AsyncWorker,
        }
    }

    /// The lookup message that selects this ring.
    ///
    /// The message variant *is* the selector — there is no ring field on the
    /// wire — so a pre-schema-4 coordinator simply never sees the async
    /// variant rather than misreading a flag.
    fn lookup(self, block: &BlockId) -> ControlMessage {
        let block = block.clone();
        match self {
            Ring::Block => ControlMessage::PlacementLookup { block, k: 1 },
            Ring::Async => ControlMessage::AsyncPlacementLookup { block, k: 1 },
        }
    }
}

/// Command-line arguments for the Talon client.
#[derive(Debug, Parser)]
#[command(name = "talon-client", version, about)]
struct Args {
    /// Address of the coordinator to query for placement.
    #[arg(long, default_value = "127.0.0.1:7000")]
    coordinator: String,
    /// Which pool answers the lookup: `block` workers or `async` workers.
    #[arg(long, value_enum, default_value_t = Ring::Block)]
    ring: Ring,
    /// Connect directly to one worker, bypassing placement (diagnostics/tests).
    #[arg(long)]
    worker: Option<String>,
    /// Resolve and print placement without connecting to the selected worker.
    #[arg(long, conflicts_with_all = ["worker", "membership_only"])]
    placement_only: bool,
    /// Print the coordinator's current worker membership and exit.
    #[arg(long, conflicts_with_all = ["worker", "placement_only"])]
    membership_only: bool,
    /// Object path, e.g. `/az/<container>/<blob>`.
    #[arg(long, required_unless_present = "membership_only")]
    path: Option<String>,
    /// Byte offset to start reading at.
    #[arg(long, default_value_t = 0)]
    offset: u64,
    /// Number of bytes to read.
    #[arg(long, required_unless_present = "membership_only")]
    len: Option<u64>,
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
    if args.membership_only {
        let mut nodes = membership_lookup(&args.coordinator).await?;
        nodes.sort_by(|left, right| {
            left.address
                .cmp(&right.address)
                .then_with(|| left.id.0.cmp(&right.id.0))
        });
        // Show only the selected ring's pool: listing both would suggest a
        // lookup could return either, which is exactly what the split
        // prevents.
        for node in nodes {
            if node.role == args.ring.role() {
                println!("member {} {}", node.id, node.address);
            }
        }
        return Ok(());
    }

    let path = args
        .path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--path is required for reads"))?;
    let len = args
        .len
        .ok_or_else(|| anyhow::anyhow!("--len is required for reads"))?;
    let object = ObjectId::from_path(path)?;
    let block = BlockId::new(
        object.clone(),
        (args.offset / PLACEMENT_BLOCK_SIZE as u64) * PLACEMENT_BLOCK_SIZE as u64,
        PLACEMENT_BLOCK_SIZE,
        Version::new(PLACEHOLDER_VERSION),
    );

    let worker_addr = match args.worker {
        Some(worker) => {
            tracing::info!(worker_addr = %worker, "using direct worker");
            worker
        }
        None => {
            // The two rings resolve differently, and deliberately so.
            let worker_addr = match args.ring {
                // Block ring: the client rebuilds the coordinator's Maglev
                // table from a membership snapshot and computes placement
                // itself (#455), so there is no placement round trip.
                Ring::Block => {
                    let membership = membership_lookup(&args.coordinator).await?;
                    let placement = CachePlacementTable::new(&membership);
                    let owner = placement.primary(&block).ok_or_else(|| {
                        anyhow::anyhow!("no worker owns this block (empty cluster?)")
                    })?;
                    tracing::info!(owner = %owner.id, "resolved owner");
                    owner.address.clone()
                }
                // Async ring: no client-side equivalent of the object ring
                // exists yet, so this still asks the coordinator and then
                // resolves the id to an address.
                Ring::Async => {
                    let owners = placement_lookup(&args.coordinator, &block, args.ring).await?;
                    let owner = owners.first().ok_or_else(|| {
                        // An empty answer here almost always means no async
                        // worker is registered, not an empty cluster — the two
                        // pools are disjoint, so a block worker is never
                        // substituted.
                        anyhow::anyhow!(
                            "no {:?} worker owns this read; \
                             is one registered with the coordinator?",
                            args.ring
                        )
                    })?;
                    tracing::info!(owner = %owner, "resolved owner");
                    resolve_address(&args.coordinator, owner).await?
                }
            };
            tracing::info!(%worker_addr, "resolved worker address");
            worker_addr
        }
    };

    if args.placement_only {
        println!(
            "placed {path} on {worker_addr} via the {:?} ring",
            args.ring
        );
        return Ok(());
    }

    // Fetch the range from the selected worker.
    let start = Instant::now();
    let bytes = fetch_range(&worker_addr, &object, args.offset, len).await?;
    let elapsed = start.elapsed();

    // Verify the worker returned the full requested range. A short read means
    // truncation (or the object ended inside the range); either way, silently
    // reporting it as success would hide corruption (issue #112).
    if (bytes.len() as u64) < len {
        anyhow::bail!(
            "short read: requested {} bytes at offset {}, got {} (truncated or past EOF)",
            len,
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

/// Look up placement on `ring` and return the ordered owner ids.
///
/// Both rings answer with the same `PlacementResponse`, so the caller learns
/// one reply shape regardless of which it asked.
async fn placement_lookup(
    coordinator: &str,
    block: &BlockId,
    ring: Ring,
) -> anyhow::Result<Vec<String>> {
    match request_control(coordinator, &ring.lookup(block)).await? {
        ControlMessage::PlacementResponse { owners, .. } => {
            Ok(owners.into_iter().map(|n| n.0).collect())
        }
        other => anyhow::bail!("unexpected placement reply: {other:?}"),
    }
}

/// Send a `MembershipQuery` and resolve `owner_id` to its worker address.
async fn resolve_address(coordinator: &str, owner_id: &str) -> anyhow::Result<String> {
    membership_lookup(coordinator)
        .await?
        .into_iter()
        .find(|n| n.id.0 == owner_id)
        .map(|n| n.address)
        .ok_or_else(|| anyhow::anyhow!("owner {owner_id} not in membership list"))
}

/// Return the coordinator's current membership snapshot.
async fn membership_lookup(coordinator: &str) -> anyhow::Result<Vec<talon_core::NodeInfo>> {
    match request_control(coordinator, &ControlMessage::MembershipQuery {}).await? {
        ControlMessage::MembershipList { nodes } => Ok(nodes),
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

#[cfg(test)]
mod tests {
    use clap::Parser;
    use talon_core::{Backend, BlockId, NodeRole, ObjectId, Version};
    use talon_transport::ControlMessage;

    use super::{Args, Ring};

    fn block() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", "part-0.parquet"),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    #[test]
    fn the_block_ring_is_the_default() {
        // Every existing invocation must keep reaching a block worker; the
        // async ring is opt-in or this is a silent routing change.
        let args = Args::try_parse_from([
            "talon-client",
            "--path",
            "/s3/bucket/object",
            "--len",
            "4096",
        ])
        .unwrap();
        assert_eq!(args.ring, Ring::Block);
    }

    #[test]
    fn the_ring_flag_selects_the_async_pool() {
        let args = Args::try_parse_from([
            "talon-client",
            "--ring",
            "async",
            "--path",
            "/s3/bucket/part-0.parquet",
            "--len",
            "4096",
        ])
        .unwrap();
        assert_eq!(args.ring, Ring::Async);
        assert_eq!(args.ring.role(), NodeRole::AsyncWorker);
    }

    #[test]
    fn an_unknown_ring_is_rejected_rather_than_defaulted() {
        // Silently falling back to the block ring would send a selective read
        // to a worker that fetches 256MB for it.
        assert!(Args::try_parse_from([
            "talon-client",
            "--ring",
            "extents",
            "--path",
            "/s3/bucket/object",
            "--len",
            "4096",
        ])
        .is_err());
    }

    #[test]
    fn each_ring_sends_its_own_lookup_message() {
        // The message variant is the selector — there is no ring field on the
        // wire — so this mapping is the entire routing decision.
        assert!(matches!(
            Ring::Block.lookup(&block()),
            ControlMessage::PlacementLookup { k: 1, .. }
        ));
        assert!(matches!(
            Ring::Async.lookup(&block()),
            ControlMessage::AsyncPlacementLookup { k: 1, .. }
        ));
    }

    #[test]
    fn a_ring_draws_only_from_its_own_role() {
        assert_eq!(Ring::Block.role(), NodeRole::Worker);
        assert_eq!(Ring::Async.role(), NodeRole::AsyncWorker);
        assert_ne!(Ring::Block.role(), Ring::Async.role());
    }

    #[test]
    fn membership_only_accepts_a_ring() {
        let args =
            Args::try_parse_from(["talon-client", "--membership-only", "--ring", "async"]).unwrap();
        assert!(args.membership_only);
        assert_eq!(args.ring, Ring::Async);
    }

    #[test]
    fn direct_worker_mode_is_parsed_without_changing_default_coordinator() {
        let args = Args::try_parse_from([
            "talon-client",
            "--worker",
            "10.0.0.7:7001",
            "--path",
            "/s3/bucket/object",
            "--len",
            "4096",
        ])
        .unwrap();

        assert_eq!(args.worker.as_deref(), Some("10.0.0.7:7001"));
        assert!(!args.placement_only);
        assert!(!args.membership_only);
        assert_eq!(args.coordinator, "127.0.0.1:7000");
        assert_eq!(args.len, Some(4096));
    }

    #[test]
    fn placement_only_mode_conflicts_with_direct_worker() {
        let args = Args::try_parse_from([
            "talon-client",
            "--placement-only",
            "--path",
            "/s3/bucket/object",
            "--len",
            "4096",
        ])
        .unwrap();
        assert!(args.placement_only);
        assert!(!args.membership_only);
        assert!(args.worker.is_none());

        assert!(Args::try_parse_from([
            "talon-client",
            "--placement-only",
            "--worker",
            "10.0.0.7:7001",
            "--path",
            "/s3/bucket/object",
            "--len",
            "4096",
        ])
        .is_err());
    }

    #[test]
    fn membership_only_mode_does_not_require_read_arguments() {
        let args = Args::try_parse_from([
            "talon-client",
            "--membership-only",
            "--coordinator",
            "c:7000",
        ])
        .unwrap();

        assert!(args.membership_only);
        assert!(!args.placement_only);
        assert!(args.worker.is_none());
        assert!(args.path.is_none());
        assert!(args.len.is_none());
    }

    #[test]
    fn read_modes_still_require_path_and_length() {
        assert!(Args::try_parse_from(["talon-client"]).is_err());
        assert!(Args::try_parse_from([
            "talon-client",
            "--placement-only",
            "--path",
            "/s3/bucket/object",
        ])
        .is_err());
        assert!(
            Args::try_parse_from(["talon-client", "--membership-only", "--placement-only",])
                .is_err()
        );
    }
}
