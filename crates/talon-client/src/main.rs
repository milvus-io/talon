//! Talon CLI client.
//!
//! Runs the same read path a FUSE mount would, without a kernel mount (this
//! sandbox has no `/dev/fuse`):
//!
//! 1. Parse `/az/<container>/<blob>` into an [`ObjectId`].
//! 2. Fetch worker membership and compute Maglev placement locally.
//! 3. Send a data-plane [`RangeRequest`] to the selected worker.
//!
//! Prints byte count + elapsed time; writes the bytes to `--out` when given so
//! the caller can `cmp` two reads for byte-exactness.

use std::time::Instant;

use clap::{Parser, Subcommand};
use talon_core::{BlockId, CachePlacementTable, NamespaceId, NodeRole, ObjectId, Version};
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

/// Command-line arguments for the Talon client.
#[derive(Parser)]
#[command(
    name = "talon-client",
    version,
    about,
    subcommand_negates_reqs = true,
    args_conflicts_with_subcommands = true
)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    /// Address of the coordinator to query for placement.
    #[arg(long, global = true, default_value = "127.0.0.1:7000")]
    coordinator: String,
    /// Cache namespace used by reads and prewarming.
    #[arg(long, global = true, default_value = "default", value_parser = |name: &str| {
        talon_core::NamespaceContext::new(name).map(|_| name.to_owned())
    })]
    namespace: String,
    /// Request property KEY=JSON; credential_broker requires url, request and origin.endpoint.
    /// S3 providers also require origin.region for request signing.
    #[arg(long = "request-option", global = true)]
    request_options: Vec<String>,
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

#[derive(Debug, Subcommand)]
enum Command {
    /// Prewarm multiple files using batched worker protocol requests.
    BatchLoad {
        /// JSON array of {path, version, size} entries.
        #[arg(long)]
        manifest: std::path::PathBuf,
        /// Logical block size; must match workers and readers.
        #[arg(long, default_value_t = PLACEMENT_BLOCK_SIZE)]
        block_size: u32,
    },
    /// Prewarm a file on the workers responsible for its blocks.
    Load {
        /// Object path, e.g. /s3/bucket/key.
        #[arg(long)]
        path: String,
        /// Source version/ETag used by normal version-pinned reads.
        #[arg(long = "version")]
        source_version: String,
        /// Required size of this version in bytes; LOAD issues no HEAD.
        #[arg(long)]
        size: u64,
        /// Logical block size; must match workers and readers.
        #[arg(long, default_value_t = PLACEMENT_BLOCK_SIZE)]
        block_size: u32,
    },
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadEntry {
    path: String,
    version: String,
    size: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "telemetry")]
    let _telemetry = talon_telemetry::export::init("talon-client")
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

async fn run_load(
    command: Command,
    coordinator: &str,
    namespace: NamespaceId,
) -> anyhow::Result<()> {
    let (entries, block_size, batch) = match command {
        Command::Load {
            path,
            source_version,
            size,
            block_size,
        } => (
            vec![LoadEntry {
                path,
                version: source_version,
                size,
            }],
            block_size,
            false,
        ),
        Command::BatchLoad {
            manifest,
            block_size,
        } => {
            let input = tokio::fs::read(manifest).await?;
            (
                serde_json::from_slice::<Vec<LoadEntry>>(&input)?,
                block_size,
                true,
            )
        }
    };
    let objects = entries
        .iter()
        .map(|entry| ObjectId::from_path(&entry.path))
        .collect::<Result<Vec<_>, _>>()?;
    let versions = entries
        .iter()
        .map(|entry| Version::new(entry.version.clone()))
        .collect::<Vec<_>>();
    let files = entries
        .iter()
        .enumerate()
        .map(|(i, entry)| talon_cache_client::FileView {
            object: &objects[i],
            version: &versions[i],
            size: entry.size,
            block_size,
        })
        .collect::<Vec<_>>();
    let coordinator = talon_cache_client::CoordinatorClient::new(coordinator);
    let reader = talon_cache_client::BlockReader::new(
        coordinator,
        std::sync::Arc::new(talon_cache_client::PlacementCache::new(30_000)),
        1,
    );
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    if batch {
        let results = match reader.batch_load(&files, now_ms, &namespace).await {
            Ok(results) => results,
            Err(error) => {
                for failure in error.failed_files() {
                    eprintln!(
                        "{} input[{}] {}: {}",
                        if failure.uncertain {
                            "unconfirmed"
                        } else {
                            "failed"
                        },
                        failure.index,
                        entries[failure.index].path,
                        failure.error
                    );
                }
                return Err(error.into());
            }
        };
        for (entry, result) in entries.iter().zip(results) {
            println!(
                "loaded {} bytes in {} blocks: {}",
                result.size, result.blocks, entry.path
            );
        }
    } else {
        let result = reader.load(&files[0], now_ms, &namespace).await?;
        println!("loaded {} bytes in {} blocks", result.size, result.blocks);
    }
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    let properties = args
        .request_options
        .iter()
        .map(|option| {
            option
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("request option must be KEY=VALUE"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let context = talon_transport::RequestContext::from_properties(&properties)
        .map_err(anyhow::Error::msg)?;
    context.scope(run_with_args(args)).await
}

async fn run_with_args(args: Args) -> anyhow::Result<()> {
    let namespace = if args.namespace == talon_core::DEFAULT_NAMESPACE {
        NamespaceId::Unattributed
    } else {
        NamespaceId::named(args.namespace)
    };
    if let Some(command) = args.command {
        return run_load(command, &args.coordinator, namespace).await;
    }
    if args.membership_only {
        let mut nodes = membership_lookup(&args.coordinator, &namespace)
            .await?
            .nodes;
        nodes.sort_by(|left, right| {
            left.address
                .cmp(&right.address)
                .then_with(|| left.id.0.cmp(&right.id.0))
        });
        for node in nodes {
            if node.role == NodeRole::Worker {
                let address = if node.address.is_empty() {
                    "unavailable"
                } else {
                    &node.address
                };
                println!("member {} {}", node.id, address);
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
            let membership = membership_lookup(&args.coordinator, &namespace).await?;
            let placement = CachePlacementTable::new(&membership.nodes);
            let owner = placement
                .primary(&block)
                .ok_or_else(|| unavailable("no registered worker owns this block".into()))?;
            tracing::info!(owner = %owner.id, "resolved owner");
            if owner.address.is_empty() {
                return Err(unavailable(format!(
                    "worker {} is offline or has conflicting instances",
                    owner.id
                )));
            }
            let worker_addr = owner.address.clone();
            tracing::info!(%worker_addr, "resolved worker address");
            worker_addr
        }
    };

    if args.placement_only {
        println!("placed {path} on {worker_addr}");
        return Ok(());
    }

    // Fetch the range from the selected worker.
    let start = Instant::now();
    let bytes = fetch_range(&worker_addr, &object, args.offset, len, &namespace).await?;
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

struct CliMembership {
    // Logical members remain present even when their serving address is absent.
    nodes: Vec<talon_core::NodeInfo>,
}

fn unavailable(message: String) -> anyhow::Error {
    talon_transport::DataPlaneError {
        code: talon_transport::DataErrorCode::Unavailable,
        message,
    }
    .into()
}

/// Resolve the published membership for this one-shot CLI operation.
async fn membership_lookup(
    coordinator: &str,
    namespace: &NamespaceId,
) -> anyhow::Result<CliMembership> {
    use talon_core::worker_membership::InstanceState;
    match request_control(coordinator, &ControlMessage::MembershipQuery {}, namespace).await? {
        ControlMessage::MembershipList { view } => {
            let nodes = view
                .workers
                .into_iter()
                .filter(|w| !w.member.retired)
                .map(|worker| talon_core::NodeInfo {
                    id: talon_core::NodeId::new(worker.member.worker_id),
                    address: match worker.state {
                        InstanceState::Serving { address, .. } => address,
                        InstanceState::Offline | InstanceState::Conflict => String::new(),
                    },
                    role: NodeRole::Worker,
                })
                .collect();
            Ok(CliMembership { nodes })
        }
        other => anyhow::bail!("unexpected membership reply: {other:?}"),
    }
}

/// Send a control request over a fresh connection and read one reply.
async fn request_control(
    addr: &str,
    msg: &ControlMessage,
    namespace: &NamespaceId,
) -> anyhow::Result<ControlMessage> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut buf = codec::encode(0, msg)?;
    talon_transport::envelope::outbound_namespace(&mut buf, addr, namespace)?;
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
    namespace: &NamespaceId,
) -> anyhow::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(worker_addr).await?;
    let req = RangeRequest {
        object: object.clone(),
        offset,
        len,
    };
    let mut buf = data::encode_request(0, &req)?;
    talon_transport::envelope::outbound_namespace(&mut buf, worker_addr, namespace)?;
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

    use super::{Args, Command};

    #[tokio::test]
    async fn read_sends_selected_namespace_and_preserves_default_wire_format() {
        use super::*;
        for name in ["default", "analytics"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = [0; HEADER_LEN];
                socket.read_exact(&mut bytes).await.unwrap();
                let header = FrameHeader::decode(&bytes).unwrap();
                let mut payload = vec![0; header.length as usize];
                socket.read_exact(&mut payload).await.unwrap();
                let (metadata, _) = talon_transport::envelope::decode(&header, &payload).unwrap();
                assert_eq!(
                    metadata.namespace.unwrap_or_default().effective_name(),
                    name
                );
                assert_eq!(header.version, if name == "default" { 1 } else { 3 });
                let mut reply = data::response_header_ok(header.request_id, 3).to_vec();
                reply.extend_from_slice(b"abc");
                socket.write_all(&reply).await.unwrap();
            });
            let bytes = fetch_range(
                &addr,
                &ObjectId::from_path("/s3/b/k").unwrap(),
                0,
                3,
                &talon_core::NamespaceId::named(name),
            )
            .await
            .unwrap();
            assert_eq!(bytes, b"abc");
            server.await.unwrap();
        }
    }

    #[test]
    fn namespace_is_global_and_validated() {
        for args in [
            vec![
                "talon-client",
                "--namespace",
                "analytics",
                "--membership-only",
            ],
            vec![
                "talon-client",
                "load",
                "--path",
                "/s3/b/k",
                "--version",
                "v1",
                "--size",
                "8",
                "--namespace",
                "analytics",
            ],
            vec![
                "talon-client",
                "batch-load",
                "--namespace",
                "analytics",
                "--manifest",
                "files.json",
            ],
        ] {
            Args::try_parse_from(args).unwrap();
        }
        for name in ["", "..", "a/b", "worker_identity", ".worker.lock", "中文"] {
            assert!(Args::try_parse_from([
                "talon-client",
                "--namespace",
                name,
                "--membership-only"
            ])
            .is_err());
        }
    }

    #[test]
    fn batch_load_command_and_manifest_require_explicit_file_coordinates() {
        let args = Args::try_parse_from(["talon-client", "batch-load", "--manifest", "files.json"])
            .unwrap();
        assert!(matches!(args.command, Some(Command::BatchLoad { .. })));
        assert!(Args::try_parse_from(["talon-client", "batch-load"]).is_err());
        let entries: Vec<super::LoadEntry> = serde_json::from_str(r#"[{"path":"/s3/bucket/a","version":"v1","size":3},{"path":"/s3/bucket/b","version":"v2","size":0}]"#).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(serde_json::from_str::<Vec<super::LoadEntry>>(
            r#"[{"path":"/s3/bucket/a","version":"v1"}]"#
        )
        .is_err());
    }

    #[test]
    fn load_command_requires_path_version_and_size_but_not_read_length() {
        let args = Args::try_parse_from([
            "talon-client",
            "load",
            "--coordinator",
            "c:7000",
            "--path",
            "/s3/bucket/key",
            "--version",
            "etag",
            "--size",
            "17",
        ])
        .unwrap();
        assert_eq!(args.coordinator, "c:7000");
        assert!(matches!(args.command, Some(Command::Load { size: 17, .. })));
        assert!(
            Args::try_parse_from(["talon-client", "load", "--path", "/s3/bucket/key"]).is_err()
        );
        assert!(
            Args::try_parse_from([
                "talon-client",
                "load",
                "--path",
                "/s3/bucket/key",
                "--version",
                "v1",
            ])
            .is_err(),
            "size is required"
        );
        assert!(Args::try_parse_from([
            "talon-client",
            "--worker",
            "w:7001",
            "load",
            "--path",
            "/s3/bucket/key",
            "--version",
            "v1"
        ])
        .is_err());
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
