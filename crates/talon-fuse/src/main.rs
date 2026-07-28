//! Talon FUSE client entry point.
//!
//! Resolves [`FuseConfig`] (defaults < file < env < CLI), builds the read-path
//! components (coordinator client, placement cache, [`BlockReader`]), populates
//! the namespace from a coordinator listing, and — when built with the `mount`
//! feature — mounts the read-only filesystem, serving until SIGINT triggers a
//! clean unmount.
//!
//! Without the `mount` feature the binary performs all the setup and validation
//! but prints a clear message instead of mounting, so it still builds and runs
//! in environments without `/dev/fuse` or libfuse.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use talon_core::{FuseConfig, FuseConfigPatch};
use talon_fuse::{BlockReader, CoordinatorClient, PlacementCache, ReadOnlyFs};
use talon_transport::ObjectEntry;

/// Command-line arguments for the Talon FUSE mount.
#[derive(Debug, Parser)]
#[command(name = "talon-fuse", version, about)]
struct Args {
    /// Path to a TOML config file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Directory to mount the Talon filesystem at.
    #[arg(long)]
    mountpoint: Option<PathBuf>,
    /// Address of the coordinator to connect to.
    #[arg(long)]
    coordinator: Option<String>,
    /// Backend namespace to enumerate, for example `az/container`.
    #[arg(long)]
    namespace_prefix: Option<String>,
    /// Logical block size in bytes.
    #[arg(long)]
    block_size: Option<u32>,
}

impl Args {
    /// Assemble the highest-precedence (CLI) config patch from parsed flags.
    fn to_patch(&self) -> FuseConfigPatch {
        FuseConfigPatch {
            mountpoint: self.mountpoint.clone(),
            coordinator: self.coordinator.clone(),
            namespace_prefix: self.namespace_prefix.clone(),
            block_size: self.block_size,
            placement_ttl_ms: None,
            readahead_blocks: None,
        }
    }
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
        Some(path) => FuseConfigPatch::from_file(path)?,
        None => FuseConfigPatch::default(),
    };
    let env = FuseConfigPatch::from_env()?;
    let cfg = FuseConfig::resolve(file, env, args.to_patch())?;

    tracing::info!(
        mountpoint = %cfg.mountpoint.display(),
        coordinator = %cfg.coordinator,
        namespace_prefix = %cfg.namespace_prefix,
        block_size = cfg.block_size,
        placement_ttl_ms = cfg.placement_ttl_ms,
        readahead_blocks = cfg.readahead_blocks,
        "starting talon-fuse"
    );

    // Read-path components, shared by the metadata and data callbacks.
    let coordinator = CoordinatorClient::new(cfg.coordinator.clone());
    let cache = Arc::new(PlacementCache::new(cfg.placement_ttl_ms));
    let reader = BlockReader::new(coordinator.clone(), cache, 1);

    // Populate the namespace before mounting. A listing failure is fatal: an
    // apparently healthy mount with an empty tree hides backend/configuration
    // failures and is less useful than an actionable startup error (#366).
    let (mount_uid, mount_gid) = mount_owner();
    let fs = Arc::new(ReadOnlyFs::new_with_owner(mount_uid, mount_gid));
    let listing = coordinator.list_objects(&cfg.namespace_prefix).await;
    populate_namespace(&fs, &cfg.namespace_prefix, listing)?;

    run_mount(cfg, fs, reader).await
}

/// Apply the coordinator's startup listing, refusing to hide listing errors
/// behind an apparently healthy empty mount.
fn populate_namespace<E>(
    fs: &ReadOnlyFs,
    namespace_prefix: &str,
    listing: Result<Vec<ObjectEntry>, E>,
) -> anyhow::Result<usize>
where
    E: std::fmt::Display,
{
    let entries = listing.map_err(|error| {
        anyhow::anyhow!("failed to list namespace prefix {namespace_prefix:?}: {error}")
    })?;
    if entries.is_empty() {
        tracing::warn!(namespace_prefix, "namespace listing returned zero objects");
    }
    let n = fs.populate_from_listing(entries.iter().map(|e| (e.path.as_str(), e.size)));
    tracing::info!(
        objects = n,
        namespace_prefix,
        "populated namespace from coordinator listing"
    );
    Ok(n)
}

#[cfg(feature = "mount")]
fn mount_owner() -> (u32, u32) {
    // SAFETY: geteuid/getegid have no preconditions and do not mutate memory.
    unsafe { (libc::geteuid(), libc::getegid()) }
}

#[cfg(not(feature = "mount"))]
fn mount_owner() -> (u32, u32) {
    (0, 0)
}

/// Mount and serve until SIGINT (built with `--features mount`).
#[cfg(feature = "mount")]
async fn run_mount(
    cfg: FuseConfig,
    fs: Arc<ReadOnlyFs>,
    reader: BlockReader,
) -> anyhow::Result<()> {
    use fuser::MountOption;
    use talon_fuse::mount::{TalonFuse, CANONICAL_MOUNT_VERSION};

    // The mount path is version-independent by design: the worker is the sole
    // authority on freshness (#119/#163) and the client never sends a version to
    // it, so every block is addressed under one canonical token (#182).
    let version = talon_core::Version::new(CANONICAL_MOUNT_VERSION);
    let handle = tokio::runtime::Handle::current();
    let stats = reader.stats().clone();
    let adapter = TalonFuse::new(fs, reader, handle, cfg.block_size, version)
        .with_readahead(cfg.readahead_blocks)
        // Write-through is enabled for the mount binary (opt-in via the `mount`
        // feature). A future config flag can gate this per-mount (#232).
        .with_read_write(true);

    // Mounted read-write (write-through to the backend, #226/#232); FSName tags
    // the mount and DefaultPermissions lets the kernel enforce the synthesized
    // perms.
    let options = vec![
        MountOption::FSName("talon".to_string()),
        MountOption::DefaultPermissions,
    ];

    // spawn_mount2 runs the session on a background thread and returns a guard;
    // dropping the guard (or an explicit unmount) tears the mount down.
    let session = fuser::spawn_mount2(adapter, &cfg.mountpoint, &options)?;
    tracing::info!(mountpoint = %cfg.mountpoint.display(), "mounted; press Ctrl-C to unmount");

    tokio::signal::ctrl_c().await?;
    tracing::info!("SIGINT received; unmounting");
    // Dropping the BackgroundSession unmounts and joins the session thread.
    drop(session);
    let s = stats.snapshot();
    tracing::info!(
        cache_hits = s.cache_hits,
        cache_misses = s.cache_misses,
        hit_ratio = s.hit_ratio(),
        worker_fetches = s.worker_fetches,
        worker_failures = s.worker_failures,
        coordinator_refreshes = s.coordinator_refreshes,
        bytes_served = s.bytes_served,
        "read-path metrics at unmount"
    );
    Ok(())
}

/// Built without the `mount` feature: set everything up but do not mount.
#[cfg(not(feature = "mount"))]
async fn run_mount(
    cfg: FuseConfig,
    _fs: Arc<ReadOnlyFs>,
    _reader: BlockReader,
) -> anyhow::Result<()> {
    tracing::warn!(
        mountpoint = %cfg.mountpoint.display(),
        "built without the `mount` feature: not mounting. \
         Rebuild with `--features mount` to enable the kernel FUSE mount."
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_prefix_cli_flag_populates_patch() {
        let args =
            Args::try_parse_from(["talon-fuse", "--namespace-prefix", "az/container/datasets"])
                .unwrap();

        assert_eq!(
            args.to_patch().namespace_prefix.as_deref(),
            Some("az/container/datasets")
        );
    }

    #[test]
    fn namespace_listing_failure_is_fatal() {
        let fs = ReadOnlyFs::new();
        let error =
            populate_namespace::<&str>(&fs, "s3/training-data", Err("coordinator unavailable"))
                .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("s3/training-data"));
        assert!(message.contains("coordinator unavailable"));
    }

    #[test]
    fn successful_namespace_listing_populates_tree() {
        let fs = ReadOnlyFs::new();
        let count = populate_namespace::<&str>(
            &fs,
            "gcs/models",
            Ok(vec![ObjectEntry {
                path: "gcs/models/checkpoint.bin".into(),
                size: 42,
            }]),
        )
        .unwrap();

        assert_eq!(count, 1);
        let gcs = fs.lookup(talon_fuse::ops::ROOT_INO, "gcs").unwrap();
        let models = fs.lookup(gcs.ino, "models").unwrap();
        assert!(fs.lookup(models.ino, "checkpoint.bin").is_ok());
    }
}
