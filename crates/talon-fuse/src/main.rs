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
        block_size = cfg.block_size,
        placement_ttl_ms = cfg.placement_ttl_ms,
        readahead_blocks = cfg.readahead_blocks,
        "starting talon-fuse"
    );

    // Read-path components, shared by the metadata and data callbacks.
    let coordinator = CoordinatorClient::new(cfg.coordinator.clone());
    let cache = Arc::new(PlacementCache::new(cfg.placement_ttl_ms));
    let reader = BlockReader::new(coordinator.clone(), cache, 1);

    // Populate the namespace from a coordinator listing (best-effort: a listing
    // failure logs and yields an empty tree rather than aborting the mount).
    let (mount_uid, mount_gid) = mount_owner();
    let fs = Arc::new(ReadOnlyFs::new_with_owner(mount_uid, mount_gid));
    match coordinator.list_objects("").await {
        Ok(entries) => {
            let n = fs.populate_from_listing(entries.iter().map(|e| (e.path.as_str(), e.size)));
            tracing::info!(objects = n, "populated namespace from coordinator listing");
        }
        Err(e) => {
            // Deliberately non-fatal for now, but loud: there is no
            // cross-backend root listing — a worker cannot enumerate every
            // bucket across S3, GCS, and Azure — so `list_objects("")` cannot
            // succeed even now that listing is implemented (#332). The mount
            // still serves reads for paths a client already knows.
            //
            // This silent degradation is why #318 and #332 went unnoticed for
            // months: the endpoints were unimplemented and the mount still
            // reported success. Populating the namespace properly needs a
            // configured prefix naming a backend and bucket, tracked in #366.
            tracing::warn!(
                error = %e,
                "namespace listing failed; the mount will appear empty until a \
                 namespace prefix is configured (#366). Reads of known paths still work."
            );
        }
    }

    run_mount(cfg, fs, reader).await
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
