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

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use talon_core::{FuseConfig, FuseConfigPatch};
use talon_fuse::{path_to_object, BlockReader, CoordinatorClient, PlacementCache, ReadOnlyFs};
use talon_transport::ObjectEntry;

/// Warn about a persisting zone-affinity fallback at most this often.
const FALLBACK_WARN_INTERVAL_MS: u64 = 300_000;

/// Zone-affinity events surfaced as throttled log lines. The mount has no
/// metrics endpoint (the gateway exports real counters), and a fallback can
/// persist for hours, so warn once per interval rather than per resolve.
#[derive(Default)]
struct LoggingZoneObserver {
    last_fallback_warn_ms: AtomicU64,
}

impl talon_cache_client::ZoneReadObserver for LoggingZoneObserver {
    fn affinity_fallback(&self) {
        let now = now_unix_ms();
        let last = self.last_fallback_warn_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= FALLBACK_WARN_INTERVAL_MS
            && self
                .last_fallback_warn_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            tracing::warn!(
                "zone affinity fell back to full membership: no same-zone workers reachable"
            );
        }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

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
    // Zone affinity (ADR 0006): env-only for the mount — the FUSE client
    // typically runs outside Kubernetes, so there is no node-label lookup.
    let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let zone = env("TALON_ZONE");
    let zone_affinity = match env("TALON_ZONE_AFFINITY") {
        None => false,
        Some(value) => talon_core::parse_bool_value(&value).ok_or_else(|| {
            anyhow::anyhow!("TALON_ZONE_AFFINITY must be true or false, got {value:?}")
        })?,
    };
    if zone_affinity || zone.is_some() {
        tracing::info!(
            zone = zone.as_deref().unwrap_or("unknown"),
            zone_affinity,
            "zone affinity configuration"
        );
    }
    let reader = BlockReader::new(coordinator.clone(), cache, 1).with_zone_affinity(
        zone,
        zone_affinity,
        Arc::new(LoggingZoneObserver::default()),
    );

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

    // Validate the entire response before mutating the tree.  Object-store keys
    // may legally contain empty, `.` or `..` components, but those cannot be
    // represented reversibly in a POSIX namespace.  Failing the mount is safer
    // than silently mapping (for example) `foo//bar` to the different key
    // `foo/bar`.  A single trailing slash is retained as a directory marker.
    validate_listing_paths(namespace_prefix, &entries)?;

    let n = fs.populate_from_listing(entries.iter().map(|e| (e.path.as_str(), e.size)));
    if n == 0 {
        tracing::warn!(
            namespace_prefix,
            "namespace listing returned zero visible objects"
        );
    }
    tracing::info!(
        objects = n,
        namespace_prefix,
        "populated namespace from coordinator listing"
    );
    Ok(n)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListingPathKind {
    File,
    DirectoryMarker,
}

const MAX_FUSE_COMPONENT_BYTES: usize = 255;

/// Validate individual paths, requested scope, and relationships across the
/// whole listing.
///
/// Object stores permit a key such as `foo` to coexist with `foo/bar`, but a
/// POSIX node cannot be both a file and a directory. Detect those collisions
/// before populating so the result does not depend on backend listing order.
fn validate_listing_paths(namespace_prefix: &str, entries: &[ObjectEntry]) -> anyhow::Result<()> {
    let namespace_prefix = namespace_prefix
        .strip_prefix('/')
        .unwrap_or(namespace_prefix);
    let mut scope_parts = namespace_prefix.splitn(3, '/');
    let backend = scope_parts.next().unwrap_or_default();
    let bucket = scope_parts.next().unwrap_or_default();
    if backend.is_empty() || bucket.is_empty() {
        anyhow::bail!("invalid namespace prefix {namespace_prefix:?}");
    }
    let key_prefix = scope_parts.next().unwrap_or_default();
    let namespace_root = format!("{backend}/{bucket}/");

    let mut paths = BTreeMap::new();
    for entry in entries {
        validate_listing_path(&entry.path)?;
        let entry_key = entry.path.strip_prefix(&namespace_root).ok_or_else(|| {
            anyhow::anyhow!(
                "namespace listing for {namespace_prefix:?} returned out-of-scope object path {:?}",
                entry.path
            )
        })?;
        if !entry_key.starts_with(key_prefix) {
            anyhow::bail!(
                "namespace listing for {namespace_prefix:?} returned out-of-scope object path {:?}",
                entry.path
            );
        }
        let (path, kind) = match entry.path.strip_suffix('/') {
            Some(path) => {
                if entry.size != 0 {
                    anyhow::bail!(
                        "namespace listing contains non-empty trailing-slash object {:?} ({} bytes); only zero-byte directory markers can be represented safely",
                        entry.path,
                        entry.size
                    );
                }
                (path, ListingPathKind::DirectoryMarker)
            }
            None => (entry.path.as_str(), ListingPathKind::File),
        };

        if let Some(existing) = paths.insert(path, kind) {
            if existing == kind {
                anyhow::bail!(
                    "namespace listing contains duplicate object path {:?}",
                    entry.path
                );
            }
            anyhow::bail!(
                "namespace listing contains conflicting file and directory marker for {path:?}"
            );
        }
    }

    for path in paths.keys().copied() {
        for (slash, _) in path.match_indices('/') {
            let ancestor = &path[..slash];
            if paths.get(ancestor) == Some(&ListingPathKind::File) {
                anyhow::bail!(
                    "namespace listing cannot represent file {ancestor:?} alongside descendant {path:?}"
                );
            }
        }
    }
    Ok(())
}

/// Require the coordinator's mount-relative path to round-trip exactly through
/// Talon's object-path mapping. Directory markers use one trailing slash,
/// which is intentionally outside [`path_to_object`]'s file-path grammar.
fn validate_listing_path(path: &str) -> anyhow::Result<()> {
    let object_path = path.strip_suffix('/').unwrap_or(path);
    if path.starts_with('/') {
        anyhow::bail!("namespace listing returned non-canonical object path {path:?}");
    }
    for component in object_path.split('/') {
        if matches!(component, "" | "." | "..") {
            anyhow::bail!("namespace listing returned non-canonical object path {path:?}");
        }
        if component.as_bytes().contains(&0) {
            anyhow::bail!("namespace listing returned object path {path:?} containing a NUL byte");
        }
        if component.len() > MAX_FUSE_COMPONENT_BYTES {
            anyhow::bail!(
                "namespace listing returned object path {path:?} with a component longer than {MAX_FUSE_COMPONENT_BYTES} bytes"
            );
        }
    }

    let object = path_to_object(object_path).map_err(|error| {
        anyhow::anyhow!("namespace listing returned invalid object path {path:?}: {error}")
    })?;
    let canonical = object.to_path();
    let canonical = canonical
        .strip_prefix('/')
        .expect("ObjectId::to_path always starts with a slash");
    if canonical != object_path {
        anyhow::bail!(
            "namespace listing returned non-canonical object path {path:?}; expected {canonical:?}"
        );
    }
    Ok(())
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

    #[test]
    fn namespace_listing_rejects_ambiguous_paths_before_populating() {
        for invalid in [
            "/s3/bucket/file.bin",
            "azure/container/file.bin",
            "s3/bucket/a//b",
            "s3/bucket/./file.bin",
            "s3/bucket/../file.bin",
            "s3/./file.bin",
            "s3/../file.bin",
            "s3/bucket/dir//",
        ] {
            let fs = ReadOnlyFs::new();
            let error = populate_namespace::<&str>(
                &fs,
                "s3/bucket",
                Ok(vec![
                    ObjectEntry {
                        path: "s3/bucket/valid.bin".into(),
                        size: 1,
                    },
                    ObjectEntry {
                        path: invalid.into(),
                        size: 2,
                    },
                ]),
            )
            .unwrap_err();

            assert!(error.to_string().contains(invalid), "{error:#}");
            assert!(
                fs.lookup(talon_fuse::ops::ROOT_INO, "s3").is_err(),
                "{invalid:?} left a partially populated namespace"
            );
        }
    }

    #[test]
    fn namespace_listing_accepts_directory_markers() {
        let entries = vec![
            ObjectEntry {
                path: "s3/bucket/empty/".into(),
                size: 0,
            },
            ObjectEntry {
                path: "s3/bucket/empty/file.bin".into(),
                size: 3,
            },
        ];

        for reverse in [false, true] {
            let fs = ReadOnlyFs::new();
            let mut ordered = entries.clone();
            if reverse {
                ordered.reverse();
            }
            let count = populate_namespace::<&str>(&fs, "s3/bucket", Ok(ordered)).unwrap();

            assert_eq!(count, 2);
            let s3 = fs.lookup(talon_fuse::ops::ROOT_INO, "s3").unwrap();
            let bucket = fs.lookup(s3.ino, "bucket").unwrap();
            let empty = fs.lookup(bucket.ino, "empty").unwrap();
            assert_eq!(empty.kind, talon_fuse::FileKind::Directory);
            assert!(fs.lookup(empty.ino, "file.bin").is_ok());
        }
    }

    #[test]
    fn namespace_listing_rejects_unrepresentable_components_before_populating() {
        let invalid = [
            "s3/bucket/nul\0name".to_string(),
            format!("s3/bucket/{}", "x".repeat(MAX_FUSE_COMPONENT_BYTES + 1)),
        ];

        for path in invalid {
            let fs = ReadOnlyFs::new();
            let error = populate_namespace::<&str>(
                &fs,
                "s3/bucket",
                Ok(vec![ObjectEntry { path, size: 1 }]),
            )
            .unwrap_err();

            assert!(error.to_string().contains("namespace listing"), "{error:#}");
            assert!(
                fs.lookup(talon_fuse::ops::ROOT_INO, "s3").is_err(),
                "invalid listing left a partially populated namespace"
            );
        }
    }

    #[test]
    fn namespace_listing_rejects_nonempty_trailing_slash_objects() {
        let fs = ReadOnlyFs::new();
        let error = populate_namespace::<&str>(
            &fs,
            "s3/bucket",
            Ok(vec![ObjectEntry {
                path: "s3/bucket/not-a-marker/".into(),
                size: 7,
            }]),
        )
        .unwrap_err();

        assert!(error.to_string().contains("non-empty trailing-slash"));
        assert!(fs.lookup(talon_fuse::ops::ROOT_INO, "s3").is_err());
    }

    #[test]
    fn namespace_listing_enforces_backend_bucket_and_raw_key_scope() {
        for (namespace_prefix, in_scope, out_of_scope) in [
            ("s3/bucket", "s3/bucket/file.bin", "gcs/bucket/file.bin"),
            ("s3/bucket", "s3/bucket/file.bin", "s3/other/file.bin"),
            (
                "s3/bucket/wanted",
                "s3/bucket/wanted.bin",
                "s3/bucket/other.bin",
            ),
            (
                "s3/bucket/dir/",
                "s3/bucket/dir/file.bin",
                "s3/bucket/dir2/file.bin",
            ),
        ] {
            let fs = ReadOnlyFs::new();
            let error = populate_namespace::<&str>(
                &fs,
                namespace_prefix,
                Ok(vec![
                    ObjectEntry {
                        path: in_scope.into(),
                        size: 1,
                    },
                    ObjectEntry {
                        path: out_of_scope.into(),
                        size: 2,
                    },
                ]),
            )
            .unwrap_err();

            assert!(error.to_string().contains("out-of-scope"), "{error:#}");
            assert!(
                fs.lookup(talon_fuse::ops::ROOT_INO, "s3").is_err(),
                "out-of-scope listing left a partially populated namespace"
            );
        }
    }

    #[test]
    fn namespace_listing_preserves_raw_prefix_semantics() {
        let fs = ReadOnlyFs::new();
        let count = populate_namespace::<&str>(
            &fs,
            "/s3/bucket/dir",
            Ok(vec![
                ObjectEntry {
                    path: "s3/bucket/dir2/file.bin".into(),
                    size: 1,
                },
                ObjectEntry {
                    path: "s3/bucket/directory/file.bin".into(),
                    size: 2,
                },
            ]),
        )
        .unwrap();
        assert_eq!(count, 2);

        let fs = ReadOnlyFs::new();
        let count = populate_namespace::<&str>(
            &fs,
            "s3/bucket/dir/",
            Ok(vec![
                ObjectEntry {
                    path: "s3/bucket/dir/".into(),
                    size: 0,
                },
                ObjectEntry {
                    path: "s3/bucket/dir/file.bin".into(),
                    size: 3,
                },
            ]),
        )
        .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn namespace_listing_rejects_tree_conflicts_in_any_order() {
        let conflicts = [
            vec![
                ObjectEntry {
                    path: "s3/bucket/foo".into(),
                    size: 1,
                },
                ObjectEntry {
                    path: "s3/bucket/foo/bar".into(),
                    size: 2,
                },
            ],
            vec![
                ObjectEntry {
                    path: "s3/bucket/foo".into(),
                    size: 1,
                },
                ObjectEntry {
                    path: "s3/bucket/foo/".into(),
                    size: 0,
                },
            ],
            vec![
                ObjectEntry {
                    path: "s3/bucket/foo".into(),
                    size: 1,
                },
                ObjectEntry {
                    path: "s3/bucket/foo/bar/".into(),
                    size: 0,
                },
            ],
            vec![
                ObjectEntry {
                    path: "s3/bucket/foo".into(),
                    size: 1,
                },
                ObjectEntry {
                    path: "s3/bucket/foo".into(),
                    size: 2,
                },
            ],
        ];

        for entries in conflicts {
            for reverse in [false, true] {
                let fs = ReadOnlyFs::new();
                let mut ordered = entries.clone();
                if reverse {
                    ordered.reverse();
                }
                let error = populate_namespace::<&str>(&fs, "s3/bucket", Ok(ordered)).unwrap_err();

                assert!(error.to_string().contains("namespace listing"), "{error:#}");
                assert!(
                    fs.lookup(talon_fuse::ops::ROOT_INO, "s3").is_err(),
                    "conflicting listing left a partially populated namespace"
                );
            }
        }
    }

    #[test]
    fn internal_only_listing_has_zero_visible_objects() {
        let fs = ReadOnlyFs::new();
        let count = populate_namespace::<&str>(
            &fs,
            "s3/bucket",
            Ok(vec![ObjectEntry {
                path: "s3/bucket/.__talon_internal/unlinked/stale/1".into(),
                size: 7,
            }]),
        )
        .unwrap();

        assert_eq!(count, 0);
    }
}
