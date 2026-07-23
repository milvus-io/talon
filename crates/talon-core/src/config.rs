//! Layered configuration resolution.
//!
//! Configuration is resolved from four layers, highest precedence first:
//!
//! 1. **CLI flags** — debugging / one-off overrides.
//! 2. **Environment** — deployment injection, secrets, node identity.
//! 3. **Config file** (TOML) — stable parameters (ports, block size, cache
//!    dirs, capacity, backend).
//! 4. **Defaults** — compiled-in fallbacks.
//!
//! Each concrete config type pairs a fully-resolved struct (e.g.
//! [`WorkerConfig`]) with a *patch* struct whose fields are all optional. A
//! patch is produced from each layer and folded onto the defaults in
//! precedence order via [`Patch::merge`]; the result is [`validate`]d.
//!
//! Secrets are read only from the environment and are never serialized or
//! logged.
//!
//! [`validate`]: WorkerConfig::validate

use serde::Deserialize;
use std::path::PathBuf;

use crate::status::MAX_STATUS_FIELD_BYTES;
use crate::{Error, Result};

/// A configuration patch: a set of optionally-present overrides.
///
/// Higher-precedence patches are merged *onto* lower-precedence values so that
/// only explicitly-set fields override what came before.
pub trait Patch {
    /// Overlay `self` onto `base`, letting `self`'s set fields win.
    fn merge(self, base: Self) -> Self;
}

/// Fully-resolved worker configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    /// Address the worker's RPC service binds to.
    pub listen: String,
    /// Address the worker's HTTP administration service binds to.
    pub admin_listen: String,
    /// Address of the coordinator to register with.
    pub coordinator: String,
    /// Logical cluster advertised in node status.
    pub cluster_id: String,
    /// Stable node identity; defaults to the RPC listen address when unset.
    pub node_id: Option<String>,
    /// Control-plane heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Logical block size in bytes (256 MiB default).
    pub block_size: u32,
    /// One or more cache directory roots on local NVMe.
    pub cache_dirs: Vec<PathBuf>,
    /// Total cache capacity in bytes across all cache dirs.
    pub capacity_bytes: u64,
    /// Azure Blob storage account for the backend origin (`None` if unset).
    ///
    /// The container is taken per-object from the request path; the SAS token is
    /// **not** stored here — it is read from the environment at use time (see
    /// [`azure_sas_from_env`]) so a secret never lands in a config struct or log.
    pub azure_account: Option<String>,
}

/// Read the Azure SAS token from the environment (`TALON_WORKER_AZURE_SAS`).
///
/// Returned as an opaque string and intended for immediate use; it is
/// deliberately kept out of [`WorkerConfig`] so it is never serialized, printed
/// via `Debug`, or logged.
pub fn azure_sas_from_env() -> Option<String> {
    std::env::var("TALON_WORKER_AZURE_SAS")
        .ok()
        .filter(|s| !s.is_empty())
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7001".into(),
            admin_listen: "127.0.0.1:8001".into(),
            coordinator: "127.0.0.1:7000".into(),
            cluster_id: "default".into(),
            node_id: None,
            heartbeat_interval_ms: 5_000,
            block_size: 256 << 20,
            cache_dirs: vec![PathBuf::from("/var/cache/talon")],
            capacity_bytes: 64 << 30,
            azure_account: None,
        }
    }
}

/// An optional-field overlay for [`WorkerConfig`].
///
/// Deserialized from the config file, and also assembled from env and CLI
/// layers. Every field is optional so a layer only overrides what it sets.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfigPatch {
    /// Override for [`WorkerConfig::listen`].
    pub listen: Option<String>,
    /// Override for [`WorkerConfig::admin_listen`].
    pub admin_listen: Option<String>,
    /// Override for [`WorkerConfig::coordinator`].
    pub coordinator: Option<String>,
    /// Override for [`WorkerConfig::cluster_id`].
    pub cluster_id: Option<String>,
    /// Override for [`WorkerConfig::node_id`].
    pub node_id: Option<String>,
    /// Override for [`WorkerConfig::heartbeat_interval_ms`].
    pub heartbeat_interval_ms: Option<u64>,
    /// Override for [`WorkerConfig::block_size`].
    pub block_size: Option<u32>,
    /// Override for [`WorkerConfig::cache_dirs`].
    pub cache_dirs: Option<Vec<PathBuf>>,
    /// Override for [`WorkerConfig::capacity_bytes`].
    pub capacity_bytes: Option<u64>,
    /// Override for [`WorkerConfig::azure_account`].
    pub azure_account: Option<String>,
}

impl Patch for WorkerConfigPatch {
    fn merge(self, base: Self) -> Self {
        Self {
            listen: self.listen.or(base.listen),
            admin_listen: self.admin_listen.or(base.admin_listen),
            coordinator: self.coordinator.or(base.coordinator),
            cluster_id: self.cluster_id.or(base.cluster_id),
            node_id: self.node_id.or(base.node_id),
            heartbeat_interval_ms: self.heartbeat_interval_ms.or(base.heartbeat_interval_ms),
            block_size: self.block_size.or(base.block_size),
            cache_dirs: self.cache_dirs.or(base.cache_dirs),
            capacity_bytes: self.capacity_bytes.or(base.capacity_bytes),
            azure_account: self.azure_account.or(base.azure_account),
        }
    }
}

impl WorkerConfigPatch {
    /// Parse a patch from a TOML config-file string.
    pub fn from_toml(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::Other(format!("invalid config file: {e}")))
    }

    /// Read a patch from a TOML file path. A missing file yields an empty patch.
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => Self::from_toml(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Assemble a patch from `TALON_WORKER_*` environment variables.
    ///
    /// Recognized keys include `TALON_WORKER_LISTEN`,
    /// `TALON_WORKER_ADMIN_LISTEN`, `TALON_WORKER_COORDINATOR`,
    /// `TALON_WORKER_CLUSTER_ID`, `TALON_WORKER_NODE_ID`,
    /// `TALON_WORKER_HEARTBEAT_INTERVAL_MS`, `TALON_WORKER_BLOCK_SIZE`,
    /// `TALON_WORKER_CACHE_DIRS` (`:`-separated), and
    /// `TALON_WORKER_CAPACITY_BYTES`, and `TALON_WORKER_AZURE_ACCOUNT`.
    pub fn from_env() -> Result<Self> {
        Self::from_env_with(|k| std::env::var(k).ok())
    }

    /// Like [`from_env`](Self::from_env) but with an injectable lookup, for
    /// tests.
    pub fn from_env_with(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let parse_u32 = |v: String, k: &str| {
            v.parse::<u32>()
                .map_err(|_| Error::Other(format!("{k}: invalid u32: {v:?}")))
        };
        let parse_u64 = |v: String, k: &str| {
            v.parse::<u64>()
                .map_err(|_| Error::Other(format!("{k}: invalid u64: {v:?}")))
        };
        Ok(Self {
            listen: get("TALON_WORKER_LISTEN"),
            admin_listen: get("TALON_WORKER_ADMIN_LISTEN"),
            coordinator: get("TALON_WORKER_COORDINATOR"),
            cluster_id: get("TALON_WORKER_CLUSTER_ID"),
            node_id: get("TALON_WORKER_NODE_ID"),
            heartbeat_interval_ms: get("TALON_WORKER_HEARTBEAT_INTERVAL_MS")
                .map(|v| parse_u64(v, "TALON_WORKER_HEARTBEAT_INTERVAL_MS"))
                .transpose()?,
            block_size: get("TALON_WORKER_BLOCK_SIZE")
                .map(|v| parse_u32(v, "TALON_WORKER_BLOCK_SIZE"))
                .transpose()?,
            cache_dirs: get("TALON_WORKER_CACHE_DIRS")
                .map(|v| v.split(':').map(PathBuf::from).collect()),
            capacity_bytes: get("TALON_WORKER_CAPACITY_BYTES")
                .map(|v| parse_u64(v, "TALON_WORKER_CAPACITY_BYTES"))
                .transpose()?,
            azure_account: get("TALON_WORKER_AZURE_ACCOUNT"),
        })
    }
}

impl WorkerConfig {
    /// Resolve config across all layers: defaults < file < env < CLI.
    ///
    /// `cli` is the highest-precedence patch (assembled from parsed CLI flags);
    /// `env` and `file` are lower. Any layer may be [`WorkerConfigPatch::default`]
    /// (empty) to skip it.
    pub fn resolve(
        file: WorkerConfigPatch,
        env: WorkerConfigPatch,
        cli: WorkerConfigPatch,
    ) -> Result<Self> {
        // Fold highest-first onto lower layers, then onto defaults.
        let merged = cli.merge(env).merge(file);
        let d = WorkerConfig::default();
        let cfg = WorkerConfig {
            listen: merged.listen.unwrap_or(d.listen),
            admin_listen: merged.admin_listen.unwrap_or(d.admin_listen),
            coordinator: merged.coordinator.unwrap_or(d.coordinator),
            cluster_id: merged.cluster_id.unwrap_or(d.cluster_id),
            node_id: merged.node_id.or(d.node_id),
            heartbeat_interval_ms: merged
                .heartbeat_interval_ms
                .unwrap_or(d.heartbeat_interval_ms),
            block_size: merged.block_size.unwrap_or(d.block_size),
            cache_dirs: merged.cache_dirs.unwrap_or(d.cache_dirs),
            capacity_bytes: merged.capacity_bytes.unwrap_or(d.capacity_bytes),
            azure_account: merged.azure_account.or(d.azure_account),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Fail fast on invalid configuration with an actionable message.
    pub fn validate(&self) -> Result<()> {
        if self.listen.is_empty() {
            return Err(Error::Other("listen address must not be empty".into()));
        }
        if self.admin_listen.is_empty() {
            return Err(Error::Other(
                "admin_listen address must not be empty".into(),
            ));
        }
        if self.coordinator.is_empty() {
            return Err(Error::Other("coordinator address must not be empty".into()));
        }
        if self.cluster_id.is_empty() {
            return Err(Error::Other("cluster_id must not be empty".into()));
        }
        if self.node_id.as_ref().is_some_and(String::is_empty) {
            return Err(Error::Other("node_id must not be empty when set".into()));
        }
        if self.heartbeat_interval_ms == 0 {
            return Err(Error::Other(
                "heartbeat_interval_ms must be greater than zero".into(),
            ));
        }
        for (name, value) in [
            ("listen", self.listen.as_str()),
            ("admin_listen", self.admin_listen.as_str()),
            ("cluster_id", self.cluster_id.as_str()),
        ] {
            if value.len() > MAX_STATUS_FIELD_BYTES {
                return Err(Error::Other(format!(
                    "{name} is {} bytes; maximum is {MAX_STATUS_FIELD_BYTES}",
                    value.len()
                )));
            }
        }
        if let Some(node_id) = &self.node_id {
            if node_id.len() > MAX_STATUS_FIELD_BYTES {
                return Err(Error::Other(format!(
                    "node_id is {} bytes; maximum is {MAX_STATUS_FIELD_BYTES}",
                    node_id.len()
                )));
            }
        }
        if self.block_size == 0 {
            return Err(Error::Other("block_size must be > 0".into()));
        }
        if self.cache_dirs.is_empty() {
            return Err(Error::Other("at least one cache_dir is required".into()));
        }
        if self.capacity_bytes < self.block_size as u64 {
            return Err(Error::Other(format!(
                "capacity_bytes ({}) must be >= block_size ({})",
                self.capacity_bytes, self.block_size
            )));
        }
        Ok(())
    }
}

/// Fully-resolved FUSE client configuration.
///
/// Mirrors the layered pattern of [`WorkerConfig`]: a resolved struct plus an
/// optional-field [`FuseConfigPatch`] folded across defaults < file < env < CLI.
/// The FUSE client is read-only; these knobs tune where it mounts, which
/// coordinator it asks for placement, and its client-side caching / readahead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseConfig {
    /// Directory to mount the Talon filesystem at.
    pub mountpoint: PathBuf,
    /// Address of the coordinator to resolve placement + membership against.
    pub coordinator: String,
    /// Logical block size in bytes (must match the cluster's; 256 MiB default).
    pub block_size: u32,
    /// Placement-cache entry TTL in milliseconds.
    ///
    /// A cached block→owners mapping is treated as a miss once older than this,
    /// bounding how long a client can act on stale placement before refreshing.
    pub placement_ttl_ms: u64,
    /// Number of blocks to prefetch ahead once a sequential read run is detected.
    ///
    /// `0` disables readahead entirely.
    pub readahead_blocks: u32,
}

impl Default for FuseConfig {
    fn default() -> Self {
        Self {
            mountpoint: PathBuf::from("/mnt/talon"),
            coordinator: "127.0.0.1:7000".into(),
            block_size: 256 << 20,
            placement_ttl_ms: 5_000,
            readahead_blocks: 4,
        }
    }
}

/// An optional-field overlay for [`FuseConfig`].
///
/// Deserialized from the config file, and also assembled from env and CLI
/// layers. Every field is optional so a layer only overrides what it sets.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FuseConfigPatch {
    /// Override for [`FuseConfig::mountpoint`].
    pub mountpoint: Option<PathBuf>,
    /// Override for [`FuseConfig::coordinator`].
    pub coordinator: Option<String>,
    /// Override for [`FuseConfig::block_size`].
    pub block_size: Option<u32>,
    /// Override for [`FuseConfig::placement_ttl_ms`].
    pub placement_ttl_ms: Option<u64>,
    /// Override for [`FuseConfig::readahead_blocks`].
    pub readahead_blocks: Option<u32>,
}

impl Patch for FuseConfigPatch {
    fn merge(self, base: Self) -> Self {
        Self {
            mountpoint: self.mountpoint.or(base.mountpoint),
            coordinator: self.coordinator.or(base.coordinator),
            block_size: self.block_size.or(base.block_size),
            placement_ttl_ms: self.placement_ttl_ms.or(base.placement_ttl_ms),
            readahead_blocks: self.readahead_blocks.or(base.readahead_blocks),
        }
    }
}

impl FuseConfigPatch {
    /// Parse a patch from a TOML config-file string.
    pub fn from_toml(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::Other(format!("invalid config file: {e}")))
    }

    /// Read a patch from a TOML file path. A missing file yields an empty patch.
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => Self::from_toml(&s),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Assemble a patch from `TALON_FUSE_*` environment variables.
    ///
    /// Recognized keys: `TALON_FUSE_MOUNTPOINT`, `TALON_FUSE_COORDINATOR`,
    /// `TALON_FUSE_BLOCK_SIZE`, `TALON_FUSE_PLACEMENT_TTL_MS`,
    /// `TALON_FUSE_READAHEAD_BLOCKS`.
    pub fn from_env() -> Result<Self> {
        Self::from_env_with(|k| std::env::var(k).ok())
    }

    /// Like [`from_env`](Self::from_env) but with an injectable lookup, for tests.
    pub fn from_env_with(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let parse_u32 = |v: String, k: &str| {
            v.parse::<u32>()
                .map_err(|_| Error::Other(format!("{k}: invalid u32: {v:?}")))
        };
        let parse_u64 = |v: String, k: &str| {
            v.parse::<u64>()
                .map_err(|_| Error::Other(format!("{k}: invalid u64: {v:?}")))
        };
        Ok(Self {
            mountpoint: get("TALON_FUSE_MOUNTPOINT").map(PathBuf::from),
            coordinator: get("TALON_FUSE_COORDINATOR"),
            block_size: get("TALON_FUSE_BLOCK_SIZE")
                .map(|v| parse_u32(v, "TALON_FUSE_BLOCK_SIZE"))
                .transpose()?,
            placement_ttl_ms: get("TALON_FUSE_PLACEMENT_TTL_MS")
                .map(|v| parse_u64(v, "TALON_FUSE_PLACEMENT_TTL_MS"))
                .transpose()?,
            readahead_blocks: get("TALON_FUSE_READAHEAD_BLOCKS")
                .map(|v| parse_u32(v, "TALON_FUSE_READAHEAD_BLOCKS"))
                .transpose()?,
        })
    }
}

impl FuseConfig {
    /// Resolve config across all layers: defaults < file < env < CLI.
    pub fn resolve(
        file: FuseConfigPatch,
        env: FuseConfigPatch,
        cli: FuseConfigPatch,
    ) -> Result<Self> {
        let merged = cli.merge(env).merge(file);
        let d = FuseConfig::default();
        let cfg = FuseConfig {
            mountpoint: merged.mountpoint.unwrap_or(d.mountpoint),
            coordinator: merged.coordinator.unwrap_or(d.coordinator),
            block_size: merged.block_size.unwrap_or(d.block_size),
            placement_ttl_ms: merged.placement_ttl_ms.unwrap_or(d.placement_ttl_ms),
            readahead_blocks: merged.readahead_blocks.unwrap_or(d.readahead_blocks),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Fail fast on invalid configuration with an actionable message.
    pub fn validate(&self) -> Result<()> {
        if self.mountpoint.as_os_str().is_empty() {
            return Err(Error::Other("mountpoint must not be empty".into()));
        }
        if self.coordinator.is_empty() {
            return Err(Error::Other("coordinator address must not be empty".into()));
        }
        if self.block_size == 0 {
            return Err(Error::Other("block_size must be > 0".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        WorkerConfig::default().validate().unwrap();
    }

    #[test]
    fn precedence_cli_over_env_over_file_over_default() {
        // block_size only in file; coordinator in file+env; listen in all three.
        let file = WorkerConfigPatch {
            listen: Some("file:1".into()),
            coordinator: Some("file-coord".into()),
            block_size: Some(1 << 20),
            ..Default::default()
        };
        let env = WorkerConfigPatch {
            listen: Some("env:1".into()),
            coordinator: Some("env-coord".into()),
            ..Default::default()
        };
        let cli = WorkerConfigPatch {
            listen: Some("cli:1".into()),
            ..Default::default()
        };

        let cfg = WorkerConfig::resolve(file, env, cli).unwrap();
        assert_eq!(cfg.listen, "cli:1"); // CLI wins
        assert_eq!(cfg.coordinator, "env-coord"); // env beats file
        assert_eq!(cfg.block_size, 1 << 20); // file beats default
        assert_eq!(cfg.capacity_bytes, WorkerConfig::default().capacity_bytes); // default
    }

    #[test]
    fn from_toml_parses_and_rejects_unknown() {
        let patch = WorkerConfigPatch::from_toml(
            "listen = \"0.0.0.0:9000\"\ncache_dirs = [\"/a\", \"/b\"]\n",
        )
        .unwrap();
        assert_eq!(patch.listen.as_deref(), Some("0.0.0.0:9000"));
        assert_eq!(patch.cache_dirs.unwrap().len(), 2);
        assert!(WorkerConfigPatch::from_toml("bogus_key = 1").is_err());
    }

    #[test]
    fn from_env_parses_typed_fields() {
        let map = |k: &str| match k {
            "TALON_WORKER_BLOCK_SIZE" => Some("1048576".to_string()),
            "TALON_WORKER_CACHE_DIRS" => Some("/x:/y:/z".to_string()),
            "TALON_WORKER_ADMIN_LISTEN" => Some("0.0.0.0:9001".to_string()),
            "TALON_WORKER_HEARTBEAT_INTERVAL_MS" => Some("2500".to_string()),
            _ => None,
        };
        let patch = WorkerConfigPatch::from_env_with(map).unwrap();
        assert_eq!(patch.block_size, Some(1 << 20));
        assert_eq!(patch.cache_dirs.as_ref().unwrap().len(), 3);
        assert_eq!(patch.admin_listen.as_deref(), Some("0.0.0.0:9001"));
        assert_eq!(patch.heartbeat_interval_ms, Some(2_500));
        assert!(patch.listen.is_none());

        let bad = |k: &str| (k == "TALON_WORKER_BLOCK_SIZE").then(|| "notanum".to_string());
        assert!(WorkerConfigPatch::from_env_with(bad).is_err());
    }

    #[test]
    fn invalid_config_fails_fast() {
        let cli = WorkerConfigPatch {
            capacity_bytes: Some(1),
            ..Default::default()
        };
        // capacity < block_size
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("capacity_bytes"));

        let cli = WorkerConfigPatch {
            heartbeat_interval_ms: Some(0),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("heartbeat_interval_ms"));

        let cli = WorkerConfigPatch {
            cluster_id: Some("x".repeat(MAX_STATUS_FIELD_BYTES + 1)),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("cluster_id"));
    }

    #[test]
    fn fuse_defaults_are_valid() {
        FuseConfig::default().validate().unwrap();
    }

    #[test]
    fn fuse_precedence_cli_over_env_over_file_over_default() {
        let file = FuseConfigPatch {
            mountpoint: Some(PathBuf::from("/file/mnt")),
            coordinator: Some("file-coord".into()),
            block_size: Some(1 << 20),
            ..Default::default()
        };
        let env = FuseConfigPatch {
            coordinator: Some("env-coord".into()),
            readahead_blocks: Some(8),
            ..Default::default()
        };
        let cli = FuseConfigPatch {
            mountpoint: Some(PathBuf::from("/cli/mnt")),
            ..Default::default()
        };
        let cfg = FuseConfig::resolve(file, env, cli).unwrap();
        assert_eq!(cfg.mountpoint, PathBuf::from("/cli/mnt")); // CLI wins
        assert_eq!(cfg.coordinator, "env-coord"); // env beats file
        assert_eq!(cfg.block_size, 1 << 20); // file beats default
        assert_eq!(cfg.readahead_blocks, 8); // env
        assert_eq!(cfg.placement_ttl_ms, FuseConfig::default().placement_ttl_ms);
        // default
    }

    #[test]
    fn fuse_from_toml_parses_and_rejects_unknown() {
        let patch = FuseConfigPatch::from_toml(
            "mountpoint = \"/mnt/x\"\nreadahead_blocks = 16\nplacement_ttl_ms = 250\n",
        )
        .unwrap();
        assert_eq!(
            patch.mountpoint.as_deref(),
            Some(std::path::Path::new("/mnt/x"))
        );
        assert_eq!(patch.readahead_blocks, Some(16));
        assert_eq!(patch.placement_ttl_ms, Some(250));
        assert!(FuseConfigPatch::from_toml("nope = true").is_err());
    }

    #[test]
    fn fuse_from_env_parses_typed_fields() {
        let map = |k: &str| match k {
            "TALON_FUSE_BLOCK_SIZE" => Some("1048576".to_string()),
            "TALON_FUSE_MOUNTPOINT" => Some("/mnt/talon".to_string()),
            "TALON_FUSE_READAHEAD_BLOCKS" => Some("2".to_string()),
            _ => None,
        };
        let patch = FuseConfigPatch::from_env_with(map).unwrap();
        assert_eq!(patch.block_size, Some(1 << 20));
        assert_eq!(
            patch.mountpoint.as_deref(),
            Some(std::path::Path::new("/mnt/talon"))
        );
        assert_eq!(patch.readahead_blocks, Some(2));
        assert!(patch.coordinator.is_none());

        let bad = |k: &str| (k == "TALON_FUSE_PLACEMENT_TTL_MS").then(|| "NaN".to_string());
        assert!(FuseConfigPatch::from_env_with(bad).is_err());
    }

    #[test]
    fn fuse_invalid_config_fails_fast() {
        let cli = FuseConfigPatch {
            block_size: Some(0),
            ..Default::default()
        };
        let err = FuseConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("block_size"));
    }
}
