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

/// One configurable setting, described once and reused by both the runtime
/// environment parser and the generated documentation.
///
/// This is the single source of truth for a process's `TALON_*` environment
/// variables: [`from_env`](WorkerConfigPatch::from_env) reads the names from
/// here, and the documentation generator renders the same list, so the config
/// reference cannot drift from the code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigVar {
    /// Environment variable name, e.g. `TALON_WORKER_LISTEN`.
    pub env: &'static str,
    /// Equivalent config-file key / CLI flag stem, e.g. `listen`.
    pub key: &'static str,
    /// Default value shown in docs, or `None` when there is no fixed default.
    pub default: Option<&'static str>,
    /// Whether this setting is also settable as a CLI flag (`--<key>`).
    pub cli: bool,
    /// Whether the value is a secret (never logged; env-only).
    pub secret: bool,
    /// One-line description.
    pub help: &'static str,
}

/// Fully-resolved worker configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    /// Address the worker's RPC service binds to.
    pub listen: String,
    /// Routable address advertised to the coordinator for clients to dial.
    ///
    /// Distinct from [`listen`](Self::listen) so a worker can bind a wildcard
    /// address (e.g. `0.0.0.0:7001`) for reachability while advertising a
    /// concrete routable address. Defaults to `listen` when unset.
    pub advertise_addr: String,
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
    /// Optional Azure endpoint host (`host` or `host:port`) overriding the public
    /// cloud `{account}.blob.core.windows.net`. Set to target an emulator
    /// (Azurite) or a latency proxy; enables path-style addressing. A value with
    /// an `http://` scheme selects plaintext; `https://` or a bare host keeps TLS.
    pub azure_endpoint: Option<String>,
    /// Optional synthetic backend latency knobs for the test/latency lab. All
    /// default to `None` (no injected latency); set any to wrap the backend HTTP
    /// client in a delay decorator.
    pub backend_delay_ms: Option<u64>,
    /// Upper bound of uniform per-request jitter (ms) added on top of the base
    /// delay. Requires the delay decorator (any latency knob set).
    pub backend_jitter_ms: Option<u64>,
    /// Optional bandwidth ceiling (bytes/second) modeled by the delay decorator.
    pub backend_throughput_bytes: Option<u64>,
    /// Number of io_uring rings serving the data plane, or `None` for the
    /// portable Tokio path.
    ///
    /// `Some(n)` runs the thread-per-core data plane: `n` threads, each with its
    /// own `monoio` ring pinned to a core, all binding the listen address with
    /// `SO_REUSEPORT` so the kernel distributes accepts. `Some(0)` means one
    /// ring per available core. Defaults to `None` while the Tokio path remains
    /// the shipped default (#285).
    pub data_plane_rings: Option<usize>,
}

/// Read the Azure SAS token from the environment (`TALON_WORKER_AZURE_SAS`).
///
/// Returned as an opaque string and intended for immediate use; it is
/// deliberately kept out of [`WorkerConfig`] so it is never serialized, printed
/// via `Debug`, or logged.
pub fn azure_sas_from_env() -> Option<String> {
    std::env::var(worker_env::AZURE_SAS)
        .ok()
        .filter(|s| !s.is_empty())
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7001".into(),
            advertise_addr: "127.0.0.1:7001".into(),
            admin_listen: "127.0.0.1:8001".into(),
            coordinator: "127.0.0.1:7000".into(),
            cluster_id: "default".into(),
            node_id: None,
            heartbeat_interval_ms: 5_000,
            block_size: 256 << 20,
            cache_dirs: vec![PathBuf::from("/var/cache/talon")],
            capacity_bytes: 64 << 30,
            azure_account: None,
            azure_endpoint: None,
            backend_delay_ms: None,
            backend_jitter_ms: None,
            backend_throughput_bytes: None,
            data_plane_rings: None,
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
    /// Override for [`WorkerConfig::advertise_addr`].
    pub advertise_addr: Option<String>,
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
    /// Override for [`WorkerConfig::azure_endpoint`].
    pub azure_endpoint: Option<String>,
    /// Override for [`WorkerConfig::backend_delay_ms`].
    pub backend_delay_ms: Option<u64>,
    /// Override for [`WorkerConfig::backend_jitter_ms`].
    pub backend_jitter_ms: Option<u64>,
    /// Override for [`WorkerConfig::backend_throughput_bytes`].
    pub backend_throughput_bytes: Option<u64>,
    /// Override for [`WorkerConfig::data_plane_rings`].
    pub data_plane_rings: Option<usize>,
}

impl Patch for WorkerConfigPatch {
    fn merge(self, base: Self) -> Self {
        Self {
            listen: self.listen.or(base.listen),
            advertise_addr: self.advertise_addr.or(base.advertise_addr),
            admin_listen: self.admin_listen.or(base.admin_listen),
            coordinator: self.coordinator.or(base.coordinator),
            cluster_id: self.cluster_id.or(base.cluster_id),
            node_id: self.node_id.or(base.node_id),
            heartbeat_interval_ms: self.heartbeat_interval_ms.or(base.heartbeat_interval_ms),
            block_size: self.block_size.or(base.block_size),
            cache_dirs: self.cache_dirs.or(base.cache_dirs),
            capacity_bytes: self.capacity_bytes.or(base.capacity_bytes),
            azure_account: self.azure_account.or(base.azure_account),
            azure_endpoint: self.azure_endpoint.or(base.azure_endpoint),
            backend_delay_ms: self.backend_delay_ms.or(base.backend_delay_ms),
            backend_jitter_ms: self.backend_jitter_ms.or(base.backend_jitter_ms),
            backend_throughput_bytes: self
                .backend_throughput_bytes
                .or(base.backend_throughput_bytes),
            data_plane_rings: self.data_plane_rings.or(base.data_plane_rings),
        }
    }
}

/// Single source of truth for the worker's `TALON_WORKER_*` environment
/// variables. [`WorkerConfigPatch::from_env_with`] reads names from here and the
/// documentation generator renders it, so the reference cannot drift.
pub const WORKER_ENV_SCHEMA: &[ConfigVar] = &[
    ConfigVar {
        env: "TALON_WORKER_LISTEN",
        key: "listen",
        default: Some("127.0.0.1:7001"),
        cli: true,
        secret: false,
        help: "Data-plane bind address.",
    },
    ConfigVar {
        env: "TALON_WORKER_ADVERTISE_ADDR",
        key: "advertise_addr",
        default: Some("<listen>"),
        cli: true,
        secret: false,
        help: "Routable address advertised to the coordinator.",
    },
    ConfigVar {
        env: "TALON_WORKER_ADMIN_LISTEN",
        key: "admin_listen",
        default: Some("127.0.0.1:8001"),
        cli: true,
        secret: false,
        help: "Admin HTTP bind address: metrics, health, status.",
    },
    ConfigVar {
        env: "TALON_WORKER_COORDINATOR",
        key: "coordinator",
        default: Some("127.0.0.1:7000"),
        cli: true,
        secret: false,
        help: "Coordinator control-plane address to register with.",
    },
    ConfigVar {
        env: "TALON_WORKER_CLUSTER_ID",
        key: "cluster_id",
        default: Some("default"),
        cli: true,
        secret: false,
        help: "Logical cluster advertised in status.",
    },
    ConfigVar {
        env: "TALON_WORKER_NODE_ID",
        key: "node_id",
        default: Some("<listen>"),
        cli: true,
        secret: false,
        help: "Stable worker node identity.",
    },
    ConfigVar {
        env: "TALON_WORKER_HEARTBEAT_INTERVAL_MS",
        key: "heartbeat_interval_ms",
        default: Some("5000"),
        cli: true,
        secret: false,
        help: "Heartbeat interval (ms).",
    },
    ConfigVar {
        env: "TALON_WORKER_BLOCK_SIZE",
        key: "block_size",
        default: Some("268435456"),
        cli: true,
        secret: false,
        help: "Logical block size (bytes).",
    },
    ConfigVar {
        env: "TALON_WORKER_CACHE_DIRS",
        key: "cache_dirs",
        default: Some("/var/cache/talon"),
        cli: false,
        secret: false,
        help: "Colon-separated cache directories.",
    },
    ConfigVar {
        env: "TALON_WORKER_CAPACITY_BYTES",
        key: "capacity_bytes",
        default: Some("68719476736"),
        cli: false,
        secret: false,
        help: "Worker cache capacity (bytes).",
    },
    ConfigVar {
        env: "TALON_WORKER_AZURE_ACCOUNT",
        key: "azure_account",
        default: None,
        cli: false,
        secret: false,
        help: "Azure blob storage account name (required to serve data).",
    },
    ConfigVar {
        env: "TALON_WORKER_AZURE_ENDPOINT",
        key: "azure_endpoint",
        default: None,
        cli: false,
        secret: false,
        help: "Azure endpoint host override (emulator/proxy); enables path-style addressing.",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND_DELAY_MS",
        key: "backend_delay_ms",
        default: None,
        cli: false,
        secret: false,
        help: "Synthetic base backend latency in ms (test/latency lab).",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND_JITTER_MS",
        key: "backend_jitter_ms",
        default: None,
        cli: false,
        secret: false,
        help: "Synthetic per-request latency jitter upper bound in ms (test/latency lab).",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND_THROUGHPUT_BYTES",
        key: "backend_throughput_bytes",
        default: None,
        cli: false,
        secret: false,
        help: "Synthetic backend bandwidth ceiling in bytes/sec (test/latency lab).",
    },
    ConfigVar {
        env: "TALON_WORKER_DATA_PLANE_RINGS",
        key: "data_plane_rings",
        default: None,
        cli: true,
        secret: false,
        help: "io_uring rings for the data plane; 0 = one per core, unset = Tokio path.",
    },
    ConfigVar {
        env: "TALON_WORKER_AZURE_SAS",
        key: "(env only)",
        default: None,
        cli: false,
        secret: true,
        help: "Azure SAS token; env-only, never from a config file or logged.",
    },
];

pub(crate) mod worker_env {
    pub const LISTEN: &str = "TALON_WORKER_LISTEN";
    pub const ADVERTISE_ADDR: &str = "TALON_WORKER_ADVERTISE_ADDR";
    pub const ADMIN_LISTEN: &str = "TALON_WORKER_ADMIN_LISTEN";
    pub const COORDINATOR: &str = "TALON_WORKER_COORDINATOR";
    pub const CLUSTER_ID: &str = "TALON_WORKER_CLUSTER_ID";
    pub const NODE_ID: &str = "TALON_WORKER_NODE_ID";
    pub const HEARTBEAT_INTERVAL_MS: &str = "TALON_WORKER_HEARTBEAT_INTERVAL_MS";
    pub const BLOCK_SIZE: &str = "TALON_WORKER_BLOCK_SIZE";
    pub const CACHE_DIRS: &str = "TALON_WORKER_CACHE_DIRS";
    pub const CAPACITY_BYTES: &str = "TALON_WORKER_CAPACITY_BYTES";
    pub const AZURE_ACCOUNT: &str = "TALON_WORKER_AZURE_ACCOUNT";
    pub const AZURE_ENDPOINT: &str = "TALON_WORKER_AZURE_ENDPOINT";
    pub const BACKEND_DELAY_MS: &str = "TALON_WORKER_BACKEND_DELAY_MS";
    pub const BACKEND_JITTER_MS: &str = "TALON_WORKER_BACKEND_JITTER_MS";
    pub const BACKEND_THROUGHPUT_BYTES: &str = "TALON_WORKER_BACKEND_THROUGHPUT_BYTES";
    pub const DATA_PLANE_RINGS: &str = "TALON_WORKER_DATA_PLANE_RINGS";
    pub const AZURE_SAS: &str = "TALON_WORKER_AZURE_SAS";
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
        let parse_usize = |v: String, k: &str| {
            v.parse::<usize>()
                .map_err(|_| Error::Other(format!("{k}: invalid usize: {v:?}")))
        };
        Ok(Self {
            listen: get(worker_env::LISTEN),
            advertise_addr: get(worker_env::ADVERTISE_ADDR),
            admin_listen: get(worker_env::ADMIN_LISTEN),
            coordinator: get(worker_env::COORDINATOR),
            cluster_id: get(worker_env::CLUSTER_ID),
            node_id: get(worker_env::NODE_ID),
            heartbeat_interval_ms: get(worker_env::HEARTBEAT_INTERVAL_MS)
                .map(|v| parse_u64(v, worker_env::HEARTBEAT_INTERVAL_MS))
                .transpose()?,
            block_size: get(worker_env::BLOCK_SIZE)
                .map(|v| parse_u32(v, worker_env::BLOCK_SIZE))
                .transpose()?,
            cache_dirs: get(worker_env::CACHE_DIRS)
                .map(|v| v.split(':').map(PathBuf::from).collect()),
            capacity_bytes: get(worker_env::CAPACITY_BYTES)
                .map(|v| parse_u64(v, worker_env::CAPACITY_BYTES))
                .transpose()?,
            azure_account: get(worker_env::AZURE_ACCOUNT),
            azure_endpoint: get(worker_env::AZURE_ENDPOINT),
            backend_delay_ms: get(worker_env::BACKEND_DELAY_MS)
                .map(|v| parse_u64(v, worker_env::BACKEND_DELAY_MS))
                .transpose()?,
            backend_jitter_ms: get(worker_env::BACKEND_JITTER_MS)
                .map(|v| parse_u64(v, worker_env::BACKEND_JITTER_MS))
                .transpose()?,
            data_plane_rings: get(worker_env::DATA_PLANE_RINGS)
                .map(|v| parse_usize(v, worker_env::DATA_PLANE_RINGS))
                .transpose()?,
            backend_throughput_bytes: get(worker_env::BACKEND_THROUGHPUT_BYTES)
                .map(|v| parse_u64(v, worker_env::BACKEND_THROUGHPUT_BYTES))
                .transpose()?,
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
        let listen = merged.listen.unwrap_or(d.listen);
        let cfg = WorkerConfig {
            // Advertise the routable address if set, else fall back to the bind
            // address (issue #118: never silently advertise a wildcard bind).
            advertise_addr: merged.advertise_addr.unwrap_or_else(|| listen.clone()),
            listen,
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
            azure_endpoint: merged.azure_endpoint.or(d.azure_endpoint),
            backend_delay_ms: merged.backend_delay_ms.or(d.backend_delay_ms),
            backend_jitter_ms: merged.backend_jitter_ms.or(d.backend_jitter_ms),
            backend_throughput_bytes: merged
                .backend_throughput_bytes
                .or(d.backend_throughput_bytes),
            data_plane_rings: merged.data_plane_rings.or(d.data_plane_rings),
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
        // The advertised address is handed to clients to dial, so it must be a
        // concrete routable address, never a wildcard bind (issue #118).
        if self.advertise_addr.is_empty() {
            return Err(Error::Other("advertise_addr must not be empty".into()));
        }
        // The advertised address is handed to clients to dial, so a wildcard
        // bind is unreachable. When it parses as an IP socket address, reject an
        // unspecified IP so every spelling of the wildcard is caught (`0.0.0.0`,
        // `::`, `[::]`, `[::0]`, `0:0:0:0:0:0:0:0`, ...), not just two string
        // prefixes (issue #118, hardened per #167). A non-IP form (hostname:port)
        // does not parse as SocketAddr and is left to DNS — it is never a
        // wildcard, so it is permitted as before.
        if let Ok(addr) = self.advertise_addr.parse::<std::net::SocketAddr>() {
            if addr.ip().is_unspecified() {
                return Err(Error::Other(format!(
                    "advertise_addr {:?} is a wildcard bind and is unreachable by clients; \
                     set advertise_addr (or TALON_WORKER_ADVERTISE_ADDR) to a routable address",
                    self.advertise_addr
                )));
            }
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

/// Single source of truth for the FUSE client's `TALON_FUSE_*` environment
/// variables, shared by the parser and the documentation generator.
pub const FUSE_ENV_SCHEMA: &[ConfigVar] = &[
    ConfigVar {
        env: "TALON_FUSE_MOUNTPOINT",
        key: "mountpoint",
        default: Some("/mnt/talon"),
        cli: true,
        secret: false,
        help: "Directory to mount the Talon filesystem at.",
    },
    ConfigVar {
        env: "TALON_FUSE_COORDINATOR",
        key: "coordinator",
        default: Some("127.0.0.1:7000"),
        cli: true,
        secret: false,
        help: "Coordinator address for placement and membership.",
    },
    ConfigVar {
        env: "TALON_FUSE_BLOCK_SIZE",
        key: "block_size",
        default: Some("268435456"),
        cli: true,
        secret: false,
        help: "Logical block size (bytes); must match the cluster.",
    },
    ConfigVar {
        env: "TALON_FUSE_PLACEMENT_TTL_MS",
        key: "placement_ttl_ms",
        default: Some("5000"),
        cli: false,
        secret: false,
        help: "Placement-cache entry TTL (ms).",
    },
    ConfigVar {
        env: "TALON_FUSE_READAHEAD_BLOCKS",
        key: "readahead_blocks",
        default: Some("4"),
        cli: false,
        secret: false,
        help: "Client-side readahead depth in blocks.",
    },
];

pub(crate) mod fuse_env {
    pub const MOUNTPOINT: &str = "TALON_FUSE_MOUNTPOINT";
    pub const COORDINATOR: &str = "TALON_FUSE_COORDINATOR";
    pub const BLOCK_SIZE: &str = "TALON_FUSE_BLOCK_SIZE";
    pub const PLACEMENT_TTL_MS: &str = "TALON_FUSE_PLACEMENT_TTL_MS";
    pub const READAHEAD_BLOCKS: &str = "TALON_FUSE_READAHEAD_BLOCKS";
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
            mountpoint: get(fuse_env::MOUNTPOINT).map(PathBuf::from),
            coordinator: get(fuse_env::COORDINATOR),
            block_size: get(fuse_env::BLOCK_SIZE)
                .map(|v| parse_u32(v, fuse_env::BLOCK_SIZE))
                .transpose()?,
            placement_ttl_ms: get(fuse_env::PLACEMENT_TTL_MS)
                .map(|v| parse_u64(v, fuse_env::PLACEMENT_TTL_MS))
                .transpose()?,
            readahead_blocks: get(fuse_env::READAHEAD_BLOCKS)
                .map(|v| parse_u32(v, fuse_env::READAHEAD_BLOCKS))
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
    fn env_schemas_are_wellformed() {
        // Each schema's env names are unique and match the TALON_ prefix so the
        // generated reference and the parser stay in lockstep.
        for (label, schema, prefix) in [
            ("worker", WORKER_ENV_SCHEMA, "TALON_WORKER_"),
            ("fuse", FUSE_ENV_SCHEMA, "TALON_FUSE_"),
        ] {
            let mut seen = std::collections::HashSet::new();
            for v in schema {
                assert!(
                    v.env.starts_with(prefix),
                    "{label}: {} lacks prefix {prefix}",
                    v.env
                );
                assert!(seen.insert(v.env), "{label}: duplicate env {}", v.env);
                assert!(!v.help.is_empty(), "{label}: {} has empty help", v.env);
            }
        }
        // The parser reads exactly the worker/fuse names declared in the
        // schema; spot-check the constants used by the parser are present.
        for name in [
            worker_env::LISTEN,
            worker_env::AZURE_ACCOUNT,
            worker_env::AZURE_SAS,
        ] {
            assert!(
                WORKER_ENV_SCHEMA.iter().any(|v| v.env == name),
                "worker schema missing {name}"
            );
        }
        for name in [fuse_env::MOUNTPOINT, fuse_env::READAHEAD_BLOCKS] {
            assert!(
                FUSE_ENV_SCHEMA.iter().any(|v| v.env == name),
                "fuse schema missing {name}"
            );
        }
    }

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
            "TALON_WORKER_AZURE_ENDPOINT" => Some("http://toxiproxy:10000".to_string()),
            "TALON_WORKER_BACKEND_DELAY_MS" => Some("300".to_string()),
            "TALON_WORKER_BACKEND_JITTER_MS" => Some("50".to_string()),
            "TALON_WORKER_BACKEND_THROUGHPUT_BYTES" => Some("1048576".to_string()),
            _ => None,
        };
        let patch = WorkerConfigPatch::from_env_with(map).unwrap();
        assert_eq!(patch.block_size, Some(1 << 20));
        assert_eq!(patch.cache_dirs.as_ref().unwrap().len(), 3);
        assert_eq!(patch.admin_listen.as_deref(), Some("0.0.0.0:9001"));
        assert_eq!(patch.heartbeat_interval_ms, Some(2_500));
        assert_eq!(
            patch.azure_endpoint.as_deref(),
            Some("http://toxiproxy:10000")
        );
        assert_eq!(patch.backend_delay_ms, Some(300));
        assert_eq!(patch.backend_jitter_ms, Some(50));
        assert_eq!(patch.backend_throughput_bytes, Some(1 << 20));
        assert!(patch.listen.is_none());

        let bad = |k: &str| (k == "TALON_WORKER_BLOCK_SIZE").then(|| "notanum".to_string());
        assert!(WorkerConfigPatch::from_env_with(bad).is_err());

        // A non-numeric latency knob is a hard error, not silently ignored.
        let bad_delay =
            |k: &str| (k == "TALON_WORKER_BACKEND_DELAY_MS").then(|| "soon".to_string());
        assert!(WorkerConfigPatch::from_env_with(bad_delay).is_err());
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
    fn advertise_addr_defaults_to_listen_and_rejects_wildcard() {
        // Unset: advertise defaults to the resolved listen address.
        let cli = WorkerConfigPatch {
            listen: Some("10.0.0.5:7001".into()),
            ..Default::default()
        };
        let cfg = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap();
        assert_eq!(cfg.advertise_addr, "10.0.0.5:7001");

        // A wildcard bind can be used for `listen` as long as `advertise_addr`
        // is a routable address.
        let cli = WorkerConfigPatch {
            listen: Some("0.0.0.0:7001".into()),
            advertise_addr: Some("10.0.0.5:7001".into()),
            ..Default::default()
        };
        let cfg = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap();
        assert_eq!(cfg.advertise_addr, "10.0.0.5:7001");

        // A wildcard advertise address is rejected (unreachable by clients).
        let cli = WorkerConfigPatch {
            advertise_addr: Some("0.0.0.0:7001".into()),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("advertise_addr"));

        // Defaulting advertise from a wildcard `listen` is likewise rejected, so
        // an operator binding 0.0.0.0 without setting advertise fails fast.
        let cli = WorkerConfigPatch {
            listen: Some("0.0.0.0:7001".into()),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("advertise_addr"));

        // Every spelling of the unspecified address is rejected, not just the
        // two string prefixes the old check matched (issue #167).
        for wildcard in [
            "[::]:7001",
            "[::0]:7001",
            "[0:0:0:0:0:0:0:0]:7001",
            "[0000:0000:0000:0000:0000:0000:0000:0000]:7001",
        ] {
            let cli = WorkerConfigPatch {
                advertise_addr: Some(wildcard.into()),
                ..Default::default()
            };
            let err =
                WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
            assert!(
                err.to_string().contains("advertise_addr"),
                "{wildcard} must be rejected as a wildcard"
            );
        }

        // A routable IPv6 address and a hostname:port form are both accepted.
        for ok in ["[2001:db8::1]:7001", "worker-0.svc:7001"] {
            let cli = WorkerConfigPatch {
                advertise_addr: Some(ok.into()),
                ..Default::default()
            };
            let cfg = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap();
            assert_eq!(cfg.advertise_addr, ok);
        }
    }

    #[test]
    fn advertise_addr_from_env() {
        let map = |k: &str| match k {
            "TALON_WORKER_ADVERTISE_ADDR" => Some("worker-1.svc:7001".to_string()),
            _ => None,
        };
        let patch = WorkerConfigPatch::from_env_with(map).unwrap();
        assert_eq!(patch.advertise_addr.as_deref(), Some("worker-1.svc:7001"));
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
