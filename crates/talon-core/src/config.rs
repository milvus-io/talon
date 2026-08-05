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
use crate::{
    ControlTlsConfig, ControlTlsConfigPatch, Error, NamespacePolicy, ObjectNamespace, Result,
};

/// A configuration patch: a set of optionally-present overrides.
///
/// Higher-precedence patches are merged *onto* lower-precedence values so that
/// only explicitly-set fields override what came before.
pub trait Patch {
    /// Overlay `self` onto `base`, letting `self`'s set fields win.
    fn merge(self, base: Self) -> Self;
}

fn merge_control_tls(
    higher: Option<ControlTlsConfigPatch>,
    lower: Option<ControlTlsConfigPatch>,
) -> Option<ControlTlsConfigPatch> {
    match (higher, lower) {
        (Some(higher), Some(lower)) => Some(higher.merge(lower)),
        (Some(patch), None) | (None, Some(patch)) => Some(patch),
        (None, None) => None,
    }
}

fn optional_control_tls_patch(
    ca_cert_path: Option<String>,
    cert_path: Option<String>,
    key_path: Option<String>,
    trust_domain: Option<String>,
) -> Option<ControlTlsConfigPatch> {
    let patch = ControlTlsConfigPatch {
        ca_cert_path: ca_cert_path.map(PathBuf::from),
        cert_path: cert_path.map(PathBuf::from),
        key_path: key_path.map(PathBuf::from),
        trust_domain,
    };
    (!patch.is_empty()).then_some(patch)
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
    /// Dedicated mTLS listener for coordinator-initiated privileged traffic.
    pub control_listen: Option<String>,
    /// Logical cluster advertised in node status.
    pub cluster_id: String,
    /// Optional mTLS material for the privileged coordinator-worker channel.
    pub control_tls: Option<ControlTlsConfig>,
    /// Operator-owned grants loaded from mounted static policy data.
    pub namespace_policy: Option<NamespacePolicy>,
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
    /// L1 DRAM cache capacity in bytes. Zero disables L1.
    pub l1_capacity_bytes: u64,
    /// Fixed L1 DRAM page size in bytes.
    pub l1_page_size_bytes: u64,
    /// Object-store backend selector: `azure` (default), `s3`, or `gcs`. The
    /// per-backend endpoint/credential fields below apply to the selected one.
    pub backend: Option<String>,
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
    /// Retries after the initial attempt for a transient backend failure
    /// (`408/429/5xx` or a transport error). `Some(0)` disables retrying;
    /// per-attempt timeouts still apply. `None` uses the built-in default.
    pub backend_max_retries: Option<u32>,
    /// Base of the exponential backoff between retries, in milliseconds. The
    /// actual wait is drawn uniformly from `[0, base * 2^attempt]` (full jitter)
    /// so a fleet does not retry in lockstep.
    pub backend_retry_base_ms: Option<u64>,
    /// Ceiling on any single backoff wait, in milliseconds. Also clamps a
    /// `Retry-After` hint from the origin.
    pub backend_retry_max_delay_ms: Option<u64>,
    /// Fixed part of the per-attempt deadline, in milliseconds, covering connect
    /// and time-to-first-byte.
    pub backend_timeout_floor_ms: Option<u64>,
    /// Throughput floor (bytes/second) used to extend the per-attempt deadline
    /// by transfer size, so a 256 MiB block fetch is not held to the same
    /// deadline as a HEAD. `Some(0)` makes the deadline a flat
    /// `backend_timeout_floor_ms`.
    pub backend_min_throughput_bytes: Option<u64>,
    /// S3 region (e.g. `us-east-1`). Required when `backend = s3`.
    pub s3_region: Option<String>,
    /// S3 endpoint host override (e.g. a MinIO/LocalStack host). Defaults to the
    /// AWS regional endpoint. A value with an `http://` scheme selects plaintext.
    pub s3_endpoint: Option<String>,
    /// S3 access key id (not secret; the secret key is env-only, see
    /// [`s3_secret_key_from_env`]).
    pub s3_access_key_id: Option<String>,
    /// Use S3 path-style addressing (`endpoint/bucket/key`). Required by most
    /// S3-compatible emulators (LocalStack, MinIO).
    pub s3_path_style: Option<bool>,
    /// GCS endpoint host override (e.g. a fake-gcs-server host). Defaults to
    /// `storage.googleapis.com`. A value with an `http://` scheme is plaintext.
    pub gcs_endpoint: Option<String>,
    /// Number of io_uring rings serving the data plane.
    ///
    /// Runs the thread-per-core data plane: `n` threads, each with its own
    /// `monoio` ring pinned to a core, all binding the listen address with
    /// `SO_REUSEPORT` so the kernel distributes accepts.
    ///
    /// - `0` (the default) means **one ring per available core**.
    /// - `n > 0` runs exactly `n` rings.
    ///
    /// The worker falls back to the portable Tokio data plane automatically
    /// when io_uring is unavailable (older kernels, restrictive seccomp, some
    /// container runtimes), so this default is safe everywhere. Set
    /// `data_plane_rings = 1` with `TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1` to
    /// pin the legacy path explicitly.
    pub data_plane_rings: usize,
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

/// Read the S3 secret access key from the environment
/// (`TALON_WORKER_S3_SECRET_ACCESS_KEY`). Kept out of [`WorkerConfig`] so the
/// secret is never serialized or logged.
pub fn s3_secret_key_from_env() -> Option<String> {
    std::env::var(worker_env::S3_SECRET_ACCESS_KEY)
        .ok()
        .filter(|s| !s.is_empty())
}

/// Read the S3 session token from the environment
/// (`TALON_WORKER_S3_SESSION_TOKEN`), for STS credentials. Env-only.
pub fn s3_session_token_from_env() -> Option<String> {
    std::env::var(worker_env::S3_SESSION_TOKEN)
        .ok()
        .filter(|s| !s.is_empty())
}

/// Read the GCS OAuth2 bearer token from the environment
/// (`TALON_WORKER_GCS_BEARER_TOKEN`). Env-only; never serialized or logged.
pub fn gcs_bearer_from_env() -> Option<String> {
    std::env::var(worker_env::GCS_BEARER_TOKEN)
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
            control_listen: None,
            cluster_id: "default".into(),
            control_tls: None,
            namespace_policy: None,
            node_id: None,
            heartbeat_interval_ms: 5_000,
            block_size: 256 << 20,
            cache_dirs: vec![PathBuf::from("/var/cache/talon")],
            capacity_bytes: 64 << 30,
            l1_capacity_bytes: 0,
            l1_page_size_bytes: 256 << 10,
            backend: None,
            azure_account: None,
            azure_endpoint: None,
            backend_delay_ms: None,
            backend_jitter_ms: None,
            backend_throughput_bytes: None,
            backend_max_retries: None,
            backend_retry_base_ms: None,
            backend_retry_max_delay_ms: None,
            backend_timeout_floor_ms: None,
            backend_min_throughput_bytes: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_path_style: None,
            gcs_endpoint: None,
            data_plane_rings: 0,
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
    /// Override for [`WorkerConfig::control_listen`].
    pub control_listen: Option<String>,
    /// Override for [`WorkerConfig::cluster_id`].
    pub cluster_id: Option<String>,
    /// Optional `[control_tls]` block.
    pub control_tls: Option<ControlTlsConfigPatch>,
    /// Path to a static namespace authorization policy.
    pub namespace_policy_path: Option<PathBuf>,
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
    /// Override for [`WorkerConfig::l1_capacity_bytes`].
    pub l1_capacity_bytes: Option<u64>,
    /// Override for [`WorkerConfig::l1_page_size_bytes`].
    pub l1_page_size_bytes: Option<u64>,
    /// Override for [`WorkerConfig::backend`].
    pub backend: Option<String>,
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
    /// Override for [`WorkerConfig::backend_max_retries`].
    pub backend_max_retries: Option<u32>,
    /// Override for [`WorkerConfig::backend_retry_base_ms`].
    pub backend_retry_base_ms: Option<u64>,
    /// Override for [`WorkerConfig::backend_retry_max_delay_ms`].
    pub backend_retry_max_delay_ms: Option<u64>,
    /// Override for [`WorkerConfig::backend_timeout_floor_ms`].
    pub backend_timeout_floor_ms: Option<u64>,
    /// Override for [`WorkerConfig::backend_min_throughput_bytes`].
    pub backend_min_throughput_bytes: Option<u64>,
    /// Override for [`WorkerConfig::s3_region`].
    pub s3_region: Option<String>,
    /// Override for [`WorkerConfig::s3_endpoint`].
    pub s3_endpoint: Option<String>,
    /// Override for [`WorkerConfig::s3_access_key_id`].
    pub s3_access_key_id: Option<String>,
    /// Override for [`WorkerConfig::s3_path_style`].
    pub s3_path_style: Option<bool>,
    /// Override for [`WorkerConfig::gcs_endpoint`].
    pub gcs_endpoint: Option<String>,
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
            control_listen: self.control_listen.or(base.control_listen),
            cluster_id: self.cluster_id.or(base.cluster_id),
            control_tls: merge_control_tls(self.control_tls, base.control_tls),
            namespace_policy_path: self.namespace_policy_path.or(base.namespace_policy_path),
            node_id: self.node_id.or(base.node_id),
            heartbeat_interval_ms: self.heartbeat_interval_ms.or(base.heartbeat_interval_ms),
            block_size: self.block_size.or(base.block_size),
            cache_dirs: self.cache_dirs.or(base.cache_dirs),
            capacity_bytes: self.capacity_bytes.or(base.capacity_bytes),
            l1_capacity_bytes: self.l1_capacity_bytes.or(base.l1_capacity_bytes),
            l1_page_size_bytes: self.l1_page_size_bytes.or(base.l1_page_size_bytes),
            backend: self.backend.or(base.backend),
            azure_account: self.azure_account.or(base.azure_account),
            azure_endpoint: self.azure_endpoint.or(base.azure_endpoint),
            backend_delay_ms: self.backend_delay_ms.or(base.backend_delay_ms),
            backend_jitter_ms: self.backend_jitter_ms.or(base.backend_jitter_ms),
            backend_throughput_bytes: self
                .backend_throughput_bytes
                .or(base.backend_throughput_bytes),
            backend_max_retries: self.backend_max_retries.or(base.backend_max_retries),
            backend_retry_base_ms: self.backend_retry_base_ms.or(base.backend_retry_base_ms),
            backend_retry_max_delay_ms: self
                .backend_retry_max_delay_ms
                .or(base.backend_retry_max_delay_ms),
            backend_timeout_floor_ms: self
                .backend_timeout_floor_ms
                .or(base.backend_timeout_floor_ms),
            backend_min_throughput_bytes: self
                .backend_min_throughput_bytes
                .or(base.backend_min_throughput_bytes),
            s3_region: self.s3_region.or(base.s3_region),
            s3_endpoint: self.s3_endpoint.or(base.s3_endpoint),
            s3_access_key_id: self.s3_access_key_id.or(base.s3_access_key_id),
            s3_path_style: self.s3_path_style.or(base.s3_path_style),
            gcs_endpoint: self.gcs_endpoint.or(base.gcs_endpoint),
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
        env: "TALON_WORKER_CONTROL_LISTEN",
        key: "control_listen",
        default: None,
        cli: true,
        secret: false,
        help: "Dedicated mTLS control bind address; requires [control_tls].",
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
        env: "TALON_WORKER_CONTROL_TLS_CA_CERT_PATH",
        key: "control_tls.ca_cert_path",
        default: None,
        cli: false,
        secret: false,
        help: "PEM CA bundle for the privileged coordinator-worker mTLS channel.",
    },
    ConfigVar {
        env: "TALON_WORKER_CONTROL_TLS_CERT_PATH",
        key: "control_tls.cert_path",
        default: None,
        cli: false,
        secret: false,
        help: "PEM worker certificate chain for the privileged mTLS channel.",
    },
    ConfigVar {
        env: "TALON_WORKER_CONTROL_TLS_KEY_PATH",
        key: "control_tls.key_path",
        default: None,
        cli: false,
        secret: false,
        help: "PEM worker private-key file path; key bytes never enter configuration.",
    },
    ConfigVar {
        env: "TALON_WORKER_CONTROL_TLS_TRUST_DOMAIN",
        key: "control_tls.trust_domain",
        default: None,
        cli: false,
        secret: false,
        help: "Lowercase DNS trust domain required in peer workload URI SANs.",
    },
    ConfigVar {
        env: "TALON_WORKER_NAMESPACE_POLICY_PATH",
        key: "namespace_policy_path",
        default: None,
        cli: false,
        secret: false,
        help: "Mounted static namespace authorization policy (TOML).",
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
        env: "TALON_WORKER_L1_CAPACITY_BYTES",
        key: "l1_capacity_bytes",
        default: Some("0"),
        cli: false,
        secret: false,
        help: "L1 DRAM cache capacity in bytes; 0 disables L1.",
    },
    ConfigVar {
        env: "TALON_WORKER_L1_PAGE_SIZE_BYTES",
        key: "l1_page_size_bytes",
        default: Some("262144"),
        cli: false,
        secret: false,
        help: "Fixed L1 DRAM page size in bytes.",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND",
        key: "backend",
        default: Some("azure"),
        cli: false,
        secret: false,
        help: "Object-store backend: azure (default), s3, or gcs.",
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
        env: "TALON_WORKER_BACKEND_MAX_RETRIES",
        key: "backend_max_retries",
        default: Some("3"),
        cli: false,
        secret: false,
        help: "Retries after the first attempt for a transient backend failure (0 disables).",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND_RETRY_BASE_MS",
        key: "backend_retry_base_ms",
        default: Some("100"),
        cli: false,
        secret: false,
        help: "Exponential backoff base in ms; the wait is jittered over [0, base * 2^attempt].",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND_RETRY_MAX_DELAY_MS",
        key: "backend_retry_max_delay_ms",
        default: Some("5000"),
        cli: false,
        secret: false,
        help: "Ceiling on a single backoff wait in ms; also clamps an origin Retry-After hint.",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND_TIMEOUT_FLOOR_MS",
        key: "backend_timeout_floor_ms",
        default: Some("5000"),
        cli: false,
        secret: false,
        help: "Fixed part of the per-attempt backend deadline in ms (connect + first byte).",
    },
    ConfigVar {
        env: "TALON_WORKER_BACKEND_MIN_THROUGHPUT_BYTES",
        key: "backend_min_throughput_bytes",
        default: Some("10485760 (10 MiB/s)"),
        cli: false,
        secret: false,
        help: "Throughput floor in bytes/sec used to extend the deadline by transfer size (0 = flat).",
    },
    ConfigVar {
        env: "TALON_WORKER_DATA_PLANE_RINGS",
        key: "data_plane_rings",
        default: Some("0 (one ring per core)"),
        cli: true,
        secret: false,
        help: "io_uring rings for the data plane; 0 = one per core. Falls back to Tokio if io_uring is unavailable.",
    },
    ConfigVar {
        env: "TALON_WORKER_S3_REGION",
        key: "s3_region",
        default: None,
        cli: false,
        secret: false,
        help: "S3 region (required when backend=s3).",
    },
    ConfigVar {
        env: "TALON_WORKER_S3_ENDPOINT",
        key: "s3_endpoint",
        default: None,
        cli: false,
        secret: false,
        help: "S3 endpoint host override (MinIO/LocalStack); http:// selects plaintext.",
    },
    ConfigVar {
        env: "TALON_WORKER_S3_ACCESS_KEY_ID",
        key: "s3_access_key_id",
        default: None,
        cli: false,
        secret: false,
        help: "S3 access key id (the secret key is env-only).",
    },
    ConfigVar {
        env: "TALON_WORKER_S3_PATH_STYLE",
        key: "s3_path_style",
        default: None,
        cli: false,
        secret: false,
        help: "S3 path-style addressing (true for most S3-compatible emulators).",
    },
    ConfigVar {
        env: "TALON_WORKER_GCS_ENDPOINT",
        key: "gcs_endpoint",
        default: None,
        cli: false,
        secret: false,
        help: "GCS endpoint host override (fake-gcs-server); http:// selects plaintext.",
    },
    ConfigVar {
        env: "TALON_WORKER_S3_SECRET_ACCESS_KEY",
        key: "(env only)",
        default: None,
        cli: false,
        secret: true,
        help: "S3 secret access key; env-only, never from a config file or logged.",
    },
    ConfigVar {
        env: "TALON_WORKER_S3_SESSION_TOKEN",
        key: "(env only)",
        default: None,
        cli: false,
        secret: true,
        help: "S3 STS session token; env-only, never from a config file or logged.",
    },
    ConfigVar {
        env: "TALON_WORKER_GCS_BEARER_TOKEN",
        key: "(env only)",
        default: None,
        cli: false,
        secret: true,
        help: "GCS OAuth2 bearer token; env-only, never from a config file or logged.",
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
    pub const CONTROL_LISTEN: &str = "TALON_WORKER_CONTROL_LISTEN";
    pub const CLUSTER_ID: &str = "TALON_WORKER_CLUSTER_ID";
    pub const NODE_ID: &str = "TALON_WORKER_NODE_ID";
    pub const CONTROL_TLS_CA_CERT_PATH: &str = "TALON_WORKER_CONTROL_TLS_CA_CERT_PATH";
    pub const CONTROL_TLS_CERT_PATH: &str = "TALON_WORKER_CONTROL_TLS_CERT_PATH";
    pub const CONTROL_TLS_KEY_PATH: &str = "TALON_WORKER_CONTROL_TLS_KEY_PATH";
    pub const CONTROL_TLS_TRUST_DOMAIN: &str = "TALON_WORKER_CONTROL_TLS_TRUST_DOMAIN";
    pub const NAMESPACE_POLICY_PATH: &str = "TALON_WORKER_NAMESPACE_POLICY_PATH";
    pub const HEARTBEAT_INTERVAL_MS: &str = "TALON_WORKER_HEARTBEAT_INTERVAL_MS";
    pub const BLOCK_SIZE: &str = "TALON_WORKER_BLOCK_SIZE";
    pub const CACHE_DIRS: &str = "TALON_WORKER_CACHE_DIRS";
    pub const CAPACITY_BYTES: &str = "TALON_WORKER_CAPACITY_BYTES";
    pub const L1_CAPACITY_BYTES: &str = "TALON_WORKER_L1_CAPACITY_BYTES";
    pub const L1_PAGE_SIZE_BYTES: &str = "TALON_WORKER_L1_PAGE_SIZE_BYTES";
    pub const BACKEND: &str = "TALON_WORKER_BACKEND";
    pub const AZURE_ACCOUNT: &str = "TALON_WORKER_AZURE_ACCOUNT";
    pub const AZURE_ENDPOINT: &str = "TALON_WORKER_AZURE_ENDPOINT";
    pub const BACKEND_DELAY_MS: &str = "TALON_WORKER_BACKEND_DELAY_MS";
    pub const BACKEND_JITTER_MS: &str = "TALON_WORKER_BACKEND_JITTER_MS";
    pub const BACKEND_THROUGHPUT_BYTES: &str = "TALON_WORKER_BACKEND_THROUGHPUT_BYTES";
    pub const BACKEND_MAX_RETRIES: &str = "TALON_WORKER_BACKEND_MAX_RETRIES";
    pub const BACKEND_RETRY_BASE_MS: &str = "TALON_WORKER_BACKEND_RETRY_BASE_MS";
    pub const BACKEND_RETRY_MAX_DELAY_MS: &str = "TALON_WORKER_BACKEND_RETRY_MAX_DELAY_MS";
    pub const BACKEND_TIMEOUT_FLOOR_MS: &str = "TALON_WORKER_BACKEND_TIMEOUT_FLOOR_MS";
    pub const BACKEND_MIN_THROUGHPUT_BYTES: &str = "TALON_WORKER_BACKEND_MIN_THROUGHPUT_BYTES";
    pub const DATA_PLANE_RINGS: &str = "TALON_WORKER_DATA_PLANE_RINGS";
    pub const S3_REGION: &str = "TALON_WORKER_S3_REGION";
    pub const S3_ENDPOINT: &str = "TALON_WORKER_S3_ENDPOINT";
    pub const S3_ACCESS_KEY_ID: &str = "TALON_WORKER_S3_ACCESS_KEY_ID";
    pub const S3_PATH_STYLE: &str = "TALON_WORKER_S3_PATH_STYLE";
    pub const S3_SECRET_ACCESS_KEY: &str = "TALON_WORKER_S3_SECRET_ACCESS_KEY";
    pub const S3_SESSION_TOKEN: &str = "TALON_WORKER_S3_SESSION_TOKEN";
    pub const GCS_ENDPOINT: &str = "TALON_WORKER_GCS_ENDPOINT";
    pub const GCS_BEARER_TOKEN: &str = "TALON_WORKER_GCS_BEARER_TOKEN";
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
        let parse_bool = |v: String, k: &str| match v.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err(Error::Other(format!("{k}: invalid bool: {v:?}"))),
        };
        Ok(Self {
            listen: get(worker_env::LISTEN),
            advertise_addr: get(worker_env::ADVERTISE_ADDR),
            admin_listen: get(worker_env::ADMIN_LISTEN),
            coordinator: get(worker_env::COORDINATOR),
            control_listen: get(worker_env::CONTROL_LISTEN),
            cluster_id: get(worker_env::CLUSTER_ID),
            node_id: get(worker_env::NODE_ID),
            control_tls: optional_control_tls_patch(
                get(worker_env::CONTROL_TLS_CA_CERT_PATH),
                get(worker_env::CONTROL_TLS_CERT_PATH),
                get(worker_env::CONTROL_TLS_KEY_PATH),
                get(worker_env::CONTROL_TLS_TRUST_DOMAIN),
            ),
            namespace_policy_path: get(worker_env::NAMESPACE_POLICY_PATH).map(PathBuf::from),
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
            l1_capacity_bytes: get(worker_env::L1_CAPACITY_BYTES)
                .map(|v| parse_u64(v, worker_env::L1_CAPACITY_BYTES))
                .transpose()?,
            l1_page_size_bytes: get(worker_env::L1_PAGE_SIZE_BYTES)
                .map(|v| parse_u64(v, worker_env::L1_PAGE_SIZE_BYTES))
                .transpose()?,
            azure_account: get(worker_env::AZURE_ACCOUNT),
            azure_endpoint: get(worker_env::AZURE_ENDPOINT),
            backend: get(worker_env::BACKEND),
            s3_region: get(worker_env::S3_REGION),
            s3_endpoint: get(worker_env::S3_ENDPOINT),
            s3_access_key_id: get(worker_env::S3_ACCESS_KEY_ID),
            s3_path_style: get(worker_env::S3_PATH_STYLE)
                .map(|v| parse_bool(v, worker_env::S3_PATH_STYLE))
                .transpose()?,
            gcs_endpoint: get(worker_env::GCS_ENDPOINT),
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
            backend_max_retries: get(worker_env::BACKEND_MAX_RETRIES)
                .map(|v| parse_u32(v, worker_env::BACKEND_MAX_RETRIES))
                .transpose()?,
            backend_retry_base_ms: get(worker_env::BACKEND_RETRY_BASE_MS)
                .map(|v| parse_u64(v, worker_env::BACKEND_RETRY_BASE_MS))
                .transpose()?,
            backend_retry_max_delay_ms: get(worker_env::BACKEND_RETRY_MAX_DELAY_MS)
                .map(|v| parse_u64(v, worker_env::BACKEND_RETRY_MAX_DELAY_MS))
                .transpose()?,
            backend_timeout_floor_ms: get(worker_env::BACKEND_TIMEOUT_FLOOR_MS)
                .map(|v| parse_u64(v, worker_env::BACKEND_TIMEOUT_FLOOR_MS))
                .transpose()?,
            backend_min_throughput_bytes: get(worker_env::BACKEND_MIN_THROUGHPUT_BYTES)
                .map(|v| parse_u64(v, worker_env::BACKEND_MIN_THROUGHPUT_BYTES))
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
        let advertise_addr = normalize_worker_advertise_addr(
            merged.advertise_addr.unwrap_or_else(|| listen.clone()),
            &listen,
        );
        let control_tls = merged.control_tls.unwrap_or_default().resolve()?;
        let namespace_policy = merged
            .namespace_policy_path
            .as_deref()
            .map(NamespacePolicy::from_file)
            .transpose()?;
        let cfg = WorkerConfig {
            // Advertise the routable address if set, else fall back to the bind
            // address (issue #118: never silently advertise a wildcard bind).
            advertise_addr,
            listen,
            admin_listen: merged.admin_listen.unwrap_or(d.admin_listen),
            coordinator: merged.coordinator.unwrap_or(d.coordinator),
            control_listen: merged.control_listen.or(d.control_listen),
            cluster_id: merged.cluster_id.unwrap_or(d.cluster_id),
            control_tls,
            namespace_policy,
            node_id: merged.node_id.or(d.node_id),
            heartbeat_interval_ms: merged
                .heartbeat_interval_ms
                .unwrap_or(d.heartbeat_interval_ms),
            block_size: merged.block_size.unwrap_or(d.block_size),
            cache_dirs: merged.cache_dirs.unwrap_or(d.cache_dirs),
            capacity_bytes: merged.capacity_bytes.unwrap_or(d.capacity_bytes),
            l1_capacity_bytes: merged.l1_capacity_bytes.unwrap_or(d.l1_capacity_bytes),
            l1_page_size_bytes: merged.l1_page_size_bytes.unwrap_or(d.l1_page_size_bytes),
            azure_account: merged.azure_account.or(d.azure_account),
            azure_endpoint: merged.azure_endpoint.or(d.azure_endpoint),
            backend: merged.backend.or(d.backend),
            s3_region: merged.s3_region.or(d.s3_region),
            s3_endpoint: merged.s3_endpoint.or(d.s3_endpoint),
            s3_access_key_id: merged.s3_access_key_id.or(d.s3_access_key_id),
            s3_path_style: merged.s3_path_style.or(d.s3_path_style),
            gcs_endpoint: merged.gcs_endpoint.or(d.gcs_endpoint),
            backend_delay_ms: merged.backend_delay_ms.or(d.backend_delay_ms),
            backend_jitter_ms: merged.backend_jitter_ms.or(d.backend_jitter_ms),
            backend_throughput_bytes: merged
                .backend_throughput_bytes
                .or(d.backend_throughput_bytes),
            backend_max_retries: merged.backend_max_retries.or(d.backend_max_retries),
            backend_retry_base_ms: merged.backend_retry_base_ms.or(d.backend_retry_base_ms),
            backend_retry_max_delay_ms: merged
                .backend_retry_max_delay_ms
                .or(d.backend_retry_max_delay_ms),
            backend_timeout_floor_ms: merged
                .backend_timeout_floor_ms
                .or(d.backend_timeout_floor_ms),
            backend_min_throughput_bytes: merged
                .backend_min_throughput_bytes
                .or(d.backend_min_throughput_bytes),
            data_plane_rings: merged.data_plane_rings.unwrap_or(d.data_plane_rings),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Check this worker's operator-owned grant for a privileged namespace.
    ///
    /// No loaded policy is an explicit deny-all state.
    pub fn authorizes_namespace(&self, target: &ObjectNamespace) -> bool {
        let node_id = self.node_id.as_deref().unwrap_or(&self.listen);
        self.namespace_policy
            .as_ref()
            .is_some_and(|policy| policy.authorizes(node_id, target))
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
        if self.control_listen.is_some() != self.control_tls.is_some() {
            return Err(Error::Other(
                "control_listen and [control_tls] must be configured together".into(),
            ));
        }
        if self.control_listen.as_ref().is_some_and(String::is_empty) {
            return Err(Error::Other(
                "control_listen address must not be empty when set".into(),
            ));
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
        if let Some(control_tls) = &self.control_tls {
            control_tls.validate()?;
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
        if self.l1_capacity_bytes > 0 {
            if self.l1_page_size_bytes == 0 {
                return Err(Error::Other(
                    "l1_page_size_bytes must be > 0 when L1 is enabled".into(),
                ));
            }
            if self.l1_page_size_bytes > self.l1_capacity_bytes {
                return Err(Error::Other(format!(
                    "l1_page_size_bytes ({}) must be <= l1_capacity_bytes ({})",
                    self.l1_page_size_bytes, self.l1_capacity_bytes
                )));
            }
            if self.l1_page_size_bytes > self.block_size as u64 {
                return Err(Error::Other(format!(
                    "l1_page_size_bytes ({}) must be <= block_size ({})",
                    self.l1_page_size_bytes, self.block_size
                )));
            }
            if u64::from(self.block_size) % self.l1_page_size_bytes != 0 {
                return Err(Error::Other(format!(
                    "block_size ({}) must be divisible by l1_page_size_bytes ({})",
                    self.block_size, self.l1_page_size_bytes
                )));
            }
        }
        Ok(())
    }
}

/// The Kubernetes Downward API can expose a Pod IP but cannot append a port.
/// Accept that common shape and inherit the configured data-plane listen port.
fn normalize_worker_advertise_addr(advertise_addr: String, listen: &str) -> String {
    let Ok(ip) = advertise_addr.parse::<std::net::IpAddr>() else {
        return advertise_addr;
    };
    let Ok(listen) = listen.parse::<std::net::SocketAddr>() else {
        return advertise_addr;
    };
    std::net::SocketAddr::new(ip, listen.port()).to_string()
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
    /// Mount-relative backend namespace to enumerate at startup.
    ///
    /// Must name at least a backend and bucket/container, for example
    /// `s3/my-bucket` or `az/my-container/datasets`.
    pub namespace_prefix: String,
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
            namespace_prefix: String::new(),
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
    /// Override for [`FuseConfig::namespace_prefix`].
    pub namespace_prefix: Option<String>,
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
            namespace_prefix: self.namespace_prefix.or(base.namespace_prefix),
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
        env: "TALON_FUSE_NAMESPACE_PREFIX",
        key: "namespace_prefix",
        default: None,
        cli: true,
        secret: false,
        help: "Backend namespace to enumerate (for example, `az/container`).",
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
    pub const NAMESPACE_PREFIX: &str = "TALON_FUSE_NAMESPACE_PREFIX";
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
    /// `TALON_FUSE_NAMESPACE_PREFIX`, `TALON_FUSE_BLOCK_SIZE`,
    /// `TALON_FUSE_PLACEMENT_TTL_MS`, `TALON_FUSE_READAHEAD_BLOCKS`.
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
            namespace_prefix: get(fuse_env::NAMESPACE_PREFIX),
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
            namespace_prefix: merged.namespace_prefix.unwrap_or(d.namespace_prefix),
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
        // Accept the historical absolute-looking spelling (`/s3/bucket`), but
        // otherwise require a canonical namespace path. Empty, `.` and `..`
        // components are ambiguous once the prefix is mapped into the FUSE
        // tree and can cause the mounted path to address a different object.
        // One trailing slash is meaningful after a non-empty key prefix (for
        // example, `s3/bucket/dir/` must not also match `dir2`) and is retained.
        let trimmed = self
            .namespace_prefix
            .strip_prefix('/')
            .unwrap_or(&self.namespace_prefix);
        if trimmed.starts_with('/') {
            return Err(Error::Other(format!(
                "namespace_prefix must not contain multiple leading slashes: {:?}",
                self.namespace_prefix
            )));
        }
        let parts: Vec<&str> = trimmed.split('/').collect();
        let backend = parts.first().copied().unwrap_or_default();
        if !matches!(
            backend.parse::<crate::Backend>(),
            Ok(parsed) if parsed.prefix() == backend
        ) {
            return Err(Error::Other(format!(
                "namespace_prefix must start with a supported backend (s3, gcs, or az): {:?}",
                self.namespace_prefix
            )));
        }
        let bucket = parts.get(1).copied().unwrap_or_default();
        if matches!(bucket, "" | "." | "..") {
            return Err(Error::Other(format!(
                "namespace_prefix must name a bucket/container (for example, az/container): {:?}",
                self.namespace_prefix
            )));
        }
        const MAX_FUSE_COMPONENT_BYTES: usize = 255;
        let invalid_component = parts.iter().enumerate().any(|(index, component)| {
            let is_key_trailing_slash =
                index + 1 == parts.len() && index >= 3 && component.is_empty();
            matches!(*component, "." | "..")
                || (component.is_empty() && !is_key_trailing_slash)
                || component.as_bytes().contains(&0)
                || component.len() > MAX_FUSE_COMPONENT_BYTES
        });
        if invalid_component {
            return Err(Error::Other(format!(
                "namespace_prefix contains an empty, `.` or `..` component, a NUL byte, or a component longer than {MAX_FUSE_COMPONENT_BYTES} bytes: {:?}",
                self.namespace_prefix
            )));
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
        for name in [
            fuse_env::MOUNTPOINT,
            fuse_env::NAMESPACE_PREFIX,
            fuse_env::READAHEAD_BLOCKS,
        ] {
            assert!(
                FUSE_ENV_SCHEMA.iter().any(|v| v.env == name),
                "fuse schema missing {name}"
            );
        }
    }

    #[test]
    fn worker_control_listener_and_tls_are_all_or_none() {
        let listener = WorkerConfigPatch {
            control_listen: Some("127.0.0.1:7002".into()),
            ..Default::default()
        };
        assert!(WorkerConfig::resolve(Default::default(), Default::default(), listener).is_err());

        let secure = WorkerConfigPatch {
            control_listen: Some("127.0.0.1:7002".into()),
            control_tls: Some(ControlTlsConfigPatch {
                ca_cert_path: Some("/tls/ca.pem".into()),
                cert_path: Some("/tls/worker.pem".into()),
                key_path: Some("/tls/worker-key.pem".into()),
                trust_domain: Some("cluster.example".into()),
            }),
            ..Default::default()
        };
        let config = WorkerConfig::resolve(Default::default(), Default::default(), secure).unwrap();
        assert_eq!(config.control_listen.as_deref(), Some("127.0.0.1:7002"));
        assert!(config.control_tls.is_some());
    }

    #[test]
    fn worker_namespace_authorization_uses_its_stable_node_id() {
        let mut config = WorkerConfig::default();
        let target = "s3/data/models/v1".parse().unwrap();
        assert!(!config.authorizes_namespace(&target));

        config.node_id = Some("worker-a".into());
        config.namespace_policy = Some(
            NamespacePolicy::from_toml(
                "version = 1\n\
                 [[workers]]\n\
                 node_id = \"worker-a\"\n\
                 grants = [\"s3/data/models\"]\n",
            )
            .unwrap(),
        );
        assert!(config.authorizes_namespace(&target));
        assert!(!config.authorizes_namespace(&"s3/data/private".parse().unwrap()));
    }

    #[test]
    fn worker_namespace_policy_path_parses_from_file_and_environment_layers() {
        let file = WorkerConfigPatch::from_toml("namespace_policy_path = \"/policy/file.toml\"\n")
            .unwrap();
        let env = WorkerConfigPatch::from_env_with(|key| {
            (key == "TALON_WORKER_NAMESPACE_POLICY_PATH").then(|| "/policy/env.toml".to_string())
        })
        .unwrap();
        let merged = env.merge(file);
        assert_eq!(
            merged.namespace_policy_path.as_deref(),
            Some(std::path::Path::new("/policy/env.toml"))
        );
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
    fn l1_config_respects_layer_precedence() {
        let file = WorkerConfigPatch {
            l1_capacity_bytes: Some(16 << 20),
            l1_page_size_bytes: Some(1 << 20),
            ..Default::default()
        };
        let env = WorkerConfigPatch {
            l1_capacity_bytes: Some(32 << 20),
            l1_page_size_bytes: Some(2 << 20),
            ..Default::default()
        };
        let cli = WorkerConfigPatch {
            l1_capacity_bytes: Some(64 << 20),
            ..Default::default()
        };

        let cfg = WorkerConfig::resolve(file, env, cli).unwrap();
        assert_eq!(cfg.l1_capacity_bytes, 64 << 20);
        assert_eq!(cfg.l1_page_size_bytes, 2 << 20);
    }

    #[test]
    fn from_toml_parses_and_rejects_unknown() {
        let patch = WorkerConfigPatch::from_toml(
            "listen = \"0.0.0.0:9000\"\ncache_dirs = [\"/a\", \"/b\"]\n\
             l1_capacity_bytes = 67108864\nl1_page_size_bytes = 1048576\n",
        )
        .unwrap();
        assert_eq!(patch.listen.as_deref(), Some("0.0.0.0:9000"));
        assert_eq!(patch.cache_dirs.unwrap().len(), 2);
        assert_eq!(patch.l1_capacity_bytes, Some(64 << 20));
        assert_eq!(patch.l1_page_size_bytes, Some(1 << 20));
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
            "TALON_WORKER_L1_CAPACITY_BYTES" => Some("67108864".to_string()),
            "TALON_WORKER_L1_PAGE_SIZE_BYTES" => Some("1048576".to_string()),
            "TALON_WORKER_BACKEND" => Some("s3".to_string()),
            "TALON_WORKER_S3_REGION" => Some("us-east-1".to_string()),
            "TALON_WORKER_S3_PATH_STYLE" => Some("true".to_string()),
            "TALON_WORKER_GCS_ENDPOINT" => Some("http://fake-gcs:4443".to_string()),
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
        assert_eq!(patch.l1_capacity_bytes, Some(64 << 20));
        assert_eq!(patch.l1_page_size_bytes, Some(1 << 20));
        assert_eq!(patch.backend.as_deref(), Some("s3"));
        assert_eq!(patch.s3_region.as_deref(), Some("us-east-1"));
        assert_eq!(patch.s3_path_style, Some(true));
        assert_eq!(patch.gcs_endpoint.as_deref(), Some("http://fake-gcs:4443"));
        assert!(patch.listen.is_none());

        let bad = |k: &str| (k == "TALON_WORKER_BLOCK_SIZE").then(|| "notanum".to_string());
        assert!(WorkerConfigPatch::from_env_with(bad).is_err());

        // A non-numeric latency knob is a hard error, not silently ignored.
        let bad_delay =
            |k: &str| (k == "TALON_WORKER_BACKEND_DELAY_MS").then(|| "soon".to_string());
        assert!(WorkerConfigPatch::from_env_with(bad_delay).is_err());

        // An invalid bool for s3_path_style is a hard error too.
        let bad_bool = |k: &str| (k == "TALON_WORKER_S3_PATH_STYLE").then(|| "maybe".to_string());
        assert!(WorkerConfigPatch::from_env_with(bad_bool).is_err());
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

        let cli = WorkerConfigPatch {
            l1_capacity_bytes: Some(1024),
            l1_page_size_bytes: Some(2048),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("l1_page_size_bytes"));

        let cli = WorkerConfigPatch {
            l1_capacity_bytes: Some(1024),
            l1_page_size_bytes: Some(0),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("must be > 0"));

        let cli = WorkerConfigPatch {
            block_size: Some(1024),
            capacity_bytes: Some(1024),
            l1_capacity_bytes: Some(4096),
            l1_page_size_bytes: Some(2048),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("must be <= block_size"));

        let cli = WorkerConfigPatch {
            block_size: Some(1024),
            capacity_bytes: Some(1024),
            l1_capacity_bytes: Some(4096),
            l1_page_size_bytes: Some(300),
            ..Default::default()
        };
        let err = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("must be divisible"));
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

        // Kubernetes injects a bare Pod IP through the Downward API. It inherits
        // the data-plane port so the registered address remains dialable.
        let cli = WorkerConfigPatch {
            listen: Some("0.0.0.0:7101".into()),
            advertise_addr: Some("10.244.1.7".into()),
            ..Default::default()
        };
        let cfg = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap();
        assert_eq!(cfg.advertise_addr, "10.244.1.7:7101");

        let cli = WorkerConfigPatch {
            listen: Some("[::]:7201".into()),
            advertise_addr: Some("fd00::7".into()),
            ..Default::default()
        };
        let cfg = WorkerConfig::resolve(Default::default(), Default::default(), cli).unwrap();
        assert_eq!(cfg.advertise_addr, "[fd00::7]:7201");

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
    fn worker_control_tls_composes_file_and_environment_layers() {
        let file = WorkerConfigPatch::from_toml(
            "control_listen = \"127.0.0.1:7002\"\n\
             [control_tls]\n\
             ca_cert_path = \"/tls/ca.pem\"\n\
             cert_path = \"/tls/file-cert.pem\"\n\
             key_path = \"/tls/key.pem\"\n\
             trust_domain = \"prod.example.com\"\n",
        )
        .unwrap();
        let env = WorkerConfigPatch::from_env_with(|key| {
            (key == "TALON_WORKER_CONTROL_TLS_CERT_PATH").then(|| "/tls/env-cert.pem".to_string())
        })
        .unwrap();
        let config = WorkerConfig::resolve(file, env, WorkerConfigPatch::default()).unwrap();
        let tls = config.control_tls.expect("control TLS configured");
        assert_eq!(tls.ca_cert_path, PathBuf::from("/tls/ca.pem"));
        assert_eq!(tls.cert_path, PathBuf::from("/tls/env-cert.pem"));
        assert_eq!(tls.key_path, PathBuf::from("/tls/key.pem"));
        assert_eq!(tls.trust_domain, "prod.example.com");

        let partial = WorkerConfigPatch::from_env_with(|key| {
            (key == "TALON_WORKER_CONTROL_TLS_KEY_PATH").then(|| "/tls/key.pem".to_string())
        })
        .unwrap();
        let error = WorkerConfig::resolve(
            WorkerConfigPatch::default(),
            partial,
            WorkerConfigPatch::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("ca_cert_path"));
    }

    #[test]
    fn fuse_namespace_prefix_is_required() {
        let err = FuseConfig::resolve(Default::default(), Default::default(), Default::default())
            .unwrap_err();
        assert!(err.to_string().contains("namespace_prefix"));
    }

    #[test]
    fn fuse_precedence_cli_over_env_over_file_over_default() {
        let file = FuseConfigPatch {
            mountpoint: Some(PathBuf::from("/file/mnt")),
            coordinator: Some("file-coord".into()),
            namespace_prefix: Some("s3/file-bucket".into()),
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
            namespace_prefix: Some("az/cli-container/datasets".into()),
            ..Default::default()
        };
        let cfg = FuseConfig::resolve(file, env, cli).unwrap();
        assert_eq!(cfg.mountpoint, PathBuf::from("/cli/mnt")); // CLI wins
        assert_eq!(cfg.coordinator, "env-coord"); // env beats file
        assert_eq!(cfg.namespace_prefix, "az/cli-container/datasets"); // CLI wins
        assert_eq!(cfg.block_size, 1 << 20); // file beats default
        assert_eq!(cfg.readahead_blocks, 8); // env
        assert_eq!(cfg.placement_ttl_ms, FuseConfig::default().placement_ttl_ms);
        // default
    }

    #[test]
    fn fuse_from_toml_parses_and_rejects_unknown() {
        let patch = FuseConfigPatch::from_toml(
            "mountpoint = \"/mnt/x\"\nnamespace_prefix = \"gcs/models/checkpoints\"\nreadahead_blocks = 16\nplacement_ttl_ms = 250\n",
        )
        .unwrap();
        assert_eq!(
            patch.mountpoint.as_deref(),
            Some(std::path::Path::new("/mnt/x"))
        );
        assert_eq!(patch.readahead_blocks, Some(16));
        assert_eq!(patch.placement_ttl_ms, Some(250));
        assert_eq!(
            patch.namespace_prefix.as_deref(),
            Some("gcs/models/checkpoints")
        );
        assert!(FuseConfigPatch::from_toml("nope = true").is_err());
    }

    #[test]
    fn fuse_from_env_parses_typed_fields() {
        let map = |k: &str| match k {
            "TALON_FUSE_BLOCK_SIZE" => Some("1048576".to_string()),
            "TALON_FUSE_MOUNTPOINT" => Some("/mnt/talon".to_string()),
            "TALON_FUSE_NAMESPACE_PREFIX" => Some("az/container".to_string()),
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
        assert_eq!(patch.namespace_prefix.as_deref(), Some("az/container"));
        assert!(patch.coordinator.is_none());

        let bad = |k: &str| (k == "TALON_FUSE_PLACEMENT_TTL_MS").then(|| "NaN".to_string());
        assert!(FuseConfigPatch::from_env_with(bad).is_err());
    }

    #[test]
    fn fuse_invalid_config_fails_fast() {
        let cli = FuseConfigPatch {
            namespace_prefix: Some("az/container".into()),
            block_size: Some(0),
            ..Default::default()
        };
        let err = FuseConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("block_size"));
    }

    #[test]
    fn fuse_namespace_prefix_requires_backend_and_bucket() {
        for invalid in [
            "",
            "unknown/bucket",
            "azure/container",
            "s3",
            "gcs/",
            "//az/container",
            "s3/./key",
            "s3/../key",
            "s3/bucket/",
            "s3/bucket//dir",
            "s3/bucket/./dir",
            "s3/bucket/../dir",
            "s3/bucket/dir//",
            "s3/buck\0et/dir",
        ] {
            let cli = FuseConfigPatch {
                namespace_prefix: Some(invalid.into()),
                ..Default::default()
            };
            let err = FuseConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
            assert!(
                err.to_string().contains("namespace_prefix"),
                "unexpected error for {invalid:?}: {err}"
            );
        }

        for valid in [
            "s3/bucket",
            "gcs/bucket/dir",
            "gcs/bucket/dir/",
            "az/container",
            "/az/container/prefix",
            "/az/container/prefix/",
        ] {
            let cli = FuseConfigPatch {
                namespace_prefix: Some(valid.into()),
                ..Default::default()
            };
            let cfg = FuseConfig::resolve(Default::default(), Default::default(), cli).unwrap();
            assert_eq!(cfg.namespace_prefix, valid);
        }

        let too_long = format!("s3/bucket/{}", "x".repeat(256));
        let cli = FuseConfigPatch {
            namespace_prefix: Some(too_long),
            ..Default::default()
        };
        let err = FuseConfig::resolve(Default::default(), Default::default(), cli).unwrap_err();
        assert!(err.to_string().contains("255"), "{err}");
    }
}
