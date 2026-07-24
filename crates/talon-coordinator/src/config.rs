//! Layered coordinator configuration.

use std::path::Path;

use serde::Deserialize;
use talon_core::{ConfigVar, Patch, MAX_STATUS_FIELD_BYTES};

#[cfg(feature = "kubernetes")]
use crate::KubernetesConfig;
use crate::{ClusterStateConfig, StateBackend};
#[cfg(feature = "etcd")]
use crate::{EtcdConfig, EtcdTlsConfig};

/// Fully resolved coordinator configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorConfig {
    /// Control-plane bind address.
    pub listen: String,
    /// Administration HTTP bind address.
    pub admin_listen: String,
    /// Administration address advertised in node status.
    pub admin_advertise: String,
    /// Logical cluster identity.
    pub cluster_id: String,
    /// Stable coordinator node identity.
    pub node_id: String,
    /// Shared-state and lease settings.
    pub state: ClusterStateConfig,
    /// etcd backend connection settings, required when `state.backend` is etcd.
    #[cfg(feature = "etcd")]
    pub etcd: Option<EtcdConfig>,
    /// Kubernetes backend settings, required when `state.backend` is kubernetes.
    #[cfg(feature = "kubernetes")]
    pub kubernetes: Option<KubernetesConfig>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7000".into(),
            admin_listen: "127.0.0.1:8000".into(),
            admin_advertise: "127.0.0.1:8000".into(),
            cluster_id: "default".into(),
            node_id: "127.0.0.1:7000".into(),
            state: ClusterStateConfig::default(),
            #[cfg(feature = "etcd")]
            etcd: None,
            #[cfg(feature = "kubernetes")]
            kubernetes: None,
        }
    }
}

/// Optional coordinator configuration overrides.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorConfigPatch {
    /// Override for [`CoordinatorConfig::listen`].
    pub listen: Option<String>,
    /// Override for [`CoordinatorConfig::admin_listen`].
    pub admin_listen: Option<String>,
    /// Override for [`CoordinatorConfig::admin_advertise`].
    pub admin_advertise: Option<String>,
    /// Override for [`CoordinatorConfig::cluster_id`].
    pub cluster_id: Option<String>,
    /// Override for [`CoordinatorConfig::node_id`].
    pub node_id: Option<String>,
    /// Shared-state backend selector.
    pub state_backend: Option<StateBackend>,
    /// Whether active-active coordinator mode is requested.
    pub ha_enabled: Option<bool>,
    /// Expected coordinator replica count.
    pub coordinator_replicas: Option<u16>,
    /// Node heartbeat interval.
    pub heartbeat_interval_ms: Option<u64>,
    /// Node unhealthy threshold.
    pub unhealthy_after_ms: Option<u64>,
    /// Node lease TTL.
    pub lease_ttl_ms: Option<u64>,
    /// Shared-state request timeout.
    pub request_timeout_ms: Option<u64>,
    /// etcd backend connection block (TOML `[etcd]`).
    #[cfg(feature = "etcd")]
    pub etcd: Option<EtcdConfig>,
    /// Kubernetes backend block (TOML `[kubernetes]`).
    #[cfg(feature = "kubernetes")]
    pub kubernetes: Option<KubernetesConfig>,
    /// env/CLI-only override for `etcd.endpoints` (never a TOML key).
    #[cfg(feature = "etcd")]
    #[serde(skip)]
    pub etcd_endpoints: Option<Vec<String>>,
    /// env/CLI-only override for `etcd.username` (never a TOML key).
    #[cfg(feature = "etcd")]
    #[serde(skip)]
    pub etcd_username: Option<String>,
    /// env/CLI-only override for `etcd.password`, so secrets stay out of the
    /// config file (never a TOML key).
    #[cfg(feature = "etcd")]
    #[serde(skip)]
    pub etcd_password: Option<String>,
    /// env/CLI-only override for the etcd CA certificate path (never a TOML key).
    #[cfg(feature = "etcd")]
    #[serde(skip)]
    pub etcd_ca_cert_path: Option<String>,
    /// env/CLI-only override for the etcd client certificate path.
    #[cfg(feature = "etcd")]
    #[serde(skip)]
    pub etcd_client_cert_path: Option<String>,
    /// env/CLI-only override for the etcd client key path.
    #[cfg(feature = "etcd")]
    #[serde(skip)]
    pub etcd_client_key_path: Option<String>,
    /// env/CLI-only override for `kubernetes.namespace` (never a TOML key).
    #[cfg(feature = "kubernetes")]
    #[serde(skip)]
    pub kubernetes_namespace: Option<String>,
}

impl Patch for CoordinatorConfigPatch {
    fn merge(self, base: Self) -> Self {
        Self {
            listen: self.listen.or(base.listen),
            admin_listen: self.admin_listen.or(base.admin_listen),
            admin_advertise: self.admin_advertise.or(base.admin_advertise),
            cluster_id: self.cluster_id.or(base.cluster_id),
            node_id: self.node_id.or(base.node_id),
            state_backend: self.state_backend.or(base.state_backend),
            ha_enabled: self.ha_enabled.or(base.ha_enabled),
            coordinator_replicas: self.coordinator_replicas.or(base.coordinator_replicas),
            heartbeat_interval_ms: self.heartbeat_interval_ms.or(base.heartbeat_interval_ms),
            unhealthy_after_ms: self.unhealthy_after_ms.or(base.unhealthy_after_ms),
            lease_ttl_ms: self.lease_ttl_ms.or(base.lease_ttl_ms),
            request_timeout_ms: self.request_timeout_ms.or(base.request_timeout_ms),
            #[cfg(feature = "etcd")]
            etcd: self.etcd.or(base.etcd),
            #[cfg(feature = "kubernetes")]
            kubernetes: self.kubernetes.or(base.kubernetes),
            #[cfg(feature = "etcd")]
            etcd_endpoints: self.etcd_endpoints.or(base.etcd_endpoints),
            #[cfg(feature = "etcd")]
            etcd_username: self.etcd_username.or(base.etcd_username),
            #[cfg(feature = "etcd")]
            etcd_password: self.etcd_password.or(base.etcd_password),
            #[cfg(feature = "etcd")]
            etcd_ca_cert_path: self.etcd_ca_cert_path.or(base.etcd_ca_cert_path),
            #[cfg(feature = "etcd")]
            etcd_client_cert_path: self.etcd_client_cert_path.or(base.etcd_client_cert_path),
            #[cfg(feature = "etcd")]
            etcd_client_key_path: self.etcd_client_key_path.or(base.etcd_client_key_path),
            #[cfg(feature = "kubernetes")]
            kubernetes_namespace: self.kubernetes_namespace.or(base.kubernetes_namespace),
        }
    }
}

/// Single source of truth for the coordinator's `TALON_COORDINATOR_*`
/// environment variables. [`CoordinatorConfigPatch::from_env_with`] reads the
/// names from this table, and the documentation generator renders it, so the
/// configuration reference cannot drift from the parser.
pub const COORDINATOR_ENV_SCHEMA: &[ConfigVar] = &[
    ConfigVar {
        env: "TALON_COORDINATOR_LISTEN",
        key: "listen",
        default: Some("127.0.0.1:7000"),
        cli: true,
        secret: false,
        help: "Control-plane bind address (workers/clients).",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_ADMIN_LISTEN",
        key: "admin_listen",
        default: Some("127.0.0.1:8000"),
        cli: true,
        secret: false,
        help: "Admin HTTP bind address: metrics, health, API, UI.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_ADMIN_ADVERTISE",
        key: "admin_advertise",
        default: Some("<admin_listen>"),
        cli: true,
        secret: false,
        help: "Admin address advertised in node status.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_CLUSTER_ID",
        key: "cluster_id",
        default: Some("default"),
        cli: true,
        secret: false,
        help: "Logical cluster identity.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_NODE_ID",
        key: "node_id",
        default: Some("<listen>"),
        cli: true,
        secret: false,
        help: "Stable coordinator node identity.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_STATE_BACKEND",
        key: "state_backend",
        default: Some("memory"),
        cli: true,
        secret: false,
        help: "Shared-state backend: memory | etcd | kubernetes.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_HA_ENABLED",
        key: "ha_enabled",
        default: Some("false"),
        cli: true,
        secret: false,
        help: "Enable active-active mode; rejects the memory backend.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_REPLICAS",
        key: "coordinator_replicas",
        default: Some("1"),
        cli: true,
        secret: false,
        help: "Expected coordinator replica count.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS",
        key: "heartbeat_interval_ms",
        default: Some("5000"),
        cli: true,
        secret: false,
        help: "Node heartbeat interval (ms).",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_UNHEALTHY_AFTER_MS",
        key: "unhealthy_after_ms",
        default: Some("15000"),
        cli: true,
        secret: false,
        help: "Silence before a node is unhealthy (ms); must exceed heartbeat.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_LEASE_TTL_MS",
        key: "lease_ttl_ms",
        default: Some("30000"),
        cli: true,
        secret: false,
        help: "Node lease TTL (ms); must exceed unhealthy_after.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_REQUEST_TIMEOUT_MS",
        key: "request_timeout_ms",
        default: Some("3000"),
        cli: true,
        secret: false,
        help: "Deadline for one authoritative backend operation (ms).",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_AUTH_TOKEN",
        key: "(env only)",
        default: None,
        cli: false,
        secret: true,
        help: "Bearer token (>= 16 chars) enabling API/UI authentication; unset disables auth.",
    },
    ConfigVar {
        env: "TALON_COORDINATOR_TRUST_FORWARDED",
        key: "(env only)",
        default: Some("false"),
        cli: false,
        secret: false,
        help: "Honor X-Forwarded-For for audit attribution behind a trusted proxy.",
    },
    #[cfg(feature = "etcd")]
    ConfigVar {
        env: "TALON_COORDINATOR_ETCD_ENDPOINTS",
        key: "etcd.endpoints",
        default: None,
        cli: false,
        secret: false,
        help: "Comma-separated etcd host:port endpoints.",
    },
    #[cfg(feature = "etcd")]
    ConfigVar {
        env: "TALON_COORDINATOR_ETCD_USERNAME",
        key: "etcd.username",
        default: None,
        cli: false,
        secret: false,
        help: "etcd username (requires password).",
    },
    #[cfg(feature = "etcd")]
    ConfigVar {
        env: "TALON_COORDINATOR_ETCD_PASSWORD",
        key: "etcd.password",
        default: None,
        cli: false,
        secret: true,
        help: "etcd password; keep in a Secret, not the config file.",
    },
    #[cfg(feature = "etcd")]
    ConfigVar {
        env: "TALON_COORDINATOR_ETCD_CA_CERT_PATH",
        key: "etcd.tls.ca_cert_path",
        default: None,
        cli: false,
        secret: false,
        help: "PEM CA certificate path; enables TLS.",
    },
    #[cfg(feature = "etcd")]
    ConfigVar {
        env: "TALON_COORDINATOR_ETCD_CLIENT_CERT_PATH",
        key: "etcd.tls.client_cert_path",
        default: None,
        cli: false,
        secret: false,
        help: "PEM client certificate path; mutual TLS.",
    },
    #[cfg(feature = "etcd")]
    ConfigVar {
        env: "TALON_COORDINATOR_ETCD_CLIENT_KEY_PATH",
        key: "etcd.tls.client_key_path",
        default: None,
        cli: false,
        secret: false,
        help: "PEM client key path; mutual TLS.",
    },
    #[cfg(feature = "kubernetes")]
    ConfigVar {
        env: "TALON_COORDINATOR_K8S_NAMESPACE",
        key: "kubernetes.namespace",
        default: Some("talon"),
        cli: false,
        secret: false,
        help: "Namespace holding Talon Lease objects.",
    },
];

/// Env-var name constants, referenced by both the schema above and the parser
/// below so the two can never disagree on a name.
pub mod env_names {
    pub const LISTEN: &str = "TALON_COORDINATOR_LISTEN";
    pub const ADMIN_LISTEN: &str = "TALON_COORDINATOR_ADMIN_LISTEN";
    pub const ADMIN_ADVERTISE: &str = "TALON_COORDINATOR_ADMIN_ADVERTISE";
    pub const CLUSTER_ID: &str = "TALON_COORDINATOR_CLUSTER_ID";
    pub const NODE_ID: &str = "TALON_COORDINATOR_NODE_ID";
    pub const STATE_BACKEND: &str = "TALON_COORDINATOR_STATE_BACKEND";
    pub const HA_ENABLED: &str = "TALON_COORDINATOR_HA_ENABLED";
    pub const REPLICAS: &str = "TALON_COORDINATOR_REPLICAS";
    pub const HEARTBEAT_INTERVAL_MS: &str = "TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS";
    pub const UNHEALTHY_AFTER_MS: &str = "TALON_COORDINATOR_UNHEALTHY_AFTER_MS";
    pub const LEASE_TTL_MS: &str = "TALON_COORDINATOR_LEASE_TTL_MS";
    pub const REQUEST_TIMEOUT_MS: &str = "TALON_COORDINATOR_REQUEST_TIMEOUT_MS";
    pub const AUTH_TOKEN: &str = "TALON_COORDINATOR_AUTH_TOKEN";
    pub const TRUST_FORWARDED: &str = "TALON_COORDINATOR_TRUST_FORWARDED";
    #[cfg(feature = "etcd")]
    pub const ETCD_ENDPOINTS: &str = "TALON_COORDINATOR_ETCD_ENDPOINTS";
    #[cfg(feature = "etcd")]
    pub const ETCD_USERNAME: &str = "TALON_COORDINATOR_ETCD_USERNAME";
    #[cfg(feature = "etcd")]
    pub const ETCD_PASSWORD: &str = "TALON_COORDINATOR_ETCD_PASSWORD";
    #[cfg(feature = "etcd")]
    pub const ETCD_CA_CERT_PATH: &str = "TALON_COORDINATOR_ETCD_CA_CERT_PATH";
    #[cfg(feature = "etcd")]
    pub const ETCD_CLIENT_CERT_PATH: &str = "TALON_COORDINATOR_ETCD_CLIENT_CERT_PATH";
    #[cfg(feature = "etcd")]
    pub const ETCD_CLIENT_KEY_PATH: &str = "TALON_COORDINATOR_ETCD_CLIENT_KEY_PATH";
    #[cfg(feature = "kubernetes")]
    pub const K8S_NAMESPACE: &str = "TALON_COORDINATOR_K8S_NAMESPACE";
}

impl CoordinatorConfigPatch {
    /// Parse a TOML patch.
    pub fn from_toml(value: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(value)?)
    }

    /// Load a TOML patch; a missing path is treated as an empty layer.
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(value) => Self::from_toml(&value),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }

    /// Read `TALON_COORDINATOR_*` environment variables.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_env_with(|key| std::env::var(key).ok())
    }

    /// Injectable environment parser used by tests.
    pub fn from_env_with(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        fn parse<T: std::str::FromStr>(value: String, key: &str) -> anyhow::Result<T> {
            value
                .parse()
                .map_err(|_| anyhow::anyhow!("{key}: invalid value {value:?}"))
        }

        Ok(Self {
            listen: get(env_names::LISTEN),
            admin_listen: get(env_names::ADMIN_LISTEN),
            admin_advertise: get(env_names::ADMIN_ADVERTISE),
            cluster_id: get(env_names::CLUSTER_ID),
            node_id: get(env_names::NODE_ID),
            state_backend: get(env_names::STATE_BACKEND)
                .map(|value| parse(value, env_names::STATE_BACKEND))
                .transpose()?,
            ha_enabled: get(env_names::HA_ENABLED)
                .map(|value| parse(value, env_names::HA_ENABLED))
                .transpose()?,
            coordinator_replicas: get(env_names::REPLICAS)
                .map(|value| parse(value, env_names::REPLICAS))
                .transpose()?,
            heartbeat_interval_ms: get(env_names::HEARTBEAT_INTERVAL_MS)
                .map(|value| parse(value, env_names::HEARTBEAT_INTERVAL_MS))
                .transpose()?,
            unhealthy_after_ms: get(env_names::UNHEALTHY_AFTER_MS)
                .map(|value| parse(value, env_names::UNHEALTHY_AFTER_MS))
                .transpose()?,
            lease_ttl_ms: get(env_names::LEASE_TTL_MS)
                .map(|value| parse(value, env_names::LEASE_TTL_MS))
                .transpose()?,
            request_timeout_ms: get(env_names::REQUEST_TIMEOUT_MS)
                .map(|value| parse(value, env_names::REQUEST_TIMEOUT_MS))
                .transpose()?,
            #[cfg(feature = "etcd")]
            etcd: None,
            #[cfg(feature = "kubernetes")]
            kubernetes: None,
            #[cfg(feature = "etcd")]
            etcd_endpoints: get(env_names::ETCD_ENDPOINTS).map(|value| {
                value
                    .split(',')
                    .map(|endpoint| endpoint.trim().to_string())
                    .filter(|endpoint| !endpoint.is_empty())
                    .collect()
            }),
            #[cfg(feature = "etcd")]
            etcd_username: get(env_names::ETCD_USERNAME),
            #[cfg(feature = "etcd")]
            etcd_password: get(env_names::ETCD_PASSWORD),
            #[cfg(feature = "etcd")]
            etcd_ca_cert_path: get(env_names::ETCD_CA_CERT_PATH),
            #[cfg(feature = "etcd")]
            etcd_client_cert_path: get(env_names::ETCD_CLIENT_CERT_PATH),
            #[cfg(feature = "etcd")]
            etcd_client_key_path: get(env_names::ETCD_CLIENT_KEY_PATH),
            #[cfg(feature = "kubernetes")]
            kubernetes_namespace: get(env_names::K8S_NAMESPACE),
        })
    }
}

impl CoordinatorConfig {
    /// Resolve defaults, file, environment, and CLI layers.
    pub fn resolve(
        file: CoordinatorConfigPatch,
        env: CoordinatorConfigPatch,
        cli: CoordinatorConfigPatch,
    ) -> anyhow::Result<Self> {
        let merged = cli.merge(env).merge(file);
        let defaults = Self::default();
        let default_state = defaults.state;
        let listen = merged.listen.unwrap_or(defaults.listen);
        let admin_listen = merged.admin_listen.unwrap_or(defaults.admin_listen);
        let cluster_id = merged.cluster_id.unwrap_or(defaults.cluster_id);

        // Fold backend blocks with their env/CLI scalar overrides. The block may
        // come entirely from an env override even with no `[etcd]`/`[kubernetes]`
        // table in the config file.
        #[cfg(feature = "etcd")]
        let etcd = {
            let mut etcd = merged.etcd;
            let has_override = merged.etcd_endpoints.is_some()
                || merged.etcd_username.is_some()
                || merged.etcd_password.is_some()
                || merged.etcd_ca_cert_path.is_some()
                || merged.etcd_client_cert_path.is_some()
                || merged.etcd_client_key_path.is_some();
            if has_override {
                let block = etcd.get_or_insert_with(EtcdConfig::default);
                if let Some(endpoints) = merged.etcd_endpoints {
                    block.endpoints = endpoints;
                }
                if let Some(username) = merged.etcd_username {
                    block.username = Some(username);
                }
                if let Some(password) = merged.etcd_password {
                    block.password = Some(password);
                }
                // TLS material paths compose over any file-provided [etcd.tls].
                if merged.etcd_ca_cert_path.is_some()
                    || merged.etcd_client_cert_path.is_some()
                    || merged.etcd_client_key_path.is_some()
                {
                    let tls = block.tls.get_or_insert_with(EtcdTlsConfig::default);
                    if let Some(path) = merged.etcd_ca_cert_path {
                        tls.ca_cert_path = Some(path.into());
                    }
                    if let Some(path) = merged.etcd_client_cert_path {
                        tls.client_cert_path = Some(path.into());
                    }
                    if let Some(path) = merged.etcd_client_key_path {
                        tls.client_key_path = Some(path.into());
                    }
                }
            }
            etcd
        };
        #[cfg(feature = "kubernetes")]
        let kubernetes = {
            let mut kubernetes = merged.kubernetes;
            if merged.kubernetes_namespace.is_some() {
                let block = kubernetes.get_or_insert_with(KubernetesConfig::default);
                if let Some(namespace) = merged.kubernetes_namespace {
                    block.namespace = namespace;
                }
            }
            // Default the backend's logical cluster id to the coordinator's when
            // left blank, so operators configure it in one place.
            if let Some(block) = kubernetes.as_mut() {
                if block.cluster_id.trim().is_empty() {
                    block.cluster_id = cluster_id.clone();
                }
            }
            kubernetes
        };

        let config = Self {
            node_id: merged.node_id.unwrap_or_else(|| listen.clone()),
            admin_advertise: merged
                .admin_advertise
                .unwrap_or_else(|| admin_listen.clone()),
            listen,
            admin_listen,
            cluster_id,
            state: ClusterStateConfig {
                backend: merged.state_backend.unwrap_or(default_state.backend),
                ha_enabled: merged.ha_enabled.unwrap_or(default_state.ha_enabled),
                coordinator_replicas: merged
                    .coordinator_replicas
                    .unwrap_or(default_state.coordinator_replicas),
                heartbeat_interval_ms: merged
                    .heartbeat_interval_ms
                    .unwrap_or(default_state.heartbeat_interval_ms),
                unhealthy_after_ms: merged
                    .unhealthy_after_ms
                    .unwrap_or(default_state.unhealthy_after_ms),
                lease_ttl_ms: merged.lease_ttl_ms.unwrap_or(default_state.lease_ttl_ms),
                request_timeout_ms: merged
                    .request_timeout_ms
                    .unwrap_or(default_state.request_timeout_ms),
            },
            #[cfg(feature = "etcd")]
            etcd,
            #[cfg(feature = "kubernetes")]
            kubernetes,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate addresses, bounded identity fields, and state timing.
    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, value) in [
            ("listen", self.listen.as_str()),
            ("admin_listen", self.admin_listen.as_str()),
            ("admin_advertise", self.admin_advertise.as_str()),
            ("cluster_id", self.cluster_id.as_str()),
            ("node_id", self.node_id.as_str()),
        ] {
            if value.is_empty() {
                anyhow::bail!("{name} must not be empty");
            }
            if value.len() > MAX_STATUS_FIELD_BYTES {
                anyhow::bail!(
                    "{name} is {} bytes; maximum is {MAX_STATUS_FIELD_BYTES}",
                    value.len()
                );
            }
        }
        self.state.validate()?;

        // The selected backend must have a matching configuration block, and the
        // block itself must be valid. Memory needs no block.
        match self.state.backend {
            StateBackend::Memory => {}
            StateBackend::Etcd => {
                #[cfg(feature = "etcd")]
                {
                    let etcd = self.etcd.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "state_backend is \"etcd\" but no [etcd] configuration was provided"
                        )
                    })?;
                    etcd.validate()?;
                }
                #[cfg(not(feature = "etcd"))]
                anyhow::bail!(
                    "state_backend is \"etcd\" but this binary was built without the \"etcd\" \
                     feature; rebuild with --features etcd"
                );
            }
            StateBackend::Kubernetes => {
                #[cfg(feature = "kubernetes")]
                {
                    let kubernetes = self.kubernetes.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "state_backend is \"kubernetes\" but no [kubernetes] configuration \
                             was provided"
                        )
                    })?;
                    kubernetes.validate()?;
                }
                #[cfg(not(feature = "kubernetes"))]
                anyhow::bail!(
                    "state_backend is \"kubernetes\" but this binary was built without the \
                     \"kubernetes\" feature; rebuild with --features kubernetes"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precedence_and_advertise_fallback_are_resolved() {
        let file = CoordinatorConfigPatch {
            listen: Some("file:7000".into()),
            admin_listen: Some("file:8000".into()),
            ..Default::default()
        };
        let env = CoordinatorConfigPatch {
            admin_listen: Some("env:8000".into()),
            cluster_id: Some("prod".into()),
            ..Default::default()
        };
        let cli = CoordinatorConfigPatch {
            listen: Some("cli:7000".into()),
            admin_advertise: Some("public:8000".into()),
            ..Default::default()
        };
        let config = CoordinatorConfig::resolve(file, env, cli).unwrap();
        assert_eq!(config.listen, "cli:7000");
        assert_eq!(config.admin_listen, "env:8000");
        assert_eq!(config.admin_advertise, "public:8000");
        assert_eq!(config.node_id, "cli:7000");
        assert_eq!(config.cluster_id, "prod");
    }

    #[test]
    fn environment_and_toml_are_typed() {
        let patch = CoordinatorConfigPatch::from_toml(
            "admin_listen = \"0.0.0.0:8000\"\nstate_backend = \"memory\"\n",
        )
        .unwrap();
        assert_eq!(patch.admin_listen.as_deref(), Some("0.0.0.0:8000"));
        assert_eq!(patch.state_backend, Some(StateBackend::Memory));

        let patch = CoordinatorConfigPatch::from_env_with(|key| match key {
            "TALON_COORDINATOR_REPLICAS" => Some("3".into()),
            "TALON_COORDINATOR_HA_ENABLED" => Some("true".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(patch.coordinator_replicas, Some(3));
        assert_eq!(patch.ha_enabled, Some(true));
    }

    #[test]
    fn invalid_identity_and_state_config_fail_fast() {
        let config = CoordinatorConfig {
            node_id: String::new(),
            ..Default::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("node_id"));

        let config = CoordinatorConfig {
            state: ClusterStateConfig {
                request_timeout_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("request_timeout_ms"));
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn etcd_backend_requires_config_block() {
        // Selecting etcd without an [etcd] block fails validation.
        let cli = CoordinatorConfigPatch {
            state_backend: Some(StateBackend::Etcd),
            ha_enabled: Some(true),
            coordinator_replicas: Some(3),
            ..Default::default()
        };
        let error = CoordinatorConfig::resolve(
            CoordinatorConfigPatch::default(),
            CoordinatorConfigPatch::default(),
            cli,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("[etcd]"), "unexpected error: {error}");
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn etcd_block_parses_from_toml_and_env_overrides_endpoints() {
        let file = CoordinatorConfigPatch::from_toml(
            "state_backend = \"etcd\"\n\
             ha_enabled = true\n\
             coordinator_replicas = 3\n\
             [etcd]\n\
             endpoints = [\"https://etcd-a:2379\"]\n\
             prefix = \"/talon\"\n\
             username = \"root\"\n",
        )
        .unwrap();
        let env = CoordinatorConfigPatch::from_env_with(|key| match key {
            "TALON_COORDINATOR_ETCD_ENDPOINTS" => {
                Some("https://etcd-x:2379, https://etcd-y:2379".into())
            }
            "TALON_COORDINATOR_ETCD_USERNAME" => Some("root".into()),
            "TALON_COORDINATOR_ETCD_PASSWORD" => Some("s3cr3t".into()),
            "TALON_COORDINATOR_ETCD_CA_CERT_PATH" => Some("/etc/talon/etcd/ca.crt".into()),
            _ => None,
        })
        .unwrap();
        let config =
            CoordinatorConfig::resolve(file, env, CoordinatorConfigPatch::default()).unwrap();
        let etcd = config.etcd.clone().expect("etcd block present");
        // env endpoints win over the file's; password comes from env only.
        assert_eq!(
            etcd.endpoints,
            vec![
                "https://etcd-x:2379".to_string(),
                "https://etcd-y:2379".to_string()
            ]
        );
        assert_eq!(etcd.password.as_deref(), Some("s3cr3t"));
        assert_eq!(
            etcd.tls.as_ref().and_then(|t| t.ca_cert_path.as_deref()),
            Some(std::path::Path::new("/etc/talon/etcd/ca.crt"))
        );
        // Secret must never appear in Debug output.
        assert!(!format!("{config:?}").contains("s3cr3t"));
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn kubernetes_block_defaults_cluster_id_and_reads_namespace_env() {
        let file = CoordinatorConfigPatch::from_toml(
            "state_backend = \"kubernetes\"\n\
             cluster_id = \"prod\"\n\
             ha_enabled = true\n\
             coordinator_replicas = 2\n\
             [kubernetes]\n\
             namespace = \"talon\"\n",
        )
        .unwrap();
        let env = CoordinatorConfigPatch::from_env_with(|key| match key {
            "TALON_COORDINATOR_K8S_NAMESPACE" => Some("talon-system".into()),
            _ => None,
        })
        .unwrap();
        let config =
            CoordinatorConfig::resolve(file, env, CoordinatorConfigPatch::default()).unwrap();
        let kubernetes = config.kubernetes.expect("kubernetes block present");
        assert_eq!(kubernetes.namespace, "talon-system");
        // cluster_id defaults from the coordinator's cluster_id.
        assert_eq!(kubernetes.cluster_id, "prod");
    }
}
