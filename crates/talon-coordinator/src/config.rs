//! Layered coordinator configuration.

use std::path::Path;

use serde::Deserialize;
use talon_core::{Patch, MAX_STATUS_FIELD_BYTES};

use crate::{ClusterStateConfig, StateBackend};

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
        }
    }
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
            listen: get("TALON_COORDINATOR_LISTEN"),
            admin_listen: get("TALON_COORDINATOR_ADMIN_LISTEN"),
            admin_advertise: get("TALON_COORDINATOR_ADMIN_ADVERTISE"),
            cluster_id: get("TALON_COORDINATOR_CLUSTER_ID"),
            node_id: get("TALON_COORDINATOR_NODE_ID"),
            state_backend: get("TALON_COORDINATOR_STATE_BACKEND")
                .map(|value| parse(value, "TALON_COORDINATOR_STATE_BACKEND"))
                .transpose()?,
            ha_enabled: get("TALON_COORDINATOR_HA_ENABLED")
                .map(|value| parse(value, "TALON_COORDINATOR_HA_ENABLED"))
                .transpose()?,
            coordinator_replicas: get("TALON_COORDINATOR_REPLICAS")
                .map(|value| parse(value, "TALON_COORDINATOR_REPLICAS"))
                .transpose()?,
            heartbeat_interval_ms: get("TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS")
                .map(|value| parse(value, "TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS"))
                .transpose()?,
            unhealthy_after_ms: get("TALON_COORDINATOR_UNHEALTHY_AFTER_MS")
                .map(|value| parse(value, "TALON_COORDINATOR_UNHEALTHY_AFTER_MS"))
                .transpose()?,
            lease_ttl_ms: get("TALON_COORDINATOR_LEASE_TTL_MS")
                .map(|value| parse(value, "TALON_COORDINATOR_LEASE_TTL_MS"))
                .transpose()?,
            request_timeout_ms: get("TALON_COORDINATOR_REQUEST_TIMEOUT_MS")
                .map(|value| parse(value, "TALON_COORDINATOR_REQUEST_TIMEOUT_MS"))
                .transpose()?,
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
        let config = Self {
            node_id: merged.node_id.unwrap_or_else(|| listen.clone()),
            admin_advertise: merged
                .admin_advertise
                .unwrap_or_else(|| admin_listen.clone()),
            listen,
            admin_listen,
            cluster_id: merged.cluster_id.unwrap_or(defaults.cluster_id),
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
}
