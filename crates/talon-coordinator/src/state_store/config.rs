//! Cluster-state backend and lease timing configuration.

use std::fmt;
use std::str::FromStr;

use clap::ValueEnum;
use serde::Deserialize;

/// Shared-state backend selected by a coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum StateBackend {
    /// Single-process development and tests only.
    Memory,
    /// External etcd v3 cluster.
    Etcd,
    /// Kubernetes API using namespaced Lease resources.
    #[value(alias = "k8s")]
    #[serde(alias = "k8s")]
    Kubernetes,
}

impl fmt::Display for StateBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Memory => "memory",
            Self::Etcd => "etcd",
            Self::Kubernetes => "kubernetes",
        })
    }
}

impl FromStr for StateBackend {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "memory" => Ok(Self::Memory),
            "etcd" => Ok(Self::Etcd),
            "kubernetes" | "k8s" => Ok(Self::Kubernetes),
            _ => Err(ConfigError::UnknownBackend(value.to_string())),
        }
    }
}

/// Backend-neutral cluster-state configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterStateConfig {
    /// Selected backend.
    pub backend: StateBackend,
    /// Whether active-active coordinator behavior is requested.
    pub ha_enabled: bool,
    /// Expected coordinator replica count.
    pub coordinator_replicas: u16,
    /// Node heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Silence interval after which a node is unhealthy.
    pub unhealthy_after_ms: u64,
    /// Silence interval after which the node lease expires.
    pub lease_ttl_ms: u64,
    /// Deadline for one authoritative backend operation.
    pub request_timeout_ms: u64,
}

impl Default for ClusterStateConfig {
    fn default() -> Self {
        Self {
            backend: StateBackend::Memory,
            ha_enabled: false,
            coordinator_replicas: 1,
            heartbeat_interval_ms: 5_000,
            unhealthy_after_ms: 15_000,
            lease_ttl_ms: 30_000,
            request_timeout_ms: 3_000,
        }
    }
}

impl ClusterStateConfig {
    /// Validate HA/backend selection and lease timing relationships.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.coordinator_replicas == 0 {
            return Err(ConfigError::ZeroReplicas);
        }
        if self.request_timeout_ms == 0 {
            return Err(ConfigError::ZeroRequestTimeout);
        }
        if self.heartbeat_interval_ms == 0
            || self.heartbeat_interval_ms >= self.unhealthy_after_ms
            || self.unhealthy_after_ms >= self.lease_ttl_ms
        {
            return Err(ConfigError::InvalidLeaseTiming {
                heartbeat_interval_ms: self.heartbeat_interval_ms,
                unhealthy_after_ms: self.unhealthy_after_ms,
                lease_ttl_ms: self.lease_ttl_ms,
            });
        }
        if self.backend == StateBackend::Memory
            && (self.ha_enabled || self.coordinator_replicas > 1)
        {
            return Err(ConfigError::MemoryBackendWithHa {
                ha_enabled: self.ha_enabled,
                coordinator_replicas: self.coordinator_replicas,
            });
        }
        Ok(())
    }
}

/// Invalid cluster-state configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// Backend selector was not recognized.
    #[error("unknown cluster-state backend {0:?}; expected memory, etcd, or kubernetes")]
    UnknownBackend(String),
    /// At least one coordinator replica is required.
    #[error("coordinator_replicas must be greater than zero")]
    ZeroReplicas,
    /// Authoritative operations require a positive timeout.
    #[error("request_timeout_ms must be greater than zero")]
    ZeroRequestTimeout,
    /// Heartbeat, unhealthy, and expiry timing were not strictly increasing.
    #[error(
        "lease timing must satisfy heartbeat_interval_ms < unhealthy_after_ms < lease_ttl_ms; \
         got {heartbeat_interval_ms} < {unhealthy_after_ms} < {lease_ttl_ms}"
    )]
    InvalidLeaseTiming {
        /// Configured heartbeat interval.
        heartbeat_interval_ms: u64,
        /// Configured unhealthy threshold.
        unhealthy_after_ms: u64,
        /// Configured lease expiry.
        lease_ttl_ms: u64,
    },
    /// The in-memory backend cannot coordinate multiple processes.
    #[error(
        "memory state backend is development-only and cannot run HA \
         (ha_enabled={ha_enabled}, coordinator_replicas={coordinator_replicas})"
    )]
    MemoryBackendWithHa {
        /// Whether HA mode was explicitly enabled.
        ha_enabled: bool,
        /// Requested coordinator replica count.
        coordinator_replicas: u16,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_selector_parses_supported_names() {
        assert_eq!("memory".parse(), Ok(StateBackend::Memory));
        assert_eq!("etcd".parse(), Ok(StateBackend::Etcd));
        assert_eq!("kubernetes".parse(), Ok(StateBackend::Kubernetes));
        assert_eq!("k8s".parse(), Ok(StateBackend::Kubernetes));
        assert!(matches!(
            "consul".parse::<StateBackend>(),
            Err(ConfigError::UnknownBackend(_))
        ));
    }

    #[test]
    fn defaults_are_valid_for_single_process_development() {
        ClusterStateConfig::default().validate().unwrap();
    }

    #[test]
    fn memory_backend_is_rejected_for_ha() {
        let config = ClusterStateConfig {
            ha_enabled: true,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MemoryBackendWithHa { .. })
        ));

        let config = ClusterStateConfig {
            coordinator_replicas: 2,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MemoryBackendWithHa { .. })
        ));
    }

    #[test]
    fn production_backends_accept_ha() {
        for backend in [StateBackend::Etcd, StateBackend::Kubernetes] {
            ClusterStateConfig {
                backend,
                ha_enabled: true,
                coordinator_replicas: 3,
                ..Default::default()
            }
            .validate()
            .unwrap();
        }
    }

    #[test]
    fn lease_timing_and_timeout_are_validated() {
        for config in [
            ClusterStateConfig {
                heartbeat_interval_ms: 0,
                ..Default::default()
            },
            ClusterStateConfig {
                unhealthy_after_ms: 5_000,
                ..Default::default()
            },
            ClusterStateConfig {
                lease_ttl_ms: 15_000,
                ..Default::default()
            },
        ] {
            assert!(matches!(
                config.validate(),
                Err(ConfigError::InvalidLeaseTiming { .. })
            ));
        }
        assert_eq!(
            ClusterStateConfig {
                request_timeout_ms: 0,
                ..Default::default()
            }
            .validate(),
            Err(ConfigError::ZeroRequestTimeout)
        );
    }
}
