//! Bounded runtime status shared by workers, coordinators, and management APIs.
//!
//! A [`NodeStatus`] is a latest-value snapshot, not a time-series sample. It is
//! small enough to carry in a control heartbeat and persist in either etcd or a
//! Kubernetes Lease annotation. Prometheus remains the source for histograms
//! and historical metrics.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::NodeInfo;

/// Current node-status value schema.
pub const NODE_STATUS_SCHEMA_VERSION: u16 = 1;

/// Maximum size of an encoded node-status value.
pub const MAX_NODE_STATUS_BYTES: usize = 16 * 1024;

/// Maximum UTF-8 byte length of an identity, version, or address field.
pub const MAX_STATUS_FIELD_BYTES: usize = 256;

/// Maximum number of deployment labels attached to a node.
pub const MAX_STATUS_LABELS: usize = 16;

/// Maximum UTF-8 byte length of a deployment-label key.
pub const MAX_STATUS_LABEL_KEY_BYTES: usize = 63;

/// Maximum UTF-8 byte length of a deployment-label value.
pub const MAX_STATUS_LABEL_VALUE_BYTES: usize = 256;

/// Health reported by a process in its latest status heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    /// The process and its required dependencies are operating normally.
    Healthy,
    /// The process is serving, but a dependency or subsystem is impaired.
    Degraded,
    /// The process cannot safely serve normal traffic.
    Unhealthy,
    /// The process has not made a health determination yet.
    Unknown,
}

/// Bounded latest-value counters and gauges needed by the management UI.
///
/// Histograms are intentionally excluded; they remain available through each
/// process's Prometheus endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMetricsSnapshot {
    /// Requests accepted since process start.
    pub requests_total: u64,
    /// Requests completed with an error since process start.
    pub errors_total: u64,
    /// Bytes returned to clients since process start.
    pub bytes_served_total: u64,
    /// Worker cache hits since process start.
    pub cache_hits_total: u64,
    /// Worker cache misses since process start.
    pub cache_misses_total: u64,
    /// Worker backend fetch failures since process start.
    pub backend_errors_total: u64,
    /// Worker evictions since process start.
    pub evictions_total: u64,
    /// Loads currently in flight.
    pub inflight_loads: u64,
    /// Blocks currently indexed by a worker.
    pub block_count: u64,
    /// Materialized pages currently indexed by a worker.
    pub page_count: u64,
    /// Bytes currently resident on a worker.
    pub resident_bytes: u64,
    /// Configured worker cache capacity in bytes.
    pub capacity_bytes: u64,
    /// Age of the coordinator's latest shared-state snapshot in milliseconds.
    pub state_snapshot_age_ms: u64,
}

/// Versioned status sent by a coordinator or worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    /// Schema version of this status value.
    pub schema_version: u16,
    /// Logical Talon cluster containing the node.
    pub cluster_id: String,
    /// Stable node identity, role, and service address.
    pub node: NodeInfo,
    /// Random process-incarnation identity generated at process start.
    pub incarnation_id: String,
    /// HTTP administration address, if enabled and advertised.
    pub admin_address: Option<String>,
    /// Talon package/build version.
    pub build_version: String,
    /// Process start time as Unix milliseconds.
    pub started_at_unix_ms: u64,
    /// Time this heartbeat snapshot was created, as Unix milliseconds.
    pub reported_at_unix_ms: u64,
    /// Monotonically increasing sequence within this process incarnation.
    pub heartbeat_seq: u64,
    /// Current process health.
    pub health: NodeHealth,
    /// Whether the process is ready to receive normal service traffic.
    pub ready: bool,
    /// Latest bounded metric snapshot.
    pub metrics: NodeMetricsSnapshot,
    /// Bounded deployment metadata such as region, zone, pod, or host.
    pub labels: BTreeMap<String, String>,
}

impl NodeStatus {
    /// Validate schema, field lengths, timestamps, and label cardinality.
    pub fn validate(&self) -> Result<(), NodeStatusError> {
        if self.schema_version != NODE_STATUS_SCHEMA_VERSION {
            return Err(NodeStatusError::UnsupportedSchema {
                got: self.schema_version,
                supported: NODE_STATUS_SCHEMA_VERSION,
            });
        }

        validate_required("cluster_id", &self.cluster_id, MAX_STATUS_FIELD_BYTES)?;
        validate_required("node.id", &self.node.id.0, MAX_STATUS_FIELD_BYTES)?;
        validate_required("node.address", &self.node.address, MAX_STATUS_FIELD_BYTES)?;
        validate_required(
            "incarnation_id",
            &self.incarnation_id,
            MAX_STATUS_FIELD_BYTES,
        )?;
        validate_required("build_version", &self.build_version, MAX_STATUS_FIELD_BYTES)?;
        if let Some(address) = &self.admin_address {
            validate_required("admin_address", address, MAX_STATUS_FIELD_BYTES)?;
        }
        if self.reported_at_unix_ms < self.started_at_unix_ms {
            return Err(NodeStatusError::ReportBeforeStart {
                started_at_unix_ms: self.started_at_unix_ms,
                reported_at_unix_ms: self.reported_at_unix_ms,
            });
        }
        if self.labels.len() > MAX_STATUS_LABELS {
            return Err(NodeStatusError::TooManyLabels {
                got: self.labels.len(),
                max: MAX_STATUS_LABELS,
            });
        }
        for (key, value) in &self.labels {
            validate_required("label key", key, MAX_STATUS_LABEL_KEY_BYTES)?;
            validate_length("label value", value, MAX_STATUS_LABEL_VALUE_BYTES)?;
        }
        Ok(())
    }
}

fn validate_required(field: &'static str, value: &str, max: usize) -> Result<(), NodeStatusError> {
    if value.is_empty() {
        return Err(NodeStatusError::EmptyField { field });
    }
    validate_length(field, value, max)
}

fn validate_length(field: &'static str, value: &str, max: usize) -> Result<(), NodeStatusError> {
    let got = value.len();
    if got > max {
        return Err(NodeStatusError::FieldTooLong { field, got, max });
    }
    Ok(())
}

/// Validation failure for a [`NodeStatus`] snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeStatusError {
    /// The value uses an unsupported status schema.
    #[error("unsupported node-status schema {got} (supported: {supported})")]
    UnsupportedSchema {
        /// Version found in the value.
        got: u16,
        /// Version understood by this build.
        supported: u16,
    },
    /// A required string field was empty.
    #[error("node-status field {field} must not be empty")]
    EmptyField {
        /// Name of the invalid field.
        field: &'static str,
    },
    /// A bounded string exceeded its UTF-8 byte limit.
    #[error("node-status field {field} is {got} bytes; maximum is {max}")]
    FieldTooLong {
        /// Name of the invalid field.
        field: &'static str,
        /// Observed UTF-8 byte length.
        got: usize,
        /// Maximum UTF-8 byte length.
        max: usize,
    },
    /// Too many deployment labels were provided.
    #[error("node status has {got} labels; maximum is {max}")]
    TooManyLabels {
        /// Observed number of labels.
        got: usize,
        /// Maximum number of labels.
        max: usize,
    },
    /// The report timestamp predates process start.
    #[error(
        "node status reported_at_unix_ms {reported_at_unix_ms} is before \
         started_at_unix_ms {started_at_unix_ms}"
    )]
    ReportBeforeStart {
        /// Process start time in Unix milliseconds.
        started_at_unix_ms: u64,
        /// Snapshot creation time in Unix milliseconds.
        reported_at_unix_ms: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeId, NodeRole};

    fn sample_status() -> NodeStatus {
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: "cluster-a".into(),
            node: NodeInfo {
                id: NodeId::new("worker-1"),
                address: "10.0.0.1:7001".into(),
                role: NodeRole::Worker,
            },
            incarnation_id: "01J2ABCDEF".into(),
            admin_address: Some("10.0.0.1:8001".into()),
            build_version: "0.1.0".into(),
            started_at_unix_ms: 1_000,
            reported_at_unix_ms: 2_000,
            heartbeat_seq: 7,
            health: NodeHealth::Healthy,
            ready: true,
            metrics: NodeMetricsSnapshot {
                requests_total: 12,
                resident_bytes: 1024,
                capacity_bytes: 4096,
                ..Default::default()
            },
            labels: BTreeMap::from([
                ("region".into(), "us-west".into()),
                ("zone".into(), "us-west-1a".into()),
            ]),
        }
    }

    #[test]
    fn valid_status_round_trips_as_json() {
        let status = sample_status();
        status.validate().unwrap();
        let encoded = serde_json::to_string(&status).unwrap();
        assert_eq!(
            serde_json::from_str::<NodeStatus>(&encoded).unwrap(),
            status
        );
    }

    #[test]
    fn rejects_unsupported_schema_and_invalid_timestamps() {
        let mut status = sample_status();
        status.schema_version += 1;
        assert!(matches!(
            status.validate(),
            Err(NodeStatusError::UnsupportedSchema { .. })
        ));

        let mut status = sample_status();
        status.reported_at_unix_ms = status.started_at_unix_ms - 1;
        assert!(matches!(
            status.validate(),
            Err(NodeStatusError::ReportBeforeStart { .. })
        ));
    }

    #[test]
    fn rejects_empty_and_oversized_fields() {
        let mut status = sample_status();
        status.cluster_id.clear();
        assert_eq!(
            status.validate(),
            Err(NodeStatusError::EmptyField {
                field: "cluster_id"
            })
        );

        let mut status = sample_status();
        status.node.address = "x".repeat(MAX_STATUS_FIELD_BYTES + 1);
        assert!(matches!(
            status.validate(),
            Err(NodeStatusError::FieldTooLong {
                field: "node.address",
                ..
            })
        ));
    }

    #[test]
    fn label_limits_are_enforced_in_utf8_bytes() {
        let mut status = sample_status();
        status.labels = (0..=MAX_STATUS_LABELS)
            .map(|i| (format!("k{i}"), "v".into()))
            .collect();
        assert_eq!(
            status.validate(),
            Err(NodeStatusError::TooManyLabels {
                got: MAX_STATUS_LABELS + 1,
                max: MAX_STATUS_LABELS
            })
        );

        let mut status = sample_status();
        status.labels = BTreeMap::from([(
            "region".into(),
            "界".repeat(MAX_STATUS_LABEL_VALUE_BYTES / 3 + 1),
        )]);
        assert!(matches!(
            status.validate(),
            Err(NodeStatusError::FieldTooLong {
                field: "label value",
                ..
            })
        ));
    }
}
