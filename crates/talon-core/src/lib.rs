//! # talon-core
//!
//! Shared types, traits, and protocol definitions for the Talon distributed
//! object store cache. All other Talon crates depend on this crate.

pub mod backend;
pub mod block;
pub mod config;
pub mod control_identity;
pub mod error;
pub mod key;
pub mod metrics;
pub mod namespace_policy;
pub mod node;
pub mod placement;
pub mod status;
pub mod store;
pub mod tenant;
pub mod trace;

pub use backend::{BackendStore, ListPage, ListedObject, ObjectStat};
pub use block::{page_len, BlockForm, BlockMeta, LoadHint, PresentBitmap};
pub use config::{
    azure_sas_from_env, gcs_bearer_from_env, parse_bool_value, s3_secret_key_from_env,
    s3_session_token_from_env, ConfigVar, FuseConfig, FuseConfigPatch, Patch, WorkerConfig,
    WorkerConfigPatch, FUSE_ENV_SCHEMA, WORKER_ENV_SCHEMA,
};
pub use control_identity::{
    ControlTlsConfig, ControlTlsConfigPatch, WorkloadIdentity, WorkloadIdentityError, WorkloadRole,
};
pub use error::{Error, Result};
pub use key::{Backend, BlockId, ObjectId, PageIndex, Version};
pub use metrics::{Counter, Gauge, Histogram, Metrics};
pub use namespace_policy::{NamespacePolicy, ObjectNamespace};
pub use node::{NodeId, NodeInfo, NodeRole};
pub use placement::{cache_membership_epoch, rank_cache_workers, CachePlacementTable};
pub use status::{
    NodeHealth, NodeMetricsSnapshot, NodeStatus, NodeStatusError, MAX_NODE_STATUS_BYTES,
    MAX_STATUS_FIELD_BYTES, MAX_STATUS_LABELS, MAX_STATUS_LABEL_KEY_BYTES,
    MAX_STATUS_LABEL_VALUE_BYTES, NODE_STATUS_SCHEMA_VERSION, NODE_ZONE_LABEL,
};
pub use store::{BlockHandle, ObjectStore};
pub use tenant::{TenantId, TenantIdError, MAX_TENANT_ID_BYTES};
pub use trace::{init_tracing, RequestId};
