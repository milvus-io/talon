//! Coordinator metrics, shared-state readiness, and administration HTTP API.
//!
//! Split into submodules along the file's three concerns: metric handles
//! ([`CoordinatorMetrics`]), readiness and shared-state access
//! ([`CoordinatorObservability`]), and the admin HTTP surface that exposes them
//! ([`serve_admin`]). Cross-cutting helpers stay here so both siblings can reach
//! them without widening visibility.

mod admin;
mod metrics;
mod state;

pub use admin::{admin_router, secured_admin_router, serve_admin, serve_admin_secured};
pub use metrics::{ControlOperation, CoordinatorConnectionGuard, CoordinatorMetrics};
pub use state::CoordinatorObservability;

use std::time::{SystemTime, UNIX_EPOCH};

use crate::StateStoreError;

fn state_error_kind(error: &StateStoreError) -> &'static str {
    match error {
        StateStoreError::Authentication { .. } => "authentication",
        StateStoreError::PermissionDenied { .. } => "permission_denied",
        StateStoreError::Timeout { .. } => "timeout",
        StateStoreError::Unavailable { .. } => "unavailable",
        StateStoreError::Compacted { .. } => "compacted",
        StateStoreError::WatchLagged { .. } => "watch_lagged",
        StateStoreError::InvalidRecord(_)
        | StateStoreError::InvalidLeaseTtl(_)
        | StateStoreError::InvalidRevision { .. } => "invalid",
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
