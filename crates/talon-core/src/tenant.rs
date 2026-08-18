//! Tenant identity for per-tenant quality-of-service.
//!
//! A *tenant* is the authenticated resource owner a request is attributed to
//! when the cluster enforces per-tenant rate limits (and, later, cache quotas).
//! Tenant identity originates at the object-store gateway, which resolves it
//! from the request credential — the provider account established during
//! authentication — and is propagated to workers on the data plane. Enforcement
//! therefore keys on a non-forgeable identity derived from the authenticated
//! request rather than on a client-supplied claim.
//!
//! Clients that connect directly to a worker and bypass the gateway — the FUSE
//! mount and the native clients — present no credential. Their traffic is
//! attributed to the reserved [`TenantId::Unattributed`] tenant so that it is
//! still counted and bounded rather than silently escaping per-tenant policy.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Maximum length, in bytes, of a tenant account identifier.
///
/// Tenant ids are derived from a gateway-authenticated provider account and
/// then travel through control-plane messages, serve as map keys on the hot
/// path, and appear in bounded telemetry. A fixed ceiling keeps every one of
/// those uses bounded regardless of the credential's contents.
pub const MAX_TENANT_ID_BYTES: usize = 256;

/// The authenticated resource owner a request is attributed to for QoS.
///
/// See the [module documentation](self) for how identity is established and why
/// the unattributed tenant exists. `TenantId` is `Hash + Eq`, so it is used
/// directly as a map key; two authenticated tenants are equal exactly when they
/// name the same provider account.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TenantId {
    /// An authenticated tenant, named by its gateway provider account.
    Principal(String),
    /// Traffic that arrived without an authenticated tenant — a direct client
    /// that bypassed the gateway.
    Unattributed,
}

impl TenantId {
    /// Human-readable label for the [`TenantId::Unattributed`] tenant in logs,
    /// the management API, and (allow-listed) telemetry.
    pub const UNATTRIBUTED_LABEL: &'static str = "unattributed";

    /// Construct a tenant from an authenticated provider account.
    ///
    /// The account is taken verbatim; call [`TenantId::validate`] at the trust
    /// boundary to reject an empty or over-long value.
    pub fn principal(account: impl Into<String>) -> Self {
        Self::Principal(account.into())
    }

    /// The reserved tenant for traffic with no authenticated identity.
    pub const fn unattributed() -> Self {
        Self::Unattributed
    }

    /// Whether this is the reserved [`TenantId::Unattributed`] tenant.
    pub fn is_unattributed(&self) -> bool {
        matches!(self, Self::Unattributed)
    }

    /// The provider account backing an authenticated tenant, or `None` for the
    /// unattributed tenant.
    ///
    /// Prefer matching on the tenant, or using it directly as a map key, over
    /// flattening it to a string: two distinct tenants never share an account,
    /// and the unattributed tenant deliberately has none.
    pub fn account(&self) -> Option<&str> {
        match self {
            Self::Principal(account) => Some(account),
            Self::Unattributed => None,
        }
    }

    /// Validate a tenant established at a trust boundary.
    ///
    /// An authenticated tenant must carry a non-empty account no longer than
    /// [`MAX_TENANT_ID_BYTES`]. The unattributed tenant is always valid.
    pub fn validate(&self) -> Result<(), TenantIdError> {
        match self {
            Self::Unattributed => Ok(()),
            Self::Principal(account) if account.is_empty() => Err(TenantIdError::Empty),
            Self::Principal(account) if account.len() > MAX_TENANT_ID_BYTES => {
                Err(TenantIdError::TooLong(account.len()))
            }
            Self::Principal(_) => Ok(()),
        }
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Principal(account) => f.write_str(account),
            Self::Unattributed => f.write_str(Self::UNATTRIBUTED_LABEL),
        }
    }
}

/// Why a tenant identifier is rejected at a trust boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TenantIdError {
    /// An authenticated tenant carried an empty account.
    #[error("tenant account is empty")]
    Empty,
    /// An authenticated tenant account exceeded the maximum tenant id length.
    #[error("tenant account of {0} bytes exceeds the maximum tenant id length")]
    TooLong(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_carries_its_account() {
        let tenant = TenantId::principal("acme");
        assert_eq!(tenant.account(), Some("acme"));
        assert!(!tenant.is_unattributed());
        assert_eq!(tenant.to_string(), "acme");
    }

    #[test]
    fn unattributed_has_no_account() {
        let tenant = TenantId::unattributed();
        assert_eq!(tenant.account(), None);
        assert!(tenant.is_unattributed());
        assert_eq!(tenant.to_string(), "unattributed");
    }

    #[test]
    fn equality_is_by_account_and_never_conflates_unattributed() {
        assert_eq!(TenantId::principal("a"), TenantId::principal("a"));
        assert_ne!(TenantId::principal("a"), TenantId::principal("b"));
        assert_ne!(TenantId::principal("a"), TenantId::Unattributed);
        // A tenant literally named like the unattributed label is still a
        // distinct authenticated tenant, not the reserved one.
        let lookalike = TenantId::principal(TenantId::UNATTRIBUTED_LABEL);
        assert_ne!(lookalike, TenantId::Unattributed);
        assert!(!lookalike.is_unattributed());
    }

    #[test]
    fn usable_as_a_map_key() {
        use std::collections::HashMap;
        let mut counts: HashMap<TenantId, u64> = HashMap::new();
        *counts.entry(TenantId::principal("a")).or_default() += 1;
        *counts.entry(TenantId::principal("a")).or_default() += 1;
        *counts.entry(TenantId::unattributed()).or_default() += 1;
        assert_eq!(counts[&TenantId::principal("a")], 2);
        assert_eq!(counts[&TenantId::unattributed()], 1);
    }

    #[test]
    fn validate_bounds_authenticated_accounts() {
        assert_eq!(TenantId::unattributed().validate(), Ok(()));
        assert_eq!(TenantId::principal("acme").validate(), Ok(()));
        assert_eq!(
            TenantId::principal("").validate(),
            Err(TenantIdError::Empty)
        );
        let overlong = "x".repeat(MAX_TENANT_ID_BYTES + 1);
        assert_eq!(
            TenantId::principal(overlong).validate(),
            Err(TenantIdError::TooLong(MAX_TENANT_ID_BYTES + 1))
        );
        assert_eq!(
            TenantId::principal("y".repeat(MAX_TENANT_ID_BYTES)).validate(),
            Ok(())
        );
    }

    #[test]
    fn serde_round_trips_both_variants() {
        for tenant in [TenantId::principal("acme"), TenantId::unattributed()] {
            let encoded = serde_json::to_string(&tenant).unwrap();
            let decoded: TenantId = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, tenant);
        }
    }
}
