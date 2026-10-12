//! Tenant identity for cache namespaces and quality of service.
//!
//! Unspecified identities resolve to `default`. `Unattributed` is retained only
//! for decoding older clients and is normalized at request boundaries. Direct
//! clients declare their identity; this is not an authentication boundary.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Maximum length, in bytes, of a tenant name.
///
/// Tenant ids are client- or gateway-supplied and then travel through
/// data-plane requests, serve as map keys on the hot path, and appear in bounded
/// telemetry. A fixed ceiling keeps every one of those uses bounded regardless
/// of the caller.
pub const MAX_TENANT_ID_BYTES: usize = 255;

/// Tenant used by requests and services without an explicit identity.
pub const DEFAULT_TENANT: &str = "default";

/// Validated, borrowed tenant identity carried by one request.
/// Copying the context never allocates or touches shared reference counts.
#[derive(Debug, Clone, Copy)]
pub struct TenantContext<'a>(&'a str);

impl<'a> TenantContext<'a> {
    /// Validate a name once without taking ownership of its storage.
    pub fn new(name: &'a str) -> Result<Self, TenantIdError> {
        if name.is_empty() {
            return Err(TenantIdError::Empty);
        }
        if name.len() > MAX_TENANT_ID_BYTES {
            return Err(TenantIdError::TooLong(name.len()));
        }
        if name == "."
            || name == ".."
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(TenantIdError::UnsafePath);
        }
        Ok(Self(name))
    }

    /// The validated effective tenant name.
    pub fn name(self) -> &'a str {
        self.0
    }
}

impl Default for TenantContext<'_> {
    fn default() -> Self {
        Self(DEFAULT_TENANT)
    }
}

/// The resource owner a request is attributed to for QoS.
///
/// See the [module documentation](self) for how identity is established and why
/// the unattributed tenant exists. `TenantId` is `Hash + Eq`, so it is used
/// directly as a map key; two named tenants are equal exactly when they share a
/// name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TenantId {
    /// A named tenant, declared by the SDK on the direct path or resolved from
    /// the service configuration at the gateway.
    Named(String),
    /// Traffic that declared no tenant.
    Unattributed,
}

impl Default for TenantId {
    fn default() -> Self {
        Self::named(DEFAULT_TENANT)
    }
}

impl TenantId {
    /// Canonical namespace name, including the legacy unspecified identity.
    pub fn effective_name(&self) -> &str {
        self.name().unwrap_or(DEFAULT_TENANT)
    }

    /// Normalize the legacy wire identity before storage or policy lookup.
    pub fn normalized(&self) -> Self {
        Self::named(self.effective_name())
    }

    /// Whether this identity selects the default namespace.
    pub fn is_default(&self) -> bool {
        self.effective_name() == DEFAULT_TENANT
    }

    /// Human-readable label for the [`TenantId::Unattributed`] tenant in logs,
    /// the management API, and (allow-listed) telemetry.
    pub const UNATTRIBUTED_LABEL: &'static str = "unattributed";

    /// Construct a named tenant.
    ///
    /// The name is taken verbatim; call [`TenantId::validate`] at the trust
    /// boundary to reject an empty or over-long value.
    pub fn named(name: impl Into<String>) -> Self {
        Self::Named(name.into())
    }

    /// The reserved tenant for traffic that declared no identity.
    pub const fn unattributed() -> Self {
        Self::Unattributed
    }

    /// Whether this is the reserved [`TenantId::Unattributed`] tenant.
    pub fn is_unattributed(&self) -> bool {
        matches!(self, Self::Unattributed)
    }

    /// The name of a named tenant, or `None` for the unattributed tenant.
    ///
    /// Use `effective_name` for storage and policy lookup so legacy unspecified
    /// identities share the default namespace.
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Named(name) => Some(name),
            Self::Unattributed => None,
        }
    }

    /// Validate a tenant established at a trust boundary.
    ///
    /// A named tenant must carry a non-empty name no longer than
    /// [`MAX_TENANT_ID_BYTES`]. The unattributed tenant is always valid.
    pub fn validate(&self) -> Result<(), TenantIdError> {
        self.context().map(|_| ())
    }

    /// Borrow and validate the effective name without copying it.
    pub fn context(&self) -> Result<TenantContext<'_>, TenantIdError> {
        TenantContext::new(self.effective_name())
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(name) => f.write_str(name),
            Self::Unattributed => f.write_str(Self::UNATTRIBUTED_LABEL),
        }
    }
}

/// Why a tenant identifier is rejected at a trust boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TenantIdError {
    /// A named tenant carried an empty name.
    #[error("tenant name is empty")]
    Empty,
    /// A named tenant exceeded the maximum tenant id length.
    #[error("tenant name of {0} bytes exceeds the maximum tenant id length")]
    TooLong(usize),
    /// Tenant ids must be single, portable filesystem components.
    #[error("tenant name must contain only ASCII letters, digits, '.', '_' or '-', and cannot be '.' or '..'")]
    UnsafePath,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_carries_its_name() {
        let tenant = TenantId::named("acme");
        assert_eq!(tenant.name(), Some("acme"));
        assert!(!tenant.is_unattributed());
        assert_eq!(tenant.to_string(), "acme");
    }

    #[test]
    fn unattributed_has_no_name() {
        let tenant = TenantId::unattributed();
        assert_eq!(tenant.name(), None);
        assert!(tenant.is_unattributed());
        assert_eq!(tenant.to_string(), "unattributed");
    }

    #[test]
    fn equality_is_by_name_and_never_conflates_unattributed() {
        assert_eq!(TenantId::named("a"), TenantId::named("a"));
        assert_ne!(TenantId::named("a"), TenantId::named("b"));
        assert_ne!(TenantId::named("a"), TenantId::Unattributed);
        // A tenant literally named like the unattributed label is still a
        // distinct named tenant, not the reserved one.
        let lookalike = TenantId::named(TenantId::UNATTRIBUTED_LABEL);
        assert_ne!(lookalike, TenantId::Unattributed);
        assert!(!lookalike.is_unattributed());
    }

    #[test]
    fn usable_as_a_map_key() {
        use std::collections::HashMap;
        let mut counts: HashMap<TenantId, u64> = HashMap::new();
        *counts.entry(TenantId::named("a")).or_default() += 1;
        *counts.entry(TenantId::named("a")).or_default() += 1;
        *counts.entry(TenantId::unattributed()).or_default() += 1;
        assert_eq!(counts[&TenantId::named("a")], 2);
        assert_eq!(counts[&TenantId::unattributed()], 1);
    }

    #[test]
    fn validate_bounds_named_tenants() {
        assert_eq!(TenantId::unattributed().validate(), Ok(()));
        assert_eq!(TenantId::named("acme").validate(), Ok(()));
        assert_eq!(TenantId::named("").validate(), Err(TenantIdError::Empty));
        let overlong = "x".repeat(MAX_TENANT_ID_BYTES + 1);
        assert_eq!(
            TenantId::named(overlong).validate(),
            Err(TenantIdError::TooLong(MAX_TENANT_ID_BYTES + 1))
        );
        assert_eq!(
            TenantId::named("y".repeat(MAX_TENANT_ID_BYTES)).validate(),
            Ok(())
        );
    }

    #[test]
    fn serde_round_trips_both_variants() {
        for tenant in [TenantId::named("acme"), TenantId::unattributed()] {
            let encoded = serde_json::to_string(&tenant).unwrap();
            let decoded: TenantId = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, tenant);
        }
    }
}
