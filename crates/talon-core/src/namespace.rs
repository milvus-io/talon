//! Cache namespace identity shared by storage, policy and quality of service.
//!
//! Unspecified identities resolve to `default`. `Unattributed` is retained only
//! for decoding older clients and is normalized at request boundaries. Direct
//! clients declare their namespace; this is not an authentication boundary.
//! A namespace may group workloads, datasets or tenants independently of
//! [`crate::ObjectNamespace`], which describes source-object authorization ranges.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Maximum length, in bytes, of a namespace name.
///
/// Namespace ids are client- or gateway-supplied and then travel through
/// data-plane requests, serve as map keys on the hot path, and appear in bounded
/// telemetry. A fixed ceiling keeps every one of those uses bounded regardless
/// of the caller.
pub const MAX_NAMESPACE_ID_BYTES: usize = 255;

/// Namespace used by requests and services without an explicit identity.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Validated, borrowed namespace identity carried by one request.
/// Copying the context never allocates or touches shared reference counts.
#[derive(Debug, Clone, Copy)]
pub struct NamespaceContext<'a>(&'a str);

impl<'a> NamespaceContext<'a> {
    /// Validate a name once without taking ownership of its storage.
    pub fn new(name: &'a str) -> Result<Self, NamespaceIdError> {
        if name.is_empty() {
            return Err(NamespaceIdError::Empty);
        }
        if name.len() > MAX_NAMESPACE_ID_BYTES {
            return Err(NamespaceIdError::TooLong(name.len()));
        }
        if matches!(name, "worker_identity" | ".worker.lock") {
            return Err(NamespaceIdError::Reserved);
        }
        if name == "."
            || name == ".."
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(NamespaceIdError::UnsafePath);
        }
        Ok(Self(name))
    }

    /// The validated effective namespace name.
    pub fn name(self) -> &'a str {
        self.0
    }
}

impl Default for NamespaceContext<'_> {
    fn default() -> Self {
        Self(DEFAULT_NAMESPACE)
    }
}

/// The cache state and policy scope selected by a request.
///
/// See the [module documentation](self) for how identity is established and why
/// the unattributed namespace exists. `NamespaceId` is `Hash + Eq`, so it is used
/// directly as a map key; two named namespaces are equal exactly when they share a
/// name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NamespaceId {
    /// A named namespace, declared by the SDK on the direct path or resolved from
    /// the service configuration at the gateway.
    Named(String),
    /// Traffic that declared no namespace.
    Unattributed,
}

impl Default for NamespaceId {
    fn default() -> Self {
        Self::named(DEFAULT_NAMESPACE)
    }
}

impl NamespaceId {
    /// Canonical namespace name, including the legacy unspecified identity.
    pub fn effective_name(&self) -> &str {
        self.name().unwrap_or(DEFAULT_NAMESPACE)
    }

    /// Normalize the legacy wire identity before storage or policy lookup.
    pub fn normalized(&self) -> Self {
        Self::named(self.effective_name())
    }

    /// Whether this identity selects the default namespace.
    pub fn is_default(&self) -> bool {
        self.effective_name() == DEFAULT_NAMESPACE
    }

    /// Human-readable label for the [`NamespaceId::Unattributed`] namespace in logs,
    /// the management API, and (allow-listed) telemetry.
    pub const UNATTRIBUTED_LABEL: &'static str = "unattributed";

    /// Construct a named namespace.
    ///
    /// The name is taken verbatim; call [`NamespaceId::validate`] at the trust
    /// boundary to reject an empty or over-long value.
    pub fn named(name: impl Into<String>) -> Self {
        Self::Named(name.into())
    }

    /// The reserved namespace for traffic that declared no identity.
    pub const fn unattributed() -> Self {
        Self::Unattributed
    }

    /// Whether this is the reserved [`NamespaceId::Unattributed`] namespace.
    pub fn is_unattributed(&self) -> bool {
        matches!(self, Self::Unattributed)
    }

    /// The name of a named namespace, or `None` for the unattributed namespace.
    ///
    /// Use `effective_name` for storage and policy lookup so legacy unspecified
    /// identities share the default namespace.
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Named(name) => Some(name),
            Self::Unattributed => None,
        }
    }

    /// Validate a namespace established at a trust boundary.
    ///
    /// A named namespace must carry a non-empty name no longer than
    /// [`MAX_NAMESPACE_ID_BYTES`]. The unattributed namespace is always valid.
    pub fn validate(&self) -> Result<(), NamespaceIdError> {
        self.context().map(|_| ())
    }

    /// Borrow and validate the effective name without copying it.
    pub fn context(&self) -> Result<NamespaceContext<'_>, NamespaceIdError> {
        NamespaceContext::new(self.effective_name())
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(name) => f.write_str(name),
            Self::Unattributed => f.write_str(Self::UNATTRIBUTED_LABEL),
        }
    }
}

/// Why a namespace identifier is rejected at a trust boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NamespaceIdError {
    /// Process metadata already occupies these names in the cache root.
    #[error("namespace name is reserved for worker metadata")]
    Reserved,
    /// A named namespace carried an empty name.
    #[error("namespace name is empty")]
    Empty,
    /// A named namespace exceeded the maximum namespace id length.
    #[error("namespace name of {0} bytes exceeds the maximum namespace id length")]
    TooLong(usize),
    /// Namespace ids must be single, portable filesystem components.
    #[error("namespace name must contain only ASCII letters, digits, '.', '_' or '-', and cannot be '.' or '..'")]
    UnsafePath,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_carries_its_name() {
        let namespace = NamespaceId::named("acme");
        assert_eq!(namespace.name(), Some("acme"));
        assert!(!namespace.is_unattributed());
        assert_eq!(namespace.to_string(), "acme");
    }

    #[test]
    fn unattributed_has_no_name() {
        let namespace = NamespaceId::unattributed();
        assert_eq!(namespace.name(), None);
        assert!(namespace.is_unattributed());
        assert_eq!(namespace.to_string(), "unattributed");
    }

    #[test]
    fn equality_is_by_name_and_never_conflates_unattributed() {
        assert_eq!(NamespaceId::named("a"), NamespaceId::named("a"));
        assert_ne!(NamespaceId::named("a"), NamespaceId::named("b"));
        assert_ne!(NamespaceId::named("a"), NamespaceId::Unattributed);
        // A namespace literally named like the unattributed label is still a
        // distinct named namespace, not the reserved one.
        let lookalike = NamespaceId::named(NamespaceId::UNATTRIBUTED_LABEL);
        assert_ne!(lookalike, NamespaceId::Unattributed);
        assert!(!lookalike.is_unattributed());
    }

    #[test]
    fn usable_as_a_map_key() {
        use std::collections::HashMap;
        let mut counts: HashMap<NamespaceId, u64> = HashMap::new();
        *counts.entry(NamespaceId::named("a")).or_default() += 1;
        *counts.entry(NamespaceId::named("a")).or_default() += 1;
        *counts.entry(NamespaceId::unattributed()).or_default() += 1;
        assert_eq!(counts[&NamespaceId::named("a")], 2);
        assert_eq!(counts[&NamespaceId::unattributed()], 1);
    }

    #[test]
    fn reserved_namespaces_are_rejected_by_borrowed_and_owned_contexts() {
        for name in ["worker_identity", ".worker.lock"] {
            assert!(matches!(
                NamespaceContext::new(name),
                Err(NamespaceIdError::Reserved)
            ));
            assert_eq!(
                NamespaceId::named(name).validate(),
                Err(NamespaceIdError::Reserved)
            );
        }
        assert!(NamespaceContext::new("worker_identity_2").is_ok());
    }

    #[test]
    fn validate_bounds_named_namespaces() {
        assert_eq!(NamespaceId::unattributed().validate(), Ok(()));
        assert_eq!(NamespaceId::named("acme").validate(), Ok(()));
        assert_eq!(
            NamespaceId::named("").validate(),
            Err(NamespaceIdError::Empty)
        );
        let overlong = "x".repeat(MAX_NAMESPACE_ID_BYTES + 1);
        assert_eq!(
            NamespaceId::named(overlong).validate(),
            Err(NamespaceIdError::TooLong(MAX_NAMESPACE_ID_BYTES + 1))
        );
        assert_eq!(
            NamespaceId::named("y".repeat(MAX_NAMESPACE_ID_BYTES)).validate(),
            Ok(())
        );
    }

    #[test]
    fn serde_round_trips_both_variants() {
        for namespace in [NamespaceId::named("acme"), NamespaceId::unattributed()] {
            let encoded = serde_json::to_string(&namespace).unwrap();
            let decoded: NamespaceId = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, namespace);
        }
    }
}
