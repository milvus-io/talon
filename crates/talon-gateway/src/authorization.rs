//! Default-deny principal and object namespace authorization.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use axum::extract::Request;
use talon_core::Backend;

use crate::{GatewayAccess, GatewayOperation, GatewayTarget, ProviderProtocol};

/// Identity established by a trusted provider authenticator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    /// Stable policy identity. This value is never emitted in metrics or audit logs.
    pub id: String,
    /// Provider tenant or storage account established from the credential.
    pub provider_account: String,
}

impl AuthenticatedPrincipal {
    /// Construct a trusted principal for insertion into request extensions.
    pub fn new(id: impl Into<String>, provider_account: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            provider_account: provider_account.into(),
        }
    }
}

/// Trusted provider credential verifier installed before authorization.
pub trait GatewayAuthenticator: Send + Sync + 'static {
    /// Validate one request and return its policy identity.
    fn authenticate(
        &self,
        request: &Request,
        protocol: ProviderProtocol,
    ) -> Result<AuthenticatedPrincipal, GatewayAuthenticationError>;
}

/// Stable authentication failure that never contains credential material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("request authentication failed")]
pub struct GatewayAuthenticationError;

/// One allow-only policy grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationGrant {
    /// Unique operator-facing identifier used only for configuration errors.
    pub id: String,
    /// Authenticated principal identity.
    pub principal: String,
    /// Object-store protocol.
    pub protocol: ProviderProtocol,
    /// Provider tenant or storage account.
    pub provider_account: String,
    /// Exact bucket or container.
    pub namespace: String,
    /// Optional literal object or listing prefix.
    pub prefix: Option<String>,
    /// Allowed operations.
    pub operations: Vec<GatewayOperation>,
}

#[derive(Debug)]
struct CompiledPolicy {
    grants: Vec<AuthorizationGrant>,
}

/// Atomically reloadable authorization policy. Invalid reloads retain the last good policy.
#[derive(Debug, Clone)]
pub struct AuthorizationPolicy {
    current: Arc<RwLock<Arc<CompiledPolicy>>>,
}

impl AuthorizationPolicy {
    /// Compile an initial policy. An empty policy is valid and denies every request.
    pub fn new(grants: Vec<AuthorizationGrant>) -> Result<Self, AuthorizationPolicyError> {
        let compiled = Arc::new(compile(grants)?);
        Ok(Self {
            current: Arc::new(RwLock::new(compiled)),
        })
    }

    /// Compile and atomically publish a replacement policy.
    pub fn reload(&self, grants: Vec<AuthorizationGrant>) -> Result<(), AuthorizationPolicyError> {
        let compiled = Arc::new(compile(grants)?);
        *self.current.write().unwrap() = compiled;
        Ok(())
    }

    pub(crate) fn allows(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol: ProviderProtocol,
        access: &GatewayAccess,
    ) -> bool {
        if access
            .provider_account
            .as_deref()
            .is_some_and(|account| account != principal.provider_account)
        {
            return false;
        }
        let (backend, namespace, requested_prefix) = target_parts(&access.target);
        if backend != protocol_backend(protocol) {
            return false;
        }
        let current = self.current.read().unwrap().clone();
        current.grants.iter().any(|grant| {
            grant.principal == principal.id
                && grant.protocol == protocol
                && grant.provider_account == principal.provider_account
                && grant.namespace == namespace
                && grant.operations.contains(&access.operation)
                && prefix_contains(grant.prefix.as_deref(), requested_prefix)
        })
    }
}

fn target_parts(target: &GatewayTarget) -> (Backend, &str, Option<&str>) {
    match target {
        GatewayTarget::Object(object) => (
            object.backend,
            object.bucket.as_str(),
            Some(object.object_path.as_str()),
        ),
        GatewayTarget::Namespace {
            backend,
            namespace,
            prefix,
        } => (*backend, namespace.as_str(), prefix.as_deref()),
    }
}

fn protocol_backend(protocol: ProviderProtocol) -> Backend {
    match protocol {
        ProviderProtocol::S3 => Backend::S3,
        ProviderProtocol::Azure => Backend::Azure,
    }
}

fn prefix_contains(grant: Option<&str>, requested: Option<&str>) -> bool {
    match (grant, requested) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(grant), Some(requested)) => requested.starts_with(grant),
    }
}

fn compile(grants: Vec<AuthorizationGrant>) -> Result<CompiledPolicy, AuthorizationPolicyError> {
    let mut ids = HashSet::new();
    let mut selectors = HashSet::new();
    for grant in &grants {
        if grant.id.is_empty()
            || grant.principal.is_empty()
            || grant.provider_account.is_empty()
            || grant.namespace.is_empty()
            || grant.operations.is_empty()
        {
            return Err(AuthorizationPolicyError::InvalidGrant(grant.id.clone()));
        }
        if !ids.insert(grant.id.clone()) {
            return Err(AuthorizationPolicyError::DuplicateGrantId(grant.id.clone()));
        }
        if grant.prefix.as_deref().is_some_and(invalid_prefix) {
            return Err(AuthorizationPolicyError::InvalidGrant(grant.id.clone()));
        }
        let mut operations = HashSet::new();
        for operation in &grant.operations {
            if *operation == GatewayOperation::Unsupported || !operations.insert(*operation) {
                return Err(AuthorizationPolicyError::InvalidGrant(grant.id.clone()));
            }
            let selector = (
                grant.principal.clone(),
                grant.protocol,
                grant.provider_account.clone(),
                grant.namespace.clone(),
                grant.prefix.clone(),
                *operation,
            );
            if !selectors.insert(selector) {
                return Err(AuthorizationPolicyError::AmbiguousGrant(grant.id.clone()));
            }
        }
    }
    Ok(CompiledPolicy { grants })
}

fn invalid_prefix(prefix: &str) -> bool {
    prefix.is_empty()
}

/// Policy compilation error. Resource values are deliberately absent from display text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationPolicyError {
    /// A grant is empty, unsupported, duplicated internally, or has an unsafe prefix.
    #[error("authorization grant {0:?} is invalid")]
    InvalidGrant(String),
    /// Grant identifiers must be unique.
    #[error("authorization grant id {0:?} is duplicated")]
    DuplicateGrantId(String),
    /// Two grants contain the same selector and operation.
    #[error("authorization grant {0:?} is ambiguous")]
    AmbiguousGrant(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::ObjectId;

    fn grant(prefix: Option<&str>) -> AuthorizationGrant {
        AuthorizationGrant {
            id: "reader".into(),
            principal: "principal-a".into(),
            protocol: ProviderProtocol::S3,
            provider_account: "account-a".into(),
            namespace: "bucket-a".into(),
            prefix: prefix.map(str::to_string),
            operations: vec![GatewayOperation::Read, GatewayOperation::List],
        }
    }

    fn access(namespace: &str, key: &str, operation: GatewayOperation) -> GatewayAccess {
        GatewayAccess {
            operation,
            provider_account: None,
            target: GatewayTarget::Object(ObjectId::new(Backend::S3, namespace, key)),
        }
    }

    fn list_access(prefix: Option<&str>) -> GatewayAccess {
        GatewayAccess {
            operation: GatewayOperation::List,
            provider_account: None,
            target: GatewayTarget::Namespace {
                backend: Backend::S3,
                namespace: "bucket-a".into(),
                prefix: prefix.map(str::to_string),
            },
        }
    }

    #[test]
    fn policy_is_default_deny_and_tenant_scoped() {
        let policy = AuthorizationPolicy::new(vec![grant(Some("tenant/"))]).unwrap();
        let principal = AuthenticatedPrincipal::new("principal-a", "account-a");
        assert!(policy.allows(
            &principal,
            ProviderProtocol::S3,
            &access("bucket-a", "tenant/object", GatewayOperation::Read)
        ));
        assert!(!policy.allows(
            &principal,
            ProviderProtocol::S3,
            &access("bucket-b", "tenant/object", GatewayOperation::Read)
        ));
        assert!(!policy.allows(
            &AuthenticatedPrincipal::new("principal-b", "account-a"),
            ProviderProtocol::S3,
            &access("bucket-a", "tenant/object", GatewayOperation::Read)
        ));
        assert!(!AuthorizationPolicy::new(Vec::new()).unwrap().allows(
            &principal,
            ProviderProtocol::S3,
            &access("bucket-a", "tenant/object", GatewayOperation::Read)
        ));
    }

    #[test]
    fn prefix_and_operation_boundaries_are_exact() {
        let policy = AuthorizationPolicy::new(vec![grant(Some("tenant/"))]).unwrap();
        let principal = AuthenticatedPrincipal::new("principal-a", "account-a");
        assert!(!policy.allows(
            &principal,
            ProviderProtocol::S3,
            &access("bucket-a", "tenant-other", GatewayOperation::Read)
        ));
        assert!(!policy.allows(
            &principal,
            ProviderProtocol::S3,
            &access("bucket-a", "tenant/object", GatewayOperation::Delete)
        ));
        assert!(policy.allows(
            &principal,
            ProviderProtocol::S3,
            &list_access(Some("tenant/nested"))
        ));
        assert!(!policy.allows(&principal, ProviderProtocol::S3, &list_access(None)));
    }

    #[test]
    fn invalid_reload_retains_last_good_policy() {
        let policy = AuthorizationPolicy::new(vec![grant(None)]).unwrap();
        let principal = AuthenticatedPrincipal::new("principal-a", "account-a");
        let access = access("bucket-a", "object", GatewayOperation::Read);
        let mut invalid = grant(None);
        invalid.operations.clear();
        assert!(policy.reload(vec![invalid]).is_err());
        assert!(policy.allows(&principal, ProviderProtocol::S3, &access));

        let mut replacement = grant(None);
        replacement.principal = "principal-b".into();
        policy.reload(vec![replacement]).unwrap();
        assert!(!policy.allows(&principal, ProviderProtocol::S3, &access));
    }

    #[test]
    fn configured_provider_account_must_match_authenticated_account() {
        let policy = AuthorizationPolicy::new(vec![grant(None)]).unwrap();
        let mut access = access("bucket-a", "object", GatewayOperation::Read);
        access.provider_account = Some("account-b".into());
        assert!(!policy.allows(
            &AuthenticatedPrincipal::new("principal-a", "account-a"),
            ProviderProtocol::S3,
            &access
        ));
    }

    #[test]
    fn duplicate_ids_and_selectors_are_rejected() {
        let first = grant(None);
        let mut duplicate_id = grant(Some("other/"));
        assert!(matches!(
            AuthorizationPolicy::new(vec![first.clone(), duplicate_id.clone()]),
            Err(AuthorizationPolicyError::DuplicateGrantId(_))
        ));

        duplicate_id.id = "second".into();
        duplicate_id.prefix = first.prefix.clone();
        assert!(matches!(
            AuthorizationPolicy::new(vec![first, duplicate_id]),
            Err(AuthorizationPolicyError::AmbiguousGrant(_))
        ));
    }
}
