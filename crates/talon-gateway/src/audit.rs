//! Bounded, redacted security audit events.

use sha2::{Digest, Sha256};

use crate::{AuthenticatedPrincipal, GatewayAccess, GatewayTarget, ProviderProtocol};

/// One authentication or authorization audit decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityAuditEvent {
    /// Opaque request correlation ID.
    pub request_id: String,
    /// Provider protocol.
    pub protocol: ProviderProtocol,
    /// Hash of the authenticated policy principal, or `anonymous`.
    pub principal: String,
    /// Bounded operation label.
    pub operation: &'static str,
    /// Hash of the provider account and scoped target, when parseable.
    pub resource: Option<String>,
    /// `allow` or `deny`.
    pub decision: &'static str,
    /// Stable bounded reason.
    pub reason: &'static str,
}

impl SecurityAuditEvent {
    pub(crate) fn new(
        request_id: &str,
        protocol: ProviderProtocol,
        principal: Option<&AuthenticatedPrincipal>,
        access: Option<&GatewayAccess>,
        decision: &'static str,
        reason: &'static str,
    ) -> Self {
        Self {
            request_id: request_id.to_string(),
            protocol,
            principal: principal.map_or_else(|| "anonymous".into(), principal_hash),
            operation: access.map_or("unsupported", |access| access.operation.label()),
            resource: access.map(resource_hash),
            decision,
            reason,
        }
    }
}

/// Destination for security audit records.
pub trait GatewayAuditSink: Send + Sync + 'static {
    /// Record one bounded event. Implementations must not add request credentials or paths.
    fn record(&self, event: SecurityAuditEvent);
}

pub(crate) struct TracingAuditSink;

impl GatewayAuditSink for TracingAuditSink {
    fn record(&self, event: SecurityAuditEvent) {
        tracing::info!(
            audit = true,
            request_id = event.request_id,
            protocol = event.protocol.label(),
            principal = event.principal,
            operation = event.operation,
            resource = event.resource.as_deref().unwrap_or("unknown"),
            decision = event.decision,
            reason = event.reason,
            "gateway security audit"
        );
    }
}

fn principal_hash(principal: &AuthenticatedPrincipal) -> String {
    hash_parts(&[&principal.id, &principal.provider_account])
}

fn resource_hash(access: &GatewayAccess) -> String {
    let mut hash = Sha256::new();
    hash_requirement(
        &mut hash,
        access.operation,
        access.provider_account.as_deref(),
        &access.target,
    );
    for requirement in &access.additional {
        hash_requirement(
            &mut hash,
            requirement.operation,
            requirement.provider_account.as_deref(),
            &requirement.target,
        );
    }
    truncate_hash(hash)
}

fn hash_requirement(
    hash: &mut Sha256,
    operation: crate::GatewayOperation,
    provider_account: Option<&str>,
    target: &GatewayTarget,
) {
    update_hash(hash, operation.label());
    update_hash(hash, provider_account.unwrap_or(""));
    match target {
        GatewayTarget::Object(object) => {
            update_hash(hash, object.backend.prefix());
            update_hash(hash, &object.bucket);
            update_hash(hash, &object.object_path);
        }
        GatewayTarget::Namespace {
            backend,
            namespace,
            prefix,
        } => {
            update_hash(hash, backend.prefix());
            update_hash(hash, namespace);
            update_hash(hash, prefix.as_deref().unwrap_or(""));
        }
        GatewayTarget::NamespaceBodyObjects { backend, namespace } => {
            update_hash(hash, backend.prefix());
            update_hash(hash, namespace);
            update_hash(hash, "*");
        }
    }
}

fn hash_parts(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        update_hash(&mut hash, part);
    }
    truncate_hash(hash)
}

fn update_hash(hash: &mut Sha256, part: &str) {
    hash.update((part.len() as u64).to_be_bytes());
    hash.update(part.as_bytes());
}

fn truncate_hash(hash: Sha256) -> String {
    let digest = hash.finalize();
    let mut output = String::with_capacity(32);
    for byte in &digest[..16] {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use talon_core::{Backend, ObjectId};

    use super::*;
    use crate::GatewayOperation;

    #[test]
    fn audit_identifiers_are_bounded_and_do_not_expose_inputs() {
        let principal = AuthenticatedPrincipal::new("tenant-secret-name", "account-secret");
        let access = GatewayAccess {
            operation: GatewayOperation::Read,
            provider_account: Some("account-secret".into()),
            target: GatewayTarget::Object(ObjectId::new(
                Backend::S3,
                "private-bucket",
                "customer/path.txt",
            )),
            additional: Vec::new(),
        };
        let event = SecurityAuditEvent::new(
            "request-id",
            ProviderProtocol::S3,
            Some(&principal),
            Some(&access),
            "allow",
            "policy_allowed",
        );
        assert_eq!(event.principal.len(), 32);
        assert_eq!(event.resource.as_ref().unwrap().len(), 32);
        let rendered = format!("{event:?}");
        for secret in [
            "tenant-secret-name",
            "account-secret",
            "private-bucket",
            "customer/path",
        ] {
            assert!(!rendered.contains(secret));
        }
    }

    #[test]
    fn copy_source_changes_resource_hash_without_being_exposed() {
        let mut first = GatewayAccess {
            operation: GatewayOperation::Write,
            provider_account: Some("account-secret".into()),
            target: GatewayTarget::Object(ObjectId::new(Backend::S3, "dst", "object")),
            additional: vec![crate::GatewayAccessRequirement {
                operation: GatewayOperation::Read,
                provider_account: Some("account-secret".into()),
                target: GatewayTarget::Object(ObjectId::new(Backend::S3, "src", "first")),
            }],
        };
        let first_hash = resource_hash(&first);
        first.additional[0].target =
            GatewayTarget::Object(ObjectId::new(Backend::S3, "src", "second-secret"));
        let second_hash = resource_hash(&first);
        assert_ne!(first_hash, second_hash);
        assert_eq!(first_hash.len(), 32);
        assert_eq!(second_hash.len(), 32);
    }
}
