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
    match &access.target {
        GatewayTarget::Object(object) => hash_parts(&[
            access.provider_account.as_deref().unwrap_or(""),
            object.backend.prefix(),
            &object.bucket,
            &object.object_path,
        ]),
        GatewayTarget::Namespace {
            backend,
            namespace,
            prefix,
        } => hash_parts(&[
            access.provider_account.as_deref().unwrap_or(""),
            backend.prefix(),
            namespace,
            prefix.as_deref().unwrap_or(""),
        ]),
    }
}

fn hash_parts(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
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
}
