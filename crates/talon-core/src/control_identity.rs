//! Workload identity and TLS configuration for the privileged control plane.
//!
//! ADR 0004 keeps PEM contents out of configuration structs and binds each
//! coordinator/worker connection to one canonical URI SAN. This module defines
//! that shared model without opening sockets or changing runtime traffic.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::Deserialize;
use url::Url;

use crate::{Error, Result};

const ID_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Role encoded in a Talon workload URI SAN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadRole {
    /// Active coordinator with management-tier authority.
    Coordinator,
    /// Cache worker with object-store access.
    Worker,
}

impl WorkloadRole {
    /// Canonical URI path spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Coordinator => "coordinator",
            Self::Worker => "worker",
        }
    }
}

impl fmt::Display for WorkloadRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WorkloadRole {
    type Err = WorkloadIdentityError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "coordinator" => Ok(Self::Coordinator),
            "worker" => Ok(Self::Worker),
            _ => Err(WorkloadIdentityError::InvalidRole(value.to_owned())),
        }
    }
}

/// Authenticated identity encoded in one canonical workload URI SAN.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkloadIdentity {
    trust_domain: String,
    cluster_id: String,
    role: WorkloadRole,
    node_id: String,
}

impl WorkloadIdentity {
    /// Construct and validate an identity.
    pub fn new(
        trust_domain: impl Into<String>,
        cluster_id: impl Into<String>,
        role: WorkloadRole,
        node_id: impl Into<String>,
    ) -> std::result::Result<Self, WorkloadIdentityError> {
        let trust_domain = trust_domain.into();
        validate_trust_domain(&trust_domain)?;
        let cluster_id = cluster_id.into();
        let node_id = node_id.into();
        if cluster_id.is_empty() {
            return Err(WorkloadIdentityError::EmptyComponent("cluster_id"));
        }
        if node_id.is_empty() {
            return Err(WorkloadIdentityError::EmptyComponent("node_id"));
        }
        Ok(Self {
            trust_domain,
            cluster_id,
            role,
            node_id,
        })
    }

    /// Parse a URI and reject every non-canonical spelling.
    pub fn parse(uri: &str) -> std::result::Result<Self, WorkloadIdentityError> {
        let parsed = Url::parse(uri).map_err(|error| WorkloadIdentityError::InvalidUri {
            detail: error.to_string(),
        })?;
        if parsed.scheme() != "spiffe" {
            return Err(WorkloadIdentityError::WrongScheme(
                parsed.scheme().to_owned(),
            ));
        }
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(WorkloadIdentityError::InvalidUri {
                detail: "userinfo, ports, query strings, and fragments are forbidden".into(),
            });
        }
        let trust_domain = parsed
            .host_str()
            .ok_or_else(|| WorkloadIdentityError::InvalidTrustDomain("missing host".into()))?;
        validate_trust_domain(trust_domain)?;

        let raw: Vec<_> = parsed.path().split('/').collect();
        if raw.len() != 5 || !raw[0].is_empty() || raw[1] != "talon" {
            return Err(WorkloadIdentityError::InvalidPath);
        }
        let cluster_id = decode_component(raw[2], "cluster_id")?;
        let role = raw[3].parse()?;
        let node_id = decode_component(raw[4], "node_id")?;
        let identity = Self::new(trust_domain, cluster_id, role, node_id)?;
        let canonical = identity.to_uri();
        if uri != canonical {
            return Err(WorkloadIdentityError::NonCanonical { canonical });
        }
        Ok(identity)
    }

    /// Canonical SPIFFE-compatible URI SAN value.
    pub fn to_uri(&self) -> String {
        format!(
            "spiffe://{}/talon/{}/{}/{}",
            self.trust_domain,
            encode_component(&self.cluster_id),
            self.role,
            encode_component(&self.node_id)
        )
    }

    /// Configured trust domain.
    pub fn trust_domain(&self) -> &str {
        &self.trust_domain
    }

    /// Logical Talon cluster.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Workload role.
    pub fn role(&self) -> WorkloadRole {
        self.role
    }

    /// Stable node identity.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl fmt::Display for WorkloadIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_uri())
    }
}

impl FromStr for WorkloadIdentity {
    type Err = WorkloadIdentityError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Why a workload URI SAN was rejected.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WorkloadIdentityError {
    /// URI parsing failed.
    #[error("invalid workload identity URI: {detail}")]
    InvalidUri { detail: String },
    /// Only the SPIFFE scheme is accepted.
    #[error("workload identity uses scheme {0:?}, expected \"spiffe\"")]
    WrongScheme(String),
    /// Trust domain is not one canonical DNS name.
    #[error("invalid workload identity trust domain: {0}")]
    InvalidTrustDomain(String),
    /// URI path does not have the fixed Talon shape.
    #[error("workload identity path must be /talon/<cluster>/<role>/<node>")]
    InvalidPath,
    /// Role is unknown.
    #[error("invalid workload role {0:?}")]
    InvalidRole(String),
    /// Required identity component is empty.
    #[error("workload identity {0} must not be empty")]
    EmptyComponent(&'static str),
    /// Percent decoding did not produce UTF-8.
    #[error("workload identity {component} is not valid UTF-8")]
    InvalidUtf8 { component: &'static str },
    /// URI is valid but has an ambiguous or non-canonical spelling.
    #[error("non-canonical workload identity; expected {canonical}")]
    NonCanonical { canonical: String },
}

/// File-backed TLS material and trust domain for a control endpoint.
///
/// This stores paths only. PEM certificate and private-key bytes are loaded by
/// the future listener implementation and must use a redacting wrapper there.
#[derive(Clone, PartialEq, Eq)]
pub struct ControlTlsConfig {
    /// CA bundle used to validate the peer certificate.
    pub ca_cert_path: PathBuf,
    /// This workload's leaf certificate chain.
    pub cert_path: PathBuf,
    /// This workload's private key file.
    pub key_path: PathBuf,
    /// Expected SPIFFE trust domain.
    pub trust_domain: String,
}

impl fmt::Debug for ControlTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlTlsConfig")
            .field("ca_cert_path", &self.ca_cert_path)
            .field("cert_path", &self.cert_path)
            .field("key_path", &"<redacted>")
            .field("trust_domain", &self.trust_domain)
            .finish()
    }
}

impl ControlTlsConfig {
    /// Validate paths and the canonical trust domain.
    pub fn validate(&self) -> Result<()> {
        for (name, path) in [
            ("ca_cert_path", &self.ca_cert_path),
            ("cert_path", &self.cert_path),
            ("key_path", &self.key_path),
        ] {
            if path.as_os_str().is_empty() {
                return Err(Error::Other(format!(
                    "control_tls.{name} must not be empty"
                )));
            }
        }
        validate_trust_domain(&self.trust_domain)
            .map_err(|error| Error::Other(format!("control_tls.trust_domain is invalid: {error}")))
    }
}

/// Optional fields contributed by one configuration layer.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlTlsConfigPatch {
    /// Override for [`ControlTlsConfig::ca_cert_path`].
    pub ca_cert_path: Option<PathBuf>,
    /// Override for [`ControlTlsConfig::cert_path`].
    pub cert_path: Option<PathBuf>,
    /// Override for [`ControlTlsConfig::key_path`].
    pub key_path: Option<PathBuf>,
    /// Override for [`ControlTlsConfig::trust_domain`].
    pub trust_domain: Option<String>,
}

impl fmt::Debug for ControlTlsConfigPatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlTlsConfigPatch")
            .field("ca_cert_path", &self.ca_cert_path)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path.as_ref().map(|_| "<redacted>"))
            .field("trust_domain", &self.trust_domain)
            .finish()
    }
}

impl ControlTlsConfigPatch {
    /// Whether this layer contributes no TLS setting.
    pub fn is_empty(&self) -> bool {
        self.ca_cert_path.is_none()
            && self.cert_path.is_none()
            && self.key_path.is_none()
            && self.trust_domain.is_none()
    }

    /// Overlay this higher-precedence layer onto `base`.
    pub fn merge(self, base: Self) -> Self {
        Self {
            ca_cert_path: self.ca_cert_path.or(base.ca_cert_path),
            cert_path: self.cert_path.or(base.cert_path),
            key_path: self.key_path.or(base.key_path),
            trust_domain: self.trust_domain.or(base.trust_domain),
        }
    }

    /// Resolve an optional block, requiring all fields when any is present.
    pub fn resolve(self) -> Result<Option<ControlTlsConfig>> {
        if self.is_empty() {
            return Ok(None);
        }
        let missing = |name: &str| {
            Error::Other(format!(
                "control_tls.{name} is required when control-plane TLS is configured"
            ))
        };
        let config = ControlTlsConfig {
            ca_cert_path: self.ca_cert_path.ok_or_else(|| missing("ca_cert_path"))?,
            cert_path: self.cert_path.ok_or_else(|| missing("cert_path"))?,
            key_path: self.key_path.ok_or_else(|| missing("key_path"))?,
            trust_domain: self.trust_domain.ok_or_else(|| missing("trust_domain"))?,
        };
        config.validate()?;
        Ok(Some(config))
    }
}

fn validate_trust_domain(value: &str) -> std::result::Result<(), WorkloadIdentityError> {
    if value.is_empty() {
        return Err(WorkloadIdentityError::InvalidTrustDomain("empty".into()));
    }
    if value != value.to_ascii_lowercase()
        || !value.is_ascii()
        || value.len() > 253
        || value.parse::<std::net::IpAddr>().is_ok()
        || value.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                || !label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                || !label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
    {
        return Err(WorkloadIdentityError::InvalidTrustDomain(
            "must be one canonical lowercase ASCII DNS name, not an IP address".into(),
        ));
    }
    let parsed = Url::parse(&format!("spiffe://{value}/"))
        .map_err(|error| WorkloadIdentityError::InvalidTrustDomain(error.to_string()))?;
    if parsed.host_str() != Some(value)
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.as_str() != format!("spiffe://{value}/")
    {
        return Err(WorkloadIdentityError::InvalidTrustDomain(
            "must be one canonical lowercase DNS name without a port".into(),
        ));
    }
    Ok(())
}

fn encode_component(value: &str) -> String {
    utf8_percent_encode(value, ID_ENCODE_SET).to_string()
}

fn decode_component(
    value: &str,
    component: &'static str,
) -> std::result::Result<String, WorkloadIdentityError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| WorkloadIdentityError::InvalidUtf8 { component })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_identity_round_trips_reserved_and_unicode_ids() {
        let identity = WorkloadIdentity::new(
            "prod.example.com",
            "cluster/east",
            WorkloadRole::Worker,
            "worker 1/东京",
        )
        .unwrap();
        let uri =
            "spiffe://prod.example.com/talon/cluster%2Feast/worker/worker%201%2F%E4%B8%9C%E4%BA%AC";
        assert_eq!(identity.to_uri(), uri);
        assert_eq!(WorkloadIdentity::parse(uri).unwrap(), identity);
    }

    #[test]
    fn noncanonical_identity_spellings_are_rejected() {
        for uri in [
            "SPIFFE://prod.example.com/talon/c/worker/n",
            "spiffe://PROD.example.com/talon/c/worker/n",
            "spiffe://prod.example.com/talon/%63/worker/n",
            "spiffe://prod.example.com/talon/c/worker/a%2fb",
            "spiffe://prod.example.com/talon/c/worker/n?admin=true",
        ] {
            assert!(
                WorkloadIdentity::parse(uri).is_err(),
                "{uri} must not acquire an identity"
            );
        }
    }

    #[test]
    fn wrong_shape_role_and_trust_domain_are_rejected() {
        for uri in [
            "https://prod.example.com/talon/c/worker/n",
            "spiffe://prod.example.com/talon/c/client/n",
            "spiffe://prod.example.com/other/c/worker/n",
            "spiffe://127.0.0.1/talon/c/worker/n",
            "spiffe://prod.example.com:443/talon/c/worker/n",
        ] {
            assert!(WorkloadIdentity::parse(uri).is_err());
        }
    }

    #[test]
    fn tls_patch_merges_layers_and_requires_a_complete_block() {
        let file = ControlTlsConfigPatch {
            ca_cert_path: Some("/tls/ca.pem".into()),
            cert_path: Some("/tls/old-cert.pem".into()),
            key_path: Some("/tls/key.pem".into()),
            trust_domain: Some("prod.example.com".into()),
        };
        let env = ControlTlsConfigPatch {
            cert_path: Some("/tls/cert.pem".into()),
            ..Default::default()
        };
        let resolved = env.merge(file).resolve().unwrap().unwrap();
        assert_eq!(resolved.cert_path, PathBuf::from("/tls/cert.pem"));
        assert_eq!(resolved.ca_cert_path, PathBuf::from("/tls/ca.pem"));

        let error = ControlTlsConfigPatch {
            trust_domain: Some("prod.example.com".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(error.to_string().contains("ca_cert_path"));
        assert_eq!(ControlTlsConfigPatch::default().resolve().unwrap(), None);
    }

    #[test]
    fn tls_config_debug_redacts_the_private_key_path() {
        let config = ControlTlsConfig {
            ca_cert_path: "/tls/ca.pem".into(),
            cert_path: "/tls/cert.pem".into(),
            key_path: "/tls/key.pem".into(),
            trust_domain: "prod.example.com".into(),
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("/tls/key.pem"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("PRIVATE KEY"));

        let patch = ControlTlsConfigPatch {
            key_path: Some("/tls/key.pem".into()),
            ..Default::default()
        };
        assert!(!format!("{patch:?}").contains("/tls/key.pem"));
    }
}
