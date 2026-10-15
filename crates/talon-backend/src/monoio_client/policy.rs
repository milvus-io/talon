//! Startup capability snapshot and pure, pre-handshake TLS selection.
use super::ktls;
use rustls::{CipherSuite, ClientConfig, ProtocolVersion, SupportedCipherSuite};
use std::{collections::BTreeSet, io, sync::Arc};

/// Minimum allowed origin TLS version. Endpoint rules may raise this floor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum TlsVersion {
    #[default]
    Tls12,
    Tls13,
}

impl std::str::FromStr for TlsVersion {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "1.2" => Ok(Self::Tls12),
            "1.3" => Ok(Self::Tls13),
            _ => Err("origin TLS minimum version must be 1.2 or 1.3"),
        }
    }
}

/// Immutable snapshot, shared by all origin clients/rings in this service.
/// An unprobed/default snapshot safely disables offload, with no lazy probing.
#[derive(Clone, Debug, Default)]
pub struct KernelTlsCapabilities {
    supported: BTreeSet<(u16, u16)>,
    rekey: BTreeSet<(u16, u16)>,
}

impl KernelTlsCapabilities {
    /// Call once during startup, before publishing the HTTP client/listeners.
    /// Each of six bounded startup probes checks TX/RX installation and,
    /// separately, key replacement. Failure does not disable basic TLS 1.3.
    pub async fn probe() -> io::Result<Self> {
        let mut capabilities = Self::default();
        for version in [0x304, 0x303] {
            for (cipher, size) in [(51, 40), (52, 56), (54, 56)] {
                let (supported, rekey) = ktls::probe(version, cipher, size).await;
                if supported {
                    capabilities.supported.insert((version, cipher));
                }
                if rekey {
                    capabilities.rekey.insert((version, cipher));
                }
            }
        }
        Ok(capabilities)
    }

    pub fn is_empty(&self) -> bool {
        self.supported.is_empty()
    }

    fn suite_key(suite: SupportedCipherSuite) -> Option<(u16, u16)> {
        use CipherSuite::*;
        let cipher = match suite.suite() {
            TLS13_AES_128_GCM_SHA256
            | TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
            | TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 => 51,
            TLS13_AES_256_GCM_SHA384
            | TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
            | TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 => 52,
            TLS13_CHACHA20_POLY1305_SHA256
            | TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
            | TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 => 54,
            _ => return None,
        };
        Some((u16::from(suite.version().version), cipher))
    }

    fn supports(&self, suite: SupportedCipherSuite) -> bool {
        Self::suite_key(suite).is_some_and(|key| self.supported.contains(&key))
    }

    pub(super) fn supports_key_update(&self, suite: SupportedCipherSuite) -> bool {
        Self::suite_key(suite).is_some_and(|key| self.rekey.contains(&key))
    }

    pub(super) fn select(&self, allowed: &ClientConfig) -> Option<Arc<ClientConfig>> {
        for version in [ProtocolVersion::TLSv1_3, ProtocolVersion::TLSv1_2] {
            let suites: Vec<_> = allowed
                .crypto_provider()
                .cipher_suites
                .iter()
                .filter(|suite| suite.version().version == version && self.supports(**suite))
                .map(|suite| suite.suite())
                .collect();
            if let Ok(config) = allowed.with_restricted_cipher_suites(&suites) {
                return Some(Arc::new(config));
            }
        }
        None
    }
}

pub(super) fn minimum(
    config: &ClientConfig,
    minimum: TlsVersion,
) -> Result<Arc<ClientConfig>, String> {
    let suites: Vec<_> = config
        .crypto_provider()
        .cipher_suites
        .iter()
        .filter(|suite| {
            minimum == TlsVersion::Tls12 || suite.version().version == ProtocolVersion::TLSv1_3
        })
        .map(|suite| suite.suite())
        .collect();
    config
        .with_restricted_cipher_suites(&suites)
        .map(Arc::new)
        .map_err(|_| "origin TLS minimum conflicts with configured protocols/ciphers".into())
}

pub(super) fn endpoint_key(endpoint: &str) -> Result<String, &'static str> {
    let url = url::Url::parse(endpoint).map_err(|_| "invalid origin TLS endpoint")?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err("origin TLS endpoint must be an HTTPS origin without credentials/path/query");
    }
    Ok(authority_key(
        url.host_str().ok_or("origin TLS endpoint has no host")?,
        url.port_or_known_default().unwrap(),
    ))
}

pub(super) fn authority_key(host: &str, port: u16) -> String {
    format!(
        "{}:{port}",
        host.trim_matches(['[', ']']).to_ascii_lowercase()
    )
}

pub(super) fn negotiation_rejected(error: &io::Error) -> bool {
    use rustls::{AlertDescription, Error, PeerIncompatible};
    matches!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<Error>()),
        Some(Error::AlertReceived(
            AlertDescription::ProtocolVersion
                | AlertDescription::HandshakeFailure
                | AlertDescription::InsufficientSecurity
        )) | Some(Error::PeerIncompatible(
            PeerIncompatible::NoCipherSuitesInCommon
                | PeerIncompatible::ServerDoesNotSupportTls12Or13
                | PeerIncompatible::ServerTlsVersionIsDisabledByOurConfig
        ))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(versions: &[&'static rustls::SupportedProtocolVersion]) -> ClientConfig {
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(versions)
            .unwrap()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth()
    }

    #[test]
    fn selects_highest_offloadable_version_and_only_supported_ciphers() {
        let config = config(rustls::DEFAULT_VERSIONS);
        for (supported, version, cipher) in [
            (
                vec![(0x303, 51)],
                ProtocolVersion::TLSv1_2,
                CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            ),
            (
                vec![(0x303, 51), (0x304, 52)],
                ProtocolVersion::TLSv1_3,
                CipherSuite::TLS13_AES_256_GCM_SHA384,
            ),
        ] {
            let capabilities = KernelTlsCapabilities {
                supported: supported.into_iter().collect(),
                ..Default::default()
            };
            let selected = capabilities.select(&config).unwrap();
            assert!(selected
                .crypto_provider()
                .cipher_suites
                .iter()
                .all(|s| s.version().version == version));
            assert!(selected
                .crypto_provider()
                .cipher_suites
                .iter()
                .any(|s| s.suite() == cipher));
            assert!(selected
                .crypto_provider()
                .cipher_suites
                .iter()
                .all(|s| capabilities.supports(*s)));
            assert!(Arc::ptr_eq(
                &config.client_auth_cert_resolver,
                &selected.client_auth_cert_resolver
            ));
        }
    }

    #[test]
    fn unavailable_kernel_or_minimum_preserves_rustls_fallback() {
        let config = config(rustls::DEFAULT_VERSIONS);
        assert!(KernelTlsCapabilities::default().select(&config).is_none());
        let only12 = KernelTlsCapabilities {
            supported: [(0x303, 51)].into(),
            ..Default::default()
        };
        let allowed = minimum(&config, TlsVersion::Tls13).unwrap();
        assert!(only12.select(&allowed).is_none());
        assert!(allowed
            .crypto_provider()
            .cipher_suites
            .iter()
            .all(|s| s.version().version == ProtocolVersion::TLSv1_3));
    }

    #[test]
    fn filtering_never_reenables_caller_disabled_versions_or_ciphers() {
        let config13 = config(&[&rustls::version::TLS13]);
        let only12 = KernelTlsCapabilities {
            supported: [(0x303, 51)].into(),
            ..Default::default()
        };
        assert!(only12.select(&config13).is_none());
        let config12 = config(&[&rustls::version::TLS12]);
        assert!(minimum(&config12, TlsVersion::Tls13).is_err());
        let config = config13
            .with_restricted_cipher_suites(&[CipherSuite::TLS13_CHACHA20_POLY1305_SHA256])
            .unwrap();
        let only_aes = KernelTlsCapabilities {
            supported: [(0x304, 51)].into(),
            ..Default::default()
        };
        assert!(only_aes.select(&config).is_none());
    }

    #[test]
    fn endpoint_rules_use_exact_normalized_https_authorities() {
        assert_eq!(
            endpoint_key("https://S3.EXAMPLE.com/").unwrap(),
            authority_key("s3.example.com", 443)
        );
        assert_eq!(
            endpoint_key("https://[::1]:8443").unwrap(),
            authority_key("::1", 8443)
        );
        assert_ne!(
            endpoint_key("https://s3.example.com:8443").unwrap(),
            endpoint_key("https://s3.example.com").unwrap()
        );
        for invalid in [
            "http://s3.example.com",
            "https://user@s3.example.com",
            "https://s3.example.com/bucket",
            "https://s3.example.com?secret=1",
        ] {
            assert!(endpoint_key(invalid).is_err());
        }
    }

    #[test]
    fn certificate_and_record_errors_never_trigger_negotiation_retry() {
        for error in [
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
            rustls::Error::DecryptError,
            rustls::Error::AlertReceived(rustls::AlertDescription::BadCertificate),
        ] {
            assert!(!negotiation_rejected(&io::Error::new(
                io::ErrorKind::InvalidData,
                error
            )));
        }
        assert!(negotiation_rejected(&io::Error::new(
            io::ErrorKind::InvalidData,
            rustls::Error::AlertReceived(rustls::AlertDescription::ProtocolVersion)
        )));
        assert!(!negotiation_rejected(&io::Error::from(
            io::ErrorKind::ConnectionReset
        )));
    }
}
