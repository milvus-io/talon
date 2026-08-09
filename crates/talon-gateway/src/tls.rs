//! Reloadable TLS 1.3 listener for the public gateway data plane.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::Connected;
use axum::serve::{IncomingStream, Listener};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use x509_cert::der::Decode;
use x509_cert::ext::pkix::name::GeneralName;
use x509_cert::ext::pkix::SubjectAltName;
use x509_cert::Certificate;

use crate::metrics::GatewayMetrics;
use crate::AuthenticatedPrincipal;

type AcceptedTls = (TlsStream<TcpStream>, GatewayConnectionInfo);
type PendingHandshake = BoxFuture<'static, Option<AcceptedTls>>;

/// Whether the listener requests or requires a client certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayClientAuthMode {
    /// Request a certificate but permit clients without one.
    Optional,
    /// Reject clients that do not provide a valid certificate.
    Required,
}

/// One URI SAN to policy-principal mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayMtlsIdentity {
    /// Exact URI SAN accepted from the client certificate.
    pub uri_san: String,
    /// Principal used by the authorization policy.
    pub principal: AuthenticatedPrincipal,
}

/// Client-certificate trust and identity configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct GatewayClientAuthConfig {
    /// PEM trust-anchor bundle. Overlapping roots permit rotation without outage.
    pub ca_certificate_path: PathBuf,
    /// Optional or required client-certificate mode.
    pub mode: GatewayClientAuthMode,
    /// Exact URI SAN trust domain.
    pub trust_domain: String,
    /// Explicit URI SAN mappings.
    pub identities: Vec<GatewayMtlsIdentity>,
}

impl fmt::Debug for GatewayClientAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayClientAuthConfig")
            .field("ca_certificate_path", &self.ca_certificate_path)
            .field("mode", &self.mode)
            .field("trust_domain", &self.trust_domain)
            .field("identity_count", &self.identities.len())
            .finish()
    }
}

/// Peer metadata established by the listener before HTTP dispatch.
#[derive(Clone)]
pub struct GatewayConnectionInfo {
    remote_addr: std::net::SocketAddr,
    principal: Option<AuthenticatedPrincipal>,
}

impl fmt::Debug for GatewayConnectionInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayConnectionInfo")
            .field("remote_addr", &self.remote_addr)
            .field("authenticated", &self.principal.is_some())
            .finish()
    }
}

impl GatewayConnectionInfo {
    /// Network peer address.
    pub fn remote_addr(&self) -> std::net::SocketAddr {
        self.remote_addr
    }

    pub(crate) fn principal(&self) -> Option<&AuthenticatedPrincipal> {
        self.principal.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn authenticated(
        remote_addr: std::net::SocketAddr,
        principal: AuthenticatedPrincipal,
    ) -> Self {
        Self {
            remote_addr,
            principal: Some(principal),
        }
    }
}

impl Connected<IncomingStream<'_, GatewayTlsListener>> for GatewayConnectionInfo {
    fn connect_info(stream: IncomingStream<'_, GatewayTlsListener>) -> Self {
        stream.remote_addr().clone()
    }
}

impl Connected<IncomingStream<'_, TcpListener>> for GatewayConnectionInfo {
    fn connect_info(stream: IncomingStream<'_, TcpListener>) -> Self {
        Self {
            remote_addr: *stream.remote_addr(),
            principal: None,
        }
    }
}

/// File-backed public server TLS configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct GatewayTlsConfig {
    /// PEM certificate chain, leaf first.
    pub certificate_path: PathBuf,
    /// PEM PKCS#8, SEC1, or PKCS#1 private key.
    pub private_key_path: PathBuf,
    /// Poll interval for certificate rotation.
    pub reload_interval: Duration,
    /// Maximum time an accepted connection may spend handshaking.
    pub handshake_timeout: Duration,
    /// Maximum accepted connections concurrently performing a TLS handshake.
    pub max_concurrent_handshakes: usize,
    /// Optional incoming client-certificate authentication.
    pub client_auth: Option<GatewayClientAuthConfig>,
}

impl fmt::Debug for GatewayTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayTlsConfig")
            .field("certificate_path", &self.certificate_path)
            .field("private_key_path", &"[REDACTED]")
            .field("reload_interval", &self.reload_interval)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("max_concurrent_handshakes", &self.max_concurrent_handshakes)
            .field("client_auth", &self.client_auth)
            .finish()
    }
}

impl GatewayTlsConfig {
    /// Validate non-secret bounds before opening files.
    pub fn validate(&self) -> Result<(), GatewayTlsError> {
        if self.reload_interval.is_zero() {
            return Err(GatewayTlsError::ZeroReloadInterval);
        }
        if self.handshake_timeout.is_zero() {
            return Err(GatewayTlsError::ZeroHandshakeTimeout);
        }
        if self.max_concurrent_handshakes == 0 {
            return Err(GatewayTlsError::ZeroHandshakeLimit);
        }
        if let Some(client_auth) = &self.client_auth {
            validate_client_auth(client_auth)?;
        }
        Ok(())
    }
}

/// Stable TLS material/configuration failure that does not expose file paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GatewayTlsError {
    /// Certificate rotation polling was disabled.
    #[error("gateway TLS reload interval must be greater than zero")]
    ZeroReloadInterval,
    /// Handshake timeout was disabled.
    #[error("gateway TLS handshake timeout must be greater than zero")]
    ZeroHandshakeTimeout,
    /// Concurrent handshake capacity was disabled.
    #[error("gateway TLS concurrent handshake limit must be greater than zero")]
    ZeroHandshakeLimit,
    /// The certificate file could not be opened or parsed.
    #[error("gateway TLS certificate chain is unreadable or invalid")]
    InvalidCertificateChain,
    /// The certificate file had no certificates.
    #[error("gateway TLS certificate chain is empty")]
    EmptyCertificateChain,
    /// The private key could not be opened or parsed.
    #[error("gateway TLS private key is unreadable or invalid")]
    InvalidPrivateKey,
    /// The key does not match the leaf certificate or rustls rejected it.
    #[error("gateway TLS certificate and private key are incompatible")]
    IncompatibleKey,
    /// The client CA bundle could not be loaded.
    #[error("gateway TLS client CA bundle is unreadable or invalid")]
    InvalidClientCa,
    /// Client identity mappings or trust domain are invalid.
    #[error("gateway TLS client identity configuration is invalid")]
    InvalidClientIdentity,
}

/// TLS listener that snapshots last-good material for each new connection.
pub struct GatewayTlsListener {
    listener: TcpListener,
    material: watch::Receiver<Arc<LoadedServerConfig>>,
    handshake_timeout: Duration,
    max_concurrent_handshakes: usize,
    handshakes: FuturesUnordered<PendingHandshake>,
    metrics: GatewayMetrics,
}

impl GatewayTlsListener {
    /// Load initial material and start bounded polling for rotations.
    pub fn load(
        listener: TcpListener,
        config: GatewayTlsConfig,
        metrics: GatewayMetrics,
    ) -> Result<Self, GatewayTlsError> {
        config.validate()?;
        let initial = Arc::new(load_server_config(&config)?);
        let (sender, receiver) = watch::channel(initial);
        spawn_reload(config.clone(), sender, metrics.clone());
        Ok(Self {
            listener,
            material: receiver,
            handshake_timeout: config.handshake_timeout,
            max_concurrent_handshakes: config.max_concurrent_handshakes,
            handshakes: FuturesUnordered::new(),
            metrics,
        })
    }
}

impl Listener for GatewayTlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = GatewayConnectionInfo;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if self.handshakes.len() >= self.max_concurrent_handshakes {
                if let Some(Some(accepted)) = self.handshakes.next().await {
                    return accepted;
                }
                continue;
            }

            tokio::select! {
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, address)) => {
                        let material = self.material.borrow().clone();
                        let timeout = self.handshake_timeout;
                        let metrics = self.metrics.clone();
                        self.handshakes.push(async move {
                            let handshake = TlsAcceptor::from(Arc::new(material.server.clone())).accept(stream);
                            match tokio::time::timeout(timeout, handshake).await {
                                Ok(Ok(stream)) => {
                                    let principal = peer_principal(&stream, material.client_auth.as_ref());
                                    Some((stream, GatewayConnectionInfo {
                                        remote_addr: address,
                                        principal,
                                    }))
                                }
                                Ok(Err(_)) => {
                                    metrics.record_tls_event("handshake_failure");
                                    tracing::debug!("gateway TLS handshake rejected before HTTP dispatch");
                                    None
                                }
                                Err(_) => {
                                    metrics.record_tls_event("handshake_timeout");
                                    tracing::debug!("gateway TLS handshake timed out before HTTP dispatch");
                                    None
                                }
                            }
                        }.boxed());
                    }
                    Err(_) => {
                        tracing::warn!("gateway TLS TCP accept failed; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
                completed = self.handshakes.next(), if !self.handshakes.is_empty() => {
                    if let Some(Some(accepted)) = completed {
                        return accepted;
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(GatewayConnectionInfo {
            remote_addr: self.listener.local_addr()?,
            principal: None,
        })
    }
}

fn spawn_reload(
    config: GatewayTlsConfig,
    sender: watch::Sender<Arc<LoadedServerConfig>>,
    metrics: GatewayMetrics,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.reload_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Initial material was loaded synchronously.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if sender.is_closed() {
                return;
            }
            match load_server_config(&config) {
                Ok(material) => {
                    sender.send_replace(Arc::new(material));
                    metrics.record_tls_event("reload_success");
                }
                Err(_) => {
                    metrics.record_tls_event("reload_failure");
                    tracing::warn!("gateway TLS reload failed; retaining last valid material");
                }
            }
        }
    });
}

struct LoadedServerConfig {
    server: ServerConfig,
    client_auth: Option<GatewayClientAuthConfig>,
}

fn validate_client_auth(config: &GatewayClientAuthConfig) -> Result<(), GatewayTlsError> {
    if config.trust_domain.is_empty() || config.identities.is_empty() {
        return Err(GatewayTlsError::InvalidClientIdentity);
    }
    let mut uris = HashSet::new();
    for identity in &config.identities {
        if identity.principal.id.is_empty()
            || identity.principal.provider_account.is_empty()
            || !uris.insert(identity.uri_san.clone())
            || !valid_workload_uri(&identity.uri_san, &config.trust_domain)
        {
            return Err(GatewayTlsError::InvalidClientIdentity);
        }
    }
    Ok(())
}

fn valid_workload_uri(uri: &str, trust_domain: &str) -> bool {
    url::Url::parse(uri).is_ok_and(|uri| {
        uri.scheme() == "spiffe"
            && uri.host_str() == Some(trust_domain)
            && uri.port().is_none()
            && uri.username().is_empty()
            && uri.password().is_none()
            && uri.path() != "/"
            && !uri.path().is_empty()
            && uri.query().is_none()
            && uri.fragment().is_none()
    })
}

fn client_verifier(
    config: &GatewayClientAuthConfig,
) -> Result<Arc<dyn ClientCertVerifier>, GatewayTlsError> {
    validate_client_auth(config)?;
    let mut roots = RootCertStore::empty();
    let certificates = CertificateDer::pem_file_iter(&config.ca_certificate_path)
        .map_err(|_| GatewayTlsError::InvalidClientCa)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| GatewayTlsError::InvalidClientCa)?;
    if certificates.is_empty() {
        return Err(GatewayTlsError::InvalidClientCa);
    }
    roots.add_parsable_certificates(certificates);
    if roots.is_empty() {
        return Err(GatewayTlsError::InvalidClientCa);
    }
    let builder = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots));
    let builder = if config.mode == GatewayClientAuthMode::Optional {
        builder.allow_unauthenticated()
    } else {
        builder
    };
    let inner = builder
        .build()
        .map_err(|_| GatewayTlsError::InvalidClientCa)?;
    Ok(Arc::new(GatewayClientVerifier {
        inner,
        trust_domain: config.trust_domain.clone(),
        identities: identity_map(config),
    }))
}

fn identity_map(config: &GatewayClientAuthConfig) -> HashMap<String, AuthenticatedPrincipal> {
    config
        .identities
        .iter()
        .map(|identity| (identity.uri_san.clone(), identity.principal.clone()))
        .collect()
}

fn peer_principal(
    stream: &TlsStream<TcpStream>,
    config: Option<&GatewayClientAuthConfig>,
) -> Option<AuthenticatedPrincipal> {
    let config = config?;
    let leaf = stream.get_ref().1.peer_certificates()?.first()?;
    let uri = certificate_uri(leaf).ok()?;
    identity_map(config).get(&uri).cloned()
}

fn certificate_uri(certificate: &CertificateDer<'_>) -> Result<String, GatewayTlsError> {
    let certificate = Certificate::from_der(certificate.as_ref())
        .map_err(|_| GatewayTlsError::InvalidClientIdentity)?;
    let extension = certificate
        .tbs_certificate
        .extensions
        .as_ref()
        .and_then(|extensions| {
            extensions
                .iter()
                .find(|extension| extension.extn_id.to_string() == "2.5.29.17")
        })
        .ok_or(GatewayTlsError::InvalidClientIdentity)?;
    let san = SubjectAltName::from_der(extension.extn_value.as_bytes())
        .map_err(|_| GatewayTlsError::InvalidClientIdentity)?;
    let uris = san
        .0
        .iter()
        .filter_map(|name| match name {
            GeneralName::UniformResourceIdentifier(uri) => Some(uri.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match uris.as_slice() {
        [uri] => Ok((*uri).to_string()),
        _ => Err(GatewayTlsError::InvalidClientIdentity),
    }
}

#[derive(Debug)]
struct GatewayClientVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    trust_domain: String,
    identities: HashMap<String, AuthenticatedPrincipal>,
}

impl ClientCertVerifier for GatewayClientVerifier {
    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let uri = certificate_uri(end_entity)
            .map_err(|error| rustls::Error::General(error.to_string()))?;
        if !valid_workload_uri(&uri, &self.trust_domain) || !self.identities.contains_key(&uri) {
            return Err(rustls::Error::General(
                "gateway TLS client identity is not authorized".into(),
            ));
        }
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn load_server_config(config: &GatewayTlsConfig) -> Result<LoadedServerConfig, GatewayTlsError> {
    let certificates = CertificateDer::pem_file_iter(&config.certificate_path)
        .map_err(|_| GatewayTlsError::InvalidCertificateChain)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| GatewayTlsError::InvalidCertificateChain)?;
    if certificates.is_empty() {
        return Err(GatewayTlsError::EmptyCertificateChain);
    }
    let key = PrivateKeyDer::from_pem_file(&config.private_key_path)
        .map_err(|_| GatewayTlsError::InvalidPrivateKey)?;
    let builder = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13]);
    let builder = if let Some(client_auth) = &config.client_auth {
        builder.with_client_cert_verifier(client_verifier(client_auth)?)
    } else {
        builder.with_no_client_auth()
    };
    let mut server = builder
        .with_single_cert(certificates, key)
        .map_err(|_| GatewayTlsError::IncompatibleKey)?;
    server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(LoadedServerConfig {
        server,
        client_auth: config.client_auth.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::extract::Request;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use talon_core::{Backend, ObjectId};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        serve_tls, AuthenticatedPrincipal, AuthorizationGrant, AuthorizationPolicy, GatewayAccess,
        GatewayAdapter, GatewayAuthenticationError, GatewayAuthenticator, GatewayConfig,
        GatewayMode, GatewayOperation, GatewayRequestContext, GatewayResponse, GatewayRuntime,
        GatewaySecurity, GatewayTarget, ProviderProtocol,
    };

    const CERT_DER: &[u8] =
        include_bytes!("../../talon-transport/tests/fixtures/control_tls/coordinator.der");
    const KEY_DER: &[u8] =
        include_bytes!("../../talon-transport/tests/fixtures/control_tls/coordinator-key.der");
    const CA_DER: &[u8] = include_bytes!("../../talon-transport/tests/fixtures/control_tls/ca.der");
    const CLIENT_DER: &[u8] =
        include_bytes!("../../talon-transport/tests/fixtures/control_tls/worker.der");
    const CLIENT_KEY_DER: &[u8] =
        include_bytes!("../../talon-transport/tests/fixtures/control_tls/worker-key.der");
    const CLIENT_URI: &str = "spiffe://cluster.example/talon/cluster-a/worker/worker-1";

    struct Adapter;

    struct Authenticator;

    impl GatewayAuthenticator for Authenticator {
        fn authenticate(
            &self,
            _request: &Request,
            _protocol: ProviderProtocol,
        ) -> Result<AuthenticatedPrincipal, GatewayAuthenticationError> {
            Ok(AuthenticatedPrincipal::new("test", "test"))
        }
    }

    #[async_trait]
    impl GatewayAdapter for Adapter {
        fn protocol(&self) -> ProviderProtocol {
            ProviderProtocol::S3
        }

        fn access(
            &self,
            _request: &Request,
            _context: &GatewayRequestContext,
        ) -> Result<GatewayAccess, Box<GatewayResponse>> {
            Ok(GatewayAccess {
                operation: GatewayOperation::Read,
                provider_account: Some("account-a".into()),
                target: GatewayTarget::Object(ObjectId::new(
                    Backend::S3,
                    "bucket-a",
                    "tenant/object",
                )),
                additional: Vec::new(),
            })
        }

        async fn handle(
            &self,
            _request: Request,
            _context: GatewayRequestContext,
        ) -> GatewayResponse {
            GatewayResponse::new(
                axum::response::Response::new(Body::empty()),
                GatewayOperation::Read,
            )
        }
    }

    fn pem(label: &str, der: &[u8]) -> String {
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            STANDARD.encode(der)
        )
    }

    fn write_material(root: &Path) -> GatewayTlsConfig {
        let certificate_path = root.join("cert.pem");
        let private_key_path = root.join("private-secret-key.pem");
        fs::write(&certificate_path, pem("CERTIFICATE", CERT_DER)).unwrap();
        fs::write(&private_key_path, pem("PRIVATE KEY", KEY_DER)).unwrap();
        GatewayTlsConfig {
            certificate_path,
            private_key_path,
            reload_interval: Duration::from_millis(20),
            handshake_timeout: Duration::from_millis(200),
            max_concurrent_handshakes: 8,
            client_auth: None,
        }
    }

    fn configure_client_auth(
        root: &Path,
        config: &mut GatewayTlsConfig,
        mode: GatewayClientAuthMode,
        uri_san: &str,
    ) {
        let ca_certificate_path = root.join("client-ca.pem");
        fs::write(&ca_certificate_path, pem("CERTIFICATE", CA_DER)).unwrap();
        config.client_auth = Some(GatewayClientAuthConfig {
            ca_certificate_path,
            mode,
            trust_domain: "cluster.example".into(),
            identities: vec![GatewayMtlsIdentity {
                uri_san: uri_san.into(),
                principal: AuthenticatedPrincipal::new("worker-reader", "account-a"),
            }],
        });
    }

    fn https_client(with_identity: bool) -> reqwest::Client {
        let mut builder = reqwest::Client::builder().danger_accept_invalid_certs(true);
        if with_identity {
            let identity = format!(
                "{}{}",
                pem("CERTIFICATE", CLIENT_DER),
                pem("PRIVATE KEY", CLIENT_KEY_DER)
            );
            builder = builder.identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap());
        }
        builder.build().unwrap()
    }

    fn mtls_runtime(address: std::net::SocketAddr) -> Arc<GatewayRuntime> {
        let runtime = Arc::new(
            GatewayRuntime::new(
                GatewayConfig {
                    bind: address,
                    mode: GatewayMode::Production,
                    ..GatewayConfig::default()
                },
                Arc::new(Adapter),
                GatewaySecurity::default(),
            )
            .unwrap(),
        );
        runtime.install_authorization(
            AuthorizationPolicy::new(vec![AuthorizationGrant {
                id: "worker-read".into(),
                principal: "worker-reader".into(),
                protocol: ProviderProtocol::S3,
                provider_account: "account-a".into(),
                namespace: "bucket-a".into(),
                prefix: Some("tenant/".into()),
                operations: vec![GatewayOperation::Read],
            }])
            .unwrap(),
        );
        runtime
    }

    #[tokio::test]
    async fn required_mtls_rejects_anonymous_and_dispatches_mapped_identity() {
        let temp = TempDir::new().unwrap();
        let mut tls = write_material(temp.path());
        configure_client_auth(
            temp.path(),
            &mut tls,
            GatewayClientAuthMode::Required,
            CLIENT_URI,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime = mtls_runtime(address);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_runtime = Arc::clone(&runtime);
        let server = tokio::spawn(async move {
            serve_tls(listener, server_runtime, tls, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });

        assert!(https_client(false)
            .get(format!("https://{address}/tenant/object"))
            .send()
            .await
            .is_err());
        let response = https_client(true)
            .get(format!("https://{address}/tenant/object"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(runtime.readiness().is_ready());

        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn optional_mtls_accepts_anonymous_but_rejects_unmapped_certificates() {
        let temp = TempDir::new().unwrap();
        let mut optional = write_material(temp.path());
        configure_client_auth(
            temp.path(),
            &mut optional,
            GatewayClientAuthMode::Optional,
            CLIENT_URI,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = GatewayMetrics::new();
        let mut tls = GatewayTlsListener::load(listener, optional, metrics).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = tls.accept().await;
            drop(stream);
        });
        assert!(https_client(false)
            .get(format!("https://{address}"))
            .send()
            .await
            .is_err());
        server.await.unwrap();

        let mut unmapped = write_material(temp.path());
        configure_client_auth(
            temp.path(),
            &mut unmapped,
            GatewayClientAuthMode::Required,
            "spiffe://cluster.example/talon/cluster-a/worker/other",
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = GatewayMetrics::new();
        let mut tls = GatewayTlsListener::load(listener, unmapped, metrics).unwrap();
        let server = tokio::spawn(async move {
            tokio::select! {
                _ = tls.accept() => {},
                _ = tokio::time::sleep(Duration::from_millis(500)) => {},
            }
        });
        assert!(https_client(true)
            .get(format!("https://{address}"))
            .send()
            .await
            .is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_listener_rejects_plaintext_and_sets_real_readiness() {
        let temp = TempDir::new().unwrap();
        let mut tls = write_material(temp.path());
        tls.handshake_timeout = Duration::from_secs(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime = Arc::new(
            GatewayRuntime::new(
                GatewayConfig {
                    bind: address,
                    mode: GatewayMode::Production,
                    ..GatewayConfig::default()
                },
                Arc::new(Adapter),
                GatewaySecurity {
                    tls: false,
                    authentication: true,
                    authorization: true,
                },
            )
            .unwrap(),
        );
        runtime.install_authentication(Arc::new(Authenticator));
        runtime.install_authorization(AuthorizationPolicy::new(Vec::new()).unwrap());
        assert!(!runtime.readiness().is_ready());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_runtime = Arc::clone(&runtime);
        let server = tokio::spawn(async move {
            serve_tls(listener, server_runtime, tls, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let stalled = TcpStream::connect(address).await.unwrap();
        let response = tokio::time::timeout(
            Duration::from_millis(500),
            client.get(format!("https://{address}/readyz")).send(),
        )
        .await
        .expect("a stalled handshake must not block later accepts")
        .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        drop(stalled);

        let plaintext = reqwest::get(format!("http://{address}/healthz")).await;
        assert!(plaintext.is_err(), "plaintext must not reach HTTP dispatch");
        assert!(runtime.readiness().is_ready());
        assert!(runtime
            .metrics()
            .render()
            .contains("event=\"handshake_failure\""));

        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn invalid_client_ca_reload_retains_last_good_material_and_is_counted() {
        let temp = TempDir::new().unwrap();
        let mut config = write_material(temp.path());
        configure_client_auth(
            temp.path(),
            &mut config,
            GatewayClientAuthMode::Required,
            CLIENT_URI,
        );
        let client_ca_path = config
            .client_auth
            .as_ref()
            .unwrap()
            .ca_certificate_path
            .clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = GatewayMetrics::new();
        let mut tls = GatewayTlsListener::load(listener, config.clone(), metrics.clone()).unwrap();
        fs::write(&client_ca_path, "not a certificate").unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if metrics.render().contains("event=\"reload_failure\"") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        fs::write(&client_ca_path, pem("CERTIFICATE", CA_DER)).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if metrics.render().contains("event=\"reload_success\"") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let client = https_client(true);
        let server = tokio::spawn(async move {
            let (stream, _) = tls.accept().await;
            drop(stream);
        });
        assert!(client
            .get(format!("https://{address}"))
            .send()
            .await
            .is_err());
        // A completed TLS handshake proves the last-good acceptor remained usable;
        // the request fails only because this focused listener has no HTTP server.
        server.await.unwrap();
    }

    #[test]
    fn bounds_and_diagnostics_do_not_expose_private_key_paths() {
        let temp = TempDir::new().unwrap();
        let mut config = write_material(temp.path());
        let debug = format!("{config:?}");
        assert!(!debug.contains("private-secret-key.pem"));
        assert!(debug.contains("[REDACTED]"));
        config.reload_interval = Duration::ZERO;
        assert_eq!(config.validate(), Err(GatewayTlsError::ZeroReloadInterval));
        config.reload_interval = Duration::from_secs(1);
        config.handshake_timeout = Duration::ZERO;
        assert_eq!(
            config.validate(),
            Err(GatewayTlsError::ZeroHandshakeTimeout)
        );
        config.handshake_timeout = Duration::from_secs(1);
        config.max_concurrent_handshakes = 0;
        assert_eq!(config.validate(), Err(GatewayTlsError::ZeroHandshakeLimit));
    }

    #[test]
    fn workload_uris_require_an_exact_portless_trust_domain_and_path() {
        assert!(valid_workload_uri(CLIENT_URI, "cluster.example"));
        assert!(!valid_workload_uri(CLIENT_URI, "other.example"));
        assert!(!valid_workload_uri(
            "spiffe://cluster.example:443/talon/worker",
            "cluster.example"
        ));
        assert!(!valid_workload_uri(
            "spiffe://cluster.example/",
            "cluster.example"
        ));
        assert!(!valid_workload_uri(
            "https://cluster.example/talon/worker",
            "cluster.example"
        ));
    }
}
