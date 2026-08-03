//! Mutual-TLS transport for the privileged coordinator-worker channel.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use talon_core::{ControlTlsConfig, WorkloadIdentity, WorkloadRole};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::watch;
use tokio_rustls::{client, server, TlsAcceptor, TlsConnector};
use x509_cert::der::Decode;
use x509_cert::ext::pkix::name::GeneralName;
use x509_cert::ext::pkix::SubjectAltName;
use x509_cert::Certificate;

const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Last-good TLS client and server configurations loaded from mounted files.
#[derive(Clone)]
pub struct ControlTlsChannel {
    config: ControlTlsConfig,
    local_identity: WorkloadIdentity,
    expected_peer_role: WorkloadRole,
    material: watch::Receiver<Arc<ControlTlsMaterial>>,
}

impl fmt::Debug for ControlTlsChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlTlsChannel")
            .field("config", &self.config)
            .field("local_identity", &self.local_identity)
            .field("expected_peer_role", &self.expected_peer_role)
            .finish_non_exhaustive()
    }
}

impl ControlTlsChannel {
    /// Load and validate the initial TLS material, then poll files for rotation.
    pub fn load(
        config: ControlTlsConfig,
        local_identity: WorkloadIdentity,
        expected_peer_role: WorkloadRole,
        reload_interval: Duration,
    ) -> Result<Self> {
        if reload_interval.is_zero() {
            anyhow::bail!("control TLS reload interval must be greater than zero");
        }
        let initial = Arc::new(ControlTlsMaterial::load(
            &config,
            &local_identity,
            expected_peer_role,
        )?);
        let (sender, receiver) = watch::channel(initial);
        let channel = Self {
            config,
            local_identity,
            expected_peer_role,
            material: receiver,
        };
        channel.spawn_reload(sender, reload_interval);
        Ok(channel)
    }

    fn spawn_reload(
        &self,
        sender: watch::Sender<Arc<ControlTlsMaterial>>,
        reload_interval: Duration,
    ) {
        let config = self.config.clone();
        let local_identity = self.local_identity.clone();
        let expected_peer_role = self.expected_peer_role;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reload_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The initial material was loaded synchronously above.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if sender.is_closed() {
                    return;
                }
                match ControlTlsMaterial::load(&config, &local_identity, expected_peer_role) {
                    Ok(material) => {
                        sender.send_replace(Arc::new(material));
                    }
                    Err(error) => {
                        tracing::warn!(%error, "control TLS reload failed; retaining last valid material");
                    }
                }
            }
        });
    }

    /// Accept one mutually authenticated TLS connection.
    pub async fn accept(&self, stream: TcpStream) -> Result<AuthenticatedServerStream> {
        let material = self.material.borrow().clone();
        let stream = tokio::time::timeout(
            TLS_HANDSHAKE_TIMEOUT,
            TlsAcceptor::from(Arc::clone(&material.server)).accept(stream),
        )
        .await
        .context("control TLS server handshake timed out")?
        .context("control TLS server handshake failed")?;
        let identity = peer_identity(
            stream.get_ref().1.peer_certificates(),
            &self.config.trust_domain,
            self.local_identity.cluster_id(),
            self.expected_peer_role,
        )?;
        Ok(AuthenticatedControlStream { stream, identity })
    }

    /// Connect to one mutually authenticated control listener.
    pub async fn connect(&self, addr: impl ToSocketAddrs) -> Result<AuthenticatedClientStream> {
        let stream = TcpStream::connect(addr).await?;
        let material = self.material.borrow().clone();
        // The custom verifier authenticates the canonical URI SAN. rustls still
        // requires a syntactically valid ServerName to start a client handshake.
        let server_name = ServerName::try_from(self.config.trust_domain.clone())
            .context("control TLS trust domain is not a valid server name")?;
        let stream = TlsConnector::from(Arc::clone(&material.client))
            .connect(server_name, stream)
            .await
            .context("control TLS client handshake failed")?;
        let identity = peer_identity(
            stream.get_ref().1.peer_certificates(),
            &self.config.trust_domain,
            self.local_identity.cluster_id(),
            self.expected_peer_role,
        )?;
        Ok(AuthenticatedControlStream { stream, identity })
    }
}

/// A TLS stream paired with the identity authenticated during its handshake.
pub struct AuthenticatedControlStream<S> {
    pub stream: S,
    pub identity: WorkloadIdentity,
}

/// Server-side authenticated stream.
pub type AuthenticatedServerStream = AuthenticatedControlStream<server::TlsStream<TcpStream>>;

/// Client-side authenticated stream.
pub type AuthenticatedClientStream = AuthenticatedControlStream<client::TlsStream<TcpStream>>;

struct ControlTlsMaterial {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
}

impl ControlTlsMaterial {
    fn load(
        config: &ControlTlsConfig,
        local_identity: &WorkloadIdentity,
        expected_peer_role: WorkloadRole,
    ) -> Result<Self> {
        let roots = Arc::new(load_roots(config)?);
        let certs = load_certificates(config)?;
        verify_local_chain(&certs, &roots, webpki::KeyUsage::server_auth())
            .context("control TLS leaf is not valid for server authentication")?;
        verify_local_chain(&certs, &roots, webpki::KeyUsage::client_auth())
            .context("control TLS leaf is not valid for client authentication")?;
        let actual = certificate_identity(&certs[0])?;
        if &actual != local_identity {
            anyhow::bail!(
                "control TLS leaf identity {actual} does not match configured identity {local_identity}"
            );
        }

        let server_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::clone(&roots))
            .build()
            .context("build control TLS client-certificate verifier")?;
        let server_verifier = Arc::new(IdentityClientVerifier {
            inner: server_verifier,
            trust_domain: config.trust_domain.clone(),
            cluster_id: local_identity.cluster_id().to_owned(),
            role: expected_peer_role,
        });
        let server = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(server_verifier)
            .with_single_cert(certs.clone(), load_private_key(config)?)
            .context("build control TLS server configuration")?;

        let chain_verifier = rustls::client::WebPkiServerVerifier::builder(Arc::clone(&roots))
            .build()
            .context("build control TLS server-certificate verifier")?;
        let client_verifier = Arc::new(IdentityServerVerifier {
            inner: chain_verifier,
            roots,
            trust_domain: config.trust_domain.clone(),
            cluster_id: local_identity.cluster_id().to_owned(),
            role: expected_peer_role,
        });
        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .dangerous()
            .with_custom_certificate_verifier(client_verifier)
            .with_client_auth_cert(certs, load_private_key(config)?)
            .context("build control TLS client configuration")?;

        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
        })
    }
}

fn verify_local_chain(
    certificates: &[CertificateDer<'_>],
    roots: &RootCertStore,
    usage: webpki::KeyUsage,
) -> Result<()> {
    let provider = rustls::crypto::ring::default_provider();
    let leaf = webpki::EndEntityCert::try_from(&certificates[0])?;
    leaf.verify_for_usage(
        provider.signature_verification_algorithms.all,
        &roots.roots,
        &certificates[1..],
        UnixTime::now(),
        usage,
        None,
        None,
    )?;
    Ok(())
}

fn load_roots(config: &ControlTlsConfig) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let certificates: Vec<_> = CertificateDer::pem_file_iter(&config.ca_cert_path)
        .with_context(|| format!("open control TLS CA bundle {:?}", config.ca_cert_path))?
        .collect::<std::result::Result<_, _>>()
        .context("parse control TLS CA bundle")?;
    if certificates.is_empty() {
        anyhow::bail!("control TLS CA bundle contains no certificates");
    }
    roots.add_parsable_certificates(certificates);
    if roots.is_empty() {
        anyhow::bail!("control TLS CA bundle contains no valid trust anchors");
    }
    Ok(roots)
}

fn load_certificates(config: &ControlTlsConfig) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_file_iter(&config.cert_path)
        .with_context(|| format!("open control TLS certificate chain {:?}", config.cert_path))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("parse control TLS certificate chain")?;
    if certificates.is_empty() {
        anyhow::bail!("control TLS certificate chain is empty");
    }
    Ok(certificates)
}

fn load_private_key(config: &ControlTlsConfig) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(&config.key_path).context("parse control TLS private-key file")
}

fn peer_identity(
    certificates: Option<&[CertificateDer<'_>]>,
    trust_domain: &str,
    cluster_id: &str,
    role: WorkloadRole,
) -> Result<WorkloadIdentity> {
    let leaf = certificates
        .and_then(|certificates| certificates.first())
        .context("control TLS peer supplied no leaf certificate")?;
    let identity = certificate_identity(leaf)?;
    validate_peer_identity(&identity, trust_domain, cluster_id, role)?;
    Ok(identity)
}

fn certificate_identity(certificate: &CertificateDer<'_>) -> Result<WorkloadIdentity> {
    let certificate = Certificate::from_der(certificate.as_ref())
        .context("parse control TLS leaf certificate")?;
    let extension = certificate
        .tbs_certificate
        .extensions
        .as_ref()
        .and_then(|extensions| {
            extensions
                .iter()
                .find(|extension| extension.extn_id.to_string() == "2.5.29.17")
        })
        .context("control TLS leaf certificate has no subjectAltName")?;
    let san = SubjectAltName::from_der(extension.extn_value.as_bytes())
        .context("read control TLS URI SAN")?;
    let uris: Vec<_> = san
        .0
        .iter()
        .filter_map(|name| match name {
            GeneralName::UniformResourceIdentifier(uri)
                if uri.as_str().starts_with("spiffe://") =>
            {
                Some(uri.as_str())
            }
            _ => None,
        })
        .collect();
    if uris.len() != 1 {
        anyhow::bail!(
            "control TLS leaf certificate must contain exactly one Talon workload URI SAN"
        );
    }
    WorkloadIdentity::parse(uris[0]).context("parse control TLS workload URI SAN")
}

fn validate_peer_identity(
    identity: &WorkloadIdentity,
    trust_domain: &str,
    cluster_id: &str,
    role: WorkloadRole,
) -> Result<()> {
    if identity.trust_domain() != trust_domain {
        anyhow::bail!("control TLS peer trust domain does not match configuration");
    }
    if identity.cluster_id() != cluster_id {
        anyhow::bail!("control TLS peer belongs to a different cluster");
    }
    if identity.role() != role {
        anyhow::bail!(
            "control TLS peer has role {}, expected {role}",
            identity.role()
        );
    }
    Ok(())
}

#[derive(Debug)]
struct IdentityClientVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    trust_domain: String,
    cluster_id: String,
    role: WorkloadRole,
}

impl ClientCertVerifier for IdentityClientVerifier {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, rustls::Error> {
        self.inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let identity = certificate_identity(end_entity)
            .map_err(|error| rustls::Error::General(error.to_string()))?;
        validate_peer_identity(&identity, &self.trust_domain, &self.cluster_id, self.role)
            .map_err(|error| rustls::Error::General(error.to_string()))?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[derive(Debug)]
struct IdentityServerVerifier {
    inner: Arc<dyn ServerCertVerifier>,
    roots: Arc<RootCertStore>,
    trust_domain: String,
    cluster_id: String,
    role: WorkloadRole,
}

impl ServerCertVerifier for IdentityServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let provider = rustls::crypto::ring::default_provider();
        let certificate = webpki::EndEntityCert::try_from(end_entity)
            .map_err(|error| rustls::Error::General(error.to_string()))?;
        certificate
            .verify_for_usage(
                provider.signature_verification_algorithms.all,
                &self.roots.roots,
                intermediates,
                now,
                webpki::KeyUsage::server_auth(),
                None,
                None,
            )
            .map_err(|error| rustls::Error::General(error.to_string()))?;
        let identity = certificate_identity(end_entity)
            .map_err(|error| rustls::Error::General(error.to_string()))?;
        validate_peer_identity(&identity, &self.trust_domain, &self.cluster_id, self.role)
            .map_err(|error| rustls::Error::General(error.to_string()))?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

    const CA_DER: &[u8] = include_bytes!("../tests/fixtures/control_tls/ca.der");
    const COORDINATOR_CERT_DER: &[u8] =
        include_bytes!("../tests/fixtures/control_tls/coordinator.der");
    const COORDINATOR_KEY_DER: &[u8] =
        include_bytes!("../tests/fixtures/control_tls/coordinator-key.der");
    const WORKER_CERT_DER: &[u8] = include_bytes!("../tests/fixtures/control_tls/worker.der");
    const WORKER_KEY_DER: &[u8] = include_bytes!("../tests/fixtures/control_tls/worker-key.der");

    fn pem(label: &str, der: &[u8]) -> String {
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            STANDARD.encode(der)
        )
    }

    fn write_identity(root: &Path, role: WorkloadRole) -> ControlTlsConfig {
        let (certificate, key) = match role {
            WorkloadRole::Coordinator => (COORDINATOR_CERT_DER, COORDINATOR_KEY_DER),
            WorkloadRole::Worker => (WORKER_CERT_DER, WORKER_KEY_DER),
        };
        fs::create_dir_all(root).unwrap();
        let ca_path = root.join("ca.pem");
        let cert_path = root.join("cert.pem");
        let key_path = root.join("key.pem");
        fs::write(&ca_path, pem("CERTIFICATE", CA_DER)).unwrap();
        fs::write(&cert_path, pem("CERTIFICATE", certificate)).unwrap();
        fs::write(&key_path, pem("PRIVATE KEY", key)).unwrap();
        ControlTlsConfig {
            ca_cert_path: ca_path,
            cert_path,
            key_path,
            trust_domain: "cluster.example".to_owned(),
        }
    }

    fn identity(role: WorkloadRole, node: &str) -> WorkloadIdentity {
        WorkloadIdentity::new("cluster.example", "cluster-a", role, node).unwrap()
    }

    #[tokio::test]
    async fn mutually_authenticates_canonical_workload_identities() {
        let temp = TempDir::new().unwrap();
        let coordinator = identity(WorkloadRole::Coordinator, "coordinator-1");
        let worker = identity(WorkloadRole::Worker, "worker-1");
        let coordinator_channel = ControlTlsChannel::load(
            write_identity(&temp.path().join("coordinator"), WorkloadRole::Coordinator),
            coordinator.clone(),
            WorkloadRole::Worker,
            Duration::from_secs(3600),
        )
        .unwrap();
        let worker_channel = ControlTlsChannel::load(
            write_identity(&temp.path().join("worker"), WorkloadRole::Worker),
            worker.clone(),
            WorkloadRole::Coordinator,
            Duration::from_secs(3600),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            coordinator_channel.accept(stream).await.unwrap().identity
        });

        let authenticated = worker_channel.connect(address).await.unwrap();
        assert_eq!(authenticated.identity, coordinator);
        assert_eq!(server.await.unwrap(), worker);
    }

    #[tokio::test]
    async fn rejects_plaintext_and_wrong_role_before_dispatch() {
        let temp = TempDir::new().unwrap();
        let coordinator = identity(WorkloadRole::Coordinator, "coordinator-1");
        let channel = ControlTlsChannel::load(
            write_identity(&temp.path().join("coordinator"), WorkloadRole::Coordinator),
            coordinator,
            WorkloadRole::Worker,
            Duration::from_secs(3600),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            channel.accept(stream).await
        });
        let mut plaintext = TcpStream::connect(address).await.unwrap();
        plaintext.write_all(b"not tls").await.unwrap();
        plaintext.shutdown().await.unwrap();
        assert!(server.await.unwrap().is_err());

        let coordinator_server = identity(WorkloadRole::Coordinator, "coordinator-1");
        let coordinator_client = coordinator_server.clone();
        let server_channel = ControlTlsChannel::load(
            write_identity(&temp.path().join("server"), WorkloadRole::Coordinator),
            coordinator_server,
            WorkloadRole::Worker,
            Duration::from_secs(3600),
        )
        .unwrap();
        let client_channel = ControlTlsChannel::load(
            write_identity(&temp.path().join("client"), WorkloadRole::Coordinator),
            coordinator_client,
            WorkloadRole::Coordinator,
            Duration::from_secs(3600),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            server_channel.accept(stream).await
        });
        let _ = client_channel.connect(address).await;
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn reload_retains_last_good_material() {
        let temp = TempDir::new().unwrap();
        let worker = identity(WorkloadRole::Worker, "worker-1");
        let config = write_identity(temp.path(), WorkloadRole::Worker);
        let channel = ControlTlsChannel::load(
            config.clone(),
            worker.clone(),
            WorkloadRole::Coordinator,
            Duration::from_secs(1),
        )
        .unwrap();
        let initial = channel.material.borrow().clone();
        fs::write(&config.cert_path, "not a certificate").unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(Arc::ptr_eq(&initial, &channel.material.borrow()));

        write_identity(temp.path(), WorkloadRole::Worker);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!Arc::ptr_eq(&initial, &channel.material.borrow()));
    }
}
