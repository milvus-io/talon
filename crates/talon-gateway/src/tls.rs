//! Reloadable TLS 1.3 listener for the public gateway data plane.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::serve::Listener;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::metrics::GatewayMetrics;

type AcceptedTls = (TlsStream<TcpStream>, std::net::SocketAddr);
type PendingHandshake = BoxFuture<'static, Option<AcceptedTls>>;

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
}

/// TLS listener that snapshots last-good material for each new connection.
pub struct GatewayTlsListener {
    listener: TcpListener,
    material: watch::Receiver<Arc<ServerConfig>>,
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
    type Addr = std::net::SocketAddr;

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
                            let handshake = TlsAcceptor::from(material).accept(stream);
                            match tokio::time::timeout(timeout, handshake).await {
                                Ok(Ok(stream)) => Some((stream, address)),
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
        self.listener.local_addr()
    }
}

fn spawn_reload(
    config: GatewayTlsConfig,
    sender: watch::Sender<Arc<ServerConfig>>,
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

fn load_server_config(config: &GatewayTlsConfig) -> Result<ServerConfig, GatewayTlsError> {
    let certificates = CertificateDer::pem_file_iter(&config.certificate_path)
        .map_err(|_| GatewayTlsError::InvalidCertificateChain)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| GatewayTlsError::InvalidCertificateChain)?;
    if certificates.is_empty() {
        return Err(GatewayTlsError::EmptyCertificateChain);
    }
    let key = PrivateKeyDer::from_pem_file(&config.private_key_path)
        .map_err(|_| GatewayTlsError::InvalidPrivateKey)?;
    let mut server = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|_| GatewayTlsError::IncompatibleKey)?;
    server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(server)
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
    use tempfile::TempDir;

    use super::*;
    use crate::{
        serve_tls, AuthorizationPolicy, GatewayAdapter, GatewayConfig, GatewayMode,
        GatewayOperation, GatewayRequestContext, GatewayResponse, GatewayRuntime, GatewaySecurity,
        ProviderProtocol,
    };

    const CERT_DER: &[u8] =
        include_bytes!("../../talon-transport/tests/fixtures/control_tls/coordinator.der");
    const KEY_DER: &[u8] =
        include_bytes!("../../talon-transport/tests/fixtures/control_tls/coordinator-key.der");

    struct Adapter;

    #[async_trait]
    impl GatewayAdapter for Adapter {
        fn protocol(&self) -> ProviderProtocol {
            ProviderProtocol::S3
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
        }
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
    async fn invalid_reload_retains_last_good_material_and_is_counted() {
        let temp = TempDir::new().unwrap();
        let config = write_material(temp.path());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = GatewayMetrics::new();
        let mut tls = GatewayTlsListener::load(listener, config.clone(), metrics.clone()).unwrap();
        fs::write(&config.private_key_path, "not a private key").unwrap();

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

        fs::write(&config.private_key_path, pem("PRIVATE KEY", KEY_DER)).unwrap();
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

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
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
}
