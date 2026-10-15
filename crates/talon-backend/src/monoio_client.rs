//! Experimental HTTP/1.1 origin transport. TCP and TLS use Monoio on a strict
//! caller-owned io_uring ring. DNS stays on Tokio's resolver. Streaming request
//! bodies and file uploads are unsupported and use the HttpClient default errors.
use crate::http::{HttpClient, HttpRequest, HttpResponse, Method};
use async_trait::async_trait;
use monoio::io::sink::SinkExt;
mod body;
mod ktls;
mod policy;
mod pool;
pub use policy::{KernelTlsCapabilities, TlsVersion};
mod tls;
use body::OriginIo;
use monoio_http::{
    common::body::{FixedBody, HttpBody},
    h1::codec::encoder::GenericEncoder,
};
use std::{collections::HashMap, sync::Arc, time::Duration};

enum Connection {
    Plain(monoio::net::TcpStream),
    Tls(Box<tls::TlsStream>),
    Ktls(Box<ktls::KtlsStream>),
}

/// Origin record-layer policy. Auto prefers kTLS using the startup snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TlsMode {
    #[default]
    Auto,
    Rustls,
    Ktls,
}

pub struct MonoioClient {
    tls: Arc<rustls::ClientConfig>,
    tls_mode: TlsMode,
    ktls_no_pad: bool,
    capabilities: KernelTlsCapabilities,
    minimum_tls: TlsVersion,
    endpoint_minimums: HashMap<String, TlsVersion>,
}

impl Default for MonoioClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MonoioClient {
    pub fn new() -> Self {
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("ring TLS protocols")
        .with_root_certificates(roots)
        .with_no_client_auth();
        Self::with_tls_config(Arc::new(config))
    }

    /// Explicit trust configuration, also used by local HTTPS integration tests.
    pub fn with_tls_config(tls: Arc<rustls::ClientConfig>) -> Self {
        let mut tls = (*tls).clone();
        // This experiment implements HTTP/1.1, so never negotiate h2.
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        tls.enable_secret_extraction = true;
        Self {
            tls: Arc::new(tls),
            tls_mode: TlsMode::Auto,
            ktls_no_pad: false,
            capabilities: KernelTlsCapabilities::default(),
            minimum_tls: TlsVersion::default(),
            endpoint_minimums: HashMap::new(),
        }
    }

    /// Select a record layer; kTLS mode fails explicitly if offload is unavailable.
    pub fn with_tls_mode(mut self, mode: TlsMode) -> Self {
        self.tls_mode = mode;
        let mut tls = (*self.tls).clone();
        tls.enable_secret_extraction = mode != TlsMode::Rustls;
        self.tls = Arc::new(tls); // Also isolates the pool from the prior mode.
        self
    }

    /// TLS 1.3 only: opt in for trusted origins known to send unpadded records.
    pub fn with_ktls_no_pad(mut self, enabled: bool) -> Self {
        self.ktls_no_pad = enabled;
        self.tls = Arc::new((*self.tls).clone());
        self
    }

    /// Install the snapshot collected once during service startup. No request
    /// performs capability probing; without a snapshot auto uses rustls.
    pub fn with_kernel_tls_capabilities(mut self, capabilities: KernelTlsCapabilities) -> Self {
        self.capabilities = capabilities;
        self.tls = Arc::new((*self.tls).clone());
        self
    }

    pub fn with_tls_min_version(mut self, minimum: TlsVersion) -> Self {
        self.minimum_tls = minimum;
        self.tls = Arc::new((*self.tls).clone());
        self
    }

    /// Raise the global floor for one HTTPS origin (host and effective port).
    pub fn with_endpoint_tls_min_version(
        mut self,
        endpoint: &str,
        minimum: TlsVersion,
    ) -> Result<Self, &'static str> {
        self.endpoint_minimums
            .insert(policy::endpoint_key(endpoint)?, minimum);
        self.tls = Arc::new((*self.tls).clone());
        Ok(self)
    }

    async fn send_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        talon_uring::require_ring().map_err(|e| e.to_string())?;
        // Parse without normalizing the signed path/query. Never include the
        // URL, authorization header, or SAS token in a transport error.
        let uri: http::Uri = request.url.parse().map_err(|_| "invalid origin URI")?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err("origin URI requires http or https".into()),
        };
        let host = uri
            .host()
            .ok_or("origin URI has no host")?
            .trim_matches(['[', ']'])
            .to_owned();
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let tls = self.tls.clone();
        let download = talon_uring::Download::current();
        if let Some(download) = &download {
            download.reset();
        }
        let tls_mode = self.tls_mode;
        let minimum = self.minimum_tls.max(
            self.endpoint_minimums
                .get(&policy::authority_key(&host, port))
                .copied()
                .unwrap_or_default(),
        );
        let capabilities = self.capabilities.clone();
        let no_pad = self.ktls_no_pad;
        let tokio = tokio::runtime::Handle::current();
        talon_uring::spawn_cancellable(async move {
                let operation = async {
                    let authority = format!(
                        "{}://{}",
                        uri.scheme_str().unwrap(),
                        uri.authority().unwrap()
                    );
                    // GET/HEAD responses are fully assembled before returning any
                    // bytes to the cache. A KeyUpdate reconnect discards that
                    // incomplete response and replays once on a fresh kTLS socket.
                    let replayable = matches!(request.method, Method::Get | Method::Head);
                    let mut replayed_key_update = false;
                    loop {
                        let pooled = if replayed_key_update { None } else { pool::take(&tls, &authority).await };
                        let mut connection = if let Some(connection) = pooled {
                            connection
                        } else {
                            monoio::time::timeout(Duration::from_secs(10), async {
                                // DNS can block in libc; resolve on Tokio's resolver before
                                // opening the payload socket on this ring.
                                let addresses = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                                    vec![std::net::SocketAddr::new(ip, port)]
                                } else {
                                    let lookup = host.clone();
                                    tokio
                                        .spawn(async move {
                                            tokio::net::lookup_host((lookup.as_str(), port))
                                                .await
                                                .map(|it| it.collect::<Vec<_>>())
                                        })
                                        .await
                                        .map_err(|_| "origin resolver stopped")?
                                        .map_err(|_| "origin DNS lookup failed")?
                                };
                                let name = https.then(|| rustls::pki_types::ServerName::try_from(host.clone()))
                                    .transpose().map_err(|_| "invalid TLS server name".to_owned())?;
                                // Pure lookups only: all kernel probes finished at startup.
                                let allowed = if https { policy::minimum(&tls, minimum)? } else { tls.clone() };
                                let preferred = if https && tls_mode != TlsMode::Rustls {
                                    capabilities.select(&allowed)
                                } else { None };
                                if https && tls_mode == TlsMode::Ktls && preferred.is_none() {
                                    return Err("origin kTLS unavailable for allowed protocols/ciphers (no matching startup capability)".into());
                                }
                                let mut use_kernel = preferred.is_some();
                                loop {
                                    let mut socket = None;
                                    for address in &addresses {
                                        if let Ok(stream) = monoio::net::TcpStream::connect(*address).await {
                                            socket = Some(stream);
                                            break;
                                        }
                                    }
                                    let stream = socket.ok_or("origin TCP connect failed")?;
                                    stream.set_nodelay(true).map_err(|_| "origin TCP configuration failed")?;
                                    if !https { break Ok::<_, String>(Connection::Plain(stream)); }
                                    let config = if use_kernel { preferred.as_ref().unwrap() } else { &allowed };
                                    let stream = match tls::TlsStream::connect(stream, config.clone(), name.clone().expect("HTTPS name")).await {
                                        Ok(stream) => stream,
                                        Err(error) if use_kernel && tls_mode == TlsMode::Auto && policy::negotiation_rejected(&error) => {
                                            // The peer may require a different version/cipher. Retry
                                            // once with the original allowed set, never a lower floor.
                                            // Certificate/authentication failures do not take this path.
                                            tracing::debug!("origin rejected kTLS-preferred negotiation; reconnecting with rustls");
                                            use_kernel = false;
                                            continue;
                                        }
                                        Err(_) => return Err("origin TLS handshake/certificate verification failed".into()),
                                    };
                                    tracing::debug!(protocol = ?stream.protocol(), origin_tls = if use_kernel { "ktls" } else { "rustls" }, "origin TLS negotiated");
                                    if !use_kernel { break Ok(Connection::Tls(Box::new(stream))); }
                                    let rekey_supported = stream.protocol().1.is_some_and(|suite| capabilities.supports_key_update(suite));
                                    match ktls::KtlsStream::connect(stream, no_pad, rekey_supported).await {
                                        Ok(stream) => {
                                            tracing::debug!(origin_tls = "ktls", "origin TLS record layer selected");
                                            break Ok(Connection::Ktls(Box::new(stream)));
                                        }
                                        Err(error) if tls_mode == TlsMode::Auto => {
                                            // Handoff may have installed only one direction. Drop
                                            // it and redo the handshake on a fresh socket. No HTTP
                                            // bytes have been sent, so this cannot replay a request.
                                            tracing::debug!(origin_tls = "rustls", reason = %error, "kTLS unavailable; reconnecting with rustls");
                                            use_kernel = false;
                                        }
                                        Err(error) => break Err(format!("origin kTLS setup failed: {error}")),
                                    }
                                }
                            })
                            .await
                            .map_err(|_| "origin connect timed out")??
                        };
                        let result = match &mut connection {
                            Connection::Plain(codec) => exchange(codec, uri.clone(), request.clone(), download.as_ref()).await,
                            Connection::Tls(codec) => exchange(codec.as_mut(), uri.clone(), request.clone(), download.as_ref()).await,
                            Connection::Ktls(codec) => exchange(codec.as_mut(), uri.clone(), request.clone(), download.as_ref()).await,
                        };
                        if result.is_err() && replayable && !replayed_key_update
                            && matches!(&connection, Connection::Ktls(stream) if stream.needs_key_update_reconnect())
                        {
                            // No rustls fallback and no reuse of this socket: the new
                            // handshake derives fresh keys and uses the same kTLS policy.
                            drop(connection);
                            replayed_key_update = true;
                            tracing::debug!("origin KeyUpdate requires reconnect; replaying GET/HEAD once on fresh kTLS connection");
                            continue;
                        }
                        let (response, reusable) = result?;
                        if reusable {
                            pool::put(&tls, authority, connection);
                        }
                        return Ok(response);
                    }

                };
                monoio::time::timeout(Duration::from_secs(300), operation)
                    .await
                    .map_err(|_| "origin io_uring request timed out".to_owned())?
            })
            .await
            .map_err(|e| format!("origin ring task stopped: {e}"))?
    }
}

async fn exchange<IO: OriginIo>(
    io: &mut IO,
    uri: http::Uri,
    request: HttpRequest,
    download: Option<&talon_uring::Download>,
) -> Result<(HttpResponse, bool), String> {
    let range_bound = request.headers.iter().find_map(|(name, value)| {
        if !name.eq_ignore_ascii_case("range") && !name.eq_ignore_ascii_case("x-ms-range") {
            return None;
        }
        let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
        let start: usize = start.parse().ok()?;
        let end: usize = end.parse().ok()?;
        end.checked_sub(start)?.checked_add(1)
    });
    let method = match request.method {
        Method::Get => http::Method::GET,
        Method::Head => http::Method::HEAD,
        Method::Put => http::Method::PUT,
        Method::Post => http::Method::POST,
        Method::Delete => http::Method::DELETE,
    };
    let head = method == http::Method::HEAD;
    let mut builder = http::Request::builder()
        .method(method)
        .version(http::Version::HTTP_11)
        .uri(uri.path_and_query().map(|p| p.as_str()).unwrap_or("/"));
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    let headers = builder
        .headers_mut()
        .ok_or("invalid origin request headers")?;
    if !headers.contains_key(http::header::HOST) {
        headers.insert(
            http::header::HOST,
            uri.authority()
                .ok_or("origin URI has no authority")?
                .as_str()
                .parse()
                .map_err(|_| "invalid origin authority")?,
        );
    }
    let close = connection_close(headers);
    let request = builder
        .body(HttpBody::fixed_body(Some(request.body)))
        .map_err(|_| "invalid origin request")?;
    GenericEncoder::new(&mut *io)
        .send_and_flush(request)
        .await
        .map_err(|_| "origin HTTP write failed")?;
    // Informational responses carry no payload. Bound the number of interim heads.
    let mut response = None;
    for _ in 0..16 {
        let next = body::head(io).await?;
        if !next.status.is_informational() {
            response = Some(next);
            break;
        }
        if next.status == http::StatusCode::SWITCHING_PROTOCOLS {
            return Err("origin protocol upgrade is unsupported".into());
        }
    }
    let parts = response.ok_or("too many origin informational responses")?;
    let no_body = head
        || parts.status == http::StatusCode::NO_CONTENT
        || parts.status == http::StatusCode::NOT_MODIFIED;
    if parts
        .headers
        .get_all(http::header::CONTENT_LENGTH)
        .iter()
        .count()
        > 1
        || (parts.headers.contains_key(http::header::CONTENT_LENGTH)
            && parts.headers.contains_key(http::header::TRANSFER_ENCODING))
    {
        return Err("ambiguous origin HTTP body framing".into());
    }
    // The native codec supports Content-Length/chunked bodies. Reject unsupported
    // EOF-delimited bodies rather than silently committing an empty response.
    if !no_body
        && !parts.headers.contains_key(http::header::CONTENT_LENGTH)
        && !parts.headers.contains_key(http::header::TRANSFER_ENCODING)
    {
        return Err("origin response requires Content-Length or chunked framing".into());
    }
    let body = if no_body {
        bytes::Bytes::new()
    } else {
        body::payload(
            io,
            &parts.headers,
            if parts.status.is_success() {
                range_bound
            } else {
                None
            },
            download,
        )
        .await?
    };
    // HEAD has different body semantics from the generic native decoder; use
    // a fresh connection next time. Errors/cancellation always drop the codec.
    let reusable = !close
        && !head
        && parts.version == http::Version::HTTP_11
        && !connection_close(&parts.headers);
    Ok((
        HttpResponse {
            status: parts.status.as_u16(),
            headers: parts
                .headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_owned()))
                .collect(),
            body,
        },
        reusable,
    ))
}

fn connection_close(headers: &http::HeaderMap) -> bool {
    headers
        .get_all(http::header::CONNECTION)
        .iter()
        .any(|value| {
            value.to_str().map_or(true, |text| {
                text.split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("close"))
            })
        })
}

#[async_trait]
impl HttpClient for MonoioClient {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        let operation = talon_telemetry::Operation::new(
            "HTTP attempt",
            "client",
            talon_telemetry::TraceParent::Inherit,
        );
        operation.text("talon.http.attempt_boundary", "monoio.execute");
        let result = operation
            .scope(async {
                talon_uring::timeout(Duration::from_secs(300), self.send_request(request))
                    .await
                    .map_err(|_| "origin request timed out".to_owned())?
            })
            .await;
        if let Ok(response) = &result {
            operation.record("http.response.status_code", response.status as u64);
            operation.record("talon.origin.body_bytes", response.body.len() as u64);
        }
        operation.outcome(if result.is_ok() { "success" } else { "error" });
        result
    }
}
