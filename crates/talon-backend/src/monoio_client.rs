//! Experimental HTTP/1.1 origin transport. TCP and TLS use Monoio on a strict
//! io_uring executor. DNS stays on Tokio's resolver. Upload streaming retains
//! the existing reqwest implementation; downloaded object bodies never use it.
use crate::http::{HttpClient, HttpRequest, HttpRequestBody, HttpResponse, Method};
use async_trait::async_trait;
use bytes::BytesMut;
use monoio::io::{sink::SinkExt, stream::Stream, AsyncReadRent, AsyncWriteRent, Split};
use monoio_http::{
    common::body::{Body, FixedBody, HttpBody},
    h1::codec::ClientCodec,
};
use std::{
    cell::RefCell,
    path::Path,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

enum Connection {
    Plain(ClientCodec<monoio::net::TcpStream>),
    Tls(ClientCodec<monoio_rustls::ClientTlsStream<monoio::net::TcpStream>>),
}
struct Idle {
    owner: Weak<rustls::ClientConfig>,
    authority: String,
    since: Instant,
    connection: Connection,
}
thread_local! {
    // Ring-local sockets must never cross executor threads. Bound idle FDs
    // across all authorities, and isolate pools by TLS/trust configuration.
    static IDLE: RefCell<Vec<Idle>> = const { RefCell::new(Vec::new()) };
}

pub struct MonoioClient {
    executor: talon_uring::Executor,
    tls: Arc<rustls::ClientConfig>,
    uploads: crate::ReqwestClient,
}

impl MonoioClient {
    pub fn new(executor: talon_uring::Executor) -> Self {
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("ring TLS protocols")
        .with_root_certificates(roots)
        .with_no_client_auth();
        Self::with_tls_config(executor, Arc::new(config))
    }

    /// Explicit trust configuration, also used by local HTTPS integration tests.
    pub fn with_tls_config(
        executor: talon_uring::Executor,
        tls: Arc<rustls::ClientConfig>,
    ) -> Self {
        let mut tls = (*tls).clone();
        // This experiment implements HTTP/1.1, so never negotiate h2.
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self {
            executor,
            tls: Arc::new(tls),
            uploads: crate::ReqwestClient::new(),
        }
    }

    async fn download(&self, request: HttpRequest) -> Result<HttpResponse, String> {
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
        let tokio = tokio::runtime::Handle::current();
        self.executor
            .run_cancellable(move || async move {
                let operation = async {
                    let authority = format!(
                        "{}://{}",
                        uri.scheme_str().unwrap(),
                        uri.authority().unwrap()
                    );
                    let pooled = IDLE.with(|pool| {
                        let mut pool = pool.borrow_mut();
                        pool.retain(|entry| {
                            entry.owner.strong_count() > 0
                                && entry.since.elapsed() < Duration::from_secs(90)
                        });
                        let index = pool.iter().position(|entry| {
                            entry.authority == authority
                                && entry.owner.ptr_eq(&Arc::downgrade(&tls))
                        })?;
                        Some(pool.swap_remove(index).connection)
                    });
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
                            let mut socket = None;
                            for address in addresses {
                                match monoio::net::TcpStream::connect(address).await {
                                    Ok(stream) => {
                                        socket = Some(stream);
                                        break;
                                    }
                                    Err(_) => continue,
                                }
                            }
                            let stream = socket.ok_or("origin TCP connect failed")?;
                            stream
                                .set_nodelay(true)
                                .map_err(|_| "origin TCP configuration failed")?;
                            if https {
                                let name = rustls::pki_types::ServerName::try_from(host)
                                    .map_err(|_| "invalid TLS server name")?;
                                let stream = monoio_rustls::TlsConnector::from(tls.clone())
                                    .connect(name, stream)
                                    .await
                                    .map_err(|_| {
                                        "origin TLS handshake/certificate verification failed"
                                    })?;
                                Ok::<_, &str>(Connection::Tls(ClientCodec::new(stream)))
                            } else {
                                Ok(Connection::Plain(ClientCodec::new(stream)))
                            }
                        })
                        .await
                        .map_err(|_| "origin connect timed out")??
                    };
                    let (response, reusable) = match &mut connection {
                        Connection::Plain(codec) => exchange(codec, uri, request).await?,
                        Connection::Tls(codec) => exchange(codec, uri, request).await?,
                    };
                    if reusable {
                        IDLE.with(|pool| {
                            let mut pool = pool.borrow_mut();
                            if pool.len() < 64 {
                                pool.push(Idle {
                                    owner: Arc::downgrade(&tls),
                                    authority,
                                    since: Instant::now(),
                                    connection,
                                });
                            }
                        });
                    }
                    Ok(response)
                };
                monoio::time::timeout(Duration::from_secs(300), operation)
                    .await
                    .map_err(|_| "origin io_uring request timed out".to_owned())?
            })
            .await
            .map_err(|e| format!("origin io_uring executor: {e}"))?
    }
}

async fn exchange<IO: AsyncReadRent + AsyncWriteRent + Split>(
    codec: &mut ClientCodec<IO>,
    uri: http::Uri,
    request: HttpRequest,
) -> Result<(HttpResponse, bool), String> {
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
    codec
        .send_and_flush(request)
        .await
        .map_err(|_| "origin HTTP write failed")?;
    // Informational responses carry no payload. Bound the number of interim heads.
    let mut response = None;
    for _ in 0..16 {
        let next = codec
            .next()
            .await
            .ok_or("origin closed before response")?
            .map_err(|_| "invalid origin HTTP response")?;
        if !next.status().is_informational() {
            response = Some(next);
            break;
        }
        if next.status() == http::StatusCode::SWITCHING_PROTOCOLS {
            return Err("origin protocol upgrade is unsupported".into());
        }
    }
    let (parts, decoder) = response
        .ok_or("too many origin informational responses")?
        .into_parts();
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
    let mut chunks = Vec::new();
    if !no_body {
        let mut body = decoder.with_io(&mut *codec);
        while let Some(chunk) = body.next_data().await {
            chunks.push(chunk.map_err(|_| "origin HTTP body truncated or invalid")?);
        }
    }
    let body = if chunks.len() == 1 {
        chunks.pop().unwrap()
    } else {
        let mut joined = BytesMut::new();
        for chunk in chunks {
            joined.extend_from_slice(&chunk);
        }
        joined.freeze()
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
                talon_uring::timeout(Duration::from_secs(300), self.download(request))
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

    async fn execute_file(
        &self,
        request: HttpRequest,
        path: &Path,
        len: u64,
    ) -> Result<HttpResponse, String> {
        self.uploads.execute_file(request, path, len).await
    }
    async fn execute_body(
        &self,
        request: HttpRequest,
        body: HttpRequestBody,
        len: u64,
    ) -> Result<HttpResponse, String> {
        self.uploads.execute_body(request, body, len).await
    }
}
