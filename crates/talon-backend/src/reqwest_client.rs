//! A concrete [`HttpClient`] backed by [`reqwest`] (rustls TLS).
//!
//! The backends ([`AzureBackend`](crate::AzureBackend) etc.) are generic over
//! [`HttpClient`] so they stay offline-testable with a mock; this is the real
//! networked implementation wired in production. It performs a ranged GET or a
//! HEAD over HTTPS and maps the response into the crate's transport-agnostic
//! [`HttpResponse`].
//!
//! Only the pieces the backends need are implemented (GET/HEAD, request headers,
//! status + response headers + body). Auth is expected to be baked into the URL
//! (e.g. an Azure SAS query string) or carried in `HttpRequest::headers`; this
//! client does no signing of its own.

use async_trait::async_trait;

use crate::http::{HttpClient, HttpRequest, HttpResponse, Method};

/// A `reqwest`-backed HTTP client.
pub struct ReqwestClient {
    inner: reqwest::Client,
}

impl ReqwestClient {
    /// Build a client with sensible pooled defaults.
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::new(),
        }
    }

    /// Build over a pre-configured [`reqwest::Client`] (timeouts, proxies, etc.).
    pub fn with_client(inner: reqwest::Client) -> Self {
        Self { inner }
    }
}

impl Default for ReqwestClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpClient for ReqwestClient {
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String> {
        let method = match req.method {
            Method::Get => reqwest::Method::GET,
            Method::Head => reqwest::Method::HEAD,
        };
        let mut builder = self.inner.request(method, &req.url);
        for (k, v) in &req.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        let resp = builder.send().await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = resp.bytes().await.map_err(|e| e.to_string())?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}
