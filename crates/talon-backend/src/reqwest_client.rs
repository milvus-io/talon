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
        let resp = builder.send().await.map_err(sanitize_error)?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = resp.bytes().await.map_err(sanitize_error)?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

/// Stringify a `reqwest::Error` **without its URL**.
///
/// `reqwest::Error`'s `Display` embeds the request URL, and the backend URL can
/// carry an Azure SAS token (or other query-string credential). This error is
/// returned verbatim to a credential-less client over the data plane, so a
/// transport-layer failure (DNS/TLS/connect/timeout) must never leak the
/// SAS-bearing URL. `without_url()` strips it before stringifying (issue #116).
fn sanitize_error(error: reqwest::Error) -> String {
    error.without_url().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpRequest;

    #[tokio::test]
    async fn transport_error_does_not_leak_sas_url() {
        // A connect failure to a URL carrying a SAS-like query string must not
        // surface that URL (and thus the token) in the returned error string.
        let secret = "sig=SUPERSECRETsignature123&se=2030-01-01";
        let url = format!("https://nonexistent-host.invalid.example/container/blob.bin?{secret}");
        let client = ReqwestClient::new();
        let err = client
            .execute(HttpRequest {
                method: Method::Get,
                url,
                headers: Vec::new(),
            })
            .await
            .expect_err("request to an unresolvable host must fail");
        assert!(
            !err.contains("SUPERSECRETsignature123"),
            "error leaked the SAS token: {err}"
        );
        assert!(
            !err.contains("nonexistent-host.invalid.example"),
            "error leaked the URL: {err}"
        );
    }
}
