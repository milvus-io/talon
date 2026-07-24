//! Azure Blob Storage [`BackendStore`] implementation.
//!
//! Fetches block/page ranges via an Azure Blob **ranged GET** and metadata via
//! **HEAD**. Azure uses the `x-ms-range` header (rather than `Range`) and
//! addresses blobs as `https://<account>.blob.core.windows.net/<container>/<blob>`.
//! The blob's ETag maps to a [`Version`] so an overwrite invalidates the cache
//! key.
//!
//! The `ObjectId::bucket` field carries the **container** name for Azure (the
//! account is part of the endpoint host). Like the other backends this is
//! generic over an [`HttpClient`] and unit-testable offline; live shared-key /
//! SAS signing is left to the concrete networked client.

use std::sync::Arc;

use async_trait::async_trait;
use talon_core::{BackendStore, Error, ObjectId, ObjectStat, Result, Version};

use crate::http::{HttpClient, HttpRequest, Method};

/// Azure Blob endpoint configuration.
#[derive(Debug, Clone)]
pub struct AzureConfig {
    /// Storage account name (forms the endpoint host).
    pub account: String,
    /// Endpoint suffix; defaults to `blob.core.windows.net`.
    pub endpoint_suffix: String,
    /// `https` when true.
    pub tls: bool,
}

impl AzureConfig {
    /// Default public-cloud config for an account.
    pub fn new(account: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            endpoint_suffix: "blob.core.windows.net".into(),
            tls: true,
        }
    }
}

/// An Azure Blob `BackendStore` over a pluggable HTTP client.
pub struct AzureBackend {
    config: AzureConfig,
    /// Optional SAS token query string (without leading `?`), or None for
    /// shared-key auth handled by the networked client.
    sas_token: Option<String>,
    http: Arc<dyn HttpClient>,
}

impl AzureBackend {
    /// Construct from config, an optional SAS token, and an HTTP client.
    pub fn new(config: AzureConfig, sas_token: Option<String>, http: Arc<dyn HttpClient>) -> Self {
        Self {
            config,
            sas_token,
            http,
        }
    }

    /// Build the blob URL: `scheme://<account>.<suffix>/<container>/<blob>`
    /// with the SAS query appended when present.
    pub fn blob_url(&self, obj: &ObjectId) -> String {
        let scheme = if self.config.tls { "https" } else { "http" };
        let blob = obj.object_path.trim_start_matches('/');
        let base = format!(
            "{scheme}://{}.{}/{}/{}",
            self.config.account, self.config.endpoint_suffix, obj.bucket, blob
        );
        match &self.sas_token {
            Some(sas) => format!("{base}?{sas}"),
            None => base,
        }
    }

    /// Format the Azure `x-ms-range` header value for `[offset, offset+len)`.
    pub fn range_header(offset: u64, len: u64) -> String {
        format!("bytes={offset}-{}", offset + len - 1)
    }

    /// The API version header Azure requires on every request.
    const API_VERSION: &'static str = "2021-12-02";

    fn common_headers(&self) -> Vec<(String, String)> {
        vec![("x-ms-version".to_string(), Self::API_VERSION.to_string())]
    }

    /// Build the ranged GET request (exposed for testing).
    pub fn build_get(&self, obj: &ObjectId, offset: u64, len: u64) -> HttpRequest {
        let mut headers = self.common_headers();
        headers.push(("x-ms-range".to_string(), Self::range_header(offset, len)));
        HttpRequest {
            method: Method::Get,
            url: self.blob_url(obj),
            headers,
        }
    }

    /// Build the HEAD request (exposed for testing).
    pub fn build_head(&self, obj: &ObjectId) -> HttpRequest {
        HttpRequest {
            method: Method::Head,
            url: self.blob_url(obj),
            headers: self.common_headers(),
        }
    }
}

#[async_trait]
impl BackendStore for AzureBackend {
    async fn fetch_range(&self, obj: &ObjectId, offset: u64, len: u64) -> Result<bytes::Bytes> {
        if len == 0 {
            return Ok(bytes::Bytes::new());
        }
        let resp = self
            .http
            .execute(self.build_get(obj, offset, len))
            .await
            .map_err(Error::Backend)?;
        match resp.status {
            200 | 206 => {
                let content_range = resp.header("content-range").map(str::to_owned);
                crate::http::range_body(
                    resp.status,
                    resp.body,
                    offset,
                    len,
                    content_range.as_deref(),
                )
                .map_err(Error::Backend)
            }
            404 => Err(Error::NotFound(obj.to_path())),
            s => Err(Error::Backend(format!(
                "Azure GET {} -> HTTP {s}",
                obj.to_path()
            ))),
        }
    }

    async fn head(&self, obj: &ObjectId) -> Result<ObjectStat> {
        let resp = self
            .http
            .execute(self.build_head(obj))
            .await
            .map_err(Error::Backend)?;
        if resp.status == 404 {
            return Err(Error::NotFound(obj.to_path()));
        }
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "Azure HEAD {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        let len = resp
            .header("content-length")
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| Error::Backend("Azure HEAD missing/invalid Content-Length".into()))?;
        let version = resp
            .header("etag")
            .map(|v| Version::new(v.trim_matches('"').to_string()))
            .filter(|v| !v.0.trim().is_empty())
            .ok_or_else(|| {
                Error::Backend(format!(
                    "Azure HEAD {} returned no ETag; refusing to cache without a version",
                    obj.to_path()
                ))
            })?;
        Ok(ObjectStat { len, version })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpResponse;
    use std::sync::Mutex;
    use talon_core::Backend;

    struct MockHttp {
        last: Mutex<Option<HttpRequest>>,
        response: HttpResponse,
    }

    impl MockHttp {
        fn new(response: HttpResponse) -> Arc<Self> {
            Arc::new(Self {
                last: Mutex::new(None),
                response,
            })
        }
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn execute(&self, req: HttpRequest) -> std::result::Result<HttpResponse, String> {
            *self.last.lock().unwrap() = Some(req);
            Ok(self.response.clone())
        }
    }

    fn obj() -> ObjectId {
        // bucket == Azure container.
        ObjectId::new(Backend::Azure, "my-container", "data/checkpoint.bin")
    }

    #[test]
    fn blob_url_with_and_without_sas() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("myacct"), None, http.clone());
        assert_eq!(
            a.blob_url(&obj()),
            "https://myacct.blob.core.windows.net/my-container/data/checkpoint.bin"
        );
        let with_sas = AzureBackend::new(
            AzureConfig::new("myacct"),
            Some("sig=abc&se=x".into()),
            http,
        );
        assert_eq!(
            with_sas.blob_url(&obj()),
            "https://myacct.blob.core.windows.net/my-container/data/checkpoint.bin?sig=abc&se=x"
        );
    }

    #[tokio::test]
    async fn fetch_range_uses_x_ms_range_and_version_header() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"az-bytes"),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http.clone());
        let got = a.fetch_range(&obj(), 8, 8).await.unwrap();
        assert_eq!(got, bytes::Bytes::from_static(b"az-bytes"));
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("x-ms-range"), Some("bytes=8-15"));
        assert_eq!(req.header("x-ms-version"), Some("2021-12-02"));
    }

    #[tokio::test]
    async fn head_parses_len_and_etag() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![
                ("Content-Length".into(), "512".into()),
                ("ETag".into(), "\"0x8DABCDEF\"".into()),
            ],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http);
        let stat = a.head(&obj()).await.unwrap();
        assert_eq!(stat.len, 512);
        assert_eq!(stat.version, Version::new("0x8DABCDEF"));
    }

    #[tokio::test]
    async fn not_found_maps_through() {
        let http = MockHttp::new(HttpResponse {
            status: 404,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http);
        assert!(matches!(
            a.fetch_range(&obj(), 0, 8).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(a.head(&obj()).await, Err(Error::NotFound(_))));
    }
}
