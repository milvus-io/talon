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
//! generic over an [`HttpClient`] and unit-testable offline. Authentication is
//! either a SAS token (in the URL query) or Shared Key (see
//! [`crate::azure_sharedkey`]), signed just before each request executes.

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
    /// Optional literal endpoint host (`host` or `host:port`) that overrides the
    /// default `{account}.{endpoint_suffix}`. Set for emulators (Azurite) or a
    /// latency proxy in front of one. `None` uses the public-cloud host.
    pub endpoint_host: Option<String>,
    /// Use path-style addressing (`{host}/{account}/{container}/{blob}`) instead
    /// of virtual-host style (`{account}.{suffix}/{container}/{blob}`). Azurite
    /// and most non-Azure emulators require path-style.
    pub path_style: bool,
}

impl AzureConfig {
    /// Default public-cloud config for an account (virtual-host, HTTPS).
    pub fn new(account: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            endpoint_suffix: "blob.core.windows.net".into(),
            tls: true,
            endpoint_host: None,
            path_style: false,
        }
    }

    /// Point at an emulator/proxy: literal `host[:port]`, path-style addressing.
    ///
    /// `tls` selects the scheme (Azurite speaks plain HTTP by default). The
    /// account name is kept for the path segment and any shared-key signing done
    /// by a networked client.
    pub fn emulator(
        account: impl Into<String>,
        endpoint_host: impl Into<String>,
        tls: bool,
    ) -> Self {
        Self {
            account: account.into(),
            endpoint_suffix: "blob.core.windows.net".into(),
            tls,
            endpoint_host: Some(endpoint_host.into()),
            path_style: true,
        }
    }
}

/// An Azure Blob `BackendStore` over a pluggable HTTP client.
pub struct AzureBackend {
    config: AzureConfig,
    /// Optional SAS token query string (without leading `?`).
    sas_token: Option<String>,
    /// Optional base64 account key for Shared Key authorization (an alternative
    /// to SAS). When set, every request is signed just before execution.
    shared_key: Option<String>,
    http: Arc<dyn HttpClient>,
}

impl AzureBackend {
    /// Construct from config, an optional SAS token, and an HTTP client.
    pub fn new(config: AzureConfig, sas_token: Option<String>, http: Arc<dyn HttpClient>) -> Self {
        Self {
            config,
            sas_token,
            shared_key: None,
            http,
        }
    }

    /// Construct with a Shared Key (base64 account key) instead of a SAS token.
    /// Every request is signed with `Authorization: SharedKey` before execution.
    pub fn with_shared_key(
        config: AzureConfig,
        shared_key: impl Into<String>,
        http: Arc<dyn HttpClient>,
    ) -> Self {
        Self {
            config,
            sas_token: None,
            shared_key: Some(shared_key.into()),
            http,
        }
    }

    /// Build the blob URL with the SAS query appended when present.
    ///
    /// Virtual-host style (default): `scheme://{account}.{suffix}/{container}/{blob}`.
    /// Path-style (emulator): `scheme://{host}/{account}/{container}/{blob}`, using
    /// `endpoint_host` as the literal host.
    pub fn blob_url(&self, obj: &ObjectId) -> String {
        let scheme = if self.config.tls { "https" } else { "http" };
        let blob = obj.object_path.trim_start_matches('/');
        let host =
            self.config.endpoint_host.clone().unwrap_or_else(|| {
                format!("{}.{}", self.config.account, self.config.endpoint_suffix)
            });
        let base = if self.config.path_style {
            format!(
                "{scheme}://{host}/{}/{}/{}",
                self.config.account, obj.bucket, blob
            )
        } else {
            format!("{scheme}://{host}/{}/{}", obj.bucket, blob)
        };
        match &self.sas_token {
            Some(sas) => format!("{base}?{sas}"),
            None => base,
        }
    }

    /// Format the Azure `x-ms-range` header value for `[offset, offset+len)`.
    ///
    /// `len` is expected `> 0` (guarded in `fetch_range`); a zero/overflowing
    /// `len` is clamped with checked arithmetic so this public helper never
    /// under/overflows into a bogus range (#167).
    pub fn range_header(offset: u64, len: u64) -> String {
        let span = len.saturating_sub(1);
        let end = offset.saturating_add(span);
        format!("bytes={offset}-{end}")
    }

    /// The API version header Azure requires on every request.
    const API_VERSION: &'static str = "2021-12-02";

    fn common_headers(&self) -> Vec<(String, String)> {
        vec![("x-ms-version".to_string(), Self::API_VERSION.to_string())]
    }

    /// Apply Shared Key authorization to `req` when a shared key is configured:
    /// stamp `x-ms-date` (RFC 1123 GMT) and add the `Authorization: SharedKey`
    /// header. A SAS-only backend (no shared key) returns the request unchanged
    /// (the SAS travels in the URL query).
    fn authorized(&self, mut req: HttpRequest) -> HttpRequest {
        let Some(key) = &self.shared_key else {
            return req;
        };
        let date = crate::azure_sharedkey::rfc1123_date(std::time::SystemTime::now());
        req.headers.push(("x-ms-date".to_string(), date));
        // A misconfigured key surfaces as an auth failure at the server; we still
        // send the request rather than panic.
        if let Ok(auth) =
            crate::azure_sharedkey::authorization_header(&req, &self.config.account, key)
        {
            req.headers.push(("Authorization".to_string(), auth));
        }
        req
    }

    /// Build the ranged GET request (exposed for testing).
    pub fn build_get(&self, obj: &ObjectId, offset: u64, len: u64) -> HttpRequest {
        self.build_get_if_match(obj, offset, len, None)
    }

    /// Build the ranged GET request with an optional `If-Match` precondition.
    ///
    /// When set, Azure returns `412 Precondition Failed` if the blob's ETag no
    /// longer matches, i.e. it was overwritten since the version was resolved
    /// (issue #163).
    pub fn build_get_if_match(
        &self,
        obj: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> HttpRequest {
        let mut headers = self.common_headers();
        headers.push(("x-ms-range".to_string(), Self::range_header(offset, len)));
        if let Some(version) = if_match {
            headers.push(("If-Match".to_string(), format!("\"{}\"", version.as_str())));
        }
        HttpRequest {
            method: Method::Get,
            url: self.blob_url(obj),
            headers,
            body: bytes::Bytes::new(),
        }
    }

    /// Build the HEAD request (exposed for testing).
    pub fn build_head(&self, obj: &ObjectId) -> HttpRequest {
        HttpRequest {
            method: Method::Head,
            url: self.blob_url(obj),
            headers: self.common_headers(),
            body: bytes::Bytes::new(),
        }
    }

    /// Build the whole-blob PUT request (exposed for testing).
    ///
    /// Azure block-blob upload requires `x-ms-blob-type: BlockBlob`. When
    /// `if_match` is set, adds `If-Match` so the blob is only replaced if its ETag
    /// still matches (`412` otherwise, #226).
    pub fn build_put(
        &self,
        obj: &ObjectId,
        body: bytes::Bytes,
        if_match: Option<&Version>,
    ) -> HttpRequest {
        let mut headers = self.common_headers();
        headers.push(("x-ms-blob-type".to_string(), "BlockBlob".to_string()));
        headers.push(("Content-Length".to_string(), body.len().to_string()));
        if let Some(version) = if_match {
            headers.push(("If-Match".to_string(), format!("\"{}\"", version.as_str())));
        }
        HttpRequest {
            method: Method::Put,
            url: self.blob_url(obj),
            headers,
            body,
        }
    }

    /// Build the DELETE request (exposed for testing).
    pub fn build_delete(&self, obj: &ObjectId) -> HttpRequest {
        HttpRequest {
            method: Method::Delete,
            url: self.blob_url(obj),
            headers: self.common_headers(),
            body: bytes::Bytes::new(),
        }
    }
}

#[async_trait]
impl BackendStore for AzureBackend {
    async fn fetch_range(&self, obj: &ObjectId, offset: u64, len: u64) -> Result<bytes::Bytes> {
        self.fetch_range_if_match(obj, offset, len, None).await
    }

    async fn fetch_range_if_match(
        &self,
        obj: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> Result<bytes::Bytes> {
        if len == 0 {
            return Ok(bytes::Bytes::new());
        }
        let resp = self
            .http
            .execute(self.authorized(self.build_get_if_match(obj, offset, len, if_match)))
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
            // Precondition failed: the blob changed since the version was
            // resolved (issue #163). Report the new ETag if present.
            412 => Err(Error::VersionMismatch {
                expected: if_match.map(|v| v.0.clone()).unwrap_or_default(),
                found: resp
                    .header("etag")
                    .map(|e| e.trim_matches('"').to_string())
                    .unwrap_or_default(),
            }),
            s => Err(Error::Backend(format!(
                "Azure GET {} -> HTTP {s}",
                obj.to_path()
            ))),
        }
    }

    async fn head(&self, obj: &ObjectId) -> Result<ObjectStat> {
        let resp = self
            .http
            .execute(self.authorized(self.build_head(obj)))
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

    async fn put(&self, obj: &ObjectId, body: bytes::Bytes) -> Result<Version> {
        self.put_if_match(obj, body, None).await
    }

    async fn put_if_match(
        &self,
        obj: &ObjectId,
        body: bytes::Bytes,
        if_match: Option<&Version>,
    ) -> Result<Version> {
        let resp = self
            .http
            .execute(self.authorized(self.build_put(obj, body, if_match)))
            .await
            .map_err(Error::Backend)?;
        if resp.status == 412 {
            return Err(Error::VersionMismatch {
                expected: if_match.map(|v| v.0.clone()).unwrap_or_default(),
                found: resp
                    .header("etag")
                    .map(|e| e.trim_matches('"').to_string())
                    .unwrap_or_default(),
            });
        }
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "Azure PUT {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        match resp
            .header("etag")
            .map(|v| Version::new(v.trim_matches('"').to_string()))
            .filter(|v| !v.0.trim().is_empty())
        {
            Some(version) => Ok(version),
            None => Ok(self.head(obj).await?.version),
        }
    }

    async fn delete(&self, obj: &ObjectId) -> Result<()> {
        let resp = self
            .http
            .execute(self.authorized(self.build_delete(obj)))
            .await
            .map_err(Error::Backend)?;
        if resp.is_success() || resp.status == 404 {
            Ok(())
        } else {
            Err(Error::Backend(format!(
                "Azure DELETE {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )))
        }
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

    #[test]
    fn blob_url_path_style_targets_emulator_host() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        // Azurite/proxy: plain HTTP, literal host:port, account in the path.
        let a = AzureBackend::new(
            AzureConfig::emulator("devstoreaccount1", "127.0.0.1:10000", false),
            None,
            http.clone(),
        );
        assert_eq!(
            a.blob_url(&obj()),
            "http://127.0.0.1:10000/devstoreaccount1/my-container/data/checkpoint.bin"
        );
        // SAS still appends in path-style.
        let with_sas = AzureBackend::new(
            AzureConfig::emulator("devstoreaccount1", "toxiproxy:10000", false),
            Some("sig=abc".into()),
            http,
        );
        assert_eq!(
            with_sas.blob_url(&obj()),
            "http://toxiproxy:10000/devstoreaccount1/my-container/data/checkpoint.bin?sig=abc"
        );
    }

    #[test]
    fn virtual_host_default_is_unchanged_by_new_fields() {
        // A default AzureConfig must produce the exact public-cloud URL — the new
        // endpoint_host/path_style fields are opt-in and inert by default.
        let cfg = AzureConfig::new("acct");
        assert!(cfg.endpoint_host.is_none());
        assert!(!cfg.path_style);
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(cfg, None, http);
        assert_eq!(
            a.blob_url(&obj()),
            "https://acct.blob.core.windows.net/my-container/data/checkpoint.bin"
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
    async fn shared_key_backend_signs_requests() {
        // A base64 account key (32 bytes). With a shared key configured, every
        // request must carry x-ms-date and an Authorization: SharedKey header.
        let key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"az-bytes"),
        });
        let a = AzureBackend::with_shared_key(AzureConfig::new("acct"), key, http.clone());
        a.fetch_range(&obj(), 0, 8).await.unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        assert!(
            req.header("x-ms-date").is_some(),
            "x-ms-date must be stamped"
        );
        let auth = req.header("Authorization").expect("Authorization header");
        assert!(auth.starts_with("SharedKey acct:"), "got {auth}");
    }

    #[tokio::test]
    async fn sas_backend_does_not_add_authorization_header() {
        // Without a shared key (SAS-only), no Authorization header is added — the
        // SAS travels in the URL query.
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"az-bytes"),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), Some("sig=x".into()), http.clone());
        a.fetch_range(&obj(), 0, 8).await.unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("Authorization"), None);
    }

    #[tokio::test]
    async fn fetch_range_if_match_sends_quoted_precondition() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"az-bytes"),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http.clone());
        let _ = a
            .fetch_range_if_match(&obj(), 8, 8, Some(&Version::new("0x8DABCDEF")))
            .await
            .unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("If-Match"), Some("\"0x8DABCDEF\""));
    }

    #[tokio::test]
    async fn fetch_range_maps_412_to_version_mismatch() {
        let http = MockHttp::new(HttpResponse {
            status: 412,
            headers: vec![("ETag".into(), "\"0xNEW\"".into())],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http);
        match a
            .fetch_range_if_match(&obj(), 0, 8, Some(&Version::new("0xOLD")))
            .await
        {
            Err(Error::VersionMismatch { expected, found }) => {
                assert_eq!(expected, "0xOLD");
                assert_eq!(found, "0xNEW");
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
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

    #[tokio::test]
    async fn put_sends_block_blob_body_and_returns_etag() {
        let http = MockHttp::new(HttpResponse {
            status: 201,
            headers: vec![("ETag".into(), "\"0xNEW\"".into())],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http.clone());
        let body = bytes::Bytes::from_static(b"az-object");
        let version = a.put(&obj(), body.clone()).await.unwrap();
        assert_eq!(version, Version::new("0xNEW"));
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.method, Method::Put);
        assert_eq!(req.body, body);
        assert_eq!(req.header("x-ms-blob-type"), Some("BlockBlob"));
    }

    #[tokio::test]
    async fn put_if_match_maps_412() {
        let http = MockHttp::new(HttpResponse {
            status: 412,
            headers: vec![("ETag".into(), "\"0xB\"".into())],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http.clone());
        match a
            .put_if_match(
                &obj(),
                bytes::Bytes::from_static(b"x"),
                Some(&Version::new("0xA")),
            )
            .await
        {
            Err(Error::VersionMismatch { expected, found }) => {
                assert_eq!(expected, "0xA");
                assert_eq!(found, "0xB");
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
        assert_eq!(
            http.last
                .lock()
                .unwrap()
                .clone()
                .unwrap()
                .header("If-Match"),
            Some("\"0xA\"")
        );
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let http = MockHttp::new(HttpResponse {
            status: 202,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http.clone());
        a.delete(&obj()).await.unwrap();
        assert_eq!(
            http.last.lock().unwrap().clone().unwrap().method,
            Method::Delete
        );
    }
}
