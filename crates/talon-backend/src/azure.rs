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

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use talon_core::{
    BackendStore, Error, ListPage, ListedObject, ObjectId, ObjectStat, Result, Version,
};

use crate::http::{HttpClient, HttpRequest, Method};

/// Percent-encode a query value. `/` is escaped: a listing prefix contains
/// slashes and an unescaped one would change the request path.
fn encode_query_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

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

    /// Build the container listing URL.
    ///
    /// Azure lists with `restype=container&comp=list` on the **container**, and
    /// a SAS token is itself a query string. Both have to coexist, so the SAS
    /// is merged with `&` rather than appended with `?` — appending would
    /// produce two `?` and a request the service rejects as malformed.
    pub fn list_url(
        &self,
        container: &str,
        prefix: &str,
        cursor: Option<&str>,
        max: u32,
    ) -> String {
        let scheme = if self.config.tls { "https" } else { "http" };
        let host =
            self.config.endpoint_host.clone().unwrap_or_else(|| {
                format!("{}.{}", self.config.account, self.config.endpoint_suffix)
            });
        let base = if self.config.path_style {
            format!("{scheme}://{host}/{}/{container}", self.config.account)
        } else {
            format!("{scheme}://{host}/{container}")
        };
        let mut query = format!("restype=container&comp=list&maxresults={max}");
        if !prefix.is_empty() {
            query.push_str(&format!("&prefix={}", encode_query_value(prefix)));
        }
        if let Some(marker) = cursor {
            query.push_str(&format!("&marker={}", encode_query_value(marker)));
        }
        match &self.sas_token {
            // The SAS is already a query string; join with `&`.
            Some(sas) => format!("{base}?{query}&{}", sas.trim_start_matches('?')),
            None => format!("{base}?{query}"),
        }
    }

    /// Parse a `List Blobs` response.
    ///
    /// `<Blob>` records are isolated before reading their fields so a blob's
    /// name cannot be paired with a neighbour's size. The continuation cursor
    /// is `<NextMarker>`, which Azure emits **empty** rather than omitting when
    /// the listing is complete.
    pub fn parse_list_response(body: &str) -> Result<ListPage> {
        let objects = crate::xml::blocks(body, "Blob")
            .into_iter()
            .map(|record| {
                let key = crate::xml::element(record, "Name")
                    .map(crate::xml::unescape)
                    .ok_or_else(|| Error::Backend("Azure listing entry has no <Name>".into()))?;
                let size = crate::xml::element(record, "Content-Length")
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .ok_or_else(|| {
                        Error::Backend(format!(
                            "Azure listing entry {key} has no usable <Content-Length>"
                        ))
                    })?;
                Ok(ListedObject { key, size })
            })
            .collect::<Result<Vec<_>>>()?;

        // Azure sends <NextMarker/> empty on the last page rather than omitting
        // it; treating an empty marker as a cursor loops forever.
        let next = crate::xml::element(body, "NextMarker")
            .map(crate::xml::unescape)
            .filter(|m| !m.trim().is_empty());
        Ok(ListPage { objects, next })
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

    fn build_streamed_put(&self, obj: &ObjectId, len: u64) -> HttpRequest {
        let mut request = self.build_put(obj, bytes::Bytes::new(), None);
        if let Some((_, value)) = request
            .headers
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        {
            *value = len.to_string();
        }
        request
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

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        cursor: Option<&str>,
        max_keys: u32,
    ) -> Result<ListPage> {
        // Azure caps maxresults at 5000, higher than S3 and GCS.
        let max_keys = max_keys.clamp(1, 5000);
        let url = self.list_url(bucket, prefix, cursor, max_keys);
        let req = HttpRequest::new(Method::Get, url, self.common_headers());
        let resp = self.http.execute(req).await.map_err(Error::Backend)?;
        if resp.status == 404 {
            return Err(Error::NotFound(bucket.to_string()));
        }
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "Azure list {bucket}/{prefix} -> HTTP {}",
                resp.status
            )));
        }
        let body = std::str::from_utf8(&resp.body)
            .map_err(|e| Error::Backend(format!("Azure listing body is not UTF-8: {e}")))?;
        Self::parse_list_response(body)
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

    async fn put_file(&self, obj: &ObjectId, path: &Path, len: u64) -> Result<Version> {
        let request = self.authorized(self.build_streamed_put(obj, len));
        let resp = self
            .http
            .execute_file(request, path, len)
            .await
            .map_err(Error::Backend)?;
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "Azure streamed PUT {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        match resp
            .header("etag")
            .map(|value| Version::new(value.trim_matches('"').to_string()))
            .filter(|version| !version.0.trim().is_empty())
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
        last_file: Mutex<Option<(HttpRequest, std::path::PathBuf, u64)>>,
        response: HttpResponse,
    }

    impl MockHttp {
        fn new(response: HttpResponse) -> Arc<Self> {
            Arc::new(Self {
                last: Mutex::new(None),
                last_file: Mutex::new(None),
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

        async fn execute_file(
            &self,
            req: HttpRequest,
            path: &Path,
            len: u64,
        ) -> std::result::Result<HttpResponse, String> {
            *self.last_file.lock().unwrap() = Some((req, path.to_path_buf(), len));
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
    async fn put_file_streams_block_blob_and_returns_etag() {
        let http = MockHttp::new(HttpResponse {
            status: 201,
            headers: vec![("ETag".into(), "\"0xSTREAM\"".into())],
            body: bytes::Bytes::new(),
        });
        let a = AzureBackend::new(AzureConfig::new("acct"), None, http.clone());
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut staged, b"azure-stream").unwrap();

        let version = a.put_file(&obj(), staged.path(), 12).await.unwrap();

        assert_eq!(version, Version::new("0xSTREAM"));
        let (req, path, len) = http.last_file.lock().unwrap().clone().unwrap();
        assert_eq!(path, staged.path());
        assert_eq!(len, 12);
        assert_eq!(req.method, Method::Put);
        assert!(req.body.is_empty());
        assert_eq!(req.header("Content-Length"), Some("12"));
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

    /// A real List Blobs body, abbreviated. Azure names the size field
    /// `Content-Length`, not `Size`.
    const LIST_BODY: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="https://acct.blob.core.windows.net/container">
  <Blobs>
    <Blob>
      <Name>data/a.parquet</Name>
      <Properties>
        <Content-Length>1048576</Content-Length>
        <Etag>0x8DABC</Etag>
      </Properties>
    </Blob>
    <Blob>
      <Name>data/b.parquet</Name>
      <Properties>
        <Content-Length>42</Content-Length>
        <Etag>0x8DABD</Etag>
      </Properties>
    </Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>"#;

    #[test]
    fn list_parses_names_and_content_lengths_pairwise() {
        let page = AzureBackend::parse_list_response(LIST_BODY).unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "data/a.parquet");
        assert_eq!(page.objects[0].size, 1_048_576);
        assert_eq!(page.objects[1].key, "data/b.parquet");
        assert_eq!(page.objects[1].size, 42);
    }

    /// Azure emits `<NextMarker />` **empty** on the last page rather than
    /// omitting it. Treating an empty marker as a cursor loops forever.
    #[test]
    fn list_treats_an_empty_next_marker_as_the_end() {
        let page = AzureBackend::parse_list_response(LIST_BODY).unwrap();
        assert_eq!(page.next, None);

        let with_marker = LIST_BODY.replace("<NextMarker />", "<NextMarker>2!abc</NextMarker>");
        let page = AzureBackend::parse_list_response(&with_marker).unwrap();
        assert_eq!(page.next.as_deref(), Some("2!abc"));
    }

    #[test]
    fn list_unescapes_blob_names() {
        let body = r#"<EnumerationResults><Blobs><Blob><Name>a&amp;b/c.bin</Name>
          <Properties><Content-Length>7</Content-Length></Properties></Blob></Blobs>
          <NextMarker /></EnumerationResults>"#;
        let page = AzureBackend::parse_list_response(body).unwrap();
        assert_eq!(page.objects[0].key, "a&b/c.bin");
    }

    #[test]
    fn list_of_an_empty_container_is_an_empty_page() {
        let body = r#"<EnumerationResults><Blobs /><NextMarker /></EnumerationResults>"#;
        let page = AzureBackend::parse_list_response(body).unwrap();
        assert!(page.objects.is_empty());
        assert_eq!(page.next, None);
    }

    /// A SAS token is itself a query string. Appending listing params with `?`
    /// would produce two `?` and a request Azure rejects as malformed.
    #[test]
    fn list_url_merges_listing_params_with_the_sas_query() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let backend = AzureBackend::new(
            AzureConfig::new("acct"),
            Some("sv=2021&sig=abc".to_string()),
            http.clone(),
        );
        let url = backend.list_url("container", "data/", None, 5000);
        assert_eq!(url.matches('?').count(), 1, "exactly one `?`: {url}");
        assert!(url.contains("restype=container&comp=list"), "url: {url}");
        assert!(
            url.contains("prefix=data%2F"),
            "prefix must be encoded: {url}"
        );
        assert!(url.contains("sv=2021&sig=abc"), "SAS must survive: {url}");
    }

    #[test]
    fn list_url_without_a_sas_has_a_single_query() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let backend = AzureBackend::new(AzureConfig::new("acct"), None, http.clone());
        let url = backend.list_url("container", "", Some("2!m"), 5000);
        assert_eq!(url.matches('?').count(), 1, "url: {url}");
        assert!(
            url.contains("marker=2%21m"),
            "marker must be encoded: {url}"
        );
    }
}
