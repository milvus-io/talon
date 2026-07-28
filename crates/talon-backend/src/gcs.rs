//! Google Cloud Storage [`BackendStore`] implementation.
//!
//! Uses the GCS XML/JSON download endpoint: a ranged GET on
//! `storage.googleapis.com/<bucket>/<object>` for block/page loads and a HEAD
//! (or metadata GET) for size + generation. The object **generation** maps to a
//! [`Version`] so a source overwrite yields a distinct cache key, analogous to
//! an S3 ETag.
//!
//! Like [`crate::s3`], the store is generic over an [`HttpClient`] so request
//! construction and response parsing are unit-testable offline; a real OAuth2
//! bearer-token client is injected in production.

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

/// GCS endpoint configuration.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// Download host; defaults to `storage.googleapis.com`.
    pub endpoint: String,
    /// `https` when true.
    pub tls: bool,
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            endpoint: "storage.googleapis.com".into(),
            tls: true,
        }
    }
}

impl GcsConfig {
    /// Point at an emulator (e.g. fake-gcs-server): a literal `host[:port]` and a
    /// TLS flag. Plaintext (`tls = false`) is typical for a local emulator. The
    /// default public-cloud config is [`GcsConfig::default`].
    pub fn emulator(endpoint: impl Into<String>, tls: bool) -> Self {
        Self {
            endpoint: endpoint.into(),
            tls,
        }
    }
}

/// A GCS `BackendStore` over a pluggable HTTP client.
pub struct GcsBackend {
    config: GcsConfig,
    /// OAuth2 bearer token (short-lived; refreshed by the caller).
    bearer_token: Option<String>,
    http: Arc<dyn HttpClient>,
}

impl GcsBackend {
    /// Construct a backend from config, an optional bearer token, and a client.
    pub fn new(config: GcsConfig, bearer_token: Option<String>, http: Arc<dyn HttpClient>) -> Self {
        Self {
            config,
            bearer_token,
            http,
        }
    }

    /// Build the object download URL (`scheme://host/bucket/object`).
    pub fn object_url(&self, obj: &ObjectId) -> String {
        let scheme = if self.config.tls { "https" } else { "http" };
        let key = obj.object_path.trim_start_matches('/');
        format!("{scheme}://{}/{}/{}", self.config.endpoint, obj.bucket, key)
    }

    /// Build the JSON API listing URL for a bucket.
    ///
    /// GCS lists through the **JSON API** (`/storage/v1/b/<bucket>/o`), not the
    /// download endpoint the object URLs use, so this does not go through
    /// [`object_url`](Self::object_url).
    pub fn list_url(&self, bucket: &str, prefix: &str, cursor: Option<&str>, max: u32) -> String {
        let scheme = if self.config.tls { "https" } else { "http" };
        let mut url = format!(
            "{scheme}://{}/storage/v1/b/{}/o?maxResults={max}",
            self.config.endpoint,
            encode_query_value(bucket)
        );
        if !prefix.is_empty() {
            url.push_str(&format!("&prefix={}", encode_query_value(prefix)));
        }
        if let Some(token) = cursor {
            url.push_str(&format!("&pageToken={}", encode_query_value(token)));
        }
        url
    }

    /// Parse a GCS JSON listing response.
    ///
    /// `size` arrives as a **string**, not a number — GCS encodes 64-bit values
    /// that way to survive JavaScript's 53-bit integers. Reading it as a JSON
    /// number silently yields nothing for every object.
    pub fn parse_list_response(body: &str) -> Result<ListPage> {
        let doc: serde_json::Value = serde_json::from_str(body)
            .map_err(|e| Error::Backend(format!("GCS listing is not valid JSON: {e}")))?;

        let objects = match doc.get("items") {
            // An empty prefix omits `items` entirely rather than sending [].
            None => Vec::new(),
            Some(items) => items
                .as_array()
                .ok_or_else(|| Error::Backend("GCS listing `items` is not an array".into()))?
                .iter()
                .map(|item| {
                    let key = item
                        .get("name")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| Error::Backend("GCS listing entry has no `name`".into()))?
                        .to_string();
                    let size = item
                        .get("size")
                        .and_then(|v| v.as_str())
                        .and_then(|v| v.parse::<u64>().ok())
                        .ok_or_else(|| {
                            Error::Backend(format!(
                                "GCS listing entry {key} has no usable `size` \
                                 (expected a decimal string)"
                            ))
                        })?;
                    Ok(ListedObject { key, size })
                })
                .collect::<Result<Vec<_>>>()?,
        };

        let next = doc
            .get("nextPageToken")
            .and_then(|v| v.as_str())
            .filter(|t| !t.is_empty())
            .map(|t| t.to_string());
        Ok(ListPage { objects, next })
    }

    /// Format an inclusive HTTP `Range` header for `[offset, offset+len)`.
    ///
    /// `len` is expected `> 0` (guarded in `fetch_range`); a zero/overflowing
    /// `len` is clamped with checked arithmetic so this public helper never
    /// under/overflows into a bogus range (#167).
    pub fn range_header(offset: u64, len: u64) -> String {
        let span = len.saturating_sub(1);
        let end = offset.saturating_add(span);
        format!("bytes={offset}-{end}")
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        match &self.bearer_token {
            Some(t) => vec![("Authorization".to_string(), format!("Bearer {t}"))],
            None => Vec::new(),
        }
    }

    /// Build the ranged GET request (exposed for testing).
    pub fn build_get(&self, obj: &ObjectId, offset: u64, len: u64) -> HttpRequest {
        self.build_get_if_match(obj, offset, len, None)
    }

    /// Build the ranged GET request with an optional generation/ETag precondition.
    ///
    /// A numeric version is treated as an object generation and sent as
    /// `x-goog-if-generation-match`; a non-numeric version (ETag fallback) is
    /// sent as `If-Match`. Either makes GCS return `412` if the object changed
    /// since the version was resolved (issue #163).
    pub fn build_get_if_match(
        &self,
        obj: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> HttpRequest {
        let mut headers = self.auth_headers();
        headers.push(("Range".to_string(), Self::range_header(offset, len)));
        if let Some(version) = if_match {
            let v = version.as_str();
            if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) {
                headers.push(("x-goog-if-generation-match".to_string(), v.to_string()));
            } else {
                headers.push(("If-Match".to_string(), format!("\"{v}\"")));
            }
        }
        HttpRequest {
            method: Method::Get,
            url: self.object_url(obj),
            headers,
            body: bytes::Bytes::new(),
        }
    }

    /// Build the HEAD request (exposed for testing).
    pub fn build_head(&self, obj: &ObjectId) -> HttpRequest {
        HttpRequest {
            method: Method::Head,
            url: self.object_url(obj),
            headers: self.auth_headers(),
            body: bytes::Bytes::new(),
        }
    }

    /// Build the whole-object PUT request (exposed for testing).
    ///
    /// A numeric `if_match` is an object generation (`x-goog-if-generation-match`);
    /// a non-numeric one is an ETag (`If-Match`). Either makes GCS reject the
    /// write with `412` if the object changed since it was read (#226).
    pub fn build_put(
        &self,
        obj: &ObjectId,
        body: bytes::Bytes,
        if_match: Option<&Version>,
    ) -> HttpRequest {
        let mut headers = self.auth_headers();
        headers.push(("Content-Length".to_string(), body.len().to_string()));
        if let Some(version) = if_match {
            let v = version.as_str();
            if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) {
                headers.push(("x-goog-if-generation-match".to_string(), v.to_string()));
            } else {
                headers.push(("If-Match".to_string(), format!("\"{v}\"")));
            }
        }
        HttpRequest {
            method: Method::Put,
            url: self.object_url(obj),
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
            url: self.object_url(obj),
            headers: self.auth_headers(),
            body: bytes::Bytes::new(),
        }
    }
}

#[async_trait]
impl BackendStore for GcsBackend {
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
            .execute(self.build_get_if_match(obj, offset, len, if_match))
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
            // Precondition failed: the object changed since the version was
            // resolved (issue #163). Report the new generation/ETag if present.
            412 => Err(Error::VersionMismatch {
                expected: if_match.map(|v| v.0.clone()).unwrap_or_default(),
                found: resp
                    .header("x-goog-generation")
                    .or_else(|| resp.header("etag"))
                    .map(|e| e.trim_matches('"').to_string())
                    .unwrap_or_default(),
            }),
            s => Err(Error::Backend(format!(
                "GCS GET {} -> HTTP {s}",
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
        // GCS caps maxResults at 1000 and silently reduces larger values;
        // clamping keeps the request honest.
        let max_keys = max_keys.clamp(1, 1000);
        let url = self.list_url(bucket, prefix, cursor, max_keys);
        let req = HttpRequest::new(Method::Get, url, self.auth_headers());
        let resp = self.http.execute(req).await.map_err(Error::Backend)?;
        if resp.status == 404 {
            return Err(Error::NotFound(bucket.to_string()));
        }
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "GCS list {bucket}/{prefix} -> HTTP {}",
                resp.status
            )));
        }
        let body = std::str::from_utf8(&resp.body)
            .map_err(|e| Error::Backend(format!("GCS listing body is not UTF-8: {e}")))?;
        Self::parse_list_response(body)
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
                "GCS HEAD {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        let len = resp
            .header("content-length")
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| Error::Backend("GCS HEAD missing/invalid Content-Length".into()))?;
        // Prefer the immutable generation; fall back to the goog-hash ETag.
        let version = resp
            .header("x-goog-generation")
            .or_else(|| resp.header("etag"))
            .map(|v| Version::new(v.trim_matches('"').to_string()))
            .filter(|v| !v.0.trim().is_empty())
            .ok_or_else(|| {
                Error::Backend(format!(
                    "GCS HEAD {} returned no generation/ETag; refusing to cache without a version",
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
            .execute(self.build_put(obj, body, if_match))
            .await
            .map_err(Error::Backend)?;
        if resp.status == 412 {
            return Err(Error::VersionMismatch {
                expected: if_match.map(|v| v.0.clone()).unwrap_or_default(),
                found: resp
                    .header("x-goog-generation")
                    .or_else(|| resp.header("etag"))
                    .map(|e| e.trim_matches('"').to_string())
                    .unwrap_or_default(),
            });
        }
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "GCS PUT {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        match resp
            .header("x-goog-generation")
            .or_else(|| resp.header("etag"))
            .map(|v| Version::new(v.trim_matches('"').to_string()))
            .filter(|v| !v.0.trim().is_empty())
        {
            Some(version) => Ok(version),
            None => Ok(self.head(obj).await?.version),
        }
    }

    async fn put_file(&self, obj: &ObjectId, path: &Path, len: u64) -> Result<Version> {
        let resp = self
            .http
            .execute_file(self.build_streamed_put(obj, len), path, len)
            .await
            .map_err(Error::Backend)?;
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "GCS streamed PUT {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        match resp
            .header("x-goog-generation")
            .or_else(|| resp.header("etag"))
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
            .execute(self.build_delete(obj))
            .await
            .map_err(Error::Backend)?;
        if resp.is_success() || resp.status == 404 {
            Ok(())
        } else {
            Err(Error::Backend(format!(
                "GCS DELETE {} -> HTTP {}",
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
        ObjectId::new(Backend::Gcs, "my-bucket", "data/checkpoint.bin")
    }

    #[test]
    fn url_and_range() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http);
        assert_eq!(
            g.object_url(&obj()),
            "https://storage.googleapis.com/my-bucket/data/checkpoint.bin"
        );
        assert_eq!(GcsBackend::range_header(0, 64), "bytes=0-63");
    }

    #[test]
    fn emulator_config_targets_a_literal_host_over_http() {
        // fake-gcs-server: plaintext, literal host:port, object addressed under it.
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::emulator("127.0.0.1:4443", false), None, http);
        assert_eq!(
            g.object_url(&obj()),
            "http://127.0.0.1:4443/my-bucket/data/checkpoint.bin"
        );
    }

    #[tokio::test]
    async fn fetch_range_sends_bearer_and_range() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"gcsby"),
        });
        let g = GcsBackend::new(GcsConfig::default(), Some("tok".into()), http.clone());
        let got = g.fetch_range(&obj(), 4, 5).await.unwrap();
        assert_eq!(got, bytes::Bytes::from_static(b"gcsby"));
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("Authorization"), Some("Bearer tok"));
        assert_eq!(req.header("Range"), Some("bytes=4-8"));
    }

    #[tokio::test]
    async fn fetch_range_if_match_numeric_uses_generation_precondition() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"gcsby"),
        });
        let g = GcsBackend::new(GcsConfig::default(), Some("tok".into()), http.clone());
        let _ = g
            .fetch_range_if_match(&obj(), 4, 5, Some(&Version::new("1699999999")))
            .await
            .unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        // A numeric version is an object generation.
        assert_eq!(req.header("x-goog-if-generation-match"), Some("1699999999"));
        assert_eq!(req.header("If-Match"), None);
    }

    #[tokio::test]
    async fn fetch_range_if_match_nonnumeric_uses_if_match() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"gcsby"),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http.clone());
        let _ = g
            .fetch_range_if_match(&obj(), 4, 5, Some(&Version::new("etagxyz")))
            .await
            .unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("If-Match"), Some("\"etagxyz\""));
        assert_eq!(req.header("x-goog-if-generation-match"), None);
    }

    #[tokio::test]
    async fn fetch_range_maps_412_to_version_mismatch() {
        let http = MockHttp::new(HttpResponse {
            status: 412,
            headers: vec![("x-goog-generation".into(), "1700000001".into())],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http);
        match g
            .fetch_range_if_match(&obj(), 0, 8, Some(&Version::new("1699999999")))
            .await
        {
            Err(Error::VersionMismatch { expected, found }) => {
                assert_eq!(expected, "1699999999");
                assert_eq!(found, "1700000001");
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn head_uses_generation_as_version() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![
                ("Content-Length".into(), "2048".into()),
                ("x-goog-generation".into(), "1699999999".into()),
                ("ETag".into(), "\"fallback\"".into()),
            ],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http);
        let stat = g.head(&obj()).await.unwrap();
        assert_eq!(stat.len, 2048);
        assert_eq!(stat.version, Version::new("1699999999"));
    }

    #[tokio::test]
    async fn missing_generation_falls_back_to_etag() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![
                ("Content-Length".into(), "1".into()),
                ("ETag".into(), "\"abc\"".into()),
            ],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http);
        assert_eq!(g.head(&obj()).await.unwrap().version, Version::new("abc"));
    }

    #[tokio::test]
    async fn not_found_maps_through() {
        let http = MockHttp::new(HttpResponse {
            status: 404,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http);
        assert!(matches!(
            g.fetch_range(&obj(), 0, 8).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(g.head(&obj()).await, Err(Error::NotFound(_))));
    }

    #[tokio::test]
    async fn put_sends_body_and_returns_generation() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![("x-goog-generation".into(), "1700000009".into())],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), Some("tok".into()), http.clone());
        let body = bytes::Bytes::from_static(b"gcs-object");
        let version = g.put(&obj(), body.clone()).await.unwrap();
        assert_eq!(version, Version::new("1700000009"));
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.method, Method::Put);
        assert_eq!(req.body, body);
        assert_eq!(req.header("Authorization"), Some("Bearer tok"));
    }

    #[tokio::test]
    async fn put_file_streams_body_and_returns_generation() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![("x-goog-generation".into(), "1700000011".into())],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), Some("tok".into()), http.clone());
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut staged, b"gcs-stream").unwrap();

        let version = g.put_file(&obj(), staged.path(), 10).await.unwrap();

        assert_eq!(version, Version::new("1700000011"));
        let (req, path, len) = http.last_file.lock().unwrap().clone().unwrap();
        assert_eq!(path, staged.path());
        assert_eq!(len, 10);
        assert_eq!(req.method, Method::Put);
        assert!(req.body.is_empty());
        assert_eq!(req.header("Content-Length"), Some("10"));
        assert_eq!(req.header("Authorization"), Some("Bearer tok"));
    }

    #[tokio::test]
    async fn put_if_match_numeric_uses_generation_precondition_and_maps_412() {
        let http = MockHttp::new(HttpResponse {
            status: 412,
            headers: vec![("x-goog-generation".into(), "1700000010".into())],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http.clone());
        match g
            .put_if_match(
                &obj(),
                bytes::Bytes::from_static(b"x"),
                Some(&Version::new("1700000009")),
            )
            .await
        {
            Err(Error::VersionMismatch { expected, found }) => {
                assert_eq!(expected, "1700000009");
                assert_eq!(found, "1700000010");
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("x-goog-if-generation-match"), Some("1700000009"));
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let http = MockHttp::new(HttpResponse {
            status: 204,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let g = GcsBackend::new(GcsConfig::default(), None, http.clone());
        g.delete(&obj()).await.unwrap();
        assert_eq!(
            http.last.lock().unwrap().clone().unwrap().method,
            Method::Delete
        );
    }

    /// A real JSON listing response. `size` is a **string** — GCS encodes
    /// 64-bit values that way, and reading it as a number yields nothing.
    const LIST_BODY: &str = r#"{
  "kind": "storage#objects",
  "items": [
    {"kind":"storage#object","name":"data/a.parquet","size":"1048576","generation":"17"},
    {"kind":"storage#object","name":"data/b.parquet","size":"42","generation":"18"}
  ]
}"#;

    #[test]
    fn list_parses_string_encoded_sizes() {
        let page = GcsBackend::parse_list_response(LIST_BODY).unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "data/a.parquet");
        assert_eq!(page.objects[0].size, 1_048_576);
        assert_eq!(page.objects[1].size, 42);
        assert_eq!(page.next, None);
    }

    /// Sizes beyond 2^53 are exactly why GCS sends them as strings; a parser
    /// that routed through f64 would round here.
    #[test]
    fn list_preserves_sizes_beyond_53_bits() {
        let body = r#"{"items":[{"name":"big","size":"9007199254740993"}]}"#;
        let page = GcsBackend::parse_list_response(body).unwrap();
        assert_eq!(page.objects[0].size, 9_007_199_254_740_993);
    }

    #[test]
    fn list_returns_the_page_token_when_present() {
        let body = r#"{"items":[{"name":"a","size":"1"}],"nextPageToken":"CgZhLnR4dA=="}"#;
        let page = GcsBackend::parse_list_response(body).unwrap();
        assert_eq!(page.next.as_deref(), Some("CgZhLnR4dA=="));
    }

    /// An empty prefix omits `items` entirely rather than sending `[]`, so a
    /// parser requiring the key would error on a legitimately empty listing.
    #[test]
    fn list_of_an_empty_prefix_omits_items_and_is_not_an_error() {
        let page = GcsBackend::parse_list_response(r#"{"kind":"storage#objects"}"#).unwrap();
        assert!(page.objects.is_empty());
        assert_eq!(page.next, None);
    }

    #[test]
    fn list_rejects_an_entry_without_a_usable_size() {
        let body = r#"{"items":[{"name":"a"}]}"#;
        assert!(GcsBackend::parse_list_response(body).is_err());
        // A numeric size is not what GCS sends; reject rather than guess.
        let numeric = r#"{"items":[{"name":"a","size":42}]}"#;
        assert!(GcsBackend::parse_list_response(numeric).is_err());
    }

    #[test]
    fn list_url_targets_the_json_api_and_encodes_the_prefix() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let backend = GcsBackend::new(GcsConfig::default(), None, http.clone());
        let url = backend.list_url("bucket", "data/sub", Some("tok+en"), 1000);
        assert!(url.contains("/storage/v1/b/bucket/o"), "url: {url}");
        assert!(
            url.contains("prefix=data%2Fsub"),
            "prefix must be encoded: {url}"
        );
        assert!(
            url.contains("pageToken=tok%2Ben"),
            "token must be encoded: {url}"
        );
        assert!(url.contains("maxResults=1000"), "url: {url}");
    }
}
