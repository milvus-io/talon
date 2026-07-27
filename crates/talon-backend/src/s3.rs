//! S3 (and S3-compatible) [`BackendStore`] implementation.
//!
//! Fetches block/page ranges via S3 **ranged GET** and object metadata via
//! **HEAD**, mapping the ETag to a [`Version`] so a source update invalidates
//! the cache key. The store is generic over an [`HttpClient`] so request
//! construction, range-header formatting, endpoint/vhost URL building, and
//! response parsing are all unit-testable without network access; a real client
//! is injected in production.
//!
//! Authentication is AWS Signature v4 (see [`crate::sigv4`]): every request is
//! signed just before execution with the configured [`S3Credentials`], region,
//! and the `s3` service. Endpoints are configurable so MinIO/Ceph and other
//! S3-compatible stores work.

use std::io::{Read, Take};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use talon_core::{BackendStore, Error, ObjectId, ObjectStat, Result, Version};

use crate::http::{HttpClient, HttpRequest, Method};

/// S3 credentials + endpoint configuration.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Region (e.g. `us-east-1`).
    pub region: String,
    /// Endpoint host, e.g. `s3.us-east-1.amazonaws.com` or a MinIO host.
    pub endpoint: String,
    /// Use path-style (`endpoint/bucket/key`) instead of virtual-host style
    /// (`bucket.endpoint/key`). Required by most S3-compatible stores.
    pub path_style: bool,
    /// `https` when true, `http` otherwise.
    pub tls: bool,
}

impl S3Config {
    /// A default AWS-style config for a region.
    pub fn aws(region: impl Into<String>) -> Self {
        let region = region.into();
        let endpoint = format!("s3.{region}.amazonaws.com");
        Self {
            region,
            endpoint,
            path_style: false,
            tls: true,
        }
    }
}

/// Static S3 credentials. Secrets come from env/config, never logged.
#[derive(Clone)]
pub struct S3Credentials {
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Optional session token (STS).
    pub session_token: Option<String>,
}

/// An S3 `BackendStore` over a pluggable HTTP client.
pub struct S3Backend {
    config: S3Config,
    creds: S3Credentials,
    http: Arc<dyn HttpClient>,
}

impl S3Backend {
    /// Construct a backend from config, credentials, and an HTTP client.
    pub fn new(config: S3Config, creds: S3Credentials, http: Arc<dyn HttpClient>) -> Self {
        Self {
            config,
            creds,
            http,
        }
    }

    /// Build the object URL for `obj` (scheme + host + key), honoring
    /// path-style vs virtual-host style.
    pub fn object_url(&self, obj: &ObjectId) -> String {
        let scheme = if self.config.tls { "https" } else { "http" };
        let key = obj.object_path.trim_start_matches('/');
        if self.config.path_style {
            format!("{scheme}://{}/{}/{}", self.config.endpoint, obj.bucket, key)
        } else {
            format!("{scheme}://{}.{}/{}", obj.bucket, self.config.endpoint, key)
        }
    }

    /// Format an HTTP `Range` header value for `[offset, offset+len)`.
    ///
    /// `len` must be `> 0` (callers guard this in `fetch_range`). A zero or
    /// overflowing `len` is clamped with checked arithmetic so this public
    /// helper can never underflow/overflow into a bogus `bytes=` range (#167):
    /// a zero `len` yields the single byte at `offset`, and an overflowing end
    /// saturates at `u64::MAX`.
    pub fn range_header(offset: u64, len: u64) -> String {
        // Inclusive end = offset + len - 1, guarded against under/overflow: a
        // zero len yields the single byte at `offset`, and an overflowing end
        // saturates at u64::MAX.
        let span = len.saturating_sub(1);
        let end = offset.saturating_add(span);
        format!("bytes={offset}-{end}")
    }

    /// Build the ranged GET request for a fetch (exposed for testing).
    ///
    /// When `if_match` is set, adds an `If-Match` header so the origin returns
    /// `412 Precondition Failed` if the object was overwritten since the version
    /// was resolved (issue #163).
    pub fn build_get(&self, obj: &ObjectId, offset: u64, len: u64) -> HttpRequest {
        self.build_get_if_match(obj, offset, len, None)
    }

    /// Build the ranged GET request with an optional `If-Match` precondition.
    pub fn build_get_if_match(
        &self,
        obj: &ObjectId,
        offset: u64,
        len: u64,
        if_match: Option<&Version>,
    ) -> HttpRequest {
        let mut headers = vec![("Range".to_string(), Self::range_header(offset, len))];
        if let Some(tok) = &self.creds.session_token {
            headers.push(("x-amz-security-token".to_string(), tok.clone()));
        }
        if let Some(version) = if_match {
            // S3 echoes the ETag with surrounding quotes; send it quoted.
            headers.push(("If-Match".to_string(), format!("\"{}\"", version.as_str())));
        }
        HttpRequest {
            method: Method::Get,
            url: self.object_url(obj),
            headers,
            body: bytes::Bytes::new(),
        }
    }

    /// Build the HEAD request for a stat (exposed for testing).
    pub fn build_head(&self, obj: &ObjectId) -> HttpRequest {
        let mut headers = Vec::new();
        if let Some(tok) = &self.creds.session_token {
            headers.push(("x-amz-security-token".to_string(), tok.clone()));
        }
        HttpRequest {
            method: Method::Head,
            url: self.object_url(obj),
            headers,
            body: bytes::Bytes::new(),
        }
    }

    /// Build the whole-object PUT request (exposed for testing).
    ///
    /// When `if_match` is set, adds an `If-Match` header so the origin rejects the
    /// write with `412` if the object changed since it was read (#226).
    pub fn build_put(
        &self,
        obj: &ObjectId,
        body: bytes::Bytes,
        if_match: Option<&Version>,
    ) -> HttpRequest {
        let mut headers = vec![("Content-Length".to_string(), body.len().to_string())];
        if let Some(tok) = &self.creds.session_token {
            headers.push(("x-amz-security-token".to_string(), tok.clone()));
        }
        if let Some(version) = if_match {
            headers.push(("If-Match".to_string(), format!("\"{}\"", version.as_str())));
        }
        HttpRequest {
            method: Method::Put,
            url: self.object_url(obj),
            headers,
            body,
        }
    }

    /// Build the DELETE request (exposed for testing).
    pub fn build_delete(&self, obj: &ObjectId) -> HttpRequest {
        let mut headers = Vec::new();
        if let Some(tok) = &self.creds.session_token {
            headers.push(("x-amz-security-token".to_string(), tok.clone()));
        }
        HttpRequest {
            method: Method::Delete,
            url: self.object_url(obj),
            headers,
            body: bytes::Bytes::new(),
        }
    }

    /// Sign `req` with AWS SigV4 for this backend's region, stamping the current
    /// wall-clock time. Called on every request just before it is executed.
    fn signed(&self, mut req: HttpRequest) -> HttpRequest {
        let date = crate::sigv4::AmzDate::from_system_time(std::time::SystemTime::now());
        crate::sigv4::sign_request(&mut req, &self.creds, &self.config.region, "s3", &date);
        req
    }

    fn signed_with_payload_hash(&self, mut req: HttpRequest, payload_hash: &str) -> HttpRequest {
        let date = crate::sigv4::AmzDate::from_system_time(std::time::SystemTime::now());
        crate::sigv4::sign_request_with_payload_hash(
            &mut req,
            &self.creds,
            &self.config.region,
            "s3",
            &date,
            payload_hash,
        );
        req
    }

    fn build_streamed_put(
        &self,
        obj: &ObjectId,
        len: u64,
        if_match: Option<&Version>,
    ) -> HttpRequest {
        let mut request = self.build_put(obj, bytes::Bytes::new(), if_match);
        if let Some((_, value)) = request
            .headers
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        {
            *value = len.to_string();
        }
        request
    }
}

fn hash_file(path: PathBuf, len: u64) -> Result<String> {
    let file = std::fs::File::open(&path)
        .map_err(|error| Error::Backend(format!("open {} for hashing: {error}", path.display())))?;
    let mut reader: Take<std::fs::File> = file.take(len);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut read_len = 0u64;
    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|error| Error::Backend(format!("hash {}: {error}", path.display())))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        read_len += count as u64;
    }
    if read_len != len {
        return Err(Error::Backend(format!(
            "{} ended after {read_len} bytes while hashing {len} bytes",
            path.display()
        )));
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Normalize an S3 ETag into a [`Version`] (strip surrounding quotes).
fn etag_to_version(etag: &str) -> Version {
    Version::new(etag.trim_matches('"').to_string())
}

#[async_trait]
impl BackendStore for S3Backend {
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
        let req = self.signed(self.build_get_if_match(obj, offset, len, if_match));
        let resp = self.http.execute(req).await.map_err(Error::Backend)?;
        // 206 (range honored) or 200 (server ignored Range, returned the whole
        // object). `range_body` yields exactly the requested window in both
        // cases, validates the 206 Content-Range, and accepts an object-end
        // short 206 (issues #117, #161).
        if resp.status == 206 || resp.status == 200 {
            let content_range = resp.header("content-range").map(str::to_owned);
            crate::http::range_body(
                resp.status,
                resp.body,
                offset,
                len,
                content_range.as_deref(),
            )
            .map_err(Error::Backend)
        } else if resp.status == 404 {
            Err(Error::NotFound(obj.to_path()))
        } else if resp.status == 412 {
            // The If-Match precondition failed: the object was overwritten since
            // the version was resolved (issue #163). The ETag now on the object
            // (if returned) is the new version.
            Err(Error::VersionMismatch {
                expected: if_match.map(|v| v.0.clone()).unwrap_or_default(),
                found: resp
                    .header("etag")
                    .map(|e| e.trim_matches('"').to_string())
                    .unwrap_or_default(),
            })
        } else {
            Err(Error::Backend(format!(
                "S3 GET {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )))
        }
    }

    async fn head(&self, obj: &ObjectId) -> Result<ObjectStat> {
        let req = self.signed(self.build_head(obj));
        let resp = self.http.execute(req).await.map_err(Error::Backend)?;
        if resp.status == 404 {
            return Err(Error::NotFound(obj.to_path()));
        }
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "S3 HEAD {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        let len = resp
            .header("content-length")
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| Error::Backend("S3 HEAD missing/invalid Content-Length".into()))?;
        let version = resp
            .header("etag")
            .map(etag_to_version)
            .filter(|v| !v.0.trim().is_empty())
            .ok_or_else(|| {
                Error::Backend(format!(
                    "S3 HEAD {} returned no usable ETag; refusing to cache without a version",
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
        let req = self.signed(self.build_put(obj, body, if_match));
        let resp = self.http.execute(req).await.map_err(Error::Backend)?;
        if resp.status == 412 {
            // The If-Match precondition failed: the object changed since it was
            // read (#226). Report the new ETag if present.
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
                "S3 PUT {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        // The PUT response's ETag is the committed object version. Fall back to a
        // HEAD only if the PUT response didn't carry one.
        match resp
            .header("etag")
            .map(etag_to_version)
            .filter(|v| !v.0.trim().is_empty())
        {
            Some(version) => Ok(version),
            None => Ok(self.head(obj).await?.version),
        }
    }

    async fn put_file(&self, obj: &ObjectId, path: &Path, len: u64) -> Result<Version> {
        let payload_hash = tokio::task::spawn_blocking({
            let path = path.to_path_buf();
            move || hash_file(path, len)
        })
        .await
        .map_err(|error| Error::Backend(format!("S3 payload hashing task failed: {error}")))??;
        let request =
            self.signed_with_payload_hash(self.build_streamed_put(obj, len, None), &payload_hash);
        let resp = self
            .http
            .execute_file(request, path, len)
            .await
            .map_err(Error::Backend)?;
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "S3 streamed PUT {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )));
        }
        match resp
            .header("etag")
            .map(etag_to_version)
            .filter(|version| !version.0.trim().is_empty())
        {
            Some(version) => Ok(version),
            None => Ok(self.head(obj).await?.version),
        }
    }

    async fn delete(&self, obj: &ObjectId) -> Result<()> {
        let req = self.signed(self.build_delete(obj));
        let resp = self.http.execute(req).await.map_err(Error::Backend)?;
        // 2xx or 404 are both success (delete is idempotent).
        if resp.is_success() || resp.status == 404 {
            Ok(())
        } else {
            Err(Error::Backend(format!(
                "S3 DELETE {} -> HTTP {}",
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

    /// A mock client that records the last request and returns a canned response.
    struct MockHttp {
        last: Mutex<Option<HttpRequest>>,
        last_file: Mutex<Option<(HttpRequest, PathBuf, u64)>>,
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

    fn creds() -> S3Credentials {
        S3Credentials {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: None,
        }
    }

    fn obj() -> ObjectId {
        ObjectId::new(Backend::S3, "my-bucket", "data/checkpoint.bin")
    }

    #[test]
    fn range_header_is_inclusive() {
        assert_eq!(S3Backend::range_header(0, 100), "bytes=0-99");
        assert_eq!(S3Backend::range_header(256, 256), "bytes=256-511");
    }

    #[test]
    fn range_header_clamps_zero_and_overflow() {
        // len == 0 must not underflow (would panic in debug / wrap in release).
        assert_eq!(S3Backend::range_header(10, 0), "bytes=10-10");
        // offset + len overflowing u64 saturates the inclusive end at u64::MAX
        // instead of wrapping to a bogus small range (#167).
        assert_eq!(
            S3Backend::range_header(u64::MAX - 1, u64::MAX),
            format!("bytes={}-{}", u64::MAX - 1, u64::MAX)
        );
    }

    #[test]
    fn url_building_vhost_and_path_style() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let vhost = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        assert_eq!(
            vhost.object_url(&obj()),
            "https://my-bucket.s3.us-east-1.amazonaws.com/data/checkpoint.bin"
        );

        let mut cfg = S3Config::aws("us-east-1");
        cfg.path_style = true;
        cfg.tls = false;
        cfg.endpoint = "minio:9000".into();
        let path = S3Backend::new(cfg, creds(), http);
        assert_eq!(
            path.object_url(&obj()),
            "http://minio:9000/my-bucket/data/checkpoint.bin"
        );
    }

    #[tokio::test]
    async fn fetch_range_returns_body_on_206() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"partial-bytes"),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        let got = s3.fetch_range(&obj(), 10, 13).await.unwrap();
        assert_eq!(got, bytes::Bytes::from_static(b"partial-bytes"));
        // The request carried the right Range header + method.
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.method, Method::Get);
        assert_eq!(req.header("Range"), Some("bytes=10-22"));
    }

    #[tokio::test]
    async fn fetch_range_slices_whole_object_on_200() {
        // A range-ignoring store/proxy returns HTTP 200 + the whole object; the
        // backend must slice out the requested window rather than caching the
        // object head under a non-zero-offset block (issue #117).
        let object: Vec<u8> = (0..255u8).cycle().take(600).collect();
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::from(object.clone()),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http);
        let got = s3.fetch_range(&obj(), 256, 256).await.unwrap();
        assert_eq!(&got[..], &object[256..512]);
    }

    #[tokio::test]
    async fn fetch_range_maps_404_to_notfound() {
        let http = MockHttp::new(HttpResponse {
            status: 404,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http);
        assert!(matches!(
            s3.fetch_range(&obj(), 0, 8).await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn fetch_range_if_match_sends_quoted_precondition() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"partial-bytes"),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        let _ = s3
            .fetch_range_if_match(&obj(), 10, 13, Some(&Version::new("abc123")))
            .await
            .unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        // S3 expects the ETag quoted in If-Match.
        assert_eq!(req.header("If-Match"), Some("\"abc123\""));
    }

    #[tokio::test]
    async fn fetch_range_maps_412_to_version_mismatch() {
        // The If-Match precondition failed: the object was overwritten since the
        // version was resolved (issue #163). The new ETag rides on the response.
        let http = MockHttp::new(HttpResponse {
            status: 412,
            headers: vec![("ETag".into(), "\"v2\"".into())],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http);
        match s3
            .fetch_range_if_match(&obj(), 0, 8, Some(&Version::new("v1")))
            .await
        {
            Err(Error::VersionMismatch { expected, found }) => {
                assert_eq!(expected, "v1");
                assert_eq!(found, "v2");
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn head_parses_len_and_etag() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![
                ("Content-Length".into(), "4096".into()),
                ("ETag".into(), "\"abc123\"".into()),
            ],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http);
        let stat = s3.head(&obj()).await.unwrap();
        assert_eq!(stat.len, 4096);
        // ETag quotes are stripped so it round-trips as a clean version token.
        assert_eq!(stat.version, Version::new("abc123"));
    }

    #[tokio::test]
    async fn head_missing_content_length_errors() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![("ETag".into(), "\"x\"".into())],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http);
        assert!(matches!(s3.head(&obj()).await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn head_missing_or_empty_etag_errors_not_empty_version() {
        // No ETag at all -> error (an empty version would collapse all object
        // generations onto one cache key and serve stale data, issue #160).
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![("Content-Length".into(), "4096".into())],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        assert!(matches!(s3.head(&obj()).await, Err(Error::Backend(_))));

        // A present-but-empty ETag is likewise refused.
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![
                ("Content-Length".into(), "4096".into()),
                ("ETag".into(), "\"\"".into()),
            ],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http);
        assert!(matches!(s3.head(&obj()).await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn session_token_header_is_attached() {
        let http = MockHttp::new(HttpResponse {
            status: 206,
            headers: vec![],
            body: bytes::Bytes::from_static(b"12345678"),
        });
        let mut c = creds();
        c.session_token = Some("token123".into());
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), c, http.clone());
        let _ = s3.fetch_range(&obj(), 0, 8).await.unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("x-amz-security-token"), Some("token123"));
    }

    #[tokio::test]
    async fn put_sends_body_and_returns_committed_version() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![("ETag".into(), "\"newetag\"".into())],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        let body = bytes::Bytes::from_static(b"hello object");
        let version = s3.put(&obj(), body.clone()).await.unwrap();
        // Committed version is parsed from the PUT response ETag (quotes stripped).
        assert_eq!(version, Version::new("newetag"));
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.method, Method::Put);
        assert_eq!(req.body, body);
        assert_eq!(req.header("Content-Length"), Some("12"));
    }

    #[tokio::test]
    async fn put_file_streams_signed_body_and_returns_committed_version() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![("ETag".into(), "\"stream-etag\"".into())],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        let mut staged = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut staged, b"sparse-stream-body").unwrap();

        let version = s3.put_file(&obj(), staged.path(), 18).await.unwrap();

        assert_eq!(version, Version::new("stream-etag"));
        let (req, path, len) = http.last_file.lock().unwrap().clone().unwrap();
        assert_eq!(path, staged.path());
        assert_eq!(len, 18);
        assert_eq!(req.method, Method::Put);
        assert!(req.body.is_empty());
        assert_eq!(req.header("Content-Length"), Some("18"));
        assert!(req.header("Authorization").is_some());
        assert_eq!(
            req.header("x-amz-content-sha256"),
            Some("324d3410a4267a26d985db5fb545b6df76b89c53268056ff4eacbb719bdec330")
        );
    }

    #[tokio::test]
    async fn put_if_match_sends_precondition_and_maps_412() {
        let http = MockHttp::new(HttpResponse {
            status: 412,
            headers: vec![("ETag".into(), "\"v2\"".into())],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        match s3
            .put_if_match(
                &obj(),
                bytes::Bytes::from_static(b"x"),
                Some(&Version::new("v1")),
            )
            .await
        {
            Err(Error::VersionMismatch { expected, found }) => {
                assert_eq!(expected, "v1");
                assert_eq!(found, "v2");
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
        let req = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(req.header("If-Match"), Some("\"v1\""));
    }

    #[tokio::test]
    async fn delete_sends_delete_and_is_idempotent() {
        // 2xx -> Ok.
        let http = MockHttp::new(HttpResponse {
            status: 204,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        s3.delete(&obj()).await.unwrap();
        assert_eq!(
            http.last.lock().unwrap().clone().unwrap().method,
            Method::Delete
        );

        // 404 -> also Ok (idempotent).
        let http404 = MockHttp::new(HttpResponse {
            status: 404,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let s3 = S3Backend::new(S3Config::aws("us-east-1"), creds(), http404);
        s3.delete(&obj()).await.unwrap();
    }
}
