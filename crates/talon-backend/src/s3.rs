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
use talon_core::{
    BackendStore, Error, ListPage, ListedObject, ObjectId, ObjectStat, Result, Version,
};

use crate::http::{
    HttpClient, HttpRequest, HttpRequestBody, HttpResponse, HttpStreamResponse, Method,
};

/// Percent-encode a query value.
///
/// The URL is signed after this, and SigV4 canonicalizes the query itself, so
/// this only has to produce a valid URL. `/` is escaped because a listing
/// prefix contains slashes and an unescaped one would change the request path.
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

    /// Build the bucket URL (no key), used by listing.
    pub fn bucket_url(&self, bucket: &str) -> String {
        let scheme = if self.config.tls { "https" } else { "http" };
        if self.config.path_style {
            format!("{scheme}://{}/{bucket}", self.config.endpoint)
        } else {
            format!("{scheme}://{bucket}.{}", self.config.endpoint)
        }
    }

    /// Parse a `ListObjectsV2` response body.
    ///
    /// `<Contents>` blocks are isolated before reading `<Key>`/`<Size>`, so a
    /// record's fields cannot be paired with a neighbour's. Keys are unescaped:
    /// a key containing `&` arrives as `&amp;` and would otherwise name a
    /// different object.
    pub fn parse_list_response(body: &str) -> Result<ListPage> {
        let objects = crate::xml::blocks(body, "Contents")
            .into_iter()
            .map(|record| {
                let key = crate::xml::element(record, "Key")
                    .map(crate::xml::unescape)
                    .ok_or_else(|| Error::Backend("S3 listing entry has no <Key>".into()))?;
                let size = crate::xml::element(record, "Size")
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .ok_or_else(|| {
                        Error::Backend(format!("S3 listing entry {key} has no usable <Size>"))
                    })?;
                Ok(ListedObject { key, size })
            })
            .collect::<Result<Vec<_>>>()?;

        // Only trust the continuation token when IsTruncated says there is
        // more; S3 may echo a token on the final page.
        let truncated = crate::xml::element(body, "IsTruncated")
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let next = if truncated {
            crate::xml::element(body, "NextContinuationToken")
                .map(crate::xml::unescape)
                .filter(|t| !t.is_empty())
        } else {
            None
        };
        Ok(ListPage { objects, next })
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

    fn session_headers(&self) -> Vec<(String, String)> {
        self.creds
            .session_token
            .as_ref()
            .map(|token| vec![("x-amz-security-token".to_string(), token.clone())])
            .unwrap_or_default()
    }

    /// Execute a raw metadata request while forwarding only validated
    /// conditional headers supplied by a protocol adapter.
    pub async fn execute_head_raw(
        &self,
        obj: &ObjectId,
        conditions: &[(String, String)],
    ) -> std::result::Result<HttpResponse, String> {
        let mut request = self.build_head(obj);
        request.headers.extend_from_slice(conditions);
        self.http.execute(self.signed(request)).await
    }

    /// Execute a whole or ranged GET without buffering its response body.
    pub async fn execute_get_stream_raw(
        &self,
        obj: &ObjectId,
        range: Option<(u64, u64)>,
        conditions: &[(String, String)],
    ) -> std::result::Result<HttpStreamResponse, String> {
        let mut headers = self.session_headers();
        if let Some((start, end)) = range {
            headers.push(("range".into(), format!("bytes={start}-{end}")));
        }
        headers.extend_from_slice(conditions);
        let request = HttpRequest::new(Method::Get, self.object_url(obj), headers);
        self.http.execute_stream(self.signed(request)).await
    }

    /// Execute one raw ListObjectsV2 page.
    pub async fn execute_list_raw(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
        encoding_type: Option<&str>,
    ) -> std::result::Result<HttpResponse, String> {
        let mut query = format!("list-type=2&max-keys={}", max_keys.min(1000));
        if !prefix.is_empty() {
            query.push_str("&prefix=");
            query.push_str(&encode_query_value(prefix));
        }
        if let Some(delimiter) = delimiter {
            query.push_str("&delimiter=");
            query.push_str(&encode_query_value(delimiter));
        }
        if let Some(token) = continuation_token {
            query.push_str("&continuation-token=");
            query.push_str(&encode_query_value(token));
        }
        if let Some(encoding_type) = encoding_type {
            query.push_str("&encoding-type=");
            query.push_str(&encode_query_value(encoding_type));
        }
        let request = HttpRequest::new(
            Method::Get,
            format!("{}?{query}", self.bucket_url(bucket)),
            self.session_headers(),
        );
        self.http.execute(self.signed(request)).await
    }

    /// Execute a single-use streaming PUT. The payload declaration is signed
    /// by the scoped origin credential and the body is never retried here.
    pub async fn execute_put_body_raw(
        &self,
        obj: &ObjectId,
        headers: &[(String, String)],
        body: HttpRequestBody,
        len: u64,
        payload_hash: &str,
    ) -> std::result::Result<HttpResponse, String> {
        let mut request = self.build_streamed_put(obj, len, None);
        request.headers.extend_from_slice(headers);
        let request = self.signed_with_payload_hash(request, payload_hash);
        self.http.execute_body(request, body, len).await
    }

    /// Execute a PUT from a verified spool file without buffering it in memory.
    pub async fn execute_put_file_raw(
        &self,
        obj: &ObjectId,
        headers: &[(String, String)],
        path: &Path,
        len: u64,
        payload_hash: &str,
    ) -> std::result::Result<HttpResponse, String> {
        let mut request = self.build_streamed_put(obj, len, None);
        request.headers.extend_from_slice(headers);
        let request = self.signed_with_payload_hash(request, payload_hash);
        self.http.execute_file(request, path, len).await
    }

    /// Execute CopyObject. `headers` contains only adapter-validated copy and
    /// metadata headers; incoming client credentials are never accepted here.
    pub async fn execute_copy_raw(
        &self,
        obj: &ObjectId,
        headers: &[(String, String)],
    ) -> std::result::Result<HttpResponse, String> {
        let mut request = self.build_streamed_put(obj, 0, None);
        request.headers.extend_from_slice(headers);
        self.http.execute(self.signed(request)).await
    }

    /// Execute an idempotent raw object DELETE with validated conditions.
    pub async fn execute_delete_raw(
        &self,
        obj: &ObjectId,
        conditions: &[(String, String)],
    ) -> std::result::Result<HttpResponse, String> {
        let mut request = self.build_delete(obj);
        request.headers.extend_from_slice(conditions);
        self.http.execute(self.signed(request)).await
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

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        cursor: Option<&str>,
        max_keys: u32,
    ) -> Result<ListPage> {
        // S3 caps a page at 1000 regardless of what is asked, so clamping here
        // keeps the request honest rather than relying on the service to
        // silently reduce it.
        let max_keys = max_keys.clamp(1, 1000);
        let mut query = format!("list-type=2&max-keys={max_keys}");
        if !prefix.is_empty() {
            query.push_str(&format!("&prefix={}", encode_query_value(prefix)));
        }
        if let Some(token) = cursor {
            query.push_str(&format!(
                "&continuation-token={}",
                encode_query_value(token)
            ));
        }
        let url = format!("{}?{query}", self.bucket_url(bucket));
        let req = self.signed(HttpRequest::new(Method::Get, url, Vec::new()));
        let resp = self.http.execute(req).await.map_err(Error::Backend)?;
        if resp.status == 404 {
            return Err(Error::NotFound(bucket.to_string()));
        }
        if !resp.is_success() {
            return Err(Error::Backend(format!(
                "S3 list {bucket}/{prefix} -> HTTP {}",
                resp.status
            )));
        }
        let body = std::str::from_utf8(&resp.body)
            .map_err(|e| Error::Backend(format!("S3 listing body is not UTF-8: {e}")))?;
        Self::parse_list_response(body)
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
        last_body: Mutex<Option<(HttpRequest, Vec<u8>, u64)>>,
        response: HttpResponse,
    }

    impl MockHttp {
        fn new(response: HttpResponse) -> Arc<Self> {
            Arc::new(Self {
                last: Mutex::new(None),
                last_file: Mutex::new(None),
                last_body: Mutex::new(None),
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

        async fn execute_body(
            &self,
            req: HttpRequest,
            mut body: HttpRequestBody,
            len: u64,
        ) -> std::result::Result<HttpResponse, String> {
            use futures::StreamExt as _;
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            *self.last_body.lock().unwrap() = Some((req, bytes, len));
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
    async fn raw_list_v2_encodes_and_signs_gateway_parameters() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::new(),
        });
        let mut config = S3Config::aws("us-east-1");
        config.path_style = true;
        config.tls = false;
        config.endpoint = "localhost:4566".into();
        let s3 = S3Backend::new(config, creds(), http.clone());

        s3.execute_list_raw(
            "my-bucket",
            "a/b",
            Some("/"),
            Some("next token"),
            7,
            Some("url"),
        )
        .await
        .unwrap();

        let request = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(request.method, Method::Get);
        assert_eq!(
            request.url,
            "http://localhost:4566/my-bucket?list-type=2&max-keys=7&prefix=a%2Fb&delimiter=%2F&continuation-token=next%20token&encoding-type=url"
        );
        assert!(request.header("authorization").is_some());
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

    /// A real ListObjectsV2 body, abbreviated. Parsing must pair each key with
    /// its own size rather than reading fields document-wide.
    const LIST_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>data/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/a.parquet</Key>
    <LastModified>2026-01-01T00:00:00.000Z</LastModified>
    <ETag>&quot;abc&quot;</ETag>
    <Size>1048576</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>data/b.parquet</Key>
    <LastModified>2026-01-02T00:00:00.000Z</LastModified>
    <ETag>&quot;def&quot;</ETag>
    <Size>42</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#;

    #[test]
    fn list_parses_keys_and_sizes_pairwise() {
        let page = S3Backend::parse_list_response(LIST_BODY).unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "data/a.parquet");
        assert_eq!(page.objects[0].size, 1_048_576);
        assert_eq!(page.objects[1].key, "data/b.parquet");
        assert_eq!(page.objects[1].size, 42);
        assert_eq!(page.next, None, "IsTruncated=false means no more pages");
    }

    /// A truncated listing must surface its continuation token, or the caller
    /// silently sees only the first page of a large prefix.
    #[test]
    fn list_returns_the_continuation_token_when_truncated() {
        let body = r#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>1PagE2Tok</NextContinuationToken>
  <Contents><Key>a</Key><Size>1</Size></Contents>
</ListBucketResult>"#;
        let page = S3Backend::parse_list_response(body).unwrap();
        assert_eq!(page.next.as_deref(), Some("1PagE2Tok"));
    }

    /// S3 may echo a token on the final page; trusting it without checking
    /// IsTruncated causes an extra round trip and, worse, a caller that never
    /// terminates if the service keeps echoing it.
    #[test]
    fn list_ignores_a_token_when_not_truncated() {
        let body = r#"<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <NextContinuationToken>stale</NextContinuationToken>
  <Contents><Key>a</Key><Size>1</Size></Contents>
</ListBucketResult>"#;
        let page = S3Backend::parse_list_response(body).unwrap();
        assert_eq!(page.next, None);
    }

    /// Keys legitimately contain `&`, which arrives escaped. Leaving it escaped
    /// names an object that does not exist.
    #[test]
    fn list_unescapes_keys() {
        let body = r#"<ListBucketResult><IsTruncated>false</IsTruncated>
  <Contents><Key>a&amp;b/c.bin</Key><Size>7</Size></Contents></ListBucketResult>"#;
        let page = S3Backend::parse_list_response(body).unwrap();
        assert_eq!(page.objects[0].key, "a&b/c.bin");
    }

    #[test]
    fn list_of_an_empty_prefix_is_an_empty_page_not_an_error() {
        let body = r#"<ListBucketResult><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>"#;
        let page = S3Backend::parse_list_response(body).unwrap();
        assert!(page.objects.is_empty());
        assert_eq!(page.next, None);
    }

    /// An entry missing its size is a malformed response, not a zero-byte
    /// object — reporting it as zero would make a reader believe the object is
    /// empty.
    #[test]
    fn list_rejects_an_entry_without_a_size() {
        let body = r#"<ListBucketResult><IsTruncated>false</IsTruncated>
  <Contents><Key>a</Key></Contents></ListBucketResult>"#;
        assert!(S3Backend::parse_list_response(body).is_err());
    }

    #[tokio::test]
    async fn list_request_carries_query_and_is_signed() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::from_static(LIST_BODY.as_bytes()),
        });
        let backend = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        let page = backend
            .list_objects("bucket", "data/", None, 1000)
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 2);

        let req = http.last.lock().unwrap().clone().unwrap();
        assert!(req.url.contains("list-type=2"), "url: {}", req.url);
        assert!(
            req.url.contains("prefix=data%2F"),
            "prefix must be encoded: {}",
            req.url
        );
        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("authorization")),
            "listing must be signed"
        );
    }

    /// max_keys above S3's ceiling is clamped rather than sent verbatim, so a
    /// caller cannot ask for an unbounded page.
    #[tokio::test]
    async fn list_clamps_max_keys_to_the_service_maximum() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::from_static(LIST_BODY.as_bytes()),
        });
        let backend = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        backend
            .list_objects("bucket", "", None, 100_000)
            .await
            .unwrap();
        let req = http.last.lock().unwrap().clone().unwrap();
        assert!(req.url.contains("max-keys=1000"), "url: {}", req.url);
    }

    #[tokio::test]
    async fn raw_streamed_put_is_resigned_and_preserves_metadata() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: vec![("etag".into(), "\"v2\"".into())],
            body: bytes::Bytes::new(),
        });
        let backend = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        let body = futures::stream::iter([Ok(bytes::Bytes::from_static(b"abc"))]);
        backend
            .execute_put_body_raw(
                &obj(),
                &[("x-amz-meta-owner".into(), "team-a".into())],
                Box::pin(body),
                3,
                "UNSIGNED-PAYLOAD",
            )
            .await
            .unwrap();

        let (request, body, len) = http.last_body.lock().unwrap().clone().unwrap();
        assert_eq!(body, b"abc");
        assert_eq!(len, 3);
        assert_eq!(
            request.header("x-amz-content-sha256"),
            Some("UNSIGNED-PAYLOAD")
        );
        assert_eq!(request.header("x-amz-meta-owner"), Some("team-a"));
        let authorization = request.header("authorization").unwrap();
        assert!(authorization.contains("x-amz-meta-owner"));
    }

    #[tokio::test]
    async fn copy_source_is_signed_with_origin_credentials() {
        let http = MockHttp::new(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: bytes::Bytes::new(),
        });
        let backend = S3Backend::new(S3Config::aws("us-east-1"), creds(), http.clone());
        backend
            .execute_copy_raw(
                &obj(),
                &[("x-amz-copy-source".into(), "/source/key".into())],
            )
            .await
            .unwrap();
        let request = http.last.lock().unwrap().clone().unwrap();
        assert_eq!(request.header("x-amz-copy-source"), Some("/source/key"));
        assert!(request
            .header("authorization")
            .unwrap()
            .contains("x-amz-copy-source"));
    }
}
