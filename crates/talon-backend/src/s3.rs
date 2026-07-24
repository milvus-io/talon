//! S3 (and S3-compatible) [`BackendStore`] implementation.
//!
//! Fetches block/page ranges via S3 **ranged GET** and object metadata via
//! **HEAD**, mapping the ETag to a [`Version`] so a source update invalidates
//! the cache key. The store is generic over an [`HttpClient`] so request
//! construction, range-header formatting, endpoint/vhost URL building, and
//! response parsing are all unit-testable without network access; a real client
//! is injected in production.
//!
//! Authentication is intentionally pluggable via [`S3Credentials`]: this v1 cut
//! wires the pieces and formats the request (including the `Range` header and
//! optional session token), leaving live SigV4 wire-signing to the concrete
//! networked client. Endpoints are configurable so MinIO/Ceph and other
//! S3-compatible stores work.

use std::sync::Arc;

use async_trait::async_trait;
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
    #[allow(dead_code)]
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
    pub fn range_header(offset: u64, len: u64) -> String {
        // HTTP ranges are inclusive on both ends.
        let end = offset + len - 1;
        format!("bytes={offset}-{end}")
    }

    /// Build the ranged GET request for a fetch (exposed for testing).
    pub fn build_get(&self, obj: &ObjectId, offset: u64, len: u64) -> HttpRequest {
        let mut headers = vec![("Range".to_string(), Self::range_header(offset, len))];
        if let Some(tok) = &self.creds.session_token {
            headers.push(("x-amz-security-token".to_string(), tok.clone()));
        }
        HttpRequest {
            method: Method::Get,
            url: self.object_url(obj),
            headers,
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
        }
    }
}

/// Normalize an S3 ETag into a [`Version`] (strip surrounding quotes).
fn etag_to_version(etag: &str) -> Version {
    Version::new(etag.trim_matches('"').to_string())
}

#[async_trait]
impl BackendStore for S3Backend {
    async fn fetch_range(&self, obj: &ObjectId, offset: u64, len: u64) -> Result<bytes::Bytes> {
        if len == 0 {
            return Ok(bytes::Bytes::new());
        }
        let req = self.build_get(obj, offset, len);
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
        } else {
            Err(Error::Backend(format!(
                "S3 GET {} -> HTTP {}",
                obj.to_path(),
                resp.status
            )))
        }
    }

    async fn head(&self, obj: &ObjectId) -> Result<ObjectStat> {
        let req = self.build_head(obj);
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
}
