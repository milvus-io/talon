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

use std::sync::Arc;

use async_trait::async_trait;
use talon_core::{BackendStore, Error, ObjectId, ObjectStat, Result, Version};

use crate::http::{HttpClient, HttpRequest, Method};

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

    /// Format an inclusive HTTP `Range` header for `[offset, offset+len)`.
    pub fn range_header(offset: u64, len: u64) -> String {
        format!("bytes={offset}-{}", offset + len - 1)
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
        }
    }

    /// Build the HEAD request (exposed for testing).
    pub fn build_head(&self, obj: &ObjectId) -> HttpRequest {
        HttpRequest {
            method: Method::Head,
            url: self.object_url(obj),
            headers: self.auth_headers(),
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
}
