//! Minimal HTTP abstraction for backend stores.
//!
//! Backend implementations ([`crate::s3`] etc.) issue ranged GETs and HEADs
//! against an object-store REST endpoint. To keep the crate dependency-light and
//! **offline-testable**, they are generic over this [`HttpClient`] trait rather
//! than bound to a concrete HTTP library: production wires a real client
//! (reqwest/hyper), tests inject a mock that returns canned responses and
//! asserts on the constructed [`HttpRequest`].

use async_trait::async_trait;

/// An HTTP method (only the verbs the backends need).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// GET, used for ranged object reads.
    Get,
    /// HEAD, used to fetch size + etag without the body.
    Head,
}

/// An outgoing HTTP request built by a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// Request method.
    pub method: Method,
    /// Fully-qualified URL.
    pub url: String,
    /// Header name/value pairs (in insertion order).
    pub headers: Vec<(String, String)>,
}

impl HttpRequest {
    /// Look up the first header value with a case-insensitive name match.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// An HTTP response returned to a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// Response body (empty for HEAD).
    pub body: bytes::Bytes,
}

impl HttpResponse {
    /// Look up the first response header value (case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Whether the status is in the 2xx range.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// An async HTTP transport a backend uses to talk to its origin store.
#[async_trait]
pub trait HttpClient: Send + Sync {
    /// Execute `req` and return the response, or an error string on transport
    /// failure (DNS/connect/timeout).
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String>;
}
