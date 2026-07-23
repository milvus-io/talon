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

/// Extract the bytes for a ranged GET of `[offset, offset+len)` from a response,
/// correcting for servers that ignore the `Range` header (issue #117).
///
/// - **206 Partial Content**: the body already *is* the requested range. We
///   accept it but still verify its length matches `len` (a 206 with a short
///   body would otherwise be cached as correct); a mismatch is an error.
/// - **200 OK**: the server ignored `Range` and returned the **whole object**.
///   A proxy or non-compliant S3 store does this. Naively caching that body
///   under the block's `BlockId` (then slicing at `offset_in_block`) serves the
///   *head* of the object for every non-zero-offset block. Instead we slice the
///   full-object body at `[offset, offset+len)` so the caller always receives
///   exactly the requested range. If the body is shorter than `offset` the
///   requested range lies past EOF and yields empty; the final block is clamped
///   to whatever remains.
///
/// Any other status is the caller's responsibility (404/errors handled there);
/// this is only invoked for 200/206.
pub fn range_body(
    status: u16,
    body: bytes::Bytes,
    offset: u64,
    len: u64,
) -> Result<bytes::Bytes, String> {
    match status {
        206 => {
            // Trust the server's partial content, but reject a short partial:
            // caching fewer bytes than requested as a "hit" is silent corruption.
            if (body.len() as u64) < len {
                return Err(format!(
                    "ranged GET returned 206 with {} bytes, expected {len}",
                    body.len()
                ));
            }
            Ok(body)
        }
        200 => {
            // Whole object returned; slice out the requested window.
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(body.len());
            let end = offset
                .checked_add(len)
                .and_then(|e| usize::try_from(e).ok())
                .unwrap_or(usize::MAX)
                .min(body.len());
            Ok(body.slice(start..end))
        }
        other => Err(format!("range_body called with unexpected status {other}")),
    }
}

/// An async HTTP transport a backend uses to talk to its origin store.
#[async_trait]
pub trait HttpClient: Send + Sync {
    /// Execute `req` and return the response, or an error string on transport
    /// failure (DNS/connect/timeout).
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String>;
}

#[cfg(test)]
mod tests {
    use super::range_body;
    use bytes::Bytes;

    #[test]
    fn partial_206_is_trusted_when_full_length() {
        let body = Bytes::from(vec![7u8; 100]);
        let out = range_body(206, body.clone(), 256, 100).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn short_206_is_rejected() {
        // A 206 that returned fewer bytes than requested must not be cached as a
        // correct block.
        let body = Bytes::from(vec![7u8; 50]);
        assert!(range_body(206, body, 0, 100).is_err());
    }

    #[test]
    fn whole_object_200_is_sliced_to_the_requested_window() {
        // Server ignored Range and returned a 300-byte whole object; a request
        // for [100, 200) must yield bytes 100..200, not the object head.
        let object: Vec<u8> = (0..255u8).cycle().take(300).collect();
        let body = Bytes::from(object.clone());
        let out = range_body(200, body, 100, 100).unwrap();
        assert_eq!(out.len(), 100);
        assert_eq!(&out[..], &object[100..200]);
    }

    #[test]
    fn whole_object_200_final_block_clamps_at_eof() {
        // Requested window runs past the object end; return what remains.
        let object: Vec<u8> = (0..255u8).cycle().take(300).collect();
        let body = Bytes::from(object.clone());
        let out = range_body(200, body, 256, 256).unwrap();
        assert_eq!(&out[..], &object[256..300]);
    }

    #[test]
    fn whole_object_200_offset_past_eof_is_empty() {
        let body = Bytes::from(vec![1u8; 100]);
        let out = range_body(200, body, 500, 100).unwrap();
        assert!(out.is_empty());
    }
}
