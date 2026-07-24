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
/// correcting for servers that ignore the `Range` header (issues #117, #161).
///
/// - **206 Partial Content**: the body is the requested range. `content_range`
///   (the `Content-Range` header, if any) is validated to start at `offset`, so
///   a server that returns 206 but ignores the offset is rejected. A body whose
///   length equals `len` is accepted; a *longer* body is rejected. A *shorter*
///   body is accepted only when `Content-Range` shows it is the tail of the
///   object (`end == total-1` and `offset+got == total`) — i.e. the requested
///   range legitimately ran past EOF (the last block of an object whose size is
///   not a multiple of the block size). Without that evidence a short 206 is a
///   truncated read and is rejected.
/// - **200 OK**: the server ignored `Range` and returned the **whole object**.
///   We slice the full body at `[offset, offset+len)` (clamped at EOF) so the
///   caller always receives exactly the requested window rather than the object
///   head.
///
/// Any other status is the caller's responsibility (404/errors handled there);
/// this is only invoked for 200/206.
pub fn range_body(
    status: u16,
    body: bytes::Bytes,
    offset: u64,
    len: u64,
    content_range: Option<&str>,
) -> Result<bytes::Bytes, String> {
    match status {
        206 => {
            // If the server sent Content-Range, verify it starts at the offset
            // we asked for. A server that returns 206 but ignores the offset
            // (bytes [0,len) instead of [offset,offset+len)) would otherwise be
            // cached as the requested window — silent corruption.
            let parsed = content_range.map(parse_content_range).transpose()?;
            if let Some(ContentRange { start, .. }) = parsed {
                if start != offset {
                    return Err(format!(
                        "ranged GET returned 206 Content-Range starting at {start}, expected {offset}"
                    ));
                }
            }
            let got = body.len() as u64;
            if got == len {
                return Ok(body);
            }
            if got > len {
                // More bytes than requested — the server misbehaved; trimming
                // could hide a range mismatch, so reject rather than guess.
                return Err(format!(
                    "ranged GET returned 206 with {got} bytes, more than the requested {len}"
                ));
            }
            // Short body: acceptable only if it is the tail of the object (the
            // requested range legitimately extends past EOF, e.g. the last block
            // of an object whose size is not a multiple of the block size). We
            // trust that when Content-Range reports the response reaches the last
            // byte of the object (end == total-1). Without Content-Range we
            // cannot distinguish EOF from a truncated read, so reject.
            match parsed {
                Some(ContentRange {
                    end,
                    total: Some(total),
                    ..
                }) if end.checked_add(1) == Some(total)
                    && offset.checked_add(got) == Some(total) =>
                {
                    Ok(body)
                }
                _ => Err(format!(
                    "ranged GET returned 206 with {got} bytes, expected {len} \
                     (not an object-end short read)"
                )),
            }
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

/// A parsed `Content-Range: bytes <start>-<end>/<total>` header (RFC 7233).
struct ContentRange {
    start: u64,
    end: u64,
    /// The object's total length, or `None` if the server sent `*`.
    total: Option<u64>,
}

/// Parse a `Content-Range` response header value. Accepts `bytes 0-99/1234` and
/// `bytes 0-99/*`; rejects anything else.
fn parse_content_range(value: &str) -> Result<ContentRange, String> {
    let malformed = || format!("malformed Content-Range: {value:?}");
    let rest = value.trim().strip_prefix("bytes ").ok_or_else(malformed)?;
    let (range, total) = rest.split_once('/').ok_or_else(malformed)?;
    let (start, end) = range.split_once('-').ok_or_else(malformed)?;
    let start = start.trim().parse::<u64>().map_err(|_| malformed())?;
    let end = end.trim().parse::<u64>().map_err(|_| malformed())?;
    if end < start {
        return Err(malformed());
    }
    let total = match total.trim() {
        "*" => None,
        t => Some(t.parse::<u64>().map_err(|_| malformed())?),
    };
    Ok(ContentRange { start, end, total })
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
        let out = range_body(206, body.clone(), 256, 100, None).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn short_206_without_content_range_is_rejected() {
        // A 206 shorter than requested, with no Content-Range to prove it's the
        // object tail, is a truncated read and must be rejected.
        let body = Bytes::from(vec![7u8; 50]);
        assert!(range_body(206, body, 0, 100, None).is_err());
    }

    #[test]
    fn short_206_at_object_end_is_accepted() {
        // Last block of a 300-byte object: request [256, 512) (len 256) but only
        // 44 bytes remain. A compliant server answers 206 with Content-Range
        // bytes 256-299/300 and 44 bytes — this must be accepted, not rejected
        // (issue #161: previously the last block of every non-aligned object
        // failed on range-honoring stores).
        let body = Bytes::from(vec![9u8; 44]);
        let out = range_body(206, body.clone(), 256, 256, Some("bytes 256-299/300")).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn short_206_not_at_object_end_is_rejected() {
        // A short 206 whose Content-Range does NOT reach the object end is a
        // genuine truncated read.
        let body = Bytes::from(vec![9u8; 50]);
        assert!(range_body(206, body, 0, 100, Some("bytes 0-49/1000")).is_err());
    }

    #[test]
    fn over_long_206_is_rejected() {
        let body = Bytes::from(vec![9u8; 150]);
        assert!(range_body(206, body, 0, 100, Some("bytes 0-149/1000")).is_err());
    }

    #[test]
    fn offset_ignoring_206_is_rejected() {
        // Server returned 206 but the range starts at 0, not the requested 256.
        let body = Bytes::from(vec![9u8; 100]);
        assert!(range_body(206, body, 256, 100, Some("bytes 0-99/1000")).is_err());
    }

    #[test]
    fn honored_206_with_matching_content_range_is_accepted() {
        let body = Bytes::from(vec![9u8; 100]);
        let out = range_body(206, body.clone(), 256, 100, Some("bytes 256-355/1000")).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn malformed_content_range_is_rejected() {
        let body = Bytes::from(vec![9u8; 100]);
        assert!(range_body(206, body, 256, 100, Some("garbage")).is_err());
    }

    #[test]
    fn content_range_with_unknown_total_short_read_is_rejected() {
        // total == "*" means we can't prove EOF, so a short body is rejected.
        let body = Bytes::from(vec![9u8; 50]);
        assert!(range_body(206, body, 0, 100, Some("bytes 0-49/*")).is_err());
    }

    #[test]
    fn whole_object_200_is_sliced_to_the_requested_window() {
        // Server ignored Range and returned a 300-byte whole object; a request
        // for [100, 200) must yield bytes 100..200, not the object head.
        let object: Vec<u8> = (0..255u8).cycle().take(300).collect();
        let body = Bytes::from(object.clone());
        let out = range_body(200, body, 100, 100, None).unwrap();
        assert_eq!(out.len(), 100);
        assert_eq!(&out[..], &object[100..200]);
    }

    #[test]
    fn whole_object_200_final_block_clamps_at_eof() {
        // Requested window runs past the object end; return what remains.
        let object: Vec<u8> = (0..255u8).cycle().take(300).collect();
        let body = Bytes::from(object.clone());
        let out = range_body(200, body, 256, 256, None).unwrap();
        assert_eq!(&out[..], &object[256..300]);
    }

    #[test]
    fn whole_object_200_offset_past_eof_is_empty() {
        let body = Bytes::from(vec![1u8; 100]);
        let out = range_body(200, body, 500, 100, None).unwrap();
        assert!(out.is_empty());
    }
}
