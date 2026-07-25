//! Azure Blob **Shared Key** authorization.
//!
//! Signs a request with the storage account's shared key as an alternative to a
//! SAS token. Builds the `StringToSign` (verb, a fixed set of standard headers,
//! the canonicalized `x-ms-*` headers, and the canonicalized resource), HMACs it
//! with the base64-decoded account key, and returns the
//! `Authorization: SharedKey <account>:<signature>` header value.
//!
//! Only what the blob backend needs is implemented: the full `SharedKey` scheme
//! (not `SharedKeyLite`), header-based signing, and requests that carry no query
//! string (so the canonicalized resource is just `/account/path`). Signing is a
//! pure function of the request + account + key, unit-testable offline.
//!
//! Reference: <https://learn.microsoft.com/rest/api/storageservices/authorize-with-shared-key>

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::http::{HttpRequest, Method};

type HmacSha256 = Hmac<Sha256>;

/// Format a `SystemTime` as an RFC 1123 date in GMT (the `x-ms-date` format),
/// e.g. `Fri, 26 Jun 2015 23:39:12 GMT`.
pub fn rfc1123_date(t: std::time::SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (secs / 86_400) as i64;
    let sod = secs % 86_400;
    let (hh, mm, ss) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    // Day of week: 1970-01-01 was a Thursday (index 4 with Sun=0).
    const DOW: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    let dow = DOW[(((days % 7) + 4 + 7) % 7) as usize];
    let (y, mo, d) = civil_from_days(days);
    const MON: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let mon = MON[(mo - 1) as usize];
    format!("{dow}, {d:02} {mon} {y:04} {hh:02}:{mm:02}:{ss:02} GMT")
}

/// Days since 1970-01-01 → civil (year, month, day). Howard Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn method_str(m: Method) -> &'static str {
    match m {
        Method::Get => "GET",
        Method::Head => "HEAD",
        Method::Put => "PUT",
        Method::Delete => "DELETE",
    }
}

/// Extract host + path from a URL (path keeps its leading `/`, query dropped).
fn host_and_path(url: &str) -> (String, String) {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let (host, rest) = match after_scheme.split_once('/') {
        Some((h, r)) => (h.to_string(), format!("/{r}")),
        None => (after_scheme.to_string(), "/".to_string()),
    };
    let path = rest.split_once('?').map(|(p, _)| p).unwrap_or(&rest);
    (host, path.to_string())
}

/// Compute the `Authorization: SharedKey ...` header value for `req`.
///
/// `account` is the storage account name; `key_b64` is the base64-encoded
/// account key. `req` must already carry its `x-ms-date` and `x-ms-version`
/// headers (the caller sets these) plus any `x-ms-range` etc. Returns `Err` if
/// the key is not valid base64.
pub fn authorization_header(
    req: &HttpRequest,
    account: &str,
    key_b64: &str,
) -> Result<String, String> {
    let key = BASE64
        .decode(key_b64)
        .map_err(|e| format!("invalid base64 account key: {e}"))?;

    let header = |name: &str| -> String {
        req.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().to_string())
            .unwrap_or_default()
    };

    // Content-Length is signed as empty when 0 (per the 2015+ signing rules).
    let content_length = match req.body.len() {
        0 => String::new(),
        n => n.to_string(),
    };

    // Canonicalized headers: all x-ms-* headers, lowercased, sorted, "k:v" joined
    // by \n. Values trimmed; requests here carry single-valued headers.
    let mut xms: Vec<(String, String)> = req
        .headers
        .iter()
        .filter(|(k, _)| k.to_ascii_lowercase().starts_with("x-ms-"))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    xms.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_headers = xms
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect::<String>();

    // Canonicalized resource: /<account><path>. No query params today.
    let (_host, path) = host_and_path(&req.url);
    let canonical_resource = format!("/{account}{path}");

    // The 20-line StringToSign (verb + standard headers + canonical headers +
    // canonical resource). Empty standard headers are blank lines.
    let string_to_sign = format!(
        "{verb}\n\
         {content_encoding}\n\
         {content_language}\n\
         {content_length}\n\
         {content_md5}\n\
         {content_type}\n\
         {date}\n\
         {if_modified_since}\n\
         {if_match}\n\
         {if_none_match}\n\
         {if_unmodified_since}\n\
         {range}\n\
         {canonical_headers}{canonical_resource}",
        verb = method_str(req.method),
        content_encoding = header("content-encoding"),
        content_language = header("content-language"),
        content_length = content_length,
        content_md5 = header("content-md5"),
        content_type = header("content-type"),
        date = "", // signed via x-ms-date instead; Date header left blank
        if_modified_since = header("if-modified-since"),
        if_match = header("if-match"),
        if_none_match = header("if-none-match"),
        if_unmodified_since = header("if-unmodified-since"),
        range = header("range"), // HTTP Range; blob uses x-ms-range (an x-ms header)
        canonical_headers = canonical_headers,
        canonical_resource = canonical_resource,
    );

    let mut mac = HmacSha256::new_from_slice(&key).map_err(|e| e.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());
    Ok(format!("SharedKey {account}:{signature}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(method: Method, url: &str, headers: Vec<(&str, &str)>) -> HttpRequest {
        HttpRequest {
            method,
            url: url.into(),
            headers: headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
        }
    }

    // A valid base64 key (the string "0123456789abcdef0123456789abcdef" encoded)
    // is deterministic across runs; we assert structure + stability, not a
    // service-verified vector (which needs a live account).
    const KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";

    #[test]
    fn rejects_bad_base64_key() {
        let r = req(
            Method::Get,
            "https://acct.blob.core.windows.net/c/b",
            vec![],
        );
        assert!(authorization_header(&r, "acct", "not base64!!!").is_err());
    }

    #[test]
    fn produces_sharedkey_header_with_account_and_signature() {
        let r = req(
            Method::Get,
            "https://acct.blob.core.windows.net/container/blob.bin",
            vec![
                ("x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT"),
                ("x-ms-version", "2021-12-02"),
                ("x-ms-range", "bytes=0-1023"),
            ],
        );
        let auth = authorization_header(&r, "acct", KEY).unwrap();
        assert!(auth.starts_with("SharedKey acct:"));
        // Signature is base64 and non-empty.
        let sig = auth.strip_prefix("SharedKey acct:").unwrap();
        assert!(!sig.is_empty());
        assert!(BASE64.decode(sig).is_ok());
    }

    #[test]
    fn signature_is_deterministic_for_same_input() {
        let r = req(
            Method::Get,
            "https://acct.blob.core.windows.net/c/b",
            vec![
                ("x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT"),
                ("x-ms-version", "2021-12-02"),
            ],
        );
        let a = authorization_header(&r, "acct", KEY).unwrap();
        let b = authorization_header(&r, "acct", KEY).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn different_xms_headers_change_the_signature() {
        let base = req(
            Method::Get,
            "https://acct.blob.core.windows.net/c/b",
            vec![
                ("x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT"),
                ("x-ms-version", "2021-12-02"),
            ],
        );
        let with_range = req(
            Method::Get,
            "https://acct.blob.core.windows.net/c/b",
            vec![
                ("x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT"),
                ("x-ms-version", "2021-12-02"),
                ("x-ms-range", "bytes=0-9"),
            ],
        );
        assert_ne!(
            authorization_header(&base, "acct", KEY).unwrap(),
            authorization_header(&with_range, "acct", KEY).unwrap()
        );
    }

    #[test]
    fn canonical_resource_uses_account_and_path() {
        // Path-style emulator URL: host has a port, path is /account/container/blob.
        let (host, path) = host_and_path("http://127.0.0.1:10000/devstoreaccount1/c/b");
        assert_eq!(host, "127.0.0.1:10000");
        assert_eq!(path, "/devstoreaccount1/c/b");
    }
}
