//! Incoming Amazon S3 Signature Version 4 authentication.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::{
    AuthenticatedPrincipal, EffectiveDecision, GatewayAuthenticationError, GatewayAuthenticator,
    ProviderProtocol, S3_CACHE_MARK_HEADER,
};

type HmacSha256 = Hmac<Sha256>;
const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const MAX_PRESIGNED_EXPIRY: u64 = 7 * 24 * 60 * 60;

/// One configured S3 client credential and its policy identity.
#[derive(Clone)]
pub struct S3ClientIdentity {
    /// Incoming access key identifier.
    pub access_key_id: String,
    /// Incoming secret access key. This is never logged or sent to the origin.
    pub secret_access_key: String,
    /// Required STS token, when this is a temporary credential.
    pub session_token: Option<String>,
    /// Principal and provider account used by authorization.
    pub principal: AuthenticatedPrincipal,
}

/// Header and presigned SigV4 verifier for one configured S3 region.
pub struct S3SigV4Authenticator {
    region: String,
    max_clock_skew: Duration,
    identities: HashMap<String, S3ClientIdentity>,
}

impl S3SigV4Authenticator {
    /// Validate and index incoming identities.
    pub fn new(
        region: impl Into<String>,
        identities: Vec<S3ClientIdentity>,
        max_clock_skew: Duration,
    ) -> Result<Self, S3IdentityError> {
        let region = region.into();
        if region.is_empty() || max_clock_skew.is_zero() || identities.is_empty() {
            return Err(S3IdentityError::InvalidConfiguration);
        }
        let mut indexed = HashMap::with_capacity(identities.len());
        for identity in identities {
            if identity.access_key_id.is_empty()
                || identity.secret_access_key.is_empty()
                || identity.principal.id.is_empty()
                || identity.principal.provider_account.is_empty()
                || identity.session_token.as_deref() == Some("")
            {
                return Err(S3IdentityError::InvalidConfiguration);
            }
            let key = identity.access_key_id.clone();
            if indexed.insert(key, identity).is_some() {
                return Err(S3IdentityError::DuplicateAccessKey);
            }
        }
        Ok(Self {
            region,
            max_clock_skew,
            identities: indexed,
        })
    }

    /// Authenticate at an explicit time for deterministic tests.
    pub fn authenticate_at(
        &self,
        request: &Request,
        now: SystemTime,
    ) -> Result<AuthenticatedPrincipal, GatewayAuthenticationError> {
        self.authenticate_inner(request, now)
            .map_err(|_| GatewayAuthenticationError)
    }

    fn authenticate_inner(
        &self,
        request: &Request,
        now: SystemTime,
    ) -> Result<AuthenticatedPrincipal, AuthFailure> {
        let query = parse_query(request.uri().query().unwrap_or(""))?;
        let signed = if query_value(&query, "X-Amz-Algorithm")?.is_some() {
            parse_presigned(request, &query, now, self.max_clock_skew)?
        } else {
            parse_header_signed(request, now, self.max_clock_skew)?
        };
        if signed.scope.region != self.region || signed.scope.service != "s3" {
            return Err(AuthFailure);
        }
        let identity = self
            .identities
            .get(&signed.scope.access_key_id)
            .ok_or(AuthFailure)?;
        validate_session_token(identity, request, &query, &signed)?;
        validate_payload_hash(request, &signed)?;
        validate_cache_mark(request, &signed.signed_headers)?;

        let canonical_headers = canonical_headers(request, &signed.signed_headers)?;
        let canonical_query = canonical_query(&query, signed.presigned);
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request.method().as_str(),
            canonical_uri(request.uri().path()),
            canonical_query,
            canonical_headers,
            signed.signed_headers.join(";"),
            signed.payload_hash
        );
        let scope = format!(
            "{}/{}/{}/aws4_request",
            signed.scope.date, signed.scope.region, signed.scope.service
        );
        let string_to_sign = format!(
            "{ALGORITHM}\n{}\n{}\n{}",
            signed.datetime,
            scope,
            sha256_hex(canonical_request.as_bytes())
        );
        verify_signature(
            &identity.secret_access_key,
            &signed.scope,
            string_to_sign.as_bytes(),
            &signed.signature,
        )?;
        Ok(identity.principal.clone())
    }
}

impl GatewayAuthenticator for S3SigV4Authenticator {
    fn authenticate(
        &self,
        request: &Request,
        protocol: ProviderProtocol,
    ) -> Result<AuthenticatedPrincipal, GatewayAuthenticationError> {
        if protocol != ProviderProtocol::S3 {
            return Err(GatewayAuthenticationError);
        }
        self.authenticate_at(request, SystemTime::now())
    }
}

/// Invalid incoming identity configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum S3IdentityError {
    /// A required bound or identity field is empty.
    #[error("S3 client identity configuration is invalid")]
    InvalidConfiguration,
    /// Access key identifiers must be unique.
    #[error("S3 client identity access keys must be unique")]
    DuplicateAccessKey,
}

#[derive(Clone)]
struct CredentialScope {
    access_key_id: String,
    date: String,
    region: String,
    service: String,
}

struct SignedRequest {
    scope: CredentialScope,
    datetime: String,
    signed_headers: Vec<String>,
    signature: Vec<u8>,
    payload_hash: String,
    presigned: bool,
}

#[derive(Debug, Clone, Copy)]
struct AuthFailure;

fn parse_header_signed(
    request: &Request,
    now: SystemTime,
    max_clock_skew: Duration,
) -> Result<SignedRequest, AuthFailure> {
    let authorization = single_header(request, "authorization")?;
    let parameters = authorization
        .strip_prefix(&format!("{ALGORITHM} "))
        .ok_or(AuthFailure)?;
    let mut fields = HashMap::new();
    for field in parameters.split(',') {
        let (name, value) = field.trim().split_once('=').ok_or(AuthFailure)?;
        if value.is_empty() || fields.insert(name, value).is_some() {
            return Err(AuthFailure);
        }
    }
    if fields.len() != 3 {
        return Err(AuthFailure);
    }
    let scope = parse_scope(fields.get("Credential").copied().ok_or(AuthFailure)?)?;
    let signed_headers =
        parse_signed_headers(fields.get("SignedHeaders").copied().ok_or(AuthFailure)?)?;
    if !signed_headers.iter().any(|header| header == "x-amz-date")
        || !signed_headers
            .iter()
            .any(|header| header == "x-amz-content-sha256")
    {
        return Err(AuthFailure);
    }
    let datetime = single_header(request, "x-amz-date")?.to_string();
    let signed_at = parse_datetime(&datetime)?;
    validate_header_time(now, signed_at, max_clock_skew)?;
    if !datetime.starts_with(&scope.date) {
        return Err(AuthFailure);
    }
    Ok(SignedRequest {
        scope,
        datetime,
        signed_headers,
        signature: decode_signature(fields.get("Signature").copied().ok_or(AuthFailure)?)?,
        payload_hash: single_header(request, "x-amz-content-sha256")?.to_string(),
        presigned: false,
    })
}

fn parse_presigned(
    request: &Request,
    query: &[(String, String)],
    now: SystemTime,
    max_clock_skew: Duration,
) -> Result<SignedRequest, AuthFailure> {
    if request.headers().contains_key("authorization")
        || query_value(query, "X-Amz-Algorithm")? != Some(ALGORITHM)
    {
        return Err(AuthFailure);
    }
    let scope = parse_scope(query_value(query, "X-Amz-Credential")?.ok_or(AuthFailure)?)?;
    let datetime = query_value(query, "X-Amz-Date")?
        .ok_or(AuthFailure)?
        .to_string();
    let signed_at = parse_datetime(&datetime)?;
    if !datetime.starts_with(&scope.date) {
        return Err(AuthFailure);
    }
    let expires = query_value(query, "X-Amz-Expires")?
        .ok_or(AuthFailure)?
        .parse::<u64>()
        .map_err(|_| AuthFailure)?;
    if expires == 0 || expires > MAX_PRESIGNED_EXPIRY {
        return Err(AuthFailure);
    }
    validate_presigned_time(now, signed_at, Duration::from_secs(expires), max_clock_skew)?;
    let signed_headers =
        parse_signed_headers(query_value(query, "X-Amz-SignedHeaders")?.ok_or(AuthFailure)?)?;
    let payload_hash = match query_value(query, "X-Amz-Content-Sha256")? {
        Some(value) => value.to_string(),
        None => "UNSIGNED-PAYLOAD".to_string(),
    };
    Ok(SignedRequest {
        scope,
        datetime,
        signed_headers,
        signature: decode_signature(query_value(query, "X-Amz-Signature")?.ok_or(AuthFailure)?)?,
        payload_hash,
        presigned: true,
    })
}

fn parse_scope(value: &str) -> Result<CredentialScope, AuthFailure> {
    let parts = value.split('/').collect::<Vec<_>>();
    if parts.len() != 5
        || parts.iter().any(|part| part.is_empty())
        || parts[4] != "aws4_request"
        || parts[1].len() != 8
        || !parts[1].bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(AuthFailure);
    }
    Ok(CredentialScope {
        access_key_id: parts[0].to_string(),
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    })
}

fn parse_signed_headers(value: &str) -> Result<Vec<String>, AuthFailure> {
    let headers = value.split(';').map(str::to_string).collect::<Vec<_>>();
    if headers.is_empty()
        || headers.iter().any(|header| {
            header.is_empty()
                || header.bytes().any(|byte| {
                    !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                })
        })
        || !headers.windows(2).all(|pair| pair[0] < pair[1])
        || !headers.iter().any(|header| header == "host")
    {
        return Err(AuthFailure);
    }
    Ok(headers)
}

fn validate_session_token(
    identity: &S3ClientIdentity,
    request: &Request,
    query: &[(String, String)],
    signed: &SignedRequest,
) -> Result<(), AuthFailure> {
    let supplied = if signed.presigned {
        query_value(query, "X-Amz-Security-Token")?
    } else {
        request
            .headers()
            .get("x-amz-security-token")
            .map(|value| value.to_str().map_err(|_| AuthFailure))
            .transpose()?
    };
    if supplied != identity.session_token.as_deref() {
        return Err(AuthFailure);
    }
    if !signed.presigned
        && supplied.is_some()
        && !signed
            .signed_headers
            .iter()
            .any(|header| header == "x-amz-security-token")
    {
        return Err(AuthFailure);
    }
    Ok(())
}

fn validate_payload_hash(request: &Request, signed: &SignedRequest) -> Result<(), AuthFailure> {
    let no_payload = matches!(
        request.method().as_str(),
        "GET" | "HEAD" | "DELETE" | "OPTIONS"
    );
    if no_payload {
        let accepted =
            signed.payload_hash == EMPTY_SHA256 || signed.payload_hash == "UNSIGNED-PAYLOAD";
        if !accepted {
            return Err(AuthFailure);
        }
    } else if signed.payload_hash != "UNSIGNED-PAYLOAD"
        && (signed.payload_hash.len() != 64
            || !signed
                .payload_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    {
        // Streaming SigV4 chunk modes need per-chunk verification and are not
        // accepted as ordinary object PUTs.
        return Err(AuthFailure);
    }
    Ok(())
}

fn validate_cache_mark(request: &Request, signed_headers: &[String]) -> Result<(), AuthFailure> {
    let mut values = request.headers().get_all(S3_CACHE_MARK_HEADER).iter();
    if let Some(value) = values.next() {
        if values.next().is_some()
            || !signed_headers
                .iter()
                .any(|header| header == S3_CACHE_MARK_HEADER)
            || EffectiveDecision::parse(value.to_str().map_err(|_| AuthFailure)?).is_err()
        {
            return Err(AuthFailure);
        }
    }
    Ok(())
}

fn canonical_headers(request: &Request, signed_headers: &[String]) -> Result<String, AuthFailure> {
    let mut canonical = String::new();
    for name in signed_headers {
        let values = request.headers().get_all(name).iter().collect::<Vec<_>>();
        if values.is_empty() {
            return Err(AuthFailure);
        }
        let value = values
            .into_iter()
            .map(|value| {
                value
                    .to_str()
                    .map(normalize_header)
                    .map_err(|_| AuthFailure)
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&value);
        canonical.push('\n');
    }
    Ok(canonical)
}

fn normalize_header(value: &str) -> String {
    value.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn single_header<'a>(request: &'a Request, name: &str) -> Result<&'a str, AuthFailure> {
    let mut values = request.headers().get_all(name).iter();
    let value = values.next().ok_or(AuthFailure)?;
    if values.next().is_some() {
        return Err(AuthFailure);
    }
    value.to_str().map_err(|_| AuthFailure)
}

fn parse_query(raw: &str) -> Result<Vec<(String, String)>, AuthFailure> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split('&')
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            Ok((percent_decode(key)?, percent_decode(value)?))
        })
        .collect()
}

fn query_value<'a>(
    query: &'a [(String, String)],
    name: &str,
) -> Result<Option<&'a str>, AuthFailure> {
    let mut matches = query
        .iter()
        .filter(|(key, _)| key == name)
        .map(|(_, value)| value.as_str());
    let value = matches.next();
    if matches.next().is_some() {
        return Err(AuthFailure);
    }
    Ok(value)
}

fn canonical_query(query: &[(String, String)], presigned: bool) -> String {
    let mut encoded = query
        .iter()
        .filter(|(key, _)| !(presigned && key == "X-Amz-Signature"))
        .map(|(key, value)| {
            (
                uri_encode(key.as_bytes(), false),
                uri_encode(value.as_bytes(), false),
            )
        })
        .collect::<Vec<_>>();
    encoded.sort();
    encoded
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_uri(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut output = String::with_capacity(path.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
            {
                push_encoded(&mut output, (high << 4) | low, false);
                index += 3;
                continue;
            }
        }
        push_encoded(&mut output, bytes[index], bytes[index] == b'/');
        index += 1;
    }
    if output.is_empty() {
        "/".to_string()
    } else {
        output
    }
}

fn percent_decode(value: &str) -> Result<String, AuthFailure> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(AuthFailure);
            }
            let high = hex_value(bytes[index + 1]).ok_or(AuthFailure)?;
            let low = hex_value(bytes[index + 2]).ok_or(AuthFailure)?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| AuthFailure)
}

fn uri_encode(bytes: &[u8], preserve_slash: bool) -> String {
    let mut output = String::with_capacity(bytes.len());
    for byte in bytes {
        push_encoded(&mut output, *byte, preserve_slash && *byte == b'/');
    }
    output
}

fn push_encoded(output: &mut String, byte: u8, preserve: bool) {
    if preserve || byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
        output.push(byte as char);
    } else {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        output.push('%');
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

fn parse_datetime(value: &str) -> Result<SystemTime, AuthFailure> {
    let bytes = value.as_bytes();
    if bytes.len() != 16
        || bytes[8] != b'T'
        || bytes[15] != b'Z'
        || !bytes[..8].iter().all(u8::is_ascii_digit)
        || !bytes[9..15].iter().all(u8::is_ascii_digit)
    {
        return Err(AuthFailure);
    }
    let year = parse_number(&value[0..4])? as i64;
    let month = parse_number(&value[4..6])?;
    let day = parse_number(&value[6..8])?;
    let hour = parse_number(&value[9..11])?;
    let minute = parse_number(&value[11..13])?;
    let second = parse_number(&value[13..15])?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(AuthFailure);
    }
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return Err(AuthFailure);
    }
    let seconds = (days as u64)
        .checked_mul(86_400)
        .and_then(|seconds| seconds.checked_add(u64::from(hour) * 3_600))
        .and_then(|seconds| seconds.checked_add(u64::from(minute) * 60))
        .and_then(|seconds| seconds.checked_add(u64::from(second)))
        .ok_or(AuthFailure)?;
    let timestamp = UNIX_EPOCH + Duration::from_secs(seconds);
    if talon_backend::AmzDate::from_system_time(timestamp).datetime != value {
        return Err(AuthFailure);
    }
    Ok(timestamp)
}

fn parse_number(value: &str) -> Result<u32, AuthFailure> {
    value.parse().map_err(|_| AuthFailure)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn validate_header_time(
    now: SystemTime,
    signed_at: SystemTime,
    max_clock_skew: Duration,
) -> Result<(), AuthFailure> {
    let difference = now
        .duration_since(signed_at)
        .or_else(|_| signed_at.duration_since(now))
        .map_err(|_| AuthFailure)?;
    if difference > max_clock_skew {
        return Err(AuthFailure);
    }
    Ok(())
}

fn validate_presigned_time(
    now: SystemTime,
    signed_at: SystemTime,
    expires: Duration,
    max_clock_skew: Duration,
) -> Result<(), AuthFailure> {
    if signed_at.duration_since(now).unwrap_or_default() > max_clock_skew {
        return Err(AuthFailure);
    }
    let deadline = signed_at
        .checked_add(expires)
        .and_then(|time| time.checked_add(max_clock_skew))
        .ok_or(AuthFailure)?;
    if now > deadline {
        return Err(AuthFailure);
    }
    Ok(())
}

fn verify_signature(
    secret: &str,
    scope: &CredentialScope,
    string_to_sign: &[u8],
    signature: &[u8],
) -> Result<(), AuthFailure> {
    let date_key = hmac(format!("AWS4{secret}").as_bytes(), scope.date.as_bytes());
    let region_key = hmac(&date_key, scope.region.as_bytes());
    let service_key = hmac(&region_key, scope.service.as_bytes());
    let signing_key = hmac(&service_key, b"aws4_request");
    let mut verifier = HmacSha256::new_from_slice(&signing_key).map_err(|_| AuthFailure)?;
    verifier.update(string_to_sign);
    verifier.verify_slice(signature).map_err(|_| AuthFailure)
}

fn hmac(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts every key length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn decode_signature(value: &str) -> Result<Vec<u8>, AuthFailure> {
    if !is_lower_hex(value, 64) {
        return Err(AuthFailure);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            Ok((hex_value(pair[0]).ok_or(AuthFailure)? << 4)
                | hex_value(pair[1]).ok_or(AuthFailure)?)
        })
        .collect()
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::header::HOST;
    use talon_backend::sigv4::{sign_request, sign_request_with_payload_hash};
    use talon_backend::{AmzDate, HttpRequest, Method, S3Credentials};

    const ACCESS_KEY: &str = "AKIDEXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const DATETIME: &str = "20130524T000000Z";

    fn now() -> SystemTime {
        parse_datetime(DATETIME).unwrap()
    }

    fn authenticator(session_token: Option<&str>) -> S3SigV4Authenticator {
        S3SigV4Authenticator::new(
            "us-east-1",
            vec![S3ClientIdentity {
                access_key_id: ACCESS_KEY.into(),
                secret_access_key: SECRET_KEY.into(),
                session_token: session_token.map(str::to_string),
                principal: AuthenticatedPrincipal::new("reader", "account-a"),
            }],
            Duration::from_secs(15 * 60),
        )
        .unwrap()
    }

    fn signed_header_request(
        path_and_query: &str,
        service: &str,
        session_token: Option<&str>,
    ) -> Request {
        let mut outgoing = HttpRequest::new(
            Method::Get,
            format!("https://example.com{path_and_query}"),
            Vec::new(),
        );
        let credentials = S3Credentials {
            access_key_id: ACCESS_KEY.into(),
            secret_access_key: SECRET_KEY.into(),
            session_token: session_token.map(str::to_string),
        };
        sign_request(
            &mut outgoing,
            &credentials,
            "us-east-1",
            service,
            &AmzDate {
                datetime: DATETIME.into(),
                date: "20130524".into(),
            },
        );
        let mut builder = Request::builder().method("GET").uri(path_and_query);
        for (name, value) in outgoing.headers {
            builder = builder.header(name, value);
        }
        builder.body(Body::empty()).unwrap()
    }

    fn signed_put(payload_hash: &str) -> Request {
        let mut outgoing = HttpRequest::new(
            Method::Put,
            "https://example.com/bucket/object".into(),
            vec![("content-length".into(), "3".into())],
        );
        sign_request_with_payload_hash(
            &mut outgoing,
            &S3Credentials {
                access_key_id: ACCESS_KEY.into(),
                secret_access_key: SECRET_KEY.into(),
                session_token: None,
            },
            "us-east-1",
            "s3",
            &AmzDate {
                datetime: DATETIME.into(),
                date: "20130524".into(),
            },
            payload_hash,
        );
        let mut builder = Request::builder().method("PUT").uri("/bucket/object");
        for (name, value) in outgoing.headers {
            builder = builder.header(name, value);
        }
        builder.body(Body::from("abc")).unwrap()
    }

    fn presigned_request(expires: u64, session_token: Option<&str>) -> Request {
        let credential = uri_encode(
            format!("{ACCESS_KEY}/20130524/us-east-1/s3/aws4_request").as_bytes(),
            false,
        );
        let mut query = format!(
            "X-Amz-Algorithm={ALGORITHM}&X-Amz-Credential={credential}&X-Amz-Date={DATETIME}&X-Amz-Expires={expires}&X-Amz-SignedHeaders=host"
        );
        if let Some(token) = session_token {
            query.push_str("&X-Amz-Security-Token=");
            query.push_str(&uri_encode(token.as_bytes(), false));
        }
        let uri = format!("/bucket/object?{query}");
        let parsed = parse_query(&query).unwrap();
        let canonical_request = format!(
            "GET\n/bucket/object\n{}\nhost:example.com\n\nhost\nUNSIGNED-PAYLOAD",
            canonical_query(&parsed, true)
        );
        let scope = CredentialScope {
            access_key_id: ACCESS_KEY.into(),
            date: "20130524".into(),
            region: "us-east-1".into(),
            service: "s3".into(),
        };
        let string_to_sign = format!(
            "{ALGORITHM}\n{DATETIME}\n20130524/us-east-1/s3/aws4_request\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let signature = signature_hex(SECRET_KEY, &scope, string_to_sign.as_bytes());
        Request::builder()
            .method("GET")
            .uri(format!("{uri}&X-Amz-Signature={signature}"))
            .header(HOST, "example.com")
            .body(Body::empty())
            .unwrap()
    }

    fn signed_cache_mark_request(mark: &str) -> Request {
        let signed_headers = [
            "host",
            "x-amz-content-sha256",
            "x-amz-date",
            S3_CACHE_MARK_HEADER,
        ];
        let canonical_headers = format!(
            "host:example.com\nx-amz-content-sha256:{EMPTY_SHA256}\nx-amz-date:{DATETIME}\n{S3_CACHE_MARK_HEADER}:{}\n",
            normalize_header(mark)
        );
        let canonical_request = format!(
            "GET\n/bucket/object\n\n{}\n{}\n{EMPTY_SHA256}",
            canonical_headers,
            signed_headers.join(";")
        );
        let scope = CredentialScope {
            access_key_id: ACCESS_KEY.into(),
            date: "20130524".into(),
            region: "us-east-1".into(),
            service: "s3".into(),
        };
        let string_to_sign = format!(
            "{ALGORITHM}\n{DATETIME}\n20130524/us-east-1/s3/aws4_request\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let signature = signature_hex(SECRET_KEY, &scope, string_to_sign.as_bytes());
        Request::builder()
            .method("GET")
            .uri("/bucket/object")
            .header(HOST, "example.com")
            .header("x-amz-content-sha256", EMPTY_SHA256)
            .header("x-amz-date", DATETIME)
            .header(S3_CACHE_MARK_HEADER, mark)
            .header(
                "authorization",
                format!(
                    "{ALGORITHM} Credential={ACCESS_KEY}/20130524/us-east-1/s3/aws4_request, SignedHeaders={}, Signature={signature}",
                    signed_headers.join(";")
                ),
            )
            .body(Body::empty())
            .unwrap()
    }

    fn signature_hex(secret: &str, scope: &CredentialScope, string_to_sign: &[u8]) -> String {
        let date_key = hmac(format!("AWS4{secret}").as_bytes(), scope.date.as_bytes());
        let region_key = hmac(&date_key, scope.region.as_bytes());
        let service_key = hmac(&region_key, scope.service.as_bytes());
        let signing_key = hmac(&service_key, b"aws4_request");
        let signature = hmac(&signing_key, string_to_sign);
        signature.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn accepts_backend_compatible_header_signature() {
        let request = signed_header_request("/bucket/object?list-type=2&prefix=dir%2F", "s3", None);
        let principal = authenticator(None)
            .authenticate_at(&request, now())
            .unwrap();
        assert_eq!(
            principal,
            AuthenticatedPrincipal::new("reader", "account-a")
        );
    }

    #[test]
    fn accepts_supported_put_payload_declarations_only() {
        let verifier = authenticator(None);
        let digest = sha256_hex(b"abc");
        assert!(verifier
            .authenticate_at(&signed_put(&digest), now())
            .is_ok());
        assert!(verifier
            .authenticate_at(&signed_put("UNSIGNED-PAYLOAD"), now())
            .is_ok());
        assert!(verifier
            .authenticate_at(&signed_put("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"), now())
            .is_err());
        assert!(verifier
            .authenticate_at(&signed_put(&digest.to_ascii_uppercase()), now())
            .is_err());
    }

    #[test]
    fn rejects_tampering_scope_replay_and_payload_hash() {
        let verifier = authenticator(None);
        let mut tampered = signed_header_request("/bucket/object", "s3", None);
        *tampered.uri_mut() = "/bucket/other".parse().unwrap();
        assert!(verifier.authenticate_at(&tampered, now()).is_err());

        let wrong_service = signed_header_request("/bucket/object", "ec2", None);
        assert!(verifier.authenticate_at(&wrong_service, now()).is_err());

        let replayed = signed_header_request("/bucket/object", "s3", None);
        assert!(verifier
            .authenticate_at(&replayed, now() + Duration::from_secs(901))
            .is_err());

        let mut payload_tampered = signed_header_request("/bucket/object", "s3", None);
        payload_tampered.headers_mut().insert(
            "x-amz-content-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .unwrap(),
        );
        assert!(verifier.authenticate_at(&payload_tampered, now()).is_err());
    }

    #[test]
    fn enforces_session_token_and_signed_cache_mark() {
        let verifier = authenticator(Some("temporary-token"));
        let request = signed_header_request("/bucket/object", "s3", Some("temporary-token"));
        assert!(verifier.authenticate_at(&request, now()).is_ok());

        let request = signed_header_request("/bucket/object", "s3", None);
        assert!(verifier.authenticate_at(&request, now()).is_err());

        let mut unsigned_mark =
            signed_header_request("/bucket/object", "s3", Some("temporary-token"));
        unsigned_mark.headers_mut().insert(
            S3_CACHE_MARK_HEADER,
            "v=1; lookup=on; populate=on; fallback=origin"
                .parse()
                .unwrap(),
        );
        assert!(verifier.authenticate_at(&unsigned_mark, now()).is_err());

        let valid_mark = signed_cache_mark_request("v=1; lookup=on; populate=off; fallback=fail");
        assert!(authenticator(None)
            .authenticate_at(&valid_mark, now())
            .is_ok());
        let invalid_mark = signed_cache_mark_request("v=1; lookup=on");
        assert!(authenticator(None)
            .authenticate_at(&invalid_mark, now())
            .is_err());
    }

    #[test]
    fn accepts_presigned_requests_and_rejects_expiry_or_signature_tampering() {
        let verifier = authenticator(None);
        let request = presigned_request(60, None);
        assert!(verifier.authenticate_at(&request, now()).is_ok());
        assert!(verifier
            .authenticate_at(&request, now() + Duration::from_secs(60 + 15 * 60 + 1))
            .is_err());

        let mut tampered = presigned_request(60, None);
        let uri = tampered.uri().to_string().replace("object?", "other?");
        *tampered.uri_mut() = uri.parse().unwrap();
        assert!(verifier.authenticate_at(&tampered, now()).is_err());
    }

    #[test]
    fn rejects_invalid_identity_sets_and_streaming_payload_modes() {
        assert!(parse_datetime("0000000\u{e9}000000Z").is_err());
        assert!(matches!(
            S3SigV4Authenticator::new("us-east-1", Vec::new(), Duration::from_secs(1)),
            Err(S3IdentityError::InvalidConfiguration)
        ));

        let mut outgoing = HttpRequest::new(
            Method::Put,
            "https://example.com/bucket/object".into(),
            Vec::new(),
        );
        sign_request_with_payload_hash(
            &mut outgoing,
            &S3Credentials {
                access_key_id: ACCESS_KEY.into(),
                secret_access_key: SECRET_KEY.into(),
                session_token: None,
            },
            "us-east-1",
            "s3",
            &AmzDate {
                datetime: DATETIME.into(),
                date: "20130524".into(),
            },
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        );
        let mut builder = Request::builder().method("PUT").uri("/bucket/object");
        for (name, value) in outgoing.headers {
            builder = builder.header(name, value);
        }
        let request = builder.body(Body::empty()).unwrap();
        assert!(authenticator(None)
            .authenticate_at(&request, now())
            .is_err());
    }
}
