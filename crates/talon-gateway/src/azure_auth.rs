//! Incoming Azure Blob Shared Key and SAS authentication.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{
    AuthenticatedPrincipal, EffectiveDecision, GatewayAuthenticationError, GatewayAuthenticator,
    ProviderProtocol, AZURE_CACHE_MARK_HEADER,
};

type HmacSha256 = Hmac<Sha256>;

/// One Azure account key and its authorization identity.
#[derive(Clone)]
pub struct AzureClientIdentity {
    /// Base64-encoded incoming account key.
    pub account_key: String,
    /// Principal and provider account used by authorization.
    pub principal: AuthenticatedPrincipal,
}

/// Shared Key and SAS verifier for one Azure storage account.
pub struct AzureStorageAuthenticator {
    account: String,
    max_clock_skew: Duration,
    transport_https: bool,
    identities: Vec<DecodedIdentity>,
}

struct DecodedIdentity {
    key: Vec<u8>,
    principal: AuthenticatedPrincipal,
}

impl AzureStorageAuthenticator {
    /// Decode and validate incoming account identities.
    pub fn new(
        account: impl Into<String>,
        identities: Vec<AzureClientIdentity>,
        max_clock_skew: Duration,
        transport_https: bool,
    ) -> Result<Self, AzureIdentityError> {
        let account = account.into();
        if account.is_empty() || identities.is_empty() || max_clock_skew.is_zero() {
            return Err(AzureIdentityError::InvalidConfiguration);
        }
        let mut decoded = Vec::with_capacity(identities.len());
        for identity in identities {
            if identity.account_key.is_empty()
                || identity.principal.id.is_empty()
                || identity.principal.provider_account != account
            {
                return Err(AzureIdentityError::InvalidConfiguration);
            }
            let key = BASE64
                .decode(identity.account_key)
                .map_err(|_| AzureIdentityError::InvalidConfiguration)?;
            if key.is_empty()
                || decoded
                    .iter()
                    .any(|current: &DecodedIdentity| current.key == key)
            {
                return Err(AzureIdentityError::DuplicateAccountKey);
            }
            decoded.push(DecodedIdentity {
                key,
                principal: identity.principal,
            });
        }
        Ok(Self {
            account,
            max_clock_skew,
            transport_https,
            identities: decoded,
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
        if request.headers().contains_key("authorization") {
            self.authenticate_shared_key(request, now)
        } else {
            self.authenticate_sas(request, now)
        }
    }

    fn authenticate_shared_key(
        &self,
        request: &Request,
        now: SystemTime,
    ) -> Result<AuthenticatedPrincipal, AuthFailure> {
        let authorization = single_header(request, "authorization")?;
        let credential = authorization
            .strip_prefix("SharedKey ")
            .ok_or(AuthFailure)?;
        let (account, signature) = credential.split_once(':').ok_or(AuthFailure)?;
        if account != self.account || signature.is_empty() || signature.contains(':') {
            return Err(AuthFailure);
        }
        let supplied = BASE64.decode(signature).map_err(|_| AuthFailure)?;
        let date = match request.headers().get("x-ms-date") {
            Some(value) => value.to_str().map_err(|_| AuthFailure)?,
            None => single_header(request, "date")?,
        };
        let signed_at = httpdate::parse_http_date(date).map_err(|_| AuthFailure)?;
        validate_time(now, signed_at, self.max_clock_skew)?;
        validate_azure_cache_mark(request)?;
        let string_to_sign = shared_key_string_to_sign(request, &self.account)?;
        self.verify_any(string_to_sign.as_bytes(), &supplied)
    }

    fn authenticate_sas(
        &self,
        request: &Request,
        now: SystemTime,
    ) -> Result<AuthenticatedPrincipal, AuthFailure> {
        if request.headers().contains_key(AZURE_CACHE_MARK_HEADER) {
            return Err(AuthFailure);
        }
        let query = parse_query(request.uri().query().unwrap_or(""))?;
        let signature = required_query(&query, "sig")?;
        let supplied = BASE64.decode(signature).map_err(|_| AuthFailure)?;
        let version = required_query(&query, "sv")?;
        if version < "2020-12-06" {
            return Err(AuthFailure);
        }
        let protocol = query_value(&query, "spr")?.unwrap_or("");
        validate_protocol(protocol, self.transport_https)?;
        if query_value(&query, "sip")?.is_some() || query_value(&query, "si")?.is_some() {
            return Err(AuthFailure);
        }
        if query_value(&query, "ses")?.is_some() {
            return Err(AuthFailure);
        }
        validate_sas_time(&query, now, self.max_clock_skew)?;
        let required_permission = required_permission(request)?;
        if !required_query(&query, "sp")?.contains(required_permission) {
            return Err(AuthFailure);
        }

        let string_to_sign = if query_value(&query, "ss")?.is_some() {
            account_sas_string_to_sign(&query, &self.account, required_permission)?
        } else {
            service_sas_string_to_sign(request, &query, &self.account, required_permission)?
        };
        self.verify_any(string_to_sign.as_bytes(), &supplied)
    }

    fn verify_any(
        &self,
        string_to_sign: &[u8],
        signature: &[u8],
    ) -> Result<AuthenticatedPrincipal, AuthFailure> {
        for identity in &self.identities {
            let mut verifier =
                HmacSha256::new_from_slice(&identity.key).map_err(|_| AuthFailure)?;
            verifier.update(string_to_sign);
            if verifier.verify_slice(signature).is_ok() {
                return Ok(identity.principal.clone());
            }
        }
        Err(AuthFailure)
    }
}

impl GatewayAuthenticator for AzureStorageAuthenticator {
    fn authenticate(
        &self,
        request: &Request,
        protocol: ProviderProtocol,
    ) -> Result<AuthenticatedPrincipal, GatewayAuthenticationError> {
        if protocol != ProviderProtocol::Azure {
            return Err(GatewayAuthenticationError);
        }
        self.authenticate_at(request, SystemTime::now())
    }
}

/// Invalid Azure incoming identity configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AzureIdentityError {
    /// A required bound, account, key, or identity field is invalid.
    #[error("Azure client identity configuration is invalid")]
    InvalidConfiguration,
    /// Account keys must not be duplicated.
    #[error("Azure client identity account keys must be unique")]
    DuplicateAccountKey,
}

#[derive(Debug, Clone, Copy)]
struct AuthFailure;

fn shared_key_string_to_sign(request: &Request, account: &str) -> Result<String, AuthFailure> {
    let header = |name: &str| -> Result<String, AuthFailure> {
        match request.headers().get(name) {
            Some(value) => Ok(value.to_str().map_err(|_| AuthFailure)?.trim().to_string()),
            None => Ok(String::new()),
        }
    };
    let content_length = match header("content-length")?.as_str() {
        "" | "0" => String::new(),
        value => value.to_string(),
    };
    let date = if request.headers().contains_key("x-ms-date") {
        String::new()
    } else {
        header("date")?
    };
    let canonical_headers = canonical_xms_headers(request)?;
    let canonical_resource = canonicalized_resource(request, account)?;
    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        request.method().as_str(),
        header("content-encoding")?,
        header("content-language")?,
        content_length,
        header("content-md5")?,
        header("content-type")?,
        date,
        header("if-modified-since")?,
        header("if-match")?,
        header("if-none-match")?,
        header("if-unmodified-since")?,
        header("range")?,
        canonical_headers,
        canonical_resource
    ))
}

fn canonical_xms_headers(request: &Request) -> Result<String, AuthFailure> {
    let mut names = request
        .headers()
        .keys()
        .filter(|name| name.as_str().starts_with("x-ms-"))
        .map(|name| name.as_str().to_string())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    let mut output = String::new();
    for name in names {
        let values = request
            .headers()
            .get_all(&name)
            .iter()
            .map(|value| {
                value
                    .to_str()
                    .map(normalize_header)
                    .map_err(|_| AuthFailure)
            })
            .collect::<Result<Vec<_>, _>>()?;
        output.push_str(&name);
        output.push(':');
        output.push_str(&values.join(","));
        output.push('\n');
    }
    Ok(output)
}

fn canonicalized_resource(request: &Request, account: &str) -> Result<String, AuthFailure> {
    let mut resource = format!("/{account}{}", request.uri().path());
    let query = parse_query(request.uri().query().unwrap_or(""))?;
    let mut canonical = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in query {
        canonical
            .entry(name.to_ascii_lowercase())
            .or_default()
            .extend(value);
    }
    for (name, mut values) in canonical {
        values.sort();
        resource.push('\n');
        resource.push_str(&name);
        resource.push(':');
        resource.push_str(&values.join(","));
    }
    Ok(resource)
}

fn account_sas_string_to_sign(
    query: &HashMap<String, Vec<String>>,
    account: &str,
    permission: char,
) -> Result<String, AuthFailure> {
    if !required_query(query, "ss")?.contains('b') {
        return Err(AuthFailure);
    }
    let resource_type = match permission {
        'l' => 'c',
        _ => 'o',
    };
    if !required_query(query, "srt")?.contains(resource_type) {
        return Err(AuthFailure);
    }
    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        account,
        required_query(query, "sp")?,
        required_query(query, "ss")?,
        required_query(query, "srt")?,
        query_value(query, "st")?.unwrap_or(""),
        required_query(query, "se")?,
        query_value(query, "sip")?.unwrap_or(""),
        query_value(query, "spr")?.unwrap_or(""),
        required_query(query, "sv")?,
        query_value(query, "ses")?.unwrap_or("")
    ))
}

fn service_sas_string_to_sign(
    request: &Request,
    query: &HashMap<String, Vec<String>>,
    account: &str,
    permission: char,
) -> Result<String, AuthFailure> {
    let signed_resource = required_query(query, "sr")?;
    let path = azure_resource_path(request.uri().path(), account)?;
    let (container, blob) = path.split_once('/').unwrap_or((path, ""));
    if container.is_empty() {
        return Err(AuthFailure);
    }
    let canonical_resource = match signed_resource {
        "b" if !blob.is_empty() && permission != 'l' => format!("/blob/{account}/{path}"),
        "c" => format!("/blob/{account}/{container}"),
        _ => return Err(AuthFailure),
    };
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        required_query(query, "sp")?,
        query_value(query, "st")?.unwrap_or(""),
        required_query(query, "se")?,
        canonical_resource,
        "",
        query_value(query, "sip")?.unwrap_or(""),
        query_value(query, "spr")?.unwrap_or(""),
        required_query(query, "sv")?,
        signed_resource,
        query_value(query, "snapshot")?.unwrap_or(""),
        query_value(query, "ses")?.unwrap_or(""),
        query_value(query, "rscc")?.unwrap_or(""),
        query_value(query, "rscd")?.unwrap_or(""),
        query_value(query, "rsce")?.unwrap_or(""),
        query_value(query, "rscl")?.unwrap_or(""),
        query_value(query, "rsct")?.unwrap_or("")
    );
    Ok(string_to_sign)
}

fn azure_resource_path<'a>(path: &'a str, account: &str) -> Result<&'a str, AuthFailure> {
    let path = path.strip_prefix('/').ok_or(AuthFailure)?;
    Ok(path
        .strip_prefix(account)
        .and_then(|path| path.strip_prefix('/'))
        .unwrap_or(path))
}

fn required_permission(request: &Request) -> Result<char, AuthFailure> {
    let query = parse_query(request.uri().query().unwrap_or(""))?;
    if request.method() == axum::http::Method::GET && query_value(&query, "comp")? == Some("list") {
        Ok('l')
    } else {
        match *request.method() {
            axum::http::Method::GET | axum::http::Method::HEAD => Ok('r'),
            axum::http::Method::PUT => Ok('w'),
            axum::http::Method::DELETE => Ok('d'),
            _ => Err(AuthFailure),
        }
    }
}

fn validate_sas_time(
    query: &HashMap<String, Vec<String>>,
    now: SystemTime,
    skew: Duration,
) -> Result<(), AuthFailure> {
    let expiry = parse_iso8601(required_query(query, "se")?)?;
    if now > expiry.checked_add(skew).ok_or(AuthFailure)? {
        return Err(AuthFailure);
    }
    if let Some(start) = query_value(query, "st")? {
        let start = parse_iso8601(start)?;
        if start.duration_since(now).unwrap_or_default() > skew {
            return Err(AuthFailure);
        }
    }
    Ok(())
}

fn validate_protocol(value: &str, transport_https: bool) -> Result<(), AuthFailure> {
    match value {
        "" | "https,http" => Ok(()),
        "https" if transport_https => Ok(()),
        _ => Err(AuthFailure),
    }
}

fn validate_azure_cache_mark(request: &Request) -> Result<(), AuthFailure> {
    let mut values = request.headers().get_all(AZURE_CACHE_MARK_HEADER).iter();
    if let Some(value) = values.next() {
        if values.next().is_some()
            || EffectiveDecision::parse(value.to_str().map_err(|_| AuthFailure)?).is_err()
        {
            return Err(AuthFailure);
        }
    }
    Ok(())
}

fn parse_query(raw: &str) -> Result<HashMap<String, Vec<String>>, AuthFailure> {
    let mut query = HashMap::<String, Vec<String>>::new();
    for (name, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        query
            .entry(name.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    Ok(query)
}

fn query_value<'a>(
    query: &'a HashMap<String, Vec<String>>,
    name: &str,
) -> Result<Option<&'a str>, AuthFailure> {
    match query.get(name).map(Vec::as_slice) {
        None => Ok(None),
        Some([value]) => Ok(Some(value)),
        Some(_) => Err(AuthFailure),
    }
}

fn required_query<'a>(
    query: &'a HashMap<String, Vec<String>>,
    name: &str,
) -> Result<&'a str, AuthFailure> {
    query_value(query, name)?
        .filter(|value| !value.is_empty())
        .ok_or(AuthFailure)
}

fn single_header<'a>(request: &'a Request, name: &str) -> Result<&'a str, AuthFailure> {
    let mut values = request.headers().get_all(name).iter();
    let value = values.next().ok_or(AuthFailure)?;
    if values.next().is_some() {
        return Err(AuthFailure);
    }
    value.to_str().map_err(|_| AuthFailure)
}

fn normalize_header(value: &str) -> String {
    value.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn validate_time(now: SystemTime, signed: SystemTime, skew: Duration) -> Result<(), AuthFailure> {
    let difference = now
        .duration_since(signed)
        .or_else(|_| signed.duration_since(now))
        .map_err(|_| AuthFailure)?;
    if difference > skew {
        return Err(AuthFailure);
    }
    Ok(())
}

fn parse_iso8601(value: &str) -> Result<SystemTime, AuthFailure> {
    let value = value.strip_suffix('Z').ok_or(AuthFailure)?;
    let whole = match value.split_once('.') {
        Some((whole, fraction))
            if !fraction.is_empty() && fraction.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            whole
        }
        Some(_) => return Err(AuthFailure),
        None => value,
    };
    let bytes = whole.as_bytes();
    if bytes.len() != 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || [0..4, 5..7, 8..10, 11..13, 14..16, 17..19]
            .iter()
            .any(|range| !bytes[range.clone()].iter().all(u8::is_ascii_digit))
    {
        return Err(AuthFailure);
    }
    let year = number(&whole[0..4])? as i64;
    let month = number(&whole[5..7])?;
    let day = number(&whole[8..10])?;
    let hour = number(&whole[11..13])?;
    let minute = number(&whole[14..16])?;
    let second = number(&whole[17..19])?;
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
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
    Ok(UNIX_EPOCH + Duration::from_secs(seconds))
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 31,
    }
}

fn number(value: &str) -> Result<u32, AuthFailure> {
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(AuthFailure);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use talon_backend::azure_sharedkey::{authorization_header, rfc1123_date};
    use talon_backend::{HttpRequest, Method};

    const ACCOUNT: &str = "devstoreaccount1";
    const KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    fn now() -> SystemTime {
        parse_iso8601("2026-08-09T12:00:00Z").unwrap()
    }

    fn authenticator(https: bool) -> AzureStorageAuthenticator {
        AzureStorageAuthenticator::new(
            ACCOUNT,
            vec![AzureClientIdentity {
                account_key: KEY.into(),
                principal: AuthenticatedPrincipal::new("azure-reader", ACCOUNT),
            }],
            Duration::from_secs(15 * 60),
            https,
        )
        .unwrap()
    }

    fn shared_key_request(path: &str, cache_mark: Option<&str>) -> Request {
        let date = rfc1123_date(now());
        let mut outgoing = HttpRequest::new(
            Method::Get,
            format!("http://example.com{path}"),
            vec![
                ("x-ms-date".into(), date),
                ("x-ms-version".into(), "2021-12-02".into()),
            ],
        );
        if let Some(mark) = cache_mark {
            outgoing
                .headers
                .push((AZURE_CACHE_MARK_HEADER.into(), mark.into()));
        }
        let authorization = authorization_header(&outgoing, ACCOUNT, KEY).unwrap();
        outgoing
            .headers
            .push(("authorization".into(), authorization));
        let mut builder = Request::builder().method("GET").uri(path);
        for (name, value) in outgoing.headers {
            builder = builder.header(name, value);
        }
        builder.body(Body::empty()).unwrap()
    }

    fn signed_query(mut values: Vec<(&str, String)>, string_to_sign: String) -> String {
        let key = BASE64.decode(KEY).unwrap();
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(string_to_sign.as_bytes());
        values.push(("sig", BASE64.encode(mac.finalize().into_bytes())));
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (name, value) in values {
            serializer.append_pair(name, &value);
        }
        serializer.finish()
    }

    fn service_sas_request(permission: &str, expiry: &str, protocol: &str) -> Request {
        let values = vec![
            ("sp", permission.to_string()),
            ("st", "2026-08-09T11:55:00Z".to_string()),
            ("se", expiry.to_string()),
            ("spr", protocol.to_string()),
            ("sv", "2021-12-02".to_string()),
            ("sr", "b".to_string()),
        ];
        let unsigned = values.iter().cloned().fold(
            HashMap::<String, Vec<String>>::new(),
            |mut map, (key, value)| {
                map.entry(key.into()).or_default().push(value);
                map
            },
        );
        let base = Request::builder()
            .method("GET")
            .uri(format!("/{ACCOUNT}/container/blob"))
            .body(Body::empty())
            .unwrap();
        let string_to_sign = service_sas_string_to_sign(&base, &unsigned, ACCOUNT, 'r').unwrap();
        let query = signed_query(values, string_to_sign);
        Request::builder()
            .method("GET")
            .uri(format!("/{ACCOUNT}/container/blob?{query}"))
            .body(Body::empty())
            .unwrap()
    }

    fn account_sas_request(permission: &str, resource_types: &str) -> Request {
        let values = vec![
            ("sp", permission.to_string()),
            ("ss", "b".to_string()),
            ("srt", resource_types.to_string()),
            ("st", "2026-08-09T11:55:00Z".to_string()),
            ("se", "2026-08-09T12:05:00Z".to_string()),
            ("spr", "https,http".to_string()),
            ("sv", "2021-12-02".to_string()),
        ];
        let unsigned = values.iter().cloned().fold(
            HashMap::<String, Vec<String>>::new(),
            |mut map, (key, value)| {
                map.entry(key.into()).or_default().push(value);
                map
            },
        );
        let validation_permission = if resource_types.contains('o') {
            'r'
        } else {
            'l'
        };
        let string_to_sign =
            account_sas_string_to_sign(&unsigned, ACCOUNT, validation_permission).unwrap();
        let query = signed_query(values, string_to_sign);
        Request::builder()
            .method("GET")
            .uri(format!("/{ACCOUNT}/container/blob?{query}"))
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn accepts_backend_compatible_shared_key_and_signed_mark() {
        let request = shared_key_request(&format!("/{ACCOUNT}/container/blob"), None);
        let principal = authenticator(false)
            .authenticate_at(&request, now())
            .unwrap();
        assert_eq!(
            principal,
            AuthenticatedPrincipal::new("azure-reader", ACCOUNT)
        );

        let request = shared_key_request(
            &format!("/{ACCOUNT}/container/blob"),
            Some("v=1; lookup=on; populate=off; fallback=fail"),
        );
        assert!(authenticator(false)
            .authenticate_at(&request, now())
            .is_ok());
    }

    #[test]
    fn shared_key_rejects_tampering_replay_and_invalid_mark() {
        let verifier = authenticator(false);
        let mut request = shared_key_request(&format!("/{ACCOUNT}/container/blob"), None);
        *request.uri_mut() = format!("/{ACCOUNT}/container/other").parse().unwrap();
        assert!(verifier.authenticate_at(&request, now()).is_err());

        let request = shared_key_request(&format!("/{ACCOUNT}/container/blob"), None);
        assert!(verifier
            .authenticate_at(&request, now() + Duration::from_secs(901))
            .is_err());
        let invalid_mark = shared_key_request(
            &format!("/{ACCOUNT}/container/blob"),
            Some("v=1; lookup=on"),
        );
        assert!(verifier.authenticate_at(&invalid_mark, now()).is_err());
    }

    #[test]
    fn validates_service_sas_signature_time_permission_and_protocol() {
        let request = service_sas_request("r", "2026-08-09T12:05:00Z", "https,http");
        assert!(authenticator(false)
            .authenticate_at(&request, now())
            .is_ok());

        // Generated by azure-storage-blob 12.26.0, independently of this verifier.
        let sdk_vector = Request::builder()
            .method("GET")
            .uri(concat!(
                "/devstoreaccount1/container/blob?",
                "se=2026-08-09T12%3A05%3A00Z&sp=r&spr=https%2Chttp&",
                "sv=2025-07-05&sr=b&",
                "sig=d8BBtODN",
                "dAvQUT4uB0gFZqe%2FNbWsERunmgZTAXn%2FvRw%3D"
            ))
            .body(Body::empty())
            .unwrap();
        assert!(authenticator(false)
            .authenticate_at(&sdk_vector, now())
            .is_ok());

        let expired = service_sas_request("r", "2026-08-09T11:00:00Z", "https,http");
        assert!(authenticator(false)
            .authenticate_at(&expired, now())
            .is_err());
        let wrong_permission = service_sas_request("w", "2026-08-09T12:05:00Z", "https,http");
        assert!(authenticator(false)
            .authenticate_at(&wrong_permission, now())
            .is_err());
        let https_only = service_sas_request("r", "2026-08-09T12:05:00Z", "https");
        assert!(authenticator(false)
            .authenticate_at(&https_only, now())
            .is_err());
        assert!(authenticator(true)
            .authenticate_at(&https_only, now())
            .is_ok());
    }

    #[test]
    fn validates_account_sas_and_rejects_sas_cache_marks() {
        let request = account_sas_request("rl", "sco");
        assert!(authenticator(false)
            .authenticate_at(&request, now())
            .is_ok());

        // Generated by azure-storage-blob 12.26.0, independently of this verifier.
        let sdk_vector = Request::builder()
            .method("GET")
            .uri(concat!(
                "/devstoreaccount1/container/blob?",
                "se=2026-08-09T12%3A05%3A00Z&sp=rl&spr=https%2Chttp&",
                "sv=2025-07-05&ss=b&srt=sco&sig=483TzcnHErNZv1uE%2F",
                "Dgpw4y90k9stgi71orjzYssU0c%3D"
            ))
            .body(Body::empty())
            .unwrap();
        assert!(authenticator(false)
            .authenticate_at(&sdk_vector, now())
            .is_ok());

        let wrong_resource = account_sas_request("rl", "sc");
        assert!(authenticator(false)
            .authenticate_at(&wrong_resource, now())
            .is_err());

        let mut marked = service_sas_request("r", "2026-08-09T12:05:00Z", "https,http");
        marked.headers_mut().insert(
            AZURE_CACHE_MARK_HEADER,
            "v=1; lookup=on; populate=on; fallback=origin"
                .parse()
                .unwrap(),
        );
        assert!(authenticator(false)
            .authenticate_at(&marked, now())
            .is_err());
    }

    #[test]
    fn rejects_invalid_identities_and_dates_without_panicking() {
        assert!(
            AzureStorageAuthenticator::new(ACCOUNT, Vec::new(), Duration::from_secs(1), false)
                .is_err()
        );
        assert!(parse_iso8601("2026-02-31T00:00:00Z").is_err());
        assert!(parse_iso8601("0000000\u{e9}000000Z").is_err());
    }
}
