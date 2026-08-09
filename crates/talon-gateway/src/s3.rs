//! Amazon S3-compatible read adapter.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use talon_backend::{HttpResponse, HttpStreamResponse, S3Backend};
use talon_cache_client::{BlockReader, CacheReadError, FileView, DEFAULT_TRANSFER_CHUNK_BYTES};
use talon_core::{Backend, ObjectId, Version};

use crate::{
    FailureReason, GatewayAdapter, GatewayOperation, GatewayOutcome, GatewayRequestContext,
    GatewayResponse, GatewayRoute, GatewayTarget, ProviderProtocol,
};

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
type CacheStream = Pin<Box<dyn Stream<Item = Result<Bytes, CacheReadError>> + Send>>;

/// Exact cache range requested by the S3 adapter.
pub struct S3CacheRequest<'a> {
    /// Stable object identity.
    pub object: &'a ObjectId,
    /// Authoritative origin ETag.
    pub version: &'a Version,
    /// Authoritative whole-object size.
    pub object_size: u64,
    /// First requested byte.
    pub offset: u64,
    /// Number of requested bytes.
    pub len: u64,
    /// Talon cache block size.
    pub block_size: u32,
    /// Maximum emitted stream chunk size.
    pub chunk_size: u32,
    /// Request time used for cache freshness decisions.
    pub now_ms: u64,
}

/// Incoming S3 addressing and cache behavior.
#[derive(Debug, Clone)]
pub struct S3AdapterConfig {
    /// Host suffix after the bucket in virtual-host-style requests.
    pub endpoint_suffix: String,
    /// Whether incoming requests use `/bucket/key` paths.
    pub path_style: bool,
    /// Talon cache block size.
    pub block_size: u32,
    /// Maximum worker response chunk passed to one HTTP frame.
    pub transfer_chunk_bytes: u32,
    /// Route used until signed cache marks are implemented by #441.
    pub default_route: GatewayRoute,
}

impl S3AdapterConfig {
    /// AWS virtual-host-style endpoint for one region.
    pub fn aws(region: impl AsRef<str>) -> Self {
        Self {
            endpoint_suffix: format!("s3.{}.amazonaws.com", region.as_ref()),
            path_style: false,
            block_size: 256 * 1024 * 1024,
            transfer_chunk_bytes: DEFAULT_TRANSFER_CHUNK_BYTES,
            default_route: GatewayRoute::Cache,
        }
    }

    /// Path-style endpoint used by S3-compatible services and emulators.
    pub fn path_style(endpoint_suffix: impl Into<String>) -> Self {
        Self {
            endpoint_suffix: endpoint_suffix.into(),
            path_style: true,
            ..Self::aws("us-east-1")
        }
    }

    fn validate(&self) -> Result<(), S3RequestError> {
        if self.endpoint_suffix.is_empty() || self.block_size == 0 || self.transfer_chunk_bytes == 0
        {
            return Err(S3RequestError::invalid(
                "InvalidArgument",
                "S3 endpoint and cache sizes must be configured",
            ));
        }
        Ok(())
    }
}

/// Cache streaming interface shared by the production reader and tests.
pub trait S3Cache: Send + Sync + 'static {
    /// Stream one exact object range from Talon.
    fn stream(&self, request: S3CacheRequest<'_>) -> Result<CacheStream, CacheReadError>;
}

impl S3Cache for BlockReader {
    fn stream(&self, request: S3CacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
        let file = FileView {
            object: request.object,
            block_size: request.block_size,
            version: request.version,
            size: request.object_size,
        };
        self.stream_range(
            &file,
            request.offset,
            request.len,
            request.chunk_size,
            request.now_ms,
        )
        .map(|stream| Box::pin(stream) as CacheStream)
    }
}

/// Scoped origin identity. Incoming client signatures are never forwarded.
#[async_trait]
pub trait S3Origin: Send + Sync + 'static {
    async fn head(
        &self,
        object: &ObjectId,
        conditions: &[(String, String)],
    ) -> Result<HttpResponse, String>;

    async fn get(
        &self,
        object: &ObjectId,
        range: Option<(u64, u64)>,
        conditions: &[(String, String)],
    ) -> Result<HttpStreamResponse, String>;

    async fn list(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
        encoding_type: Option<&str>,
    ) -> Result<HttpResponse, String>;
}

#[async_trait]
impl S3Origin for S3Backend {
    async fn head(
        &self,
        object: &ObjectId,
        conditions: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_head_raw(object, conditions).await
    }

    async fn get(
        &self,
        object: &ObjectId,
        range: Option<(u64, u64)>,
        conditions: &[(String, String)],
    ) -> Result<HttpStreamResponse, String> {
        self.execute_get_stream_raw(object, range, conditions).await
    }

    async fn list(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
        encoding_type: Option<&str>,
    ) -> Result<HttpResponse, String> {
        self.execute_list_raw(
            bucket,
            prefix,
            delimiter,
            continuation_token,
            max_keys,
            encoding_type,
        )
        .await
    }
}

/// S3 protocol adapter over Talon and a scoped origin identity.
pub struct S3Adapter {
    config: S3AdapterConfig,
    cache: Arc<dyn S3Cache>,
    origin: Arc<dyn S3Origin>,
}

impl S3Adapter {
    /// Construct an S3 compatibility adapter.
    pub fn new(
        config: S3AdapterConfig,
        cache: Arc<dyn S3Cache>,
        origin: Arc<dyn S3Origin>,
    ) -> Result<Self, String> {
        config.validate().map_err(|error| error.message)?;
        Ok(Self {
            config,
            cache,
            origin,
        })
    }
}

#[derive(Debug)]
struct ParsedTarget {
    bucket: String,
    key: Option<String>,
}

fn parse_target(
    request: &Request,
    config: &S3AdapterConfig,
) -> Result<ParsedTarget, S3RequestError> {
    let segments = request
        .uri()
        .path()
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let (bucket, key_index) = if config.path_style {
        let bucket = segments.first().copied().unwrap_or_default();
        (decode_path(bucket)?, 1)
    } else {
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| S3RequestError::invalid("InvalidURI", "missing Host header"))?;
        let host = host.split(':').next().unwrap_or(host);
        let suffix = format!(".{}", config.endpoint_suffix);
        let bucket = host.strip_suffix(&suffix).ok_or_else(|| {
            S3RequestError::invalid("InvalidURI", "host is not the configured S3 endpoint")
        })?;
        (bucket.to_string(), 0)
    };
    if bucket.is_empty() {
        return Err(S3RequestError::invalid("InvalidURI", "missing bucket"));
    }
    let raw_key = segments.get(key_index..).unwrap_or_default().join("/");
    let key = if raw_key.is_empty() {
        None
    } else {
        Some(decode_path(&raw_key)?)
    };
    Ok(ParsedTarget { bucket, key })
}

fn validate_percent_encoding(value: &str, component: &str) -> Result<(), S3RequestError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(S3RequestError::invalid(
                    "InvalidURI",
                    format!("{component} contains malformed percent encoding"),
                ));
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    Ok(())
}

fn decode_path(value: &str) -> Result<String, S3RequestError> {
    validate_percent_encoding(value, "path")?;
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| S3RequestError::invalid("InvalidURI", "path is not valid UTF-8"))
}

#[derive(Default)]
struct S3Query {
    list_type: Option<String>,
    prefix: Option<String>,
    delimiter: Option<String>,
    continuation_token: Option<String>,
    max_keys: Option<String>,
    encoding_type: Option<String>,
}

impl S3Query {
    fn parse(query: Option<&str>) -> Result<Self, S3RequestError> {
        validate_percent_encoding(query.unwrap_or_default(), "query")?;
        let mut parsed = Self::default();
        for (name, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
            match name.as_ref() {
                "list-type" => parsed.list_type = Some(value.into_owned()),
                "prefix" => parsed.prefix = Some(value.into_owned()),
                "delimiter" => parsed.delimiter = Some(value.into_owned()),
                "continuation-token" => parsed.continuation_token = Some(value.into_owned()),
                "max-keys" => parsed.max_keys = Some(value.into_owned()),
                "encoding-type" => parsed.encoding_type = Some(value.into_owned()),
                _ => {}
            }
        }
        if let Some(value) = parsed.encoding_type.as_deref() {
            if value != "url" {
                return Err(S3RequestError::invalid(
                    "InvalidArgument",
                    "encoding-type must be url",
                ));
            }
        }
        Ok(parsed)
    }

    fn is_list_v2(&self) -> bool {
        self.list_type.as_deref() == Some("2")
    }

    fn max_keys(&self) -> Result<u32, S3RequestError> {
        match self.max_keys.as_deref() {
            None => Ok(1000),
            Some(value) => value
                .parse::<u32>()
                .ok()
                .filter(|value| *value <= 1000)
                .ok_or_else(|| {
                    S3RequestError::invalid(
                        "InvalidArgument",
                        "max-keys must be between 0 and 1000",
                    )
                }),
        }
    }
}

fn conditional_headers(headers: &HeaderMap) -> Result<Vec<(String, String)>, S3RequestError> {
    const NAMES: [&str; 4] = [
        "if-match",
        "if-none-match",
        "if-modified-since",
        "if-unmodified-since",
    ];
    let mut conditions = Vec::new();
    for name in NAMES {
        if let Some(value) = headers.get(name) {
            let value = value.to_str().map_err(|_| {
                S3RequestError::invalid("InvalidArgument", format!("{name} is not ASCII"))
            })?;
            conditions.push((name.to_string(), value.to_string()));
        }
    }
    Ok(conditions)
}

fn requested_range(
    headers: &HeaderMap,
    object_size: u64,
) -> Result<Option<(u64, u64)>, S3RequestError> {
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| S3RequestError::invalid("InvalidArgument", "Range is not ASCII"))?;
    if value.contains(',') {
        return Err(S3RequestError::invalid_range(object_size));
    }
    let range = value
        .strip_prefix("bytes=")
        .ok_or_else(|| S3RequestError::invalid_range(object_size))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| S3RequestError::invalid_range(object_size))?;
    if object_size == 0 {
        return Err(S3RequestError::invalid_range(object_size));
    }
    if start.is_empty() {
        let suffix = end
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| S3RequestError::invalid_range(object_size))?;
        return Ok(Some((object_size.saturating_sub(suffix), object_size - 1)));
    }
    let start = start
        .parse::<u64>()
        .map_err(|_| S3RequestError::invalid_range(object_size))?;
    if start >= object_size {
        return Err(S3RequestError::invalid_range(object_size));
    }
    let end = if end.is_empty() {
        object_size - 1
    } else {
        end.parse::<u64>()
            .map_err(|_| S3RequestError::invalid_range(object_size))?
            .min(object_size - 1)
    };
    if end < start {
        return Err(S3RequestError::invalid_range(object_size));
    }
    Ok(Some((start, end)))
}

fn if_range_matches(headers: &HeaderMap, metadata: &HttpResponse, etag: &str) -> bool {
    let Some(value) = headers
        .get(header::IF_RANGE)
        .and_then(|value| value.to_str().ok())
    else {
        return true;
    };
    if value.starts_with('"') || value.starts_with("W/") {
        return !value.starts_with("W/") && value.trim_matches('"') == etag;
    }
    let (Ok(condition), Some(last_modified)) = (
        httpdate::parse_http_date(value),
        metadata.header("last-modified"),
    ) else {
        return false;
    };
    httpdate::parse_http_date(last_modified)
        .map(|modified| modified <= condition)
        .unwrap_or(false)
}

fn required_header<'a>(response: &'a HttpResponse, name: &str) -> Result<&'a str, S3RequestError> {
    response
        .header(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| S3RequestError::internal(format!("origin response omitted {name}")))
}

fn required_u64_header(response: &HttpResponse, name: &str) -> Result<u64, S3RequestError> {
    required_header(response, name)?
        .parse::<u64>()
        .map_err(|_| S3RequestError::internal(format!("origin returned invalid {name}")))
}

fn origin_conditions(conditions: &[(String, String)], etag: &str) -> Vec<(String, String)> {
    let mut output = conditions
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("if-match"))
        .cloned()
        .collect::<Vec<_>>();
    output.push(("if-match".into(), format!("\"{etag}\"")));
    output
}

struct ReadAccounting {
    route: GatewayRoute,
    outcome: GatewayOutcome,
    requested_bytes: u64,
    cache_bytes: u64,
    origin_bytes: u64,
}

fn read_response(
    object: ObjectId,
    metadata: HttpResponse,
    range: Option<(u64, u64)>,
    body: Body,
    accounting: ReadAccounting,
    context: &GatewayRequestContext,
) -> GatewayResponse {
    let mut response = Response::new(body);
    *response.status_mut() = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    copy_origin_headers(&metadata.headers, response.headers_mut());
    let content_length = range.map_or_else(
        || metadata.header("content-length").unwrap_or("0").to_string(),
        |(start, end)| {
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {start}-{end}/{}",
                    metadata.header("content-length").unwrap_or("0")
                ))
                .expect("numeric content range is a header value"),
            );
            (end - start + 1).to_string()
        },
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length).expect("numeric length is a header value"),
    );
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    stamp_gateway_headers(response.headers_mut(), &context.request_id);
    GatewayResponse {
        response,
        operation: GatewayOperation::Read,
        target: Some(GatewayTarget::Object(object)),
        route: accounting.route,
        outcome: accounting.outcome,
        failure: None,
        requested_bytes: accounting.requested_bytes,
        cache_bytes: accounting.cache_bytes,
        origin_bytes: accounting.origin_bytes,
    }
}

fn raw_response(
    origin: HttpResponse,
    operation: GatewayOperation,
    target: Option<GatewayTarget>,
    context: &GatewayRequestContext,
) -> GatewayResponse {
    let status = StatusCode::from_u16(origin.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let body_len = origin.body.len() as u64;
    let content_length = origin.header("content-length").map(str::to_owned);
    let mut response = Response::new(Body::from(origin.body));
    *response.status_mut() = status;
    copy_origin_headers(&origin.headers, response.headers_mut());
    if let Some(value) = content_length.and_then(|value| HeaderValue::from_str(&value).ok()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    stamp_gateway_headers(response.headers_mut(), &context.request_id);
    GatewayResponse {
        response,
        operation,
        target,
        route: GatewayRoute::Origin,
        outcome: if status.is_success() || status == StatusCode::NOT_MODIFIED {
            GatewayOutcome::Complete
        } else {
            GatewayOutcome::Failed
        },
        failure: failure_for_status(status),
        requested_bytes: 0,
        cache_bytes: 0,
        origin_bytes: body_len,
    }
}

async fn raw_streaming_response(
    mut origin: HttpStreamResponse,
    operation: GatewayOperation,
    target: Option<GatewayTarget>,
    context: &GatewayRequestContext,
) -> Result<GatewayResponse, S3RequestError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = origin.body.next().await {
        let chunk = chunk.map_err(S3RequestError::origin_unavailable)?;
        if bytes.len().saturating_add(chunk.len()) > MAX_ERROR_BODY_BYTES {
            return Err(S3RequestError::internal(
                "origin error response exceeded the bounded limit",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(raw_response(
        HttpResponse {
            status: origin.status,
            headers: origin.headers,
            body: Bytes::from(bytes),
        },
        operation,
        target,
        context,
    ))
}

fn copy_origin_headers(headers: &[(String, String)], output: &mut HeaderMap) {
    for (name, value) in headers {
        if is_hop_by_hop(name)
            || name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("content-range")
            || name.eq_ignore_ascii_case("x-amz-request-id")
            || name.eq_ignore_ascii_case("x-amz-id-2")
        {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        output.append(name, value);
    }
}

fn merge_get_metadata(
    mut metadata: HttpResponse,
    get_headers: &[(String, String)],
) -> HttpResponse {
    let mut merged = Vec::with_capacity(metadata.headers.len() + get_headers.len());
    for (name, value) in get_headers {
        if !name.eq_ignore_ascii_case("content-length")
            && !name.eq_ignore_ascii_case("content-range")
        {
            merged.push((name.clone(), value.clone()));
        }
    }
    for (name, value) in metadata.headers.drain(..) {
        if name.eq_ignore_ascii_case("content-length")
            || !merged
                .iter()
                .any(|(existing, _)| existing.eq_ignore_ascii_case(&name))
        {
            merged.push((name, value));
        }
    }
    metadata.headers = merged;
    metadata
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn stamp_gateway_headers(headers: &mut HeaderMap, request_id: &str) {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        headers.insert("x-amz-request-id", value.clone());
        headers.insert("x-amz-id-2", value);
    }
}

fn failure_for_status(status: StatusCode) -> Option<FailureReason> {
    match status {
        StatusCode::NOT_MODIFIED => None,
        status if status.is_success() => None,
        StatusCode::BAD_REQUEST | StatusCode::RANGE_NOT_SATISFIABLE => {
            Some(FailureReason::InvalidRequest)
        }
        StatusCode::UNAUTHORIZED => Some(FailureReason::Authentication),
        StatusCode::FORBIDDEN => Some(FailureReason::Authorization),
        StatusCode::NOT_FOUND => Some(FailureReason::NotFound),
        StatusCode::PRECONDITION_FAILED => Some(FailureReason::Precondition),
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => Some(FailureReason::Timeout),
        StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED => {
            Some(FailureReason::Unsupported)
        }
        _ => Some(FailureReason::Origin),
    }
}

#[derive(Debug)]
struct S3RequestError {
    status: StatusCode,
    code: &'static str,
    message: String,
    failure: FailureReason,
    content_range: Option<String>,
}

impl S3RequestError {
    fn invalid(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            message: message.into(),
            failure: FailureReason::InvalidRequest,
            content_range: None,
        }
    }

    fn invalid_range(size: u64) -> Self {
        Self {
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            code: "InvalidRange",
            message: "The requested range is not satisfiable".into(),
            failure: FailureReason::InvalidRequest,
            content_range: Some(format!("bytes */{size}")),
        }
    }

    fn origin_unavailable(message: impl Into<String>) -> Self {
        let _ = message.into();
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServiceUnavailable",
            message: "Please reduce your request rate".into(),
            failure: FailureReason::Origin,
            content_range: None,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "InternalError",
            message: message.into(),
            failure: FailureReason::Internal,
            content_range: None,
        }
    }
}

fn cache_error(error: CacheReadError) -> S3RequestError {
    match error {
        CacheReadError::InvalidRequest(message) => S3RequestError::invalid("InvalidRange", message),
        CacheReadError::NotFound(message) | CacheReadError::CacheMiss(message) => S3RequestError {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchKey",
            message,
            failure: FailureReason::NotFound,
            content_range: None,
        },
        CacheReadError::VersionMismatch(message) => S3RequestError {
            status: StatusCode::PRECONDITION_FAILED,
            code: "PreconditionFailed",
            message,
            failure: FailureReason::Precondition,
            content_range: None,
        },
        CacheReadError::Timeout(message) => S3RequestError {
            status: StatusCode::GATEWAY_TIMEOUT,
            code: "RequestTimeout",
            message,
            failure: FailureReason::Timeout,
            content_range: None,
        },
        CacheReadError::Unavailable(message) => S3RequestError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServiceUnavailable",
            message,
            failure: FailureReason::CacheUnavailable,
            content_range: None,
        },
        CacheReadError::Origin(message) => S3RequestError::origin_unavailable(message),
        CacheReadError::Protocol(message)
        | CacheReadError::Internal(message)
        | CacheReadError::Unknown(message) => S3RequestError::internal(message),
    }
}

fn error_response(error: S3RequestError, request_id: &str) -> GatewayResponse {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message><RequestId>{}</RequestId><HostId>{}</HostId></Error>",
        xml_escape(error.code),
        xml_escape(&error.message),
        xml_escape(request_id),
        xml_escape(request_id),
    );
    let mut response = (error.status, Body::from(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    if let Some(content_range) = error.content_range {
        if let Ok(value) = HeaderValue::from_str(&content_range) {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
    }
    stamp_gateway_headers(response.headers_mut(), request_id);
    GatewayResponse {
        response,
        operation: GatewayOperation::Unsupported,
        target: None,
        route: GatewayRoute::None,
        outcome: GatewayOutcome::Failed,
        failure: Some(error.failure),
        requested_bytes: 0,
        cache_bytes: 0,
        origin_bytes: 0,
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[async_trait]
impl GatewayAdapter for S3Adapter {
    fn protocol(&self) -> ProviderProtocol {
        ProviderProtocol::S3
    }

    async fn handle(&self, request: Request, context: GatewayRequestContext) -> GatewayResponse {
        match self.handle_request(request, &context).await {
            Ok(response) => response,
            Err(error) => error_response(error, &context.request_id),
        }
    }
}

impl S3Adapter {
    async fn handle_request(
        &self,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let target = parse_target(&request, &self.config)?;
        let query = S3Query::parse(request.uri().query())?;
        if request.method() == axum::http::Method::GET && target.key.is_none() && query.is_list_v2()
        {
            return self.list(target.bucket, query, context).await;
        }
        let key = target
            .key
            .ok_or_else(|| S3RequestError::invalid("InvalidURI", "request has no object key"))?;
        let object = ObjectId::new(Backend::S3, target.bucket, key);
        match *request.method() {
            axum::http::Method::HEAD => self.head(object, request.headers(), context).await,
            axum::http::Method::GET => self.get(object, request.headers(), context).await,
            _ => Err(S3RequestError {
                status: StatusCode::METHOD_NOT_ALLOWED,
                code: "MethodNotAllowed",
                message: "The specified method is not allowed".into(),
                failure: FailureReason::Unsupported,
                content_range: None,
            }),
        }
    }

    async fn head(
        &self,
        object: ObjectId,
        headers: &HeaderMap,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let conditions = conditional_headers(headers)?;
        let response = self
            .origin
            .head(&object, &conditions)
            .await
            .map_err(S3RequestError::origin_unavailable)?;
        Ok(raw_response(
            response,
            GatewayOperation::Stat,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn list(
        &self,
        bucket: String,
        query: S3Query,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let max_keys = query.max_keys()?;
        let response = self
            .origin
            .list(
                &bucket,
                query.prefix.as_deref().unwrap_or(""),
                query.delimiter.as_deref(),
                query.continuation_token.as_deref(),
                max_keys,
                query.encoding_type.as_deref(),
            )
            .await
            .map_err(S3RequestError::origin_unavailable)?;
        Ok(raw_response(
            response,
            GatewayOperation::List,
            Some(GatewayTarget::Namespace {
                backend: Backend::S3,
                namespace: bucket,
                prefix: query.prefix,
            }),
            context,
        ))
    }

    async fn get(
        &self,
        object: ObjectId,
        headers: &HeaderMap,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let conditions = conditional_headers(headers)?;
        let metadata = self
            .origin
            .head(&object, &conditions)
            .await
            .map_err(S3RequestError::origin_unavailable)?;
        if !(200..300).contains(&metadata.status) {
            return Ok(raw_response(
                metadata,
                GatewayOperation::Read,
                Some(GatewayTarget::Object(object)),
                context,
            ));
        }
        let size = required_u64_header(&metadata, "content-length")?;
        let etag = required_header(&metadata, "etag")?
            .trim_matches('"')
            .to_string();
        let version = Version::new(etag.clone());
        let mut range = requested_range(headers, size)?;
        if !if_range_matches(headers, &metadata, &etag) {
            range = None;
        }
        let (offset, len) = match range {
            Some((start, end)) => (start, end - start + 1),
            None => (0, size),
        };
        let route = self.config.default_route;
        if route == GatewayRoute::Origin {
            return self
                .origin_get(object, range, conditions, metadata, route, context)
                .await;
        }

        let mut cache = match self.cache.stream(S3CacheRequest {
            object: &object,
            version: &version,
            object_size: size,
            offset,
            len,
            block_size: self.config.block_size,
            chunk_size: self.config.transfer_chunk_bytes,
            now_ms: unix_millis(),
        }) {
            Ok(cache) => cache,
            Err(error) if error.fallback_eligible() && route == GatewayRoute::Cache => {
                return self
                    .origin_get(
                        object,
                        range,
                        origin_conditions(&conditions, &etag),
                        metadata,
                        GatewayRoute::Cache,
                        context,
                    )
                    .await
                    .map(|mut response| {
                        response.outcome = GatewayOutcome::Fallback;
                        response
                    });
            }
            Err(error) => return Err(cache_error(error)),
        };
        match cache.next().await {
            Some(Ok(first)) => {
                let body = futures::stream::once(async move { Ok(first) }).chain(cache);
                Ok(read_response(
                    object,
                    metadata,
                    range,
                    Body::from_stream(body),
                    ReadAccounting {
                        route,
                        outcome: GatewayOutcome::Complete,
                        requested_bytes: len,
                        cache_bytes: len,
                        origin_bytes: 0,
                    },
                    context,
                ))
            }
            None if len == 0 => Ok(read_response(
                object,
                metadata,
                range,
                Body::empty(),
                ReadAccounting {
                    route,
                    outcome: GatewayOutcome::Complete,
                    requested_bytes: 0,
                    cache_bytes: 0,
                    origin_bytes: 0,
                },
                context,
            )),
            Some(Err(error)) if error.fallback_eligible() && route == GatewayRoute::Cache => self
                .origin_get(
                    object,
                    range,
                    origin_conditions(&conditions, &etag),
                    metadata,
                    GatewayRoute::Cache,
                    context,
                )
                .await
                .map(|mut response| {
                    response.outcome = GatewayOutcome::Fallback;
                    response
                }),
            Some(Err(error)) => Err(cache_error(error)),
            None => Err(S3RequestError::internal(
                "cache stream ended before producing the requested bytes",
            )),
        }
    }

    async fn origin_get(
        &self,
        object: ObjectId,
        range: Option<(u64, u64)>,
        conditions: Vec<(String, String)>,
        metadata: HttpResponse,
        route: GatewayRoute,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let response = self
            .origin
            .get(&object, range, &conditions)
            .await
            .map_err(S3RequestError::origin_unavailable)?;
        if !response.is_success() {
            return raw_streaming_response(
                response,
                GatewayOperation::Read,
                Some(GatewayTarget::Object(object)),
                context,
            )
            .await;
        }
        if range.is_some() && response.status != 206 {
            return Err(S3RequestError::internal(format!(
                "origin returned HTTP {} for a ranged GET",
                response.status
            )));
        }
        if range.is_none() && response.status != 200 {
            return Err(S3RequestError::internal(format!(
                "origin returned HTTP {} for a whole GET",
                response.status
            )));
        }
        let len = match range {
            Some((start, end)) => end - start + 1,
            None => required_u64_header(&metadata, "content-length")?,
        };
        let response_metadata = merge_get_metadata(metadata, &response.headers);
        let body = response
            .body
            .map(|chunk| chunk.map_err(std::io::Error::other));
        Ok(read_response(
            object,
            response_metadata,
            range,
            Body::from_stream(body),
            ReadAccounting {
                route,
                outcome: GatewayOutcome::Bypass,
                requested_bytes: len,
                cache_bytes: 0,
                origin_bytes: len,
            },
            context,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use axum::body::to_bytes;

    type ListRequest = (String, String, Option<String>, Option<String>, u32);

    struct MockCache {
        response: Result<Vec<Bytes>, CacheReadError>,
    }

    impl S3Cache for MockCache {
        fn stream(&self, _request: S3CacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
            match &self.response {
                Ok(chunks) => Ok(Box::pin(futures::stream::iter(
                    chunks.clone().into_iter().map(Ok),
                ))),
                Err(CacheReadError::Unavailable(message)) => {
                    Err(CacheReadError::Unavailable(message.clone()))
                }
                _ => unreachable!("test cache response"),
            }
        }
    }

    struct MockOrigin {
        lists: Mutex<Vec<ListRequest>>,
    }

    impl MockOrigin {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                lists: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl S3Origin for MockOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            Ok(HttpResponse {
                status: 200,
                headers: vec![
                    ("content-length".into(), "6".into()),
                    ("etag".into(), "\"v1\"".into()),
                    (
                        "last-modified".into(),
                        "Sun, 06 Nov 1994 08:49:37 GMT".into(),
                    ),
                ],
                body: Bytes::new(),
            })
        }

        async fn get(
            &self,
            _object: &ObjectId,
            range: Option<(u64, u64)>,
            _conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            let bytes = match range {
                Some((start, end)) => {
                    Bytes::from_static(b"abcdef").slice(start as usize..=end as usize)
                }
                None => Bytes::from_static(b"abcdef"),
            };
            Ok(HttpStreamResponse {
                status: if range.is_some() { 206 } else { 200 },
                headers: Vec::new(),
                body: Box::pin(futures::stream::once(async move { Ok(bytes) })),
            })
        }

        async fn list(
            &self,
            bucket: &str,
            prefix: &str,
            delimiter: Option<&str>,
            continuation_token: Option<&str>,
            max_keys: u32,
            _encoding_type: Option<&str>,
        ) -> Result<HttpResponse, String> {
            self.lists.lock().unwrap().push((
                bucket.into(),
                prefix.into(),
                delimiter.map(str::to_string),
                continuation_token.map(str::to_string),
                max_keys,
            ));
            Ok(HttpResponse {
                status: 200,
                headers: vec![("content-type".into(), "application/xml".into())],
                body: Bytes::from_static(b"<ListBucketResult/>"),
            })
        }
    }

    fn adapter(cache: MockCache, origin: Arc<MockOrigin>) -> S3Adapter {
        S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::new(cache),
            origin,
        )
        .unwrap()
    }

    fn request(method: axum::http::Method, uri: &str) -> Request {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap()
    }

    fn context() -> GatewayRequestContext {
        GatewayRequestContext {
            request_id: "00000000-0000-4000-8000-000000000001".into(),
            started: std::time::Instant::now(),
        }
    }

    #[test]
    fn parses_path_and_virtual_host_targets() {
        let path = request(axum::http::Method::GET, "/bucket/a%20b/c");
        let parsed = parse_target(&path, &S3AdapterConfig::path_style("localhost")).unwrap();
        assert_eq!(parsed.bucket, "bucket");
        assert_eq!(parsed.key.as_deref(), Some("a b/c"));

        let virtual_request = Request::builder()
            .uri("/key")
            .header(header::HOST, "bucket.s3.us-east-1.amazonaws.com")
            .body(Body::empty())
            .unwrap();
        let parsed = parse_target(&virtual_request, &S3AdapterConfig::aws("us-east-1")).unwrap();
        assert_eq!(parsed.bucket, "bucket");
        assert_eq!(parsed.key.as_deref(), Some("key"));
        assert_eq!(
            S3Query::parse(Some("list-type=2&max-keys=0"))
                .unwrap()
                .max_keys()
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn serves_cached_full_and_exact_range_reads() {
        let origin = MockOrigin::new();
        let full = adapter(
            MockCache {
                response: Ok(vec![Bytes::from_static(b"abc"), Bytes::from_static(b"def")]),
            },
            Arc::clone(&origin),
        )
        .handle(request(axum::http::Method::GET, "/bucket/key"), context())
        .await;
        assert_eq!(full.response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(full.response.into_body(), 16).await.unwrap(),
            "abcdef"
        );

        let mut range_request = request(axum::http::Method::GET, "/bucket/key");
        range_request
            .headers_mut()
            .insert(header::RANGE, HeaderValue::from_static("bytes=1-3"));
        let range = adapter(
            MockCache {
                response: Ok(vec![Bytes::from_static(b"bcd")]),
            },
            origin,
        )
        .handle(range_request, context())
        .await;
        assert_eq!(range.response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            range.response.headers()[header::CONTENT_RANGE],
            "bytes 1-3/6"
        );
        assert_eq!(
            to_bytes(range.response.into_body(), 16).await.unwrap(),
            "bcd"
        );
    }

    #[tokio::test]
    async fn unavailable_cache_falls_back_to_conditional_origin() {
        let response = adapter(
            MockCache {
                response: Err(CacheReadError::Unavailable("worker down".into())),
            },
            MockOrigin::new(),
        )
        .handle(request(axum::http::Method::GET, "/bucket/key"), context())
        .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(response.outcome, GatewayOutcome::Fallback);
        assert_eq!(
            to_bytes(response.response.into_body(), 16).await.unwrap(),
            "abcdef"
        );
    }

    #[tokio::test]
    async fn forwards_bounded_list_v2_parameters() {
        let origin = MockOrigin::new();
        let response = adapter(
            MockCache {
                response: Ok(Vec::new()),
            },
            Arc::clone(&origin),
        )
        .handle(
            request(
                axum::http::Method::GET,
                "/bucket?list-type=2&prefix=a%2Fb&delimiter=%2F&continuation-token=next&max-keys=7",
            ),
            context(),
        )
        .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(
            origin.lists.lock().unwrap().as_slice(),
            &[(
                "bucket".into(),
                "a/b".into(),
                Some("/".into()),
                Some("next".into()),
                7,
            )]
        );
    }

    #[tokio::test]
    async fn rejects_multi_range_with_s3_xml_error() {
        let mut multi = request(axum::http::Method::GET, "/bucket/key");
        multi
            .headers_mut()
            .insert(header::RANGE, HeaderValue::from_static("bytes=0-1,4-5"));
        let response = adapter(
            MockCache {
                response: Ok(Vec::new()),
            },
            MockOrigin::new(),
        )
        .handle(multi, context())
        .await;
        assert_eq!(
            response.response.status(),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        let body = to_bytes(response.response.into_body(), 4096).await.unwrap();
        assert!(body
            .windows(b"<Code>InvalidRange</Code>".len())
            .any(|window| window == b"<Code>InvalidRange</Code>"));
    }
}
