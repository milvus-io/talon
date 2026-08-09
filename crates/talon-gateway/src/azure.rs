//! Azure Blob Storage read adapter.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use talon_backend::{AzureBackend, HttpResponse, HttpStreamResponse};
use talon_cache_client::{BlockReader, CacheReadError, FileView, DEFAULT_TRANSFER_CHUNK_BYTES};
use talon_core::{Backend, ObjectId, Version};

use crate::{
    FailureReason, GatewayAdapter, GatewayOperation, GatewayOutcome, GatewayRequestContext,
    GatewayResponse, GatewayRoute, GatewayTarget, ProviderProtocol,
};

const AZURE_API_VERSION: &str = "2021-12-02";
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

type CacheStream = Pin<Box<dyn Stream<Item = Result<Bytes, CacheReadError>> + Send>>;

/// Exact cache range requested by the protocol adapter.
pub struct AzureCacheRequest<'a> {
    /// Stable object identity.
    pub object: &'a ObjectId,
    /// Authoritative origin version.
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

/// Azure request addressing and cache behavior.
#[derive(Debug, Clone)]
pub struct AzureAdapterConfig {
    /// The only storage account served by this process.
    pub account: String,
    /// Public-cloud host suffix used by virtual-host requests.
    pub endpoint_suffix: String,
    /// Whether incoming requests use Azurite-style `/account/container/blob` paths.
    pub path_style: bool,
    /// Talon cache block size.
    pub block_size: u32,
    /// Maximum worker response chunk passed to one HTTP body frame.
    pub transfer_chunk_bytes: u32,
    /// Route used until signed cache marks are implemented by #441.
    pub default_route: GatewayRoute,
}

impl AzureAdapterConfig {
    /// Public Azure endpoint for one account.
    pub fn public_cloud(account: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            endpoint_suffix: "blob.core.windows.net".into(),
            path_style: false,
            block_size: 256 * 1024 * 1024,
            transfer_chunk_bytes: DEFAULT_TRANSFER_CHUNK_BYTES,
            default_route: GatewayRoute::Cache,
        }
    }

    /// Azurite-style path addressing for one account.
    pub fn path_style(account: impl Into<String>) -> Self {
        Self {
            path_style: true,
            ..Self::public_cloud(account)
        }
    }

    fn validate(&self) -> Result<(), AzureRequestError> {
        if self.account.is_empty() || self.block_size == 0 || self.transfer_chunk_bytes == 0 {
            return Err(AzureRequestError::invalid(
                "InvalidHeaderValue",
                "Azure adapter account and cache sizes must be configured",
            ));
        }
        Ok(())
    }
}

/// Cache streaming seam used by the production reader and focused adapter tests.
pub trait AzureCache: Send + Sync + 'static {
    /// Stream one exact object range from Talon.
    fn stream(&self, request: AzureCacheRequest<'_>) -> Result<CacheStream, CacheReadError>;
}

impl AzureCache for BlockReader {
    fn stream(&self, request: AzureCacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
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

/// Origin seam. The production implementation keeps GET bodies streaming.
#[async_trait]
pub trait AzureOrigin: Send + Sync + 'static {
    /// Execute a conditional blob metadata request.
    async fn head(
        &self,
        object: &ObjectId,
        conditions: &[(String, String)],
    ) -> Result<HttpResponse, String>;

    /// Execute a whole or ranged blob GET.
    async fn get(
        &self,
        object: &ObjectId,
        range: Option<(u64, u64)>,
        conditions: &[(String, String)],
    ) -> Result<HttpStreamResponse, String>;

    /// Execute one List Blobs page.
    async fn list(
        &self,
        container: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_results: u32,
    ) -> Result<HttpResponse, String>;
}

#[async_trait]
impl AzureOrigin for AzureBackend {
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
        container: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_results: u32,
    ) -> Result<HttpResponse, String> {
        self.execute_list_raw(container, prefix, delimiter, marker, max_results, &[])
            .await
    }
}

/// Azure protocol adapter over a Talon cache and scoped origin identity.
pub struct AzureBlobAdapter {
    config: AzureAdapterConfig,
    cache: Arc<dyn AzureCache>,
    origin: Arc<dyn AzureOrigin>,
}

impl AzureBlobAdapter {
    /// Construct one-account Azure compatibility adapter.
    pub fn new(
        config: AzureAdapterConfig,
        cache: Arc<dyn AzureCache>,
        origin: Arc<dyn AzureOrigin>,
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
    container: String,
    blob: Option<String>,
}

fn parse_target(
    request: &Request,
    config: &AzureAdapterConfig,
) -> Result<ParsedTarget, AzureRequestError> {
    let raw_segments = request
        .uri()
        .path()
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let (account, container_index) = if config.path_style {
        let raw_account = raw_segments.first().copied().unwrap_or_default();
        (Some(decode_path(raw_account)?), 1)
    } else {
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| AzureRequestError::invalid("InvalidUri", "missing Host header"))?;
        let host = host.split(':').next().unwrap_or(host);
        let suffix = format!(".{}", config.endpoint_suffix);
        let account = host.strip_suffix(&suffix).ok_or_else(|| {
            AzureRequestError::invalid("InvalidUri", "host is not an Azure Blob endpoint")
        })?;
        (Some(account.to_string()), 0)
    };
    if account.as_deref() != Some(config.account.as_str()) {
        return Err(AzureRequestError::invalid(
            "InvalidUri",
            "request targets a different storage account",
        ));
    }
    let container = raw_segments
        .get(container_index)
        .copied()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AzureRequestError::invalid("InvalidUri", "missing container"))?;
    let container = decode_path(container)?;
    let raw_blob = raw_segments
        .get(container_index + 1..)
        .unwrap_or_default()
        .join("/");
    let blob = if raw_blob.is_empty() {
        None
    } else {
        Some(decode_path(&raw_blob)?)
    };
    Ok(ParsedTarget { container, blob })
}

fn decode_path(value: &str) -> Result<String, AzureRequestError> {
    validate_percent_encoding(value, "path")?;
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| AzureRequestError::invalid("InvalidUri", "path is not valid UTF-8"))
}

fn validate_percent_encoding(value: &str, component: &str) -> Result<(), AzureRequestError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(AzureRequestError::invalid(
                    "InvalidUri",
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

#[derive(Default)]
struct AzureQuery {
    restype: Option<String>,
    comp: Option<String>,
    prefix: Option<String>,
    delimiter: Option<String>,
    marker: Option<String>,
    maxresults: Option<String>,
}

impl AzureQuery {
    fn parse(query: Option<&str>) -> Result<Self, AzureRequestError> {
        validate_percent_encoding(query.unwrap_or_default(), "query")?;
        let mut parsed = Self::default();
        for (key, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
            match key.as_ref() {
                "restype" => parsed.restype = Some(value.into_owned()),
                "comp" => parsed.comp = Some(value.into_owned()),
                "prefix" => parsed.prefix = Some(value.into_owned()),
                "delimiter" => parsed.delimiter = Some(value.into_owned()),
                "marker" => parsed.marker = Some(value.into_owned()),
                "maxresults" => parsed.maxresults = Some(value.into_owned()),
                _ => {}
            }
        }
        Ok(parsed)
    }

    fn is_list(&self) -> bool {
        self.restype.as_deref() == Some("container") && self.comp.as_deref() == Some("list")
    }

    fn max_results(&self) -> Result<u32, AzureRequestError> {
        match self.maxresults.as_deref() {
            None => Ok(5000),
            Some(value) => value
                .parse::<u32>()
                .ok()
                .filter(|value| (1..=5000).contains(value))
                .ok_or_else(|| {
                    AzureRequestError::invalid(
                        "InvalidQueryParameterValue",
                        "maxresults must be between 1 and 5000",
                    )
                }),
        }
    }
}

fn conditional_headers(headers: &HeaderMap) -> Result<Vec<(String, String)>, AzureRequestError> {
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
                AzureRequestError::invalid(
                    "InvalidHeaderValue",
                    format!("{name} is not a valid HTTP header value"),
                )
            })?;
            conditions.push((name.to_string(), value.to_string()));
        }
    }
    Ok(conditions)
}

fn requested_range(
    headers: &HeaderMap,
    object_size: u64,
) -> Result<Option<(u64, u64)>, AzureRequestError> {
    let value = headers
        .get("x-ms-range")
        .or_else(|| headers.get(header::RANGE));
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        AzureRequestError::invalid("InvalidHeaderValue", "range header is not ASCII")
    })?;
    if value.contains(',') {
        return Err(AzureRequestError::invalid(
            "MultipleConditionHeadersNotSupported",
            "multiple byte ranges are not supported",
        ));
    }
    let range = value.strip_prefix("bytes=").ok_or_else(|| {
        AzureRequestError::invalid("InvalidHeaderValue", "range unit must be bytes")
    })?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| AzureRequestError::invalid("InvalidHeaderValue", "malformed byte range"))?;
    if object_size == 0 {
        return Err(AzureRequestError::invalid_range(object_size));
    }
    if start.is_empty() {
        let suffix = end
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| AzureRequestError::invalid_range(object_size))?;
        let start = object_size.saturating_sub(suffix);
        return Ok(Some((start, object_size - 1)));
    }
    let start = start
        .parse::<u64>()
        .map_err(|_| AzureRequestError::invalid_range(object_size))?;
    if start >= object_size {
        return Err(AzureRequestError::invalid_range(object_size));
    }
    let end = if end.is_empty() {
        object_size - 1
    } else {
        end.parse::<u64>()
            .map_err(|_| AzureRequestError::invalid_range(object_size))?
            .min(object_size - 1)
    };
    if end < start {
        return Err(AzureRequestError::invalid_range(object_size));
    }
    Ok(Some((start, end)))
}

fn if_range_matches(
    headers: &HeaderMap,
    metadata: &HttpResponse,
    etag: &str,
) -> Result<bool, AzureRequestError> {
    let Some(value) = headers.get(header::IF_RANGE) else {
        return Ok(true);
    };
    let value = value
        .to_str()
        .map_err(|_| AzureRequestError::invalid("InvalidHeaderValue", "If-Range is not ASCII"))?;
    if value.starts_with('"') || value.starts_with("W/") {
        return Ok(!value.starts_with("W/") && value.trim_matches('"') == etag);
    }
    let Ok(condition_date) = httpdate::parse_http_date(value) else {
        return Ok(false);
    };
    let Some(last_modified) = metadata.header("last-modified") else {
        return Ok(false);
    };
    Ok(httpdate::parse_http_date(last_modified)
        .map(|modified| modified <= condition_date)
        .unwrap_or(false))
}

fn required_header<'a>(
    response: &'a HttpResponse,
    name: &str,
) -> Result<&'a str, AzureRequestError> {
    response
        .header(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AzureRequestError::internal(format!("origin response omitted {name}")))
}

fn required_u64_header(response: &HttpResponse, name: &str) -> Result<u64, AzureRequestError> {
    required_header(response, name)?
        .parse::<u64>()
        .map_err(|_| AzureRequestError::internal(format!("origin returned invalid {name}")))
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
    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
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

fn raw_buffered_response(
    origin: HttpResponse,
    operation: GatewayOperation,
    route: GatewayRoute,
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
        route,
        outcome: if status.is_success() {
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
    route: GatewayRoute,
    target: Option<GatewayTarget>,
    context: &GatewayRequestContext,
) -> Result<GatewayResponse, AzureRequestError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = origin.body.next().await {
        let chunk = chunk.map_err(AzureRequestError::origin_unavailable)?;
        if bytes.len().saturating_add(chunk.len()) > MAX_ERROR_BODY_BYTES {
            return Err(AzureRequestError::internal(
                "origin error response exceeded the bounded error body limit",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(raw_buffered_response(
        HttpResponse {
            status: origin.status,
            headers: origin.headers,
            body: Bytes::from(bytes),
        },
        operation,
        route,
        target,
        context,
    ))
}

fn copy_origin_headers(headers: &[(String, String)], output: &mut HeaderMap) {
    for (name, value) in headers {
        if is_hop_by_hop(name)
            || name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("content-range")
            || name.eq_ignore_ascii_case("x-ms-request-id")
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
        headers.insert("x-ms-request-id", value);
    }
    headers.insert("x-ms-version", HeaderValue::from_static(AZURE_API_VERSION));
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
struct AzureRequestError {
    status: StatusCode,
    code: &'static str,
    message: String,
    failure: FailureReason,
    content_range: Option<String>,
}

impl AzureRequestError {
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
            message: "the requested range is not satisfiable".into(),
            failure: FailureReason::InvalidRequest,
            content_range: Some(format!("bytes */{size}")),
        }
    }

    fn origin_unavailable(message: impl Into<String>) -> Self {
        let _ = message.into();
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServerBusy",
            message: "the storage service is temporarily unavailable".into(),
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

fn cache_error(error: CacheReadError) -> AzureRequestError {
    match error {
        CacheReadError::InvalidRequest(message) => {
            AzureRequestError::invalid("InvalidRange", message)
        }
        CacheReadError::NotFound(message) => AzureRequestError {
            status: StatusCode::NOT_FOUND,
            code: "BlobNotFound",
            message,
            failure: FailureReason::NotFound,
            content_range: None,
        },
        CacheReadError::VersionMismatch(message) => AzureRequestError {
            status: StatusCode::PRECONDITION_FAILED,
            code: "ConditionNotMet",
            message,
            failure: FailureReason::Precondition,
            content_range: None,
        },
        CacheReadError::CacheMiss(message) => AzureRequestError {
            status: StatusCode::NOT_FOUND,
            code: "BlobNotFound",
            message,
            failure: FailureReason::CacheUnavailable,
            content_range: None,
        },
        CacheReadError::Timeout(message) => AzureRequestError {
            status: StatusCode::GATEWAY_TIMEOUT,
            code: "OperationTimedOut",
            message,
            failure: FailureReason::Timeout,
            content_range: None,
        },
        CacheReadError::Unavailable(message) => AzureRequestError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServerBusy",
            message,
            failure: FailureReason::CacheUnavailable,
            content_range: None,
        },
        CacheReadError::Origin(message) => AzureRequestError::origin_unavailable(message),
        CacheReadError::Protocol(message)
        | CacheReadError::Internal(message)
        | CacheReadError::Unknown(message) => AzureRequestError::internal(message),
    }
}

fn error_response(error: AzureRequestError, request_id: &str) -> GatewayResponse {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>{}</Code><Message>{}</Message><RequestId>{}</RequestId></Error>",
        xml_escape(error.code),
        xml_escape(&error.message),
        xml_escape(request_id),
    );
    let mut response = (error.status, Body::from(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
        .headers_mut()
        .insert("x-ms-error-code", HeaderValue::from_static(error.code));
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

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use axum::body::to_bytes;

    type GetRequest = (Option<(u64, u64)>, Vec<(String, String)>);
    type ListRequest = (String, String, Option<String>, Option<String>, u32);

    #[derive(Clone)]
    enum CacheBehavior {
        Data(Vec<Bytes>),
        Unavailable,
    }

    struct MockCache {
        behavior: CacheBehavior,
        request: Mutex<Option<(ObjectId, Version, u64, u64)>>,
    }

    impl MockCache {
        fn data(chunks: &[&'static [u8]]) -> Arc<Self> {
            Arc::new(Self {
                behavior: CacheBehavior::Data(
                    chunks
                        .iter()
                        .map(|chunk| Bytes::from_static(chunk))
                        .collect(),
                ),
                request: Mutex::new(None),
            })
        }

        fn unavailable() -> Arc<Self> {
            Arc::new(Self {
                behavior: CacheBehavior::Unavailable,
                request: Mutex::new(None),
            })
        }
    }

    impl AzureCache for MockCache {
        fn stream(&self, request: AzureCacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
            *self.request.lock().unwrap() = Some((
                request.object.clone(),
                request.version.clone(),
                request.offset,
                request.len,
            ));
            match &self.behavior {
                CacheBehavior::Data(chunks) => Ok(Box::pin(futures::stream::iter(
                    chunks.clone().into_iter().map(Ok),
                ))),
                CacheBehavior::Unavailable => Err(CacheReadError::Unavailable("down".into())),
            }
        }
    }

    struct MockOrigin {
        head_response: HttpResponse,
        list_response: HttpResponse,
        get_status: u16,
        get_headers: Vec<(String, String)>,
        get_chunks: Vec<Bytes>,
        head_conditions: Mutex<Vec<(String, String)>>,
        get_request: Mutex<Option<GetRequest>>,
        list_request: Mutex<Option<ListRequest>>,
        get_polls: Arc<AtomicUsize>,
        get_dropped: Arc<AtomicBool>,
    }

    impl MockOrigin {
        fn object(body: &[&'static [u8]]) -> Arc<Self> {
            Arc::new(Self {
                head_response: HttpResponse {
                    status: 200,
                    headers: vec![
                        ("Content-Length".into(), "6".into()),
                        ("ETag".into(), "\"v1\"".into()),
                        ("Content-Type".into(), "text/plain".into()),
                        (
                            "Last-Modified".into(),
                            "Sun, 06 Nov 1994 08:49:37 GMT".into(),
                        ),
                        ("x-ms-blob-type".into(), "BlockBlob".into()),
                    ],
                    body: Bytes::new(),
                },
                list_response: HttpResponse {
                    status: 200,
                    headers: vec![("Content-Type".into(), "application/xml".into())],
                    body: Bytes::from_static(b"<EnumerationResults/>"),
                },
                get_status: 200,
                get_headers: vec![("Content-Type".into(), "application/custom".into())],
                get_chunks: body.iter().map(|chunk| Bytes::from_static(chunk)).collect(),
                head_conditions: Mutex::new(Vec::new()),
                get_request: Mutex::new(None),
                list_request: Mutex::new(None),
                get_polls: Arc::new(AtomicUsize::new(0)),
                get_dropped: Arc::new(AtomicBool::new(false)),
            })
        }
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl AzureOrigin for MockOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            *self.head_conditions.lock().unwrap() = conditions.to_vec();
            Ok(self.head_response.clone())
        }

        async fn get(
            &self,
            _object: &ObjectId,
            range: Option<(u64, u64)>,
            conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            *self.get_request.lock().unwrap() = Some((range, conditions.to_vec()));
            let chunks = VecDeque::from(self.get_chunks.clone());
            let polls = Arc::clone(&self.get_polls);
            let guard = DropSignal(Arc::clone(&self.get_dropped));
            let body = futures::stream::unfold((chunks, guard), move |(mut chunks, guard)| {
                let polls = Arc::clone(&polls);
                async move {
                    polls.fetch_add(1, Ordering::SeqCst);
                    chunks.pop_front().map(|chunk| (Ok(chunk), (chunks, guard)))
                }
            });
            Ok(HttpStreamResponse {
                status: self.get_status,
                headers: self.get_headers.clone(),
                body: Box::pin(body),
            })
        }

        async fn list(
            &self,
            container: &str,
            prefix: &str,
            delimiter: Option<&str>,
            marker: Option<&str>,
            max_results: u32,
        ) -> Result<HttpResponse, String> {
            *self.list_request.lock().unwrap() = Some((
                container.to_string(),
                prefix.to_string(),
                delimiter.map(str::to_owned),
                marker.map(str::to_owned),
                max_results,
            ));
            Ok(self.list_response.clone())
        }
    }

    fn context() -> GatewayRequestContext {
        GatewayRequestContext {
            request_id: "0011223344556677".into(),
            started: std::time::Instant::now(),
        }
    }

    fn request(method: axum::http::Method, uri: &str) -> Request {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "acct.blob.core.windows.net")
            .body(Body::empty())
            .unwrap()
    }

    fn adapter(
        mut config: AzureAdapterConfig,
        cache: Arc<dyn AzureCache>,
        origin: Arc<dyn AzureOrigin>,
    ) -> AzureBlobAdapter {
        config.block_size = 4;
        config.transfer_chunk_bytes = 2;
        AzureBlobAdapter::new(config, cache, origin).unwrap()
    }

    #[test]
    fn virtual_host_and_path_style_targets_decode_without_normalizing_slashes() {
        let virtual_request = request(axum::http::Method::GET, "/container/dir%2Fpart/blob%20name");
        let target =
            parse_target(&virtual_request, &AzureAdapterConfig::public_cloud("acct")).unwrap();
        assert_eq!(target.container, "container");
        assert_eq!(target.blob.as_deref(), Some("dir/part/blob name"));

        let path_request = Request::builder()
            .uri("/devstoreaccount1/container/a%2Fb")
            .header(header::HOST, "127.0.0.1:10000")
            .body(Body::empty())
            .unwrap();
        let target = parse_target(
            &path_request,
            &AzureAdapterConfig::path_style("devstoreaccount1"),
        )
        .unwrap();
        assert_eq!(target.container, "container");
        assert_eq!(target.blob.as_deref(), Some("a/b"));
    }

    #[test]
    fn malformed_encoding_and_cross_account_hosts_are_rejected() {
        let malformed = request(axum::http::Method::GET, "/container/bad%2");
        assert!(parse_target(&malformed, &AzureAdapterConfig::public_cloud("acct")).is_err());
        let other = Request::builder()
            .uri("/container/blob")
            .header(header::HOST, "other.blob.core.windows.net")
            .body(Body::empty())
            .unwrap();
        assert!(parse_target(&other, &AzureAdapterConfig::public_cloud("acct")).is_err());
        assert!(AzureQuery::parse(Some("prefix=bad%XX")).is_err());
    }

    #[test]
    fn ranges_cover_closed_open_suffix_and_invalid_forms() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=2-20"));
        assert_eq!(requested_range(&headers, 10).unwrap(), Some((2, 9)));
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=3-"));
        assert_eq!(requested_range(&headers, 10).unwrap(), Some((3, 9)));
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=-4"));
        assert_eq!(requested_range(&headers, 10).unwrap(), Some((6, 9)));
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=10-"));
        assert_eq!(
            requested_range(&headers, 10).unwrap_err().status,
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-1,3-4"));
        assert!(requested_range(&headers, 10).is_err());
    }

    #[tokio::test]
    async fn ranged_cache_read_uses_authoritative_metadata_and_streams_exact_bytes() {
        let cache = MockCache::data(&[b"bc", b"de"]);
        let origin = MockOrigin::object(&[]);
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            cache.clone(),
            origin.clone(),
        );
        let mut request = request(axum::http::Method::GET, "/container/blob");
        request
            .headers_mut()
            .insert(header::RANGE, HeaderValue::from_static("bytes=1-4"));
        request
            .headers_mut()
            .insert(header::IF_MATCH, HeaderValue::from_static("\"v1\""));
        let response = adapter.handle(request, context()).await;
        assert_eq!(response.response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.response.headers()[header::CONTENT_LENGTH], "4");
        assert_eq!(
            response.response.headers()[header::CONTENT_RANGE],
            "bytes 1-4/6"
        );
        assert_eq!(
            response.response.headers()[header::CONTENT_TYPE],
            "text/plain"
        );
        assert_eq!(response.route, GatewayRoute::Cache);
        assert!(origin.get_request.lock().unwrap().is_none());
        assert_eq!(
            cache
                .request
                .lock()
                .unwrap()
                .as_ref()
                .map(|value| (value.2, value.3)),
            Some((1, 4))
        );
        assert_eq!(
            origin.head_conditions.lock().unwrap().as_slice(),
            &[("if-match".to_string(), "\"v1\"".to_string())]
        );
        let body = to_bytes(response.response.into_body(), 16).await.unwrap();
        assert_eq!(&body[..], b"bcde");
    }

    #[tokio::test]
    async fn infrastructure_failure_falls_back_to_demand_driven_origin_stream() {
        let cache = MockCache::unavailable();
        let origin = MockOrigin::object(&[b"abc", b"def"]);
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            cache,
            origin.clone(),
        );
        let response = adapter
            .handle(
                request(axum::http::Method::GET, "/container/blob"),
                context(),
            )
            .await;
        assert_eq!(response.outcome, GatewayOutcome::Fallback);
        assert_eq!(origin.get_polls.load(Ordering::SeqCst), 0);
        assert_eq!(
            response.response.headers()[header::CONTENT_TYPE],
            "application/custom"
        );
        let mut body = response.response.into_body().into_data_stream();
        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(origin.get_polls.load(Ordering::SeqCst), 1);
        drop(body);
        assert!(origin.get_dropped.load(Ordering::SeqCst));
        let request = origin.get_request.lock().unwrap();
        let (_, conditions) = request.as_ref().unwrap();
        assert!(conditions
            .iter()
            .any(|(name, value)| name == "if-match" && value == "\"v1\""));
    }

    #[tokio::test]
    async fn head_preserves_content_length_and_list_forwards_pagination() {
        let cache = MockCache::data(&[]);
        let origin = MockOrigin::object(&[]);
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            cache,
            origin.clone(),
        );
        let head = adapter
            .handle(
                request(axum::http::Method::HEAD, "/container/blob"),
                context(),
            )
            .await;
        assert_eq!(head.response.headers()[header::CONTENT_LENGTH], "6");
        assert_eq!(
            head.response.headers()["x-ms-request-id"],
            "0011223344556677"
        );

        let list = adapter
            .handle(
                request(
                    axum::http::Method::GET,
                    "/container?restype=container&comp=list&prefix=a%2Fb&delimiter=%2F&marker=next&maxresults=7",
                ),
                context(),
            )
            .await;
        assert_eq!(list.response.status(), StatusCode::OK);
        assert_eq!(
            *origin.list_request.lock().unwrap(),
            Some((
                "container".into(),
                "a/b".into(),
                Some("/".into()),
                Some("next".into()),
                7,
            ))
        );
    }

    #[tokio::test]
    async fn invalid_range_is_an_azure_xml_error_without_origin_get() {
        let cache = MockCache::data(&[]);
        let origin = MockOrigin::object(&[]);
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            cache,
            origin.clone(),
        );
        let mut request = request(axum::http::Method::GET, "/container/blob");
        request
            .headers_mut()
            .insert(header::RANGE, HeaderValue::from_static("bytes=9-"));
        let response = adapter.handle(request, context()).await;
        assert_eq!(
            response.response.status(),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(
            response.response.headers()["x-ms-error-code"],
            "InvalidRange"
        );
        assert_eq!(
            response.response.headers()[header::CONTENT_RANGE],
            "bytes */6"
        );
        let body = to_bytes(response.response.into_body(), 4096).await.unwrap();
        assert!(body.windows(20).any(|part| part == b"<Code>InvalidRange</"));
        assert!(origin.get_request.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn ranged_origin_read_rejects_a_whole_object_response() {
        let cache = MockCache::data(&[]);
        let origin = MockOrigin::object(&[b"abcdef"]);
        let mut config = AzureAdapterConfig::public_cloud("acct");
        config.default_route = GatewayRoute::Origin;
        let adapter = adapter(config, cache, origin);
        let mut request = request(axum::http::Method::GET, "/container/blob");
        request
            .headers_mut()
            .insert(header::RANGE, HeaderValue::from_static("bytes=1-3"));

        let response = adapter.handle(request, context()).await;

        assert_eq!(
            response.response.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(response.outcome, GatewayOutcome::Failed);
    }
}

#[async_trait]
impl GatewayAdapter for AzureBlobAdapter {
    fn protocol(&self) -> ProviderProtocol {
        ProviderProtocol::Azure
    }

    async fn handle(&self, request: Request, context: GatewayRequestContext) -> GatewayResponse {
        match self.handle_request(request, &context).await {
            Ok(response) => response,
            Err(error) => error_response(error, &context.request_id),
        }
    }
}

impl AzureBlobAdapter {
    async fn handle_request(
        &self,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        let target = parse_target(&request, &self.config)?;
        let query = AzureQuery::parse(request.uri().query())?;
        if request.method() == axum::http::Method::GET && target.blob.is_none() && query.is_list() {
            return self.list(target.container, query, context).await;
        }
        let blob = target.blob.ok_or_else(|| {
            AzureRequestError::invalid("InvalidUri", "request does not identify a blob")
        })?;
        let object = ObjectId::new(Backend::Azure, target.container, blob);
        match *request.method() {
            axum::http::Method::HEAD => self.head(object, request.headers(), context).await,
            axum::http::Method::GET => self.get(object, request.headers(), context).await,
            _ => Err(AzureRequestError {
                status: StatusCode::METHOD_NOT_ALLOWED,
                code: "UnsupportedHttpVerb",
                message: "the requested HTTP method is not supported".into(),
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
    ) -> Result<GatewayResponse, AzureRequestError> {
        let conditions = conditional_headers(headers)?;
        let response = self
            .origin
            .head(&object, &conditions)
            .await
            .map_err(AzureRequestError::origin_unavailable)?;
        Ok(raw_buffered_response(
            response,
            GatewayOperation::Stat,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn list(
        &self,
        container: String,
        query: AzureQuery,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        let max_results = query.max_results()?;
        let response = self
            .origin
            .list(
                &container,
                query.prefix.as_deref().unwrap_or(""),
                query.delimiter.as_deref(),
                query.marker.as_deref(),
                max_results,
            )
            .await
            .map_err(AzureRequestError::origin_unavailable)?;
        Ok(raw_buffered_response(
            response,
            GatewayOperation::List,
            GatewayRoute::Origin,
            Some(GatewayTarget::Namespace {
                backend: Backend::Azure,
                namespace: container,
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
    ) -> Result<GatewayResponse, AzureRequestError> {
        let conditions = conditional_headers(headers)?;
        let metadata = self
            .origin
            .head(&object, &conditions)
            .await
            .map_err(AzureRequestError::origin_unavailable)?;
        if !(200..300).contains(&metadata.status) {
            return Ok(raw_buffered_response(
                metadata,
                GatewayOperation::Read,
                GatewayRoute::Origin,
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
        if !if_range_matches(headers, &metadata, &etag)? {
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

        let now_ms = unix_millis();
        let mut cache = match self.cache.stream(AzureCacheRequest {
            object: &object,
            version: &version,
            object_size: size,
            offset,
            len,
            block_size: self.config.block_size,
            chunk_size: self.config.transfer_chunk_bytes,
            now_ms,
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
        let first = cache.next().await;
        match first {
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
            None => Err(AzureRequestError::internal(
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
    ) -> Result<GatewayResponse, AzureRequestError> {
        let response = self
            .origin
            .get(&object, range, &conditions)
            .await
            .map_err(AzureRequestError::origin_unavailable)?;
        if !response.is_success() {
            return raw_streaming_response(
                response,
                GatewayOperation::Read,
                GatewayRoute::Origin,
                Some(GatewayTarget::Object(object)),
                context,
            )
            .await;
        }
        let response_status = response.status;
        if range.is_some() && response_status != 206 {
            return Err(AzureRequestError::internal(format!(
                "origin returned HTTP {response_status} for a ranged GET"
            )));
        }
        if range.is_none() && response_status != 200 {
            return Err(AzureRequestError::internal(format!(
                "origin returned HTTP {response_status} for a whole GET"
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
