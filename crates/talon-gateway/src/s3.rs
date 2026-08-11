//! Amazon S3-compatible read adapter.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use talon_backend::{
    HttpRequestBody, HttpResponse, HttpStreamResponse, Method, S3Backend, S3MultipartRequest,
    S3PresignedQuery,
};
use talon_cache_client::{BlockReader, CacheReadError, FileView, DEFAULT_TRANSFER_CHUNK_BYTES};
use talon_core::{Backend, ObjectId, Version};
use tokio::io::AsyncWriteExt;

use crate::{
    AuthenticatedPrincipal, AuthorizationPolicy, EffectiveDecision, FailureReason, GatewayAccess,
    GatewayAccessRequirement, GatewayAdapter, GatewayOperation, GatewayOutcome,
    GatewayRequestContext, GatewayResponse, GatewayRoute, GatewayTarget, ProviderProtocol,
    S3_CACHE_MARK_HEADER,
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
    /// Region clients must use in their SigV4 credential scope. Returned by
    /// `GetBucketLocation` so SDKs resolve the gateway's signing region, which
    /// can differ from the origin bucket's real region.
    pub region: String,
    /// Whether incoming requests use `/bucket/key` paths.
    pub path_style: bool,
    /// Talon cache block size.
    pub block_size: u32,
    /// Maximum worker response chunk passed to one HTTP frame.
    pub transfer_chunk_bytes: u32,
    /// Route used until signed cache marks are implemented by #441.
    pub default_route: GatewayRoute,
    /// Maximum active multipart uploads tracked by one gateway process.
    pub max_multipart_uploads: usize,
    /// Lifetime of an inactive multipart binding.
    pub multipart_state_ttl: Duration,
}

impl S3AdapterConfig {
    /// AWS virtual-host-style endpoint for one region.
    pub fn aws(region: impl AsRef<str>) -> Self {
        Self {
            endpoint_suffix: format!("s3.{}.amazonaws.com", region.as_ref()),
            region: region.as_ref().to_string(),
            path_style: false,
            block_size: 256 * 1024 * 1024,
            transfer_chunk_bytes: DEFAULT_TRANSFER_CHUNK_BYTES,
            default_route: GatewayRoute::Cache,
            max_multipart_uploads: 1024,
            multipart_state_ttl: Duration::from_secs(24 * 60 * 60),
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
        if self.endpoint_suffix.is_empty()
            || self.region.is_empty()
            || self.block_size == 0
            || self.transfer_chunk_bytes == 0
            || self.max_multipart_uploads == 0
            || self.multipart_state_ttl.is_zero()
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

    /// Drop local placement knowledge after an origin mutation commits.
    fn invalidate_object(&self, _object: &ObjectId) -> usize {
        0
    }
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

    fn invalidate_object(&self, object: &ObjectId) -> usize {
        BlockReader::invalidate_object(self, object)
    }
}

/// Origin execution seam. Service-auth methods and explicit request-scoped
/// presigned methods are separate so one can never silently substitute for the
/// other.
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

    async fn head_bucket(&self, _bucket: &str) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement HeadBucket".into())
    }

    async fn list_v1(
        &self,
        _bucket: &str,
        _prefix: &str,
        _delimiter: Option<&str>,
        _marker: Option<&str>,
        _max_keys: u32,
        _encoding_type: Option<&str>,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement ListObjects V1".into())
    }

    async fn put_body(
        &self,
        _object: &ObjectId,
        _headers: &[(String, String)],
        _body: HttpRequestBody,
        _len: u64,
        _payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement PUT".into())
    }

    async fn put_file(
        &self,
        _object: &ObjectId,
        _headers: &[(String, String)],
        _path: &std::path::Path,
        _len: u64,
        _payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement file PUT".into())
    }

    async fn copy(
        &self,
        _object: &ObjectId,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement CopyObject".into())
    }

    async fn delete(
        &self,
        _object: &ObjectId,
        _conditions: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement DELETE".into())
    }

    async fn delete_objects(
        &self,
        _bucket: &str,
        _headers: &[(String, String)],
        _body: Bytes,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement DeleteObjects".into())
    }

    async fn multipart(&self, _request: S3MultipartRequest<'_>) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement multipart requests".into())
    }

    async fn multipart_body(
        &self,
        _request: S3MultipartRequest<'_>,
        _body: HttpRequestBody,
        _len: u64,
        _payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement multipart request bodies".into())
    }

    async fn multipart_file(
        &self,
        _request: S3MultipartRequest<'_>,
        _path: &std::path::Path,
        _len: u64,
        _payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement multipart file bodies".into())
    }

    async fn presigned_object(
        &self,
        _method: Method,
        _object: &ObjectId,
        _credential: &S3PresignedQuery,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement presigned requests".into())
    }

    async fn presigned_object_stream(
        &self,
        _method: Method,
        _object: &ObjectId,
        _credential: &S3PresignedQuery,
        _headers: &[(String, String)],
    ) -> Result<HttpStreamResponse, String> {
        Err("S3 origin does not implement streaming presigned requests".into())
    }

    async fn presigned_object_body(
        &self,
        _method: Method,
        _object: &ObjectId,
        _credential: &S3PresignedQuery,
        _headers: &[(String, String)],
        _body: HttpRequestBody,
        _len: u64,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement presigned request bodies".into())
    }

    async fn presigned_object_file(
        &self,
        _method: Method,
        _object: &ObjectId,
        _credential: &S3PresignedQuery,
        _headers: &[(String, String)],
        _path: &std::path::Path,
        _len: u64,
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement presigned request files".into())
    }

    async fn presigned_bucket(
        &self,
        _method: Method,
        _bucket: &str,
        _credential: &S3PresignedQuery,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("S3 origin does not implement bucket presigned requests".into())
    }
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

    async fn head_bucket(&self, bucket: &str) -> Result<HttpResponse, String> {
        self.execute_head_bucket_raw(bucket).await
    }

    async fn list_v1(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_keys: u32,
        encoding_type: Option<&str>,
    ) -> Result<HttpResponse, String> {
        self.execute_list_v1_raw(bucket, prefix, delimiter, marker, max_keys, encoding_type)
            .await
    }

    async fn put_body(
        &self,
        object: &ObjectId,
        headers: &[(String, String)],
        body: HttpRequestBody,
        len: u64,
        payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        self.execute_put_body_raw(object, headers, body, len, payload_hash)
            .await
    }

    async fn put_file(
        &self,
        object: &ObjectId,
        headers: &[(String, String)],
        path: &std::path::Path,
        len: u64,
        payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        self.execute_put_file_raw(object, headers, path, len, payload_hash)
            .await
    }

    async fn copy(
        &self,
        object: &ObjectId,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_copy_raw(object, headers).await
    }

    async fn delete(
        &self,
        object: &ObjectId,
        conditions: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_delete_raw(object, conditions).await
    }

    async fn delete_objects(
        &self,
        bucket: &str,
        headers: &[(String, String)],
        body: Bytes,
    ) -> Result<HttpResponse, String> {
        self.execute_delete_objects_raw(bucket, headers, body).await
    }

    async fn multipart(&self, request: S3MultipartRequest<'_>) -> Result<HttpResponse, String> {
        self.execute_multipart_raw(request).await
    }

    async fn multipart_body(
        &self,
        request: S3MultipartRequest<'_>,
        body: HttpRequestBody,
        len: u64,
        payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        self.execute_multipart_body_raw(request, body, len, payload_hash)
            .await
    }

    async fn multipart_file(
        &self,
        request: S3MultipartRequest<'_>,
        path: &std::path::Path,
        len: u64,
        payload_hash: &str,
    ) -> Result<HttpResponse, String> {
        self.execute_multipart_file_raw(request, path, len, payload_hash)
            .await
    }

    async fn presigned_object(
        &self,
        method: Method,
        object: &ObjectId,
        credential: &S3PresignedQuery,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_presigned_raw(method, object, credential, headers)
            .await
    }

    async fn presigned_object_stream(
        &self,
        method: Method,
        object: &ObjectId,
        credential: &S3PresignedQuery,
        headers: &[(String, String)],
    ) -> Result<HttpStreamResponse, String> {
        self.execute_presigned_stream_raw(method, object, credential, headers)
            .await
    }

    async fn presigned_object_body(
        &self,
        method: Method,
        object: &ObjectId,
        credential: &S3PresignedQuery,
        headers: &[(String, String)],
        body: HttpRequestBody,
        len: u64,
    ) -> Result<HttpResponse, String> {
        self.execute_presigned_body_raw(method, object, credential, headers, body, len)
            .await
    }

    async fn presigned_object_file(
        &self,
        method: Method,
        object: &ObjectId,
        credential: &S3PresignedQuery,
        headers: &[(String, String)],
        path: &std::path::Path,
        len: u64,
    ) -> Result<HttpResponse, String> {
        self.execute_presigned_file_raw(method, object, credential, headers, path, len)
            .await
    }

    async fn presigned_bucket(
        &self,
        method: Method,
        bucket: &str,
        credential: &S3PresignedQuery,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_presigned_bucket_raw(method, bucket, credential, headers)
            .await
    }
}

/// S3 protocol adapter over Talon and a scoped origin identity.
pub struct S3Adapter {
    config: S3AdapterConfig,
    cache: Arc<dyn S3Cache>,
    origin: Arc<dyn S3Origin>,
    uploads: MultipartRegistry,
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
            uploads: MultipartRegistry::new(
                config.max_multipart_uploads,
                config.multipart_state_ttl,
            ),
            config,
            cache,
            origin,
        })
    }
}

#[derive(Clone)]
struct MultipartBinding {
    object: ObjectId,
    principal: AuthenticatedPrincipal,
    decision: EffectiveDecision,
    touched: Instant,
}

#[derive(Default)]
struct MultipartRegistryState {
    uploads: HashMap<String, MultipartBinding>,
    reservations: usize,
}

struct MultipartRegistry {
    state: Mutex<MultipartRegistryState>,
    max_uploads: usize,
    ttl: Duration,
}

impl MultipartRegistry {
    fn new(max_uploads: usize, ttl: Duration) -> Self {
        Self {
            state: Mutex::new(MultipartRegistryState::default()),
            max_uploads,
            ttl,
        }
    }

    fn reserve(&self, now: Instant) -> bool {
        let mut state = self.state.lock().unwrap();
        self.prune_locked(&mut state, now);
        if state.uploads.len().saturating_add(state.reservations) >= self.max_uploads {
            return false;
        }
        state.reservations += 1;
        true
    }

    fn cancel_reservation(&self) {
        let mut state = self.state.lock().unwrap();
        state.reservations = state.reservations.saturating_sub(1);
    }

    fn publish(&self, upload_id: String, binding: MultipartBinding) -> bool {
        let mut state = self.state.lock().unwrap();
        state.reservations = state.reservations.saturating_sub(1);
        if state.uploads.contains_key(&upload_id) {
            return false;
        }
        state.uploads.insert(upload_id, binding);
        true
    }

    fn authorize(
        &self,
        upload_id: &str,
        object: &ObjectId,
        principal: &AuthenticatedPrincipal,
        decision: EffectiveDecision,
        now: Instant,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        self.prune_locked(&mut state, now);
        let Some(binding) = state.uploads.get_mut(upload_id) else {
            return false;
        };
        if binding.object != *object
            || binding.principal != *principal
            || binding.decision != decision
        {
            return false;
        }
        binding.touched = now;
        true
    }

    fn remove(&self, upload_id: &str) {
        self.state.lock().unwrap().uploads.remove(upload_id);
    }

    fn prune_locked(&self, state: &mut MultipartRegistryState, now: Instant) {
        state
            .uploads
            .retain(|_, binding| now.saturating_duration_since(binding.touched) <= self.ttl);
    }
}

#[derive(Debug)]
struct ParsedTarget {
    bucket: String,
    key: Option<String>,
}

/// Bucket-level probes that carry no object key.
#[derive(Debug, PartialEq, Eq)]
enum BucketProbe {
    /// `HEAD /<bucket>` existence check, passed through to the origin.
    Head,
    /// `GET /<bucket>?location` signing-region query, answered locally.
    Location,
}

/// A bucket-level `GET` is a V1 listing only when its query carries nothing
/// but listing parameters. Known sub-resources (`uploads`), multipart
/// parameters, unrecognized query keys (`versions`, `acl`, ...), and
/// unsupported `list-type` values name different S3 operations, so they are
/// rejected rather than approximated by a listing.
fn require_v1_list_shape(query: &S3Query) -> Result<(), S3RequestError> {
    if query.list_type.is_some() {
        return Err(S3RequestError::invalid(
            "InvalidArgument",
            "list-type must be 2",
        ));
    }
    if query.uploads.is_some()
        || query.upload_id.is_some()
        || query.part_number.is_some()
        || query.part_number_marker.is_some()
        || query.max_parts.is_some()
        || query.delete.is_some()
        || query.unknown.is_some()
    {
        return Err(S3RequestError::invalid(
            "NotImplemented",
            "bucket sub-resource requests are not supported",
        ));
    }
    Ok(())
}

/// A bucket-level `POST` is a DeleteObjects request only when its query is
/// exactly the bare `delete` sub-resource. Any other parameter names a
/// different operation, so the combination is rejected rather than guessed.
fn require_delete_objects_shape(query: &S3Query) -> Result<(), S3RequestError> {
    let bare_delete = query.delete.as_deref() == Some("")
        && query.list_type.is_none()
        && query.prefix.is_none()
        && query.delimiter.is_none()
        && query.continuation_token.is_none()
        && query.max_keys.is_none()
        && query.encoding_type.is_none()
        && query.uploads.is_none()
        && query.upload_id.is_none()
        && query.part_number.is_none()
        && query.part_number_marker.is_none()
        && query.max_parts.is_none()
        && query.location.is_none()
        && query.marker.is_none()
        && query.unknown.is_none();
    if !bare_delete {
        return Err(S3RequestError::invalid(
            "InvalidRequest",
            "invalid DeleteObjects request",
        ));
    }
    Ok(())
}

/// The S3 DeleteObjects limit on objects per request.
const MAX_DELETE_OBJECTS_KEYS: usize = 1000;

/// Ceiling on a DeleteObjects body, which is the one request body the gateway
/// must hold in memory: it is parsed to authorize each key and then forwarded
/// unchanged, so it can neither stream nor spool. The largest legitimate
/// document — 1000 keys of 1024 bytes with every byte entity-escaped, plus
/// framing — stays under this, while the limit keeps the path independent of
/// the object-upload body cap an operator may raise for large writes.
const MAX_DELETE_OBJECTS_BODY_BYTES: u64 = 8 * 1024 * 1024;

fn malformed_delete_xml(message: &'static str) -> S3RequestError {
    S3RequestError::invalid("MalformedXML", message)
}

/// Parse a DeleteObjects `<Delete>` document into its object keys.
///
/// The gateway authorizes exactly the keys it parses and then forwards the
/// body unchanged, so any construct this parser and the origin's XML parser
/// could interpret differently — doctypes, comments, CDATA, processing
/// instructions, unknown elements, unresolved entities — fails closed
/// instead of being skipped.
fn parse_delete_objects(body: &[u8]) -> Result<Vec<String>, S3RequestError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| malformed_delete_xml("the request body is not valid UTF-8"))?;
    let mut input = skip_xml_whitespace(text.trim_start_matches('\u{feff}'));
    if let Some(rest) = input.strip_prefix("<?xml") {
        let end = rest
            .find("?>")
            .ok_or_else(|| malformed_delete_xml("unterminated XML declaration"))?;
        input = skip_xml_whitespace(&rest[end + 2..]);
    }
    let mut rest = expect_open_tag(input, "Delete")?;
    let mut keys = Vec::new();
    loop {
        rest = skip_xml_whitespace(rest);
        if let Some(after) = rest.strip_prefix("</Delete>") {
            if !skip_xml_whitespace(after).is_empty() {
                return Err(malformed_delete_xml("content after the Delete element"));
            }
            break;
        }
        if peek_open_tag(rest, "Object") {
            let (key, after) = parse_delete_object_entry(rest)?;
            if keys.len() == MAX_DELETE_OBJECTS_KEYS {
                return Err(malformed_delete_xml(
                    "DeleteObjects accepts at most 1000 objects",
                ));
            }
            keys.push(key);
            rest = after;
        } else if peek_open_tag(rest, "Quiet") {
            // The flag never changes which keys are authorized, so accept the
            // whole `xs:boolean` lexical space that origin parsers accept
            // rather than rejecting a batch the origin would have run.
            let (value, after) = parse_text_element(rest, "Quiet")?;
            if !matches!(
                value.trim_matches(is_xml_whitespace),
                "true" | "false" | "1" | "0"
            ) {
                return Err(malformed_delete_xml("Quiet must be a boolean"));
            }
            rest = after;
        } else {
            return Err(malformed_delete_xml("unsupported content in Delete"));
        }
    }
    if keys.is_empty() {
        return Err(malformed_delete_xml("Delete names no objects"));
    }
    Ok(keys)
}

fn parse_delete_object_entry(input: &str) -> Result<(String, &str), S3RequestError> {
    let mut rest = expect_open_tag(input, "Object")?;
    let mut key = None;
    loop {
        rest = skip_xml_whitespace(rest);
        if let Some(after) = rest.strip_prefix("</Object>") {
            let key = key.ok_or_else(|| malformed_delete_xml("Object names no Key"))?;
            return Ok((key, after));
        }
        if peek_open_tag(rest, "Key") {
            if key.is_some() {
                return Err(malformed_delete_xml("Object names more than one Key"));
            }
            let (value, after) = parse_text_element(rest, "Key")?;
            if value.is_empty() {
                return Err(malformed_delete_xml("Object names an empty Key"));
            }
            if value.len() > 1024 {
                return Err(S3RequestError::invalid(
                    "KeyTooLongError",
                    "the object key is longer than 1024 bytes",
                ));
            }
            key = Some(value);
            rest = after;
        } else if peek_open_tag(rest, "VersionId")
            || peek_open_tag(rest, "ETag")
            || peek_open_tag(rest, "LastModifiedTime")
            || peek_open_tag(rest, "Size")
        {
            return Err(S3RequestError::invalid(
                "NotImplemented",
                "versioned or conditional DeleteObjects entries are not supported",
            ));
        } else {
            return Err(malformed_delete_xml("unsupported content in Object"));
        }
    }
}

/// Consume `<tag ...attributes>` and return the remainder. Attribute values
/// are tokenized with their quotes so a quoted `>` cannot truncate the tag,
/// which would make this parser and the origin's disagree on the content.
fn expect_open_tag<'a>(input: &'a str, tag: &str) -> Result<&'a str, S3RequestError> {
    let rest = input
        .strip_prefix('<')
        .and_then(|rest| rest.strip_prefix(tag))
        .ok_or_else(|| malformed_delete_xml("unexpected element"))?;
    let bytes = rest.as_bytes();
    match bytes.first() {
        Some(b'>') => return Ok(&rest[1..]),
        Some(byte) if is_xml_whitespace_byte(*byte) => {}
        _ => return Err(malformed_delete_xml("unexpected element")),
    }
    let mut index = 0;
    loop {
        while index < bytes.len() && is_xml_whitespace_byte(bytes[index]) {
            index += 1;
        }
        match bytes.get(index) {
            None => return Err(malformed_delete_xml("unterminated tag")),
            Some(b'>') => return Ok(&rest[index + 1..]),
            Some(_) => {}
        }
        let name_start = index;
        while index < bytes.len()
            && bytes[index] != b'='
            && bytes[index] != b'>'
            && !is_xml_whitespace_byte(bytes[index])
        {
            index += 1;
        }
        if index == name_start || bytes.get(index) != Some(&b'=') {
            return Err(malformed_delete_xml("malformed attribute"));
        }
        index += 1;
        let quote = match bytes.get(index) {
            Some(quote @ (b'"' | b'\'')) => *quote,
            _ => return Err(malformed_delete_xml("malformed attribute")),
        };
        index += 1;
        while index < bytes.len() && bytes[index] != quote {
            index += 1;
        }
        if index >= bytes.len() {
            return Err(malformed_delete_xml("malformed attribute"));
        }
        index += 1;
    }
}

fn peek_open_tag(input: &str, tag: &str) -> bool {
    input
        .strip_prefix('<')
        .and_then(|rest| rest.strip_prefix(tag))
        .is_some_and(|rest| rest.starts_with('>') || rest.starts_with(is_xml_whitespace))
}

fn parse_text_element<'a>(input: &'a str, tag: &str) -> Result<(String, &'a str), S3RequestError> {
    let rest = expect_open_tag(input, tag)?;
    let end = rest
        .find('<')
        .ok_or_else(|| malformed_delete_xml("unterminated element"))?;
    let text = &rest[..end];
    let close = format!("</{tag}>");
    let rest = rest[end..]
        .strip_prefix(close.as_str())
        .ok_or_else(|| malformed_delete_xml("unexpected markup inside an element"))?;
    Ok((unescape_strict(text)?, rest))
}

/// Decode XML character data accepting only the five predefined entities and
/// well-formed numeric character references. Anything else fails closed so
/// the key text the gateway authorizes can never differ from the origin's
/// decoding of the same body.
fn unescape_strict(text: &str) -> Result<String, S3RequestError> {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find('&') {
        output.push_str(&rest[..index]);
        rest = &rest[index + 1..];
        let end = rest
            .find(';')
            .ok_or_else(|| malformed_delete_xml("unterminated character reference"))?;
        let entity = &rest[..end];
        rest = &rest[end + 1..];
        match entity {
            "amp" => output.push('&'),
            "lt" => output.push('<'),
            "gt" => output.push('>'),
            "quot" => output.push('"'),
            "apos" => output.push('\''),
            _ => {
                // Rust's integer parsers accept a leading `+`, which the XML
                // `CharRef` production does not, so the digit run is checked
                // before parsing rather than after.
                let (digits, radix) = match entity
                    .strip_prefix("#x")
                    .or_else(|| entity.strip_prefix("#X"))
                {
                    Some(digits) => (digits, 16),
                    None => (
                        entity.strip_prefix('#').ok_or_else(|| {
                            malformed_delete_xml("unsupported character reference")
                        })?,
                        10,
                    ),
                };
                let digits_are_valid = !digits.is_empty()
                    && digits.bytes().all(|digit| match radix {
                        16 => digit.is_ascii_hexdigit(),
                        _ => digit.is_ascii_digit(),
                    });
                if !digits_are_valid {
                    return Err(malformed_delete_xml("unsupported character reference"));
                }
                let value = u32::from_str_radix(digits, radix)
                    .map_err(|_| malformed_delete_xml("unsupported character reference"))?;
                let value = char::from_u32(value)
                    .filter(|value| *value != '\0')
                    .ok_or_else(|| malformed_delete_xml("unsupported character reference"))?;
                output.push(value);
            }
        }
    }
    output.push_str(rest);
    Ok(output)
}

fn skip_xml_whitespace(input: &str) -> &str {
    input.trim_start_matches(is_xml_whitespace)
}

/// The XML 1.0 `S` production. Deliberately narrower than Rust's ASCII
/// whitespace, which also covers form feed and vertical tab — characters an
/// origin parser rejects outright rather than treating as separators.
fn is_xml_whitespace(value: char) -> bool {
    matches!(value, ' ' | '\t' | '\r' | '\n')
}

fn is_xml_whitespace_byte(value: u8) -> bool {
    matches!(value, b' ' | b'\t' | b'\r' | b'\n')
}

fn bucket_probe(
    method: &axum::http::Method,
    target: &ParsedTarget,
    query: &S3Query,
) -> Option<BucketProbe> {
    if target.key.is_some() {
        return None;
    }
    match *method {
        axum::http::Method::HEAD => Some(BucketProbe::Head),
        axum::http::Method::GET if query.location.is_some() => Some(BucketProbe::Location),
        _ => None,
    }
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
    uploads: Option<String>,
    upload_id: Option<String>,
    part_number: Option<String>,
    part_number_marker: Option<String>,
    max_parts: Option<String>,
    location: Option<String>,
    marker: Option<String>,
    delete: Option<String>,
    /// First query key outside the recognized set, kept so bucket-level
    /// requests can reject unknown sub-resources instead of approximating
    /// them with a listing.
    unknown: Option<String>,
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
                "uploads" => set_query_value(&mut parsed.uploads, value.into_owned(), "uploads")?,
                "uploadId" => {
                    set_query_value(&mut parsed.upload_id, value.into_owned(), "uploadId")?
                }
                "partNumber" => {
                    set_query_value(&mut parsed.part_number, value.into_owned(), "partNumber")?
                }
                "part-number-marker" => set_query_value(
                    &mut parsed.part_number_marker,
                    value.into_owned(),
                    "part-number-marker",
                )?,
                "max-parts" => {
                    set_query_value(&mut parsed.max_parts, value.into_owned(), "max-parts")?
                }
                "location" => {
                    set_query_value(&mut parsed.location, value.into_owned(), "location")?
                }
                "marker" => set_query_value(&mut parsed.marker, value.into_owned(), "marker")?,
                "delete" => set_query_value(&mut parsed.delete, value.into_owned(), "delete")?,
                other => {
                    if parsed.unknown.is_none() {
                        parsed.unknown = Some(other.to_string());
                    }
                }
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

    fn multipart_operation(
        &self,
        method: &axum::http::Method,
        copy: bool,
    ) -> Result<Option<MultipartOperation>, S3RequestError> {
        let multipart = self.uploads.is_some()
            || self.upload_id.is_some()
            || self.part_number.is_some()
            || self.part_number_marker.is_some()
            || self.max_parts.is_some();
        if !multipart {
            return Ok(None);
        }
        if let Some(value) = self.uploads.as_deref() {
            if method == axum::http::Method::POST
                && value.is_empty()
                && self.upload_id.is_none()
                && self.part_number.is_none()
                && self.part_number_marker.is_none()
                && self.max_parts.is_none()
            {
                return Ok(Some(MultipartOperation::Create));
            }
            return Err(S3RequestError::invalid(
                "InvalidRequest",
                "invalid CreateMultipartUpload request",
            ));
        }
        let upload_id = self
            .upload_id
            .as_deref()
            .filter(|value| !value.is_empty() && value.len() <= 2048)
            .ok_or_else(|| S3RequestError::invalid("InvalidArgument", "uploadId is invalid"))?
            .to_string();
        match *method {
            axum::http::Method::PUT => {
                if self.part_number_marker.is_some() || self.max_parts.is_some() {
                    return Err(S3RequestError::invalid(
                        "InvalidArgument",
                        "invalid UploadPart request",
                    ));
                }
                let part_number = parse_part_number(self.part_number.as_deref())?;
                Ok(Some(MultipartOperation::UploadPart {
                    upload_id,
                    part_number,
                    copy,
                }))
            }
            axum::http::Method::GET if self.part_number.is_none() => {
                let part_number_marker = self
                    .part_number_marker
                    .as_deref()
                    .map(parse_part_marker)
                    .transpose()?;
                let max_parts = self
                    .max_parts
                    .as_deref()
                    .map(parse_max_parts)
                    .transpose()?
                    .unwrap_or(1000);
                Ok(Some(MultipartOperation::ListParts {
                    upload_id,
                    part_number_marker,
                    max_parts,
                }))
            }
            axum::http::Method::POST
                if self.part_number.is_none()
                    && self.part_number_marker.is_none()
                    && self.max_parts.is_none() =>
            {
                Ok(Some(MultipartOperation::Complete { upload_id }))
            }
            axum::http::Method::DELETE
                if self.part_number.is_none()
                    && self.part_number_marker.is_none()
                    && self.max_parts.is_none() =>
            {
                Ok(Some(MultipartOperation::Abort { upload_id }))
            }
            _ => Err(S3RequestError::invalid(
                "InvalidRequest",
                "unsupported multipart request shape",
            )),
        }
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

fn set_query_value(
    slot: &mut Option<String>,
    value: String,
    name: &str,
) -> Result<(), S3RequestError> {
    if slot.replace(value).is_some() {
        return Err(S3RequestError::invalid(
            "InvalidArgument",
            format!("{name} must occur exactly once"),
        ));
    }
    Ok(())
}

fn parse_part_number(value: Option<&str>) -> Result<u16, S3RequestError> {
    value
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|value| (1..=10_000).contains(value))
        .ok_or_else(|| {
            S3RequestError::invalid("InvalidArgument", "partNumber must be between 1 and 10000")
        })
}

fn parse_part_marker(value: &str) -> Result<u16, S3RequestError> {
    value
        .parse::<u16>()
        .ok()
        .filter(|value| *value <= 10_000)
        .ok_or_else(|| S3RequestError::invalid("InvalidArgument", "part-number-marker is invalid"))
}

fn parse_max_parts(value: &str) -> Result<u16, S3RequestError> {
    value
        .parse::<u16>()
        .ok()
        .filter(|value| *value <= 1000)
        .ok_or_else(|| S3RequestError::invalid("InvalidArgument", "max-parts is invalid"))
}

#[derive(Debug)]
enum MultipartOperation {
    Create,
    UploadPart {
        upload_id: String,
        part_number: u16,
        copy: bool,
    },
    ListParts {
        upload_id: String,
        part_number_marker: Option<u16>,
        max_parts: u16,
    },
    Complete {
        upload_id: String,
    },
    Abort {
        upload_id: String,
    },
}

impl MultipartOperation {
    fn upload_id(&self) -> Option<&str> {
        match self {
            Self::Create => None,
            Self::UploadPart { upload_id, .. }
            | Self::ListParts { upload_id, .. }
            | Self::Complete { upload_id }
            | Self::Abort { upload_id } => Some(upload_id),
        }
    }

    fn query(&self) -> String {
        match self {
            Self::Create => "uploads=".into(),
            Self::UploadPart {
                upload_id,
                part_number,
                ..
            } => format!(
                "partNumber={part_number}&uploadId={}",
                encode_query_value(upload_id)
            ),
            Self::ListParts {
                upload_id,
                part_number_marker,
                max_parts,
            } => {
                let mut query = format!("uploadId={}", encode_query_value(upload_id));
                if let Some(marker) = part_number_marker {
                    query.push_str(&format!("&part-number-marker={marker}"));
                }
                query.push_str(&format!("&max-parts={max_parts}"));
                query
            }
            Self::Complete { upload_id } | Self::Abort { upload_id } => {
                format!("uploadId={}", encode_query_value(upload_id))
            }
        }
    }
}

fn encode_query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn multipart_principal(request: &Request) -> Result<AuthenticatedPrincipal, S3RequestError> {
    request
        .extensions()
        .get::<AuthenticatedPrincipal>()
        .cloned()
        .ok_or_else(S3RequestError::access_denied)
}

fn cache_decision(headers: &HeaderMap) -> Result<EffectiveDecision, S3RequestError> {
    let mut values = headers.get_all(S3_CACHE_MARK_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(EffectiveDecision::default());
    };
    if values.next().is_some() {
        return Err(S3RequestError::invalid(
            "InvalidRequest",
            "cache decision must occur exactly once",
        ));
    }
    EffectiveDecision::parse(
        value
            .to_str()
            .map_err(|_| S3RequestError::invalid("InvalidRequest", "cache decision is invalid"))?,
    )
    .map_err(|_| S3RequestError::invalid("InvalidRequest", "cache decision is invalid"))
}

fn single_content_length(headers: &HeaderMap) -> Result<u64, S3RequestError> {
    let values = headers.get_all(header::CONTENT_LENGTH);
    let mut values = values.iter();
    let value = values.next().ok_or_else(|| {
        S3RequestError::invalid("MissingContentLength", "Content-Length is required")
    })?;
    if values.next().is_some() {
        return Err(S3RequestError::invalid(
            "InvalidArgument",
            "Content-Length must occur exactly once",
        ));
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| S3RequestError::invalid("InvalidArgument", "Content-Length is invalid"))
}

fn payload_declaration(request: &Request) -> Result<String, S3RequestError> {
    if let Some(value) = request.headers().get("x-amz-content-sha256") {
        return value
            .to_str()
            .map(str::to_string)
            .map_err(|_| S3RequestError::invalid("InvalidArgument", "payload hash is invalid"));
    }
    for (name, value) in
        url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
    {
        if name.eq_ignore_ascii_case("X-Amz-Content-Sha256") {
            return Ok(value.into_owned());
        }
    }
    Ok("UNSIGNED-PAYLOAD".into())
}

fn mutation_headers(
    headers: &HeaderMap,
    copy: bool,
) -> Result<Vec<(String, String)>, S3RequestError> {
    const EXACT: [&str; 19] = [
        "cache-control",
        "content-disposition",
        "content-encoding",
        "content-language",
        "content-md5",
        "content-type",
        "expires",
        "if-match",
        "if-none-match",
        "x-amz-acl",
        "x-amz-checksum-algorithm",
        "x-amz-copy-source",
        "x-amz-copy-source-range",
        "x-amz-metadata-directive",
        "x-amz-storage-class",
        "x-amz-tagging",
        "x-amz-tagging-directive",
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
    ];
    let mut output = Vec::new();
    for (name, value) in headers {
        let name = name.as_str().to_ascii_lowercase();
        let allowed = EXACT.contains(&name.as_str())
            || name.starts_with("x-amz-meta-")
            || name.starts_with("x-amz-checksum-")
            || (copy && name.starts_with("x-amz-copy-source-if-"));
        if !allowed || (!copy && name.starts_with("x-amz-copy-")) {
            continue;
        }
        if output
            .iter()
            .any(|(existing, _): &(String, String)| existing == &name)
        {
            return Err(S3RequestError::invalid(
                "InvalidArgument",
                format!("{name} must occur exactly once"),
            ));
        }
        let value = value.to_str().map_err(|_| {
            S3RequestError::invalid("InvalidArgument", format!("{name} is invalid"))
        })?;
        output.push((name, value.to_string()));
    }
    Ok(output)
}

fn copy_source(headers: &HeaderMap) -> Result<Option<ObjectId>, S3RequestError> {
    let Some(value) = headers.get("x-amz-copy-source") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| S3RequestError::invalid("InvalidArgument", "copy source is invalid"))?;
    if value.contains('?') {
        return Err(S3RequestError::invalid(
            "NotImplemented",
            "versioned CopyObject is not supported",
        ));
    }
    let decoded = decode_path(value.trim_start_matches('/'))?;
    let (bucket, key) = decoded
        .split_once('/')
        .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
        .ok_or_else(|| S3RequestError::invalid("InvalidArgument", "copy source is invalid"))?;
    Ok(Some(ObjectId::new(Backend::S3, bucket, key)))
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

fn copy_response_failed(response: &HttpResponse) -> bool {
    if !response.is_success() {
        return true;
    }
    std::str::from_utf8(&response.body)
        .ok()
        .and_then(|xml| talon_backend::xml::element(xml, "Error"))
        .is_some()
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
    indeterminate_commit: bool,
}

impl S3RequestError {
    fn invalid(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            message: message.into(),
            failure: FailureReason::InvalidRequest,
            content_range: None,
            indeterminate_commit: false,
        }
    }

    fn invalid_range(size: u64) -> Self {
        Self {
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            code: "InvalidRange",
            message: "The requested range is not satisfiable".into(),
            failure: FailureReason::InvalidRequest,
            content_range: Some(format!("bytes */{size}")),
            indeterminate_commit: false,
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchKey",
            message: "The specified key does not exist".into(),
            failure: FailureReason::NotFound,
            content_range: None,
            indeterminate_commit: false,
        }
    }

    fn no_such_upload() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchUpload",
            message: "The specified multipart upload does not exist".into(),
            failure: FailureReason::NotFound,
            content_range: None,
            indeterminate_commit: false,
        }
    }

    fn access_denied() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "AccessDenied",
            message: "Access Denied".into(),
            failure: FailureReason::Authorization,
            content_range: None,
            indeterminate_commit: false,
        }
    }

    fn multipart_capacity() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServiceUnavailable",
            message: "Multipart upload capacity is temporarily exhausted".into(),
            failure: FailureReason::Internal,
            content_range: None,
            indeterminate_commit: false,
        }
    }

    fn precondition_failed() -> Self {
        Self {
            status: StatusCode::PRECONDITION_FAILED,
            code: "PreconditionFailed",
            message: "At least one precondition failed".into(),
            failure: FailureReason::Precondition,
            content_range: None,
            indeterminate_commit: false,
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
            indeterminate_commit: false,
        }
    }

    fn indeterminate_commit() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "InternalError",
            message: "The mutation result is indeterminate; inspect the object before retrying"
                .into(),
            failure: FailureReason::Origin,
            content_range: None,
            indeterminate_commit: true,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "InternalError",
            message: message.into(),
            failure: FailureReason::Internal,
            content_range: None,
            indeterminate_commit: false,
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
            indeterminate_commit: false,
        },
        CacheReadError::VersionMismatch(message) => S3RequestError {
            status: StatusCode::PRECONDITION_FAILED,
            code: "PreconditionFailed",
            message,
            failure: FailureReason::Precondition,
            content_range: None,
            indeterminate_commit: false,
        },
        CacheReadError::Timeout(message) => S3RequestError {
            status: StatusCode::GATEWAY_TIMEOUT,
            code: "RequestTimeout",
            message,
            failure: FailureReason::Timeout,
            content_range: None,
            indeterminate_commit: false,
        },
        CacheReadError::Unavailable(message) => S3RequestError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServiceUnavailable",
            message,
            failure: FailureReason::CacheUnavailable,
            content_range: None,
            indeterminate_commit: false,
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
    if error.indeterminate_commit {
        response.headers_mut().insert(
            "x-talon-commit-state",
            HeaderValue::from_static("indeterminate"),
        );
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

/// A write body prepared for one origin request.
enum WriteBody {
    /// Streamed to the origin as `UNSIGNED-PAYLOAD` (a plain unsigned body, or
    /// an aws-chunked body already decoded to its payload).
    Stream {
        body: HttpRequestBody,
        len: u64,
        payload_hash: String,
    },
    /// A declared-SHA-256 body spooled and verified before dispatch.
    Spool {
        spool: tempfile::NamedTempFile,
        len: u64,
        hash: String,
    },
}

impl WriteBody {
    fn len(&self) -> u64 {
        match self {
            Self::Stream { len, .. } | Self::Spool { len, .. } => *len,
        }
    }
}

/// Resolve the sanitized origin headers and forwardable body for a PutObject or
/// UploadPart request, covering all three payload declarations: plain
/// `UNSIGNED-PAYLOAD` (streamed), a hex SHA-256 (spooled and verified), and
/// `STREAMING-UNSIGNED-PAYLOAD-TRAILER` (aws-chunked framing decoded to its
/// payload, then streamed as `UNSIGNED-PAYLOAD`).
async fn prepare_write_body(
    request: Request,
) -> Result<(Vec<(String, String)>, WriteBody), S3RequestError> {
    let payload_hash = payload_declaration(&request)?;
    if payload_hash == crate::s3_auth::STREAMING_UNSIGNED_TRAILER {
        let len = decoded_content_length(request.headers())?;
        let algorithm = declared_trailer_algorithm(request.headers())?;
        let headers = strip_aws_chunked_encoding(mutation_headers(request.headers(), false)?);
        let raw = request
            .into_body()
            .into_data_stream()
            .map(|chunk| chunk.map_err(|error| error.to_string()));
        // Decode and verify into a spool: the trailer checksum arrives after
        // the payload, so the whole body is verified before the origin is
        // dispatched. A length or checksum mismatch fails closed here as a 4xx
        // rather than a partial object at the origin.
        let decoded = crate::aws_chunked::decode_aws_chunked(raw, len, algorithm);
        let spool = spool_decoded(decoded, len).await?;
        return Ok((
            headers,
            WriteBody::Spool {
                spool,
                len,
                hash: "UNSIGNED-PAYLOAD".into(),
            },
        ));
    }
    let len = single_content_length(request.headers())?;
    let headers = mutation_headers(request.headers(), false)?;
    if payload_hash == "UNSIGNED-PAYLOAD" {
        let body = request
            .into_body()
            .into_data_stream()
            .map(|chunk| chunk.map_err(|error| error.to_string()));
        Ok((
            headers,
            WriteBody::Stream {
                body: Box::pin(body),
                len,
                payload_hash,
            },
        ))
    } else {
        let body = request.into_body().into_data_stream();
        let (spool, actual_hash) = spool_and_hash(body, len).await?;
        if actual_hash != payload_hash {
            return Err(S3RequestError::invalid(
                "XAmzContentSHA256Mismatch",
                "The provided payload hash does not match the request body",
            ));
        }
        Ok((
            headers,
            WriteBody::Spool {
                spool,
                len,
                hash: actual_hash,
            },
        ))
    }
}

/// Read the authoritative decoded length of an aws-chunked body. It replaces
/// `Content-Length`, which the framing removes, so a missing value is refused
/// rather than guessed, and a duplicated one is refused just like
/// `Content-Length` itself.
fn decoded_content_length(headers: &HeaderMap) -> Result<u64, S3RequestError> {
    let mut values = headers.get_all("x-amz-decoded-content-length").iter();
    let value = values.next().ok_or_else(|| {
        S3RequestError::invalid(
            "MissingContentLength",
            "x-amz-decoded-content-length is required for an aws-chunked body",
        )
    })?;
    if values.next().is_some() {
        return Err(S3RequestError::invalid(
            "InvalidArgument",
            "x-amz-decoded-content-length must occur exactly once",
        ));
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            S3RequestError::invalid("InvalidArgument", "x-amz-decoded-content-length is invalid")
        })
}

/// Resolve the checksum algorithm the client promised in `x-amz-trailer`.
/// `None` means the body carries no trailer checksum. A declared algorithm the
/// gateway cannot compute is rejected before dispatch rather than skipped.
fn declared_trailer_algorithm(
    headers: &HeaderMap,
) -> Result<Option<crate::aws_chunked::ChecksumAlgorithm>, S3RequestError> {
    let Some(value) = headers.get("x-amz-trailer") else {
        return Ok(None);
    };
    let name = value
        .to_str()
        .map_err(|_| S3RequestError::invalid("InvalidArgument", "x-amz-trailer is invalid"))?;
    // aws-chunked declares exactly one trailer for the flexible checksum.
    match crate::aws_chunked::ChecksumAlgorithm::from_trailer_name(name) {
        Some(algorithm) => Ok(Some(algorithm)),
        None => Err(S3RequestError::invalid(
            "InvalidRequest",
            "the declared aws-chunked trailer checksum is not supported",
        )),
    }
}

/// Consume a decoded aws-chunked stream into a spool file, mapping decode and
/// verification failures to S3 client errors so they are rejected before the
/// origin is dispatched.
async fn spool_decoded<S>(
    mut decoded: S,
    expected_len: u64,
) -> Result<tempfile::NamedTempFile, S3RequestError>
where
    S: Stream<Item = Result<Bytes, crate::aws_chunked::ChunkDecodeError>> + Unpin,
{
    use crate::aws_chunked::ChunkDecodeError;
    let spool = tempfile::NamedTempFile::new()
        .map_err(|_| S3RequestError::internal("could not create upload spool"))?;
    let file = spool
        .reopen()
        .map_err(|_| S3RequestError::internal("could not open upload spool"))?;
    let mut file = tokio::fs::File::from_std(file);
    let mut received = 0u64;
    while let Some(chunk) = decoded.next().await {
        let chunk = chunk.map_err(|error| match error {
            ChunkDecodeError::ChecksumMismatch => S3RequestError::invalid(
                "BadDigest",
                "The aws-chunked trailer checksum did not match the payload",
            ),
            ChunkDecodeError::LengthMismatch { .. } | ChunkDecodeError::MissingChecksum => {
                S3RequestError::invalid(
                    "IncompleteBody",
                    "The aws-chunked body did not match its declared length or checksum",
                )
            }
            ChunkDecodeError::Malformed(_) | ChunkDecodeError::Source(_) => {
                S3RequestError::invalid("InvalidArgument", "The aws-chunked body is malformed")
            }
        })?;
        received = received.saturating_add(chunk.len() as u64);
        file.write_all(&chunk)
            .await
            .map_err(|_| S3RequestError::internal("could not spool upload body"))?;
    }
    debug_assert_eq!(
        received, expected_len,
        "decoder enforces the decoded length"
    );
    let _ = expected_len;
    file.flush()
        .await
        .map_err(|_| S3RequestError::internal("could not flush upload spool"))?;
    Ok(spool)
}

/// Remove the `aws-chunked` token from a forwarded `content-encoding` header so
/// the origin is not told the decoded payload is still chunk-framed. Any other
/// encodings (a real object `content-encoding`) are preserved.
fn strip_aws_chunked_encoding(headers: Vec<(String, String)>) -> Vec<(String, String)> {
    headers
        .into_iter()
        .filter_map(|(name, value)| {
            if !name.eq_ignore_ascii_case("content-encoding") {
                return Some((name, value));
            }
            let kept: Vec<&str> = value
                .split(',')
                .map(str::trim)
                .filter(|token| !token.eq_ignore_ascii_case("aws-chunked") && !token.is_empty())
                .collect();
            if kept.is_empty() {
                None
            } else {
                Some((name, kept.join(",")))
            }
        })
        .collect()
}

async fn spool_and_hash<S>(
    mut body: S,
    expected_len: u64,
) -> Result<(tempfile::NamedTempFile, String), S3RequestError>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    let spool = tempfile::NamedTempFile::new()
        .map_err(|_| S3RequestError::internal("could not create upload spool"))?;
    let file = spool
        .reopen()
        .map_err(|_| S3RequestError::internal("could not open upload spool"))?;
    let mut file = tokio::fs::File::from_std(file);
    let mut hasher = Sha256::new();
    let mut received = 0u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| {
            S3RequestError::invalid("IncompleteBody", "The request body could not be read")
        })?;
        received = received.checked_add(chunk.len() as u64).ok_or_else(|| {
            S3RequestError::invalid("InvalidArgument", "request body is too large")
        })?;
        if received > expected_len {
            return Err(S3RequestError::invalid(
                "InvalidArgument",
                "request body exceeds Content-Length",
            ));
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|_| S3RequestError::internal("could not spool upload body"))?;
    }
    if received != expected_len {
        return Err(S3RequestError::invalid(
            "IncompleteBody",
            "request body is shorter than Content-Length",
        ));
    }
    file.flush()
        .await
        .map_err(|_| S3RequestError::internal("could not flush upload spool"))?;
    Ok((spool, format!("{:x}", hasher.finalize())))
}

/// Collect a small request body into memory, enforcing the declared
/// Content-Length exactly. Used for bodies the gateway must parse before
/// dispatch (DeleteObjects); the runtime body limit bounds them from above.
async fn collect_body<S>(mut body: S, expected_len: u64) -> Result<Vec<u8>, S3RequestError>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    let mut collected = Vec::new();
    let mut received = 0u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| {
            S3RequestError::invalid("IncompleteBody", "The request body could not be read")
        })?;
        received = received.checked_add(chunk.len() as u64).ok_or_else(|| {
            S3RequestError::invalid("InvalidArgument", "request body is too large")
        })?;
        if received > expected_len {
            return Err(S3RequestError::invalid(
                "InvalidArgument",
                "request body exceeds Content-Length",
            ));
        }
        collected.extend_from_slice(&chunk);
    }
    if received != expected_len {
        return Err(S3RequestError::invalid(
            "IncompleteBody",
            "request body is shorter than Content-Length",
        ));
    }
    Ok(collected)
}

#[async_trait]
impl GatewayAdapter for S3Adapter {
    fn protocol(&self) -> ProviderProtocol {
        ProviderProtocol::S3
    }

    fn access(
        &self,
        request: &Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayAccess, Box<GatewayResponse>> {
        self.classify_access(request)
            .map_err(|error| Box::new(error_response(error, &context.request_id)))
    }

    async fn handle(&self, request: Request, context: GatewayRequestContext) -> GatewayResponse {
        match self.handle_request(request, &context).await {
            Ok(response) => response,
            Err(error) => error_response(error, &context.request_id),
        }
    }
}

impl S3Adapter {
    fn classify_access(&self, request: &Request) -> Result<GatewayAccess, S3RequestError> {
        let target = parse_target(request, &self.config)?;
        let query = S3Query::parse(request.uri().query())?;
        if request.method() == axum::http::Method::GET && target.key.is_none() && query.is_list_v2()
        {
            return Ok(GatewayAccess {
                operation: GatewayOperation::List,
                provider_account: None,
                target: GatewayTarget::Namespace {
                    backend: Backend::S3,
                    namespace: target.bucket,
                    prefix: query.prefix,
                },
                additional: Vec::new(),
            });
        }
        if bucket_probe(request.method(), &target, &query).is_some() {
            return Ok(GatewayAccess {
                operation: GatewayOperation::Probe,
                provider_account: None,
                target: GatewayTarget::Namespace {
                    backend: Backend::S3,
                    namespace: target.bucket,
                    prefix: None,
                },
                additional: Vec::new(),
            });
        }
        if request.method() == axum::http::Method::POST
            && target.key.is_none()
            && query.delete.is_some()
        {
            require_delete_objects_shape(&query)?;
            return Ok(GatewayAccess {
                operation: GatewayOperation::Delete,
                provider_account: None,
                target: GatewayTarget::NamespaceBodyObjects {
                    backend: Backend::S3,
                    namespace: target.bucket,
                },
                additional: Vec::new(),
            });
        }
        if request.method() == axum::http::Method::GET && target.key.is_none() {
            require_v1_list_shape(&query)?;
            return Ok(GatewayAccess {
                operation: GatewayOperation::List,
                provider_account: None,
                target: GatewayTarget::Namespace {
                    backend: Backend::S3,
                    namespace: target.bucket,
                    prefix: query.prefix,
                },
                additional: Vec::new(),
            });
        }
        let key = target
            .key
            .ok_or_else(|| S3RequestError::invalid("InvalidURI", "request has no object key"))?;
        if query.delete.is_some() {
            return Err(S3RequestError::invalid(
                "InvalidRequest",
                "delete is a bucket-level sub-resource",
            ));
        }
        let multipart = query.multipart_operation(
            request.method(),
            request.headers().contains_key("x-amz-copy-source"),
        )?;
        let operation = match multipart.as_ref() {
            Some(MultipartOperation::ListParts { .. }) => GatewayOperation::List,
            Some(MultipartOperation::Abort { .. }) => GatewayOperation::Delete,
            Some(_) => GatewayOperation::Write,
            None => match *request.method() {
                axum::http::Method::HEAD => GatewayOperation::Stat,
                axum::http::Method::GET => GatewayOperation::Read,
                axum::http::Method::PUT | axum::http::Method::POST => GatewayOperation::Write,
                axum::http::Method::DELETE => GatewayOperation::Delete,
                _ => GatewayOperation::Unsupported,
            },
        };
        let mut access = GatewayAccess {
            operation,
            provider_account: None,
            target: GatewayTarget::Object(ObjectId::new(Backend::S3, target.bucket, key)),
            additional: Vec::new(),
        };
        if request.method() == axum::http::Method::PUT {
            if let Some(source) = copy_source(request.headers())? {
                access.additional.push(GatewayAccessRequirement {
                    operation: GatewayOperation::Read,
                    provider_account: None,
                    target: GatewayTarget::Object(source),
                });
            }
        }
        Ok(access)
    }

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
        match bucket_probe(request.method(), &target, &query) {
            Some(BucketProbe::Head) => return self.head_bucket(target.bucket, context).await,
            Some(BucketProbe::Location) => return Ok(self.bucket_location(target.bucket, context)),
            None => {}
        }
        if request.method() == axum::http::Method::POST
            && target.key.is_none()
            && query.delete.is_some()
        {
            require_delete_objects_shape(&query)?;
            return self.delete_objects(target.bucket, request, context).await;
        }
        if request.method() == axum::http::Method::GET && target.key.is_none() {
            require_v1_list_shape(&query)?;
            return self.list_v1(target.bucket, query, context).await;
        }
        let key = target
            .key
            .ok_or_else(|| S3RequestError::invalid("InvalidURI", "request has no object key"))?;
        if query.delete.is_some() {
            return Err(S3RequestError::invalid(
                "InvalidRequest",
                "delete is a bucket-level sub-resource",
            ));
        }
        let object = ObjectId::new(Backend::S3, target.bucket, key);
        if let Some(operation) = query.multipart_operation(
            request.method(),
            request.headers().contains_key("x-amz-copy-source"),
        )? {
            return self.multipart(object, operation, request, context).await;
        }
        match *request.method() {
            axum::http::Method::HEAD => self.head(object, request.headers(), context).await,
            axum::http::Method::GET => self.get(object, request.headers(), context).await,
            axum::http::Method::PUT => self.put(object, request, context).await,
            axum::http::Method::DELETE => self.delete(object, request.headers(), context).await,
            _ => Err(S3RequestError {
                status: StatusCode::METHOD_NOT_ALLOWED,
                code: "MethodNotAllowed",
                message: "The specified method is not allowed".into(),
                failure: FailureReason::Unsupported,
                content_range: None,
                indeterminate_commit: false,
            }),
        }
    }

    async fn multipart(
        &self,
        object: ObjectId,
        operation: MultipartOperation,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let principal = multipart_principal(&request)?;
        let decision = cache_decision(request.headers())?;
        if matches!(operation, MultipartOperation::Create) {
            return self
                .create_multipart(object, request.headers(), principal, decision, context)
                .await;
        }
        let upload_id = operation
            .upload_id()
            .expect("non-create multipart operation has an upload ID");
        if !self
            .uploads
            .authorize(upload_id, &object, &principal, decision, Instant::now())
        {
            return Err(S3RequestError::no_such_upload());
        }
        match operation {
            MultipartOperation::Create => unreachable!(),
            MultipartOperation::UploadPart {
                part_number,
                copy,
                upload_id,
            } => {
                self.upload_part(object, upload_id, part_number, copy, request, context)
                    .await
            }
            MultipartOperation::ListParts {
                upload_id,
                part_number_marker,
                max_parts,
            } => {
                self.list_parts(object, upload_id, part_number_marker, max_parts, context)
                    .await
            }
            MultipartOperation::Complete { upload_id } => {
                self.complete_multipart(object, upload_id, request, context)
                    .await
            }
            MultipartOperation::Abort { upload_id } => {
                self.abort_multipart(object, upload_id, context).await
            }
        }
    }

    async fn create_multipart(
        &self,
        object: ObjectId,
        headers: &HeaderMap,
        principal: AuthenticatedPrincipal,
        decision: EffectiveDecision,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        if !self.uploads.reserve(Instant::now()) {
            return Err(S3RequestError::multipart_capacity());
        }
        let headers = match mutation_headers(headers, false) {
            Ok(headers) => headers,
            Err(error) => {
                self.uploads.cancel_reservation();
                return Err(error);
            }
        };
        let response = match self
            .origin
            .multipart(S3MultipartRequest {
                method: Method::Post,
                object: &object,
                query: "uploads=",
                headers: &headers,
            })
            .await
        {
            Ok(response) => response,
            Err(_) => {
                self.uploads.cancel_reservation();
                return Err(S3RequestError::indeterminate_commit());
            }
        };
        if !response.is_success() {
            self.uploads.cancel_reservation();
            return Ok(raw_response(
                response,
                GatewayOperation::Write,
                Some(GatewayTarget::Object(object)),
                context,
            ));
        }
        let upload_id = std::str::from_utf8(&response.body)
            .ok()
            .and_then(|xml| talon_backend::xml::element(xml, "UploadId"))
            .map(talon_backend::xml::unescape)
            .filter(|upload_id| !upload_id.is_empty() && upload_id.len() <= 2048);
        let Some(upload_id) = upload_id else {
            self.uploads.cancel_reservation();
            return Err(S3RequestError::indeterminate_commit());
        };
        let published = self.uploads.publish(
            upload_id,
            MultipartBinding {
                object: object.clone(),
                principal,
                decision,
                touched: Instant::now(),
            },
        );
        if !published {
            return Err(S3RequestError::indeterminate_commit());
        }
        Ok(raw_response(
            response,
            GatewayOperation::Write,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn upload_part(
        &self,
        object: ObjectId,
        upload_id: String,
        part_number: u16,
        copy: bool,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let query = MultipartOperation::UploadPart {
            upload_id,
            part_number,
            copy,
        }
        .query();
        if copy {
            let headers = mutation_headers(request.headers(), true)?;
            let response = self
                .origin
                .multipart(S3MultipartRequest {
                    method: Method::Put,
                    object: &object,
                    query: &query,
                    headers: &headers,
                })
                .await
                .map_err(|_| S3RequestError::indeterminate_commit())?;
            let failed = copy_response_failed(&response);
            let mut response = raw_response(
                response,
                GatewayOperation::Write,
                Some(GatewayTarget::Object(object)),
                context,
            );
            if failed && response.response.status().is_success() {
                response.outcome = GatewayOutcome::Failed;
                response.failure = Some(FailureReason::Origin);
            }
            return Ok(response);
        }
        let (headers, body) = prepare_write_body(request).await?;
        let len = body.len();
        let response = match body {
            WriteBody::Stream {
                body,
                len,
                payload_hash,
            } => {
                self.origin
                    .multipart_body(
                        S3MultipartRequest {
                            method: Method::Put,
                            object: &object,
                            query: &query,
                            headers: &headers,
                        },
                        body,
                        len,
                        &payload_hash,
                    )
                    .await
            }
            WriteBody::Spool { spool, len, hash } => {
                self.origin
                    .multipart_file(
                        S3MultipartRequest {
                            method: Method::Put,
                            object: &object,
                            query: &query,
                            headers: &headers,
                        },
                        spool.path(),
                        len,
                        &hash,
                    )
                    .await
            }
        }
        .map_err(|_| S3RequestError::indeterminate_commit())?;
        let mut response = raw_response(
            response,
            GatewayOperation::Write,
            Some(GatewayTarget::Object(object)),
            context,
        );
        response.requested_bytes = len;
        Ok(response)
    }

    async fn list_parts(
        &self,
        object: ObjectId,
        upload_id: String,
        part_number_marker: Option<u16>,
        max_parts: u16,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let query = MultipartOperation::ListParts {
            upload_id,
            part_number_marker,
            max_parts,
        }
        .query();
        let response = self
            .origin
            .multipart(S3MultipartRequest {
                method: Method::Get,
                object: &object,
                query: &query,
                headers: &[],
            })
            .await
            .map_err(S3RequestError::origin_unavailable)?;
        Ok(raw_response(
            response,
            GatewayOperation::List,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn complete_multipart(
        &self,
        object: ObjectId,
        upload_id: String,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let query = MultipartOperation::Complete {
            upload_id: upload_id.clone(),
        }
        .query();
        let len = single_content_length(request.headers())?;
        let payload_hash = payload_declaration(&request)?;
        let headers = mutation_headers(request.headers(), false)?;
        let body = request.into_body().into_data_stream();
        let response = if payload_hash == "UNSIGNED-PAYLOAD" {
            let body = body.map(|chunk| chunk.map_err(|error| error.to_string()));
            self.origin
                .multipart_body(
                    S3MultipartRequest {
                        method: Method::Post,
                        object: &object,
                        query: &query,
                        headers: &headers,
                    },
                    Box::pin(body),
                    len,
                    &payload_hash,
                )
                .await
        } else {
            let (spool, actual_hash) = spool_and_hash(body, len).await?;
            if actual_hash != payload_hash {
                return Err(S3RequestError::invalid(
                    "XAmzContentSHA256Mismatch",
                    "The provided payload hash does not match the request body",
                ));
            }
            self.origin
                .multipart_file(
                    S3MultipartRequest {
                        method: Method::Post,
                        object: &object,
                        query: &query,
                        headers: &headers,
                    },
                    spool.path(),
                    len,
                    &actual_hash,
                )
                .await
        }
        .map_err(|_| S3RequestError::indeterminate_commit())?;
        let embedded_error = copy_response_failed(&response);
        if (response.is_success() && !embedded_error) || response.status == 404 {
            self.uploads.remove(&upload_id);
        }
        if response.is_success() && !embedded_error {
            let _ = self.cache.invalidate_object(&object);
        }
        let mut response = raw_response(
            response,
            GatewayOperation::Write,
            Some(GatewayTarget::Object(object)),
            context,
        );
        response.requested_bytes = len;
        if embedded_error && response.response.status().is_success() {
            response.outcome = GatewayOutcome::Failed;
            response.failure = Some(FailureReason::Origin);
        }
        Ok(response)
    }

    async fn abort_multipart(
        &self,
        object: ObjectId,
        upload_id: String,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let query = MultipartOperation::Abort {
            upload_id: upload_id.clone(),
        }
        .query();
        let response = self
            .origin
            .multipart(S3MultipartRequest {
                method: Method::Delete,
                object: &object,
                query: &query,
                headers: &[],
            })
            .await
            .map_err(|_| S3RequestError::indeterminate_commit())?;
        if response.is_success() || response.status == 404 {
            self.uploads.remove(&upload_id);
        }
        Ok(raw_response(
            response,
            GatewayOperation::Delete,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn put(
        &self,
        object: ObjectId,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        if copy_source(request.headers())?.is_some() {
            return self.copy(object, request.headers(), context).await;
        }
        let (headers, body) = prepare_write_body(request).await?;
        let len = body.len();
        let response = match body {
            WriteBody::Stream {
                body,
                len,
                payload_hash,
            } => {
                self.origin
                    .put_body(&object, &headers, body, len, &payload_hash)
                    .await
            }
            WriteBody::Spool { spool, len, hash } => {
                self.origin
                    .put_file(&object, &headers, spool.path(), len, &hash)
                    .await
            }
        }
        .map_err(|_| S3RequestError::indeterminate_commit())?;
        if response.is_success() {
            let _ = self.cache.invalidate_object(&object);
        }
        let mut response = raw_response(
            response,
            GatewayOperation::Write,
            Some(GatewayTarget::Object(object)),
            context,
        );
        response.requested_bytes = len;
        Ok(response)
    }

    async fn copy(
        &self,
        object: ObjectId,
        headers: &HeaderMap,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let headers = mutation_headers(headers, true)?;
        let response = self
            .origin
            .copy(&object, &headers)
            .await
            .map_err(|_| S3RequestError::indeterminate_commit())?;
        let failed = copy_response_failed(&response);
        if !failed {
            let _ = self.cache.invalidate_object(&object);
        }
        let mut response = raw_response(
            response,
            GatewayOperation::Write,
            Some(GatewayTarget::Object(object)),
            context,
        );
        if failed && response.response.status().is_success() {
            response.outcome = GatewayOutcome::Failed;
            response.failure = Some(FailureReason::Origin);
        }
        Ok(response)
    }

    async fn delete(
        &self,
        object: ObjectId,
        headers: &HeaderMap,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let conditions = conditional_headers(headers)?;
        let response = self
            .origin
            .delete(&object, &conditions)
            .await
            .map_err(|_| S3RequestError::indeterminate_commit())?;
        if response.is_success() || response.status == 404 {
            let _ = self.cache.invalidate_object(&object);
        }
        Ok(raw_response(
            response,
            GatewayOperation::Delete,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    /// Batch DeleteObjects. The XML body is parsed so every named key is
    /// authorized against the per-request policy snapshot before dispatch,
    /// then the body passes through unchanged so client checksums stay
    /// valid; a confirmed origin outcome invalidates every requested key
    /// because quiet-mode responses omit the per-key results.
    async fn delete_objects(
        &self,
        bucket: String,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let policy = request.extensions().get::<AuthorizationPolicy>().cloned();
        let principal = request
            .extensions()
            .get::<AuthenticatedPrincipal>()
            .cloned();
        let len = single_content_length(request.headers())?;
        if len > MAX_DELETE_OBJECTS_BODY_BYTES {
            return Err(S3RequestError::invalid(
                "MaxMessageLengthExceeded",
                "the DeleteObjects request body is too large",
            ));
        }
        let payload_hash = payload_declaration(&request)?;
        let headers = mutation_headers(request.headers(), false)?;
        let body = collect_body(request.into_body().into_data_stream(), len).await?;
        if payload_hash != "UNSIGNED-PAYLOAD" {
            let actual_hash = format!("{:x}", Sha256::digest(&body));
            if actual_hash != payload_hash {
                return Err(S3RequestError::invalid(
                    "XAmzContentSHA256Mismatch",
                    "The provided payload hash does not match the request body",
                ));
            }
        }
        let keys = parse_delete_objects(&body)?;
        if let Some(policy) = policy {
            let Some(principal) = principal else {
                return Err(S3RequestError::access_denied());
            };
            for key in &keys {
                let access = GatewayAccess {
                    operation: GatewayOperation::Delete,
                    provider_account: None,
                    target: GatewayTarget::Object(ObjectId::new(
                        Backend::S3,
                        bucket.clone(),
                        key.clone(),
                    )),
                    additional: Vec::new(),
                };
                if !policy.allows(&principal, ProviderProtocol::S3, &access) {
                    return Err(S3RequestError::access_denied());
                }
            }
        }
        let response = self
            .origin
            .delete_objects(&bucket, &headers, Bytes::from(body))
            .await
            .map_err(|_| S3RequestError::indeterminate_commit())?;
        if response.is_success() || response.status == 404 {
            for key in &keys {
                let _ = self.cache.invalidate_object(&ObjectId::new(
                    Backend::S3,
                    bucket.clone(),
                    key.clone(),
                ));
            }
        }
        let mut response = raw_response(
            response,
            GatewayOperation::Delete,
            Some(GatewayTarget::NamespaceBodyObjects {
                backend: Backend::S3,
                namespace: bucket,
            }),
            context,
        );
        response.requested_bytes = len;
        Ok(response)
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

    async fn head_bucket(
        &self,
        bucket: String,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let mut response = self
            .origin
            .head_bucket(&bucket)
            .await
            .map_err(S3RequestError::origin_unavailable)?;
        // The origin's real bucket region must not leak: SDKs that resolve a
        // signing region from this header would re-sign for the origin's
        // region and then fail this gateway's SigV4 scope check.
        response
            .headers
            .retain(|(name, _)| !name.eq_ignore_ascii_case("x-amz-bucket-region"));
        response
            .headers
            .push(("x-amz-bucket-region".into(), self.config.region.clone()));
        Ok(raw_response(
            response,
            GatewayOperation::Probe,
            Some(GatewayTarget::Namespace {
                backend: Backend::S3,
                namespace: bucket,
                prefix: None,
            }),
            context,
        ))
    }

    /// Answer `GetBucketLocation` locally. Clients ask this to learn the
    /// region for their SigV4 credential scope, and requests to this gateway
    /// must be signed with the gateway's configured region, which can differ
    /// from the origin bucket's real region.
    fn bucket_location(&self, bucket: String, context: &GatewayRequestContext) -> GatewayResponse {
        let constraint = if self.config.region == "us-east-1" {
            ""
        } else {
            self.config.region.as_str()
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
            xml_escape(constraint),
        );
        let mut response = (StatusCode::OK, Body::from(body)).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        stamp_gateway_headers(response.headers_mut(), &context.request_id);
        GatewayResponse {
            response,
            operation: GatewayOperation::Probe,
            target: Some(GatewayTarget::Namespace {
                backend: Backend::S3,
                namespace: bucket,
                prefix: None,
            }),
            route: GatewayRoute::None,
            outcome: GatewayOutcome::Complete,
            failure: None,
            requested_bytes: 0,
            cache_bytes: 0,
            origin_bytes: 0,
        }
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

    /// ListObjects (V1): the listing dialect of the AWS C++ SDK's
    /// `ListObjectsRequest`. The origin's V1 response body passes back
    /// unchanged, including `Marker`/`NextMarker` pagination.
    async fn list_v1(
        &self,
        bucket: String,
        query: S3Query,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, S3RequestError> {
        let max_keys = query.max_keys()?;
        let response = self
            .origin
            .list_v1(
                &bucket,
                query.prefix.as_deref().unwrap_or(""),
                query.delimiter.as_deref(),
                query.marker.as_deref(),
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
        if metadata.status == 404 {
            return Err(S3RequestError::not_found());
        }
        if metadata.status == 412 {
            return Err(S3RequestError::precondition_failed());
        }
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
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
                Err(CacheReadError::Timeout(message)) => {
                    Err(CacheReadError::Timeout(message.clone()))
                }
                Err(CacheReadError::Protocol(message)) => {
                    Err(CacheReadError::Protocol(message.clone()))
                }
                _ => unreachable!("test cache response"),
            }
        }
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct DemandCache {
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl DemandCache {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                polls: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::new(AtomicBool::new(false)),
            })
        }
    }

    impl S3Cache for DemandCache {
        fn stream(&self, _request: S3CacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
            let chunks = VecDeque::from([Bytes::from_static(b"abc"), Bytes::from_static(b"def")]);
            let polls = Arc::clone(&self.polls);
            let guard = DropSignal(Arc::clone(&self.dropped));
            Ok(Box::pin(futures::stream::unfold(
                (chunks, guard),
                move |(mut chunks, guard)| {
                    let polls = Arc::clone(&polls);
                    async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        chunks.pop_front().map(|chunk| (Ok(chunk), (chunks, guard)))
                    }
                },
            )))
        }
    }

    struct MockOrigin {
        lists: Mutex<Vec<ListRequest>>,
    }

    struct MutationCache {
        invalidations: AtomicUsize,
    }

    impl S3Cache for MutationCache {
        fn stream(&self, _request: S3CacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
            unreachable!("mutation tests do not read")
        }

        fn invalidate_object(&self, _object: &ObjectId) -> usize {
            self.invalidations.fetch_add(1, Ordering::SeqCst);
            1
        }
    }

    struct MutationOrigin {
        response: Mutex<Result<HttpResponse, String>>,
        calls: AtomicUsize,
        body: Mutex<Vec<u8>>,
        headers: Mutex<Vec<(String, String)>>,
    }

    type MultipartCall = (Method, String, Vec<(String, String)>, Vec<u8>);

    struct MultipartOrigin {
        responses: Mutex<VecDeque<Result<HttpResponse, String>>>,
        calls: Mutex<Vec<MultipartCall>>,
    }

    impl MultipartOrigin {
        fn new(responses: impl IntoIterator<Item = Result<HttpResponse, String>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn response(&self) -> Result<HttpResponse, String> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("multipart test provided one response per origin call")
        }
    }

    struct StallingMutationOrigin;

    #[async_trait]
    impl S3Origin for StallingMutationOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            unreachable!("mutation test does not stat")
        }

        async fn get(
            &self,
            _object: &ObjectId,
            _range: Option<(u64, u64)>,
            _conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            unreachable!("mutation test does not read")
        }

        async fn list(
            &self,
            _bucket: &str,
            _prefix: &str,
            _delimiter: Option<&str>,
            _continuation_token: Option<&str>,
            _max_keys: u32,
            _encoding_type: Option<&str>,
        ) -> Result<HttpResponse, String> {
            unreachable!("mutation test does not list")
        }

        async fn put_body(
            &self,
            _object: &ObjectId,
            _headers: &[(String, String)],
            mut body: HttpRequestBody,
            _len: u64,
            _payload_hash: &str,
        ) -> Result<HttpResponse, String> {
            let _ = body.next().await;
            futures::future::pending().await
        }
    }

    impl MutationOrigin {
        fn new(response: Result<HttpResponse, String>) -> Arc<Self> {
            Arc::new(Self {
                response: Mutex::new(response),
                calls: AtomicUsize::new(0),
                body: Mutex::new(Vec::new()),
                headers: Mutex::new(Vec::new()),
            })
        }

        fn response(&self) -> Result<HttpResponse, String> {
            self.response.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl S3Origin for MutationOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            unreachable!("mutation tests do not stat")
        }

        async fn get(
            &self,
            _object: &ObjectId,
            _range: Option<(u64, u64)>,
            _conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            unreachable!("mutation tests do not read")
        }

        async fn list(
            &self,
            _bucket: &str,
            _prefix: &str,
            _delimiter: Option<&str>,
            _continuation_token: Option<&str>,
            _max_keys: u32,
            _encoding_type: Option<&str>,
        ) -> Result<HttpResponse, String> {
            unreachable!("mutation tests do not list")
        }

        async fn put_body(
            &self,
            _object: &ObjectId,
            headers: &[(String, String)],
            mut body: HttpRequestBody,
            _len: u64,
            _payload_hash: &str,
        ) -> Result<HttpResponse, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.headers.lock().unwrap() = headers.to_vec();
            while let Some(chunk) = body.next().await {
                self.body.lock().unwrap().extend_from_slice(&chunk?);
            }
            self.response()
        }

        async fn put_file(
            &self,
            _object: &ObjectId,
            headers: &[(String, String)],
            path: &std::path::Path,
            _len: u64,
            _payload_hash: &str,
        ) -> Result<HttpResponse, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.headers.lock().unwrap() = headers.to_vec();
            *self.body.lock().unwrap() = tokio::fs::read(path).await.map_err(|e| e.to_string())?;
            self.response()
        }

        async fn copy(
            &self,
            _object: &ObjectId,
            headers: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.headers.lock().unwrap() = headers.to_vec();
            self.response()
        }

        async fn delete(
            &self,
            _object: &ObjectId,
            conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.headers.lock().unwrap() = conditions.to_vec();
            self.response()
        }

        async fn delete_objects(
            &self,
            _bucket: &str,
            headers: &[(String, String)],
            body: Bytes,
        ) -> Result<HttpResponse, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.headers.lock().unwrap() = headers.to_vec();
            *self.body.lock().unwrap() = body.to_vec();
            self.response()
        }
    }

    #[async_trait]
    impl S3Origin for MultipartOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            unreachable!("multipart tests do not stat")
        }

        async fn get(
            &self,
            _object: &ObjectId,
            _range: Option<(u64, u64)>,
            _conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            unreachable!("multipart tests do not read objects")
        }

        async fn list(
            &self,
            _bucket: &str,
            _prefix: &str,
            _delimiter: Option<&str>,
            _continuation_token: Option<&str>,
            _max_keys: u32,
            _encoding_type: Option<&str>,
        ) -> Result<HttpResponse, String> {
            unreachable!("multipart tests do not list objects")
        }

        async fn multipart(&self, request: S3MultipartRequest<'_>) -> Result<HttpResponse, String> {
            self.calls.lock().unwrap().push((
                request.method,
                request.query.into(),
                request.headers.to_vec(),
                Vec::new(),
            ));
            self.response()
        }

        async fn multipart_body(
            &self,
            request: S3MultipartRequest<'_>,
            mut body: HttpRequestBody,
            _len: u64,
            _payload_hash: &str,
        ) -> Result<HttpResponse, String> {
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            self.calls.lock().unwrap().push((
                request.method,
                request.query.into(),
                request.headers.to_vec(),
                bytes,
            ));
            self.response()
        }

        async fn multipart_file(
            &self,
            request: S3MultipartRequest<'_>,
            path: &std::path::Path,
            _len: u64,
            _payload_hash: &str,
        ) -> Result<HttpResponse, String> {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|error| error.to_string())?;
            self.calls.lock().unwrap().push((
                request.method,
                request.query.into(),
                request.headers.to_vec(),
                bytes,
            ));
            self.response()
        }
    }

    fn mutation_adapter(cache: Arc<MutationCache>, origin: Arc<MutationOrigin>) -> S3Adapter {
        S3Adapter::new(S3AdapterConfig::path_style("localhost"), cache, origin).unwrap()
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

    struct UnavailableOrigin;

    #[async_trait]
    impl S3Origin for UnavailableOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            Err("connect failed at https://s3.example/?secret=credential".into())
        }

        async fn get(
            &self,
            _object: &ObjectId,
            _range: Option<(u64, u64)>,
            _conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            unreachable!("HEAD failure must stop GET dispatch")
        }

        async fn list(
            &self,
            _bucket: &str,
            _prefix: &str,
            _delimiter: Option<&str>,
            _continuation_token: Option<&str>,
            _max_keys: u32,
            _encoding_type: Option<&str>,
        ) -> Result<HttpResponse, String> {
            unreachable!("test only dispatches object reads")
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

    fn authenticated_request(method: &str, uri: &str, body: Body) -> Request {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .body(body)
            .unwrap();
        request
            .extensions_mut()
            .insert(AuthenticatedPrincipal::new("multipart-user", "account-a"));
        request
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

    #[test]
    fn parses_only_supported_multipart_shapes() {
        let upload = S3Query::parse(Some("partNumber=7&uploadId=id%2Ftoken"))
            .unwrap()
            .multipart_operation(&axum::http::Method::PUT, false)
            .unwrap()
            .unwrap();
        assert_eq!(
            upload.query(),
            "partNumber=7&uploadId=id%2Ftoken",
            "opaque upload IDs are decoded once and safely re-encoded"
        );
        assert!(S3Query::parse(Some("uploadId=a&uploadId=b")).is_err());
        assert!(S3Query::parse(Some("partNumber=0&uploadId=id"))
            .unwrap()
            .multipart_operation(&axum::http::Method::PUT, false)
            .is_err());
        assert!(S3Query::parse(Some("uploads=&uploadId=id"))
            .unwrap()
            .multipart_operation(&axum::http::Method::POST, false)
            .is_err());
    }

    type V1ListRequest = (String, String, Option<String>, Option<String>, u32);

    struct BucketOrigin {
        responses: Mutex<VecDeque<Result<HttpResponse, String>>>,
        buckets: Mutex<Vec<String>>,
        v1_lists: Mutex<Vec<V1ListRequest>>,
    }

    impl BucketOrigin {
        fn new(responses: impl IntoIterator<Item = Result<HttpResponse, String>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses.into_iter().collect()),
                buckets: Mutex::new(Vec::new()),
                v1_lists: Mutex::new(Vec::new()),
            })
        }

        fn next_response(&self) -> Result<HttpResponse, String> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("bucket probe test provided one response per call")
        }
    }

    #[async_trait]
    impl S3Origin for BucketOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            unreachable!("bucket probe tests do not stat objects")
        }

        async fn get(
            &self,
            _object: &ObjectId,
            _range: Option<(u64, u64)>,
            _conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            unreachable!("bucket probe tests do not read objects")
        }

        async fn list(
            &self,
            _bucket: &str,
            _prefix: &str,
            _delimiter: Option<&str>,
            _continuation_token: Option<&str>,
            _max_keys: u32,
            _encoding_type: Option<&str>,
        ) -> Result<HttpResponse, String> {
            unreachable!("bucket probe tests do not list")
        }

        async fn head_bucket(&self, bucket: &str) -> Result<HttpResponse, String> {
            self.buckets.lock().unwrap().push(bucket.to_string());
            self.next_response()
        }

        async fn list_v1(
            &self,
            bucket: &str,
            prefix: &str,
            delimiter: Option<&str>,
            marker: Option<&str>,
            max_keys: u32,
            _encoding_type: Option<&str>,
        ) -> Result<HttpResponse, String> {
            self.v1_lists.lock().unwrap().push((
                bucket.to_string(),
                prefix.to_string(),
                delimiter.map(str::to_string),
                marker.map(str::to_string),
                max_keys,
            ));
            self.next_response()
        }
    }

    fn bucket_probe_adapter(origin: Arc<BucketOrigin>, region: &str) -> S3Adapter {
        let mut config = S3AdapterConfig::path_style("localhost");
        config.region = region.into();
        S3Adapter::new(
            config,
            Arc::new(MockCache {
                response: Ok(vec![]),
            }),
            origin,
        )
        .unwrap()
    }

    #[test]
    fn bucket_probes_classify_as_namespace_probe() {
        let adapter = bucket_probe_adapter(BucketOrigin::new([]), "us-east-1");
        for probe in [
            request(axum::http::Method::HEAD, "/bucket"),
            request(axum::http::Method::GET, "/bucket?location="),
        ] {
            let access = adapter.classify_access(&probe).unwrap();
            assert_eq!(access.operation, GatewayOperation::Probe);
            assert_eq!(
                access.target,
                GatewayTarget::Namespace {
                    backend: Backend::S3,
                    namespace: "bucket".into(),
                    prefix: None,
                }
            );
        }
        let list = adapter
            .classify_access(&request(axum::http::Method::GET, "/bucket?list-type=2"))
            .unwrap();
        assert_eq!(list.operation, GatewayOperation::List);
        let v1 = adapter
            .classify_access(&request(axum::http::Method::GET, "/bucket?prefix=a/"))
            .unwrap();
        assert_eq!(v1.operation, GatewayOperation::List);
        assert_eq!(
            v1.target,
            GatewayTarget::Namespace {
                backend: Backend::S3,
                namespace: "bucket".into(),
                prefix: Some("a/".into()),
            }
        );
        let bare = adapter
            .classify_access(&request(axum::http::Method::GET, "/bucket"))
            .unwrap();
        assert_eq!(bare.operation, GatewayOperation::List);
        assert_eq!(
            bare.target,
            GatewayTarget::Namespace {
                backend: Backend::S3,
                namespace: "bucket".into(),
                prefix: None,
            },
            "a bare bucket GET is a whole-bucket V1 listing"
        );
        assert!(
            adapter
                .classify_access(&request(axum::http::Method::PUT, "/bucket"))
                .is_err(),
            "bucket-level mutations stay rejected"
        );
        assert!(S3Query::parse(Some("location=&location=")).is_err());
    }

    #[test]
    fn bucket_sub_resources_are_rejected_not_approximated() {
        let adapter = bucket_probe_adapter(BucketOrigin::new([]), "us-east-1");
        for (uri, code) in [
            ("/bucket?uploads=", "NotImplemented"),
            ("/bucket?versions", "NotImplemented"),
            ("/bucket?acl", "NotImplemented"),
            ("/bucket?delete=", "NotImplemented"),
            ("/bucket?list-type=3", "InvalidArgument"),
        ] {
            let error = adapter
                .classify_access(&request(axum::http::Method::GET, uri))
                .unwrap_err();
            assert_eq!(error.code, code, "{uri} must not become a V1 listing");
        }
    }

    fn delete_objects_xml(keys: &[&str]) -> String {
        let mut body = String::from("<Delete>");
        for key in keys {
            body.push_str("<Object><Key>");
            body.push_str(key);
            body.push_str("</Key></Object>");
        }
        body.push_str("</Delete>");
        body
    }

    fn batch_delete_request(uri: &str, body: &str, headers: &[(&str, &str)]) -> Request {
        let mut builder = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::HOST, "localhost")
            .header(header::CONTENT_LENGTH, body.len().to_string());
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let mut request = builder.body(Body::from(body.to_string())).unwrap();
        request
            .extensions_mut()
            .insert(AuthenticatedPrincipal::new("multipart-user", "account-a"));
        request
    }

    async fn expect_request_error(adapter: &S3Adapter, request: Request) -> S3RequestError {
        match adapter.handle_request(request, &context()).await {
            Ok(_) => panic!("the request must fail"),
            Err(error) => error,
        }
    }

    fn batch_delete_policy(prefix: Option<&str>) -> AuthorizationPolicy {
        AuthorizationPolicy::new(vec![crate::AuthorizationGrant {
            id: "batch-deleter".into(),
            principal: "multipart-user".into(),
            protocol: ProviderProtocol::S3,
            provider_account: "account-a".into(),
            namespace: "bucket".into(),
            prefix: prefix.map(str::to_string),
            operations: vec![GatewayOperation::Delete],
        }])
        .unwrap()
    }

    #[test]
    fn batch_delete_classifies_as_a_body_object_namespace_delete() {
        let adapter = bucket_probe_adapter(BucketOrigin::new([]), "us-east-1");
        for uri in ["/bucket?delete=", "/bucket?delete"] {
            let access = adapter
                .classify_access(&request(axum::http::Method::POST, uri))
                .unwrap();
            assert_eq!(access.operation, GatewayOperation::Delete);
            assert_eq!(
                access.target,
                GatewayTarget::NamespaceBodyObjects {
                    backend: Backend::S3,
                    namespace: "bucket".into(),
                }
            );
            assert!(access.additional.is_empty());
        }
        for (method, uri, code) in [
            (
                axum::http::Method::POST,
                "/bucket?delete=1",
                "InvalidRequest",
            ),
            (
                axum::http::Method::POST,
                "/bucket?delete=&uploads=",
                "InvalidRequest",
            ),
            (
                axum::http::Method::POST,
                "/bucket?delete=&prefix=a/",
                "InvalidRequest",
            ),
            (
                axum::http::Method::POST,
                "/bucket/key?delete=",
                "InvalidRequest",
            ),
            (
                axum::http::Method::DELETE,
                "/bucket/key?delete=",
                "InvalidRequest",
            ),
        ] {
            let error = adapter.classify_access(&request(method, uri)).unwrap_err();
            assert_eq!(error.code, code, "{uri} must not be reinterpreted");
        }
    }

    #[tokio::test]
    async fn batch_delete_forwards_the_body_and_invalidates_every_key() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: vec![("content-type".into(), "application/xml".into())],
            body: Bytes::from_static(
                b"<DeleteResult><Deleted><Key>logs/a</Key></Deleted></DeleteResult>",
            ),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        let body = delete_objects_xml(&["logs/a", "logs/b"]);
        let request = batch_delete_request(
            "/bucket?delete=",
            &body,
            &[
                ("content-md5", "3ZG9/9OGCkoTBd6aa5jXbg=="),
                ("authorization", "AWS4-HMAC-SHA256 client-material"),
            ],
        );
        let response = adapter.handle_request(request, &context()).await.unwrap();
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(response.operation, GatewayOperation::Delete);
        assert_eq!(response.requested_bytes, body.len() as u64);
        assert_eq!(*origin.body.lock().unwrap(), body.as_bytes());
        let headers = origin.headers.lock().unwrap().clone();
        assert!(
            headers
                .iter()
                .any(|(name, value)| name == "content-md5" && value == "3ZG9/9OGCkoTBd6aa5jXbg=="),
            "the client checksum travels with the unmodified body"
        );
        assert!(
            headers.iter().all(|(name, _)| name != "authorization"),
            "client credentials never reach the origin"
        );
        assert_eq!(
            cache.invalidations.load(Ordering::SeqCst),
            2,
            "confirmed outcomes invalidate every requested key, not the \
             response echo, because quiet mode omits it"
        );
        let bytes = to_bytes(response.response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8(bytes.to_vec())
            .unwrap()
            .contains("<DeleteResult>"));
    }

    #[tokio::test]
    async fn batch_delete_authorizes_every_key_before_dispatch() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(b"<DeleteResult/>"),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));

        let body = delete_objects_xml(&["tenant/a", "other/b"]);
        let mut request = batch_delete_request("/bucket?delete=", &body, &[]);
        request
            .extensions_mut()
            .insert(batch_delete_policy(Some("tenant/")));
        let error = expect_request_error(&adapter, request).await;
        assert_eq!(error.code, "AccessDenied");
        assert_eq!(
            origin.calls.load(Ordering::SeqCst),
            0,
            "one uncovered key rejects the whole batch before dispatch"
        );
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);

        let body = delete_objects_xml(&["tenant/a", "tenant/b"]);
        let mut request = batch_delete_request("/bucket?delete=", &body, &[]);
        request
            .extensions_mut()
            .insert(batch_delete_policy(Some("tenant/")));
        adapter.handle_request(request, &context()).await.unwrap();
        assert_eq!(origin.calls.load(Ordering::SeqCst), 1);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 2);

        let body = delete_objects_xml(&["tenant/a"]);
        let mut request = Request::builder()
            .method("POST")
            .uri("/bucket?delete=")
            .header(header::HOST, "localhost")
            .header(header::CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .unwrap();
        request.extensions_mut().insert(batch_delete_policy(None));
        let error = expect_request_error(&adapter, request).await;
        assert_eq!(
            error.code, "AccessDenied",
            "an installed policy without a principal fails closed"
        );
    }

    #[tokio::test]
    async fn batch_delete_rejects_bodies_it_cannot_prove_it_understood() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(b"<DeleteResult/>"),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        let thousand_and_one = {
            let mut body = String::from("<Delete>");
            for index in 0..=MAX_DELETE_OBJECTS_KEYS {
                body.push_str(&format!("<Object><Key>k{index}</Key></Object>"));
            }
            body.push_str("</Delete>");
            body
        };
        for (body, code) in [
            ("<Delete></Delete>", "MalformedXML"),
            ("not xml", "MalformedXML"),
            (
                "<Delete><Object><Key>a</Key><VersionId>v1</VersionId></Object></Delete>",
                "NotImplemented",
            ),
            (
                "<!DOCTYPE d [<!ENTITY x \"y\">]><Delete><Object><Key>a</Key></Object></Delete>",
                "MalformedXML",
            ),
            (
                "<Delete><!-- hidden --><Object><Key>a</Key></Object></Delete>",
                "MalformedXML",
            ),
            (
                "<Delete><Object><Key><![CDATA[a]]></Key></Object></Delete>",
                "MalformedXML",
            ),
            (
                "<Delete><Object><Key>a</Key></Object></Delete>trailing",
                "MalformedXML",
            ),
            (
                "<Delete><Object><Key>&unknown;</Key></Object></Delete>",
                "MalformedXML",
            ),
            (
                "<Delete><Unknown/><Object><Key>a</Key></Object></Delete>",
                "MalformedXML",
            ),
            ("<Delete><Object></Object></Delete>", "MalformedXML"),
            (thousand_and_one.as_str(), "MalformedXML"),
        ] {
            let request = batch_delete_request("/bucket?delete=", body, &[]);
            let error = expect_request_error(&adapter, request).await;
            assert_eq!(error.code, code, "{body:.60} must fail closed");
        }
        assert_eq!(
            origin.calls.load(Ordering::SeqCst),
            0,
            "no unparsable body may reach the origin"
        );
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn batch_delete_verifies_a_declared_payload_hash() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(b"<DeleteResult/>"),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        let body = delete_objects_xml(&["logs/a"]);
        let tampered = batch_delete_request(
            "/bucket?delete=",
            &body,
            &[(
                "x-amz-content-sha256",
                "0000000000000000000000000000000000000000000000000000000000000000",
            )],
        );
        let error = expect_request_error(&adapter, tampered).await;
        assert_eq!(error.code, "XAmzContentSHA256Mismatch");
        assert_eq!(origin.calls.load(Ordering::SeqCst), 0);

        let declared = format!("{:x}", Sha256::digest(body.as_bytes()));
        let verified = batch_delete_request(
            "/bucket?delete=",
            &body,
            &[("x-amz-content-sha256", declared.as_str())],
        );
        adapter.handle_request(verified, &context()).await.unwrap();
        assert_eq!(origin.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn batch_delete_failures_do_not_invalidate() {
        let body = delete_objects_xml(&["logs/a", "logs/b"]);

        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Err("connection reset".into()));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        let request = batch_delete_request("/bucket?delete=", &body, &[]);
        let error = expect_request_error(&adapter, request).await;
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(error.indeterminate_commit);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);

        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 400,
            headers: Vec::new(),
            body: Bytes::from_static(b"<Error><Code>MalformedXML</Code></Error>"),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        let request = batch_delete_request("/bucket?delete=", &body, &[]);
        let response = adapter.handle_request(request, &context()).await.unwrap();
        assert_eq!(response.response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.outcome, GatewayOutcome::Failed);
        assert_eq!(
            cache.invalidations.load(Ordering::SeqCst),
            0,
            "an origin-rejected batch deleted nothing"
        );

        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 404,
            headers: Vec::new(),
            body: Bytes::from_static(b"<Error><Code>NoSuchBucket</Code></Error>"),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        let request = batch_delete_request("/bucket?delete=", &body, &[]);
        adapter.handle_request(request, &context()).await.unwrap();
        assert_eq!(
            cache.invalidations.load(Ordering::SeqCst),
            2,
            "a missing bucket confirms every key is absent, like single DELETE"
        );
    }

    #[test]
    fn parse_delete_objects_is_strict_and_decodes_entities() {
        let keys = parse_delete_objects(
            concat!(
                "\u{feff}<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
                "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
                "  <Object><Key>plain/key.bin</Key></Object>\n",
                "  <Object><Key>escaped/&amp;&lt;&gt;&quot;&apos;</Key></Object>\n",
                "  <Object><Key>numeric/&#65;&#x42;&#xA;</Key></Object>\n",
                "  <Quiet>true</Quiet>\n",
                "</Delete>",
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(
            keys,
            vec![
                "plain/key.bin".to_string(),
                "escaped/&<>\"'".to_string(),
                "numeric/AB\n".to_string(),
            ]
        );
        assert!(parse_delete_objects(
            b"<Delete><Object><Key>a</Key></Object><Quiet>maybe</Quiet></Delete>"
        )
        .is_err());
        let long_key = "k".repeat(1025);
        let error =
            parse_delete_objects(delete_objects_xml(&[long_key.as_str()]).as_bytes()).unwrap_err();
        assert_eq!(error.code, "KeyTooLongError");
        let error = parse_delete_objects(
            b"<Delete><Object attr=\"a>b\"><Key>trick</Key></Object></Delete>",
        )
        .map(|keys| keys.join(","));
        assert_eq!(
            error.unwrap(),
            "trick",
            "quoted '>' inside attributes must not truncate tag parsing"
        );
    }

    #[test]
    fn parse_delete_objects_matches_origin_parsers_on_boundary_syntax() {
        for quiet in ["true", "false", "1", "0", " true ", "\n\tfalse\r\n"] {
            let body =
                format!("<Delete><Object><Key>a</Key></Object><Quiet>{quiet}</Quiet></Delete>");
            assert_eq!(
                parse_delete_objects(body.as_bytes()).unwrap(),
                vec!["a".to_string()],
                "<Quiet>{quiet}</Quiet> is a boolean origin parsers accept"
            );
        }

        // Rust's integer parsers accept a leading `+`; the XML CharRef
        // production does not, so accepting one would decode a key the origin
        // rejects outright.
        for reference in ["&#+65;", "&#x+41;", "&#;", "&#x;", "&#xzz;", "&#4 1;"] {
            let body = format!("<Delete><Object><Key>{reference}</Key></Object></Delete>");
            let error = parse_delete_objects(body.as_bytes()).unwrap_err();
            assert_eq!(
                error.code, "MalformedXML",
                "{reference} is not a well-formed character reference"
            );
        }
        assert_eq!(
            parse_delete_objects(b"<Delete><Object><Key>&#x0041;</Key></Object></Delete>").unwrap(),
            vec!["A".to_string()],
            "leading zeros stay legal inside a character reference"
        );

        // Form feed and vertical tab are ASCII whitespace but not XML
        // whitespace: an origin parser rejects the document outright.
        for illegal in ["\u{0c}", "\u{0b}"] {
            let body = format!("<Delete>{illegal}<Object><Key>a</Key></Object></Delete>");
            let error = parse_delete_objects(body.as_bytes()).unwrap_err();
            assert_eq!(
                error.code, "MalformedXML",
                "{illegal:?} is not XML whitespace"
            );
            let tagged =
                format!("<Delete><Object{illegal}attr=\"v\"><Key>a</Key></Object></Delete>");
            assert_eq!(
                parse_delete_objects(tagged.as_bytes()).unwrap_err().code,
                "MalformedXML",
                "{illegal:?} does not separate attributes either"
            );
        }
        assert_eq!(
            parse_delete_objects(b"<Delete>\r\n\t <Object>\r\n<Key>a</Key>\t</Object>\n</Delete>")
                .unwrap(),
            vec!["a".to_string()],
            "the XML S production stays accepted"
        );
    }

    #[tokio::test]
    async fn batch_delete_bounds_the_body_it_must_buffer() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(b"<DeleteResult/>"),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        let body = delete_objects_xml(&["logs/a"]);

        // The declared length alone rejects the request: the body is parsed to
        // authorize each key and forwarded unchanged, so it cannot stream or
        // spool, and this ceiling must not follow the object-upload cap.
        let oversized = Request::builder()
            .method("POST")
            .uri("/bucket?delete=")
            .header(header::HOST, "localhost")
            .header(
                header::CONTENT_LENGTH,
                (MAX_DELETE_OBJECTS_BODY_BYTES + 1).to_string(),
            )
            .body(Body::from(body.clone()))
            .unwrap();
        let error = expect_request_error(&adapter, oversized).await;
        assert_eq!(error.code, "MaxMessageLengthExceeded");
        assert_eq!(
            origin.calls.load(Ordering::SeqCst),
            0,
            "an oversized declaration is rejected before the body is read"
        );

        for (declared, code) in [
            (body.len() as u64 + 1, "IncompleteBody"),
            (body.len() as u64 - 1, "InvalidArgument"),
        ] {
            let mismatched = Request::builder()
                .method("POST")
                .uri("/bucket?delete=")
                .header(header::HOST, "localhost")
                .header(header::CONTENT_LENGTH, declared.to_string())
                .body(Body::from(body.clone()))
                .unwrap();
            let error = expect_request_error(&adapter, mismatched).await;
            assert_eq!(
                error.code, code,
                "a {declared}-byte declaration must not parse"
            );
        }

        let unlabelled = Request::builder()
            .method("POST")
            .uri("/bucket?delete=")
            .header(header::HOST, "localhost")
            .body(Body::from(body))
            .unwrap();
        let error = expect_request_error(&adapter, unlabelled).await;
        assert_eq!(error.code, "MissingContentLength");
        assert_eq!(origin.calls.load(Ordering::SeqCst), 0);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn object_level_delete_sub_resource_never_dispatches() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 204,
            headers: Vec::new(),
            body: Bytes::new(),
        }));
        let adapter = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin));
        // handle_request carries its own guard because a deployment without an
        // installed policy never calls classify_access.
        for method in ["DELETE", "POST", "GET", "PUT"] {
            let request = Request::builder()
                .method(method)
                .uri("/bucket/key?delete=")
                .header(header::HOST, "localhost")
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap();
            let error = expect_request_error(&adapter, request).await;
            assert_eq!(
                error.code, "InvalidRequest",
                "{method} /bucket/key?delete= must not act on the object"
            );
        }
        assert_eq!(origin.calls.load(Ordering::SeqCst), 0);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn v1_listing_forwards_validated_parameters() {
        let origin = BucketOrigin::new([Ok(HttpResponse {
            status: 200,
            headers: vec![("content-type".into(), "application/xml".into())],
            body: Bytes::from_static(
                b"<?xml version=\"1.0\"?><ListBucketResult><IsTruncated>true</IsTruncated>\
                  <NextMarker>b.txt</NextMarker><Contents><Key>a.txt</Key></Contents>\
                  </ListBucketResult>",
            ),
        })]);
        let adapter = bucket_probe_adapter(Arc::clone(&origin), "us-east-1");
        let response = adapter
            .handle(
                request(
                    axum::http::Method::GET,
                    "/bucket?prefix=gateway%2F&marker=start%2Fkey&max-keys=7",
                ),
                context(),
            )
            .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(response.operation, GatewayOperation::List);
        assert_eq!(response.outcome, GatewayOutcome::Complete);
        let body = to_bytes(response.response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .contains("<NextMarker>b.txt</NextMarker>"));
        assert_eq!(
            *origin.v1_lists.lock().unwrap(),
            vec![(
                "bucket".to_string(),
                "gateway/".to_string(),
                None,
                Some("start/key".to_string()),
                7,
            )]
        );
    }

    #[tokio::test]
    async fn head_bucket_passes_the_origin_status_through() {
        let origin = BucketOrigin::new([
            Ok(HttpResponse {
                status: 200,
                headers: vec![("x-amz-bucket-region".into(), "eu-central-1".into())],
                body: Bytes::new(),
            }),
            Ok(HttpResponse {
                status: 404,
                headers: Vec::new(),
                body: Bytes::new(),
            }),
            Ok(HttpResponse {
                status: 403,
                headers: Vec::new(),
                body: Bytes::new(),
            }),
        ]);
        let adapter = bucket_probe_adapter(Arc::clone(&origin), "us-west-2");

        let found = adapter
            .handle(request(axum::http::Method::HEAD, "/bucket"), context())
            .await;
        assert_eq!(found.response.status(), StatusCode::OK);
        assert_eq!(found.operation, GatewayOperation::Probe);
        assert_eq!(found.outcome, GatewayOutcome::Complete);
        assert_eq!(
            found.response.headers().get("x-amz-bucket-region").unwrap(),
            "us-west-2",
            "the origin's real region must not leak past the gateway"
        );

        let missing = adapter
            .handle(request(axum::http::Method::HEAD, "/bucket"), context())
            .await;
        assert_eq!(missing.response.status(), StatusCode::NOT_FOUND);
        assert_eq!(missing.outcome, GatewayOutcome::Failed);
        assert_eq!(missing.failure, Some(FailureReason::NotFound));

        let denied = adapter
            .handle(request(axum::http::Method::HEAD, "/bucket"), context())
            .await;
        assert_eq!(denied.response.status(), StatusCode::FORBIDDEN);
        assert_eq!(denied.outcome, GatewayOutcome::Failed);
        assert_eq!(denied.failure, Some(FailureReason::Authorization));

        assert_eq!(
            *origin.buckets.lock().unwrap(),
            vec!["bucket", "bucket", "bucket"]
        );
    }

    #[tokio::test]
    async fn head_bucket_without_origin_support_fails_closed() {
        let adapter = S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::new(MockCache {
                response: Ok(vec![]),
            }),
            Arc::new(StallingMutationOrigin),
        )
        .unwrap();
        let response = adapter
            .handle(request(axum::http::Method::HEAD, "/bucket"), context())
            .await;
        assert_eq!(response.response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn bucket_location_is_answered_locally_from_the_gateway_region() {
        let adapter = S3Adapter::new(
            {
                let mut config = S3AdapterConfig::path_style("localhost");
                config.region = "us-west-2".into();
                config
            },
            Arc::new(MockCache {
                response: Ok(vec![]),
            }),
            Arc::new(StallingMutationOrigin),
        )
        .unwrap();
        let response = adapter
            .handle(
                request(axum::http::Method::GET, "/bucket?location="),
                context(),
            )
            .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(response.operation, GatewayOperation::Probe);
        assert_eq!(response.route, GatewayRoute::None);
        assert_eq!(response.outcome, GatewayOutcome::Complete);
        assert!(response.response.headers().contains_key("x-amz-request-id"));
        let body = to_bytes(response.response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains(">us-west-2</LocationConstraint>"), "{body}");
    }

    #[tokio::test]
    async fn bucket_location_for_us_east_1_is_the_empty_constraint() {
        let adapter = S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::new(MockCache {
                response: Ok(vec![]),
            }),
            Arc::new(StallingMutationOrigin),
        )
        .unwrap();
        let response = adapter
            .handle(
                request(axum::http::Method::GET, "/bucket?location="),
                context(),
            )
            .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        let body = to_bytes(response.response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(
            body.contains("doc/2006-03-01/\"></LocationConstraint>"),
            "us-east-1 must serialize as the empty constraint: {body}"
        );
    }

    #[test]
    fn multipart_registry_is_bounded_expires_and_fails_closed() {
        let registry = MultipartRegistry::new(1, Duration::from_secs(5));
        let now = Instant::now();
        let object = ObjectId::new(Backend::S3, "bucket", "key");
        let principal = AuthenticatedPrincipal::new("user", "account");
        assert!(registry.reserve(now));
        assert!(!registry.reserve(now));
        assert!(registry.publish(
            "upload".into(),
            MultipartBinding {
                object: object.clone(),
                principal: principal.clone(),
                decision: EffectiveDecision::DEFAULT,
                touched: now,
            }
        ));
        assert!(!registry.authorize(
            "upload",
            &ObjectId::new(Backend::S3, "bucket", "other"),
            &principal,
            EffectiveDecision::DEFAULT,
            now,
        ));
        assert!(!registry.authorize(
            "upload",
            &object,
            &AuthenticatedPrincipal::new("other", "account"),
            EffectiveDecision::DEFAULT,
            now,
        ));
        assert!(!registry.authorize(
            "upload",
            &object,
            &principal,
            EffectiveDecision::ORIGIN_ONLY,
            now,
        ));
        assert!(!registry.authorize(
            "upload",
            &object,
            &principal,
            EffectiveDecision::DEFAULT,
            now + Duration::from_secs(6),
        ));
        assert!(registry.reserve(now + Duration::from_secs(6)));
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
    async fn timeout_falls_back_but_protocol_failure_is_terminal() {
        let timeout = adapter(
            MockCache {
                response: Err(CacheReadError::Timeout("slow worker".into())),
            },
            MockOrigin::new(),
        )
        .handle(request(axum::http::Method::GET, "/bucket/key"), context())
        .await;
        assert_eq!(timeout.response.status(), StatusCode::OK);
        assert_eq!(timeout.outcome, GatewayOutcome::Fallback);

        let protocol = adapter(
            MockCache {
                response: Err(CacheReadError::Protocol("bad frame".into())),
            },
            MockOrigin::new(),
        )
        .handle(request(axum::http::Method::GET, "/bucket/key"), context())
        .await;
        assert_eq!(
            protocol.response.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(protocol.outcome, GatewayOutcome::Failed);
    }

    #[tokio::test]
    async fn cache_stream_respects_backpressure_and_cancellation() {
        let cache = DemandCache::new();
        let response = S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::clone(&cache) as Arc<dyn S3Cache>,
            MockOrigin::new(),
        )
        .unwrap()
        .handle(request(axum::http::Method::GET, "/bucket/key"), context())
        .await;

        assert_eq!(cache.polls.load(Ordering::SeqCst), 1);
        tokio::task::yield_now().await;
        assert_eq!(cache.polls.load(Ordering::SeqCst), 1);
        let mut body = response.response.into_body().into_data_stream();
        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(cache.polls.load(Ordering::SeqCst), 1);
        drop(body);
        assert!(cache.dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn origin_outage_returns_a_sanitized_s3_error() {
        let response = S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::new(MockCache {
                response: Ok(Vec::new()),
            }),
            Arc::new(UnavailableOrigin),
        )
        .unwrap()
        .handle(request(axum::http::Method::GET, "/bucket/key"), context())
        .await;

        assert_eq!(response.response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.response.into_body(), 4096).await.unwrap();
        assert!(!body.windows(6).any(|window| window == b"secret"));
        assert!(!body.windows(10).any(|window| window == b"credential"));
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

    #[test]
    fn copy_access_requires_source_read_and_destination_write() {
        let mut copy = request(axum::http::Method::PUT, "/destination/copied");
        copy.headers_mut().insert(
            "x-amz-copy-source",
            HeaderValue::from_static("/source/original"),
        );
        let access = adapter(
            MockCache {
                response: Ok(Vec::new()),
            },
            MockOrigin::new(),
        )
        .classify_access(&copy)
        .unwrap();
        assert_eq!(access.operation, GatewayOperation::Write);
        assert_eq!(access.additional.len(), 1);
        assert_eq!(access.additional[0].operation, GatewayOperation::Read);
        assert_eq!(
            access.additional[0].target,
            GatewayTarget::Object(ObjectId::new(Backend::S3, "source", "original"))
        );

        let mut part_copy = request(
            axum::http::Method::PUT,
            "/destination/copied?partNumber=1&uploadId=opaque",
        );
        part_copy.headers_mut().insert(
            "x-amz-copy-source",
            HeaderValue::from_static("/source/original"),
        );
        let access = adapter(
            MockCache {
                response: Ok(Vec::new()),
            },
            MockOrigin::new(),
        )
        .classify_access(&part_copy)
        .unwrap();
        assert_eq!(access.operation, GatewayOperation::Write);
        assert_eq!(access.additional.len(), 1);
        assert_eq!(access.additional[0].operation, GatewayOperation::Read);
    }

    #[tokio::test]
    async fn verified_put_rejects_tampering_before_origin_dispatch() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::new(),
        }));
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header(header::CONTENT_LENGTH, "3")
            .header(
                "x-amz-content-sha256",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from("abc"))
            .unwrap();
        let response = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin))
            .handle(request, context())
            .await;
        assert_eq!(response.response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(origin.calls.load(Ordering::SeqCst), 0);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unsigned_put_streams_sanitized_headers_and_invalidates_after_commit() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: vec![("etag".into(), "\"v2\"".into())],
            body: Bytes::new(),
        }));
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header(header::CONTENT_LENGTH, "3")
            .header("authorization", "incoming-secret")
            .header("x-amz-date", "incoming-date")
            .header("x-amz-security-token", "incoming-token")
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("x-amz-meta-owner", "team-a")
            .body(Body::from("abc"))
            .unwrap();
        let response = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin))
            .handle(request, context())
            .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(origin.body.lock().unwrap().as_slice(), b"abc");
        assert_eq!(
            origin.headers.lock().unwrap().as_slice(),
            &[("x-amz-meta-owner".into(), "team-a".into())]
        );
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn streaming_trailer_put_decodes_framing_before_the_origin() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: vec![("etag".into(), "\"v1\"".into())],
            body: Bytes::new(),
        }));
        // Two aws-chunked frames wrapping "hello world", with a real sha256
        // trailer checksum, exactly as an AWS SDK sends by default.
        let wire = concat!(
            "6\r\nhello \r\n",
            "5\r\nworld\r\n",
            "0\r\n",
            "x-amz-checksum-sha256:uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=\r\n\r\n",
        );
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            .header("x-amz-decoded-content-length", "11")
            .header("content-encoding", "aws-chunked")
            .header("x-amz-trailer", "x-amz-checksum-sha256")
            .header("x-amz-meta-owner", "team-a")
            .body(Body::from(wire))
            .unwrap();
        let response = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin))
            .handle(request, context())
            .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(
            origin.body.lock().unwrap().as_slice(),
            b"hello world",
            "the origin must receive the decoded payload, not the framing"
        );
        let forwarded = origin.headers.lock().unwrap().clone();
        assert!(
            !forwarded
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("content-encoding")),
            "the aws-chunked content-encoding must not reach the origin: {forwarded:?}"
        );
        assert!(forwarded
            .iter()
            .any(|(name, value)| name == "x-amz-meta-owner" && value == "team-a"));
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn streaming_trailer_put_without_decoded_length_is_refused() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::new(),
        }));
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            .body(Body::from("6\r\nhello \r\n0\r\n\r\n"))
            .unwrap();
        let response = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin))
            .handle(request, context())
            .await;
        assert_eq!(response.response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(origin.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn streaming_trailer_put_with_a_wrong_checksum_never_reaches_the_origin() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::new(),
        }));
        // Correct framing and length, but the trailer checksum is wrong.
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            .header("x-amz-decoded-content-length", "11")
            .header("x-amz-trailer", "x-amz-checksum-sha256")
            .body(Body::from(
                "b\r\nhello world\r\n0\r\nx-amz-checksum-sha256:AAAA\r\n\r\n",
            ))
            .unwrap();
        let response = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin))
            .handle(request, context())
            .await;
        assert_eq!(response.response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            origin.calls.load(Ordering::SeqCst),
            0,
            "a corrupt payload must be rejected before the origin, not stored"
        );
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn streaming_trailer_put_with_an_unsupported_algorithm_is_refused() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::new(),
        }));
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            .header("x-amz-decoded-content-length", "11")
            .header("x-amz-trailer", "x-amz-checksum-md5")
            .body(Body::from("b\r\nhello world\r\n0\r\n\r\n"))
            .unwrap();
        let response = mutation_adapter(Arc::clone(&cache), Arc::clone(&origin))
            .handle(request, context())
            .await;
        assert_eq!(response.response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(origin.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failed_or_indeterminate_put_never_invalidates_cache() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let rejected = MutationOrigin::new(Ok(HttpResponse {
            status: 412,
            headers: Vec::new(),
            body: Bytes::new(),
        }));
        let put = || {
            Request::builder()
                .method("PUT")
                .uri("/bucket/key")
                .header(header::HOST, "localhost")
                .header(header::CONTENT_LENGTH, "3")
                .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                .body(Body::from("abc"))
                .unwrap()
        };
        let response = mutation_adapter(Arc::clone(&cache), rejected)
            .handle(put(), context())
            .await;
        assert_eq!(response.response.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);

        let unavailable = MutationOrigin::new(Err("response lost".into()));
        let response = mutation_adapter(Arc::clone(&cache), unavailable)
            .handle(put(), context())
            .await;
        assert_eq!(
            response.response.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            response.response.headers()["x-talon-commit-state"],
            "indeterminate"
        );
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn embedded_copy_error_does_not_invalidate_destination() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MutationOrigin::new(Ok(HttpResponse {
            status: 200,
            headers: vec![("content-type".into(), "application/xml".into())],
            body: Bytes::from_static(
                b"<Error><Code>InternalError</Code><Message>copy failed</Message></Error>",
            ),
        }));
        let request = Request::builder()
            .method("PUT")
            .uri("/destination/copied")
            .header(header::HOST, "localhost")
            .header("x-amz-copy-source", "/source/original")
            .body(Body::empty())
            .unwrap();
        let response = mutation_adapter(Arc::clone(&cache), origin)
            .handle(request, context())
            .await;
        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(response.outcome, GatewayOutcome::Failed);
        assert_eq!(response.failure, Some(FailureReason::Origin));
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn multipart_binding_and_completion_are_fail_closed() {
        let cache = Arc::new(MutationCache {
            invalidations: AtomicUsize::new(0),
        });
        let origin = MultipartOrigin::new([
            Ok(HttpResponse {
                status: 200,
                headers: vec![("content-type".into(), "application/xml".into())],
                body: Bytes::from_static(
                    b"<InitiateMultipartUploadResult><UploadId>opaque/id</UploadId></InitiateMultipartUploadResult>",
                ),
            }),
            Ok(HttpResponse {
                status: 200,
                headers: vec![("content-type".into(), "application/xml".into())],
                body: Bytes::from_static(
                    b"<Error><Code>InternalError</Code><Message>complete failed</Message></Error>",
                ),
            }),
            Ok(HttpResponse {
                status: 200,
                headers: vec![("content-type".into(), "application/xml".into())],
                body: Bytes::from_static(b"<CompleteMultipartUploadResult/>")
            }),
        ]);
        let adapter = S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::clone(&cache) as Arc<dyn S3Cache>,
            Arc::clone(&origin) as Arc<dyn S3Origin>,
        )
        .unwrap();

        let create = adapter
            .handle(
                authenticated_request("POST", "/bucket/key?uploads", Body::empty()),
                context(),
            )
            .await;
        assert_eq!(create.response.status(), StatusCode::OK);
        assert_eq!(origin.calls.lock().unwrap()[0].1, "uploads=");

        let mut wrong_principal =
            authenticated_request("GET", "/bucket/key?uploadId=opaque%2Fid", Body::empty());
        wrong_principal
            .extensions_mut()
            .insert(AuthenticatedPrincipal::new("different-user", "account-a"));
        let rejected = adapter.handle(wrong_principal, context()).await;
        assert_eq!(rejected.response.status(), StatusCode::NOT_FOUND);
        assert_eq!(origin.calls.lock().unwrap().len(), 1);

        let complete_body = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>";
        let complete = || {
            let mut request = authenticated_request(
                "POST",
                "/bucket/key?uploadId=opaque%2Fid",
                Body::from(complete_body),
            );
            request.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&complete_body.len().to_string()).unwrap(),
            );
            request.headers_mut().insert(
                "x-amz-content-sha256",
                HeaderValue::from_static("UNSIGNED-PAYLOAD"),
            );
            request
        };
        let embedded_error = adapter.handle(complete(), context()).await;
        assert_eq!(embedded_error.response.status(), StatusCode::OK);
        assert_eq!(embedded_error.outcome, GatewayOutcome::Failed);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 0);

        let success = adapter.handle(complete(), context()).await;
        assert_eq!(success.response.status(), StatusCode::OK);
        assert_eq!(success.outcome, GatewayOutcome::Complete);
        assert_eq!(cache.invalidations.load(Ordering::SeqCst), 1);
        assert_eq!(origin.calls.lock().unwrap()[2].0, Method::Post);
        assert_eq!(origin.calls.lock().unwrap()[2].3, complete_body.as_bytes());

        let removed = adapter
            .handle(
                authenticated_request("GET", "/bucket/key?uploadId=opaque%2Fid", Body::empty()),
                context(),
            )
            .await;
        assert_eq!(removed.response.status(), StatusCode::NOT_FOUND);
        assert_eq!(origin.calls.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn streaming_put_is_demand_driven_and_cancellation_drops_the_body() {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let chunks = VecDeque::from([Bytes::from_static(b"abc"), Bytes::from_static(b"def")]);
        let stream_polls = Arc::clone(&polls);
        let guard = DropSignal(Arc::clone(&dropped));
        let stream = futures::stream::unfold((chunks, guard), move |(mut chunks, guard)| {
            let polls = Arc::clone(&stream_polls);
            async move {
                polls.fetch_add(1, Ordering::SeqCst);
                chunks
                    .pop_front()
                    .map(|chunk| (Ok::<_, std::io::Error>(chunk), (chunks, guard)))
            }
        });
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header(header::CONTENT_LENGTH, "6")
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .body(Body::from_stream(stream))
            .unwrap();
        let adapter = S3Adapter::new(
            S3AdapterConfig::path_style("localhost"),
            Arc::new(MutationCache {
                invalidations: AtomicUsize::new(0),
            }),
            Arc::new(StallingMutationOrigin),
        )
        .unwrap();
        let task = tokio::spawn(async move { adapter.handle(request, context()).await });
        for _ in 0..20 {
            if polls.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
        assert!(dropped.load(Ordering::SeqCst));
    }
}
