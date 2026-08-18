//! Azure Blob Storage read adapter.

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
use talon_backend::{
    AzureBackend, AzureSas, HttpRequestBody, HttpResponse, HttpStreamResponse, Method,
};
use talon_cache_client::{BlockReader, CacheReadError, FileView, DEFAULT_TRANSFER_CHUNK_BYTES};
use talon_core::{Backend, ObjectId, Version};

use crate::{
    AuthenticatedPrincipal, EffectiveDecision, FailureReason, GatewayAccess, GatewayAdapter,
    GatewayOperation, GatewayOutcome, GatewayRequestContext, GatewayResponse, GatewayRoute,
    GatewayTarget, ProviderProtocol, AZURE_CACHE_MARK_HEADER,
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
    /// Maximum active block-staging bindings tracked by one process.
    pub max_block_bindings: usize,
    /// Lifetime of an inactive block-staging binding.
    pub block_binding_ttl: Duration,
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
            max_block_bindings: 1024,
            block_binding_ttl: Duration::from_secs(24 * 60 * 60),
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
        if self.account.is_empty()
            || self.block_size == 0
            || self.transfer_chunk_bytes == 0
            || self.max_block_bindings == 0
            || self.block_binding_ttl.is_zero()
        {
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

    /// Invalidate every cached placement for one blob after a confirmed mutation.
    fn invalidate_object(&self, _object: &ObjectId) -> usize {
        0
    }
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

    fn invalidate_object(&self, object: &ObjectId) -> usize {
        BlockReader::invalidate_object(self, object)
    }
}

/// Origin seam. Service-auth methods and explicit request-scoped SAS methods
/// are separate; the production implementation keeps GET bodies streaming.
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

    /// Stream one ordinary Put Blob body exactly once.
    async fn put_body(
        &self,
        _object: &ObjectId,
        _headers: &[(String, String)],
        _body: HttpRequestBody,
        _len: u64,
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement Put Blob".into())
    }

    /// Copy one same-account source blob to a destination.
    async fn copy(
        &self,
        _destination: &ObjectId,
        _source: &ObjectId,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement Copy Blob".into())
    }

    /// Execute one bodyless PUT subresource operation.
    async fn put_subresource(
        &self,
        _object: &ObjectId,
        _query: &str,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement blob property mutations".into())
    }

    /// Execute one conditional Delete Blob request.
    async fn delete(
        &self,
        _object: &ObjectId,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement Delete Blob".into())
    }

    /// Stream one Put Block or Put Block List request exactly once.
    async fn put_block_body(
        &self,
        _object: &ObjectId,
        _query: &str,
        _headers: &[(String, String)],
        _body: HttpRequestBody,
        _len: u64,
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement block staging".into())
    }

    /// Stage one block from a same-account source URL.
    async fn put_block_from_url(
        &self,
        _destination: &ObjectId,
        _source: &ObjectId,
        _query: &str,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement Put Block From URL".into())
    }

    /// Get one committed or uncommitted block list.
    async fn get_block_list(
        &self,
        _object: &ObjectId,
        _query: &str,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement Get Block List".into())
    }

    async fn sas_object(
        &self,
        _method: Method,
        _object: &ObjectId,
        _credential: &AzureSas,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement SAS requests".into())
    }

    async fn sas_object_stream(
        &self,
        _method: Method,
        _object: &ObjectId,
        _credential: &AzureSas,
        _headers: &[(String, String)],
    ) -> Result<HttpStreamResponse, String> {
        Err("Azure origin does not implement streaming SAS requests".into())
    }

    async fn sas_object_body(
        &self,
        _method: Method,
        _object: &ObjectId,
        _credential: &AzureSas,
        _headers: &[(String, String)],
        _body: HttpRequestBody,
        _len: u64,
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement SAS request bodies".into())
    }

    async fn sas_container(
        &self,
        _method: Method,
        _container: &str,
        _credential: &AzureSas,
        _headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        Err("Azure origin does not implement container SAS requests".into())
    }
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

    async fn put_body(
        &self,
        object: &ObjectId,
        headers: &[(String, String)],
        body: HttpRequestBody,
        len: u64,
    ) -> Result<HttpResponse, String> {
        self.execute_put_body_raw(object, headers, body, len).await
    }

    async fn copy(
        &self,
        destination: &ObjectId,
        source: &ObjectId,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_copy_raw(destination, source, headers).await
    }

    async fn put_subresource(
        &self,
        object: &ObjectId,
        query: &str,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_put_raw(object, query, headers).await
    }

    async fn delete(
        &self,
        object: &ObjectId,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_delete_raw(object, headers).await
    }

    async fn put_block_body(
        &self,
        object: &ObjectId,
        query: &str,
        headers: &[(String, String)],
        body: HttpRequestBody,
        len: u64,
    ) -> Result<HttpResponse, String> {
        self.execute_put_query_body_raw(object, query, headers, body, len)
            .await
    }

    async fn put_block_from_url(
        &self,
        destination: &ObjectId,
        source: &ObjectId,
        query: &str,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_copy_query_raw(destination, source, query, headers)
            .await
    }

    async fn get_block_list(
        &self,
        object: &ObjectId,
        query: &str,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_get_query_raw(object, query, headers).await
    }

    async fn sas_object(
        &self,
        method: Method,
        object: &ObjectId,
        credential: &AzureSas,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_sas_raw(method, object, credential, headers)
            .await
    }

    async fn sas_object_stream(
        &self,
        method: Method,
        object: &ObjectId,
        credential: &AzureSas,
        headers: &[(String, String)],
    ) -> Result<HttpStreamResponse, String> {
        self.execute_sas_stream_raw(method, object, credential, headers)
            .await
    }

    async fn sas_object_body(
        &self,
        method: Method,
        object: &ObjectId,
        credential: &AzureSas,
        headers: &[(String, String)],
        body: HttpRequestBody,
        len: u64,
    ) -> Result<HttpResponse, String> {
        self.execute_sas_body_raw(method, object, credential, headers, body, len)
            .await
    }

    async fn sas_container(
        &self,
        method: Method,
        container: &str,
        credential: &AzureSas,
        headers: &[(String, String)],
    ) -> Result<HttpResponse, String> {
        self.execute_sas_container_raw(method, container, credential, headers)
            .await
    }
}

/// Azure protocol adapter over a Talon cache and scoped origin identity.
pub struct AzureBlobAdapter {
    config: AzureAdapterConfig,
    cache: Arc<dyn AzureCache>,
    origin: Arc<dyn AzureOrigin>,
    blocks: BlockRegistry,
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
            blocks: BlockRegistry::new(config.max_block_bindings, config.block_binding_ttl),
            config,
            cache,
            origin,
        })
    }
}

#[derive(Clone)]
struct BlockBinding {
    principal: AuthenticatedPrincipal,
    decision: EffectiveDecision,
    touched: Instant,
}

struct BlockRegistry {
    bindings: Mutex<HashMap<ObjectId, BlockBinding>>,
    max_bindings: usize,
    ttl: Duration,
}

impl BlockRegistry {
    fn new(max_bindings: usize, ttl: Duration) -> Self {
        Self {
            bindings: Mutex::new(HashMap::new()),
            max_bindings,
            ttl,
        }
    }

    fn bind_or_authorize(
        &self,
        object: &ObjectId,
        principal: &AuthenticatedPrincipal,
        decision: EffectiveDecision,
        now: Instant,
    ) -> bool {
        let mut bindings = self.bindings.lock().unwrap();
        bindings.retain(|_, binding| now.saturating_duration_since(binding.touched) <= self.ttl);
        if let Some(binding) = bindings.get_mut(object) {
            if binding.principal != *principal || binding.decision != decision {
                return false;
            }
            binding.touched = now;
            return true;
        }
        if bindings.len() >= self.max_bindings {
            return false;
        }
        bindings.insert(
            object.clone(),
            BlockBinding {
                principal: principal.clone(),
                decision,
                touched: now,
            },
        );
        true
    }

    fn authorize(
        &self,
        object: &ObjectId,
        principal: &AuthenticatedPrincipal,
        decision: EffectiveDecision,
        now: Instant,
    ) -> bool {
        let mut bindings = self.bindings.lock().unwrap();
        bindings.retain(|_, binding| now.saturating_duration_since(binding.touched) <= self.ttl);
        let Some(binding) = bindings.get_mut(object) else {
            return false;
        };
        if binding.principal != *principal || binding.decision != decision {
            return false;
        }
        binding.touched = now;
        true
    }

    fn remove(&self, object: &ObjectId) {
        self.bindings.lock().unwrap().remove(object);
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
    blockid: Option<String>,
    blocklisttype: Option<String>,
}

impl AzureQuery {
    fn parse(query: Option<&str>) -> Result<Self, AzureRequestError> {
        validate_percent_encoding(query.unwrap_or_default(), "query")?;
        let mut parsed = Self::default();
        for (key, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
            match key.as_ref() {
                "restype" => set_query_value(&mut parsed.restype, value.into_owned(), "restype")?,
                "comp" => set_query_value(&mut parsed.comp, value.into_owned(), "comp")?,
                "prefix" => set_query_value(&mut parsed.prefix, value.into_owned(), "prefix")?,
                "delimiter" => {
                    set_query_value(&mut parsed.delimiter, value.into_owned(), "delimiter")?
                }
                "marker" => set_query_value(&mut parsed.marker, value.into_owned(), "marker")?,
                "maxresults" => {
                    set_query_value(&mut parsed.maxresults, value.into_owned(), "maxresults")?
                }
                "blockid" => set_query_value(&mut parsed.blockid, value.into_owned(), "blockid")?,
                "blocklisttype" => set_query_value(
                    &mut parsed.blocklisttype,
                    value.into_owned(),
                    "blocklisttype",
                )?,
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

fn set_query_value(
    slot: &mut Option<String>,
    value: String,
    name: &str,
) -> Result<(), AzureRequestError> {
    if slot.replace(value).is_some() {
        return Err(AzureRequestError::invalid(
            "InvalidQueryParameterValue",
            format!("{name} must occur exactly once"),
        ));
    }
    Ok(())
}

#[derive(Debug)]
enum AzureMutation {
    PutBlob,
    Copy {
        source: ObjectId,
    },
    SetMetadata,
    SetProperties,
    Delete,
    PutBlock {
        block_id: String,
        source: Option<ObjectId>,
    },
    PutBlockList,
    GetBlockList {
        kind: BlockListKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockListKind {
    Committed,
    Uncommitted,
    All,
}

impl BlockListKind {
    fn parse(value: Option<&str>) -> Result<Self, AzureRequestError> {
        match value {
            Some("committed") => Ok(Self::Committed),
            Some("uncommitted") => Ok(Self::Uncommitted),
            Some("all") => Ok(Self::All),
            _ => Err(AzureRequestError::invalid(
                "InvalidQueryParameterValue",
                "blocklisttype must be committed, uncommitted, or all",
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Uncommitted => "uncommitted",
            Self::All => "all",
        }
    }
}

fn encode_query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn classify_mutation(
    request: &Request,
    query: &AzureQuery,
    config: &AzureAdapterConfig,
) -> Result<Option<AzureMutation>, AzureRequestError> {
    match *request.method() {
        axum::http::Method::PUT => match query.comp.as_deref() {
            None if request.headers().contains_key("x-ms-copy-source") => {
                Ok(Some(AzureMutation::Copy {
                    source: copy_source(request.headers(), config)?,
                }))
            }
            None => {
                require_block_blob(request.headers())?;
                Ok(Some(AzureMutation::PutBlob))
            }
            Some("metadata") if !request.headers().contains_key("x-ms-copy-source") => {
                Ok(Some(AzureMutation::SetMetadata))
            }
            Some("properties") if !request.headers().contains_key("x-ms-copy-source") => {
                Ok(Some(AzureMutation::SetProperties))
            }
            Some("block") => {
                let block_id = query
                    .blockid
                    .clone()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        AzureRequestError::invalid(
                            "InvalidQueryParameterValue",
                            "blockid is required",
                        )
                    })?;
                let source = request
                    .headers()
                    .contains_key("x-ms-copy-source")
                    .then(|| copy_source(request.headers(), config))
                    .transpose()?;
                Ok(Some(AzureMutation::PutBlock { block_id, source }))
            }
            Some("blocklist") if !request.headers().contains_key("x-ms-copy-source") => {
                Ok(Some(AzureMutation::PutBlockList))
            }
            _ => Err(AzureRequestError::unsupported(
                "the requested blob PUT operation is not supported",
            )),
        },
        axum::http::Method::DELETE if query.comp.is_none() => Ok(Some(AzureMutation::Delete)),
        axum::http::Method::DELETE => Err(AzureRequestError::unsupported(
            "the requested blob DELETE operation is not supported",
        )),
        axum::http::Method::GET if query.comp.as_deref() == Some("blocklist") => {
            Ok(Some(AzureMutation::GetBlockList {
                kind: BlockListKind::parse(query.blocklisttype.as_deref())?,
            }))
        }
        _ => Ok(None),
    }
}

fn require_block_blob(headers: &HeaderMap) -> Result<(), AzureRequestError> {
    let mut values = headers.get_all("x-ms-blob-type").iter();
    let value = values
        .next()
        .and_then(|value| value.to_str().ok())
        .filter(|value| *value == "BlockBlob")
        .ok_or_else(|| {
            AzureRequestError::invalid("InvalidHeaderValue", "x-ms-blob-type must be BlockBlob")
        })?;
    let _ = value;
    if values.next().is_some() {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "x-ms-blob-type must occur exactly once",
        ));
    }
    Ok(())
}

fn copy_source(
    headers: &HeaderMap,
    config: &AzureAdapterConfig,
) -> Result<ObjectId, AzureRequestError> {
    let mut values = headers.get_all("x-ms-copy-source").iter();
    let value = values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            AzureRequestError::invalid("InvalidHeaderValue", "copy source is invalid")
        })?;
    if values.next().is_some() {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "x-ms-copy-source must occur exactly once",
        ));
    }
    let source = url::Url::parse(value)
        .map_err(|_| AzureRequestError::invalid("InvalidHeaderValue", "copy source is invalid"))?;
    if !matches!(source.scheme(), "http" | "https")
        || !source.username().is_empty()
        || source.password().is_some()
        || source.query().is_some()
        || source.fragment().is_some()
    {
        return Err(AzureRequestError::invalid(
            "CopySourceNotSupported",
            "copy source credentials, versions, and fragments are not supported",
        ));
    }
    let segments = source
        .path()
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let public_account = source.host_str().and_then(|host| {
        host.strip_suffix(&format!(".{}", config.endpoint_suffix))
            .map(str::to_string)
    });
    let (account, container_index) = if let Some(account) = public_account {
        (Some(account), 0)
    } else if config.path_style {
        (
            segments
                .first()
                .map(|segment| decode_path(segment))
                .transpose()?,
            1,
        )
    } else {
        return Err(AzureRequestError::invalid(
            "CopySourceNotSupported",
            "copy source must use the configured storage endpoint",
        ));
    };
    if account.as_deref() != Some(config.account.as_str()) {
        return Err(AzureRequestError::invalid(
            "CopySourceNotSupported",
            "copy source must belong to the configured storage account",
        ));
    }
    let container = segments
        .get(container_index)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            AzureRequestError::invalid("InvalidHeaderValue", "copy source is invalid")
        })?;
    let blob = segments
        .get(container_index + 1..)
        .unwrap_or_default()
        .join("/");
    if blob.is_empty() {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "copy source must identify a blob",
        ));
    }
    Ok(ObjectId::new(
        Backend::Azure,
        decode_path(container)?,
        decode_path(&blob)?,
    ))
}

fn single_content_length(headers: &HeaderMap) -> Result<u64, AzureRequestError> {
    if headers.contains_key(header::TRANSFER_ENCODING) {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "Transfer-Encoding is not supported for Azure mutations",
        ));
    }
    let mut values = headers.get_all(header::CONTENT_LENGTH).iter();
    let value = values.next().ok_or_else(|| {
        AzureRequestError::invalid("MissingRequiredHeader", "Content-Length is required")
    })?;
    if values.next().is_some() {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "Content-Length must occur exactly once",
        ));
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            AzureRequestError::invalid("InvalidHeaderValue", "Content-Length is invalid")
        })
}

fn require_empty_body(headers: &HeaderMap) -> Result<(), AzureRequestError> {
    if headers.contains_key(header::TRANSFER_ENCODING) {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "this operation requires an empty body",
        ));
    }
    if headers.contains_key(header::CONTENT_LENGTH) && single_content_length(headers)? != 0 {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "this operation requires an empty body",
        ));
    }
    Ok(())
}

fn mutation_headers(
    headers: &HeaderMap,
    mutation: &AzureMutation,
) -> Result<Vec<(String, String)>, AzureRequestError> {
    const CONDITIONS: [&str; 6] = [
        "if-match",
        "if-none-match",
        "if-modified-since",
        "if-unmodified-since",
        "x-ms-if-tags",
        "x-ms-lease-id",
    ];
    const CONTENT: [&str; 8] = [
        "cache-control",
        "content-disposition",
        "content-encoding",
        "content-language",
        "content-md5",
        "content-type",
        "expires",
        "x-ms-content-crc64",
    ];
    let mut output = Vec::new();
    for (name, value) in headers {
        let name = name.as_str().to_ascii_lowercase();
        let allowed = CONDITIONS.contains(&name.as_str())
            || match mutation {
                AzureMutation::PutBlob => {
                    CONTENT.contains(&name.as_str())
                        || name.starts_with("x-ms-meta-")
                        || matches!(
                            name.as_str(),
                            "x-ms-tags"
                                | "x-ms-access-tier"
                                | "x-ms-encryption-scope"
                                | "x-ms-encryption-key"
                                | "x-ms-encryption-key-sha256"
                        )
                }
                AzureMutation::Copy { .. } => {
                    name.starts_with("x-ms-meta-")
                        || name.starts_with("x-ms-source-if-")
                        || matches!(
                            name.as_str(),
                            "x-ms-requires-sync"
                                | "x-ms-source-lease-id"
                                | "x-ms-source-content-md5"
                                | "x-ms-metadata-directive"
                                | "x-ms-tags"
                                | "x-ms-access-tier"
                        )
                }
                AzureMutation::SetMetadata => name.starts_with("x-ms-meta-"),
                AzureMutation::SetProperties => {
                    CONTENT.contains(&name.as_str()) || name.starts_with("x-ms-blob-content-")
                }
                AzureMutation::Delete => name == "x-ms-delete-snapshots",
                AzureMutation::PutBlock { source, .. } => {
                    matches!(
                        name.as_str(),
                        "content-md5"
                            | "x-ms-content-crc64"
                            | "x-ms-encryption-scope"
                            | "x-ms-encryption-key"
                            | "x-ms-encryption-key-sha256"
                    ) || (source.is_some()
                        && (name.starts_with("x-ms-source-if-")
                            || matches!(
                                name.as_str(),
                                "x-ms-source-range"
                                    | "x-ms-source-content-md5"
                                    | "x-ms-source-content-crc64"
                            )))
                }
                AzureMutation::PutBlockList => {
                    CONTENT.contains(&name.as_str())
                        || name.starts_with("x-ms-blob-content-")
                        || name.starts_with("x-ms-meta-")
                        || matches!(name.as_str(), "x-ms-tags" | "x-ms-access-tier")
                }
                AzureMutation::GetBlockList { .. } => false,
            };
        if !allowed {
            continue;
        }
        if output
            .iter()
            .any(|(existing, _): &(String, String)| existing == &name)
        {
            return Err(AzureRequestError::invalid(
                "InvalidHeaderValue",
                format!("{name} must occur exactly once"),
            ));
        }
        let value = value.to_str().map_err(|_| {
            AzureRequestError::invalid("InvalidHeaderValue", format!("{name} is invalid"))
        })?;
        output.push((name, value.to_string()));
    }
    Ok(output)
}

fn block_principal(request: &Request) -> Result<AuthenticatedPrincipal, AzureRequestError> {
    request
        .extensions()
        .get::<AuthenticatedPrincipal>()
        .cloned()
        .ok_or_else(|| AzureRequestError {
            status: StatusCode::FORBIDDEN,
            code: "AuthorizationFailure",
            message: "the request is not authorized for this block operation".into(),
            failure: FailureReason::Authorization,
            content_range: None,
            indeterminate_commit: false,
        })
}

fn block_cache_decision(headers: &HeaderMap) -> Result<EffectiveDecision, AzureRequestError> {
    let mut values = headers.get_all(AZURE_CACHE_MARK_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(EffectiveDecision::default());
    };
    if values.next().is_some() {
        return Err(AzureRequestError::invalid(
            "InvalidHeaderValue",
            "cache decision must occur exactly once",
        ));
    }
    EffectiveDecision::parse(value.to_str().map_err(|_| {
        AzureRequestError::invalid("InvalidHeaderValue", "cache decision is invalid")
    })?)
    .map_err(|_| AzureRequestError::invalid("InvalidHeaderValue", "cache decision is invalid"))
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
    indeterminate_commit: bool,
}

impl AzureRequestError {
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
            message: "the requested range is not satisfiable".into(),
            failure: FailureReason::InvalidRequest,
            content_range: Some(format!("bytes */{size}")),
            indeterminate_commit: false,
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
            indeterminate_commit: false,
        }
    }

    fn indeterminate_commit() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "InternalError",
            message: "the mutation result is indeterminate; inspect the blob before retrying"
                .into(),
            failure: FailureReason::Origin,
            content_range: None,
            indeterminate_commit: true,
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            code: "UnsupportedHttpVerb",
            message: message.into(),
            failure: FailureReason::Unsupported,
            content_range: None,
            indeterminate_commit: false,
        }
    }

    fn block_binding() -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "BlockListUnavailable",
            message: "the block staging context is unavailable".into(),
            failure: FailureReason::Precondition,
            content_range: None,
            indeterminate_commit: false,
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
            indeterminate_commit: false,
        },
        CacheReadError::VersionMismatch(message) => AzureRequestError {
            status: StatusCode::PRECONDITION_FAILED,
            code: "ConditionNotMet",
            message,
            failure: FailureReason::Precondition,
            content_range: None,
            indeterminate_commit: false,
        },
        CacheReadError::CacheMiss(message) => AzureRequestError {
            status: StatusCode::NOT_FOUND,
            code: "BlobNotFound",
            message,
            failure: FailureReason::CacheUnavailable,
            content_range: None,
            indeterminate_commit: false,
        },
        CacheReadError::Timeout(message) => AzureRequestError {
            status: StatusCode::GATEWAY_TIMEOUT,
            code: "OperationTimedOut",
            message,
            failure: FailureReason::Timeout,
            content_range: None,
            indeterminate_commit: false,
        },
        CacheReadError::Unavailable(message) => AzureRequestError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServerBusy",
            message,
            failure: FailureReason::CacheUnavailable,
            content_range: None,
            indeterminate_commit: false,
        },
        CacheReadError::Origin(message) => AzureRequestError::origin_unavailable(message),
        CacheReadError::RateLimited(message) => AzureRequestError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServerBusy",
            message,
            failure: FailureReason::RateLimited,
            content_range: None,
            indeterminate_commit: false,
        },
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
        Timeout,
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

        fn timeout() -> Arc<Self> {
            Arc::new(Self {
                behavior: CacheBehavior::Timeout,
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
                CacheBehavior::Timeout => Err(CacheReadError::Timeout("slow worker".into())),
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

    impl AzureCache for DemandCache {
        fn stream(&self, _request: AzureCacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
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

    struct UnavailableOrigin;

    #[async_trait]
    impl AzureOrigin for UnavailableOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            Err("connect failed at https://account.example/?sig=secret".into())
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
            _container: &str,
            _prefix: &str,
            _delimiter: Option<&str>,
            _marker: Option<&str>,
            _max_results: u32,
        ) -> Result<HttpResponse, String> {
            unreachable!("test only dispatches object reads")
        }
    }

    #[derive(Default)]
    struct TrackingCache {
        invalidated: Mutex<Vec<ObjectId>>,
    }

    impl AzureCache for TrackingCache {
        fn stream(&self, _request: AzureCacheRequest<'_>) -> Result<CacheStream, CacheReadError> {
            unreachable!("mutation tests do not read from cache")
        }

        fn invalidate_object(&self, object: &ObjectId) -> usize {
            self.invalidated.lock().unwrap().push(object.clone());
            1
        }
    }

    type MutationRecord = (ObjectId, Vec<(String, String)>);
    type CopyRecord = (ObjectId, ObjectId, Vec<(String, String)>);
    type SubresourceRecord = (ObjectId, String, Vec<(String, String)>);

    struct MutationOrigin {
        put_status: u16,
        copy_status: u16,
        subresource_status: u16,
        delete_status: u16,
        put_transport_error: bool,
        put: Mutex<Option<MutationRecord>>,
        put_body: Mutex<Vec<u8>>,
        copy: Mutex<Option<CopyRecord>>,
        subresources: Mutex<Vec<SubresourceRecord>>,
        delete: Mutex<Option<MutationRecord>>,
    }

    impl MutationOrigin {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                put_status: 201,
                copy_status: 202,
                subresource_status: 200,
                delete_status: 202,
                put_transport_error: false,
                put: Mutex::new(None),
                put_body: Mutex::new(Vec::new()),
                copy: Mutex::new(None),
                subresources: Mutex::new(Vec::new()),
                delete: Mutex::new(None),
            })
        }

        fn response(status: u16) -> HttpResponse {
            HttpResponse {
                status,
                headers: Vec::new(),
                body: Bytes::new(),
            }
        }

        fn with_put_result(status: u16, transport_error: bool) -> Arc<Self> {
            let mut origin = Self::new();
            let inner = Arc::get_mut(&mut origin).unwrap();
            inner.put_status = status;
            inner.put_transport_error = transport_error;
            origin
        }

        fn with_delete_status(status: u16) -> Arc<Self> {
            let mut origin = Self::new();
            Arc::get_mut(&mut origin).unwrap().delete_status = status;
            origin
        }
    }

    #[async_trait]
    impl AzureOrigin for MutationOrigin {
        async fn head(
            &self,
            _object: &ObjectId,
            _conditions: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            unreachable!("mutation tests do not issue HEAD")
        }

        async fn get(
            &self,
            _object: &ObjectId,
            _range: Option<(u64, u64)>,
            _conditions: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            unreachable!("mutation tests do not issue GET")
        }

        async fn list(
            &self,
            _container: &str,
            _prefix: &str,
            _delimiter: Option<&str>,
            _marker: Option<&str>,
            _max_results: u32,
        ) -> Result<HttpResponse, String> {
            unreachable!("mutation tests do not issue LIST")
        }

        async fn put_body(
            &self,
            object: &ObjectId,
            headers: &[(String, String)],
            mut body: HttpRequestBody,
            _len: u64,
        ) -> Result<HttpResponse, String> {
            *self.put.lock().unwrap() = Some((object.clone(), headers.to_vec()));
            if self.put_transport_error {
                return Err("connection lost after dispatch".into());
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            *self.put_body.lock().unwrap() = bytes;
            Ok(Self::response(self.put_status))
        }

        async fn copy(
            &self,
            destination: &ObjectId,
            source: &ObjectId,
            headers: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            *self.copy.lock().unwrap() =
                Some((destination.clone(), source.clone(), headers.to_vec()));
            Ok(Self::response(self.copy_status))
        }

        async fn put_subresource(
            &self,
            object: &ObjectId,
            query: &str,
            headers: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            self.subresources.lock().unwrap().push((
                object.clone(),
                query.to_string(),
                headers.to_vec(),
            ));
            Ok(Self::response(self.subresource_status))
        }

        async fn delete(
            &self,
            object: &ObjectId,
            headers: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            *self.delete.lock().unwrap() = Some((object.clone(), headers.to_vec()));
            Ok(Self::response(self.delete_status))
        }
    }

    fn context() -> GatewayRequestContext {
        GatewayRequestContext {
            request_id: "00112233-4455-4677-8899-aabbccddeeff".into(),
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

    fn body_request(method: axum::http::Method, uri: &str, body: &'static [u8]) -> Request {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "acct.blob.core.windows.net")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
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
    async fn cache_timeout_is_eligible_for_origin_fallback() {
        let origin = MockOrigin::object(&[b"abc", b"def"]);
        let response = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            MockCache::timeout(),
            origin,
        )
        .handle(
            request(axum::http::Method::GET, "/container/blob"),
            context(),
        )
        .await;

        assert_eq!(response.response.status(), StatusCode::OK);
        assert_eq!(response.outcome, GatewayOutcome::Fallback);
    }

    #[tokio::test]
    async fn cache_stream_respects_backpressure_and_cancellation() {
        let cache = DemandCache::new();
        let response = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::clone(&cache) as Arc<dyn AzureCache>,
            MockOrigin::object(&[]),
        )
        .handle(
            request(axum::http::Method::GET, "/container/blob"),
            context(),
        )
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
    async fn origin_outage_returns_a_sanitized_azure_error() {
        let response = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            MockCache::data(&[]),
            Arc::new(UnavailableOrigin),
        )
        .handle(
            request(axum::http::Method::GET, "/container/blob"),
            context(),
        )
        .await;

        assert_eq!(response.response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.response.headers()["x-ms-error-code"], "ServerBusy");
        let body = to_bytes(response.response.into_body(), 4096).await.unwrap();
        assert!(!body.windows(6).any(|window| window == b"secret"));
        assert!(!body.windows(4).any(|window| window == b"sig="));
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
            "00112233-4455-4677-8899-aabbccddeeff"
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

    #[test]
    fn copy_operations_require_write_on_destination_and_read_on_source() {
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::new(TrackingCache::default()),
            MutationOrigin::new(),
        );
        let mut copy = request(axum::http::Method::PUT, "/dest/copied.bin");
        copy.headers_mut().insert(
            "x-ms-copy-source",
            HeaderValue::from_static("https://acct.blob.core.windows.net/source/original.bin"),
        );

        let mut block = request(
            axum::http::Method::PUT,
            "/dest/copied.bin?comp=block&blockid=YQ%3D%3D",
        );
        block.headers_mut().insert(
            "x-ms-copy-source",
            HeaderValue::from_static("https://acct.blob.core.windows.net/source/original.bin"),
        );

        for request in [&copy, &block] {
            let access = adapter.classify_access(request).unwrap();
            assert_eq!(access.operation, GatewayOperation::Write);
            assert_eq!(
                access.target,
                GatewayTarget::Object(ObjectId::new(Backend::Azure, "dest", "copied.bin"))
            );
            assert_eq!(access.additional.len(), 1);
            assert_eq!(access.additional[0].operation, GatewayOperation::Read);
            assert_eq!(
                access.additional[0].target,
                GatewayTarget::Object(ObjectId::new(Backend::Azure, "source", "original.bin"))
            );
        }
    }

    #[test]
    fn block_queries_reject_duplicate_ids_and_preserve_decoded_bytes() {
        assert!(AzureQuery::parse(Some("comp=block&blockid=YQ%3D%3D&blockid=Yg%3D%3D")).is_err());
        let query = AzureQuery::parse(Some("comp=block&blockid=YStiLw%3D%3D")).unwrap();
        let request = request(
            axum::http::Method::PUT,
            "/c/blob?comp=block&blockid=YStiLw%3D%3D",
        );
        let mutation =
            classify_mutation(&request, &query, &AzureAdapterConfig::public_cloud("acct"))
                .unwrap()
                .unwrap();
        assert!(
            matches!(mutation, AzureMutation::PutBlock { block_id, source: None } if block_id == "YStiLw==")
        );
    }

    #[test]
    fn copy_rejects_credentials_and_cross_account_sources_before_dispatch() {
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::new(TrackingCache::default()),
            MutationOrigin::new(),
        );
        for source in [
            "https://acct.blob.core.windows.net/source/blob?sig=secret",
            "https://other.blob.core.windows.net/source/blob",
            "https://attacker.invalid/acct/source/blob",
        ] {
            let mut request = request(axum::http::Method::PUT, "/dest/blob");
            request
                .headers_mut()
                .insert("x-ms-copy-source", HeaderValue::from_str(source).unwrap());
            let error = adapter.classify_access(&request).unwrap_err();
            assert_eq!(error.code, "CopySourceNotSupported");
        }
    }

    #[tokio::test]
    async fn mutations_reject_transfer_encoded_bodies_before_origin_dispatch() {
        let origin = MutationOrigin::new();
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::new(TrackingCache::default()),
            Arc::clone(&origin) as Arc<dyn AzureOrigin>,
        );
        let mut put = request(axum::http::Method::PUT, "/container/blob");
        put.headers_mut()
            .insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
        put.headers_mut().insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );

        let response = adapter.handle(put, context()).await;

        assert_eq!(response.response.status(), StatusCode::BAD_REQUEST);
        assert!(origin.put.lock().unwrap().is_none());

        let mut delete = request(axum::http::Method::DELETE, "/container/blob");
        delete.headers_mut().insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        let response = adapter.handle(delete, context()).await;
        assert_eq!(response.response.status(), StatusCode::BAD_REQUEST);
        assert!(origin.delete.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn put_streams_once_forwards_only_allowed_headers_and_invalidates_on_success() {
        let cache = Arc::new(TrackingCache::default());
        let origin = MutationOrigin::new();
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::clone(&cache) as Arc<dyn AzureCache>,
            Arc::clone(&origin) as Arc<dyn AzureOrigin>,
        );
        let mut request =
            body_request(axum::http::Method::PUT, "/container/blob", b"streamed-body");
        for (name, value) in [
            ("x-ms-blob-type", "BlockBlob"),
            ("content-type", "application/custom"),
            ("x-ms-meta-owner", "talon"),
            ("if-none-match", "*"),
            ("authorization", "SharedKey attacker:secret"),
            ("x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT"),
            ("x-ms-version", "2099-01-01"),
        ] {
            request.headers_mut().insert(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }

        let response = adapter.handle(request, context()).await;

        assert_eq!(response.response.status(), StatusCode::CREATED);
        assert_eq!(*origin.put_body.lock().unwrap(), b"streamed-body");
        let put = origin.put.lock().unwrap();
        let headers = &put.as_ref().unwrap().1;
        assert!(headers
            .iter()
            .any(|item| item == &("content-type".into(), "application/custom".into())));
        assert!(headers
            .iter()
            .any(|item| item == &("x-ms-meta-owner".into(), "talon".into())));
        assert!(headers
            .iter()
            .any(|item| item == &("if-none-match".into(), "*".into())));
        assert!(!headers.iter().any(|(name, _)| matches!(
            name.as_str(),
            "authorization" | "x-ms-date" | "x-ms-version" | "x-ms-blob-type"
        )));
        assert_eq!(cache.invalidated.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn failed_and_indeterminate_puts_do_not_invalidate_cache() {
        for (status, transport_error, expected_status, indeterminate) in [
            (412, false, StatusCode::PRECONDITION_FAILED, false),
            (201, true, StatusCode::INTERNAL_SERVER_ERROR, true),
        ] {
            let cache = Arc::new(TrackingCache::default());
            let origin = MutationOrigin::with_put_result(status, transport_error);
            let adapter = adapter(
                AzureAdapterConfig::public_cloud("acct"),
                Arc::clone(&cache) as Arc<dyn AzureCache>,
                origin,
            );
            let mut request = body_request(axum::http::Method::PUT, "/container/blob", b"data");
            request
                .headers_mut()
                .insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));

            let response = adapter.handle(request, context()).await;

            assert_eq!(response.response.status(), expected_status);
            assert_eq!(
                response
                    .response
                    .headers()
                    .get("x-talon-commit-state")
                    .is_some(),
                indeterminate
            );
            assert!(cache.invalidated.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn copy_metadata_properties_and_delete_dispatch_canonical_operations() {
        let cache = Arc::new(TrackingCache::default());
        let origin = MutationOrigin::new();
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::clone(&cache) as Arc<dyn AzureCache>,
            Arc::clone(&origin) as Arc<dyn AzureOrigin>,
        );

        let mut copy = request(axum::http::Method::PUT, "/dest/copied.bin");
        copy.headers_mut().insert(
            "x-ms-copy-source",
            HeaderValue::from_static("https://acct.blob.core.windows.net/source/original.bin"),
        );
        copy.headers_mut().insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("SharedKey attacker:secret"),
        );
        assert_eq!(
            adapter.handle(copy, context()).await.response.status(),
            StatusCode::ACCEPTED
        );
        {
            let copy = origin.copy.lock().unwrap();
            let (_, source, headers) = copy.as_ref().unwrap();
            assert_eq!(
                source,
                &ObjectId::new(Backend::Azure, "source", "original.bin")
            );
            assert!(!headers
                .iter()
                .any(|(name, _)| name == "authorization" || name == "x-ms-copy-source"));
        }

        let mut metadata = request(axum::http::Method::PUT, "/dest/copied.bin?comp=metadata");
        metadata
            .headers_mut()
            .insert("x-ms-meta-owner", HeaderValue::from_static("talon"));
        assert_eq!(
            adapter.handle(metadata, context()).await.response.status(),
            StatusCode::OK
        );
        let mut properties = request(axum::http::Method::PUT, "/dest/copied.bin?comp=properties");
        properties.headers_mut().insert(
            "x-ms-blob-content-type",
            HeaderValue::from_static("application/octet-stream"),
        );
        assert_eq!(
            adapter
                .handle(properties, context())
                .await
                .response
                .status(),
            StatusCode::OK
        );
        {
            let subresources = origin.subresources.lock().unwrap();
            assert_eq!(subresources[0].1, "comp=metadata");
            assert_eq!(subresources[1].1, "comp=properties");
        }

        assert_eq!(
            adapter
                .handle(
                    request(axum::http::Method::DELETE, "/dest/copied.bin"),
                    context()
                )
                .await
                .response
                .status(),
            StatusCode::ACCEPTED
        );
        assert_eq!(cache.invalidated.lock().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn delete_not_found_confirms_absence_and_invalidates_cache() {
        let cache = Arc::new(TrackingCache::default());
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::clone(&cache) as Arc<dyn AzureCache>,
            MutationOrigin::with_delete_status(404),
        );

        let response = adapter
            .handle(
                request(axum::http::Method::DELETE, "/container/missing"),
                context(),
            )
            .await;

        assert_eq!(response.response.status(), StatusCode::NOT_FOUND);
        assert_eq!(cache.invalidated.lock().unwrap().len(), 1);
    }

    struct BlockOrigin {
        calls: Mutex<Vec<(String, Vec<u8>)>>,
        commit_status: Mutex<u16>,
    }

    impl BlockOrigin {
        fn new(commit_status: u16) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                commit_status: Mutex::new(commit_status),
            })
        }
    }

    #[async_trait]
    impl AzureOrigin for BlockOrigin {
        async fn head(&self, _: &ObjectId, _: &[(String, String)]) -> Result<HttpResponse, String> {
            unreachable!()
        }
        async fn get(
            &self,
            _: &ObjectId,
            _: Option<(u64, u64)>,
            _: &[(String, String)],
        ) -> Result<HttpStreamResponse, String> {
            unreachable!()
        }
        async fn list(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: u32,
        ) -> Result<HttpResponse, String> {
            unreachable!()
        }

        async fn put_block_body(
            &self,
            _: &ObjectId,
            query: &str,
            _: &[(String, String)],
            mut body: HttpRequestBody,
            _: u64,
        ) -> Result<HttpResponse, String> {
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            self.calls.lock().unwrap().push((query.into(), bytes));
            let status = if query == "comp=blocklist" {
                *self.commit_status.lock().unwrap()
            } else {
                201
            };
            Ok(MutationOrigin::response(status))
        }

        async fn get_block_list(
            &self,
            _: &ObjectId,
            query: &str,
            _: &[(String, String)],
        ) -> Result<HttpResponse, String> {
            self.calls.lock().unwrap().push((query.into(), Vec::new()));
            Ok(HttpResponse {
                status: 200,
                headers: vec![("content-type".into(), "application/xml".into())],
                body: Bytes::from_static(b"<BlockList/>"),
            })
        }
    }

    fn authenticated(mut request: Request, id: &str) -> Request {
        request
            .extensions_mut()
            .insert(AuthenticatedPrincipal::new(id, "acct"));
        request
    }

    #[test]
    fn block_registry_is_bounded_expires_and_matches_binding() {
        let registry = BlockRegistry::new(1, Duration::from_secs(5));
        let first = ObjectId::new(Backend::Azure, "c", "first");
        let second = ObjectId::new(Backend::Azure, "c", "second");
        let principal = AuthenticatedPrincipal::new("client", "acct");
        let now = Instant::now();
        assert!(registry.bind_or_authorize(&first, &principal, EffectiveDecision::default(), now));
        assert!(!registry.bind_or_authorize(
            &first,
            &AuthenticatedPrincipal::new("other", "acct"),
            EffectiveDecision::default(),
            now
        ));
        assert!(!registry.authorize(&first, &principal, EffectiveDecision::ORIGIN_ONLY, now));
        assert!(!registry.bind_or_authorize(
            &second,
            &principal,
            EffectiveDecision::default(),
            now
        ));
        assert!(registry.bind_or_authorize(
            &second,
            &principal,
            EffectiveDecision::default(),
            now + Duration::from_secs(6)
        ));
    }

    #[tokio::test]
    async fn block_commit_preserves_body_and_only_success_removes_binding() {
        let cache = Arc::new(TrackingCache::default());
        let origin = BlockOrigin::new(400);
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::clone(&cache) as Arc<dyn AzureCache>,
            Arc::clone(&origin) as Arc<dyn AzureOrigin>,
        );
        let stage = authenticated(
            body_request(
                axum::http::Method::PUT,
                "/c/blob?comp=block&blockid=YStiLw%3D%3D",
                b"block",
            ),
            "client",
        );
        assert_eq!(
            adapter.handle(stage, context()).await.response.status(),
            StatusCode::CREATED
        );

        let xml = b"<?xml version=\"1.0\"?><BlockList><Latest>YStiLw==</Latest></BlockList>";
        let failed = authenticated(
            body_request(axum::http::Method::PUT, "/c/blob?comp=blocklist", xml),
            "client",
        );
        assert_eq!(
            adapter.handle(failed, context()).await.response.status(),
            StatusCode::BAD_REQUEST
        );
        assert!(cache.invalidated.lock().unwrap().is_empty());

        *origin.commit_status.lock().unwrap() = 201;
        let committed = authenticated(
            body_request(axum::http::Method::PUT, "/c/blob?comp=blocklist", xml),
            "client",
        );
        assert_eq!(
            adapter.handle(committed, context()).await.response.status(),
            StatusCode::CREATED
        );
        assert_eq!(cache.invalidated.lock().unwrap().len(), 1);
        {
            let calls = origin.calls.lock().unwrap();
            assert_eq!(calls[0].0, "comp=block&blockid=YStiLw%3D%3D");
            assert_eq!(calls[1].1, xml);
            assert_eq!(calls[2].1, xml);
        }

        let repeated = authenticated(
            body_request(axum::http::Method::PUT, "/c/blob?comp=blocklist", xml),
            "client",
        );
        assert_eq!(
            adapter.handle(repeated, context()).await.response.status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn uncommitted_list_requires_binding_but_committed_list_does_not() {
        let origin = BlockOrigin::new(201);
        let adapter = adapter(
            AzureAdapterConfig::public_cloud("acct"),
            Arc::new(TrackingCache::default()),
            origin,
        );
        let committed = request(
            axum::http::Method::GET,
            "/c/blob?comp=blocklist&blocklisttype=committed",
        );
        assert_eq!(
            adapter.handle(committed, context()).await.response.status(),
            StatusCode::OK
        );
        let uncommitted = authenticated(
            request(
                axum::http::Method::GET,
                "/c/blob?comp=blocklist&blocklisttype=uncommitted",
            ),
            "client",
        );
        assert_eq!(
            adapter
                .handle(uncommitted, context())
                .await
                .response
                .status(),
            StatusCode::CONFLICT
        );
    }
}

#[async_trait]
impl GatewayAdapter for AzureBlobAdapter {
    fn protocol(&self) -> ProviderProtocol {
        ProviderProtocol::Azure
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

impl AzureBlobAdapter {
    fn classify_access(&self, request: &Request) -> Result<GatewayAccess, AzureRequestError> {
        let target = parse_target(request, &self.config)?;
        let query = AzureQuery::parse(request.uri().query())?;
        if request.method() == axum::http::Method::GET && target.blob.is_none() && query.is_list() {
            return Ok(GatewayAccess {
                operation: GatewayOperation::List,
                provider_account: Some(self.config.account.clone()),
                target: GatewayTarget::Namespace {
                    backend: Backend::Azure,
                    namespace: target.container,
                    prefix: query.prefix,
                },
                additional: Vec::new(),
            });
        }
        let blob = target.blob.ok_or_else(|| {
            AzureRequestError::invalid("InvalidUri", "request does not identify a blob")
        })?;
        let mutation = classify_mutation(request, &query, &self.config)?;
        let operation = match mutation.as_ref() {
            Some(AzureMutation::Delete) => GatewayOperation::Delete,
            Some(AzureMutation::GetBlockList { .. }) => GatewayOperation::Read,
            Some(_) => GatewayOperation::Write,
            None => match *request.method() {
                axum::http::Method::HEAD => GatewayOperation::Stat,
                axum::http::Method::GET => GatewayOperation::Read,
                _ => GatewayOperation::Unsupported,
            },
        };
        let mut access = GatewayAccess {
            operation,
            provider_account: Some(self.config.account.clone()),
            target: GatewayTarget::Object(ObjectId::new(Backend::Azure, target.container, blob)),
            additional: Vec::new(),
        };
        let source = match mutation {
            Some(AzureMutation::Copy { source }) => Some(source),
            Some(AzureMutation::PutBlock { source, .. }) => source,
            _ => None,
        };
        if let Some(source) = source {
            access.additional.push(crate::GatewayAccessRequirement {
                operation: GatewayOperation::Read,
                provider_account: Some(self.config.account.clone()),
                target: GatewayTarget::Object(source),
            });
        }
        Ok(access)
    }

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
        if let Some(mutation) = classify_mutation(&request, &query, &self.config)? {
            return match mutation {
                AzureMutation::PutBlob => self.put(object, request, context).await,
                AzureMutation::Copy { source } => {
                    self.copy(object, source, request.headers(), context).await
                }
                AzureMutation::SetMetadata => {
                    self.put_subresource(object, "comp=metadata", request.headers(), context)
                        .await
                }
                AzureMutation::SetProperties => {
                    self.put_subresource(object, "comp=properties", request.headers(), context)
                        .await
                }
                AzureMutation::Delete => self.delete(object, request.headers(), context).await,
                AzureMutation::PutBlock { block_id, source } => {
                    self.put_block(object, block_id, source, request, context)
                        .await
                }
                AzureMutation::PutBlockList => self.put_block_list(object, request, context).await,
                AzureMutation::GetBlockList { kind } => {
                    self.get_block_list(object, kind, request, context).await
                }
            };
        }
        match *request.method() {
            axum::http::Method::HEAD => self.head(object, request.headers(), context).await,
            axum::http::Method::GET => self.get(object, request.headers(), context).await,
            _ => Err(AzureRequestError::unsupported(
                "the requested HTTP method is not supported",
            )),
        }
    }

    async fn put_block(
        &self,
        object: ObjectId,
        block_id: String,
        source: Option<ObjectId>,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        let principal = block_principal(&request)?;
        let decision = block_cache_decision(request.headers())?;
        let mutation = AzureMutation::PutBlock {
            block_id: block_id.clone(),
            source: source.clone(),
        };
        let headers = mutation_headers(request.headers(), &mutation)?;
        let len = if source.is_some() {
            require_empty_body(request.headers())?;
            None
        } else {
            Some(single_content_length(request.headers())?)
        };
        if !self
            .blocks
            .bind_or_authorize(&object, &principal, decision, Instant::now())
        {
            return Err(AzureRequestError::block_binding());
        }
        let query = format!("comp=block&blockid={}", encode_query_value(&block_id));
        let response = if let Some(source) = source {
            self.origin
                .put_block_from_url(&object, &source, &query, &headers)
                .await
        } else {
            let len = len.expect("Put Block body length was validated");
            let body = request
                .into_body()
                .into_data_stream()
                .map(|chunk| chunk.map_err(|error| error.to_string()));
            self.origin
                .put_block_body(&object, &query, &headers, Box::pin(body), len)
                .await
        }
        .map_err(|_| AzureRequestError::indeterminate_commit())?;
        Ok(raw_buffered_response(
            response,
            GatewayOperation::Write,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn put_block_list(
        &self,
        object: ObjectId,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        let principal = block_principal(&request)?;
        let decision = block_cache_decision(request.headers())?;
        if !self
            .blocks
            .authorize(&object, &principal, decision, Instant::now())
        {
            return Err(AzureRequestError::block_binding());
        }
        let len = single_content_length(request.headers())?;
        let headers = mutation_headers(request.headers(), &AzureMutation::PutBlockList)?;
        let body = request
            .into_body()
            .into_data_stream()
            .map(|chunk| chunk.map_err(|error| error.to_string()));
        let response = self
            .origin
            .put_block_body(&object, "comp=blocklist", &headers, Box::pin(body), len)
            .await
            .map_err(|_| AzureRequestError::indeterminate_commit())?;
        if response.is_success() {
            self.blocks.remove(&object);
            let _ = self.cache.invalidate_object(&object);
        }
        Ok(raw_buffered_response(
            response,
            GatewayOperation::Write,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn get_block_list(
        &self,
        object: ObjectId,
        kind: BlockListKind,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        if kind != BlockListKind::Committed {
            let principal = block_principal(&request)?;
            let decision = block_cache_decision(request.headers())?;
            if !self
                .blocks
                .authorize(&object, &principal, decision, Instant::now())
            {
                return Err(AzureRequestError::block_binding());
            }
        }
        require_empty_body(request.headers())?;
        let mutation = AzureMutation::GetBlockList { kind };
        let headers = mutation_headers(request.headers(), &mutation)?;
        let query = format!("comp=blocklist&blocklisttype={}", kind.as_str());
        let response = self
            .origin
            .get_block_list(&object, &query, &headers)
            .await
            .map_err(AzureRequestError::origin_unavailable)?;
        Ok(raw_buffered_response(
            response,
            GatewayOperation::Read,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn put(
        &self,
        object: ObjectId,
        request: Request,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        let len = single_content_length(request.headers())?;
        let headers = mutation_headers(request.headers(), &AzureMutation::PutBlob)?;
        let body = request
            .into_body()
            .into_data_stream()
            .map(|chunk| chunk.map_err(|error| error.to_string()));
        let response = self
            .origin
            .put_body(&object, &headers, Box::pin(body), len)
            .await
            .map_err(|_| AzureRequestError::indeterminate_commit())?;
        if response.is_success() {
            let _ = self.cache.invalidate_object(&object);
        }
        let mut response = raw_buffered_response(
            response,
            GatewayOperation::Write,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(object)),
            context,
        );
        response.requested_bytes = len;
        Ok(response)
    }

    async fn copy(
        &self,
        destination: ObjectId,
        source: ObjectId,
        headers: &HeaderMap,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        require_empty_body(headers)?;
        let headers = mutation_headers(
            headers,
            &AzureMutation::Copy {
                source: source.clone(),
            },
        )?;
        let response = self
            .origin
            .copy(&destination, &source, &headers)
            .await
            .map_err(|_| AzureRequestError::indeterminate_commit())?;
        if response.is_success() {
            let _ = self.cache.invalidate_object(&destination);
        }
        Ok(raw_buffered_response(
            response,
            GatewayOperation::Write,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(destination)),
            context,
        ))
    }

    async fn put_subresource(
        &self,
        object: ObjectId,
        query: &str,
        headers: &HeaderMap,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        require_empty_body(headers)?;
        let mutation = if query == "comp=metadata" {
            AzureMutation::SetMetadata
        } else {
            AzureMutation::SetProperties
        };
        let headers = mutation_headers(headers, &mutation)?;
        let response = self
            .origin
            .put_subresource(&object, query, &headers)
            .await
            .map_err(|_| AzureRequestError::indeterminate_commit())?;
        if response.is_success() {
            let _ = self.cache.invalidate_object(&object);
        }
        Ok(raw_buffered_response(
            response,
            GatewayOperation::Write,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(object)),
            context,
        ))
    }

    async fn delete(
        &self,
        object: ObjectId,
        headers: &HeaderMap,
        context: &GatewayRequestContext,
    ) -> Result<GatewayResponse, AzureRequestError> {
        require_empty_body(headers)?;
        let headers = mutation_headers(headers, &AzureMutation::Delete)?;
        let response = self
            .origin
            .delete(&object, &headers)
            .await
            .map_err(|_| AzureRequestError::indeterminate_commit())?;
        if response.is_success() || response.status == 404 {
            let _ = self.cache.invalidate_object(&object);
        }
        Ok(raw_buffered_response(
            response,
            GatewayOperation::Delete,
            GatewayRoute::Origin,
            Some(GatewayTarget::Object(object)),
            context,
        ))
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
