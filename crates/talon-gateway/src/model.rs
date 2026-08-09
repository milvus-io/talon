//! Provider-neutral request and result contract.

use std::time::Instant;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::Request;
use axum::response::Response;
use talon_core::{Backend, ObjectId};

/// Object-store protocol served by one gateway process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderProtocol {
    /// Amazon S3-compatible HTTP API.
    S3,
    /// Azure Blob Storage-compatible HTTP API.
    Azure,
}

impl ProviderProtocol {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Azure => "azure",
        }
    }
}

/// Provider-neutral operation used by policy and bounded-cardinality metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GatewayOperation {
    /// Resolve object metadata.
    Stat,
    /// Read a full object or one byte range.
    Read,
    /// List a namespace or prefix.
    List,
    /// Create or replace object data.
    Write,
    /// Delete object data.
    Delete,
    /// An operation intentionally outside the compatibility matrix.
    Unsupported,
}

impl GatewayOperation {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Stat => "stat",
            Self::Read => "read",
            Self::List => "list",
            Self::Write => "write",
            Self::Delete => "delete",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Canonical target produced by a protocol adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayTarget {
    /// One canonical Talon object.
    Object(ObjectId),
    /// A provider namespace listing. Prefixes stay out of logs and metrics.
    Namespace {
        /// Canonical backend family.
        backend: Backend,
        /// Bucket or container.
        namespace: String,
        /// Optional provider prefix.
        prefix: Option<String>,
    },
}

/// Internal cache/origin route selected after authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayRoute {
    /// Use Talon and allow worker read-through population.
    Cache,
    /// Use Talon but never fetch missing bytes from the origin.
    CacheOnly,
    /// Bypass Talon and call the origin directly.
    Origin,
    /// No data route was selected, for validation/auth failures.
    None,
}

impl GatewayRoute {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::CacheOnly => "cache_only",
            Self::Origin => "origin",
            Self::None => "none",
        }
    }
}

/// Provider-neutral request outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayOutcome {
    /// Bytes came from an already resident cache entry.
    Hit,
    /// A worker filled missing bytes from the origin.
    Fill,
    /// The request intentionally bypassed Talon.
    Bypass,
    /// Cache infrastructure failed and policy allowed direct-origin fallback.
    Fallback,
    /// Cache-only routing found no resident bytes.
    CacheOnlyMiss,
    /// Metadata or a body completed without a cache classification.
    Complete,
    /// The request failed before completion.
    Failed,
}

impl GatewayOutcome {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Fill => "fill",
            Self::Bypass => "bypass",
            Self::Fallback => "fallback",
            Self::CacheOnlyMiss => "cache_only_miss",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

/// Stable failure dimensions. Diagnostic strings are deliberately excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// Request syntax or range was invalid.
    InvalidRequest,
    /// Client authentication failed.
    Authentication,
    /// The authenticated principal lacks permission.
    Authorization,
    /// The authoritative object does not exist.
    NotFound,
    /// A conditional request failed.
    Precondition,
    /// Cache infrastructure was unavailable.
    CacheUnavailable,
    /// A cache or origin deadline elapsed.
    Timeout,
    /// The origin returned an operation failure.
    Origin,
    /// A framing or internal invariant failed.
    Internal,
    /// The operation is not in the advertised compatibility matrix.
    Unsupported,
}

impl FailureReason {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::Authentication => "authentication",
            Self::Authorization => "authorization",
            Self::NotFound => "not_found",
            Self::Precondition => "precondition",
            Self::CacheUnavailable => "cache_unavailable",
            Self::Timeout => "timeout",
            Self::Origin => "origin",
            Self::Internal => "internal",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Per-request context generated before provider parsing.
#[derive(Debug, Clone)]
pub struct GatewayRequestContext {
    /// Stable opaque ID returned on every response.
    pub request_id: String,
    /// Monotonic request start used for latency accounting.
    pub started: Instant,
}

/// Provider-neutral resource and operation resolved without backend access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAccess {
    /// Operation requested by the client.
    pub operation: GatewayOperation,
    /// Provider account fixed by the listener, when the protocol exposes one.
    pub provider_account: Option<String>,
    /// Canonical namespace and optional object or listing prefix.
    pub target: GatewayTarget,
    /// Additional resources that the same request must be authorized to access.
    pub additional: Vec<GatewayAccessRequirement>,
}

/// One secondary authorization requirement for a multi-resource operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAccessRequirement {
    /// Required operation on the secondary target.
    pub operation: GatewayOperation,
    /// Provider tenant or storage account, when protocol parsing establishes it.
    pub provider_account: Option<String>,
    /// Canonical secondary target.
    pub target: GatewayTarget,
}

/// Adapter response plus provider-neutral accounting metadata.
pub struct GatewayResponse {
    /// Provider-correct status, headers, and streaming body.
    pub response: Response<Body>,
    /// Parsed operation.
    pub operation: GatewayOperation,
    /// Canonical target, retained for policy but never used as a metric label.
    pub target: Option<GatewayTarget>,
    /// Selected route.
    pub route: GatewayRoute,
    /// Result classification.
    pub outcome: GatewayOutcome,
    /// Stable failure class, if any.
    pub failure: Option<FailureReason>,
    /// Bytes requested by the client, when known.
    pub requested_bytes: u64,
    /// Bytes read through Talon workers.
    pub cache_bytes: u64,
    /// Bytes read directly from the origin.
    pub origin_bytes: u64,
}

impl GatewayResponse {
    /// Construct a response with conservative, non-data defaults.
    pub fn new(response: Response<Body>, operation: GatewayOperation) -> Self {
        Self {
            response,
            operation,
            target: None,
            route: GatewayRoute::None,
            outcome: GatewayOutcome::Complete,
            failure: None,
            requested_bytes: 0,
            cache_bytes: 0,
            origin_bytes: 0,
        }
    }
}

/// Protocol-specific parser/authenticator/response translator.
#[async_trait]
pub trait GatewayAdapter: Send + Sync + 'static {
    /// Protocol label for metrics and logs.
    fn protocol(&self) -> ProviderProtocol;

    /// Resolve an authorization target without cache or origin access.
    fn access(
        &self,
        _request: &Request,
        _context: &GatewayRequestContext,
    ) -> Result<GatewayAccess, Box<GatewayResponse>> {
        let mut response = GatewayResponse::new(
            Response::builder()
                .status(501)
                .body(Body::empty())
                .expect("static response is valid"),
            GatewayOperation::Unsupported,
        );
        response.outcome = GatewayOutcome::Failed;
        response.failure = Some(FailureReason::Unsupported);
        Err(Box::new(response))
    }

    /// Execute and translate one request after shared authorization.
    async fn handle(&self, request: Request, context: GatewayRequestContext) -> GatewayResponse;
}
