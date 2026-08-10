//! Provider-neutral, bounded HTTP runtime shared by Talon's object-store gateways.

mod audit;
mod authorization;
pub mod azure;
pub mod azure_auth;
mod cache_mark;
mod config;
mod metrics;
mod model;
mod runtime;
pub mod s3;
pub mod s3_auth;
mod tls;

pub use audit::{GatewayAuditSink, SecurityAuditEvent};
pub use authorization::{
    AuthenticatedPrincipal, AuthorizationGrant, AuthorizationPolicy, AuthorizationPolicyError,
    GatewayAuthenticationError, GatewayAuthenticator,
};
pub use cache_mark::{
    CacheFallback, CacheLookup, CacheMarkError, CachePopulation, EffectiveDecision,
    AZURE_CACHE_MARK_HEADER, S3_CACHE_MARK_HEADER,
};
pub use config::{GatewayConfig, GatewayConfigError, GatewayMode, GatewaySecurity, OriginAuthMode};
pub use metrics::GatewayMetrics;
pub use model::{
    FailureReason, GatewayAccess, GatewayAccessRequirement, GatewayAdapter, GatewayOperation,
    GatewayOutcome, GatewayRequestContext, GatewayResponse, GatewayRoute, GatewayTarget,
    ProviderProtocol,
};
pub use runtime::{
    gateway_router, serve, serve_tls, GatewayReadiness, GatewayRuntime, GatewayTlsServeError,
    REQUEST_ID_HEADER,
};
pub use tls::{
    GatewayClientAuthConfig, GatewayClientAuthMode, GatewayConnectionInfo, GatewayMtlsIdentity,
    GatewayTlsConfig, GatewayTlsError, GatewayTlsListener,
};
