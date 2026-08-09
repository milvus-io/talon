//! Provider-neutral, bounded HTTP runtime shared by Talon's object-store gateways.

pub mod azure;
mod cache_mark;
mod config;
mod metrics;
mod model;
mod runtime;
pub mod s3;

pub use cache_mark::{
    CacheFallback, CacheLookup, CacheMarkError, CachePopulation, EffectiveDecision,
    AZURE_CACHE_MARK_HEADER, S3_CACHE_MARK_HEADER,
};
pub use config::{GatewayConfig, GatewayConfigError, GatewayMode, GatewaySecurity};
pub use metrics::GatewayMetrics;
pub use model::{
    FailureReason, GatewayAdapter, GatewayOperation, GatewayOutcome, GatewayRequestContext,
    GatewayResponse, GatewayRoute, GatewayTarget, ProviderProtocol,
};
pub use runtime::{gateway_router, serve, GatewayReadiness, GatewayRuntime, REQUEST_ID_HEADER};
