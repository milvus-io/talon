//! Provider-neutral, bounded HTTP runtime shared by Talon's object-store gateways.

mod config;
mod metrics;
mod model;
mod runtime;

pub use config::{GatewayConfig, GatewayConfigError, GatewayMode, GatewaySecurity};
pub use metrics::GatewayMetrics;
pub use model::{
    FailureReason, GatewayAdapter, GatewayOperation, GatewayOutcome, GatewayRequestContext,
    GatewayResponse, GatewayRoute, GatewayTarget, ProviderProtocol,
};
pub use runtime::{gateway_router, serve, GatewayReadiness, GatewayRuntime, REQUEST_ID_HEADER};
