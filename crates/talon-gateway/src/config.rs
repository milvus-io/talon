//! Runtime limits and fail-closed deployment mode.

use std::net::SocketAddr;
use std::time::Duration;

/// Whether a gateway is an explicitly local development process or production.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayMode {
    /// Unauthenticated mode, accepted only on a loopback listener.
    Development,
    /// Public data-plane mode; readiness requires TLS, authn, and authz.
    Production,
}

/// Security components installed by the production security layer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewaySecurity {
    /// Client connections are protected by TLS.
    pub tls: bool,
    /// Provider credentials are authenticated before dispatch. Runtime installation manages this bit.
    pub authentication: bool,
    /// The authenticated principal is checked against a policy. Runtime installation manages this bit.
    pub authorization: bool,
}

impl GatewaySecurity {
    pub(crate) fn production_ready(self) -> bool {
        self.tls && self.authentication && self.authorization
    }
}

/// Shared HTTP server limits.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Listener address used to enforce loopback-only development mode.
    pub bind: SocketAddr,
    /// Deployment security mode.
    pub mode: GatewayMode,
    /// Maximum raw request-target length.
    pub max_request_target_bytes: usize,
    /// Maximum number of request headers.
    pub max_header_count: usize,
    /// Maximum aggregate request header name/value bytes.
    pub max_header_bytes: usize,
    /// Maximum request body bytes, including chunked bodies.
    pub max_body_bytes: usize,
    /// Maximum active provider requests. Operational endpoints are exempt.
    pub max_concurrency: usize,
    /// Deadline for adapter dispatch before response headers exist.
    pub request_deadline: Duration,
    /// Maximum wait between request or response body frames.
    pub body_idle_timeout: Duration,
    /// Time allowed for active requests after shutdown begins.
    pub graceful_shutdown_timeout: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".parse().expect("literal socket address"),
            mode: GatewayMode::Development,
            max_request_target_bytes: 16 * 1024,
            max_header_count: 100,
            max_header_bytes: 64 * 1024,
            max_body_bytes: 16 * 1024 * 1024,
            max_concurrency: 1_024,
            request_deadline: Duration::from_secs(30),
            body_idle_timeout: Duration::from_secs(30),
            graceful_shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl GatewayConfig {
    /// Reject unsafe development listeners and zero-valued limits.
    pub fn validate(&self) -> Result<(), GatewayConfigError> {
        if self.mode == GatewayMode::Development && !self.bind.ip().is_loopback() {
            return Err(GatewayConfigError::DevelopmentRequiresLoopback);
        }
        if self.max_request_target_bytes == 0
            || self.max_header_count == 0
            || self.max_header_bytes == 0
            || self.max_body_bytes == 0
            || self.max_concurrency == 0
            || self.request_deadline.is_zero()
            || self.body_idle_timeout.is_zero()
            || self.graceful_shutdown_timeout.is_zero()
        {
            return Err(GatewayConfigError::ZeroLimit);
        }
        Ok(())
    }
}

/// Invalid shared gateway configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayConfigError {
    /// Development mode would expose an unauthenticated public listener.
    #[error("unauthenticated development mode requires a loopback bind address")]
    DevelopmentRequiresLoopback,
    /// A resource or time bound was disabled.
    #[error("gateway limits and timeouts must be greater than zero")]
    ZeroLimit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn development_mode_is_loopback_only() {
        let mut config = GatewayConfig {
            bind: "0.0.0.0:8080".parse().unwrap(),
            ..GatewayConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(GatewayConfigError::DevelopmentRequiresLoopback)
        );
        config.mode = GatewayMode::Production;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn every_limit_is_nonzero() {
        let config = GatewayConfig {
            max_concurrency: 0,
            ..GatewayConfig::default()
        };
        assert_eq!(config.validate(), Err(GatewayConfigError::ZeroLimit));
    }
}
