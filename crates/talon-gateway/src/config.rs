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

/// Identity used when a gateway request must reach the object-store origin.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OriginAuthMode {
    /// Sign origin requests with the gateway's configured service identity.
    #[default]
    Service,
    /// Forward the request's origin-issued capability without substitution.
    TrustedPassthrough,
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
    /// How origin-bound requests are authenticated.
    pub origin_auth: OriginAuthMode,
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
            origin_auth: OriginAuthMode::Service,
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
        if self.origin_auth == OriginAuthMode::TrustedPassthrough {
            if self.mode != GatewayMode::Development {
                return Err(GatewayConfigError::TrustedPassthroughRequiresDevelopment);
            }
            if !self.bind.ip().is_loopback() {
                return Err(GatewayConfigError::TrustedPassthroughRequiresLoopback);
            }
        }
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
    /// Credential passthrough was combined with production security semantics.
    #[error("trusted origin credential passthrough requires development mode")]
    TrustedPassthroughRequiresDevelopment,
    /// Credential passthrough would expose unauthenticated cache hits publicly.
    #[error("trusted origin credential passthrough requires a loopback bind address")]
    TrustedPassthroughRequiresLoopback,
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

    #[test]
    fn trusted_passthrough_is_development_loopback_only() {
        let mut config = GatewayConfig {
            origin_auth: OriginAuthMode::TrustedPassthrough,
            mode: GatewayMode::Production,
            ..GatewayConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(GatewayConfigError::TrustedPassthroughRequiresDevelopment)
        );

        config.mode = GatewayMode::Development;
        config.bind = "0.0.0.0:8080".parse().unwrap();
        assert_eq!(
            config.validate(),
            Err(GatewayConfigError::TrustedPassthroughRequiresLoopback)
        );

        config.bind = "127.0.0.1:8080".parse().unwrap();
        assert!(config.validate().is_ok());
    }
}
