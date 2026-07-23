//! Management-plane security: authentication, security headers, and request
//! hardening for the coordinator administration surface (issue #85).
//!
//! Trust model and route classes:
//! - **Public, unauthenticated**: `/healthz`, `/readyz`, `/metrics`. Liveness
//!   and scrape endpoints must stay reachable by orchestrators and Prometheus
//!   without credentials, and they expose no sensitive cluster data. Operators
//!   who want these protected bind them to a separate interface at the proxy.
//! - **Protected**: the management API (`/api/v1`) and UI (`/`, `/ui`). When
//!   authentication is enabled these require a valid bearer token and **fail
//!   closed** (401) otherwise.
//!
//! Authentication modes ([`AuthMode`]):
//! - `Disabled` — no auth (development, or when a trusted reverse proxy in front
//!   of the coordinator terminates auth). This is the default and is logged
//!   loudly so it is never a silent production mistake.
//! - `BearerToken` — a shared secret presented as `Authorization: Bearer <token>`.
//!   The token is compared in constant time and never logged.
//!
//! TLS is intentionally **reverse-proxy-only** in v1: the coordinator serves
//! plain HTTP and is expected to sit behind an ingress/proxy that terminates TLS
//! and (optionally) mTLS. This keeps certificate rotation in the proxy's
//! well-trodden path rather than reimplementing it here; the trade-off is
//! documented for operators. `TrustProxy` records which forwarded headers are
//! honored so audit logging attributes the real client.
//!
//! Every management response also carries a standard set of security headers
//! (frame-ancestors deny, nosniff, referrer policy, no-store cache on protected
//! data), and request bodies are size-capped.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

/// Maximum accepted request body for management endpoints. The API is read-only
/// (no large uploads), so a small cap bounds memory and rejects abuse early.
pub const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;

/// Default per-request handling timeout for management endpoints.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Authentication mode for the protected management surface.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum AuthMode {
    /// No authentication. Development, or a trusted reverse proxy handles auth.
    #[default]
    Disabled,
    /// Shared bearer token in `Authorization: Bearer <token>`.
    BearerToken {
        /// The expected secret. Never logged or surfaced in status/metrics.
        token: String,
    },
}

/// Management security configuration.
#[derive(Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(default)]
pub struct SecurityConfig {
    /// Authentication mode for `/api/v1` and the UI.
    pub auth: AuthMode,
    /// Whether to honor `X-Forwarded-For` from a trusted proxy for audit logs.
    pub trust_forwarded_headers: bool,
}

impl std::fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the token.
        let auth = match &self.auth {
            AuthMode::Disabled => "disabled",
            AuthMode::BearerToken { .. } => "bearer_token(<redacted>)",
        };
        f.debug_struct("SecurityConfig")
            .field("auth", &auth)
            .field("trust_forwarded_headers", &self.trust_forwarded_headers)
            .finish()
    }
}

impl SecurityConfig {
    /// Whether authentication is enabled (protected routes require a token).
    pub fn auth_enabled(&self) -> bool {
        !matches!(self.auth, AuthMode::Disabled)
    }

    /// Validate the security configuration. An enabled auth mode must carry a
    /// non-trivial secret so "enabled" can never mean "accepts empty token".
    pub fn validate(&self) -> Result<(), SecurityConfigError> {
        if let AuthMode::BearerToken { token } = &self.auth {
            if token.len() < 16 {
                return Err(SecurityConfigError::WeakToken);
            }
        }
        Ok(())
    }
}

/// Invalid security configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecurityConfigError {
    /// The bearer token is missing or too short to be a real secret.
    #[error("bearer token must be at least 16 characters")]
    WeakToken,
}

/// Constant-time comparison for equal-length secrets. Lengths are compared
/// first (a token's length is not itself secret), then every byte is mixed so
/// no early return leaks which byte differed.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Extract and verify a bearer token from the `Authorization` header.
fn bearer_ok(headers: &axum::http::HeaderMap, expected: &str) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(text) = value.to_str() else {
        return false;
    };
    let Some(token) = text.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(token.trim().as_bytes(), expected.as_bytes())
}

/// Apply the standard management security headers to any response.
pub fn apply_security_headers(resp: &mut Response, protected: bool) {
    let h = resp.headers_mut();
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    // Protected (data/UI) responses must not be stored by shared caches; the UI
    // asset layer sets its own long-lived caching and is exempt via `protected`.
    if protected {
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
}

/// Axum middleware enforcing authentication on protected routes and stamping
/// security headers on every response.
///
/// Health/metrics paths are always allowed through unauthenticated; everything
/// else requires a valid token when auth is enabled. Rejections are constant,
/// credential-free 401s carrying a `WWW-Authenticate` challenge.
pub async fn guard(config: Arc<SecurityConfig>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();
    let public = is_public_path(&path);

    if !public && config.auth_enabled() {
        let authorized = match &config.auth {
            AuthMode::Disabled => true,
            AuthMode::BearerToken { token } => bearer_ok(request.headers(), token),
        };
        if !authorized {
            let mut resp = (
                StatusCode::UNAUTHORIZED,
                [(
                    header::WWW_AUTHENTICATE,
                    "Bearer realm=\"talon-management\"",
                )],
                Body::from("{\"error\":\"unauthorized\",\"message\":\"authentication required\"}"),
            )
                .into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            apply_security_headers(&mut resp, true);
            return resp;
        }
    }

    let mut resp = next.run(request).await;
    apply_security_headers(&mut resp, !public);
    resp
}

/// Whether a path is a public, always-unauthenticated operational endpoint.
pub fn is_public_path(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz" | "/metrics")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_only_equal() {
        assert!(constant_time_eq(b"secret-token-1234", b"secret-token-1234"));
        assert!(!constant_time_eq(
            b"secret-token-1234",
            b"secret-token-9999"
        ));
        assert!(!constant_time_eq(b"short", b"longer-value"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn bearer_ok_parses_and_verifies() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer the-expected-token-value"),
        );
        assert!(bearer_ok(&headers, "the-expected-token-value"));
        assert!(!bearer_ok(&headers, "wrong-token"));

        let mut basic = axum::http::HeaderMap::new();
        basic.insert(header::AUTHORIZATION, HeaderValue::from_static("Basic abc"));
        assert!(!bearer_ok(&basic, "the-expected-token-value"));

        let empty = axum::http::HeaderMap::new();
        assert!(!bearer_ok(&empty, "x"));
    }

    #[test]
    fn public_paths_are_operational_only() {
        assert!(is_public_path("/healthz"));
        assert!(is_public_path("/readyz"));
        assert!(is_public_path("/metrics"));
        assert!(!is_public_path("/api/v1/cluster"));
        assert!(!is_public_path("/ui"));
        assert!(!is_public_path("/"));
    }

    #[test]
    fn validate_rejects_weak_token() {
        let weak = SecurityConfig {
            auth: AuthMode::BearerToken {
                token: "short".into(),
            },
            trust_forwarded_headers: false,
        };
        assert_eq!(weak.validate(), Err(SecurityConfigError::WeakToken));

        let strong = SecurityConfig {
            auth: AuthMode::BearerToken {
                token: "a-sufficiently-long-secret".into(),
            },
            trust_forwarded_headers: false,
        };
        strong.validate().unwrap();
        assert!(strong.auth_enabled());
        assert!(!SecurityConfig::default().auth_enabled());
    }

    #[test]
    fn debug_redacts_token() {
        let cfg = SecurityConfig {
            auth: AuthMode::BearerToken {
                token: "super-secret-value-here".into(),
            },
            trust_forwarded_headers: true,
        };
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("super-secret-value-here"));
        assert!(rendered.contains("<redacted>"));
    }
}
