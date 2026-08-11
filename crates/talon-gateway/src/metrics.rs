//! Low-cardinality gateway metrics.

use std::time::Duration;

use talon_core::metrics::{labels, Counter, Histogram, Metrics};

use crate::model::{
    FailureReason, GatewayOperation, GatewayOutcome, GatewayRoute, ProviderProtocol,
};

/// Metrics shared by all provider adapters.
#[derive(Clone)]
pub struct GatewayMetrics {
    registry: Metrics,
}

impl Default for GatewayMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl GatewayMetrics {
    /// Create an empty gateway registry.
    pub fn new() -> Self {
        Self {
            registry: Metrics::new(),
        }
    }

    /// Render Prometheus text exposition.
    pub fn render(&self) -> String {
        self.registry.render()
    }

    pub(crate) fn record_tls_event(&self, event: &'static str) {
        self.registry
            .counter(
                "talon_gateway_tls_events_total",
                "Gateway TLS handshake and reload outcomes.",
                labels(&[("event", event)]),
            )
            .inc();
    }

    /// Count one origin credential refresh outcome (workload identity).
    ///
    /// Public because credentials are resolved in the gateway binary before
    /// the runtime exists; `result` is `refresh_success` or `refresh_failure`.
    pub fn record_origin_credentials(&self, result: &'static str) {
        self.registry
            .counter(
                "talon_gateway_origin_credentials_total",
                "Origin credential refresh outcomes.",
                labels(&[("result", result)]),
            )
            .inc();
    }

    /// Publish when the current origin credentials expire (unix seconds,
    /// `0` when the mechanism reports no expiry or credentials are static).
    pub fn set_origin_credentials_expiry(&self, unix_seconds: f64) {
        self.registry
            .gauge(
                "talon_gateway_origin_credentials_expiry_seconds",
                "Unix time when the current origin credentials expire.",
                labels(&[]),
            )
            .set(unix_seconds);
    }

    /// Count one identity or authorization file reload poll outcome.
    ///
    /// Public because the reload loop lives in the gateway binary; both label
    /// values must stay bounded (`file` names a configured input, `result` is
    /// `success`, `failure`, or `unchanged`).
    pub fn record_auth_reload(&self, file: &'static str, result: &'static str) {
        self.registry
            .counter(
                "talon_gateway_auth_reload_polls_total",
                "Gateway identity and authorization file reload poll outcomes.",
                labels(&[("file", file), ("result", result)]),
            )
            .inc();
    }

    pub(crate) fn record_authorization(&self, decision: &'static str) {
        self.registry
            .counter(
                "talon_gateway_authorization_total",
                "Gateway authorization decisions.",
                labels(&[("decision", decision)]),
            )
            .inc();
    }

    pub(crate) fn record_authentication(&self, result: &'static str) {
        self.registry
            .counter(
                "talon_gateway_authentication_total",
                "Gateway authentication results.",
                labels(&[("result", result)]),
            )
            .inc();
    }

    pub(crate) fn record_headers(&self, observation: RequestObservation) {
        let failure = observation.failure.map_or("none", FailureReason::label);
        let response_class = match observation.status / 100 {
            1 => "1xx",
            2 => "2xx",
            3 => "3xx",
            4 => "4xx",
            _ => "5xx",
        };
        self.registry
            .counter(
                "talon_gateway_requests_total",
                "Gateway requests by bounded provider-neutral result dimensions.",
                labels(&[
                    ("protocol", observation.protocol.label()),
                    ("operation", observation.operation.label()),
                    ("route", observation.route.label()),
                    ("outcome", observation.outcome.label()),
                    ("response_class", response_class),
                    ("failure", failure),
                ]),
            )
            .inc();
        self.byte_counter(observation.protocol, observation.operation, "requested")
            .add(observation.requested_bytes);
        self.byte_counter(observation.protocol, observation.operation, "cache")
            .add(observation.cache_bytes);
        self.byte_counter(observation.protocol, observation.operation, "origin")
            .add(observation.origin_bytes);
        self.latency_histogram(observation.protocol, observation.operation, "headers")
            .observe(observation.headers_latency.as_secs_f64());
    }

    pub(crate) fn response_observer(
        &self,
        protocol: ProviderProtocol,
        operation: GatewayOperation,
    ) -> ResponseObserver {
        ResponseObserver {
            bytes: self.byte_counter(protocol, operation, "response"),
            total_latency: self.latency_histogram(protocol, operation, "total"),
        }
    }

    fn byte_counter(
        &self,
        protocol: ProviderProtocol,
        operation: GatewayOperation,
        source: &'static str,
    ) -> Counter {
        self.registry.counter(
            "talon_gateway_bytes_total",
            "Gateway bytes by bounded source dimension.",
            labels(&[
                ("protocol", protocol.label()),
                ("operation", operation.label()),
                ("source", source),
            ]),
        )
    }

    fn latency_histogram(
        &self,
        protocol: ProviderProtocol,
        operation: GatewayOperation,
        phase: &'static str,
    ) -> Histogram {
        self.registry.histogram(
            "talon_gateway_request_duration_seconds",
            "Gateway time to response headers and body completion.",
            labels(&[
                ("protocol", protocol.label()),
                ("operation", operation.label()),
                ("phase", phase),
            ]),
        )
    }
}

pub(crate) struct RequestObservation {
    pub protocol: ProviderProtocol,
    pub operation: GatewayOperation,
    pub route: GatewayRoute,
    pub outcome: GatewayOutcome,
    pub failure: Option<FailureReason>,
    pub status: u16,
    pub requested_bytes: u64,
    pub cache_bytes: u64,
    pub origin_bytes: u64,
    pub headers_latency: Duration,
}

pub(crate) struct ResponseObserver {
    bytes: Counter,
    total_latency: Histogram,
}

impl ResponseObserver {
    pub(crate) fn complete(self, bytes: u64, elapsed: Duration) {
        self.bytes.add(bytes);
        self.total_latency.observe(elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_labels_never_include_targets_or_credentials() {
        let metrics = GatewayMetrics::new();
        metrics.record_headers(RequestObservation {
            protocol: ProviderProtocol::Azure,
            operation: GatewayOperation::Read,
            route: GatewayRoute::Cache,
            outcome: GatewayOutcome::Hit,
            failure: None,
            status: 206,
            requested_bytes: 10,
            cache_bytes: 10,
            origin_bytes: 0,
            headers_latency: Duration::from_millis(2),
        });
        metrics
            .response_observer(ProviderProtocol::Azure, GatewayOperation::Read)
            .complete(10, Duration::from_millis(3));
        metrics.record_auth_reload("s3_identities", "failure");
        let rendered = metrics.render();
        assert!(rendered.contains("protocol=\"azure\""));
        assert!(rendered.contains("source=\"response\""));
        assert!(rendered.contains(
            "talon_gateway_auth_reload_polls_total{file=\"s3_identities\",result=\"failure\"}"
        ));
        assert!(!rendered.contains("object"));
        assert!(!rendered.contains("credential"));
    }

    #[test]
    fn origin_credential_metrics_render_bounded_labels() {
        let metrics = GatewayMetrics::new();
        metrics.record_origin_credentials("refresh_failure");
        metrics.set_origin_credentials_expiry(1_754_000_000.0);
        let rendered = metrics.render();
        assert!(rendered
            .contains("talon_gateway_origin_credentials_total{result=\"refresh_failure\"} 1"));
        assert!(rendered.contains("talon_gateway_origin_credentials_expiry_seconds"));
    }
}
