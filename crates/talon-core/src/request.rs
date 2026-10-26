//! Sensitive request-local origin credential configuration.

use serde::{Deserialize, Serialize};

pub const CREDENTIAL_BROKER_KEY: &str = "credential_broker";
/// Default deadline for one Broker credential exchange.
pub const BROKER_REQUEST_TIMEOUT_MS: u64 = 3_000;
/// Initial acquisition makes one attempt and three retries.
pub const BROKER_INITIAL_ATTEMPTS: u32 = 4;
/// First exponential backoff delay between initial attempts.
pub const BROKER_INITIAL_RETRY_DELAY_MS: u64 = 100;
/// Delays between initial attempts: 100, 200 and 400 milliseconds.
pub const BROKER_INITIAL_BACKOFF_MS: u64 = BROKER_INITIAL_RETRY_DELAY_MS * 7;
pub const MAX_CREDENTIAL_BROKER_BYTES: usize = 16 * 1024;
// Match serde_json's default inbound recursion budget, including the Broker object.
const JSON_RECURSION_LIMIT: usize = 128;
const S3_ORIGIN_REGION_CSPS: &[&str] = &["aws", "ali", "tc", "hwc"];

/// Optional origin settings scoped to a single request. The Broker deadline is
/// effective; reserved transport fields do not change the storage HTTP client.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OriginOptions {
    /// Literal `host[:port]` or HTTP(S) URL; a bare host defaults to HTTPS.
    /// For Azure this is the full account host, not a cloud endpoint suffix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// S3 signing region; required in request descriptors and reserved for other
    /// backends. Does not change the region sent to the credential Broker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_ssl: Option<bool>,
    /// S3/Azure addressing mode; reserved for GCS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_virtual_host: Option<bool>,
    /// Reserved; not applied to requests. PEM content, never a local file path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_ca_pem: Option<String>,
    /// Reserved; accepted versions do not imply HTTP transport support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_min_version: Option<String>,
    /// Deadline for each credential Broker HTTP exchange; defaults to 3 seconds.
    /// Does not change object storage transport timeouts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,
    /// Reserved; not applied to HTTP connection limits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,
}

impl std::fmt::Debug for OriginOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OriginOptions([REDACTED])")
    }
}

impl OriginOptions {
    fn deserialize_optional<'de, D>(deserializer: D) -> Result<Option<Self>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OriginVisitor;
        impl<'de> serde::de::Visitor<'de> for OriginVisitor {
            type Value = Option<OriginOptions>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an origin object or null")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                // Struct derives also accept arrays; the wire contract requires
                // an object while retaining duplicate-field rejection.
                deserializer.deserialize_map(self)
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                OriginOptions::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(Some)
            }
        }
        deserializer.deserialize_option(OriginVisitor)
    }

    /// Check provider-independent settings without exposing their values.
    /// Reserved transport fields receive schema validation only.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(value) = &self.endpoint {
            let (scheme, authority) = match value.split_once("://") {
                Some((scheme, authority)) => {
                    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https")
                    {
                        return Err("credential_broker origin endpoint must be HTTP(S)".into());
                    }
                    (Some(scheme), authority)
                }
                None => (None, value.as_str()),
            };
            // Inspect the raw authority before URL parsing: parsers normalize
            // dot segments, backslashes and some whitespace before validation.
            let authority = authority.strip_suffix('/').unwrap_or(authority);
            if authority.is_empty()
                || value
                    .chars()
                    .any(|ch| ch.is_whitespace() || ch.is_control())
                || authority.contains(['/', '\\', '@', '?', '#'])
                || !authority.is_ascii()
                || authority.contains('%')
            {
                return Err(
                    "credential_broker origin endpoint must contain only a host and port".into(),
                );
            }
            let endpoint = url::Url::parse(&format!("https://{authority}"))
                .map_err(|_| "invalid credential_broker origin endpoint")?;
            let valid_host = match endpoint.host() {
                Some(url::Host::Ipv4(address)) => {
                    authority.split(':').next() == Some(address.to_string().as_str())
                }
                Some(url::Host::Ipv6(_)) => true,
                Some(url::Host::Domain(host)) => {
                    let host = host.strip_suffix('.').unwrap_or(host);
                    host.split('.').all(|label| {
                        label
                            .as_bytes()
                            .first()
                            .is_some_and(u8::is_ascii_alphanumeric)
                            && label
                                .as_bytes()
                                .last()
                                .is_some_and(u8::is_ascii_alphanumeric)
                            && label
                                .bytes()
                                .all(|ch| ch.is_ascii_alphanumeric() || ch == b'-')
                    }) && (!host.contains('.')
                        || host.rsplit('.').next().unwrap().as_bytes()[0].is_ascii_alphabetic())
                }
                None => false,
            };
            if !valid_host {
                return Err("invalid credential_broker origin endpoint".into());
            }
            if let (Some(scheme), Some(use_ssl)) = (scheme, self.use_ssl) {
                if scheme.eq_ignore_ascii_case("https") != use_ssl {
                    return Err("credential_broker origin endpoint conflicts with use_ssl".into());
                }
            }
        }
        if self
            .region
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
            || self
                .tls_ca_pem
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err("credential_broker origin region and CA must not be empty".into());
        }
        if self
            .tls_min_version
            .as_ref()
            .is_some_and(|value| !matches!(value.as_str(), "1.0" | "1.1" | "1.2" | "1.3"))
        {
            return Err("credential_broker origin TLS minimum must be 1.0, 1.1, 1.2 or 1.3".into());
        }
        if self.request_timeout_ms == Some(0) || self.max_connections == Some(0) {
            return Err(
                "credential_broker origin timeout and connection limit must be positive".into(),
            );
        }
        Ok(())
    }
}

/// Broker configuration is never included in diagnostics, even through a
/// containing request's derived `Debug` implementation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfig {
    pub url: String,
    pub request: serde_json::Value,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "OriginOptions::deserialize_optional"
    )]
    pub origin: Option<OriginOptions>,
}

impl std::fmt::Debug for BrokerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BrokerConfig([REDACTED])")
    }
}

impl BrokerConfig {
    /// Parse a bounded request descriptor without including caller-controlled JSON or
    /// URL values in errors. Cloud-specific validation happens at the backend.
    pub fn from_json(value: &str) -> Result<Self, String> {
        Ok(ValidatedBrokerConfig::from_json(value)?.config)
    }

    /// Validate provider-independent values without serializing the descriptor.
    pub fn validate(&self) -> Result<(), String> {
        if !self.request.is_object() {
            return Err("credential_broker request must be a JSON object".into());
        }
        if let Some(origin) = &self.origin {
            origin.validate()?;
        }
        let endpoint = url::Url::parse(&self.url)
            .map_err(|_| "credential_broker URL must be a full HTTP(S) URL")?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(
                "credential_broker URL must be HTTP(S) without userinfo or fragment".into(),
            );
        }
        Ok(())
    }

    /// Request-scoped credentials must choose their own origin endpoint and S3 signing region.
    /// Static configurations may instead obtain these values from worker settings.
    pub fn validate_request(&self) -> Result<(), String> {
        self.validate()?;
        if self
            .origin
            .as_ref()
            .and_then(|origin| origin.endpoint.as_ref())
            .is_none()
        {
            return Err("credential_broker origin endpoint is required".into());
        }
        if self
            .request
            .get("csp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|csp| S3_ORIGIN_REGION_CSPS.contains(&csp))
            && self
                .origin
                .as_ref()
                .and_then(|origin| origin.region.as_ref())
                .is_none()
        {
            return Err("credential_broker origin region is required for S3".into());
        }
        Ok(())
    }
}

/// Immutable request-boundary descriptor with reusable canonical wire bytes.
/// Backend normalization is separate and never changes this wire representation.
pub struct ValidatedBrokerConfig {
    config: BrokerConfig,
    canonical_json: Box<[u8]>,
}

impl ValidatedBrokerConfig {
    pub fn from_json(value: &str) -> Result<Self, String> {
        if value.len() > MAX_CREDENTIAL_BROKER_BYTES {
            return Err("credential_broker exceeds the request size limit".into());
        }
        let config: BrokerConfig = serde_json::from_str(value)
            .map_err(|_| "credential_broker must contain a URL and a request object")?;
        Self::try_from(config)
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    pub fn canonical_json(&self) -> &[u8] {
        &self.canonical_json
    }
}

impl TryFrom<BrokerConfig> for ValidatedBrokerConfig {
    type Error = String;

    fn try_from(config: BrokerConfig) -> Result<Self, Self::Error> {
        fn within_nesting_limit(value: &serde_json::Value, depth: usize) -> bool {
            match value {
                serde_json::Value::Array(values) => {
                    depth < JSON_RECURSION_LIMIT
                        && values
                            .iter()
                            .all(|value| within_nesting_limit(value, depth + 1))
                }
                serde_json::Value::Object(values) => {
                    depth < JSON_RECURSION_LIMIT
                        && values
                            .values()
                            .all(|value| within_nesting_limit(value, depth + 1))
                }
                _ => true,
            }
        }

        config.validate_request()?;
        // The outer Broker object and request object consume the first two
        // levels. Check before serialization, with traversal bounded by the
        // parser's budget even for programmatically constructed Values.
        if !within_nesting_limit(&config.request, 2) {
            return Err("credential_broker exceeds the request nesting limit".into());
        }
        let canonical_json =
            serde_json::to_vec(&config).map_err(|_| "invalid credential_broker request")?;
        if canonical_json.len() > MAX_CREDENTIAL_BROKER_BYTES {
            return Err("credential_broker exceeds the request size limit".into());
        }
        Ok(Self {
            config,
            canonical_json: canonical_json.into_boxed_slice(),
        })
    }
}

impl PartialEq for ValidatedBrokerConfig {
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
    }
}
impl Eq for ValidatedBrokerConfig {}

impl std::fmt::Debug for ValidatedBrokerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ValidatedBrokerConfig([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_request_descriptors_require_an_explicit_origin_region() {
        for csp in ["aws", "ali", "tc", "hwc"] {
            for region in [
                None,
                Some(serde_json::Value::Null),
                Some(serde_json::json!("")),
                Some(serde_json::json!(" \t\n")),
            ] {
                let mut value = serde_json::json!({
                    "url": "https://broker/sentinel",
                    "request": {"csp": csp, "region": "broker-sentinel"},
                    "origin": {"endpoint": "storage.example"}
                });
                if let Some(region) = region {
                    value["origin"]["region"] = region;
                }
                let json = value.to_string();
                let raw: BrokerConfig = serde_json::from_str(&json).unwrap();
                for result in [
                    ValidatedBrokerConfig::try_from(raw),
                    ValidatedBrokerConfig::from_json(&json),
                ] {
                    let error = result.expect_err("S3 request origin region must be explicit");
                    assert!(!error.contains("sentinel"));
                }
                let error = BrokerConfig::from_json(&json).unwrap_err();
                assert!(!error.contains("sentinel"));
            }
            let value = serde_json::json!({
                "url": "https://broker", "request": {"csp": csp, "region": "broker-region"},
                "origin": {"endpoint": "storage.example", "region": "origin-region"}
            });
            let config = BrokerConfig::from_json(&value.to_string()).unwrap();
            assert_eq!(
                config.origin.unwrap().region.as_deref(),
                Some("origin-region")
            );
        }
    }

    #[test]
    fn non_s3_request_descriptors_allow_missing_origin_region() {
        for request in [
            serde_json::json!({}),
            serde_json::json!({"csp": "gcp"}),
            serde_json::json!({"csp": "azure"}),
            serde_json::json!({"csp": "unknown"}),
        ] {
            for origin in [
                serde_json::json!({"endpoint": "storage.example"}),
                serde_json::json!({"endpoint": "storage.example", "region": null}),
            ] {
                let value = serde_json::json!({
                    "url": "https://broker", "request": request, "origin": origin
                });
                assert!(BrokerConfig::from_json(&value.to_string()).is_ok());
            }
        }
    }

    #[test]
    fn static_s3_configurations_allow_missing_origin_region() {
        for csp in ["aws", "ali", "tc", "hwc"] {
            for origin in [
                None,
                Some(OriginOptions {
                    request_timeout_ms: Some(17),
                    ..Default::default()
                }),
                Some(OriginOptions {
                    endpoint: Some("storage.example".into()),
                    ..Default::default()
                }),
            ] {
                let config = BrokerConfig {
                    url: "https://broker".into(),
                    request: serde_json::json!({"csp": csp, "region": "broker-region"}),
                    origin,
                };
                assert!(config.validate().is_ok());
            }
        }
    }

    #[test]
    fn request_descriptors_require_an_origin_endpoint_for_every_cloud() {
        for csp in ["aws", "gcp", "azure"] {
            for origin in [
                None,
                Some(serde_json::Value::Null),
                Some(serde_json::json!({})),
                Some(serde_json::json!({"request_timeout_ms": 17})),
                Some(serde_json::json!({"endpoint": null})),
                Some(serde_json::json!({"endpoint": ""})),
                Some(serde_json::json!({"endpoint": " \t\n"})),
            ] {
                let mut value = serde_json::json!({
                    "url": "https://broker/sentinel",
                    "request": {"csp": csp, "token": "sentinel"}
                });
                if let Some(origin) = origin {
                    value["origin"] = origin;
                }
                let json = value.to_string();
                let raw: BrokerConfig = serde_json::from_str(&json).unwrap();
                for result in [
                    ValidatedBrokerConfig::try_from(raw),
                    ValidatedBrokerConfig::from_json(&json),
                ] {
                    let error = result.expect_err("request endpoint must be explicit");
                    assert!(!error.contains("sentinel"));
                }
                let error = BrokerConfig::from_json(&json).unwrap_err();
                assert!(!error.contains("sentinel"));
            }
        }
    }

    #[test]
    fn validated_descriptor_keeps_canonical_bytes_and_semantic_equality() {
        let first = ValidatedBrokerConfig::from_json(
            r#"{ "request": {"b":2,"a":1}, "url":"https://broker", "origin":{"endpoint":"storage.example"} }"#,
        )
        .unwrap();
        let second = ValidatedBrokerConfig::from_json(
            r#"{"url":"https://broker","request":{"a":1,"b":2},"origin":{"endpoint":"storage.example"}}"#,
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.canonical_json(),
            br#"{"url":"https://broker","request":{"a":1,"b":2},"origin":{"endpoint":"storage.example"}}"#
        );
        let mut raw = first.config().clone();
        raw.request = serde_json::json!([]);
        assert!(raw.validate().is_err());
        assert!(ValidatedBrokerConfig::try_from(raw).is_err());
        assert!(!format!("{first:?}").contains("https://broker"));
    }

    #[test]
    fn direct_descriptor_conversion_preserves_inbound_nesting_limit() {
        for kind in ["arrays", "objects", "mixed"] {
            for (depth, leaf, accepted) in [
                (125, serde_json::Value::Null, true),
                (126, serde_json::Value::Null, false),
                (124, serde_json::json!([]), true),
                (125, serde_json::json!([]), false),
                (124, serde_json::json!({}), true),
                (125, serde_json::json!({}), false),
                (130, serde_json::Value::Null, false),
            ] {
                let mut nested = leaf;
                for level in 0..depth {
                    nested = if kind == "arrays" || (kind == "mixed" && level % 2 == 0) {
                        serde_json::Value::Array(vec![nested])
                    } else {
                        serde_json::Value::Object([("x".into(), nested)].into_iter().collect())
                    };
                }
                let config = BrokerConfig {
                    url: "https://broker".into(),
                    request: serde_json::Value::Object(
                        [("nested".into(), nested)].into_iter().collect(),
                    ),
                    origin: Some(OriginOptions {
                        endpoint: Some("storage.example".into()),
                        ..Default::default()
                    }),
                };
                let json = serde_json::to_string(&config).unwrap();
                assert!(json.len() < 1024);
                assert_eq!(
                    serde_json::from_str::<BrokerConfig>(&json).is_ok(),
                    accepted,
                    "parser {kind} at depth {depth}"
                );
                assert_eq!(
                    BrokerConfig::from_json(&json).is_ok(),
                    accepted,
                    "inbound {kind} at depth {depth}"
                );
                let direct = ValidatedBrokerConfig::try_from(config);
                assert_eq!(direct.is_ok(), accepted, "direct {kind} at depth {depth}");
                if let Ok(direct) = direct {
                    assert_eq!(
                        ValidatedBrokerConfig::from_json(
                            std::str::from_utf8(direct.canonical_json()).unwrap()
                        )
                        .unwrap(),
                        direct
                    );
                }
            }
        }
    }

    #[test]
    fn optional_origin_preserves_static_configurations() {
        let legacy = serde_json::json!({"url": "https://broker", "request": {}});
        for value in [
            legacy.clone(),
            serde_json::json!({
                "url": "https://broker", "request": {}, "origin": null
            }),
        ] {
            let config: BrokerConfig = serde_json::from_value(value).unwrap();
            assert!(config.validate().is_ok());
            assert_eq!(serde_json::to_value(config).unwrap(), legacy);
        }
        let empty = serde_json::json!({"url": "https://broker", "request": {}, "origin": {}});
        let config: BrokerConfig = serde_json::from_value(empty.clone()).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(serde_json::to_value(config).unwrap(), empty);
        let timeout = serde_json::json!({
            "url": "https://broker", "request": {}, "origin": {"request_timeout_ms": 17}
        });
        let config: BrokerConfig = serde_json::from_value(timeout.clone()).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(serde_json::to_value(config).unwrap(), timeout);
    }

    #[test]
    fn origin_options_round_trip_without_exposing_values() {
        let value = serde_json::json!({
            "url": "https://broker", "request": {"token": "sentinel"},
            "origin": {
                "endpoint": "https://sentinel:9443", "region": "sentinel",
                "use_ssl": true, "use_virtual_host": false,
                "tls_ca_pem": "sentinel", "tls_min_version": "1.3",
                "request_timeout_ms": 18446744073709551615_u64,
                "max_connections": 4294967295_u32
            }
        });
        let config = BrokerConfig::from_json(&value.to_string()).unwrap();
        assert!(!format!("{config:?}").contains("sentinel"));
        assert!(!format!("{:?}", config.origin.as_ref().unwrap()).contains("sentinel"));
        assert_eq!(serde_json::to_value(config).unwrap(), value);
    }

    #[test]
    fn descriptor_debug_redacts_broker_values() {
        let config = BrokerConfig::from_json(
            r#"{"url":"https://broker/sentinel","request":{"token":"sentinel"},"origin":{"endpoint":"sentinel.example"}}"#,
        )
        .unwrap();
        assert!(!format!("{config:?}").contains("sentinel"));
    }
}
