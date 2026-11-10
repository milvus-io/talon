//! Complete runtime overrides loaded from a local or ConfigMap-projected file.
use crate::RateLimitPolicy;
use serde::{Deserialize, Serialize};

/// Only fields with live runtime consumers belong in this contract.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDynamicConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limits: Option<RateLimitPolicy>,
    #[serde(default)]
    pub namespaces: std::collections::BTreeMap<String, NamespaceDynamicConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_tti_hours: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paged_miss_run_concurrency: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_scan_batch_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_delete_batch_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_io_max_mb_per_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_delete_max_per_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_eviction_high_watermark: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub async_eviction_low_watermark: Option<f64>,
}
impl WorkerDynamicConfig {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(hours) = self.page_tti_hours {
            let millis = hours
                .checked_mul(3_600_000)
                .ok_or("TTI duration overflows")?;
            if std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(millis))
                .is_none()
            {
                return Err("TTI duration overflows clock".into());
            }
        }
        if self.namespaces.len() > 4096 {
            return Err("too many namespace configurations".into());
        }
        for (name, config) in &self.namespaces {
            crate::NamespaceId::named(name)
                .validate()
                .map_err(|e| e.to_string())?;
            config.validate()?;
        }
        if let Some(policy) = &self.rate_limits {
            if policy.overrides.len() > 4096 {
                return Err("too many namespace overrides".into());
            }
            for name in policy.overrides.keys() {
                crate::NamespaceId::named(name)
                    .validate()
                    .map_err(|e| e.to_string())?;
            }
            policy.validate().map_err(|e| e.to_string())?;
        }
        if [
            self.paged_miss_run_concurrency,
            self.background_scan_batch_size,
            self.background_delete_batch_size,
        ]
        .contains(&Some(0))
        {
            return Err("concurrency and batch sizes must be positive".into());
        }
        if self
            .background_io_max_mb_per_sec
            .is_some_and(|rate| rate > u64::MAX / 1_000_000)
        {
            return Err("background I/O rate overflows bytes per second".into());
        }
        match (
            self.async_eviction_low_watermark,
            self.async_eviction_high_watermark,
        ) {
            (None, None) => {}
            (Some(low), Some(high))
                if low.is_finite()
                    && high.is_finite()
                    && low > 0.0
                    && low < high
                    && high <= 1.0 => {}
            _ => return Err("set both watermarks with 0 < low < high <= 1".into()),
        }
        Ok(())
    }
    pub fn parse(text: &str) -> Result<Self, String> {
        Ok(WorkerConfigDocument::parse(text)?.tuning)
    }

    pub fn from_patch(patch: &crate::WorkerConfigPatch) -> Self {
        Self {
            rate_limits: patch.rate_limits.clone(),
            namespaces: patch
                .namespaces
                .as_ref()
                .map(|namespaces| {
                    namespaces
                        .iter()
                        .map(|(name, own)| {
                            (
                                name.clone(),
                                NamespaceDynamicConfig {
                                    page_tti_hours: own.page_tti_hours,
                                    rate_limits: own.rate_limits,
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            page_tti_hours: patch.page_tti_hours,
            paged_miss_run_concurrency: patch.paged_miss_run_concurrency,
            background_scan_batch_size: patch.background_scan_batch_size,
            background_delete_batch_size: patch.background_delete_batch_size,
            background_io_max_mb_per_sec: patch.background_io_max_mb_per_sec,
            background_delete_max_per_sec: patch.background_delete_max_per_sec,
            async_eviction_high_watermark: patch.async_eviction_high_watermark,
            async_eviction_low_watermark: patch.async_eviction_low_watermark,
        }
    }

    pub fn from_config(config: &crate::WorkerConfig) -> Self {
        Self {
            rate_limits: Some(config.rate_limits.clone()),
            namespaces: Default::default(),
            page_tti_hours: Some(config.page_tti_hours),
            paged_miss_run_concurrency: Some(config.paged_miss_run_concurrency),
            background_scan_batch_size: Some(config.background_scan_batch_size),
            background_delete_batch_size: Some(config.background_delete_batch_size),
            background_io_max_mb_per_sec: Some(config.background_io_max_mb_per_sec),
            background_delete_max_per_sec: Some(config.background_delete_max_per_sec),
            async_eviction_high_watermark: Some(config.async_eviction_high_watermark),
            async_eviction_low_watermark: Some(config.async_eviction_low_watermark),
        }
    }
}

/// A complete override for one namespace. Omitted fields inherit worker defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NamespaceDynamicConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limits: Option<crate::MetricLimits>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_tti_hours: Option<u64>,
}
impl NamespaceDynamicConfig {
    pub fn validate(&self) -> Result<(), String> {
        // Validate the same bounds as global tuning without recursively visiting namespaces.
        let config = WorkerDynamicConfig {
            page_tti_hours: self.page_tti_hours,
            ..Default::default()
        };
        config.validate()?;
        if let Some(limits) = &self.rate_limits {
            limits.validate().map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
impl WorkerDynamicConfig {
    pub fn settings_for(&self, namespace: &str) -> NamespaceDynamicConfig {
        let own = self.namespaces.get(namespace).cloned().unwrap_or_default();
        NamespaceDynamicConfig {
            rate_limits: own.rate_limits,
            page_tti_hours: own.page_tti_hours.or(self.page_tti_hours),
        }
    }
    pub fn effective_rate_limits(&self, startup: &RateLimitPolicy) -> RateLimitPolicy {
        let mut policy = self.rate_limits.as_ref().unwrap_or(startup).clone();
        if self
            .namespaces
            .values()
            .any(|config| config.rate_limits.is_some())
        {
            if !policy.enabled {
                policy.default = Default::default();
                policy.overrides.clear();
            }
            policy.enabled = true;
            for (name, config) in &self.namespaces {
                if let Some(limits) = config.rate_limits {
                    policy.overrides.insert(name.clone(), limits);
                }
            }
        }
        policy
    }
}

/// One public configuration document for both startup and live tuning.
pub struct WorkerConfigDocument {
    pub patch: crate::WorkerConfigPatch,
    pub tuning: WorkerDynamicConfig,
}
impl WorkerConfigDocument {
    pub fn parse(text: &str) -> Result<Self, String> {
        if text.len() > 65536 {
            return Err("configuration exceeds 64 KiB".into());
        }
        let patch = crate::WorkerConfigPatch::from_toml(text).map_err(|e| e.to_string())?;
        for (key, present) in [
            ("node_id", patch.node_id.is_some()),
            ("advertise_addr", patch.advertise_addr.is_some()),
        ] {
            if present {
                return Err(format!(
                    "{key} is node-local and must not be set in the shared Worker configuration"
                ));
            }
        }
        let bucket_secrets = patch.buckets.as_ref().is_some_and(|buckets| {
            buckets.iter().any(|b| {
                b.s3_access_key_id.is_some()
                    || b.s3_secret_access_key.is_some()
                    || b.s3_session_token.is_some()
                    || b.gcs_access_key_id.is_some()
                    || b.gcs_secret_access_key.is_some()
                    || b.azure_shared_key.is_some()
                    || b.credential_broker_url.is_some()
                    || b.credential_broker_request.is_some()
            })
        });
        if bucket_secrets
            || patch.s3_access_key_id.is_some()
            || patch.s3_secret_access_key.is_some()
            || patch.s3_session_token.is_some()
            || patch.gcs_access_key_id.is_some()
            || patch.gcs_secret_access_key.is_some()
            || patch.azure_shared_key.is_some()
            || patch.credential_broker_url.is_some()
            || patch.credential_broker_request.is_some()
        {
            return Err("credentials must be supplied through Secrets".into());
        }
        if patch.namespaces.as_ref().is_some_and(|namespaces| {
            namespaces
                .values()
                .any(|own| own.paged_miss_run_concurrency.is_some())
        }) {
            return Err("paged_miss_run_concurrency is Worker-global".into());
        }
        let tuning = WorkerDynamicConfig::from_patch(&patch);
        tuning.validate()?;
        Ok(Self { patch, tuning })
    }
}

pub const HOT_FIELDS: &[&str] = &[
    "page_tti_hours",
    "paged_miss_run_concurrency",
    "background_scan_batch_size",
    "background_delete_batch_size",
    "background_io_max_mb_per_sec",
    "background_delete_max_per_sec",
    "async_eviction_high_watermark",
    "async_eviction_low_watermark",
    "rate_limits",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unified_toml_reuses_startup_parser_and_extracts_tuning() {
        let text = r#"
listen = "127.0.0.1:9001"
cache_dirs = ["/cache"]
paged_miss_run_concurrency = 2
[namespaces.a]
page_tti_hours = 1
rate_limits = { read_iops = { rate = 10 } }
"#;
        let document = WorkerConfigDocument::parse(text).unwrap();
        let legacy = crate::WorkerConfigPatch::from_toml(text).unwrap();
        assert_eq!(document.patch.listen, legacy.listen);
        let cfg =
            crate::WorkerConfig::resolve(document.patch, Default::default(), Default::default())
                .unwrap();
        assert_eq!(cfg.listen, "127.0.0.1:9001");
        assert_eq!(cfg.paged_miss_run_concurrency, 2);
        assert_eq!(
            cfg.rate_limits
                .limits_for(&crate::NamespaceId::named("a"))
                .read_iops
                .unwrap()
                .rate,
            10
        );
        assert_eq!(document.tuning.namespaces["a"].page_tti_hours, Some(1));
        assert_eq!(
            document.tuning.namespaces["a"]
                .rate_limits
                .unwrap()
                .read_iops
                .unwrap()
                .rate,
            10
        );
        assert!(WorkerConfigDocument::parse(r#"{"listen":"127.0.0.1:9001"}"#).is_err());
        assert!(WorkerConfigDocument::parse("").is_ok());
    }

    #[test]
    fn unified_toml_rejects_node_identity_secrets_and_invalid_fields() {
        for field in [
            "node_id",
            "advertise_addr",
            "s3_access_key_id",
            "s3_secret_access_key",
            "credential_broker_url",
        ] {
            let text = format!("{field} = \"private-value\"\n");
            let error = WorkerConfigDocument::parse(&text)
                .err()
                .expect("must reject");
            assert!(!error.contains("private-value"));
        }
        for text in [
            "unknown = 1",
            "listen = 42",
            "page_tti_hours = 18446744073709551615",
            "paged_miss_run_concurrency = 0",
            "background_scan_batch_size = 0",
            "async_eviction_low_watermark = 0.5",
            "[[buckets]]\nname = 'x'\ncredentials_source = 'aksk'\ns3_secret_access_key = 'private-value'",
        ] { assert!(WorkerConfigDocument::parse(text).is_err(), "{text}"); }
        for field in [
            "paged_miss_run_concurrency",
            "background_scan_batch_size",
            "background_delete_batch_size",
            "background_io_max_mb_per_sec",
            "background_delete_max_per_sec",
            "async_eviction_high_watermark",
            "async_eviction_low_watermark",
        ] {
            assert!(WorkerConfigDocument::parse(&format!("[namespaces.a]\n{field} = 1")).is_err());
        }
        let env = crate::WorkerConfigPatch {
            advertise_addr: Some("10.0.0.7".into()),
            node_id: Some("worker-7".into()),
            ..Default::default()
        };
        let cfg =
            crate::WorkerConfig::resolve(Default::default(), env, Default::default()).unwrap();
        assert_eq!(cfg.node_id.as_deref(), Some("worker-7"));
        assert!(cfg.advertise_addr.starts_with("10.0.0.7:"));
    }
}
