//! Ordered wildcard policies for cache namespace names.
use crate::{Error, MetricLimits, Result};
use serde::{Deserialize, Serialize};

/// Per-namespace cache overrides. Omitted fields inherit the worker defaults.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NamespaceCacheConfig {
    pub l2_page_size_bytes: Option<u64>,
    pub l1_page_size_bytes: Option<u64>,
    pub page_tti_hours: Option<u64>,
    pub page_access_checkpoint_interval_minutes: Option<u64>,
    pub page_gc_interval_minutes: Option<u64>,
    pub paged_miss_run_concurrency: Option<usize>,
    pub rate_limits: Option<MetricLimits>,
}

/// A complete override for one namespace. Omitted fields inherit worker defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NamespaceDynamicConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limits: Option<MetricLimits>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_tti_hours: Option<u64>,
}

/// An override keyed by a case-sensitive namespace wildcard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceRule {
    pub pattern: String,
    pub config: NamespaceDynamicConfig,
}

pub fn validate(rules: &[NamespaceRule]) -> Result<()> {
    if rules.len() > 256 {
        return Err(Error::Other(
            "too many namespace rules (maximum 256)".into(),
        ));
    }
    for rule in rules {
        let pattern = &rule.pattern;
        if pattern.is_empty()
            || pattern.len() > 255
            || !pattern
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'*' | b'?'))
        {
            return Err(Error::Other(format!(
                "invalid namespace pattern: {pattern:?}"
            )));
        }
    }
    Ok(())
}

/// Case-sensitive full-name matching. `*` matches zero or more bytes, `?` one.
pub fn matches(pattern: &str, name: &str) -> bool {
    let (p, n) = (pattern.as_bytes(), name.as_bytes());
    let (mut i, mut j, mut star, mut retry) = (0, 0, None, 0);
    while j < n.len() {
        if i < p.len() && (p[i] == b'?' || p[i] == n[j]) {
            i += 1;
            j += 1;
        } else if i < p.len() && p[i] == b'*' {
            star = Some(i);
            i += 1;
            retry = j;
        } else if let Some(s) = star {
            retry += 1;
            j = retry;
            i = s + 1;
        } else {
            return false;
        }
    }
    while i < p.len() && p[i] == b'*' {
        i += 1;
    }
    i == p.len()
}

/// Merge wildcard overrides in order, then apply fields from the exact match.
pub fn resolve(
    rules: &[NamespaceRule],
    exact: Option<&NamespaceCacheConfig>,
    name: &str,
) -> NamespaceCacheConfig {
    let mut result = exact.cloned().unwrap_or_default();
    result.page_tti_hours = result
        .page_tti_hours
        .or_else(|| resolve_field(rules, name, |rule| rule.page_tti_hours));
    result.rate_limits = result
        .rate_limits
        .or_else(|| resolve_field(rules, name, |rule| rule.rate_limits));
    result
}

/// Pick the last matching rule that sets the requested field.
pub(crate) fn resolve_field<V>(
    rules: &[NamespaceRule],
    name: &str,
    field: impl Fn(&NamespaceDynamicConfig) -> Option<V>,
) -> Option<V> {
    rules
        .iter()
        .rev()
        .filter(|rule| matches(&rule.pattern, name))
        .find_map(|rule| field(&rule.config))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wildcard_full_name_matching() {
        for (pattern, name, expected) in [
            ("prod-*", "prod-a", true),
            ("prod-*", "xprod-a", false),
            ("a?", "ab", true),
            ("a?", "a", false),
            ("a*b*c", "axbybc", true),
            ("*", "default", true),
            ("prod-*", "Prod-a", false),
            ("a**", "a", true),
            ("*a", "abc", false),
        ] {
            assert_eq!(matches(pattern, name), expected, "{pattern}: {name}");
        }
    }
    #[test]
    fn rules_layer_fields_and_exact_entries_win() {
        let config = crate::WorkerConfig::resolve(
            crate::WorkerConfigPatch::from_toml(
                r#"
[[namespace_rules]]
pattern = "prod-*"
config = { page_tti_hours = 12, rate_limits = { read_iops = { rate = 10 } } }
[[namespace_rules]]
pattern = "*-analytics"
config = { page_tti_hours = 24 }
[namespaces.prod-analytics]
page_tti_hours = 0
rate_limits = {}
"#,
            )
            .unwrap(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let ns = crate::NamespaceId::named("prod-other");
        assert_eq!(config.for_namespace(&ns).unwrap().page_tti_hours, 12);
        assert_eq!(
            config.rate_limits.limits_for(&ns).read_iops.unwrap().rate,
            10
        );
        let exact = crate::NamespaceId::named("prod-analytics");
        assert_eq!(config.for_namespace(&exact).unwrap().page_tti_hours, 0);
        assert!(config.rate_limits.limits_for(&exact).is_empty());
        assert_eq!(
            config
                .for_namespace(&crate::NamespaceId::named("dev-analytics"))
                .unwrap()
                .page_tti_hours,
            24
        );
        let dynamic = crate::dynamic_config::WorkerDynamicConfig::from_config(&config);
        assert_eq!(dynamic.settings_for("prod-future").page_tti_hours, Some(12));
    }

    #[test]
    fn startup_and_reload_layer_limits_over_enabled_or_disabled_policy() {
        for enabled in [false, true] {
            let input = format!(
                r#"
[[namespace_rules]]
pattern = "prod-*"
config = {{ rate_limits = {{ read_iops = {{ rate = 10 }} }} }}
[[namespace_rules]]
pattern = "prod-us-*"
config = {{ rate_limits = {{ read_iops = {{ rate = 20 }} }} }}
[[namespace_rules]]
pattern = "*"
config = {{ page_tti_hours = 24 }}
[rate_limits]
enabled = {enabled}
default = {{ read_iops = {{ rate = 30 }} }}
overrides = {{ "prod-us-explicit" = {{ read_iops = {{ rate = 40 }} }} }}
[namespaces.prod-us-exempt]
rate_limits = {{}}
"#
            );
            let document = crate::dynamic_config::WorkerConfigDocument::parse(&input).unwrap();
            let startup = crate::WorkerConfig::resolve(
                document.patch,
                Default::default(),
                Default::default(),
            )
            .unwrap()
            .rate_limits;
            let reload = document.tuning.effective_rate_limits(&Default::default());
            assert_eq!(startup, reload);
            assert_eq!(reload.namespace_rules, document.tuning.namespace_rules);
            assert!(reload.enabled);
            for (name, expected) in [
                ("prod-eu", Some(10)),
                ("prod-us-new", Some(20)),
                ("prod-us-explicit", Some(if enabled { 40 } else { 20 })),
                ("prod-us-exempt", None),
                ("other", enabled.then_some(30)),
            ] {
                assert_eq!(
                    reload
                        .limits_for(&crate::NamespaceId::named(name))
                        .read_iops
                        .map(|limit| limit.rate),
                    expected,
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn invalid_patterns_and_limits_are_rejected() {
        for pattern in ["", "a/b", "[a-z]", "a\\b"] {
            assert!(validate(&[NamespaceRule {
                pattern: pattern.into(),
                config: Default::default()
            }])
            .is_err());
        }
        assert!(crate::dynamic_config::WorkerConfigDocument::parse("[[namespace_rules]]\npattern = '*'\nconfig = { page_tti_hours = 18446744073709551615 }").is_err());
    }
}
