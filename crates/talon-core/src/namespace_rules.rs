//! Ordered wildcard policies for cache namespace names.
use crate::{Error, NamespaceCacheConfig, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceRule {
    pub pattern: String,
    pub config: crate::dynamic_config::NamespaceDynamicConfig,
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

pub fn resolve(
    rules: &[NamespaceRule],
    exact: Option<&NamespaceCacheConfig>,
    name: &str,
) -> NamespaceCacheConfig {
    let mut result = NamespaceCacheConfig::default();
    for rule in rules.iter().filter(|r| matches(&r.pattern, name)) {
        result.page_tti_hours = rule.config.page_tti_hours.or(result.page_tti_hours);
        result.rate_limits = rule.config.rate_limits.or(result.rate_limits);
    }
    if let Some(own) = exact {
        macro_rules! merge { ($($field:ident),*) => { $(result.$field = own.$field.or(result.$field);)* }; }
        merge!(
            l2_page_size_bytes,
            l1_page_size_bytes,
            page_tti_hours,
            page_access_checkpoint_interval_minutes,
            page_gc_interval_minutes,
            paged_miss_run_concurrency,
            rate_limits
        );
    }
    result
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
