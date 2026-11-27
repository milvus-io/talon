use talon_core::config::NamespaceCacheConfig;
use talon_core::dynamic_config::{NamespaceDynamicConfig, WorkerDynamicConfig};
use talon_core::namespace_rules::{self, NamespaceRule};
use talon_core::{Error, MetricLimits, NamespaceId, RateLimit, RateLimitPolicy, WorkerConfig};

#[test]
fn namespace_config_returns_validated_worker_config() {
    let namespace = NamespaceId::named("tenant");
    let mut config = WorkerConfig::default();
    config.namespaces.insert(
        "tenant".into(),
        NamespaceCacheConfig {
            page_tti_hours: Some(1),
            ..Default::default()
        },
    );
    let resolved: WorkerConfig = config.for_namespace(&namespace).unwrap();
    assert_eq!(resolved.page_tti_hours, 1);
    assert_eq!(resolved.block_size, config.block_size);
    assert_eq!(resolved.capacity_bytes, config.capacity_bytes);
    assert!(resolved.namespaces.is_empty());

    config.backend = Some("unknown-store".into());
    config.listen.clear();
    assert!(matches!(
        config.for_namespace(&namespace),
        Err(Error::Other(message)) if message == "unknown backend: unknown-store"
    ));
}

#[test]
fn public_namespace_rule_values_merge_by_field() {
    let limits = MetricLimits {
        read_iops: Some(RateLimit::new(10, 1.0).unwrap()),
        ..Default::default()
    };
    let rules: Vec<NamespaceRule> = vec![
        NamespaceRule {
            pattern: "prod-*".into(),
            config: NamespaceDynamicConfig {
                page_tti_hours: Some(2),
                rate_limits: Some(limits),
            },
        },
        NamespaceRule {
            pattern: "prod-east-*".into(),
            config: NamespaceDynamicConfig {
                page_tti_hours: Some(3),
                rate_limits: None,
            },
        },
    ];
    let exact = talon_core::NamespaceCacheConfig {
        l2_page_size_bytes: Some(1024),
        rate_limits: Some(MetricLimits::default()),
        ..Default::default()
    };
    namespace_rules::validate(&[]).unwrap();
    let name = "prod-east-api";
    let resolved = namespace_rules::resolve(&rules, Some(&exact), name);
    assert_eq!(resolved.page_tti_hours, Some(3));
    assert_eq!(resolved.rate_limits, exact.rate_limits);
    assert_eq!(resolved.l2_page_size_bytes, exact.l2_page_size_bytes);

    let policy = RateLimitPolicy {
        enabled: true,
        namespace_rules: rules.clone(),
        ..Default::default()
    };
    assert_eq!(policy.limits_for(&NamespaceId::named(name)), limits);
    let tuning = WorkerDynamicConfig {
        namespace_rules: rules.clone(),
        ..Default::default()
    };
    assert_eq!(
        tuning
            .effective_rate_limits(&Default::default())
            .namespace_rules,
        rules
    );
}
