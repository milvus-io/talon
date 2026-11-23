use super::*;
use crate::{MetricLimits, NamespaceCacheConfig};

fn startup() -> WorkerConfig {
    WorkerConfig::resolve(Default::default(), Default::default(), Default::default()).unwrap()
}

fn plan(
    startup: &WorkerConfig,
    text: &str,
    env: WorkerConfigPatch,
    cli: WorkerConfigPatch,
) -> WorkerConfigPlan {
    WorkerConfigPlan::resolve(
        startup,
        WorkerConfigDocument::parse(text).unwrap(),
        env,
        cli,
    )
    .unwrap()
}

#[test]
fn hot_fields_apply_while_startup_fields_require_restart() {
    let startup = startup();
    let plan = plan(
        &startup,
        r#"
listen = "127.0.0.1:9001"
background_task_concurrency = 3
page_tti_hours = 2
paged_miss_run_concurrency = 3
background_scan_batch_size = 7
background_delete_batch_size = 5
background_io_max_mb_per_sec = 11
background_delete_max_per_sec = 13
async_eviction_high_watermark = 0.9
async_eviction_low_watermark = 0.6
[rate_limits]
enabled = true
default = { read_iops = { rate = 17 } }
"#,
        Default::default(),
        Default::default(),
    );
    let expected = serde_json::json!({
        "page_tti_hours": 2,
        "paged_miss_run_concurrency": 3,
        "background_scan_batch_size": 7,
        "background_delete_batch_size": 5,
        "background_io_max_mb_per_sec": 11,
        "background_delete_max_per_sec": 13,
        "async_eviction_high_watermark": 0.9,
        "async_eviction_low_watermark": 0.6,
    });
    for (field, value) in expected.as_object().unwrap() {
        assert_eq!(plan.effective_config[field], *value, "{field}");
        assert_eq!(plan.desired_config[field], *value, "{field}");
    }
    assert_eq!(
        plan.effective_config["rate_limits"]["default"]["read_iops"]["rate"],
        17
    );
    assert_eq!(plan.effective_config["listen"], startup.listen);
    assert_eq!(plan.desired_config["listen"], "127.0.0.1:9001");
    assert_eq!(
        plan.effective_config["background_task_concurrency"],
        startup.background_task_concurrency
    );
    assert_eq!(plan.desired_config["background_task_concurrency"], 3);
    assert_eq!(
        plan.pending_restart_fields,
        ["advertise_addr", "background_task_concurrency", "listen"]
    );
    assert_eq!(plan.tuning.page_tti_hours, Some(2));
    assert_eq!(plan.tuning.paged_miss_run_concurrency, Some(3));
    assert_eq!(
        plan.tuning
            .rate_limits
            .unwrap()
            .default
            .read_iops
            .unwrap()
            .rate,
        17
    );
}

#[test]
fn namespaces_keep_startup_geometry_and_normalize_live_inheritance() {
    let mut startup = startup();
    startup.namespaces.insert(
        "removed".into(),
        NamespaceCacheConfig {
            l2_page_size_bytes: Some(4096),
            page_access_checkpoint_interval_minutes: Some(3),
            ..Default::default()
        },
    );
    let plan = plan(
        &startup,
        r#"
page_tti_hours = 2
[[namespace_rules]]
pattern = "prod-*"
config = { page_tti_hours = 1, rate_limits = { read_iops = { rate = 10 } } }
[namespaces.prod-a]
page_tti_hours = 0
rate_limits = {}
"#,
        Default::default(),
        Default::default(),
    );
    assert_eq!(plan.pending_restart_fields, ["namespaces"]);
    assert_eq!(
        plan.effective_config["namespaces"]["removed"]["l2_page_size_bytes"],
        4096
    );
    assert_eq!(
        plan.effective_config["namespaces"]["removed"]["page_access_checkpoint_interval_minutes"],
        3
    );
    assert!(plan.desired_config["namespaces"]["removed"]["l2_page_size_bytes"].is_null());
    for view in [&plan.effective_config, &plan.desired_config] {
        assert_eq!(view["namespaces"]["removed"]["page_tti_hours"], 2);
        assert_eq!(view["namespaces"]["prod-a"]["page_tti_hours"], 0);
    }
    assert_eq!(plan.tuning.settings_for("removed").page_tti_hours, Some(2));
    assert_eq!(plan.tuning.settings_for("prod-a").page_tti_hours, Some(0));
    assert_eq!(
        plan.tuning.settings_for("prod-future").page_tti_hours,
        Some(1)
    );
    let limits = plan.tuning.effective_rate_limits(&startup.rate_limits);
    assert!(limits.enabled);
    assert!(limits.limits_for(&NamespaceId::named("prod-a")).is_empty());
    assert_eq!(
        limits
            .limits_for(&NamespaceId::named("prod-future"))
            .read_iops
            .unwrap()
            .rate,
        10
    );
}

#[test]
fn higher_priority_patches_override_file_and_keep_declared_namespace_views() {
    let env = WorkerConfigPatch::from_toml(
        "page_tti_hours = 2\n[namespaces.env-only]\npage_tti_hours = 4",
    )
    .unwrap();
    let cli = WorkerConfigPatch::from_toml("page_tti_hours = 3").unwrap();
    let plan = plan(
        &startup(),
        "page_tti_hours = 1\n[namespaces.file-only]\npage_tti_hours = 5",
        env,
        cli,
    );
    assert_eq!(plan.effective_config["page_tti_hours"], 3);
    assert_eq!(plan.desired_config["page_tti_hours"], 3);
    assert_eq!(plan.tuning.settings_for("env-only").page_tti_hours, Some(4));
    assert_eq!(
        plan.tuning.settings_for("file-only").page_tti_hours,
        Some(3)
    );
    assert_eq!(
        plan.effective_config["namespaces"]["file-only"]["page_tti_hours"],
        3
    );
    assert_eq!(
        plan.desired_config["namespaces"]["file-only"]["page_tti_hours"],
        3
    );
    assert!(plan.pending_restart_fields.is_empty());
}

#[test]
fn removing_hot_overrides_restores_defaults_without_restart() {
    let file = WorkerConfigPatch::from_toml(
        "paged_miss_run_concurrency = 3\n[namespaces.a]\npage_tti_hours = 2",
    )
    .unwrap();
    let startup = WorkerConfig::resolve(file, Default::default(), Default::default()).unwrap();
    let plan = plan(&startup, "", Default::default(), Default::default());
    assert_eq!(
        plan.tuning.paged_miss_run_concurrency,
        Some(WorkerConfig::default().paged_miss_run_concurrency)
    );
    assert_eq!(plan.tuning.settings_for("a").page_tti_hours, Some(0));
    assert_eq!(
        plan.effective_config["namespaces"]["a"]["page_tti_hours"],
        0
    );
    assert_eq!(plan.desired_config["namespaces"]["a"]["page_tti_hours"], 0);
    assert!(plan.pending_restart_fields.is_empty());
}

#[test]
fn tti_validation_uses_actual_storage_and_keeps_zero_disabled() {
    for hours in [None, Some(0)] {
        let settings = NamespaceDynamicConfig {
            page_tti_hours: hours,
            rate_limits: None,
        };
        assert!(settings.validate_tti_storage(false, u64::MAX).is_ok());
    }
    let settings = NamespaceDynamicConfig {
        page_tti_hours: Some(1),
        rate_limits: None,
    };
    assert_eq!(
        settings.validate_tti_storage(false, 1),
        Err("TTI requires paged L2 or small-object storage".into())
    );
    assert!(settings.validate_tti_storage(true, 3_600_000).is_ok());
    assert_eq!(
        settings.validate_tti_storage(true, 3_600_001),
        Err("checkpoint interval exceeds TTI".into())
    );
    let settings = NamespaceDynamicConfig {
        page_tti_hours: Some(u64::MAX),
        rate_limits: Some(MetricLimits::default()),
    };
    assert_eq!(
        settings.validate_tti_storage(true, 1),
        Err("TTI duration overflows".into())
    );
}

#[test]
fn status_views_keep_skipped_credentials_redacted() {
    let mut startup = startup();
    startup.s3_access_key_id = Some("redaction-fixture-id".into());
    startup.s3_secret_access_key = Some(String::from("redaction-fixture-value").into());
    let plan = plan(
        &startup,
        "paged_miss_run_concurrency = 3",
        Default::default(),
        Default::default(),
    );
    for view in [&plan.effective_config, &plan.desired_config] {
        assert!(view.get("s3_access_key_id").is_none());
        assert!(view.get("s3_secret_access_key").is_none());
        assert!(!view.to_string().contains("redaction-fixture"));
    }
    assert!(plan.pending_restart_fields.is_empty());
}
