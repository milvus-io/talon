//! Talon observability assets: Prometheus recording rules, alert rules, and the
//! Grafana dashboard, embedded and validated so CI fails if they drift from the
//! metric contract or become malformed.
//!
//! The YAML/JSON files under `deploy/observability/` are the deployable source
//! of truth. This crate does not re-implement them; it [`include_str!`]s them so
//! the existing `cargo test` job validates their syntax and invariants without a
//! separate CI service (there is no live Prometheus/Grafana in CI). The parsed
//! accessors are also usable by the coordinator's asset-serving layer if it
//! chooses to embed the dashboard later.

use serde::Deserialize;

pub mod deploy;

/// Raw Prometheus recording-rules YAML.
pub const RECORDING_RULES_YAML: &str =
    include_str!("../../../deploy/observability/prometheus/talon.rules.yml");

/// Raw Prometheus alerting-rules YAML.
pub const ALERT_RULES_YAML: &str =
    include_str!("../../../deploy/observability/prometheus/talon.alerts.yml");

/// Raw Grafana dashboard JSON.
pub const DASHBOARD_JSON: &str =
    include_str!("../../../deploy/observability/grafana/talon-cluster-overview.json");

/// A Prometheus rule group file (`groups:` at the top level).
#[derive(Debug, Clone, Deserialize)]
pub struct RuleFile {
    /// Ordered rule groups.
    pub groups: Vec<RuleGroup>,
}

/// One named group of rules evaluated at a shared interval.
#[derive(Debug, Clone, Deserialize)]
pub struct RuleGroup {
    /// Group name (must be unique within a file).
    pub name: String,
    /// Optional evaluation interval (e.g. `30s`).
    #[serde(default)]
    pub interval: Option<String>,
    /// Rules in the group.
    pub rules: Vec<Rule>,
}

/// A single recording or alerting rule.
///
/// Recording rules set `record`; alerting rules set `alert` plus `labels` and
/// `annotations`. Exactly one of `record`/`alert` is present in a valid file.
#[derive(Debug, Clone, Deserialize)]
pub struct Rule {
    /// Recording-rule output series name.
    #[serde(default)]
    pub record: Option<String>,
    /// Alert name.
    #[serde(default)]
    pub alert: Option<String>,
    /// PromQL expression.
    pub expr: String,
    /// Pending duration before an alert fires.
    #[serde(default, rename = "for")]
    pub for_: Option<String>,
    /// Rule labels (severity/component for alerts).
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    /// Alert annotations (summary/description/runbook_url).
    #[serde(default)]
    pub annotations: std::collections::BTreeMap<String, String>,
}

/// Parse the recording rules, panicking with context on malformed YAML.
pub fn recording_rules() -> RuleFile {
    serde_yaml::from_str(RECORDING_RULES_YAML).expect("recording rules YAML must parse")
}

/// Parse the alert rules, panicking with context on malformed YAML.
pub fn alert_rules() -> RuleFile {
    serde_yaml::from_str(ALERT_RULES_YAML).expect("alert rules YAML must parse")
}

/// Parse the Grafana dashboard, panicking with context on malformed JSON.
pub fn dashboard() -> serde_json::Value {
    serde_json::from_str(DASHBOARD_JSON).expect("dashboard JSON must parse")
}

impl RuleFile {
    /// Iterate every rule across all groups.
    pub fn iter_rules(&self) -> impl Iterator<Item = &Rule> {
        self.groups.iter().flat_map(|g| g.rules.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The stable metric names delivered by #76 (worker) and #77 (coordinator).
    /// Every base metric referenced by a rule must be one of these, so a rule
    /// can never silently depend on a renamed or nonexistent series. Recording
    /// rules also publish derived names, which are added below.
    const CONTRACT_METRICS: &[&str] = &[
        // worker (#76)
        "talon_worker_requests_total",
        "talon_worker_request_errors_total",
        "talon_worker_request_duration_seconds_bucket",
        "talon_worker_request_duration_seconds_count",
        "talon_worker_cache_hits_total",
        "talon_worker_cache_misses_total",
        "talon_worker_bytes_served_total",
        "talon_worker_backend_fetch_errors_total",
        "talon_worker_backend_fetch_duration_seconds_count",
        "talon_worker_capacity_bytes",
        "talon_worker_resident_bytes",
        "talon_worker_ready",
        // coordinator (#77)
        "talon_coordinator_control_requests_total",
        "talon_coordinator_control_errors_total",
        "talon_coordinator_control_duration_seconds_bucket",
        "talon_coordinator_placement_errors_total",
        "talon_coordinator_state_store_errors_total",
        "talon_coordinator_state_snapshot_age_seconds",
        "talon_coordinator_live_nodes",
        "talon_coordinator_ready",
        // scrape meta (always present)
        "up",
    ];

    /// Extract identifier-shaped tokens that look like metric or recording-rule
    /// names from a PromQL expression. This is a lint, not a full parser: it
    /// pulls every `[a-zA-Z_:][a-zA-Z0-9_:]*` token and lets the caller filter
    /// out functions and derived names.
    fn referenced_names(expr: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let bytes = expr.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_alphabetic() || c == '_' || c == ':' {
                let start = i;
                while i < bytes.len() {
                    let d = bytes[i] as char;
                    if d.is_ascii_alphanumeric() || d == '_' || d == ':' {
                        i += 1;
                    } else {
                        break;
                    }
                }
                out.insert(expr[start..i].to_string());
            } else {
                i += 1;
            }
        }
        out
    }

    /// PromQL functions/keywords that are not metric names.
    const PROMQL_KEYWORDS: &[&str] = &[
        "sum",
        "rate",
        "by",
        "on",
        "clamp_min",
        "clamp_max",
        "histogram_quantile",
        "max",
        "min",
        "avg",
        "count",
        "vector",
        "and",
        "or",
        "unless",
        "le",
        "cluster",
        "instance",
        "job",
        "role",
        "health",
        "worker",
        "healthy",
        "increase",
        "irate",
        "humanizePercentage",
        "printf",
    ];

    #[test]
    fn recording_and_alert_files_parse() {
        assert!(!recording_rules().groups.is_empty());
        assert!(!alert_rules().groups.is_empty());
        assert!(dashboard().is_object());
    }

    #[test]
    fn group_names_are_unique() {
        for file in [recording_rules(), alert_rules()] {
            let mut seen = BTreeSet::new();
            for g in &file.groups {
                assert!(seen.insert(g.name.clone()), "duplicate group {}", g.name);
            }
        }
    }

    #[test]
    fn recording_rules_only_reference_contract_metrics() {
        // Names a recording rule is allowed to reference: the raw contract plus
        // the derived series recording rules themselves publish.
        let recorded: BTreeSet<String> = recording_rules()
            .iter_rules()
            .filter_map(|r| r.record.clone())
            .collect();
        let mut allowed: BTreeSet<String> =
            CONTRACT_METRICS.iter().map(|s| s.to_string()).collect();
        allowed.extend(recorded.iter().cloned());
        allowed.extend(PROMQL_KEYWORDS.iter().map(|s| s.to_string()));

        for rule in recording_rules().iter_rules() {
            for name in referenced_names(&rule.expr) {
                // Skip numeric-suffixed histogram le tokens and pure keywords.
                if allowed.contains(&name) {
                    continue;
                }
                // A bare metric-looking token (contains talon_ or ':') that is
                // not allowed is a contract violation.
                assert!(
                    !name.starts_with("talon_") && !name.contains(':'),
                    "rule {:?} references unknown metric `{}`",
                    rule.record,
                    name
                );
            }
        }
    }

    #[test]
    fn alerts_reference_recorded_or_contract_metrics() {
        let recorded: BTreeSet<String> = recording_rules()
            .iter_rules()
            .filter_map(|r| r.record.clone())
            .collect();
        let mut allowed: BTreeSet<String> =
            CONTRACT_METRICS.iter().map(|s| s.to_string()).collect();
        allowed.extend(recorded);
        allowed.extend(PROMQL_KEYWORDS.iter().map(|s| s.to_string()));

        for rule in alert_rules().iter_rules() {
            for name in referenced_names(&rule.expr) {
                if allowed.contains(&name) {
                    continue;
                }
                assert!(
                    !name.starts_with("talon_") && !name.contains(':'),
                    "alert {:?} references unknown metric `{}`",
                    rule.alert,
                    name
                );
            }
        }
    }

    #[test]
    fn every_alert_has_required_labels_and_annotations() {
        for rule in alert_rules().iter_rules() {
            let alert = rule.alert.clone().expect("alert rule must set `alert`");
            // Severity is required and constrained to the routing vocabulary.
            let severity = rule
                .labels
                .get("severity")
                .unwrap_or_else(|| panic!("alert {alert} missing severity label"));
            assert!(
                matches!(severity.as_str(), "critical" | "warning"),
                "alert {alert} has non-standard severity {severity:?}"
            );
            assert!(
                rule.labels.contains_key("component"),
                "alert {alert} missing component label"
            );
            for key in ["summary", "description", "runbook_url"] {
                assert!(
                    rule.annotations.contains_key(key),
                    "alert {alert} missing {key} annotation"
                );
            }
            // A pending duration avoids paging on a single scrape blip.
            assert!(rule.for_.is_some(), "alert {alert} missing `for` duration");
            // Runbook links must point at the operations runbook anchor.
            let runbook = &rule.annotations["runbook_url"];
            assert!(
                runbook.contains("docs/operations/runbook.md#"),
                "alert {alert} runbook_url {runbook:?} lacks a stable anchor"
            );
        }
    }

    #[test]
    fn recording_rule_names_follow_level_metric_convention() {
        // Recording-rule output names must contain a ':' (the Prometheus
        // level:metric:op convention) so they never collide with raw metrics.
        for rule in recording_rules().iter_rules() {
            let name = rule.record.clone().expect("recording rule sets `record`");
            assert!(
                name.contains(':'),
                "recording rule `{name}` should use level:metric:op naming"
            );
        }
    }

    #[test]
    fn dashboard_is_bounded_and_cluster_scoped() {
        let d = dashboard();
        // A cluster template variable must exist so queries stay scoped.
        let vars = d["templating"]["list"]
            .as_array()
            .expect("dashboard has templating list");
        assert!(
            vars.iter().any(|v| v["name"] == "cluster"),
            "dashboard must expose a `cluster` template variable"
        );
        // No panel may reference a per-object / high-cardinality label.
        let json = DASHBOARD_JSON;
        for banned in ["object_path", "block_id", "bucket"] {
            assert!(
                !json.contains(banned),
                "dashboard must not use high-cardinality label `{banned}`"
            );
        }
        // Every panel target must scope by the cluster variable.
        let panels = d["panels"].as_array().expect("dashboard has panels");
        for panel in panels {
            if let Some(targets) = panel["targets"].as_array() {
                for t in targets {
                    let expr = t["expr"].as_str().unwrap_or("");
                    assert!(
                        expr.contains("$cluster") || expr.contains("vector("),
                        "panel {:?} target `{}` is not cluster-scoped",
                        panel["title"],
                        expr
                    );
                }
            }
        }
    }

    #[test]
    fn representative_promql_matches_fixture_semantics() {
        // Ground-truth the hit-rate recording rule's *shape* against a hand
        // computed fixture: hits=8/s, misses=2/s -> 0.8. We can't run PromQL
        // here, but we assert the expression is the ratio we documented so a
        // future edit that flips numerator/denominator is caught.
        let rule = recording_rules()
            .iter_rules()
            .find(|r| r.record.as_deref() == Some("cluster:talon_worker_cache_hit:ratio5m"))
            .expect("hit-rate rule exists")
            .clone();
        let expr = rule.expr.replace(['\n', ' '], "");
        // numerator is hits, denominator is hits+misses.
        assert!(expr.starts_with("sumby(cluster)(rate(talon_worker_cache_hits_total[5m]))/"));
        assert!(expr.contains("talon_worker_cache_hits_total[5m]))+sumby(cluster)(rate(talon_worker_cache_misses_total[5m]))"));
    }
}
