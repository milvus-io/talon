//! Validation of the Kubernetes HA deployment manifests under
//! `deploy/kubernetes/` (issue #86).
//!
//! CI has no Kubernetes API server, so — like the observability assets — the
//! manifests are embedded and structurally validated in the `cargo test` job:
//! every document parses as YAML, the coordinator Deployments request the
//! required replica count and probes, exactly one state backend is selected per
//! Deployment, and no real secret values are committed. This catches drift
//! (renamed env var, dropped probe, an accidentally committed credential)
//! without booting a cluster.

use serde::Deserialize;

/// Kubernetes-backend coordinator Deployment.
pub const COORDINATOR_KUBERNETES_YAML: &str =
    include_str!("../../../deploy/kubernetes/coordinator-kubernetes.yaml");
/// etcd-backend coordinator Deployment.
pub const COORDINATOR_ETCD_YAML: &str =
    include_str!("../../../deploy/kubernetes/coordinator-etcd.yaml");
/// Service + PodDisruptionBudget.
pub const SERVICE_YAML: &str = include_str!("../../../deploy/kubernetes/service.yaml");
/// Least-privilege RBAC for the Kubernetes backend.
pub const RBAC_YAML: &str = include_str!("../../../deploy/kubernetes/rbac.yaml");
/// Example Secret template for etcd credentials/TLS.
pub const ETCD_SECRET_EXAMPLE_YAML: &str =
    include_str!("../../../deploy/kubernetes/etcd-secret.example.yaml");
/// Prometheus Operator ServiceMonitor.
pub const SERVICEMONITOR_YAML: &str =
    include_str!("../../../deploy/kubernetes/servicemonitor.yaml");

/// Parse every `---`-separated document in a multi-doc YAML file.
pub fn parse_documents(yaml: &str) -> Vec<serde_yaml::Value> {
    serde_yaml::Deserializer::from_str(yaml)
        .map(|de| serde_yaml::Value::deserialize(de).expect("manifest document must parse"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(yaml: &str) -> Vec<String> {
        parse_documents(yaml)
            .iter()
            .filter_map(|d| d.get("kind").and_then(|k| k.as_str()).map(String::from))
            .collect()
    }

    #[test]
    fn all_manifests_parse() {
        for (name, yaml) in [
            ("coordinator-kubernetes", COORDINATOR_KUBERNETES_YAML),
            ("coordinator-etcd", COORDINATOR_ETCD_YAML),
            ("service", SERVICE_YAML),
            ("rbac", RBAC_YAML),
            ("etcd-secret.example", ETCD_SECRET_EXAMPLE_YAML),
            ("servicemonitor", SERVICEMONITOR_YAML),
        ] {
            let docs = parse_documents(yaml);
            assert!(!docs.is_empty(), "{name} produced no documents");
        }
    }

    #[test]
    fn both_coordinator_deployments_run_three_replicas_behind_a_service() {
        for yaml in [COORDINATOR_KUBERNETES_YAML, COORDINATOR_ETCD_YAML] {
            let deployment = parse_documents(yaml)
                .into_iter()
                .find(|d| d.get("kind").and_then(|k| k.as_str()) == Some("Deployment"))
                .expect("a Deployment");
            let replicas = deployment["spec"]["replicas"].as_u64().unwrap();
            assert!(replicas >= 3, "HA needs >= 3 replicas, got {replicas}");
            // Zero-unavailable rolling update preserves quorum.
            assert_eq!(
                deployment["spec"]["strategy"]["rollingUpdate"]["maxUnavailable"]
                    .as_u64()
                    .unwrap(),
                0
            );
        }
        // The Service + PDB exist and the PDB keeps a majority available.
        let svc_kinds = kinds(SERVICE_YAML);
        assert!(svc_kinds.contains(&"Service".to_string()));
        assert!(svc_kinds.contains(&"PodDisruptionBudget".to_string()));
        let pdb = parse_documents(SERVICE_YAML)
            .into_iter()
            .find(|d| d.get("kind").and_then(|k| k.as_str()) == Some("PodDisruptionBudget"))
            .unwrap();
        assert_eq!(pdb["spec"]["minAvailable"].as_u64().unwrap(), 2);
    }

    #[test]
    fn each_deployment_selects_exactly_one_backend() {
        let backend_of = |yaml: &str| -> String {
            let dep = parse_documents(yaml)
                .into_iter()
                .find(|d| d.get("kind").and_then(|k| k.as_str()) == Some("Deployment"))
                .unwrap();
            let envs = dep["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_sequence()
                .unwrap()
                .clone();
            let backends: Vec<String> = envs
                .iter()
                .filter(|e| e["name"].as_str() == Some("TALON_COORDINATOR_STATE_BACKEND"))
                .map(|e| e["value"].as_str().unwrap_or("").to_string())
                .collect();
            assert_eq!(backends.len(), 1, "exactly one backend selector");
            backends[0].clone()
        };
        assert_eq!(backend_of(COORDINATOR_KUBERNETES_YAML), "kubernetes");
        assert_eq!(backend_of(COORDINATOR_ETCD_YAML), "etcd");
    }

    #[test]
    fn deployments_have_all_three_probes_and_graceful_termination() {
        for yaml in [COORDINATOR_KUBERNETES_YAML, COORDINATOR_ETCD_YAML] {
            let dep = parse_documents(yaml)
                .into_iter()
                .find(|d| d.get("kind").and_then(|k| k.as_str()) == Some("Deployment"))
                .unwrap();
            let spec = &dep["spec"]["template"]["spec"];
            assert!(spec["terminationGracePeriodSeconds"].as_u64().unwrap() >= 20);
            let c = &spec["containers"][0];
            for probe in ["startupProbe", "livenessProbe", "readinessProbe"] {
                assert!(!c[probe].is_null(), "{yaml} missing {probe}");
            }
            // Topology spread keeps a node loss from taking quorum.
            assert!(!dep["spec"]["template"]["spec"]["topologySpreadConstraints"].is_null());
        }
    }

    #[test]
    fn kubernetes_backend_uses_least_privilege_lease_rbac() {
        let docs = parse_documents(RBAC_YAML);
        let role = docs
            .iter()
            .find(|d| d.get("kind").and_then(|k| k.as_str()) == Some("Role"))
            .expect("a namespaced Role, not ClusterRole");
        let rules = role["rules"].as_sequence().unwrap();
        // Only coordination.k8s.io/leases, nothing cluster-wide or secrets.
        for rule in rules {
            let groups = rule["apiGroups"].as_sequence().unwrap();
            assert!(groups
                .iter()
                .all(|g| g.as_str() == Some("coordination.k8s.io")));
            let resources = rule["resources"].as_sequence().unwrap();
            assert!(resources.iter().all(|r| r.as_str() == Some("leases")));
        }
        // No ClusterRole anywhere (would be over-broad).
        assert!(!kinds(RBAC_YAML).contains(&"ClusterRole".to_string()));
    }

    #[test]
    fn etcd_credentials_come_from_secrets_not_inline() {
        let dep = parse_documents(COORDINATOR_ETCD_YAML)
            .into_iter()
            .find(|d| d.get("kind").and_then(|k| k.as_str()) == Some("Deployment"))
            .unwrap();
        let envs = dep["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_sequence()
            .unwrap();
        // Password must be a secretKeyRef, never an inline value.
        let pw = envs
            .iter()
            .find(|e| e["name"].as_str() == Some("TALON_COORDINATOR_ETCD_PASSWORD"))
            .expect("etcd password env");
        assert!(pw.get("value").is_none(), "password must not be inline");
        assert!(!pw["valueFrom"]["secretKeyRef"].is_null());
    }

    #[test]
    fn example_secret_contains_no_real_looking_values() {
        // The committed example must be an obvious placeholder, never a real
        // credential. Every non-endpoint secret value is REPLACE_ME.
        let docs = parse_documents(ETCD_SECRET_EXAMPLE_YAML);
        for doc in docs {
            if doc.get("kind").and_then(|k| k.as_str()) != Some("Secret") {
                continue;
            }
            if let Some(data) = doc.get("stringData").and_then(|d| d.as_mapping()) {
                for (k, v) in data {
                    let key = k.as_str().unwrap_or("");
                    let val = v.as_str().unwrap_or("");
                    // Only credential/material keys must be placeholders;
                    // endpoints and username are not sensitive.
                    if matches!(key, "endpoints" | "username") {
                        continue;
                    }
                    assert!(
                        val.contains("REPLACE_ME"),
                        "example secret key {key} must be a placeholder"
                    );
                }
            }
        }
    }
}
