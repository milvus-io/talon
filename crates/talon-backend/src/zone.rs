//! Deployment-zone self-discovery (ADR 0006).
//!
//! Resolution order: the `TALON_ZONE` environment variable wins; otherwise
//! the process asks the Kubernetes API for its own node's standard
//! `topology.kubernetes.io/zone` label, using the node name injected through
//! the downward API as `TALON_NODE_NAME` and the pod's service-account
//! credentials (requires a `get nodes` RBAC grant). Cloud instance-metadata
//! endpoints are deliberately not used: they are unreachable from pods under
//! common EKS node settings.
//!
//! Resolution never fails startup. The zone is an optimization; a process
//! without one keeps today's zone-free behavior.

use std::path::Path;
use std::sync::Arc;

use crate::http::{HttpClient, HttpRequest, Method};
use crate::reqwest_client::ReqwestClient;
use crate::workload_identity::EnvFn;

/// Standard Kubernetes node label carrying the zone.
const KUBERNETES_ZONE_LABEL: &str = "topology.kubernetes.io/zone";

/// Mounted service-account credential directory inside a pod.
const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// A resolved deployment zone plus the bounded mechanism label for logs.
pub struct ResolvedZone {
    /// The zone, or `None` when no mechanism produced one.
    pub zone: Option<String>,
    /// Which mechanism answered: `env`, `kubernetes`, or `unknown`.
    pub source: &'static str,
}

/// Resolve this process's deployment zone.
///
/// Never returns an error: a failed lookup logs a warning (status only,
/// never credentials) and yields an unknown zone.
pub async fn resolve_zone() -> ResolvedZone {
    let env = |name: &str| crate::workload_identity::env_var(name);
    if let Some(zone) = env("TALON_ZONE") {
        return ResolvedZone {
            zone: Some(zone),
            source: "env",
        };
    }
    match kubernetes_node_zone(&env).await {
        Ok(Some(zone)) => ResolvedZone {
            zone: Some(zone),
            source: "kubernetes",
        },
        Ok(None) => ResolvedZone {
            zone: None,
            source: "unknown",
        },
        Err(error) => {
            tracing::warn!(%error, "zone lookup failed; running without a zone");
            ResolvedZone {
                zone: None,
                source: "unknown",
            }
        }
    }
}

/// Look up this pod's node zone through the in-cluster Kubernetes API.
///
/// Missing prerequisites (not on Kubernetes, or `TALON_NODE_NAME` not
/// injected) are a clean `None`; only an attempted-but-failed lookup is an
/// error.
async fn kubernetes_node_zone(env: EnvFn<'_>) -> Result<Option<String>, String> {
    let (Some(host), Some(node)) = (env("KUBERNETES_SERVICE_HOST"), env("TALON_NODE_NAME")) else {
        return Ok(None);
    };
    let port = env("KUBERNETES_SERVICE_PORT").unwrap_or_else(|| "443".to_string());
    let dir = Path::new(SERVICE_ACCOUNT_DIR);
    let token = std::fs::read_to_string(dir.join("token"))
        .map_err(|error| format!("service-account token is unreadable: {error}"))?;
    let ca = std::fs::read(dir.join("ca.crt"))
        .map_err(|error| format!("cluster CA is unreadable: {error}"))?;
    let certificate = reqwest::Certificate::from_pem(&ca)
        .map_err(|_| "cluster CA is not valid PEM".to_string())?;
    let client = reqwest::Client::builder()
        .add_root_certificate(certificate)
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|error| format!("kubernetes client construction failed: {error}"))?;
    let http: Arc<dyn HttpClient> = Arc::new(ReqwestClient::with_client(client));
    node_zone(&format!("https://{host}:{port}"), token.trim(), &node, http).await
}

/// Fetch one node's zone label from the Kubernetes API.
///
/// Errors carry the HTTP status only, never the bearer token.
pub async fn node_zone(
    api_base: &str,
    token: &str,
    node: &str,
    http: Arc<dyn HttpClient>,
) -> Result<Option<String>, String> {
    let request = HttpRequest::new(
        Method::Get,
        format!("{api_base}/api/v1/nodes/{node}"),
        vec![("authorization".to_string(), format!("Bearer {token}"))],
    );
    let response = http.execute(request).await?;
    if !response.is_success() {
        return Err(format!(
            "kubernetes node lookup returned status {}",
            response.status
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|_| "kubernetes node response is not valid JSON".to_string())?;
    // JSON-pointer escaping: the label key itself contains a slash.
    let pointer = format!(
        "/metadata/labels/{}",
        KUBERNETES_ZONE_LABEL.replace('/', "~1")
    );
    Ok(value
        .pointer(&pointer)
        .and_then(|zone| zone.as_str())
        .map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::HttpResponse;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MockHttp {
        response: HttpResponse,
        requests: Mutex<Vec<HttpRequest>>,
    }

    impl MockHttp {
        fn new(status: u16, body: &str) -> Arc<Self> {
            Arc::new(Self {
                response: HttpResponse {
                    status,
                    headers: Vec::new(),
                    body: bytes::Bytes::from(body.to_string()),
                },
                requests: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl HttpClient for MockHttp {
        async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, String> {
            self.requests.lock().unwrap().push(req);
            Ok(self.response.clone())
        }
    }

    #[tokio::test]
    async fn node_zone_reads_the_standard_label() {
        let http = MockHttp::new(
            200,
            r#"{"metadata":{"name":"node-1","labels":{"topology.kubernetes.io/zone":"us-west-2a","other":"x"}}}"#,
        );
        let zone = node_zone("https://10.96.0.1:443", "sa-token", "node-1", http.clone())
            .await
            .unwrap();
        assert_eq!(zone.as_deref(), Some("us-west-2a"));
        let request = http.requests.lock().unwrap()[0].clone();
        assert_eq!(request.url, "https://10.96.0.1:443/api/v1/nodes/node-1");
        assert_eq!(request.header("authorization"), Some("Bearer sa-token"));
    }

    #[tokio::test]
    async fn missing_label_is_none_and_denied_lookup_hides_the_token() {
        let http = MockHttp::new(200, r#"{"metadata":{"name":"node-1","labels":{}}}"#);
        let zone = node_zone("https://api", "sa-token", "node-1", http)
            .await
            .unwrap();
        assert!(zone.is_none());

        let denied = MockHttp::new(403, r#"{"kind":"Status","reason":"Forbidden"}"#);
        let error = node_zone("https://api", "sa-token", "node-1", denied)
            .await
            .unwrap_err();
        assert!(error.contains("403"));
        assert!(!error.contains("sa-token"));
    }

    #[tokio::test]
    async fn off_cluster_prerequisites_resolve_to_a_clean_none() {
        let empty: HashMap<String, String> = HashMap::new();
        let env = move |name: &str| empty.get(name).cloned();
        assert_eq!(kubernetes_node_zone(&env).await.unwrap(), None);
    }
}
