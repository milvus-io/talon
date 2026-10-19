//! Shared deployment configuration used by the Rust, C, and Python SDKs.
use crate::Error;

/// Per-client AZ routing. Defaults are read from the environment at build time.
/// Without `TALON_ZONE`, discover the node zone using the same resolver as
/// Worker/Gateway. Explicit builder options bypass environment and discovery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ZoneOptions {
    /// Reader zone, using the same spelling as Worker membership.
    pub zone: Option<String>,
    /// Require same-zone members; unknown or absent zones fail closed. Defaults off.
    pub affinity: bool,
}

impl ZoneOptions {
    /// Read `TALON_ZONE` and `TALON_ZONE_AFFINITY`, discovering the zone from
    /// Kubernetes when unset. Requires `TALON_NODE_NAME`, in-cluster credentials
    /// and `get nodes` RBAC. Lookup runs once and may block for the HTTP timeout
    /// (5 seconds). Safe to call inside or outside a Tokio runtime.
    /// Invalid booleans or zone lengths fail construction rather than silently
    /// disabling the requested routing policy.
    pub fn from_env() -> Result<Self, Error> {
        Self::resolve_with(|key| std::env::var(key).ok(), discover_zone)
    }

    fn resolve_with(
        get: impl Fn(&str) -> Option<String>,
        discover: impl FnOnce() -> Option<String>,
    ) -> Result<Self, Error> {
        let boolean = |key| match get(key).filter(|v| !v.trim().is_empty()) {
            None => Ok(false),
            Some(value) => talon_core::parse_bool_value(&value)
                .ok_or_else(|| Error::InvalidArgument(format!("{key} must be a boolean"))),
        };
        let mut value = Self {
            zone: get("TALON_ZONE").filter(|v| !v.trim().is_empty()),
            affinity: boolean("TALON_ZONE_AFFINITY")?,
        };
        value.validate()?;
        if value.zone.is_none()
            && get("KUBERNETES_SERVICE_HOST").is_some_and(|v| !v.is_empty())
            && get("TALON_NODE_NAME").is_some_and(|v| !v.is_empty())
        {
            value.zone = discover();
            value.validate()?;
        }
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self
            .zone
            .as_ref()
            .is_some_and(|z| z.trim().is_empty() || z.len() > 256)
        {
            return Err(Error::InvalidArgument(
                "zone must contain 1..=256 bytes".into(),
            ));
        }
        Ok(())
    }
}

// Construction is synchronous in Rust and the C/Python wrappers. A dedicated
// thread avoids nesting block_on (or dropping a runtime) on a caller's Tokio
// worker, including a current-thread runtime. The thread is joined before return.
fn discover_zone() -> Option<String> {
    match run_discovery(|| async { talon_backend::resolve_zone().await.zone }) {
        Ok(zone) => zone,
        Err(error) => {
            tracing::warn!(%error, "zone discovery failed; zone remains unknown");
            None
        }
    }
}

fn run_discovery<F>(resolve: impl FnOnce() -> F + Send + 'static) -> Result<Option<String>, String>
where
    F: std::future::Future<Output = Option<String>>,
{
    std::thread::Builder::new()
        .name("talon-zone-discovery".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("zone runtime creation failed: {error}"))?;
            Ok(runtime.block_on(resolve()))
        })
        .map_err(|error| format!("zone thread creation failed: {error}"))?
        .join()
        .map_err(|_| "zone discovery thread panicked".to_string())?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(get: impl Fn(&str) -> Option<String>) -> Result<ZoneOptions, Error> {
        ZoneOptions::resolve_with(get, || panic!("unexpected Kubernetes lookup"))
    }

    #[test]
    fn deployment_environment_defaults_and_validation() {
        assert_eq!(resolve(|_| None).unwrap(), ZoneOptions::default());
        let configured = resolve(|key| {
            Some(
                match key {
                    "TALON_ZONE" => "zone-a",
                    "TALON_ZONE_AFFINITY" => "true",
                    _ => return None,
                }
                .into(),
            )
        })
        .unwrap();
        assert_eq!(configured.zone.as_deref(), Some("zone-a"));
        assert!(configured.affinity);
        assert!(resolve(|k| (k == "TALON_ZONE_AFFINITY").then(|| "typo".into())).is_err());
        assert!(resolve(|k| (k == "TALON_ZONE").then(|| "z".repeat(257))).is_err());
    }

    #[test]
    fn kubernetes_discovery_preserves_strict_policy_and_validates_zone() {
        let env = |key: &str| match key {
            "KUBERNETES_SERVICE_HOST" => Some("api".into()),
            "TALON_NODE_NAME" => Some("node-1".into()),
            "TALON_ZONE_AFFINITY" => Some("true".into()),
            _ => None,
        };
        let discovered = ZoneOptions::resolve_with(env, || Some("zone-a".into())).unwrap();
        assert_eq!(discovered.zone.as_deref(), Some("zone-a"));
        assert!(discovered.affinity);
        let unknown = ZoneOptions::resolve_with(env, || None).unwrap();
        assert_eq!(unknown.zone, None);
        assert!(unknown.affinity);
        assert!(ZoneOptions::resolve_with(env, || Some("x".repeat(257))).is_err());
        let explicit = ZoneOptions::resolve_with(
            |key| {
                if key == "TALON_ZONE" {
                    Some("override".into())
                } else {
                    env(key)
                }
            },
            || panic!("explicit zone must bypass lookup"),
        )
        .unwrap();
        assert_eq!(explicit.zone.as_deref(), Some("override"));
    }

    fn assert_runtime_bridge() {
        assert_eq!(
            run_discovery(|| async {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                Some("zone-a".into())
            })
            .unwrap()
            .as_deref(),
            Some("zone-a")
        );
    }

    #[test]
    fn discovery_without_runtime() {
        assert_runtime_bridge();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn discovery_inside_current_thread_runtime() {
        assert_runtime_bridge();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn discovery_inside_multi_thread_runtime() {
        assert_runtime_bridge();
    }
}
