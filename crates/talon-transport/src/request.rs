//! Request-local credentials, independent of tracing and runtime instrumentation.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use talon_core::{ValidatedBrokerConfig, CREDENTIAL_BROKER_KEY};

/// Default initial Broker acquisition: four exchanges plus 100/200/400ms backoff.
pub const CREDENTIAL_REQUEST_TIMEOUT: Duration = Duration::from_millis(
    talon_core::BROKER_REQUEST_TIMEOUT_MS * talon_core::BROKER_INITIAL_ATTEMPTS as u64
        + talon_core::BROKER_INITIAL_BACKOFF_MS,
);

tokio::task_local! {
    static CURRENT: RequestContext;
}

/// Owned once at the public request boundary and shared across its RPCs.
#[derive(Clone, Default, Debug)]
pub struct RequestContext {
    pub credential_broker: Option<Arc<ValidatedBrokerConfig>>,
}

impl RequestContext {
    pub fn from_properties(properties: &[(&str, &str)]) -> Result<Self, String> {
        let mut context = Self::default();
        for (key, value) in properties {
            if *key != CREDENTIAL_BROKER_KEY {
                return Err("unsupported request property".into());
            }
            if context.credential_broker.is_some() {
                return Err("duplicate credential_broker request property".into());
            }
            context.credential_broker = Some(Arc::new(ValidatedBrokerConfig::from_json(value)?));
        }
        Ok(context)
    }

    /// Installs even an empty scope, preventing a nested unrelated request from
    /// inheriting credentials. The scope is restored on every future poll.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        CURRENT.scope(self.clone(), future).await
    }
}

pub fn current_credential_broker() -> Option<Arc<ValidatedBrokerConfig>> {
    CURRENT
        .try_with(|context| context.credential_broker.clone())
        .ok()
        .flatten()
}

/// Preserve the origin/RPC operation budget and add the complete initial
/// acquisition budget when request-specific Broker credentials are present.
pub fn request_timeout(base: Duration) -> Duration {
    let Some(broker) = current_credential_broker() else {
        return base;
    };
    let exchange = Duration::from_millis(
        broker
            .config()
            .origin
            .as_ref()
            .and_then(|origin| origin.request_timeout_ms)
            .unwrap_or(talon_core::BROKER_REQUEST_TIMEOUT_MS),
    );
    base.saturating_add(exchange.saturating_mul(talon_core::BROKER_INITIAL_ATTEMPTS))
        .saturating_add(Duration::from_millis(talon_core::BROKER_INITIAL_BACKOFF_MS))
}
