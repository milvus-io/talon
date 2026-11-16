//! ConfigMap-projected file loading, validation, and last-good runtime state.
use crate::{WorkerObservability, WorkerRuntime};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use talon_core::dynamic_config::WorkerDynamicConfig;

const RELOAD_INTERVAL: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Default, Serialize)]
pub struct ConfigReloadStatus {
    pub enabled: bool,
    pub applied_hash: Option<String>,
    pub observed_hash: Option<String>,
    pub last_error: Option<String>,
    pub config: Option<WorkerDynamicConfig>,
    pub effective_config: Option<serde_json::Value>,
    pub desired_config: Option<serde_json::Value>,
    pub restart_required: bool,
    pub pending_restart_fields: Vec<String>,
}

#[derive(Default)]
pub struct DynamicConfig {
    status: ConfigReloadStatus,
    startup: Option<talon_core::WorkerConfig>,
    env: talon_core::WorkerConfigPatch,
    cli: talon_core::WorkerConfigPatch,
}
impl DynamicConfig {
    pub fn new(
        startup: talon_core::WorkerConfig,
        env: talon_core::WorkerConfigPatch,
        cli: talon_core::WorkerConfigPatch,
    ) -> Self {
        let effective = serde_json::to_value(&startup).expect("worker config serialization");
        Self {
            status: ConfigReloadStatus {
                effective_config: Some(effective),
                ..Default::default()
            },
            startup: Some(startup),
            env,
            cli,
        }
    }

    pub fn status(&self) -> ConfigReloadStatus {
        self.status.clone()
    }

    pub fn record_error(&mut self, error: String) {
        let error: String = error.chars().take(256).collect();
        if self.status.last_error.as_ref() != Some(&error) {
            tracing::warn!(%error, "worker runtime configuration rejected; retaining last good settings");
        }
        self.status.enabled = true;
        self.status.last_error = Some(error);
    }

    pub fn apply_runtime(&mut self, text: &str, runtime: &WorkerRuntime) -> bool {
        let result = (|| -> Result<(), String> {
            let (next, views) = if let Some(startup) = &self.startup {
                let document = talon_core::dynamic_config::WorkerConfigDocument::parse(text)?;
                let plan = talon_core::dynamic_config::WorkerConfigPlan::resolve(
                    startup,
                    document,
                    self.env.clone(),
                    self.cli.clone(),
                )?;
                (
                    plan.tuning,
                    Some((
                        plan.effective_config,
                        plan.desired_config,
                        plan.pending_restart_fields,
                    )),
                )
            } else {
                (WorkerDynamicConfig::parse(text)?, None)
            };
            runtime.validate_dynamic_tti(&next)?;
            if self.status.config.as_ref() != Some(&next) {
                let limiter = runtime.rate_limiter();
                let policy = next.effective_rate_limits(limiter.startup_policy());
                if self.status.config.as_ref().map_or(true, |current| {
                    current.effective_rate_limits(limiter.startup_policy()) != policy
                }) {
                    limiter.update(policy).map_err(|e| e.to_string())?;
                }
                runtime.update_dynamic_settings(next.clone());
                self.status.config = Some(next);
            }
            if let Some((effective, desired, pending)) = views {
                self.status.effective_config = Some(effective);
                self.status.desired_config = Some(desired);
                self.status.restart_required = !pending.is_empty();
                self.status.pending_restart_fields = pending;
            }
            self.status.enabled = true;
            let hash = format!("{:x}", Sha256::digest(text.as_bytes()));
            self.status.observed_hash = Some(hash.clone());
            self.status.applied_hash = (!self.status.restart_required).then_some(hash);
            self.status.last_error = None;
            Ok(())
        })();
        match result {
            Ok(()) => true,
            Err(error) => {
                self.record_error(error);
                false
            }
        }
    }
}

/// Reopen the projected path on every check; Kubernetes replaces its symlink target.
pub async fn read_snapshot(path: &Path) -> std::io::Result<String> {
    use tokio::io::AsyncReadExt;
    let file = tokio::fs::File::open(path).await?;
    let mut text = String::new();
    file.take(65537).read_to_string(&mut text).await?;
    Ok(text)
}

pub async fn reload_once(
    path: &Path,
    state: &mut DynamicConfig,
    runtime: &WorkerRuntime,
    observability: &WorkerObservability,
) {
    match tokio::time::timeout(READ_TIMEOUT, read_snapshot(path)).await {
        Ok(Ok(text)) => {
            state.apply_runtime(&text, runtime);
        }
        Ok(Err(error)) => state.record_error(error.to_string()),
        Err(_) => state.record_error("configuration read timed out".into()),
    }
    observability.set_config_reload_status(state.status());
}

pub fn spawn_reload(
    path: PathBuf,
    mut state: DynamicConfig,
    runtime: Arc<WorkerRuntime>,
    observability: Arc<WorkerObservability>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RELOAD_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = observability.shutdown().stopped() => return,
                _ = ticker.tick() => reload_once(&path, &mut state, &runtime, &observability).await,
            }
        }
    })
}
