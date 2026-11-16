//! Resolve one reload into live tuning and restart-aware status views.
use super::{NamespaceDynamicConfig, WorkerConfigDocument, WorkerDynamicConfig};
use crate::{NamespaceId, WorkerConfig, WorkerConfigPatch};
use serde_json::Value;
use std::collections::BTreeSet;

/// A validated configuration proposal; runtime storage validation precedes publication.
pub struct WorkerConfigPlan {
    pub tuning: WorkerDynamicConfig,
    pub effective_config: Value,
    pub desired_config: Value,
    pub pending_restart_fields: Vec<String>,
}

impl WorkerConfigPlan {
    /// Preserve startup storage settings while applying only live fields from the resolved document.
    pub fn resolve(
        startup: &WorkerConfig,
        document: WorkerConfigDocument,
        env: WorkerConfigPatch,
        cli: WorkerConfigPatch,
    ) -> Result<Self, String> {
        let requested_names = document
            .tuning
            .namespaces
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut desired = WorkerConfig::resolve(document.patch, env, cli)
            .map_err(|_| "worker configuration validation failed".to_string())?;
        let mut effective = startup.clone();
        WorkerDynamicConfig::apply_resolved(&mut effective, &desired);
        let mut tuning = WorkerDynamicConfig::from_config(&desired);
        let mut names = requested_names;
        names.extend(startup.namespaces.keys().cloned());
        names.extend(desired.namespaces.keys().cloned());
        for name in names {
            let namespace = NamespaceId::named(&name);
            let resolved = desired
                .for_namespace(&namespace)
                .map_err(|e| e.to_string())?;
            let limits = desired.rate_limits.limits_for(&namespace);
            tuning.namespaces.insert(
                name.clone(),
                NamespaceDynamicConfig {
                    page_tti_hours: Some(resolved.page_tti_hours),
                    rate_limits: None,
                },
            );
            let effective_ns = effective.namespaces.entry(name.clone()).or_default();
            effective_ns.page_tti_hours = Some(resolved.page_tti_hours);
            effective_ns.rate_limits = Some(limits);
            let desired_ns = desired.namespaces.entry(name).or_default();
            desired_ns.page_tti_hours = Some(resolved.page_tti_hours);
            desired_ns.rate_limits = Some(limits);
        }
        tuning.validate()?;
        let effective_config = serde_json::to_value(effective).map_err(|e| e.to_string())?;
        let desired_config = serde_json::to_value(desired).map_err(|e| e.to_string())?;
        let pending_restart_fields = desired_config
            .as_object()
            .expect("WorkerConfig is an object")
            .iter()
            .filter(|(key, value)| effective_config.get(*key) != Some(*value))
            .map(|(key, _)| key.clone())
            .collect();
        Ok(Self {
            tuning,
            effective_config,
            desired_config,
            pending_restart_fields,
        })
    }
}

#[cfg(test)]
mod tests;
