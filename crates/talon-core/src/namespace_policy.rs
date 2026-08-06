//! Static namespace authorization policy for privileged control-plane traffic.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Deserializer};

use crate::{Backend, Error, Result};

const POLICY_VERSION: u16 = 1;

/// A canonical object-store namespace: `backend/bucket[/path-prefix]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectNamespace {
    backend: Backend,
    bucket: String,
    prefix: Option<String>,
}

impl ObjectNamespace {
    /// Return the object-store backend.
    pub fn backend(&self) -> Backend {
        self.backend
    }

    /// Return the bucket or container name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Return the optional object-key prefix.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Whether this namespace grant contains `target`.
    pub fn contains(&self, target: &Self) -> bool {
        if self.backend != target.backend || self.bucket != target.bucket {
            return false;
        }
        match (&self.prefix, &target.prefix) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(grant), Some(target)) => {
                target == grant
                    || target
                        .strip_prefix(grant)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            }
        }
    }
}

impl FromStr for ObjectNamespace {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.starts_with('/') || value.ends_with('/') {
            return Err(Error::Other(format!(
                "namespace must not start or end with '/': {value:?}"
            )));
        }
        let mut parts = value.split('/');
        let backend_name = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or_else(|| Error::Other(format!("namespace is missing a backend: {value:?}")))?;
        let backend = backend_name.parse::<Backend>()?;
        if backend.prefix() != backend_name {
            return Err(Error::Other(format!(
                "namespace backend is not canonical: {backend_name:?}"
            )));
        }
        let bucket = parts
            .next()
            .filter(|part| valid_component(part))
            .ok_or_else(|| {
                Error::Other(format!(
                    "namespace is missing or has an invalid bucket/container: {value:?}"
                ))
            })?;
        let prefix_parts = parts
            .map(|part| {
                valid_component(part).then_some(part).ok_or_else(|| {
                    Error::Other(format!(
                        "namespace has an invalid path component: {value:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let prefix = (!prefix_parts.is_empty()).then(|| prefix_parts.join("/"));
        Ok(Self {
            backend,
            bucket: bucket.to_string(),
            prefix,
        })
    }
}

impl fmt::Display for ObjectNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.backend, self.bucket)?;
        if let Some(prefix) = &self.prefix {
            write!(f, "/{prefix}")?;
        }
        Ok(())
    }
}

fn valid_component(component: &str) -> bool {
    !component.is_empty() && component != "." && component != ".." && !component.contains('\0')
}

impl<'de> Deserialize<'de> for ObjectNamespace {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Operator-owned namespace grants indexed by stable worker node ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePolicy {
    workers: HashMap<String, WorkerPolicy>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    version: u16,
    #[serde(default)]
    workers: Vec<WorkerGrants>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerGrants {
    node_id: String,
    #[serde(default)]
    control_address: Option<String>,
    #[serde(default)]
    grants: Vec<ObjectNamespace>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerPolicy {
    control_address: Option<String>,
    grants: Vec<ObjectNamespace>,
}

impl NamespacePolicy {
    /// Parse and validate a versioned TOML policy document.
    pub fn from_toml(value: &str) -> Result<Self> {
        let raw: PolicyFile = toml::from_str(value)
            .map_err(|error| Error::Other(format!("invalid namespace policy: {error}")))?;
        if raw.version != POLICY_VERSION {
            return Err(Error::Other(format!(
                "unsupported namespace policy version {}; expected {POLICY_VERSION}",
                raw.version
            )));
        }
        let mut workers = HashMap::with_capacity(raw.workers.len());
        for worker in raw.workers {
            if worker.node_id.is_empty() {
                return Err(Error::Other(
                    "namespace policy worker node_id must not be empty".into(),
                ));
            }
            if worker
                .control_address
                .as_ref()
                .is_some_and(String::is_empty)
            {
                return Err(Error::Other(format!(
                    "namespace policy worker {:?} has an empty control_address",
                    worker.node_id
                )));
            }
            let policy = WorkerPolicy {
                control_address: worker.control_address,
                grants: worker.grants,
            };
            if workers.insert(worker.node_id.clone(), policy).is_some() {
                return Err(Error::Other(format!(
                    "namespace policy contains duplicate worker node_id {:?}",
                    worker.node_id
                )));
            }
        }
        Ok(Self { workers })
    }

    /// Load and validate a TOML policy file. A missing configured file is an error.
    pub fn from_file(path: &Path) -> Result<Self> {
        let value = std::fs::read_to_string(path).map_err(|error| {
            Error::Other(format!(
                "failed to read namespace policy {}: {error}",
                path.display()
            ))
        })?;
        Self::from_toml(&value)
    }

    /// Whether the operator policy authorizes `worker_id` for `target`.
    pub fn authorizes(&self, worker_id: &str, target: &ObjectNamespace) -> bool {
        self.workers
            .get(worker_id)
            .is_some_and(|worker| worker.grants.iter().any(|grant| grant.contains(target)))
    }

    /// Configured privileged control address for a worker, if any.
    pub fn control_address(&self, worker_id: &str) -> Option<&str> {
        self.workers
            .get(worker_id)
            .and_then(|worker| worker.control_address.as_deref())
    }

    /// Canonical namespaces configured for a worker.
    pub fn grants(&self, worker_id: &str) -> &[ObjectNamespace] {
        self.workers
            .get(worker_id)
            .map_or(&[], |worker| worker.grants.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_namespace_rejects_ambiguous_paths() {
        for invalid in [
            "",
            "/s3/bucket",
            "s3",
            "s3/",
            "s3/bucket/",
            "s3//key",
            "s3/bucket/a//b",
            "s3/bucket/./a",
            "s3/bucket/a/../b",
            "unknown/bucket",
        ] {
            assert!(invalid.parse::<ObjectNamespace>().is_err(), "{invalid}");
        }
        assert!("azure/container/models".parse::<ObjectNamespace>().is_err());
        assert_eq!(
            "az/container/models"
                .parse::<ObjectNamespace>()
                .unwrap()
                .to_string(),
            "az/container/models"
        );
    }

    #[test]
    fn grants_match_only_on_path_component_boundaries() {
        let grant = "s3/data/models".parse::<ObjectNamespace>().unwrap();
        assert!(grant.contains(&"s3/data/models".parse().unwrap()));
        assert!(grant.contains(&"s3/data/models/v1".parse().unwrap()));
        assert!(!grant.contains(&"s3/data/modelsmith".parse().unwrap()));
        assert!(!grant.contains(&"s3/data".parse().unwrap()));
        assert!(!grant.contains(&"gcs/data/models".parse().unwrap()));
    }

    #[test]
    fn policy_is_worker_scoped_and_fail_closed() {
        let policy = NamespacePolicy::from_toml(
            "version = 1\n\
             [[workers]]\n\
             node_id = \"worker-a\"\n\
             control_address = \"worker-a:7002\"\n\
             grants = [\"s3/data/models\", \"gcs/checkpoints\"]\n",
        )
        .unwrap();
        assert!(policy.authorizes("worker-a", &"s3/data/models/v1".parse().unwrap()));
        assert!(!policy.authorizes("worker-a", &"s3/data/private".parse().unwrap()));
        assert!(!policy.authorizes("worker-b", &"s3/data/models".parse().unwrap()));
        assert_eq!(policy.control_address("worker-a"), Some("worker-a:7002"));
        assert_eq!(policy.grants("worker-a").len(), 2);
    }

    #[test]
    fn policy_rejects_unknown_versions_and_duplicate_workers() {
        assert!(NamespacePolicy::from_toml("version = 2").is_err());
        assert!(NamespacePolicy::from_toml(
            "version = 1\n\
             [[workers]]\nnode_id = \"same\"\n\
             [[workers]]\nnode_id = \"same\"\n"
        )
        .is_err());
    }

    #[test]
    fn policy_rejects_an_empty_control_address() {
        assert!(NamespacePolicy::from_toml(
            "version = 1\n\
             [[workers]]\n\
             node_id = \"worker-a\"\n\
             control_address = \"\"\n"
        )
        .is_err());
    }

    #[test]
    fn configured_policy_file_is_loaded_and_missing_files_fail() {
        let path = std::env::temp_dir().join(format!(
            "talon-namespace-policy-{}-{}.toml",
            std::process::id(),
            line!()
        ));
        std::fs::write(
            &path,
            "version = 1\n[[workers]]\nnode_id = \"worker-a\"\ngrants = [\"s3/data\"]\n",
        )
        .unwrap();
        let policy = NamespacePolicy::from_file(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert!(policy.authorizes("worker-a", &"s3/data/models".parse().unwrap()));
        assert!(NamespacePolicy::from_file(&path).is_err());
    }
}
