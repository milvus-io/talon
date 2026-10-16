//! Shared deployment configuration used by the Rust, C, and Python SDKs.
use crate::Error;

/// Per-client AZ routing. Defaults are read from the environment at build time.
/// SDKs do not query Kubernetes; inject the actual pod zone as `TALON_ZONE`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ZoneOptions {
    /// Reader zone, using the same spelling as Worker membership.
    pub zone: Option<String>,
    /// Rank same-zone members when present. Defaults to false.
    pub affinity: bool,
    /// Try nonlocal read candidates after retryable local failures. Defaults off.
    /// LOAD and BatchLoad never use these fallback candidates.
    pub fallback: bool,
}

impl ZoneOptions {
    /// Read `TALON_ZONE`, `TALON_ZONE_AFFINITY`, and `TALON_ZONE_FALLBACK`.
    /// Invalid booleans or zone lengths fail construction rather than silently
    /// disabling the requested routing policy.
    pub fn from_env() -> Result<Self, Error> {
        Self::resolve(|key| std::env::var(key).ok())
    }

    fn resolve(get: impl Fn(&str) -> Option<String>) -> Result<Self, Error> {
        let boolean = |key| match get(key).filter(|v| !v.trim().is_empty()) {
            None => Ok(false),
            Some(value) => talon_core::parse_bool_value(&value)
                .ok_or_else(|| Error::InvalidArgument(format!("{key} must be a boolean"))),
        };
        let value = Self {
            zone: get("TALON_ZONE").filter(|v| !v.trim().is_empty()),
            affinity: boolean("TALON_ZONE_AFFINITY")?,
            fallback: boolean("TALON_ZONE_FALLBACK")?,
        };
        value.validate()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_environment_defaults_and_validation() {
        assert_eq!(
            ZoneOptions::resolve(|_| None).unwrap(),
            ZoneOptions::default()
        );
        let configured = ZoneOptions::resolve(|key| {
            Some(
                match key {
                    "TALON_ZONE" => "zone-a",
                    _ => "true",
                }
                .into(),
            )
        })
        .unwrap();
        assert_eq!(configured.zone.as_deref(), Some("zone-a"));
        assert!(configured.affinity && configured.fallback);
        for key in ["TALON_ZONE_AFFINITY", "TALON_ZONE_FALLBACK"] {
            assert!(ZoneOptions::resolve(|k| (k == key).then(|| "typo".into())).is_err());
        }
        assert!(ZoneOptions::resolve(|k| (k == "TALON_ZONE").then(|| "z".repeat(257))).is_err());
    }
}
