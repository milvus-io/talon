//! Strict client cache-routing mark contract.

use std::fmt;

use thiserror::Error;

/// S3 cache mark. SigV4 authentication must require this header in
/// `SignedHeaders` before applying a non-default decision.
pub const S3_CACHE_MARK_HEADER: &str = "x-talon-cache-mark";

/// Azure cache mark. The `x-ms-` prefix makes the header part of Shared Key's
/// canonicalized headers; arbitrary extension headers are not signed by Azure.
pub const AZURE_CACHE_MARK_HEADER: &str = "x-ms-talon-cache-mark";

/// Whether a foreground request may read already resident cache data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheLookup {
    /// Consult Talon before reading the origin.
    On,
    /// Skip Talon lookup.
    Off,
}

/// Whether origin bytes may be admitted to Talon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePopulation {
    /// Populate or refresh cache data.
    On,
    /// Do not populate cache data.
    Off,
}

/// Behavior after an eligible cache-path failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheFallback {
    /// Fall back to an independently authenticated origin request.
    Origin,
    /// Fail without accessing the origin.
    Fail,
}

/// Validated provider-neutral cache routing decision.
///
/// Fields are private so callers cannot construct contradictory combinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveDecision {
    lookup: CacheLookup,
    population: CachePopulation,
    fallback: CacheFallback,
}

impl EffectiveDecision {
    /// Existing read-through behavior used when no cache mark is present.
    pub const DEFAULT: Self =
        Self::new(CacheLookup::On, CachePopulation::On, CacheFallback::Origin);

    /// Consult resident cache data, but do not fill a miss.
    pub const LOOKUP_NO_FILL: Self =
        Self::new(CacheLookup::On, CachePopulation::Off, CacheFallback::Origin);

    /// Bypass Talon entirely.
    pub const ORIGIN_ONLY: Self = Self::new(
        CacheLookup::Off,
        CachePopulation::Off,
        CacheFallback::Origin,
    );

    /// Skip lookup and refresh Talon from the origin.
    pub const REFRESH: Self =
        Self::new(CacheLookup::Off, CachePopulation::On, CacheFallback::Origin);

    /// Read only already resident cache data.
    pub const CACHE_ONLY: Self =
        Self::new(CacheLookup::On, CachePopulation::Off, CacheFallback::Fail);

    const fn new(
        lookup: CacheLookup,
        population: CachePopulation,
        fallback: CacheFallback,
    ) -> Self {
        Self {
            lookup,
            population,
            fallback,
        }
    }

    /// Parse a present cache-mark header.
    ///
    /// Absence is represented by [`Default`], rather than an empty string.
    pub fn parse(value: &str) -> Result<Self, CacheMarkError> {
        let mut version = None;
        let mut lookup = None;
        let mut population = None;
        let mut fallback = None;

        if value.is_empty() {
            return Err(CacheMarkError::Empty);
        }
        for field in value.split(';') {
            let field = field.trim();
            if field.is_empty() {
                return Err(CacheMarkError::MalformedField);
            }
            let (name, value) = field
                .split_once('=')
                .ok_or(CacheMarkError::MalformedField)?;
            let name = name.trim();
            let value = value.trim();
            if name.is_empty() || value.is_empty() || value.contains('=') {
                return Err(CacheMarkError::MalformedField);
            }
            match name {
                "v" => set_once(&mut version, parse_version(value)?)?,
                "lookup" => set_once(&mut lookup, parse_lookup(value)?)?,
                "populate" => set_once(&mut population, parse_population(value)?)?,
                "fallback" => set_once(&mut fallback, parse_fallback(value)?)?,
                _ => return Err(CacheMarkError::UnknownField),
            }
        }

        let version = version.ok_or(CacheMarkError::MissingField)?;
        if version != 1 {
            return Err(CacheMarkError::UnsupportedVersion);
        }
        let decision = Self::new(
            lookup.ok_or(CacheMarkError::MissingField)?,
            population.ok_or(CacheMarkError::MissingField)?,
            fallback.ok_or(CacheMarkError::MissingField)?,
        );
        match decision {
            Self::DEFAULT
            | Self::LOOKUP_NO_FILL
            | Self::ORIGIN_ONLY
            | Self::REFRESH
            | Self::CACHE_ONLY => Ok(decision),
            _ => Err(CacheMarkError::ContradictoryDecision),
        }
    }

    /// Whether resident cache data may be consulted.
    pub const fn lookup(self) -> CacheLookup {
        self.lookup
    }

    /// Whether origin bytes may be admitted to the cache.
    pub const fn population(self) -> CachePopulation {
        self.population
    }

    /// Behavior after an eligible cache infrastructure failure.
    pub const fn fallback(self) -> CacheFallback {
        self.fallback
    }
}

impl Default for EffectiveDecision {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for EffectiveDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lookup = match self.lookup {
            CacheLookup::On => "on",
            CacheLookup::Off => "off",
        };
        let population = match self.population {
            CachePopulation::On => "on",
            CachePopulation::Off => "off",
        };
        let fallback = match self.fallback {
            CacheFallback::Origin => "origin",
            CacheFallback::Fail => "fail",
        };
        write!(
            formatter,
            "v=1; lookup={lookup}; populate={population}; fallback={fallback}"
        )
    }
}

/// Stable cache-mark validation failure that never contains client input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CacheMarkError {
    /// A present header had no value.
    #[error("cache mark is empty")]
    Empty,
    /// A field was not one `name=value` pair.
    #[error("cache mark contains a malformed field")]
    MalformedField,
    /// A required field was absent.
    #[error("cache mark is missing a required field")]
    MissingField,
    /// A field appeared more than once.
    #[error("cache mark contains a duplicate field")]
    DuplicateField,
    /// An extension field is not understood.
    #[error("cache mark contains an unknown field")]
    UnknownField,
    /// The version syntax was invalid.
    #[error("cache mark version is invalid")]
    InvalidVersion,
    /// The version is well formed but unsupported.
    #[error("cache mark version is unsupported")]
    UnsupportedVersion,
    /// A field value was outside its closed vocabulary.
    #[error("cache mark contains an invalid value")]
    InvalidValue,
    /// The fields are individually valid but have no safe routing meaning.
    #[error("cache mark fields form a contradictory decision")]
    ContradictoryDecision,
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<(), CacheMarkError> {
    if slot.replace(value).is_some() {
        Err(CacheMarkError::DuplicateField)
    } else {
        Ok(())
    }
}

fn parse_version(value: &str) -> Result<u32, CacheMarkError> {
    value.parse().map_err(|_| CacheMarkError::InvalidVersion)
}

fn parse_lookup(value: &str) -> Result<CacheLookup, CacheMarkError> {
    match value {
        "on" => Ok(CacheLookup::On),
        "off" => Ok(CacheLookup::Off),
        _ => Err(CacheMarkError::InvalidValue),
    }
}

fn parse_population(value: &str) -> Result<CachePopulation, CacheMarkError> {
    match value {
        "on" => Ok(CachePopulation::On),
        "off" => Ok(CachePopulation::Off),
        _ => Err(CacheMarkError::InvalidValue),
    }
}

fn parse_fallback(value: &str) -> Result<CacheFallback, CacheMarkError> {
    match value {
        "origin" => Ok(CacheFallback::Origin),
        "fail" => Ok(CacheFallback::Fail),
        _ => Err(CacheMarkError::InvalidValue),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_canonical_decision_in_any_field_order() {
        let cases = [
            (
                EffectiveDecision::DEFAULT,
                "v=1; lookup=on; populate=on; fallback=origin",
            ),
            (
                EffectiveDecision::LOOKUP_NO_FILL,
                "fallback=origin; populate=off; lookup=on; v=1",
            ),
            (
                EffectiveDecision::ORIGIN_ONLY,
                "v=1;lookup=off;populate=off;fallback=origin",
            ),
            (
                EffectiveDecision::REFRESH,
                " v = 1 ; lookup = off ; populate = on ; fallback = origin ",
            ),
            (
                EffectiveDecision::CACHE_ONLY,
                "v=1; lookup=on; populate=off; fallback=fail",
            ),
        ];

        for (expected, wire) in cases {
            assert_eq!(EffectiveDecision::parse(wire), Ok(expected), "{wire}");
            assert_eq!(
                EffectiveDecision::parse(&expected.to_string()),
                Ok(expected)
            );
        }
    }

    #[test]
    fn absence_uses_the_read_through_default() {
        let decision = EffectiveDecision::default();
        assert_eq!(decision, EffectiveDecision::DEFAULT);
        assert_eq!(decision.lookup(), CacheLookup::On);
        assert_eq!(decision.population(), CachePopulation::On);
        assert_eq!(decision.fallback(), CacheFallback::Origin);
    }

    #[test]
    fn rejects_malformed_incomplete_and_duplicate_marks() {
        let cases = [
            ("", CacheMarkError::Empty),
            ("v=1;", CacheMarkError::MalformedField),
            ("v=1; lookup", CacheMarkError::MalformedField),
            (
                "v=1=2; lookup=on; populate=on; fallback=origin",
                CacheMarkError::MalformedField,
            ),
            ("v=1; lookup=on; populate=on", CacheMarkError::MissingField),
            (
                "v=1; v=1; lookup=on; populate=on; fallback=origin",
                CacheMarkError::DuplicateField,
            ),
            (
                "v=1; lookup=on; populate=on; fallback=origin; future=on",
                CacheMarkError::UnknownField,
            ),
        ];

        for (wire, expected) in cases {
            assert_eq!(EffectiveDecision::parse(wire), Err(expected), "{wire}");
        }
    }

    #[test]
    fn rejects_unknown_versions_values_and_case_variants() {
        let cases = [
            (
                "v=two; lookup=on; populate=on; fallback=origin",
                CacheMarkError::InvalidVersion,
            ),
            (
                "v=2; lookup=on; populate=on; fallback=origin",
                CacheMarkError::UnsupportedVersion,
            ),
            (
                "v=1; lookup=yes; populate=on; fallback=origin",
                CacheMarkError::InvalidValue,
            ),
            (
                "v=1; lookup=on; populate=ON; fallback=origin",
                CacheMarkError::InvalidValue,
            ),
            (
                "V=1; lookup=on; populate=on; fallback=origin",
                CacheMarkError::UnknownField,
            ),
        ];

        for (wire, expected) in cases {
            assert_eq!(EffectiveDecision::parse(wire), Err(expected), "{wire}");
        }
    }

    #[test]
    fn fail_fallback_is_only_valid_for_cache_only_reads() {
        for wire in [
            "v=1; lookup=on; populate=on; fallback=fail",
            "v=1; lookup=off; populate=on; fallback=fail",
            "v=1; lookup=off; populate=off; fallback=fail",
        ] {
            assert_eq!(
                EffectiveDecision::parse(wire),
                Err(CacheMarkError::ContradictoryDecision),
                "{wire}"
            );
        }
    }

    #[test]
    fn validation_errors_never_echo_client_input() {
        let secret = "secret-client-value";
        let error = EffectiveDecision::parse(&format!(
            "v=1; lookup=on; populate=on; fallback=origin; {secret}=on"
        ))
        .unwrap_err();
        assert!(!error.to_string().contains(secret));
    }
}
