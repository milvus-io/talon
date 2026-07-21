//! Cache key definitions.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A key identifying an object in the cache.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey(pub String);

impl CacheKey {
    /// Create a new cache key.
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Return the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for CacheKey {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
