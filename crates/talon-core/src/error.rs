//! Error types shared across Talon crates.

use thiserror::Error;

/// Convenience alias used throughout Talon.
pub type Result<T> = std::result::Result<T, Error>;

/// The top-level error type for Talon operations.
#[derive(Debug, Error)]
pub enum Error {
    /// The requested object was not found in the cache.
    #[error("object not found: {0}")]
    NotFound(String),

    /// A worker or coordinator node was unreachable.
    #[error("node unavailable: {0}")]
    NodeUnavailable(String),

    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Any other error.
    #[error("{0}")]
    Other(String),
}
