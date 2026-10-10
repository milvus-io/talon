//! Native async client for reading objects through a Talon cache cluster.
//!
//! # Typed errors
//!
//! The SDK re-exports the error types carried by [`Error`], along with
//! [`CacheReadError`] and the worker's [`DataErrorCode`]. These are the original
//! types, including their existing variants, conversions and error sources.
//! Treat them as public API; diagnostic text is for people, not classification.
//!
//! Save the outer diagnostic before consuming an error to classify its cause.
//! In particular, an exhausted replica read includes the last worker address
//! that would be lost by keeping only `source.to_string()`.
//! This example needs only `talon-rust-client` as a direct Talon dependency:
//!
//! ```
//! use talon_rust_client::{
//!     BlockReadError, CacheReadError, DataErrorCode, DataPlaneError, Error, WorkerError,
//! };
//!
//! // Application-specific classification; retry and fallback policy belong
//! // to the caller. Neither action is performed by this example.
//! fn classify(error: Error) -> (CacheReadError, String) {
//!     let diagnostic = error.to_string();
//!     let class = match error {
//!         Error::InvalidUri(_) | Error::InvalidArgument(_) =>
//!             CacheReadError::InvalidRequest(diagnostic.clone()),
//!         Error::Coordinator(source)
//!         | Error::Block(BlockReadError::Coordinator(source)) => source.into(),
//!         Error::Block(BlockReadError::Worker(source))
//!         | Error::Block(BlockReadError::Target { source, .. })
//!         | Error::Block(BlockReadError::AllReplicasFailed { source, .. }) => source.into(),
//!         Error::Block(BlockReadError::NoOwners) =>
//!             CacheReadError::Unavailable(diagnostic.clone()),
//!     };
//!     (class, diagnostic)
//! }
//!
//! let error = Error::from(BlockReadError::AllReplicasFailed {
//!     worker: "worker-1:9000".into(),
//!     worker_id: "worker-1".into(),
//!     instance_id: Some("instance-1".into()),
//!     source: WorkerError::Remote(DataPlaneError {
//!         code: DataErrorCode::Timeout,
//!         message: "request timed out".into(),
//!     }),
//! });
//! let (class, diagnostic) = classify(error);
//! assert!(matches!(class, CacheReadError::Timeout(_)));
//! assert!(diagnostic.contains("worker-1:9000"));
//! assert!(diagnostic.contains("request timed out"));
//! ```

mod client;
mod error;

pub use client::{
    parse_uri, Client, ClientBuilder, LoadRequest, DEFAULT_MAX_IN_FLIGHT_BLOCK_READS,
};
pub use error::{Error, ErrorKind, UriError};
pub use talon_cache_client::{
    BlockReadError, CacheReadError, CoordinatorError, LoadError, LoadFailure, LoadResult,
    ObjectStat, WorkerError, WorkerLoadError,
};
pub use talon_core::{ObjectId, Version, UNCHECKED_VERSION};
pub use talon_transport::{DataErrorCode, DataPlaneError, ObjectEntry};

pub use talon_telemetry::{RequestOptions, TraceContext, TraceParent};
