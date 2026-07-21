//! # talon-backend
//!
//! [`BackendStore`](talon_core::BackendStore) implementations for the blob
//! stores Talon loads from on a cache miss: S3 (and S3-compatible), with GCS and
//! Azure to follow. Each backend is generic over an [`http::HttpClient`] so
//! request construction and response parsing are unit-testable offline; a real
//! networked client is injected in production.

pub mod gcs;
pub mod http;
pub mod s3;

pub use gcs::{GcsBackend, GcsConfig};
pub use http::{HttpClient, HttpRequest, HttpResponse, Method};
pub use s3::{S3Backend, S3Config, S3Credentials};
