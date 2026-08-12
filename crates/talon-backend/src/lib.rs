//! # talon-backend
//!
//! [`BackendStore`](talon_core::BackendStore) implementations for the blob
//! stores Talon loads from on a cache miss: S3 (and S3-compatible), with GCS and
//! Azure to follow. Each backend is generic over an [`http::HttpClient`] so
//! request construction and response parsing are unit-testable offline; a real
//! networked client is injected in production.
//!
//! The crate also hosts Talon's deployment-platform introspection — workload
//! identity ([`credentials`]) and zone discovery ([`resolve_zone`]) — which
//! shares the same env/HTTP seams and keeps heavier HTTP dependencies out of
//! `talon-core`.

pub mod azure;
pub mod azure_sharedkey;
pub mod credentials;
pub mod delay;
pub mod gcs;
pub mod http;
mod query_auth;
pub mod reqwest_client;
pub mod retry;
pub mod s3;
pub mod sigv4;
pub mod workload_identity;
pub mod xml;
pub mod zone;

pub use azure::{AzureBackend, AzureConfig};
pub use credentials::{
    CredentialsFetch, CredentialsObserver, NoopObserver, ProvideBearerToken, ProvideS3Credentials,
    RefreshPolicy, RefreshingCredentials, StaticBearerToken, StaticS3Credentials,
};
pub use delay::{DelayConfig, DelayingHttpClient};
pub use gcs::{GcsBackend, GcsConfig};
pub use http::{
    HttpClient, HttpRequest, HttpRequestBody, HttpResponse, HttpStreamResponse, Method,
};
pub use query_auth::{AzureSas, S3PresignedQuery};
pub use reqwest_client::ReqwestClient;
pub use retry::{RetryConfig, RetryObserver, RetryingHttpClient};
pub use s3::{S3Backend, S3Config, S3Credentials, S3MultipartRequest};
pub use sigv4::{sign_request, AmzDate};
pub use workload_identity::{
    resolve_azure_bearer, resolve_gcs_bearer, resolve_s3_credentials, ResolvedBearerToken,
    ResolvedS3Credentials,
};
pub use zone::{resolve_zone, ResolvedZone};
