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
pub mod broker;
pub mod credentials;
pub mod delay;
pub mod endpoint;
pub mod gcs;
pub mod http;
#[cfg(feature = "uring")]
pub mod monoio_client;
pub mod origin;
pub mod reqwest_client;
pub mod retry;
pub mod routing;
pub mod s3;
pub mod sigv4;
pub mod workload_identity;
pub mod xml;
pub mod zone;

// Timers wake the runtime driving the request. Tokio remains the portable
// implementation when native origin I/O is not compiled in.
mod time {
    #[cfg(feature = "uring")]
    pub use talon_uring::{sleep, timeout};
    #[cfg(not(feature = "uring"))]
    pub use tokio::time::{sleep, timeout};
}

pub use azure::{AzureBackend, AzureConfig, AzureCredentials, AzureSas};
pub use broker::{resolve_azure_broker_sas, BrokerConfig};
pub use credentials::{
    CredentialsFetch, CredentialsObserver, NoopObserver, ProvideBearerToken, ProvideS3Credentials,
    ProvideSasToken, RefreshPolicy, RefreshingCredentials, StaticBearerToken, StaticS3Credentials,
};
pub use delay::{DelayConfig, DelayingHttpClient};
pub use endpoint::endpoint_host;
pub use gcs::{GcsBackend, GcsConfig, GcsCredentials, GcsHmacCredentials};
pub use http::{
    HttpClient, HttpRequest, HttpRequestBody, HttpResponse, HttpStreamResponse, Method,
};
pub use reqwest_client::ReqwestClient;
pub use retry::{RetryConfig, RetryObserver, RetryingHttpClient};
pub use routing::{
    BackendResolver, BackendRouter, DefaultBackendResolver, DynamicBackendDefaults,
    LegacyBackendFactory, RouteRequest,
};
pub use s3::{S3Backend, S3Config, S3Credentials, S3MultipartRequest, S3PresignedQuery};
pub use sigv4::{sign_request, AmzDate};
pub use workload_identity::{
    resolve_azure_credentials, resolve_gcs_credentials, resolve_s3_credentials, CredentialMode,
    CredentialOptions, ResolvedAzureCredentials, ResolvedGcsCredentials, ResolvedS3Credentials,
};
pub use zone::{resolve_zone, ResolvedZone};
