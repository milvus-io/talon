//! Canonical cloud origins. Boundary inputs are consumed once; prepared origins
//! retain only effective addressing and a typed credential source.
use std::sync::Arc;

use talon_core::{Backend, BackendStore, OriginOptions};

use crate::broker::{self, NormalizedBrokerConfig};
use crate::workload_identity::{
    self as identity, ResolutionContext, SelectedAzure, SelectedGcs, SelectedS3,
};
use crate::{
    AzureBackend, AzureConfig, AzureCredentials, CredentialOptions, CredentialsObserver,
    GcsBackend, GcsConfig, GcsHmacCredentials, HttpClient, S3Backend, S3Config, S3Credentials,
};

/// Legacy process settings retain permissive endpoint parsing. Explicit routes
/// also validate URL syntax and cloud name components before acquisition.
#[derive(Clone, Copy)]
pub enum OriginValidation {
    Legacy,
    Routed,
}

pub struct S3OriginInput {
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
    pub use_ssl: Option<bool>,
    pub validation: OriginValidation,
    pub keys: Option<S3Credentials>,
    pub credentials: CredentialOptions,
}

pub enum GcsCredentialInput {
    Resolve {
        keys: Option<GcsHmacCredentials>,
        options: Box<CredentialOptions>,
    },
    /// Legacy fixed bearer or anonymous emulator access.
    Fixed(Option<String>),
}
pub struct GcsOriginInput {
    pub endpoint: Option<String>,
    pub use_ssl: Option<bool>,
    pub validation: OriginValidation,
    pub credentials: GcsCredentialInput,
}

pub enum AzureCredentialInput {
    Resolve {
        shared_key: Option<String>,
        options: Box<CredentialOptions>,
    },
    FixedSas(String),
}
pub struct AzureOriginInput {
    pub account: String,
    pub endpoint: Option<String>,
    pub endpoint_suffix: Option<String>,
    pub path_style: Option<bool>,
    pub use_ssl: Option<bool>,
    pub validation: OriginValidation,
    pub credentials: AzureCredentialInput,
}

pub struct S3Origin {
    pub(crate) config: S3Config,
    credentials: SelectedS3,
}
pub struct GcsOrigin {
    pub(crate) config: GcsConfig,
    credentials: GcsAuth,
}
pub struct AzureOrigin {
    pub(crate) config: AzureConfig,
    credentials: AzureAuth,
}
enum GcsAuth {
    Selected(SelectedGcs),
    Fixed(Option<String>),
}
enum AzureAuth {
    Selected(SelectedAzure),
    FixedSas(String),
}

/// Shared compatibility check also used by gateway's injected settings lookup,
/// which intentionally does not require a complete Broker configuration yet.
pub fn validate_s3_broker_endpoint(
    endpoint: Option<&str>,
    request: Option<&serde_json::Value>,
) -> Result<(), String> {
    if endpoint.is_none()
        && request.is_some_and(|request| {
            request
                .get(broker::FIELD_CSP)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|csp| broker::S3_COMPATIBLE_CSPS.contains(&csp))
        })
    {
        return Err("Ali/Tencent/Huawei Broker credentials require an explicit s3_endpoint".into());
    }
    Ok(())
}

fn endpoint(
    value: &str,
    use_ssl: Option<bool>,
    validation: OriginValidation,
) -> Result<(String, bool), String> {
    // Select scheme before URL normalization can erase an explicit default port.
    let selected;
    let value = if let Some(tls) = use_ssl {
        let authority = match value.split_once("://") {
            Some((scheme, authority))
                if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") =>
            {
                authority
            }
            Some(_) => return Err("bucket endpoint must use HTTP(S)".into()),
            None => value,
        };
        selected = format!("{}://{authority}", if tls { "https" } else { "http" });
        &selected
    } else {
        value
    };
    if matches!(validation, OriginValidation::Legacy) {
        return Ok(crate::endpoint_host(value));
    }
    let invalid = || {
        "bucket endpoint must be an HTTP(S) URL or host without userinfo, query, fragment or whitespace".to_owned()
    };
    if value.is_empty()
        || value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control() || b == b'\\')
    {
        return Err(invalid());
    }
    let full = if value.contains("://") {
        value.to_owned()
    } else {
        format!("https://{value}")
    };
    let url = url::Url::parse(&full).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(crate::endpoint_host(url.as_str()))
}

impl S3Origin {
    pub fn prepare(input: S3OriginInput) -> Result<Self, String> {
        if matches!(input.validation, OriginValidation::Routed)
            && (input.region.is_empty()
                || !input
                    .region
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')))
        {
            return Err("S3 region contains invalid characters".into());
        }
        let mut config = S3Config::aws(input.region);
        config.path_style = input.path_style;
        if let Some(value) = &input.endpoint {
            (config.endpoint, config.tls) = endpoint(value, input.use_ssl, input.validation)?;
        }
        if let Some(tls) = input.use_ssl {
            config.tls = tls;
        }
        let credentials = identity::select_s3(input.keys, input.credentials, &identity::env_var)?;
        validate_s3_broker_endpoint(
            input.endpoint.as_deref(),
            credentials.broker().map(NormalizedBrokerConfig::request),
        )?;
        Ok(Self {
            config,
            credentials,
        })
    }
    pub async fn build(
        self,
        http: Arc<dyn HttpClient>,
        exchange: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> Result<(S3Backend, &'static str), String> {
        let resolved = identity::acquire_s3(
            self.credentials,
            ResolutionContext::process(exchange, observer),
        )
        .await?;
        Ok((
            S3Backend::with_credentials_provider(self.config, resolved.provider, http),
            resolved.source,
        ))
    }
}
impl GcsOrigin {
    pub fn prepare(input: GcsOriginInput) -> Result<Self, String> {
        let mut config = match input.endpoint {
            Some(value) => {
                let (endpoint, tls) = endpoint(&value, input.use_ssl, input.validation)?;
                GcsConfig { endpoint, tls }
            }
            None => GcsConfig::default(),
        };
        if let Some(tls) = input.use_ssl {
            config.tls = tls;
        }
        let credentials = match input.credentials {
            GcsCredentialInput::Resolve { keys, options } => {
                GcsAuth::Selected(identity::select_gcs(keys, *options, &identity::env_var)?)
            }
            GcsCredentialInput::Fixed(token) => GcsAuth::Fixed(token),
        };
        Ok(Self {
            config,
            credentials,
        })
    }
    pub async fn build(
        self,
        http: Arc<dyn HttpClient>,
        exchange: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> Result<(GcsBackend, &'static str), String> {
        match self.credentials {
            GcsAuth::Fixed(token) => Ok((GcsBackend::new(self.config, token, http), "static")),
            GcsAuth::Selected(selected) => {
                let resolved =
                    identity::acquire_gcs(selected, ResolutionContext::process(exchange, observer))
                        .await?;
                Ok((
                    GcsBackend::with_credentials(self.config, resolved.provider, http),
                    resolved.source,
                ))
            }
        }
    }
}
impl AzureOrigin {
    pub fn prepare(input: AzureOriginInput) -> Result<Self, String> {
        if matches!(input.validation, OriginValidation::Routed)
            && (input.account.is_empty()
                || !input.account.bytes().all(|b| b.is_ascii_alphanumeric()))
        {
            return Err("Azure account contains invalid characters".into());
        }
        let mut config = match input.endpoint {
            Some(value) => {
                let (host, tls) = endpoint(&value, input.use_ssl, input.validation)?;
                AzureConfig::emulator(&input.account, host, tls)
            }
            None => {
                let mut config = AzureConfig::new(&input.account);
                if let Some(suffix) = input.endpoint_suffix {
                    config.endpoint_suffix = suffix;
                }
                config
            }
        };
        if let Some(path_style) = input.path_style {
            config.path_style = path_style;
        }
        if let Some(tls) = input.use_ssl {
            config.tls = tls;
        }
        let credentials = match input.credentials {
            AzureCredentialInput::Resolve {
                shared_key,
                options,
            } => AzureAuth::Selected(identity::select_azure(
                &input.account,
                shared_key,
                *options,
                &identity::env_var,
            )?),
            AzureCredentialInput::FixedSas(sas) => AzureAuth::FixedSas(sas),
        };
        Ok(Self {
            config,
            credentials,
        })
    }
    pub async fn build(
        self,
        http: Arc<dyn HttpClient>,
        exchange: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> Result<(AzureBackend, &'static str), String> {
        let (auth, source) = match self.credentials {
            AzureAuth::FixedSas(sas) => (AzureCredentials::Sas(sas), "static"),
            AzureAuth::Selected(selected) => {
                let resolved = identity::acquire_azure(
                    selected,
                    ResolutionContext::process(exchange, observer),
                )
                .await?;
                (resolved.provider, resolved.source)
            }
        };
        Ok((
            AzureBackend::with_credentials(self.config, auth, http),
            source,
        ))
    }
}

/// Validated cloud connection and credential-source configuration, before acquisition.
pub(crate) enum BackendBuildConfig {
    S3(S3Origin),
    Gcs(GcsOrigin),
    Azure(AzureOrigin),
}
impl BackendBuildConfig {
    pub(crate) fn broker(&self) -> Option<&NormalizedBrokerConfig> {
        match self {
            Self::S3(origin) => origin.credentials.broker(),
            Self::Gcs(GcsOrigin {
                credentials: GcsAuth::Selected(selected),
                ..
            }) => selected.broker(),
            Self::Azure(AzureOrigin {
                credentials: AzureAuth::Selected(selected),
                ..
            }) => selected.broker(),
            _ => None,
        }
    }
    pub(crate) fn key(&self) -> serde_json::Value {
        match self {
            Self::S3(origin) => {
                let v = &origin.config;
                serde_json::json!(["s3", v.region, v.endpoint, v.path_style, v.tls])
            }
            Self::Gcs(origin) => {
                let v = &origin.config;
                serde_json::json!(["gcs", v.endpoint, v.tls])
            }
            Self::Azure(origin) => {
                let v = &origin.config;
                serde_json::json!([
                    "azure",
                    v.account,
                    v.endpoint_suffix,
                    v.endpoint_host,
                    v.path_style,
                    v.tls
                ])
            }
        }
    }
    pub(crate) async fn build(
        self,
        http: Arc<dyn HttpClient>,
        exchange: Arc<dyn HttpClient>,
        observer: Arc<dyn CredentialsObserver>,
    ) -> Result<Arc<dyn BackendStore>, String> {
        Ok(match self {
            Self::S3(origin) => {
                Arc::new(origin.build(http, exchange, observer).await?.0) as Arc<dyn BackendStore>
            }
            Self::Gcs(origin) => Arc::new(origin.build(http, exchange, observer).await?.0),
            Self::Azure(origin) => Arc::new(origin.build(http, exchange, observer).await?.0),
        })
    }

    /// Dynamic input consumes addressing fields. Retain the Broker deadline
    /// with reserved fields in the existing cache identity for compatibility.
    /// Never round-trip through a static bucket DTO.
    pub(crate) fn dynamic(
        backend: Backend,
        broker: crate::BrokerConfig,
        defaults: &crate::DynamicBackendDefaults,
    ) -> Result<(Self, Option<OriginOptions>), String> {
        broker.validate_request()?;
        let mut origin = broker.origin.clone().expect("validated request origin");
        let endpoint = origin.endpoint.take().expect("validated request endpoint");
        let use_ssl = origin.use_ssl.take();
        let options = CredentialOptions {
            source: Some("broker".into()),
            broker: Some(broker.clone()),
            ..Default::default()
        };
        let validation = OriginValidation::Routed;
        let prepared = match backend {
            Backend::S3 => {
                let region = origin
                    .region
                    .take()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or("credential_broker origin region is required for S3")?;
                Self::S3(S3Origin::prepare(S3OriginInput {
                    region,
                    endpoint: Some(endpoint),
                    path_style: origin
                        .use_virtual_host
                        .take()
                        .map(|v| !v)
                        .or(defaults.s3_path_style)
                        .unwrap_or(false),
                    use_ssl,
                    validation,
                    keys: None,
                    credentials: options,
                })?)
            }
            Backend::Gcs => Self::Gcs(GcsOrigin::prepare(GcsOriginInput {
                endpoint: Some(endpoint),
                use_ssl,
                validation,
                credentials: GcsCredentialInput::Resolve {
                    keys: None,
                    options: Box::new(options),
                },
            })?),
            Backend::Azure => {
                let account = broker
                    .request
                    .get(broker::FIELD_AZURE_ACCOUNT_NAME)
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or("bucket requires nonempty azure_account")?
                    .to_owned();
                let path_style = Some(origin.use_virtual_host.take().map(|v| !v).unwrap_or(false));
                Self::Azure(AzureOrigin::prepare(AzureOriginInput {
                    account,
                    endpoint: Some(endpoint),
                    endpoint_suffix: None,
                    path_style,
                    use_ssl,
                    validation,
                    credentials: AzureCredentialInput::Resolve {
                        shared_key: None,
                        options: Box::new(options),
                    },
                })?)
            }
        };
        Ok((
            prepared,
            (origin != OriginOptions::default()).then_some(origin),
        ))
    }
}
