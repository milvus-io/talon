use talon_cache_client::{BlockReadError, CoordinatorError};

/// Structured failures from parsing an object URI.
#[derive(Debug, thiserror::Error)]
pub enum UriError {
    #[error("expected a scheme://bucket/key URI, got {uri:?} (schemes: s3, gcs, az)")]
    MissingScheme { uri: String },
    #[error("unknown backend scheme {scheme:?}; expected s3, gcs, or az")]
    UnknownScheme { scheme: String },
    #[error("URI is missing an object key: {uri:?} (expected {scheme}://bucket/key)")]
    MissingKey { uri: String, scheme: String },
    #[error("URI has an empty bucket: {uri:?}")]
    EmptyBucket { uri: String },
    #[error("URI has an empty object key: {uri:?}")]
    EmptyKey { uri: String },
}

/// Errors returned by the native Rust client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    InvalidUri(#[from] UriError),
    #[error("{0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    #[error(transparent)]
    Block(#[from] BlockReadError),
}

/// Stable public classification. This is advisory; the SDK never executes fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidArgument,
    Unavailable,
    Timeout,
    NotFound,
    CacheMiss,
    VersionMismatch,
    Origin,
    RateLimited,
    Protocol,
    Internal,
    Cancelled,
    Unknown,
}
impl Error {
    pub fn kind(&self) -> ErrorKind {
        use talon_cache_client::WorkerError;
        fn io(error: &std::io::Error) -> ErrorKind {
            match error.kind() {
                std::io::ErrorKind::TimedOut => ErrorKind::Timeout,
                std::io::ErrorKind::Interrupted => ErrorKind::Cancelled,
                std::io::ErrorKind::InvalidInput => ErrorKind::InvalidArgument,
                _ => ErrorKind::Unavailable,
            }
        }
        fn remote(code: talon_transport::DataErrorCode) -> ErrorKind {
            use talon_transport::DataErrorCode as C;
            match code {
                C::Unknown => ErrorKind::Unknown,
                C::InvalidRequest => ErrorKind::InvalidArgument,
                C::Unavailable => ErrorKind::Unavailable,
                C::Timeout => ErrorKind::Timeout,
                C::NotFound => ErrorKind::NotFound,
                C::CacheMiss => ErrorKind::CacheMiss,
                C::VersionMismatch => ErrorKind::VersionMismatch,
                C::Origin => ErrorKind::Origin,
                C::RateLimited => ErrorKind::RateLimited,
                C::Internal => ErrorKind::Internal,
            }
        }
        fn worker(error: &WorkerError) -> ErrorKind {
            match error {
                WorkerError::Io(e) => io(e),
                WorkerError::Remote(e) => remote(e.code),
                _ => ErrorKind::Protocol,
            }
        }
        fn coordinator(error: &CoordinatorError) -> ErrorKind {
            match error {
                CoordinatorError::Io(e) => io(e),
                CoordinatorError::Remote(e) => remote(e.code),
                _ => ErrorKind::Protocol,
            }
        }
        match self {
            Self::InvalidUri(_) | Self::InvalidArgument(_) => ErrorKind::InvalidArgument,
            Self::Coordinator(e) | Self::Block(BlockReadError::Coordinator(e)) => coordinator(e),
            Self::Block(
                BlockReadError::Worker(e)
                | BlockReadError::AllReplicasFailed { source: e, .. }
                | BlockReadError::Target { source: e, .. },
            ) => worker(e),
            Self::Block(BlockReadError::NoOwners | BlockReadError::UnresolvedOwner) => {
                ErrorKind::Unavailable
            }
        }
    }
    pub fn fallback_eligible(&self) -> bool {
        matches!(self.kind(), ErrorKind::Unavailable | ErrorKind::Timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classification_follows_causes_without_parsing_messages() {
        use talon_transport::{DataErrorCode as C, DataPlaneError};
        for (code, expected) in [
            (C::Unavailable, ErrorKind::Unavailable),
            (C::VersionMismatch, ErrorKind::VersionMismatch),
            (C::RateLimited, ErrorKind::RateLimited),
            (C::Origin, ErrorKind::Origin),
        ] {
            let error = Error::Block(BlockReadError::AllReplicasFailed {
                worker: "w".into(),
                source: talon_cache_client::WorkerError::Remote(DataPlaneError {
                    code,
                    message: "timeout unavailable".into(),
                }),
            });
            assert_eq!(error.kind(), expected);
            assert_eq!(error.fallback_eligible(), code == C::Unavailable);
        }
        let timeout = Error::Coordinator(CoordinatorError::Io(std::io::ErrorKind::TimedOut.into()));
        assert_eq!(timeout.kind(), ErrorKind::Timeout);
        let cancelled =
            Error::Coordinator(CoordinatorError::Io(std::io::ErrorKind::Interrupted.into()));
        assert!(!cancelled.fallback_eligible());
    }
}
