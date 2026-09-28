//! Also compiled by the standalone consumer, whose only dependency is the SDK.

use std::error::Error as _;
use std::io::{Error as IoError, ErrorKind};
use std::mem::discriminant;

use talon_rust_client::{
    BlockReadError, CacheReadError, CoordinatorError, DataErrorCode, DataPlaneError, Error,
    UriError, WorkerError,
};

// Deliberately misleading words must never override the typed cause.
const MESSAGE: &str = "diagnostic mentions NotFound Timeout; use the typed cause";

// An example consumer's classification, not a new SDK retry/fallback policy.
fn classify(error: Error) -> (CacheReadError, String) {
    let diagnostic = error.to_string();
    let class = match error {
        Error::InvalidUri(_) | Error::InvalidArgument(_) => {
            CacheReadError::InvalidRequest(diagnostic.clone())
        }
        Error::Coordinator(source) | Error::Block(BlockReadError::Coordinator(source)) => {
            source.into()
        }
        Error::Block(BlockReadError::Worker(source)) => source.into(),
        Error::Block(BlockReadError::NoOwners) => CacheReadError::Unavailable(diagnostic.clone()),
    };
    (class, diagnostic)
}

fn worker_error(source: WorkerError) -> Error {
    let diagnostic = source.to_string();
    let error: Error = BlockReadError::from(source).into();
    assert_eq!(error.to_string(), diagnostic);
    error
}

fn assert_class(error: Error, expected: &CacheReadError) {
    let original = error.to_string();
    let (class, diagnostic) = classify(error);
    assert_eq!(discriminant(&class), discriminant(expected), "{class:?}");
    assert_eq!(diagnostic, original);
}

#[test]
fn remote_codes_preserve_typed_classification() {
    let cases = [
        (
            DataErrorCode::InvalidRequest,
            CacheReadError::InvalidRequest(MESSAGE.into()),
        ),
        (
            DataErrorCode::NotFound,
            CacheReadError::NotFound(MESSAGE.into()),
        ),
        (
            DataErrorCode::Timeout,
            CacheReadError::Timeout(MESSAGE.into()),
        ),
        (
            DataErrorCode::Unavailable,
            CacheReadError::Unavailable(MESSAGE.into()),
        ),
        (
            DataErrorCode::RateLimited,
            CacheReadError::RateLimited(MESSAGE.into()),
        ),
        (
            DataErrorCode::VersionMismatch,
            CacheReadError::VersionMismatch(MESSAGE.into()),
        ),
        (
            DataErrorCode::Origin,
            CacheReadError::Origin(MESSAGE.into()),
        ),
        (
            DataErrorCode::Internal,
            CacheReadError::Internal(MESSAGE.into()),
        ),
        (
            DataErrorCode::Unknown,
            CacheReadError::Unknown(MESSAGE.into()),
        ),
        (
            DataErrorCode::CacheMiss,
            CacheReadError::CacheMiss(MESSAGE.into()),
        ),
    ];
    for (code, expected) in cases {
        let error = worker_error(WorkerError::Remote(DataPlaneError {
            code,
            message: MESSAGE.into(),
        }));

        let (class, diagnostic) = classify(error);
        assert_eq!(discriminant(&class), discriminant(&expected), "{code:?}");
        assert_eq!(class.to_string(), expected.to_string());
        assert!(diagnostic.contains(MESSAGE));
    }
}

#[test]
fn io_classification_uses_error_kind_in_all_sdk_paths() {
    for (kind, expected) in [
        (ErrorKind::TimedOut, CacheReadError::Timeout(MESSAGE.into())),
        (
            ErrorKind::ConnectionRefused,
            CacheReadError::Unavailable(MESSAGE.into()),
        ),
        (
            ErrorKind::ConnectionReset,
            CacheReadError::Unavailable(MESSAGE.into()),
        ),
    ] {
        let source = WorkerError::from(IoError::new(kind, MESSAGE));
        assert_class(worker_error(source), &expected);

        for block in [false, true] {
            let source = CoordinatorError::from(IoError::new(kind, MESSAGE));
            let error = if block {
                Error::from(BlockReadError::from(source))
            } else {
                source.into()
            };
            let preserved = error.source().unwrap().downcast_ref::<IoError>().unwrap();
            assert_eq!(preserved.kind(), kind);
            assert_eq!(preserved.to_string(), MESSAGE);
            assert_class(error, &expected);
        }
    }
}

#[test]
fn protocol_length_errors_stay_protocol_errors() {
    let expected = CacheReadError::Protocol(String::new());

    for source in [
        WorkerError::RangeLengthMismatch {
            expected: 100,
            actual: 99,
        },
        WorkerError::PayloadTooLarge {
            length: 100,
            cap: 99,
        },
    ] {
        assert_class(worker_error(source), &expected);
    }

    for block in [false, true] {
        let source = CoordinatorError::PayloadTooLarge {
            length: 100,
            cap: 99,
        };
        let error = if block {
            Error::from(BlockReadError::from(source))
        } else {
            source.into()
        };
        assert_class(error, &expected);
    }
}

#[test]
fn placement_failures_are_unavailable_to_the_consumer() {
    assert_class(
        BlockReadError::NoOwners.into(),
        &CacheReadError::Unavailable(String::new()),
    );
}

#[test]
fn invalid_inputs_are_invalid_requests_to_the_consumer() {
    for error in [
        Error::InvalidArgument(MESSAGE.into()),
        UriError::MissingScheme {
            uri: MESSAGE.into(),
        }
        .into(),
    ] {
        assert_class(error, &CacheReadError::InvalidRequest(String::new()));
    }
}
