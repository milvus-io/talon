//! Stable data-plane error classification shared by both worker runtimes.

use talon_core::Error;
use talon_transport::{
    codec, encode_typed_error, ControlMessage, DataErrorCode, DataPlaneError, FrameHeader, MsgType,
};

/// Preserve the request's reply contract when namespace or credential resolution fails.
pub(crate) fn encode_request_error(
    header: &FrameHeader,
    payload: &[u8],
    error: DataPlaneError,
) -> Result<Vec<u8>, codec::CodecError> {
    let frame = if header.msg_type == MsgType::Control {
        let mut request = header.encode().to_vec();
        request.extend_from_slice(payload);
        let reply = match codec::decode_request(&request) {
            Ok((_, ControlMessage::StatObject { .. } | ControlMessage::ListObjects { .. })) => {
                ControlMessage::ControlFailure {
                    code: error.code,
                    message: error.message,
                }
            }
            // LOAD clients consume failed acknowledgements, including batch failures.
            _ => ControlMessage::Ack {
                ok: false,
                detail: Some(error.message),
            },
        };
        codec::encode(header.request_id, &reply)?
    } else {
        encode_typed_error(header.request_id, error.code, error.message)
    };
    Ok(talon_transport::envelope::response_version(
        frame,
        header.version,
    ))
}

/// Marker returned when a cache-only request is not fully resident.
#[derive(Debug)]
pub(crate) struct CacheMiss;

impl std::fmt::Display for CacheMiss {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("requested range is not fully resident")
    }
}

impl std::error::Error for CacheMiss {}

/// Encode an error returned by [`crate::WorkerRuntime`] for a range request.
pub(crate) fn encode_runtime_error(request_id: u32, error: &anyhow::Error) -> Vec<u8> {
    talon_telemetry::outcome(if classify(error) == DataErrorCode::CacheMiss {
        "cache_miss"
    } else {
        "error"
    });
    encode_typed_error(request_id, classify(error), error.to_string())
}

pub(crate) fn classify(error: &anyhow::Error) -> DataErrorCode {
    if error.downcast_ref::<CacheMiss>().is_some() {
        return DataErrorCode::CacheMiss;
    }
    if let Some(error) = error.downcast_ref::<Error>() {
        return match error {
            Error::NotFound(_) => DataErrorCode::NotFound,
            Error::NodeUnavailable(_) => DataErrorCode::Unavailable,
            Error::Backend(_) => DataErrorCode::Origin,
            Error::VersionMismatch { .. } => DataErrorCode::VersionMismatch,
            Error::Io(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                DataErrorCode::Timeout
            }
            Error::Io(_) => DataErrorCode::Unavailable,
            Error::Unsupported(_) | Error::Serialization(_) | Error::Other(_) => {
                DataErrorCode::Internal
            }
        };
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return if error.kind() == std::io::ErrorKind::TimedOut {
            DataErrorCode::Timeout
        } else {
            DataErrorCode::Unavailable
        };
    }
    DataErrorCode::Internal
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_transport::{decode_error_payload, FrameHeader, HEADER_LEN};

    fn encoded_code(error: anyhow::Error) -> DataErrorCode {
        let frame = encode_runtime_error(1, &error);
        let header = FrameHeader::decode(&frame).unwrap();
        decode_error_payload(&frame[HEADER_LEN..HEADER_LEN + header.length as usize]).code
    }

    #[test]
    fn core_failures_have_stable_codes() {
        assert_eq!(encoded_code(CacheMiss.into()), DataErrorCode::CacheMiss);
        assert_eq!(
            encoded_code(Error::NotFound("x".into()).into()),
            DataErrorCode::NotFound
        );
        assert_eq!(
            encoded_code(
                Error::VersionMismatch {
                    expected: "a".into(),
                    found: "b".into(),
                }
                .into()
            ),
            DataErrorCode::VersionMismatch
        );
        assert_eq!(
            encoded_code(Error::Backend("origin failed".into()).into()),
            DataErrorCode::Origin
        );
    }

    #[test]
    fn timeout_is_distinct_from_other_io() {
        assert_eq!(
            encoded_code(std::io::Error::new(std::io::ErrorKind::TimedOut, "slow").into()),
            DataErrorCode::Timeout
        );
        assert_eq!(
            encoded_code(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset").into()),
            DataErrorCode::Unavailable
        );
    }
}
