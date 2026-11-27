//! Shared semantics for origin-backed Range reads, independent of socket I/O.
use std::time::Duration;

use talon_core::{NamespaceId, RequestId, Version};
use talon_transport::{data, DataErrorCode, DataPlaneError, FrameHeader, MsgType, HEADER_LEN};

use crate::{ServeOutcome, Throttled, WorkerMetrics, WorkerObservability, WorkerRuntime};

#[derive(Debug)]
pub(crate) enum RangeError {
    Request(DataPlaneError),
    Throttled(Throttled),
    Runtime(anyhow::Error),
}

impl RangeError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Request(DataPlaneError {
            code: DataErrorCode::InvalidRequest,
            message: message.into(),
        })
    }

    pub(crate) fn response(&self, header: &FrameHeader) -> Vec<u8> {
        let mut frame = match self {
            Self::Request(error) => {
                data::encode_typed_error(header.request_id, error.code, &error.message)
            }
            Self::Throttled(error) => data::encode_typed_error(
                header.request_id,
                DataErrorCode::RateLimited,
                format!(
                    "namespace rate limit exceeded on {}; retry after {} ms",
                    error.metric.label(),
                    error.retry_after.as_millis()
                ),
            ),
            Self::Runtime(error) => {
                crate::data_error::encode_runtime_error(header.request_id, error)
            }
        };
        frame[2] = header.version;
        talon_telemetry::outcome("error");
        frame
    }

    /// Count a rejection only after its error frame has been sent.
    pub(crate) fn record(&self, metrics: &WorkerMetrics, elapsed: Duration) {
        match self {
            Self::Throttled(error) => metrics.record_rate_limited(error.metric),
            _ => metrics.record_request_error(elapsed),
        }
    }
}

/// The caller has already selected the namespace and credential scope with `for_request`.
pub(crate) async fn serve(
    header: &FrameHeader,
    payload: &[u8],
    worker: &WorkerRuntime,
    observability: &WorkerObservability,
) -> Result<ServeOutcome, RangeError> {
    if !matches!(
        header.msg_type,
        MsgType::GetRange
            | MsgType::GetRangeNamespace
            | MsgType::GetVersionedRange
            | MsgType::GetVersionedRangeNamespace
            | MsgType::GetExactRange
            | MsgType::GetExactRangeNamespace
    ) {
        return Err(RangeError::invalid(
            "worker only serves GetRange/Put/Delete/StatObject/ListObjects",
        ));
    }
    let (h, req, expected_version, namespace) = decode_range_with_namespace(header, payload)
        .map_err(|error| RangeError::invalid(format!("bad request: {error}")))?;
    namespace
        .validate()
        .map_err(|error| RangeError::invalid(format!("invalid namespace: {error}")))?;
    if !observability.is_ready() {
        return Err(RangeError::Request(DataPlaneError {
            code: DataErrorCode::Unavailable,
            message: "worker is not ready".into(),
        }));
    }
    let end = req.offset.checked_add(req.len);
    let exact = matches!(
        h.msg_type,
        MsgType::GetExactRange | MsgType::GetExactRangeNamespace
    );
    if end.is_none()
        || (exact && req.len > talon_core::MAX_EXACT_READ_LEN)
        || expected_version
            .as_ref()
            .map(|(_, size)| *size)
            .is_some_and(|size| end.is_some_and(|end| end > size))
    {
        return Err(RangeError::invalid(
            "range exceeds the object size, u64 bounds, or the 1 MiB exact-read limit",
        ));
    }
    if expected_version
        .as_ref()
        .is_some_and(|(version, _)| version.0.trim().is_empty())
    {
        return Err(RangeError::invalid(
            "version-pinned range request has an empty source version",
        ));
    }
    worker
        .rate_limiter()
        .admit(worker.namespace(), req.len)
        .map_err(RangeError::Throttled)?;
    let outcome = match expected_version.as_ref() {
        Some((version, size)) if exact => worker.serve_exact(&req, version, *size).await,
        Some((version, size)) => worker.serve_versioned(&req, version, *size).await,
        None => worker.serve(&req).await,
    };
    outcome.map_err(|error| {
        tracing::error!(
            req = %RequestId(h.request_id),
            object = %req.object.to_path(),
            offset = req.offset,
            len = req.len,
            error = %error,
            "serving range failed"
        );
        RangeError::Runtime(error)
    })
}

// Source version and its caller-supplied total length.
type PinnedVersion = (Version, u64);

/// Decode origin-backed coordinates, the optional exact version, and namespace.
fn decode_range_with_namespace(
    header: &FrameHeader,
    payload: &[u8],
) -> Result<
    (
        FrameHeader,
        data::RangeRequest,
        Option<PinnedVersion>,
        NamespaceId,
    ),
    talon_transport::DataError,
> {
    let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
    full.extend_from_slice(&header.encode());
    full.extend_from_slice(payload);
    match header.msg_type {
        MsgType::GetRange => {
            let (frame, request) = data::decode_request(&full)?;
            Ok((frame, request, None, NamespaceId::Unattributed))
        }
        MsgType::GetRangeNamespace => {
            let (frame, scoped) = data::decode_namespace_request(&full)?;
            Ok((frame, scoped.request, None, scoped.namespace))
        }
        MsgType::GetVersionedRange | MsgType::GetExactRange => {
            let (frame, versioned) = if header.msg_type == MsgType::GetExactRange {
                data::decode_exact_request(&full)?
            } else {
                data::decode_versioned_request(&full)?
            };
            Ok((
                frame,
                versioned.request,
                Some((versioned.version, versioned.object_len)),
                NamespaceId::Unattributed,
            ))
        }
        MsgType::GetVersionedRangeNamespace | MsgType::GetExactRangeNamespace => {
            let (frame, scoped) = if header.msg_type == MsgType::GetExactRangeNamespace {
                data::decode_exact_namespace_request(&full)?
            } else {
                data::decode_versioned_namespace_request(&full)?
            };
            Ok((
                frame,
                scoped.request.request,
                Some((scoped.request.version, scoped.request.object_len)),
                scoped.namespace,
            ))
        }
        other => Err(talon_transport::DataError::NotGetRange(other)),
    }
}

#[cfg(test)]
mod tests;
