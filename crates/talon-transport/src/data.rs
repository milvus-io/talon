//! Data-plane range request/response framing.
//!
//! Unlike the control plane (a bincode [`ControlMessage`](crate::ControlMessage)
//! envelope), a data-plane fetch is a [`MsgType::GetRange`] frame whose payload
//! is a small bincode [`RangeRequest`] naming the object + byte range. The
//! worker replies with a `GetRange` frame carrying the **raw bytes** of the
//! range (no envelope), or the [`Flags::ERROR`] bit set and a UTF-8 error string
//! as the payload.
//!
//! Keeping the request body tiny and the response body raw means the hot read
//! path can be served straight from a file (`sendfile`) in production; this
//! module only handles the request encode/decode and the response header shape.

use serde::{Deserialize, Serialize};
use talon_core::{BlockId, ObjectId, TenantId, Version};

use crate::frame::{Flags, FrameError, FrameHeader, MsgType, HEADER_LEN};

const ERROR_ENVELOPE_MAGIC: &[u8; 4] = b"TLE1";

/// Stable machine-readable classification for a data-plane failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataErrorCode {
    /// A legacy or otherwise unclassified failure.
    Unknown,
    /// The request could not be parsed or names an invalid range.
    InvalidRequest,
    /// The requested object does not exist at the origin.
    NotFound,
    /// The requested block is absent and origin fetch was disabled.
    CacheMiss,
    /// The worker or one of its required services is unavailable.
    Unavailable,
    /// The operation exceeded its deadline.
    Timeout,
    /// The source object changed relative to the requested version.
    VersionMismatch,
    /// The authoritative origin returned an operation failure.
    Origin,
    /// The worker encountered an internal or protocol failure.
    Internal,
}

/// A decoded data-plane error, including legacy string-only replies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPlaneError {
    /// Stable error class. Legacy replies use [`DataErrorCode::Unknown`].
    pub code: DataErrorCode,
    /// Human-readable diagnostic text; never use it for control flow.
    pub message: String,
}

impl std::fmt::Display for DataPlaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for DataPlaneError {}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorEnvelope {
    code: DataErrorCode,
    message: String,
}

/// A client→worker request to read `[offset, offset+len)` of an object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeRequest {
    /// The source object whose bytes are requested.
    pub object: ObjectId,
    /// Byte offset within the object to start reading at.
    pub offset: u64,
    /// Number of bytes to read.
    pub len: u64,
}

/// A [`RangeRequest`] annotated with the tenant it is attributed to.
///
/// Sent as a [`MsgType::GetRangeTenant`] frame so per-tenant QoS on the direct
/// data plane keys on a client-declared tenant. The reply is an ordinary
/// `GetRange` frame, identical to a plain request's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantScopedRange {
    /// The tenant this request is attributed to.
    pub tenant: TenantId,
    /// The underlying range request.
    pub request: RangeRequest,
}

/// A cache-only range probe carrying the exact versioned cache identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedRangeRequest {
    /// The source object whose resident bytes are requested.
    pub object: ObjectId,
    /// Exact origin version used in the cache block key.
    pub version: Version,
    /// Byte offset within the object to start reading at.
    pub offset: u64,
    /// Number of bytes to read.
    pub len: u64,
}

/// A client→worker request to write a whole object (write-through, #226).
///
/// This is the framed **header**; the raw object bytes (`body_len` of them)
/// follow the frame on the wire, so the worker can `splice` them straight to a
/// staging file without buffering them through the codec. `object` names the
/// destination and `body_len` is the exact number of trailing raw bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutRequest {
    /// The destination object to (over)write.
    pub object: ObjectId,
    /// Number of raw object bytes that follow this header on the wire.
    pub body_len: u64,
}

/// A request to admit one complete origin-fetched block into the local cache.
///
/// The raw block bytes (`body_len` of them) follow this framed header. The
/// worker validates the block geometry against `object_len` and never accesses
/// its backend while handling this operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedBlockPutRequest {
    /// Exact versioned cache identity of the block.
    pub block: BlockId,
    /// Authoritative total object length observed by the gateway.
    pub object_len: u64,
    /// Number of raw block bytes following this header.
    pub body_len: u64,
}

/// A client→worker request to delete an object (#226). No body follows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// The object to delete.
    pub object: ObjectId,
}

/// Errors from data-plane range encode/decode.
#[derive(Debug, thiserror::Error)]
pub enum DataError {
    /// The framing header was invalid.
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    /// A non-`GetRange` frame was handed to the data codec.
    #[error("expected a GetRange frame, got {0:?}")]
    NotGetRange(MsgType),
    /// A non-`Put` frame was handed to the put codec.
    #[error("expected a Put frame, got {0:?}")]
    NotPut(MsgType),
    /// A non-`AdmitCachedBlock` frame was handed to the admission codec.
    #[error("expected an AdmitCachedBlock frame, got {0:?}")]
    NotCachedBlockPut(MsgType),
    /// A non-`Delete` frame was handed to the delete codec.
    #[error("expected a Delete frame, got {0:?}")]
    NotDelete(MsgType),
    /// The header's declared length did not match the available payload bytes.
    #[error("length mismatch: header says {declared}, have {actual}")]
    LengthMismatch {
        /// Length advertised by the frame header.
        declared: usize,
        /// Bytes actually present after the header.
        actual: usize,
    },
    /// bincode failed to (de)serialize the request body.
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

/// Encode a [`RangeRequest`] into `header || bincode(req)`.
pub fn encode_request(request_id: u32, req: &RangeRequest) -> Result<Vec<u8>, DataError> {
    let body = bincode::serialize(req)?;
    let header = FrameHeader::new(MsgType::GetRange, request_id, body.len() as u32);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Decode a framed [`RangeRequest`] buffer into its header and request.
pub fn decode_request(buf: &[u8]) -> Result<(FrameHeader, RangeRequest), DataError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::GetRange {
        return Err(DataError::NotGetRange(header.msg_type));
    }
    let declared = header.length as usize;
    let body = &buf[HEADER_LEN..];
    if body.len() != declared {
        return Err(DataError::LengthMismatch {
            declared,
            actual: body.len(),
        });
    }
    let req: RangeRequest = bincode::deserialize(body)?;
    Ok((header, req))
}

/// Encode a tenant-attributed range request as a [`MsgType::GetRangeTenant`]
/// frame (`header || bincode(TenantScopedRange)`).
///
/// The reply is an ordinary `GetRange` frame, so a caller reads it back exactly
/// as it reads a plain [`encode_request`] reply. A worker that predates
/// per-tenant QoS rejects the distinct message type (fail-closed) rather than
/// misreading the tenant prefix as part of the request, so this must be sent
/// only to workers known to understand it.
pub fn encode_tenant_request(
    request_id: u32,
    scoped: &TenantScopedRange,
) -> Result<Vec<u8>, DataError> {
    let body = bincode::serialize(scoped)?;
    let header = FrameHeader::new(MsgType::GetRangeTenant, request_id, body.len() as u32);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Decode a [`MsgType::GetRangeTenant`] frame into its header and scoped request.
pub fn decode_tenant_request(buf: &[u8]) -> Result<(FrameHeader, TenantScopedRange), DataError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::GetRangeTenant {
        return Err(DataError::NotGetRange(header.msg_type));
    }
    let declared = header.length as usize;
    let body = &buf[HEADER_LEN..];
    if body.len() != declared {
        return Err(DataError::LengthMismatch {
            declared,
            actual: body.len(),
        });
    }
    let scoped = bincode::deserialize(body)?;
    Ok((header, scoped))
}

/// Encode a cache-only range probe. Older workers reject its distinct message
/// type instead of accidentally treating it as an origin-backed read.
pub fn encode_cached_request(
    request_id: u32,
    req: &CachedRangeRequest,
) -> Result<Vec<u8>, DataError> {
    let body = bincode::serialize(req)?;
    let header = FrameHeader::new(MsgType::GetCachedRange, request_id, body.len() as u32);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Decode a framed cache-only range probe.
pub fn decode_cached_request(buf: &[u8]) -> Result<(FrameHeader, CachedRangeRequest), DataError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::GetCachedRange {
        return Err(DataError::NotGetRange(header.msg_type));
    }
    let declared = header.length as usize;
    let body = &buf[HEADER_LEN..];
    if body.len() != declared {
        return Err(DataError::LengthMismatch {
            declared,
            actual: body.len(),
        });
    }
    let req = bincode::deserialize(body)?;
    Ok((header, req))
}

/// Encode a [`PutRequest`] header into `header || bincode(req)`.
///
/// The caller then writes exactly `req.body_len` raw object bytes after this
/// buffer (streamed, not buffered here). The frame header's `length` covers only
/// the small bincode header, so the receiver reads the header first and then
/// streams the trailing body.
pub fn encode_put_header(request_id: u32, req: &PutRequest) -> Result<Vec<u8>, DataError> {
    let body = bincode::serialize(req)?;
    let header = FrameHeader::new(MsgType::Put, request_id, body.len() as u32);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Decode a framed [`PutRequest`] header (header + bincode, no body).
///
/// `buf` must contain exactly the header frame (16-byte header + the bincode
/// `PutRequest`); the raw object body is read separately from the stream using
/// the returned `body_len`.
pub fn decode_put_header(buf: &[u8]) -> Result<(FrameHeader, PutRequest), DataError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::Put {
        return Err(DataError::NotPut(header.msg_type));
    }
    let declared = header.length as usize;
    let body = &buf[HEADER_LEN..];
    if body.len() != declared {
        return Err(DataError::LengthMismatch {
            declared,
            actual: body.len(),
        });
    }
    let req: PutRequest = bincode::deserialize(body)?;
    Ok((header, req))
}

/// Encode a cache-admission header. The caller writes exactly `body_len` raw
/// bytes after the returned frame.
pub fn encode_cached_block_put_header(
    request_id: u32,
    req: &CachedBlockPutRequest,
) -> Result<Vec<u8>, DataError> {
    let body = bincode::serialize(req)?;
    let header = FrameHeader::new(MsgType::AdmitCachedBlock, request_id, body.len() as u32);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Decode a cache-admission header (without its trailing raw body).
pub fn decode_cached_block_put_header(
    buf: &[u8],
) -> Result<(FrameHeader, CachedBlockPutRequest), DataError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::AdmitCachedBlock {
        return Err(DataError::NotCachedBlockPut(header.msg_type));
    }
    let declared = header.length as usize;
    let body = &buf[HEADER_LEN..];
    if body.len() != declared {
        return Err(DataError::LengthMismatch {
            declared,
            actual: body.len(),
        });
    }
    Ok((header, bincode::deserialize(body)?))
}

/// Encode a [`DeleteRequest`] into `header || bincode(req)` (no body follows).
pub fn encode_delete(request_id: u32, req: &DeleteRequest) -> Result<Vec<u8>, DataError> {
    let body = bincode::serialize(req)?;
    let header = FrameHeader::new(MsgType::Delete, request_id, body.len() as u32);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Decode a framed [`DeleteRequest`] buffer into its header and request.
pub fn decode_delete(buf: &[u8]) -> Result<(FrameHeader, DeleteRequest), DataError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::Delete {
        return Err(DataError::NotDelete(header.msg_type));
    }
    let declared = header.length as usize;
    let body = &buf[HEADER_LEN..];
    if body.len() != declared {
        return Err(DataError::LengthMismatch {
            declared,
            actual: body.len(),
        });
    }
    let req: DeleteRequest = bincode::deserialize(body)?;
    Ok((header, req))
}

/// Build a successful data response header for `len` raw payload bytes.
///
/// The caller writes this 16-byte header followed by exactly `len` bytes (the
/// range contents).
pub fn response_header_ok(request_id: u32, len: u32) -> [u8; HEADER_LEN] {
    FrameHeader::new(MsgType::GetRange, request_id, len).encode()
}

/// Build an error response header + body (`ERROR` flag set, UTF-8 message).
pub fn encode_error(request_id: u32, message: &str) -> Vec<u8> {
    let body = message.as_bytes();
    let mut header = FrameHeader::new(MsgType::GetRange, request_id, body.len() as u32);
    header.flags = Flags(Flags::ERROR);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(body);
    buf
}

/// Build a typed error response understood by new clients.
///
/// The `ERROR` frame shape is unchanged. A short magic prefix distinguishes
/// the bincode envelope from legacy UTF-8 error payloads, so rolling upgrades
/// remain fail-closed in both directions.
pub fn encode_typed_error(
    request_id: u32,
    code: DataErrorCode,
    message: impl Into<String>,
) -> Vec<u8> {
    let envelope = ErrorEnvelope {
        code,
        message: message.into(),
    };
    let encoded = bincode::serialize(&envelope).expect("error envelope is serializable");
    let mut body = Vec::with_capacity(ERROR_ENVELOPE_MAGIC.len() + encoded.len());
    body.extend_from_slice(ERROR_ENVELOPE_MAGIC);
    body.extend_from_slice(&encoded);
    let mut header = FrameHeader::new(MsgType::GetRange, request_id, body.len() as u32);
    header.flags = Flags(Flags::ERROR);
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&body);
    out
}

/// Decode a typed or legacy error-frame payload.
///
/// A malformed typed envelope is classified as [`DataErrorCode::Internal`]
/// rather than falling back to lossy string matching.
pub fn decode_error_payload(body: &[u8]) -> DataPlaneError {
    let Some(encoded) = body.strip_prefix(ERROR_ENVELOPE_MAGIC) else {
        return DataPlaneError {
            code: DataErrorCode::Unknown,
            message: String::from_utf8_lossy(body).into_owned(),
        };
    };
    match bincode::deserialize::<ErrorEnvelope>(encoded) {
        Ok(envelope) => DataPlaneError {
            code: envelope.code,
            message: envelope.message,
        },
        Err(error) => DataPlaneError {
            code: DataErrorCode::Internal,
            message: format!("malformed typed worker error: {error}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;

    fn req() -> RangeRequest {
        RangeRequest {
            object: ObjectId::new(Backend::Azure, "container", "path/to/blob.bin"),
            offset: 1 << 20,
            len: 4096,
        }
    }

    #[test]
    fn request_round_trips() {
        let buf = encode_request(11, &req()).unwrap();
        let (header, back) = decode_request(&buf).unwrap();
        assert_eq!(header.msg_type, MsgType::GetRange);
        assert_eq!(header.request_id, 11);
        assert_eq!(back, req());
    }

    #[test]
    fn cached_request_round_trips_with_version() {
        let request = CachedRangeRequest {
            object: req().object,
            version: Version::new("etag-v2"),
            offset: 17,
            len: 23,
        };
        let encoded = encode_cached_request(12, &request).unwrap();
        let (header, decoded) = decode_cached_request(&encoded).unwrap();
        assert_eq!(header.msg_type, MsgType::GetCachedRange);
        assert_eq!(header.request_id, 12);
        assert_eq!(decoded, request);
    }

    #[test]
    fn non_get_range_frame_rejected() {
        let mut buf = FrameHeader::new(MsgType::Control, 0, 0).encode().to_vec();
        buf.truncate(HEADER_LEN);
        assert!(matches!(
            decode_request(&buf),
            Err(DataError::NotGetRange(MsgType::Control))
        ));
    }

    #[test]
    fn truncated_body_rejected() {
        let mut buf = encode_request(1, &req()).unwrap();
        buf.pop();
        assert!(matches!(
            decode_request(&buf),
            Err(DataError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn tenant_request_round_trips() {
        let scoped = TenantScopedRange {
            tenant: TenantId::named("acme"),
            request: req(),
        };
        let buf = encode_tenant_request(5, &scoped).unwrap();
        let header = FrameHeader::decode(&buf).unwrap();
        assert_eq!(header.msg_type, MsgType::GetRangeTenant);
        let (decoded_header, back) = decode_tenant_request(&buf).unwrap();
        assert_eq!(decoded_header.request_id, 5);
        assert_eq!(back, scoped);
        assert_eq!(back.tenant, TenantId::named("acme"));
    }

    #[test]
    fn unattributed_tenant_request_round_trips() {
        let scoped = TenantScopedRange {
            tenant: TenantId::unattributed(),
            request: req(),
        };
        let buf = encode_tenant_request(1, &scoped).unwrap();
        let (_header, back) = decode_tenant_request(&buf).unwrap();
        assert_eq!(back.tenant, TenantId::unattributed());
        assert_eq!(back.request, req());
    }

    #[test]
    fn tenant_frame_is_not_misread_by_the_plain_range_decoder() {
        // Fail-closed: the distinct message type means an older GetRange decoder
        // rejects the frame rather than reading the tenant prefix as a request.
        let scoped = TenantScopedRange {
            tenant: TenantId::named("acme"),
            request: req(),
        };
        let buf = encode_tenant_request(5, &scoped).unwrap();
        assert!(matches!(
            decode_request(&buf),
            Err(DataError::NotGetRange(MsgType::GetRangeTenant))
        ));
    }

    #[test]
    fn error_response_sets_error_flag() {
        let buf = encode_error(7, "boom");
        let header = FrameHeader::decode(&buf).unwrap();
        assert!(header.flags.contains(Flags::ERROR));
        assert_eq!(header.request_id, 7);
        assert_eq!(&buf[HEADER_LEN..], b"boom");
    }

    #[test]
    fn typed_error_round_trips_with_a_stable_code() {
        let buf = encode_typed_error(9, DataErrorCode::VersionMismatch, "object changed");
        let header = FrameHeader::decode(&buf).unwrap();
        assert!(header.flags.contains(Flags::ERROR));
        assert_eq!(header.request_id, 9);
        assert_eq!(
            decode_error_payload(&buf[HEADER_LEN..]),
            DataPlaneError {
                code: DataErrorCode::VersionMismatch,
                message: "object changed".into(),
            }
        );
    }

    #[test]
    fn legacy_error_is_unknown_without_string_classification() {
        let buf = encode_error(4, "object not found: bucket/key");
        assert_eq!(
            decode_error_payload(&buf[HEADER_LEN..]),
            DataPlaneError {
                code: DataErrorCode::Unknown,
                message: "object not found: bucket/key".into(),
            }
        );
    }

    #[test]
    fn malformed_typed_error_fails_closed_as_internal() {
        let error = decode_error_payload(b"TLE1not-bincode");
        assert_eq!(error.code, DataErrorCode::Internal);
        assert!(error.message.contains("malformed typed worker error"));
    }

    #[test]
    fn ok_response_header_declares_len() {
        let h = response_header_ok(3, 8192);
        let header = FrameHeader::decode(&h).unwrap();
        assert_eq!(header.length, 8192);
        assert!(!header.flags.contains(Flags::ERROR));
    }

    fn obj() -> ObjectId {
        ObjectId::new(Backend::S3, "bucket", "path/to/object.bin")
    }

    #[test]
    fn put_header_round_trips() {
        let req = PutRequest {
            object: obj(),
            body_len: 1 << 30, // 1 GiB body streamed separately
        };
        let buf = encode_put_header(42, &req).unwrap();
        let (header, back) = decode_put_header(&buf).unwrap();
        assert_eq!(header.msg_type, MsgType::Put);
        assert_eq!(header.request_id, 42);
        assert_eq!(back, req);
        // The frame length covers only the small header, NOT the body.
        assert!((header.length as usize) < 1024);
    }

    #[test]
    fn cached_block_put_header_round_trips() {
        let request = CachedBlockPutRequest {
            block: BlockId::new(obj(), 8, 8, Version::new("etag-v3")),
            object_len: 13,
            body_len: 5,
        };
        let encoded = encode_cached_block_put_header(43, &request).unwrap();
        let (header, decoded) = decode_cached_block_put_header(&encoded).unwrap();
        assert_eq!(header.msg_type, MsgType::AdmitCachedBlock);
        assert_eq!(header.request_id, 43);
        assert_eq!(decoded, request);
        assert!((header.length as usize) < 1024);
    }

    #[test]
    fn delete_round_trips() {
        let req = DeleteRequest { object: obj() };
        let buf = encode_delete(9, &req).unwrap();
        let (header, back) = decode_delete(&buf).unwrap();
        assert_eq!(header.msg_type, MsgType::Delete);
        assert_eq!(header.request_id, 9);
        assert_eq!(back, req);
    }

    #[test]
    fn put_codec_rejects_wrong_frame_type() {
        // A Delete frame handed to the put decoder is rejected, and vice versa.
        let del = encode_delete(1, &DeleteRequest { object: obj() }).unwrap();
        assert!(matches!(
            decode_put_header(&del),
            Err(DataError::NotPut(MsgType::Delete))
        ));
        let put = encode_put_header(
            1,
            &PutRequest {
                object: obj(),
                body_len: 0,
            },
        )
        .unwrap();
        assert!(matches!(
            decode_delete(&put),
            Err(DataError::NotDelete(MsgType::Put))
        ));
    }

    #[test]
    fn put_header_truncated_body_rejected() {
        let mut buf = encode_put_header(
            1,
            &PutRequest {
                object: obj(),
                body_len: 10,
            },
        )
        .unwrap();
        buf.pop();
        assert!(matches!(
            decode_put_header(&buf),
            Err(DataError::LengthMismatch { .. })
        ));
    }
}
