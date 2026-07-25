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
use talon_core::ObjectId;

use crate::frame::{Flags, FrameError, FrameHeader, MsgType, HEADER_LEN};

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
    fn error_response_sets_error_flag() {
        let buf = encode_error(7, "boom");
        let header = FrameHeader::decode(&buf).unwrap();
        assert!(header.flags.contains(Flags::ERROR));
        assert_eq!(header.request_id, 7);
        assert_eq!(&buf[HEADER_LEN..], b"boom");
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
