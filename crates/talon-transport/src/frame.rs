//! Shared frame header for the control and data planes.
//!
//! # Byte layout (16 bytes, big-endian)
//!
//! ```text
//! offset  size  field        description
//! 0       2     magic        constant 0x_TAL_ -> [`MAGIC`], rejects non-Talon streams
//! 2       1     version      protocol version -> [`PROTOCOL_VERSION`]
//! 3       1     msg_type     [`MsgType`] discriminant
//! 4       2     flags        [`Flags`] bitset (e.g. END_OF_STREAM)
//! 6       2     reserved     must be zero; reserved for future use
//! 8       4     request_id   correlates a response with its request
//! 12      4     length       byte length of the payload that follows the header
//! ```
//!
//! The header is fixed-size and allocation-free to encode/decode. On the
//! control plane the `length` bytes that follow are a bincode message; on the
//! data plane they are raw payload (block/page bytes) suitable for
//! `sendfile`/`splice`.

use std::fmt;

/// Length of an encoded [`FrameHeader`] in bytes.
pub const HEADER_LEN: usize = 16;

/// Magic marker at the start of every frame: ASCII `"TL"`.
pub const MAGIC: u16 = u16::from_be_bytes(*b"TL");

/// Current wire-protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Upper bound on a single frame's payload length (256 MiB block + slack).
///
/// Guards against a malformed or hostile peer advertising a huge `length` and
/// forcing a large allocation. Whole 256 MiB blocks are streamed via
/// `sendfile` and are not required to fit in one framed control payload.
pub const MAX_PAYLOAD_LEN: u32 = 320 << 20;

/// The kind of message a frame carries.
///
/// Data-plane variants (`Get`, `GetRange`, `Put`) are followed by raw bytes;
/// control-plane variants are followed by a bincode message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    /// Control-plane request/response envelope (bincode payload).
    Control = 0,
    /// Data-plane whole-block fetch.
    Get = 1,
    /// Data-plane sub-range fetch.
    GetRange = 2,
    /// Data-plane ingest.
    Put = 3,
    /// Ping/keepalive; no payload.
    Ping = 4,
}

impl MsgType {
    /// Convert a raw discriminant into a [`MsgType`].
    pub fn from_u8(v: u8) -> Result<Self, FrameError> {
        Ok(match v {
            0 => MsgType::Control,
            1 => MsgType::Get,
            2 => MsgType::GetRange,
            3 => MsgType::Put,
            4 => MsgType::Ping,
            other => return Err(FrameError::UnknownMsgType(other)),
        })
    }
}

/// Frame flag bitset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Flags(pub u16);

impl Flags {
    /// No flags set.
    pub const EMPTY: Flags = Flags(0);
    /// This frame is the last in a multi-frame message/stream.
    pub const END_OF_STREAM: u16 = 0b0000_0001;
    /// The payload/operation encountered an error (data-plane error signal).
    pub const ERROR: u16 = 0b0000_0010;

    /// Return true if all bits in `mask` are set.
    pub fn contains(self, mask: u16) -> bool {
        self.0 & mask == mask
    }

    /// Return a copy with the bits in `mask` set.
    pub fn with(self, mask: u16) -> Flags {
        Flags(self.0 | mask)
    }
}

/// Errors produced while decoding a [`FrameHeader`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// The input was shorter than [`HEADER_LEN`].
    #[error("frame header truncated: need {HEADER_LEN} bytes, got {0}")]
    Truncated(usize),
    /// The leading magic bytes did not match [`MAGIC`].
    #[error("bad frame magic: 0x{0:04x}")]
    BadMagic(u16),
    /// The protocol version is not supported.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    /// The `msg_type` discriminant is unknown.
    #[error("unknown message type: {0}")]
    UnknownMsgType(u8),
    /// The advertised payload length exceeds [`MAX_PAYLOAD_LEN`].
    #[error("payload length {0} exceeds max {MAX_PAYLOAD_LEN}")]
    PayloadTooLarge(u32),
}

/// A fixed-size, versioned frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Kind of message the frame carries.
    pub msg_type: MsgType,
    /// Frame flags.
    pub flags: Flags,
    /// Correlates a response with its request.
    pub request_id: u32,
    /// Byte length of the payload following the header.
    pub length: u32,
}

impl FrameHeader {
    /// Construct a header with empty flags and the given request id/length.
    pub fn new(msg_type: MsgType, request_id: u32, length: u32) -> Self {
        Self {
            msg_type,
            flags: Flags::EMPTY,
            request_id,
            length,
        }
    }

    /// Encode into a fixed [`HEADER_LEN`]-byte array. Never allocates.
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..2].copy_from_slice(&MAGIC.to_be_bytes());
        buf[2] = PROTOCOL_VERSION;
        buf[3] = self.msg_type as u8;
        buf[4..6].copy_from_slice(&self.flags.0.to_be_bytes());
        // buf[6..8] reserved, left zero.
        buf[8..12].copy_from_slice(&self.request_id.to_be_bytes());
        buf[12..16].copy_from_slice(&self.length.to_be_bytes());
        buf
    }

    /// Decode from a byte slice, validating magic, version, type, and length.
    ///
    /// Only the first [`HEADER_LEN`] bytes are consumed; any trailing bytes
    /// (the payload) are ignored here and read separately by the caller.
    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < HEADER_LEN {
            return Err(FrameError::Truncated(buf.len()));
        }
        let magic = u16::from_be_bytes([buf[0], buf[1]]);
        if magic != MAGIC {
            return Err(FrameError::BadMagic(magic));
        }
        let version = buf[2];
        if version != PROTOCOL_VERSION {
            return Err(FrameError::UnsupportedVersion(version));
        }
        let msg_type = MsgType::from_u8(buf[3])?;
        let flags = Flags(u16::from_be_bytes([buf[4], buf[5]]));
        let request_id = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let length = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        if length > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(length));
        }
        Ok(Self {
            msg_type,
            flags,
            request_id,
            length,
        })
    }
}

impl fmt::Display for FrameHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}(req={}, len={}, flags=0x{:04x})",
            self.msg_type, self.request_id, self.length, self.flags.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_types() {
        for (i, ty) in [
            MsgType::Control,
            MsgType::Get,
            MsgType::GetRange,
            MsgType::Put,
            MsgType::Ping,
        ]
        .into_iter()
        .enumerate()
        {
            let h = FrameHeader {
                msg_type: ty,
                flags: Flags::EMPTY.with(Flags::END_OF_STREAM),
                request_id: 0xDEAD_0000 + i as u32,
                length: 4096 + i as u32,
            };
            let bytes = h.encode();
            assert_eq!(bytes.len(), HEADER_LEN);
            let back = FrameHeader::decode(&bytes).unwrap();
            assert_eq!(h, back);
            assert!(back.flags.contains(Flags::END_OF_STREAM));
        }
    }

    #[test]
    fn decode_truncated() {
        let bytes = FrameHeader::new(MsgType::Get, 1, 0).encode();
        let err = FrameHeader::decode(&bytes[..HEADER_LEN - 1]).unwrap_err();
        assert_eq!(err, FrameError::Truncated(HEADER_LEN - 1));
    }

    #[test]
    fn decode_bad_magic() {
        let mut bytes = FrameHeader::new(MsgType::Get, 1, 0).encode();
        bytes[0] ^= 0xFF;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(FrameError::BadMagic(_))
        ));
    }

    #[test]
    fn decode_bad_version() {
        let mut bytes = FrameHeader::new(MsgType::Get, 1, 0).encode();
        bytes[2] = PROTOCOL_VERSION + 1;
        assert_eq!(
            FrameHeader::decode(&bytes),
            Err(FrameError::UnsupportedVersion(PROTOCOL_VERSION + 1))
        );
    }

    #[test]
    fn decode_unknown_type() {
        let mut bytes = FrameHeader::new(MsgType::Get, 1, 0).encode();
        bytes[3] = 0xFF;
        assert_eq!(
            FrameHeader::decode(&bytes),
            Err(FrameError::UnknownMsgType(0xFF))
        );
    }

    #[test]
    fn decode_oversized_payload() {
        let mut bytes = FrameHeader::new(MsgType::Put, 1, 0).encode();
        bytes[12..16].copy_from_slice(&(MAX_PAYLOAD_LEN + 1).to_be_bytes());
        assert_eq!(
            FrameHeader::decode(&bytes),
            Err(FrameError::PayloadTooLarge(MAX_PAYLOAD_LEN + 1))
        );
    }

    #[test]
    fn trailing_bytes_ignored() {
        let mut bytes = FrameHeader::new(MsgType::Control, 9, 3).encode().to_vec();
        bytes.extend_from_slice(&[1, 2, 3]);
        let h = FrameHeader::decode(&bytes).unwrap();
        assert_eq!(h.length, 3);
        assert_eq!(h.request_id, 9);
    }
}
