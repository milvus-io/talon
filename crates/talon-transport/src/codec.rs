//! Control-plane message codec.
//!
//! Control messages are a versioned [`ControlMessage`] enum, serialized with
//! **bincode** and framed by a [`FrameHeader`] whose `msg_type` is
//! [`Control`](crate::MsgType::Control). No JSON is used on the wire.
//!
//! [`encode`] produces `header || bincode(msg)` as a single buffer;
//! [`decode`] validates the header, checks the declared length against the
//! remaining bytes, and deserializes. Unknown or newer message shapes surface a
//! [`CodecError`] rather than panicking.

use serde::{Deserialize, Serialize};
use talon_core::{BlockId, NodeId, NodeInfo, ObjectId};

use crate::frame::{FrameError, FrameHeader, MsgType, HEADER_LEN};

/// Wire schema version for the control message set.
///
/// Bumped when [`ControlMessage`] changes in an incompatible way. Carried in
/// the envelope so a peer can reject a mismatched schema instead of
/// misinterpreting bytes.
pub const CONTROL_SCHEMA_VERSION: u16 = 1;

/// A single control-plane message.
///
/// Deliberately minimal for v1; extend per consumer (membership, placement,
/// load). `#[non_exhaustive]` so adding a variant is not a breaking change for
/// matchers that already have a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ControlMessage {
    /// Worker → coordinator: announce presence and address.
    Register {
        /// Identity of the registering node.
        node: NodeInfo,
    },
    /// Worker → coordinator: periodic liveness + block-count summary.
    Heartbeat {
        /// The reporting worker.
        node: NodeId,
        /// Number of blocks currently resident.
        block_count: u64,
    },
    /// Client → coordinator: where does this block live?
    PlacementLookup {
        /// The block being located.
        block: BlockId,
        /// Number of replicas requested (RF=1 → 1 in v1).
        k: u8,
    },
    /// Coordinator → client: ordered owners + the epoch they were computed at.
    PlacementResponse {
        /// Ordered replica node ids (highest weight first).
        owners: Vec<NodeId>,
        /// Placement epoch these owners were computed against.
        epoch: u64,
    },
    /// Coordinator → worker: prewarm/load an object range.
    Load {
        /// The source object to load from.
        object: ObjectId,
        /// Byte offset to begin loading at.
        offset: u64,
        /// Number of bytes to load.
        len: u64,
    },
    /// Coordinator → all: the placement epoch advanced.
    EpochBump {
        /// The new epoch value.
        epoch: u64,
    },
    /// Client → coordinator: list the currently-known live nodes.
    ///
    /// A placement lookup returns owner [`NodeId`]s; the client resolves those
    /// ids to worker addresses with this query.
    MembershipQuery {},
    /// Coordinator → client: the current membership snapshot (id + address).
    MembershipList {
        /// All nodes the coordinator currently knows about.
        nodes: Vec<NodeInfo>,
    },
    /// Generic acknowledgement / error reply.
    Ack {
        /// True on success; false carries `detail`.
        ok: bool,
        /// Optional human-readable detail (error text).
        detail: Option<String>,
    },
}

/// The framed control envelope actually written to the wire.
///
/// Wraps a [`ControlMessage`] with the schema version so the receiver can
/// reject an incompatible schema before trusting the payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Envelope {
    schema: u16,
    message: ControlMessage,
}

/// Errors from control-message encode/decode.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The framing header was invalid.
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    /// A non-control frame was handed to the control codec.
    #[error("expected a Control frame, got {0:?}")]
    NotControl(MsgType),
    /// The header's declared length did not match the available payload bytes.
    #[error("length mismatch: header says {declared}, have {actual}")]
    LengthMismatch {
        /// Length advertised by the frame header.
        declared: usize,
        /// Bytes actually present after the header.
        actual: usize,
    },
    /// The message schema version is not understood.
    #[error("unsupported control schema {got} (this build speaks {ours})")]
    UnsupportedSchema {
        /// Schema version seen on the wire.
        got: u16,
        /// Schema version this build supports.
        ours: u16,
    },
    /// bincode failed to (de)serialize the message body.
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

/// Encode a control message into `header || bincode(envelope)`.
pub fn encode(request_id: u32, message: &ControlMessage) -> Result<Vec<u8>, CodecError> {
    let env = Envelope {
        schema: CONTROL_SCHEMA_VERSION,
        message: message.clone(),
    };
    let body = bincode::serialize(&env)?;
    let header = FrameHeader::new(MsgType::Control, request_id, body.len() as u32);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&body);
    Ok(buf)
}

/// Decode a full framed control buffer into its header and message.
///
/// Validates magic/version/type/length via [`FrameHeader::decode`], ensures the
/// frame is [`MsgType::Control`], checks the declared payload length against the
/// bytes present, and rejects an unknown schema version.
pub fn decode(buf: &[u8]) -> Result<(FrameHeader, ControlMessage), CodecError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::Control {
        return Err(CodecError::NotControl(header.msg_type));
    }
    let declared = header.length as usize;
    let body = &buf[HEADER_LEN..];
    if body.len() != declared {
        return Err(CodecError::LengthMismatch {
            declared,
            actual: body.len(),
        });
    }
    let env: Envelope = bincode::deserialize(body)?;
    if env.schema != CONTROL_SCHEMA_VERSION {
        return Err(CodecError::UnsupportedSchema {
            got: env.schema,
            ours: CONTROL_SCHEMA_VERSION,
        });
    }
    Ok((header, env.message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, NodeRole, Version};

    fn sample_messages() -> Vec<ControlMessage> {
        let node = NodeInfo {
            id: NodeId::new("worker-1"),
            address: "10.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let block = BlockId::new(
            ObjectId::new(Backend::S3, "bkt", "path/obj.bin"),
            256 << 20,
            256 << 20,
            Version::new("etag-xyz"),
        );
        vec![
            ControlMessage::Register { node: node.clone() },
            ControlMessage::Heartbeat {
                node: node.id.clone(),
                block_count: 42,
            },
            ControlMessage::PlacementLookup {
                block: block.clone(),
                k: 1,
            },
            ControlMessage::PlacementResponse {
                owners: vec![NodeId::new("a"), NodeId::new("b")],
                epoch: 7,
            },
            ControlMessage::Load {
                object: block.object.clone(),
                offset: 0,
                len: 1 << 20,
            },
            ControlMessage::EpochBump { epoch: 8 },
            ControlMessage::MembershipQuery {},
            ControlMessage::MembershipList {
                nodes: vec![node.clone()],
            },
            ControlMessage::Ack {
                ok: false,
                detail: Some("nope".into()),
            },
        ]
    }

    #[test]
    fn every_variant_round_trips() {
        for (i, msg) in sample_messages().into_iter().enumerate() {
            let buf = encode(i as u32, &msg).unwrap();
            let (header, back) = decode(&buf).unwrap();
            assert_eq!(header.msg_type, MsgType::Control);
            assert_eq!(header.request_id, i as u32);
            assert_eq!(header.length as usize, buf.len() - HEADER_LEN);
            assert_eq!(back, msg);
        }
    }

    #[test]
    fn non_control_frame_rejected() {
        // Hand-build a Get frame with a control-looking body.
        let body = bincode::serialize(&Envelope {
            schema: CONTROL_SCHEMA_VERSION,
            message: ControlMessage::EpochBump { epoch: 1 },
        })
        .unwrap();
        let mut buf = FrameHeader::new(MsgType::Get, 0, body.len() as u32)
            .encode()
            .to_vec();
        buf.extend_from_slice(&body);
        assert!(matches!(
            decode(&buf),
            Err(CodecError::NotControl(MsgType::Get))
        ));
    }

    #[test]
    fn truncated_body_rejected() {
        let mut buf = encode(1, &ControlMessage::EpochBump { epoch: 5 }).unwrap();
        buf.pop(); // drop a payload byte; header length now disagrees
        assert!(matches!(
            decode(&buf),
            Err(CodecError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn unknown_schema_rejected_not_panicked() {
        // Encode with a bumped schema and confirm decode reports it cleanly.
        let env = Envelope {
            schema: 999,
            message: ControlMessage::EpochBump { epoch: 1 },
        };
        let body = bincode::serialize(&env).unwrap();
        let mut buf = FrameHeader::new(MsgType::Control, 0, body.len() as u32)
            .encode()
            .to_vec();
        buf.extend_from_slice(&body);
        assert!(matches!(
            decode(&buf),
            Err(CodecError::UnsupportedSchema { got: 999, ours: 1 })
        ));
    }

    #[test]
    fn garbage_body_errors_gracefully() {
        let mut buf = FrameHeader::new(MsgType::Control, 0, 3).encode().to_vec();
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF]);
        assert!(decode(&buf).is_err());
    }
}
