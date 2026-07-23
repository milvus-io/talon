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
use talon_core::{
    BlockId, NodeId, NodeInfo, NodeStatus, NodeStatusError, ObjectId, MAX_NODE_STATUS_BYTES,
};

use crate::frame::{FrameError, FrameHeader, MsgType, HEADER_LEN};

/// Wire schema version for the control message set.
///
/// Bumped when [`ControlMessage`] changes in an incompatible way. Carried in
/// the envelope so a peer can reject a mismatched schema instead of
/// misinterpreting bytes.
pub const CONTROL_SCHEMA_VERSION: u16 = 2;

/// Oldest control schema this build can decode.
pub const MIN_CONTROL_SCHEMA_VERSION: u16 = 1;

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
    /// Node → coordinator: complete runtime status and metric snapshot.
    ///
    /// This is the first schema-v2 message. The legacy [`Heartbeat`](Self::Heartbeat)
    /// remains available during rolling upgrades.
    NodeStatusHeartbeat {
        /// Bounded, versioned status snapshot.
        status: Box<NodeStatus>,
    },
    /// Client → coordinator: what is this object's size and version?
    ///
    /// Backs FUSE `getattr`: the client needs the object length to report file
    /// size and the version/etag to address blocks. Schema v2. The coordinator
    /// answers from the backend `HEAD` (or its index).
    StatObject {
        /// The object to stat.
        object: ObjectId,
    },
    /// Coordinator → client: an object's size and version.
    ObjectStat {
        /// Total object length in bytes.
        size: u64,
        /// Current source version/etag of the object.
        version: String,
    },
}

impl ControlMessage {
    /// Oldest control schema that can represent this message.
    pub fn minimum_schema(&self) -> u16 {
        match self {
            Self::NodeStatusHeartbeat { .. } => 2,
            Self::StatObject { .. } | Self::ObjectStat { .. } => 2,
            _ => MIN_CONTROL_SCHEMA_VERSION,
        }
    }
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
    /// The selected schema predates the requested message.
    #[error("control message requires schema {required}, but schema {selected} was selected")]
    MessageRequiresSchema {
        /// Oldest schema that supports the message.
        required: u16,
        /// Schema selected by the caller or envelope.
        selected: u16,
    },
    /// A node status failed its bounded-field validation.
    #[error("invalid node status: {0}")]
    InvalidNodeStatus(#[from] NodeStatusError),
    /// A valid node status exceeded the encoded-value limit.
    #[error("encoded node status is {got} bytes; maximum is {max}")]
    NodeStatusTooLarge {
        /// Encoded status size.
        got: usize,
        /// Maximum encoded status size.
        max: usize,
    },
    /// bincode failed to (de)serialize the message body.
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

/// Encode a control message into `header || bincode(envelope)`.
pub fn encode(request_id: u32, message: &ControlMessage) -> Result<Vec<u8>, CodecError> {
    encode_for_schema(request_id, message, message.minimum_schema())
}

/// Encode with an explicitly selected supported schema.
///
/// Existing v1 messages can be forced to v1 or v2. A v2-only message returns
/// [`CodecError::MessageRequiresSchema`] when v1 is selected.
pub fn encode_for_schema(
    request_id: u32,
    message: &ControlMessage,
    schema: u16,
) -> Result<Vec<u8>, CodecError> {
    validate_schema(schema, CONTROL_SCHEMA_VERSION)?;
    validate_message(message, schema)?;
    let env = Envelope {
        schema,
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
    decode_with_max_schema(buf, CONTROL_SCHEMA_VERSION)
}

fn decode_with_max_schema(
    buf: &[u8],
    max_schema: u16,
) -> Result<(FrameHeader, ControlMessage), CodecError> {
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
    // The schema is the first field in the fixed-int bincode envelope. Check it
    // before deserializing the message so an older peer rejects a newer enum
    // shape cleanly rather than reporting a misleading bincode failure.
    let schema = peek_schema(body)?;
    validate_schema(schema, max_schema)?;
    let env: Envelope = bincode::deserialize(body)?;
    validate_message(&env.message, env.schema)?;
    Ok((header, env.message))
}

fn peek_schema(body: &[u8]) -> Result<u16, CodecError> {
    if body.len() < std::mem::size_of::<u16>() {
        // Preserve the established bincode error classification for a
        // truncated envelope.
        let _: Envelope = bincode::deserialize(body)?;
        unreachable!("deserializing a truncated envelope cannot succeed");
    }
    Ok(u16::from_le_bytes([body[0], body[1]]))
}

fn validate_schema(schema: u16, max_schema: u16) -> Result<(), CodecError> {
    if !(MIN_CONTROL_SCHEMA_VERSION..=max_schema).contains(&schema) {
        return Err(CodecError::UnsupportedSchema {
            got: schema,
            ours: max_schema,
        });
    }
    Ok(())
}

fn validate_message(message: &ControlMessage, schema: u16) -> Result<(), CodecError> {
    let required = message.minimum_schema();
    if schema < required {
        return Err(CodecError::MessageRequiresSchema {
            required,
            selected: schema,
        });
    }
    if let ControlMessage::NodeStatusHeartbeat { status } = message {
        status.validate()?;
        let got = bincode::serialized_size(status)? as usize;
        if got > MAX_NODE_STATUS_BYTES {
            return Err(CodecError::NodeStatusTooLarge {
                got,
                max: MAX_NODE_STATUS_BYTES,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use talon_core::{
        Backend, NodeHealth, NodeMetricsSnapshot, NodeRole, Version, NODE_STATUS_SCHEMA_VERSION,
    };

    fn sample_status(node: NodeInfo) -> NodeStatus {
        NodeStatus {
            schema_version: NODE_STATUS_SCHEMA_VERSION,
            cluster_id: "cluster-a".into(),
            node,
            incarnation_id: "incarnation-1".into(),
            admin_address: Some("10.0.0.1:8001".into()),
            build_version: "0.1.0".into(),
            started_at_unix_ms: 1_000,
            reported_at_unix_ms: 2_000,
            heartbeat_seq: 3,
            health: NodeHealth::Healthy,
            ready: true,
            metrics: NodeMetricsSnapshot {
                block_count: 42,
                resident_bytes: 1024,
                capacity_bytes: 4096,
                ..Default::default()
            },
            labels: BTreeMap::from([("zone".into(), "us-west-1a".into())]),
        }
    }

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
            ControlMessage::NodeStatusHeartbeat {
                status: Box::new(sample_status(node)),
            },
            ControlMessage::StatObject {
                object: block.object.clone(),
            },
            ControlMessage::ObjectStat {
                size: 2_500_000_000,
                version: "etag-xyz".into(),
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
    fn existing_messages_default_to_v1_during_rolling_upgrade() {
        let msg = ControlMessage::Heartbeat {
            node: NodeId::new("worker-1"),
            block_count: 42,
        };
        let buf = encode(1, &msg).unwrap();
        assert_eq!(peek_schema(&buf[HEADER_LEN..]).unwrap(), 1);
        assert_eq!(decode_with_max_schema(&buf, 1).unwrap().1, msg);
    }

    #[test]
    fn new_status_heartbeat_uses_v2_and_old_peer_rejects_cleanly() {
        let node = NodeInfo {
            id: NodeId::new("worker-1"),
            address: "10.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let msg = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(sample_status(node)),
        };
        let buf = encode(1, &msg).unwrap();
        assert_eq!(peek_schema(&buf[HEADER_LEN..]).unwrap(), 2);
        assert!(matches!(
            decode_with_max_schema(&buf, 1),
            Err(CodecError::UnsupportedSchema { got: 2, ours: 1 })
        ));
        assert_eq!(decode(&buf).unwrap().1, msg);
    }

    #[test]
    fn v2_message_cannot_be_mislabeled_as_v1() {
        let node = NodeInfo {
            id: NodeId::new("worker-1"),
            address: "10.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let msg = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(sample_status(node)),
        };
        assert!(matches!(
            encode_for_schema(1, &msg, 1),
            Err(CodecError::MessageRequiresSchema {
                required: 2,
                selected: 1
            })
        ));
    }

    #[test]
    fn stat_object_and_object_stat_are_v2() {
        let stat_req = ControlMessage::StatObject {
            object: ObjectId::new(Backend::S3, "bkt", "path/obj.bin"),
        };
        let stat_resp = ControlMessage::ObjectStat {
            size: 2_500_000_000,
            version: "etag-xyz".into(),
        };
        for msg in [stat_req, stat_resp] {
            let buf = encode(1, &msg).unwrap();
            // Encoded at schema v2 and round-trips.
            assert_eq!(peek_schema(&buf[HEADER_LEN..]).unwrap(), 2);
            assert_eq!(decode(&buf).unwrap().1, msg);
            // An old v1-only peer rejects it cleanly rather than misreading it.
            assert!(matches!(
                decode_with_max_schema(&buf, 1),
                Err(CodecError::UnsupportedSchema { got: 2, ours: 1 })
            ));
            // Forcing v1 on encode is refused.
            assert!(matches!(
                encode_for_schema(1, &msg, 1),
                Err(CodecError::MessageRequiresSchema {
                    required: 2,
                    selected: 1
                })
            ));
        }
    }

    #[test]
    fn malformed_node_status_is_rejected_on_encode_and_decode() {
        let node = NodeInfo {
            id: NodeId::new("worker-1"),
            address: "10.0.0.1:7001".into(),
            role: NodeRole::Worker,
        };
        let mut status = sample_status(node);
        status.cluster_id.clear();
        let msg = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(status),
        };
        assert!(matches!(
            encode(1, &msg),
            Err(CodecError::InvalidNodeStatus(NodeStatusError::EmptyField {
                field: "cluster_id"
            }))
        ));

        let body = bincode::serialize(&Envelope {
            schema: 2,
            message: msg,
        })
        .unwrap();
        let mut buf = FrameHeader::new(MsgType::Control, 1, body.len() as u32)
            .encode()
            .to_vec();
        buf.extend_from_slice(&body);
        assert!(matches!(
            decode(&buf),
            Err(CodecError::InvalidNodeStatus(_))
        ));
    }

    #[test]
    fn maximal_bounded_status_fits_the_wire_limit() {
        let node = NodeInfo {
            id: NodeId::new("n".repeat(talon_core::MAX_STATUS_FIELD_BYTES)),
            address: "a".repeat(talon_core::MAX_STATUS_FIELD_BYTES),
            role: NodeRole::Worker,
        };
        let mut status = sample_status(node);
        status.cluster_id = "c".repeat(talon_core::MAX_STATUS_FIELD_BYTES);
        status.incarnation_id = "i".repeat(talon_core::MAX_STATUS_FIELD_BYTES);
        status.admin_address = Some("m".repeat(talon_core::MAX_STATUS_FIELD_BYTES));
        status.build_version = "v".repeat(talon_core::MAX_STATUS_FIELD_BYTES);
        status.labels = (0..talon_core::MAX_STATUS_LABELS)
            .map(|i| {
                (
                    format!(
                        "{i:02}{}",
                        "k".repeat(talon_core::MAX_STATUS_LABEL_KEY_BYTES - 2)
                    ),
                    "x".repeat(talon_core::MAX_STATUS_LABEL_VALUE_BYTES),
                )
            })
            .collect();

        status.validate().unwrap();
        let encoded_size = bincode::serialized_size(&status).unwrap() as usize;
        assert!(
            encoded_size <= MAX_NODE_STATUS_BYTES,
            "{encoded_size} exceeds {MAX_NODE_STATUS_BYTES}"
        );
        let msg = ControlMessage::NodeStatusHeartbeat {
            status: Box::new(status),
        };
        assert_eq!(decode(&encode(1, &msg).unwrap()).unwrap().1, msg);
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
            Err(CodecError::UnsupportedSchema { got: 999, ours: 2 })
        ));
    }

    #[test]
    fn garbage_body_errors_gracefully() {
        let mut buf = FrameHeader::new(MsgType::Control, 0, 3).encode().to_vec();
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF]);
        assert!(decode(&buf).is_err());
    }
}
