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
pub const CONTROL_SCHEMA_VERSION: u16 = 6;

/// Oldest control schema this build can decode.
pub const MIN_CONTROL_SCHEMA_VERSION: u16 = CONTROL_SCHEMA_VERSION;

/// A single control-plane message.
///
/// Membership, placement and load use one versioned contract.
/// `#[non_exhaustive]` allows consumers to reject unhandled message variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ControlMessage {
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
    /// Client → coordinator: persistent logical members and their live instances.
    MembershipQuery {},
    /// Coordinator → client: topology, availability, zones and discovery freshness.
    MembershipList {
        view: talon_core::worker_membership::WorkerDiscovery,
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
    /// `ready` reports local readiness, before coordinator service admission.
    NodeStatusHeartbeat {
        /// Bounded, versioned status snapshot.
        status: Box<NodeStatus>,
    },
    /// Client → coordinator: what is this object's size and version?
    ///
    /// Backs FUSE `getattr`: the client needs the object length to report file
    /// size and the version/etag to address blocks. The coordinator
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
    /// Client → coordinator: list objects under a mount-relative prefix.
    ///
    /// Backs FUSE `readdir`: the client asks for the objects beneath a
    /// directory prefix (e.g. `s3/bucket/dir`) and synthesizes the namespace
    /// tree from the returned paths.
    ListObjects {
        /// Mount-relative prefix to list under (may be empty for the root).
        prefix: String,
    },
    /// Coordinator → client: object entries matching a `ListObjects` prefix.
    ObjectList {
        /// Matching objects as `(mount-relative path, size in bytes)` pairs.
        entries: Vec<ObjectEntry>,
    },
    /// Client → coordinator: current mapping revision for a namespace.
    ///
    /// A client refreshes through this after a [`ControlMessage::StaleMapping`]
    /// rather than polling, so a fenced operation costs one extra round trip
    /// rather than a retry loop.
    MappingRevisionQuery {
        /// Namespace whose revision is requested.
        namespace: String,
    },
    /// Coordinator → client: the namespace's current mapping revision.
    MappingRevisionValue {
        /// Namespace the revision belongs to.
        namespace: String,
        /// Current revision. Zero means the namespace has never had a hard-link
        /// transition, which needs no stored record (ADR 0003 §3).
        revision: u64,
    },
    /// Worker → client: the request carried a revision that is not current.
    ///
    /// ADR 0003 §5: "Stale clients receive `STALE_MAPPING`, refresh through any
    /// coordinator, and retry."
    ///
    /// Carries the revision the worker holds so the client can decide what to
    /// do without a second query: a *lower* value means the worker is behind and
    /// the client should retry elsewhere or wait, while a *higher* one means the
    /// client's cache is stale and must be refreshed.
    StaleMapping {
        /// Namespace the fence applies to.
        namespace: String,
        /// Revision the worker currently holds.
        current: u64,
    },
    /// Coordinator → worker: refresh a namespace's authoritative mapping revision.
    MappingRevisionUpdate {
        /// Logical cluster containing both workloads.
        cluster_id: String,
        /// Canonical object-store namespace.
        namespace: String,
        /// Authoritative TMS mapping revision.
        revision: u64,
        /// Stable coordinator identity bound to its URI SAN.
        coordinator_id: String,
        /// Current coordinator process incarnation.
        coordinator_incarnation: String,
    },
    /// Worker → coordinator: actual locally-held revision after an update.
    MappingRevisionAck {
        /// Logical cluster containing both workloads.
        cluster_id: String,
        /// Canonical object-store namespace.
        namespace: String,
        /// Worker's actual held revision, which may exceed the update.
        revision: u64,
        /// Stable worker identity bound to its URI SAN.
        worker_id: String,
        /// Current worker process incarnation.
        worker_incarnation: String,
    },
    /// Coordinator → worker: status acceptance and permission to serve.
    NodeStatusAck {
        accepted: bool,
        serving: bool,
        detail: Option<String>,
    },
    ControlFailure {
        code: crate::DataErrorCode,
        message: String,
    },
    /// Client → worker: warm exactly one assigned block.
    LoadBlock {
        /// Complete versioned block identity.
        block: BlockId,
        /// Logical bytes in the block (the last block may be short).
        len: u64,
    },
    /// Client → worker: execute several block loads in one exchange.
    BatchLoad {
        /// Ordered assignments; acknowledged together after all complete.
        blocks: Vec<LoadBlockRequest>,
    },
    /// Worker → client: all assignments were attempted; omitted indices succeeded.
    BatchLoadResult {
        /// Failures ordered by the zero-based assignment index in the request.
        failures: Vec<LoadBlockFailure>,
    },
}

/// Maximum assignments in one batch LOAD frame.
pub const MAX_BATCH_LOAD_BLOCKS: usize = 1024;
/// Maximum encoded batch body, leaving room under the control-frame cap for tracing.
pub const MAX_BATCH_LOAD_BYTES: u64 =
    (crate::MAX_CONTROL_PAYLOAD_LEN - crate::envelope::ENVELOPE_OVERHEAD) as u64;
/// Bincode schema (u16), enum tag (u32), and vector length (u64).
pub const BATCH_LOAD_BODY_OVERHEAD: u64 = 14;

/// Maximum UTF-8 bytes in one batch failure diagnostic.
pub const MAX_LOAD_ERROR_BYTES: usize = 256;

/// A failed assignment in a batch reply. File identities remain in the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadBlockFailure {
    /// Zero-based assignment index, not a file index.
    pub index: u32,
    /// Bounded diagnostic; clients must not classify errors by its text.
    pub error: String,
}

impl LoadBlockFailure {
    /// Build a bounded diagnostic without splitting a UTF-8 code point.
    pub fn new(index: u32, mut error: String) -> Self {
        let mut end = error.len().min(MAX_LOAD_ERROR_BYTES);
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        error.truncate(end);
        Self { index, error }
    }
}

/// One version-pinned block assignment within a batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadBlockRequest {
    /// Complete versioned block identity.
    pub block: BlockId,
    /// Logical block length, including a possibly short final block.
    pub len: u64,
}

impl LoadBlockRequest {
    /// Exact encoded size used to pack bounded batch frames.
    pub fn encoded_len(&self) -> Result<u64, CodecError> {
        Ok(bincode::serialized_size(self)?)
    }
}

/// One object listing entry: its mount-relative path and byte size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectEntry {
    /// Mount-relative object path, e.g. `s3/bucket/dir/file.bin`.
    pub path: String,
    /// Object size in bytes.
    pub size: u64,
}

/// A membership entry paired with its deployment zone.
///
/// Used by placement consumers projecting the instance-aware discovery view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZonedNodeInfo {
    /// The unchanged v1 node identity.
    pub info: NodeInfo,
    /// Deployment zone reported by the node, when known.
    pub zone: Option<String>,
}

impl ControlMessage {
    /// Schema for the single supported control contract.
    pub fn minimum_schema(&self) -> u16 {
        CONTROL_SCHEMA_VERSION
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
    /// Batch LOAD exceeds its count or byte limit, or contains no assignments.
    #[error("invalid batch load: {0}")]
    InvalidBatchLoad(String),
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
/// Incompatible schemas are rejected before serializing any message.
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
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len() + crate::envelope::outbound_reserve());
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

/// Decode a request; v2 responses deliberately use the original decode API.
pub fn decode_request(buf: &[u8]) -> Result<(FrameHeader, ControlMessage), CodecError> {
    let header = FrameHeader::decode(buf)?;
    if header.msg_type != MsgType::Control {
        return Err(CodecError::NotControl(header.msg_type));
    }
    if header.version == 1 {
        return decode(buf);
    }
    let (_, business) = crate::envelope::decode(&header, &buf[HEADER_LEN..])?;
    decode_business(header, business, CONTROL_SCHEMA_VERSION)
}

/// Read the schema after the caller has validated a request with `decode_request`.
pub fn request_schema(header: &FrameHeader, payload: &[u8]) -> Result<u16, CodecError> {
    let (_, business) = crate::envelope::decode(header, payload)?;
    peek_schema(business)
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
    decode_business(header, body, max_schema)
}

fn decode_business(
    header: FrameHeader,
    body: &[u8],
    max_schema: u16,
) -> Result<(FrameHeader, ControlMessage), CodecError> {
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
    if let ControlMessage::BatchLoad { blocks } = message {
        if blocks.is_empty() || blocks.len() > MAX_BATCH_LOAD_BLOCKS {
            return Err(CodecError::InvalidBatchLoad(format!(
                "expected 1..={MAX_BATCH_LOAD_BLOCKS} assignments"
            )));
        }
        if 2 + bincode::serialized_size(message)? > MAX_BATCH_LOAD_BYTES {
            return Err(CodecError::InvalidBatchLoad(
                "encoded body exceeds control-frame limit".into(),
            ));
        }
    }
    if let ControlMessage::BatchLoadResult { failures } = message {
        if failures.len() > MAX_BATCH_LOAD_BLOCKS
            || failures.iter().any(|f| {
                f.index as usize >= MAX_BATCH_LOAD_BLOCKS || f.error.len() > MAX_LOAD_ERROR_BYTES
            })
            || failures
                .windows(2)
                .any(|pair| pair[0].index >= pair[1].index)
        {
            return Err(CodecError::InvalidBatchLoad(
                "invalid batch failure list".into(),
            ));
        }
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
                view: talon_core::worker_membership::WorkerDiscovery::retained(
                    &Default::default(),
                    &[],
                ),
            },
            ControlMessage::Ack {
                ok: false,
                detail: Some("nope".into()),
            },
            ControlMessage::NodeStatusHeartbeat {
                status: Box::new(sample_status(node)),
            },
            ControlMessage::NodeStatusAck {
                accepted: true,
                serving: false,
                detail: None,
            },
            ControlMessage::MappingRevisionQuery {
                namespace: "ns".into(),
            },
            ControlMessage::MappingRevisionValue {
                namespace: "ns".into(),
                revision: 1,
            },
            ControlMessage::StaleMapping {
                namespace: "ns".into(),
                current: 1,
            },
            ControlMessage::StatObject {
                object: block.object.clone(),
            },
            ControlMessage::ObjectStat {
                size: 2_500_000_000,
                version: "etag-xyz".into(),
            },
            ControlMessage::ListObjects {
                prefix: "s3/bkt/dir".into(),
            },
            ControlMessage::ObjectList {
                entries: vec![
                    ObjectEntry {
                        path: "s3/bkt/dir/a.bin".into(),
                        size: 10,
                    },
                    ObjectEntry {
                        path: "s3/bkt/dir/b.bin".into(),
                        size: 20,
                    },
                ],
            },
            ControlMessage::MappingRevisionUpdate {
                cluster_id: "cluster-a".into(),
                namespace: "s3/bkt/models".into(),
                revision: 7,
                coordinator_id: "coordinator-1".into(),
                coordinator_incarnation: "coord-inc-1".into(),
            },
            ControlMessage::MappingRevisionAck {
                cluster_id: "cluster-a".into(),
                namespace: "s3/bkt/models".into(),
                revision: 7,
                worker_id: "worker-1".into(),
                worker_incarnation: "worker-inc-1".into(),
            },
        ]
    }

    #[test]
    fn incompatible_schemas_are_rejected_before_message_decoding() {
        for message in sample_messages() {
            let frame = encode(1, &message).unwrap();
            assert_eq!(
                peek_schema(&frame[HEADER_LEN..]).unwrap(),
                CONTROL_SCHEMA_VERSION
            );
            assert!(decode_with_max_schema(&frame, 5).is_err());
            for schema in [0, 1, 2, 3, 4, 5, 7] {
                assert!(matches!(
                    encode_for_schema(1, &message, schema),
                    Err(CodecError::UnsupportedSchema { .. })
                ));
                let mut incompatible = frame.clone();
                incompatible[HEADER_LEN..HEADER_LEN + 2].copy_from_slice(&schema.to_le_bytes());
                // Even invalid enum bytes must not be interpreted under an old schema.
                incompatible[HEADER_LEN + 2..HEADER_LEN + 6].fill(255);
                assert!(
                    matches!(decode(&incompatible), Err(CodecError::UnsupportedSchema { got, .. }) if got == schema)
                );
            }
        }
    }

    #[test]
    fn batch_failure_reply_is_bounded_ordered_and_preserves_utf8() {
        let failure = LoadBlockFailure::new(0, "错".repeat(200));
        assert!(failure.error.len() <= MAX_LOAD_ERROR_BYTES);
        let failures = (0..MAX_BATCH_LOAD_BLOCKS)
            .map(|index| LoadBlockFailure::new(index as u32, "x".repeat(256)))
            .collect();
        let reply = ControlMessage::BatchLoadResult { failures };
        let frame = encode(1, &reply).unwrap();
        assert!(frame.len() < MAX_BATCH_LOAD_BYTES as usize);
        assert_eq!(decode(&frame).unwrap().1, reply);
        for failures in [
            vec![failure.clone(), failure],
            vec![LoadBlockFailure::new(1024, "bad index".into())],
            vec![LoadBlockFailure {
                index: 0,
                error: "x".repeat(257),
            }],
        ] {
            let message = ControlMessage::BatchLoadResult { failures };
            assert!(encode(1, &message).is_err());
            let body = bincode::serialize(&Envelope {
                schema: CONTROL_SCHEMA_VERSION,
                message,
            })
            .unwrap();
            let mut raw = FrameHeader::new(MsgType::Control, 1, body.len() as u32)
                .encode()
                .to_vec();
            raw.extend(body);
            assert!(decode(&raw).is_err());
        }
    }

    #[test]
    fn batch_load_round_trip_and_bounds_apply_on_encode_and_decode() {
        let request = LoadBlockRequest {
            block: BlockId::new(
                talon_core::ObjectId::new(talon_core::Backend::S3, "bucket", "key"),
                0,
                8,
                talon_core::Version::new("v1"),
            ),
            len: 8,
        };
        let valid = ControlMessage::BatchLoad {
            blocks: vec![request.clone(); MAX_BATCH_LOAD_BLOCKS],
        };
        let encoded = encode(42, &valid).unwrap();
        assert_eq!(decode(&encoded).unwrap().1, valid);
        assert_eq!(
            (encoded.len() - HEADER_LEN) as u64,
            BATCH_LOAD_BODY_OVERHEAD
                + MAX_BATCH_LOAD_BLOCKS as u64 * request.encoded_len().unwrap()
        );
        assert!(matches!(
            decode_with_max_schema(&encoded, 5),
            Err(CodecError::UnsupportedSchema { .. })
        ));
        let mut huge = request.clone();
        huge.block.version = talon_core::Version::new("x".repeat(MAX_BATCH_LOAD_BYTES as usize));
        for blocks in [vec![], vec![request; MAX_BATCH_LOAD_BLOCKS + 1], vec![huge]] {
            let message = ControlMessage::BatchLoad { blocks };
            assert!(matches!(
                encode(1, &message),
                Err(CodecError::InvalidBatchLoad(_))
            ));
            let body = bincode::serialize(&Envelope { schema: 6, message }).unwrap();
            let mut frame = FrameHeader::new(MsgType::Control, 1, body.len() as u32)
                .encode()
                .to_vec();
            frame.extend_from_slice(&body);
            assert!(matches!(
                decode(&frame),
                Err(CodecError::InvalidBatchLoad(_))
            ));
        }
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
            schema: CONTROL_SCHEMA_VERSION,
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
        // `ours` reads the constant rather than a literal so a schema bump does
        // not require editing this assertion -- the property under test is that
        // an unknown schema is reported, not which version we happen to be on.
        assert!(matches!(
            decode(&buf),
            Err(CodecError::UnsupportedSchema {
                got: 999,
                ours
            }) if ours == CONTROL_SCHEMA_VERSION
        ));
    }

    #[test]
    fn garbage_body_errors_gracefully() {
        let mut buf = FrameHeader::new(MsgType::Control, 0, 3).encode().to_vec();
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF]);
        assert!(decode(&buf).is_err());
    }
    #[test]
    fn membership_discovery_uses_the_current_schema() {
        use talon_core::worker_membership::*;
        let view = WorkerDiscovery::retained(
            &MemberRegistry {
                members: vec![WorkerMember {
                    worker_id: "offline".into(),
                    zone: None,
                    retired: false,
                }],
                ..Default::default()
            },
            &[],
        );
        let message = ControlMessage::MembershipList { view };
        assert_eq!(message.minimum_schema(), 6);
        let encoded = encode(7, &message).unwrap();
        assert_eq!(decode(&encoded).unwrap().1, message);
        assert_eq!(ControlMessage::MembershipQuery {}.minimum_schema(), 6);
    }
    #[tokio::test]
    async fn largest_admitted_discovery_with_maximum_instances_fits_reader_cap() {
        use talon_core::worker_membership::{MemberRegistry, WorkerDiscovery, WorkerMember};
        let mut registry = MemberRegistry {
            ..Default::default()
        };
        loop {
            registry.members.push(WorkerMember {
                worker_id: format!("w{:05}", registry.members.len()),
                zone: Some(String::new()),
                retired: false,
            });
            if registry.validate().is_err() {
                registry.members.pop();
                break;
            }
        }
        registry.validate().unwrap();
        // This population is also below the persistent JSON resource limit;
        // the discovery budget, rather than JSON capacity, bounds it.
        assert!(serde_json::to_vec(&registry).unwrap().len() < 512 * 1024);
        let instances: Vec<_> = registry
            .members
            .iter()
            .map(|member| {
                let mut status = sample_status(NodeInfo {
                    id: NodeId::new(member.worker_id.clone()),
                    address: "a".repeat(talon_core::MAX_STATUS_FIELD_BYTES),
                    role: NodeRole::Worker,
                });
                status.incarnation_id = "i".repeat(talon_core::MAX_STATUS_FIELD_BYTES);
                status.validate().unwrap();
                status
            })
            .collect();
        let view = WorkerDiscovery::retained(&registry, &instances);
        let message = ControlMessage::MembershipList { view };
        let encoded = encode(7, &message).unwrap();
        let payload_bytes = encoded.len() - HEADER_LEN;
        assert!(payload_bytes <= crate::MAX_CONTROL_PAYLOAD_LEN as usize);
        assert!(payload_bytes > crate::MAX_CONTROL_PAYLOAD_LEN as usize - 2_000);
        // Exercise the actual capped stream reader, not merely bincode decode.
        let (_, payload) =
            crate::read_frame(&mut encoded.as_slice(), std::time::Duration::from_secs(2))
                .await
                .unwrap();
        assert_eq!(payload.len(), payload_bytes);
        assert_eq!(decode(&encoded).unwrap().1, message);
    }
}
