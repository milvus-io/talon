//! Generate wire-protocol conformance vectors (#311).
//!
//! Emits byte-exact encodings of known messages so a non-Rust client can assert
//! against the real implementation instead of against a prose description that
//! may have drifted from it.
//!
//! The vectors are **generated, never hand-written**: a test asserts the
//! committed file matches what the current code produces, so a change that
//! alters the wire fails that test with a visible diff. That is the point —
//! without it, adding a field to a `ControlMessage` variant would compile,
//! pass every Rust test, and silently break every other client.
//!
//! ```sh
//! cargo run -p talon-transport --bin talon-gen-conformance-vectors -- \
//!     crates/talon-transport/tests/conformance_vectors.json
//! ```

use talon_core::{Backend, BlockId, NodeId, NodeInfo, NodeRole, ObjectId, Version};
use talon_transport::codec::{self, ControlMessage, ObjectEntry};
use talon_transport::data::{self, RangeRequest};
use talon_transport::frame::{FrameHeader, MsgType};

/// One named vector: a message, the bytes it encodes to, and why it is here.
struct Vector {
    name: &'static str,
    note: &'static str,
    bytes: Vec<u8>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn control(
    name: &'static str,
    note: &'static str,
    request_id: u32,
    msg: &ControlMessage,
) -> Vector {
    Vector {
        name,
        note,
        bytes: codec::encode(request_id, msg).expect("encode control message"),
    }
}

fn object(bucket: &str, path: &str) -> ObjectId {
    ObjectId::new(Backend::Azure, bucket, path)
}

fn vectors() -> Vec<Vector> {
    vec![
        // --- Frame header on its own ---------------------------------------
        Vector {
            name: "frame_header.get_range",
            note: "16-byte header alone: magic 'TL', version 1, type GetRange, request id 7, length 42",
            bytes: FrameHeader::new(MsgType::GetRange, 7, 42).encode().to_vec(),
        },
        Vector {
            name: "frame_header.zero_length",
            note: "A zero-length payload is legal; decoders must not treat it as EOF",
            bytes: FrameHeader::new(MsgType::Ping, 0, 0).encode().to_vec(),
        },
        // --- Control plane: the read path ----------------------------------
        control(
            "control.placement_lookup",
            "Client to coordinator: which workers hold this block",
            1,
            &ControlMessage::PlacementLookup {
                block: BlockId::new(
                    object("container", "path/to/object"),
                    268_435_456,
                    256 << 20,
                    Version::new("v1"),
                ),
                k: 1,
            },
        ),
        control(
            "control.async_placement_lookup",
            "Schema 4: the async worker ring. Same body shape as placement_lookup, different variant tag; only the object is hashed, so offset and version do not affect the answer",
            1,
            &ControlMessage::AsyncPlacementLookup {
                block: BlockId::new(
                    object("container", "path/to/object.parquet"),
                    268_435_456,
                    256 << 20,
                    Version::new("v1"),
                ),
                k: 1,
            },
        ),
        control(
            "control.membership_list.async_worker",
            "NodeRole discriminant 2: appended after Coordinator (0) and Worker (1), which must keep their tags",
            2,
            &ControlMessage::MembershipList {
                nodes: vec![NodeInfo {
                    id: NodeId::new("async-worker-a"),
                    address: "10.0.0.9:7001".into(),
                    role: NodeRole::AsyncWorker,
                }],
            },
        ),
        control(
            "control.placement_response",
            "Coordinator reply: ordered owners plus the epoch they were computed at",
            1,
            &ControlMessage::PlacementResponse {
                owners: vec![NodeId::new("worker-a"), NodeId::new("worker-b")],
                epoch: 42,
            },
        ),
        control(
            "control.placement_response.empty_owners",
            "No owners: an empty Vec is a u64 length prefix of zero, not an absent field",
            1,
            &ControlMessage::PlacementResponse {
                owners: vec![],
                epoch: 0,
            },
        ),
        control(
            "control.membership_query",
            "A unit-struct variant: the tag with no body",
            2,
            &ControlMessage::MembershipQuery {},
        ),
        control(
            "control.membership_list",
            "Node ids resolved to dialable addresses",
            2,
            &ControlMessage::MembershipList {
                nodes: vec![NodeInfo {
                    id: NodeId::new("worker-a"),
                    address: "10.0.0.1:7001".into(),
                    role: NodeRole::Worker,
                }],
            },
        ),
        control(
            "control.stat_object",
            "Backs getattr: object size and version",
            3,
            &ControlMessage::StatObject {
                object: object("container", "path/to/object"),
            },
        ),
        control(
            "control.object_stat.large_size",
            "A size above 2^32 - decoders that read u32 will silently truncate here",
            3,
            &ControlMessage::ObjectStat {
                size: 5_000_000_000,
                version: "0x8DABCDEF".into(),
            },
        ),
        control(
            "control.list_objects.empty_prefix",
            "An empty string is a u64 length prefix of zero followed by no bytes",
            4,
            &ControlMessage::ListObjects {
                prefix: String::new(),
            },
        ),
        control(
            "control.object_list.utf8",
            "Multi-byte UTF-8 in an object key: the length prefix counts bytes, not characters",
            4,
            &ControlMessage::ObjectList {
                entries: vec![
                    ObjectEntry {
                        path: "az/container/\u{6570}\u{636e}/\u{6587}\u{4ef6}.parquet".into(),
                        size: 1024,
                    },
                    ObjectEntry {
                        path: "az/container/empty".into(),
                        size: 0,
                    },
                ],
            },
        ),
        control(
            "control.mapping_revision_update",
            "Coordinator to worker: refresh the local namespace mapping fence",
            5,
            &ControlMessage::MappingRevisionUpdate {
                cluster_id: "cluster-a".into(),
                namespace: "s3/datasets/training".into(),
                revision: 7,
                coordinator_id: "coordinator-a".into(),
                coordinator_incarnation: "coordinator-incarnation-1".into(),
            },
        ),
        control(
            "control.mapping_revision_ack",
            "Worker to coordinator: acknowledge the actual held mapping revision",
            5,
            &ControlMessage::MappingRevisionAck {
                cluster_id: "cluster-a".into(),
                namespace: "s3/datasets/training".into(),
                revision: 7,
                worker_id: "worker-a".into(),
                worker_incarnation: "worker-incarnation-1".into(),
            },
        ),
        // --- Data plane -----------------------------------------------------
        Vector {
            name: "data.range_request",
            note: "Header plus a bincode RangeRequest body",
            bytes: data::encode_request(
                9,
                &RangeRequest {
                    object: object("container", "path/to/object"),
                    offset: 65536,
                    len: 4096,
                },
            )
            .expect("encode range request"),
        },
        Vector {
            name: "data.response_header_ok",
            note: "Success response header; the raw payload bytes follow unwrapped so the worker can sendfile them",
            bytes: data::response_header_ok(9, 4096).to_vec(),
        },
        Vector {
            name: "data.error_response",
            note: "ERROR flag set; the body is a UTF-8 message, not a payload",
            bytes: data::encode_error(9, "worker is not ready"),
        },
    ]
}

fn main() -> anyhow::Result<()> {
    let out = std::env::args().nth(1);
    let vectors = vectors();

    let mut s = String::new();
    s.push_str("{\n");
    s.push_str("  \"_comment\": \"Generated by talon-gen-conformance-vectors. Do not edit by hand; run `just gen-conformance-vectors`. A failing conformance test means the wire format changed.\",\n");
    s.push_str(&format!(
        "  \"control_schema_version\": {},\n",
        codec::CONTROL_SCHEMA_VERSION
    ));
    s.push_str(&format!(
        "  \"min_control_schema_version\": {},\n",
        codec::MIN_CONTROL_SCHEMA_VERSION
    ));
    s.push_str("  \"vectors\": [\n");
    for (i, vector) in vectors.iter().enumerate() {
        let comma = if i + 1 == vectors.len() { "" } else { "," };
        s.push_str(&format!(
            "    {{ \"name\": \"{}\", \"note\": \"{}\", \"hex\": \"{}\" }}{}\n",
            vector.name,
            vector.note,
            hex(&vector.bytes),
            comma
        ));
    }
    s.push_str("  ]\n}\n");

    match out {
        Some(path) => {
            std::fs::write(&path, &s)?;
            eprintln!("wrote {path} ({} vectors)", vectors.len());
        }
        None => print!("{s}"),
    }
    Ok(())
}
