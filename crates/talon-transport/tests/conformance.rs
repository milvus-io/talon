//! The wire format is a contract, and this test is what enforces it (#311).
//!
//! `conformance_vectors.json` holds byte-exact encodings of known messages.
//! Non-Rust clients assert against that file, so it is only useful if it is
//! guaranteed to describe what this code actually emits.
//!
//! Hence this test: it regenerates the vectors and compares them to the
//! committed file. A change that alters the wire — adding a field to a
//! `ControlMessage` variant, reordering variants, widening an integer — would
//! otherwise compile, pass every other test, and silently break every client
//! that is not Rust. Here it fails with a visible diff.
//!
//! If this test fails and the wire change was intentional:
//!
//! 1. run `just gen-conformance-vectors` to refresh the file,
//! 2. bump `CONTROL_SCHEMA_VERSION` if the change is not backward compatible,
//! 3. update the other client implementations,
//! 4. commit the regenerated vectors in the same change, so the diff is
//!    reviewable.

use std::process::Command;

const COMMITTED: &str = include_str!("conformance_vectors.json");

/// Regenerate the vectors with the current code and compare to the committed
/// file.
#[test]
fn committed_vectors_match_current_encoding() {
    let output = Command::new(env!("CARGO"))
        .args([
            "run",
            "--quiet",
            "-p",
            "talon-transport",
            "--bin",
            "talon-gen-conformance-vectors",
        ])
        .current_dir(workspace_root())
        .output()
        .expect("run the vector generator");

    assert!(
        output.status.success(),
        "generator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let generated = String::from_utf8(output.stdout).expect("generator emitted valid UTF-8");

    if generated.trim() != COMMITTED.trim() {
        let (first_committed, first_generated) = first_difference(COMMITTED, &generated);
        panic!(
            "wire format changed: the committed conformance vectors no longer match \
             what this code produces.\n\n  committed: {first_committed}\n  generated: {first_generated}\n\n\
             If this was intentional, run `just gen-conformance-vectors`, bump \
             CONTROL_SCHEMA_VERSION if the change is not backward compatible, update the \
             other client implementations, and commit the regenerated file in the same change."
        );
    }
}

/// The first differing line from each side, so the failure names the message
/// that changed rather than dumping the whole file.
fn first_difference(committed: &str, generated: &str) -> (String, String) {
    for (a, b) in committed.lines().zip(generated.lines()) {
        if a != b {
            return (a.trim().to_string(), b.trim().to_string());
        }
    }
    (
        format!("{} lines", committed.lines().count()),
        format!("{} lines", generated.lines().count()),
    )
}

fn workspace_root() -> std::path::PathBuf {
    // tests/ -> talon-transport/ -> crates/ -> workspace root
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

/// The vectors are only meaningful if the Rust client agrees with them, so
/// decode a representative sample back and check the round trip.
#[test]
fn vectors_decode_back_to_the_expected_messages() {
    use talon_transport::codec::{self, ControlMessage};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};

    let doc: serde_json::Value = serde_json::from_str(COMMITTED).expect("vectors parse as JSON");
    let vectors = doc["vectors"].as_array().expect("vectors array");

    let find = |name: &str| -> Vec<u8> {
        let hex = vectors
            .iter()
            .find(|v| v["name"] == name)
            .unwrap_or_else(|| panic!("vector {name} missing"))["hex"]
            .as_str()
            .expect("hex string");
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
            .collect()
    };

    // A control message decodes to the values the generator encoded.
    let bytes = find("control.object_stat.large_size");
    let (header, msg) = codec::decode(&bytes).expect("decode ObjectStat");
    assert_eq!(header.request_id, 3);
    match msg {
        ControlMessage::ObjectStat { size, version } => {
            // Above 2^32: a decoder reading u32 would truncate this.
            assert_eq!(size, 5_000_000_000);
            assert_eq!(version, "0x8DABCDEF");
        }
        other => panic!("expected ObjectStat, got {other:?}"),
    }

    // Multi-byte UTF-8 survives the round trip with byte-counted lengths.
    let bytes = find("control.object_list.utf8");
    let (_, msg) = codec::decode(&bytes).expect("decode ObjectList");
    match msg {
        ControlMessage::ObjectList { entries } => {
            assert_eq!(entries.len(), 2);
            assert!(entries[0].path.contains('数'));
            assert_eq!(entries[1].size, 0);
        }
        other => panic!("expected ObjectList, got {other:?}"),
    }

    // A bare frame header parses without a payload.
    let bytes = find("frame_header.zero_length");
    let header = FrameHeader::decode(&bytes[..HEADER_LEN]).expect("decode header");
    assert_eq!(header.length, 0);

    // The error response carries its message as the body and sets the flag.
    let bytes = find("data.error_response");
    let header = FrameHeader::decode(&bytes[..HEADER_LEN]).expect("decode header");
    assert!(header.flags.contains(talon_transport::Flags::ERROR));
    assert_eq!(
        String::from_utf8_lossy(&bytes[HEADER_LEN..]),
        "worker is not ready"
    );
}
