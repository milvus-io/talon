//! Shared input-schema vectors, separate from byte-exact protocol conformance.
//! Per-codec expectations preserve existing URI, nesting and encoded-size rules.

use std::collections::HashSet;

use serde::Deserialize;
use talon_core::{BrokerConfig, ValidatedBrokerConfig, MAX_CREDENTIAL_BROKER_BYTES};

const CORPUS: &str = include_str!("../../../test/sdk/credential_broker_validity.jsonl");
const PRIVATE_MARKER: &str = "PRIVATE_FIXTURE_MARKER";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Vector {
    id: String,
    java_valid: bool,
    rust_valid: bool,
    compatibility: String,
    utf8_hex: String,
}

#[test]
fn shared_descriptor_validity_and_redaction() {
    let mut ids = HashSet::new();
    let mut edges = 0;
    for (index, line) in CORPUS.lines().enumerate() {
        let vector: Vector = serde_json::from_str(line)
            .unwrap_or_else(|_| panic!("invalid corpus record at line {}", index + 1));
        let id = &vector.id;
        assert!(
            !id.is_empty()
                && id.bytes().all(|ch| ch.is_ascii_lowercase()
                    || ch.is_ascii_digit()
                    || b"_.-".contains(&ch)),
            "invalid corpus id at line {}",
            index + 1
        );
        assert!(ids.insert(id.clone()), "duplicate corpus id {id}");
        assert!(
            matches!(
                vector.compatibility.as_str(),
                "portable"
                    | "java_uri"
                    | "java_nesting_64"
                    | "rust_canonical_output_size"
                    | "rust_struct_sequence"
            ),
            "unknown compatibility label for {id}"
        );
        assert_eq!(
            vector.compatibility == "portable",
            vector.java_valid == vector.rust_valid,
            "unlabeled codec expectation for {id}"
        );
        edges += usize::from(vector.compatibility != "portable");
        let hex = &vector.utf8_hex;
        assert!(
            hex.len().is_multiple_of(2)
                && hex
                    .bytes()
                    .all(|ch| ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch)),
            "invalid corpus hex for {id}"
        );
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|offset| u8::from_str_radix(&hex[offset..offset + 2], 16).unwrap())
            .collect();
        let json =
            String::from_utf8(bytes).unwrap_or_else(|_| panic!("invalid corpus UTF-8 for {id}"));
        let raw = BrokerConfig::from_json(&json);
        let validated = ValidatedBrokerConfig::from_json(&json);
        assert_eq!(
            raw.is_ok(),
            vector.rust_valid,
            "raw descriptor validity for {id}"
        );
        assert_eq!(
            validated.is_ok(),
            vector.rust_valid,
            "validated descriptor validity for {id}"
        );
        match (raw, validated) {
            (Ok(raw), Ok(validated)) => {
                assert!(
                    raw == *validated.config(),
                    "raw and validated values differ for {id}"
                );
                assert!(validated.canonical_json().len() <= MAX_CREDENTIAL_BROKER_BYTES);
                assert!(
                    !format!("{raw:?}").contains(PRIVATE_MARKER),
                    "raw Debug leaked for {id}"
                );
                assert!(
                    !format!("{validated:?}").contains(PRIVATE_MARKER),
                    "validated Debug leaked for {id}"
                );
                if let Some(origin) = &raw.origin {
                    assert!(
                        !format!("{origin:?}").contains(PRIVATE_MARKER),
                        "origin Debug leaked for {id}"
                    );
                }
            }
            (Err(raw), Err(validated)) => {
                assert!(!raw.contains(PRIVATE_MARKER), "raw error leaked for {id}");
                assert!(
                    !validated.contains(PRIVATE_MARKER),
                    "validated error leaked for {id}"
                );
            }
            _ => unreachable!("validity disagreement for {id}"),
        }
    }
    assert!(!ids.is_empty(), "empty corpus");
    assert!(edges > 0, "missing codec edges");
}
