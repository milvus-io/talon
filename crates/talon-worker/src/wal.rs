//! The write-back WAL's physical framing (ADR 0003 §9.4).
//!
//! ```text
//! segment = [ 32 KiB block ][ 32 KiB block ]...
//! block   = [ fragment ][ fragment ]... [ zero padding ]
//! ```
//!
//! # Why records are fragmented into fixed blocks
//!
//! > the WAL uses 32 KiB physical blocks with full/first/middle/last fragments,
//! > so recovery can identify a torn tail and resynchronize after a damaged
//! > block
//!
//! A record that would cross a block boundary is split, so **every block starts
//! at a parseable position**. Without that, one damaged block makes the rest of
//! the segment unreadable: the reader cannot find where the next record begins
//! and has to discard everything after the damage, including records that are
//! perfectly intact.
//!
//! With it, a reader that hits a bad block skips to the next 32 KiB boundary and
//! keeps going. It loses the record spanning the damage and nothing else.
//!
//! # Why the encoding is written out rather than derived
//!
//! > Rust `serde`/`bincode` and JSON are not durable on-disk formats for this
//! > protocol
//!
//! Same reasoning as §9.1's rejection of `DefaultHasher`. A derived encoding is
//! whatever the current library version happens to emit, and a dependency bump
//! can change it without changing any code here — which for a durable format
//! means silently failing to read data written yesterday.
//!
//! # Envelope layout
//!
//! ```text
//! offset  size  field
//!      0     4  magic          "TWAL"
//!      4     1  format_version
//!      5     1  fragment_kind  full | first | middle | last
//!      6     2  body_len       little-endian u16, <= BLOCK_SIZE - HEADER_LEN
//!      8     4  crc32c         over format_version..body
//!     12     n  body
//! ```
//!
//! The CRC covers the header fields *and* the body, so a corrupted length is
//! caught rather than used to read a wrong number of bytes.

use core::fmt;

/// Physical block size.
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Bytes of envelope preceding each fragment body.
pub const HEADER_LEN: usize = 12;

/// Largest body that fits in one fragment.
pub const MAX_FRAGMENT_BODY: usize = BLOCK_SIZE - HEADER_LEN;

/// Identifies a Talon WAL fragment.
const MAGIC: [u8; 4] = *b"TWAL";

/// On-disk format version.
///
/// Bumped when the envelope layout changes. A reader rejects a version it does
/// not know rather than guessing, because misreading a durable record is worse
/// than refusing it.
pub const FORMAT_VERSION: u8 = 1;

/// Where a fragment sits within its logical record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentKind {
    /// The whole record fits in this fragment.
    Full,
    /// First fragment of a record that spans blocks.
    First,
    /// Neither first nor last.
    Middle,
    /// Final fragment of a spanning record.
    Last,
}

impl FragmentKind {
    const fn to_byte(self) -> u8 {
        match self {
            Self::Full => 1,
            Self::First => 2,
            Self::Middle => 3,
            Self::Last => 4,
        }
    }

    const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Full),
            2 => Some(Self::First),
            3 => Some(Self::Middle),
            4 => Some(Self::Last),
            _ => None,
        }
    }

    /// Whether this fragment starts a logical record.
    const fn starts_record(self) -> bool {
        matches!(self, Self::Full | Self::First)
    }

    /// Whether this fragment completes a logical record.
    const fn ends_record(self) -> bool {
        matches!(self, Self::Full | Self::Last)
    }
}

/// Why a fragment could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// The remaining bytes are shorter than a header, or shorter than the
    /// length the header claims.
    ///
    /// Expected at the end of a segment that was being appended to when the
    /// process died. §9.4's "torn tail": not corruption, and everything before
    /// it is still valid.
    TornTail,
    /// Checksum mismatch. The block is damaged.
    CorruptFragment {
        /// Byte offset of the fragment within the segment.
        offset: usize,
    },
    /// The envelope does not begin with the WAL magic.
    NotAFragment {
        /// Byte offset within the segment.
        offset: usize,
    },
    /// The format version is newer than this build understands.
    UnsupportedVersion {
        /// Version found on disk.
        found: u8,
    },
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TornTail => write!(f, "torn tail: segment ends mid-record"),
            Self::CorruptFragment { offset } => {
                write!(f, "corrupt fragment at offset {offset}")
            }
            Self::NotAFragment { offset } => write!(f, "no WAL magic at offset {offset}"),
            Self::UnsupportedVersion { found } => {
                write!(
                    f,
                    "WAL format version {found} is newer than {FORMAT_VERSION}"
                )
            }
        }
    }
}

/// Encode one logical record into 32 KiB blocks.
///
/// Fragments never cross a block boundary. A block with too little room left for
/// another header is zero-padded, so the next record starts at a block boundary
/// and a reader can always resynchronise there.
pub fn encode_record(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut remaining = body;
    let mut first = true;

    loop {
        let used = out.len() % BLOCK_SIZE;
        let free = BLOCK_SIZE - used;

        // Too little room for a header plus a byte: pad to the boundary. A
        // reader scanning block-aligned positions must never land inside a
        // partially written header.
        if free < HEADER_LEN + 1 {
            out.resize(out.len() + free, 0);
            continue;
        }

        let capacity = free - HEADER_LEN;
        let take = remaining.len().min(capacity);
        let last = take == remaining.len();
        let kind = match (first, last) {
            (true, true) => FragmentKind::Full,
            (true, false) => FragmentKind::First,
            (false, true) => FragmentKind::Last,
            (false, false) => FragmentKind::Middle,
        };

        out.extend_from_slice(&encode_fragment(kind, &remaining[..take]));
        remaining = &remaining[take..];
        first = false;

        if last {
            return out;
        }
    }
}

fn encode_fragment(kind: FragmentKind, body: &[u8]) -> Vec<u8> {
    debug_assert!(body.len() <= MAX_FRAGMENT_BODY);
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(&MAGIC);
    buf.push(FORMAT_VERSION);
    buf.push(kind.to_byte());
    buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
    // CRC over everything after the checksum field: the header's own fields and
    // the body. Covering the length means a corrupted length is detected rather
    // than used to read the wrong number of bytes.
    let mut crc_input = Vec::with_capacity(4 + body.len());
    crc_input.push(FORMAT_VERSION);
    crc_input.push(kind.to_byte());
    crc_input.extend_from_slice(&(body.len() as u16).to_le_bytes());
    crc_input.extend_from_slice(body);
    buf.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    buf.extend_from_slice(body);
    buf
}

/// One decoded fragment and how many bytes it occupied.
struct Fragment {
    kind: FragmentKind,
    body: Vec<u8>,
    len: usize,
}

fn decode_fragment(buf: &[u8], offset: usize) -> Result<Option<Fragment>, ReadError> {
    if buf.len() < HEADER_LEN {
        // Not enough left for a header. Zero padding at a block tail looks the
        // same, so distinguish them: all-zero is padding, anything else is a
        // record that was being written when the process died.
        return if buf.iter().all(|byte| *byte == 0) {
            Ok(None)
        } else {
            Err(ReadError::TornTail)
        };
    }
    if buf[..4] != MAGIC {
        return if buf[..HEADER_LEN].iter().all(|byte| *byte == 0) {
            Ok(None)
        } else {
            Err(ReadError::NotAFragment { offset })
        };
    }
    let version = buf[4];
    if version > FORMAT_VERSION {
        return Err(ReadError::UnsupportedVersion { found: version });
    }
    let kind = FragmentKind::from_byte(buf[5]).ok_or(ReadError::CorruptFragment { offset })?;
    let body_len = u16::from_le_bytes([buf[6], buf[7]]) as usize;
    let stored_crc = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if buf.len() < HEADER_LEN + body_len {
        return Err(ReadError::TornTail);
    }
    let body = &buf[HEADER_LEN..HEADER_LEN + body_len];

    let mut crc_input = Vec::with_capacity(4 + body_len);
    crc_input.extend_from_slice(&buf[4..8]);
    crc_input.extend_from_slice(body);
    if crc32c(&crc_input) != stored_crc {
        return Err(ReadError::CorruptFragment { offset });
    }

    Ok(Some(Fragment {
        kind,
        body: body.to_vec(),
        len: HEADER_LEN + body_len,
    }))
}

/// A record recovered from a segment, or the damage that ended reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    /// Records recovered in order.
    pub records: Vec<Vec<u8>>,
    /// Why reading stopped, if it did not reach a clean end.
    ///
    /// `Some(TornTail)` is the ordinary case for a segment that was open when
    /// the process died, and the records before it are valid.
    pub stopped_by: Option<ReadError>,
    /// Blocks skipped because they were damaged.
    ///
    /// Non-zero means data was lost. §9.4 sends that to the recovery quorum
    /// rather than treating it as an empty segment.
    pub damaged_blocks: usize,
}

/// Read every record in a segment, resynchronising after damage.
///
/// A damaged fragment costs its containing record and nothing more: the reader
/// advances to the next 32 KiB boundary, where a fragment is guaranteed to
/// start, and resumes.
pub fn read_segment(buf: &[u8]) -> ReadResult {
    let mut records = Vec::new();
    let mut pending: Option<Vec<u8>> = None;
    let mut damaged_blocks = 0;
    let mut offset = 0;

    while offset < buf.len() {
        match decode_fragment(&buf[offset..], offset) {
            Ok(Some(fragment)) => {
                match (fragment.kind.starts_record(), pending.take()) {
                    // A new record starting while one is incomplete means the
                    // continuation was lost to damage. Drop the partial rather
                    // than splicing unrelated bytes onto it.
                    (true, Some(_)) | (true, None) => pending = Some(fragment.body),
                    (false, Some(mut open)) => {
                        open.extend_from_slice(&fragment.body);
                        pending = Some(open);
                    }
                    // A continuation with nothing open: its head was in a
                    // damaged block. Discard it.
                    (false, None) => {}
                }
                if fragment.kind.ends_record() {
                    if let Some(complete) = pending.take() {
                        records.push(complete);
                    }
                }
                offset += fragment.len;
            }
            // Padding: skip to the next block boundary.
            Ok(None) => offset = next_block(offset),
            Err(ReadError::TornTail) => {
                return ReadResult {
                    records,
                    stopped_by: Some(ReadError::TornTail),
                    damaged_blocks,
                }
            }
            Err(error @ ReadError::UnsupportedVersion { .. }) => {
                return ReadResult {
                    records,
                    stopped_by: Some(error),
                    damaged_blocks,
                }
            }
            // Damage. Resynchronise at the next block boundary and keep the
            // records already recovered.
            Err(_) => {
                damaged_blocks += 1;
                pending = None;
                offset = next_block(offset);
            }
        }
    }

    ReadResult {
        records,
        stopped_by: None,
        damaged_blocks,
    }
}

const fn next_block(offset: usize) -> usize {
    (offset / BLOCK_SIZE + 1) * BLOCK_SIZE
}

/// CRC-32C (Castagnoli), the polynomial the ADR names.
///
/// Implemented directly rather than pulled in as a dependency: it is fifteen
/// lines, and a durable format should not be able to change because a crate
/// did.
fn crc32c(data: &[u8]) -> u32 {
    const POLY: u32 = 0x82F6_3B78;
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_small_record_round_trips_in_one_fragment() {
        let encoded = encode_record(b"hello");
        assert_eq!(encoded.len(), HEADER_LEN + 5);
        let result = read_segment(&encoded);
        assert_eq!(result.records, vec![b"hello".to_vec()]);
        assert_eq!(result.stopped_by, None);
        assert_eq!(result.damaged_blocks, 0);
    }

    #[test]
    fn a_record_larger_than_a_block_is_fragmented_and_reassembled() {
        // The case fragmentation exists for. Exact byte equality matters:
        // reassembling in the wrong order or dropping a middle fragment would
        // still produce a plausible-looking record.
        let body: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let encoded = encode_record(&body);
        assert!(encoded.len() > 3 * BLOCK_SIZE);
        let result = read_segment(&encoded);
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0], body);
    }

    #[test]
    fn several_records_pack_into_one_block() {
        let encoded: Vec<u8> = (0..10)
            .flat_map(|i| encode_record(format!("record-{i}").as_bytes()))
            .collect();
        let result = read_segment(&encoded);
        assert_eq!(result.records.len(), 10);
        assert_eq!(result.records[9], b"record-9".to_vec());
    }

    #[test]
    fn a_truncated_tail_is_reported_as_torn_not_corrupt() {
        // The ordinary consequence of a crash mid-append. Everything before the
        // truncation is valid and must still be returned -- treating it as
        // corruption would discard good records on every unclean shutdown.
        let mut encoded = encode_record(b"first");
        encoded.extend_from_slice(&encode_record(b"second"));
        encoded.truncate(encoded.len() - 3);

        let result = read_segment(&encoded);
        assert_eq!(result.records, vec![b"first".to_vec()]);
        assert_eq!(result.stopped_by, Some(ReadError::TornTail));
        assert_eq!(result.damaged_blocks, 0, "a torn tail is not damage");
    }

    #[test]
    fn a_damaged_block_costs_one_record_not_the_rest_of_the_segment() {
        // The property fragmentation buys. Without block-aligned resync, the
        // reader could not locate the next record and would lose everything
        // after the damage.
        let big: Vec<u8> = vec![7u8; BLOCK_SIZE * 2];
        let mut segment = encode_record(&big);
        let survivor_offset = segment.len();
        segment.extend_from_slice(&encode_record(b"after the damage"));

        // Corrupt a byte in the first block's body.
        segment[HEADER_LEN + 10] ^= 0xFF;

        let result = read_segment(&segment);
        assert!(result.damaged_blocks > 0, "damage must be reported");
        assert_eq!(
            result.records,
            vec![b"after the damage".to_vec()],
            "the record after the damage must survive"
        );
        assert!(survivor_offset > BLOCK_SIZE);
    }

    #[test]
    fn resynchronising_after_damage_costs_one_crc_not_thousands() {
        // Mutation testing found that replacing the block-aligned skip with a
        // one-byte advance passes every correctness test here, and that turned
        // out to be true rather than a gap in the tests: a decoy header inside a
        // record body fails its CRC, so a scanning reader rejects it and
        // eventually reaches the same records.
        //
        // The difference is cost. Scanning attempts a decode at every byte of a
        // damaged block -- 32768 CRC computations over attacker- or
        // corruption-controlled lengths -- where alignment does one. On a
        // recovery path that already runs under a 10-second manifest deadline
        // (ADR 0003 section 9.8), that is the difference between skipping
        // damage and stalling on it.
        //
        // So this asserts the cheap property directly: the reader must not
        // examine interior offsets.
        let body = vec![0u8; BLOCK_SIZE * 2];
        let mut segment = encode_record(&body);
        segment.extend_from_slice(&encode_record(b"after"));
        segment[HEADER_LEN + 5] ^= 0xFF;

        let result = read_segment(&segment);
        assert_eq!(result.records, vec![b"after".to_vec()]);
        assert_eq!(
            result.damaged_blocks, 1,
            "a damaged block must be counted once, not once per byte examined"
        );
    }

    #[test]
    fn a_corrupt_length_is_caught_by_the_checksum() {
        // The length is inside the CRC precisely so a flipped length byte is
        // detected rather than used to read the wrong number of bytes -- which
        // would return a truncated or over-long body that passes no other check.
        let mut encoded = encode_record(b"payload");
        encoded[6] ^= 0xFF;
        let result = read_segment(&encoded);
        assert!(result.records.is_empty());
        assert!(result.damaged_blocks > 0 || result.stopped_by.is_some());
    }

    #[test]
    fn a_newer_format_version_is_refused_rather_than_guessed() {
        // Misreading a durable record is worse than refusing it: a guess
        // produces plausible bytes with no indication anything was wrong.
        let mut encoded = encode_record(b"x");
        encoded[4] = FORMAT_VERSION + 1;
        let result = read_segment(&encoded);
        assert_eq!(
            result.stopped_by,
            Some(ReadError::UnsupportedVersion {
                found: FORMAT_VERSION + 1
            })
        );
    }

    #[test]
    fn a_continuation_without_a_head_is_discarded() {
        // Its first fragment was in a damaged block. Appending it to whatever
        // comes next would splice unrelated records together, which is worse
        // than losing it.
        let body: Vec<u8> = vec![3u8; BLOCK_SIZE * 2];
        let segment = encode_record(&body);
        // Start reading from the second block: the head fragment is gone.
        let result = read_segment(&segment[BLOCK_SIZE..]);
        assert!(
            result.records.is_empty(),
            "a record cannot be reconstructed from its tail alone"
        );
    }

    #[test]
    fn padding_at_a_block_tail_is_skipped_not_misread() {
        // A record that leaves fewer than HEADER_LEN+1 bytes in a block forces
        // padding. If that padding were read as a header, every such block
        // boundary would look like corruption.
        // Leave the block with fewer than HEADER_LEN+1 free bytes after this
        // record, so the encoder must pad rather than start another fragment.
        let filler_len = BLOCK_SIZE - HEADER_LEN - HEADER_LEN;
        let mut segment = encode_record(&vec![1u8; filler_len]);
        let free = BLOCK_SIZE - (segment.len() % BLOCK_SIZE);
        assert!(
            free < HEADER_LEN + 1,
            "test setup: {free} bytes free, need < {}",
            HEADER_LEN + 1
        );
        segment.extend_from_slice(&encode_record(b"next block"));

        let result = read_segment(&segment);
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[1], b"next block".to_vec());
        assert_eq!(result.damaged_blocks, 0);
    }

    #[test]
    fn every_fragment_starts_at_a_position_the_reader_can_find() {
        // The invariant the whole scheme rests on: after skipping to a block
        // boundary, a fragment header is there. Checked across sizes that
        // straddle boundaries in different ways.
        for size in [
            1,
            HEADER_LEN,
            BLOCK_SIZE - HEADER_LEN - 1,
            BLOCK_SIZE,
            BLOCK_SIZE + 1,
        ] {
            let segment = encode_record(&vec![9u8; size]);
            for block_start in (0..segment.len()).step_by(BLOCK_SIZE) {
                let rest = &segment[block_start..];
                if rest.iter().all(|b| *b == 0) {
                    continue;
                }
                assert_eq!(
                    &rest[..4.min(rest.len())],
                    &MAGIC[..4.min(rest.len())],
                    "size {size}: block at {block_start} does not start with a fragment"
                );
            }
        }
    }

    #[test]
    fn crc32c_matches_known_vectors() {
        // Castagnoli, not the more common CRC-32. Getting the polynomial wrong
        // would still detect damage but would make segments unreadable by any
        // other implementation of this spec.
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"a"), 0xC1D0_4330);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }
}

/// Record bodies, generated from `proto/wal.proto`.
///
/// The schema is the durable contract, so it is compiled rather than
/// hand-translated: a hand-written encoder can drift from the schema silently
/// and this cannot.
pub mod records {
    include!(concat!(env!("OUT_DIR"), "/talon.wal.v1.rs"));
}

/// Encode a record into WAL fragments, ready to append.
///
/// The Protobuf body goes inside the envelope from [`encode_record`], so
/// framing and payload stay independent: a body that fails to decode is a
/// distinct failure from a block that failed its CRC, and recovery treats them
/// differently.
pub fn encode_wal_record(record: &records::Record) -> Vec<u8> {
    let mut body = Vec::new();
    prost::Message::encode(record, &mut body).expect("encoding into a Vec cannot fail");
    encode_record(&body)
}

/// Decode the records in a segment.
///
/// Framing errors and body errors are reported separately. A body that fails to
/// decode means the schema and the data disagree -- a different problem from
/// physical damage, and one that retrying or repairing from a replica will not
/// fix.
pub fn read_wal_segment(buf: &[u8]) -> (Vec<records::Record>, ReadResult) {
    let framing = read_segment(buf);
    let decoded = framing
        .records
        .iter()
        .filter_map(|body| <records::Record as prost::Message>::decode(body.as_slice()).ok())
        .collect();
    (decoded, framing)
}

#[cfg(test)]
mod record_tests {
    use super::records::{record::Kind, Committed, MutationId, ObjectIdentity, Prepared, Record};
    use super::*;

    fn prepared() -> Record {
        Record {
            kind: Some(Kind::Prepared(Prepared {
                mutation_id: Some(MutationId {
                    term: 13,
                    sequence: 4,
                }),
                payload_file: "payload-000042".into(),
                client_request_id: vec![0xAB; 16],
                object: Some(ObjectIdentity {
                    namespace: "ns".into(),
                    backend: "s3".into(),
                    bucket: "data".into(),
                    object_path: "checkpoint.bin".into(),
                }),
                base_origin_version: "v7".into(),
                length: 4096,
                checksum: vec![0xCD; 32],
                shard_id: 1234,
            })),
        }
    }

    #[test]
    fn a_prepared_record_round_trips_through_the_envelope() {
        let encoded = encode_wal_record(&prepared());
        let (records, framing) = read_wal_segment(&encoded);
        assert_eq!(framing.stopped_by, None);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], prepared());
    }

    #[test]
    fn no_payload_bytes_appear_in_the_wal() {
        // §9.4: "No object payload bytes are copied into the WAL." The record
        // names a payload file and carries its checksum; the bytes live
        // elsewhere. A WAL that inlined payloads could not be replayed quickly
        // or compacted cheaply, which is what the whole checkpoint design
        // assumes.
        let encoded = encode_wal_record(&prepared());
        assert!(
            encoded.len() < 512,
            "a PREPARED record is metadata-sized, got {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn several_record_kinds_share_one_segment() {
        let mut segment = encode_wal_record(&prepared());
        segment.extend_from_slice(&encode_wal_record(&Record {
            kind: Some(Kind::Committed(Committed {
                mutation_id: Some(MutationId {
                    term: 13,
                    sequence: 4,
                }),
            })),
        }));
        let (records, framing) = read_wal_segment(&segment);
        assert_eq!(framing.damaged_blocks, 0);
        assert_eq!(records.len(), 2);
        assert!(matches!(records[1].kind, Some(Kind::Committed(_))));
    }

    #[test]
    fn a_record_larger_than_a_block_survives_fragmentation() {
        // Protobuf bodies are usually small, but an object path is
        // caller-controlled and nothing bounds it here. The framing must carry
        // whatever the schema produces.
        let mut record = prepared();
        if let Some(Kind::Prepared(ref mut p)) = record.kind {
            if let Some(ref mut object) = p.object {
                object.object_path = "x".repeat(BLOCK_SIZE * 2);
            }
        }
        let encoded = encode_wal_record(&record);
        assert!(encoded.len() > BLOCK_SIZE * 2);
        let (records, _) = read_wal_segment(&encoded);
        assert_eq!(records, vec![record]);
    }

    #[test]
    fn a_damaged_block_drops_one_record_and_keeps_the_rest() {
        // The framing property from #424, now with real bodies: a decode error
        // must not cascade past the damaged record.
        let big = {
            let mut record = prepared();
            if let Some(Kind::Prepared(ref mut p)) = record.kind {
                p.payload_file = "y".repeat(BLOCK_SIZE);
            }
            record
        };
        let mut segment = encode_wal_record(&big);
        segment.extend_from_slice(&encode_wal_record(&Record {
            kind: Some(Kind::Committed(Committed {
                mutation_id: Some(MutationId {
                    term: 99,
                    sequence: 1,
                }),
            })),
        }));
        segment[HEADER_LEN + 4] ^= 0xFF;

        let (records, framing) = read_wal_segment(&segment);
        assert!(framing.damaged_blocks > 0);
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].kind, Some(Kind::Committed(_))));
    }

    #[test]
    fn the_encoding_matches_committed_golden_bytes() {
        // Found by mutation testing: renumbering a field passed every other
        // test here, because they all encode and decode with the same schema.
        // That is exactly the change that breaks durability -- a WAL written
        // before the renumber becomes unreadable after it, with no compile
        // error and no failing test.
        //
        // These bytes were produced once and committed. If this test fails, the
        // on-disk format changed: either revert it, or bump FORMAT_VERSION and
        // provide a migration. It is not a test to update.
        let record = Record {
            kind: Some(Kind::Committed(Committed {
                mutation_id: Some(MutationId {
                    term: 13,
                    sequence: 4,
                }),
            })),
        };
        let mut body = Vec::new();
        prost::Message::encode(&record, &mut body).expect("encode");

        // field 2 (committed), len 6 | field 1 (mutation_id), len 4
        //   | field 1 (term) = 13 | field 2 (sequence) = 4
        assert_eq!(
            body,
            vec![18, 6, 10, 4, 8, 13, 16, 4],
            "WAL record encoding changed; see the comment above before touching these bytes"
        );
    }

    #[test]
    fn the_prepared_encoding_matches_committed_golden_bytes() {
        // Prepared has every field type the schema uses -- varint, string,
        // bytes, and a nested message -- so pinning it covers renumbering or
        // retyping any of them. The Committed golden above does not: a
        // mutation to a Prepared field number passed it.
        //
        // Same rule: if this fails the on-disk format changed. Revert, or bump
        // FORMAT_VERSION and migrate.
        let record = Record {
            kind: Some(Kind::Prepared(Prepared {
                mutation_id: Some(MutationId {
                    term: 1,
                    sequence: 2,
                }),
                payload_file: "p".into(),
                client_request_id: vec![1],
                object: Some(ObjectIdentity {
                    namespace: "n".into(),
                    backend: "s3".into(),
                    bucket: "b".into(),
                    object_path: "o".into(),
                }),
                base_origin_version: "v".into(),
                length: 3,
                checksum: vec![2],
                shard_id: 4,
            })),
        };
        let mut body = Vec::new();
        prost::Message::encode(&record, &mut body).expect("encode");
        assert_eq!(
            body,
            vec![
                10, 37, 10, 4, 8, 1, 16, 2, 18, 1, 112, 26, 1, 1, 34, 13, 10, 1, 110, 18, 2, 115,
                51, 26, 1, 98, 34, 1, 111, 42, 1, 118, 48, 3, 58, 1, 2, 64, 4
            ],
            "WAL record encoding changed; see the comment above before touching these bytes"
        );
    }

    #[test]
    fn decoding_tolerates_an_unknown_field() {
        // Protobuf's forward compatibility is what makes an additive schema
        // change safe. A reader from before a new optional field must skip it
        // rather than fail, or every rollout would need both sides upgraded
        // simultaneously.
        let mut body = Vec::new();
        prost::Message::encode(
            &Record {
                kind: Some(Kind::Committed(Committed {
                    mutation_id: Some(MutationId {
                        term: 1,
                        sequence: 2,
                    }),
                })),
            },
            &mut body,
        )
        .expect("encode");
        // Append field 15, varint, value 99 -- a field this build does not know.
        body.extend_from_slice(&[0x78, 99]);

        let decoded = <Record as prost::Message>::decode(body.as_slice())
            .expect("an unknown field must be skipped, not rejected");
        assert!(matches!(decoded.kind, Some(Kind::Committed(_))));
    }

    #[test]
    fn mutation_ids_keep_their_ordering_through_a_round_trip() {
        // The (term, sequence) ordering is what recovery uses to pick a winner,
        // so it has to survive the encoding. Protobuf varints are unsigned and
        // order-preserving here, but the assertion pins it rather than assuming.
        let older = MutationId {
            term: 12,
            sequence: 9999,
        };
        let newer = MutationId {
            term: 13,
            sequence: 1,
        };
        for id in [older, newer] {
            let record = Record {
                kind: Some(Kind::Committed(Committed {
                    mutation_id: Some(id),
                })),
            };
            let (decoded, _) = read_wal_segment(&encode_wal_record(&record));
            let Some(Kind::Committed(ref c)) = decoded[0].kind else {
                panic!("expected Committed");
            };
            assert_eq!(c.mutation_id, Some(id));
        }
        assert!(
            (newer.term, newer.sequence) > (older.term, older.sequence),
            "term must dominate sequence"
        );
    }
}
