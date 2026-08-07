// SPDX-License-Identifier: Apache-2.0
//! The on-disk checkpoint format for the NVMe tier, and its eviction log.
//!
//! A shard's extents are addressable only through run descriptors that live in
//! memory, so a restart without a checkpoint leaves 64 MiB regions of perfectly
//! good bytes that nothing can name. A checkpoint is those descriptors written
//! down: the entry map, the interned stream names it refers to, and the region
//! bookkeeping needed to keep packing where the last run left off.
//!
//! Modelled on Velox's `SsdFile` checkpoint, which solves the same problem for
//! the cache this tier is modelled on — three files per shard, an eviction log
//! covering the window between checkpoints, and a recovery path that treats any
//! damage as a reason to start cold rather than to fail.
//!
//! # Why this is hand-rolled rather than serde
//!
//! ADR 0003 §9.4: `serde`/`bincode`/JSON are not durable on-disk formats in
//! Talon. The reasoning there is about the write-back WAL, where the format is a
//! recovery contract, but it applies here for a cheaper reason too — a
//! derive-driven layout changes silently when a field is added, and this file is
//! read by a future binary that may not match the one that wrote it. A fixed
//! little-endian layout with an explicit version makes that a rejection instead
//! of a misparse.
//!
//! # Layout
//!
//! ```text
//! header    magic u32 | format u32 | flags u32 | max_regions u32
//!           num_regions u32 | num_streams u32 | num_entries u64
//! body      region_sizes   num_regions x u32
//!           region_scores  num_regions x f64
//!           streams        num_streams x (id u64, backend u8,
//!                                         bucket len-prefixed, path len-prefixed)
//!           entries        num_entries x (stream_id u64, offset u64, region u32,
//!                                         offset_in_region u32, size u32,
//!                                         checksum u32)
//! trailer   digest u64 (xxh3 of everything above) | end marker u64
//! ```
//!
//! The digest and the end marker together are what make a torn write
//! detectable: a checkpoint truncated at any length fails one or the other, and
//! [`decode`] returns an error rather than a half-populated map.
//!
//! See ADR 0005 §7.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use talon_core::{Backend, ObjectId};

use super::region::{ExtentRun, REGION_SIZE};
use super::ExtentKey;

/// `"TLCP"` read as a little-endian `u32`.
pub const CHECKPOINT_MAGIC: u32 = 0x5043_4c54;

/// Current checkpoint layout version. A file naming any other version is not
/// read — this is a cache, so rejecting costs a cold start and nothing else.
pub const FORMAT_VERSION: u32 = 1;

/// Terminates a complete checkpoint. Velox's `kCheckpointEndMarker`, kept
/// identical because the two formats serve the same purpose and a shared
/// constant makes that lineage greppable.
pub const CHECKPOINT_END: u64 = 0xcbed_f11e;

/// Set in `flags` when the writing shard had checksums enabled.
const FLAG_CHECKSUMS: u32 = 1;

const HEADER_LEN: usize = 32;
const TRAILER_LEN: usize = 16;
const ENTRY_LEN: usize = 32;

/// Why a checkpoint could not be read.
///
/// Every variant means the same thing to the caller — start cold — but they are
/// distinguished because "the operator changed the capacity" and "the file is
/// corrupt" want different log lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    /// Fewer bytes than the header and trailer require.
    TooShort,
    /// The file does not begin with [`CHECKPOINT_MAGIC`].
    BadMagic,
    /// Written by a different layout version.
    UnknownVersion(u32),
    /// The end marker is absent, so the write did not complete.
    NoEndMarker,
    /// The body does not match its digest.
    DigestMismatch,
    /// The header's counts do not agree with the body's length, or a record is
    /// internally inconsistent.
    Malformed(&'static str),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => write!(f, "checkpoint is shorter than its header and trailer"),
            Self::BadMagic => write!(f, "checkpoint magic does not match"),
            Self::UnknownVersion(v) => write!(f, "checkpoint format version {v} is not supported"),
            Self::NoEndMarker => write!(f, "checkpoint has no end marker; the write was torn"),
            Self::DigestMismatch => write!(f, "checkpoint digest does not match its body"),
            Self::Malformed(what) => write!(f, "checkpoint is malformed: {what}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

/// Everything one shard needs to become addressable again.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointData {
    /// Region cap the writing shard was configured with. A recovering shard
    /// with a different cap rejects the checkpoint — the capacity changed, and
    /// region indices no longer mean the same thing.
    pub max_regions: u32,
    /// Whether the writing shard stored extent digests.
    pub checksums_enabled: bool,
    /// Bytes packed into each region, indexed by region.
    pub region_sizes: Vec<u32>,
    /// Decayed read-volume score per region, so reclamation does not restart
    /// from a flat distribution and evict a genuinely hot region first.
    pub region_scores: Vec<f64>,
    /// Interned stream ids and the objects they name. Only streams this shard's
    /// entries refer to are written.
    pub streams: Vec<(u64, ObjectId)>,
    /// The entry map.
    pub entries: Vec<(ExtentKey, ExtentRun)>,
}

impl CheckpointData {
    /// Number of regions covered, which both size vectors must agree on.
    pub fn num_regions(&self) -> usize {
        self.region_sizes.len()
    }
}

/// Stable on-disk discriminant for a backend.
///
/// Deliberately not `Backend as u8`: this value is written to disk, and a
/// future reordering of the enum would silently reinterpret every recovered
/// stream as belonging to a different store. Append only.
fn backend_code(backend: Backend) -> u8 {
    match backend {
        Backend::S3 => 0,
        Backend::Gcs => 1,
        Backend::Azure => 2,
    }
}

fn backend_from_code(code: u8) -> Option<Backend> {
    match code {
        0 => Some(Backend::S3),
        1 => Some(Backend::Gcs),
        2 => Some(Backend::Azure),
        _ => None,
    }
}

/// Serialize a checkpoint, digest and end marker included.
pub fn encode(data: &CheckpointData) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        HEADER_LEN + data.entries.len() * ENTRY_LEN + data.num_regions() * 12 + TRAILER_LEN,
    );

    out.extend_from_slice(&CHECKPOINT_MAGIC.to_le_bytes());
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    let flags = if data.checksums_enabled {
        FLAG_CHECKSUMS
    } else {
        0
    };
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&data.max_regions.to_le_bytes());
    out.extend_from_slice(&(data.num_regions() as u32).to_le_bytes());
    out.extend_from_slice(&(data.streams.len() as u32).to_le_bytes());
    out.extend_from_slice(&(data.entries.len() as u64).to_le_bytes());

    for &size in &data.region_sizes {
        out.extend_from_slice(&size.to_le_bytes());
    }
    for &score in &data.region_scores {
        out.extend_from_slice(&score.to_bits().to_le_bytes());
    }

    for (id, object) in &data.streams {
        out.extend_from_slice(&id.to_le_bytes());
        out.push(backend_code(object.backend));
        put_str(&mut out, &object.bucket);
        put_str(&mut out, &object.object_path);
    }

    for (key, run) in &data.entries {
        out.extend_from_slice(&key.stream_id.to_le_bytes());
        out.extend_from_slice(&key.offset.to_le_bytes());
        out.extend_from_slice(&run.region.to_le_bytes());
        out.extend_from_slice(&run.offset_in_region.to_le_bytes());
        out.extend_from_slice(&run.size.to_le_bytes());
        out.extend_from_slice(&run.checksum.to_le_bytes());
    }

    let digest = xxhash_rust::xxh3::xxh3_64(&out);
    out.extend_from_slice(&digest.to_le_bytes());
    out.extend_from_slice(&CHECKPOINT_END.to_le_bytes());
    out
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// A bounds-checked little-endian reader.
///
/// Every read goes through this rather than indexing, so a truncated or
/// hostile file produces [`CheckpointError`] instead of a panic. Nothing is
/// allocated from a length field before that length has been shown to fit in
/// what remains.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CheckpointError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(CheckpointError::Malformed("length overflow"))?;
        if end > self.bytes.len() {
            return Err(CheckpointError::Malformed("record runs past end of file"));
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, CheckpointError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CheckpointError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, CheckpointError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn f64(&mut self) -> Result<f64, CheckpointError> {
        Ok(f64::from_bits(self.u64()?))
    }

    fn string(&mut self) -> Result<String, CheckpointError> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| CheckpointError::Malformed("stream name is not utf-8"))
    }
}

/// Parse a checkpoint, verifying the digest and end marker before trusting any
/// of it.
///
/// Structural consistency is checked here — region indices in range, runs
/// inside their region, counts matching the body length. Policy checks that
/// depend on the recovering shard's configuration (capacity, checksum mode,
/// evicted regions) belong to the caller.
pub fn decode(bytes: &[u8]) -> Result<CheckpointData, CheckpointError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(CheckpointError::TooShort);
    }

    let body_len = bytes.len() - TRAILER_LEN;
    let mut trailer = Cursor {
        bytes,
        pos: body_len,
    };
    let stored_digest = trailer.u64()?;
    if trailer.u64()? != CHECKPOINT_END {
        return Err(CheckpointError::NoEndMarker);
    }
    // Checked before the header so a file that is complete-but-corrupt is
    // reported as a digest failure rather than as whatever the corrupt header
    // happens to say.
    if xxhash_rust::xxh3::xxh3_64(&bytes[..body_len]) != stored_digest {
        return Err(CheckpointError::DigestMismatch);
    }

    let mut c = Cursor { bytes, pos: 0 };
    if c.u32()? != CHECKPOINT_MAGIC {
        return Err(CheckpointError::BadMagic);
    }
    let version = c.u32()?;
    if version != FORMAT_VERSION {
        return Err(CheckpointError::UnknownVersion(version));
    }
    let flags = c.u32()?;
    let max_regions = c.u32()?;
    let num_regions = c.u32()? as usize;
    let num_streams = c.u32()? as usize;
    let num_entries = c.u64()?;

    if num_regions > max_regions as usize {
        return Err(CheckpointError::Malformed(
            "more regions than the cap allows",
        ));
    }
    // The counts are attacker-free but not trustworthy: a corrupt-but-matching
    // header is impossible past the digest check, yet a *valid* checkpoint from
    // a future writer could still name counts this build cannot hold. Bound the
    // allocation by what the file could physically contain.
    let entry_bytes = (num_entries as usize)
        .checked_mul(ENTRY_LEN)
        .ok_or(CheckpointError::Malformed("entry count overflows"))?;
    if entry_bytes > body_len {
        return Err(CheckpointError::Malformed(
            "entry count exceeds file length",
        ));
    }

    let mut region_sizes = Vec::with_capacity(num_regions);
    for _ in 0..num_regions {
        let size = c.u32()?;
        if size as u64 > REGION_SIZE {
            return Err(CheckpointError::Malformed("region is larger than a region"));
        }
        region_sizes.push(size);
    }
    let mut region_scores = Vec::with_capacity(num_regions);
    for _ in 0..num_regions {
        let score = c.f64()?;
        // A NaN score would poison `total_cmp` ordering in reclamation and make
        // the coldest-region choice arbitrary. Treat it as no information.
        region_scores.push(if score.is_finite() { score } else { 0.0 });
    }

    let mut streams = Vec::with_capacity(num_streams);
    for _ in 0..num_streams {
        let id = c.u64()?;
        let backend =
            backend_from_code(c.u8()?).ok_or(CheckpointError::Malformed("unknown backend code"))?;
        let bucket = c.string()?;
        let object_path = c.string()?;
        streams.push((id, ObjectId::new(backend, bucket, object_path)));
    }

    let mut entries = Vec::with_capacity(num_entries as usize);
    for _ in 0..num_entries {
        let stream_id = c.u64()?;
        let offset = c.u64()?;
        let region = c.u32()?;
        let offset_in_region = c.u32()?;
        let size = c.u32()?;
        let checksum = c.u32()?;

        if region as usize >= num_regions {
            return Err(CheckpointError::Malformed(
                "entry names a region that does not exist",
            ));
        }
        if offset_in_region as u64 + size as u64 > REGION_SIZE {
            return Err(CheckpointError::Malformed(
                "entry runs past the end of its region",
            ));
        }
        // An entry beyond its region's high-water mark would read bytes the
        // shard does not believe it wrote.
        if offset_in_region + size > region_sizes[region as usize] {
            return Err(CheckpointError::Malformed(
                "entry runs past its region's filled length",
            ));
        }
        entries.push((
            ExtentKey::new(stream_id, offset),
            ExtentRun {
                region,
                offset_in_region,
                size,
                checksum,
            },
        ));
    }

    if c.pos != body_len {
        return Err(CheckpointError::Malformed(
            "trailing bytes after the entry map",
        ));
    }

    Ok(CheckpointData {
        max_regions,
        checksums_enabled: flags & FLAG_CHECKSUMS != 0,
        region_sizes,
        region_scores,
        streams,
        entries,
    })
}

/// Append-only record of regions reclaimed since the last checkpoint.
///
/// This is what makes a stale checkpoint safe. Evicting region R and packing
/// new extents into it invalidates every checkpointed entry that named R, but
/// the checkpoint is only rewritten periodically — so a crash in between would
/// otherwise recover entries pointing at bytes that have since been overwritten.
/// The extent digest would usually catch that, but only when checksums are on
/// and only on the non-zero-copy path, which is not a guarantee.
///
/// Each append is `fsync`ed **before** the region is reused. That is a sync per
/// reclamation, not per write: regions are 64 MiB, so the cost is negligible
/// against the 64 MiB of writes needed to fill one.
#[derive(Debug)]
pub struct EvictionLog {
    path: PathBuf,
    file: std::fs::File,
}

impl EvictionLog {
    /// Open or create the log at `path`, positioned to append.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        Ok(Self { path, file })
    }

    /// Record reclaimed regions and make the record durable.
    pub fn append(&mut self, regions: &[u32]) -> std::io::Result<()> {
        if regions.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(regions.len() * 4);
        for &r in regions {
            buf.extend_from_slice(&r.to_le_bytes());
        }
        self.file.write_all(&buf)?;
        self.file.sync_data()
    }

    /// Every region named by the log.
    ///
    /// A torn tail — a crash mid-append leaving fewer than four bytes — is
    /// dropped rather than treated as damage. The log is a superset hint: an
    /// extra region in it costs recoverable extents, a missing one costs
    /// correctness, so the read is deliberately biased toward keeping whatever
    /// whole records survived.
    pub fn read(path: &Path) -> std::io::Result<Vec<u32>> {
        let mut file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }

    /// Drop every record, after a checkpoint has made them redundant.
    pub fn clear(&mut self) -> std::io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_data()
    }

    /// Path of the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        tag.hash(&mut h);
        let mut p = std::env::temp_dir();
        p.push(format!("talon-cpt-{}-{:x}", std::process::id(), h.finish()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample() -> CheckpointData {
        CheckpointData {
            max_regions: 4,
            checksums_enabled: true,
            region_sizes: vec![8192, 4096],
            region_scores: vec![12.5, 0.25],
            streams: vec![
                (0, ObjectId::new(Backend::S3, "wh", "part-0.parquet")),
                (
                    7,
                    ObjectId::new(Backend::Azure, "acct/cont", "part-1.lance"),
                ),
            ],
            entries: vec![
                (
                    ExtentKey::new(0, 0),
                    ExtentRun {
                        region: 0,
                        offset_in_region: 0,
                        size: 4096,
                        checksum: 0xdead_beef,
                    },
                ),
                (
                    ExtentKey::new(7, 65536),
                    ExtentRun {
                        region: 1,
                        offset_in_region: 0,
                        size: 4096,
                        checksum: 0,
                    },
                ),
            ],
        }
    }

    #[test]
    fn a_checkpoint_round_trips_exactly() {
        let data = sample();
        assert_eq!(decode(&encode(&data)).unwrap(), data);
    }

    #[test]
    fn an_empty_checkpoint_round_trips() {
        // A shard that has been reclaimed down to nothing still checkpoints, and
        // recovering it must produce an empty map rather than an error.
        let data = CheckpointData {
            max_regions: 4,
            checksums_enabled: false,
            region_sizes: Vec::new(),
            region_scores: Vec::new(),
            streams: Vec::new(),
            entries: Vec::new(),
        };
        assert_eq!(decode(&encode(&data)).unwrap(), data);
    }

    #[test]
    fn the_checksum_mode_survives_the_round_trip() {
        // Recovery rejects a checkpoint written in the other mode, so this bit
        // has to be readable before anything else is trusted.
        for enabled in [true, false] {
            let mut data = sample();
            data.checksums_enabled = enabled;
            assert_eq!(decode(&encode(&data)).unwrap().checksums_enabled, enabled);
        }
    }

    /// The crash-safety test: a checkpoint cut at *any* length must be rejected,
    /// not partially believed. A partially believed checkpoint is the one
    /// failure mode that serves wrong bytes instead of refetching them.
    #[test]
    fn a_checkpoint_truncated_at_any_length_is_rejected() {
        let full = encode(&sample());
        for cut in 0..full.len() {
            let err = decode(&full[..cut]).unwrap_err();
            assert!(
                matches!(
                    err,
                    CheckpointError::TooShort
                        | CheckpointError::NoEndMarker
                        | CheckpointError::DigestMismatch
                        | CheckpointError::Malformed(_)
                ),
                "prefix of {cut} bytes was accepted as {err:?}"
            );
        }
        assert!(decode(&full).is_ok(), "the whole file must still be valid");
    }

    #[test]
    fn a_single_flipped_bit_anywhere_is_caught() {
        let full = encode(&sample());
        for byte in 0..full.len() {
            let mut corrupt = full.clone();
            corrupt[byte] ^= 0x01;
            assert!(
                decode(&corrupt).is_err(),
                "a flipped bit at byte {byte} went undetected"
            );
        }
    }

    #[test]
    fn a_file_that_is_not_a_checkpoint_is_rejected_by_magic() {
        let mut bytes = encode(&sample());
        bytes[..4].copy_from_slice(&0x1234_5678u32.to_le_bytes());
        // Re-digest so the magic check is what fires, not the digest check.
        let body_len = bytes.len() - TRAILER_LEN;
        let digest = xxhash_rust::xxh3::xxh3_64(&bytes[..body_len]);
        bytes[body_len..body_len + 8].copy_from_slice(&digest.to_le_bytes());
        assert_eq!(decode(&bytes), Err(CheckpointError::BadMagic));
    }

    #[test]
    fn a_future_format_version_is_rejected_rather_than_guessed() {
        let mut bytes = encode(&sample());
        bytes[4..8].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        let body_len = bytes.len() - TRAILER_LEN;
        let digest = xxhash_rust::xxh3::xxh3_64(&bytes[..body_len]);
        bytes[body_len..body_len + 8].copy_from_slice(&digest.to_le_bytes());
        assert_eq!(
            decode(&bytes),
            Err(CheckpointError::UnknownVersion(FORMAT_VERSION + 1))
        );
    }

    #[test]
    fn an_entry_naming_a_region_that_does_not_exist_is_rejected() {
        // Not reachable from a correct writer, but a recovered map that indexes
        // outside `region_sizes` would panic the shard on the first read.
        let mut data = sample();
        data.entries[0].1.region = 9;
        assert!(matches!(
            decode(&encode(&data)),
            Err(CheckpointError::Malformed(_))
        ));
    }

    #[test]
    fn an_entry_running_past_its_regions_filled_length_is_rejected() {
        let mut data = sample();
        data.region_sizes[0] = 128;
        assert!(matches!(
            decode(&encode(&data)),
            Err(CheckpointError::Malformed(_))
        ));
    }

    #[test]
    fn backend_codes_are_append_only() {
        // These are written to disk. Reordering them would silently reinterpret
        // every recovered stream as living in a different store, which is a
        // cross-backend read rather than a miss.
        assert_eq!(backend_code(Backend::S3), 0);
        assert_eq!(backend_code(Backend::Gcs), 1);
        assert_eq!(backend_code(Backend::Azure), 2);
        for b in [Backend::S3, Backend::Gcs, Backend::Azure] {
            assert_eq!(backend_from_code(backend_code(b)), Some(b));
        }
        assert_eq!(backend_from_code(3), None);
    }

    #[test]
    fn object_names_survive_verbatim() {
        // Paths carry spaces, unicode, and `=` from hive partitioning. A
        // recovered name that differs by one byte interns to a new id and the
        // extents are silently orphaned.
        let awkward = "dt=2026-08-07/région ‰/part 0.parquet";
        let data = CheckpointData {
            max_regions: 1,
            checksums_enabled: false,
            region_sizes: vec![0],
            region_scores: vec![0.0],
            streams: vec![(3, ObjectId::new(Backend::Gcs, "bücket", awkward))],
            entries: Vec::new(),
        };
        let back = decode(&encode(&data)).unwrap();
        assert_eq!(back.streams[0].1.object_path, awkward);
        assert_eq!(back.streams[0].1.bucket, "bücket");
    }

    #[test]
    fn a_nan_region_score_is_read_as_zero() {
        // `total_cmp` orders NaN, but not usefully: a NaN score would make the
        // coldest-region choice arbitrary for the life of the process.
        let mut data = sample();
        data.region_scores[0] = f64::NAN;
        assert_eq!(decode(&encode(&data)).unwrap().region_scores[0], 0.0);
    }

    #[test]
    fn the_eviction_log_round_trips_and_clears() {
        let dir = tmp_dir("evlog");
        let path = dir.join("shard.log");
        let mut log = EvictionLog::open(path.clone()).unwrap();

        log.append(&[3, 1]).unwrap();
        log.append(&[7]).unwrap();
        assert_eq!(EvictionLog::read(&path).unwrap(), vec![3, 1, 7]);

        log.clear().unwrap();
        assert!(EvictionLog::read(&path).unwrap().is_empty());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_missing_eviction_log_reads_as_empty() {
        let dir = tmp_dir("evlog-missing");
        assert!(EvictionLog::read(&dir.join("absent.log"))
            .unwrap()
            .is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_torn_eviction_log_keeps_its_whole_records() {
        // A crash mid-append leaves a partial u32. Losing the whole log there
        // would resurrect entries in regions that were already overwritten.
        let dir = tmp_dir("evlog-torn");
        let path = dir.join("shard.log");
        {
            let mut log = EvictionLog::open(path.clone()).unwrap();
            log.append(&[5, 6]).unwrap();
        }
        let mut raw = std::fs::read(&path).unwrap();
        raw.extend_from_slice(&[0xff, 0xff]);
        std::fs::write(&path, &raw).unwrap();

        assert_eq!(EvictionLog::read(&path).unwrap(), vec![5, 6]);

        std::fs::remove_dir_all(dir).ok();
    }
}
