use super::{checksum_len, format, journal::Record, key_codec, keys::Keys};
use crate::block_key::ObjectRef;
use crate::small_store::key::SmallIdentity;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::io;
use std::sync::OnceLock;

const OFFSET_BITS: u32 = 26;
const OFFSET_LIMIT: u32 = 1 << OFFSET_BITS;
const LENGTH_LIMIT: u32 = 1 << 22;

#[derive(Clone, Copy)]
struct CompactRow {
    extent: [u8; 6],
    hash: [u8; 4],
}

#[derive(Clone, Copy)]
struct WideRow {
    offset: u32,
    len: u32,
    hash: u32,
}

const _: () = assert!(size_of::<CompactRow>() == 10 && size_of::<WideRow>() == 12);

impl CompactRow {
    fn offset(&self) -> u32 {
        u32::from_le_bytes(self.extent[..4].try_into().unwrap()) & (OFFSET_LIMIT - 1)
    }

    fn len(&self) -> u32 {
        u32::from_le_bytes(self.extent[2..].try_into().unwrap()) >> (OFFSET_BITS - 16)
    }

    fn set_len_zero(&mut self) {
        // The first 26 bits are the offset, including two bits of extent[3].
        self.extent[3] &= 3;
        self.extent[4..].fill(0);
    }
}

impl From<CompactRow> for WideRow {
    fn from(row: CompactRow) -> Self {
        Self {
            offset: row.offset(),
            len: row.len(),
            hash: u32::from_le_bytes(row.hash),
        }
    }
}

#[derive(Clone)]
enum Rows {
    Compact(Vec<CompactRow>),
    Wide(Vec<WideRow>),
}

impl Default for Rows {
    fn default() -> Self {
        Self::Compact(Vec::new())
    }
}

impl Rows {
    fn len(&self) -> usize {
        match self {
            Self::Compact(rows) => rows.len(),
            Self::Wide(rows) => rows.len(),
        }
    }

    fn get(&self, index: usize) -> Option<WideRow> {
        match self {
            Self::Compact(rows) => rows.get(index).copied().map(WideRow::from),
            Self::Wide(rows) => rows.get(index).copied(),
        }
    }

    fn partition(&self, before: impl Fn(u32) -> bool) -> usize {
        match self {
            Self::Compact(rows) => rows.partition_point(|row| before(row.offset())),
            Self::Wide(rows) => rows.partition_point(|row| before(row.offset)),
        }
    }

    fn store(&mut self, index: usize, row: WideRow, replace: bool) {
        if row.offset >= OFFSET_LIMIT || row.len >= LENGTH_LIMIT {
            if let Self::Compact(rows) = self {
                *self = Self::Wide(rows.iter().copied().map(WideRow::from).collect());
            }
        }
        fn store<T>(rows: &mut Vec<T>, index: usize, row: T, replace: bool) {
            if replace {
                rows[index] = row;
            } else {
                if rows.len() == rows.capacity() {
                    rows.reserve_exact(64);
                }
                rows.insert(index, row);
            }
        }
        match self {
            Self::Compact(rows) => {
                let extent = u64::from(row.offset) | (u64::from(row.len) << OFFSET_BITS);
                let row = CompactRow {
                    extent: extent.to_le_bytes()[..6].try_into().unwrap(),
                    hash: row.hash.to_le_bytes(),
                };
                store(rows, index, row, replace);
            }
            Self::Wide(rows) => store(rows, index, row, replace),
        }
    }

    fn remove(&mut self, index: usize) {
        match self {
            Self::Compact(rows) => rows[index].set_len_zero(),
            Self::Wide(rows) => rows[index].len = 0,
        }
    }

    fn retain_live(&mut self) {
        match self {
            Self::Compact(rows) => rows.retain(|row| row.len() != 0),
            Self::Wide(rows) => rows.retain(|row| row.len != 0),
        }
    }

    fn shrink_to_fit(&mut self) {
        match self {
            Self::Compact(rows) => rows.shrink_to_fit(),
            Self::Wide(rows) => rows.shrink_to_fit(),
        }
    }
}

#[derive(Clone, Default)]
enum Checksums {
    #[default]
    Unset,
    Legacy(Vec<u64>),
    Trailer,
}

#[derive(Clone, Default)]
pub(super) struct Records {
    rows: Rows,
    checksums: Checksums,
    keys: Keys,
    live: usize,
}

#[derive(Clone, Copy)]
pub(super) struct Metadata {
    pub offset: u64,
    pub len: u64,
    pub checksum: Option<u64>,
    pub hash: u32,
}

impl Metadata {
    pub fn stored_len(&self) -> u64 {
        self.len + checksum_len(self.checksum)
    }
}

pub(super) fn key_hash(key: &dyn SmallIdentity) -> u32 {
    static HASH: OnceLock<RandomState> = OnceLock::new();
    HASH.get_or_init(RandomState::new).hash_one(key.view()) as u32
}

impl Records {
    pub fn len(&self) -> usize {
        self.live
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    pub fn metadata(&self, index: usize) -> Option<Metadata> {
        let row = self.rows.get(index).filter(|row| row.len != 0)?;
        let checksum = match &self.checksums {
            Checksums::Legacy(checksums) => Some(checksums[index]),
            _ => None,
        };
        Some(Metadata {
            offset: u64::from(row.offset),
            len: u64::from(row.len),
            checksum,
            hash: row.hash,
        })
    }

    pub fn matches(&self, row: usize, key: &[u8]) -> bool {
        if !matches!(self.rows.get(row), Some(meta) if meta.len != 0) {
            return false;
        }
        let Some((prefix, suffix)) = self.keys.get(row) else {
            return false;
        };
        key.len() == prefix.len() + suffix.len()
            && key.starts_with(prefix)
            && &key[prefix.len()..] == suffix
    }

    pub fn get(&self, row: usize) -> Option<(u64, Record)> {
        let meta = self.metadata(row)?;
        let (prefix, suffix) = self.keys.get(row).expect("small-object key");
        Some((
            meta.offset,
            Record {
                key: key_codec::decode(&[prefix, suffix].concat()),
                len: meta.len,
                checksum: meta.checksum,
            },
        ))
    }

    pub fn object<'a>(&'a self, row: usize, scratch: &'a mut [Vec<u8>; 3]) -> ObjectRef<'a> {
        let [scratch, bucket, path] = scratch;
        let (prefix, suffix) = self.keys.get(row).expect("small-object key");
        let key = if prefix.is_empty() {
            suffix
        } else if suffix.is_empty() {
            prefix
        } else {
            scratch.clear();
            scratch.extend_from_slice(prefix);
            scratch.extend_from_slice(suffix);
            scratch
        };
        key_codec::object(key, bucket, path)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Record)> + '_ {
        (0..self.slots()).filter_map(|row| self.get(row))
    }

    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = Record> + '_ {
        self.iter().map(|(_, record)| record)
    }

    pub fn before(&self, end: u64) -> impl Iterator<Item = (u64, Record)> + '_ {
        self.iter().take_while(move |(offset, _)| *offset < end)
    }

    pub fn overlaps(&self, offset: u64, len: u64) -> bool {
        let next = self.position(offset);
        self.crosses_before(next, offset)
            || (next..self.slots())
                .find_map(|row| self.metadata(row))
                .is_some_and(|row| row.offset < offset + len)
    }

    pub fn crosses(&self, end: u64) -> bool {
        self.crosses_before(self.position(end), end)
    }

    fn crosses_before(&self, next: usize, end: u64) -> bool {
        (0..next)
            .rev()
            .find_map(|row| self.metadata(row))
            .is_some_and(|row| row.offset + row.stored_len() > end)
    }

    pub fn insert(&mut self, offset: u64, record: Record) -> io::Result<usize> {
        format::validate_key(&record.key)?;
        if !record.key.view().valid_extent(record.len)
            || offset >= 1 << 30
            || record.len == 0
            || record.len > 128 << 20
            || offset + record.stored_len() > 1 << 30
        {
            return Err(format::invalid("invalid small-object record extent"));
        }
        if matches!(
            (&self.checksums, record.checksum),
            (Checksums::Trailer, Some(_)) | (Checksums::Legacy(_), None)
        ) {
            return Err(format::invalid(
                "mixed small-object checksum formats in segment",
            ));
        }
        let row = WideRow {
            offset: offset as u32,
            len: record.len as u32,
            hash: key_hash(&record.key),
        };
        let key = key_codec::encode(&record.key);
        let vacant = self.position(offset);
        let replace = self
            .rows
            .get(vacant)
            .is_some_and(|row| u64::from(row.offset) == offset && row.len == 0);
        let index = if replace {
            vacant
        } else {
            self.rows.partition(|o| u64::from(o) <= offset)
        };
        self.keys.insert(index, &key, replace)?;
        if matches!(self.checksums, Checksums::Unset) {
            self.checksums = if record.checksum.is_some() {
                Checksums::Legacy(Vec::new())
            } else {
                Checksums::Trailer
            };
        }
        if let (Checksums::Legacy(checksums), Some(checksum)) =
            (&mut self.checksums, record.checksum)
        {
            if replace {
                checksums[index] = checksum;
            } else {
                if checksums.len() == checksums.capacity() {
                    checksums.reserve_exact(64);
                }
                checksums.insert(index, checksum);
            }
        }
        self.rows.store(index, row, replace);
        self.live += 1;
        Ok(index)
    }

    pub fn remove(&mut self, offset: &u64) -> Option<()> {
        let index = (self.position(*offset)..self.slots())
            .take_while(|&i| u64::from(self.rows.get(i).unwrap().offset) == *offset)
            .find(|&i| self.rows.get(i).unwrap().len != 0)?;
        self.keys.remove(index);
        self.rows.remove(index);
        self.live -= 1;
        if (self.slots() - self.live) * 100 >= self.slots() {
            self.compact();
        }
        Some(())
    }

    pub fn retain_before(&mut self, end: u64) {
        for index in self.position(end)..self.slots() {
            if self.rows.get(index).unwrap().len != 0 {
                self.rows.remove(index);
                self.live -= 1;
            }
        }
        self.compact();
    }

    pub fn slots(&self) -> usize {
        self.rows.len()
    }

    pub fn position(&self, offset: u64) -> usize {
        self.rows.partition(|o| u64::from(o) < offset)
    }

    pub(super) fn offset_at(&self, row: usize) -> u64 {
        u64::from(self.rows.get(row).unwrap().offset)
    }

    pub fn compacts_after_remove(&self) -> bool {
        (self.slots() - self.live + 1) * 100 >= self.slots()
    }

    pub fn shrink_to_fit(&mut self) {
        self.rows.shrink_to_fit();
        if let Checksums::Legacy(checksums) = &mut self.checksums {
            checksums.shrink_to_fit();
        }
        self.keys.shrink_to_fit();
    }

    pub fn seal(&mut self) {
        self.keys.seal();
        self.shrink_to_fit();
    }

    fn compact(&mut self) {
        let live = |row| self.rows.get(row).unwrap().len != 0;
        self.keys.retain(live);
        if let Checksums::Legacy(checksums) = &mut self.checksums {
            let mut index = 0;
            checksums.retain(|_| {
                let keep = live(index);
                index += 1;
                keep
            });
        }
        self.rows.retain_live();
        self.shrink_to_fit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::small_store::key::SmallKey;
    use talon_core::{Backend, BlockId, ObjectId, Version};

    fn record(path: &str) -> Record {
        Record {
            key: SmallKey::new(&BlockId::new(
                ObjectId::new(Backend::Azure, "桶", path),
                0,
                256 << 20,
                Version::new("版本"),
            )),
            len: 10,
            checksum: Some(u64::MAX),
        }
    }

    #[test]
    fn compact_rows_keep_full_extents_and_widen_at_either_limit() {
        let mut records = Records::default();
        let mut last_compact = record("compact");
        last_compact.checksum = None;
        last_compact.len = u64::from(LENGTH_LIMIT - 1);
        let offset = u64::from(OFFSET_LIMIT - 1);
        let end = offset + last_compact.stored_len();
        let row = records.insert(offset, last_compact).unwrap();
        let meta = records.metadata(row).unwrap();
        assert_eq!(
            (meta.offset, meta.len),
            (offset, u64::from(LENGTH_LIMIT - 1))
        );
        assert!(matches!(records.rows, Rows::Compact(_)));
        assert!(records.overlaps(end - 1, 1));
        assert!(records.crosses(end - 1));
        assert!(!records.crosses(end));
        assert!(!records.overlaps(end, 1));

        for (offset, len) in [(end, 1), (0, u64::from(LENGTH_LIMIT))] {
            let mut copy = records.clone();
            let mut wide = record("wide");
            wide.checksum = None;
            wide.len = len;
            let inserted = copy.insert(offset, wide).unwrap();
            assert!(matches!(copy.rows, Rows::Wide(_)));
            let meta = copy.metadata(inserted).unwrap();
            assert_eq!((meta.offset, meta.len), (offset, len));
            copy.remove(&offset).unwrap();
            let meta = copy.metadata(0).unwrap();
            assert_eq!(meta.offset, u64::from(OFFSET_LIMIT - 1));
            assert_eq!(meta.len, u64::from(LENGTH_LIMIT - 1));
        }
    }

    #[test]
    fn out_of_order_extents_preserve_identity_and_boundaries() {
        let mut records = Records::default();
        let expected = record("路径/é");
        records.insert(20, expected.clone()).unwrap();
        records.insert(0, record("first")).unwrap();
        assert_eq!(records.get(1).unwrap().1.to_owned().key, expected.key);
        assert_eq!(records.get(1).unwrap().1.checksum, Some(u64::MAX));
        assert!(records.overlaps(15, 6));
        assert!(!records.overlaps(10, 10));
        assert!(records.crosses(25));
        assert!(!records.crosses(20));
        assert_eq!(records.before(20).count(), 1);
        records.remove(&0).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records.get(0).unwrap().0, 20);
        records.retain_before(20);
        assert!(records.is_empty());
    }

    #[test]
    fn exact_offsets_and_kinds_survive_compaction_and_snapshot() {
        for source_offset in [0, 1 << 40] {
            let mut records = Records::default();
            let mut ordinary = record("same-key");
            let mut block = ordinary.key.materialize();
            block.block_size = 16;
            ordinary.key = SmallKey::new(&block);
            ordinary.len = 8;
            records.insert(0, ordinary.clone()).unwrap();
            block.offset = source_offset;
            let exact = crate::eviction::CacheUnit::ExactRange(block);
            records
                .insert(
                    16,
                    Record {
                        key: SmallKey::new(&exact),
                        len: 16,
                        checksum: Some(7),
                    },
                )
                .unwrap();
            records.seal();
            let snapshot = records.clone();
            assert_ne!(records.get(0).unwrap().1.key, records.get(1).unwrap().1.key);
            records.remove(&0).unwrap();
            assert_eq!(records.get(0).unwrap().1.key.view().cache_unit(), exact);
            assert_eq!(snapshot.get(1).unwrap().1.key.view().cache_unit(), exact);
            assert_eq!(snapshot.get(0).unwrap().1.to_owned().key, ordinary.key);
            records.retain_before(16);
            assert!(records.is_empty());
            records.insert(16, ordinary.clone()).unwrap();
            assert_eq!(records.get(0).unwrap().1.to_owned().key, ordinary.key);
        }
    }

    #[test]
    fn maximum_key_and_extent_are_lossless() {
        for byte in ["x", "\0"] {
            let mut records = Records::default();
            let mut value = record("");
            let key = value.key.view();
            let path = byte.repeat(format::MAX_KEY - format::key_len(&key));
            value = record(&path);
            value.len = 128 << 20;
            let slot = records
                .insert((1 << 30) - value.len, value.clone())
                .unwrap();
            records.seal();
            let restored = records.get(slot).unwrap().1.to_owned();
            assert_eq!(restored.key, value.key);
            assert_eq!(restored.len, value.len);
            assert!(records.insert(1 << 30, record("bad")).is_err());
        }
    }

    #[test]
    fn compressed_keys_survive_reordering_deletion_and_snapshot_clones() {
        let mut records = Records::default();
        let mut expected = std::collections::BTreeMap::new();
        for i in (0..401_u64).rev() {
            let value = record(&format!("路径/{:012}", i));
            records.insert(i * 20, value.clone()).unwrap();
            expected.insert(i * 20, value);
        }
        records.seal();
        let snapshot = records.clone();
        for i in (0..401_u64).step_by(7) {
            records.remove(&(i * 20)).unwrap();
            expected.remove(&(i * 20));
        }
        for i in (0..401_u64).step_by(7) {
            let value = record(&format!("replacement/{i:012}"));
            records.insert(i * 20, value.clone()).unwrap();
            expected.insert(i * 20, value);
        }
        for (row, ((offset, actual), (&want_offset, want))) in
            records.iter().zip(&expected).enumerate()
        {
            assert_eq!(offset, want_offset);
            assert_eq!(actual.key, want.key);
            assert_eq!(records.get(row).unwrap().1.key, want.key);
            assert!(records.matches(row, &key_codec::encode(&want.key)));
            assert_eq!(records.metadata(row).unwrap().hash, key_hash(&want.key));
            assert_eq!(
                records.metadata(row).unwrap().stored_len(),
                want.stored_len()
            );
        }
        assert_eq!(records.len(), expected.len());
        assert_eq!(snapshot.len(), 401);
        assert_eq!(
            snapshot.get(0).unwrap().1.key,
            record("路径/000000000000").key
        );
        records.retain_before(1000);
        assert_eq!(records.len(), 50);
        assert_eq!(records.iter().count(), 50);
    }

    #[test]
    fn duplicate_keys_remain_distinct_by_extent() {
        let mut records = Records::default();
        let value = record("same");
        for i in 0..300 {
            records.insert(i * 10, value.clone()).unwrap();
        }
        records.seal();
        assert_eq!(records.len(), 300);
        for (row, (offset, actual)) in records.iter().enumerate() {
            assert_eq!(offset, row as u64 * 10);
            assert_eq!(actual.key, value.key);
        }
        records.remove(&1280).unwrap();
        assert_eq!(records.len(), 299);
        assert_eq!(records.iter().count(), 299);
    }

    #[test]
    fn path_prefixes_are_shared_across_range_offsets_sizes_and_versions() {
        let mut records = Records::default();
        let prefix = "路径/".repeat(100);
        let mut expected = Vec::new();
        for i in 0..64_u64 {
            let key = crate::eviction::CacheUnit::ExactRange(BlockId::new(
                ObjectId::new(Backend::Azure, "桶\0", format!("{prefix}{i}")),
                i << 40,
                i as u32 + 1,
                Version::new(format!("版本\0{i}")),
            ));
            let value = Record {
                key: SmallKey::new(&key),
                len: i + 1,
                checksum: Some(i),
            };
            records.insert(i * 100, value.clone()).unwrap();
            expected.push(value);
        }
        records.seal();
        for (row, value) in expected.iter().enumerate() {
            assert!(records.keys.get(row).unwrap().0.len() >= prefix.len());
            assert_eq!(records.get(row).unwrap().1.key, value.key);
            assert!(records.matches(row, &key_codec::encode(&value.key)));
            assert!(!records.matches(
                row,
                &key_codec::encode(&expected[(row + 1) % expected.len()].key)
            ));
        }
        records.remove(&100).unwrap();
        assert!(!records.matches(1, &key_codec::encode(&expected[1].key)));
    }
}
