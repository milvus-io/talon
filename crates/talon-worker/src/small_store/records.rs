use super::{format, journal::Record, key_codec};
use crate::small_store::key::SmallIdentity;
use fast_radix_trie::RadixMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::io;
use std::sync::OnceLock;

const WIDTH: usize = 26;
pub(super) const KEY_GROUP: usize = 128;

#[derive(Clone, Default)]
pub(super) struct Records {
    rows: Vec<[u8; WIDTH]>,
    keys: Vec<RadixMap<()>>,
    live: usize,
}

#[derive(Clone, Copy)]
pub(super) struct Metadata {
    pub offset: u64,
    pub len: u64,
    pub checksum: u64,
    pub hash: u64,
    pub charge: u64,
}

pub(super) fn key_hash(key: &dyn SmallIdentity) -> u64 {
    static HASH: OnceLock<RandomState> = OnceLock::new();
    HASH.get_or_init(RandomState::new).hash_one(key.view())
}

fn n32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}

impl Records {
    pub fn len(&self) -> usize {
        self.live
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    pub fn metadata(&self, row: usize) -> Option<Metadata> {
        let row = self.rows.get(row)?;
        let len = u64::from(n32(&row[4..8]));
        (len != 0).then(|| Metadata {
            offset: u64::from(n32(&row[..4])),
            len,
            checksum: u64::from_le_bytes(row[8..16].try_into().unwrap()),
            hash: u64::from_le_bytes(row[16..24].try_into().unwrap()),
            charge: len + u64::from(u16::from_le_bytes(row[24..26].try_into().unwrap())) + 25 + 37,
        })
    }

    pub fn matches(&self, row: usize, key: &mut Vec<u8>) -> bool {
        let Some(keys) = self.keys.get(row / KEY_GROUP) else {
            return false;
        };
        key.push((row % KEY_GROUP) as u8);
        let found = keys.contains_key(&*key);
        key.pop();
        found
    }

    fn encoded(&self, row: usize) -> Option<Vec<u8>> {
        self.keys
            .get(row / KEY_GROUP)?
            .keys()
            .find_map(|mut key| (key.pop()? as usize == row % KEY_GROUP).then_some(key))
    }

    pub fn get(&self, row: usize) -> Option<(u64, Record)> {
        let meta = self.metadata(row)?;
        Some((
            meta.offset,
            Record {
                key: key_codec::decode(&self.encoded(row).expect("small-object key")),
                len: meta.len,
                checksum: meta.checksum,
            },
        ))
    }

    pub fn batch(&self, start: usize) -> Vec<(u64, Record)> {
        assert_eq!(start % KEY_GROUP, 0);
        let Some(keys) = self.keys.get(start / KEY_GROUP) else {
            return Vec::new();
        };
        let mut records: Vec<_> = keys
            .keys()
            .filter_map(|mut key| {
                let row = start + usize::from(key.pop().unwrap());
                let meta = self.metadata(row)?;
                Some((
                    row,
                    meta.offset,
                    Record {
                        key: key_codec::decode(&key),
                        len: meta.len,
                        checksum: meta.checksum,
                    },
                ))
            })
            .collect();
        records.sort_unstable_by_key(|(row, _, _)| *row);
        records
            .into_iter()
            .map(|(_, offset, record)| (offset, record))
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, Record)> + '_ {
        (0..self.rows.len())
            .step_by(KEY_GROUP)
            .flat_map(|start| self.batch(start))
    }

    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = Record> + '_ {
        self.iter().map(|(_, record)| record)
    }

    pub fn before(&self, end: u64) -> impl Iterator<Item = (u64, Record)> + '_ {
        self.iter().take_while(move |(offset, _)| *offset < end)
    }

    pub fn overlaps(&self, offset: u64, len: u64) -> bool {
        let next = self
            .rows
            .partition_point(|row| u64::from(n32(&row[..4])) < offset);
        let live = |row: &&[u8; WIDTH]| n32(&row[4..8]) != 0;
        self.rows[..next]
            .iter()
            .rev()
            .find(live)
            .is_some_and(|row| u64::from(n32(&row[..4])) + u64::from(n32(&row[4..8])) > offset)
            || self.rows[next..]
                .iter()
                .find(live)
                .is_some_and(|row| u64::from(n32(&row[..4])) < offset + len)
    }

    pub fn crosses(&self, end: u64) -> bool {
        let next = self
            .rows
            .partition_point(|row| u64::from(n32(&row[..4])) < end);
        self.rows[..next]
            .iter()
            .rev()
            .find(|row| n32(&row[4..8]) != 0)
            .is_some_and(|row| u64::from(n32(&row[..4])) + u64::from(n32(&row[4..8])) > end)
    }

    pub fn insert(&mut self, offset: u64, record: Record) -> io::Result<usize> {
        format::validate_key(&record.key)?;
        let key = record.key.view();
        if !key.valid_extent(record.len)
            || offset >= 1 << 30
            || record.len == 0
            || record.len > 128 << 20
            || offset + record.len > 1 << 30
        {
            return Err(format::invalid("invalid small-object record extent"));
        }
        let mut row = [0; WIDTH];
        row[..4].copy_from_slice(&(offset as u32).to_le_bytes());
        row[4..8].copy_from_slice(&(record.len as u32).to_le_bytes());
        row[8..16].copy_from_slice(&record.checksum.to_le_bytes());
        row[16..24].copy_from_slice(&key_hash(&record.key).to_le_bytes());
        row[24..26].copy_from_slice(&((format::key_len(&record.key) - 25) as u16).to_le_bytes());
        let key = key_codec::encode(&record.key);
        let vacant = self.position(offset);
        if self
            .rows
            .get(vacant)
            .is_some_and(|row| u64::from(n32(&row[..4])) == offset && n32(&row[4..8]) == 0)
        {
            self.rows[vacant] = row;
            self.insert_key(vacant, key);
            self.live += 1;
            return Ok(vacant);
        }
        let index = self
            .rows
            .partition_point(|row| u64::from(n32(&row[..4])) <= offset);
        if self.rows.len() == self.rows.capacity() {
            self.rows.reserve_exact(64);
        }
        if index == self.rows.len() {
            self.rows.push(row);
            self.insert_key(index, key);
        } else {
            let start = index / KEY_GROUP * KEY_GROUP;
            let mut keys = self.encoded_from(start);
            keys.insert(index - start, Some(key));
            self.rows.insert(index, row);
            self.replace_keys(start, keys);
        }
        self.live += 1;
        Ok(index)
    }

    fn insert_key(&mut self, row: usize, mut key: Vec<u8>) {
        while self.keys.len() <= row / KEY_GROUP {
            self.keys.push(RadixMap::new());
        }
        key.push((row % KEY_GROUP) as u8);
        assert!(self.keys[row / KEY_GROUP].insert(key, ()).is_none());
    }

    fn encoded_from(&self, start: usize) -> Vec<Option<Vec<u8>>> {
        let mut keys = vec![None; self.rows.len() - start];
        for (group, trie) in self.keys.iter().enumerate().skip(start / KEY_GROUP) {
            for mut key in trie.keys() {
                let row = group * KEY_GROUP + usize::from(key.pop().unwrap());
                keys[row - start] = Some(key);
            }
        }
        keys
    }

    fn replace_keys(&mut self, start: usize, keys: Vec<Option<Vec<u8>>>) {
        self.keys.truncate(start / KEY_GROUP);
        for (row, key) in keys.into_iter().enumerate() {
            if let Some(key) = key {
                self.insert_key(start + row, key);
            }
        }
    }

    pub fn remove(&mut self, offset: &u64) -> Option<()> {
        let index = self
            .rows
            .partition_point(|row| u64::from(n32(&row[..4])) < *offset);
        let index = (index..self.rows.len())
            .take_while(|&i| u64::from(n32(&self.rows[i][..4])) == *offset)
            .find(|&i| n32(&self.rows[i][4..8]) != 0)?;
        self.rows[index][4..8].fill(0);
        self.live -= 1;
        if (self.rows.len() - self.live) * 100 >= self.rows.len() {
            self.compact();
        } else {
            let mut key = self.encoded(index).expect("small-object key");
            key.push((index % KEY_GROUP) as u8);
            self.keys[index / KEY_GROUP].remove(key);
        }
        Some(())
    }

    pub fn retain_before(&mut self, end: u64) {
        for row in &mut self.rows {
            if u64::from(n32(&row[..4])) >= end && n32(&row[4..8]) != 0 {
                row[4..8].fill(0);
                self.live -= 1;
            }
        }
        self.compact();
    }

    pub fn slots(&self) -> usize {
        self.rows.len()
    }

    pub fn position(&self, offset: u64) -> usize {
        self.rows
            .partition_point(|row| u64::from(n32(&row[..4])) < offset)
    }

    pub fn compacts_after_remove(&self) -> bool {
        (self.rows.len() - self.live + 1) * 100 >= self.rows.len()
    }

    pub fn shrink_to_fit(&mut self) {
        self.rows.shrink_to_fit();
        self.keys.shrink_to_fit();
    }

    fn compact(&mut self) {
        let keys = self
            .encoded_from(0)
            .into_iter()
            .zip(&self.rows)
            .filter_map(|(key, row)| (n32(&row[4..8]) != 0).then_some(key))
            .collect();
        self.rows.retain(|row| n32(&row[4..8]) != 0);
        self.replace_keys(0, keys);
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
            checksum: u64::MAX,
        }
    }

    #[test]
    fn out_of_order_extents_preserve_identity_and_boundaries() {
        let mut records = Records::default();
        let expected = record("路径/é");
        records.insert(20, expected.clone()).unwrap();
        records.insert(0, record("first")).unwrap();
        assert_eq!(records.get(1).unwrap().1.to_owned().key, expected.key);
        assert_eq!(records.get(1).unwrap().1.checksum, u64::MAX);
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
                        checksum: 7,
                    },
                )
                .unwrap();
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
        let mut records = Records::default();
        let mut value = record("");
        let key = value.key.view();
        let path = "x".repeat(format::MAX_KEY - format::key_len(&key));
        value = record(&path);
        value.len = 128 << 20;
        let slot = records
            .insert((1 << 30) - value.len, value.clone())
            .unwrap();
        let restored = records.get(slot).unwrap().1.to_owned();
        assert_eq!(restored.key, value.key);
        assert_eq!(restored.len, value.len);
        assert!(records.insert(1 << 30, record("bad")).is_err());
    }

    #[test]
    fn grouped_keys_survive_reordering_deletion_and_snapshot_clones() {
        let mut records = Records::default();
        let mut expected = std::collections::BTreeMap::new();
        for i in (0..401_u64).rev() {
            let value = record(&format!("路径/{:012}", i));
            records.insert(i * 20, value.clone()).unwrap();
            expected.insert(i * 20, value);
        }
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
            assert!(records.matches(row, &mut key_codec::encode(&want.key)));
            assert_eq!(records.metadata(row).unwrap().hash, key_hash(&want.key));
            assert_eq!(
                records.metadata(row).unwrap().charge,
                super::super::charge(&want.key, want.len)
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
        assert_eq!(records.len(), 300);
        for (row, (offset, actual)) in records.iter().enumerate() {
            assert_eq!(offset, row as u64 * 10);
            assert_eq!(actual.key, value.key);
        }
        records.remove(&1280).unwrap();
        assert_eq!(records.len(), 299);
        assert_eq!(records.iter().count(), 299);
    }
}
