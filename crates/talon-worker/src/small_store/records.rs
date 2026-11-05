use super::{format, journal::Record};
use crate::block_key::{BlockIdentity, BlockKey, BlockRef, ObjectRef};
use std::io;
use talon_core::Backend;

const WIDTH: usize = 30;

#[derive(Clone, Default)]
pub(super) struct Records {
    rows: Vec<[u8; WIDTH]>,
    text: Vec<u8>,
    live: usize,
}

#[derive(Clone, Copy)]
pub(super) struct RecordRef<'a> {
    pub key: BlockRef<'a>,
    pub len: u64,
    pub checksum: u64,
}

impl RecordRef<'_> {
    pub fn to_owned(self) -> Record {
        Record {
            key: BlockKey::new(&self.key),
            len: self.len,
            checksum: self.checksum,
        }
    }
}

fn n32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}

fn n16(bytes: &[u8]) -> usize {
    u16::from_le_bytes(bytes.try_into().unwrap()) as usize
}

impl Records {
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.live
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    pub fn get(&self, row: usize) -> Option<(u64, RecordRef<'_>)> {
        let row = self.rows.get(row)?;
        let length = n32(&row[4..8]);
        let len = length & ((1 << 28) - 1);
        if len == 0 {
            return None;
        }
        let backend = match length >> 28 {
            0 => Backend::S3,
            1 => Backend::Gcs,
            2 => Backend::Azure,
            _ => unreachable!("validated small-object backend"),
        };
        let start = n32(&row[16..20]) as usize;
        let end = start + n16(&row[24..26]);
        let bucket = start + n16(&row[26..28]);
        let path = start + n16(&row[28..30]);
        let string = |a, b| std::str::from_utf8(&self.text[a..b]).unwrap();
        Some((
            u64::from(n32(&row[..4])),
            RecordRef {
                key: BlockRef {
                    object: ObjectRef {
                        backend,
                        bucket: string(start, bucket),
                        object_path: string(bucket, path),
                    },
                    offset: 0,
                    block_size: n32(&row[20..24]),
                    version: string(path, end),
                },
                len: u64::from(len),
                checksum: u64::from_le_bytes(row[8..16].try_into().unwrap()),
            },
        ))
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, RecordRef<'_>)> {
        (0..self.rows.len()).filter_map(|row| self.get(row))
    }

    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = RecordRef<'_>> {
        self.iter().map(|(_, record)| record)
    }

    pub fn before(&self, end: u64) -> impl Iterator<Item = (u64, RecordRef<'_>)> {
        self.iter().filter(move |(offset, _)| *offset < end)
    }

    pub fn overlaps(&self, offset: u64, len: u64) -> bool {
        self.iter()
            .any(|(other, record)| other < offset + len && offset < other + record.len)
    }

    pub fn crosses(&self, end: u64) -> bool {
        self.before(end)
            .any(|(offset, record)| offset + record.len > end)
    }

    pub fn insert(&mut self, offset: u64, record: Record) -> io::Result<usize> {
        format::validate_key(&record.key)?;
        let key = record.key.view();
        if key.offset != 0
            || offset >= 1 << 30
            || record.len == 0
            || record.len > 128 << 20
            || offset + record.len > 1 << 30
        {
            return Err(format::invalid("invalid small-object record extent"));
        }
        let backend = match key.object.backend {
            Backend::S3 => 0,
            Backend::Gcs => 1,
            Backend::Azure => 2,
        };
        let length = key.object.bucket.len() + key.object.object_path.len() + key.version.len();
        let start = u32::try_from(self.text.len())
            .map_err(|_| format::invalid("small-object key arena exhausted"))?;
        if self
            .text
            .len()
            .checked_add(length)
            .is_none_or(|end| end > u32::MAX as usize)
        {
            return Err(format::invalid("small-object key arena exhausted"));
        }
        let mut row = [0; WIDTH];
        row[..4].copy_from_slice(&(offset as u32).to_le_bytes());
        row[4..8].copy_from_slice(&((record.len as u32) | (backend << 28)).to_le_bytes());
        row[8..16].copy_from_slice(&record.checksum.to_le_bytes());
        row[16..20].copy_from_slice(&start.to_le_bytes());
        row[20..24].copy_from_slice(&key.block_size.to_le_bytes());
        row[24..26].copy_from_slice(&(length as u16).to_le_bytes());
        row[26..28].copy_from_slice(&(key.object.bucket.len() as u16).to_le_bytes());
        row[28..30].copy_from_slice(
            &((key.object.bucket.len() + key.object.object_path.len()) as u16).to_le_bytes(),
        );
        let needed = self.text.len() + length;
        if needed > self.text.capacity() {
            self.text
                .reserve_exact(needed.div_ceil(4096) * 4096 - self.text.len());
        }
        if self.rows.len() == self.rows.capacity() {
            self.rows.reserve_exact(64);
        }
        self.text.extend_from_slice(key.object.bucket.as_bytes());
        self.text
            .extend_from_slice(key.object.object_path.as_bytes());
        self.text.extend_from_slice(key.version.as_bytes());
        let index = self.rows.len();
        self.rows.push(row);
        self.live += 1;
        Ok(index)
    }

    pub fn remove(&mut self, offset: &u64) -> Option<()> {
        let row = self.rows.iter_mut().find(|row| {
            u64::from(n32(&row[..4])) == *offset && n32(&row[4..8]) & ((1 << 28) - 1) != 0
        })?;
        row[4..8].fill(0);
        self.live -= 1;
        if (self.rows.len() - self.live) * 100 >= self.rows.len() {
            self.compact();
        }
        Some(())
    }

    pub fn retain_before(&mut self, end: u64) {
        for row in &mut self.rows {
            if u64::from(n32(&row[..4])) >= end && n32(&row[4..8]) & ((1 << 28) - 1) != 0 {
                row[4..8].fill(0);
                self.live -= 1;
            }
        }
        self.compact();
    }

    pub fn slots(&self) -> usize {
        self.rows.len()
    }

    pub fn shrink_to_fit(&mut self) {
        self.rows.shrink_to_fit();
        self.text.shrink_to_fit();
    }

    fn compact(&mut self) {
        let live = |row: &&[u8; WIDTH]| n32(&row[4..8]) & ((1 << 28) - 1) != 0;
        let bytes = self
            .rows
            .iter()
            .filter(live)
            .map(|row| n16(&row[24..26]))
            .sum();
        let mut rows = Vec::with_capacity(self.live);
        let mut text = Vec::with_capacity(bytes);
        for row in self.rows.iter().filter(live) {
            let start = n32(&row[16..20]) as usize;
            let end = start + n16(&row[24..26]);
            let mut row = *row;
            row[16..20].copy_from_slice(&(text.len() as u32).to_le_bytes());
            text.extend_from_slice(&self.text[start..end]);
            rows.push(row);
        }
        self.rows = rows;
        self.text = text;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{BlockId, ObjectId, Version};

    fn record(path: &str) -> Record {
        Record {
            key: BlockKey::new(&BlockId::new(
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
        let slot = records.insert(20, expected.clone()).unwrap();
        records.insert(0, record("first")).unwrap();
        assert_eq!(records.get(slot).unwrap().1.to_owned().key, expected.key);
        assert_eq!(records.get(slot).unwrap().1.checksum, u64::MAX);
        assert!(records.overlaps(15, 6));
        assert!(!records.overlaps(10, 10));
        assert!(records.crosses(25));
        assert!(!records.crosses(20));
        assert_eq!(records.before(20).count(), 1);
        records.remove(&0).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records.get(slot).unwrap().0, 20);
        records.retain_before(20);
        assert!(records.is_empty());
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
}
