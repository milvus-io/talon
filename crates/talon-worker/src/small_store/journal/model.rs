//! The writer's checkpointable metadata state. Segment maps use copy-on-write
//! so a snapshot can be encoded off the I/O thread while mutations continue.
use super::super::{
    checksum_len, key_codec,
    records::{key_hash, Records},
};
use super::*;
use crate::small_store::key::SmallKey;

#[derive(Clone)]
pub(crate) struct Record {
    pub key: SmallKey,
    pub len: u64,
    /// Legacy records carry the checksum here. New records have an on-disk trailer.
    pub checksum: Option<u64>,
}

impl Record {
    #[cfg(test)]
    pub(in crate::small_store) fn stored_len(&self) -> u64 {
        self.len + checksum_len(self.checksum)
    }
}

const INSERT_LEGACY: u8 = 0;
const INSERT_WITH_TRAILER: u8 = 9;

#[derive(Clone)]
pub(crate) struct SegmentMeta {
    pub slot: usize,
    pub bytes: u64,
    pub durable_end: u64,
    pub metadata: u64,
    pub sealed: bool,
    pub records: Records,
}

impl SegmentMeta {
    pub fn recovered_metadata(&self) -> u64 {
        if self.sealed {
            self.metadata
        } else {
            let mut scratch = Vec::new();
            8 + (0..self.records.position(self.durable_end))
                .filter_map(|row| self.records.metadata(row).map(|meta| (row, meta)))
                .map(|(row, meta)| {
                    let key = self.records.encoded_key(row, &mut scratch);
                    format::metadata_bytes_for_key_len(key_codec::logical_len(key))
                        - checksum_len(meta.checksum)
                })
                .sum::<u64>()
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Model {
    pub segments: BTreeMap<u64, Arc<SegmentMeta>>,
    pub next: u64,
    pub(in crate::small_store) snapshot_restore: snapshot::Restore,
}

pub(crate) struct EncodedRecord {
    pub(super) key: Vec<u8>,
    pub(super) len: u64,
    pub(super) checksum: Option<u64>,
    pub(super) hash: u32,
    key_len: usize,
}

pub(crate) enum Event {
    Insert {
        slot: usize,
        segment: u64,
        offset: u64,
        record: Record,
    },
    ReplayInsert {
        slot: usize,
        segment: u64,
        offset: u64,
        record: EncodedRecord,
    },
    Delete {
        segment: u64,
        offset: u64,
    },
    Seal {
        segment: u64,
    },
    Flush {
        segment: u64,
        durable_end: u64,
    },
    Recover {
        segment: u64,
    },
    SetNext {
        next: u64,
    },
    Replace {
        old: u64,
        new: Option<u64>,
    },
}

impl Event {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut b = Vec::new();
        match self {
            Self::ReplayInsert { .. } => return Err(invalid("cannot append a decoded insert")),
            Self::SetNext { next } => {
                b.push(5);
                b.extend_from_slice(&next.to_le_bytes());
            }
            Self::Insert {
                slot,
                segment,
                offset,
                record,
            } => {
                b.push(if record.checksum.is_some() {
                    INSERT_LEGACY
                } else {
                    INSERT_WITH_TRAILER
                });
                for n in [*slot as u64, *segment, *offset, record.len] {
                    b.extend_from_slice(&n.to_le_bytes());
                }
                if let Some(checksum) = record.checksum {
                    b.extend_from_slice(&checksum.to_le_bytes());
                }
                format::encode_key(&mut b, &record.key)?;
            }
            Self::Delete { segment, offset } => {
                b.push(1);
                b.extend_from_slice(&segment.to_le_bytes());
                b.extend_from_slice(&offset.to_le_bytes());
            }
            Self::Seal { segment } => {
                b.push(2);
                b.extend_from_slice(&segment.to_le_bytes());
            }
            Self::Flush {
                segment,
                durable_end,
            } => {
                b.push(6);
                b.extend_from_slice(&segment.to_le_bytes());
                b.extend_from_slice(&durable_end.to_le_bytes());
            }
            Self::Recover { segment } => {
                b.push(7);
                b.extend_from_slice(&segment.to_le_bytes());
            }
            Self::Replace { old, new } => {
                b.push(3);
                b.extend_from_slice(&old.to_le_bytes());
                b.extend_from_slice(&new.unwrap_or(0).to_le_bytes());
            }
        }
        Ok(b)
    }

    pub fn decode(mut b: &[u8]) -> io::Result<Self> {
        let kind = take_byte(&mut b)?;
        let event = match kind {
            5 => Self::SetNext {
                next: format::u64(&mut b)?,
            },
            INSERT_LEGACY | INSERT_WITH_TRAILER => {
                let slot = usize::try_from(format::u64(&mut b)?)
                    .map_err(|_| invalid("stream slot overflow"))?;
                let segment = format::u64(&mut b)?;
                let offset = format::u64(&mut b)?;
                let len = format::u64(&mut b)?;
                let checksum = if kind == INSERT_LEGACY {
                    Some(format::u64(&mut b)?)
                } else {
                    None
                };
                let id = format::view(b)?;
                if len > 128 << 20 || !id.valid_extent(len) {
                    return Err(invalid("journal extent"));
                }
                let record = EncodedRecord {
                    key: key_codec::encode(&id),
                    key_len: b.len(),
                    hash: key_hash(&id),
                    len,
                    checksum,
                };
                b = &[];
                Self::ReplayInsert {
                    slot,
                    segment,
                    offset,
                    record,
                }
            }
            1 => Self::Delete {
                segment: format::u64(&mut b)?,
                offset: format::u64(&mut b)?,
            },
            2 => Self::Seal {
                segment: format::u64(&mut b)?,
            },
            6 => Self::Flush {
                segment: format::u64(&mut b)?,
                durable_end: format::u64(&mut b)?,
            },
            7 => Self::Recover {
                segment: format::u64(&mut b)?,
            },
            3 => {
                let old = format::u64(&mut b)?;
                let new = format::u64(&mut b)?;
                Self::Replace {
                    old,
                    new: (new != 0).then_some(new),
                }
            }
            _ => return Err(invalid("journal operation")),
        };
        if !b.is_empty() {
            return Err(invalid("journal trailing bytes"));
        }
        Ok(event)
    }
}

impl Model {
    pub fn apply(&mut self, event: Event) -> io::Result<()> {
        match event {
            Event::SetNext { next } => {
                if next == 0 {
                    return Err(invalid("zero segment allocator"));
                }
                self.next = self.next.max(next);
            }
            Event::Insert {
                slot,
                segment,
                offset,
                record,
            } => {
                use super::super::key::SmallIdentity;
                format::validate_key(&record.key)?;
                if !record.key.view().valid_extent(record.len) || record.len > 128 << 20 {
                    return Err(invalid("journal extent"));
                }
                self.insert(
                    slot,
                    segment,
                    offset,
                    EncodedRecord {
                        key: key_codec::encode(&record.key),
                        key_len: format::key_len(&record.key),
                        hash: key_hash(&record.key),
                        len: record.len,
                        checksum: record.checksum,
                    },
                )?;
            }
            Event::ReplayInsert {
                slot,
                segment,
                offset,
                record,
            } => {
                self.insert(slot, segment, offset, record)?;
            }
            Event::Delete { segment, offset } => {
                let s = Arc::make_mut(
                    self.segments
                        .get_mut(&segment)
                        .ok_or_else(|| invalid("delete missing segment"))?,
                );
                if s.records.remove(&offset).is_none() {
                    return Err(invalid("delete missing record"));
                }
                s.metadata += 21;
            }
            Event::Seal { segment } => {
                let s = self
                    .segments
                    .get_mut(&segment)
                    .ok_or_else(|| invalid("seal missing segment"))?;
                Self::seal(s);
            }
            Event::Flush {
                segment,
                durable_end,
            } => {
                let s = Arc::make_mut(
                    self.segments
                        .get_mut(&segment)
                        .ok_or_else(|| invalid("flush missing segment"))?,
                );
                if s.sealed
                    || durable_end == 0
                    || durable_end < s.durable_end
                    || durable_end > s.bytes
                    || s.records.crosses(durable_end)
                {
                    return Err(invalid("invalid durable segment boundary"));
                }
                s.durable_end = durable_end;
            }
            Event::Recover { segment } => {
                let s = Arc::make_mut(
                    self.segments
                        .get_mut(&segment)
                        .ok_or_else(|| invalid("recover missing segment"))?,
                );
                if s.sealed || s.durable_end == 0 {
                    return Err(invalid("invalid segment recovery"));
                }
                s.metadata = s.recovered_metadata();
                s.records.retain_before(s.durable_end);
                s.bytes = s.durable_end;
                s.sealed = true;
                s.records.seal();
            }
            Event::Replace { old, new } => {
                if let Some(new) = new {
                    if old == new {
                        return Err(invalid("self relocation"));
                    }
                    let s = self
                        .segments
                        .get_mut(&new)
                        .ok_or_else(|| invalid("replacement missing segment"))?;
                    Self::seal(s);
                }
                // Empty standby/active files have no Insert record.
                self.segments.remove(&old);
            }
        }
        Ok(())
    }

    fn seal(s: &mut Arc<SegmentMeta>) {
        if !s.sealed {
            let s = Arc::make_mut(s);
            s.durable_end = s.bytes;
            s.sealed = true;
            s.records.seal();
        }
    }

    fn insert(
        &mut self,
        slot: usize,
        segment: u64,
        offset: u64,
        record: EncodedRecord,
    ) -> io::Result<()> {
        if segment == 0 {
            return Err(invalid("zero segment id"));
        }
        self.next = self.next.max(
            segment
                .checked_add(1)
                .ok_or_else(|| invalid("segment id overflow"))?,
        );
        let s = Arc::make_mut(self.segments.entry(segment).or_insert_with(|| {
            Arc::new(SegmentMeta {
                slot,
                bytes: 0,
                durable_end: 0,
                metadata: 8,
                sealed: false,
                records: Records::default(),
            })
        }));
        if s.slot != slot || s.sealed || offset < s.durable_end {
            return Err(invalid("insert into sealed/wrong stream"));
        }
        let stored_len = record.len + checksum_len(record.checksum);
        let end = offset
            .checked_add(stored_len)
            .ok_or_else(|| invalid("extent overflow"))?;
        if s.records.overlaps(offset, stored_len) {
            return Err(invalid("overlapping journal extent"));
        }
        s.records.insert_encoded(
            offset,
            record.len,
            record.checksum,
            record.hash,
            &record.key,
        )?;
        s.bytes = s.bytes.max(end);
        s.metadata +=
            format::metadata_bytes_for_key_len(record.key_len) - checksum_len(record.checksum);
        Ok(())
    }
}
fn take_byte(b: &mut &[u8]) -> io::Result<u8> {
    let (&first, rest) = b
        .split_first()
        .ok_or_else(|| invalid("missing operation"))?;
    *b = rest;
    Ok(first)
}

impl talon_journal::Machine for Model {
    type Record = Event;
    type SnapshotRecord = snapshot::SnapshotRecord;
    type Snapshot = snapshot::Snapshot;
    fn encoded_len(event: &Event) -> io::Result<usize> {
        Ok(match event {
            Event::ReplayInsert { .. } => return Err(invalid("cannot append a decoded insert")),
            Event::Insert { record, .. } => {
                41 + format::key_len(&record.key) - checksum_len(record.checksum) as usize
            }
            Event::Delete { .. } | Event::Replace { .. } | Event::Flush { .. } => 17,
            Event::Seal { .. } | Event::SetNext { .. } | Event::Recover { .. } => 9,
        })
    }
    fn encode(event: &Event) -> io::Result<Bytes> {
        event.encode().map(Bytes::from)
    }
    fn decode(bytes: &[u8]) -> io::Result<Event> {
        Event::decode(bytes)
    }
    fn apply(&mut self, _: u64, event: Event) -> io::Result<()> {
        self.apply(event)
    }
    fn snapshot(&self) -> io::Result<Self::Snapshot> {
        Ok(snapshot::Snapshot::new(self))
    }
    fn decode_snapshot(bytes: &[u8]) -> io::Result<Self::SnapshotRecord> {
        snapshot::decode(bytes)
    }
    fn restore(&mut self, record: Self::SnapshotRecord) -> io::Result<()> {
        snapshot::restore(self, record)
    }
    fn finish_snapshot_restore(&mut self) -> io::Result<()> {
        self.snapshot_restore.finish()?;
        self.snapshot_restore = Default::default();
        Ok(())
    }
}
