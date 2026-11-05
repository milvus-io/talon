//! The writer's checkpointable metadata state. Segment maps use copy-on-write
//! so a snapshot can be encoded off the I/O thread while mutations continue.
use super::super::records::Records;
use super::*;
use crate::small_store::key::SmallKey;

#[derive(Clone)]
pub(crate) struct Record {
    pub key: SmallKey,
    pub len: u64,
    pub checksum: u64,
}

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
            8 + self
                .records
                .before(self.durable_end)
                .map(|(_, r)| 37 + format::key_len(&r.key) as u64)
                .sum::<u64>()
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Model {
    pub segments: BTreeMap<u64, Arc<SegmentMeta>>,
    pub next: u64,
}

pub(crate) enum Event {
    Insert {
        slot: usize,
        segment: u64,
        offset: u64,
        record: Record,
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
                b.push(0);
                for n in [*slot as u64, *segment, *offset, record.len, record.checksum] {
                    b.extend_from_slice(&n.to_le_bytes());
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
            0 => {
                let slot = usize::try_from(format::u64(&mut b)?)
                    .map_err(|_| invalid("stream slot overflow"))?;
                let segment = format::u64(&mut b)?;
                let offset = format::u64(&mut b)?;
                let len = format::u64(&mut b)?;
                let checksum = format::u64(&mut b)?;
                let id = format::view(b)?;
                if len > 128 << 20 || !id.valid_extent(len) {
                    return Err(invalid("journal extent"));
                }
                let key = SmallKey::new(&id);
                b = &[];
                Self::Insert {
                    slot,
                    segment,
                    offset,
                    record: Record { key, len, checksum },
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
                let end = offset
                    .checked_add(record.len)
                    .ok_or_else(|| invalid("extent overflow"))?;
                if s.records.overlaps(offset, record.len) {
                    return Err(invalid("overlapping journal extent"));
                }
                s.bytes = s.bytes.max(end);
                s.metadata += 37 + format::key_len(&record.key) as u64;
                s.records.insert(offset, record)?;
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
                let s = Arc::make_mut(
                    self.segments
                        .get_mut(&segment)
                        .ok_or_else(|| invalid("seal missing segment"))?,
                );
                s.durable_end = s.bytes;
                s.sealed = true;
                s.records.shrink_to_fit();
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
                s.records.shrink_to_fit();
            }
            Event::Replace { old, new } => {
                if let Some(new) = new {
                    if old == new {
                        return Err(invalid("self relocation"));
                    }
                    let s = Arc::make_mut(
                        self.segments
                            .get_mut(&new)
                            .ok_or_else(|| invalid("replacement missing segment"))?,
                    );
                    s.durable_end = s.bytes;
                    s.sealed = true;
                    s.records.shrink_to_fit();
                }
                // Empty standby/active files have no Insert record.
                self.segments.remove(&old);
            }
        }
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

/// Frozen segment maps keep key buffers shared while the CPU encoder walks them.
pub(crate) struct Snapshot {
    next: Option<u64>,
    segments: std::collections::btree_map::IntoIter<u64, Arc<SegmentMeta>>,
    current: Option<(u64, Arc<SegmentMeta>, usize)>,
    batch: std::vec::IntoIter<(u64, Record)>,
}
pub(crate) enum SnapshotRecord {
    Next(u64),
    Segment(u64, SegmentMeta),
    Insert(Event),
}
impl Iterator for Snapshot {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.next.take() {
            let mut b = vec![5];
            b.extend_from_slice(&next.to_le_bytes());
            return Some(Ok(b.into()));
        }
        if let Some((id, s, after)) = &mut self.current {
            while *after < s.records.slots() || self.batch.len() != 0 {
                if self.batch.len() == 0 {
                    self.batch = s.records.batch(*after).into_iter();
                    *after += super::super::records::KEY_GROUP;
                }
                let Some((offset, r)) = self.batch.next() else {
                    continue;
                };
                return Some(
                    Event::Insert {
                        slot: s.slot,
                        segment: *id,
                        offset,
                        record: r,
                    }
                    .encode()
                    .map(Bytes::from),
                );
            }
            self.current = None;
        }
        let (id, s) = self.segments.next()?;
        let mut b = vec![8];
        for n in [id, s.slot as u64, s.bytes, s.metadata] {
            b.extend_from_slice(&n.to_le_bytes());
        }
        b.push(u8::from(s.sealed));
        b.extend_from_slice(&s.durable_end.to_le_bytes());
        self.current = Some((id, s, 0));
        Some(Ok(b.into()))
    }
}
impl talon_journal::Machine for Model {
    type Record = Event;
    type SnapshotRecord = SnapshotRecord;
    type Snapshot = Snapshot;
    fn encoded_len(event: &Event) -> io::Result<usize> {
        Ok(match event {
            Event::Insert { record, .. } => 41 + format::key_len(&record.key),
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
    fn snapshot(&self) -> io::Result<Snapshot> {
        Ok(Snapshot {
            next: Some(self.next),
            segments: self.segments.clone().into_iter(),
            current: None,
            batch: Vec::new().into_iter(),
        })
    }
    fn decode_snapshot(bytes: &[u8]) -> io::Result<SnapshotRecord> {
        let mut b = bytes;
        let kind = take_byte(&mut b)?;
        match kind {
            5 => {
                let next = format::u64(&mut b)?;
                if !b.is_empty() || next == 0 {
                    return Err(invalid("snapshot allocator"));
                }
                Ok(SnapshotRecord::Next(next))
            }
            4 | 8 => {
                let id = format::u64(&mut b)?;
                let slot =
                    usize::try_from(format::u64(&mut b)?).map_err(|_| invalid("slot overflow"))?;
                let bytes = format::u64(&mut b)?;
                let metadata = format::u64(&mut b)?;
                let sealed = match take_byte(&mut b)? {
                    0 => false,
                    1 => true,
                    _ => return Err(invalid("snapshot seal")),
                };
                let durable_end = if kind == 8 {
                    format::u64(&mut b)?
                } else if sealed {
                    bytes
                } else {
                    0
                };
                if !b.is_empty() || durable_end > bytes || (sealed && durable_end != bytes) {
                    return Err(invalid("invalid snapshot segment header"));
                }
                Ok(SnapshotRecord::Segment(
                    id,
                    SegmentMeta {
                        slot,
                        bytes,
                        durable_end,
                        metadata,
                        sealed,
                        records: Records::default(),
                    },
                ))
            }
            0 => Ok(SnapshotRecord::Insert(Event::decode(bytes)?)),
            _ => Err(invalid("snapshot operation")),
        }
    }
    fn restore(&mut self, record: SnapshotRecord) -> io::Result<()> {
        match record {
            SnapshotRecord::Next(next) => {
                if !self.segments.is_empty() {
                    return Err(invalid("snapshot allocator order"));
                }
                self.next = next;
            }
            SnapshotRecord::Segment(id, s) => {
                if id == 0 || id >= self.next || self.segments.insert(id, Arc::new(s)).is_some() {
                    return Err(invalid("snapshot segment"));
                }
            }
            SnapshotRecord::Insert(Event::Insert {
                slot,
                segment,
                offset,
                record,
            }) => {
                let s = Arc::make_mut(
                    self.segments
                        .get_mut(&segment)
                        .ok_or_else(|| invalid("snapshot missing segment"))?,
                );
                if s.slot != slot
                    || offset
                        .checked_add(record.len)
                        .map_or(true, |end| end > s.bytes)
                    || (offset < s.durable_end && offset + record.len > s.durable_end)
                    || s.records.overlaps(offset, record.len)
                {
                    return Err(invalid("snapshot extent"));
                }
                s.records.insert(offset, record)?;
            }
            _ => return Err(invalid("snapshot insert")),
        }
        Ok(())
    }
}
