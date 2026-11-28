//! Segment headers and bounded row batches share the journal snapshot cut.
//! Each batch carries its own packed key prefixes for parallel decoding.
use super::{
    super::{
        format, key_codec,
        records::{key_hash, Metadata, Records},
    },
    invalid, Model, SegmentMeta,
};
use bytes::Bytes;
use std::{
    collections::{btree_map, hash_map::Entry, HashMap},
    io,
    ops::Range,
    sync::Arc,
};

const ALLOCATOR: u8 = 10;
const SEGMENT: u8 = 11;
const ROWS: u8 = 12;
const BATCH_BYTES: usize = 128 << 10;
const BATCH_ROWS: usize = 4096;
const NO_PREFIX: u16 = u16::MAX;
const MAX_PACKED_KEY: usize = 2 * format::MAX_KEY;

pub(crate) struct Snapshot {
    next: Option<u64>,
    segments: btree_map::IntoIter<u64, Arc<SegmentMeta>>,
    current: Option<(u64, Arc<SegmentMeta>, usize)>,
}

impl Snapshot {
    pub fn new(model: &Model) -> Self {
        Self {
            next: Some(model.next.max(1)),
            segments: model.segments.clone().into_iter(),
            current: None,
        }
    }
}

impl Iterator for Snapshot {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.next.take() {
            let mut bytes = vec![ALLOCATOR];
            bytes.extend_from_slice(&next.to_le_bytes());
            return Some(Ok(bytes.into()));
        }
        if let Some((id, segment, row)) = &mut self.current {
            if let Some(bytes) = encode_rows(*id, &segment.records, row) {
                return Some(Ok(bytes));
            }
        }
        self.current = None;
        let (id, segment) = self.segments.next()?;
        let mut bytes = vec![SEGMENT];
        for n in [
            id,
            segment.slot as u64,
            segment.bytes,
            segment.metadata,
            segment.durable_end,
        ] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        bytes.push(u8::from(segment.sealed));
        bytes.extend_from_slice(&(segment.records.len() as u32).to_le_bytes());
        self.current = Some((id, segment, 0));
        Some(Ok(bytes.into()))
    }
}

fn encode_rows(id: u64, records: &Records, row: &mut usize) -> Option<Bytes> {
    let mut rows = Vec::new();
    let mut dictionary = Vec::new();
    let mut prefixes = HashMap::new();
    let mut count = 0u16;
    let mut legacy = false;
    while *row < records.slots() {
        let Some(meta) = records.metadata(*row) else {
            *row += 1;
            continue;
        };
        let (prefix, suffix) = records.key_parts(*row);
        let next_prefix = prefixes.len() as u16;
        let entry = (!prefix.is_empty()).then(|| prefixes.entry(prefix));
        let extra = 14
            + 8 * usize::from(meta.checksum.is_some())
            + suffix.len()
            + if matches!(entry, Some(Entry::Vacant(_))) {
                4 + prefix.len()
            } else {
                0
            };
        if count != 0
            && (usize::from(count) == BATCH_ROWS
                || rows.len() + dictionary.len() + extra > BATCH_BYTES)
        {
            break;
        }
        let prefix = match entry {
            None => NO_PREFIX,
            Some(Entry::Occupied(entry)) => *entry.get(),
            Some(Entry::Vacant(entry)) => {
                dictionary.extend_from_slice(&(prefix.len() as u32).to_le_bytes());
                dictionary.extend_from_slice(prefix);
                *entry.insert(next_prefix)
            }
        };
        rows.extend_from_slice(&(meta.offset as u32).to_le_bytes());
        rows.extend_from_slice(&(meta.len as u32).to_le_bytes());
        legacy = meta.checksum.is_some();
        if let Some(checksum) = meta.checksum {
            rows.extend_from_slice(&checksum.to_le_bytes());
        }
        rows.extend_from_slice(&prefix.to_le_bytes());
        rows.extend_from_slice(&(suffix.len() as u32).to_le_bytes());
        rows.extend_from_slice(suffix);
        count += 1;
        *row += 1;
    }
    if count == 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(14 + dictionary.len() + rows.len());
    bytes.push(ROWS);
    bytes.extend_from_slice(&id.to_le_bytes());
    bytes.push(u8::from(legacy));
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&(prefixes.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&dictionary);
    bytes.extend_from_slice(&rows);
    Some(bytes.into())
}

pub(crate) enum SnapshotRecord {
    Next(u64),
    Segment(u64, SegmentMeta, usize),
    Rows(Batch),
}

pub(crate) struct Batch {
    segment: u64,
    bytes: Bytes,
    prefixes: Vec<Range<usize>>,
    rows: Vec<Row>,
}

struct Row {
    meta: Metadata,
    prefix: Option<usize>,
    suffix: Range<usize>,
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> io::Result<&'a [u8]> {
    let (body, rest) = input
        .split_at_checked(len)
        .ok_or_else(|| invalid("truncated small snapshot"))?;
    *input = rest;
    Ok(body)
}

fn count(input: &mut &[u8]) -> io::Result<usize> {
    Ok(usize::from(u16::from_le_bytes(
        take(input, 2)?.try_into().unwrap(),
    )))
}

fn flag(input: &mut &[u8]) -> io::Result<bool> {
    match take(input, 1)?[0] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(invalid("small snapshot flag")),
    }
}

pub(super) fn decode(bytes: &[u8]) -> io::Result<SnapshotRecord> {
    decode_record(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            invalid("truncated small snapshot")
        } else {
            error
        }
    })
}

fn decode_record(bytes: &[u8]) -> io::Result<SnapshotRecord> {
    let mut input = bytes;
    let record = match take(&mut input, 1)?[0] {
        ALLOCATOR => {
            let next = format::u64(&mut input)?;
            if next == 0 {
                return Err(invalid("small snapshot allocator"));
            }
            SnapshotRecord::Next(next)
        }
        SEGMENT => {
            let id = format::u64(&mut input)?;
            let slot = usize::try_from(format::u64(&mut input)?)
                .map_err(|_| invalid("small snapshot slot"))?;
            let bytes = format::u64(&mut input)?;
            let metadata = format::u64(&mut input)?;
            let durable_end = format::u64(&mut input)?;
            let sealed = flag(&mut input)?;
            let rows = format::u32(&mut input)? as usize;
            if bytes == 0
                || bytes > 1 << 30
                || !(8..=1 << 48).contains(&metadata)
                || durable_end > bytes
                || (sealed && durable_end != bytes)
                || rows as u64 > bytes
            {
                return Err(invalid("small snapshot segment bounds"));
            }
            SnapshotRecord::Segment(
                id,
                SegmentMeta {
                    slot,
                    bytes,
                    durable_end,
                    metadata,
                    sealed,
                    records: Records::default(),
                },
                rows,
            )
        }
        ROWS => SnapshotRecord::Rows(decode_rows(bytes, &mut input)?),
        _ => {
            return Err(invalid(
                "unsupported small-store snapshot format; use an empty cache directory",
            ))
        }
    };
    if !input.is_empty() {
        return Err(invalid("small snapshot trailing bytes"));
    }
    Ok(record)
}

fn decode_rows(bytes: &[u8], input: &mut &[u8]) -> io::Result<Batch> {
    let segment = format::u64(input)?;
    let legacy = flag(input)?;
    let rows = count(input)?;
    let prefixes = count(input)?;
    if rows == 0
        || rows > BATCH_ROWS
        || prefixes > rows
        || input.len() < rows * (14 + 8 * usize::from(legacy))
        || bytes.len() > BATCH_BYTES.max(MAX_PACKED_KEY + 40)
    {
        return Err(invalid("small snapshot batch bounds"));
    }
    let span = |input: &mut &[u8], len| -> io::Result<Range<usize>> {
        if len > MAX_PACKED_KEY {
            return Err(invalid("small snapshot packed key length"));
        }
        let start = bytes.len() - input.len();
        take(input, len)?;
        Ok(start..start + len)
    };
    let mut dictionary = Vec::with_capacity(prefixes);
    for _ in 0..prefixes {
        let len = format::u32(input)? as usize;
        if len == 0 {
            return Err(invalid("empty small snapshot prefix"));
        }
        dictionary.push(span(input, len)?);
    }
    let mut batch = Vec::with_capacity(rows);
    let mut key = Vec::new();
    let mut scratch = Default::default();
    for _ in 0..rows {
        let offset = u64::from(format::u32(input)?);
        let len = u64::from(format::u32(input)?);
        let checksum = legacy.then(|| format::u64(input)).transpose()?;
        let prefix_id = count(input)?;
        let suffix_len = format::u32(input)? as usize;
        let suffix = span(input, suffix_len)?;
        let (prefix, encoded) = if prefix_id == usize::from(NO_PREFIX) {
            (None, &bytes[suffix.clone()])
        } else {
            let prefix = dictionary
                .get(prefix_id)
                .ok_or_else(|| invalid("small snapshot prefix index"))?;
            if prefix.len() + suffix.len() > MAX_PACKED_KEY {
                return Err(invalid("small snapshot packed key length"));
            }
            key.clear();
            key.extend_from_slice(&bytes[prefix.clone()]);
            key.extend_from_slice(&bytes[suffix.clone()]);
            (Some(prefix_id), key.as_slice())
        };
        let view = key_codec::validated(encoded, &mut scratch)?;
        if len > 128 << 20 || !view.valid_extent(len) {
            return Err(invalid("small snapshot key extent"));
        }
        batch.push(Row {
            meta: Metadata {
                offset,
                len,
                checksum,
                hash: key_hash(&view),
            },
            prefix,
            suffix,
        });
    }
    Ok(Batch {
        segment,
        bytes: Bytes::copy_from_slice(bytes),
        prefixes: dictionary,
        rows: batch,
    })
}

#[derive(Clone, Default)]
pub(crate) struct Restore {
    allocator: bool,
    remaining: usize,
    extent_end: u64,
    prefixes: HashMap<Box<[u8]>, u32>,
}

impl Restore {
    pub fn finish(&self) -> io::Result<()> {
        if self.remaining != 0 {
            return Err(invalid("small snapshot segment row count"));
        }
        Ok(())
    }
}

pub(super) fn restore(model: &mut Model, record: SnapshotRecord) -> io::Result<()> {
    let state = &mut model.snapshot_restore;
    match record {
        SnapshotRecord::Next(next) => {
            if state.allocator || !model.segments.is_empty() {
                return Err(invalid("small snapshot allocator order"));
            }
            state.allocator = true;
            model.next = next;
        }
        SnapshotRecord::Segment(id, mut segment, rows) => {
            state.finish()?;
            if !state.allocator
                || id == 0
                || id >= model.next
                || model
                    .segments
                    .last_key_value()
                    .is_some_and(|(&last, _)| last >= id)
            {
                return Err(invalid("small snapshot segment order"));
            }
            state.remaining = rows;
            state.extent_end = 0;
            segment.records.reserve_snapshot(rows, segment.bytes)?;
            if rows == 0 && segment.sealed {
                segment.records.freeze_snapshot();
            }
            model.segments.insert(id, Arc::new(segment));
        }
        SnapshotRecord::Rows(batch) => {
            let mut last = model
                .segments
                .last_entry()
                .ok_or_else(|| invalid("small snapshot missing segment"))?;
            if *last.key() != batch.segment || batch.rows.len() > state.remaining {
                return Err(invalid("small snapshot batch order or count"));
            }
            let segment = Arc::get_mut(last.get_mut()).expect("snapshot recovery owns its segment");
            let mut prefix_ids = vec![None; batch.prefixes.len()];
            for row in &batch.rows {
                let meta = row.meta;
                let end = meta.offset + meta.stored_len();
                if meta.offset < state.extent_end
                    || end > segment.bytes
                    || (meta.offset < segment.durable_end && end > segment.durable_end)
                {
                    return Err(invalid("small snapshot extent bounds or order"));
                }
                // Batches are validated in parallel and number prefixes
                // locally. Intern each referenced byte span once per segment.
                let prefix = if let Some(id) = row.prefix {
                    let prefix_id = &mut prefix_ids[id];
                    if prefix_id.is_none() {
                        let bytes = &batch.bytes[batch.prefixes[id].clone()];
                        let global_id = if let Some(&id) = state.prefixes.get(bytes) {
                            id
                        } else {
                            let id = segment.records.append_snapshot_prefix(bytes)?;
                            state.prefixes.insert(bytes.into(), id);
                            id
                        };
                        *prefix_id = Some(global_id);
                    }
                    *prefix_id
                } else {
                    None
                };
                segment.records.append_snapshot_row(
                    meta,
                    prefix,
                    &batch.bytes[row.suffix.clone()],
                )?;
                state.extent_end = end;
            }
            state.remaining -= batch.rows.len();
            if state.remaining == 0 {
                state.prefixes.clear();
                if segment.sealed {
                    segment.records.freeze_snapshot();
                }
            }
        }
    }
    Ok(())
}
