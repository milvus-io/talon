//! File-cache residency state and codec; the shared journal owns ordering and I/O.
use super::rows::{BlockRow, BlockRows, Slab};
use super::DiskId;
use crate::block_key::{BlockIdentity, BlockKey};
use crate::{
    index::{StoredBlock, StoredMeta},
    page_table::PageMap,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io, path::Path, sync::Arc};
use talon_core::{BlockForm, BlockId, PageIndex, PresentBitmap};
use talon_journal::Machine;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Block {
    pub id: BlockKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_id: Option<DiskId>,
    pub len: u64,
    pub pages: PageMap<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Operation {
    Put { page: u32, len: u64 },
    PutWhole { len: u64 },
    DeletePage { page: u32 },
    DeleteBlock,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Intent {
    pub store: String,
    pub block: BlockId,
    pub operation: Operation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<u8>,
    // A live Begin derives this from the catalog. A pending snapshot retains
    // the address even if the Begin that first allocated it was compacted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_id: Option<DiskId>,
}

/// Domain records contain no physical journal framing or checkpoint generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Record {
    WholeStore { path: String, page_size: u32 },
    PagedStore { path: String, page_size: u32 },
    Begin { intent: Intent },
    Complete { transaction: u64, allocated: u64 },
    Resolve { transaction: u64 },
}

#[derive(Default)]
pub(super) struct Store {
    /// Zero denotes whole blocks, represented by one allocation at index zero.
    pub(super) page_size: u32,
    pub(super) blocks: BlockRows,
}

#[derive(Default)]
pub(super) struct State {
    pub(super) stores: BTreeMap<String, Store>,
    pub(super) pending: BTreeMap<u64, Intent>,
}

impl State {
    pub(super) fn disk_id(&self, store: &str, block: &BlockId) -> Option<DiskId> {
        self.stores
            .get(store)?
            .blocks
            .get(block as &dyn BlockIdentity)?
            .disk_id
    }

    fn apply_record(&mut self, lsn: u64, record: Record) -> anyhow::Result<()> {
        let paged_format = matches!(record, Record::PagedStore { .. });
        match record {
            Record::WholeStore { path, page_size } | Record::PagedStore { path, page_size } => {
                anyhow::ensure!(
                    paged_format == (page_size > 0),
                    "invalid catalog store format"
                );
                anyhow::ensure!(
                    valid_store(&path)
                        && (Path::new(&path).file_name().unwrap() == "whole") == (page_size == 0),
                    "invalid catalog store"
                );
                if let Some(store) = self.stores.get(&path) {
                    anyhow::ensure!(store.page_size == page_size, "catalog geometry changed");
                }
                let store = self.stores.entry(path).or_default();
                store.page_size = page_size;
            }
            Record::Begin { mut intent } => {
                anyhow::ensure!(
                    lsn > 0 && !self.pending.contains_key(&lsn),
                    "invalid catalog transaction"
                );
                let store = self
                    .stores
                    .get(&intent.store)
                    .ok_or_else(|| anyhow::anyhow!("unknown catalog store"))?;
                if let Operation::Put { page, len } = intent.operation {
                    anyhow::ensure!(
                        store.page_size > 0
                            && page < intent.block.page_count(store.page_size)
                            && len <= u64::from(intent.block.block_size),
                        "invalid page intent"
                    );
                }
                if let Operation::PutWhole { len } = intent.operation {
                    anyhow::ensure!(
                        store.page_size == 0 && len <= u64::from(intent.block.block_size),
                        "invalid whole block intent"
                    );
                }
                if let Operation::DeletePage { page } = intent.operation {
                    anyhow::ensure!(
                        store.page_size > 0 && page < intent.block.page_count(store.page_size),
                        "invalid delete page"
                    );
                }
                let mut disk_id = None;
                for other in self
                    .pending
                    .values()
                    .filter(|other| other.store == intent.store && other.block == intent.block)
                {
                    let overlaps = match (&other.operation, &intent.operation) {
                        (Operation::DeleteBlock | Operation::PutWhole { .. }, _)
                        | (_, Operation::DeleteBlock | Operation::PutWhole { .. }) => true,
                        (
                            Operation::Put { page: a, .. } | Operation::DeletePage { page: a },
                            Operation::Put { page: b, .. } | Operation::DeletePage { page: b },
                        ) => a == b,
                    };
                    anyhow::ensure!(
                        !overlaps,
                        "overlapping catalog mutation: caller must hold the page/block gate"
                    );
                    disk_id = disk_id.or(other.disk_id);
                }
                disk_id = self.disk_id(&intent.store, &intent.block).or(disk_id);
                if let Some(restored) = intent.disk_id {
                    anyhow::ensure!(
                        (1..=lsn).contains(&restored.sequence())
                            && disk_id.map_or(true, |id| id == restored),
                        "inconsistent pending disk ID"
                    );
                }
                intent.disk_id = Some(match disk_id.or(intent.disk_id) {
                    Some(id) => id,
                    None => DiskId::new(
                        lsn,
                        intent
                            .shard
                            .ok_or_else(|| anyhow::anyhow!("missing block shard"))?,
                    )?,
                });
                self.pending.insert(lsn, intent);
            }
            Record::Resolve { transaction } => {
                let intent = self
                    .pending
                    .remove(&transaction)
                    .ok_or_else(|| anyhow::anyhow!("unknown catalog transaction"))?;
                let store = self.stores.get_mut(&intent.store).unwrap();
                match intent.operation {
                    Operation::Put { page, .. } | Operation::DeletePage { page } => {
                        store.blocks.remove_page(&intent.block, page);
                    }
                    Operation::DeleteBlock | Operation::PutWhole { .. } => {
                        store.blocks.remove(&intent.block as &dyn BlockIdentity);
                    }
                }
            }
            Record::Complete {
                transaction,
                allocated,
            } => {
                let intent = self
                    .pending
                    .remove(&transaction)
                    .ok_or_else(|| anyhow::anyhow!("unknown catalog transaction"))?;
                let store = self.stores.get_mut(&intent.store).unwrap();
                match intent.operation {
                    Operation::PutWhole { len } => {
                        store.blocks.insert(BlockRow::new(
                            BlockKey::new(&intent.block),
                            intent.disk_id,
                            len,
                            0,
                            allocated,
                        ));
                    }
                    Operation::Put { page, len } => {
                        if let Some(row) = store.blocks.get_mut(&intent.block) {
                            row.put(len, page, allocated);
                        } else {
                            store.blocks.insert(BlockRow::new(
                                BlockKey::new(&intent.block),
                                intent.disk_id,
                                len,
                                page,
                                allocated,
                            ));
                        }
                    }
                    Operation::DeletePage { page } => {
                        store.blocks.remove_page(&intent.block, page);
                    }
                    Operation::DeleteBlock => {
                        store.blocks.remove(&intent.block as &dyn BlockIdentity);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn entries(&self, path: &str) -> anyhow::Result<Option<Vec<StoredBlock>>> {
        self.entries_with_threads(path, crate::recovery::workers())
    }

    pub(super) fn entries_with_threads(
        &self,
        path: &str,
        threads: usize,
    ) -> anyhow::Result<Option<Vec<StoredBlock>>> {
        let Some(store) = self.stores.get(path) else {
            return Ok(None);
        };
        let blocks: Vec<_> = store.blocks.values().collect();
        Ok(Some(crate::recovery::map(blocks, threads, |b| {
            if store.page_size == 0 {
                return StoredBlock {
                    meta: StoredMeta {
                        id: b.id.clone(),
                        disk_id: b.disk_id,
                        len: b.len(),
                        form: BlockForm::Whole,
                    },
                    allocated: vec![(None, b.pages_after(None).next().unwrap().1)],
                };
            }
            let mut present = PresentBitmap::new(b.id.view().page_count(store.page_size));
            let mut allocated = Vec::with_capacity(b.page_count());
            for (page, bytes) in b.pages_after(None) {
                present.set(PageIndex(page));
                allocated.push((Some(PageIndex(page)), bytes));
            }
            StoredBlock {
                meta: StoredMeta {
                    id: b.id.clone(),
                    disk_id: b.disk_id,
                    len: b.len(),
                    form: BlockForm::Paged {
                        page_size: store.page_size,
                        present,
                    },
                },
                allocated,
            }
        })))
    }
}

pub(super) fn valid_store(path: &str) -> bool {
    matches!(path, "paged" | "whole")
}

#[derive(Serialize, Deserialize)]
pub(super) enum SnapshotRecord {
    WholeStore {
        path: String,
        page_size: u32,
    },
    PagedStore {
        path: String,
        page_size: u32,
    },
    Block {
        store: String,
        block: Block,
        // Older snapshots contain one complete block without this field.
        #[serde(default)]
        continuation: bool,
    },
    Pending {
        transaction: u64,
        intent: Intent,
    },
}

enum Frozen {
    Store(String, u32),
    Slab(Arc<str>, Slab),
    Pending(u64, Intent),
}

/// Freeze references to slabs. Subsequent writes clone touched slabs, and
/// shared multipage arrays clone only when their row changes.
pub(super) struct Snapshot {
    records: std::vec::IntoIter<Frozen>,
    slab: Option<FrozenSlab>,
}

struct FrozenSlab {
    store: Arc<str>,
    rows: Slab,
    slot: usize,
    after: Option<u32>,
}

// A page mapping needs at most 35 JSON bytes (u32 index and u64 allocation).
// Bound encoding independently of block geometry, without cloning the full map.
const SNAPSHOT_PAGES: usize = 1024;

impl Iterator for Snapshot {
    type Item = io::Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = loop {
            if let Some(slab) = &mut self.slab {
                while let Some(row) = slab.rows.get(slab.slot) {
                    if let Some(row) = row {
                        let pages: PageMap<_> =
                            row.pages_after(slab.after).take(SNAPSHOT_PAGES).collect();
                        if let Some((&last, _)) = pages.last_key_value() {
                            let continuation = slab.after.replace(last).is_some();
                            let record = SnapshotRecord::Block {
                                store: slab.store.to_string(),
                                block: Block {
                                    id: row.id.clone(),
                                    disk_id: row.disk_id,
                                    len: row.len(),
                                    pages,
                                },
                                continuation,
                            };
                            return Some(
                                serde_json::to_vec(&record)
                                    .map(Bytes::from)
                                    .map_err(io::Error::other),
                            );
                        }
                    }
                    slab.slot += 1;
                    slab.after = None;
                }
                self.slab = None;
            }
            match self.records.next()? {
                Frozen::Store(path, 0) => break SnapshotRecord::WholeStore { path, page_size: 0 },
                Frozen::Store(path, page_size) => {
                    break SnapshotRecord::PagedStore { path, page_size }
                }
                Frozen::Slab(store, rows) => {
                    self.slab = Some(FrozenSlab {
                        store,
                        rows,
                        slot: 0,
                        after: None,
                    });
                }
                Frozen::Pending(transaction, intent) => {
                    break SnapshotRecord::Pending {
                        transaction,
                        intent,
                    }
                }
            }
        };
        Some(
            serde_json::to_vec(&record)
                .map(Bytes::from)
                .map_err(io::Error::other),
        )
    }
}

fn invalid(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

impl Machine for State {
    type Record = Record;
    type SnapshotRecord = SnapshotRecord;
    type Snapshot = Snapshot;

    fn encoded_len(record: &Record) -> io::Result<usize> {
        struct Counter(usize);
        impl io::Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| invalid("catalog record size overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter(0);
        serde_json::to_writer(&mut counter, record).map_err(invalid)?;
        Ok(counter.0)
    }

    fn encode(record: &Record) -> io::Result<Bytes> {
        serde_json::to_vec(record).map(Bytes::from).map_err(invalid)
    }

    fn decode(bytes: &[u8]) -> io::Result<Record> {
        serde_json::from_slice(bytes).map_err(invalid)
    }

    fn apply(&mut self, lsn: u64, record: Record) -> io::Result<()> {
        self.apply_record(lsn, record)
            .map_err(|e| invalid(e.to_string()))
    }

    fn snapshot(&self) -> io::Result<Snapshot> {
        let mut records = Vec::new();
        for (path, store) in &self.stores {
            records.push(Frozen::Store(path.clone(), store.page_size));
            let path: Arc<str> = Arc::from(path.as_str());
            records.extend(
                store
                    .blocks
                    .slabs()
                    .map(|slab| Frozen::Slab(path.clone(), slab)),
            );
        }
        records.extend(
            self.pending
                .iter()
                .map(|(id, intent)| Frozen::Pending(*id, intent.clone())),
        );
        Ok(Snapshot {
            records: records.into_iter(),
            slab: None,
        })
    }

    fn decode_snapshot(bytes: &[u8]) -> io::Result<SnapshotRecord> {
        serde_json::from_slice(bytes).map_err(invalid)
    }

    fn restore(&mut self, record: SnapshotRecord) -> io::Result<()> {
        match record {
            SnapshotRecord::WholeStore { path, page_size } => {
                if self.stores.contains_key(&path) {
                    return Err(invalid("duplicate snapshot store"));
                }
                self.apply(0, Record::WholeStore { path, page_size })
            }
            SnapshotRecord::PagedStore { path, page_size } => {
                if self.stores.contains_key(&path) {
                    return Err(invalid("duplicate snapshot store"));
                }
                self.apply(0, Record::PagedStore { path, page_size })
            }
            SnapshotRecord::Block {
                store,
                block,
                continuation,
            } => {
                let store = self
                    .stores
                    .get_mut(&store)
                    .ok_or_else(|| invalid("unknown snapshot store"))?;
                if block.disk_id.is_none()
                    || block.len > u64::from(block.id.view().block_size)
                    || block.pages.is_empty()
                    || if store.page_size == 0 {
                        continuation || block.pages.len() != 1 || !block.pages.contains_key(&0)
                    } else {
                        block
                            .pages
                            .keys()
                            .any(|page| *page >= block.id.view().page_count(store.page_size))
                    }
                {
                    return Err(invalid("invalid snapshot block"));
                }
                if continuation {
                    let previous = store
                        .blocks
                        .get_mut(&block.id)
                        .ok_or_else(|| invalid("snapshot continuation without block"))?;
                    if previous.len() != block.len
                        || previous.disk_id != block.disk_id
                        || previous.last_page() >= *block.pages.first_key_value().unwrap().0
                    {
                        return Err(invalid("invalid/duplicate snapshot page chunk"));
                    }
                    for (page, bytes) in block.pages {
                        previous.put(block.len, page, bytes);
                    }
                } else {
                    if store.blocks.get(&block.id).is_some() {
                        return Err(invalid("duplicate snapshot block"));
                    }
                    let mut pages = block.pages.into_iter();
                    let (first, bytes) = pages.next().unwrap();
                    let mut row = BlockRow::new(block.id, block.disk_id, block.len, first, bytes);
                    for (page, bytes) in pages {
                        row.put(block.len, page, bytes);
                    }
                    store.blocks.insert(row);
                }
                Ok(())
            }
            SnapshotRecord::Pending {
                transaction,
                intent,
            } => {
                if transaction == 0 || self.pending.contains_key(&transaction) {
                    return Err(invalid("invalid/duplicate snapshot transaction"));
                }
                if intent.disk_id.is_none() {
                    return Err(invalid("missing pending disk ID"));
                }
                self.apply(transaction, Record::Begin { intent })
            }
        }
    }
}
