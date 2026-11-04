//! File-cache residency state and codec; the shared journal owns ordering and I/O.
use crate::block_key::{BlockIdentity, BlockKey};
use crate::{
    index::{StoredBlock, StoredMeta},
    page_table::PageMap,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::Path,
    sync::Arc,
};
use talon_core::{BlockForm, BlockId, PageIndex, PresentBitmap};
use talon_journal::Machine;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Block {
    pub id: BlockKey,
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
}

/// Domain records contain no physical journal framing or checkpoint generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Record {
    Store { path: String, page_size: u32 },
    Begin { intent: Intent },
    Complete { transaction: u64, allocated: u64 },
    Resolve { transaction: u64 },
}

#[derive(Default)]
pub(super) struct Store {
    /// Zero denotes whole blocks, represented by one allocation at index zero.
    pub(super) page_size: u32,
    pub(super) blocks: HashMap<BlockKey, Arc<Block>>,
}

#[derive(Default)]
pub(super) struct State {
    pub(super) stores: BTreeMap<String, Store>,
    pub(super) pending: BTreeMap<u64, Intent>,
}

impl State {
    fn apply_record(&mut self, lsn: u64, record: Record) -> anyhow::Result<()> {
        match record {
            Record::Store { path, page_size } => {
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
            Record::Begin { intent } => {
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
                anyhow::ensure!(
                    !self.pending.values().any(|other| {
                        other.store == intent.store
                            && other.block == intent.block
                            && match (&other.operation, &intent.operation) {
                                (Operation::DeleteBlock | Operation::PutWhole { .. }, _)
                                | (_, Operation::DeleteBlock | Operation::PutWhole { .. }) => true,
                                (
                                    Operation::Put { page: a, .. }
                                    | Operation::DeletePage { page: a },
                                    Operation::Put { page: b, .. }
                                    | Operation::DeletePage { page: b },
                                ) => a == b,
                            }
                    }),
                    "overlapping catalog mutation: caller must hold the page/block gate"
                );
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
                        if let Some(block) =
                            store.blocks.get_mut(&intent.block as &dyn BlockIdentity)
                        {
                            Arc::make_mut(block).pages.remove(&page);
                        }
                        if store
                            .blocks
                            .get(&intent.block as &dyn BlockIdentity)
                            .is_some_and(|b| b.pages.is_empty())
                        {
                            store.blocks.remove(&intent.block as &dyn BlockIdentity);
                        }
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
                        let entry = store
                            .blocks
                            .entry(BlockKey::new(&intent.block as &dyn BlockIdentity));
                        let id = entry.key().clone();
                        let block = Arc::new(Block {
                            id,
                            len,
                            pages: [(0, allocated)].into(),
                        });
                        match entry {
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                entry.insert(block);
                            }
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(block);
                            }
                        }
                    }
                    Operation::Put { page, len } => {
                        use std::collections::hash_map::Entry;
                        let block = match store
                            .blocks
                            .entry(BlockKey::new(&intent.block as &dyn BlockIdentity))
                        {
                            Entry::Occupied(entry) => entry.into_mut(),
                            Entry::Vacant(entry) => {
                                let id = entry.key().clone();
                                entry.insert(Arc::new(Block {
                                    id,
                                    len,
                                    pages: PageMap::default(),
                                }))
                            }
                        };
                        let block = Arc::make_mut(block);
                        block.len = len;
                        block.pages.insert(page, allocated);
                    }
                    Operation::DeletePage { page } => {
                        if let Some(block) =
                            store.blocks.get_mut(&intent.block as &dyn BlockIdentity)
                        {
                            Arc::make_mut(block).pages.remove(&page);
                        }
                        if store
                            .blocks
                            .get(&intent.block as &dyn BlockIdentity)
                            .is_some_and(|b| b.pages.is_empty())
                        {
                            store.blocks.remove(&intent.block as &dyn BlockIdentity);
                        }
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
        let blocks: Vec<_> = store
            .blocks
            .values()
            .filter(|b| !b.pages.is_empty())
            .collect();
        Ok(Some(crate::recovery::map(blocks, threads, |b| {
            if store.page_size == 0 {
                return StoredBlock {
                    meta: StoredMeta {
                        id: b.id.clone(),
                        len: b.len,
                        form: BlockForm::Whole,
                    },
                    allocated: vec![(None, b.pages[&0])],
                };
            }
            let mut present = PresentBitmap::new(b.id.view().page_count(store.page_size));
            for page in b.pages.keys() {
                present.set(PageIndex(*page));
            }
            StoredBlock {
                meta: StoredMeta {
                    id: b.id.clone(),
                    len: b.len,
                    form: BlockForm::Paged {
                        page_size: store.page_size,
                        present,
                    },
                },
                allocated: b
                    .pages
                    .iter()
                    .map(|(p, n)| (Some(PageIndex(*p)), *n))
                    .collect(),
            }
        })))
    }
}

pub(super) fn valid_store(path: &str) -> bool {
    matches!(path, "paged" | "whole")
}

#[derive(Serialize, Deserialize)]
pub(super) enum SnapshotRecord {
    Store {
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
    Block(Arc<str>, Arc<Block>),
    Pending(u64, Intent),
}

/// Freeze references to blocks; only a concurrently mutated block is copied.
/// Encoding runs on the journal's snapshot CPU thread, one record at a time.
pub(super) struct Snapshot {
    records: std::vec::IntoIter<Frozen>,
    block: Option<(Arc<str>, Arc<Block>, Option<u32>)>,
}

// A page mapping needs at most 35 JSON bytes (u32 index and u64 allocation).
// Bound encoding independently of block geometry, without cloning the full map.
const SNAPSHOT_PAGES: usize = 1024;

impl Iterator for Snapshot {
    type Item = io::Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        use std::ops::Bound::{Excluded, Unbounded};
        let record = loop {
            if let Some((store, block, after)) = &mut self.block {
                let pages: PageMap<_> = block
                    .pages
                    .range((after.map_or(Unbounded, Excluded), Unbounded))
                    .take(SNAPSHOT_PAGES)
                    .map(|(&page, &bytes)| (page, bytes))
                    .collect();
                if let Some((&last, _)) = pages.last_key_value() {
                    let continuation = after.is_some();
                    *after = Some(last);
                    break SnapshotRecord::Block {
                        store: store.to_string(),
                        block: Block {
                            id: block.id.clone(),
                            len: block.len,
                            pages,
                        },
                        continuation,
                    };
                }
                self.block = None;
            }
            match self.records.next()? {
                Frozen::Store(path, page_size) => break SnapshotRecord::Store { path, page_size },
                Frozen::Block(store, block) => self.block = Some((store, block, None)),
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
                    .values()
                    .map(|block| Frozen::Block(path.clone(), block.clone())),
            );
        }
        records.extend(
            self.pending
                .iter()
                .map(|(id, intent)| Frozen::Pending(*id, intent.clone())),
        );
        Ok(Snapshot {
            records: records.into_iter(),
            block: None,
        })
    }

    fn decode_snapshot(bytes: &[u8]) -> io::Result<SnapshotRecord> {
        serde_json::from_slice(bytes).map_err(invalid)
    }

    fn restore(&mut self, record: SnapshotRecord) -> io::Result<()> {
        match record {
            SnapshotRecord::Store { path, page_size } => {
                if self.stores.contains_key(&path) {
                    return Err(invalid("duplicate snapshot store"));
                }
                self.apply(0, Record::Store { path, page_size })
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
                if block.len > u64::from(block.id.view().block_size)
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
                    if previous.len != block.len
                        || previous.pages.last_key_value().unwrap().0
                            >= block.pages.first_key_value().unwrap().0
                    {
                        return Err(invalid("invalid/duplicate snapshot page chunk"));
                    }
                    Arc::make_mut(previous).pages.extend(block.pages);
                } else {
                    if store.blocks.contains_key(&block.id) {
                        return Err(invalid("duplicate snapshot block"));
                    }
                    store.blocks.insert(block.id.clone(), Arc::new(block));
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
                self.apply(transaction, Record::Begin { intent })
            }
        }
    }
}
