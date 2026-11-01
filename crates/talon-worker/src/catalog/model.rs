//! Paged residency state and codec; the shared journal owns ordering and I/O.
use crate::index::StoredBlock;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::Path,
    sync::Arc,
};
use talon_core::{BlockForm, BlockId, BlockMeta, PageIndex, PresentBitmap};
use talon_journal::Machine;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Block {
    pub id: BlockId,
    pub len: u64,
    pub pages: BTreeMap<u32, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Operation {
    Put { page: u32, len: u64 },
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
    pub(super) page_size: u32,
    pub(super) blocks: HashMap<BlockId, Arc<Block>>,
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
                anyhow::ensure!(page_size > 0 && valid_store(&path), "invalid catalog store");
                let store = self.stores.entry(path).or_default();
                anyhow::ensure!(
                    store.page_size == 0 || store.page_size == page_size,
                    "catalog geometry changed"
                );
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
                        page < intent.block.page_count(store.page_size)
                            && len <= u64::from(intent.block.block_size),
                        "invalid page intent"
                    );
                }
                if let Operation::DeletePage { page } = intent.operation {
                    anyhow::ensure!(
                        page < intent.block.page_count(store.page_size),
                        "invalid delete page"
                    );
                }
                anyhow::ensure!(
                    !self.pending.values().any(|other| {
                        other.store == intent.store
                            && other.block == intent.block
                            && match (&other.operation, &intent.operation) {
                                (Operation::DeleteBlock, _) | (_, Operation::DeleteBlock) => true,
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
                        if let Some(block) = store.blocks.get_mut(&intent.block) {
                            Arc::make_mut(block).pages.remove(&page);
                        }
                        if store
                            .blocks
                            .get(&intent.block)
                            .is_some_and(|b| b.pages.is_empty())
                        {
                            store.blocks.remove(&intent.block);
                        }
                    }
                    Operation::DeleteBlock => {
                        store.blocks.remove(&intent.block);
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
                    Operation::Put { page, len } => {
                        let block = store.blocks.entry(intent.block.clone()).or_insert_with(|| {
                            Arc::new(Block {
                                id: intent.block,
                                len,
                                pages: BTreeMap::new(),
                            })
                        });
                        let block = Arc::make_mut(block);
                        block.len = len;
                        block.pages.insert(page, allocated);
                    }
                    Operation::DeletePage { page } => {
                        if let Some(block) = store.blocks.get_mut(&intent.block) {
                            Arc::make_mut(block).pages.remove(&page);
                        }
                        if store
                            .blocks
                            .get(&intent.block)
                            .is_some_and(|b| b.pages.is_empty())
                        {
                            store.blocks.remove(&intent.block);
                        }
                    }
                    Operation::DeleteBlock => {
                        store.blocks.remove(&intent.block);
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
            let mut present = PresentBitmap::new(b.id.page_count(store.page_size));
            for page in b.pages.keys() {
                present.set(PageIndex(*page));
            }
            StoredBlock {
                meta: BlockMeta {
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
    let p = Path::new(path);
    !p.is_absolute()
        && p.components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
        && p.file_name().is_some_and(|n| n == "paged")
}

#[derive(Serialize, Deserialize)]
pub(super) enum SnapshotRecord {
    Store { path: String, page_size: u32 },
    Block { store: String, block: Block },
    Pending { transaction: u64, intent: Intent },
}

enum Frozen {
    Store(String, u32),
    Block(Arc<str>, Arc<Block>),
    Pending(u64, Intent),
}

/// Freeze references to blocks; only a concurrently mutated block is copied.
/// Encoding runs on the journal's snapshot CPU thread, one record at a time.
pub(super) struct Snapshot(std::vec::IntoIter<Frozen>);

impl Iterator for Snapshot {
    type Item = io::Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|record| {
            let record = match record {
                Frozen::Store(path, page_size) => SnapshotRecord::Store { path, page_size },
                Frozen::Block(store, block) => SnapshotRecord::Block {
                    store: store.to_string(),
                    block: block.as_ref().clone(),
                },
                Frozen::Pending(transaction, intent) => SnapshotRecord::Pending {
                    transaction,
                    intent,
                },
            };
            serde_json::to_vec(&record)
                .map(Bytes::from)
                .map_err(io::Error::other)
        })
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
        Ok(Snapshot(records.into_iter()))
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
            SnapshotRecord::Block { store, block } => {
                let store = self
                    .stores
                    .get_mut(&store)
                    .ok_or_else(|| invalid("unknown snapshot store"))?;
                if block.len > u64::from(block.id.block_size)
                    || block.pages.is_empty()
                    || block
                        .pages
                        .keys()
                        .any(|page| *page >= block.id.page_count(store.page_size))
                    || store.blocks.contains_key(&block.id)
                {
                    return Err(invalid("invalid/duplicate snapshot block"));
                }
                store.blocks.insert(block.id.clone(), Arc::new(block));
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
