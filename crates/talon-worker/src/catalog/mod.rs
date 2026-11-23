//! One namespace/scope's whole-block and paged residency journal.
//! Data files are bracketed by durable intents and completions. Only the
//! shared engine owns journal framing, checkpoint publication and the I/O ring.
#[cfg(test)]
mod benchmark;
mod model;
mod reconcile;
#[cfg(test)]
mod tests;

use crate::{
    block_key::{BlockIdentity, BlockKey},
    index::StoredBlock,
    page_access_store::CacheRootLock,
};
pub(crate) use model::Operation;
use model::{valid_store, Record, State};
use std::{
    collections::HashMap,
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use talon_core::BlockId;
use talon_journal::{Ack, Handle, JournalService, Options, StreamOptions};

pub(crate) const DIRECTORY: &str = ".paged-catalog";
const TRANSACTIONS: usize = 1024;

pub(crate) type WholeLocations = Arc<RwLock<HashMap<BlockKey, DiskId>>>;

/// A block's shard and first durable Begin sequence, local to one catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub(crate) struct DiskId(NonZeroU64);

impl DiskId {
    const SEQUENCE_MASK: u64 = (1 << 56) - 1;

    fn new(sequence: u64, shard: u8) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (1..=Self::SEQUENCE_MASK).contains(&sequence),
            "invalid disk ID sequence"
        );
        Ok(Self(
            NonZeroU64::new((u64::from(shard) << 56) | sequence).unwrap(),
        ))
    }

    fn sequence(self) -> u64 {
        self.0.get() & Self::SEQUENCE_MASK
    }

    pub(crate) fn path(self, root: &Path, extension: &str) -> PathBuf {
        let id = self.0.get();
        let shard = id >> 56;
        root.join(format!("{shard:02x}"))
            .join(format!("disk-{id:016x}.{extension}"))
    }
}

pub(crate) fn transaction_budget() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(TRANSACTIONS))
}

/// Business admission spans both journal records and data I/O. Engine admission
/// separately bounds encoded requests across all streams on this cache root.
pub(crate) struct Transaction {
    pub(crate) id: u64,
    pub(crate) disk_id: DiskId,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

pub(crate) struct Catalog {
    root: PathBuf,
    journal: Handle<State>,
    admission: Arc<tokio::sync::Semaphore>,
}

pub(crate) fn service(lock: Arc<CacheRootLock>) -> anyhow::Result<Arc<JournalService>> {
    let options = Options::default();
    #[cfg(test)]
    let options = Options {
        require_native: std::env::var_os("TALON_CATALOG_REQUIRE_URING").is_some(),
        ..options
    };
    Ok(JournalService::open(lock.root.clone(), lock, options)?)
}

impl Catalog {
    #[cfg(test)]
    pub(crate) fn open(lock: Arc<CacheRootLock>) -> anyhow::Result<Arc<Self>> {
        let service = service(lock)?;
        Self::with_service(&service, service.root(), transaction_budget(), None)
    }

    pub(crate) fn with_service(
        service: &Arc<JournalService>,
        root: &Path,
        admission: Arc<tokio::sync::Semaphore>,
        maintenance: Option<Arc<dyn talon_journal::Maintenance>>,
    ) -> anyhow::Result<Arc<Self>> {
        let root = root.canonicalize()?;
        let relative = root.strip_prefix(service.root())?.join(DIRECTORY);
        let journal = service.open_stream_with_maintenance(
            relative,
            State::default(),
            StreamOptions::default(),
            maintenance,
        )?;
        let catalog = Arc::new(Self {
            root,
            journal,
            admission,
        });
        catalog.reconcile()?;
        Ok(catalog)
    }

    pub(crate) fn store_path(&self, path: &Path) -> anyhow::Result<String> {
        let path = path
            .strip_prefix(&self.root)?
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("non-UTF8 cache path"))?
            .to_owned();
        anyhow::ensure!(valid_store(&path), "invalid catalog path");
        Ok(path)
    }

    pub(crate) fn poison(&self) {
        self.journal.poison();
    }

    pub(crate) fn recover(&self, path: &str) -> anyhow::Result<Option<Vec<StoredBlock>>> {
        let path = path.to_owned();
        Ok(self
            .journal
            .query_blocking(move |state| state.entries(&path).map_err(std::io::Error::other))?)
    }

    pub(crate) fn registered(&self, path: &str, page_size: u32) -> anyhow::Result<bool> {
        let path = path.to_owned();
        Ok(self
            .journal
            .query_blocking(move |state| match state.stores.get(&path) {
                None => Ok(false),
                Some(store) if store.page_size == page_size => Ok(true),
                Some(_) => Err(crate::cache_reset::incompatible(
                    "catalog page geometry changed",
                )),
            })?)
    }

    pub(crate) fn register(&self, path: &str, page_size: u32) -> anyhow::Result<()> {
        let record = if page_size == 0 {
            Record::WholeStore {
                path: path.into(),
                page_size,
            }
        } else {
            Record::PagedStore {
                path: path.into(),
                page_size,
            }
        };
        futures::executor::block_on(tokio::task::unconstrained(
            self.journal.append(record, Ack::Durable),
        ))?;
        Ok(())
    }

    pub(crate) async fn begin(
        &self,
        store: &str,
        block: &BlockId,
        operation: Operation,
    ) -> anyhow::Result<Transaction> {
        let permit = self.admission.clone().acquire_owned().await?;
        let (id, disk_id) = self
            .journal
            .append_with(
                Record::Begin {
                    intent: model::Intent {
                        store: store.into(),
                        block: block.clone(),
                        operation,
                        shard: Some(crate::page_lifecycle::disk_shard(block) as u8),
                        disk_id: None,
                    },
                },
                |state, id| {
                    let disk_id = state.pending[&id]
                        .disk_id
                        .expect("applied Begin has a disk ID");
                    move || (id, disk_id)
                },
            )
            .await?;
        Ok(Transaction {
            id,
            disk_id,
            _permit: permit,
        })
    }

    pub(crate) fn disk_id(&self, store: &str, block: &BlockId) -> std::io::Result<Option<DiskId>> {
        let store = store.to_owned();
        let block = block.clone();
        self.journal
            .query_blocking(move |state| Ok(state.disk_id(&store, &block)))
    }

    pub(crate) async fn lookup_disk_id(
        &self,
        store: &str,
        block: &BlockId,
    ) -> std::io::Result<Option<DiskId>> {
        let store = store.to_owned();
        let block = block.clone();
        self.journal
            .query(move |state| Ok(state.disk_id(&store, &block)))
            .await
    }

    pub(crate) async fn complete(
        &self,
        transaction: Transaction,
        allocated: u64,
    ) -> anyhow::Result<()> {
        self.journal
            .append(
                Record::Complete {
                    transaction: transaction.id,
                    allocated,
                },
                Ack::Durable,
            )
            .await?;
        Ok(())
    }

    pub(crate) fn recover_whole_locations(&self) -> std::io::Result<WholeLocations> {
        let locations = self.journal.query_blocking(|state| {
            Ok(state.stores["whole"]
                .blocks
                .values()
                .map(|block| {
                    (
                        block.id.clone(),
                        block.disk_id.expect("whole block has a disk ID"),
                    )
                })
                .collect())
        })?;
        Ok(Arc::new(RwLock::new(locations)))
    }

    pub(crate) async fn complete_whole(
        &self,
        transaction: Transaction,
        allocated: u64,
        id: BlockId,
        locations: WholeLocations,
    ) -> anyhow::Result<()> {
        self.journal
            .append_with(
                Record::Complete {
                    transaction: transaction.id,
                    allocated,
                },
                move |state, _| {
                    let key = state.stores["whole"]
                        .blocks
                        .get(&id as &dyn BlockIdentity)
                        .map(|block| block.id.clone());
                    let disk_id = transaction.disk_id;
                    move || {
                        let mut locations = locations.write().unwrap();
                        if let Some(key) = key {
                            locations.insert(key, disk_id);
                        } else {
                            locations.remove(&id as &dyn BlockIdentity);
                        }
                    }
                },
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn checkpoint(&self) -> anyhow::Result<()> {
        Ok(self.journal.checkpoint().await?)
    }

    pub(crate) async fn pages(&self, store: &str, block: &BlockId) -> anyhow::Result<Vec<u32>> {
        let store = store.to_owned();
        let block = block.clone();
        Ok(self
            .journal
            .query(move |state| {
                Ok(state
                    .stores
                    .get(&store)
                    .and_then(|store| {
                        store
                            .blocks
                            .get(&block as &dyn crate::block_key::BlockIdentity)
                    })
                    .map(|block| block.pages.keys().copied().collect())
                    .unwrap_or_default())
            })
            .await?)
    }
}
