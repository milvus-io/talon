//! Paged residency adapter for the shared cache-root journal service.
//! Data files are bracketed by durable intents and completions. Only the
//! shared engine owns journal framing, checkpoint publication and the I/O ring.
#[cfg(test)]
mod benchmark;
mod model;
mod reconcile;
#[cfg(test)]
mod tests;

use crate::{index::StoredBlock, page_access_store::CacheRootLock};
pub(crate) use model::Operation;
use model::{valid_store, Record, State};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use talon_core::BlockId;
use talon_journal::{Ack, Handle, JournalService, Options, StreamOptions};

pub(crate) const DIRECTORY: &str = ".paged-catalog";
const TRANSACTIONS: usize = 1024;

/// Business admission spans both journal records and data I/O. Engine admission
/// separately bounds encoded requests across all streams on this cache root.
pub(crate) struct Transaction {
    pub(crate) id: u64,
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
        Self::with_service(&service(lock)?)
    }

    pub(crate) fn with_service(service: &Arc<JournalService>) -> anyhow::Result<Arc<Self>> {
        let journal = service.open_stream(
            PathBuf::from(DIRECTORY),
            State::default(),
            StreamOptions::default(),
        )?;
        let catalog = Arc::new(Self {
            root: service.root().to_owned(),
            journal,
            admission: Arc::new(tokio::sync::Semaphore::new(TRANSACTIONS)),
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
                Some(_) => Err(std::io::Error::other("catalog page geometry changed")),
            })?)
    }

    pub(crate) fn register(&self, path: &str, page_size: u32) -> anyhow::Result<()> {
        futures::executor::block_on(tokio::task::unconstrained(self.journal.append(
            Record::Store {
                path: path.into(),
                page_size,
            },
            Ack::Durable,
        )))?;
        Ok(())
    }

    pub(crate) async fn begin(
        &self,
        store: &str,
        block: &BlockId,
        operation: Operation,
    ) -> anyhow::Result<Transaction> {
        let permit = self.admission.clone().acquire_owned().await?;
        let id = self
            .journal
            .append(
                Record::Begin {
                    intent: model::Intent {
                        store: store.into(),
                        block: block.clone(),
                        operation,
                    },
                },
                Ack::Durable,
            )
            .await?;
        Ok(Transaction {
            id,
            _permit: permit,
        })
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
                    .and_then(|store| store.blocks.get(&block))
                    .map(|block| block.pages.keys().copied().collect())
                    .unwrap_or_default())
            })
            .await?)
    }
}
