//! Startup-only data cleanup belongs to the paged adapter, not talon-journal.
//! Each cleanup is idempotent and resolved durably before paths can be reused.
use super::*;
use std::{fs, io};

fn remove(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn sync_dir(path: &Path) -> io::Result<()> {
    match fs::File::open(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result?.sync_all(),
    }
}

impl Catalog {
    pub(super) fn reconcile(&self) -> anyhow::Result<()> {
        let pending = self
            .journal
            .query_blocking(|state| Ok(state.pending.clone()))?;
        if pending.is_empty() {
            return Ok(());
        }
        let result = (|| {
            for (transaction, intent) in pending {
                let dir =
                    crate::paged_store::block_dir(&self.root.join(&intent.store), &intent.block);
                match intent.operation {
                    Operation::Put { page, .. } | Operation::DeletePage { page } => {
                        remove(&dir.join(format!("{page}.page")))?;
                        remove(&dir.join(format!("{page}.page.catalog.{transaction}")))?;
                        sync_dir(&dir)?;
                    }
                    Operation::DeleteBlock => {
                        let pages = futures::executor::block_on(tokio::task::unconstrained(
                            self.pages(&intent.store, &intent.block),
                        ))?;
                        for page in pages {
                            remove(&dir.join(format!("{page}.page")))?;
                        }
                        sync_dir(&dir)?;
                        match fs::remove_dir(&dir) {
                            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                            result => result?,
                        }
                        if let Some(parent) = dir.parent() {
                            sync_dir(parent)?;
                        }
                    }
                }
                // A crash before this ack repeats the exact cleanup. A crash
                // after it cannot revive the pending intent or its residency.
                futures::executor::block_on(tokio::task::unconstrained(
                    self.journal
                        .append(Record::Resolve { transaction }, Ack::Durable),
                ))?;
            }
            futures::executor::block_on(tokio::task::unconstrained(self.journal.checkpoint()))?;
            Ok(())
        })();
        if result.is_err() {
            self.journal.poison();
        }
        result
    }
}
