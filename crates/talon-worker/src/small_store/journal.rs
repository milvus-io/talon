//! Small-store state machine adapter. All log I/O, queues, snapshots and replay
//! belong to talon-journal; this module owns only segment recovery semantics.
mod model;
#[cfg(test)]
mod tests;
use super::{format, invalid};
use bytes::Bytes;
pub(super) use model::{Event, Model, Record, SegmentMeta};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use talon_journal::{Ack, Handle, JournalService, Options, StreamOptions};

#[cfg(test)]
#[derive(Default)]
pub(super) struct TestControl {
    pub fail: std::sync::atomic::AtomicBool,
}
pub(super) struct Journal {
    root: PathBuf,
    handle: Handle<Model>,
    pub(super) next: Arc<AtomicU64>,
    #[cfg(test)]
    pub(super) control: Arc<TestControl>,
}
impl Journal {
    pub fn open(
        root: PathBuf,
        lease: Option<Arc<crate::page_access_store::CacheRootLock>>,
        maintenance: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<(Arc<Self>, Model)> {
        reject_legacy(&root)?;
        let keepalive: Arc<dyn Send + Sync> = match lease {
            Some(lease) => lease,
            None => Arc::new(()),
        };
        let service = JournalService::open(root.clone(), keepalive, Options::default())?;
        Self::open_with_service(root, &service, PathBuf::from("metadata"), maintenance)
    }
    pub fn open_shared(
        root: PathBuf,
        service: &Arc<JournalService>,
        maintenance: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<(Arc<Self>, Model)> {
        reject_legacy(&root)?;
        std::fs::create_dir_all(&root)?;
        let root = root.canonicalize()?;
        let relative = root
            .join("metadata")
            .strip_prefix(service.root())
            .map_err(|_| invalid("small store outside journal cache root"))?
            .to_owned();
        Self::open_with_service(root, service, relative, maintenance)
    }
    fn open_with_service(
        root: PathBuf,
        service: &Arc<JournalService>,
        relative_metadata: PathBuf,
        maintenance: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<(Arc<Self>, Model)> {
        reject_legacy(&root)?;
        std::fs::create_dir_all(root.join("data"))?;
        std::fs::File::open(root.join("data"))?.sync_all()?;
        std::fs::File::open(&root)?.sync_all()?;
        let replay_started = std::time::Instant::now();
        let handle = service.open_stream_with_maintenance(
            relative_metadata,
            Model {
                next: 1,
                ..Default::default()
            },
            StreamOptions::default(),
            maintenance.map(|budget| budget as Arc<dyn talon_journal::Maintenance>),
        )?;
        let replay_ms = replay_started.elapsed().as_secs_f64() * 1000.0;
        let clone_started = std::time::Instant::now();
        let model = handle.query_blocking(|m| Ok(m.clone()))?;
        let clone_ms = clone_started.elapsed().as_secs_f64() * 1000.0;
        tracing::info!(root = %root.display(), segments = model.segments.len(),
            replay_ms, clone_ms, "small journal model restored");
        let next = Arc::new(AtomicU64::new(model.next.max(1)));
        Ok((
            Arc::new(Self {
                root,
                handle,
                next,
                #[cfg(test)]
                control: Default::default(),
            }),
            model,
        ))
    }

    /// Called only after all sealed payloads and identities have been validated
    /// and their locators constructed. Never delete orphan data on that failure.
    pub(super) fn finish_recovery(&self, model: &Model) -> io::Result<()> {
        let root = &self.root;
        let handle = &self.handle;
        let mut next = model.next;
        let reconcile_started = std::time::Instant::now();
        let mut orphans = Vec::new();
        for shard in std::fs::read_dir(root.join("data"))? {
            let shard = shard?;
            let name = shard.file_name();
            let name = name.to_string_lossy();
            let number = u8::from_str_radix(&name, 16).map_err(|_| invalid("data shard name"))?;
            if name != format!("{number:02x}") || !shard.file_type()?.is_dir() {
                return Err(invalid("data shard directory"));
            }
            for entry in std::fs::read_dir(shard.path())? {
                let path = entry?.path();
                if path.extension().and_then(|s| s.to_str()) != Some("data") {
                    continue;
                }
                let id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| invalid("segment filename"))?;
                if path != data_path(root, id) {
                    return Err(invalid("segment shard mismatch"));
                }
                next = next.max(
                    id.checked_add(1)
                        .ok_or_else(|| invalid("segment id overflow"))?,
                );
                if !model.segments.get(&id).is_some_and(|s| s.sealed) {
                    orphans.push(path);
                }
            }
        }
        let abandoned: Vec<_> = model
            .segments
            .iter()
            .filter_map(|(&id, s)| (!s.sealed).then_some(id))
            .collect();
        // Resolve old unsealed state before deleting its payloads or accepting
        // new inserts. The allocator high-water mark survives orphan cleanup.
        futures::executor::block_on(tokio::task::unconstrained(async {
            for old in abandoned {
                handle
                    .append(Event::Replace { old, new: None }, Ack::Durable)
                    .await?;
            }
            handle
                .append(Event::SetNext { next: next.max(1) }, Ack::Durable)
                .await?;
            Ok::<_, io::Error>(())
        }))?;
        for path in orphans {
            std::fs::remove_file(path)?;
        }
        self.next.store(next.max(1), Ordering::Relaxed);
        tracing::info!(root = %root.display(),
            reconcile_ms = reconcile_started.elapsed().as_secs_f64() * 1000.0,
            "small journal recovery reconciled");
        Ok(())
    }
    pub fn set_budget(&self, budget: Arc<crate::background::BackgroundBudget>) {
        self.handle.set_maintenance(budget);
    }
    pub fn allocate(&self) -> io::Result<u64> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| invalid("segment id overflow"))
    }
    pub fn data_path(&self, id: u64) -> PathBuf {
        data_path(&self.root, id)
    }
    pub fn allocated(&self) -> u64 {
        self.handle.allocated_bytes()
    }
    pub async fn append(&self, event: Event, durable: bool) -> io::Result<()> {
        #[cfg(test)]
        if self.control.fail.load(Ordering::Relaxed) {
            self.handle.poison();
            return Err(io::Error::other("injected metadata write failure"));
        }
        self.handle
            .append(event, if durable { Ack::Durable } else { Ack::Written })
            .await
            .map(|_| ())
    }
    pub async fn checkpoint(&self) -> io::Result<()> {
        self.handle
            .append(
                Event::SetNext {
                    next: self.next.load(Ordering::Relaxed),
                },
                Ack::Written,
            )
            .await?;
        self.handle.checkpoint().await
    }
}
fn data_path(root: &Path, id: u64) -> PathBuf {
    root.join("data")
        .join(format!("{:02x}", id % 256))
        .join(format!("{id:020}.data"))
}
fn reject_legacy(root: &Path) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let path = entry.path();
        if name == "MANIFEST"
            || name == "MANIFEST.tmp"
            || name == "CURRENT"
            || name.starts_with("ring-")
            || matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("data" | "index")
            )
        {
            return Err(invalid(
                "unsupported legacy small-store format; use an empty cache directory",
            ));
        }
    }
    Ok(())
}
