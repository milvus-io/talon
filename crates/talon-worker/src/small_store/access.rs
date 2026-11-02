//! Advisory whole-object recency, independent of segment placement/residency.
//! A snapshot carries exact versioned keys so relocation cannot reset idle age.
use super::*;
use std::sync::atomic::Ordering;

pub(super) const FILE: &str = "access.checkpoint";
const MAGIC: &[u8; 8] = b"TLSA0001";

impl SmallObjectStore {
    /// Startup only, after policy reconstruction and before serving.
    pub(super) fn restore_access(&self) {
        let result = (|| -> io::Result<()> {
            let bytes = match std::fs::read(self.root.join(FILE)) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e),
            };
            let body = bytes
                .strip_prefix(MAGIC.as_slice())
                .ok_or_else(|| invalid("small access snapshot version"))?;
            let mut restored = Vec::new();
            // Validate the entire snapshot before applying any ages. This file
            // never establishes presence and must not resurrect a deleted key.
            for mut record in format::frames(body)? {
                let last = format::u64(&mut record)?;
                let id = format::id(record)?;
                if let Some(access) = self.lookup(&id)?.and_then(|loc| loc.access) {
                    restored.push((access, last));
                }
            }
            for (access, last) in restored {
                access.restore(last);
            }
            Ok(())
        })();
        if let Err(error) = result {
            // Access age is advisory. Old caches and corrupt snapshots receive
            // a fresh idle window; valid payloads remain usable.
            tracing::warn!(%error, root = %self.root.display(), "small object access recovery failed; starting fresh idle windows");
        }
    }

    pub(crate) async fn checkpoint_access(
        self: &Arc<Self>,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<()> {
        let store = self.clone();
        self.tasks
            .run(async move {
                // Owned through rename+directory sync even if the caller is cancelled.
                let _gate = store.access_checkpoint.lock().await;
                let snapshot_store = store.clone();
                let bytes = tokio::task::spawn_blocking(move || -> io::Result<Bytes> {
                    let mut bytes = MAGIC.to_vec();
                    for stream in snapshot_store.streams() {
                        let entries: Vec<_> = stream
                            .entries
                            .read()
                            .unwrap()
                            .iter()
                            .filter_map(|(key, loc)| {
                                loc.access.clone().map(|access| (key.clone(), access))
                            })
                            .collect();
                        for (key, access) in entries {
                            let mut record = access.last_access().to_le_bytes().to_vec();
                            record.extend_from_slice(&key);
                            format::frame(&mut bytes, &record)?;
                        }
                    }
                    Ok(bytes.into())
                })
                .await
                .map_err(io::Error::other)??;
                if let Some(budget) = &budget {
                    budget.bytes(bytes.len()).await;
                }
                let _io = match &budget {
                    Some(b) => Some(b.io().await),
                    None => None,
                };
                let tmp = store.root.join("access.checkpoint.tmp");
                let fd = disk_io::open(tmp.clone(), disk_io::Open::Replace).await?;
                disk_io::write(fd.clone(), bytes, 0).await?;
                disk_io::sync(fd.clone()).await?;
                let allocated = disk_io::allocated(fd).await?;
                disk_io::rename(tmp, store.root.join(FILE)).await?;
                store.access_bytes.store(allocated, Ordering::Relaxed);
                disk_io::sync_dir(store.root.clone()).await
            })
            .await
    }
}
