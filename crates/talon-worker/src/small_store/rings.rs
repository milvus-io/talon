//! Independent append/rollover pipelines on the worker's existing rings.
//! Slot zero retains the legacy root and serves non-ring callers. Native ring N
//! owns slot N+1. Readers and lifecycle operations can address every slot.
use super::*;
use crate::miss::{InFlightGuard, InFlightLoads, LoadKey};
use tokio::sync::OnceCell;

type Stream = Arc<OnceCell<Arc<SegmentStore>>>;

pub struct SmallObjectStore {
    root: PathBuf,
    legacy: Arc<SegmentStore>,
    streams: RwLock<BTreeMap<usize, Stream>>,
    // Only in-flight identities are retained; no second resident-object index.
    mutations: Arc<InFlightLoads>,
    tasks: Arc<crate::page_gc::Mutations>,
}

impl SmallObjectStore {
    pub fn open(
        root: PathBuf,
        threshold: u64,
        segment_size: u64,
        capacity: u64,
        root_lock: Option<Arc<crate::page_access_store::CacheRootLock>>,
    ) -> io::Result<Self> {
        let legacy = Arc::new(SegmentStore::open(
            root.clone(),
            threshold,
            segment_size,
            capacity,
            root_lock.clone(),
        )?);
        let mut streams = BTreeMap::new();
        streams.insert(0, Arc::new(OnceCell::new_with(Some(legacy.clone()))));
        // Recover all historical slots, even after reducing the ring count or
        // disabling new small-object admissions. Never remap persisted files.
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str().and_then(|s| s.strip_prefix("ring-")) else {
                continue;
            };
            let slot = name
                .parse::<usize>()
                .map_err(|_| invalid("small object ring directory"))?;
            if slot == 0 || slot.to_string() != name || !entry.file_type()?.is_dir() {
                return Err(invalid("small object ring directory"));
            }
            let stream = Arc::new(SegmentStore::open(
                entry.path(),
                threshold,
                segment_size,
                capacity,
                root_lock.clone(),
            )?);
            // Duplicate identities across streams must fail closed: otherwise
            // deleting one copy could expose another after restart.
            for key in stream.entries.read().unwrap().keys() {
                if streams.values().any(|old| {
                    old.get()
                        .unwrap()
                        .entries
                        .read()
                        .unwrap()
                        .contains_key(key.as_ref())
                }) {
                    return Err(invalid("duplicate small object across ring streams"));
                }
            }
            streams.insert(slot, Arc::new(OnceCell::new_with(Some(stream))));
        }
        Ok(Self {
            root,
            legacy,
            streams: RwLock::new(streams),
            mutations: Arc::new(InFlightLoads::new()),
            tasks: Arc::new(crate::page_gc::Mutations::new(true)),
        })
    }

    fn streams(&self) -> Vec<Arc<SegmentStore>> {
        self.streams
            .read()
            .unwrap()
            .values()
            .filter_map(|s| s.get().cloned())
            .collect()
    }

    async fn writer(&self, slot: usize) -> io::Result<Arc<SegmentStore>> {
        let existing = self.streams.read().unwrap().get(&slot).cloned();
        let cell = existing.unwrap_or_else(|| {
            self.streams
                .write()
                .unwrap()
                .entry(slot)
                .or_default()
                .clone()
        });
        cell.get_or_try_init(|| async {
            let root = self.root.join(format!("ring-{slot}"));
            disk_io::create_dir(root.clone()).await?;
            // A durable child manifest also needs its directory name durable.
            disk_io::sync_dir(self.root.clone()).await?;
            Ok(Arc::new(SegmentStore::empty(
                root,
                self.legacy.threshold,
                self.legacy.segment_size,
                self.legacy._root_lock.clone(),
            )))
        })
        .await
        .cloned()
    }

    fn owner(&self, id: &BlockId) -> Option<Arc<SegmentStore>> {
        self.streams().into_iter().find(|s| s.contains(id))
    }

    async fn mutation(&self, id: &BlockId) -> InFlightGuard {
        let key = LoadKey::Whole(id.clone());
        loop {
            if let Some(guard) = self.mutations.admit_owned(key.clone()) {
                return guard;
            }
            self.mutations.wait(&key).await;
        }
    }

    pub fn accepts(&self, id: &BlockId, len: u64) -> bool {
        self.legacy.accepts(id, len)
    }
    pub fn contains(&self, id: &BlockId) -> bool {
        self.owner(id).is_some()
    }

    pub fn get(&self, id: &BlockId) -> io::Result<Option<BlockHandle>> {
        for stream in self.streams() {
            if let Some(handle) = stream.get(id)? {
                return Ok(Some(handle));
            }
        }
        Ok(None)
    }

    pub async fn read(&self, id: &BlockId) -> io::Result<Option<Bytes>> {
        for stream in self.streams() {
            if let Some(bytes) = stream.read(id).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }

    pub async fn put(self: &Arc<Self>, id: &BlockId, value: Bytes) -> io::Result<u64> {
        // Capture before an owned task can run on a different Tokio thread.
        let slot = talon_uring::ring_id().map_or(0, |id| id + 1);
        let store = self.clone();
        let id = id.clone();
        self.tasks
            .run(async move {
                let _guard = store.mutation(&id).await;
                // Existing identities stay in their original stream, including
                // immutable-byte validation. New objects append on this ring.
                let stream = match store.owner(&id) {
                    Some(stream) => stream,
                    None => store.writer(slot).await?,
                };
                stream.put_inner(&id, value).await
            })
            .await
    }

    pub async fn delete(self: &Arc<Self>, id: &BlockId) -> io::Result<()> {
        let store = self.clone();
        let id = id.clone();
        self.tasks
            .run(async move {
                let _guard = store.mutation(&id).await;
                if let Some(stream) = store.owner(&id) {
                    stream.delete_inner(&id).await?;
                }
                Ok(())
            })
            .await
    }

    pub fn stats(&self) -> io::Result<SmallStoreStats> {
        let mut total = SmallStoreStats::default();
        for stream in self.streams() {
            let stats = stream.stats()?;
            total.live_bytes += stats.live_bytes;
            total.allocated_bytes += stats.allocated_bytes;
            total.retired_bytes += stats.retired_bytes;
            total.dead_bytes += stats.dead_bytes;
            total.segments += stats.segments;
        }
        Ok(total)
    }

    pub(crate) fn restore_index(&self, index: &crate::BlockIndex) -> io::Result<()> {
        for stream in self.streams() {
            stream.restore_index(index)?;
        }
        Ok(())
    }

    pub async fn maintain(self: &Arc<Self>, checkpoint: bool) -> io::Result<bool> {
        self.maintain_with_budget(checkpoint, None).await
    }

    pub(crate) async fn maintain_with_budget(
        self: &Arc<Self>,
        checkpoint: bool,
        budget: Option<Arc<crate::background::BackgroundBudget>>,
    ) -> io::Result<bool> {
        let store = self.clone();
        self.tasks
            .run(async move {
                let mut reclaimed = false;
                for stream in store.streams() {
                    reclaimed |= stream.maintain_inner(checkpoint, budget.clone()).await?;
                }
                Ok(reclaimed)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use talon_core::{Backend, ObjectId, Version};

    fn id(name: &str) -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "bucket", name),
            0,
            1 << 20,
            Version::new("v1"),
        )
    }
    fn open(root: &Path) -> Arc<SmallObjectStore> {
        Arc::new(
            SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 4 << 20, None).unwrap(),
        )
    }
    fn native<T>(slot: usize, work: impl std::future::Future<Output = T>) -> T {
        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let ring = talon_uring::LocalRing::enter_with_id(slot);
                let result = work.await;
                ring.drain().await;
                result
            })
    }

    #[test]
    fn blocked_ring_append_does_not_block_another_ring_and_recovery_keeps_all_slots() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let release = Arc::new(tokio::sync::Notify::new());
        let (entered, held) = std::sync::mpsc::channel();
        let a = store.clone();
        let unblock = release.clone();
        let first = std::thread::spawn(move || {
            native(0, async move {
                a.put(&id("a"), Bytes::from_static(b"ring zero"))
                    .await
                    .unwrap();
                let writer = a.writer(1).await.unwrap();
                writer.tasks.drain().await;
                let _gate = writer.state.lock().await;
                entered.send(()).unwrap();
                unblock.notified().await;
            })
        });
        held.recv_timeout(Duration::from_secs(5)).unwrap();
        let b = store.clone();
        let second = std::thread::spawn(move || {
            native(1, async move {
                disk_io::BLOCKING_CALLS.with(|c| c.set(0));
                let result = talon_uring::timeout(
                    Duration::from_secs(2),
                    b.put(&id("b"), Bytes::from_static(b"ring one")),
                )
                .await;
                assert_eq!(
                    b.read(&id("a")).await.unwrap().unwrap().as_ref(),
                    b"ring zero"
                );
                assert_eq!(disk_io::BLOCKING_CALLS.with(|c| c.get()), 0);
                result
            })
        });
        let result = second.join().unwrap();
        release.notify_one();
        first.join().unwrap();
        result
            .expect("another ring must not wait for ring zero's append lock")
            .unwrap();
        assert!(dir.path().join("ring-1/1.data").exists());
        assert!(dir.path().join("ring-2/1.data").exists());
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.maintain(true)).unwrap();
        drop(store);
        // Reopen without any native ring, with admissions disabled. Historical
        // slots remain readable even if the new worker has fewer rings.
        let recovered = Arc::new(
            SmallObjectStore::open(dir.path().to_owned(), 0, 256 << 10, 4 << 20, None).unwrap(),
        );
        rt.block_on(async {
            assert_eq!(
                recovered.read(&id("a")).await.unwrap().unwrap().as_ref(),
                b"ring zero"
            );
            assert_eq!(
                recovered.read(&id("b")).await.unwrap().unwrap().as_ref(),
                b"ring one"
            );
            let handle = recovered.get(&id("a")).unwrap().unwrap();
            recovered.delete(&id("a")).await.unwrap();
            recovered.maintain(false).await.unwrap();
            assert!(!recovered.contains(&id("a")));
            assert!(recovered.stats().unwrap().retired_bytes > 0);
            drop(handle);
        });
        drop(recovered);
        let recovered = open(dir.path());
        assert!(!recovered.contains(&id("a")));
        assert!(recovered.contains(&id("b")));
    }

    #[test]
    fn native_ring_submits_another_append_before_first_completes() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        native(0, async {
            let stream = store.writer(1).await.unwrap();
            let pause = Arc::new(super::super::tests::AppendPause::default());
            *stream.append_pause.lock().unwrap() = Some(pause.clone());
            disk_io::BLOCKING_CALLS.with(|c| c.set(0));
            let first = store.clone();
            let a = talon_uring::spawn_local(async move {
                first.put(&id("a"), Bytes::from_static(b"first")).await
            });
            pause.entered.notified().await;
            talon_uring::timeout(
                Duration::from_secs(2),
                store.put(&id("b"), Bytes::from_static(b"second")),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(!store.contains(&id("a")) && store.contains(&id("b")));
            pause.resume.notify_one();
            a.await.unwrap().unwrap();
            store.maintain(true).await.unwrap();
            assert_eq!(disk_io::BLOCKING_CALLS.with(|c| c.get()), 0);
        });
        drop(store);
        let recovered = open(dir.path());
        assert!(recovered.contains(&id("a")) && recovered.contains(&id("b")));
    }

    #[test]
    fn concurrent_ring_writes_preserve_one_immutable_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|ring| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    native(ring, async move {
                        barrier.wait();
                        store
                            .put(&id("shared"), Bytes::from(vec![ring as u8; 3072]))
                            .await
                    })
                })
            })
            .collect();
        let success: Vec<_> = threads
            .into_iter()
            .map(|t| t.join().unwrap().is_ok())
            .collect();
        assert_eq!(success.iter().filter(|ok| **ok).count(), 1);
        assert_eq!(
            store
                .streams()
                .iter()
                .filter(|s| s.contains(&id("shared")))
                .count(),
            1
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store.maintain(true)).unwrap();
        drop(store);
        let store = open(dir.path());
        let value = rt.block_on(store.read(&id("shared"))).unwrap().unwrap();
        assert_eq!(value.as_ref(), vec![if success[0] { 0 } else { 1 }; 3072]);
    }

    #[tokio::test]
    async fn legacy_root_and_duplicate_detection() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = Arc::new(
            SegmentStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None).unwrap(),
        );
        legacy
            .put(&id("old"), Bytes::from_static(b"legacy"))
            .await
            .unwrap();
        legacy.maintain(true).await.unwrap();
        legacy.tasks.drain().await;
        drop(legacy);
        let store = open(dir.path());
        assert_eq!(
            store.read(&id("old")).await.unwrap().unwrap().as_ref(),
            b"legacy"
        );
        drop(store);
        let ring = dir.path().join("ring-1");
        std::fs::create_dir(&ring).unwrap();
        for file in ["MANIFEST", "1.data", "1.index"] {
            std::fs::copy(dir.path().join(file), ring.join(file)).unwrap();
        }
        assert!(
            SmallObjectStore::open(dir.path().to_owned(), 128 << 10, 256 << 10, 4 << 20, None)
                .is_err()
        );
    }
}
