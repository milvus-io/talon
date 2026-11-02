use super::*;
use crate::eviction::access_millis;
use std::path::Path;
use talon_core::{Backend, ObjectId, Version};

fn id(n: usize) -> BlockId {
    BlockId::new(
        ObjectId::new(Backend::S3, "bucket", format!("item-{n}")),
        0,
        1 << 20,
        Version::new("v1"),
    )
}
fn open(root: &Path) -> Arc<SmallObjectStore> {
    open_with_policy(root).0
}
fn open_with_policy(root: &Path) -> (Arc<SmallObjectStore>, Arc<Lru>) {
    let store = Arc::new(
        SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 16 << 20, None).unwrap(),
    );
    let mut units = Vec::new();
    store.restore_policy_units(&mut units);
    let policy = Arc::new(crate::Lru::from_recovered(units, store.len(), true));
    store.bind_policy(policy.clone());
    store.enable_access_tracking().unwrap();
    (store, policy)
}
async fn put(store: &Arc<SmallObjectStore>, id: &BlockId) -> AccessHandle {
    store.put(id, Bytes::from_static(b"payload")).await.unwrap();
    store.track_access(id).unwrap();
    store.access(id).unwrap()
}
async fn flush(store: &Arc<SmallObjectStore>) {
    store.checkpoint_access(None).await.unwrap();
}
fn log(root: &Path) -> HashMap<BlockId, u64> {
    let bytes = std::fs::read(root.join(LOG)).unwrap();
    let mut ages = HashMap::new();
    decode(&bytes[HEADER as usize..], &mut ages).unwrap();
    ages
}

#[tokio::test]
async fn sparse_hits_append_once_and_clean_flush_does_not_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    for n in 0..100 {
        put(&store, &id(n)).await;
    }
    store.maintain(true).await.unwrap();
    flush(&store).await;
    let baseline = std::fs::read(dir.path().join(FILE)).unwrap();
    let access = store.access(&id(7)).unwrap();
    for _ in 0..10_000 {
        access.touch();
    }
    flush(&store).await;
    let bytes = std::fs::read(dir.path().join(LOG)).unwrap();
    assert_eq!(format::frames(&bytes[HEADER as usize..]).unwrap().len(), 1);
    assert_eq!(
        bytes.len(),
        HEADER as usize + 20 + format::key(&id(7)).unwrap().len()
    );
    assert_eq!(std::fs::read(dir.path().join(FILE)).unwrap(), baseline);
    assert_eq!(log(dir.path())[&id(7)], access.last_access());
    flush(&store).await;
    assert_eq!(std::fs::read(dir.path().join(LOG)).unwrap(), bytes);
    assert_eq!(
        store.access_bytes.load(Ordering::Relaxed),
        allocated(dir.path()).unwrap()
    );
    let last = access.last_access();
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.access(&id(7)).unwrap().last_access(), last);
    flush(&store).await;
    assert_eq!(std::fs::read(dir.path().join(LOG)).unwrap(), bytes);
}

#[tokio::test]
async fn late_reader_cannot_overwrite_readmitted_identity_or_resurrect_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (store, policy) = open_with_policy(dir.path());
    let old = put(&store, &id(0)).await;
    put(&store, &id(1)).await;
    flush(&store).await;
    store.delete(&id(0)).await.unwrap();
    policy.remove_block(&id(0));
    let new = put(&store, &id(0)).await;
    old.touch(); // arrives after the new token was queued
    new.restore(access_millis() - 10_000);
    store.access(&id(1)).unwrap().touch();
    store.delete(&id(1)).await.unwrap();
    policy.remove_block(&id(1));
    flush(&store).await;
    let ages = log(dir.path());
    assert_eq!(ages.len(), 1);
    assert_eq!(ages[&id(0)], new.last_access());
    assert_eq!(
        format::frames(&std::fs::read(dir.path().join(LOG)).unwrap()[HEADER as usize..])
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn hits_during_flush_survive_caller_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let access = put(&store, &id(0)).await;
    flush(&store).await;
    access.restore(access_millis() - 20_000);
    access.touch();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *store.access_pause.lock().unwrap() = Some((entered.clone(), release.clone()));
    let s = store.clone();
    let caller = tokio::spawn(async move { flush(&s).await });
    entered.notified().await;
    caller.abort();
    let _ = caller.await;
    // The first batch has sampled; a new hit must remain queued.
    access.touch();
    assert_eq!(store.access_dirty.pending.lock().unwrap().keys.len(), 1);
    release.notify_one();
    store.tasks.drain().await;
    flush(&store).await;
    let bytes = std::fs::read(dir.path().join(LOG)).unwrap();
    assert_eq!(format::frames(&bytes[HEADER as usize..]).unwrap().len(), 2);
    assert_eq!(log(dir.path())[&id(0)], access.last_access());
}

#[tokio::test]
async fn failed_append_retries_as_full_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let access = put(&store, &id(0)).await;
    flush(&store).await;
    access.touch();
    std::fs::remove_file(dir.path().join(LOG)).unwrap();
    assert!(store.checkpoint_access(None).await.is_err());
    assert!(store.access_disk.lock().unwrap().full);
    flush(&store).await;
    assert_eq!(store.access_disk.lock().unwrap().generation, 2);
    assert!(log(dir.path()).is_empty());
}

#[tokio::test]
async fn published_snapshot_ignores_previous_generation_log_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let access = put(&store, &id(0)).await;
    store.maintain(true).await.unwrap();
    access.restore(access_millis() - 30_000);
    flush(&store).await;
    access.touch();
    flush(&store).await;
    let old_log = std::fs::read(dir.path().join(LOG)).unwrap();
    store.access_disk.lock().unwrap().full = true;
    // Fail after durable snapshot replacement, before the new log exists.
    std::fs::create_dir(dir.path().join("access.log.tmp")).unwrap();
    assert!(store.checkpoint_access(None).await.is_err());
    assert_eq!(std::fs::read(dir.path().join(LOG)).unwrap(), old_log);
    let last = access.last_access();
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.access(&id(0)).unwrap().last_access(), last);
    assert!(store.access_disk.lock().unwrap().full);
    std::fs::remove_dir(dir.path().join("access.log.tmp")).unwrap();
    flush(&store).await;
}

#[tokio::test]
async fn corrupt_log_discards_all_advisory_ages_but_keeps_payload() {
    for tail in [vec![0; 20]] {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let access = put(&store, &id(0)).await;
        store.maintain(true).await.unwrap();
        access.restore(access_millis() - 30_000);
        flush(&store).await;
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join(LOG))
            .unwrap()
            .write_all(&tail)
            .unwrap();
        drop(store);
        let before = access_millis();
        let store = open(dir.path());
        assert!(store.access(&id(0)).unwrap().last_access() >= before);
        assert_eq!(
            store.read(&id(0)).await.unwrap().unwrap(),
            Bytes::from_static(b"payload")
        );
        flush(&store).await;
        assert!(log(dir.path()).is_empty());
    }
}

#[tokio::test]
async fn legacy_snapshot_migrates_and_log_threshold_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id(0)).await;
    store.maintain(true).await.unwrap();
    let cold = access_millis() - 30_000;
    let mut bytes = LEGACY.to_vec();
    encode(&mut bytes, &format::key(&id(0)).unwrap(), cold).unwrap();
    std::fs::write(dir.path().join(FILE), bytes).unwrap();
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.access(&id(0)).unwrap().last_access(), cold);
    flush(&store).await;
    store.access(&id(0)).unwrap().touch();
    flush(&store).await;
    assert_eq!(log(dir.path()).len(), 1);
    store.access_disk.lock().unwrap().log_bytes = COMPACT_BYTES;
    flush(&store).await;
    assert_eq!(store.access_disk.lock().unwrap().generation, 2);
    assert!(log(dir.path()).is_empty());
}

#[tokio::test]
async fn dirty_budget_overflow_falls_back_without_losing_live_ages() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let access = put(&store, &id(0)).await;
    flush(&store).await;
    store.access_dirty.pending.lock().unwrap().bytes = MAX_DIRTY_BYTES;
    access.touch();
    assert!(store.access_dirty.overflow.load(Ordering::Acquire));
    assert!(store.access_dirty.pending.lock().unwrap().keys.is_empty());
    flush(&store).await;
    assert_eq!(store.access_disk.lock().unwrap().generation, 2);
    access.touch();
    flush(&store).await;
    assert_eq!(log(dir.path())[&id(0)], access.last_access());
}

#[tokio::test]
async fn torn_append_preserves_base_and_complete_deltas_then_repairs() {
    for tail in [vec![0], vec![100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]] {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let cold = put(&store, &id(0)).await;
        let hot = put(&store, &id(1)).await;
        store.maintain(true).await.unwrap();
        let old = access_millis() - 30_000;
        cold.restore(old);
        hot.restore(old);
        flush(&store).await;
        hot.touch();
        flush(&store).await;
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join(LOG))
            .unwrap()
            .write_all(&tail)
            .unwrap();
        drop(store);
        let store = open(dir.path());
        assert_eq!(store.access(&id(0)).unwrap().last_access(), old);
        assert_eq!(
            store.access(&id(1)).unwrap().last_access(),
            hot.last_access()
        );
        assert!(store.access_disk.lock().unwrap().full);
        flush(&store).await;
        assert!(log(dir.path()).is_empty());
    }
}

// Synthetic resident metadata only: measures access-file work, not payload
// admission, native ring throughput, or production startup. All files use TMPDIR.
#[tokio::test]
#[ignore]
async fn million_item_sparse_access_probe() {
    let count: usize = std::env::var("TALON_ACCESS_PROBE_ITEMS")
        .ok()
        .map_or(1_000_000, |s| s.parse().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let fd: Arc<OwnedFd> = Arc::new(
        std::fs::File::create(dir.path().join("probe.data"))
            .unwrap()
            .into(),
    );
    let stream = store.streams().remove(0);
    for n in 0..count {
        let mut block = id(n);
        block.object.object_path = format!("{n:092}"); // bucket + path + version = 100 bytes
        let key: Arc<[u8]> = Arc::from(format::key(&block).unwrap());
        stream.entries.write().unwrap().insert(
            key.clone(),
            Location {
                fd: fd.clone(),
                segment: 1,
                offset: n as u64,
                len: 1,
                checksum: 0,
                access: Some(store.attach_access(key, AccessHandle::default(), false)),
            },
        );
    }
    let start = Instant::now();
    flush(&store).await;
    let full_ms = start.elapsed().as_millis();
    let full_bytes = std::fs::metadata(dir.path().join(FILE)).unwrap().len();
    for n in 0..count / 100 {
        let mut block = id(n);
        block.object.object_path = format!("{n:092}");
        store.access(&block).unwrap().touch();
    }
    let start = Instant::now();
    flush(&store).await;
    let delta_ms = start.elapsed().as_millis();
    let delta_bytes = std::fs::metadata(dir.path().join(LOG)).unwrap().len() - HEADER;
    assert_eq!(delta_bytes, (count / 100 * 145) as u64);
    let start = Instant::now();
    flush(&store).await;
    eprintln!("access_probe items={count} dirty={} full_bytes={full_bytes} delta_bytes={delta_bytes} full_ms={full_ms} delta_ms={delta_ms} clean_us={}", count / 100, start.elapsed().as_micros());
}

#[tokio::test]
async fn newly_recovered_key_without_age_is_saved_without_a_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    put(&store, &id(0)).await;
    store.maintain(true).await.unwrap();
    flush(&store).await;
    put(&store, &id(1)).await;
    store.maintain(true).await.unwrap();
    drop(store);
    let store = open(dir.path());
    let recovered_age = store.access(&id(1)).unwrap().last_access();
    assert!(store.access_disk.lock().unwrap().full);
    flush(&store).await;
    let mut ages = HashMap::new();
    decode(
        &std::fs::read(dir.path().join(FILE)).unwrap()[HEADER as usize..],
        &mut ages,
    )
    .unwrap();
    assert_eq!(ages[&id(1)], recovered_age);
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.access(&id(1)).unwrap().last_access(), recovered_age);
}
