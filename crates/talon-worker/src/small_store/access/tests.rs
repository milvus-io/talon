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

#[tokio::test]
async fn clocks_are_allocated_only_when_enabled_and_restore_persisted_ages() {
    fn untracked(root: &Path) -> (Arc<SmallObjectStore>, Arc<Lru>) {
        let store = Arc::new(
            SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 16 << 20, None).unwrap(),
        );
        let index = Arc::new(crate::BlockIndex::new());
        let mut units = Vec::new();
        store.restore_policy_units(&index, &mut units, false);
        let policy = Arc::new(Lru::from_recovered(index, units, store.len(), false));
        store.bind_policy(policy.clone());
        (store, policy)
    }
    let root = tempfile::tempdir().unwrap();
    let age;
    {
        let (store, policy) = untracked(root.path());
        store
            .put(&id(1), Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert_eq!(store.access(&id(1)).unwrap().last_access(), 0);
        store.enable_access_tracking().unwrap();
        let access = store.access(&id(1)).unwrap();
        let indexed = policy
            .index()
            .policy_get(&crate::CacheUnit::Whole(id(1)))
            .unwrap()
            .access;
        assert!(Arc::ptr_eq(access.clock(), indexed.clock()));
        age = access_millis().saturating_sub(10_000);
        access.restore(age);
        store
            .put(&id(2), Bytes::from_static(b"payload"))
            .await
            .unwrap();
        assert!(store.access(&id(2)).unwrap().last_access() > 0);
        store.maintain(true).await.unwrap();
        store.checkpoint_access(None, None).await.unwrap();
    }
    let (store, policy) = untracked(root.path());
    assert_eq!(store.access(&id(1)).unwrap().last_access(), 0);
    assert_eq!(
        policy
            .index()
            .policy_get(&crate::CacheUnit::Whole(id(1)))
            .unwrap()
            .access
            .last_access(),
        0
    );
    store.enable_access_tracking().unwrap();
    assert_eq!(store.access(&id(1)).unwrap().last_access(), age);
    assert_eq!(
        policy
            .index()
            .policy_get(&crate::CacheUnit::Whole(id(1)))
            .unwrap()
            .access
            .last_access(),
        age
    );
}
fn open(root: &Path) -> Arc<SmallObjectStore> {
    open_with_policy(root).0
}
fn open_with_policy(root: &Path) -> (Arc<SmallObjectStore>, Arc<Lru>) {
    let store = Arc::new(
        SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 16 << 20, None).unwrap(),
    );
    let mut units = Vec::new();
    let index = Arc::new(crate::BlockIndex::new());
    store.restore_policy_units(&index, &mut units, true);
    let policy = Arc::new(crate::Lru::from_recovered(index, units, store.len(), true));
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
    store.checkpoint_access(None, None).await.unwrap();
}
fn log(root: &Path) -> HashMap<CacheUnit, u64> {
    let bytes = std::fs::read(root.join(LOG)).unwrap();
    let mut ages = HashMap::new();
    decode(&bytes[HEADER as usize..], &mut ages).unwrap();
    ages
}

fn open_compact(root: &Path) -> (Arc<SmallObjectStore>, Arc<Lru>) {
    let store = Arc::new(
        SmallObjectStore::open(root.to_owned(), 128 << 10, 256 << 10, 16 << 20, None).unwrap(),
    );
    let index = Arc::new(crate::BlockIndex::new());
    let mut units = Vec::new();
    store.restore_policy_units(&index, &mut units, false);
    assert!(units.is_empty());
    store.init_recency();
    let policy = Arc::new(Lru::from_recovered(index, units, store.len(), true));
    store.bind_policy(policy.clone());
    store.enable_access_tracking().unwrap();
    (store, policy)
}

fn compact_age(store: &SmallObjectStore, key: &BlockId) -> u64 {
    store.compact_access_age(key)
}

fn set_compact_age(store: &SmallObjectStore, key: &BlockId, age: u64) {
    let catalog = store.journal.model.0.read().unwrap();
    let slot = catalog
        .find(&key.view(), super::super::lookup::VISIBLE)
        .unwrap();
    catalog
        .recency_at(slot)
        .unwrap()
        .1
        .store(age, Ordering::Relaxed);
}

#[tokio::test]
async fn compact_access_does_not_persist_ages_for_unacknowledged_addresses() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("metadata/journal.00000000000000000001");
    let key = id(1);
    let cold = access_millis() - 30_000;
    let (lost_address, durable_wal_len) = {
        let (store, _) = open_compact(dir.path());
        let durable_wal_len = std::fs::metadata(&wal).unwrap().len();
        store.put(&key, Bytes::from_static(b"old")).await.unwrap();
        let loc = store.lookup_entry(&key).unwrap().unwrap();
        set_compact_age(&store, &key, cold);
        // Model application precedes the durable ack. Simulate that window.
        store
            .journal
            .append(
                journal::Event::Flush {
                    segment: loc.segment,
                    durable_end: loc.offset + loc.stored_len(),
                },
                false,
            )
            .await
            .unwrap();
        flush(&store).await;
        assert_eq!(
            std::fs::metadata(dir.path().join(FILE)).unwrap().len(),
            HEADER + 12
        );
        store.track_access(&key).unwrap();
        flush(&store).await;
        assert!(compact::read_log(dir.path()).is_empty());
        ((loc.segment, loc.offset), durable_wal_len)
    };

    talon_journal::JournalService::wait_for_retiring(dir.path()).unwrap();
    // Model power loss of both the WAL tail and the new payload directory
    // entries, while the independently synced access files survive.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal)
        .unwrap()
        .set_len(durable_wal_len)
        .unwrap();
    for shard in std::fs::read_dir(dir.path().join("data")).unwrap() {
        std::fs::remove_dir_all(shard.unwrap().path()).unwrap();
    }
    let replacement = id(2);
    {
        let (store, _) = open_compact(dir.path());
        assert!(!store.contains(&key));
        store
            .put(&replacement, Bytes::from_static(b"new"))
            .await
            .unwrap();
        let loc = store.lookup_entry(&replacement).unwrap().unwrap();
        assert_eq!((loc.segment, loc.offset), lost_address);
        store.maintain(true).await.unwrap();
    }
    let (store, _) = open_compact(dir.path());
    assert!(compact_age(&store, &replacement) > cold);
}

#[tokio::test]
async fn compact_access_relocation_keeps_durable_ages_through_log_repair_or_failure() {
    use std::io::Write;

    for scenario in [
        "torn",
        "missing",
        "stale",
        "open-failure",
        "sync-failure",
        "missing-sync-failure",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let key = id(3);
        let cold = access_millis() - 30_000;
        let log = dir.path().join(LOG);
        let source;
        {
            let (store, policy) = open_compact(dir.path());
            for n in 0..6 {
                store
                    .put(&id(n), Bytes::from(vec![n as u8; 48 << 10]))
                    .await
                    .unwrap();
            }
            store.maintain(true).await.unwrap();
            source = store.lookup_entry(&key).unwrap().unwrap().segment;
            set_compact_age(&store, &key, cold);
            flush(&store).await;
            for n in 0..3 {
                store.delete(&id(n)).await.unwrap();
                policy.remove(&CacheUnit::Whole(id(n)));
            }
        }
        match scenario {
            "torn" => {
                let mut tail = [0; 96];
                tail[..4].copy_from_slice(&120_u32.to_le_bytes());
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&log)
                    .unwrap()
                    .write_all(&tail)
                    .unwrap();
            }
            "missing" | "missing-sync-failure" => std::fs::remove_file(&log).unwrap(),
            "stale" => {
                let mut bytes = std::fs::read(&log).unwrap();
                bytes[8..16].copy_from_slice(&99u64.to_le_bytes());
                std::fs::write(&log, bytes).unwrap();
            }
            _ => {}
        }
        let (store, _) = open_compact(dir.path());
        assert_eq!(compact_age(&store, &key), cold, "{scenario}");
        if scenario.ends_with("failure") {
            let saved = log.with_extension("saved");
            if scenario == "open-failure" {
                std::fs::rename(&log, &saved).unwrap();
            } else {
                store
                    .journal
                    .control
                    .fail_access_sync
                    .store(true, Ordering::Relaxed);
            }
            assert!(!store.maintain(false).await.unwrap(), "{scenario}");
            assert_eq!(store.lookup_entry(&key).unwrap().unwrap().segment, source);
            assert_eq!(compact_age(&store, &key), cold, "{scenario}");
            if scenario == "missing-sync-failure" {
                let disk = store.journal.access.disk.lock().unwrap();
                assert_eq!(disk.log_bytes, 0);
                assert!(disk.log_truncate);
            }
            if scenario == "open-failure" {
                std::fs::rename(saved, &log).unwrap();
            } else {
                let other = id(4);
                assert_eq!(store.lookup_entry(&other).unwrap().unwrap().segment, source);
                store.delete(&other).await.unwrap();
            }
        }
        assert!(store.maintain(false).await.unwrap(), "{scenario}");
        assert_ne!(store.lookup_entry(&key).unwrap().unwrap().segment, source);
        assert!(!compact::read_log(dir.path()).is_empty(), "{scenario}");
        assert!(
            !store.journal.access.disk.lock().unwrap().log_truncate,
            "{scenario}"
        );
        drop(store);

        // No access checkpoint between durable Replace and reopen.
        let (reopened, _) = open_compact(dir.path());
        assert_eq!(compact_age(&reopened, &key), cold, "{scenario}");
        assert!(
            !reopened.journal.access.disk.lock().unwrap().log_truncate,
            "{scenario}"
        );
    }
}

#[test]
fn access_temporary_files_count_toward_allocation_and_are_reclaimed_on_open() {
    for compact in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut blocks = 0;
        for name in [SNAPSHOT_TEMP, LOG_TEMP] {
            let path = root.join(name);
            std::fs::write(&path, vec![1; 65536]).unwrap();
            blocks += std::fs::metadata(&path).unwrap().blocks();
        }
        assert_eq!(allocated(root).unwrap(), blocks * 512);
        let store = if compact {
            open_compact(root).0
        } else {
            open(root)
        };
        assert!(!root.join(SNAPSHOT_TEMP).exists());
        assert!(!root.join(LOG_TEMP).exists());
        assert_eq!(store.journal.access.allocated_bytes(), 0);
    }
}

#[tokio::test]
async fn compact_reset_does_not_replay_a_stale_log_after_snapshot_publication() {
    for generation in [1_u64, u64::MAX] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let key = id(1);
        {
            let (store, _) = open_compact(root);
            store
                .put(&key, Bytes::from_static(b"payload"))
                .await
                .unwrap();
            store.maintain(true).await.unwrap();
            flush(&store).await;
            store.track_access(&key).unwrap();
            flush(&store).await;
        }
        let mut old_log = std::fs::read(root.join(LOG)).unwrap();
        old_log[8..HEADER as usize].copy_from_slice(&generation.to_le_bytes());
        std::fs::write(root.join(LOG), &old_log).unwrap();
        std::fs::remove_file(root.join(FILE)).unwrap();

        let (store, _) = open_compact(root);
        let expected = if generation == u64::MAX {
            0
        } else {
            generation
        };
        assert_eq!(
            store.journal.access.disk.lock().unwrap().generation,
            expected
        );
        assert_eq!(root.join(LOG).exists(), generation != u64::MAX);
        let cold = access_millis() - 20_000;
        set_compact_age(&store, &key, cold);
        // Leave the old log in place after the new snapshot has been renamed.
        std::fs::create_dir(root.join(LOG_TEMP)).unwrap();
        assert!(store.checkpoint_access(None, None).await.is_err());
        assert_eq!(
            store.journal.access.disk.lock().unwrap().generation,
            expected + 1
        );
        drop(store);
        let (reopened, _) = open_compact(root);
        assert_eq!(compact_age(&reopened, &key), cold);
    }
}

#[tokio::test]
async fn compact_shutdown_saves_a_clean_delta_and_bounds_io_admission() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let (store, _) = open_compact(dir.path());
    let key = id(1);
    store
        .put(&key, Bytes::from_static(b"payload"))
        .await
        .unwrap();
    store.maintain(true).await.unwrap();
    set_compact_age(&store, &key, access_millis() - 20_000);
    flush(&store).await;
    assert!(!store.journal.access.disk.lock().unwrap().full);
    store.track_access(&key).unwrap();
    let age = compact_age(&store, &key);
    let budget = crate::background::BackgroundBudget::new(&talon_core::WorkerConfig {
        background_io_concurrency: 1,
        background_io_max_mb_per_sec: 0,
        ..Default::default()
    });
    store
        .checkpoint_access_on_shutdown(
            Some(budget.clone()),
            TokioInstant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    let state = *store.journal.access.disk.lock().unwrap();
    assert!(!state.full);
    assert!(!state.log_truncate);
    assert_eq!(budget.charged_bytes(), state.log_bytes - HEADER);
    assert_eq!(compact::read_log(dir.path())[0].2, age);

    store.track_access(&key).unwrap();
    let _io = budget.io().await;
    let error = store
        .checkpoint_access_on_shutdown(
            Some(budget.clone()),
            TokioInstant::now() + Duration::from_millis(20),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    let stopped = *store.journal.access.disk.lock().unwrap();
    assert!(stopped.full);
    assert!(!stopped.log_truncate);
    assert_eq!(stopped.log_bytes, state.log_bytes);
    assert_eq!(compact::read_log(dir.path()).len(), 1);
    drop(store);
    let (reopened, _) = open_compact(dir.path());
    assert_eq!(compact_age(&reopened, &key), age);
}

#[tokio::test]
async fn compact_delta_keeps_its_durable_prefix_on_byte_bound_or_scheduler_stop() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let (store, _) = open_compact(dir.path());
    let rows = 24_000;
    let first = id(0);
    let last = id(rows - 1);
    let cold = access_millis() - 30_000;
    for n in 0..rows {
        store.put(&id(n), Bytes::from_static(b"x")).await.unwrap();
    }
    store.maintain(true).await.unwrap();
    let touch_all = || {
        for n in 0..rows {
            store.track_access(&id(n)).unwrap();
        }
    };
    let assert_prefix = || {
        let state = *store.journal.access.disk.lock().unwrap();
        let persisted = compact::read_log(dir.path());
        assert!(state.full);
        assert!(!state.log_truncate);
        assert_eq!(
            state.log_bytes,
            std::fs::metadata(dir.path().join(LOG)).unwrap().len()
        );
        assert!(!persisted.is_empty());
        assert!(persisted.len() < rows);
        state.log_bytes - HEADER
    };
    flush(&store).await;
    touch_all();
    let cap = (WRITE_BYTES + 8 * 1024) as u64;
    let deadline = TokioInstant::now() + Duration::from_secs(5);
    let error = store
        .flush_access(
            None,
            Flush::Shutdown {
                deadline,
                bytes: cap,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    let prefix = assert_prefix();
    assert!(prefix <= cap);

    set_compact_age(&store, &first, cold);
    set_compact_age(&store, &last, cold);
    flush(&store).await;
    assert_eq!(store.journal.access.disk.lock().unwrap().log_bytes, HEADER);
    touch_all();
    let first_address = store.lookup_entry(&first).unwrap().unwrap();
    let first_age = compact_age(&store, &first);
    let budget = crate::background::BackgroundBudget::new(&talon_core::WorkerConfig {
        background_io_concurrency: 1,
        background_io_max_mb_per_sec: 1,
        ..Default::default()
    });
    let held = budget.io().await;
    let mut scheduler = crate::background::BackgroundScheduler::new(1);
    let stop = scheduler.stop_signal();
    let (done, mut finished) = tokio::sync::oneshot::channel();
    let mut done = Some(done);
    scheduler.register("compact_access_test", Duration::from_secs(60), {
        let store = store.clone();
        let budget = budget.clone();
        move || {
            let store = store.clone();
            let budget = budget.clone();
            let stop = stop.clone();
            let done = done.take().unwrap();
            async move {
                done.send(store.checkpoint_access(Some(budget), Some(&stop)).await)
                    .unwrap();
            }
        }
    });
    let scheduler = scheduler.start();
    tokio::time::timeout(Duration::from_secs(3), async {
        while budget.charged_bytes() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let first_group = budget.charged_bytes();
    // Queue the next group behind a reservation. The first group's credit
    // was reserved before it and can finish as soon as the slot is released.
    let reservation = 1 << 30;
    let mut credit = std::pin::pin!(budget.bytes(reservation));
    assert!(futures::poll!(credit.as_mut()).is_pending());
    drop(held);
    tokio::time::timeout(Duration::from_secs(3), async {
        while budget.charged_bytes() <= first_group + reservation as u64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        finished.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let competing_io = futures::FutureExt::now_or_never(budget.io())
        .expect("competing maintenance can acquire the slot during the next pacing wait");
    assert_eq!(assert_prefix(), first_group);
    assert!(compact::read_log(dir.path()).contains(&(
        first_address.segment,
        first_address.offset as u32,
        first_age,
    )));
    scheduler.begin_shutdown();
    tokio::time::timeout(Duration::from_secs(1), scheduler.shutdown())
        .await
        .unwrap();
    assert_eq!(
        finished.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(assert_prefix(), first_group);
    drop(competing_io);
    assert_eq!(budget.available_io(), 1);
    drop(store);
    let (reopened, _) = open_compact(dir.path());
    assert_eq!(compact_age(&reopened, &first), first_age);
    assert_eq!(compact_age(&reopened, &last), cold);
}

#[tokio::test]
async fn compact_access_ignores_removed_addresses_and_merges_valid_log_prefix() {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let retired = id(1);
    let live = id(2);
    let cold = access_millis() - 30_000;
    let updated;
    {
        let (store, _) = open_compact(dir.path());
        for key in [&retired, &live] {
            store
                .put(key, Bytes::from_static(b"payload"))
                .await
                .unwrap();
            set_compact_age(&store, key, cold);
        }
        store.maintain(true).await.unwrap();
        flush(&store).await;
        let old = store.lookup_entry(&retired).unwrap().unwrap().segment;
        store.delete(&retired).await.unwrap();
        store
            .put(&retired, Bytes::from_static(b"replacement"))
            .await
            .unwrap();
        store.maintain(true).await.unwrap();
        assert_ne!(store.lookup_entry(&retired).unwrap().unwrap().segment, old);
        store.track_access(&live).unwrap();
        updated = compact_age(&store, &live);
        flush(&store).await;
        std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join(LOG))
            .unwrap()
            .write_all(&[1, 2])
            .unwrap();
    }
    let (store, _) = open_compact(dir.path());
    assert_eq!(compact_age(&store, &live), updated);
    assert!(compact_age(&store, &retired) > cold);
    assert!(store.journal.access.disk.lock().unwrap().full);
}

#[tokio::test]
async fn compact_access_resets_a_snapshot_truncated_at_a_frame_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let key = id(1);
    let cold = access_millis() - 30_000;
    {
        let (store, _) = open_compact(dir.path());
        store
            .put(&key, Bytes::from_static(b"payload"))
            .await
            .unwrap();
        store.maintain(true).await.unwrap();
        set_compact_age(&store, &key, cold);
        flush(&store).await;
    }
    let checkpoint = dir.path().join(FILE);
    let mut bytes = std::fs::read(&checkpoint).unwrap();
    bytes.truncate(bytes.len() - 12);
    std::fs::write(checkpoint, bytes).unwrap();
    let (store, _) = open_compact(dir.path());
    assert!(store.contains(&key));
    assert!(compact_age(&store, &key) > cold);
    assert!(store.journal.access.disk.lock().unwrap().full);
}

#[tokio::test]
async fn compact_read_updates_the_matched_row_without_a_policy_ticket() {
    use super::super::lookup::{REFERENCED, VISIBLE};

    let dir = tempfile::tempdir().unwrap();
    let (store, policy) = open_compact(dir.path());
    let key = id(1);
    store
        .put(&key, Bytes::from_static(b"payload"))
        .await
        .unwrap();
    let cold = access_millis() - 30_000;
    let revision = {
        let catalog = store.journal.model.0.read().unwrap();
        let slot = catalog.find(&key.view(), VISIBLE).unwrap();
        let (_, time) = catalog.recency_at(slot).unwrap();
        time.store(cold, Ordering::Relaxed);
        catalog.lookup.take_reference(slot);
        catalog.revision.load(Ordering::Relaxed)
    };

    assert!(store.get_range(&key, 5, 3).await.is_err());
    assert!(store.get_range(&id(2), 0, 3).await.unwrap().is_none());
    assert_eq!(compact_age(&store, &key), cold);
    {
        let catalog = store.journal.model.0.read().unwrap();
        let slot = catalog.find(&key.view(), VISIBLE).unwrap();
        assert_eq!(catalog.lookup.flags(slot) & REFERENCED, 0);
    }

    let handle = store.get_range(&key, 2, 3).await.unwrap().unwrap();
    let data = super::super::disk_io::read(handle.fd, handle.offset, handle.len as usize)
        .await
        .unwrap();
    assert_eq!(&data[..], b"ylo");
    assert!(compact_age(&store, &key) > cold);
    {
        let catalog = store.journal.model.0.read().unwrap();
        let slot = catalog.find(&key.view(), VISIBLE).unwrap();
        assert_ne!(catalog.lookup.flags(slot) & REFERENCED, 0);
        assert_eq!(catalog.revision.load(Ordering::Relaxed), revision);
        assert_eq!(catalog.tickets.len(), 0);
    }

    let entry = policy
        .index()
        .policy_get(&CacheUnit::Whole(key.clone()))
        .unwrap();
    let ticket = entry.access.small().unwrap();
    let revision = ticket.snapshot();
    assert!(ticket.is_current(revision));
    assert!(store.get(&key).await.unwrap().is_some());
    assert!(!ticket.is_current(revision));
}

#[tokio::test]
async fn compact_access_resets_legacy_ages_and_rechecks_idle_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let key = id(1);
    let cold = access_millis() - 30_000;
    {
        let store = open(dir.path());
        put(&store, &key).await;
        store.maintain(true).await.unwrap();
        let mut checkpoint = LEGACY.to_vec();
        encode(&mut checkpoint, &key, cold).unwrap();
        std::fs::write(dir.path().join(FILE), checkpoint).unwrap();
    }

    let (store, _) = open_compact(dir.path());
    assert!(compact_age(&store, &key) > cold);
    set_compact_age(&store, &key, cold);
    flush(&store).await;
    let now = access_millis();
    let mut candidates =
        store.idle_recency_candidates(&mut IdleRecencyCursor::default(), now, 10_000, 10, 10);
    assert_eq!(candidates.len(), 1);
    let (unit, ticket) = candidates.pop().unwrap();
    store.read(&key).await.unwrap().unwrap();
    assert!(!store
        .delete_idle(unit, ticket, move || (now, 10_000))
        .await
        .unwrap());
    let touched = compact_age(&store, &key);
    assert!(touched >= now);
    flush(&store).await;
    let loc = store.lookup_entry(&key).unwrap().unwrap();
    assert_eq!(
        compact::read_log(dir.path()),
        [(loc.segment, loc.offset as u32, touched)]
    );
    drop(store);

    let (store, policy) = open_compact(dir.path());
    assert_eq!(compact_age(&store, &key), touched);
    let now = touched + 30_000;
    let (unit, ticket) = store
        .idle_recency_candidates(&mut IdleRecencyCursor::default(), now, 10_000, 10, 10)
        .pop()
        .unwrap();
    assert!(store
        .delete_idle(unit, ticket, move || (now, 10_000))
        .await
        .unwrap());
    assert!(!store.contains(&key));
    assert_eq!(policy.len(), 0);
}

#[tokio::test]
async fn compact_access_relocation_keeps_only_source_dirty_updates_pending() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let (store, policy) = open_compact(dir.path());
    for n in 0..6 {
        store
            .put(&id(n), Bytes::from(vec![n as u8; 48 << 10]))
            .await
            .unwrap();
    }
    store.maintain(true).await.unwrap();
    for n in 0..3 {
        store.delete(&id(n)).await.unwrap();
        policy.remove(&CacheUnit::Whole(id(n)));
    }
    let touched = id(3);
    let clean = id(4);
    let cold = access_millis() - 30_000;
    set_compact_age(&store, &touched, cold);
    set_compact_age(&store, &clean, cold);
    flush(&store).await;

    let pause = Arc::new(crate::small_store::tests::AppendPause::default());
    *store.streams()[0].relocation_pause.lock().unwrap() = Some(pause.clone());
    let compacting = {
        let store = store.clone();
        tokio::spawn(async move { store.maintain(false).await.unwrap() })
    };
    tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
        .await
        .unwrap();
    let translated = compact::read_log(dir.path());
    assert_eq!(translated.len(), 2);
    store.track_access(&touched).unwrap();
    let hot = compact_age(&store, &touched);
    assert!(hot > cold);
    pause.resume.notify_one();
    assert!(compacting.await.unwrap());
    flush(&store).await;

    let moved = store.lookup_entry(&touched).unwrap().unwrap();
    let log = compact::read_log(dir.path());
    assert_eq!(&log[..translated.len()], translated);
    assert_eq!(
        &log[translated.len()..],
        &[(moved.segment, moved.offset as u32, hot)]
    );
    assert_eq!(compact_age(&store, &clean), cold);
}

#[tokio::test]
async fn compact_access_keeps_ages_through_row_compaction_and_relocation() {
    let dir = tempfile::tempdir().unwrap();
    let (store, policy) = open_compact(dir.path());
    for n in 0..6 {
        store
            .put(&id(n), Bytes::from(vec![n as u8; 48 << 10]))
            .await
            .unwrap();
    }
    let key = id(3);
    let before = store.lookup_entry(&key).unwrap().unwrap();
    let retained = store.get_range(&key, 4, 8).await.unwrap().unwrap();
    let age = compact_age(&store, &key);
    for n in 0..3 {
        store.delete(&id(n)).await.unwrap();
        policy.remove(&CacheUnit::Whole(id(n)));
    }
    store.maintain(true).await.unwrap();
    let after = store.lookup_entry(&key).unwrap().unwrap();
    assert_ne!(before.segment, after.segment);
    assert_eq!(compact_age(&store, &key), age);
    let data = super::super::disk_io::read(retained.fd, retained.offset, retained.len as usize)
        .await
        .unwrap();
    assert_eq!(&data[..], &[3; 8]);
    flush(&store).await;
    store.journal.access.disk.lock().unwrap().log_bytes = MIN_COMPACT_BYTES;
    flush(&store).await;
    assert!(compact::read_log(dir.path()).is_empty());
    drop(store);
    let (store, _) = open_compact(dir.path());
    assert_eq!(compact_age(&store, &key), age);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn packed_no_deadline_deletes_defer_on_busy_mutation_registry() {
    use crate::small_store::SmallStaleCursor;

    let dir = tempfile::tempdir().unwrap();
    let (store, policy) = open_compact(dir.path());
    let key = id(1);
    store
        .put(&key, Bytes::from_static(b"payload"))
        .await
        .unwrap();
    store.maintain(true).await.unwrap();
    let now = access_millis() + 30_000;
    let age = 10_000;
    let (unit, ticket) = store
        .idle_recency_candidates(&mut IdleRecencyCursor::default(), now, age, 10, 10)
        .pop()
        .unwrap();
    let (candidate, revision) = store
        .stale_candidates(&mut SmallStaleCursor::default(), now, age, 10, 10, |_| true)
        .1
        .pop()
        .unwrap();
    let catalog_revision = store.catalog_revision();
    let runtime = tokio::runtime::Handle::current();
    // An accidental append fails the operation even if it leaves the row visible.
    store.journal.control.fail.store(true, Ordering::Relaxed);
    store.under_mutation_registry(|| {
        runtime.block_on(async {
            assert!(!store
                .delete_idle(unit, ticket, move || (now, age))
                .await
                .unwrap());
            assert!(!store
                .delete_gc(candidate, revision, None, || true)
                .await
                .unwrap());
        })
    });
    store.journal.control.fail.store(false, Ordering::Relaxed);
    assert_eq!(store.catalog_revision(), catalog_revision);
    assert_eq!(policy.len(), 1);
    assert_eq!(store.read(&key).await.unwrap().unwrap(), b"payload"[..]);
}

#[tokio::test]
async fn full_access_checkpoint_and_relocation_share_the_publication_boundary() {
    use std::time::Duration;

    for checkpoint_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (store, policy) = open_compact(dir.path());
        for n in 0..6 {
            store
                .put(&id(n), Bytes::from(vec![n as u8; 48 << 10]))
                .await
                .unwrap();
        }
        store.maintain(true).await.unwrap();
        let key = id(3);
        let source = store.lookup_entry(&key).unwrap().unwrap().segment;
        let cold = access_millis() - 30_000;
        {
            let catalog = store.journal.model.0.read().unwrap();
            let slot = catalog
                .find(&key.view(), super::super::lookup::VISIBLE)
                .unwrap();
            let (_, time) = catalog.recency_at(slot).unwrap();
            time.store(cold, Ordering::Relaxed);
        }
        flush(&store).await;
        for n in 0..3 {
            store.delete(&id(n)).await.unwrap();
            policy.remove(&CacheUnit::Whole(id(n)));
        }
        store.journal.access.disk.lock().unwrap().full = true;

        let budget = crate::background::BackgroundBudget::new(&talon_core::WorkerConfig {
            background_io_concurrency: 1,
            background_io_max_mb_per_sec: 1,
            background_delete_max_per_sec: 0,
            ..Default::default()
        });
        let relocating = || {
            let store = store.clone();
            let budget = budget.clone();
            tokio::spawn(async move {
                store
                    .maintain_with_budget(false, Some(budget))
                    .await
                    .unwrap()
            })
        };
        let checkpointing = || {
            let store = store.clone();
            let budget = budget.clone();
            tokio::spawn(async move { store.checkpoint_access(Some(budget), None).await.unwrap() })
        };
        let source_is_journaled = || {
            store
                .journal
                .model
                .0
                .read()
                .unwrap()
                .model
                .segments
                .contains_key(&source)
        };
        if checkpoint_first {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            *store.access_pause.lock().unwrap() = Some((entered.clone(), release.clone()));
            let checkpoint = checkpointing();
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            let mut relocate = relocating();
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut relocate)
                    .await
                    .is_err()
            );
            assert!(source_is_journaled());
            tokio::time::timeout(
                Duration::from_secs(5),
                store.put(&id(6), Bytes::from_static(b"append")),
            )
            .await
            .unwrap()
            .unwrap();
            // The snapshot has written its header and relocation is waiting for it.
            // Another disk user can acquire the sole permit and keep it while
            // the snapshot charges its next chunk against the byte rate.
            let io = tokio::time::timeout(Duration::from_secs(5), budget.io())
                .await
                .unwrap();
            let charged = budget.charged_bytes();
            release.notify_one();
            tokio::time::timeout(Duration::from_secs(5), async {
                while budget.charged_bytes() == charged {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let fd = disk_io::open(dir.path().join(FILE), disk_io::Open::Read)
                .await
                .unwrap();
            disk_io::sync(fd).await.unwrap();
            drop(io);
            tokio::time::timeout(Duration::from_secs(5), checkpoint)
                .await
                .unwrap()
                .unwrap();
            assert!(tokio::time::timeout(Duration::from_secs(5), relocate)
                .await
                .unwrap()
                .unwrap());
        } else {
            let reader = store.journal.readers.read().await;
            let pause = Arc::new(crate::small_store::tests::AppendPause::default());
            *store.streams()[0].relocation_pause.lock().unwrap() = Some(pause.clone());
            let relocate = relocating();
            tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
                .await
                .unwrap();
            assert!(!source_is_journaled());
            let mut checkpoint = checkpointing();
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut checkpoint)
                    .await
                    .is_err()
            );
            pause.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(5), checkpoint)
                .await
                .unwrap()
                .unwrap();
            assert!(!relocate.is_finished());
            drop(reader);
            assert!(relocate.await.unwrap());
        }
        assert_ne!(store.lookup_entry(&key).unwrap().unwrap().segment, source);
        drop(store);

        let (store, _) = open_compact(dir.path());
        assert_eq!(compact_age(&store, &key), cold);
    }
}

#[tokio::test]
async fn compact_delta_visits_only_dirty_rows_after_row_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let (store, policy) = open_compact(dir.path());
    for n in 0..192 {
        store
            .put(&id(n), Bytes::from_static(b"data"))
            .await
            .unwrap();
    }
    store.streams()[0].flush().await.unwrap();
    flush(&store).await;

    let pending = || {
        let catalog = store.journal.model.0.read().unwrap();
        let mut offsets = Vec::new();
        let done = catalog.visit_dirty_recency(&mut (0, 0), (u64::MAX, u64::MAX), 16, |id, row| {
            offsets.push((id, row));
            true
        });
        assert!(done);
        offsets
    };
    assert!(pending().is_empty());

    // The second delete crosses the 1% compaction threshold, moving dirty
    // rows that previously straddled two bitmap-word boundaries.
    for n in [64, 129] {
        assert!(store.get(&id(n)).await.unwrap().is_some());
    }
    let two_rows = pending();
    assert_eq!(two_rows.len(), 2);
    for n in [0, 1] {
        store.delete(&id(n)).await.unwrap();
        policy.remove(&CacheUnit::Whole(id(n)));
    }
    assert_eq!(
        pending(),
        two_rows
            .into_iter()
            .map(|(segment, row)| (segment, row - 2))
            .collect::<Vec<_>>()
    );

    flush(&store).await;
    assert!(pending().is_empty());
    let expected = [64, 129].map(|n| {
        let loc = store.lookup_entry(&id(n)).unwrap().unwrap();
        (loc.segment, loc.offset as u32, compact_age(&store, &id(n)))
    });
    assert_eq!(compact::read_log(dir.path()), expected);
}

#[tokio::test]
async fn compact_full_checkpoint_retries_after_sampling_with_one_io_permit() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = open_compact(dir.path());
    let budget = crate::background::BackgroundBudget::new(&talon_core::WorkerConfig {
        background_io_concurrency: 1,
        background_io_max_mb_per_sec: 0,
        ..Default::default()
    });
    let flush_with_budget = || async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.checkpoint_access(Some(budget.clone()), None),
        )
        .await
        .expect("a full checkpoint must make progress with a single I/O permit")
    };
    let key = id(1);
    store
        .put(&key, Bytes::from_static(b"payload"))
        .await
        .unwrap();
    store.maintain(true).await.unwrap();
    {
        let catalog = store.journal.model.0.read().unwrap();
        let slot = catalog
            .find(&key.view(), super::super::lookup::VISIBLE)
            .unwrap();
        let (_, time) = catalog.recency_at(slot).unwrap();
        time.store(access_millis() - 30_000, Ordering::Relaxed);
    }
    flush_with_budget().await.unwrap();
    assert!(store.get(&key).await.unwrap().is_some());
    let touched = compact_age(&store, &key);

    store.journal.access.disk.lock().unwrap().full = true;
    // Fail after the full snapshot sampled the dirty rows and replaced FILE.
    let log_temp = dir.path().join("access.log.tmp");
    std::fs::create_dir(&log_temp).unwrap();
    assert!(flush_with_budget().await.is_err());
    assert!(store.journal.access.disk.lock().unwrap().full);
    std::fs::remove_dir(log_temp).unwrap();
    flush_with_budget().await.unwrap();
    drop(store);

    let (store, _) = open_compact(dir.path());
    assert_eq!(compact_age(&store, &key), touched);
}

#[tokio::test]
async fn compact_idle_sweep_revisits_old_rows_during_active_appends() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = open_compact(dir.path());
    let mut cursor = IdleRecencyCursor::default();
    let age = 10_000;
    for key in [id(0), id(1)] {
        store
            .put(&key, Bytes::from_static(b"payload"))
            .await
            .unwrap();
    }
    let now = access_millis();
    assert!(store
        .idle_recency_candidates(&mut cursor, now, age, 1, 10)
        .is_empty());

    let expired_at = now + age + 1;
    store
        .put(&id(2), Bytes::from_static(b"payload"))
        .await
        .unwrap();
    let _ = store.idle_recency_candidates(&mut cursor, expired_at, age, 1, 10);
    store
        .put(&id(3), Bytes::from_static(b"payload"))
        .await
        .unwrap();
    let (unit, ticket) = store
        .idle_recency_candidates(&mut cursor, expired_at, age, 1, 10)
        .pop()
        .expect("the next sweep revisits the oldest row");
    let oldest = id(0);
    assert_eq!(unit, CacheUnit::Whole(oldest.clone()));
    assert!(store
        .delete_idle(unit, ticket, move || (expired_at, age))
        .await
        .unwrap());
    assert!(!store.contains(&oldest));
}

#[test]
fn access_records_preserve_whole_and_exact_range_identities() {
    let whole = CacheUnit::Whole(id(0));
    let exact = CacheUnit::ExactRange(id(0));
    let mut shifted = id(0);
    shifted.offset = 1;
    let shifted = CacheUnit::ExactRange(shifted);
    let mut bytes = Vec::new();
    for (unit, age) in [(&whole, 10), (&exact, 20), (&shifted, 30), (&exact, 15)] {
        encode(&mut bytes, unit, age).unwrap();
    }
    let mut ages = HashMap::new();
    decode(&bytes, &mut ages).unwrap();
    assert_eq!(
        ages,
        HashMap::from([(whole, 10), (exact, 20), (shifted, 30)])
    );
    let mut deltas = HashMap::new();
    assert!(!decode_log(&bytes, &mut deltas).unwrap());
    assert_eq!(deltas, ages);
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
    assert_eq!(
        log(dir.path())[&CacheUnit::Whole(id(7))],
        access.last_access()
    );
    flush(&store).await;
    assert_eq!(std::fs::read(dir.path().join(LOG)).unwrap(), bytes);
    assert_eq!(
        store.journal.access.allocated_bytes(),
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
    assert_eq!(ages[&CacheUnit::Whole(id(0))], new.last_access());
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
    assert_eq!(
        log(dir.path())[&CacheUnit::Whole(id(0))],
        access.last_access()
    );
}

#[tokio::test]
async fn failed_append_retries_as_full_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let access = put(&store, &id(0)).await;
    flush(&store).await;
    access.touch();
    std::fs::remove_file(dir.path().join(LOG)).unwrap();
    assert!(store.checkpoint_access(None, None).await.is_err());
    assert!(store.journal.access.disk.lock().unwrap().full);
    flush(&store).await;
    assert_eq!(store.journal.access.disk.lock().unwrap().generation, 2);
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
    store.journal.access.disk.lock().unwrap().full = true;
    // Fail after durable snapshot replacement, before the new log exists.
    std::fs::create_dir(dir.path().join("access.log.tmp")).unwrap();
    assert!(store.checkpoint_access(None, None).await.is_err());
    assert_eq!(std::fs::read(dir.path().join(LOG)).unwrap(), old_log);
    let last = access.last_access();
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.access(&id(0)).unwrap().last_access(), last);
    assert!(store.journal.access.disk.lock().unwrap().full);
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
    encode(&mut bytes, &id(0), cold).unwrap();
    std::fs::write(dir.path().join(FILE), bytes).unwrap();
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.access(&id(0)).unwrap().last_access(), cold);
    flush(&store).await;
    store.access(&id(0)).unwrap().touch();
    flush(&store).await;
    assert_eq!(log(dir.path()).len(), 1);
    store.journal.access.disk.lock().unwrap().log_bytes = MIN_COMPACT_BYTES;
    flush(&store).await;
    assert_eq!(store.journal.access.disk.lock().unwrap().generation, 2);
    assert!(log(dir.path()).is_empty());
}

#[tokio::test]
async fn log_compaction_requires_one_mib_and_half_the_snapshot_size() {
    for (snapshot_bytes, threshold) in [
        (64 << 10, 1 << 20),
        (1536 << 10, 1 << 20),
        (2 << 20, 1 << 20),
        (4 << 20, 2 << 20),
        ((4 << 20) + 1, (2 << 20) + 1),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        put(&store, &id(0)).await;
        flush(&store).await;
        let baseline = std::fs::read(dir.path().join(FILE)).unwrap();
        {
            let mut state = store.journal.access.disk.lock().unwrap();
            state.snapshot_bytes = snapshot_bytes;
            state.log_bytes = threshold - 1;
        }
        flush(&store).await;
        assert_eq!(store.journal.access.disk.lock().unwrap().generation, 1);
        assert_eq!(std::fs::read(dir.path().join(FILE)).unwrap(), baseline);

        store.journal.access.disk.lock().unwrap().log_bytes = threshold;
        flush(&store).await;
        assert_eq!(store.journal.access.disk.lock().unwrap().generation, 2);
        assert_eq!(
            std::fs::metadata(dir.path().join(LOG)).unwrap().len(),
            HEADER
        );
    }
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
    assert_eq!(store.journal.access.disk.lock().unwrap().generation, 2);
    access.touch();
    flush(&store).await;
    assert_eq!(
        log(dir.path())[&CacheUnit::Whole(id(0))],
        access.last_access()
    );
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
        assert!(store.journal.access.disk.lock().unwrap().full);
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
    let file = Arc::new(SegmentFile {
        path: dir.path().join("probe.data"),
        fd: tokio::sync::Mutex::new(Arc::downgrade(&fd)),
    });
    let stream = store.streams().remove(0);
    for n in 0..count {
        let mut block = id(n);
        block.object.object_path = format!("{n:092}"); // bucket + path + version = 100 bytes
        let key = SmallKey::new(&block);
        stream.entries.write().unwrap().insert(
            key.clone(),
            Location {
                file: file.clone(),
                segment: 1,
                offset: n as u64,
                len: 1,
                checksum: Some(0),
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
    assert!(store.journal.access.disk.lock().unwrap().full);
    flush(&store).await;
    let mut ages = HashMap::new();
    decode(
        &std::fs::read(dir.path().join(FILE)).unwrap()[HEADER as usize..],
        &mut ages,
    )
    .unwrap();
    assert_eq!(ages[&CacheUnit::Whole(id(1))], recovered_age);
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.access(&id(1)).unwrap().last_access(), recovered_age);
}
