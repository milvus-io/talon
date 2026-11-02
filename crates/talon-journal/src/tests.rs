use super::*;
use std::{sync::atomic::AtomicUsize, time::Duration};
#[derive(Default)]
pub(crate) struct Pause {
    pub entered: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
}
#[derive(Default)]
pub(crate) struct Control {
    pub write_pause: Mutex<Option<Arc<Pause>>>,
    pub sync_pause: Mutex<Option<Arc<Pause>>>,
    pub snapshot_finalize_pause: Mutex<Option<Arc<Pause>>>,
    pub writes: AtomicUsize,
    pub syncs: AtomicUsize,
    pub fail_sync: AtomicBool,
}
pub(crate) async fn pause(slot: &Mutex<Option<Arc<Pause>>>) {
    let pause = slot.lock().unwrap().take();
    if let Some(p) = pause {
        p.entered.notify_one();
        p.resume.notified().await;
    }
}
pub(crate) fn crash(stage: &str) {
    if std::env::var("TALON_JOURNAL_CRASH_STAGE").as_deref() == Ok(stage) {
        std::fs::write(
            std::env::var_os("TALON_JOURNAL_CRASH_READY").unwrap(),
            b"ready",
        )
        .unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[derive(Clone, Default)]
struct State(BTreeMap<u64, u64>);
impl Machine for State {
    type Record = (u64, u64);
    type SnapshotRecord = (u64, u64);
    type Snapshot = std::vec::IntoIter<io::Result<Bytes>>;
    fn encoded_len(_: &Self::Record) -> io::Result<usize> {
        Ok(16)
    }
    fn encode(r: &Self::Record) -> io::Result<Bytes> {
        Ok([r.0.to_le_bytes(), r.1.to_le_bytes()].concat().into())
    }
    fn decode(b: &[u8]) -> io::Result<Self::Record> {
        if b.len() != 16 {
            return Err(invalid("test record length"));
        }
        Ok((
            u64::from_le_bytes(b[..8].try_into().unwrap()),
            u64::from_le_bytes(b[8..].try_into().unwrap()),
        ))
    }
    fn apply(&mut self, _: Lsn, r: Self::Record) -> io::Result<()> {
        if r.1 == 0 {
            self.0.remove(&r.0);
        } else {
            self.0.insert(r.0, r.1);
        }
        Ok(())
    }
    fn snapshot(&self) -> io::Result<Self::Snapshot> {
        Ok(self
            .0
            .iter()
            .map(|(k, v)| Self::encode(&(*k, *v)))
            .collect::<Vec<_>>()
            .into_iter())
    }
    fn decode_snapshot(b: &[u8]) -> io::Result<Self::SnapshotRecord> {
        Self::decode(b)
    }
    fn restore(&mut self, r: Self::SnapshotRecord) -> io::Result<()> {
        self.apply(0, r)
    }
}
fn service(root: &Path) -> Arc<JournalService> {
    JournalService::open(
        root.to_owned(),
        Arc::new(()),
        Options {
            require_native: true,
            ..Default::default()
        },
    )
    .unwrap()
}
fn stream(s: &Arc<JournalService>, name: &str) -> Handle<State> {
    s.open_stream(name.into(), State::default(), StreamOptions::default())
        .unwrap()
}

#[test]
fn blocking_queries_do_not_exhaust_outer_tokio_budget() {
    const CHILD: &str = "TALON_JOURNAL_COOP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let root = tempfile::tempdir().unwrap();
                let service = service(root.path());
                let handle = stream(&service, "a");
                // Synchronous startup calls cannot yield back to the outer
                // Tokio executor to replenish its cooperative budget.
                for _ in 0..512 {
                    assert_eq!(handle.query_blocking(|s| Ok(s.0.len())).unwrap(), 0);
                }
            });
        return;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::blocking_queries_do_not_exhaust_outer_tokio_budget",
        ])
        .env(CHILD, "1")
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if started.elapsed() > Duration::from_secs(5) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("blocking query spun after exhausting the outer Tokio budget");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn snapshot_and_tail_recover_and_streams_remain_independent() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = stream(&s, "a");
    let b = stream(&s, "b");
    assert_eq!(a.append((1, 1), Ack::Written).await.unwrap(), 1);
    a.checkpoint().await.unwrap();
    a.append((2, 2), Ack::Durable).await.unwrap();
    a.append((1, 0), Ack::Durable).await.unwrap();
    b.append((1, 9), Ack::Durable).await.unwrap();
    a.poison();
    assert!(a.append((3, 3), Ack::Written).await.is_err());
    b.append((2, 8), Ack::Durable).await.unwrap();
    drop(a);
    drop(b);
    drop(s);
    let s = service(dir.path());
    let a = stream(&s, "a");
    let b = stream(&s, "b");
    assert_eq!(
        a.query(|m| Ok(m.0.clone())).await.unwrap(),
        BTreeMap::from([(2, 2)])
    );
    assert_eq!(
        b.query(|m| Ok(m.0.clone())).await.unwrap(),
        BTreeMap::from([(1, 9), (2, 8)])
    );
}
#[tokio::test]
async fn batches_are_serialized_while_other_streams_progress() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = stream(&s, "a");
    let b = stream(&s, "b");
    let pause = Arc::new(Pause::default());
    *a.inner.control.write_pause.lock().unwrap() = Some(pause.clone());
    let h = a.clone();
    let first = tokio::spawn(async move { h.append((1, 1), Ack::Written).await });
    pause.entered.notified().await;
    let h = a.clone();
    let second = tokio::spawn(async move { h.append((2, 2), Ack::Durable).await });
    let progress = tokio::time::timeout(Duration::from_secs(2), async {
        while s.admission.count.available_permits() > s.options.max_requests - 2 {
            tokio::task::yield_now().await;
        }
        b.append((3, 3), Ack::Durable).await.unwrap();
    })
    .await;
    // The second request is admitted but must not extend the log past the
    // paused first batch. Other streams still complete durable writes.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let writes = a.inner.control.writes.load(Ordering::Relaxed);
    let length = std::fs::metadata(dir.path().join("a/journal.00000000000000000001"))
        .unwrap()
        .len();
    let syncs = a.inner.control.syncs.load(Ordering::Relaxed);
    pause.resume.notify_one();
    progress.unwrap();
    assert_eq!(writes, 1);
    assert_eq!(length, 0);
    assert_eq!(syncs, 0);
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert!(a.inner.control.syncs.load(Ordering::Relaxed) > 0);
}
#[tokio::test]
async fn cancelled_request_retains_service_admission_until_io_completes() {
    let dir = tempfile::tempdir().unwrap();
    let s = JournalService::open(
        dir.path().to_owned(),
        Arc::new(()),
        Options {
            require_native: true,
            max_requests: 1,
            max_bytes: 48,
            ..Default::default()
        },
    )
    .unwrap();
    let a = s
        .open_stream(
            "a".into(),
            State::default(),
            StreamOptions {
                max_record_bytes: 16,
                ..Default::default()
            },
        )
        .unwrap();
    let pause = Arc::new(Pause::default());
    *a.inner.control.write_pause.lock().unwrap() = Some(pause.clone());
    let h = a.clone();
    let first = tokio::spawn(async move { h.append((1, 1), Ack::Written).await });
    pause.entered.notified().await;
    first.abort();
    let _ = first.await;
    assert_eq!(s.admission.count.available_permits(), 0);
    assert_eq!(s.admission.bytes.available_permits(), 0);
    let h = a.clone();
    let mut second = tokio::spawn(async move { h.append((2, 2), Ack::Durable).await });
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut second)
        .await
        .is_err());
    pause.resume.notify_one();
    second.await.unwrap().unwrap();
    assert_eq!(s.admission.count.available_permits(), 1);
    assert_eq!(s.admission.bytes.available_permits(), 48);
}
#[tokio::test]
async fn queries_do_not_expose_state_after_failed_sync() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = stream(&s, "a");
    a.append((1, 1), Ack::Written).await.unwrap();
    a.inner.control.fail_sync.store(true, Ordering::Relaxed);
    assert!(a.query(|m| Ok(m.0.clone())).await.is_err());
}
#[test]
fn frames_reject_corrupt_headers_and_order_parallel_decode() {
    let bytes = frame::encode(1, &State::encode(&(1, 2)).unwrap()).unwrap();
    for at in [0, 8, 12, 16, 24, 32] {
        let mut b = bytes.to_vec();
        b[at] ^= 1;
        assert!(frame::decode(b, State::decode).is_err());
    }
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("log");
    let mut bytes = Vec::new();
    for n in 1..10000 {
        bytes.extend_from_slice(&frame::encode(n, &State::encode(&(n, n)).unwrap()).unwrap());
    }
    std::fs::write(&p, &bytes).unwrap();
    for threads in [1, 4] {
        let mut last = 0;
        let end = replay::read(&p, false, threads, State::decode, |n, r| {
            assert_eq!(n, last + 1);
            assert_eq!(r, (n, n));
            last = n;
            Ok(())
        })
        .unwrap();
        assert_eq!(end, bytes.len() as u64);
    }
    bytes.extend_from_slice(&[1, 2, 3]);
    std::fs::write(&p, &bytes).unwrap();
    assert_eq!(
        replay::read(&p, true, 4, State::decode, |_, _| Ok(())).unwrap(),
        bytes.len() as u64 - 3
    );
    assert!(replay::read(&p, false, 4, State::decode, |_, _| Ok(())).is_err());
}
#[tokio::test]
async fn partial_tail_repaired_but_checksum_corruption_fails_closed() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = stream(&s, "a");
    a.append((1, 1), Ack::Durable).await.unwrap();
    drop(a);
    drop(s);
    JournalService::wait_for_retiring(dir.path()).unwrap();
    let p = dir.path().join("a/journal.00000000000000000001");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    let s = service(dir.path());
    let a = stream(&s, "a");
    assert_eq!(a.append((2, 2), Ack::Durable).await.unwrap(), 2);
    drop(a);
    drop(s);
    JournalService::wait_for_retiring(dir.path()).unwrap();
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[32] ^= 1;
    std::fs::write(&p, bytes).unwrap();
    let s = service(dir.path());
    assert!(s
        .open_stream("a".into(), State::default(), StreamOptions::default())
        .is_err());
}
#[tokio::test]
async fn crash_child() {
    let Ok(root) = std::env::var("TALON_JOURNAL_CHILD_ROOT") else {
        return;
    };
    let s = service(Path::new(&root));
    let a = stream(&s, "a");
    a.append((1, 1), Ack::Durable).await.unwrap();
    let stage = std::env::var("TALON_JOURNAL_CHILD_STAGE").unwrap();
    if stage == "queued_append" {
        let pause = Arc::new(Pause::default());
        *a.inner.control.write_pause.lock().unwrap() = Some(pause.clone());
        let h = a.clone();
        let _first = tokio::spawn(async move { h.append((2, 2), Ack::Written).await });
        pause.entered.notified().await;
        let h = a.clone();
        let _second = tokio::spawn(async move { h.append((3, 3), Ack::Durable).await });
        while s.admission.count.available_permits() > s.options.max_requests - 2 {
            tokio::task::yield_now().await;
        }
        // Let the shared reactor run while the first batch remains paused.
        // With parallel writes the later batch creates an interior log hole.
        let b = stream(&s, "b");
        b.append((9, 9), Ack::Durable).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        std::env::set_var("TALON_JOURNAL_CRASH_STAGE", &stage);
        crash(&stage);
        unreachable!();
    }
    a.checkpoint().await.unwrap();
    a.append((2, 2), Ack::Durable).await.unwrap();
    a.append((1, 0), Ack::Durable).await.unwrap();
    std::env::set_var("TALON_JOURNAL_CRASH_STAGE", stage);
    a.checkpoint().await.unwrap();
}
#[test]
fn process_loss_at_write_and_checkpoint_boundaries() {
    for stage in [
        "queued_append",
        "rotate",
        "snapshot_file",
        "snapshot_current",
        "cleanup",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ready = dir.path().join("ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::crash_child", "--nocapture"])
            .env("TALON_JOURNAL_CHILD_ROOT", dir.path())
            .env("TALON_JOURNAL_CHILD_STAGE", stage)
            .env("TALON_JOURNAL_CRASH_READY", &ready)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let start = std::time::Instant::now();
        while !ready.exists() && start.elapsed() < Duration::from_secs(10) {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let reached = ready.exists();
        let _ = child.kill();
        child.wait().unwrap();
        assert!(reached, "{stage}");
        let s = service(dir.path());
        let a = stream(&s, "a");
        assert_eq!(
            a.query_blocking(|m| Ok(m.0.clone())).unwrap(),
            BTreeMap::from([if stage == "queued_append" {
                (1, 1)
            } else {
                (2, 2)
            }])
        );
    }
}

struct BlockSnapshot {
    gate: Arc<tokio::sync::Semaphore>,
    entered: tokio::sync::Notify,
}
impl BlockSnapshot {
    fn new(gate: Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            gate,
            entered: tokio::sync::Notify::new(),
        }
    }
}
impl Maintenance for BlockSnapshot {
    fn bytes(&self, _: usize) -> futures::future::LocalBoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn io(&self) -> futures::future::LocalBoxFuture<'_, Box<dyn Send>> {
        Box::pin(async move {
            self.entered.notify_one();
            Box::new(self.gate.clone().acquire_owned().await.unwrap()) as Box<dyn Send>
        })
    }
    fn delete(&self) -> futures::future::LocalBoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
#[tokio::test]
async fn queued_checkpoint_does_not_block_append_holding_snapshot_permit() {
    let dir = tempfile::tempdir().unwrap();
    let s = JournalService::open(
        dir.path().to_owned(),
        Arc::new(()),
        Options {
            max_requests: 1,
            require_native: true,
            ..Default::default()
        },
    )
    .unwrap();
    let gate = Arc::new(Semaphore::new(1));
    let permit = gate.clone().acquire_owned().await.unwrap();
    let policy = Arc::new(BlockSnapshot::new(gate.clone()));
    let a = s
        .open_stream_with_maintenance(
            "a".into(),
            State::default(),
            StreamOptions {
                checkpoint_bytes: 96,
                ..Default::default()
            },
            Some(policy.clone()),
        )
        .unwrap();
    a.append((1, 1), Ack::Durable).await.unwrap();
    a.append((2, 2), Ack::Durable).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), policy.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while s.admission.count.available_permits() != s.options.max_requests {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut checkpoint = Box::pin(a.checkpoint());
    assert!(futures::poll!(checkpoint.as_mut()).is_pending());
    let h = a.clone();
    let mut append = tokio::spawn(async move {
        let _permit = permit;
        h.append((3, 3), Ack::Durable).await
    });
    let result = tokio::time::timeout(Duration::from_secs(2), &mut append).await;
    if result.is_err() {
        gate.add_permits(1);
        append.await.unwrap().unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), checkpoint)
        .await
        .unwrap()
        .unwrap();
    result
        .expect("queued checkpoint blocked the holder of its snapshot permit")
        .unwrap()
        .unwrap();
    assert_eq!(a.query(|m| Ok(m.0[&3])).await.unwrap(), 3);
    assert!(!dir.path().join("a/journal.00000000000000000002").exists());
    drop(a);
    drop(s);
    let s = service(dir.path());
    assert_eq!(stream(&s, "a").query(|m| Ok(m.0.len())).await.unwrap(), 3);
}

#[tokio::test]
async fn snapshot_finalization_holds_budget_through_publication_and_cleanup() {
    struct Policy {
        gate: Arc<Semaphore>,
        unbudgeted_deletes: AtomicUsize,
        deletes: AtomicUsize,
    }
    impl Maintenance for Policy {
        fn bytes(&self, _: usize) -> LocalBoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn io(&self) -> LocalBoxFuture<'_, Box<dyn Send>> {
            Box::pin(async move {
                Box::new(self.gate.clone().acquire_owned().await.unwrap()) as Box<dyn Send>
            })
        }
        fn delete(&self) -> LocalBoxFuture<'_, ()> {
            Box::pin(async move {
                self.deletes.fetch_add(1, Ordering::Relaxed);
                if self.gate.available_permits() != 0 {
                    self.unbudgeted_deletes.fetch_add(1, Ordering::Relaxed);
                }
            })
        }
    }
    for populated in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let s = service(dir.path());
        let a = stream(&s, "a");
        let policy = Arc::new(Policy {
            gate: Arc::new(Semaphore::new(1)),
            unbudgeted_deletes: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
        });
        a.set_maintenance(policy.clone());
        if populated {
            a.append((1, 1), Ack::Durable).await.unwrap();
        }
        let pause = Arc::new(Pause::default());
        *a.inner.control.snapshot_finalize_pause.lock().unwrap() = Some(pause.clone());
        let h = a.clone();
        let checkpoint = tokio::spawn(async move { h.checkpoint().await });
        tokio::time::timeout(Duration::from_secs(2), pause.entered.notified())
            .await
            .unwrap();
        let permit = policy.gate.clone().acquire_owned().await.unwrap();
        let current = std::fs::read(dir.path().join("a/CURRENT")).unwrap();
        pause.resume.notify_one();
        let progress =
            tokio::time::timeout(Duration::from_secs(2), a.append((2, 2), Ack::Durable)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let unpublished = !dir.path().join("a/snapshot.00000000000000000002").exists()
            && std::fs::read(dir.path().join("a/CURRENT")).unwrap() == current
            && dir.path().join("a/journal.00000000000000000001").exists();
        drop(permit);
        tokio::time::timeout(Duration::from_secs(2), checkpoint)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        progress
            .expect("waiting for maintenance must not block tail acknowledgements")
            .unwrap();
        assert!(
            unpublished,
            "snapshot finalized without the maintenance permit"
        );
        assert!(policy.deletes.load(Ordering::Relaxed) > 0);
        assert_eq!(policy.unbudgeted_deletes.load(Ordering::Relaxed), 0);
        assert_eq!(policy.gate.available_permits(), 1);
    }
}

#[tokio::test]
async fn poisoned_writer_releases_waiters_before_draining_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = gate.clone().acquire_owned().await.unwrap();
    let policy = Arc::new(BlockSnapshot::new(gate.clone()));
    let a = s
        .open_stream_with_maintenance(
            "a".into(),
            State::default(),
            StreamOptions {
                checkpoint_bytes: 96,
                ..Default::default()
            },
            Some(policy.clone()),
        )
        .unwrap();
    a.append((1, 1), Ack::Durable).await.unwrap();
    a.append((2, 2), Ack::Durable).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), policy.entered.notified())
        .await
        .unwrap();

    let pause = Arc::new(Pause::default());
    *a.inner.control.write_pause.lock().unwrap() = Some(pause.clone());
    let h = a.clone();
    let writing = tokio::spawn(async move { h.append((3, 3), Ack::Written).await });
    pause.entered.notified().await;
    let h = a.clone();
    let mut queued = tokio::spawn(async move {
        let _permit = permit;
        h.append((4, 4), Ack::Durable).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while s.admission.count.available_permits() > s.options.max_requests - 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    a.poison();
    pause.resume.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), &mut queued).await;
    // Release a stuck writer before reporting failure, so teardown cannot hang.
    if result.is_err() {
        gate.add_permits(1);
        assert!(queued.await.unwrap().is_err());
    }
    let _ = writing.await.unwrap();
    drop(a);
    drop(s);
    let reopened = service(dir.path());
    assert!(result
        .expect("queued acknowledgement retained the snapshot permit")
        .unwrap()
        .is_err());
    drop(reopened);
}

#[tokio::test]
async fn recovered_tail_uses_initial_maintenance_for_automatic_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = stream(&s, "a");
    a.append((1, 1), Ack::Durable).await.unwrap();
    a.append((2, 2), Ack::Durable).await.unwrap();
    drop(a);
    drop(s);

    let s = service(dir.path());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let policy = Arc::new(BlockSnapshot::new(gate.clone()));
    let a = s
        .open_stream_with_maintenance(
            "a".into(),
            State::default(),
            StreamOptions {
                checkpoint_bytes: 96,
                ..Default::default()
            },
            Some(policy.clone()),
        )
        .unwrap();
    // Recovery alone starts this checkpoint, before any new request is sent.
    tokio::time::timeout(Duration::from_secs(2), policy.entered.notified())
        .await
        .unwrap();
    assert!(!dir.path().join("a/snapshot.tmp").exists());
    let progress =
        tokio::time::timeout(Duration::from_secs(2), a.append((1, 0), Ack::Durable)).await;
    gate.add_permits(1);
    progress.unwrap().unwrap();
    a.checkpoint().await.unwrap();
    drop(a);
    drop(s);
    let s = service(dir.path());
    let a = stream(&s, "a");
    assert_eq!(
        a.query(|m| Ok(m.0.clone())).await.unwrap(),
        BTreeMap::from([(2, 2)])
    );
}
#[tokio::test]
async fn automatic_snapshot_overlaps_tail_writes_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = s
        .open_stream(
            "a".into(),
            State::default(),
            StreamOptions {
                checkpoint_bytes: 96,
                ..Default::default()
            },
        )
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    a.set_maintenance(Arc::new(BlockSnapshot::new(gate.clone())));
    a.append((1, 1), Ack::Written).await.unwrap();
    a.append((2, 2), Ack::Durable).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !dir.path().join("a/journal.00000000000000000002").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let progress =
        tokio::time::timeout(Duration::from_secs(2), a.append((1, 0), Ack::Durable)).await;
    gate.add_permits(1);
    progress.unwrap().unwrap();
    a.checkpoint().await.unwrap();
    drop(a);
    drop(s);
    let s = service(dir.path());
    let a = stream(&s, "a");
    assert_eq!(
        a.query(|m| Ok(m.0.clone())).await.unwrap(),
        BTreeMap::from([(2, 2)])
    );
}
#[derive(Default)]
struct PanicSnapshot(State);
impl Machine for PanicSnapshot {
    type Record = (u64, u64);
    type SnapshotRecord = (u64, u64);
    type Snapshot = Box<dyn Iterator<Item = io::Result<Bytes>> + Send>;
    fn encoded_len(r: &Self::Record) -> io::Result<usize> {
        State::encoded_len(r)
    }
    fn encode(r: &Self::Record) -> io::Result<Bytes> {
        State::encode(r)
    }
    fn decode(b: &[u8]) -> io::Result<Self::Record> {
        State::decode(b)
    }
    fn apply(&mut self, n: Lsn, r: Self::Record) -> io::Result<()> {
        self.0.apply(n, r)
    }
    fn snapshot(&self) -> io::Result<Self::Snapshot> {
        Ok(Box::new(std::iter::from_fn(|| {
            panic!("injected snapshot encoder panic")
        })))
    }
    fn decode_snapshot(b: &[u8]) -> io::Result<Self::SnapshotRecord> {
        State::decode(b)
    }
    fn restore(&mut self, r: Self::SnapshotRecord) -> io::Result<()> {
        self.0.restore(r)
    }
}
#[tokio::test]
async fn encoder_panic_never_publishes_a_partial_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = s
        .open_stream(
            "a".into(),
            PanicSnapshot::default(),
            StreamOptions::default(),
        )
        .unwrap();
    a.append((1, 1), Ack::Durable).await.unwrap();
    assert!(a.checkpoint().await.is_err());
    drop(a);
    drop(s);
    let s = service(dir.path());
    let a = stream(&s, "a");
    assert_eq!(
        a.query(|m| Ok(m.0.clone())).await.unwrap(),
        BTreeMap::from([(1, 1)])
    );
}
#[tokio::test]
async fn service_lease_and_reopen_wait_for_cancelled_inflight_write() {
    struct Lease(Arc<AtomicBool>);
    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let released = Arc::new(AtomicBool::new(false));
    let s = JournalService::open(
        dir.path().to_owned(),
        Arc::new(Lease(released.clone())),
        Options {
            require_native: true,
            ..Default::default()
        },
    )
    .unwrap();
    let a = stream(&s, "a");
    let pause = Arc::new(Pause::default());
    *a.inner.control.write_pause.lock().unwrap() = Some(pause.clone());
    let h = a.clone();
    let request = tokio::spawn(async move { h.append((1, 1), Ack::Written).await });
    pause.entered.notified().await;
    request.abort();
    let _ = request.await;
    drop(a);
    drop(s);
    assert!(!released.load(Ordering::Acquire));
    let root = dir.path().to_owned();
    let (send, wait) = std::sync::mpsc::channel();
    let reopener = std::thread::spawn(move || {
        send.send(service(&root)).unwrap();
    });
    let early = wait.recv_timeout(Duration::from_millis(20));
    pause.resume.notify_one();
    assert!(early.is_err());
    let reopened = wait.recv_timeout(Duration::from_secs(2)).unwrap();
    reopener.join().unwrap();
    assert!(released.load(Ordering::Acquire));
    drop(reopened);
}
#[tokio::test]
async fn compatibility_driver_can_write_checkpoint_and_recover() {
    let dir = tempfile::tempdir().unwrap();
    let s = JournalService::open(dir.path().to_owned(), Arc::new(()), Options::default()).unwrap();
    if std::env::var_os("TALON_JOURNAL_REQUIRE_LEGACY").is_some() {
        assert!(!s.is_native());
    }
    let a = stream(&s, "a");
    a.append((1, 1), Ack::Durable).await.unwrap();
    a.checkpoint().await.unwrap();
    drop(a);
    drop(s);
    let s = JournalService::open(dir.path().to_owned(), Arc::new(()), Options::default()).unwrap();
    let a = stream(&s, "a");
    assert_eq!(
        a.query(|m| Ok(m.0.clone())).await.unwrap(),
        BTreeMap::from([(1, 1)])
    );
}

#[tokio::test]
async fn valid_frames_with_lsn_gap_fail_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let a = stream(&s, "a");
    a.append((1, 1), Ack::Durable).await.unwrap();
    drop(a);
    drop(s);
    JournalService::wait_for_retiring(dir.path()).unwrap();
    use std::io::Write;
    let p = dir.path().join("a/journal.00000000000000000001");
    std::fs::OpenOptions::new()
        .append(true)
        .open(p)
        .unwrap()
        .write_all(&frame::encode(3, &State::encode(&(2, 2)).unwrap()).unwrap())
        .unwrap();
    let s = service(dir.path());
    assert!(s
        .open_stream("a".into(), State::default(), StreamOptions::default())
        .is_err());
}
#[test]
fn stream_paths_reject_symlink_ancestors_and_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    std::os::unix::fs::symlink(outside.path(), dir.path().join("escape")).unwrap();
    for p in ["escape/stream", "../outside", "/absolute"] {
        assert!(s
            .open_stream(p.into(), State::default(), StreamOptions::default())
            .is_err());
    }
    assert!(std::fs::read_dir(outside.path()).unwrap().next().is_none());
}

#[tokio::test]
async fn stream_record_limit_rejects_frames_larger_than_the_format_supports() {
    let dir = tempfile::tempdir().unwrap();
    let service = JournalService::open(
        dir.path().to_owned(),
        Arc::new(()),
        Options {
            require_native: true,
            max_bytes: frame::HEADER + frame::MAX_RECORD + 1,
            ..Default::default()
        },
    )
    .unwrap();
    let rejected = service.open_stream(
        "bounded".into(),
        State::default(),
        StreamOptions {
            max_record_bytes: frame::MAX_RECORD + 1,
            ..Default::default()
        },
    );
    assert_eq!(rejected.err().unwrap().kind(), io::ErrorKind::InvalidData);
    assert!(!dir.path().join("bounded").exists());
    let handle = service
        .open_stream(
            "bounded".into(),
            State::default(),
            StreamOptions {
                max_record_bytes: frame::MAX_RECORD,
                ..Default::default()
            },
        )
        .unwrap();
    handle.append((1, 2), Ack::Durable).await.unwrap();
    assert_eq!(handle.query(|s| Ok(s.0[&1])).await.unwrap(), 2);
}

#[tokio::test]
async fn service_bounds_snapshot_encoders_without_blocking_other_appends() {
    let dir = tempfile::tempdir().unwrap();
    let s = service(dir.path());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut checkpoints = Vec::new();
    for name in ["a", "b"] {
        let a = stream(&s, name);
        a.set_maintenance(Arc::new(BlockSnapshot::new(gate.clone())));
        a.append((1, 1), Ack::Written).await.unwrap();
        checkpoints.push(tokio::spawn(async move { a.checkpoint().await }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while s.snapshot_slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let c = stream(&s, "c");
    c.append((1, 1), Ack::Written).await.unwrap();
    let h = c.clone();
    let third = tokio::spawn(async move { h.checkpoint().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !dir.path().join("c/journal.00000000000000000002").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!dir.path().join("c/snapshot.tmp").exists());
    let progress =
        tokio::time::timeout(Duration::from_secs(2), c.append((2, 2), Ack::Durable)).await;
    gate.add_permits(2);
    progress.unwrap().unwrap();
    for task in checkpoints {
        task.await.unwrap().unwrap();
    }
    third.await.unwrap().unwrap();
}
