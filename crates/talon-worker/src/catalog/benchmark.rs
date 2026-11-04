//! Manual benchmark of the shared engine with the real paged adapter.
use super::*;
use crate::block_key::BlockKey;
use std::time::Instant;
use talon_core::{Backend, ObjectId, Version};
use talon_journal::Machine;

#[test]
#[ignore = "manual native journal startup benchmark"]
fn restart_replay_and_index() {
    if let Some(root) = std::env::var_os("TALON_CATALOG_BENCH_ROOT") {
        let root = PathBuf::from(root);
        let threads: usize = std::env::var("TALON_CATALOG_BENCH_THREADS")
            .unwrap()
            .parse()
            .unwrap();
        let before = usage();
        let started = Instant::now();
        let lock = Arc::new(CacheRootLock::acquire(&root).unwrap());
        let service = JournalService::open(
            root.clone(),
            lock,
            Options {
                require_native: true,
                recovery_threads: threads,
                ..Options::default()
            },
        )
        .unwrap();
        let catalog =
            Catalog::with_service(&service, service.root(), transaction_budget(), None).unwrap();
        let replay_seconds = started.elapsed().as_secs_f64();
        let entries = catalog
            .journal
            .query_blocking(move |state| {
                state
                    .entries_with_threads("paged", threads)
                    .map_err(std::io::Error::other)
            })
            .unwrap()
            .unwrap();
        let mut index = crate::BlockIndex::new();
        index.restore_allocated(entries, threads);
        assert_eq!(index.page_count(), index.len() as u64 * 32);
        let seconds = started.elapsed().as_secs_f64();
        let after = usage();
        println!(
            "CATALOG_BENCH {}",
            serde_json::json!({
                "threads":threads, "blocks":index.len(), "replay_seconds":replay_seconds,
                "index_seconds":seconds-replay_seconds, "seconds":seconds,
                "cpu_cores":(after.0-before.0)/seconds, "peak_rss_kib":after.1,
            })
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let blocks = std::env::var("TALON_CATALOG_BENCH_BLOCKS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(100_000);
    let mut state = State::default();
    state
        .apply(
            0,
            Record::Store {
                path: "paged".into(),
                page_size: 131072,
            },
        )
        .unwrap();
    for n in 0..blocks {
        let id = BlockId::new(
            ObjectId::new(
                Backend::S3,
                "bucket",
                format!("objects/2026/part-{n:016}/data.lance"),
            ),
            0,
            4 * 1024 * 1024,
            Version::new("source-version-1"),
        );
        state
            .restore(model::SnapshotRecord::Block {
                continuation: false,
                store: "paged".into(),
                block: model::Block {
                    id: BlockKey::new(&id),
                    len: 4 * 1024 * 1024,
                    pages: (0..32).map(|p| (p, 131072)).collect(),
                },
            })
            .unwrap();
    }
    let lock = Arc::new(CacheRootLock::acquire(root.path()).unwrap());
    let service = JournalService::open(
        root.path().to_owned(),
        lock,
        Options {
            require_native: true,
            ..Options::default()
        },
    )
    .unwrap();
    let journal = service
        .open_stream(PathBuf::from(DIRECTORY), state, StreamOptions::default())
        .unwrap();
    futures::executor::block_on(tokio::task::unconstrained(journal.checkpoint())).unwrap();
    drop(journal);
    drop(service);
    JournalService::wait_for_retiring(root.path()).unwrap();
    for _ in 0..3 {
        for threads in [1, 4, 8, 16] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "catalog::benchmark::restart_replay_and_index",
                    "--ignored",
                    "--nocapture",
                ])
                .env("TALON_CATALOG_BENCH_ROOT", root.path())
                .env("TALON_CATALOG_BENCH_THREADS", threads.to_string())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            for line in String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|l| l.starts_with("CATALOG_BENCH "))
            {
                println!("{line}");
            }
        }
    }
}

fn usage() -> (f64, i64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied struct on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    (
        usage.ru_utime.tv_sec as f64
            + usage.ru_stime.tv_sec as f64
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6,
        usage.ru_maxrss,
    )
}
