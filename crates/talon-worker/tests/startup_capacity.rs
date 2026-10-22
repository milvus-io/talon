//! An invalid disk capacity must fail before identity initialization or serving.
use std::time::Duration;

#[tokio::test]
async fn oversized_cache_capacity_exits_before_initializing_identity() {
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache");
    // Cargo puts integration tests in <profile>/deps and builds the worker in
    // <profile>, as also used by the coordinator process tests.
    let binary = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("talon-worker");
    let mut command = tokio::process::Command::new(binary);
    command
        .env_clear()
        .env("TMPDIR", temp.path())
        .env("TALON_WORKER_CACHE_DIRS", &cache)
        .env("TALON_WORKER_CAPACITY_BYTES", u64::MAX.to_string())
        .env("TALON_WORKER_NODE_ID", "capacity-test-worker")
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(10), command.output())
        .await
        .expect("invalid capacity must not enter the serving loop")
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("exceeds filesystem capacity"), "{stderr}");
    assert!(stderr.contains(&u64::MAX.to_string()), "{stderr}");
    assert!(stderr.contains(&cache.display().to_string()), "{stderr}");
    assert!(cache.is_dir());
    assert!(std::fs::read_dir(&cache).unwrap().next().is_none());
}
