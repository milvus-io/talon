//! Exercise the real startup entry point without requiring io_uring or a coordinator.
use std::path::Path;
use std::time::Duration;

use talon_worker::{identity::WorkerIdentity, page_access_store::CacheRootLock};

fn command(root: &Path) -> tokio::process::Command {
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
        .env("TMPDIR", root)
        .env("TALON_ZONE", "test")
        .env("TALON_WORKER_CACHE_DIRS", root)
        .env("TALON_WORKER_CLUSTER_ID", "new-cluster")
        .env("TALON_WORKER_NODE_ID", "new-worker")
        .env("TALON_WORKER_BLOCK_SIZE", "16")
        .env("TALON_WORKER_L2_PAGE_SIZE_BYTES", "4")
        .env("TALON_WORKER_CAPACITY_BYTES", "1048576")
        .env("TALON_WORKER_BACKEND", "s3")
        // Stop after local identity initialization, before networking/store rings.
        .env("TALON_WORKER_ORIGIN_IO", "invalid")
        .kill_on_drop(true);
    command
}

async fn failure(command: &mut tokio::process::Command) -> String {
    let output = tokio::time::timeout(Duration::from_secs(10), command.output())
        .await
        .expect("startup must fail before serving")
        .unwrap();
    assert!(!output.status.success());
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[tokio::test]
async fn startup_reset_defaults_on_and_can_be_disabled_explicitly() {
    for mode in ["default", "env", "cli", "env-false", "cli-false"] {
        let root = tempfile::tempdir().unwrap();
        let lock = CacheRootLock::acquire(root.path()).unwrap();
        lock.load_identity("old-cluster", Some("old-worker"), 64, 16)
            .unwrap();
        drop(lock);
        let identity_path = root.path().join("worker_identity");
        let original = std::fs::read(&identity_path).unwrap();
        std::fs::write(root.path().join("old-state"), b"old").unwrap();
        let mut command = command(root.path());
        match mode {
            "env" => {
                command.env("TALON_WORKER_RESET_INCOMPATIBLE_CACHE", "true");
            }
            "cli" => {
                command.arg("--reset-incompatible-cache");
            }
            "env-false" => {
                command.env("TALON_WORKER_RESET_INCOMPATIBLE_CACHE", "false");
            }
            "cli-false" => {
                command
                    .env("TALON_WORKER_RESET_INCOMPATIBLE_CACHE", "true")
                    .arg("--reset-incompatible-cache=false");
            }
            _ => {}
        }
        let output = failure(&mut command).await;
        let enabled = mode != "env-false" && mode != "cli-false";
        assert_eq!(root.path().join("old-state").exists(), !enabled, "{output}");
        if enabled {
            assert!(
                output.contains("TALON_WORKER_ORIGIN_IO must be tokio or uring"),
                "{output}"
            );
            assert!(
                output.contains("resetting incompatible local cache"),
                "{output}"
            );
            let identity: WorkerIdentity =
                serde_json::from_slice(&std::fs::read(identity_path).unwrap()).unwrap();
            assert_eq!(identity.cluster_id, "new-cluster");
            assert_eq!(identity.worker_id, "new-worker");
            assert_eq!((identity.block_size, identity.page_size), (16, 4));
        } else {
            assert!(output.contains("different cluster"), "{output}");
            assert_eq!(std::fs::read(identity_path).unwrap(), original);
        }
    }
}

#[tokio::test]
async fn lock_capacity_and_filesystem_errors_do_not_reset() {
    for case in ["lock", "capacity", "io"] {
        let root = tempfile::tempdir().unwrap();
        let lock = CacheRootLock::acquire(root.path()).unwrap();
        std::fs::write(root.path().join("old-state"), b"old").unwrap();
        // A directory cannot be read as identity; this is an OS I/O error.
        std::fs::create_dir(root.path().join("worker_identity")).unwrap();
        let held = (case == "lock").then_some(lock);
        let mut command = command(root.path());
        command.arg("--reset-incompatible-cache");
        if case == "capacity" {
            command.env("TALON_WORKER_CAPACITY_BYTES", u64::MAX.to_string());
        }
        let output = failure(&mut command).await;
        let expected = match case {
            "lock" => "already in use or cannot be locked",
            "capacity" => "exceeds filesystem capacity",
            _ => "Is a directory",
        };
        assert!(output.contains(expected), "{case}: {output}");
        assert!(
            !output.contains("resetting incompatible local cache"),
            "{case}: {output}"
        );
        assert!(root.path().join("old-state").exists(), "{case}");
        assert!(root.path().join("worker_identity").is_dir(), "{case}");
        drop(held);
    }
}
