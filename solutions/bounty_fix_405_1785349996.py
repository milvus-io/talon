### Technical Overview

#### Root Cause Analysis

The intermittent failure in the `fuse mount e2e` job where all mount tests panic simultaneously with `fusermount3: mount failed: Operation not permitted` is caused by two main issues in Linux containerized CI environments (such as GitHub Actions):

1. **Parallel Execution & Mount Race Conditions**: By default, `cargo test` (or equivalent test runners) executes test cases concurrently in separate threads. When multiple tests attempt to invoke `fusermount3` and create/tear down mount points simultaneously within unprivileged or namespace-restricted containers, kernel lock contention or namespace restrictions on `/dev/fuse` cause `fusermount3` to fail for all active threads at once with `EPERM` (`Operation not permitted`).
2. **CI Permission Restrictions & Stale Mount Cleanup**: Unprivileged container runners often lack appropriate setuid flags on `fusermount3`, lack `user_allow_other` in `/etc/fuse.conf`, or have restrictive `/dev/fuse` permissions. Furthermore, asynchronous unmounting can leave mount points busy or in a lingering state between test runs.

#### Key Fixes Applied

1. **Serial Execution for FUSE E2E Tests**: Configured cargo test runner invocation for FUSE e2e tests to use `-- --test-threads=1` so mounts and unmounts execute deterministically without concurrent race conditions.
2. **CI Environment Setup**: Ensured `/dev/fuse` permissions (`chmod 666 /dev/fuse`), SUID bit on `fusermount3` (`chmod u+s /usr/bin/fusermount3`), and enabled `user_allow_other` in `/etc/fuse.conf`.
3. **Mount Retry Logic with Exponential Backoff**: Implemented automated mount retries in the helper module to handle transient kernel device busy/permission states during quick sequence mount/unmount cycles.

---

### Solution Implementation

#### 1. CI Workflow Configuration (`.github/workflows/fuse-e2e.yml`)

```yaml
name: FUSE Mount E2E Tests

on:
  push:
    branches: [ main, master ]
  pull_request:

jobs:
  fuse-e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install FUSE 3 Dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y fuse3 libfuse3-dev
          
          # Fix /dev/fuse permissions
          sudo chmod 666 /dev/fuse
          
          # Enable user_allow_other in fuse.conf
          echo "user_allow_other" | sudo tee -a /etc/fuse.conf
          
          # Ensure fusermount3 setuid bit is set for unprivileged execution
          sudo chmod u+s /usr/bin/fusermount3

      - name: Setup Rust Toolchain
        uses: dtolnay/rust-toolchain@stable

      - name: Run FUSE E2E Tests Serially
        env:
          TALON_REQUIRE_FUSE: "1"
          RUST_BACKTRACE: "1"
        run: |
          # Note: FUSE mount tests must run serially (--test-threads=1) 
          # to prevent concurrent fusermount3 EPERM race conditions.
          cargo test --test fuse_e2e -- --test-threads=1
```

---

#### 2. Rust FUSE Mount Helper with Retry Logic (`src/fuse_mount.rs`)

```rust
use std::fs;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

pub struct FuseMountGuard<'a> {
    pub mount_point: &'a Path,
}

impl<'a> Drop for FuseMountGuard<'a> {
    fn drop(&mut self) {
        let _ = unmount_fuse(self.mount_point);
    }
}

/// Unmounts a FUSE filesystem, trying lazy unmount if standard unmount fails.
pub fn unmount_fuse(mount_point: &Path) -> Result<(), String> {
    let mount_str = mount_point.to_string_lossy();
    
    // Attempt standard fusermount3 unmount
    let status = Command::new("fusermount3")
        .args(["-u", &mount_str])
        .status();

    if let Ok(st) if st.success() => return Ok(()),
    _ => {}

    // Fallback: Lazy unmount (-z) if device is busy or temporarily locked
    let lazy_status = Command::new("fusermount3")
        .args(["-u", "-z", &mount_str])
        .status()
        .map_err(|e| format!("Failed to execute fusermount3 lazy unmount: {}", e))?;

    if lazy_status.success() {
        Ok(())
    } else {
        Err(format!("fusermount3 -u -z failed for {}", mount_str))
    }
}

/// Mounts a FUSE filesystem with backoff retry to mitigate transient EPERM failures in CI.
pub fn mount_fuse_with_retry<F>(
    mount_point: &Path,
    mount_fn: F,
    max_retries: u32,
) -> Result<FuseMountGuard, String>
where
    F: Fn(&Path) -> Result<(), String>,
{
    fs::create_dir_all(mount_point)
        .map_err(|e| format!("Failed to create mount directory: {}", e))?;

    let mut delay = Duration::from_millis(100);

    for attempt in 1..=max_retries {
        // Ensure path is unmounted prior to mounting
        let _ = unmount_fuse(mount_point);

        match mount_fn(mount_point) {
            Ok(_) => {
                return Ok(FuseMountGuard { mount_point });
            }
            Err(err) => {
                if attempt == max_retries {
                    return Err(format!(
                        "FUSE mount failed after {} attempts. Last error: {}",
                        max_retries, err
                    ));
                }
                
                // Exponential backoff with delay
                thread::sleep(delay);
                delay *= 2;
            }
        }
    }

    Err("FUSE mount failed unexpectedly".to_string())
}
```

---

#### 3. Python Test Runner / Harness Solution (`scripts/run_fuse_e2e.py`)

If tests are driven via a Python harness:

```python
#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from pathlib import Path

def setup_fuse_permissions():
    """Ensure standard CI FUSE permissions are correctly configured."""
    try:
        # Enable permissions on /dev/fuse if running with root privileges
        if os.geteuid() == 0:
            os.chmod("/dev/fuse", 0o666)
            fusermount_path = "/usr/bin/fusermount3"
            if os.path.exists(fusermount_path):
                os.chmod(fusermount_path, 0o4755)
            
            fuse_conf = Path("/etc/fuse.conf")
            if fuse_conf.exists():
                content = fuse_conf.read_text()
                if "user_allow_other" not in content:
                    with fuse_conf.open("a") as f:
                        f.write("\nuser_allow_other\n")
    except Exception as e:
        print(f"[WARN] Failed to adjust FUSE permissions: {e}", file=sys.stderr)

def run_fuse_tests():
    setup_fuse_permissions()
    
    env = os.environ.copy()
    env["TALON_REQUIRE_FUSE"] = "1"
    
    # Run tests sequentially (--test-threads=1) to prevent fusermount3 lock contention
    cmd = ["cargo", "test", "--test", "fuse_e2e", "--", "--test-threads=1"]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, env=env)
    return result.returncode

if __name__ == "__main__":
    sys.exit(run_fuse_tests())
```