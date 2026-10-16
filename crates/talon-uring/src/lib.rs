//! Native cache I/O and task ownership on the caller's existing Monoio ring.
use std::{io, path::Path};

mod aligned;
pub use aligned::{AlignedBuf, Download, Payload};
mod direct;
pub use direct::DirectIoAlignment;
mod read;
pub use read::{open_read, read_file};

mod local;
pub use local::{on_ring, require_ring, sleep, spawn_cancellable, spawn_local, timeout, LocalRing};

/// Publish an immutable cache file. Must run on a native io_uring runtime.
/// Both payload and metadata use native open/write/fsync/rename operations.
pub async fn atomic_write(path: &Path, bytes: bytes::Bytes) -> io::Result<()> {
    atomic_write_inner(path, bytes, false, false)
        .await
        .map(|_| ())
}

/// Write the original download allocation, including its initialized tail.
/// Logical length is unchanged; files keep padding without a truncate. External
/// admissions without a safe aligned view use buffered I/O without copying.
/// Returns the published data file's allocated bytes (`st_blocks * 512`).
pub async fn atomic_write_payload(
    path: &Path,
    payload: Payload,
    alignment: DirectIoAlignment,
) -> io::Result<u64> {
    let allocated = match payload.direct_bytes(alignment) {
        Some(bytes) => atomic_write_inner(path, bytes, true, true).await,
        None => atomic_write_inner(path, payload.into_bytes(), false, true).await,
    }?;
    Ok(allocated.expect("payload writes always measure allocation"))
}

async fn atomic_write_inner(
    path: &Path,
    bytes: bytes::Bytes,
    direct: bool,
    measure_allocation: bool,
) -> io::Result<Option<u64>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("cache file has no parent"))?;
    create_dirs(parent).await?;
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::other("cache file has no name"))?;
    let (staged, file) = loop {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut name = name.to_os_string();
        name.push(format!(".tmp.{}.{}", std::process::id(), sequence));
        let staged = path.with_file_name(name);
        match monoio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(if direct { libc::O_DIRECT } else { 0 })
            .open(&staged)
            .await
        {
            Ok(file) => break (staged, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    // Cleanup is allowed only after create_new proves that we own this file.
    let result = async {
        file.write_all_at(bytes, 0).await.0?;
        file.sync_all().await?;
        let allocated = if measure_allocation {
            // st_blocks uses fixed 512-byte units, independent of Direct I/O alignment.
            Some(file.metadata().await?.blocks() * 512)
        } else {
            None
        };
        file.close().await?;
        monoio::fs::rename(&staged, path).await?;
        Ok(allocated)
    }
    .await;
    if result.is_err() {
        let _ = monoio::fs::remove_file(&staged).await;
    }
    result
}

// Monoio 0.2.4's recursive helper uses synchronous metadata and does not tolerate
// concurrent creation of intermediate directories. Concurrent page fills need
// both mkdir and the EEXIST directory check to stay on the ring.
async fn create_dirs(path: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match ensure_dir(current).await {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current);
                current = current.parent().ok_or(error)?;
                if current.as_os_str().is_empty() {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    for path in missing.into_iter().rev() {
        ensure_dir(path).await?;
    }
    Ok(())
}

async fn ensure_dir(path: &Path) -> io::Result<()> {
    match monoio::fs::create_dir(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let file = monoio::fs::File::open(path).await?;
            let directory = file.metadata().await?.is_dir();
            file.close().await?;
            if directory {
                Ok(())
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}
