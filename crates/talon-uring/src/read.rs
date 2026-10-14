//! Buffered cache reads submitted to the caller's native ring.
use std::{
    io,
    os::fd::{AsFd, OwnedFd},
    path::Path,
};

/// Open and stat a buffered descriptor for file-to-socket serving. The duplicate
/// is an ordinary owned fd, so it can live in the process-wide immutable-fd cache.
pub async fn open_read(path: &Path) -> io::Result<(OwnedFd, u64)> {
    let file = monoio::fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    // fcntl(F_DUPFD_CLOEXEC) only duplicates the descriptor; path lookup and
    // filesystem metadata I/O have already completed asynchronously.
    use std::os::fd::{AsRawFd, BorrowedFd};
    // SAFETY: file remains alive through duplication.
    let fd = unsafe { BorrowedFd::borrow_raw(file.as_raw_fd()) }
        .as_fd()
        .try_clone_to_owned()?;
    file.close().await?;
    Ok((fd, len))
}

/// Read an exact byte window, or the whole file, without a blocking-pool hop.
pub async fn read_file(path: &Path, range: Option<(u64, u64)>) -> io::Result<bytes::Bytes> {
    let file = monoio::fs::File::open(path).await?;
    let file_len = file.metadata().await?.len();
    let (offset, len) = range.unwrap_or((0, file_len));
    if offset.checked_add(len).map_or(true, |end| end > file_len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache read out of bounds",
        ));
    }
    let size = usize::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cache read exceeds usize"))?;
    let (result, bytes) = file.read_exact_at(Vec::with_capacity(size), offset).await;
    result?;
    file.close().await?;
    Ok(bytes.into())
}
