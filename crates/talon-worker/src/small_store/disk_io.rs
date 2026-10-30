//! Buffered I/O on the current native ring; Tokio callers use its blocking pool.
//! Never run filesystem syscalls on a reactor or require O_DIRECT alignment.
use bytes::Bytes;
use std::{
    io,
    os::fd::{AsRawFd, BorrowedFd, OwnedFd},
    path::PathBuf,
    sync::Arc,
};

#[cfg(test)]
thread_local! {
    pub(super) static NATIVE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static BLOCKING_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Copy)]
pub(super) enum Open {
    Read,
    Create,
    Replace,
}

pub(super) async fn create_dir(path: PathBuf) -> io::Result<()> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            monoio::fs::create_dir(path).await
        }))
        .await;
    }
    blocking(move || std::fs::create_dir(path)).await
}

pub(super) async fn open(path: PathBuf, mode: Open) -> io::Result<Arc<OwnedFd>> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            let mut options = monoio::fs::OpenOptions::new();
            options.read(true);
            match mode {
                Open::Read => {}
                Open::Create => {
                    options.write(true).create_new(true);
                }
                Open::Replace => {
                    options.write(true).create(true).truncate(true);
                }
            }
            let file = options.open(path).await?;
            // SAFETY: file owns this descriptor throughout duplication.
            let fd = unsafe { BorrowedFd::borrow_raw(file.as_raw_fd()) }.try_clone_to_owned()?;
            file.close().await?;
            Ok(Arc::new(fd))
        }))
        .await;
    }
    blocking(move || {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        match mode {
            Open::Read => {}
            Open::Create => {
                options.write(true).create_new(true);
            }
            Open::Replace => {
                options.write(true).create(true).truncate(true);
            }
        }
        Ok(Arc::new(options.open(path)?.into()))
    })
    .await
}

pub(super) async fn write(fd: Arc<OwnedFd>, bytes: Bytes, offset: u64) -> io::Result<()> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            let file = native(&fd)?;
            file.write_all_at(bytes, offset).await.0?;
            file.close().await
        }))
        .await;
    }
    blocking(move || {
        use std::os::unix::fs::FileExt;
        std::fs::File::from(fd.try_clone()?).write_all_at(&bytes, offset)
    })
    .await
}

pub(super) async fn read(fd: Arc<OwnedFd>, offset: u64, len: usize) -> io::Result<Bytes> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            let file = native(&fd)?;
            let (result, bytes) = file.read_exact_at(Vec::with_capacity(len), offset).await;
            result?;
            file.close().await?;
            Ok(bytes.into())
        }))
        .await;
    }
    blocking(move || {
        use std::os::unix::fs::FileExt;
        let mut bytes = vec![0; len];
        std::fs::File::from(fd.try_clone()?).read_exact_at(&mut bytes, offset)?;
        Ok(bytes.into())
    })
    .await
}

pub(super) async fn sync(fd: Arc<OwnedFd>) -> io::Result<()> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            let file = native(&fd)?;
            file.sync_all().await?;
            file.close().await
        }))
        .await;
    }
    blocking(move || std::fs::File::from(fd.try_clone()?).sync_all()).await
}

pub(super) async fn allocated(fd: Arc<OwnedFd>) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            let file = native(&fd)?;
            let bytes = file.metadata().await?.blocks() * 512;
            file.close().await?;
            Ok(bytes)
        }))
        .await;
    }
    blocking(move || Ok(std::fs::File::from(fd.try_clone()?).metadata()?.blocks() * 512)).await
}

pub(super) async fn rename(from: PathBuf, to: PathBuf) -> io::Result<()> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            monoio::fs::rename(from, to).await
        }))
        .await;
    }
    blocking(move || std::fs::rename(from, to)).await
}

pub(super) async fn remove(path: PathBuf) -> io::Result<()> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            monoio::fs::remove_file(path).await
        }))
        .await;
    }
    blocking(move || std::fs::remove_file(path)).await
}

pub(super) async fn sync_dir(path: PathBuf) -> io::Result<()> {
    sync(open(path, Open::Read).await?).await
}

fn native(fd: &OwnedFd) -> io::Result<monoio::fs::File> {
    monoio::fs::File::from_std(std::fs::File::from(fd.try_clone()?))
}
async fn receive<T>(receiver: tokio::sync::oneshot::Receiver<io::Result<T>>) -> io::Result<T> {
    #[cfg(test)]
    NATIVE_CALLS.with(|c| c.set(c.get() + 1));
    receiver
        .await
        .map_err(|e| io::Error::other(format!("small store ring task stopped: {e}")))?
}
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    #[cfg(test)]
    BLOCKING_CALLS.with(|c| c.set(c.get() + 1));
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| io::Error::other(format!("small store I/O task stopped: {e}")))?
}
