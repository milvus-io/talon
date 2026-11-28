//! Buffered I/O on the current native ring; Tokio callers use its blocking pool.
//! Never run filesystem syscalls on a reactor or require O_DIRECT alignment.
use bytes::{Buf, Bytes};
use std::{
    future::Future,
    io,
    os::fd::{AsRawFd, BorrowedFd, OwnedFd},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[cfg(test)]
thread_local! {
    pub(super) static NATIVE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static BLOCKING_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Copy)]
pub(super) enum Open {
    Read,
    Update,
    Create,
    Replace,
}

pub(super) async fn create_dir(path: PathBuf) -> io::Result<()> {
    let result = if talon_uring::on_ring() {
        receive(talon_uring::spawn_local(async move {
            monoio::fs::create_dir(path).await
        }))
        .await
    } else {
        blocking(move || std::fs::create_dir(path)).await
    };
    match result {
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        result => result,
    }
}

pub(super) async fn open(path: PathBuf, mode: Open) -> io::Result<Arc<OwnedFd>> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            let mut options = monoio::fs::OpenOptions::new();
            options.read(true);
            match mode {
                Open::Read => {}
                Open::Update => {
                    options.write(true);
                }
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
            Open::Update => {
                options.write(true);
            }
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

/// An active segment's descriptor and shared allocation observations.
pub(super) struct SegmentWriter {
    pub(super) fd: Arc<OwnedFd>,
    allocation: AllocationSampler,
}

impl SegmentWriter {
    pub(super) fn new(fd: Arc<OwnedFd>) -> Self {
        Self {
            fd,
            allocation: Default::default(),
        }
    }

    /// Write a record without copying the payload. Sample even a failed write,
    /// since its completed prefix may have allocated disk blocks.
    pub(super) async fn append(
        self: Arc<Self>,
        payload: Bytes,
        checksum: u64,
        offset: u64,
    ) -> io::Result<(io::Result<()>, io::Result<u64>)> {
        let trailer = checksum.to_le_bytes();
        #[cfg(target_os = "linux")]
        if talon_uring::on_ring() {
            return receive(talon_uring::spawn_local(async move {
                let file = native(&self.fd)?;
                let payload = async {
                    let mut offset = offset;
                    let mut parts = [payload, Bytes::copy_from_slice(&trailer)];
                    while !parts[1].is_empty() {
                        let (written, returned) = file.write_vectored_at(parts, offset).await?;
                        parts = returned;
                        advance_record(&mut parts, &mut offset, written)?;
                    }
                    Ok(())
                }
                .await;
                #[cfg(test)]
                if payload.is_ok() {
                    super::tests::crash("append_payload");
                }
                let allocated = self
                    .allocation
                    .observe(async { Ok(file.metadata().await?.blocks() * 512) })
                    .await;
                Ok((payload.and(file.close().await), allocated))
            }))
            .await;
        }
        blocking(move || {
            let payload = write_record_blocking(&self.fd, &payload, &trailer, offset);
            #[cfg(test)]
            if payload.is_ok() {
                super::tests::crash("append_payload");
            }
            let allocated = self
                .allocation
                .observe_blocking(|| allocated_blocking(&self.fd));
            Ok((payload, allocated))
        })
        .await
    }
}

fn write_record_blocking(
    fd: &OwnedFd,
    payload: &[u8],
    trailer: &[u8],
    mut offset: u64,
) -> io::Result<()> {
    let mut parts = [payload, trailer];
    while !parts[1].is_empty() {
        let position = libc::off_t::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file offset out of range"))?;
        let vectors = parts.map(|bytes| libc::iovec {
            iov_base: bytes.as_ptr() as *mut _,
            iov_len: bytes.len(),
        });
        // SAFETY: pwritev only reads these slices and returns before they change.
        let n = unsafe {
            libc::pwritev(
                fd.as_raw_fd(),
                vectors.as_ptr(),
                vectors.len() as _,
                position,
            )
        };
        if n >= 0 {
            advance_record(&mut parts, &mut offset, n as usize)?;
        } else {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
    Ok(())
}

fn advance_record(parts: &mut [impl Buf; 2], offset: &mut u64, written: usize) -> io::Result<()> {
    if written == 0 {
        return Err(io::ErrorKind::WriteZero.into());
    }
    let payload_bytes = written.min(parts[0].remaining());
    parts[0].advance(payload_bytes);
    parts[1].advance(written - payload_bytes);
    *offset += written as u64;
    Ok(())
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

pub(super) async fn truncate(fd: Arc<OwnedFd>, len: u64) -> io::Result<()> {
    blocking(move || std::fs::File::from(fd.try_clone()?).set_len(len)).await
}

pub(super) async fn allocated(fd: Arc<OwnedFd>) -> io::Result<u64> {
    if talon_uring::on_ring() {
        return receive(talon_uring::spawn_local(async move {
            let file = native(&fd)?;
            let bytes = file.metadata().await?.blocks() * 512;
            file.close().await?;
            Ok(bytes)
        }))
        .await;
    }
    blocking(move || allocated_blocking(&fd)).await
}

fn allocated_blocking(fd: &OwnedFd) -> io::Result<u64> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fd remains live and fstat initializes stat on success.
    if unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fstat returned success.
    Ok(unsafe { stat.assume_init() }.st_blocks as u64 * 512)
}

#[derive(Default)]
struct AllocationSampler {
    completed: AtomicU64,
    // (last sampled completion, physical bytes)
    sampled: tokio::sync::Mutex<(u64, u64)>,
}

impl AllocationSampler {
    /// Register after payload I/O finishes. One stat covers all completions
    /// before its cut. Take the count before submitting the stat.
    async fn observe(&self, stat: impl Future<Output = io::Result<u64>>) -> io::Result<u64> {
        let completion = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        let mut sampled = self.sampled.lock().await;
        if completion > sampled.0 {
            let cut = self.completed.load(Ordering::Acquire);
            *sampled = (cut, stat.await?);
        }
        Ok(sampled.1)
    }

    fn observe_blocking(&self, stat: impl FnOnce() -> io::Result<u64>) -> io::Result<u64> {
        let completion = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        let mut sampled = self.sampled.blocking_lock();
        if completion > sampled.0 {
            let cut = self.completed.load(Ordering::Acquire);
            *sampled = (cut, stat()?);
        }
        Ok(sampled.1)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{pin::pin, task::Poll};

    #[tokio::test]
    async fn allocation_sample_covers_only_completions_before_its_cut() {
        let sampler = AllocationSampler::default();
        let ready = tokio::sync::Notify::new();
        let mut first = pin!(sampler.observe(async {
            ready.notified().await;
            Ok(4096)
        }));
        assert!(futures::poll!(first.as_mut()).is_pending());

        let mut second = pin!(sampler.observe(async { Ok(8192) }));
        let mut third = pin!(sampler.observe(async { panic!("covered by second sample") }));
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(futures::poll!(third.as_mut()).is_pending());
        ready.notify_one();

        assert!(matches!(futures::poll!(first), Poll::Ready(Ok(4096))));
        assert_eq!(second.await.unwrap(), 8192);
        assert_eq!(third.await.unwrap(), 8192);
    }
}
