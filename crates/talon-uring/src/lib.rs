//! Run I/O on the caller's Monoio ring, with a dedicated ring for portable callers.
use futures::{future::LocalBoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{future::Future, io, path::Path, sync::Arc};

mod local;
pub use local::{on_ring, sleep, spawn_local, timeout, LocalRing};

type Job = Box<dyn FnOnce() -> LocalBoxFuture<'static, ()> + Send>;

/// Clones share one ring and a bound covering queued and executing operations.
/// Accepted disk work survives caller cancellation. Dropping all handles closes
/// admission; the thread drains accepted work before dropping the ring.
#[derive(Clone)]
pub struct Executor {
    sender: flume::Sender<Job>,
    slots: Arc<tokio::sync::Semaphore>,
}

impl Executor {
    /// Strictly io_uring: no legacy/epoll fallback when the experiment is enabled.
    pub fn new(name: &str, capacity: usize) -> io::Result<Self> {
        if capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid io_uring capacity",
            ));
        }
        let (sender, receiver) = flume::bounded::<Job>(capacity);
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .with_entries(1024)
                    .enable_all()
                    .build();
                let mut runtime = match runtime {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                let _ = ready.send(Ok(()));
                runtime.block_on(async move {
                    let mut pending = FuturesUnordered::new();
                    loop {
                        if pending.is_empty() {
                            match receiver.recv_async().await {
                                Ok(job) => pending.push(job()),
                                Err(_) => break,
                            }
                        } else {
                            futures::select! {
                                job = receiver.recv_async().fuse() => match job {
                                    Ok(job) => pending.push(job()),
                                    Err(_) => break,
                                },
                                _ = pending.next() => {},
                            }
                        }
                    }
                    while pending.next().await.is_some() {}
                });
            })?;
        started
            .recv()
            .map_err(|_| io::Error::other("io_uring thread exited during startup"))??;
        Ok(Self {
            sender,
            slots: Arc::new(tokio::sync::Semaphore::new(capacity)),
        })
    }

    /// Construct the non-Send future on the ring. Keep accepted work alive even
    /// when the caller goes away (needed for write/fsync/rename transactions).
    pub async fn run<F, Fut, T>(&self, work: F) -> io::Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.submit(work, false).await
    }

    /// Abort network work when its requester (including a retry timeout) drops.
    pub async fn run_cancellable<F, Fut, T>(&self, work: F) -> io::Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.submit(work, true).await
    }

    async fn submit<F, Fut, T>(&self, work: F, cancellable: bool) -> io::Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io::Error::other("io_uring admission closed"))?;
        let (mut result, wait) = tokio::sync::oneshot::channel();
        let job: Job = Box::new(move || {
            Box::pin(async move {
                let _permit = permit;
                if cancellable {
                    if result.is_closed() {
                        return;
                    }
                    let work = work().fuse();
                    futures::pin_mut!(work);
                    let value = futures::select! {
                        value = work => value,
                        _ = result.closed().fuse() => return,
                    };
                    let _ = result.send(value);
                } else {
                    let _ = result.send(work().await);
                }
            })
        });
        if on_ring() {
            // The request already owns a native ring. Keep network/file I/O on
            // that thread instead of bouncing through the fallback executor.
            spawn_local(job());
        } else {
            self.sender
                .send_async(job)
                .await
                .map_err(|_| io::Error::other("io_uring executor stopped"))?;
        }
        wait.await
            .map_err(|_| io::Error::other("io_uring task stopped"))
    }
}

/// Publish an immutable cache file. Must run on a native io_uring runtime.
/// Both payload and metadata use native open/write/fsync/rename operations.
pub async fn atomic_write(path: &Path, bytes: bytes::Bytes) -> io::Result<()> {
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
        file.close().await?;
        monoio::fs::rename(&staged, path).await
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
