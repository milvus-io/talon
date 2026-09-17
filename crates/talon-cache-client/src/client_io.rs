//! Request-level handoff for callers outside the native Monoio runtime.
//! There are no per-read/write commands or stream trait adapters here.
use crate::lock::MutexExt;
#[cfg(target_os = "linux")]
use crate::rpc::{Error, Reply, Request};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[cfg(target_os = "linux")]
use std::{io, time::Duration};

/// TCP execution backend used by Rust-based SDK clients.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ClientIoBackend {
    /// Complete requests run on Monoio on Linux; initialization failure uses Tokio.
    #[default]
    Auto,
    /// Execute complete requests using Tokio's sockets and timers.
    Tokio,
    /// Require Monoio's io_uring runtime; initialization failure is returned.
    IoUring,
}
#[derive(Default)]
pub(crate) struct Counts(Mutex<HashMap<String, Arc<AtomicUsize>>>);
impl Counts {
    #[cfg(target_os = "linux")]
    pub(crate) fn for_addr(&self, addr: &str) -> Arc<AtomicUsize> {
        self.0
            .lock_recover()
            .entry(addr.to_owned())
            .or_default()
            .clone()
    }
    pub(crate) fn get(&self, addr: &str) -> usize {
        self.0
            .lock_recover()
            .get(addr)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }
}
#[cfg(target_os = "linux")]
#[derive(Clone)]
pub(crate) struct Config {
    pub max_idle: usize,
    pub ttl: Duration,
    pub connect: Duration,
    pub request: Duration,
    pub counts: Arc<Counts>,
}
#[cfg(target_os = "linux")]
impl Default for Config {
    fn default() -> Self {
        Self {
            max_idle: crate::pool::DEFAULT_MAX_IDLE_PER_ADDR,
            ttl: crate::pool::DEFAULT_IDLE_TTL,
            connect: crate::pool::DEFAULT_CONNECT_TIMEOUT,
            request: crate::pool::DEFAULT_REQUEST_TIMEOUT,
            counts: Arc::default(),
        }
    }
}
#[cfg(target_os = "linux")]
pub(crate) struct Outcome {
    pub result: Result<Reply, Error>,
    pub reused: bool,
}
#[cfg(target_os = "linux")]
impl Outcome {
    pub(crate) fn error(error: Error) -> Self {
        Self {
            result: Err(error),
            reused: false,
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use bridge::Endpoint;

#[cfg(target_os = "linux")]
mod bridge {
    use super::*;
    use crate::monoio_client::{LocalClient, MonoioClient};
    use std::{
        rc::Rc,
        sync::{atomic::AtomicU64, OnceLock, Weak},
    };
    use tokio::sync::{mpsc, oneshot};
    static DRIVER: Mutex<Weak<Driver>> = Mutex::new(Weak::new());
    static UNAVAILABLE: OnceLock<(io::ErrorKind, String)> = OnceLock::new();
    static NEXT_POOL: AtomicU64 = AtomicU64::new(1);
    struct Job {
        pool: Arc<Endpoint>,
        addr: String,
        request: Request,
        result: oneshot::Sender<Outcome>,
    }
    struct Driver {
        jobs: mpsc::Sender<Job>,
        remove: mpsc::UnboundedSender<u64>,
    }
    pub(crate) struct Endpoint {
        driver: Arc<Driver>,
        id: u64,
        config: Config,
    }
    impl Drop for Endpoint {
        fn drop(&mut self) {
            let _ = self.driver.remove.send(self.id);
        }
    }
    impl Endpoint {
        pub(crate) fn new(config: Config) -> io::Result<Arc<Self>> {
            Ok(Arc::new(Self {
                driver: driver()?,
                id: NEXT_POOL.fetch_add(1, Ordering::Relaxed),
                config,
            }))
        }
        pub(crate) async fn call(self: &Arc<Self>, addr: &str, request: Request) -> Outcome {
            let (result, receiver) = oneshot::channel();
            let job = Job {
                pool: self.clone(),
                addr: addr.to_owned(),
                request,
                result,
            };
            match tokio::time::timeout(self.config.connect, self.driver.jobs.send(job)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Outcome::error(closed().into()),
                Err(_) => {
                    return Outcome::error(
                        crate::pool::timeout_error("native RPC admission", self.config.connect)
                            .into(),
                    )
                }
            }
            receiver
                .await
                .unwrap_or_else(|_| Outcome::error(closed().into()))
        }
    }
    fn closed() -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, "native SDK executor closed")
    }
    fn driver() -> io::Result<Arc<Driver>> {
        if let Some((kind, text)) = UNAVAILABLE.get() {
            return Err(io::Error::new(*kind, text.clone()));
        }
        let mut shared = DRIVER.lock_recover();
        if let Some(driver) = shared.upgrade() {
            return Ok(driver);
        }
        let (jobs, mut rx) = mpsc::channel::<Job>(64);
        let (remove, mut removed) = mpsc::unbounded_channel();
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("talon-client-ring".into())
            .spawn(move || {
                let mut runtime = match monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .with_entries(1024)
                    .enable_timer()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(())).is_err() {
                    return;
                }
                runtime.block_on(async move {
                    use futures::stream::{FuturesUnordered, StreamExt};
                    let mut pools: HashMap<u64, LocalClient> = HashMap::new();
                    let mut active = FuturesUnordered::new();
                    let mut ended = false;
                    let mut remove_open = true;
                    loop {
                        tokio::select! {
                            id = removed.recv(), if !ended && remove_open => match id {
                                Some(id) => { pools.remove(&id); },
                                None => remove_open = false,
                            },
                            job = rx.recv(), if !ended && active.len() < 1024 => match job {
                                Some(mut job) => {
                                    if job.result.is_closed() {
                                        continue;
                                    }
                                    let local = pools.entry(job.pool.id)
                                        .or_insert_with(|| Rc::new(MonoioClient::from_config(job.pool.config.clone())))
                                        .clone();
                                    active.push(async move {
                                        tokio::select! {
                                            biased;
                                            _ = job.result.closed() => {},
                                            outcome = local.execute(&job.addr, job.request) => {
                                                let _ = job.result.send(outcome);
                                            },
                                        }
                                        // The pool handle stays alive through cancellation cleanup.
                                        drop(job.pool);
                                    });
                                },
                                None => ended = true,
                            },
                            _ = active.next(), if !active.is_empty() => {},
                            else => break,
                        }
                    }
                    drop(pools);
                });
            })?;
        if let Err(error) = started.recv().map_err(|_| closed())? {
            let _ = UNAVAILABLE.set((error.kind(), error.to_string()));
            return Err(error);
        }
        let driver = Arc::new(Driver { jobs, remove });
        *shared = Arc::downgrade(&driver);
        Ok(driver)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[tokio::test]
    #[ignore = "run through scripts/test_client_no_uring.py with process-local seccomp"]
    async fn auto_falls_back_when_ring_setup_is_denied() {
        use crate::{ClientIoBackend, ConnectionPool, WorkerClient};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        assert!(monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .build()
            .is_err());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.unwrap();
            let mut h = [0; talon_transport::HEADER_LEN];
            peer.read_exact(&mut h).await.unwrap();
            let h = talon_transport::FrameHeader::decode(&h).unwrap();
            let mut payload = vec![0; h.length as usize];
            peer.read_exact(&mut payload).await.unwrap();
            peer.write_all(&talon_transport::response_header_ok(h.request_id, 1))
                .await
                .unwrap();
            peer.write_all(b"x").await.unwrap();
        });
        let object = talon_core::ObjectId::new(talon_core::Backend::S3, "bucket", "key");
        let auto = WorkerClient::with_pool(
            &addr,
            std::sync::Arc::new(ConnectionPool::new().with_io_backend(ClientIoBackend::Auto)),
        );
        assert_eq!(auto.fetch_range(&object, 0, 1).await.unwrap(), b"x");
        server.await.unwrap();
        let strict = WorkerClient::with_pool(
            &addr,
            std::sync::Arc::new(ConnectionPool::new().with_io_backend(ClientIoBackend::IoUring)),
        );
        match strict.fetch_range(&object, 0, 1).await {
            Err(crate::WorkerError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied)
            }
            other => panic!("strict ring backend should preserve EPERM: {other:?}"),
        }
    }
}
