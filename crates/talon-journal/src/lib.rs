//! Cache metadata journal: one private runtime, independent typed streams.
mod disk;
mod frame;
mod lifecycle;
mod replay;
#[cfg(test)]
mod tests;
mod writer;

use bytes::Bytes;
use futures::{future::LocalBoxFuture, stream::FuturesUnordered, StreamExt};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

pub type Lsn = u64;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    Written,
    Durable,
}

pub trait Machine: Send + 'static {
    type Record: Send + 'static;
    type SnapshotRecord: Send + 'static;
    type Snapshot: Iterator<Item = io::Result<Bytes>> + Send + 'static;
    fn encoded_len(record: &Self::Record) -> io::Result<usize>;
    fn encode(record: &Self::Record) -> io::Result<Bytes>;
    fn decode(bytes: &[u8]) -> io::Result<Self::Record>;
    fn apply(&mut self, lsn: Lsn, record: Self::Record) -> io::Result<()>;
    fn snapshot(&self) -> io::Result<Self::Snapshot>;
    fn decode_snapshot(bytes: &[u8]) -> io::Result<Self::SnapshotRecord>;
    fn restore(&mut self, record: Self::SnapshotRecord) -> io::Result<()>;
}

#[derive(Clone, Debug)]
pub struct Options {
    pub require_native: bool,
    pub max_requests: usize,
    pub max_bytes: usize,
    pub recovery_threads: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            require_native: false,
            max_requests: 1024,
            max_bytes: 8 << 20,
            recovery_threads: std::thread::available_parallelism()
                .map_or(1, |n| n.get())
                .min(4),
        }
    }
}
#[derive(Clone, Debug)]
pub struct StreamOptions {
    pub checkpoint_bytes: u64,
    pub max_record_bytes: usize,
}
impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            checkpoint_bytes: 64 << 20,
            max_record_bytes: 4 << 20,
        }
    }
}

/// Worker policy can rate-limit snapshots without coupling this crate to Worker.
pub trait Maintenance: Send + Sync {
    fn bytes(&self, bytes: usize) -> futures::future::LocalBoxFuture<'_, ()>;
    fn io(&self) -> futures::future::LocalBoxFuture<'_, Box<dyn Send>>;
    fn delete(&self) -> futures::future::LocalBoxFuture<'_, ()>;
}

type Launch = Box<dyn FnOnce() -> LocalBoxFuture<'static, ()> + Send>;
type Query<M> = Box<dyn FnOnce(&M) + Send>;
struct Admission {
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_bytes: usize,
}
struct Permits {
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
impl Admission {
    async fn acquire(&self, bytes: usize) -> io::Result<Permits> {
        if bytes > self.max_bytes {
            return Err(invalid("journal request exceeds service byte budget"));
        }
        Ok(Permits {
            _count: self
                .count
                .clone()
                .acquire_owned()
                .await
                .map_err(io::Error::other)?,
            _bytes: self
                .bytes
                .clone()
                .acquire_many_owned(bytes as u32)
                .await
                .map_err(io::Error::other)?,
        })
    }
}
enum Command<M: Machine> {
    Append {
        record: M::Record,
        bytes: Bytes,
        ack: Ack,
        done: oneshot::Sender<io::Result<Lsn>>,
    },
    Query(Query<M>),
    Checkpoint(oneshot::Sender<io::Result<()>>),
}
struct Request<M: Machine> {
    command: Command<M>,
    _permits: Permits,
}

pub struct JournalService {
    root: PathBuf,
    launch: Option<mpsc::Sender<Launch>>,
    admission: Arc<Admission>,
    options: Options,
    life: Arc<lifecycle::Life>,
    streams: lifecycle::Registry,
    native: bool,
    snapshot_slots: Arc<Semaphore>,
}
impl JournalService {
    pub fn is_native(&self) -> bool {
        self.native
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn wait_for_retiring(root: &Path) -> io::Result<()> {
        lifecycle::wait_for_retiring(root)
    }
    pub fn open(
        root: PathBuf,
        keepalive: Arc<dyn Send + Sync>,
        options: Options,
    ) -> io::Result<Arc<Self>> {
        if options.max_requests == 0
            || options.max_bytes == 0
            || options.max_bytes > u32::MAX as usize
        {
            return Err(invalid("invalid journal service budgets"));
        }
        std::fs::create_dir_all(&root)?;
        let root = root.canonicalize()?;
        let life = lifecycle::enter_service(&root)?;
        let exit = lifecycle::Exit(life.clone());
        let (launch, mut commands) = mpsc::channel::<Launch>(128);
        let (ready, wait) = std::sync::mpsc::sync_channel(1);
        let require_native = options.require_native;
        std::thread::Builder::new()
            .name("cache-journal".into())
            .spawn(move || {
                let _exit = exit;
                let _lease = keepalive;
                let runtime = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_all()
                    .build();
                match runtime {
                    Err(e) => {
                        let _ = ready.send(Err(e));
                    }
                    Ok(mut runtime) => runtime.block_on(async move {
                        let native = !monoio::utils::is_legacy();
                        if require_native && !native {
                            let _ =
                                ready.send(Err(io::Error::other("native journal ring required")));
                            return;
                        }
                        tracing::info!(native, "cache journal I/O thread ready");
                        let _ = ready.send(Ok(native));
                        let mut streams = FuturesUnordered::new();
                        let mut closed = false;
                        loop {
                            if closed && streams.is_empty() {
                                break;
                            }
                            tokio::select! {
                                command = commands.recv(), if !closed => match command {
                                    Some(launch) => streams.push(launch()), None => closed = true,
                                },
                                _ = streams.next(), if !streams.is_empty() => {},
                            }
                        }
                    }),
                }
            })?;
        let native = wait.recv().map_err(io::Error::other)??;
        Ok(Arc::new(Self {
            native,
            snapshot_slots: Arc::new(Semaphore::new(2)),
            root,
            launch: Some(launch),
            admission: Arc::new(Admission {
                count: Arc::new(Semaphore::new(options.max_requests)),
                bytes: Arc::new(Semaphore::new(options.max_bytes)),
                max_bytes: options.max_bytes,
            }),
            options,
            life,
            streams: Default::default(),
        }))
    }
    pub fn open_stream<M: Machine>(
        self: &Arc<Self>,
        relative_path: PathBuf,
        machine: M,
        options: StreamOptions,
    ) -> io::Result<Handle<M>> {
        self.open_stream_with_maintenance(relative_path, machine, options, None)
    }
    /// Install maintenance limits before recovery can trigger an automatic checkpoint.
    pub fn open_stream_with_maintenance<M: Machine>(
        self: &Arc<Self>,
        relative_path: PathBuf,
        machine: M,
        options: StreamOptions,
        maintenance: Option<Arc<dyn Maintenance>>,
    ) -> io::Result<Handle<M>> {
        if relative_path.as_os_str().is_empty()
            || !relative_path
                .components()
                .all(|p| matches!(p, std::path::Component::Normal(_)))
        {
            return Err(invalid(
                "journal stream path must be relative normal components",
            ));
        }
        if options.checkpoint_bytes == 0
            || options.max_record_bytes == 0
            || options.max_record_bytes > self.options.max_bytes.saturating_sub(frame::HEADER)
        {
            return Err(invalid("invalid journal stream limits"));
        }
        let mut path = self.root.clone();
        for component in relative_path.components() {
            path.push(component.as_os_str());
            match std::fs::create_dir(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    if !std::fs::symlink_metadata(&path)?.is_dir() {
                        return Err(invalid("journal stream path must contain real directories"));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        let life = lifecycle::enter(&self.streams, &path)?;
        let exit = lifecycle::Exit(life.clone());
        // Startup-only: persist every newly created stream ancestor before an
        // acknowledgement can depend on that path surviving a crash.
        let mut parent = path.parent();
        while let Some(dir) = parent.filter(|p| p.starts_with(&self.root)) {
            std::fs::File::open(dir)?.sync_all()?;
            parent = dir.parent();
        }
        let (send, receive) = mpsc::channel(self.options.max_requests);
        let (ready, wait) = std::sync::mpsc::sync_channel(1);
        let failed = Arc::new(AtomicBool::new(false));
        let poison = failed.clone();
        let allocated = Arc::new(AtomicU64::new(0));
        let accounting = allocated.clone();
        let maintenance = Arc::new(std::sync::RwLock::new(maintenance));
        let policy = maintenance.clone();
        let recovery_threads = self.options.recovery_threads;
        let max_record = options.max_record_bytes;
        let snapshot_slots = self.snapshot_slots.clone();
        #[cfg(test)]
        let control = Arc::new(tests::Control::default());
        #[cfg(test)]
        let test_control = control.clone();
        let task: Launch = Box::new(move || {
            Box::pin(async move {
                let _exit = exit;
                match disk::Disk::open(path, machine, options, recovery_threads, accounting).await {
                    Ok((disk, machine)) => {
                        #[cfg(test)]
                        let disk = disk.with_control(test_control);
                        let _ = ready.send(Ok(()));
                        if let Err(error) =
                            writer::run(disk, machine, receive, &poison, policy, snapshot_slots)
                                .await
                        {
                            poison.store(true, Ordering::Release);
                            tracing::error!(%error, "journal stream stopped; restart required");
                        }
                    }
                    Err(e) => {
                        let _ = ready.send(Err(e));
                    }
                }
            })
        });
        futures::executor::block_on(tokio::task::unconstrained(
            self.launch.as_ref().unwrap().send(task),
        ))
        .map_err(|_| stopped())?;
        wait.recv().map_err(io::Error::other)??;
        Ok(Handle {
            inner: Arc::new(HandleInner {
                send: Some(send),
                failed,
                allocated,
                maintenance,
                max_record,
                #[cfg(test)]
                control,
                life,
                service: self.clone(),
            }),
        })
    }
}
impl Drop for JournalService {
    fn drop(&mut self) {
        self.life.close();
        self.launch.take();
    }
}
struct HandleInner<M: Machine> {
    send: Option<mpsc::Sender<Request<M>>>,
    failed: Arc<AtomicBool>,
    allocated: Arc<AtomicU64>,
    maintenance: Arc<std::sync::RwLock<Option<Arc<dyn Maintenance>>>>,
    max_record: usize,
    #[cfg(test)]
    control: Arc<tests::Control>,
    life: Arc<lifecycle::Life>,
    service: Arc<JournalService>,
}
impl<M: Machine> Drop for HandleInner<M> {
    fn drop(&mut self) {
        self.life.close();
        self.send.take();
    }
}
pub struct Handle<M: Machine> {
    inner: Arc<HandleInner<M>>,
}
impl<M: Machine> Clone for Handle<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<M: Machine> Handle<M> {
    pub fn poison(&self) {
        self.inner.failed.store(true, Ordering::Release);
    }
    pub fn allocated_bytes(&self) -> u64 {
        self.inner.allocated.load(Ordering::Relaxed)
    }
    pub fn set_maintenance(&self, policy: Arc<dyn Maintenance>) {
        *self.inner.maintenance.write().unwrap() = Some(policy);
    }
    async fn send(&self, command: Command<M>, permits: Permits) -> io::Result<()> {
        if self.inner.failed.load(Ordering::Acquire) {
            return Err(stopped());
        }
        self.inner
            .send
            .as_ref()
            .unwrap()
            .send(Request {
                command,
                _permits: permits,
            })
            .await
            .map_err(|_| stopped())
    }
    pub async fn append(&self, record: M::Record, ack: Ack) -> io::Result<Lsn> {
        let limit = M::encoded_len(&record)?;
        if limit > self.inner.max_record {
            return Err(invalid("journal record exceeds stream budget"));
        }
        let permits = self
            .inner
            .service
            .admission
            .acquire(frame::HEADER + limit)
            .await?;
        let bytes = M::encode(&record)?;
        if bytes.len() > limit {
            return Err(invalid("journal encoded_len underestimated record"));
        }
        let (done, wait) = oneshot::channel();
        self.send(
            Command::Append {
                record,
                bytes,
                ack,
                done,
            },
            permits,
        )
        .await?;
        wait.await.map_err(|_| stopped())?
    }
    pub async fn query<R: Send + 'static>(
        &self,
        query: impl FnOnce(&M) -> io::Result<R> + Send + 'static,
    ) -> io::Result<R> {
        let permits = self.inner.service.admission.acquire(1).await?;
        let (done, wait) = oneshot::channel();
        self.send(
            Command::Query(Box::new(move |machine| {
                let _ = done.send(query(machine));
            })),
            permits,
        )
        .await?;
        wait.await.map_err(|_| stopped())?
    }
    pub fn query_blocking<R: Send + 'static>(
        &self,
        query: impl FnOnce(&M) -> io::Result<R> + Send + 'static,
    ) -> io::Result<R> {
        // Startup may run inside a Tokio task. This synchronous bridge cannot
        // yield to that executor to replenish its cooperative polling budget.
        // The independent journal thread still enforces admission and ordering.
        futures::executor::block_on(tokio::task::unconstrained(self.query(query)))
    }
    pub async fn checkpoint(&self) -> io::Result<()> {
        let permits = self.inner.service.admission.acquire(1).await?;
        let (done, wait) = oneshot::channel();
        self.send(Command::Checkpoint(done), permits).await?;
        wait.await.map_err(|_| stopped())?
    }
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn stopped() -> io::Error {
    io::Error::other("journal stream stopped; restart required")
}
