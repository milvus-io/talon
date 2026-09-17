//! Whole-operation execution for language bindings. Only owned arguments and
//! results cross threads; native pools, planning and all child RPCs stay local.
use std::{
    future::Future,
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use talon_cache_client::read_buffer::{ReadBuffer, ReadDestination, ReadTarget};

#[cfg(target_os = "linux")]
use std::{
    cell::RefCell,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};
use talon_cache_client::rpc::RequestExecutor;
use tokio::sync::{mpsc, oneshot};

use crate::{
    Client, ClientBuilder, ClientIoBackend, CoordinatorError, Error, ObjectEntry, ObjectId,
    ObjectStat, RequestOptions, TraceContext, TraceParent,
};

static NEXT_GROUP: AtomicUsize = AtomicUsize::new(1);
#[cfg(target_os = "linux")]
thread_local! {
    static NATIVE_LANE: RefCell<Option<Rc<NativeLane>>> = const { RefCell::new(None) };
}

/// Thread-safe SDK handle with an owned execution group per constructed client.
/// Clones share the group, metadata caches and aggregate idle connection limits. Each read,
/// stat or list crosses the queue once, regardless of its number of child RPCs.
///
/// Methods enqueue eagerly; awaiting their results needs no caller runtime.
/// Dropping a returned future cancels queued or active work. Pending result
/// futures keep the runtime alive; callback operations require the caller to keep
/// a handle alive. The last owner dropping cancels remaining work and tears down
/// the runtime on its own thread, never in the caller. Submission is nonblocking;
/// tasks are submitted without an SDK-wide admission limit, as with Tokio spawn.
/// Native callback continuations go directly to their ring scheduler.
#[derive(Clone)]
pub struct HostedClient {
    group: Arc<Group>,
    coordinator: Arc<str>,
    block_size: u32,
    backend: ClientIoBackend,
}

struct Group {
    id: usize,
    lanes: Vec<Lane>,
    _stops: Vec<oneshot::Sender<()>>,
    next: AtomicUsize,
    threads: usize,
    tokio: Option<(tokio::runtime::Handle, Arc<Client>)>,
}
struct Lane {
    jobs: mpsc::UnboundedSender<Job>,
}
struct Job {
    command: Command,
    parent: Option<TraceContext>,
    dispatch: Option<tracing::Dispatch>,
}

struct Destination(Box<dyn ReadDestination>);
// SAFETY: delegate the owned allocation contract to the erased destination.
unsafe impl ReadDestination for Destination {
    fn raw_parts(&mut self) -> (*mut u8, usize) {
        self.0.raw_parts()
    }
}

enum Command {
    Read {
        object: ObjectId,
        offset: u64,
        length: Option<u64>,
        stat: Option<ObjectStat>,
        result: Completion<Vec<u8>>,
    },
    ReadInto {
        object: ObjectId,
        offset: u64,
        buffer: Destination,
        stat: Option<ObjectStat>,
        result: Completion<usize>,
    },
    ReadTarget {
        object: ObjectId,
        offset: u64,
        target: ReadTarget,
        stat: Option<ObjectStat>,
        result: Completion<usize>,
    },
    Stat {
        object: ObjectId,
        result: Completion<ObjectStat>,
    },
    List {
        prefix: String,
        result: Completion<Vec<ObjectEntry>>,
    },
}

enum Completion<T> {
    Future(oneshot::Sender<Result<T, Error>>),
    Callback(Box<dyn FnOnce(Result<T, Error>) + Send>),
}

fn runtime_error(error: io::Error) -> Error {
    CoordinatorError::Io(error).into()
}
fn closed() -> Error {
    runtime_error(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "SDK operation executor closed",
    ))
}

pub(crate) fn resolve_threads(explicit: Option<usize>) -> Result<usize, Error> {
    fn positive(value: usize) -> Result<usize, Error> {
        if value == 0 {
            Err(Error::InvalidArgument(
                "I/O thread count must be positive".into(),
            ))
        } else {
            Ok(value)
        }
    }
    if let Some(value) = explicit {
        return positive(value);
    }
    for name in ["TALON_CLIENT_IO_THREADS", "TOKIO_WORKER_THREADS"] {
        match std::env::var(name) {
            Ok(value) => {
                return positive(value.parse().map_err(|_| {
                    Error::InvalidArgument(format!("{name} must be a positive integer"))
                })?)
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(error) => return Err(Error::InvalidArgument(format!("{name}: {error}"))),
        }
    }
    Ok(std::thread::available_parallelism().map_or(1, |n| n.get()))
}

// Until every runtime is ready, errors cancel and join all started threads.
// No partial group is published, and Auto cannot mix native and Tokio lanes.
struct Starting {
    id: usize,
    lanes: Vec<Lane>,
    stops: Vec<oneshot::Sender<()>>,
    threads: Vec<std::thread::JoinHandle<()>>,
}
impl Default for Starting {
    fn default() -> Self {
        Self {
            id: NEXT_GROUP.fetch_add(1, Ordering::Relaxed),
            lanes: Vec::new(),
            stops: Vec::new(),
            threads: Vec::new(),
        }
    }
}
impl Starting {
    fn lane(&mut self) -> (mpsc::UnboundedReceiver<Job>, oneshot::Receiver<()>) {
        let (jobs, rx) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel();
        self.lanes.push(Lane { jobs });
        self.stops.push(stop);
        (rx, stopped)
    }
    fn finish(mut self) -> Arc<Group> {
        self.threads.clear(); // Runtime teardown remains on the owning threads.
        Arc::new(Group {
            id: self.id,
            lanes: std::mem::take(&mut self.lanes),
            _stops: std::mem::take(&mut self.stops),
            next: AtomicUsize::new(0),
            threads: 0,
            tokio: None,
        })
    }
}
impl Drop for Starting {
    fn drop(&mut self) {
        self.stops.clear();
        self.lanes.clear();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

#[cfg(target_os = "linux")]
fn native_group(client: Client, count: usize, idle_limit: usize) -> io::Result<Arc<Group>> {
    use talon_cache_client::monoio_client::SharedIdleBudget;
    let control = Arc::new(SharedIdleBudget::new(idle_limit));
    let data = Arc::new(SharedIdleBudget::new(idle_limit));
    let mut starting = Starting::default();
    let group_id = starting.id;
    for _index in 0..count {
        let (rx, stopped) = starting.lane();
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let (client, control, data) = (client.clone(), control.clone(), data.clone());
        starting.threads.push(
            std::thread::Builder::new()
                .name(format!("talon-sdk-ring-{_index}"))
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
                    runtime.block_on(async move {
                        let lane = Rc::new(NativeLane {
                            group: group_id,
                            client: Rc::new(client.on_ring(&control, &data)),
                            tasks: Rc::new(RefCell::new(LocalTasks::default())),
                        });
                        NATIVE_LANE.with(|slot| *slot.borrow_mut() = Some(lane.clone()));
                        if ready.send(Ok(())).is_ok() {
                            serve_native(lane, rx, stopped).await;
                        }
                        NATIVE_LANE.with(|slot| slot.borrow_mut().take());
                    });
                })?,
        );
        started
            .recv()
            .map_err(|_| io::Error::other("SDK ring startup thread exited"))??;
    }
    let mut group = starting.finish();
    Arc::get_mut(&mut group).unwrap().threads = count;
    Ok(group)
}

fn tokio_group(client: Client, count: usize) -> io::Result<Arc<Group>> {
    let mut starting = Starting::default();
    let (stop, stopped) = oneshot::channel();
    starting.stops.push(stop);
    let (ready, started) = std::sync::mpsc::sync_channel(1);
    starting.threads.push(
        std::thread::Builder::new()
            .name("talon-sdk-runtime".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(count)
                    .thread_name("talon-sdk-tokio")
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(runtime.handle().clone())).is_ok() {
                    runtime.block_on(async {
                        let _ = stopped.await;
                    });
                }
            })?,
    );
    let handle = started
        .recv()
        .map_err(|_| io::Error::other("SDK Tokio startup thread exited"))??;
    let mut group = starting.finish();
    let inner = Arc::get_mut(&mut group).unwrap();
    inner.threads = count;
    inner.tokio = Some((handle, Arc::new(client)));
    Ok(group)
}

pub(crate) fn build(
    builder: ClientBuilder,
    backend: ClientIoBackend,
    count: usize,
) -> Result<HostedClient, Error> {
    #[cfg(target_os = "linux")]
    let idle_limit = builder.max_idle_per_addr;
    let client = builder.with_io_backend(ClientIoBackend::Tokio).build()?;
    let coordinator = Arc::from(client.coordinator_addr());
    let block_size = client.block_size();
    let mut selected = ClientIoBackend::Tokio;
    let group = if backend == ClientIoBackend::Tokio {
        tokio_group(client, count).map_err(runtime_error)?
    } else {
        #[cfg(target_os = "linux")]
        let native = native_group(client.clone(), count, idle_limit);
        #[cfg(not(target_os = "linux"))]
        let native: io::Result<Arc<Group>> = Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring requires Linux",
        ));
        match native {
            Ok(group) => {
                selected = ClientIoBackend::IoUring;
                group
            }
            Err(error) if backend == ClientIoBackend::IoUring => return Err(runtime_error(error)),
            Err(_) => tokio_group(client, count).map_err(runtime_error)?,
        }
    };
    Ok(HostedClient {
        group,
        coordinator,
        block_size,
        backend: selected,
    })
}

impl HostedClient {
    /// The backend selected at construction; never Auto and never changed by an RPC error.
    pub fn io_backend(&self) -> ClientIoBackend {
        self.backend
    }
    /// Execution parallelism selected at construction. Clones share these threads.
    pub fn io_threads(&self) -> usize {
        self.group.threads
    }
    /// Configured coordinator address.
    pub fn coordinator_addr(&self) -> &str {
        &self.coordinator
    }
    /// Configured logical block size.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Read an owned result, inheriting the caller's current trace context.
    pub fn read(
        &self,
        object: &ObjectId,
        offset: u64,
        length: Option<u64>,
        stat: Option<&ObjectStat>,
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send + 'static {
        self.read_with_options(object, offset, length, stat, &RequestOptions::default())
    }

    /// Enqueue a complete read with explicit trace parent selection. Exact-version,
    /// EOF and input-validation semantics are the same as [`Client::read_with_options`].
    pub fn read_with_options(
        &self,
        object: &ObjectId,
        offset: u64,
        length: Option<u64>,
        stat: Option<&ObjectStat>,
        options: &RequestOptions<'_>,
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send + 'static {
        let (result, receiver) = oneshot::channel();
        self.submit(
            Command::Read {
                object: object.clone(),
                offset,
                length,
                stat: stat.cloned(),
                result: Completion::Future(result),
            },
            receiver,
            options,
        )
    }

    /// Receive directly into a destination, returning its ownership on completion.
    pub fn read_into<B: ReadDestination>(
        &self,
        object: &ObjectId,
        offset: u64,
        buffer: B,
        stat: Option<&ObjectStat>,
    ) -> impl Future<Output = (Result<usize, Error>, B)> + Send + 'static {
        self.read_into_with_options(object, offset, buffer, stat, &RequestOptions::default())
    }

    /// Direct destination read with explicit trace parent selection.
    /// Errors may leave partial contents. Cancellation retains the buffer until
    /// outstanding kernel operations have stopped accessing it.
    pub fn read_into_with_options<B: ReadDestination>(
        &self,
        object: &ObjectId,
        offset: u64,
        buffer: B,
        stat: Option<&ObjectStat>,
        options: &RequestOptions<'_>,
    ) -> impl Future<Output = (Result<usize, Error>, B)> + Send + 'static {
        let mut buffer = ReadBuffer::new(buffer);
        let (result, receiver) = oneshot::channel();
        let submitted = self.enqueue(
            Command::ReadTarget {
                object: object.clone(),
                offset,
                target: buffer.take_target(),
                stat: stat.cloned(),
                result: Completion::Future(result),
            },
            options,
        );
        let lifetime = self.group.clone();
        async move {
            let result = match submitted {
                Ok(()) => receiver.await.unwrap_or_else(|_| Err(closed())),
                Err(error) => Err(error),
            };
            let buffer = buffer.finish().await;
            drop(lifetime);
            (result, buffer)
        }
    }

    /// Enqueue a stat with explicit trace parent selection.
    pub fn stat_with_options(
        &self,
        object: &ObjectId,
        options: &RequestOptions<'_>,
    ) -> impl Future<Output = Result<ObjectStat, Error>> + Send + 'static {
        let (result, receiver) = oneshot::channel();
        self.submit(
            Command::Stat {
                object: object.clone(),
                result: Completion::Future(result),
            },
            receiver,
            options,
        )
    }

    /// Enqueue a complete object listing.
    pub fn list(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Vec<ObjectEntry>, Error>> + Send + 'static {
        let (result, receiver) = oneshot::channel();
        self.submit(
            Command::List {
                prefix: prefix.to_owned(),
                result: Completion::Future(result),
            },
            receiver,
            &RequestOptions::default(),
        )
    }

    /// Move a destination handle to the SDK thread and execute the complete read.
    /// The handle must keep its destination valid until dropped. It is dropped
    /// before the callback, which receives the byte count or error. This lets
    /// foreign bindings reuse their caller-owned destination without assembling
    /// an intermediate payload buffer. Kernel I/O retains this destination handle
    /// through completion, including cancellation.
    ///
    /// The callback runs inline on the operation thread and must not block.
    /// Keep this client alive through callback completion; dropping all handles
    /// cancels callback operations that have not completed.
    pub fn read_into_with_callback(
        &self,
        object: &ObjectId,
        offset: u64,
        buffer: impl ReadDestination,
        stat: Option<&ObjectStat>,
        options: &RequestOptions<'_>,
        callback: impl FnOnce(Result<usize, Error>) + Send + 'static,
    ) -> Result<(), Error> {
        self.read_into_owned_with_callback(
            object.clone(),
            offset,
            buffer,
            stat.cloned(),
            options,
            callback,
        )
    }

    /// Submit owned metadata without cloning its URI/version strings. This has
    /// the same buffer, inline-callback and cancellation contract as
    /// [`Self::read_into_with_callback`]. Language bindings already own these
    /// arguments after validating/copying the foreign input.
    pub fn read_into_owned_with_callback(
        &self,
        object: ObjectId,
        offset: u64,
        buffer: impl ReadDestination,
        stat: Option<ObjectStat>,
        options: &RequestOptions<'_>,
        callback: impl FnOnce(Result<usize, Error>) + Send + 'static,
    ) -> Result<(), Error> {
        let parent = capture_parent(options);
        let dispatch = capture_dispatch();
        if let Some((runtime, client)) = &self.group.tokio {
            let client = client.clone();
            runtime.spawn(with_dispatch(dispatch, async move {
                let options = RequestOptions {
                    parent: parent
                        .as_ref()
                        .map(TraceParent::Explicit)
                        .unwrap_or(TraceParent::Root),
                };
                let (value, buffer) = client
                    .read_into_with_options(&object, offset, buffer, stat.as_ref(), &options)
                    .await;
                drop(buffer);
                callback(value);
            }));
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        if let Some(lane) = NATIVE_LANE.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|lane| lane.group == self.group.id)
                .cloned()
        }) {
            let client = lane.client.clone();
            return lane.spawn(with_dispatch(dispatch, async move {
                let options = RequestOptions {
                    parent: parent
                        .as_ref()
                        .map(TraceParent::Explicit)
                        .unwrap_or(TraceParent::Root),
                };
                let (value, buffer) = client
                    .read_into_with_options(&object, offset, buffer, stat.as_ref(), &options)
                    .await;
                drop(buffer);
                callback(value);
            }));
        }
        self.enqueue(
            Command::ReadInto {
                object,
                offset,
                buffer: Destination(Box::new(buffer)),
                stat,
                result: Completion::Callback(Box::new(callback)),
            },
            options,
        )
    }

    /// Submit a stat with the same inline callback and lifetime contract as
    /// [`Self::read_into_with_callback`].
    pub fn stat_with_callback(
        &self,
        object: &ObjectId,
        options: &RequestOptions<'_>,
        callback: impl FnOnce(Result<ObjectStat, Error>) + Send + 'static,
    ) -> Result<(), Error> {
        self.enqueue(
            Command::Stat {
                object: object.clone(),
                result: Completion::Callback(Box::new(callback)),
            },
            options,
        )
    }

    fn enqueue(&self, command: Command, options: &RequestOptions<'_>) -> Result<(), Error> {
        let job = Job {
            command,
            parent: capture_parent(options),
            dispatch: capture_dispatch(),
        };
        if let Some((runtime, client)) = &self.group.tokio {
            let client = client.clone();
            runtime.spawn(async move { run_job(&client, job).await });
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        if let Some(lane) = NATIVE_LANE.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|lane| lane.group == self.group.id)
                .cloned()
        }) {
            return lane.submit(job);
        }
        let index = self.group.next.fetch_add(1, Ordering::Relaxed) % self.group.lanes.len();
        self.group.lanes[index].jobs.send(job).map_err(|_| closed())
    }

    fn submit<T: Send + 'static>(
        &self,
        command: Command,
        receiver: oneshot::Receiver<Result<T, Error>>,
        options: &RequestOptions<'_>,
    ) -> impl Future<Output = Result<T, Error>> + Send + 'static {
        let submitted = self.enqueue(command, options);
        let lifetime = self.group.clone();
        async move {
            submitted?;
            let result = receiver.await.map_err(|_| closed())?;
            drop(lifetime);
            result
        }
    }
}

async fn complete<T>(result: Completion<T>, operation: impl Future<Output = Result<T, Error>>) {
    match result {
        Completion::Future(mut result) => {
            tokio::select! {
                biased;
                _ = result.closed() => {},
                value = operation => { let _ = result.send(value); },
            }
        }
        Completion::Callback(callback) => callback(operation.await),
    }
}

async fn run_job<P: RequestExecutor + Default>(client: &Client<P>, job: Job) {
    let operation = async move {
        let options = RequestOptions {
            parent: job
                .parent
                .as_ref()
                .map(TraceParent::Explicit)
                .unwrap_or(TraceParent::Root),
        };
        match job.command {
            Command::Read {
                object,
                offset,
                length,
                stat,
                result,
            } => {
                complete(
                    result,
                    client.read_with_options(&object, offset, length, stat.as_ref(), &options),
                )
                .await;
            }
            Command::ReadInto {
                object,
                offset,
                buffer,
                stat,
                result,
            } => {
                let read = async {
                    let (value, buffer) = client
                        .read_into_with_options(&object, offset, buffer, stat.as_ref(), &options)
                        .await;
                    drop(buffer);
                    value
                };
                complete(result, read).await;
            }
            Command::ReadTarget {
                object,
                offset,
                target,
                stat,
                result,
            } => {
                complete(
                    result,
                    client.read_target_with_options(
                        &object,
                        offset,
                        target,
                        stat.as_ref(),
                        &options,
                    ),
                )
                .await;
            }
            Command::Stat { object, result } => {
                complete(result, client.stat_with_options(&object, &options)).await
            }
            Command::List { prefix, result } => {
                let op = talon_telemetry::Operation::new("talon.list", "internal", options.parent);
                complete(result, op.scope(client.list(&prefix))).await;
            }
        }
    };
    with_dispatch(job.dispatch, operation).await;
}

fn capture_parent(options: &RequestOptions<'_>) -> Option<TraceContext> {
    match options.parent {
        TraceParent::Explicit(parent) => Some(parent.clone()),
        TraceParent::Inherit => talon_telemetry::current_carrier(),
        TraceParent::Root => None,
    }
}
fn capture_dispatch() -> Option<tracing::Dispatch> {
    tracing::dispatcher::get_default(|dispatch| {
        (!dispatch.is::<tracing::subscriber::NoSubscriber>()).then(|| dispatch.clone())
    })
}
// A structural wrapper stores F once. An async match wrapping F in two
// different await branches retained a second full operation frame (~17 KiB).
struct Dispatched<F> {
    dispatch: Option<tracing::Dispatch>,
    operation: F,
}
fn with_dispatch<F: Future<Output = ()>>(
    dispatch: Option<tracing::Dispatch>,
    operation: F,
) -> Dispatched<F> {
    Dispatched {
        dispatch,
        operation,
    }
}
impl<F: Future<Output = ()>> Future for Dispatched<F> {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        // SAFETY: operation is structurally pinned; this wrapper never moves it.
        let this = unsafe { self.get_unchecked_mut() };
        let mut operation = unsafe { std::pin::Pin::new_unchecked(&mut this.operation) };
        match &this.dispatch {
            Some(dispatch) => {
                tracing::dispatcher::with_default(dispatch, || operation.as_mut().poll(cx))
            }
            None if tracing::dispatcher::get_default(|d| {
                d.is::<tracing::subscriber::NoSubscriber>()
            }) =>
            {
                operation.poll(cx)
            }
            None => tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
                operation.as_mut().poll(cx)
            }),
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct LocalTasks {
    closing: bool,
    slots: Vec<Option<Waker>>,
    free: Vec<usize>,
    active: usize,
    drained: Option<Waker>,
}
#[cfg(target_os = "linux")]
struct NativeLane {
    group: usize,
    client: Rc<Client<talon_cache_client::monoio_client::MonoioClient>>,
    tasks: Rc<RefCell<LocalTasks>>,
}
#[cfg(target_os = "linux")]
impl NativeLane {
    fn submit(&self, job: Job) -> Result<(), Error> {
        let client = self.client.clone();
        self.spawn(async move { run_job(&client, job).await })
    }
    fn spawn(&self, operation: impl Future<Output = ()> + 'static) -> Result<(), Error> {
        let index = {
            let mut tasks = self.tasks.borrow_mut();
            if tasks.closing {
                return Err(closed());
            }
            tasks.active += 1;
            if let Some(index) = tasks.free.pop() {
                index
            } else {
                let index = tasks.slots.len();
                tasks.slots.push(None);
                index
            }
        };
        monoio::spawn(NativeTask {
            operation,
            tasks: self.tasks.clone(),
            index,
        });
        Ok(())
    }
}
// A local slot tracks shutdown only. It replaces an outer FuturesUnordered and
// global semaphore; all normal scheduling is performed by Monoio itself.
#[cfg(target_os = "linux")]
struct NativeTask<F> {
    operation: F,
    tasks: Rc<RefCell<LocalTasks>>,
    index: usize,
}
#[cfg(target_os = "linux")]
impl<F: Future<Output = ()>> Future for NativeTask<F> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: operation is structurally pinned and never moved by Drop.
        let this = unsafe { self.get_unchecked_mut() };
        {
            let mut tasks = this.tasks.borrow_mut();
            if tasks.closing {
                return Poll::Ready(());
            }
            let slot = &mut tasks.slots[this.index];
            if slot.as_ref().map_or(true, |w| !w.will_wake(cx.waker())) {
                *slot = Some(cx.waker().clone());
            }
        }
        unsafe { Pin::new_unchecked(&mut this.operation) }.poll(cx)
    }
}
#[cfg(target_os = "linux")]
impl<F> Drop for NativeTask<F> {
    fn drop(&mut self) {
        let mut tasks = self.tasks.borrow_mut();
        tasks.slots[self.index] = None;
        tasks.free.push(self.index);
        tasks.active -= 1;
        if tasks.active == 0 {
            if let Some(waker) = tasks.drained.take() {
                waker.wake();
            }
        }
    }
}
#[cfg(target_os = "linux")]
async fn serve_native(
    lane: Rc<NativeLane>,
    mut jobs: mpsc::UnboundedReceiver<Job>,
    mut stopped: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut stopped => break,
            job = jobs.recv() => match job { Some(job) => { let _ = lane.submit(job); }, None => break },
        }
    }
    let wakers = {
        let mut tasks = lane.tasks.borrow_mut();
        tasks.closing = true;
        tasks
            .slots
            .iter_mut()
            .filter_map(Option::take)
            .collect::<Vec<_>>()
    };
    for waker in wakers {
        waker.wake();
    }
    std::future::poll_fn(|cx| {
        let mut tasks = lane.tasks.borrow_mut();
        if tasks.active == 0 {
            Poll::Ready(())
        } else {
            tasks.drained = Some(cx.waker().clone());
            Poll::Pending
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "subprocess helper for isolated environment validation"]
    fn thread_configuration_child() {
        let expected = std::env::var("TALON_TEST_EXPECT_THREADS").unwrap();
        if expected == "error" {
            assert!(resolve_threads(None).is_err());
        } else {
            let expected = if expected == "default" {
                std::thread::available_parallelism().map_or(1, |n| n.get())
            } else {
                expected.parse::<usize>().unwrap()
            };
            assert_eq!(resolve_threads(None).unwrap(), expected);
        }
        // Explicit configuration always wins over either environment variable.
        assert_eq!(resolve_threads(Some(4)).unwrap(), 4);
    }

    #[test]
    fn thread_configuration_preserves_defaults_and_legacy_override() {
        for (talon, legacy, expected) in [
            (None, None, "default"),
            (None, Some("3"), "3"),
            (Some("2"), Some("3"), "2"),
            (Some("0"), Some("3"), "error"),
            (Some("bad"), None, "error"),
        ] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .args([
                    "--exact",
                    "hosted::tests::thread_configuration_child",
                    "--ignored",
                ])
                .env_remove("TALON_CLIENT_IO_THREADS")
                .env_remove("TOKIO_WORKER_THREADS")
                .env("TALON_TEST_EXPECT_THREADS", expected);
            if let Some(value) = talon {
                child.env("TALON_CLIENT_IO_THREADS", value);
            }
            if let Some(value) = legacy {
                child.env("TOKIO_WORKER_THREADS", value);
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }

    #[test]
    fn zero_explicit_threads_is_rejected() {
        assert!(matches!(
            resolve_threads(Some(0)),
            Err(Error::InvalidArgument(_))
        ));
        assert_eq!(resolve_threads(Some(3)).unwrap(), 3);
    }

    #[test]
    fn unpublished_group_cancels_and_joins_every_started_thread() {
        let exited = Arc::new(AtomicUsize::new(0));
        let mut starting = Starting::default();
        for _ in 0..3 {
            let (_jobs, stopped) = starting.lane();
            let exited = exited.clone();
            starting.threads.push(std::thread::spawn(move || {
                assert!(futures::executor::block_on(stopped).is_err());
                exited.fetch_add(1, Ordering::Relaxed);
            }));
        }
        drop(starting);
        assert_eq!(exited.load(Ordering::Relaxed), 3);
    }
}
