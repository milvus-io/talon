//! Native completion-based RPC execution. Call directly from a Monoio runtime;
//! sockets, pooling, timers, file reads and response parsing stay on that thread.
use crate::client_io::{Config, Outcome};
use crate::rpc::{self, Error, Reply, Request, Runtime, Socket};
use bytes::Bytes;
use futures::{future::Shared, FutureExt};
use monoio::{
    buf::{IoBufMut, VecBuf},
    io::{
        AsyncReadRent, AsyncReadRentExt, AsyncWriteRentExt, OwnedReadHalf, OwnedWriteHalf,
        Splitable,
    },
};
use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::{Duration, Instant},
};

/// A per-peer idle connection budget shared by runtime-local pools. Sockets
/// never move between rings. Control and data pools should use separate budgets.
pub struct SharedIdleBudget {
    limit: usize,
    counts: Arc<crate::client_io::Counts>,
}
impl SharedIdleBudget {
    /// Create a budget; zero disables idle retention.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            counts: Arc::default(),
        }
    }
    /// Current aggregate idle count for a peer across all participating rings.
    pub fn idle_count(&self, addr: &str) -> usize {
        self.counts.get(addr)
    }
}

struct Idle {
    socket: NativeSocket,
    returned: Instant,
}
struct Bucket {
    idle: Vec<Idle>,
    count: Arc<AtomicUsize>,
}
type SharedWakeup = (monoio::time::Instant, Shared<Pin<Box<monoio::time::Sleep>>>);

/// A native, thread-local Monoio RPC client. It must be created, polled and
/// dropped on its Monoio runtime. Share it with `Rc`, including concurrent calls.
/// No dispatcher, Tokio I/O adapter or borrowed kernel buffer is used here.
pub struct MonoioClient {
    config: Config,
    idle: RefCell<HashMap<String, Bucket>>,
    wakeup: RefCell<Option<SharedWakeup>>,
}
impl Default for MonoioClient {
    fn default() -> Self {
        Self::new()
    }
}
impl MonoioClient {
    /// Create a native client with the SDK's default pool limits and deadlines.
    pub fn new() -> Self {
        Self::from_config(Config::default())
    }
    /// Set the per-peer idle limit and TTL; changing settings drops idle sockets.
    pub fn with_limits(self, max_idle: usize, ttl: Duration) -> Self {
        Self::from_config(Config {
            max_idle,
            ttl,
            ..self.config.clone()
        })
    }
    /// Replace this pool's idle limit with a shared per-peer budget. Existing
    /// idle sockets are dropped; subsequent returns compete for the same limit.
    pub fn with_idle_budget(self, budget: &SharedIdleBudget) -> Self {
        Self::from_config(Config {
            max_idle: budget.limit,
            counts: budget.counts.clone(),
            ..self.config.clone()
        })
    }
    /// Set connect and full-exchange deadlines, enforced by Monoio timers.
    pub fn with_timeouts(self, connect: Duration, request: Duration) -> Self {
        Self::from_config(Config {
            connect,
            request,
            ..self.config.clone()
        })
    }
    pub(crate) fn from_config(config: Config) -> Self {
        Self {
            config,
            idle: RefCell::new(HashMap::new()),
            wakeup: RefCell::new(None),
        }
    }
    /// Execute one complete RPC against a resolved address. A failed reused
    /// connection retries once only when the request's replay policy permits it.
    /// Owned responses can transfer to another runtime after this future completes.
    pub async fn request(&self, addr: SocketAddr, request: Request) -> Result<Reply, Error> {
        self.execute(&addr.to_string(), request).await.result
    }
    /// Fetch a range directly on the current Monoio runtime.
    pub async fn fetch_range(
        &self,
        addr: SocketAddr,
        object: &talon_core::ObjectId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, crate::WorkerError> {
        let frame = talon_transport::encode_request(
            talon_core::RequestId::next().0,
            &talon_transport::RangeRequest {
                object: object.clone(),
                offset,
                len,
            },
        )?;
        match self
            .request(addr, Request::range(frame, len))
            .await
            .map_err(Error::worker)?
        {
            Reply::Range(bytes) => Ok(bytes),
            _ => unreachable!("range response"),
        }
    }
    /// Number of retained idle connections for a resolved peer.
    pub fn idle_count(&self, addr: SocketAddr) -> usize {
        self.idle
            .borrow()
            .get(&addr.to_string())
            .map_or(0, |b| b.idle.len())
    }
    fn take(&self, addr: &str) -> Option<NativeSocket> {
        let mut idle = self.idle.borrow_mut();
        let bucket = idle.get_mut(addr)?;
        while let Some(entry) = bucket.idle.pop() {
            bucket.count.fetch_sub(1, Ordering::Relaxed);
            if entry.returned.elapsed() < self.config.ttl {
                return Some(entry.socket);
            }
        }
        None
    }
    fn release(&self, addr: &str, socket: NativeSocket) {
        let mut idle = self.idle.borrow_mut();
        let bucket = idle.entry(addr.to_owned()).or_insert_with(|| Bucket {
            idle: Vec::new(),
            count: self.config.counts.for_addr(addr),
        });
        if bucket
            .count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < self.config.max_idle).then_some(count + 1)
            })
            .is_ok()
        {
            bucket.idle.push(Idle {
                socket,
                returned: Instant::now(),
            });
        }
    }
    async fn connect(&self, addr: &str) -> io::Result<NativeSocket> {
        Native::timeout(self.config.connect, async {
            let addresses = match addr.parse::<SocketAddr>() {
                Ok(addr) => vec![addr],
                Err(_) => resolve(addr).await?,
            };
            let mut last = io::Error::new(io::ErrorKind::InvalidInput, "no socket addresses");
            for addr in addresses {
                match monoio::net::TcpStream::connect_addr(addr).await {
                    Ok(s) => {
                        let (read, write) = s.into_split();
                        return Ok(NativeSocket { read, write });
                    }
                    Err(e) => last = e,
                }
            }
            Err(last)
        })
        .await?
    }
    pub(crate) async fn execute(&self, addr: &str, mut req: Request) -> Outcome {
        self.execute_inner(addr, &mut req).await
    }
    fn deadline<F: Future>(&self, timeout: Duration, future: F) -> NativeDeadline<'_, F> {
        let now = monoio::time::Instant::now();
        NativeDeadline {
            client: self,
            future,
            timeout,
            start: now,
            deadline: now.checked_add(timeout),
            initial_done: timeout <= Duration::from_millis(10),
            wakeup: None,
            private: None,
        }
    }
    async fn execute_inner(&self, addr: &str, req: &mut Request) -> Outcome {
        let timeout = req.timeout.unwrap_or(self.config.request);
        let mut file = match self.deadline(timeout, rpc::prepare::<Native>(req)).await {
            Ok(Ok(file)) => file,
            Ok(Err(error)) => return Outcome::error(error),
            Err(error) => return Outcome::error(error.into()),
        };
        let mut reused_any = false;
        for attempt in 0..2 {
            let pooled = if attempt == 0 { self.take(addr) } else { None };
            let reused = pooled.is_some();
            reused_any |= reused;
            let mut socket = match pooled {
                Some(s) => s,
                None => match self.connect(addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        return Outcome {
                            result: Err(e.into()),
                            reused: reused_any,
                        }
                    }
                },
            };
            let mut retry_safe = true;
            let result = self
                .deadline(
                    timeout,
                    rpc::exchange::<Native>(&mut socket, req, &mut file, &mut retry_safe),
                )
                .await
                .unwrap_or_else(|e| Err(e.into()));
            match result {
                Ok(reply) => {
                    self.release(addr, socket);
                    return Outcome {
                        result: Ok(reply),
                        reused: reused_any,
                    };
                }
                Err(error) if attempt == 0 && reused && retry_safe && error.transport() => {}
                Err(error) => {
                    return Outcome {
                        result: Err(error),
                        reused: reused_any,
                    }
                }
            }
        }
        unreachable!("at most one retry")
    }
}
// Like the Tokio deadline policy, but store the request future once, in place.
// The shared initial sleep is allocated only once per 10 ms window per client.
struct NativeDeadline<'a, F> {
    client: &'a MonoioClient,
    future: F,
    timeout: Duration,
    start: monoio::time::Instant,
    deadline: Option<monoio::time::Instant>,
    initial_done: bool,
    wakeup: Option<Shared<Pin<Box<monoio::time::Sleep>>>>,
    private: Option<monoio::time::Sleep>,
}
impl<F: Future> Future for NativeDeadline<'_, F> {
    type Output = io::Result<F::Output>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY: future and private sleep are structurally pinned. Neither is
        // moved after polling; private is initialized in place through Pin::set.
        let this = unsafe { self.get_unchecked_mut() };
        if let Poll::Ready(value) = unsafe { Pin::new_unchecked(&mut this.future) }.poll(cx) {
            return Poll::Ready(Ok(value));
        }
        let Some(deadline) = this.deadline else {
            return Poll::Pending;
        };
        if !this.initial_done {
            let wakeup = this.wakeup.get_or_insert_with(|| {
                let mut slot = this.client.wakeup.borrow_mut();
                if let Some((at, sleep)) = &*slot {
                    if *at > this.start {
                        return sleep.clone();
                    }
                }
                let at = this.start + Duration::from_millis(10);
                let sleep = Box::pin(monoio::time::sleep_until(at)).shared();
                *slot = Some((at, sleep.clone()));
                sleep
            });
            if wakeup.poll_unpin(cx).is_pending() {
                return Poll::Pending;
            }
            this.initial_done = true;
            this.wakeup = None;
        }
        let mut private = unsafe { Pin::new_unchecked(&mut this.private) };
        if private.is_none() {
            private.set(Some(monoio::time::sleep_until(deadline)));
        }
        private
            .as_pin_mut()
            .unwrap()
            .poll(cx)
            .map(|()| Err(crate::pool::timeout_error("RPC", this.timeout)))
    }
}

impl Drop for MonoioClient {
    fn drop(&mut self) {
        for bucket in self.idle.get_mut().values() {
            bucket.count.fetch_sub(bucket.idle.len(), Ordering::Relaxed);
        }
    }
}

pub(crate) struct Native;
struct NativeSocket {
    read: OwnedReadHalf<monoio::net::TcpStream>,
    write: OwnedWriteHalf<monoio::net::TcpStream>,
}
impl AsyncReadRent for NativeSocket {
    async fn read<T: IoBufMut>(&mut self, buffer: T) -> monoio::BufResult<usize, T> {
        self.read.read(buffer).await
    }
    async fn readv<T: monoio::buf::IoVecBufMut>(
        &mut self,
        buffer: T,
    ) -> monoio::BufResult<usize, T> {
        self.read.readv(buffer).await
    }
}
impl Socket for NativeSocket {
    async fn read_into(&mut self, target: &mut crate::read_buffer::ReadTarget) -> io::Result<()> {
        if target.is_empty() {
            return Ok(());
        }
        let lease = target.lease().await;
        let (result, lease) = AsyncReadRentExt::read_exact(self, lease).await;
        drop(lease);
        result.map(|_| ())
    }
    async fn response_into(
        &mut self,
        expected: rpc::Expected,
        target: &mut crate::read_buffer::ReadTarget,
    ) -> Result<Reply, Error> {
        range_response_into(self, expected, target).await
    }

    async fn response(&mut self, expected: rpc::Expected) -> Result<Reply, Error> {
        match expected {
            rpc::Expected::Range(len) if len > 0 => {
                range_response(self, expected, len.min(RECEIVE_PREFIX as u64) as usize).await
            }
            _ => rpc::read_response(self, expected).await,
        }
    }

    async fn write(&mut self, bytes: Bytes) -> io::Result<()> {
        self.write.write_all(bytes).await.0.map(|_| ())
    }
    async fn write_vec(&mut self, bytes: Vec<u8>) -> io::Result<Vec<u8>> {
        let (r, b) = self.write.write_all(bytes).await;
        r?;
        Ok(b)
    }
    async fn read_exact(&mut self, len: usize) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let buffer = Vec::with_capacity(len).slice_mut(..len);
        let (result, buffer) = AsyncReadRentExt::read_exact(self, buffer).await;
        result?;
        Ok(buffer.into_inner())
    }
}
// Speculate only a bounded prefix, independent of the untrusted response length.
// Scatter the first receive into separate owned header/body allocations so the
// header removal never shifts body bytes. A larger validated reply may grow
// the body allocation when receiving the remainder.
const RECEIVE_PREFIX: usize = 64 * 1024;
async fn range_response(
    socket: &mut impl AsyncReadRent,
    expected: rpc::Expected,
    prefix_len: usize,
) -> Result<Reply, Error> {
    use talon_transport::HEADER_LEN;
    let mut buffers = VecBuf::from(vec![vec![0; HEADER_LEN], vec![0; prefix_len]]);
    // A single readv, not read_vectored_exact: an error reply or fragmented
    // header may be shorter than the successful range we requested.
    let received = loop {
        let (result, returned) = socket.readv(buffers).await;
        buffers = returned;
        match result {
            Ok(0) => {
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, "response header missing").into(),
                )
            }
            Ok(n) => break n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    };
    let mut buffers: Vec<Vec<u8>> = buffers.into();
    let mut body = buffers.pop().expect("body buffer");
    let mut header = buffers.pop().expect("header buffer");
    // VecBuf leaves untouched vectors at their original initialized length.
    header.truncate(received.min(HEADER_LEN));
    body.truncate(received.saturating_sub(HEADER_LEN));
    if header.len() < HEADER_LEN {
        header = receive_rest(socket, header, HEADER_LEN).await?;
    }
    let decoded = rpc::response_header(&header, expected)?;
    let length = decoded.length as usize;
    if body.len() > length {
        // No pipelining: bytes beyond this reply cannot be silently discarded
        // before returning the connection to the pool.
        return Err(
            crate::WorkerError::Encode(talon_transport::DataError::LengthMismatch {
                declared: length,
                actual: body.len(),
            })
            .into(),
        );
    }
    body = receive_rest(socket, body, length).await?;
    rpc::decode(header, decoded, body, expected)
}

// Only protocol metadata is separately allocated; the second iovec points at
// the caller's final destination, retained by the CQE-owned lease.
struct ReceiveMetadata {
    header: [u8; talon_transport::HEADER_LEN],
    vectors: [libc::iovec; 2],
}
thread_local! {
    // Addresses must stay stable through CQE, including dropped read futures.
    // Only completed operations return descriptors to this bounded local cache.
    #[allow(clippy::vec_box)] // Pool stable allocations; moving an in-flight iovec is unsound.
    static RECEIVE_METADATA: RefCell<Vec<Box<ReceiveMetadata>>> = const { RefCell::new(Vec::new()) };
}
struct Metadata {
    storage: Option<Box<ReceiveMetadata>>,
    initialized: usize,
}
impl Metadata {
    fn receive(mut lease: crate::read_buffer::Lease) -> DirectReceive {
        let mut storage = RECEIVE_METADATA
            .with(|pool| pool.borrow_mut().pop())
            .unwrap_or_else(|| {
                Box::new(ReceiveMetadata {
                    header: [0; talon_transport::HEADER_LEN],
                    vectors: [libc::iovec {
                        iov_base: std::ptr::null_mut(),
                        iov_len: 0,
                    }; 2],
                })
            });
        storage.vectors = [
            libc::iovec {
                iov_base: storage.header.as_mut_ptr().cast(),
                iov_len: storage.header.len(),
            },
            libc::iovec {
                iov_base: lease.write_ptr().cast(),
                iov_len: lease.bytes_total(),
            },
        ];
        DirectReceive {
            metadata: Self {
                storage: Some(storage),
                initialized: 0,
            },
            _lease: lease,
        }
    }
}
impl Drop for Metadata {
    fn drop(&mut self) {
        if let Some(mut storage) = self.storage.take() {
            // Do not retain stale caller pointers in the cache.
            for vector in &mut storage.vectors {
                vector.iov_base = std::ptr::null_mut();
                vector.iov_len = 0;
            }
            let _ = RECEIVE_METADATA.try_with(|pool| {
                let mut pool = pool.borrow_mut();
                if pool.len() < 256 {
                    pool.push(storage);
                }
            });
        }
    }
}
// SAFETY: the boxed metadata stays at a stable address through completion.
unsafe impl monoio::buf::IoBuf for Metadata {
    fn read_ptr(&self) -> *const u8 {
        self.storage.as_ref().unwrap().header.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.initialized
    }
}
unsafe impl IoBufMut for Metadata {
    fn write_ptr(&mut self) -> *mut u8 {
        self.storage.as_mut().unwrap().header.as_mut_ptr()
    }
    fn bytes_total(&mut self) -> usize {
        talon_transport::HEADER_LEN
    }
    unsafe fn set_init(&mut self, len: usize) {
        self.initialized = self.initialized.max(len);
    }
}
struct DirectReceive {
    metadata: Metadata,
    _lease: crate::read_buffer::Lease,
}
// SAFETY: header, destination and iovec array all have stable, owned storage.
// Monoio retains the entire value when cancelling an in-flight operation.
unsafe impl monoio::buf::IoVecBufMut for DirectReceive {
    fn write_iovec_ptr(&mut self) -> *mut libc::iovec {
        self.metadata.storage.as_mut().unwrap().vectors.as_mut_ptr()
    }
    fn write_iovec_len(&mut self) -> usize {
        2
    }
    unsafe fn set_init(&mut self, len: usize) {
        self.metadata.initialized = len.min(talon_transport::HEADER_LEN);
        unsafe {
            self._lease
                .set_init(len.saturating_sub(talon_transport::HEADER_LEN));
        }
    }
}
async fn range_response_into(
    socket: &mut impl AsyncReadRent,
    expected: rpc::Expected,
    target: &mut crate::read_buffer::ReadTarget,
) -> Result<Reply, Error> {
    use talon_transport::HEADER_LEN;
    let mut receive = Metadata::receive(target.lease().await);
    let received = loop {
        let (result, returned) = socket.readv(receive).await;
        receive = returned;
        match result {
            Ok(0) => {
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, "response header missing").into(),
                )
            }
            Ok(n) => break n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    };
    let DirectReceive {
        mut metadata,
        _lease: lease,
        ..
    } = receive;
    drop(lease);
    let header_received = received.min(HEADER_LEN);
    if header_received < HEADER_LEN {
        let (result, slice) = socket
            .read_exact(metadata.slice_mut(header_received..HEADER_LEN))
            .await;
        metadata = slice.into_inner();
        result?;
    }
    let header = &metadata.storage.as_ref().unwrap().header;
    let decoded = rpc::response_header(header, expected)?;
    let prefix = received.saturating_sub(HEADER_LEN);
    if decoded.flags.contains(talon_transport::Flags::ERROR) {
        if prefix > decoded.length as usize {
            return Err(
                crate::WorkerError::Encode(talon_transport::DataError::LengthMismatch {
                    declared: decoded.length as usize,
                    actual: prefix,
                })
                .into(),
            );
        }
        // This is a bounded protocol error, not object data. Decode it separately.
        // SAFETY: the completed readv initialized exactly this payload prefix.
        let body = unsafe { target.initialized_prefix(prefix) }.to_vec();
        let body = receive_rest(socket, body, decoded.length as usize).await?;
        return rpc::decode(header.to_vec(), decoded, body, expected);
    }
    if prefix < target.len() {
        // SAFETY: the completed readv initialized the prefix.
        let mut lease = target.lease().await;
        unsafe {
            lease.set_init(prefix);
        }
        let lease = lease.slice_mut(prefix..target.len());
        let (result, lease) = socket.read_exact(lease).await;
        drop(lease);
        result?;
    }
    Ok(Reply::Written(target.len()))
}

async fn receive_rest(
    socket: &mut impl AsyncReadRent,
    mut bytes: Vec<u8>,
    length: usize,
) -> io::Result<Vec<u8>> {
    let received = bytes.len();
    if received < length {
        bytes.reserve(length - received);
        let (result, slice) = socket.read_exact(bytes.slice_mut(received..length)).await;
        result?;
        bytes = slice.into_inner();
    }
    Ok(bytes)
}

impl Runtime for Native {
    type File = monoio::fs::File;
    async fn open(path: &std::path::Path) -> io::Result<(Self::File, u64)> {
        let file = monoio::fs::File::open(path).await?;
        let len = file.metadata().await?.len();
        Ok((file, len))
    }
    async fn read_file(
        file: &mut Self::File,
        mut buffer: Vec<u8>,
        offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        buffer.clear();
        let slice = buffer.slice_mut(..len);
        let (result, slice) = file.read_at(slice, offset).await;
        let n = result?;
        let mut buffer = slice.into_inner();
        buffer.truncate(n);
        Ok(buffer)
    }
    async fn timeout<F: Future>(duration: Duration, future: F) -> io::Result<F::Output> {
        monoio::time::timeout(duration, future)
            .await
            .map_err(|_| crate::pool::timeout_error("RPC", duration))
    }
}

pub(crate) type LocalClient = Rc<MonoioClient>;

impl std::fmt::Debug for MonoioClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonoioClient")
            .field("peers", &self.idle.borrow().len())
            .finish_non_exhaustive()
    }
}
impl rpc::RequestExecutor for MonoioClient {
    async fn execute_range_into(
        &self,
        addr: &str,
        frame: Vec<u8>,
        target: &mut crate::read_buffer::ReadTarget,
    ) -> Result<Reply, Error> {
        // Range reads do not carry upload/file/control state through the hot
        // future. Both directions can be armed on the same ring submission.
        let frame = Bytes::from(frame);
        let expected = rpc::Expected::Range(target.len() as u64);
        for attempt in 0..2 {
            let pooled = if attempt == 0 { self.take(addr) } else { None };
            let reused = pooled.is_some();
            let mut socket = match pooled {
                Some(socket) => socket,
                None => self.connect(addr).await?,
            };
            talon_telemetry::record("talon.pool.reused", reused as u64);
            let result = self
                .deadline(self.config.request, async {
                    let send = async {
                        socket.write.write_all(frame.clone()).await.0?;
                        Ok::<(), Error>(())
                    };
                    let receive = range_response_into(&mut socket.read, expected, target);
                    futures::try_join!(send, receive).map(|(_, reply)| reply)
                })
                .await
                .unwrap_or_else(|e| Err(e.into()));
            target.wait_idle().await;
            match result {
                Ok(reply) => {
                    self.release(addr, socket);
                    return Ok(reply);
                }
                Err(error) if attempt == 0 && reused && error.transport() => {}
                Err(error) => return Err(error),
            }
        }
        unreachable!("at most one retry")
    }
    async fn execute_request(&self, addr: &str, request: Request) -> Result<Reply, Error> {
        let outcome = self.execute(addr, request).await;
        talon_telemetry::record("talon.pool.reused", outcome.reused as u64);
        outcome.result
    }
}

// System DNS is blocking. Two lazily started workers isolate it from both the
// caller and ring; the queue bounds outstanding lookups and cancelled jobs skip
// resolution. Numeric addresses never start these threads.
async fn resolve(addr: &str) -> io::Result<Vec<SocketAddr>> {
    use crate::lock::MutexExt;
    use std::{
        net::ToSocketAddrs,
        sync::{mpsc, Mutex, OnceLock},
    };
    type Lookup = (
        String,
        tokio::sync::oneshot::Sender<io::Result<Vec<SocketAddr>>>,
    );
    static DNS: OnceLock<Result<mpsc::SyncSender<Lookup>, String>> = OnceLock::new();
    let sender = DNS
        .get_or_init(|| {
            let (sender, receiver) = mpsc::sync_channel::<Lookup>(64);
            let receiver = Arc::new(Mutex::new(receiver));
            for index in 0..2 {
                let receiver = receiver.clone();
                std::thread::Builder::new()
                    .name(format!("talon-client-dns-{index}"))
                    .spawn(move || loop {
                        let job = receiver.lock_recover().recv();
                        let Ok((addr, reply)) = job else {
                            break;
                        };
                        if !reply.is_closed() {
                            let _ = reply.send(addr.to_socket_addrs().map(|iter| iter.collect()));
                        }
                    })
                    .map_err(|e| e.to_string())?;
            }
            Ok(sender)
        })
        .as_ref()
        .map_err(|e| io::Error::other(e.clone()))?;
    let (reply, result) = tokio::sync::oneshot::channel();
    sender
        .try_send((addr.to_owned(), reply))
        .map_err(|e| match e {
            mpsc::TrySendError::Full(_) => {
                io::Error::new(io::ErrorKind::WouldBlock, "DNS queue full")
            }
            mpsc::TrySendError::Disconnected(_) => {
                io::Error::new(io::ErrorKind::BrokenPipe, "DNS resolver stopped")
            }
        })?;
    result.await.map_err(io::Error::other)?
}

#[cfg(test)]
#[path = "monoio_receive_tests.rs"]
mod receive_tests;
