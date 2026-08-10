//! C ABI bindings for the Talon client.
//!
//! The exported API is intentionally opaque: C callers receive handles and
//! result objects, while Rust owns the async runtime, placement cache, and
//! callback dispatch machinery.

#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use talon_cache_client::block_reader::FileView;
use talon_cache_client::{BlockReader, CoordinatorClient, PlacementCache};
use talon_core::{ObjectId, Version};

const DEFAULT_BLOCK_SIZE: u32 = 256 << 20;
const PLACEMENT_TTL_MS: u64 = 30_000;
const REPLICAS_K: u8 = 1;

const STATUS_OK: c_int = 0;
const STATUS_INVALID_ARGUMENT: c_int = 1;
const STATUS_RUNTIME_ERROR: c_int = 2;
const STATUS_OPERATION_ERROR: c_int = 4;

const OPERATION_READ: c_int = 1;
const OPERATION_STAT: c_int = 2;

type TalonCallback = unsafe extern "C" fn(*mut TalonResult, *mut c_void);
type TalonTaskFn = unsafe extern "C" fn(*mut c_void);
type TalonExecutorSubmitFn = unsafe extern "C" fn(*mut c_void, TalonTaskFn, *mut c_void);

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Optional callback executor supplied by the caller.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TalonCallbackExecutor {
    /// Opaque context passed to `submit`.
    pub executor_ctx: *mut c_void,
    /// Function that schedules a callback task.
    pub submit: Option<TalonExecutorSubmitFn>,
}

unsafe impl Send for TalonCallbackExecutor {}
unsafe impl Sync for TalonCallbackExecutor {}

/// Client construction options.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TalonClientOptions {
    /// Logical block size. Zero means the SDK default.
    pub block_size: u32,
    /// Optional caller-owned callback executor. Without one, callbacks run on
    /// the Tokio runtime thread that completed the operation.
    pub callback_executor: *const TalonCallbackExecutor,
}

/// Opaque client handle.
pub struct TalonClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    runtime: Arc<tokio::runtime::Runtime>,
    coordinator: CoordinatorClient,
    reader: BlockReader,
    block_size: u32,
    dispatcher: Arc<CallbackDispatcher>,
    next_request_id: AtomicU64,
}

/// Opaque operation result.
pub struct TalonResult {
    operation: c_int,
    status: c_int,
    request_id: u64,
    bytes_written: usize,
    object_size: u64,
    version: Option<CString>,
    error: Option<CString>,
}

struct ReadBuffer {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for ReadBuffer {}

impl ReadBuffer {
    unsafe fn into_mut_slice(self) -> &'static mut [u8] {
        std::slice::from_raw_parts_mut(self.ptr, self.len)
    }
}

#[derive(Clone, Copy)]
struct UserData(*mut c_void);

unsafe impl Send for UserData {}
unsafe impl Sync for UserData {}

struct CallbackTask {
    callback: TalonCallback,
    result: *mut TalonResult,
    user_data: UserData,
}

unsafe impl Send for CallbackTask {}

impl CallbackTask {
    fn run(self) {
        unsafe {
            (self.callback)(self.result, self.user_data.0);
        }
    }
}

enum CallbackDispatcher {
    Inline,
    Custom(TalonCallbackExecutor),
}

unsafe impl Send for CallbackDispatcher {}
unsafe impl Sync for CallbackDispatcher {}

impl CallbackDispatcher {
    fn new_custom(executor: TalonCallbackExecutor) -> Self {
        Self::Custom(executor)
    }

    fn dispatch(&self, task: CallbackTask) -> Result<(), CallbackTask> {
        match self {
            CallbackDispatcher::Inline => {
                task.run();
                Ok(())
            }
            CallbackDispatcher::Custom(executor) => {
                let Some(submit) = executor.submit else {
                    return Err(task);
                };
                let task_ctx = Box::into_raw(Box::new(task)).cast::<c_void>();
                unsafe {
                    submit(executor.executor_ctx, run_callback_task, task_ctx);
                }
                Ok(())
            }
        }
    }
}

unsafe extern "C" fn run_callback_task(task_ctx: *mut c_void) {
    if task_ctx.is_null() {
        return;
    }
    let task = unsafe { Box::from_raw(task_ctx.cast::<CallbackTask>()) };
    task.run();
}

/// Initialize client options with SDK defaults.
#[no_mangle]
pub unsafe extern "C" fn talon_client_options_init(options: *mut TalonClientOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        ptr::write(
            options,
            TalonClientOptions {
                block_size: DEFAULT_BLOCK_SIZE,
                callback_executor: ptr::null(),
            },
        );
    }
}

/// Create a Talon client.
#[no_mangle]
pub unsafe extern "C" fn talon_client_new(
    coordinator_addr: *const c_char,
    options: *const TalonClientOptions,
    out: *mut *mut TalonClient,
) -> c_int {
    ffi_status(|| {
        if coordinator_addr.is_null() {
            return Err((STATUS_INVALID_ARGUMENT, "coordinator_addr is null".into()));
        }
        if out.is_null() {
            return Err((STATUS_INVALID_ARGUMENT, "out is null".into()));
        }

        let coordinator_addr = c_string(coordinator_addr, "coordinator_addr")?;
        let options = if options.is_null() {
            TalonClientOptions {
                block_size: DEFAULT_BLOCK_SIZE,
                callback_executor: ptr::null(),
            }
        } else {
            unsafe { *options }
        };
        let block_size = if options.block_size == 0 {
            DEFAULT_BLOCK_SIZE
        } else {
            options.block_size
        };
        let dispatcher = if options.callback_executor.is_null() {
            CallbackDispatcher::Inline
        } else {
            let executor = unsafe { *options.callback_executor };
            if executor.submit.is_none() {
                return Err((
                    STATUS_INVALID_ARGUMENT,
                    "callback executor submit is null".into(),
                ));
            }
            CallbackDispatcher::new_custom(executor)
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|error| (STATUS_RUNTIME_ERROR, error.to_string()))?;
        let coordinator = CoordinatorClient::new(coordinator_addr);
        let cache = Arc::new(PlacementCache::new(PLACEMENT_TTL_MS));
        let reader = BlockReader::new(coordinator.clone(), cache, REPLICAS_K);
        let client = Box::new(TalonClient {
            inner: Arc::new(ClientInner {
                runtime: Arc::new(runtime),
                coordinator,
                reader,
                block_size,
                dispatcher: Arc::new(dispatcher),
                next_request_id: AtomicU64::new(1),
            }),
        });
        unsafe {
            *out = Box::into_raw(client);
        }
        Ok(())
    })
}

/// Free a Talon client handle.
#[no_mangle]
pub unsafe extern "C" fn talon_client_free(client: *mut TalonClient) {
    if client.is_null() {
        return;
    }
    let client = unsafe { Box::from_raw(client) };
    if tokio::runtime::Handle::try_current().is_ok() {
        let _ = std::thread::spawn(move || drop(client)).join();
    } else {
        drop(client);
    }
}

/// Submit an async read.
#[no_mangle]
pub unsafe extern "C" fn talon_read_async(
    client: *mut TalonClient,
    uri: *const c_char,
    offset: u64,
    dst: *mut u8,
    dst_len: usize,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    ffi_status(|| {
        let inner = client_inner(client)?;
        if uri.is_null() {
            return Err((STATUS_INVALID_ARGUMENT, "uri is null".into()));
        }
        if dst_len > 0 && dst.is_null() {
            return Err((STATUS_INVALID_ARGUMENT, "dst is null".into()));
        }
        if dst_len > isize::MAX as usize {
            return Err((STATUS_INVALID_ARGUMENT, "dst_len exceeds isize::MAX".into()));
        }
        let Some(callback) = callback else {
            return Err((STATUS_INVALID_ARGUMENT, "callback is null".into()));
        };
        if request_id_out.is_null() {
            return Err((STATUS_INVALID_ARGUMENT, "request_id_out is null".into()));
        }

        let uri = c_string(uri, "uri")?;
        let object = parse_uri(&uri)?;
        let request_id = inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        unsafe {
            *request_id_out = request_id;
        }

        let read_buffer = ReadBuffer {
            ptr: dst,
            len: dst_len,
        };
        let user_data = UserData(user_data);
        let runtime = Arc::clone(&inner.runtime);
        let coordinator = inner.coordinator.clone();
        let reader = inner.reader.clone();
        let dispatcher = Arc::clone(&inner.dispatcher);
        let block_size = inner.block_size;
        runtime.spawn(async move {
            let result = async {
                if read_buffer.len == 0 {
                    return Ok(0);
                }
                let stat = coordinator
                    .stat_object(&object)
                    .await
                    .map_err(|error| error.to_string())?;
                let version = Version::new(stat.version.as_str());
                let file = FileView {
                    object: &object,
                    block_size,
                    version: &version,
                    size: stat.size,
                };
                let dst = unsafe { read_buffer.into_mut_slice() };
                reader
                    .read_into(&file, offset, dst, now_ms())
                    .await
                    .map_err(|error| error.to_string())
            }
            .await;
            dispatch_result(
                dispatcher,
                callback,
                user_data,
                TalonResult::read(request_id, result),
            );
        });
        Ok(())
    })
}

/// Submit an async stat.
#[no_mangle]
pub unsafe extern "C" fn talon_stat_async(
    client: *mut TalonClient,
    uri: *const c_char,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    ffi_status(|| {
        let inner = client_inner(client)?;
        if uri.is_null() {
            return Err((STATUS_INVALID_ARGUMENT, "uri is null".into()));
        }
        let Some(callback) = callback else {
            return Err((STATUS_INVALID_ARGUMENT, "callback is null".into()));
        };
        if request_id_out.is_null() {
            return Err((STATUS_INVALID_ARGUMENT, "request_id_out is null".into()));
        }

        let uri = c_string(uri, "uri")?;
        let object = parse_uri(&uri)?;
        let request_id = inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        unsafe {
            *request_id_out = request_id;
        }

        let user_data = UserData(user_data);
        let runtime = Arc::clone(&inner.runtime);
        let coordinator = inner.coordinator.clone();
        let dispatcher = Arc::clone(&inner.dispatcher);
        runtime.spawn(async move {
            let result = coordinator
                .stat_object(&object)
                .await
                .map_err(|error| error.to_string());
            dispatch_result(
                dispatcher,
                callback,
                user_data,
                TalonResult::stat(request_id, result),
            );
        });
        Ok(())
    })
}

/// Return the operation status.
#[no_mangle]
pub unsafe extern "C" fn talon_result_status(result: *const TalonResult) -> c_int {
    unsafe {
        result
            .as_ref()
            .map(|r| r.status)
            .unwrap_or(STATUS_INVALID_ARGUMENT)
    }
}

/// Return the operation kind.
#[no_mangle]
pub unsafe extern "C" fn talon_result_operation(result: *const TalonResult) -> c_int {
    unsafe { result.as_ref().map(|r| r.operation).unwrap_or(0) }
}

/// Return the SDK request id.
#[no_mangle]
pub unsafe extern "C" fn talon_result_request_id(result: *const TalonResult) -> u64 {
    unsafe { result.as_ref().map(|r| r.request_id).unwrap_or(0) }
}

/// Return bytes written for a read result.
#[no_mangle]
pub unsafe extern "C" fn talon_result_bytes_written(result: *const TalonResult) -> usize {
    unsafe { result.as_ref().map(|r| r.bytes_written).unwrap_or(0) }
}

/// Return object size for a stat result.
#[no_mangle]
pub unsafe extern "C" fn talon_result_object_size(result: *const TalonResult) -> u64 {
    unsafe { result.as_ref().map(|r| r.object_size).unwrap_or(0) }
}

/// Return object version for a stat result, or null.
#[no_mangle]
pub unsafe extern "C" fn talon_result_version(result: *const TalonResult) -> *const c_char {
    unsafe {
        result
            .as_ref()
            .and_then(|r| r.version.as_ref())
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null())
    }
}

/// Return operation error text, or null.
#[no_mangle]
pub unsafe extern "C" fn talon_result_error(result: *const TalonResult) -> *const c_char {
    unsafe {
        result
            .as_ref()
            .and_then(|r| r.error.as_ref())
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null())
    }
}

/// Free an operation result.
#[no_mangle]
pub unsafe extern "C" fn talon_result_free(result: *mut TalonResult) {
    if result.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(result));
    }
}

/// Return the last synchronous FFI error for the current thread.
#[no_mangle]
pub unsafe extern "C" fn talon_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|error| error.as_ptr())
            .unwrap_or(ptr::null())
    })
}

impl TalonResult {
    fn read(request_id: u64, result: Result<usize, String>) -> Self {
        match result {
            Ok(bytes_written) => Self {
                operation: OPERATION_READ,
                status: STATUS_OK,
                request_id,
                bytes_written,
                object_size: 0,
                version: None,
                error: None,
            },
            Err(error) => Self::operation_error(OPERATION_READ, request_id, error),
        }
    }

    fn stat(
        request_id: u64,
        result: Result<talon_cache_client::coordinator_client::ObjectStat, String>,
    ) -> Self {
        match result {
            Ok(stat) => Self {
                operation: OPERATION_STAT,
                status: STATUS_OK,
                request_id,
                bytes_written: 0,
                object_size: stat.size,
                version: Some(cstring_lossy(stat.version)),
                error: None,
            },
            Err(error) => Self::operation_error(OPERATION_STAT, request_id, error),
        }
    }

    fn operation_error(operation: c_int, request_id: u64, error: String) -> Self {
        Self {
            operation,
            status: STATUS_OPERATION_ERROR,
            request_id,
            bytes_written: 0,
            object_size: 0,
            version: None,
            error: Some(cstring_lossy(error)),
        }
    }
}

fn dispatch_result(
    dispatcher: Arc<CallbackDispatcher>,
    callback: TalonCallback,
    user_data: UserData,
    result: TalonResult,
) {
    let result = Box::into_raw(Box::new(result));
    let task = CallbackTask {
        callback,
        result,
        user_data,
    };
    if let Err(task) = dispatcher.dispatch(task) {
        unsafe {
            drop(Box::from_raw(task.result));
        }
    }
}

fn client_inner(client: *mut TalonClient) -> Result<Arc<ClientInner>, (c_int, String)> {
    if client.is_null() {
        return Err((STATUS_INVALID_ARGUMENT, "client is null".into()));
    }
    let client = unsafe { &*client };
    Ok(Arc::clone(&client.inner))
}

fn c_string(ptr: *const c_char, name: &str) -> Result<String, (c_int, String)> {
    let s = unsafe { CStr::from_ptr(ptr) }.to_str().map_err(|error| {
        (
            STATUS_INVALID_ARGUMENT,
            format!("{name} is not UTF-8: {error}"),
        )
    })?;
    Ok(s.to_string())
}

fn parse_uri(uri: &str) -> Result<ObjectId, (c_int, String)> {
    let (scheme, rest) = uri.split_once("://").ok_or_else(|| {
        (
            STATUS_INVALID_ARGUMENT,
            format!("expected a scheme://bucket/key URI, got {uri:?} (schemes: s3, gcs, az)"),
        )
    })?;
    let backend = scheme.parse().map_err(|_| {
        (
            STATUS_INVALID_ARGUMENT,
            format!("unknown backend scheme {scheme:?}"),
        )
    })?;
    let (bucket, key) = rest.split_once('/').ok_or_else(|| {
        (
            STATUS_INVALID_ARGUMENT,
            format!("URI is missing an object key: {uri:?}"),
        )
    })?;
    if bucket.is_empty() {
        return Err((
            STATUS_INVALID_ARGUMENT,
            format!("URI has an empty bucket: {uri:?}"),
        ));
    }
    if key.is_empty() {
        return Err((
            STATUS_INVALID_ARGUMENT,
            format!("URI has an empty object key: {uri:?}"),
        ));
    }
    Ok(ObjectId::new(backend, bucket, key))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn ffi_status<F>(f: F) -> c_int
where
    F: FnOnce() -> Result<(), (c_int, String)>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(())) => {
            clear_last_error();
            STATUS_OK
        }
        Ok(Err((status, error))) => {
            set_last_error(error);
            status
        }
        Err(_) => {
            set_last_error("panic across FFI boundary".into());
            STATUS_RUNTIME_ERROR
        }
    }
}

fn set_last_error(error: String) {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(cstring_lossy(error));
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn cstring_lossy(value: String) -> CString {
    let bytes: Vec<u8> = value.into_bytes().into_iter().filter(|b| *b != 0).collect();
    CString::new(bytes).unwrap_or_else(|_| CString::new("invalid string").unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    use talon_core::{NodeId, NodeInfo, NodeRole};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use talon_transport::{decode_request, response_header_ok, ControlMessage, RangeRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    unsafe extern "C" {
        fn talon_c_api_smoke_test() -> c_int;
    }

    struct CallbackState {
        done: Mutex<Option<CallbackSnapshot>>,
        ready: Condvar,
    }

    #[derive(Debug)]
    struct CallbackSnapshot {
        operation: c_int,
        status: c_int,
        request_id: u64,
        bytes_written: usize,
        object_size: u64,
        version: Option<String>,
        error: Option<String>,
    }

    impl CallbackState {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                done: Mutex::new(None),
                ready: Condvar::new(),
            })
        }

        fn user_data(self: &Arc<Self>) -> *mut c_void {
            Arc::into_raw(Arc::clone(self)).cast_mut().cast::<c_void>()
        }

        fn wait(&self) -> CallbackSnapshot {
            let mut guard = self.done.lock().unwrap();
            loop {
                if let Some(snapshot) = guard.take() {
                    return snapshot;
                }
                let (next, timeout) = self
                    .ready
                    .wait_timeout(guard, Duration::from_secs(5))
                    .unwrap();
                assert!(!timeout.timed_out(), "callback did not fire");
                guard = next;
            }
        }
    }

    unsafe extern "C" fn capture_callback(result: *mut TalonResult, user_data: *mut c_void) {
        let state = unsafe { Arc::from_raw(user_data.cast::<CallbackState>()) };
        let version = unsafe { talon_result_version(result) };
        let version = if version.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(version) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        let error = unsafe { talon_result_error(result) };
        let error = if error.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(error) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        let snapshot = CallbackSnapshot {
            operation: unsafe { talon_result_operation(result) },
            status: unsafe { talon_result_status(result) },
            request_id: unsafe { talon_result_request_id(result) },
            bytes_written: unsafe { talon_result_bytes_written(result) },
            object_size: unsafe { talon_result_object_size(result) },
            version,
            error,
        };
        unsafe {
            talon_result_free(result);
        }
        let mut guard = state.done.lock().unwrap();
        *guard = Some(snapshot);
        state.ready.notify_one();
    }

    unsafe extern "C" fn capture_callback_thread(result: *mut TalonResult, user_data: *mut c_void) {
        let callback_thread =
            unsafe { Arc::from_raw(user_data.cast::<Mutex<Option<std::thread::ThreadId>>>()) };
        *callback_thread.lock().unwrap() = Some(std::thread::current().id());
        unsafe {
            talon_result_free(result);
        }
    }

    async fn mock_worker() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if sock.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; header.length as usize];
                    sock.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_header, req): (_, RangeRequest) = decode_request(&full).unwrap();
                    let payload: Vec<u8> = (0..req.len)
                        .map(|i| ((req.offset + i) % 251) as u8)
                        .collect();
                    let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                    out.extend_from_slice(&payload);
                    sock.write_all(&out).await.unwrap();
                    sock.flush().await.unwrap();
                });
            }
        });
        addr
    }

    async fn mock_coordinator(worker_addr: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let worker_addr = worker_addr.clone();
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if sock.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; header.length as usize];
                    sock.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_header, msg) = talon_transport::decode(&full).unwrap();
                    let reply = match msg {
                        ControlMessage::StatObject { .. } => ControlMessage::ObjectStat {
                            size: 8192,
                            version: "test-version".into(),
                        },
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![NodeInfo {
                                id: NodeId::new("worker-a"),
                                address: worker_addr,
                                role: NodeRole::Worker,
                            }],
                        },
                        _ => ControlMessage::Ack {
                            ok: false,
                            detail: None,
                        },
                    };
                    sock.write_all(&talon_transport::encode(0, &reply).unwrap())
                        .await
                        .unwrap();
                    sock.flush().await.unwrap();
                });
            }
        });
        addr
    }

    fn cstring(value: &str) -> CString {
        CString::new(value).unwrap()
    }

    async fn new_client() -> (*mut TalonClient, String) {
        let worker = mock_worker().await;
        let coordinator = mock_coordinator(worker).await;
        let coordinator_c = cstring(&coordinator);
        let mut options = TalonClientOptions {
            block_size: 0,
            callback_executor: ptr::null(),
        };
        unsafe {
            talon_client_options_init(&mut options);
        }
        let mut client = ptr::null_mut();
        let status = unsafe { talon_client_new(coordinator_c.as_ptr(), &options, &mut client) };
        assert_eq!(status, STATUS_OK);
        assert!(!client.is_null());
        (client, coordinator)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_create_and_free_with_default_options() {
        let (client, _coordinator) = new_client().await;
        unsafe {
            talon_client_free(client);
        }
    }

    #[test]
    fn inline_dispatch_result_runs_callback_synchronously_on_submitting_thread() {
        let callback_thread = Arc::new(Mutex::new(None));
        let expected_thread = std::thread::current().id();
        dispatch_result(
            Arc::new(CallbackDispatcher::Inline),
            capture_callback_thread,
            UserData(
                Arc::into_raw(Arc::clone(&callback_thread))
                    .cast_mut()
                    .cast::<c_void>(),
            ),
            TalonResult::read(1, Ok(0)),
        );

        assert_eq!(*callback_thread.lock().unwrap(), Some(expected_thread));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_read_invokes_inline_callback_and_fills_buffer() {
        let (client, _coordinator) = new_client().await;
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let mut dst = vec![0u8; 4096];
        let mut request_id = 0u64;

        let status = unsafe {
            talon_read_async(
                client,
                uri.as_ptr(),
                100,
                dst.as_mut_ptr(),
                dst.len(),
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.operation, OPERATION_READ);
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(snapshot.request_id, request_id);
        assert_eq!(snapshot.bytes_written, dst.len());
        assert_eq!(dst[0], 100);
        assert_eq!(dst[1], 101);
        assert!(snapshot.error.is_none());

        unsafe {
            talon_client_free(client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_length_read_accepts_null_buffer() {
        let (client, _coordinator) = new_client().await;
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let mut request_id = 0u64;

        let status = unsafe {
            talon_read_async(
                client,
                uri.as_ptr(),
                100,
                ptr::null_mut(),
                0,
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.operation, OPERATION_READ);
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(snapshot.request_id, request_id);
        assert_eq!(snapshot.bytes_written, 0);
        assert!(snapshot.error.is_none());

        unsafe {
            talon_client_free(client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_stat_invokes_inline_callback() {
        let (client, _coordinator) = new_client().await;
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let mut request_id = 0u64;

        let status = unsafe {
            talon_stat_async(
                client,
                uri.as_ptr(),
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.operation, OPERATION_STAT);
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(snapshot.request_id, request_id);
        assert_eq!(snapshot.object_size, 8192);
        assert_eq!(snapshot.version.as_deref(), Some("test-version"));

        unsafe {
            talon_client_free(client);
        }
    }

    unsafe extern "C" fn custom_submit(
        executor_ctx: *mut c_void,
        run: TalonTaskFn,
        task_ctx: *mut c_void,
    ) {
        let calls = unsafe { &*(executor_ctx.cast::<AtomicUsize>()) };
        calls.fetch_add(1, Ordering::SeqCst);
        let task_ctx = task_ctx as usize;
        std::thread::spawn(move || unsafe {
            run(task_ctx as *mut c_void);
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_scheduler_hook_receives_callback_job() {
        let worker = mock_worker().await;
        let coordinator = mock_coordinator(worker).await;
        let coordinator_c = cstring(&coordinator);
        let calls = AtomicUsize::new(0);
        let executor = TalonCallbackExecutor {
            executor_ctx: (&calls as *const AtomicUsize).cast_mut().cast::<c_void>(),
            submit: Some(custom_submit),
        };
        let options = TalonClientOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            callback_executor: &executor,
        };
        let mut client = ptr::null_mut();
        let status = unsafe { talon_client_new(coordinator_c.as_ptr(), &options, &mut client) };
        assert_eq!(status, STATUS_OK);

        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let mut request_id = 0u64;
        let status = unsafe {
            talon_stat_async(
                client,
                uri.as_ptr(),
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        unsafe {
            talon_client_free(client);
        }
    }

    #[test]
    fn invalid_arguments_fail_synchronously() {
        let mut request_id = 0u64;
        let uri = cstring("s3://bucket/object.bin");
        let status = unsafe {
            talon_read_async(
                ptr::null_mut(),
                uri.as_ptr(),
                0,
                ptr::null_mut(),
                0,
                Some(capture_callback),
                ptr::null_mut(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_INVALID_ARGUMENT);
        let msg = unsafe { CStr::from_ptr(talon_last_error()) }.to_string_lossy();
        assert!(msg.contains("client is null"));
    }

    #[test]
    fn c_header_and_abi_smoke_test() {
        let status = unsafe { talon_c_api_smoke_test() };
        assert_eq!(status, 0, "C API smoke test failed at check {status}");
    }
}
