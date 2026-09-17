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

use talon_rust_client::{
    parse_uri as parse_rust_uri, ClientBuilder, Error as RustError, HostedClient as RustClient,
    ObjectId, ObjectStat as RustObjectStat, UriError,
};

mod trace_options;
pub use trace_options::{talon_request_options_init, TalonRequestOptions};

#[cfg(feature = "telemetry")]
static TELEMETRY: std::sync::Mutex<
    Option<(talon_telemetry::export::ExportOwner, tracing::Dispatch)>,
> = std::sync::Mutex::new(None);

/// Explicit process initialization. Subscriber remains scoped to Talon tasks.
#[no_mangle]
pub extern "C" fn talon_telemetry_init() -> c_int {
    ffi_status(|| {
        #[cfg(feature = "telemetry")]
        {
            let mut session = TELEMETRY.lock().unwrap();
            if session.is_some() {
                return Err((
                    STATUS_INVALID_ARGUMENT,
                    "telemetry already initialized".into(),
                ));
            }
            *session = Some(
                talon_telemetry::export::init_scoped("talon-c")
                    .map_err(|e| (STATUS_RUNTIME_ERROR, e.to_string()))?,
            );
            Ok(())
        }
        #[cfg(not(feature = "telemetry"))]
        {
            talon_telemetry::Config::from_env()
                .and_then(talon_telemetry::configure)
                .map_err(|e| (STATUS_INVALID_ARGUMENT, e))
        }
    })
}

/// Call after all client operations finish, outside SDK callbacks.
#[no_mangle]
pub extern "C" fn talon_telemetry_shutdown() {
    #[cfg(feature = "telemetry")]
    if let Some((owner, _)) = TELEMETRY.lock().unwrap().take() {
        owner.shutdown();
    }
}

fn telemetry_dispatch() -> tracing::Dispatch {
    #[cfg(feature = "telemetry")]
    if let Some((_, dispatch)) = TELEMETRY.lock().unwrap().as_ref() {
        return dispatch.clone();
    }
    tracing::dispatcher::get_default(Clone::clone)
}

fn with_telemetry<T>(f: impl FnOnce() -> T) -> T {
    let dispatch = telemetry_dispatch();
    if dispatch.is::<tracing::subscriber::NoSubscriber>()
        && tracing::dispatcher::get_default(|current| {
            current.is::<tracing::subscriber::NoSubscriber>()
        })
    {
        f()
    } else {
        tracing::dispatcher::with_default(&dispatch, f)
    }
}

const DEFAULT_BLOCK_SIZE: u32 = 256 << 20;
const DEFAULT_MAX_IDLE_PER_ADDR: u32 = 8;

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
    /// the SDK operation thread (inline).
    pub callback_executor: *const TalonCallbackExecutor,
    /// Maximum idle connections per peer in each pool. Zero uses the default 8.
    /// Does not limit active connections.
    pub max_idle_per_addr: u32,
}

/// Opaque client handle.
pub struct TalonClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    client: Arc<RustClient>,
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

// SAFETY: the unsafe C API requires exclusive, valid writable storage through
// callback completion. The private wrapper is created after pointer validation.
unsafe impl talon_rust_client::ReadDestination for ReadBuffer {
    fn raw_parts(&mut self) -> (*mut u8, usize) {
        let ptr = if self.len == 0 {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            self.ptr
        };
        (ptr, self.len)
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
                max_idle_per_addr: DEFAULT_MAX_IDLE_PER_ADDR,
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
                max_idle_per_addr: DEFAULT_MAX_IDLE_PER_ADDR,
            }
        } else {
            unsafe { *options }
        };
        let block_size = if options.block_size == 0 {
            DEFAULT_BLOCK_SIZE
        } else {
            options.block_size
        };
        let max_idle_per_addr = if options.max_idle_per_addr == 0 {
            DEFAULT_MAX_IDLE_PER_ADDR
        } else {
            options.max_idle_per_addr
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

        let rust_client = ClientBuilder::default()
            .with_coordinator(coordinator_addr)
            .with_block_size(block_size)
            .with_max_idle_per_addr(max_idle_per_addr as usize)
            .build_hosted()
            .map_err(|error| match error {
                RustError::InvalidArgument(_) | RustError::InvalidUri(_) => {
                    (STATUS_INVALID_ARGUMENT, error.to_string())
                }
                _ => (STATUS_RUNTIME_ERROR, error.to_string()),
            })?;
        let client = Box::new(TalonClient {
            inner: Arc::new(ClientInner {
                client: Arc::new(rust_client),
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
    // Runtime teardown happens on the SDK thread, including when freed in a callback.
    drop(unsafe { Box::from_raw(client) });
}

/// Submit an async read.
///
/// `version` and `object_size` are each optional and independently nullable;
/// NULL means "the caller does not have this value". The read takes the fast
/// path that skips the `StatObject` round trip **only when both are non-NULL** —
/// then it reads under the caller-supplied size and exact source version. A
/// worker may serve that generation from cache or conditionally fetch it from
/// the backend, but never substitutes newer bytes. If either is NULL
/// the SDK resolves both with a `StatObject` first (the historical behavior) and
/// any lone value that was supplied is ignored, since a stat is authoritative
/// for both and a read cannot skip it without both halves.
///
/// When taken, `*object_size` is the object's total byte length and bounds the
/// read at EOF (a POSIX short read): a value smaller than `offset + dst_len`
/// yields a short read, `0` denotes a genuinely empty object (an unambiguous
/// zero-byte read), and a value larger than the object surfaces as a read error
/// rather than fabricated bytes. A caller that has no valid size passes NULL.
///
/// Blocks spanned by the read are fetched concurrently.
#[no_mangle]
pub unsafe extern "C" fn talon_read_async(
    client: *mut TalonClient,
    uri: *const c_char,
    offset: u64,
    dst: *mut u8,
    dst_len: usize,
    version: *const c_char,
    object_size: *const u64,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    unsafe {
        talon_read_async_with_options(
            client,
            uri,
            offset,
            dst,
            dst_len,
            version,
            object_size,
            ptr::null(),
            callback,
            user_data,
            request_id_out,
        )
    }
}

/// Submit with copied request-local W3C context.
#[no_mangle]
pub unsafe extern "C" fn talon_read_async_with_options(
    client: *mut TalonClient,
    uri: *const c_char,
    offset: u64,
    dst: *mut u8,
    dst_len: usize,
    version: *const c_char,
    object_size: *const u64,
    options: *const TalonRequestOptions,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    ffi_status(|| {
        let trace_context = unsafe { trace_options::copy_options(options) }?;
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
        // A read skips StatObject only when the caller supplies both halves of
        // the object's identity; a NULL in either means "resolve it".
        let known_version = if version.is_null() {
            None
        } else {
            Some(c_string(version, "version")?)
        };
        let known_size = if object_size.is_null() {
            None
        } else {
            Some(unsafe { *object_size })
        };
        let request_id = inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        unsafe {
            *request_id_out = request_id;
        }

        let read_buffer = ReadBuffer {
            ptr: dst,
            len: dst_len,
        };
        let user_data = UserData(user_data);
        let client = Arc::clone(&inner.client);
        let dispatcher = Arc::clone(&inner.dispatcher);
        let known_stat = match (known_version, known_size) {
            (Some(version), Some(size)) => Some(RustObjectStat { size, version }),
            _ => None,
        };
        let options = talon_rust_client::RequestOptions {
            parent: trace_context
                .as_ref()
                .map(talon_rust_client::TraceParent::Explicit)
                .unwrap_or(talon_rust_client::TraceParent::Root),
        };
        // Receive directly into the caller destination on the owning runtime thread. The
        // existing dispatcher invokes inline or calls the user's executor hook.
        with_telemetry(|| {
            client.read_into_owned_with_callback(
                object,
                offset,
                read_buffer,
                known_stat,
                &options,
                move |result| {
                    let result = result.map_err(|error| error.to_string());
                    dispatch_result(
                        dispatcher,
                        callback,
                        user_data,
                        TalonResult::read(request_id, result),
                    );
                },
            )
        })
        .map_err(|error| (STATUS_RUNTIME_ERROR, error.to_string()))?;
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
    unsafe {
        talon_stat_async_with_options(
            client,
            uri,
            ptr::null(),
            callback,
            user_data,
            request_id_out,
        )
    }
}

/// Submit with copied request-local W3C context.
#[no_mangle]
pub unsafe extern "C" fn talon_stat_async_with_options(
    client: *mut TalonClient,
    uri: *const c_char,
    options: *const TalonRequestOptions,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    ffi_status(|| {
        let trace_context = unsafe { trace_options::copy_options(options) }?;
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
        let client = Arc::clone(&inner.client);
        let dispatcher = Arc::clone(&inner.dispatcher);
        let options = talon_rust_client::RequestOptions {
            parent: trace_context
                .as_ref()
                .map(talon_rust_client::TraceParent::Explicit)
                .unwrap_or(talon_rust_client::TraceParent::Root),
        };
        with_telemetry(|| {
            client.stat_with_callback(&object, &options, move |result| {
                dispatch_result(
                    dispatcher,
                    callback,
                    user_data,
                    TalonResult::stat(request_id, result.map_err(|error| error.to_string())),
                );
            })
        })
        .map_err(|error| (STATUS_RUNTIME_ERROR, error.to_string()))?;
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

    fn stat(request_id: u64, result: Result<RustObjectStat, String>) -> Self {
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
    parse_rust_uri(uri).map_err(|error| {
        let message = match error {
            RustError::InvalidUri(UriError::UnknownScheme { scheme }) => {
                format!("unknown backend scheme {scheme:?}")
            }
            RustError::InvalidUri(UriError::MissingKey { uri, .. }) => {
                format!("URI is missing an object key: {uri:?}")
            }
            error => error.to_string(),
        };
        (STATUS_INVALID_ARGUMENT, message)
    })
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
    use talon_transport::{decode_versioned_request, response_header_ok, ControlMessage};
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
        thread: Option<String>,
        in_tokio: bool,
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
            thread: std::thread::current().name().map(str::to_owned),
            in_tokio: tokio::runtime::Handle::try_current().is_ok(),
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

    async fn mock_worker(observed_versions: Option<Arc<Mutex<Vec<String>>>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let observed_versions = observed_versions.clone();
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
                    let (_header, versioned) = decode_versioned_request(&full).unwrap();
                    if let Some(observed_versions) = observed_versions {
                        observed_versions
                            .lock()
                            .unwrap()
                            .push(versioned.version.as_str().to_owned());
                    }
                    let req = versioned.request;
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
        mock_coordinator_counting(worker_addr, Arc::new(AtomicUsize::new(0))).await
    }

    /// Like [`mock_coordinator`], but records how many `StatObject` control
    /// messages it served, so a test can prove the version fast path skips them.
    async fn mock_coordinator_counting(
        worker_addr: String,
        stat_calls: Arc<AtomicUsize>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let worker_addr = worker_addr.clone();
                let stat_calls = Arc::clone(&stat_calls);
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
                        ControlMessage::StatObject { .. } => {
                            stat_calls.fetch_add(1, Ordering::SeqCst);
                            ControlMessage::ObjectStat {
                                size: 8192,
                                version: "test-version".into(),
                            }
                        }
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![NodeInfo {
                                id: NodeId::new("worker-a"),
                                address: worker_addr,
                                role: NodeRole::Worker,
                            }],
                        },
                        ControlMessage::MembershipQueryV2 {} => ControlMessage::MembershipListV2 {
                            nodes: vec![talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("worker-a"),
                                    address: worker_addr,
                                    role: NodeRole::Worker,
                                },
                                zone: None,
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
        let worker = mock_worker(None).await;
        let coordinator = mock_coordinator(worker).await;
        let coordinator_c = cstring(&coordinator);
        let mut options = TalonClientOptions {
            block_size: 0,
            callback_executor: ptr::null(),
            max_idle_per_addr: 0,
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

    /// Build a client with a caller-chosen block size whose coordinator counts
    /// the `StatObject` calls it serves.
    async fn new_client_counting(
        block_size: u32,
    ) -> (*mut TalonClient, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
        let observed_versions = Arc::new(Mutex::new(Vec::new()));
        let worker = mock_worker(Some(Arc::clone(&observed_versions))).await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_coordinator_counting(worker, Arc::clone(&stat_calls)).await;
        let coordinator_c = cstring(&coordinator);
        let options = TalonClientOptions {
            block_size,
            callback_executor: ptr::null(),
            max_idle_per_addr: 0,
        };
        let mut client = ptr::null_mut();
        let status = unsafe { talon_client_new(coordinator_c.as_ptr(), &options, &mut client) };
        assert_eq!(status, STATUS_OK);
        assert!(!client.is_null());
        (client, stat_calls, observed_versions)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_create_and_free_with_default_options() {
        let (client, _coordinator) = new_client().await;
        unsafe {
            talon_client_free(client);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires Linux io_uring; run explicitly with --ignored"]
    fn native_c_entry_and_inline_callback_without_tokio() {
        let coordinator = cstring("unused:1");
        let mut client = ptr::null_mut();
        assert_eq!(
            unsafe { talon_client_new(coordinator.as_ptr(), ptr::null(), &mut client) },
            STATUS_OK
        );
        assert_eq!(
            unsafe { &*client }.inner.client.io_backend(),
            talon_rust_client::ClientIoBackend::IoUring
        );
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/key");
        let mut request_id = 0;
        assert_eq!(
            unsafe {
                talon_read_async(
                    client,
                    uri.as_ptr(),
                    0,
                    ptr::null_mut(),
                    0,
                    ptr::null(),
                    ptr::null(),
                    Some(capture_callback),
                    state.user_data(),
                    &mut request_id,
                )
            },
            STATUS_OK
        );
        let snapshot = state.wait();
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(snapshot.bytes_written, 0);
        assert!(snapshot
            .thread
            .as_deref()
            .unwrap()
            .starts_with("talon-sdk-"));
        assert!(!snapshot.in_tokio);
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
                ptr::null(),
                ptr::null(),
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert!(snapshot
            .thread
            .as_deref()
            .unwrap()
            .starts_with("talon-sdk-"));
        let backend = unsafe { &*client }.inner.client.io_backend();
        assert_eq!(
            snapshot.in_tokio,
            backend == talon_rust_client::ClientIoBackend::Tokio
        );
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
                ptr::null(),
                ptr::null(),
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
    async fn read_with_exact_version_and_size_skips_stat_and_clamps() {
        let (client, stat_calls, observed_versions) = new_client_counting(0).await;
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let version = cstring("caller-known-version");
        let object_size: u64 = 512;
        let mut dst = vec![0u8; 4096];
        let mut request_id = 0u64;

        let status = unsafe {
            talon_read_async(
                client,
                uri.as_ptr(),
                100,
                dst.as_mut_ptr(),
                dst.len(),
                version.as_ptr(),
                &object_size,
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(snapshot.bytes_written, 412);
        let expected: Vec<u8> = (0..412).map(|j| ((100 + j) % 251) as u8).collect();
        assert_eq!(&dst[..412], expected);
        assert!(dst[412..].iter().all(|byte| *byte == 0));
        assert_eq!(
            stat_calls.load(Ordering::SeqCst),
            0,
            "a caller-supplied version and size must skip the StatObject round trip"
        );
        assert_eq!(
            observed_versions.lock().unwrap().as_slice(),
            ["caller-known-version"],
            "the C caller's version must reach the worker unchanged"
        );
        assert!(snapshot.error.is_none());

        unsafe {
            talon_client_free(client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_without_version_calls_stat() {
        let (client, stat_calls, _observed_versions) = new_client_counting(0).await;
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
                ptr::null(),
                ptr::null(),
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(
            stat_calls.load(Ordering::SeqCst),
            1,
            "a NULL version must resolve metadata with StatObject"
        );

        unsafe {
            talon_client_free(client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn version_without_size_falls_back_to_stat() {
        // A caller that knows the version but has no valid size passes a NULL
        // object_size; the read must resolve both via StatObject, not guess.
        let (client, stat_calls, _observed_versions) = new_client_counting(0).await;
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let version = cstring("caller-known-version");
        let mut dst = vec![0u8; 4096];
        let mut request_id = 0u64;

        let status = unsafe {
            talon_read_async(
                client,
                uri.as_ptr(),
                100,
                dst.as_mut_ptr(),
                dst.len(),
                version.as_ptr(),
                ptr::null(),
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(snapshot.bytes_written, dst.len());
        assert_eq!(
            stat_calls.load(Ordering::SeqCst),
            1,
            "a NULL size must resolve metadata with StatObject even when a version is given"
        );

        unsafe {
            talon_client_free(client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_object_fast_path_reads_zero_without_stat() {
        // object_size pointing at 0 is a genuinely empty object, distinct from a
        // NULL "unknown": the read returns zero bytes and still skips the stat.
        let (client, stat_calls, _observed_versions) = new_client_counting(0).await;
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let version = cstring("caller-known-version");
        let object_size: u64 = 0;
        let mut dst = vec![0u8; 128];
        let mut request_id = 0u64;

        let status = unsafe {
            talon_read_async(
                client,
                uri.as_ptr(),
                0,
                dst.as_mut_ptr(),
                dst.len(),
                version.as_ptr(),
                &object_size,
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(snapshot.bytes_written, 0);
        assert_eq!(
            stat_calls.load(Ordering::SeqCst),
            0,
            "a size of 0 is an empty object, not a reason to stat"
        );

        unsafe {
            talon_client_free(client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_block_read_receives_into_uninitialized_caller_storage() {
        // A 1 KiB block size makes a 4 KiB read span five blocks, so the
        // concurrent fetches must land in the right disjoint sub-slices.
        let (client, _stat_calls, observed_versions) = new_client_counting(1024).await;
        let state = CallbackState::new();
        let uri = cstring("s3://bucket/object.bin");
        let version = cstring("caller-known-version");
        let object_size: u64 = 1 << 20;
        let mut dst = vec![std::mem::MaybeUninit::<u8>::uninit(); 4096];
        let mut request_id = 0u64;

        let status = unsafe {
            talon_read_async(
                client,
                uri.as_ptr(),
                100,
                dst.as_mut_ptr().cast(),
                dst.len(),
                version.as_ptr(),
                &object_size,
                Some(capture_callback),
                state.user_data(),
                &mut request_id,
            )
        };
        assert_eq!(status, STATUS_OK);
        let snapshot = state.wait();
        assert_eq!(snapshot.status, STATUS_OK, "error: {:?}", snapshot.error);
        assert_eq!(snapshot.bytes_written, dst.len());
        // The mock worker fills each byte with (absolute_offset % 251), so a
        // correct reassembly reproduces that sequence across every block.
        let expected: Vec<u8> = (0..dst.len()).map(|j| ((100 + j) % 251) as u8).collect();
        // Only the successful byte count is initialized and may be observed.
        let received = unsafe {
            std::slice::from_raw_parts(dst.as_ptr().cast::<u8>(), snapshot.bytes_written)
        };
        assert_eq!(received, expected);
        assert!(
            observed_versions
                .lock()
                .unwrap()
                .iter()
                .all(|version| version == "caller-known-version"),
            "every block request must carry the C caller's exact version"
        );

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
        let worker = mock_worker(None).await;
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
            max_idle_per_addr: 0,
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
        assert!(!snapshot
            .thread
            .as_deref()
            .unwrap_or("")
            .starts_with("talon-sdk-"));
        assert!(!snapshot.in_tokio);
        assert_eq!(snapshot.status, STATUS_OK);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        unsafe {
            talon_client_free(client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_uri_messages_preserve_the_c_contract() {
        let (client, _) = new_client().await;
        for (uri, expected) in [
            ("ftp://bucket/key", "unknown backend scheme \"ftp\""),
            (
                "az://bucket",
                "URI is missing an object key: \"az://bucket\"",
            ),
        ] {
            let uri = cstring(uri);
            let mut request_id = 0;
            let status = unsafe {
                talon_read_async(
                    client,
                    uri.as_ptr(),
                    0,
                    ptr::null_mut(),
                    0,
                    ptr::null(),
                    ptr::null(),
                    Some(capture_callback),
                    ptr::null_mut(),
                    &mut request_id,
                )
            };

            assert_eq!(status, STATUS_INVALID_ARGUMENT);
            let message = unsafe { CStr::from_ptr(talon_last_error()) }
                .to_string_lossy()
                .into_owned();
            assert_eq!(message, expected);
        }

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
                ptr::null(),
                ptr::null(),
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
