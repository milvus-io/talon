use super::*;

/// One file to prewarm. Strings are copied before submission returns.
#[repr(C)]
pub struct TalonLoadRequest {
    /// Object URI, such as s3://bucket/key.
    pub uri: *const c_char,
    /// Exact source version; required even for an empty object.
    pub version: *const c_char,
    /// Size of that source version; no HEAD is issued.
    pub size: u64,
}

/// Completed file prewarm, in input order. Residency remains subject to eviction.
#[repr(C)]
pub struct TalonLoadResult {
    /// Object size in bytes.
    pub size: u64,
    /// Number of blocks warmed on their primary owners.
    pub blocks: u64,
}

/// Submit a single file prewarm. See talon.h for ownership and callback rules.
#[no_mangle]
pub unsafe extern "C" fn talon_load_async(
    client: *mut TalonClient,
    uri: *const c_char,
    version: *const c_char,
    size: u64,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    talon_load_async_with_options(
        client,
        uri,
        version,
        size,
        ptr::null(),
        callback,
        user_data,
        request_id_out,
    )
}

/// Submit a single file prewarm with copied request-local trace context.
#[no_mangle]
pub unsafe extern "C" fn talon_load_async_with_options(
    client: *mut TalonClient,
    uri: *const c_char,
    version: *const c_char,
    size: u64,
    options: *const TalonRequestOptions,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    let request = TalonLoadRequest { uri, version, size };
    submit(
        client,
        &request,
        1,
        false,
        options,
        callback,
        user_data,
        request_id_out,
    )
}

/// Submit files using protocol-level batches, not repeated single LOAD RPCs.
#[no_mangle]
pub unsafe extern "C" fn talon_batch_load_async(
    client: *mut TalonClient,
    requests: *const TalonLoadRequest,
    count: usize,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    talon_batch_load_async_with_options(
        client,
        requests,
        count,
        ptr::null(),
        callback,
        user_data,
        request_id_out,
    )
}

/// Submit a protocol batch with copied request-local trace context.
#[no_mangle]
pub unsafe extern "C" fn talon_batch_load_async_with_options(
    client: *mut TalonClient,
    requests: *const TalonLoadRequest,
    count: usize,
    options: *const TalonRequestOptions,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    submit(
        client,
        requests,
        count,
        true,
        options,
        callback,
        user_data,
        request_id_out,
    )
}

#[allow(clippy::too_many_arguments)]
unsafe fn submit(
    client: *mut TalonClient,
    requests: *const TalonLoadRequest,
    count: usize,
    batch: bool,
    options: *const TalonRequestOptions,
    callback: Option<TalonCallback>,
    user_data: *mut c_void,
    request_id_out: *mut u64,
) -> c_int {
    ffi_status(|| {
        let inner = client_inner(client)?;
        let parent = trace_options::copy_options(options)?;
        let callback = callback.ok_or((STATUS_INVALID_ARGUMENT, "callback is null".into()))?;
        if request_id_out.is_null()
            || (count != 0 && requests.is_null())
            || count > isize::MAX as usize / std::mem::size_of::<TalonLoadRequest>()
        {
            return Err((
                STATUS_INVALID_ARGUMENT,
                "invalid load array or request_id_out".into(),
            ));
        }
        let requests = if count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(requests, count)
        };
        let requests = requests
            .iter()
            .map(|request| {
                if request.uri.is_null() || request.version.is_null() {
                    return Err((
                        STATUS_INVALID_ARGUMENT,
                        "load requires uri and version".into(),
                    ));
                }
                let object = parse_uri(&c_string(request.uri, "uri")?)?;
                let version = c_string(request.version, "version")?;
                if version.trim().is_empty() {
                    return Err((
                        STATUS_INVALID_ARGUMENT,
                        "load requires a non-empty version".into(),
                    ));
                }
                Ok(talon_rust_client::LoadRequest {
                    object,
                    version: talon_rust_client::Version::new(version),
                    size: request.size,
                })
            })
            .collect::<Result<Vec<_>, (c_int, String)>>()?;
        let request_id = inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        *request_id_out = request_id;
        let client = Arc::clone(&inner.client);
        let dispatcher = Arc::clone(&inner.dispatcher);
        let user_data = UserData(user_data);
        let task = async move {
            let operation = talon_telemetry::Operation::new(
                "talon.c.load",
                "internal",
                parent
                    .as_ref()
                    .map(talon_telemetry::TraceParent::Explicit)
                    .unwrap_or(talon_telemetry::TraceParent::Root),
            );
            let result = operation
                .scope(async {
                    if batch {
                        client.batch_load(&requests).await
                    } else {
                        let request = &requests[0];
                        client
                            .load(&request.object, &request.version, request.size)
                            .await
                            .map(|result| vec![result])
                    }
                })
                .await;
            operation.outcome(if result.is_ok() { "success" } else { "error" });
            let kind = if batch {
                OPERATION_BATCH_LOAD
            } else {
                OPERATION_LOAD
            };
            let result = match result {
                Ok(loads) => TalonResult {
                    operation: kind,
                    status: STATUS_OK,
                    request_id,
                    bytes_written: 0,
                    object_size: 0,
                    version: None,
                    error: None,
                    load_failures: Vec::new(),
                    loads: loads
                        .into_iter()
                        .map(|r| TalonLoadResult {
                            size: r.size,
                            blocks: r.blocks,
                        })
                        .collect(),
                },
                Err(error) => {
                    let mut result = TalonResult::classified_error(
                        kind,
                        request_id,
                        talon_rust_client::ErrorKind::from(&error),
                        error.to_string(),
                    );
                    result.load_failures = error
                        .failed_files()
                        .iter()
                        .map(|f| (f.clone(), cstring_lossy(f.error.clone())))
                        .collect();
                    result
                }
            };
            dispatch_result(dispatcher, callback, user_data, result);
        };
        use tracing::instrument::WithSubscriber;
        if talon_telemetry::enabled() {
            inner
                .runtime
                .spawn(task.with_subscriber(telemetry_dispatch()));
        } else {
            inner.runtime.spawn(task);
        }
        Ok(())
    })
}

/// Number of file results on successful LOAD/batch LOAD; otherwise zero.
#[no_mangle]
pub unsafe extern "C" fn talon_result_load_count(result: *const TalonResult) -> usize {
    result.as_ref().map_or(0, |r| r.loads.len())
}

/// Borrow a file result until talon_result_free; null for an invalid index.
#[no_mangle]
pub unsafe extern "C" fn talon_result_load(
    result: *const TalonResult,
    index: usize,
) -> *const TalonLoadResult {
    result
        .as_ref()
        .and_then(|r| r.loads.get(index))
        .map_or(ptr::null(), |r| r)
}

/// Number of failed or unconfirmed input files in a submitted batch.
#[no_mangle]
pub unsafe extern "C" fn talon_result_load_failure_count(result: *const TalonResult) -> usize {
    result.as_ref().map_or(0, |r| r.load_failures.len())
}

/// Zero-based input index; SIZE_MAX for an invalid result/failure index.
#[no_mangle]
pub unsafe extern "C" fn talon_result_load_failure_index(
    result: *const TalonResult,
    index: usize,
) -> usize {
    result
        .as_ref()
        .and_then(|r| r.load_failures.get(index))
        .map_or(usize::MAX, |f| f.0.index)
}

/// 1 for unconfirmed completion, 0 for confirmed failure, -1 for invalid index.
#[no_mangle]
pub unsafe extern "C" fn talon_result_load_failure_uncertain(
    result: *const TalonResult,
    index: usize,
) -> c_int {
    result
        .as_ref()
        .and_then(|r| r.load_failures.get(index))
        .map_or(-1, |f| c_int::from(f.0.uncertain))
}

/// Borrow the per-file diagnostic until talon_result_free; NULL for invalid index.
#[no_mangle]
pub unsafe extern "C" fn talon_result_load_failure_error(
    result: *const TalonResult,
    index: usize,
) -> *const c_char {
    result
        .as_ref()
        .and_then(|r| r.load_failures.get(index))
        .map_or(ptr::null(), |f| f.1.as_ptr())
}
