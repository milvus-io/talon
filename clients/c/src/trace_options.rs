//! Versioned C request options. Carrier pointers are copied before submission.
use super::*;
use talon_rust_client::TraceContext;

#[repr(C)]
pub struct TalonRequestOptions {
    pub struct_size: u32,
    pub version: u32,
    pub flags: u32,
    pub parent_mode: u32,
    pub traceparent: *const c_char,
    pub traceparent_len: usize,
    pub tracestate: *const c_char,
    pub tracestate_len: usize,
    /// Optional request namespace; null selects `default`.
    pub namespace_name: *const c_char,
}

const V1_PREFIX_SIZE: usize = std::mem::offset_of!(TalonRequestOptions, namespace_name);

#[no_mangle]
pub unsafe extern "C" fn talon_request_options_init(
    options: *mut TalonRequestOptions,
    size: usize,
) -> c_int {
    ffi_status(|| {
        if options.is_null() || size < V1_PREFIX_SIZE || size > u32::MAX as usize {
            return Err((
                STATUS_INVALID_ARGUMENT,
                "invalid request options size".into(),
            ));
        }
        unsafe {
            // Older callers only allocated the original v1 prefix. Initialize
            // that prefix without ever forming a reference to the larger type.
            ptr::write_bytes(options.cast::<u8>(), 0, V1_PREFIX_SIZE);
            ptr::addr_of_mut!((*options).struct_size).write(size as u32);
            ptr::addr_of_mut!((*options).version).write(1);
            if size >= std::mem::size_of::<TalonRequestOptions>() {
                ptr::addr_of_mut!((*options).namespace_name).write(ptr::null());
            }
        }
        Ok(())
    })
}

pub unsafe fn copy_options(
    options: *const TalonRequestOptions,
) -> Result<(Option<TraceContext>, Option<String>), (c_int, String)> {
    if options.is_null() {
        return Ok((None, None));
    }
    // Only the size word is readable until its advertised size is validated.
    let size = unsafe { ptr::addr_of!((*options).struct_size).read() } as usize;
    if size < V1_PREFIX_SIZE {
        return Err((STATUS_INVALID_ARGUMENT, "request options too short".into()));
    }
    // Read fields individually: a v1 caller may have no allocation for the tail.
    let version = unsafe { ptr::addr_of!((*options).version).read() };
    let flags = unsafe { ptr::addr_of!((*options).flags).read() };
    let parent_mode = unsafe { ptr::addr_of!((*options).parent_mode).read() };
    let traceparent = unsafe { ptr::addr_of!((*options).traceparent).read() };
    let traceparent_len = unsafe { ptr::addr_of!((*options).traceparent_len).read() };
    let tracestate = unsafe { ptr::addr_of!((*options).tracestate).read() };
    let tracestate_len = unsafe { ptr::addr_of!((*options).tracestate_len).read() };
    if version != 1
        || flags != 0
        || parent_mode > 1
        || (traceparent_len > 0 && traceparent.is_null())
        || (tracestate_len > 0 && tracestate.is_null())
    {
        return Err((
            STATUS_INVALID_ARGUMENT,
            "invalid request options ABI".into(),
        ));
    }
    // Namespace is routing data, so validation and ownership never depend on
    // tracing being enabled or on the presence of a valid W3C parent.
    let namespace = if size >= std::mem::size_of::<TalonRequestOptions>() {
        let name = unsafe { ptr::addr_of!((*options).namespace_name).read() };
        if name.is_null() {
            None
        } else {
            let name = c_string(name, "namespace_name")?;
            talon_core::NamespaceContext::new(&name)
                .map_err(|error| (STATUS_INVALID_ARGUMENT, error.to_string()))?;
            Some(name)
        }
    } else {
        None
    };
    if parent_mode == 0 || !talon_telemetry::enabled() || traceparent_len > 1024 {
        return Ok((None, namespace));
    }
    unsafe fn text<'a>(p: *const c_char, len: usize) -> Option<&'a str> {
        if len == 0 {
            return Some("");
        }
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(p.cast(), len) }).ok()
    }
    let Some(parent) = (unsafe { text(traceparent, traceparent_len) }) else {
        return Ok((None, namespace));
    };
    let state = if tracestate_len <= 512 {
        unsafe { text(tracestate, tracestate_len) }
    } else {
        None
    };
    Ok((TraceContext::from_w3c(parent, state), namespace))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn submission_owns_carrier_after_callers_storage_is_destroyed() {
        talon_telemetry::configure(talon_telemetry::Config {
            mode: talon_telemetry::Mode::Propagate,
            ..Default::default()
        })
        .unwrap();
        let parent = String::from("00-11111111111111111111111111111111-2222222222222222-01");
        let state = String::from("test=value");
        let namespace = CString::new("team-a").unwrap();
        let options = TalonRequestOptions {
            struct_size: std::mem::size_of::<TalonRequestOptions>() as u32,
            version: 1,
            flags: 0,
            parent_mode: 1,
            traceparent: parent.as_ptr().cast(),
            traceparent_len: parent.len(),
            tracestate: state.as_ptr().cast(),
            tracestate_len: state.len(),
            namespace_name: namespace.as_ptr(),
        };
        let (copied, copied_namespace) = unsafe { copy_options(&options) }.unwrap();
        let copied = copied.unwrap();
        drop(parent);
        drop(state);
        drop(namespace);
        assert_eq!(
            copied.traceparent(),
            "00-11111111111111111111111111111111-2222222222222222-01"
        );
        assert_eq!(copied.tracestate(), "test=value");
        assert_eq!(copied_namespace.as_deref(), Some("team-a"));
    }

    #[test]
    fn namespace_does_not_require_a_trace_parent_and_rejects_invalid_names() {
        let mut options = std::mem::MaybeUninit::<TalonRequestOptions>::uninit();
        assert_eq!(
            unsafe {
                talon_request_options_init(
                    options.as_mut_ptr(),
                    std::mem::size_of::<TalonRequestOptions>(),
                )
            },
            STATUS_OK
        );
        let mut options = unsafe { options.assume_init() };
        for name in ["team-a.b_1", "default", &"x".repeat(255)] {
            let namespace = CString::new(name).unwrap();
            options.namespace_name = namespace.as_ptr();
            let (trace, copied) = unsafe { copy_options(&options) }.unwrap();
            drop(namespace);
            assert!(trace.is_none());
            assert_eq!(copied.as_deref(), Some(name));
        }
        for name in [
            "",
            ".",
            "..",
            "a/b",
            "a b",
            "租户",
            "worker_identity",
            ".worker.lock",
            &"x".repeat(256),
        ] {
            let namespace = CString::new(name).unwrap();
            options.namespace_name = namespace.as_ptr();
            assert_eq!(
                unsafe { copy_options(&options) }.unwrap_err().0,
                STATUS_INVALID_ARGUMENT
            );
        }
        let invalid_utf8 = [0xffu8, 0];
        options.namespace_name = invalid_utf8.as_ptr().cast();
        assert_eq!(
            unsafe { copy_options(&options) }.unwrap_err().0,
            STATUS_INVALID_ARGUMENT
        );
    }

    #[test]
    fn old_request_options_size_neither_reads_nor_writes_the_new_tail() {
        let old_size = std::mem::offset_of!(TalonRequestOptions, namespace_name);
        let full_size = std::mem::size_of::<TalonRequestOptions>();
        let mut storage = vec![usize::MAX; full_size / std::mem::size_of::<usize>() + 1];
        let options = storage.as_mut_ptr().cast::<TalonRequestOptions>();
        assert_eq!(
            unsafe { talon_request_options_init(options, old_size) },
            STATUS_OK
        );
        assert!(storage[old_size / std::mem::size_of::<usize>()..]
            .iter()
            .all(|word| *word == usize::MAX));
        let (trace, namespace) = unsafe { copy_options(options) }.unwrap();
        assert!(trace.is_none());
        assert!(namespace.is_none());

        // An allocation containing only the original v1 fields must also work.
        let mut old_storage = vec![0usize; old_size / std::mem::size_of::<usize>()];
        let old_options = old_storage.as_mut_ptr().cast::<TalonRequestOptions>();
        assert_eq!(
            unsafe { talon_request_options_init(old_options, old_size) },
            STATUS_OK
        );
        assert!(unsafe { copy_options(old_options) }.unwrap().1.is_none());

        assert_eq!(
            unsafe { talon_request_options_init(options, full_size) },
            STATUS_OK
        );
        assert!(unsafe { ptr::addr_of!((*options).namespace_name).read() }.is_null());
        assert_eq!(
            storage[full_size / std::mem::size_of::<usize>()],
            usize::MAX
        );
    }
    #[test]
    fn options_size_and_version_are_checked_before_fields() {
        let size = 4u32;
        assert!(unsafe { copy_options((&size as *const u32).cast()) }.is_err());
        let mut options = std::mem::MaybeUninit::<TalonRequestOptions>::uninit();
        assert_eq!(
            unsafe {
                talon_request_options_init(
                    options.as_mut_ptr(),
                    std::mem::size_of::<TalonRequestOptions>(),
                )
            },
            STATUS_OK
        );
        let mut options = unsafe { options.assume_init() };
        let (trace, namespace) = unsafe { copy_options(&options) }.unwrap();
        assert!(trace.is_none());
        assert!(namespace.is_none());
        options.version = 2;
        assert!(unsafe { copy_options(&options) }.is_err());
    }
}
