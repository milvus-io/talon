//! Versioned C request options. All caller-owned buffers are copied before submission.
use super::*;
use talon_rust_client::TraceContext;

#[repr(C)]
pub struct TalonRequestProperty {
    pub key: *const c_char,
    pub key_len: usize,
    pub value: *const c_char,
    pub value_len: usize,
}

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
    pub properties: *const TalonRequestProperty,
    pub properties_len: usize,
}

#[derive(Default)]
pub struct OwnedRequestOptions {
    pub parent: Option<TraceContext>,
    pub namespace: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[no_mangle]
pub unsafe extern "C" fn talon_request_options_init(
    options: *mut TalonRequestOptions,
    size: usize,
) -> c_int {
    ffi_status(|| {
        if options.is_null()
            || size < std::mem::size_of::<TalonRequestOptions>()
            || size > u32::MAX as usize
        {
            return Err((
                STATUS_INVALID_ARGUMENT,
                "invalid request options size".into(),
            ));
        }
        unsafe {
            options.write(TalonRequestOptions {
                struct_size: size as u32,
                version: 2,
                flags: 0,
                parent_mode: 0,
                traceparent: ptr::null(),
                traceparent_len: 0,
                tracestate: ptr::null(),
                tracestate_len: 0,
                namespace_name: ptr::null(),
                properties: ptr::null(),
                properties_len: 0,
            });
        }
        Ok(())
    })
}

pub unsafe fn copy_options(
    options: *const TalonRequestOptions,
) -> Result<OwnedRequestOptions, (c_int, String)> {
    if options.is_null() {
        return Ok(OwnedRequestOptions::default());
    }
    // Only the size word is readable until its advertised size is validated.
    let size = unsafe { ptr::addr_of!((*options).struct_size).read() } as usize;
    if size < std::mem::size_of::<TalonRequestOptions>() {
        return Err((STATUS_INVALID_ARGUMENT, "request options too short".into()));
    }
    let o = unsafe { &*options };
    if o.version != 2
        || o.flags != 0
        || o.parent_mode > 1
        || (o.traceparent_len > 0 && o.traceparent.is_null())
        || (o.tracestate_len > 0 && o.tracestate.is_null())
    {
        return Err((
            STATUS_INVALID_ARGUMENT,
            "invalid request options ABI".into(),
        ));
    }
    let mut result = OwnedRequestOptions::default();
    // Routing and credentials must be validated and copied even without tracing.
    if !o.namespace_name.is_null() {
        let namespace = c_string(o.namespace_name, "namespace_name")?;
        talon_core::NamespaceContext::new(&namespace)
            .map_err(|error| (STATUS_INVALID_ARGUMENT, error.to_string()))?;
        result.namespace = Some(namespace);
    }
    if o.properties_len > 1 || (o.properties_len > 0 && o.properties.is_null()) {
        return Err((STATUS_INVALID_ARGUMENT, "invalid request properties".into()));
    }
    if o.properties_len == 1 {
        let property = unsafe { &*o.properties };
        if property.key.is_null()
            || property.value.is_null()
            || property.key_len == 0
            || property.key_len > 128
            || property.value_len == 0
            || property.value_len > talon_core::MAX_CREDENTIAL_BROKER_BYTES
        {
            return Err((
                STATUS_INVALID_ARGUMENT,
                "invalid request property buffers".into(),
            ));
        }
        let key = std::str::from_utf8(unsafe {
            std::slice::from_raw_parts(property.key.cast(), property.key_len)
        })
        .map_err(|_| {
            (
                STATUS_INVALID_ARGUMENT,
                "request property key must be UTF-8".into(),
            )
        })?;
        let value = std::str::from_utf8(unsafe {
            std::slice::from_raw_parts(property.value.cast(), property.value_len)
        })
        .map_err(|_| {
            (
                STATUS_INVALID_ARGUMENT,
                "request property value must be UTF-8".into(),
            )
        })?;
        talon_transport::RequestContext::from_properties(&[(key, value)])
            .map_err(|error| (STATUS_INVALID_ARGUMENT, error.to_string()))?;
        result.properties.push((key.to_owned(), value.to_owned()));
    }
    if o.parent_mode == 0 || !talon_telemetry::enabled() || o.traceparent_len > 1024 {
        return Ok(result);
    }
    unsafe fn text<'a>(p: *const c_char, len: usize) -> Option<&'a str> {
        if len == 0 {
            return Some("");
        }
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(p.cast(), len) }).ok()
    }
    let Some(parent) = (unsafe { text(o.traceparent, o.traceparent_len) }) else {
        return Ok(result);
    };
    let state = if o.tracestate_len <= 512 {
        unsafe { text(o.tracestate, o.tracestate_len) }
    } else {
        None
    };
    result.parent = TraceContext::from_w3c(parent, state);
    Ok(result)
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
        let options = TalonRequestOptions {
            struct_size: std::mem::size_of::<TalonRequestOptions>() as u32,
            version: 2,
            flags: 0,
            parent_mode: 1,
            traceparent: parent.as_ptr().cast(),
            traceparent_len: parent.len(),
            tracestate: state.as_ptr().cast(),
            tracestate_len: state.len(),
            namespace_name: ptr::null(),
            properties: ptr::null(),
            properties_len: 0,
        };
        let copied = unsafe { copy_options(&options) }.unwrap().parent.unwrap();
        drop(parent);
        drop(state);
        assert_eq!(
            copied.traceparent(),
            "00-11111111111111111111111111111111-2222222222222222-01"
        );
        assert_eq!(copied.tracestate(), "test=value");
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
            let copied = unsafe { copy_options(&options) }.unwrap();
            drop(namespace);
            assert!(copied.parent.is_none());
            assert_eq!(copied.namespace.as_deref(), Some(name));
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
                unsafe { copy_options(&options) }.err().unwrap().0,
                STATUS_INVALID_ARGUMENT
            );
        }
        let invalid_utf8 = [0xffu8, 0];
        options.namespace_name = invalid_utf8.as_ptr().cast();
        assert_eq!(
            unsafe { copy_options(&options) }.err().unwrap().0,
            STATUS_INVALID_ARGUMENT
        );
    }

    #[test]
    fn options_size_and_version_are_checked_before_fields() {
        let mut storage = [u32::MAX; 2];
        assert_eq!(
            unsafe { talon_request_options_init(storage.as_mut_ptr().cast(), 8) },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(storage, [u32::MAX; 2]);
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
        let defaults = unsafe { copy_options(ptr::null()) }.unwrap();
        assert!(defaults.parent.is_none());
        assert!(defaults.namespace.is_none());
        assert!(defaults.properties.is_empty());
        let defaults = unsafe { copy_options(&options) }.unwrap();
        assert!(defaults.parent.is_none());
        assert!(defaults.namespace.is_none());
        assert!(defaults.properties.is_empty());
        options.version = 1;
        assert!(unsafe { copy_options(&options) }.is_err());
        options.version = 99;
        assert!(unsafe { copy_options(&options) }.is_err());
    }

    #[test]
    fn namespace_and_request_properties_are_owned_without_a_trace_parent() {
        let namespace = CString::new("team-a").unwrap();
        let key = String::from("credential_broker");
        let value = String::from(
            r#"{"url":"http://localhost/broker","request":{"csp":"aws","region":"us-west-2","bucket":"fixture","roleArn":"fixture-role"},"origin":{"endpoint":"storage.example","region":"us-west-2"}}"#,
        );
        let property = TalonRequestProperty {
            key: key.as_ptr().cast(),
            key_len: key.len(),
            value: value.as_ptr().cast(),
            value_len: value.len(),
        };
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
        assert_eq!(options.version, 2);
        options.namespace_name = namespace.as_ptr();
        options.properties = &property;
        options.properties_len = 1;
        let copied = unsafe { copy_options(&options) }.unwrap();
        drop(namespace);
        drop(key);
        drop(value);
        assert!(copied.parent.is_none());
        assert_eq!(copied.namespace.as_deref(), Some("team-a"));
        assert_eq!(copied.properties[0].0, "credential_broker");
        assert!(copied.properties[0]
            .1
            .contains("\"roleArn\":\"fixture-role\""));
    }
}
