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
                version: 1,
                flags: 0,
                parent_mode: 0,
                traceparent: ptr::null(),
                traceparent_len: 0,
                tracestate: ptr::null(),
                tracestate_len: 0,
            });
        }
        Ok(())
    })
}

pub unsafe fn copy_options(
    options: *const TalonRequestOptions,
) -> Result<Option<TraceContext>, (c_int, String)> {
    if options.is_null() {
        return Ok(None);
    }
    // Only the size word is readable until its advertised size is validated.
    let size = unsafe { ptr::addr_of!((*options).struct_size).read() } as usize;
    if size < std::mem::size_of::<TalonRequestOptions>() {
        return Err((STATUS_INVALID_ARGUMENT, "request options too short".into()));
    }
    let o = unsafe { &*options };
    if o.version != 1
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
    if o.parent_mode == 0 || !talon_telemetry::enabled() || o.traceparent_len > 1024 {
        return Ok(None);
    }
    unsafe fn text<'a>(p: *const c_char, len: usize) -> Option<&'a str> {
        if len == 0 {
            return Some("");
        }
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(p.cast(), len) }).ok()
    }
    let Some(parent) = (unsafe { text(o.traceparent, o.traceparent_len) }) else {
        return Ok(None);
    };
    let state = if o.tracestate_len <= 512 {
        unsafe { text(o.tracestate, o.tracestate_len) }
    } else {
        None
    };
    Ok(TraceContext::from_w3c(parent, state))
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
            version: 1,
            flags: 0,
            parent_mode: 1,
            traceparent: parent.as_ptr().cast(),
            traceparent_len: parent.len(),
            tracestate: state.as_ptr().cast(),
            tracestate_len: state.len(),
        };
        let copied = unsafe { copy_options(&options) }.unwrap().unwrap();
        drop(parent);
        drop(state);
        assert_eq!(
            copied.traceparent(),
            "00-11111111111111111111111111111111-2222222222222222-01"
        );
        assert_eq!(copied.tracestate(), "test=value");
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
        assert!(unsafe { copy_options(&options) }.unwrap().is_none());
        options.version = 2;
        assert!(unsafe { copy_options(&options) }.is_err());
    }
}
