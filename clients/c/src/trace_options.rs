//! Size/version checked request options; submission owns copied tenant and trace strings.
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
    pub tenant: *const c_char,
    pub block_size: u32,
}

#[derive(Default)]
pub struct OwnedOptions {
    pub parent: Option<TraceContext>,
    pub tenant: Option<String>,
    pub block_size: Option<u32>,
}
impl OwnedOptions {
    pub fn borrowed(&self) -> talon_rust_client::RequestOptions<'_> {
        talon_rust_client::RequestOptions {
            parent: self
                .parent
                .as_ref()
                .map(talon_rust_client::TraceParent::Explicit)
                .unwrap_or(talon_rust_client::TraceParent::Root),
            tenant: self.tenant.as_deref(),
            block_size: self.block_size,
        }
    }
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
        options.write(TalonRequestOptions {
            struct_size: size as u32,
            version: 1,
            flags: 0,
            parent_mode: 0,
            traceparent: ptr::null(),
            traceparent_len: 0,
            tracestate: ptr::null(),
            tracestate_len: 0,
            tenant: ptr::null(),
            block_size: 0,
        });
        Ok(())
    })
}

pub unsafe fn copy_options(
    options: *const TalonRequestOptions,
) -> Result<OwnedOptions, (c_int, String)> {
    if options.is_null() {
        return Ok(OwnedOptions::default());
    }
    let size = ptr::addr_of!((*options).struct_size).read() as usize;
    if size < std::mem::size_of::<TalonRequestOptions>() {
        return Err((STATUS_INVALID_ARGUMENT, "request options too short".into()));
    }
    let o = &*options;
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
    let mut copied = OwnedOptions::default();
    if !o.tenant.is_null() {
        let name = c_string(o.tenant, "tenant")?;
        talon_rust_client::TenantId::named(&name)
            .validate()
            .map_err(|e| (STATUS_INVALID_ARGUMENT, e.to_string()))?;
        copied.tenant = Some(name);
    }
    copied.block_size = (o.block_size != 0).then_some(o.block_size);
    if o.parent_mode == 0 || !talon_telemetry::enabled() || o.traceparent_len > 1024 {
        return Ok(copied);
    }
    unsafe fn text<'a>(p: *const c_char, len: usize) -> Option<&'a str> {
        if len == 0 {
            return Some("");
        }
        std::str::from_utf8(std::slice::from_raw_parts(p.cast(), len)).ok()
    }
    let Some(parent) = text(o.traceparent, o.traceparent_len) else {
        return Ok(copied);
    };
    let state = if o.tracestate_len <= 512 {
        text(o.tracestate, o.tracestate_len)
    } else {
        None
    };
    copied.parent = TraceContext::from_w3c(parent, state);
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn submission_owns_tenant_and_omission_selects_default() {
        let mut options = std::mem::MaybeUninit::<TalonRequestOptions>::uninit();
        unsafe {
            assert_eq!(
                talon_request_options_init(
                    options.as_mut_ptr(),
                    std::mem::size_of::<TalonRequestOptions>()
                ),
                STATUS_OK
            );
        }
        let mut options = unsafe { options.assume_init() };
        assert_eq!(options.version, 1);
        let defaults = unsafe { copy_options(&options) }.unwrap();
        assert!(defaults.borrowed().tenant.is_none());
        assert!(defaults.borrowed().block_size.is_none());
        let name = CString::new("tenant-a").unwrap();
        options.tenant = name.as_ptr();
        options.block_size = 4096;
        let copied = unsafe { copy_options(&options) }.unwrap();
        drop(name);
        assert_eq!(copied.borrowed().tenant, Some("tenant-a"));
        assert_eq!(copied.borrowed().block_size, Some(4096));
    }

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
            tenant: ptr::null(),
            block_size: 0,
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
        assert!(unsafe { copy_options(&options) }.unwrap().parent.is_none());
        options.version = 2;
        assert!(unsafe { copy_options(&options) }.is_err());
    }
}
