//! Measure request setup separately from network, buffer and serialization costs.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::task::{Context, Poll};

use talon_rust_client::{parse_uri, ClientBuilder, RequestOptions, Version};

struct CountAllocations;
thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}

unsafe impl GlobalAlloc for CountAllocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountAllocations = CountAllocations;

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("an empty request must not perform I/O"),
    }
}

#[test]
fn default_and_named_request_setup_do_not_allocate() {
    talon_telemetry::configure(talon_telemetry::Config::default()).unwrap();
    let client = ClientBuilder::default()
        .with_coordinator("127.0.0.1:1")
        .build()
        .unwrap();
    let object = parse_uri("s3://bucket/key").unwrap();
    let version = Version::new("v1");
    let exercise = || {
        for namespace in [None, Some("a"), Some("b"), None] {
            let options = RequestOptions {
                namespace,
                block_size: Some(4096),
                ..Default::default()
            };
            assert!(
                ready(client.read_with_options(&object, 0, Some(0), None, &options))
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                ready(client.read_into_with_options(&object, 0, &mut [], None, &options)).unwrap(),
                0
            );
            assert_eq!(
                ready(client.load_with_options(&object, &version, 0, &options))
                    .unwrap()
                    .blocks,
                0
            );
            assert!(ready(client.batch_load_with_options(&[], &options))
                .unwrap()
                .is_empty());
        }
    };
    // Exclude one-time telemetry/TLS initialization from steady-state request setup.
    exercise();
    ALLOCATIONS.with(|count| count.set(Some(0)));
    exercise();
    let allocations = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(
        allocations, 0,
        "selecting a request namespace must not allocate client copies"
    );
}
