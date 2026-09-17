//! Owned destinations whose storage outlives cancelled kernel operations.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{cell::UnsafeCell, sync::Arc};
use tokio::sync::Notify;

/// An owned destination with stable backing storage, which may be uninitialized.
/// `Vec<u8>`, boxed slices/arrays and `BytesMut` implement this trait. Inline
/// arrays must be boxed before submission so returning ownership cannot copy data.
///
/// ```compile_fail
/// use talon_cache_client::read_buffer::ReadBuffer;
/// let _ = ReadBuffer::new([0u8; 4096]); // an inline array would move its payload
/// ```
///
/// # Safety
/// `raw_parts` must return a non-null, aligned pointer and a length no greater
/// than `isize::MAX`, describing exclusively writable storage. That allocation
/// must remain at the same address even when the owner is moved, until the owner
/// is dropped or the recovered owner is explicitly modified by the caller.
/// No aliases may access the bytes while the operation owns the destination.
/// Recovery on error does not imply initialization; only a successful byte count
/// identifies received bytes. Implementations must not expose uninitialized
/// contents as initialized values, including from Drop.
pub unsafe trait ReadDestination: Send + 'static {
    /// Return the stable writable allocation; called once when taking ownership.
    fn raw_parts(&mut self) -> (*mut u8, usize);
}
// SAFETY: moving these handles never moves their backing allocations. Their
// contents are accessed only through the exclusive target while the SDK owns them.
unsafe impl ReadDestination for Vec<u8> {
    fn raw_parts(&mut self) -> (*mut u8, usize) {
        (self.as_mut_ptr(), self.len())
    }
}
unsafe impl<B: AsMut<[u8]> + Send + ?Sized + 'static> ReadDestination for Box<B> {
    fn raw_parts(&mut self) -> (*mut u8, usize) {
        let bytes = self.as_mut().as_mut();
        (bytes.as_mut_ptr(), bytes.len())
    }
}
unsafe impl ReadDestination for bytes::BytesMut {
    fn raw_parts(&mut self) -> (*mut u8, usize) {
        (self.as_mut_ptr(), self.len())
    }
}

// One allocation holds the destination handle, recovery notification and the
// root region's access state. `users` counts targets/leases, not the recovery
// handle. After its acquire load sees zero, no owner may touch payload memory.
struct State {
    users: AtomicUsize,
    recovered: Notify,
    access: Access,
}
impl Default for State {
    fn default() -> Self {
        Self {
            users: AtomicUsize::new(1),
            recovered: Notify::new(),
            access: Access::default(),
        }
    }
}
trait Backing: Send + Sync {
    fn state(&self) -> &State;
}
struct Storage<B> {
    buffer: UnsafeCell<Option<B>>,
    state: State,
}
// SAFETY: only recovery accesses the handle, after every target/kernel lease
// has released its `users` reference. Outstanding owners only use raw regions.
unsafe impl<B: Send> Sync for Storage<B> {}
impl<B: Send> Backing for Storage<B> {
    fn state(&self) -> &State {
        &self.state
    }
}
struct Owner {
    storage: Arc<dyn Backing>,
}
impl Clone for Owner {
    fn clone(&self) -> Self {
        if self.storage.state().users.fetch_add(1, Ordering::Relaxed) >= isize::MAX as usize {
            std::process::abort();
        }
        Self {
            storage: self.storage.clone(),
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let state = self.storage.state();
        if state.users.fetch_sub(1, Ordering::Release) == 1 {
            state.recovered.notify_one();
        }
        // After the release decrement, this destructor never accesses payload
        // or its handle. Recovery can take B even before this Arc is dropped.
    }
}

/// Own a destination until all targets and cancelled kernel operations retire.
/// The wrapper allocates one control object; it never allocates/copies payload.
pub struct ReadBuffer<B> {
    storage: Arc<Storage<B>>,
    target: Option<ReadTarget>,
}
impl<B: ReadDestination> ReadBuffer<B> {
    /// Retain a destination without moving or copying its backing storage.
    pub fn new(mut buffer: B) -> Self {
        let (ptr, len) = buffer.raw_parts();
        let storage = Arc::new(Storage {
            buffer: UnsafeCell::new(Some(buffer)),
            state: State::default(),
        });
        let target = ReadTarget {
            ptr,
            len,
            owner: Owner {
                storage: storage.clone(),
            },
            access: None,
        };
        Self {
            storage,
            target: Some(target),
        }
    }
    /// Take the unique writable view. It can be partitioned, never duplicated.
    pub fn take_target(&mut self) -> ReadTarget {
        self.target.take().expect("destination already taken")
    }
    /// Return the exact original destination after every payload user retires.
    /// Dropping recovery leaves the allocation with outstanding kernel leases.
    pub async fn finish(mut self) -> B {
        drop(self.target.take());
        loop {
            let notified = self.storage.state.recovered.notified();
            if self.storage.state.users.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
        // SAFETY: the acquire load pairs with all owners' release decrements.
        // No target/kernel operation remains, and only this recovery handle can
        // take B. Other Arc holders finishing Drop never access this cell.
        unsafe { &mut *self.storage.buffer.get() }
            .take()
            .expect("destination recovered once")
    }
}

#[derive(Default)]
struct Access {
    busy: AtomicBool,
    finished: Notify,
}

/// Exclusive region of an owned destination. Only genuine subdivisions need
/// separate access state; a single-block read uses the inline root state.
pub struct ReadTarget {
    ptr: *mut u8,
    len: usize,
    owner: Owner,
    access: Option<Arc<Access>>,
}
// SAFETY: the owner retains storage; only an exclusive target or its kernel
// lease can write each region. Shared access only observes completion state.
unsafe impl Send for ReadTarget {}
unsafe impl Sync for ReadTarget {}
impl ReadTarget {
    fn access(&self) -> &Access {
        self.access
            .as_deref()
            .unwrap_or(&self.owner.storage.state().access)
    }
    /// Number of writable bytes in this region.
    pub fn len(&self) -> usize {
        self.len
    }
    /// Whether this region contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Shorten a region without allocating a discarded tail. No I/O may be active.
    pub fn truncate(&mut self, len: usize) {
        assert!(len <= self.len && !self.access().busy.load(Ordering::Acquire));
        self.len = len;
    }
    /// Partition into disjoint destinations. No I/O may be outstanding.
    pub fn split_at(self, at: usize) -> (Self, Self) {
        assert!(at <= self.len);
        assert!(
            !self.access().busy.load(Ordering::Acquire),
            "kernel lease still active"
        );
        let tail = Self {
            // SAFETY: at is within the same exclusively owned region.
            ptr: unsafe { self.ptr.add(at) },
            len: self.len - at,
            owner: self.owner.clone(),
            access: Some(Arc::default()),
        };
        (Self { len: at, ..self }, tail)
    }
    /// Wait for a cancelled operation to stop accessing the region.
    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.access().finished.notified();
            if !self.access().busy.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }
    pub(crate) async fn uninit_bytes_mut(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
        self.wait_idle().await;
        // SAFETY: exclusive region, live owner, no outstanding kernel lease.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.cast(), self.len) }
    }
    /// Caller must have initialized this prefix through completed I/O.
    #[cfg(target_os = "linux")]
    pub(crate) unsafe fn initialized_prefix(&self, len: usize) -> &[u8] {
        assert!(len <= self.len && !self.access().busy.load(Ordering::Acquire));
        unsafe { std::slice::from_raw_parts(self.ptr, len) }
    }
    #[cfg(target_os = "linux")]
    pub(crate) async fn lease(&mut self) -> Lease {
        self.wait_idle().await;
        assert!(!self.access().busy.swap(true, Ordering::AcqRel));
        Lease {
            initialized: 0,
            ptr: self.ptr,
            len: self.len,
            _owner: self.owner.clone(),
            access: self.access.clone(),
        }
    }
    // Only a cross-runtime request needs a loan. Native and Tokio local
    // executors borrow the target and avoid this allocation entirely.
    pub(crate) async fn lend(&mut self) -> Self {
        self.wait_idle().await;
        assert!(!self.access().busy.swap(true, Ordering::AcqRel));
        let loan = Arc::new(Loan {
            owner: self.owner.clone(),
            access: self.access.clone(),
            state: State::default(),
        });
        Self {
            ptr: self.ptr,
            len: self.len,
            owner: Owner { storage: loan },
            access: None,
        }
    }
}
#[cfg(target_os = "linux")]
pub(crate) struct Lease {
    initialized: usize,
    ptr: *mut u8,
    len: usize,
    _owner: Owner,
    access: Option<Arc<Access>>,
}
#[cfg(target_os = "linux")]
impl Drop for Lease {
    fn drop(&mut self) {
        let access = self
            .access
            .as_deref()
            .unwrap_or(&self._owner.storage.state().access);
        access.busy.store(false, Ordering::Release);
        access.finished.notify_one();
    }
}
// SAFETY: the lease retains the allocation and exclusive region through CQE,
// including when Monoio retains a dropped operation for cancellation cleanup.
#[cfg(target_os = "linux")]
unsafe impl monoio::buf::IoBuf for Lease {
    fn read_ptr(&self) -> *const u8 {
        self.ptr
    }
    fn bytes_init(&self) -> usize {
        self.initialized
    }
}
#[cfg(target_os = "linux")]
unsafe impl monoio::buf::IoBufMut for Lease {
    fn write_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
    fn bytes_total(&mut self) -> usize {
        self.len
    }
    unsafe fn set_init(&mut self, len: usize) {
        self.initialized = self.initialized.max(len);
    }
}
struct Loan {
    owner: Owner,
    access: Option<Arc<Access>>,
    state: State,
}
impl Backing for Loan {
    fn state(&self) -> &State {
        &self.state
    }
}
impl Drop for Loan {
    fn drop(&mut self) {
        let access = self
            .access
            .as_deref()
            .unwrap_or(&self.owner.storage.state().access);
        access.busy.store(false, Ordering::Release);
        access.finished.notify_one();
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use monoio::buf::IoBufMut;
    use std::sync::atomic::AtomicUsize;

    struct Tracked {
        bytes: [u8; 32],
        dropped: Arc<AtomicUsize>,
    }
    impl AsMut<[u8]> for Tracked {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.bytes
        }
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn returning_buffer_waits_for_cancelled_descendant_lease() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut buffer = ReadBuffer::new(Box::new(Tracked {
            bytes: [0; 32],
            dropped: dropped.clone(),
        }));
        let mut root = buffer.take_target();
        let (mut first, mut second) = root.lend().await.split_at(11);
        let mut kernel = first.lease().await;
        for byte in second.uninit_bytes_mut().await {
            byte.write(2);
        }
        let pointer = kernel.write_ptr();
        drop((first, second)); // simulated dropped request still has a kernel owner
        let mut idle = Box::pin(root.wait_idle());
        assert!(futures::poll!(&mut idle).is_pending());
        drop(idle);
        drop(root);
        let mut finished = Box::pin(buffer.finish());
        assert!(futures::poll!(&mut finished).is_pending());
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        unsafe {
            pointer.write_bytes(1, 11);
        }
        drop(kernel); // completion finally retires the pointer
        let returned = finished.await;
        assert_eq!(
            returned.bytes.as_ptr(),
            pointer,
            "returning the owner must not move payload bytes"
        );
        assert_eq!(&returned.bytes[..11], &[1; 11]);
        assert_eq!(&returned.bytes[11..], &[2; 21]);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(returned);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_recovery_future_keeps_storage_until_completion() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut buffer = ReadBuffer::new(Box::new(Tracked {
            bytes: [0; 32],
            dropped: dropped.clone(),
        }));
        let mut target = buffer.take_target();
        let mut kernel = target.lease().await;
        drop(target);
        let mut finish = Box::pin(buffer.finish());
        assert!(futures::poll!(&mut finish).is_pending());
        drop(finish);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        unsafe {
            kernel.write_ptr().write_bytes(7, 32);
        }
        drop(kernel);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_waits_for_prior_request_and_reuses_same_address() {
        let mut buffer = ReadBuffer::new(vec![0; 32]);
        let mut target = buffer.take_target();
        let mut request = target.lend().await;
        let mut previous = request.lease().await;
        let pointer = previous.write_ptr();
        drop(request);
        let mut retry = Box::pin(target.lend());
        assert!(futures::poll!(&mut retry).is_pending());
        drop(previous);
        let mut next = retry.await;
        let mut lease = next.lease().await;
        assert_eq!(lease.write_ptr(), pointer);
        drop((lease, next, target));
        assert_eq!(buffer.finish().await, vec![0; 32]);
    }
}
