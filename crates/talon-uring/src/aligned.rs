//! Stable download allocations with separate logical and direct-write views.
use crate::DirectIoAlignment;
use bytes::Bytes;
use monoio::buf::{IoBuf, IoBufMut};
use std::{
    io,
    ops::Range,
    sync::{Arc, Mutex},
};

/// An immutable payload. Only `bytes` contributes to logical length; `backing`
/// retains the original initialized allocation, including its zero-filled tail.
#[derive(Clone)]
pub struct Payload {
    bytes: Bytes,
    backing: Option<(Bytes, usize)>,
}
impl From<Bytes> for Payload {
    fn from(bytes: Bytes) -> Self {
        Self {
            bytes,
            backing: None,
        }
    }
}
impl std::ops::Deref for Payload {
    type Target = Bytes;
    fn deref(&self) -> &Bytes {
        &self.bytes
    }
}
impl Payload {
    /// Return the logical view, retaining its allocation without copying.
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    /// Slice logical bytes while retaining the original padded owner.
    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            bytes: Bytes::slice(&self.bytes, range),
            backing: self.backing.clone(),
        }
    }

    /// Return a safe direct-write view, without moving or modifying payload.
    /// Interior slices cannot use other payload bytes as padding. External
    /// admissions and unaligned page starts retain the buffered path.
    pub(crate) fn direct_bytes(&self, alignment: DirectIoAlignment) -> Option<Bytes> {
        if self.bytes.is_empty() || self.bytes.as_ptr() as usize % alignment.memory() != 0 {
            return None;
        }
        let size = alignment.round_len(self.bytes.len()).ok()?;
        if size == self.bytes.len() {
            return Some(self.bytes.clone());
        }
        let (backing, body_len) = self.backing.as_ref()?;
        let start = (self.bytes.as_ptr() as usize).checked_sub(backing.as_ptr() as usize)?;
        // Only the final logical tail has initialized padding after it.
        if start.checked_add(self.bytes.len())? != *body_len {
            return None;
        }
        let end = start.checked_add(size)?;
        (end <= backing.len()).then(|| backing.slice(start..end))
    }
}

/// Pass the cache's startup-probed geometry through existing Bytes-based
/// backend APIs. Each fetch has its own capture, including across retries.
/// The HTTP client explicitly carries this context across its ring task hop.
/// No process-wide pointer registry or payload copy is involved.
#[derive(Clone)]
pub struct Download {
    alignment: DirectIoAlignment,
    received: Arc<Mutex<Option<Payload>>>,
}
tokio::task_local! { static DOWNLOAD: Download; }
impl Download {
    /// Snapshot the fetch context before moving work to another ring task.
    pub fn current() -> Option<Self> {
        DOWNLOAD.try_with(Clone::clone).ok()
    }
    /// Geometry selected by the destination cache store.
    pub fn alignment(&self) -> DirectIoAlignment {
        self.alignment
    }

    /// Release the previous response before receiving a retry's allocation.
    pub fn reset(&self) {
        self.received.lock().unwrap().take();
    }

    /// Retain the completed receive allocation and return its logical bytes.
    pub fn finish(&self, buffer: AlignedBuf, len: usize) -> Bytes {
        let payload = buffer.freeze(len);
        let bytes = payload.bytes.clone();
        *self.received.lock().unwrap() = Some(payload);
        bytes
    }

    /// Run a backend fetch and retain padding only for bytes from its response.
    pub async fn capture<E>(
        alignment: Option<DirectIoAlignment>,
        fetch: impl std::future::Future<Output = Result<Bytes, E>>,
    ) -> Result<Payload, E> {
        let Some(alignment) = alignment else {
            return fetch.await.map(Payload::from);
        };
        let capture = Self {
            alignment,
            received: Arc::default(),
        };
        let bytes = DOWNLOAD.scope(capture.clone(), fetch).await?;
        let received = capture.received.lock().unwrap().take();
        // Backends may slice a 200 response, or return a different allocation.
        // Retain padding only when the returned bytes belong to this body.
        if let Some(mut payload) = received {
            let start = (bytes.as_ptr() as usize).checked_sub(payload.bytes.as_ptr() as usize);
            if start
                .and_then(|s| s.checked_add(bytes.len()))
                .is_some_and(|end| end <= payload.bytes.len())
            {
                payload.bytes = bytes;
                return Ok(payload);
            }
        }
        Ok(bytes.into())
    }
}

/// One allocation, sized before receive. I/O sees only the logical window;
/// padding is initialized once and never receives HTTP framing or payload.
pub struct AlignedBuf {
    allocation: Vec<u8>,
    start: usize,
    len: usize,
    alignment: DirectIoAlignment,
}
impl AlignedBuf {
    /// Allocate an aligned logical receive window and initialized tail padding.
    pub fn new(len: usize, alignment: DirectIoAlignment) -> io::Result<Self> {
        let total = alignment
            .round_len(len)?
            .checked_add(alignment.memory() - 1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "download too large"))?;
        let mut allocation = Vec::new();
        allocation
            .try_reserve_exact(total)
            .map_err(|_| io::Error::other("download buffer allocation failed"))?;
        allocation.resize(total, 0);
        let start = allocation.as_ptr().align_offset(alignment.memory());
        Ok(Self {
            allocation,
            start,
            len,
            alignment,
        })
    }

    /// Freeze a completed receive without copying or exposing padding as data.
    ///
    /// Panics if `len` exceeds the receive window.
    pub fn freeze(mut self, len: usize) -> Payload {
        assert!(len <= self.len);
        self.len = self
            .alignment
            .round_len(len)
            .expect("length fits allocation");
        let backing = Bytes::from_owner(self);
        Payload {
            bytes: Bytes::slice(&backing, ..len),
            backing: Some((backing, len)),
        }
    }
}
impl AsRef<[u8]> for AlignedBuf {
    fn as_ref(&self) -> &[u8] {
        &self.allocation[self.start..self.start + self.len]
    }
}
// SAFETY: the allocation never moves after submission. All exposed bytes are
// initialized and its owner remains alive through I/O completion.
unsafe impl IoBuf for AlignedBuf {
    fn read_ptr(&self) -> *const u8 {
        self.as_ref().as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.len
    }
}
unsafe impl IoBufMut for AlignedBuf {
    fn write_ptr(&mut self) -> *mut u8 {
        self.allocation[self.start..].as_mut_ptr()
    }
    fn bytes_total(&mut self) -> usize {
        self.len
    }
    unsafe fn set_init(&mut self, _len: usize) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn padding_and_page_slices_keep_the_receive_pointer() {
        for (memory, offset) in [(512, 512), (4096, 512), (4096, 4096)] {
            let alignment = DirectIoAlignment::new(memory, offset).unwrap();
            for len in [0, 1, 511, 512, 6000, 8192, 12289] {
                let buffer = AlignedBuf::new(len, alignment).unwrap();
                let ptr = buffer.as_ref().as_ptr();
                assert_eq!(ptr as usize % memory, 0);
                let payload = buffer.freeze(len);
                assert_eq!(payload.len(), len);
                if len == 0 {
                    assert!(payload.direct_bytes(alignment).is_none());
                    continue;
                }
                let direct = payload.direct_bytes(alignment).unwrap();
                assert_eq!(direct.as_ptr(), ptr);
                assert_eq!(direct.len(), alignment.round_len(len).unwrap());
                assert!(direct[len..].iter().all(|&b| b == 0));
                if len > 8192 {
                    let page = payload.slice(8192..len);
                    drop(payload);
                    let direct = page.direct_bytes(alignment).unwrap();
                    assert_eq!(direct.as_ptr() as usize, ptr as usize + 8192);
                    assert_eq!(page.len(), len - 8192);
                }
            }
        }
    }
    #[test]
    fn never_extends_external_or_interior_slices() {
        let alignment = DirectIoAlignment::new(512, 512).unwrap();
        let payload = AlignedBuf::new(6000, alignment).unwrap().freeze(6000);
        assert!(Payload::from(payload.bytes.clone())
            .direct_bytes(alignment)
            .is_none());
        assert!(payload.slice(0..5999).direct_bytes(alignment).is_none());
        assert!(payload.slice(1..6000).direct_bytes(alignment).is_none());
        assert_eq!(
            payload
                .slice(512..6000)
                .direct_bytes(alignment)
                .unwrap()
                .len(),
            5632
        );
        assert_eq!(
            payload.slice(0..512).direct_bytes(alignment).unwrap().len(),
            512
        );
        assert!(AlignedBuf::new(usize::MAX, alignment).is_err());
    }
    #[tokio::test]
    async fn capture_is_scoped_and_preserves_backend_slices() {
        let alignment = DirectIoAlignment::new(512, 512).unwrap();
        let fetch = || {
            Download::capture(Some(alignment), async {
                let capture = Download::current().unwrap();
                tokio::task::yield_now().await;
                // A retry replaces the previous attempt's retained allocation.
                capture.finish(AlignedBuf::new(100, alignment).unwrap(), 100);
                let bytes = capture.finish(AlignedBuf::new(6000, alignment).unwrap(), 6000);
                tokio::task::yield_now().await;
                Ok::<_, ()>(Bytes::slice(&bytes, 512..))
            })
        };
        let (a, b) = tokio::join!(fetch(), fetch());
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.direct_bytes(alignment).unwrap().as_ptr(), a.as_ptr());
        assert_ne!(a.as_ptr(), b.as_ptr());
        assert!(Download::current().is_none());
        let foreign = Download::capture(Some(alignment), async {
            Download::current()
                .unwrap()
                .finish(AlignedBuf::new(6000, alignment).unwrap(), 6000);
            Ok::<_, ()>(Bytes::from_static(b"other"))
        })
        .await
        .unwrap();
        assert!(foreign.backing.is_none());
    }
}
