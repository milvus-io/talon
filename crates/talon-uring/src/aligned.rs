//! Stable, owned download buffers; freezing and slicing never move payload bytes.
use bytes::Bytes;
use monoio::buf::{IoBuf, IoBufMut};
use std::io;

/// Cache data uses a 4 KiB direct-write unit. Filesystems must support this unit;
/// an unsupported O_DIRECT operation is reported, never retried as buffered I/O.
pub const DIRECT_ALIGNMENT: usize = 4096;

/// An aligned window in one allocation. The backing Vec never grows after it
/// has been exposed to I/O. Padding contains no downloaded payload.
pub struct AlignedBuf {
    allocation: Vec<u8>,
    start: usize,
    len: usize,
}
impl AlignedBuf {
    pub fn new(len: usize) -> io::Result<Self> {
        let total = len
            .checked_add(DIRECT_ALIGNMENT - 1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "download too large"))?;
        let mut allocation = Vec::new();
        allocation
            .try_reserve_exact(total)
            .map_err(|_| io::Error::other("download buffer allocation failed"))?;
        allocation.resize(total, 0);
        let start = allocation.as_ptr().align_offset(DIRECT_ALIGNMENT);
        Ok(Self {
            allocation,
            start,
            len,
        })
    }

    /// Transfer ownership into Bytes, retaining the exact allocation and pointer.
    pub fn freeze(mut self, len: usize) -> Bytes {
        assert!(len <= self.len);
        self.len = len;
        Bytes::from_owner(self)
    }
}
impl AsRef<[u8]> for AlignedBuf {
    fn as_ref(&self) -> &[u8] {
        &self.allocation[self.start..self.start + self.len]
    }
}
// SAFETY: the allocation is never resized and ownership is retained by Monoio
// through each completion. All exposed bytes are initialized, including padding.
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
    fn freezing_and_page_slicing_preserve_allocation() {
        let buffer = AlignedBuf::new(12289).unwrap();
        let address = buffer.as_ref().as_ptr() as usize;
        assert_eq!(address % DIRECT_ALIGNMENT, 0);
        let bytes = buffer.freeze(12289);
        assert_eq!(bytes.as_ptr() as usize, address);
        assert_eq!(
            Bytes::slice(&bytes, 4096..8192).as_ptr() as usize,
            address + 4096
        );
        assert_eq!(Bytes::slice(&bytes, 12288..).len(), 1);
    }
}
