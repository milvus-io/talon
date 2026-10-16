//! Cache-filesystem geometry, queried once during store initialization.
use std::{
    io,
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, MetadataExt},
    },
    path::Path,
};

/// Independent buffer-address and file-offset/length requirements for direct I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectIoAlignment {
    memory: usize,
    offset: usize,
}
impl DirectIoAlignment {
    /// Construct geometry with nonzero, power-of-two alignments.
    pub fn new(memory: usize, offset: usize) -> io::Result<Self> {
        if !memory.is_power_of_two() || !offset.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid direct I/O alignment",
            ));
        }
        Ok(Self { memory, offset })
    }
    /// Required buffer-address alignment in bytes.
    pub fn memory(self) -> usize {
        self.memory
    }
    /// Required file-offset and write-length alignment in bytes.
    pub fn offset(self) -> usize {
        self.offset
    }
    /// Round a logical length up to the write alignment, rejecting overflow.
    pub fn round_len(self, len: usize) -> io::Result<usize> {
        len.checked_add(self.offset - 1)
            .map(|n| n & !(self.offset - 1))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "direct I/O length overflow")
            })
    }
    /// Probe a regular file in the actual cache directory, not the directory's
    /// block size or a guessed device sector size. Older kernels without
    /// STATX_DIOALIGN use verified direct writes to this private temporary file.
    pub fn probe(root: &Path) -> io::Result<Self> {
        // Unlinked temporary inode: even a startup crash leaves no probe file.
        let file = tempfile::tempfile_in(root)?;
        let mut stat: libc::statx = unsafe { std::mem::zeroed() };
        // SAFETY: the descriptor is live, the empty path is NUL terminated and
        // stat points to writable storage of the ABI-prescribed size.
        let result = unsafe {
            libc::statx(
                file.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH,
                libc::STATX_DIOALIGN,
                &mut stat,
            )
        };
        if result == 0 && stat.stx_mask & libc::STATX_DIOALIGN != 0 {
            return Self::from_statx(&stat);
        }
        if result != 0 {
            let error = io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS | libc::EINVAL | libc::EOPNOTSUPP)
            ) {
                return Err(error);
            }
        }
        Self::probe_writes(&file)
    }
    fn probe_writes(file: &std::fs::File) -> io::Result<Self> {
        // The filesystem block size and VM page size only bound the search;
        // neither is assumed to be the minimum direct-I/O alignment.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(io::Error::other("cannot query VM page size"));
        }
        let page_size = page_size as usize;
        let block_size = usize::try_from(file.metadata()?.blksize()).map_err(io::Error::other)?;
        let bound = page_size
            .max(block_size)
            .checked_next_power_of_two()
            .ok_or_else(|| io::Error::other("direct I/O probe size overflow"))?;
        let capacity = bound
            .checked_mul(2)
            .ok_or_else(|| io::Error::other("direct I/O probe size overflow"))?;
        let buffer = crate::AlignedBuf::new(capacity, Self::new(bound, bound)?)?;
        // SAFETY: the fd is live and exclusively owned by this probe. Set
        // O_DIRECT before any writes; never mix it with buffered writes.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags | libc::O_DIRECT) } < 0
        {
            return Err(io::Error::last_os_error());
        }
        let write = |start: usize, len: usize, offset: usize| -> io::Result<bool> {
            let written = loop {
                match file.write_at(&buffer.as_ref()[start..start + len], offset as u64) {
                    Ok(written) => break written,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) if error.raw_os_error() == Some(libc::EINVAL) => return Ok(false),
                    Err(error) => return Err(error),
                }
            };
            if written != len {
                return Err(io::Error::other("short direct I/O probe write"));
            }
            // Success alone is insufficient: some filesystems silently buffer
            // misaligned writes. mincore must see no resident file pages.
            verify_uncached(file, page_size)?;
            Ok(true)
        };
        if !write(0, bound, 0)? {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cannot establish direct I/O probe alignment",
            ));
        }
        let find = |test: &dyn Fn(usize) -> io::Result<bool>| -> io::Result<usize> {
            let mut alignment = 1;
            loop {
                if test(alignment)? {
                    return Ok(alignment);
                }
                if alignment == bound {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "cannot establish direct I/O probe alignment",
                    ));
                }
                alignment *= 2;
            }
        };
        let offset = find(&|n| Ok(write(0, n, 0)? && write(0, bound, n)?))?;
        let memory = find(&|n| write(n, bound, 0))?;
        Self::new(memory, offset)
    }

    fn from_statx(stat: &libc::statx) -> io::Result<Self> {
        if stat.stx_mask & libc::STATX_DIOALIGN == 0
            || stat.stx_dio_mem_align == 0
            || stat.stx_dio_offset_align == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cache file does not report supported Direct I/O alignment",
            ));
        }
        Self::new(
            stat.stx_dio_mem_align as usize,
            stat.stx_dio_offset_align as usize,
        )
    }
}
/// Query file-page residency without faulting any data into the mapping.
fn verify_uncached(file: &std::fs::File, page_size: usize) -> io::Result<()> {
    let len = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
    let mut resident = vec![0u8; len.div_ceil(page_size)];
    // SAFETY: this exclusively owned file has nonzero length after the probe
    // write. The mapping is not dereferenced and is unmapped on every path;
    // mincore's vector has one byte for every mapped VM page.
    unsafe {
        let mapping = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        );
        if mapping == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let result = libc::mincore(mapping, len, resident.as_mut_ptr());
        let error = io::Error::last_os_error();
        libc::munmap(mapping, len);
        if result != 0 {
            return Err(error);
        }
    }
    if resident.iter().any(|page| page & 1 != 0) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "direct I/O probe populated the page cache",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn probe_rejects_buffered_write_success() {
        use std::io::Write;
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"buffered").unwrap();
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        assert_eq!(
            verify_uncached(&file, page_size).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[test]
    fn unsupported_is_not_zero_alignment() {
        let mut stat: libc::statx = unsafe { std::mem::zeroed() };
        assert!(DirectIoAlignment::from_statx(&stat).is_err());
        stat.stx_mask = libc::STATX_DIOALIGN;
        assert!(DirectIoAlignment::from_statx(&stat).is_err());
        stat.stx_dio_mem_align = 4096;
        stat.stx_dio_offset_align = 512;
        assert_eq!(
            DirectIoAlignment::from_statx(&stat).unwrap(),
            DirectIoAlignment::new(4096, 512).unwrap()
        );
        assert!(DirectIoAlignment::new(0, 512).is_err());
        assert!(DirectIoAlignment::new(512, 513).is_err());
    }
}
