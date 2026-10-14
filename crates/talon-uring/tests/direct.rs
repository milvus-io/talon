use monoio::io::AsyncReadRentExt;
use std::os::fd::AsRawFd;

// Inspect residency before reading the file, so byte equality checks do not
// accidentally hide buffered writes. The fixture's files are owned by this uid.
fn resident_pages(path: &std::path::Path, len: usize) -> Vec<u8> {
    if len == 0 {
        return Vec::new();
    }
    let file = std::fs::File::open(path).unwrap();
    let pages = len.div_ceil(4096);
    let mut resident = vec![0; pages];
    unsafe {
        assert_eq!(libc::sysconf(libc::_SC_PAGESIZE), 4096);
        let mapping = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        );
        assert_ne!(mapping, libc::MAP_FAILED);
        let result = libc::mincore(mapping, len, resident.as_mut_ptr());
        let error = std::io::Error::last_os_error();
        assert_eq!(libc::munmap(mapping, len), 0);
        assert_eq!(result, 0, "{error}");
    }
    resident.into_iter().map(|value| value & 1).collect()
}

#[test]
#[ignore = "requires native io_uring and a filesystem supporting O_DIRECT"]
fn direct_aligned_files_and_buffered_other_lengths_preserve_exact_bytes() {
    let root = tempfile::tempdir().unwrap();
    let mut ring = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    for len in [0, 31, 4095, 4096, 4097, 8192, 10240] {
        let expected: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let input = expected.clone();
        let path = root.path().join(len.to_string());
        let target = path.clone();
        ring.block_on(async move {
            let buffer = talon_uring::AlignedBuf::new(len).unwrap();
            let address = buffer.as_ref().as_ptr();
            let (result, buffer) = input.as_slice().read_exact(buffer).await;
            result.unwrap();
            let bytes = buffer.freeze(len);
            assert_eq!(bytes.as_ptr(), address);
            talon_uring::atomic_write_payload(&target, bytes).await
        })
        .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len as u64);
        let expected_resident = u8::from(len % 4096 != 0);
        assert!(
            resident_pages(&path, len)
                .iter()
                .all(|&page| page == expected_resident),
            "len={len}"
        );
        assert_eq!(std::fs::read(path).unwrap(), expected);
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 7);
}
