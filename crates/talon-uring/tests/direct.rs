use monoio::io::AsyncReadRentExt;
use std::os::fd::AsRawFd;

// Inspect residency before reading the file, so byte equality checks do not
// accidentally hide buffered writes. The fixture's files are owned by this uid.
fn resident_pages(path: &std::path::Path, len: usize) -> Vec<u8> {
    if len == 0 {
        return Vec::new();
    }
    let file = std::fs::File::open(path).unwrap();
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let pages = len.div_ceil(page_size);
    let mut resident = vec![0; pages];
    unsafe {
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
fn probed_direct_writes_pad_in_place_without_populating_page_cache() {
    let root = tempfile::tempdir().unwrap();
    let alignment = talon_uring::DirectIoAlignment::probe(root.path()).unwrap();
    eprintln!(
        "probed memory={} write={}",
        alignment.memory(),
        alignment.offset()
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    let mut ring = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    for len in [0, 31, 511, 512, 4095, 4096, 4097, 6000, 8192, 10240] {
        let expected: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let input = expected.clone();
        let path = root.path().join(len.to_string());
        let target = path.clone();
        let allocated = ring
            .block_on(async move {
                let buffer = talon_uring::AlignedBuf::new(len, alignment).unwrap();
                let address = buffer.as_ref().as_ptr();
                let (result, buffer) = input.as_slice().read_exact(buffer).await;
                result.unwrap();
                let bytes = buffer.freeze(len);
                if len > 0 {
                    assert_eq!(bytes.as_ptr(), address);
                }
                talon_uring::atomic_write_payload(&target, bytes, alignment).await
            })
            .unwrap();
        use std::os::unix::fs::MetadataExt;
        assert_eq!(allocated, std::fs::metadata(&path).unwrap().blocks() * 512);
        let stored_len = alignment.round_len(len).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), stored_len as u64);
        let expected_resident = 0;
        assert!(
            resident_pages(&path, stored_len)
                .iter()
                .all(|&page| page == expected_resident),
            "len={len}"
        );
        let stored = std::fs::read(path).unwrap();
        assert_eq!(&stored[..len], expected);
        assert!(stored[len..].iter().all(|&b| b == 0));
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 10);
    // An external buffer has no retained padding owner; keep its exact length.
    let path = root.path().join("external");
    ring.block_on(talon_uring::atomic_write_payload(
        &path,
        bytes::Bytes::from_static(b"external").into(),
        alignment,
    ))
    .unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 8);
    assert!(resident_pages(&path, 8).iter().all(|&p| p == 1));
    assert_eq!(std::fs::read(path).unwrap(), b"external");
}
