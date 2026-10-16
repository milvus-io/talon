#[test]
#[ignore = "requires a kernel/container allowing native io_uring"]
fn staging_collision_preserves_existing_file() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("block");
    // This test binary has not called atomic_write yet, so its sequence starts at 0.
    let occupied = path.with_file_name(format!("block.tmp.{}.0", std::process::id()));
    std::fs::write(&occupied, b"owned by another writer").unwrap();
    let target = path.clone();
    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap()
        .block_on(async move {
            talon_uring::atomic_write(&target, bytes::Bytes::from_static(b"complete")).await
        })
        .unwrap();
    assert_eq!(std::fs::read(path).unwrap(), b"complete");
    assert_eq!(std::fs::read(occupied).unwrap(), b"owned by another writer");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 2);
}
