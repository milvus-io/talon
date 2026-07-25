//! Shared backend-conformance assertions run by each backend's e2e test against
//! its emulator (LocalStack / fake-gcs / Azurite).
//!
//! Every backend must satisfy the same `BackendStore` contract, so instead of
//! copying assertions into three e2e files, each e2e builds its backend + a
//! seeded object id and calls [`run`] here. This guarantees S3, GCS, and Azure
//! are all held to identical fetch/head/range/version/not-found semantics.
//!
//! The seeded object is expected to be 4096 bytes where byte i == (i % 251).

use std::sync::Arc;

use talon_core::{BackendStore, Error, ObjectId, Version};

/// The size every e2e seeds its object to.
pub const OBJECT_LEN: u64 = 4096;

/// Deterministic content byte for absolute offset `i`.
pub fn content_byte(i: u64) -> u8 {
    (i % 251) as u8
}

/// Run the full conformance suite against `backend`:
/// - `present` is a seeded object of [`OBJECT_LEN`] bytes,
/// - `missing` is an object key that does not exist (for the 404 path),
/// - `check_preconditions` runs the If-Match precondition assertions; disable it
///   for emulators that do not enforce preconditions (fake-gcs-server ignores
///   them, so a stale precondition is not rejected there — a limitation of the
///   emulator, not the backend).
pub async fn run(
    backend: Arc<dyn BackendStore>,
    present: &ObjectId,
    missing: &ObjectId,
    check_preconditions: bool,
) {
    head_reports_size_and_version(&*backend, present).await;
    ranges_return_exact_bytes(&*backend, present).await;
    whole_object_read(&*backend, present).await;
    tail_read_past_eof_clamps(&*backend, present).await;
    if check_preconditions {
        let version = backend.head(present).await.unwrap().version;
        matching_precondition_succeeds(&*backend, present, &version).await;
        stale_precondition_is_rejected(&*backend, present).await;
    }
    missing_object_is_not_found(&*backend, missing).await;
}

async fn head_reports_size_and_version(backend: &dyn BackendStore, obj: &ObjectId) {
    let stat = backend.head(obj).await.expect("HEAD should succeed");
    assert_eq!(stat.len, OBJECT_LEN, "HEAD reported wrong size");
    assert!(
        !stat.version.as_str().is_empty(),
        "HEAD must return a non-empty version"
    );
}

async fn ranges_return_exact_bytes(backend: &dyn BackendStore, obj: &ObjectId) {
    // A few ranges: the head, an interior window, and a small aligned window.
    for (offset, len) in [(0u64, 16u64), (1000, 256), (2048, 512)] {
        let got = backend
            .fetch_range(obj, offset, len)
            .await
            .unwrap_or_else(|e| panic!("fetch_range({offset},{len}) failed: {e:?}"));
        assert_eq!(got.len(), len as usize, "range length mismatch");
        let expected: Vec<u8> = (offset..offset + len).map(content_byte).collect();
        assert_eq!(&got[..], &expected[..], "range bytes mismatch at {offset}");
    }
}

async fn whole_object_read(backend: &dyn BackendStore, obj: &ObjectId) {
    let got = backend
        .fetch_range(obj, 0, OBJECT_LEN)
        .await
        .expect("whole-object read");
    assert_eq!(got.len(), OBJECT_LEN as usize);
    let expected: Vec<u8> = (0..OBJECT_LEN).map(content_byte).collect();
    assert_eq!(&got[..], &expected[..]);
}

async fn tail_read_past_eof_clamps(backend: &dyn BackendStore, obj: &ObjectId) {
    // Request a window that runs past EOF; the backend returns just the tail.
    let offset = OBJECT_LEN - 100;
    let got = backend
        .fetch_range(obj, offset, 4096)
        .await
        .expect("tail read");
    assert_eq!(got.len(), 100, "tail read should clamp at EOF");
    let expected: Vec<u8> = (offset..OBJECT_LEN).map(content_byte).collect();
    assert_eq!(&got[..], &expected[..]);
}

// Only invoked when preconditions are checked; the gcs binary compiles this
// module without calling them, so silence its per-binary dead-code warning.
#[allow(dead_code)]
async fn matching_precondition_succeeds(
    backend: &dyn BackendStore,
    obj: &ObjectId,
    version: &Version,
) {
    let got = backend
        .fetch_range_if_match(obj, 0, 16, Some(version))
        .await
        .expect("If-Match with the current version should succeed");
    let expected: Vec<u8> = (0..16).map(content_byte).collect();
    assert_eq!(&got[..], &expected[..]);
}

#[allow(dead_code)]
async fn stale_precondition_is_rejected(backend: &dyn BackendStore, obj: &ObjectId) {
    // A wrong version must be rejected as a VersionMismatch, not silently served.
    let bogus = Version::new("0xDEADBEEFDEADBEEF");
    match backend.fetch_range_if_match(obj, 0, 16, Some(&bogus)).await {
        Err(Error::VersionMismatch { .. }) => {}
        other => panic!("stale If-Match should be VersionMismatch, got {other:?}"),
    }
}

async fn missing_object_is_not_found(backend: &dyn BackendStore, missing: &ObjectId) {
    match backend.head(missing).await {
        Err(Error::NotFound(_)) => {}
        other => panic!("HEAD of a missing object should be NotFound, got {other:?}"),
    }
    match backend.fetch_range(missing, 0, 16).await {
        Err(Error::NotFound(_)) => {}
        other => panic!("GET of a missing object should be NotFound, got {other:?}"),
    }
}
