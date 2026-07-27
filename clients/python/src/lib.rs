//! Python bindings for the Talon client (#312).
//!
//! Binds the Rust read path rather than reimplementing it. Block splitting,
//! placement caching, replica fallback, and connection pooling already exist in
//! `talon-fuse` and are exercised by the FUSE client; duplicating them in
//! Python would duplicate their bugs too.
//!
//! `talon-fuse`'s default features exclude `fuser`, so no FUSE or libfuse
//! dependency reaches the extension module.
//!
//! # Threading
//!
//! Every blocking call releases the GIL for its duration, so a threaded data
//! loader is limited by the network rather than serialised on the interpreter.
//! The runtime is a multi-threaded Tokio runtime owned by the client, shared by
//! all calls on it.

// pyo3 0.22's #[pymethods] expansion converts every returned error through
// Into<PyErr>, which is a no-op when the error already is one. clippy flags the
// generated code; there is no source-level change that avoids it, and the lint
// is not about anything under our control.
#![allow(clippy::useless_conversion)]

use std::sync::Arc;

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use talon_core::{ObjectId, Version};
use talon_fuse::block_reader::FileView;
use talon_fuse::{BlockReader, CoordinatorClient, PlacementCache};

/// Placement entries older than this are re-resolved. Matches the FUSE client's
/// default so both see the same staleness window.
const PLACEMENT_TTL_MS: u64 = 30_000;
/// Replicas to request per placement lookup. RF=1 in v1.
const REPLICAS_K: u8 = 1;

/// Milliseconds since the epoch, for placement-cache expiry.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Map a client-side failure to a Python exception, preserving the worker's own
/// message. Flattening these to a generic error would hide the distinction
/// between "object does not exist" and "every replica is down".
fn io_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyIOError::new_err(e.to_string())
}

/// Parse a `scheme://bucket/key` URI into an [`ObjectId`].
///
/// The schemes match the namespaces the FUSE mount exposes (`s3`, `gcs`, `az`),
/// so a path used with one client addresses the same object in the other.
///
/// Returns a plain `String` error rather than a `PyErr` so the parsing rules can
/// be tested without an initialised interpreter; [`parse_uri`] wraps it.
fn parse_uri_inner(uri: &str) -> Result<ObjectId, String> {
    let (scheme, rest) = uri.split_once("://").ok_or_else(|| {
        format!("expected a scheme://bucket/key URI, got {uri:?} (schemes: s3, gcs, az)")
    })?;
    let backend = scheme
        .parse()
        .map_err(|_| format!("unknown backend scheme {scheme:?}; expected s3, gcs, or az"))?;
    let (bucket, key) = rest.split_once('/').ok_or_else(|| {
        format!("URI is missing an object key: {uri:?} (expected {scheme}://bucket/key)")
    })?;
    if bucket.is_empty() {
        return Err(format!("URI has an empty bucket: {uri:?}"));
    }
    if key.is_empty() {
        return Err(format!("URI has an empty object key: {uri:?}"));
    }
    Ok(ObjectId::new(backend, bucket, key))
}

/// An object's size and source version.
#[pyclass(module = "talon", frozen)]
#[derive(Clone)]
pub struct ObjectStat {
    /// Total object length in bytes.
    #[pyo3(get)]
    pub size: u64,
    /// Source version (ETag) the object is currently at.
    #[pyo3(get)]
    pub version: String,
}

#[pymethods]
impl ObjectStat {
    fn __repr__(&self) -> String {
        format!("ObjectStat(size={}, version={:?})", self.size, self.version)
    }
}

/// One entry from a listing: a mount-relative path and its size.
#[pyclass(module = "talon", frozen)]
#[derive(Clone)]
pub struct ObjectEntry {
    /// Mount-relative object path.
    #[pyo3(get)]
    pub path: String,
    /// Object size in bytes.
    #[pyo3(get)]
    pub size: u64,
}

#[pymethods]
impl ObjectEntry {
    fn __repr__(&self) -> String {
        format!("ObjectEntry(path={:?}, size={})", self.path, self.size)
    }
}

/// A client for reading objects through a Talon cache cluster.
#[pyclass(module = "talon")]
pub struct Client {
    runtime: Arc<tokio::runtime::Runtime>,
    coordinator: CoordinatorClient,
    reader: BlockReader,
    block_size: u32,
}

#[pymethods]
impl Client {
    /// Connect to a coordinator.
    ///
    /// `block_size` must match the workers' configured block size; placement is
    /// computed per block, so a mismatch addresses the wrong blocks. It
    /// defaults to the worker default of 256 MiB.
    #[new]
    #[pyo3(signature = (coordinator, *, block_size = 256 << 20))]
    fn new(coordinator: &str, block_size: u32) -> PyResult<Self> {
        if block_size == 0 {
            return Err(PyValueError::new_err("block_size must be non-zero"));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(io_err)?;
        let coordinator_client = CoordinatorClient::new(coordinator);
        let cache = Arc::new(PlacementCache::new(PLACEMENT_TTL_MS));
        let reader = BlockReader::new(coordinator_client.clone(), cache, REPLICAS_K);
        Ok(Self {
            runtime: Arc::new(runtime),
            coordinator: coordinator_client,
            reader,
            block_size,
        })
    }

    /// Read `length` bytes from `uri` starting at `offset`.
    ///
    /// Returns `bytes`. A read at or past end-of-file returns an empty buffer,
    /// and a read overlapping the end is truncated to what exists — POSIX short
    /// read semantics, so callers must check the returned length rather than
    /// assuming they got what they asked for.
    ///
    /// Ranges spanning block boundaries are split and fetched per block, each
    /// benefiting independently from the placement cache.
    ///
    /// `version` and `size` are resolved with a `stat` when omitted. Pass them
    /// to skip that round trip when they are already known — for example when
    /// reading many ranges of the same object.
    #[pyo3(signature = (uri, *, offset = 0, length = None, version = None, size = None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        uri: &str,
        offset: u64,
        length: Option<u64>,
        version: Option<&str>,
        size: Option<u64>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let object = parse_uri_inner(uri).map_err(PyValueError::new_err)?;
        let known_version = version.map(Version::new);
        let runtime = Arc::clone(&self.runtime);
        let reader = self.reader.clone();
        let coordinator = self.coordinator.clone();
        let block_size = self.block_size;

        // Release the GIL: this is network I/O, and holding it would serialise
        // every reader thread in the process on one request.
        let bytes = py.allow_threads(move || {
            runtime.block_on(async move {
                // One stat covers both, so only issue it when something is
                // actually missing.
                let (version, file_size) = match (known_version, size) {
                    (Some(v), Some(s)) => (v, s),
                    (known, known_size) => {
                        let stat = coordinator
                            .stat_object(&object)
                            .await
                            .map_err(|e| e.to_string())?;
                        (
                            known.unwrap_or_else(|| Version::new(stat.version.as_str())),
                            known_size.unwrap_or(stat.size),
                        )
                    }
                };
                let len = length.unwrap_or_else(|| file_size.saturating_sub(offset));
                let file = FileView {
                    object: &object,
                    block_size,
                    version: &version,
                    size: file_size,
                };
                reader
                    .read(&file, offset, len, now_ms())
                    .await
                    .map_err(|e| e.to_string())
            })
        });
        let bytes = bytes.map_err(PyIOError::new_err)?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Return an object's size and version.
    fn stat(&self, py: Python<'_>, uri: &str) -> PyResult<ObjectStat> {
        let object = parse_uri_inner(uri).map_err(PyValueError::new_err)?;
        let runtime = Arc::clone(&self.runtime);
        let coordinator = self.coordinator.clone();
        let stat = py.allow_threads(move || {
            runtime.block_on(async move {
                coordinator
                    .stat_object(&object)
                    .await
                    .map_err(|e| e.to_string())
            })
        });
        let stat = stat.map_err(PyIOError::new_err)?;
        Ok(ObjectStat {
            size: stat.size,
            version: stat.version.as_str().to_string(),
        })
    }

    /// List objects under a mount-relative prefix, e.g. `az/container/dir`.
    ///
    /// **Not usable yet**: `ListObjects` needs a listing capability on
    /// `BackendStore`, which the S3, GCS, and Azure backends do not yet have
    /// (#332). `stat` and `read` are unaffected.
    fn list(&self, py: Python<'_>, prefix: &str) -> PyResult<Vec<ObjectEntry>> {
        let runtime = Arc::clone(&self.runtime);
        let coordinator = self.coordinator.clone();
        let prefix = prefix.to_string();
        let entries = py.allow_threads(move || {
            runtime.block_on(async move {
                coordinator
                    .list_objects(&prefix)
                    .await
                    .map_err(|e| e.to_string())
            })
        });
        let entries = entries.map_err(PyIOError::new_err)?;
        Ok(entries
            .into_iter()
            .map(|e| ObjectEntry {
                path: e.path,
                size: e.size,
            })
            .collect())
    }

    /// The coordinator address this client is connected to.
    #[getter]
    fn coordinator(&self) -> &str {
        self.reader.coordinator_addr()
    }

    fn __repr__(&self) -> String {
        format!(
            "Client(coordinator={:?}, block_size={})",
            self.reader.coordinator_addr(),
            self.block_size
        )
    }

    /// Support `with talon.Client(...) as client:`.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type = None, _exc_value = None, _traceback = None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        // Connections are pooled and closed when the client drops; nothing to
        // do here, but the context-manager protocol is what Python users expect.
        false
    }
}

#[pymodule]
fn talon(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<ObjectStat>()?;
    m.add_class::<ObjectEntry>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::Backend;

    #[test]
    fn parse_uri_accepts_each_backend_scheme() {
        for (uri, backend) in [
            ("s3://bucket/key", Backend::S3),
            ("gcs://bucket/key", Backend::Gcs),
            ("az://container/key", Backend::Azure),
        ] {
            let id = parse_uri_inner(uri).expect("valid uri");
            assert_eq!(id.backend, backend);
            assert_eq!(id.object_path, "key");
        }
    }

    #[test]
    fn parse_uri_keeps_nested_keys_intact() {
        let id = parse_uri_inner("az://container/a/b/c.parquet").expect("valid uri");
        assert_eq!(id.bucket, "container");
        assert_eq!(id.object_path, "a/b/c.parquet");
    }

    /// Malformed URIs must name what is wrong; a bare "invalid input" sends the
    /// caller to the debugger for something the message could have answered.
    #[test]
    fn parse_uri_rejects_malformed_input_with_a_useful_message() {
        for (uri, expected) in [
            ("bucket/key", "scheme://bucket/key"),
            ("ftp://bucket/key", "unknown backend scheme"),
            ("az://bucket", "missing an object key"),
            ("az:///key", "empty bucket"),
            ("az://bucket/", "empty object key"),
        ] {
            let msg = parse_uri_inner(uri).expect_err("should reject");
            assert!(
                msg.contains(expected),
                "error for {uri:?} should mention {expected:?}, got {msg:?}"
            );
        }
    }
}
