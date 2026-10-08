//! Python bindings for the Talon client (#312).
//!
//! Adapts the native Rust client rather than reimplementing URI parsing, stat
//! fallback, range planning, placement, or block reads in the binding.
//!
//! # Threading
//!
//! Every blocking call releases the GIL for its duration, so a threaded data
//! loader is limited by the network rather than serialised on the interpreter.
//! The Rust SDK owns the fixed Tokio I/O threads and connection pools.
//! Python only waits for completion; it never drives socket I/O.

// pyo3 0.22's #[pymethods] expansion converts every returned error through
// Into<PyErr>, which is a no-op when the error already is one. clippy flags the
// generated code; there is no source-level change that avoids it, and the lint
// is not about anything under our control.
#![allow(clippy::useless_conversion)]

use std::sync::Arc;

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use talon_rust_client::{
    parse_uri, Client as RustClient, ClientBuilder, Error as RustError,
    ObjectStat as RustObjectStat,
};

/// Capture language context while the GIL and caller context are still active.
fn capture_trace(
    py: Python<'_>,
    explicit: Option<std::collections::HashMap<String, String>>,
) -> Option<talon_telemetry::TraceContext> {
    if !talon_telemetry::enabled() {
        return None;
    }
    let carrier = explicit.or_else(|| {
        let module = py.import_bound("opentelemetry.propagate").ok()?;
        let carrier = pyo3::types::PyDict::new_bound(py);
        module.getattr("inject").ok()?.call1((&carrier,)).ok()?;
        carrier.extract().ok()
    })?;
    talon_telemetry::TraceContext::from_w3c(
        carrier.get("traceparent")?,
        carrier.get("tracestate").map(String::as_str),
    )
}

#[cfg(feature = "telemetry")]
static TELEMETRY: std::sync::Mutex<
    Option<(talon_telemetry::export::ExportOwner, tracing::Dispatch)>,
> = std::sync::Mutex::new(None);

/// Explicit initialization; Python host provider/subscriber is never replaced.
#[pyfunction]
fn configure_telemetry() -> PyResult<()> {
    #[cfg(feature = "telemetry")]
    {
        let mut session = TELEMETRY.lock().unwrap();
        if session.is_some() {
            return Err(PyValueError::new_err("telemetry already initialized"));
        }
        *session = Some(
            talon_telemetry::export::init_scoped("talon-python")
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        );
        Ok(())
    }
    #[cfg(not(feature = "telemetry"))]
    talon_telemetry::Config::from_env()
        .and_then(talon_telemetry::configure)
        .map_err(PyValueError::new_err)
}

/// Drain clients before shutdown. Export waiting happens without the GIL.
#[pyfunction]
fn shutdown_telemetry(py: Python<'_>) {
    #[cfg(feature = "telemetry")]
    {
        let session = TELEMETRY.lock().unwrap().take();
        if let Some((owner, _)) = session {
            py.allow_threads(move || owner.shutdown());
        }
    }
    let _ = py;
}

fn with_telemetry<T>(f: impl FnOnce() -> T) -> T {
    #[cfg(feature = "telemetry")]
    if talon_telemetry::enabled() {
        let dispatch = TELEMETRY.lock().unwrap().as_ref().map(|(_, d)| d.clone());
        if let Some(dispatch) = dispatch {
            return tracing::dispatcher::with_default(&dispatch, f);
        }
    }
    f()
}

/// Preserve the SDK's input-versus-I/O error distinction at the Python boundary.
fn client_err(error: RustError) -> PyErr {
    let message = error.to_string();
    match error {
        RustError::InvalidUri(_) | RustError::InvalidArgument(_) => PyValueError::new_err(message),
        RustError::Coordinator(_) | RustError::Block(_) => PyIOError::new_err(message),
    }
}

fn known_stat_from_pair(
    known_version: Option<String>,
    known_size: Option<u64>,
) -> PyResult<Option<RustObjectStat>> {
    match (known_version, known_size) {
        (Some(version), Some(size)) => Ok(Some(RustObjectStat { size, version })),
        (None, None) => Ok(None),
        _ => Err(PyValueError::new_err(
            "version and size must be supplied together",
        )),
    }
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
    client: Arc<RustClient>,
}

#[pymethods]
impl Client {
    /// Connect to a coordinator.
    ///
    /// `block_size` must match the workers' configured block size; placement is
    /// computed per block, so a mismatch addresses the wrong blocks. It
    /// defaults to the worker default of 256 MiB.
    /// `max_idle_per_addr` is the positive idle connection limit per peer in
    /// both coordinator and worker pools; it does not limit active connections.
    #[new]
    #[pyo3(signature = (coordinator, *, block_size = 256 << 20, max_idle_per_addr = 8))]
    fn new(coordinator: &str, block_size: u32, max_idle_per_addr: usize) -> PyResult<Self> {
        let client = ClientBuilder::default()
            .with_coordinator(coordinator)
            .with_block_size(block_size)
            .with_max_idle_per_addr(max_idle_per_addr)
            .build()
            .map_err(client_err)?;
        Ok(Self {
            client: Arc::new(client),
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
    /// `version` and `size` must be supplied together or both omitted. A supplied
    /// pair skips `stat` and pins the read to that exact source generation.
    /// Supplying only one raises `ValueError`, since metadata from different
    /// generations cannot safely be combined.
    #[pyo3(signature = (uri, *, offset = 0, length = None, version = None, size = None, trace_context = None))]
    #[allow(clippy::too_many_arguments)]
    fn read<'py>(
        &self,
        py: Python<'py>,
        uri: &str,
        offset: u64,
        length: Option<u64>,
        version: Option<&str>,
        size: Option<u64>,
        trace_context: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let trace_context = capture_trace(py, trace_context);
        let object = parse_uri(uri).map_err(|error| PyValueError::new_err(error.to_string()))?;
        let known_stat = known_stat_from_pair(version.map(str::to_owned), size)?;
        let client = Arc::clone(&self.client);

        // Release the GIL: this is network I/O, and holding it would serialise
        // every reader thread in the process on one request.
        let bytes = py.allow_threads(move || {
            with_telemetry(|| {
                futures::executor::block_on(async move {
                    let operation = talon_telemetry::Operation::new(
                        "talon.python.read",
                        "internal",
                        trace_context
                            .as_ref()
                            .map(talon_telemetry::TraceParent::Explicit)
                            .unwrap_or(talon_telemetry::TraceParent::Root),
                    );
                    let result = operation
                        .scope(async {
                            client
                                .read(&object, offset, length, known_stat.as_ref())
                                .await
                        })
                        .await;
                    operation.outcome(if result.is_ok() { "success" } else { "error" });
                    result
                })
            })
        });
        let bytes = bytes.map_err(client_err)?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Return an object's size and version.
    #[pyo3(signature = (uri, *, trace_context = None))]
    fn stat(
        &self,
        py: Python<'_>,
        uri: &str,
        trace_context: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<ObjectStat> {
        let trace_context = capture_trace(py, trace_context);
        let object = parse_uri(uri).map_err(|error| PyValueError::new_err(error.to_string()))?;
        let client = Arc::clone(&self.client);
        let stat = py.allow_threads(move || {
            with_telemetry(|| {
                futures::executor::block_on(async move {
                    let options = talon_telemetry::RequestOptions {
                        parent: trace_context
                            .as_ref()
                            .map(talon_telemetry::TraceParent::Explicit)
                            .unwrap_or(talon_telemetry::TraceParent::Root),
                    };
                    client.stat_with_options(&object, &options).await
                })
            })
        });
        let stat = stat.map_err(client_err)?;
        Ok(ObjectStat {
            size: stat.size,
            version: stat.version,
        })
    }

    /// List objects under a mount-relative prefix, e.g. `az/container/dir`.
    ///
    /// The prefix names a backend and bucket (`az/container`), optionally
    /// followed by a key prefix. Returned paths are in the same namespace, so
    /// they can be passed straight to [`read`](Self::read) after converting to
    /// a URI.
    ///
    /// The control protocol carries one bounded response. If a prefix exceeds
    /// the server's object, page, or payload limit, the call fails explicitly
    /// instead of returning an incomplete list; use a narrower prefix.
    fn list(&self, py: Python<'_>, prefix: &str) -> PyResult<Vec<ObjectEntry>> {
        let client = Arc::clone(&self.client);
        let prefix = prefix.to_string();
        let entries = py.allow_threads(move || {
            futures::executor::block_on(async move { client.list(&prefix).await })
        });
        let entries = entries.map_err(client_err)?;
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
        self.client.coordinator_addr()
    }

    fn __repr__(&self) -> String {
        format!(
            "Client(coordinator={:?}, block_size={})",
            self.client.coordinator_addr(),
            self.client.block_size()
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
    m.add_function(wrap_pyfunction!(configure_telemetry, m)?)?;
    m.add_function(wrap_pyfunction!(shutdown_telemetry, m)?)?;
    m.add_class::<Client>()?;
    m.add_class::<ObjectStat>()?;
    m.add_class::<ObjectEntry>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_accepts_pool_limit_keyword_and_rejects_zero() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let client_type = py.get_type_bound::<Client>();
            client_type.call1(("127.0.0.1:7000",)).unwrap();
            let kwargs = pyo3::types::PyDict::new_bound(py);
            for limit in [1, 32] {
                kwargs.set_item("max_idle_per_addr", limit).unwrap();
                client_type
                    .call(("127.0.0.1:7000",), Some(&kwargs))
                    .unwrap();
            }
            kwargs.set_item("max_idle_per_addr", 0).unwrap();
            let error = client_type
                .call(("127.0.0.1:7000",), Some(&kwargs))
                .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn version_and_size_must_describe_one_generation() {
        assert!(known_stat_from_pair(Some("v1".into()), None).is_err());
        assert!(known_stat_from_pair(None, Some(4096)).is_err());

        let complete = known_stat_from_pair(Some("v1".into()), Some(4096))
            .unwrap()
            .unwrap();
        assert_eq!(complete.version, "v1");
        assert_eq!(complete.size, 4096);
        assert!(known_stat_from_pair(None, None).unwrap().is_none());
    }

    #[test]
    fn invalid_read_argument_raises_value_error() {
        pyo3::prepare_freethreaded_python();
        let client = Client::new("unused", 1, 8).unwrap();
        let oversized = (isize::MAX as u64).saturating_add(1);

        Python::with_gil(|py| {
            let error = match client.read(
                py,
                "s3://bucket/key",
                0,
                Some(oversized),
                Some("version"),
                Some(oversized),
                None,
            ) {
                Ok(_) => panic!("oversized read must fail"),
                Err(error) => error,
            };

            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn coordinator_failure_raises_io_error() {
        pyo3::prepare_freethreaded_python();
        let client = Client::new("127.0.0.1:0", 1, 8).unwrap();

        Python::with_gil(|py| {
            let error = match client.stat(py, "s3://bucket/key", None) {
                Ok(_) => panic!("stat without a coordinator must fail"),
                Err(error) => error,
            };

            assert!(error.is_instance_of::<PyIOError>(py));
        });
    }
}
