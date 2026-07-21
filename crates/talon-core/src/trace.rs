//! Structured tracing helpers for the request lifecycle.
//!
//! Thin conveniences over the [`tracing`] crate that keep span **fields
//! consistent with the metrics labels** (`backend`, `form`, `block`, `worker`)
//! and thread a [`RequestId`] across control/data hops so a single request is
//! traceable end-to-end.
//!
//! Spans are created at `debug`/`info` level; when the subscriber disables that
//! level the macros compile to near-nothing, so the hot path pays almost no
//! cost. Use [`init_tracing`] once per process to install an `EnvFilter`-based
//! subscriber (honoring `RUST_LOG`).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// A process-unique request identifier, propagated across hops.
///
/// Rendered as zero-padded hex so it groups nicely in logs. The `u32` matches
/// the frame header's `request_id` field so the same value can travel on the
/// wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(pub u32);

impl RequestId {
    /// Allocate the next monotonically increasing request id for this process.
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        RequestId(COUNTER.fetch_add(1, Ordering::Relaxed) as u32)
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

/// The physical form of a block, as a span/label value (matches metrics).
pub const FORM_WHOLE: &str = "whole";
/// The physical form of a block, as a span/label value (matches metrics).
pub const FORM_PAGED: &str = "paged";

/// Install a global `tracing` subscriber driven by `RUST_LOG` (default `info`).
///
/// Idempotent-friendly: returns `false` if a subscriber was already set (e.g.
/// in tests) instead of panicking.
pub fn init_tracing() -> bool {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .is_ok()
}

/// Create an `info` span for a GET request lifecycle.
///
/// Fields: `req`, `block` (its `Display`), and `form`. Kept in a macro so the
/// call site owns the span guard and the fields are recorded lazily.
#[macro_export]
macro_rules! get_span {
    ($req:expr, $block:expr, $form:expr) => {
        ::tracing::info_span!(
            "get",
            req = %$req,
            block = %$block,
            form = $form,
        )
    };
}

/// Create an `info` span for a PUT/ingest lifecycle.
#[macro_export]
macro_rules! put_span {
    ($req:expr, $block:expr) => {
        ::tracing::info_span!(
            "put",
            req = %$req,
            block = %$block,
        )
    };
}

/// Create an `info` span for a LOAD/prewarm lifecycle.
#[macro_export]
macro_rules! load_span {
    ($req:expr, $backend:expr, $object:expr) => {
        ::tracing::info_span!(
            "load",
            req = %$req,
            backend = %$backend,
            object = %$object,
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_are_monotonic_and_formatted() {
        let a = RequestId::next();
        let b = RequestId::next();
        assert!(b.0 > a.0);
        assert_eq!(format!("{}", RequestId(0x2a)), "0000002a");
    }

    #[test]
    fn spans_compile_and_enter() {
        // Without a subscriber these are cheap no-ops, but must still build and
        // enter cleanly with the expected field types.
        use crate::{Backend, BlockId, ObjectId, Version};
        let req = RequestId::next();
        let block = BlockId::new(
            ObjectId::new(Backend::S3, "b", "o"),
            0,
            256 << 20,
            Version::new("v1"),
        );
        let s = get_span!(req, block, FORM_WHOLE);
        let _g = s.enter();
        let p = put_span!(req, block);
        let _g2 = p.enter();
        let l = load_span!(req, block.object.backend, block.object.to_path());
        let _g3 = l.enter();
    }

    #[test]
    fn init_tracing_is_safe_to_call() {
        // May return false if another test already installed a subscriber.
        let _ = init_tracing();
    }
}
