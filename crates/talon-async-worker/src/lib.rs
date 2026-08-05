// SPDX-License-Identifier: Apache-2.0
//! # talon-async-worker
//!
//! A worker that caches **extents** — arbitrary byte ranges of an object —
//! rather than fixed 256MB blocks.
//!
//! [`talon-worker`] materializes a whole block on any touch, which is right for
//! a sequential scan and wrong for a query engine reading a Parquet or Lance
//! footer and then cherry-picking column chunks: a few kilobytes of useful data
//! costs a 256MB transfer. This worker caches exactly the range that was asked
//! for, so a 4KB footer read costs 4KB.
//!
//! There is no block concept here at all. A cache entry is keyed by
//! [`ExtentKey`] — an interned `(object, version)` stream id plus a byte offset
//! — and holds whatever length was fetched. Blocks survive only where Talon
//! actually needs them, between the coordinator and the client for placement;
//! the data-plane protocol already speaks `(object, offset, len)` and is
//! unchanged, so this worker is wire-compatible with existing clients.
//!
//! See ADR 0005 for the design and the tradeoffs it accepts.
//!
//! [`talon-worker`]: https://docs.rs/talon-worker

#![deny(missing_docs)]

pub mod cache;

pub use cache::memory::{EvictionSink, MemoryCache, MemoryCacheStats};
pub use cache::ExtentKey;
