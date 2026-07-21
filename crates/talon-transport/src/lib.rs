//! # talon-transport
//!
//! Wire-protocol primitives shared by Talon's control and data planes.
//!
//! The only thing defined here in this first cut is the [`FrameHeader`]: a
//! compact, versioned, fixed-size header that prefixes every frame on both
//! planes. The control plane follows the header with a bincode-encoded message;
//! the data plane follows it with raw payload bytes (never wrapped), so the hot
//! path can `sendfile`/`splice` straight from a file into the socket.
//!
//! See [`frame`] for the byte layout.

pub mod frame;

pub use frame::{Flags, FrameError, FrameHeader, MsgType, HEADER_LEN, MAGIC, PROTOCOL_VERSION};
