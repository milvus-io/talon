//! # talon-transport
//!
//! Wire-protocol primitives shared by Talon's control and data planes.
//!
//! [`FrameHeader`] is the compact, versioned, fixed-size header that prefixes
//! every frame on both planes. The control plane follows the header with a
//! bincode-encoded [`ControlMessage`] (see [`codec`]); the data plane follows
//! it with raw payload bytes (never wrapped), so the hot path can
//! `sendfile`/`splice` straight from a file into the socket.
//!
//! See [`frame`] for the byte layout.

pub mod codec;
pub mod frame;
pub mod pool;

pub use codec::{decode, encode, CodecError, ControlMessage, CONTROL_SCHEMA_VERSION};
pub use frame::{Flags, FrameError, FrameHeader, MsgType, HEADER_LEN, MAGIC, PROTOCOL_VERSION};
pub use pool::{Channel, CheckoutError, Connector, Pool, PoolConfig};
