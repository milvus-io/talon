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
pub mod data;
pub mod frame;
pub mod pool;
pub mod runtime;

pub use codec::{
    decode, encode, encode_for_schema, CodecError, ControlMessage, ObjectEntry,
    CONTROL_SCHEMA_VERSION, MIN_CONTROL_SCHEMA_VERSION,
};
pub use data::{
    decode_request, encode_error, encode_request, response_header_ok, DataError, RangeRequest,
};
pub use frame::{Flags, FrameError, FrameHeader, MsgType, HEADER_LEN, MAGIC, PROTOCOL_VERSION};
pub use pool::{Channel, CheckoutError, Connector, Pool, PoolConfig};
pub use runtime::{spawn_blocking, Handler, Server, Shutdown};
