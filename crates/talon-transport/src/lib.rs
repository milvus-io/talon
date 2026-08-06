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
pub mod control_tls;
pub mod data;
pub mod frame;
pub mod limits;
pub mod pool;
pub mod uring;

pub use codec::{
    decode, encode, encode_for_schema, CodecError, ControlMessage, ObjectEntry, Ring,
    CONTROL_SCHEMA_VERSION, MIN_CONTROL_SCHEMA_VERSION,
};
pub use data::{
    decode_delete, decode_put_header, decode_request, encode_delete, encode_error,
    encode_put_header, encode_request, response_header_ok, DataError, DeleteRequest, PutRequest,
    RangeRequest,
};
pub use frame::{Flags, FrameError, FrameHeader, MsgType, HEADER_LEN, MAGIC, PROTOCOL_VERSION};
pub use limits::{
    max_payload_for, read_frame, ConnectionLimit, ReadFrameError, DEFAULT_READ_TIMEOUT,
    MAX_CONTROL_PAYLOAD_LEN,
};
pub use pool::{Channel, CheckoutError, Connector, Pool, PoolConfig};
