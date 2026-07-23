//! Inbound-frame resource limits and a safe framed reader (issue #111).
//!
//! The naive accept-loop pattern reads a [`FrameHeader`] and then immediately
//! allocates `header.length` bytes (up to [`MAX_PAYLOAD_LEN`] = 320 MiB) *before*
//! checking the message type, with no read timeout. An unauthenticated peer can
//! advertise a huge length, send nothing, and pin a committed 320 MiB buffer per
//! connection indefinitely; a handful of connections exhaust memory.
//!
//! [`read_frame`] closes that hole:
//! - it validates the advertised length against a **per-message-type cap**
//!   *before* allocating — control/ping frames are capped far below the
//!   data-plane maximum, so a control listener never allocates 320 MiB;
//! - it bounds both the header and payload reads with a **timeout**, so a peer
//!   that stalls mid-frame is dropped instead of holding the buffer forever.
//!
//! Connection-count limiting is orthogonal and provided by [`ConnectionLimit`],
//! a cloneable semaphore whose permits are held for a connection's lifetime.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::frame::{FrameHeader, MsgType, HEADER_LEN, MAX_PAYLOAD_LEN};

/// Maximum payload for a control-plane frame (bincode `ControlMessage`). These
/// are small membership/placement messages; 1 MiB is generous and keeps a
/// control listener from ever committing a data-plane-sized buffer.
pub const MAX_CONTROL_PAYLOAD_LEN: u32 = 1 << 20;

/// Maximum payload for a `Ping` frame — it carries no payload at all.
pub const MAX_PING_PAYLOAD_LEN: u32 = 0;

/// Default timeout for reading a single frame (header + payload). A peer that
/// cannot deliver a frame within this window is dropped.
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The maximum accepted payload length for a given message type.
///
/// Data-plane frames (`Get`/`GetRange`/`Put`) may be large (a block), so they
/// keep the transport maximum; control and ping frames are capped tightly.
pub fn max_payload_for(msg_type: MsgType) -> u32 {
    match msg_type {
        MsgType::Control => MAX_CONTROL_PAYLOAD_LEN,
        MsgType::Ping => MAX_PING_PAYLOAD_LEN,
        MsgType::Get | MsgType::GetRange | MsgType::Put => MAX_PAYLOAD_LEN,
    }
}

/// Error from [`read_frame`].
#[derive(Debug, thiserror::Error)]
pub enum ReadFrameError {
    /// The connection closed cleanly before a frame started.
    #[error("connection closed")]
    Eof,
    /// A frame's advertised length exceeds the cap for its message type.
    #[error("payload length {length} exceeds cap {cap} for {msg_type:?}")]
    PayloadTooLarge {
        /// The message type whose cap was exceeded.
        msg_type: MsgType,
        /// The advertised payload length.
        length: u32,
        /// The cap for that message type.
        cap: u32,
    },
    /// The header could not be decoded (bad magic/version/type/oversized).
    #[error("invalid frame header: {0}")]
    Header(#[from] crate::frame::FrameError),
    /// Reading the header or payload timed out.
    #[error("timed out reading frame")]
    Timeout,
    /// Transport I/O error.
    #[error("io error: {0}")]
    Io(io::Error),
}

/// Read exactly one frame from `stream`, allocating the payload buffer **only
/// after** the message type's size cap is satisfied, and bounding the read with
/// `timeout`.
///
/// Returns the decoded header and the raw payload bytes. A clean EOF at a frame
/// boundary is [`ReadFrameError::Eof`].
pub async fn read_frame<R>(
    stream: &mut R,
    timeout: Duration,
) -> Result<(FrameHeader, Vec<u8>), ReadFrameError>
where
    R: AsyncRead + Unpin,
{
    let mut header_buf = [0u8; HEADER_LEN];
    match tokio::time::timeout(timeout, stream.read_exact(&mut header_buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(ReadFrameError::Eof),
        Ok(Err(e)) => return Err(ReadFrameError::Io(e)),
        Err(_) => return Err(ReadFrameError::Timeout),
    }

    // Decode the header (validates magic/version/type and the global max), then
    // enforce the per-type cap BEFORE allocating the payload.
    let header = FrameHeader::decode(&header_buf)?;
    let cap = max_payload_for(header.msg_type);
    if header.length > cap {
        return Err(ReadFrameError::PayloadTooLarge {
            msg_type: header.msg_type,
            length: header.length,
            cap,
        });
    }

    let mut payload = vec![0u8; header.length as usize];
    match tokio::time::timeout(timeout, stream.read_exact(&mut payload)).await {
        Ok(Ok(_)) => Ok((header, payload)),
        Ok(Err(e)) => Err(ReadFrameError::Io(e)),
        Err(_) => Err(ReadFrameError::Timeout),
    }
}

/// A cloneable connection-count limiter backed by a semaphore.
///
/// Acquire a permit before serving a connection and hold it (via the returned
/// [`OwnedSemaphorePermit`]) for the connection's lifetime; the accept loop
/// bounds concurrency to the configured maximum.
#[derive(Clone)]
pub struct ConnectionLimit {
    semaphore: Arc<Semaphore>,
}

impl ConnectionLimit {
    /// Create a limiter allowing at most `max` concurrent connections.
    pub fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max.max(1))),
        }
    }

    /// Acquire a permit, waiting if the limit is currently reached. The permit
    /// is released when dropped (i.e. when the connection ends).
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("connection semaphore is never closed")
    }

    /// Permits currently available (for tests/metrics).
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::Flags;

    fn frame_bytes(msg_type: MsgType, length_override: Option<u32>, payload: &[u8]) -> Vec<u8> {
        let header = FrameHeader {
            msg_type,
            flags: Flags::default(),
            request_id: 1,
            length: length_override.unwrap_or(payload.len() as u32),
        };
        let mut buf = header.encode().to_vec();
        buf.extend_from_slice(payload);
        buf
    }

    #[tokio::test]
    async fn reads_a_valid_control_frame() {
        let bytes = frame_bytes(MsgType::Control, None, b"hello");
        let mut cursor = std::io::Cursor::new(bytes);
        let (header, payload) = read_frame(&mut cursor, DEFAULT_READ_TIMEOUT).await.unwrap();
        assert_eq!(header.msg_type, MsgType::Control);
        assert_eq!(payload, b"hello");
    }

    #[tokio::test]
    async fn oversized_control_frame_is_rejected_before_alloc() {
        // A control header advertising a data-plane-sized payload is rejected by
        // the per-type cap without allocating it. We only write the header, so a
        // reader that tried to allocate+read the payload would instead time out
        // or hang; the cap check fires first on the header alone.
        let header = FrameHeader {
            msg_type: MsgType::Control,
            flags: Flags::default(),
            request_id: 1,
            length: MAX_CONTROL_PAYLOAD_LEN + 1,
        };
        let mut cursor = std::io::Cursor::new(header.encode().to_vec());
        let err = read_frame(&mut cursor, DEFAULT_READ_TIMEOUT)
            .await
            .unwrap_err();
        assert!(matches!(err, ReadFrameError::PayloadTooLarge { .. }));
    }

    #[tokio::test]
    async fn clean_eof_is_reported() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        let err = read_frame(&mut cursor, DEFAULT_READ_TIMEOUT)
            .await
            .unwrap_err();
        assert!(matches!(err, ReadFrameError::Eof));
    }

    #[tokio::test]
    async fn data_frame_keeps_the_large_cap() {
        assert_eq!(max_payload_for(MsgType::GetRange), MAX_PAYLOAD_LEN);
        assert_eq!(max_payload_for(MsgType::Control), MAX_CONTROL_PAYLOAD_LEN);
        assert_eq!(max_payload_for(MsgType::Ping), 0);
    }

    #[tokio::test]
    async fn connection_limit_bounds_permits() {
        let limit = ConnectionLimit::new(2);
        assert_eq!(limit.available(), 2);
        let _p1 = limit.acquire().await;
        let _p2 = limit.acquire().await;
        assert_eq!(limit.available(), 0);
        drop(_p1);
        assert_eq!(limit.available(), 1);
    }
}
