//! Per-process runtime scaffolding for the master/worker/client.
//!
//! DESIGN.md's Layer-1 design runs each process on a single-threaded
//! `monoio::Runtime<IoUringDriver>` that owns accept, frame-header read/write,
//! small control messages, scheduling, timers, and metrics — while **large
//! payload bytes never traverse this ring** (they move via `sendfile`/`splice`
//! in the blocking helper pool).
//!
//! This module provides that shape on a portable Tokio backend so it builds and
//! is testable everywhere (io_uring is not always available in CI/sandboxes);
//! the monoio driver is a drop-in swap behind the same [`Server`] API. It gives:
//!
//! - an **accept loop** ([`Server::serve`]) that reads a [`FrameHeader`] per
//!   connection and dispatches to a [`Handler`],
//! - an **off-ring blocking pool** ([`spawn_blocking`]) for the zero-copy
//!   syscalls, so no `sendfile`/`splice` ever blocks the ring, and
//! - **graceful shutdown** via a [`Shutdown`] handle that stops the accept loop
//!   and lets in-flight connections drain.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use crate::frame::FrameHeader;

/// A per-connection request handler.
///
/// Called once per received frame with the decoded header and the payload bytes
/// (already read off the ring for control frames). Returns the bytes to write
/// back (a framed response), or `None` to send nothing.
pub trait Handler: Send + Sync + 'static {
    /// Handle one frame; return an optional response buffer to write back.
    fn handle(&self, header: FrameHeader, payload: Vec<u8>) -> Option<Vec<u8>>;
}

impl<F> Handler for F
where
    F: Fn(FrameHeader, Vec<u8>) -> Option<Vec<u8>> + Send + Sync + 'static,
{
    fn handle(&self, header: FrameHeader, payload: Vec<u8>) -> Option<Vec<u8>> {
        self(header, payload)
    }
}

/// A handle used to request graceful shutdown of a running [`Server`].
///
/// Level-triggered: once [`trigger`](Self::trigger) is called the flag stays
/// set, so a `serve` loop that reaches its `select!` afterwards still observes
/// shutdown (no lost-wakeup race).
#[derive(Clone, Default)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Shutdown {
    /// Create a fresh shutdown handle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal the server to stop accepting new connections and drain.
    pub fn trigger(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether shutdown has been requested.
    pub fn is_triggered(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        // Create the notified future *before* checking the flag so a trigger
        // that races between the check and the await is not lost.
        let notified = self.notify.notified();
        if self.is_triggered() {
            return;
        }
        notified.await;
    }
}

/// The single-ring server: accept + frame-dispatch loop.
pub struct Server<H: Handler> {
    handler: Arc<H>,
}

impl<H: Handler> Server<H> {
    /// Create a server bound to `handler`.
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    /// Bind `addr` and run the accept loop until `shutdown` is triggered.
    ///
    /// Each accepted connection is handled on its own task: it reads
    /// length-prefixed frames (header + `length` payload bytes), dispatches to
    /// the [`Handler`], and writes any response. Payloads are capped by the
    /// header decoder's `MAX_PAYLOAD_LEN` guard.
    pub async fn serve(&self, addr: &str, shutdown: Shutdown) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            tokio::select! {
                _ = shutdown.wait() => {
                    tracing::info!("runtime: shutdown requested, stopping accept loop");
                    return Ok(());
                }
                accepted = listener.accept() => {
                    let (stream, _peer) = accepted?;
                    let handler = Arc::clone(&self.handler);
                    tokio::spawn(async move {
                        if let Err(e) = handle_conn(stream, handler).await {
                            tracing::debug!(error = %e, "runtime: connection ended");
                        }
                    });
                }
            }
        }
    }

    /// Run the accept loop over an already-bound listener (used with [`bind`]).
    pub async fn serve_on(&self, listener: TcpListener, shutdown: Shutdown) -> std::io::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.wait() => return Ok(()),
                accepted = listener.accept() => {
                    let (stream, _peer) = accepted?;
                    let handler = Arc::clone(&self.handler);
                    tokio::spawn(async move {
                        let _ = handle_conn(stream, handler).await;
                    });
                }
            }
        }
    }
}

/// Bind an address for the accept loop, returning the listener + resolved addr.
///
/// Lets a caller (e.g. a test) learn the ephemeral port before the loop starts.
pub async fn bind(addr: &str) -> std::io::Result<(TcpListener, std::net::SocketAddr)> {
    let l = TcpListener::bind(addr).await?;
    let a = l.local_addr()?;
    Ok((l, a))
}

async fn handle_conn<H: Handler>(mut stream: TcpStream, handler: Arc<H>) -> std::io::Result<()> {
    loop {
        // Read one frame with the per-message-type size cap enforced before
        // allocation and a read timeout, so a peer cannot pin a large buffer by
        // advertising a huge length and stalling (issue #111).
        let (header, payload) =
            match crate::limits::read_frame(&mut stream, crate::limits::DEFAULT_READ_TIMEOUT).await
            {
                Ok(frame) => frame,
                Err(crate::limits::ReadFrameError::Eof) => return Ok(()),
                Err(crate::limits::ReadFrameError::Timeout) => return Ok(()),
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                }
            };

        if let Some(resp) = handler.handle(header, payload) {
            stream.write_all(&resp).await?;
            stream.flush().await?;
        }
    }
}

/// Run a blocking closure on the off-ring blocking pool.
///
/// The zero-copy syscalls (`sendfile`/`splice`) and other blocking work must
/// never run on the ring; this hands them to Tokio's blocking thread pool and
/// awaits the result, keeping large-payload movement off the control ring.
pub async fn spawn_blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .expect("blocking task panicked")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{MsgType, HEADER_LEN};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn accepts_and_echoes_a_control_frame() {
        // Handler echoes the frame back (header + payload).
        let server = Server::new(|header: FrameHeader, payload: Vec<u8>| {
            let mut out = header.encode().to_vec();
            out.extend_from_slice(&payload);
            Some(out)
        });
        let (listener, addr) = bind("127.0.0.1:0").await.unwrap();
        let shutdown = Shutdown::new();
        let sd = shutdown.clone();
        let handle = tokio::spawn(async move { server.serve_on(listener, sd).await });

        // Client sends a Control frame and expects the same bytes back.
        let mut client = TcpStream::connect(addr).await.unwrap();
        let body = b"ping-body";
        let hdr = FrameHeader::new(MsgType::Control, 7, body.len() as u32);
        client.write_all(&hdr.encode()).await.unwrap();
        client.write_all(body).await.unwrap();

        let mut echoed_hdr = [0u8; HEADER_LEN];
        client.read_exact(&mut echoed_hdr).await.unwrap();
        let back = FrameHeader::decode(&echoed_hdr).unwrap();
        assert_eq!(back.request_id, 7);
        assert_eq!(back.length as usize, body.len());
        let mut echoed_body = vec![0u8; body.len()];
        client.read_exact(&mut echoed_body).await.unwrap();
        assert_eq!(echoed_body, body);

        shutdown.trigger();
        // Accept loop returns cleanly after shutdown.
        let _ = handle.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_stops_accept_loop() {
        let server = Server::new(|_h: FrameHeader, _p: Vec<u8>| None);
        let (listener, _addr) = bind("127.0.0.1:0").await.unwrap();
        let shutdown = Shutdown::new();
        let sd = shutdown.clone();
        let handle = tokio::spawn(async move { server.serve_on(listener, sd).await });
        // Trigger immediately; the loop must return Ok without a connection.
        shutdown.trigger();
        let res = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn blocking_work_runs_off_ring() {
        // The closure runs on the blocking pool, not the current worker; assert
        // it executes and returns its value.
        static RAN: AtomicBool = AtomicBool::new(false);
        let out = spawn_blocking(|| {
            RAN.store(true, Ordering::SeqCst);
            // A different OS thread than the async caller's runtime worker.
            40 + 2
        })
        .await;
        assert_eq!(out, 42);
        assert!(RAN.load(Ordering::SeqCst));
    }
}
