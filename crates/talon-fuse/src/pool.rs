//! A small per-address TCP connection pool for the client hot path.
//!
//! Both the [`CoordinatorClient`](crate::CoordinatorClient) and the
//! [`WorkerClient`](crate::WorkerClient) previously dialed a **fresh** TCP
//! connection per request and dropped it after one exchange. On the read hot
//! path — a placement lookup plus a range fetch per block, many blocks per file
//! — that pays a full TCP handshake (and the server's connection-limit permit
//! dance) every time. This pool lets warm requests reuse an established
//! connection (issue #181).
//!
//! # Model: exclusive checkout, not multiplexing
//!
//! A caller [`checkout`](ConnectionPool::checkout)s a connection (an idle pooled
//! one, or a freshly dialed one), owns it exclusively for one request/response,
//! then either [`release`](ConnectionPool::release)s it back on success or simply
//! drops it on any error. There is no response demultiplexing: one in-flight
//! request per connection. This is deliberately the simplest correct design —
//! the wire protocol is request/response framed, so an exclusive connection needs
//! no `request_id` matching.
//!
//! # Correctness invariants
//!
//! - **Only healthy connections are pooled.** A connection is returned to the
//!   pool *only* after a fully successful exchange; any I/O or protocol error
//!   drops it, so a broken socket is never handed to the next caller. This keeps
//!   the caller's replica-fallback / refresh logic intact — a dead peer still
//!   surfaces as a connect/IO error on the next `checkout`, not a silent hang.
//! - **Idle connections expire.** A pooled connection older than the idle TTL is
//!   discarded on checkout rather than reused, so a long-lived mount does not
//!   hold file descriptors against every peer forever, and a peer that silently
//!   dropped an idle connection is not reused past its likely-dead window.
//! - **Bounded.** At most `max_idle_per_addr` connections are kept idle per
//!   address; extras are dropped on release, so the client pool cannot exhaust a
//!   server's `ConnectionLimit`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::lock::MutexExt;
use tokio::net::TcpStream;

/// Default maximum idle connections kept per peer address.
pub const DEFAULT_MAX_IDLE_PER_ADDR: usize = 8;

/// Default idle lifetime before a pooled connection is discarded.
pub const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(30);

/// One idle connection plus the instant it was returned to the pool.
struct Idle {
    stream: TcpStream,
    returned_at: Instant,
}

/// A cloneable-by-`Arc` pool of reusable TCP connections keyed by peer address.
///
/// Wrap in an `Arc` to share across clones of a client. The pool holds only
/// *idle* connections; a checked-out connection lives with its caller until
/// released or dropped.
pub struct ConnectionPool {
    idle: Mutex<HashMap<String, Vec<Idle>>>,
    max_idle_per_addr: usize,
    idle_ttl: Duration,
}

impl ConnectionPool {
    /// Create a pool with the default idle bound and TTL.
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_IDLE_PER_ADDR, DEFAULT_IDLE_TTL)
    }

    /// Create a pool with explicit idle bound and TTL (for tuning/tests).
    pub fn with_limits(max_idle_per_addr: usize, idle_ttl: Duration) -> Self {
        Self {
            idle: Mutex::new(HashMap::new()),
            max_idle_per_addr: max_idle_per_addr.max(1),
            idle_ttl,
        }
    }

    /// Take a ready connection for `addr`: a fresh-enough pooled one, or a newly
    /// dialed one if none is available.
    ///
    /// Returns the stream and whether it came from the pool (`reused == true`).
    /// A reused connection may have been closed by the peer since it was pooled
    /// (server idle timeout, restart); callers should retry once on a fresh dial
    /// if a reused connection errors — see [`fresh`](Self::fresh).
    pub async fn checkout(&self, addr: &str) -> std::io::Result<(TcpStream, bool)> {
        if let Some(stream) = self.take_idle(addr) {
            return Ok((stream, true));
        }
        Ok((self.fresh(addr).await?, false))
    }

    /// Dial a brand-new connection to `addr`, bypassing the idle pool.
    pub async fn fresh(&self, addr: &str) -> std::io::Result<TcpStream> {
        TcpStream::connect(addr).await
    }

    /// Pop a non-stale idle connection for `addr`, discarding expired ones.
    fn take_idle(&self, addr: &str) -> Option<TcpStream> {
        let mut guard = self.idle.lock_recover();
        let bucket = guard.get_mut(addr)?;
        while let Some(idle) = bucket.pop() {
            if idle.returned_at.elapsed() < self.idle_ttl {
                return Some(idle.stream);
            }
            // else: too old, drop it and try the next.
        }
        None
    }

    /// Return a healthy connection to the pool for reuse.
    ///
    /// Call this only after a request/response completed without error. Extra
    /// connections beyond `max_idle_per_addr` are dropped rather than pooled.
    pub fn release(&self, addr: &str, stream: TcpStream) {
        let mut guard = self.idle.lock_recover();
        let bucket = guard.entry(addr.to_string()).or_default();
        if bucket.len() < self.max_idle_per_addr {
            bucket.push(Idle {
                stream,
                returned_at: Instant::now(),
            });
        }
        // else: at capacity; drop `stream` to close it.
    }

    /// Number of idle connections currently pooled for `addr` (for tests).
    pub fn idle_count(&self, addr: &str) -> usize {
        self.idle
            .lock()
            .unwrap()
            .get(addr)
            .map(|b| b.len())
            .unwrap_or(0)
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TcpStream is not Debug; summarize the pool without touching sockets.
        let addrs = self.idle.lock_recover().len();
        f.debug_struct("ConnectionPool")
            .field("max_idle_per_addr", &self.max_idle_per_addr)
            .field("idle_ttl", &self.idle_ttl)
            .field("addresses_pooled", &addrs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// An echo server that handles many sequential 1-byte exchanges per
    /// connection, counting how many connections it accepts.
    async fn echo_server(accepts: Arc<std::sync::atomic::AtomicU32>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                accepts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = [0u8; 1];
                    while sock.read_exact(&mut buf).await.is_ok() {
                        if sock.write_all(&buf).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn reuses_a_released_connection() {
        use std::sync::atomic::Ordering;
        let accepts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let addr = echo_server(Arc::clone(&accepts)).await;
        let pool = ConnectionPool::new();

        // Two sequential request/response cycles that release the connection back.
        for i in 0..2 {
            let (mut conn, reused) = pool.checkout(&addr).await.unwrap();
            assert_eq!(reused, i == 1, "second checkout reuses the pooled conn");
            conn.write_all(b"x").await.unwrap();
            let mut buf = [0u8; 1];
            conn.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"x");
            pool.release(&addr, conn);
        }

        // The second checkout reused the pooled connection: only one accept.
        assert_eq!(accepts.load(Ordering::SeqCst), 1, "connection was reused");
        assert_eq!(pool.idle_count(&addr), 1);
    }

    #[tokio::test]
    async fn dropped_connection_is_not_reused() {
        use std::sync::atomic::Ordering;
        let accepts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let addr = echo_server(Arc::clone(&accepts)).await;
        let pool = ConnectionPool::new();

        // First cycle: do NOT release (simulate an error dropping the conn).
        {
            let (mut conn, _reused) = pool.checkout(&addr).await.unwrap();
            conn.write_all(b"x").await.unwrap();
            let mut buf = [0u8; 1];
            conn.read_exact(&mut buf).await.unwrap();
            // conn dropped here without release.
        }
        assert_eq!(pool.idle_count(&addr), 0);
        // Second checkout must dial a fresh connection (nothing to reuse).
        let (mut conn, reused) = pool.checkout(&addr).await.unwrap();
        assert!(!reused, "no idle conn to reuse");
        // Round-trip to synchronize with the server's accept before counting.
        conn.write_all(b"y").await.unwrap();
        let mut buf = [0u8; 1];
        conn.read_exact(&mut buf).await.unwrap();
        assert_eq!(accepts.load(Ordering::SeqCst), 2, "no stale reuse");
    }

    #[tokio::test]
    async fn expired_idle_connection_is_discarded() {
        use std::sync::atomic::Ordering;
        let accepts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let addr = echo_server(Arc::clone(&accepts)).await;
        // Zero TTL: every pooled connection is immediately stale.
        let pool = ConnectionPool::with_limits(8, Duration::ZERO);

        let (conn, _) = pool.checkout(&addr).await.unwrap();
        pool.release(&addr, conn);
        assert_eq!(pool.idle_count(&addr), 1);
        // Checkout discards the expired one and dials fresh.
        let (mut conn, reused) = pool.checkout(&addr).await.unwrap();
        assert!(!reused, "expired conn is not reused");
        conn.write_all(b"y").await.unwrap();
        let mut buf = [0u8; 1];
        conn.read_exact(&mut buf).await.unwrap();
        assert_eq!(accepts.load(Ordering::SeqCst), 2, "expired conn not reused");
    }

    #[tokio::test]
    async fn idle_pool_is_bounded_per_addr() {
        let accepts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let addr = echo_server(Arc::clone(&accepts)).await;
        let pool = ConnectionPool::with_limits(2, DEFAULT_IDLE_TTL);

        // Release three connections; only two are kept.
        let (c1, _) = pool.checkout(&addr).await.unwrap();
        let (c2, _) = pool.checkout(&addr).await.unwrap();
        let (c3, _) = pool.checkout(&addr).await.unwrap();
        pool.release(&addr, c1);
        pool.release(&addr, c2);
        pool.release(&addr, c3);
        assert_eq!(pool.idle_count(&addr), 2, "bounded to max_idle_per_addr");
    }
}
