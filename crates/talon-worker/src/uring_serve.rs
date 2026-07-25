//! Thread-per-core io_uring data-plane runtime (#285).
//!
//! monoio has no work-stealing scheduler. It scales by running N independent
//! single-threaded runtimes — one per core — that share nothing. This module
//! provides that shape for the worker's data plane:
//!
//! - **one ring per thread**, each pinned to a core with `bind_to_cpu_set`, so
//!   a connection's protocol scheduling never migrates between cores;
//! - **`SO_REUSEPORT`**, so every ring binds the same listen address and the
//!   *kernel* distributes accepts by 4-tuple hash. There is no shared accept
//!   queue and no thundering herd;
//! - **a per-ring blocking pool**, because the zero-copy `sendfile`/`splice`
//!   syscalls are blocking and must never run on a ring (a slow client would
//!   otherwise stall every connection that ring owns).
//!
//! Measured at 8.05x scaling on 8 rings, with per-core throughput flat from 1
//! to 16 rings — i.e. no cross-ring contention (#273).
//!
//! # What is *not* sharded
//!
//! Shared worker state stays shared. Benchmarks showed that sharding
//! `BlockIndex` per ring costs 30-67% in cross-ring forwarding (connections are
//! distributed by TCP 4-tuple, blocks by `block_id`, so ~1/N of requests land
//! on the owning ring), and that per-shard eviction budgets waste capacity —
//! with 256 MiB blocks over 64 GiB there are only ~256 slots, far too few for
//! hash uniformity. So each ring holds an `Arc` of the same `WorkerRuntime`.
//! That is sound: `!Send` constrains *futures crossing threads*, not shared
//! state read from within a ring.

use std::sync::Arc;

/// How many rings to run for a configured value.
///
/// `Some(0)` means "one per available core"; any other `Some(n)` is taken
/// literally. Callers pass the resolved count to [`serve`].
pub fn resolve_ring_count(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// A per-ring connection handler.
///
/// Each ring calls this once per accepted connection, on its own thread, with
/// its own ring driving the future. The future is deliberately **not** `Send`:
/// it never leaves the thread that accepted the connection.
pub trait RingHandler: Clone + 'static {
    /// Serve one accepted connection to completion.
    fn handle(
        &self,
        stream: monoio::net::TcpStream,
    ) -> impl std::future::Future<Output = anyhow::Result<()>>;
}

/// Run the data plane on `rings` io_uring rings bound to `addr`.
///
/// Blocks until every ring thread exits. Each thread:
/// 1. pins itself to a core,
/// 2. builds an `IoUringDriver` runtime with `blocking_threads` helper threads
///    for `sendfile`,
/// 3. binds `addr` with `SO_REUSEPORT` and accepts forever.
///
/// `handler` is cloned per ring; share state through an `Arc` inside it.
///
/// # Tokio coexistence
///
/// `tokio_handle` is entered on every ring thread before the ring runs. This is
/// required, not optional: `WorkerRuntime` reaches Tokio internally —
/// `block_store` runs filesystem I/O on `tokio::task::spawn_blocking` so a large
/// read or write-plus-fsync never stalls the reactor (#115), and parts of the
/// miss path use Tokio timers and sync primitives. Without an entered handle
/// those calls panic with *"there is no reactor running"*.
///
/// The split is deliberate: the ring owns protocol scheduling and hands
/// `sendfile` to its own blocking pool, while Tokio's blocking pool absorbs
/// filesystem work that belongs on neither. Note this means two blocking pools
/// coexist, so size them with the pinned ring count in mind.
///
/// # Errors
///
/// Returns an error if a ring thread fails to bind. A bind failure on *any*
/// ring is fatal rather than degraded-but-running: a worker silently serving on
/// fewer rings than configured would be an invisible capacity loss.
pub fn serve<H>(
    addr: String,
    rings: usize,
    blocking_threads: usize,
    handler: H,
    tokio_handle: tokio::runtime::Handle,
) -> anyhow::Result<()>
where
    H: RingHandler + Send,
{
    let ready = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut threads = Vec::with_capacity(rings);

    for ring_id in 0..rings {
        let addr = addr.clone();
        let handler = handler.clone();
        let ready = Arc::clone(&ready);
        let tokio_handle = tokio_handle.clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("talon-ring-{ring_id}"))
                .spawn(move || {
                    // Required before the ring runs: WorkerRuntime reaches Tokio
                    // internally. See "Tokio coexistence" on this function.
                    let _tokio_guard = tokio_handle.enter();

                    // Pin before building the ring so its memory is allocated on the
                    // node this thread will actually run on. A failure here is not
                    // fatal — pinning is an optimization, not a correctness
                    // requirement — but it is worth surfacing.
                    if let Err(e) = monoio::utils::bind_to_cpu_set(vec![ring_id]) {
                        tracing::warn!(ring = ring_id, error = ?e, "could not pin ring to core");
                    }

                    // The blocking pool is what keeps sendfile off the ring. monoio
                    // panics if spawn_blocking is called without one attached, so
                    // this is not optional.
                    let pool = monoio::blocking::DefaultThreadPool::new(blocking_threads);
                    let mut rt = match monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                        .attach_thread_pool(Box::new(pool))
                        .enable_timer()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            ready.lock().unwrap().push(format!("ring {ring_id}: {e}"));
                            return;
                        }
                    };

                    rt.block_on(async move {
                        // SO_REUSEPORT is default-true in ListenerConfig: every ring
                        // binds the same address and the kernel spreads accepts.
                        let cfg = monoio::net::ListenerConfig::default();
                        let listener =
                            match monoio::net::TcpListener::bind_with_config(addr.as_str(), &cfg) {
                                Ok(l) => l,
                                Err(e) => {
                                    ready.lock().unwrap().push(format!("ring {ring_id}: {e}"));
                                    return;
                                }
                            };
                        tracing::info!(ring = ring_id, %addr, "data-plane ring listening");

                        loop {
                            let (stream, peer) = match listener.accept().await {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::warn!(ring = ring_id, error = %e, "accept failed");
                                    continue;
                                }
                            };
                            let _ = stream.set_nodelay(true);
                            let handler = handler.clone();
                            monoio::spawn(async move {
                                if let Err(e) = handler.handle(stream).await {
                                    tracing::debug!(?peer, error = %e, "connection ended");
                                }
                            });
                        }
                    });
                })?,
        );
    }

    for t in threads {
        let _ = t.join();
    }
    let errors = ready.lock().unwrap();
    if !errors.is_empty() {
        anyhow::bail!("data-plane ring startup failed: {}", errors.join("; "));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[test]
    fn resolve_ring_count_honours_explicit_values() {
        assert_eq!(resolve_ring_count(1), 1);
        assert_eq!(resolve_ring_count(4), 4);
    }

    #[test]
    fn resolve_ring_count_zero_means_one_per_core() {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(resolve_ring_count(0), cores);
    }

    /// Echo handler that records which OS thread served each connection and
    /// counts connections through shared state.
    #[derive(Clone)]
    struct EchoHandler {
        served: Arc<AtomicUsize>,
        threads: Arc<Mutex<HashSet<std::thread::ThreadId>>>,
    }

    impl RingHandler for EchoHandler {
        async fn handle(&self, mut stream: monoio::net::TcpStream) -> anyhow::Result<()> {
            self.threads
                .lock()
                .unwrap()
                .insert(std::thread::current().id());
            let (res, buf) = stream.read_exact(vec![0u8; 4]).await;
            res?;
            let (res, _) = stream.write_all(buf).await;
            res?;
            self.served.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Wait for `counter` to reach `want`, or panic after a deadline.
    ///
    /// The handler increments *after* writing its response, so a client can
    /// observe its echo before the server-side increment is visible. Asserting
    /// the count immediately is therefore racy — poll instead.
    fn await_count(counter: &AtomicUsize, want: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while counter.load(Ordering::Relaxed) < want {
            assert!(
                std::time::Instant::now() < deadline,
                "counter reached {} of {want}",
                counter.load(Ordering::Relaxed)
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(counter.load(Ordering::Relaxed), want);
    }

    fn client_roundtrip(addr: &str, payload: &[u8; 4]) -> Vec<u8> {
        use std::io::{Read, Write};
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        s.write_all(payload).unwrap();
        let mut out = vec![0u8; 4];
        s.read_exact(&mut out).unwrap();
        out
    }

    /// Several rings bind the same port via SO_REUSEPORT, serve real traffic,
    /// and share state through an Arc — the shape the data plane relies on.
    #[test]
    fn rings_share_a_port_and_serve_traffic() {
        // Pick a free port, then release it so the rings can SO_REUSEPORT bind.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);

        let served = Arc::new(AtomicUsize::new(0));
        let threads = Arc::new(Mutex::new(HashSet::new()));
        let handler = EchoHandler {
            served: Arc::clone(&served),
            threads: Arc::clone(&threads),
        };

        let serve_addr = addr.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let _ = serve(serve_addr, 4, 2, handler, rt.handle().clone());
        });

        // Wait for at least one ring to bind.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if std::net::TcpStream::connect(&addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "rings never bound");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        for _ in 0..40 {
            assert_eq!(client_roundtrip(&addr, b"ping"), b"ping");
        }

        await_count(&served, 40);
        // The kernel hashes by 4-tuple, so distribution is not guaranteed to be
        // even — but with 40 distinct ephemeral ports across 4 rings it must
        // land on more than one thread. This is what proves the accepts really
        // are being spread rather than all handled by one ring.
        let distinct = threads.lock().unwrap().len();
        assert!(
            distinct > 1,
            "expected multiple rings to serve, got {distinct}"
        );
    }

    /// A handler holding an Arc observes writes made from any ring: shared
    /// state is genuinely shared, not per-ring copies.
    #[test]
    fn handler_state_is_shared_across_rings() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);

        let served = Arc::new(AtomicUsize::new(0));
        let handler = EchoHandler {
            served: Arc::clone(&served),
            threads: Arc::new(Mutex::new(HashSet::new())),
        };
        let serve_addr = addr.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let _ = serve(serve_addr, 2, 1, handler, rt.handle().clone());
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if std::net::TcpStream::connect(&addr).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "rings never bound");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        for _ in 0..10 {
            client_roundtrip(&addr, b"ping");
        }
        // One shared counter, incremented from whichever ring served.
        await_count(&served, 10);
    }
}
