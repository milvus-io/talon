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
//! - **ring-native file-to-pipe-to-socket splice**, with no per-ring send pool.
//!
//! Ring scaling and send throughput must be measured on the deployment kernel:
//! io_uring submission can still dispatch splice operations to kernel io-wq.
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

use crate::ConnectionAdmission;

/// The CPUs this process is actually allowed to run on, in ascending order.
///
/// Reads the thread's affinity mask rather than assuming CPUs are numbered
/// `0..N`. Under a restrictive cpuset — a Kubernetes pod with a CPU manager
/// policy, a `taskset`, or a container pinned to a NUMA node — the allowed CPUs
/// can be an arbitrary subset such as `[8, 9, 10]`. Pinning ring *i* to CPU *i*
/// in that environment is wrong twice over: `sched_setaffinity` succeeds and
/// silently moves the thread **outside** its cpuset, stealing time from
/// whatever the orchestrator placed on that CPU (a colocated training job, say),
/// and the rings stop being one-per-core because several may land on the same
/// CPU.
///
/// Returns an empty vector if the mask cannot be read, which callers treat as
/// "do not pin".
pub fn allowed_cpus() -> Vec<usize> {
    // SAFETY: `set` is zero-initialised and its size is passed explicitly;
    // `sched_getaffinity` only writes within that size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
            .collect()
    }
}

/// Whether this host can actually run the io_uring data plane.
///
/// io_uring needs a recent enough kernel *and* permission to use it: the
/// `io_uring_disabled` sysctl, a seccomp filter, or a container runtime's
/// default profile can each block the syscall on a kernel that otherwise
/// supports it. Probing the real capability is therefore the only reliable
/// check — a kernel version comparison would be wrong in exactly the
/// environments that matter.
///
/// The worker calls this before binding and falls back to the Tokio data plane
/// when it returns `false`, so the io_uring default is safe on hosts that
/// cannot run it.
pub fn io_uring_available() -> bool {
    static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        monoio::utils::detect_uring()
            && monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .with_iowq_max_workers(8, 8)
                .build()
                .is_ok()
    })
}

/// How many rings to run for a configured value.
///
/// `Some(0)` means "one per available core"; any other `Some(n)` is taken
/// literally. Callers pass the resolved count to [`serve`].
pub fn resolve_ring_count(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    // One ring per unit of CPU we may actually consume.
    //
    // Two limits apply and they are not the same number. The affinity mask says
    // *which* CPUs we may run on; a cgroup CPU quota says *how much* CPU time we
    // may consume. A pod with `cpu.max = 1500000/100000` on a 16-CPU node is
    // allowed on all 16 CPUs but is only entitled to 15 CPUs' worth of time.
    // Sizing to the affinity mask there would start 16 rings competing for 15
    // CPUs of quota, so they would throttle each other — exactly the
    // unpredictable behaviour thread-per-core exists to avoid.
    //
    // `available_parallelism` accounts for both, so it decides the count. The
    // affinity list decides *where* each ring pins (see `allowed_cpus`), which
    // is a separate question.
    let by_quota = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let by_affinity = allowed_cpus().len();
    if by_affinity == 0 {
        return by_quota;
    }
    by_quota.min(by_affinity).max(1)
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

/// Keep our place in the admission queue across stop checks. Unlike accept,
/// semaphore acquisition is cancellation-safe and can be dropped on shutdown.
async fn acquire_until_stopped(
    admission: &ConnectionAdmission,
    stop: &std::sync::atomic::AtomicBool,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    let mut pending = std::pin::pin!(admission.acquire());
    while !stop.load(std::sync::atomic::Ordering::Acquire) {
        if let Ok(permit) =
            monoio::time::timeout(std::time::Duration::from_millis(100), pending.as_mut()).await
        {
            return Some(permit);
        }
    }
    None
}

/// Keep one accept alive across stop checks. Dropping a pending Monoio accept
/// loses the accepted FD if completion races its asynchronous cancellation.
async fn accept_until_stopped(
    listener: &monoio::net::TcpListener,
    stop: &std::sync::atomic::AtomicBool,
) -> std::io::Result<Option<(monoio::net::TcpStream, std::net::SocketAddr)>> {
    use std::sync::atomic::Ordering;
    if stop.load(Ordering::Acquire) {
        return Ok(None);
    }
    let cancel = monoio::io::Canceller::new();
    let pending = listener.cancelable_accept(cancel.handle());
    let mut pending = std::pin::pin!(pending);
    loop {
        match monoio::time::timeout(std::time::Duration::from_millis(100), pending.as_mut()).await {
            Ok(result) => return result.map(Some),
            Err(_) if stop.load(Ordering::Acquire) => {
                cancel.cancel();
                // Cancellation can lose to a successful accept. Await and drop
                // that stream too, so every completed FD gets an owner.
                drop(pending.await);
                return Ok(None);
            }
            Err(_) => {}
        }
    }
}

/// Run the data plane on `rings` io_uring rings bound to `addr`.
///
/// Blocks until every ring thread exits. Each thread:
/// 1. pins itself to a core,
/// 2. builds an `IoUringDriver` runtime without a send helper pool,
/// 3. binds `addr` with `SO_REUSEPORT` and accepts forever.
///
/// Every ring shares `admission`. A ring acquires capacity before `accept`, so
/// excess peers remain in that listener's kernel backlog without an accepted FD
/// or Monoio task. The permit moves into the connection task and is released on
/// every completion, error, or runtime-cancellation path.
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
/// The ring owns protocol scheduling and resident file-to-socket splice.
/// Tokio's blocking pool still handles cache writes and other filesystem work.
/// Kernel io-wq workers may execute buffered file I/O submitted via io_uring.
///
/// # Errors
///
/// Returns an error if a ring thread fails to bind. A bind failure on *any*
/// ring is fatal rather than degraded-but-running: a worker silently serving on
/// fewer rings than configured would be an invisible capacity loss.
pub fn serve<H>(
    addr: String,
    rings: usize,
    admission: ConnectionAdmission,
    handler: H,
    tokio_handle: tokio::runtime::Handle,
) -> anyhow::Result<()>
where
    H: RingHandler + Send,
{
    serve_with_shutdown(
        addr,
        rings,
        admission,
        handler,
        tokio_handle,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
}

/// Like [`serve`], with a cooperative accept-loop stop flag for worker shutdown.
pub fn serve_with_shutdown<H>(
    addr: String,
    rings: usize,
    admission: ConnectionAdmission,
    handler: H,
    tokio_handle: tokio::runtime::Handle,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()>
where
    H: RingHandler + Send,
{
    let ready = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut threads = Vec::with_capacity(rings);

    // Resolved once: every ring pins into this set. Fewer allowed CPUs than
    // rings means rings share CPUs, which is a legitimate (if suboptimal)
    // configuration rather than an error.
    let allowed = Arc::new(allowed_cpus());
    if allowed.len() < rings {
        tracing::warn!(
            rings,
            allowed_cpus = allowed.len(),
            "more rings than allowed CPUs; rings will share CPUs"
        );
    }

    for ring_id in 0..rings {
        let addr = addr.clone();
        let admission = admission.clone();
        let handler = handler.clone();
        let ready = Arc::clone(&ready);
        let tokio_handle = tokio_handle.clone();
        let allowed = Arc::clone(&allowed);
        let stop = Arc::clone(&stop);
        threads.push(
            std::thread::Builder::new()
                .name(format!("talon-ring-{ring_id}"))
                .spawn(move || {
                    // Required before the ring runs: WorkerRuntime reaches Tokio
                    // internally. See "Tokio coexistence" on this function.
                    let _tokio_guard = tokio_handle.enter();

                    // Pin before building the ring so its memory is allocated on
                    // the node this thread will actually run on. Pin to a CPU
                    // from the *allowed* set rather than to `ring_id`, so a
                    // restricted cpuset (Kubernetes CPU manager, taskset, NUMA
                    // pinning) is respected instead of escaped. A failure here is
                    // not fatal — pinning is an optimization, not a correctness
                    // requirement — but it is worth surfacing.
                    match allowed.get(ring_id % allowed.len().max(1)) {
                        Some(&cpu) => {
                            if let Err(e) = monoio::utils::bind_to_cpu_set(vec![cpu]) {
                                tracing::warn!(
                                    ring = ring_id,
                                    cpu,
                                    error = ?e,
                                    "could not pin ring to cpu"
                                );
                            }
                        }
                        // Affinity unreadable: leave the thread unpinned rather
                        // than guess a CPU that may not be ours.
                        None => tracing::warn!(
                            ring = ring_id,
                            "cpu affinity unavailable; ring left unpinned"
                        ),
                    }

                    // Resident sends submit splice operations to this same
                    // ring. No user-space send helper threads are created. Bound
                    // both io-wq classes: regular-file and pipe/socket splice
                    // may otherwise grow kernel workers with connection count.
                    let mut rt = match monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                        .with_iowq_max_workers(8, 8)
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
                            // Preserve the worker-global pre-accept budget while
                            // still allowing shutdown when all permits are held.
                            let Some(permit) = acquire_until_stopped(&admission, &stop).await
                            else {
                                break;
                            };
                            let (stream, peer) = match accept_until_stopped(&listener, &stop).await
                            {
                                Ok(Some(v)) => v,
                                Ok(None) => break,
                                Err(e) => {
                                    tracing::warn!(ring = ring_id, error = %e, "accept failed");
                                    continue;
                                }
                            };
                            let _ = stream.set_nodelay(true);
                            let handler = handler.clone();
                            monoio::spawn(async move {
                                // The permit covers the accepted FD and task for
                                // their complete lifetime.
                                let _permit = permit;
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

    fn admission(capacity: usize) -> ConnectionAdmission {
        ConnectionAdmission::new(capacity, crate::WorkerMetrics::new(0))
    }

    #[test]
    fn saturated_admission_stops_without_leaking_capacity() {
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async {
            let admission = admission(1);
            let held = admission.acquire().await;
            let stop = std::sync::atomic::AtomicBool::new(false);
            let mut waiting = Box::pin(acquire_until_stopped(&admission, &stop));
            assert!(futures::poll!(&mut waiting).is_pending());
            stop.store(true, Ordering::Release);
            assert!(
                monoio::time::timeout(std::time::Duration::from_secs(2), waiting)
                    .await
                    .unwrap()
                    .is_none()
            );
            drop(held);
            let replacement =
                monoio::time::timeout(std::time::Duration::from_secs(2), admission.acquire())
                    .await
                    .unwrap();
            drop(replacement);
        });
    }

    #[test]
    fn accept_stop_checks_preserve_pending_accept_and_close_racing_fds() {
        // FD accounting must be isolated from the other socket tests.
        const CHILD: &str = "TALON_ACCEPT_STOP_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "uring_serve::tests::accept_stop_checks_preserve_pending_accept_and_close_racing_fds",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async {
            use std::time::Duration;
            let listener = monoio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            monoio::time::sleep(Duration::from_millis(1)).await;
            let before = std::fs::read_dir("/proc/self/fd").unwrap().count();
            for cancel in [false, true] {
                for _ in 0..4 {
                    let stop = std::sync::atomic::AtomicBool::new(false);
                    let mut pending = Box::pin(accept_until_stopped(&listener, &stop));
                    assert!(futures::poll!(&mut pending).is_pending());
                    // Submit accept and cross multiple periodic checks with no client.
                    for _ in 0..2 {
                        monoio::time::sleep(Duration::from_millis(110)).await;
                        assert!(futures::poll!(&mut pending).is_pending());
                    }
                    if cancel {
                        monoio::time::sleep(Duration::from_millis(110)).await;
                        stop.store(true, Ordering::Release);
                        // Queue cancellation, then make accept complete before
                        // that cancellation is submitted to the kernel.
                        assert!(futures::poll!(&mut pending).is_pending());
                    }
                    let client = std::net::TcpStream::connect(addr).unwrap();
                    std::thread::sleep(Duration::from_millis(5));
                    let accepted = monoio::time::timeout(Duration::from_secs(2), pending)
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(accepted.is_none(), cancel);
                    drop(accepted);
                    drop(client);
                    // Let Monoio complete any asynchronous closes.
                    monoio::time::sleep(Duration::from_millis(5)).await;
                    assert_eq!(std::fs::read_dir("/proc/self/fd").unwrap().count(), before);
                }
            }
            // Shutdown with no racing connection must also finish promptly.
            let stop = std::sync::atomic::AtomicBool::new(false);
            let mut pending = Box::pin(accept_until_stopped(&listener, &stop));
            assert!(futures::poll!(&mut pending).is_pending());
            monoio::time::sleep(Duration::from_millis(5)).await;
            stop.store(true, Ordering::Release);
            assert!(monoio::time::timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap()
                .is_none());
        });
    }

    /// The default configuration must select the io_uring data plane, and it
    /// must be usable on any host CI runs on — the fallback exists precisely so
    /// this default is safe, so a failure here means the default is unsafe.
    /// The allowed-CPU list must be non-empty and within the kernel's range on
    /// any host the tests run on.
    #[test]
    fn allowed_cpus_is_readable_and_sane() {
        let cpus = allowed_cpus();
        assert!(!cpus.is_empty(), "affinity mask should be readable");
        assert!(cpus.windows(2).all(|w| w[0] < w[1]), "must be ascending");
        assert!(cpus.iter().all(|&c| c < libc::CPU_SETSIZE as usize));
    }

    /// The regression this module's pinning logic exists to prevent.
    ///
    /// Under a restricted cpuset the allowed CPUs may be an arbitrary subset
    /// (e.g. `[8, 9]`). Pinning ring *i* to CPU *i* would silently escape the
    /// cpuset — `sched_setaffinity` succeeds — and steal time from whatever the
    /// orchestrator placed on CPU 0. Every CPU a ring pins to must come from
    /// the allowed set.
    #[test]
    fn rings_pin_only_within_the_allowed_cpu_set() {
        let allowed = allowed_cpus();
        assert!(!allowed.is_empty());

        // The mapping used by `serve`, for more rings than CPUs and fewer.
        for rings in [1usize, allowed.len(), allowed.len() * 2 + 1] {
            for ring_id in 0..rings {
                let cpu = allowed[ring_id % allowed.len()];
                assert!(
                    allowed.contains(&cpu),
                    "ring {ring_id} of {rings} pinned to cpu {cpu}, outside the allowed set {allowed:?}"
                );
            }
        }
    }

    /// With at least as many CPUs as rings, each ring gets a distinct CPU —
    /// otherwise thread-per-core degenerates into threads sharing cores.
    #[test]
    fn rings_get_distinct_cpus_when_available() {
        let allowed = allowed_cpus();
        let rings = resolve_ring_count(0).min(allowed.len());
        let assigned: std::collections::HashSet<usize> =
            (0..rings).map(|i| allowed[i % allowed.len()]).collect();
        assert_eq!(assigned.len(), rings, "each ring should get its own cpu");
    }

    /// The default ring count must respect **both** limits: how many CPUs we
    /// may run on (affinity) and how much CPU time we may consume (cgroup
    /// quota). These differ — this host allows 16 CPUs but grants 15 CPUs of
    /// quota — and starting a ring per allowed CPU would have them throttling
    /// each other. This is what makes the default safe in a limited container.
    #[test]
    fn default_ring_count_respects_quota_and_affinity() {
        let rings = resolve_ring_count(0);
        let by_quota = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let by_affinity = allowed_cpus().len();
        assert!(rings >= 1);
        assert!(rings <= by_quota, "must not exceed the cpu-time quota");
        if by_affinity > 0 {
            assert!(rings <= by_affinity, "must not exceed the affinity mask");
        }
    }

    #[test]
    fn io_uring_availability_is_detectable() {
        // Must not panic and must agree with itself across calls (it is cached).
        let first = io_uring_available();
        assert_eq!(first, io_uring_available());
    }

    /// The shipped default is 0, which must resolve to one ring per core rather
    /// than zero rings.
    #[test]
    fn default_config_resolves_to_one_ring_per_core() {
        let default_rings = talon_core::WorkerConfig::default().data_plane_rings;
        assert_eq!(
            default_rings, 0,
            "the shipped default should be one-per-core"
        );
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(
            resolve_ring_count(default_rings),
            cores.min(allowed_cpus().len().max(1))
        );
        assert!(resolve_ring_count(default_rings) >= 1);
    }

    #[test]
    fn resolve_ring_count_honours_explicit_values() {
        assert_eq!(resolve_ring_count(1), 1);
        assert_eq!(resolve_ring_count(4), 4);
    }

    #[test]
    fn resolve_ring_count_zero_means_one_per_usable_cpu() {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(
            resolve_ring_count(0),
            cores.min(allowed_cpus().len().max(1))
        );
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
            let _ = serve(serve_addr, 4, admission(128), handler, rt.handle().clone());
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
            let _ = serve(serve_addr, 2, admission(128), handler, rt.handle().clone());
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

    /// Resources held by one synthetic connection handler.
    struct HandlerResourceGuard {
        active: Arc<AtomicUsize>,
        resident_bytes: Arc<AtomicUsize>,
        bytes: usize,
    }

    impl Drop for HandlerResourceGuard {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::Relaxed);
            self.resident_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        }
    }

    /// An idle handler with deterministic per-task residency. Every admitted
    /// connection blocks on one byte, then returns an error to exercise permit
    /// release on the error path.
    #[derive(Clone)]
    struct IdleHandler {
        started: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        resident_bytes: Arc<AtomicUsize>,
        bytes_per_connection: usize,
    }

    impl RingHandler for IdleHandler {
        async fn handle(&self, mut stream: monoio::net::TcpStream) -> anyhow::Result<()> {
            let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak.fetch_max(active, Ordering::Relaxed);
            self.started.fetch_add(1, Ordering::Relaxed);
            self.resident_bytes
                .fetch_add(self.bytes_per_connection, Ordering::Relaxed);
            let _guard = HandlerResourceGuard {
                active: Arc::clone(&self.active),
                resident_bytes: Arc::clone(&self.resident_bytes),
                bytes: self.bytes_per_connection,
            };
            let allocation = vec![0xa5; self.bytes_per_connection];
            let (result, _) = stream.read_exact(vec![0u8; 1]).await;
            std::hint::black_box(&allocation);
            result?;
            anyhow::bail!("synthetic handler error")
        }
    }

    /// Count established sockets owned by this process whose local port is the
    /// server port. Correlating `/proc/net/tcp` inodes with `/proc/self/fd`
    /// excludes client-side sockets in this test and connections still queued in
    /// the kernel backlog.
    fn accepted_server_fds(port: u16) -> usize {
        let socket_inodes: HashSet<String> = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .filter_map(|target| {
                let target = target.to_string_lossy();
                target
                    .strip_prefix("socket:[")
                    .and_then(|value| value.strip_suffix(']'))
                    .map(str::to_owned)
            })
            .collect();
        let expected_port = format!("{port:04X}");
        std::fs::read_to_string("/proc/net/tcp")
            .unwrap()
            .lines()
            .skip(1)
            .filter(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                fields.get(1).is_some_and(|local| {
                    local
                        .rsplit_once(':')
                        .is_some_and(|(_, local_port)| local_port == expected_port)
                }) && fields.get(3) == Some(&"01")
                    && fields
                        .get(9)
                        .is_some_and(|inode| socket_inodes.contains(*inode))
            })
            .count()
    }

    fn await_zero(counter: &AtomicUsize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while counter.load(Ordering::Relaxed) != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "counter remained at {}",
                counter.load(Ordering::Relaxed)
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Admission happens before both accept and spawn. Excess idle clients stay
    /// in the kernel backlog, so accepted FDs, handler tasks, and handler-owned
    /// resident memory all remain bounded by one budget shared across rings.
    #[test]
    fn global_admission_bounds_accepts_tasks_and_residency() {
        const CAPACITY: usize = 2;
        const CLIENTS: usize = 32;
        const BYTES_PER_CONNECTION: usize = 256 * 1024;

        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let socket_addr = probe.local_addr().unwrap();
        let addr = socket_addr.to_string();
        drop(probe);

        let started = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let resident_bytes = Arc::new(AtomicUsize::new(0));
        let handler = IdleHandler {
            started: Arc::clone(&started),
            active: Arc::clone(&active),
            peak: Arc::clone(&peak),
            resident_bytes: Arc::clone(&resident_bytes),
            bytes_per_connection: BYTES_PER_CONNECTION,
        };
        let metrics = crate::WorkerMetrics::new(0);
        let connection_admission = ConnectionAdmission::new(CAPACITY, metrics.clone());
        let serve_addr = addr.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let _ = serve(
                serve_addr,
                2,
                connection_admission,
                handler,
                rt.handle().clone(),
            );
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let first = loop {
            match std::net::TcpStream::connect(&addr) {
                Ok(stream) => break stream,
                Err(_) => {
                    assert!(std::time::Instant::now() < deadline, "rings never bound");
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
            }
        };
        let mut clients = Vec::with_capacity(CLIENTS);
        clients.push(first);
        for _ in 1..CLIENTS {
            clients.push(std::net::TcpStream::connect(&addr).unwrap());
        }

        await_count(&started, CAPACITY);
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(started.load(Ordering::Relaxed), CAPACITY);
        assert_eq!(active.load(Ordering::Relaxed), CAPACITY);
        assert_eq!(peak.load(Ordering::Relaxed), CAPACITY);
        assert_eq!(
            resident_bytes.load(Ordering::Relaxed),
            CAPACITY * BYTES_PER_CONNECTION
        );
        assert_eq!(accepted_server_fds(socket_addr.port()), CAPACITY);
        let saturation = metrics
            .render()
            .lines()
            .find(|line| line.starts_with("talon_worker_connection_admission_saturated_total "))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap();
        assert!(saturation > 0);

        // Every error completion releases its permit, allowing all queued peers
        // to enter without ever exceeding the peak.
        for client in &mut clients {
            std::io::Write::write_all(client, b"x").unwrap();
        }
        await_count(&started, CLIENTS);
        await_zero(&active);
        assert_eq!(peak.load(Ordering::Relaxed), CAPACITY);
        assert_eq!(resident_bytes.load(Ordering::Relaxed), 0);
    }
}
