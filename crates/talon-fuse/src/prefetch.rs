//! Readahead prefetch driver: turns sequential detection into warm-up fetches.
//!
//! [`crate::readahead::ReadaheadState`] *detects* sequential
//! access and *plans* which block indices to prefetch; this module *executes*
//! that plan. On each read the caller feeds the block index to
//! [`Prefetcher::on_read`], which asks the detector for the next-N block
//! indices and issues a small, **fire-and-forget** warm-up read for each
//! through a cloned [`BlockReader`].
//!
//! # Why a warm-up read warms anything
//!
//! v1 has no client-side data cache — the client relies on the kernel page
//! cache and, crucially, on the *worker* caching blocks it serves. Issuing a
//! read for an upcoming block therefore causes the owning worker to load and
//! cache it (via its miss path), so the subsequent foreground read hits a warm
//! worker. The prefetch also warms the client's
//! [`PlacementCache`](crate::placement_cache::PlacementCache), saving a
//! coordinator round-trip on the upcoming block.
//!
//! # Bounded and non-blocking
//!
//! Prefetch must never slow the foreground read or exhaust resources:
//!
//! - Each prefetch runs on a detached task; the foreground path does not await.
//! - A semaphore caps the number of in-flight prefetches; if the cap is hit,
//!   the prefetch for that block is skipped (dropped, not queued) — losing a
//!   speculative fetch is harmless.
//! - Only a tiny probe (the first byte of each block) is fetched, enough to
//!   drive the worker's load/commit without moving a whole 256 MiB block to the
//!   client.

use std::sync::Arc;

use talon_core::{ObjectId, Version};
use tokio::sync::Semaphore;

use crate::block_reader::BlockReader;
use crate::mapping::resolve_read;
use crate::readahead::{ReadaheadConfig, ReadaheadState};

/// Drives readahead prefetch for a single open file handle.
pub struct Prefetcher {
    reader: BlockReader,
    state: ReadaheadState,
    /// Bounds in-flight prefetch tasks; a full permit set skips new prefetches.
    inflight: Arc<Semaphore>,
    object: ObjectId,
    block_size: u32,
    version: Version,
    /// Total object size, so a prefetch never targets a block past EOF.
    size: u64,
}

impl Prefetcher {
    /// Create a prefetcher for one open file over the given reader.
    ///
    /// `max_inflight` caps concurrent speculative fetches across this handle.
    pub fn new(
        reader: BlockReader,
        config: ReadaheadConfig,
        max_inflight: usize,
        object: ObjectId,
        block_size: u32,
        version: Version,
        size: u64,
    ) -> Self {
        Self {
            reader,
            state: ReadaheadState::new(config),
            inflight: Arc::new(Semaphore::new(max_inflight.max(1))),
            object,
            block_size,
            version,
            size,
        }
    }

    /// Whether the handle is currently in a detected sequential run.
    pub fn is_sequential(&self) -> bool {
        self.state.is_sequential()
    }

    /// Record a foreground read at `block_index` and fire off any prefetches.
    ///
    /// Returns the block indices for which a prefetch task was actually spawned
    /// (after EOF and in-flight-cap filtering), primarily for tests/metrics.
    /// Never blocks on the prefetch itself.
    pub fn on_read(&mut self, block_index: u64, now_ms: u64) -> Vec<u64> {
        let planned = self.state.on_read(block_index);
        let mut spawned = Vec::new();
        let bs = self.block_size as u64;
        for idx in planned {
            let block_start = idx * bs;
            // Never prefetch a block that starts at or past EOF.
            if block_start >= self.size {
                continue;
            }
            // Acquire a permit without waiting; skip (drop) if none free.
            let permit = match Arc::clone(&self.inflight).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => continue,
            };
            let reader = self.reader.clone();
            let target =
                match resolve_read(&self.object, block_start, self.block_size, &self.version) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
            spawned.push(idx);
            tokio::spawn(async move {
                // Hold the permit for the duration; a one-byte probe is enough
                // to drive the worker's load/commit of the block.
                let _permit = permit;
                let _ = reader.read_block(&target.block, 0, 1, now_ms).await;
            });
        }
        spawned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator_client::CoordinatorClient;
    use crate::placement_cache::PlacementCache;
    use std::sync::atomic::{AtomicU32, Ordering};
    use talon_core::{Backend, NodeId, NodeInfo, NodeRole};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use talon_transport::{decode_request, response_header_ok, ControlMessage, RangeRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn mock_coordinator(worker_addr: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let worker_addr = worker_addr.clone();
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    s.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_h, msg) = talon_transport::decode(&full).unwrap();
                    let reply = match msg {
                        ControlMessage::PlacementLookup { .. } => {
                            ControlMessage::PlacementResponse {
                                owners: vec![NodeId::new("w1")],
                                epoch: 1,
                            }
                        }
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![NodeInfo {
                                id: NodeId::new("w1"),
                                address: worker_addr.clone(),
                                role: NodeRole::Worker,
                            }],
                        },
                        _ => ControlMessage::Ack {
                            ok: false,
                            detail: None,
                        },
                    };
                    let out = talon_transport::encode(0, &reply).unwrap();
                    s.write_all(&out).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    /// Mock worker recording the set of absolute offsets it was asked to serve.
    async fn mock_worker(count: Arc<AtomicU32>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let count = Arc::clone(&count);
                tokio::spawn(async move {
                    let mut hdr = [0u8; HEADER_LEN];
                    if s.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let h = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; h.length as usize];
                    s.read_exact(&mut body).await.unwrap();
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_h, req): (_, RangeRequest) = decode_request(&full).unwrap();
                    count.fetch_add(1, Ordering::SeqCst);
                    let payload = vec![0u8; req.len as usize];
                    let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                    out.extend_from_slice(&payload);
                    s.write_all(&out).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    fn reader(coord_addr: String) -> BlockReader {
        BlockReader::new(
            CoordinatorClient::new(coord_addr),
            Arc::new(PlacementCache::new(10_000)),
            1,
        )
    }

    fn prefetcher(reader: BlockReader) -> Prefetcher {
        Prefetcher::new(
            reader,
            ReadaheadConfig {
                trigger_run: 2,
                window: 3,
            },
            8,
            ObjectId::new(Backend::S3, "b", "o/1"),
            1024,
            Version::new("v1"),
            1_000_000,
        )
    }

    #[tokio::test]
    async fn random_access_never_prefetches() {
        let count = Arc::new(AtomicU32::new(0));
        let worker = mock_worker(Arc::clone(&count)).await;
        let coord = mock_coordinator(worker).await;
        let mut pf = prefetcher(reader(coord));
        // Jumps, not consecutive → no run, no prefetch.
        assert!(pf.on_read(0, 0).is_empty());
        assert!(pf.on_read(5, 0).is_empty());
        assert!(pf.on_read(2, 0).is_empty());
        assert!(!pf.is_sequential());
        // Give any (erroneously) spawned tasks a chance to run.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn sequential_run_prefetches_next_blocks() {
        let count = Arc::new(AtomicU32::new(0));
        let worker = mock_worker(Arc::clone(&count)).await;
        let coord = mock_coordinator(worker).await;
        let mut pf = prefetcher(reader(coord));

        assert!(pf.on_read(0, 0).is_empty()); // no run yet
        let spawned = pf.on_read(1, 0); // run hits trigger → prefetch window
        assert!(pf.is_sequential());
        assert!(!spawned.is_empty(), "sequential read should prefetch");
        // Blocks ahead of the cursor (2,3,4) are what get prefetched.
        assert_eq!(spawned, vec![2, 3, 4]);

        // Let the detached prefetch tasks reach the worker.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(count.load(Ordering::SeqCst), spawned.len() as u32);
    }

    #[tokio::test]
    async fn prefetch_skips_blocks_past_eof() {
        let count = Arc::new(AtomicU32::new(0));
        let worker = mock_worker(Arc::clone(&count)).await;
        let coord = mock_coordinator(worker).await;
        // Tiny file: only ~2 blocks of 1024 exist (size 2000).
        let mut pf = Prefetcher::new(
            reader(coord),
            ReadaheadConfig {
                trigger_run: 2,
                window: 5,
            },
            8,
            ObjectId::new(Backend::S3, "b", "o/1"),
            1024,
            Version::new("v1"),
            2000,
        );
        assert!(pf.on_read(0, 0).is_empty());
        let spawned = pf.on_read(1, 0);
        // Only block index 1 (offset 1024) is < EOF; 2.. are past it. Planner
        // proposes 2,3,4,5,6 but all start >= 2048 >= 2000 → none spawned.
        assert!(
            spawned.is_empty(),
            "no in-range block to prefetch: {spawned:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
