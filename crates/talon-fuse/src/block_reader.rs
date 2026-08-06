//! Block read orchestration: local placement cache -> membership -> worker.
//!
//! [`BlockReader`] is the heart of the FUSE read path. Given a [`BlockId`] and a
//! sub-range within it, it answers "where does this block live?" from the
//! client-side [`PlacementCache`] when warm. On a miss it ranks a cached worker
//! membership snapshot locally, then fetches the bytes from the owning worker
//! with a [`WorkerClient`].
//!
//! # Cached addresses, not node ids
//!
//! The placement cache stores an ordered list of **worker addresses** (what the
//! client actually dials), derived from the same membership snapshot used for
//! local HRW ranking. Storing the dialable address keeps the hot path
//! allocation-light and makes replica fallback a simple walk down the ordered
//! list.
//!
//! On a fetch failure the reader walks the cached replicas in order; if all are
//! exhausted it invalidates the entry and attempts one membership refresh
//! before giving up. A failed refresh keeps using the last-good snapshot.
//! [`BlockReader::observe_epoch`] reconciles the cache when a different
//! membership token is seen. Multi-block splitting is handled by
//! [`crate::read_plan`] and readahead by [`crate::prefetch`].

use std::sync::Arc;

use talon_core::{rank_cache_workers, BlockId, ObjectId, Version};

use crate::coordinator_client::{CoordinatorClient, CoordinatorError};
use crate::membership_cache::{MembershipCache, MembershipSnapshot};
use crate::metrics::ReadStats;
use crate::placement_cache::{Cached, PlacementCache, RefreshReason};
use crate::pool::ConnectionPool;
use crate::read_plan::plan_read;
use crate::worker_client::{WorkerClient, WorkerError};

/// Errors from a block read.
#[derive(Debug, thiserror::Error)]
pub enum BlockReadError {
    /// Membership refresh failed before any snapshot was cached.
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    /// The worker fetch failed.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    /// The cluster returned no owners for the block (empty cluster).
    #[error("no owners for block")]
    NoOwners,
    /// An owner id had no resolvable worker address in membership.
    #[error("owner has no known worker address")]
    UnresolvedOwner,
    /// Every replica failed, including after a membership refresh.
    #[error("all replicas failed after refresh")]
    AllReplicasFailed,
}

/// The coordinates of an open file needed to plan a read: its object identity,
/// logical block size, source version/etag, and total size (for EOF clamping).
///
/// Grouping these keeps [`BlockReader::read`] to a small argument list and
/// mirrors what a `getattr`/HEAD lookup yields for an open handle.
#[derive(Debug, Clone)]
pub struct FileView<'a> {
    /// The object being read.
    pub object: &'a ObjectId,
    /// Logical block size in bytes.
    pub block_size: u32,
    /// Source version/etag guarding the blocks.
    pub version: &'a Version,
    /// Total object length, used to clamp reads at EOF.
    pub size: u64,
}

/// Orchestrates local block placement and worker reads with caching.
#[derive(Clone)]
pub struct BlockReader {
    coordinator: CoordinatorClient,
    cache: Arc<PlacementCache>,
    /// Number of workers retained from local ranking (RF=1 -> 1 in v1).
    replicas_k: u8,
    /// Read-path counters (cache hit/miss, worker fetches, bytes served).
    stats: ReadStats,
    /// Shared pool of worker connections, so warm fetches skip the TCP handshake
    /// (issue #181).
    worker_pool: Arc<ConnectionPool>,
    /// Last authoritative worker set; stale snapshots remain usable on outage.
    membership: Arc<MembershipCache>,
    /// Serializes cold refreshes so an expired snapshot causes one control request.
    membership_refresh: Arc<tokio::sync::Mutex<()>>,
}

impl BlockReader {
    /// Create a reader over the given coordinator client and placement cache.
    ///
    /// `replicas_k` is how many owners to retain from local placement; with RF=1
    /// this is `1`, but a larger value reserves an ordered fallback list.
    /// Metrics are collected into a fresh [`ReadStats`]; use
    /// [`with_stats`](Self::with_stats) to share an existing one.
    pub fn new(coordinator: CoordinatorClient, cache: Arc<PlacementCache>, replicas_k: u8) -> Self {
        Self::with_stats(coordinator, cache, replicas_k, ReadStats::new())
    }

    /// Like [`new`](Self::new) but records metrics into the provided
    /// [`ReadStats`], so a caller (e.g. the mount layer) can observe the same
    /// counters this reader bumps.
    pub fn with_stats(
        coordinator: CoordinatorClient,
        cache: Arc<PlacementCache>,
        replicas_k: u8,
        stats: ReadStats,
    ) -> Self {
        let membership = Arc::new(MembershipCache::new(cache.ttl_ms()));
        Self {
            coordinator,
            cache,
            replicas_k: replicas_k.max(1),
            stats,
            worker_pool: Arc::new(ConnectionPool::new()),
            membership,
            membership_refresh: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// The coordinator address this reader resolves placement against.
    pub fn coordinator_addr(&self) -> &str {
        self.coordinator.addr()
    }

    /// The read-path counters this reader updates.
    pub fn stats(&self) -> &ReadStats {
        &self.stats
    }

    /// Read `len` bytes at `offset_in_block` within `block`.
    ///
    /// Resolves placement (cache hit, else local HRW over cached membership),
    /// then fetches the sub-range from an owner. The
    /// absolute object offset handed to the worker is
    /// `block.offset + offset_in_block`.
    ///
    /// # Replica fallback & refresh
    ///
    /// A worker that is unreachable ([`WorkerError::Io`], a
    /// [`RefreshReason::ConnectFailure`]) or that no longer holds the block
    /// ([`WorkerError::Remote`], a [`RefreshReason::WrongOwner`]) does not fail
    /// the read outright: the reader walks the ordered replica list from the
    /// cached placement. If every cached replica is exhausted, it invalidates
    /// the entry and performs **one** membership refresh (which may return a
    /// different worker set), then retries against the fresh primary.
    /// Only if that also fails does the error propagate.
    pub async fn read_block(
        &self,
        block: &BlockId,
        offset_in_block: u32,
        len: u32,
        now_ms: u64,
    ) -> Result<Vec<u8>, BlockReadError> {
        let cached = match self.cache.get(block, now_ms) {
            Some(c) => {
                self.stats.record_cache_hit();
                c
            }
            None => {
                self.stats.record_cache_miss();
                self.resolve_and_cache(block, now_ms).await?
            }
        };
        let abs_offset = block.offset + offset_in_block as u64;

        // First pass: walk the cached replica list in order.
        match self
            .try_replicas(block, &cached.replicas, abs_offset, len)
            .await
        {
            Ok(bytes) => {
                self.stats.add_bytes_served(bytes.len() as u64);
                return Ok(bytes);
            }
            Err(reason) => {
                // Every cached replica failed; drop the stale placement and do a
                // single membership refresh before giving up.
                tracing::debug!(%block, ?reason, "all cached replicas failed; refreshing placement");
                self.cache.invalidate(block, reason);
                self.stats.record_coordinator_refresh();
            }
        }

        let fresh = self.resolve_and_cache_forced(block, now_ms).await?;
        let bytes = self
            .try_replicas(block, &fresh.replicas, abs_offset, len)
            .await
            .map_err(|_| BlockReadError::AllReplicasFailed)?;
        self.stats.add_bytes_served(bytes.len() as u64);
        Ok(bytes)
    }

    /// Try each replica address in order; return the first success, or the
    /// [`RefreshReason`] describing why the whole list failed.
    ///
    /// A connect failure or a remote "not present" is retryable against the next
    /// replica; the returned reason reflects the last failure so the caller can
    /// record an accurate invalidation cause.
    async fn try_replicas(
        &self,
        block: &BlockId,
        replicas: &[String],
        abs_offset: u64,
        len: u32,
    ) -> Result<Vec<u8>, RefreshReason> {
        if replicas.is_empty() {
            return Err(RefreshReason::WrongOwner);
        }
        let mut last = RefreshReason::WrongOwner;
        for addr in replicas {
            let worker = WorkerClient::with_pool(addr.clone(), Arc::clone(&self.worker_pool));
            self.stats.record_worker_fetch();
            match worker
                .fetch_range(&block.object, abs_offset, len as u64)
                .await
            {
                Ok(bytes) if bytes.len() as u64 == u64::from(len) => return Ok(bytes),
                Ok(_) => {
                    self.stats.record_worker_failure();
                    last = RefreshReason::WrongOwner;
                }
                Err(e) => {
                    self.stats.record_worker_failure();
                    last = match e {
                        WorkerError::Io(_) => RefreshReason::ConnectFailure,
                        // A framing/encode error is not a placement problem, but
                        // treat it as a wrong-owner refresh so we still recover.
                        _ => RefreshReason::WrongOwner,
                    };
                }
            }
        }
        Err(last)
    }

    /// Reconcile the cache against an observed placement version.
    ///
    /// When a response (placement or otherwise) carries a version token that
    /// differs from a cached entry's, that entry is dropped so the next read
    /// re-looks-up. This is the client half of the coordinator's placement
    /// versioning: a membership change changes the deterministic version token,
    /// and any client holding a placement computed against a different set
    /// refreshes. The token is a content hash, not an ordered counter, so any
    /// difference triggers a refresh (see [`crate::PlacementCache::observe_epoch`]).
    /// Returns `true` if the entry was invalidated.
    pub fn observe_epoch(&self, block: &BlockId, observed_epoch: u64) -> bool {
        self.cache.observe_epoch(block, observed_epoch)
    }

    /// Read `[offset, offset+len)` of a file, spanning block boundaries.
    ///
    /// Splits the request into per-block segments via
    /// [`crate::read_plan::plan_read`] (clamped to `file.size` at EOF),
    /// fetches each segment through [`read_block`](Self::read_block) — so each
    /// segment independently benefits from the placement cache — and
    /// concatenates the results in order. A read at or past EOF returns an empty
    /// buffer (POSIX short read).
    pub async fn read(
        &self,
        file: &FileView<'_>,
        offset: u64,
        len: u64,
        now_ms: u64,
    ) -> Result<Vec<u8>, BlockReadError> {
        let plan = plan_read(
            file.object,
            offset,
            len,
            file.block_size,
            file.version,
            file.size,
        );
        let mut out = Vec::with_capacity(plan.iter().map(|s| s.len as usize).sum());
        for seg in plan {
            let bytes = self
                .read_block(&seg.block, seg.offset_in_block, seg.len, now_ms)
                .await?;
            if bytes.len() as u64 != u64::from(seg.len) {
                return Err(WorkerError::RangeLengthMismatch {
                    expected: u64::from(seg.len),
                    actual: bytes.len() as u64,
                }
                .into());
            }
            out.extend_from_slice(&bytes);
        }
        Ok(out)
    }

    /// Resolve the worker address that owns `object` (for a write/delete).
    ///
    /// Placement is by `BlockId`; a write addresses the object's first block
    /// under `version` (the mount uses a canonical version, #182), so this
    /// resolves the primary owner of block 0 through the same client-side
    /// placement path reads use, reusing the placement cache. Returns the
    /// dialable `host:port` of the primary owner.
    pub async fn resolve_owner(
        &self,
        object: &ObjectId,
        block_size: u32,
        version: &Version,
        now_ms: u64,
    ) -> Result<String, BlockReadError> {
        let block = BlockId::new(object.clone(), 0, block_size, version.clone());
        let cached = match self.cache.get(&block, now_ms) {
            Some(c) => c,
            None => self.resolve_and_cache(&block, now_ms).await?,
        };
        cached
            .replicas
            .first()
            .cloned()
            .ok_or(BlockReadError::UnresolvedOwner)
    }

    /// Rank the block against cached membership and cache the ordered addresses.
    async fn resolve_and_cache(
        &self,
        block: &BlockId,
        now_ms: u64,
    ) -> Result<Cached, BlockReadError> {
        self.resolve_and_cache_inner(block, now_ms, false).await
    }

    async fn resolve_and_cache_forced(
        &self,
        block: &BlockId,
        now_ms: u64,
    ) -> Result<Cached, BlockReadError> {
        self.resolve_and_cache_inner(block, now_ms, true).await
    }

    async fn resolve_and_cache_inner(
        &self,
        block: &BlockId,
        now_ms: u64,
        force_membership_refresh: bool,
    ) -> Result<Cached, BlockReadError> {
        let membership = self
            .membership_snapshot(now_ms, force_membership_refresh)
            .await?;
        let owners = rank_cache_workers(block, &membership.nodes, self.replicas_k as usize);
        if owners.is_empty() {
            return Err(BlockReadError::NoOwners);
        }
        let replicas: Vec<String> = owners
            .iter()
            .filter_map(|id| {
                membership
                    .nodes
                    .iter()
                    .find(|node| &node.id == id)
                    .map(|node| node.address.clone())
            })
            .collect();
        if replicas.is_empty() {
            return Err(BlockReadError::UnresolvedOwner);
        }
        let cached = Cached {
            replicas,
            epoch: membership.epoch,
        };
        self.cache.insert(block.clone(), cached.clone(), now_ms);
        Ok(cached)
    }

    async fn membership_snapshot(
        &self,
        now_ms: u64,
        force_refresh: bool,
    ) -> Result<MembershipSnapshot, BlockReadError> {
        if !force_refresh {
            if let Some(snapshot) = self.membership.fresh(now_ms) {
                return Ok(snapshot);
            }
        }
        let _refresh = self.membership_refresh.lock().await;
        if !force_refresh {
            if let Some(snapshot) = self.membership.fresh(now_ms) {
                return Ok(snapshot);
            }
        }
        match self.coordinator.membership().await {
            Ok(nodes) => {
                let (snapshot, changed) = self.membership.replace(nodes, now_ms);
                if changed {
                    self.cache.clear();
                }
                Ok(snapshot)
            }
            Err(error) => {
                if let Some(snapshot) = self.membership.last_good() {
                    tracing::warn!(%error, "membership refresh failed; using last-good snapshot");
                    Ok(snapshot)
                } else {
                    Err(error.into())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, NodeId, NodeInfo, NodeRole, ObjectId, Version};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use talon_transport::{
        decode_request, encode_error, response_header_ok, ControlMessage, RangeRequest,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn block() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", "o/1"),
            256 << 20, // second block, non-zero offset
            256 << 20,
            Version::new("v1"),
        )
    }

    /// A mock coordinator that advertises one worker membership entry.
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

    /// A mock worker that returns deterministic bytes for the requested range,
    /// and records how many fetches it served.
    async fn mock_worker(hits: Arc<std::sync::atomic::AtomicU32>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let hits = Arc::clone(&hits);
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
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    // Encode the absolute offset into the bytes so tests can
                    // verify the worker got the right sub-range.
                    let payload: Vec<u8> = (0..req.len)
                        .map(|i| ((req.offset + i) % 256) as u8)
                        .collect();
                    let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                    out.extend_from_slice(&payload);
                    s.write_all(&out).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    /// A mock worker that always replies with an ERROR frame ("not present"),
    /// counting how many requests it saw. Loops so it survives retries.
    async fn spawn_erroring_worker(count: Arc<std::sync::atomic::AtomicU32>) -> String {
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
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    s.write_all(&encode_error(0, "block not present"))
                        .await
                        .unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    /// A mock worker that returns a self-consistent but one-byte-short success
    /// frame, counting how many requests it saw.
    async fn spawn_short_reply_worker(count: Arc<std::sync::atomic::AtomicU32>) -> String {
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
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let short_len = req.len.saturating_sub(1) as usize;
                    let payload = vec![0u8; short_len];
                    let mut out = response_header_ok(0, short_len as u32).to_vec();
                    out.extend_from_slice(&payload);
                    s.write_all(&out).await.unwrap();
                    s.flush().await.unwrap();
                });
            }
        });
        addr
    }

    /// Advertise two workers while assigning `primary` to whichever stable ID
    /// ranks first for the test block.
    async fn mock_coordinator_two(primary: String, secondary: String) -> String {
        let candidates = vec![
            NodeInfo {
                id: NodeId::new("w1"),
                address: String::new(),
                role: NodeRole::Worker,
            },
            NodeInfo {
                id: NodeId::new("w2"),
                address: String::new(),
                role: NodeRole::Worker,
            },
        ];
        let first = rank_cache_workers(&block(), &candidates, 1).remove(0);
        let (w1, w2) = if first == NodeId::new("w1") {
            (primary, secondary)
        } else {
            (secondary, primary)
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let (w1, w2) = (w1.clone(), w2.clone());
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
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![
                                NodeInfo {
                                    id: NodeId::new("w1"),
                                    address: w1.clone(),
                                    role: NodeRole::Worker,
                                },
                                NodeInfo {
                                    id: NodeId::new("w2"),
                                    address: w2.clone(),
                                    role: NodeRole::Worker,
                                },
                            ],
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

    #[tokio::test]
    async fn miss_then_hit_fetches_correct_range() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;

        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        let blk = block();
        // First read: cache miss → coordinator resolve → worker fetch.
        let bytes = reader.read_block(&blk, 100, 64, 0).await.unwrap();
        assert_eq!(bytes.len(), 64);
        let abs = blk.offset + 100;
        assert_eq!(bytes[0], (abs % 256) as u8);
        assert_eq!(bytes[1], ((abs + 1) % 256) as u8);
        assert_eq!(cache.len(), 1, "placement cached after miss");

        // Second read: cache hit (still 1 entry), worker serves again.
        let _ = reader.read_block(&blk, 0, 16, 1).await.unwrap();
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);

        // Metrics reflect one miss then one hit, two worker fetches, bytes served.
        let snap = reader.stats().snapshot();
        assert_eq!(snap.cache_misses, 1);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.worker_fetches, 2);
        assert_eq!(snap.worker_failures, 0);
        assert_eq!(snap.bytes_served, 64 + 16);
        assert_eq!(snap.hit_ratio(), 0.5);
    }

    #[tokio::test]
    async fn empty_cluster_yields_no_owners() {
        // Coordinator advertises an empty worker membership.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            s.read_exact(&mut hdr).await.unwrap();
            let h = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; h.length as usize];
            s.read_exact(&mut body).await.unwrap();
            let reply = ControlMessage::MembershipList { nodes: Vec::new() };
            s.write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            s.flush().await.unwrap();
        });
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(addr), cache, 1);
        let err = reader.read_block(&block(), 0, 16, 0).await.unwrap_err();
        assert!(matches!(err, BlockReadError::NoOwners));
    }

    #[tokio::test]
    async fn coordinator_outage_uses_last_good_membership_for_a_new_block() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = [0u8; HEADER_LEN];
            stream.read_exact(&mut header).await.unwrap();
            let header = FrameHeader::decode(&header).unwrap();
            let mut body = vec![0u8; header.length as usize];
            stream.read_exact(&mut body).await.unwrap();
            let reply = ControlMessage::MembershipList {
                nodes: vec![NodeInfo {
                    id: NodeId::new("worker-a"),
                    address: worker_addr,
                    role: NodeRole::Worker,
                }],
            };
            stream
                .write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            stream.flush().await.unwrap();
            // Dropping the listener simulates the complete management-plane outage.
        });

        let cache = Arc::new(PlacementCache::new(0));
        let reader = BlockReader::new(CoordinatorClient::new(coordinator), cache, 1);
        reader.read_block(&block(), 0, 16, 0).await.unwrap();
        let mut next = block();
        next.offset += u64::from(next.block_size);
        let bytes = reader.read_block(&next, 0, 16, 1).await.unwrap();

        assert_eq!(bytes.len(), 16);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn all_replicas_failing_errors_after_refresh() {
        // Single owner whose worker serves exactly one error then closes. The
        // reader tries it, refreshes (same owner), and on the second attempt the
        // worker is gone → connect failure → AllReplicasFailed.
        let worker_addr = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let a = listener.local_addr().unwrap().to_string();
            tokio::spawn(async move {
                let (mut s, _) = listener.accept().await.unwrap();
                let mut hdr = [0u8; HEADER_LEN];
                s.read_exact(&mut hdr).await.unwrap();
                let h = FrameHeader::decode(&hdr).unwrap();
                let mut body = vec![0u8; h.length as usize];
                s.read_exact(&mut body).await.unwrap();
                s.write_all(&encode_error(0, "block not present"))
                    .await
                    .unwrap();
                s.flush().await.unwrap();
            });
            a
        };
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), cache, 1);
        let err = reader.read_block(&block(), 0, 16, 0).await.unwrap_err();
        assert!(matches!(err, BlockReadError::AllReplicasFailed));
    }

    #[tokio::test]
    async fn falls_back_to_second_replica_on_wrong_owner() {
        // Primary w1 always errors "not present"; secondary w2 serves the bytes.
        // The reader must walk from w1 to w2 within the cached list — no refresh.
        let bad = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let good = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let w1 = spawn_erroring_worker(Arc::clone(&bad)).await;
        let w2 = mock_worker(Arc::clone(&good)).await;
        let coord = mock_coordinator_two(w1, w2).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        // Request k=2 so both owners are cached.
        let reader = BlockReader::new(CoordinatorClient::new(coord), Arc::clone(&cache), 2);

        let bytes = reader.read_block(&block(), 0, 32, 0).await.unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(
            bad.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "primary tried"
        );
        assert_eq!(
            good.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "fell back to w2"
        );
        // Placement stays cached (fallback within the list, no invalidation).
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn short_reply_from_worker_falls_back_to_next_replica() {
        let short = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let good = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let w1 = spawn_short_reply_worker(Arc::clone(&short)).await;
        let w2 = mock_worker(Arc::clone(&good)).await;
        let coord = mock_coordinator_two(w1, w2).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord), Arc::clone(&cache), 2);

        let bytes = reader.read_block(&block(), 0, 32, 0).await.unwrap();
        assert_eq!(bytes.len(), 32);
        assert_eq!(
            short.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "short primary reply was rejected"
        );
        assert_eq!(
            good.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "read fell back to the healthy replica"
        );
        assert_eq!(cache.len(), 1, "fallback did not invalidate placement");
        let stats = reader.stats().snapshot();
        assert_eq!(stats.worker_fetches, 2);
        assert_eq!(stats.worker_failures, 1);
        assert_eq!(stats.coordinator_refreshes, 0);
        assert_eq!(stats.bytes_served, 32);
    }

    #[tokio::test]
    async fn observe_epoch_invalidates_stale_entry() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        // Warm the cache from the locally versioned membership snapshot.
        let _ = reader.read_block(&block(), 0, 8, 0).await.unwrap();
        assert_eq!(cache.len(), 1);
        let epoch = cache.get(&block(), 0).unwrap().epoch;
        // The identical version token does not invalidate.
        assert!(!reader.observe_epoch(&block(), epoch));
        assert_eq!(cache.len(), 1);
        // A different token drops the entry so the next read re-looks-up.
        assert!(reader.observe_epoch(&block(), epoch.wrapping_add(1)));
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn multi_block_read_stitches_in_order() {
        // Small block size so a modest read spans several blocks.
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), Arc::clone(&cache), 1);

        let obj = ObjectId::new(Backend::S3, "b", "o/1");
        let ver = Version::new("v1");
        let bs = 1024u32;
        let size = 100_000u64;
        // Read 900..900+2300 → spans 4 blocks (tail, full, full, head).
        let offset = 900u64;
        let len = 2300u64;
        let file = FileView {
            object: &obj,
            block_size: bs,
            version: &ver,
            size,
        };
        let bytes = reader.read(&file, offset, len, 0).await.unwrap();
        assert_eq!(bytes.len() as u64, len);
        // The mock worker fills each byte with (absolute_offset % 256); the
        // stitched buffer must be contiguous across block boundaries.
        for (i, b) in bytes.iter().enumerate() {
            assert_eq!(*b, ((offset + i as u64) % 256) as u8, "byte {i} mismatch");
        }
        // Four distinct blocks were fetched (one worker call each).
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(cache.len(), 4, "each block's placement cached");
    }

    #[tokio::test]
    async fn read_past_eof_is_empty_without_fetch() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker_addr = mock_worker(Arc::clone(&hits)).await;
        let coord_addr = mock_coordinator(worker_addr).await;
        let cache = Arc::new(PlacementCache::new(10_000));
        let reader = BlockReader::new(CoordinatorClient::new(coord_addr), cache, 1);

        let obj = ObjectId::new(Backend::S3, "b", "o/1");
        let ver = Version::new("v1");
        let file = FileView {
            object: &obj,
            block_size: 1024,
            version: &ver,
            size: 1500,
        };
        let bytes = reader.read(&file, 5000, 10, 0).await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
