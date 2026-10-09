//! Client-side file prewarm using the read path's cached membership and Maglev table.

use std::collections::HashMap;
use std::time::Duration;

use futures::stream::{self, StreamExt, TryStreamExt};

use crate::{iter_read, BlockReadError, BlockReader, FileView, WorkerLoadError};
use talon_transport::{
    codec::{BATCH_LOAD_BODY_OVERHEAD, MAX_BATCH_LOAD_BLOCKS, MAX_BATCH_LOAD_BYTES},
    LoadBlockRequest,
};

/// Completed prewarm; cache entries remain subject to normal eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadResult {
    /// Object length in bytes.
    pub size: u64,
    /// Number of blocks loaded on their primary owners.
    pub blocks: u64,
}

/// Failures from client-side prewarm. Completed fills remain cached on error.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The supplied file coordinates cannot describe a valid load.
    #[error("invalid load request: {0}")]
    InvalidArgument(String),
    /// No usable membership or placement is available.
    #[error("load placement: {0}")]
    Placement(#[from] BlockReadError),
    /// One selected primary could not complete its assignment.
    #[error("worker {worker} failed loading offset {offset}: {source}")]
    Worker {
        /// Selected primary address.
        worker: String,
        /// Offset of the failing block in the object.
        offset: u64,
        /// Worker transport/protocol error or rejection.
        #[source]
        source: WorkerLoadError,
    },
    /// A worker failed a batch; its diagnostic identifies the failing block.
    #[error("worker {worker} failed batch load: {source}")]
    WorkerBatch {
        /// Selected primary address.
        worker: String,
        /// Transport error or rejection; completed fills remain cached.
        #[source]
        source: WorkerLoadError,
    },
    /// The operation exhausted its overall time budget.
    #[error("load timed out; completed blocks remain cached")]
    Timeout,
    /// Concurrent discovery changed this client's view while it was loading.
    #[error("worker membership changed during load; retry against current placement")]
    MembershipChanged,
}

fn validate_file(file: &FileView<'_>) -> Result<LoadResult, LoadError> {
    if file.block_size == 0 || file.version.as_str().trim().is_empty() {
        return Err(LoadError::InvalidArgument(
            "non-empty version and non-zero block size required".into(),
        ));
    }
    let bs = u64::from(file.block_size);
    let blocks = file.size.div_ceil(bs);
    if blocks.checked_mul(bs).is_none() {
        return Err(LoadError::InvalidArgument(
            "object extent overflows block addressing".into(),
        ));
    }
    Ok(LoadResult {
        size: file.size,
        blocks,
    })
}

impl BlockReader {
    /// Warm a file directly on its primary workers using the read placement.
    ///
    /// The caller supplies version and size, so no HEAD is issued. Membership
    /// discovery reuses the read path's bounded instance cache;
    /// no LOAD is sent to the coordinator. Each load uses one logical topology, at most
    /// eight pending block futures, and a 30-minute overall deadline. Clones
    /// share eight active block requests. Dropping this future stops dispatch;
    /// workers may finish already accepted requests.
    pub async fn load(&self, file: &FileView<'_>, _now_ms: u64) -> Result<LoadResult, LoadError> {
        let LoadResult { blocks, .. } = validate_file(file)?;
        if blocks == 0 {
            return Ok(LoadResult { size: 0, blocks: 0 });
        }
        tokio::time::timeout(Duration::from_secs(30 * 60), async {
            let snapshot = self.membership_snapshot().await?;
            if snapshot.placement.worker_count() == 0 {
                return Err(LoadError::Placement(BlockReadError::NoOwners));
            }
            stream::iter(iter_read(
                file.object,
                0,
                file.size,
                file.block_size,
                file.version,
                file.size,
            ))
            .map(|segment| {
                let topology = &snapshot.placement;
                async move {
                    let _permit = self
                        .load_slots
                        .acquire()
                        .await
                        .expect("load semaphore is never closed");
                    let current = self.membership_snapshot().await?;
                    if !std::sync::Arc::ptr_eq(&current.placement, topology) {
                        return Err(LoadError::MembershipChanged);
                    }
                    let client = current.owner(&segment.block)?;
                    client
                        .load_block(&segment.block, u64::from(segment.len))
                        .await
                        .map_err(|source| LoadError::Worker {
                            worker: client.addr().into(),
                            offset: segment.block.offset,
                            source,
                        })
                }
            })
            .buffer_unordered(8)
            .try_for_each(|()| async { Ok(()) })
            .await?;
            // Do not contact discovery just to acknowledge success. This checks
            // changes observed by concurrent operations on the shared client.
            if self.membership.last_good().is_some_and(|current| {
                !std::sync::Arc::ptr_eq(&current.placement, &snapshot.placement)
            }) {
                return Err(LoadError::MembershipChanged);
            }
            Ok(LoadResult {
                size: file.size,
                blocks,
            })
        })
        .await
        .map_err(|_| LoadError::Timeout)?
    }

    /// Prewarm multiple files using protocol-level batches grouped by primary.
    /// One frame carries up to 1024 assignments within the control-frame byte
    /// limit (including tracing overhead), with one Ack.
    /// Results follow input order, including empty files and duplicates. One
    /// logical topology and 30-minute deadline cover the entire call. Instance
    /// discovery is refreshed before dispatch when its validity has expired.
    /// Single and batch loads share eight active RPCs. Failure cancels pending
    /// dispatch; already accepted worker batches may finish. This is not atomic.
    pub async fn batch_load(
        &self,
        files: &[FileView<'_>],
        _now_ms: u64,
    ) -> Result<Vec<LoadResult>, LoadError> {
        let results = files
            .iter()
            .map(validate_file)
            .collect::<Result<Vec<_>, _>>()?;
        // Reject unencodable individual assignments before any worker I/O.
        for file in files.iter().filter(|file| file.size != 0) {
            let request = LoadBlockRequest {
                block: talon_core::BlockId::new(
                    file.object.clone(),
                    0,
                    file.block_size,
                    file.version.clone(),
                ),
                len: file.size.min(u64::from(file.block_size)),
            };
            let bytes = request
                .encoded_len()
                .map_err(|e| LoadError::InvalidArgument(e.to_string()))?;
            if BATCH_LOAD_BODY_OVERHEAD + bytes > MAX_BATCH_LOAD_BYTES {
                return Err(LoadError::InvalidArgument(
                    "block identity exceeds batch frame limit".into(),
                ));
            }
        }
        if results.iter().all(|result| result.blocks == 0) {
            return Ok(results);
        }
        tokio::time::timeout(Duration::from_secs(30 * 60), async {
            let snapshot = self.membership_snapshot().await?;
            if snapshot.placement.worker_count() == 0 {
                return Err(LoadError::Placement(BlockReadError::NoOwners));
            }
            let mut plan = files.iter().flat_map(|file| {
                iter_read(
                    file.object,
                    0,
                    file.size,
                    file.block_size,
                    file.version,
                    file.size,
                )
            });
            loop {
                // Bound planning independently of total file/block count.
                let mut groups: HashMap<String, Vec<LoadBlockRequest>> = HashMap::new();
                let mut planned_bytes = 0;
                for segment in plan.by_ref().take(MAX_BATCH_LOAD_BLOCKS * 8) {
                    let owner = snapshot
                        .placement
                        .primary(&segment.block)
                        .expect("non-empty placement");
                    let request = LoadBlockRequest {
                        block: segment.block,
                        len: u64::from(segment.len),
                    };
                    planned_bytes += request
                        .encoded_len()
                        .map_err(|e| LoadError::InvalidArgument(e.to_string()))?;
                    groups.entry(owner.id.0.clone()).or_default().push(request);
                    if planned_bytes >= MAX_BATCH_LOAD_BYTES * 8 {
                        break;
                    }
                }
                if groups.is_empty() {
                    break;
                }
                let mut batches = Vec::new();
                for (worker, requests) in groups {
                    let mut batch = Vec::new();
                    let mut bytes = BATCH_LOAD_BODY_OVERHEAD;
                    for request in requests {
                        let size = request
                            .encoded_len()
                            .map_err(|e| LoadError::InvalidArgument(e.to_string()))?;
                        if batch.len() == MAX_BATCH_LOAD_BLOCKS
                            || bytes + size > MAX_BATCH_LOAD_BYTES
                        {
                            batches.push((worker.clone(), std::mem::take(&mut batch)));
                            bytes = BATCH_LOAD_BODY_OVERHEAD;
                        }
                        bytes += size;
                        batch.push(request);
                    }
                    if !batch.is_empty() {
                        batches.push((worker, batch));
                    }
                }
                stream::iter(batches)
                    .map(|(_worker_id, blocks)| {
                        let topology = &snapshot.placement;
                        async move {
                            let _permit = self
                                .load_slots
                                .acquire()
                                .await
                                .expect("load semaphore is never closed");
                            let current = self.membership_snapshot().await?;
                            if !std::sync::Arc::ptr_eq(&current.placement, topology) {
                                return Err(LoadError::MembershipChanged);
                            }
                            let client = current.owner(&blocks[0].block)?;
                            client.batch_load(&blocks).await.map_err(|source| {
                                LoadError::WorkerBatch {
                                    worker: client.addr().into(),
                                    source,
                                }
                            })
                        }
                    })
                    .buffer_unordered(8)
                    .try_for_each(|()| async { Ok(()) })
                    .await?;
            }
            if self.membership.last_good().is_some_and(|current| {
                !std::sync::Arc::ptr_eq(&current.placement, &snapshot.placement)
            }) {
                return Err(LoadError::MembershipChanged);
            }
            Ok(results)
        })
        .await
        .map_err(|_| LoadError::Timeout)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CoordinatorClient, PlacementCache, WorkerClient};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use talon_core::{Backend, BlockId, NodeId, NodeInfo, NodeRole, ObjectId, Version};
    use talon_transport::{codec, ControlMessage, FrameHeader, HEADER_LEN};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn receive(stream: &mut TcpStream) -> Option<(u32, ControlMessage)> {
        let mut header = [0; HEADER_LEN];
        stream.read_exact(&mut header).await.ok()?;
        let parsed = FrameHeader::decode(&header).unwrap();
        let mut frame = header.to_vec();
        frame.resize(HEADER_LEN + parsed.length as usize, 0);
        stream.read_exact(&mut frame[HEADER_LEN..]).await.unwrap();
        Some((parsed.request_id, codec::decode_request(&frame).unwrap().1))
    }

    struct Worker {
        node: NodeInfo,
        requests: Arc<Mutex<Vec<(BlockId, u64)>>>,
        batch_sizes: Arc<Mutex<Vec<usize>>>,
        peak: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Worker {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn worker(id: &str, delay: Duration, ok: bool) -> Worker {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node = NodeInfo {
            id: NodeId::new(id),
            address: listener.local_addr().unwrap().to_string(),
            role: NodeRole::Worker,
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let recorded_batches = batch_sizes.clone();
        let peak = Arc::new(AtomicUsize::new(0));
        let recorded = requests.clone();
        let peak_load = peak.clone();
        let active = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        let (requests, peak, active) = (recorded.clone(), peak_load.clone(), active.clone());
                        let batches = recorded_batches.clone();
                        connections.spawn(async move {
                            while let Some((id, request)) = receive(&mut stream).await {
                                match request {
                                    ControlMessage::LoadBlock { block, len } => requests.lock().unwrap().push((block, len)),
                                    ControlMessage::BatchLoad { blocks } => {
                                        batches.lock().unwrap().push(blocks.len());
                                        requests.lock().unwrap().extend(blocks.into_iter().map(|r| (r.block, r.len)));
                                    }
                                    other => panic!("unexpected request: {other:?}"),
                                }
                                peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                                tokio::time::sleep(delay).await;
                                active.fetch_sub(1, Ordering::SeqCst);
                                let response = ControlMessage::Ack { ok, detail: (!ok).then(|| "origin unavailable".into()) };
                                if stream.write_all(&codec::encode(id, &response).unwrap()).await.is_err() { break; }
                            }
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
                }
            }
        });
        Worker {
            node,
            requests,
            batch_sizes,
            peak,
            task,
        }
    }

    fn reader(address: String, nodes: &[NodeInfo]) -> BlockReader {
        let reader = BlockReader::new(
            CoordinatorClient::new(address),
            Arc::new(PlacementCache::new(30_000)),
            1,
        );
        if !nodes.is_empty() {
            install_membership(&reader, nodes, std::time::Instant::now());
        }
        reader
    }
    fn install_membership(reader: &BlockReader, nodes: &[NodeInfo], observed: std::time::Instant) {
        let ControlMessage::MembershipList { mut view } =
            crate::membership_fixture::plain(nodes.to_vec())
        else {
            unreachable!()
        };
        let registry = talon_core::worker_membership::MemberRegistry {
            format_version: 1,
            members: view.workers.iter().map(|w| w.member.clone()).collect(),
        };
        view.topology_token = registry.topology_token();
        reader
            .membership
            .replace(view, observed, &crate::ConnectionPool::new());
    }

    fn object() -> ObjectId {
        ObjectId::new(Backend::S3, "bucket", "file")
    }

    #[tokio::test]
    async fn load_does_not_route_to_offline_conflicting_or_expired_instances() {
        use talon_core::worker_membership::InstanceState;
        let a = worker("a", Duration::ZERO, true).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let file = FileView {
            object: &object(),
            version: &Version::new("v1"),
            size: 8,
            block_size: 8,
        };
        for state in [InstanceState::Offline, InstanceState::Conflict] {
            let ControlMessage::MembershipList { mut view } =
                crate::membership_fixture::plain(vec![a.node.clone()])
            else {
                unreachable!()
            };
            view.workers[0].state = state;
            reader.membership.replace(
                view,
                std::time::Instant::now(),
                &crate::ConnectionPool::new(),
            );
            for error in [
                reader.load(&file, 100).await.unwrap_err(),
                reader
                    .batch_load(std::slice::from_ref(&file), 100)
                    .await
                    .unwrap_err(),
            ] {
                assert!(
                    matches!(error, LoadError::Placement(BlockReadError::Worker(crate::WorkerError::Remote(ref error))) if error.code == talon_transport::DataErrorCode::Unavailable)
                );
            }
        }
        let expired = WorkerClient::new(a.node.address.clone())
            .with_read_retry_deadline(std::time::Instant::now());
        let request = LoadBlockRequest {
            block: BlockId::new(object(), 0, 8, Version::new("v1")),
            len: 8,
        };
        assert!(
            matches!(expired.load_block(&request.block, request.len).await, Err(WorkerLoadError::Instance(crate::WorkerError::Remote(error))) if error.code == talon_transport::DataErrorCode::Unavailable)
        );
        assert!(
            matches!(expired.batch_load(&[request]).await, Err(WorkerLoadError::Instance(crate::WorkerError::Remote(error))) if error.code == talon_transport::DataErrorCode::Unavailable)
        );
        assert!(a.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn batch_load_sends_1024_assignments_per_frame_and_preserves_input_results() {
        let a = worker("a", Duration::ZERO, true).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let objects = (0..1025)
            .map(|i| ObjectId::new(Backend::S3, "bucket", format!("file-{i}")))
            .collect::<Vec<_>>();
        let version = Version::new("v1");
        let mut files = objects
            .iter()
            .enumerate()
            .map(|(i, object)| FileView {
                object,
                version: &version,
                size: (i % 8 + 1) as u64,
                block_size: 8,
            })
            .collect::<Vec<_>>();
        files.push(FileView {
            object: &objects[0],
            version: &version,
            size: 0,
            block_size: 8,
        });
        let results = reader.batch_load(&files, 100).await.unwrap();
        assert_eq!(results.len(), files.len());
        for (result, file) in results.iter().zip(&files) {
            assert_eq!(result.size, file.size);
            assert_eq!(result.blocks, u64::from(file.size != 0));
        }
        let mut sizes = a.batch_sizes.lock().unwrap().clone();
        sizes.sort_unstable();
        assert_eq!(
            sizes,
            [1, 1024],
            "1025 files must use two batch frames, not 1025 single LOADs"
        );
        assert_eq!(a.requests.lock().unwrap().len(), 1025);
    }

    #[tokio::test]
    async fn batch_load_groups_multiple_files_by_maglev_owner() {
        let a = worker("a", Duration::ZERO, true).await;
        let b = worker("b", Duration::ZERO, true).await;
        let reader = reader("127.0.0.1:0".into(), &[a.node.clone(), b.node.clone()]);
        let object_a = object();
        let object_b = ObjectId::new(Backend::S3, "bucket", "other");
        let version = Version::new("v2");
        let files = [
            FileView {
                object: &object_a,
                version: &version,
                size: 131,
                block_size: 8,
            },
            FileView {
                object: &object_b,
                version: &version,
                size: 25,
                block_size: 8,
            },
        ];
        assert_eq!(
            reader.batch_load(&files, 100).await.unwrap(),
            [
                LoadResult {
                    size: 131,
                    blocks: 17
                },
                LoadResult {
                    size: 25,
                    blocks: 4
                }
            ]
        );
        let snapshot = reader.membership.last_good().unwrap();
        let mut total = 0;
        for worker in [&a, &b] {
            let requests = worker.requests.lock().unwrap();
            assert!(!requests.is_empty());
            assert_eq!(
                worker.batch_sizes.lock().unwrap().as_slice(),
                &[requests.len()]
            );
            for (block, len) in requests.iter() {
                assert_eq!(
                    snapshot.placement.primary(block).unwrap().id,
                    worker.node.id
                );
                assert_eq!(block.version, version);
                let size = if block.object == object_a { 131 } else { 25 };
                assert_eq!(*len, 8.min(size - block.offset));
            }
            total += requests.len();
        }
        assert_eq!(total, 21);
    }

    #[tokio::test]
    async fn batch_load_splits_long_identities_by_bytes_and_rejects_invalid_input_before_io() {
        let a = worker("a", Duration::ZERO, true).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let object = ObjectId::new(Backend::S3, "bucket", "x".repeat(600_000));
        let version = Version::new("v1");
        let files = [FileView {
            object: &object,
            version: &version,
            size: 16,
            block_size: 8,
        }];
        reader.batch_load(&files, 100).await.unwrap();
        assert_eq!(a.batch_sizes.lock().unwrap().as_slice(), &[1, 1]);
        let invalid = FileView {
            block_size: 0,
            ..files[0]
        };
        assert!(matches!(
            reader.batch_load(&[files[0].clone(), invalid], 100).await,
            Err(LoadError::InvalidArgument(_))
        ));
        assert!(reader.batch_load(&[], 100).await.unwrap().is_empty());
        assert_eq!(a.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn batch_and_single_load_share_capacity_and_batch_failure_is_not_retried() {
        let a = worker("a", Duration::from_millis(10), true).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let file = FileView {
            object: &object(),
            version: &Version::new("v1"),
            size: 8192 * 8,
            block_size: 8,
        };
        let files = [file.clone()];
        let single_file = FileView { size: 128, ..file };
        let (batch, single) = tokio::join!(
            reader.batch_load(&files, 100),
            reader.load(&single_file, 100)
        );
        batch.unwrap();
        single.unwrap();
        assert_eq!(a.peak.load(Ordering::SeqCst), 8);
        assert_eq!(a.batch_sizes.lock().unwrap().len(), 8);
        let rejected = worker("rejected", Duration::ZERO, false).await;
        let reader = self::reader("127.0.0.1:0".into(), std::slice::from_ref(&rejected.node));
        assert!(matches!(
            reader
                .batch_load(&[FileView { size: 8, ..file }], 100)
                .await,
            Err(LoadError::WorkerBatch {
                source: WorkerLoadError::Rejected(_),
                ..
            })
        ));
        assert_eq!(rejected.batch_sizes.lock().unwrap().len(), 1);
        assert_eq!(reader.load_slots.available_permits(), 8);
    }

    #[tokio::test]
    async fn load_directly_routes_all_blocks_without_contacting_cached_discovery() {
        let discovery = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = worker("a", Duration::ZERO, true).await;
        let b = worker("b", Duration::ZERO, true).await;
        let c = worker("c", Duration::ZERO, true).await;
        let reader = reader(
            discovery.local_addr().unwrap().to_string(),
            &[a.node.clone(), b.node.clone(), c.node.clone()],
        );
        let result = reader
            .load(
                &FileView {
                    object: &object(),
                    version: &Version::new("v1"),
                    size: 131,
                    block_size: 8,
                },
                100,
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            LoadResult {
                size: 131,
                blocks: 17
            }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), discovery.accept())
                .await
                .is_err(),
            "LOAD must not contact coordinator with fresh membership"
        );
        let placement = reader.membership.last_good().unwrap().placement;
        let mut offsets = Vec::new();
        for worker in [&a, &b, &c] {
            let requests = worker.requests.lock().unwrap();
            assert!(!requests.is_empty());
            for (block, len) in requests.iter() {
                assert_eq!(placement.primary(block).unwrap().id, worker.node.id);
                assert_eq!(block.version, Version::new("v1"));
                assert_eq!(*len, if block.offset == 128 { 3 } else { 8 });
                offsets.push(block.offset);
            }
        }
        offsets.sort_unstable();
        assert_eq!(offsets, (0..17).map(|n| n * 8).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn discovered_membership_keeps_load_working_after_coordinator_stops() {
        let a = worker("a", Duration::ZERO, true).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reader = reader(listener.local_addr().unwrap().to_string(), &[]);
        let node = a.node.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (id, request) = receive(&mut socket).await.unwrap();
            assert_eq!(request, ControlMessage::MembershipQuery {});
            let reply = crate::membership_fixture::plain(vec![node]);
            socket
                .write_all(&codec::encode(id, &reply).unwrap())
                .await
                .unwrap();
            // Dropping listener and socket ends discovery before any LOAD reply.
        });
        let file = FileView {
            object: &object(),
            version: &Version::new("v1"),
            size: 17,
            block_size: 8,
        };
        reader.load(&file, 100).await.unwrap();
        server.await.unwrap();
        reader.load(&file, 101).await.unwrap();
        reader
            .batch_load(std::slice::from_ref(&file), 102)
            .await
            .unwrap();
        assert_eq!(a.requests.lock().unwrap().len(), 9);
        assert_eq!(a.batch_sizes.lock().unwrap().as_slice(), &[3]);
        install_membership(
            &reader,
            std::slice::from_ref(&a.node),
            std::time::Instant::now() - Duration::from_secs(1),
        );
        assert!(
            reader.load(&file, 103).await.is_err(),
            "expired discovery must not dial a stale instance during outage"
        );
        assert!(reader.batch_load(&[file], 104).await.is_err());
        assert_eq!(a.requests.lock().unwrap().len(), 9);
    }

    #[tokio::test]
    async fn load_clones_share_a_bounded_window_and_release_it_after_timeout() {
        let a = worker("a", Duration::from_millis(10), true).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let clone = reader.clone();
        let file = FileView {
            object: &object(),
            version: &Version::new("v1"),
            size: 800,
            block_size: 8,
        };
        let (left, right) = tokio::join!(reader.load(&file, 100), clone.load(&file, 100));
        assert_eq!(left.unwrap().blocks, 100);
        assert_eq!(right.unwrap().blocks, 100);
        assert_eq!(a.peak.load(Ordering::SeqCst), 8);
        let stalled = worker("stalled", Duration::from_secs(240), true).await;
        let reader = self::reader("127.0.0.1:0".into(), std::slice::from_ref(&stalled.node));
        let clone = reader.clone();
        let pending = tokio::spawn(async move {
            clone
                .load(
                    &FileView {
                        object: &object(),
                        version: &Version::new("v1"),
                        size: 8,
                        block_size: 8,
                    },
                    100,
                )
                .await
        });
        wait_for_requests(&stalled, 1).await;
        // Pause only after real TCP I/O completes: auto-advancing time while
        // sockets connect can jump straight to the overall LOAD deadline.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(121)).await;
        let result = pending.await.unwrap();
        assert!(
            matches!(result, Err(LoadError::Worker { source: WorkerLoadError::Io(error), .. }) if error.kind() == std::io::ErrorKind::TimedOut)
        );
        assert_eq!(reader.load_slots.available_permits(), 8);
    }

    async fn wait_for_requests(worker: &Worker, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while worker.requests.lock().unwrap().len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancelling_load_stops_dispatch_and_releases_client_capacity() {
        let a = worker("a", Duration::from_secs(240), true).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let clone = reader.clone();
        let pending = tokio::spawn(async move {
            clone
                .load(
                    &FileView {
                        object: &object(),
                        version: &Version::new("v1"),
                        size: 800,
                        block_size: 8,
                    },
                    100,
                )
                .await
        });
        wait_for_requests(&a, 8).await;
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        assert_eq!(reader.load_slots.available_permits(), 8);
        assert_eq!(a.requests.lock().unwrap().len(), 8);
    }

    #[tokio::test]
    async fn concurrent_membership_change_invalidates_load_completion() {
        let a = worker("a", Duration::from_millis(10), true).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let file = FileView {
            object: &object(),
            version: &Version::new("v1"),
            size: 8,
            block_size: 8,
        };
        let (result, ()) = tokio::join!(reader.load(&file, 100), async {
            wait_for_requests(&a, 1).await;
            install_membership(&reader, &[], std::time::Instant::now());
        });
        assert!(matches!(result, Err(LoadError::MembershipChanged)));
    }

    #[tokio::test]
    async fn load_retries_a_closed_pooled_connection_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = WorkerClient::new(listener.local_addr().unwrap().to_string());
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (id, message) = receive(&mut stream).await.unwrap();
                assert!(matches!(message, ControlMessage::LoadBlock { .. }));
                stream
                    .write_all(
                        &codec::encode(
                            id,
                            &ControlMessage::Ack {
                                ok: true,
                                detail: None,
                            },
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                // Retire the connection after its acknowledgement.
            }
        });
        let block = BlockId::new(object(), 0, 8, Version::new("v1"));
        client.load_block(&block, 8).await.unwrap();
        client.load_block(&block, 8).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn load_surfaces_worker_failure_and_rejects_invalid_coordinates_without_io() {
        let a = worker("a", Duration::ZERO, false).await;
        let reader = reader("127.0.0.1:0".into(), std::slice::from_ref(&a.node));
        let file = FileView {
            object: &object(),
            version: &Version::new("v1"),
            size: 8,
            block_size: 8,
        };
        assert!(matches!(
            reader
                .load(
                    &FileView {
                        size: u64::MAX,
                        ..file
                    },
                    100
                )
                .await,
            Err(LoadError::InvalidArgument(_))
        ));
        assert!(matches!(
            reader
                .load(
                    &FileView {
                        block_size: 0,
                        ..file
                    },
                    100
                )
                .await,
            Err(LoadError::InvalidArgument(_))
        ));
        assert_eq!(
            reader
                .load(&FileView { size: 0, ..file }, 100)
                .await
                .unwrap()
                .blocks,
            0
        );
        assert!(a.requests.lock().unwrap().is_empty());
        let error = reader.load(&file, 100).await.unwrap_err();
        assert!(matches!(
            error,
            LoadError::Worker {
                offset: 0,
                source: WorkerLoadError::Rejected(_),
                ..
            }
        ));
        assert_eq!(
            a.requests.lock().unwrap().len(),
            1,
            "rejections must not be retried"
        );
    }

    #[tokio::test]
    async fn load_rejects_oversized_and_uncorrelated_worker_replies() {
        for oversized in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (id, _) = receive(&mut stream).await.unwrap();
                let header = FrameHeader::new(
                    talon_transport::MsgType::Control,
                    if oversized { id } else { id.wrapping_add(1) },
                    if oversized {
                        talon_transport::MAX_CONTROL_PAYLOAD_LEN + 1
                    } else {
                        0
                    },
                );
                stream.write_all(&header.encode()).await.unwrap();
            });
            let block = BlockId::new(object(), 0, 8, Version::new("v1"));
            assert!(matches!(
                WorkerClient::new(address).load_block(&block, 8).await,
                Err(WorkerLoadError::Protocol(_))
            ));
            server.await.unwrap();
        }
    }
}
