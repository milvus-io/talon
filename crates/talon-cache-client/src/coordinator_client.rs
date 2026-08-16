//! Control-plane client for talking to the coordinator.
//!
//! The FUSE read path fetches one answer from the coordinator:
//!
//! - **membership** - the client caches healthy worker IDs and addresses from a
//!   [`MembershipQuery`](ControlMessage::MembershipQuery), then computes block
//!   placement locally. ([`CoordinatorClient::membership`])
//!
//! [`CoordinatorClient::placement_lookup`] and
//! [`CoordinatorClient::locate_primary`] remain available for wire compatibility
//! with older callers, but the current read path does not use them.
//!
//! Both are single request/response round-trips over the control plane: write
//! one [`MsgType::Control`] frame carrying a bincode [`ControlMessage`], read
//! one framed [`ControlMessage`] back. Connections are reused from a shared
//! [`ConnectionPool`] so warm lookups skip the TCP handshake (issue #181). The
//! transport framing/codec is reused verbatim
//! ([`talon_transport::encode`]/[`decode`](talon_transport::decode)), so this
//! module only owns the connect + read-a-frame glue and the response matching.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use talon_core::{BlockId, NodeId, NodeInfo, ObjectId};
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::{ControlMessage, ZonedNodeInfo, MAX_CONTROL_PAYLOAD_LEN};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pool::ConnectionPool;

/// How long to keep answering zoned membership from the v1 query after the
/// schema-v5 query failed (an older coordinator drops the connection), so an
/// un-upgraded coordinator is not re-probed on every refresh.
const MEMBERSHIP_V2_RETRY_COOLDOWN_MS: u64 = 60_000;

/// Placement answer for a block: ordered owners + the epoch they hold at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// Ordered replica node ids (primary first). Empty if no nodes.
    pub owners: Vec<NodeId>,
    /// Placement epoch these owners were computed against.
    pub epoch: u64,
}

/// An object's metadata as answered by the coordinator, for `getattr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStat {
    /// Total object length in bytes (the file size reported to FUSE).
    pub size: u64,
    /// Current source version/etag, used to address the object's blocks.
    pub version: String,
}

/// Errors from a coordinator round-trip.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    /// Failed to connect or an I/O error mid-request.
    #[error("coordinator I/O: {0}")]
    Io(#[from] std::io::Error),
    /// A frame could not be encoded/decoded.
    #[error("coordinator codec: {0}")]
    Codec(#[from] talon_transport::CodecError),
    /// The coordinator advertised a control payload above the safety cap.
    #[error("coordinator payload length {length} exceeds cap {cap}")]
    PayloadTooLarge {
        /// Number of bytes advertised by the coordinator.
        length: u32,
        /// Maximum accepted control payload size.
        cap: u32,
    },
    /// The coordinator replied, but with an unexpected message shape.
    #[error("unexpected reply to {expected}: {got:?}")]
    Unexpected {
        /// The request kind we sent.
        expected: &'static str,
        /// The reply we did not expect.
        got: Box<ControlMessage>,
    },
}

/// A thin control-plane client bound to one coordinator address.
///
/// Reuses connections from a shared [`ConnectionPool`] so warm lookups skip the
/// TCP handshake (issue #181). Cloneable — clones share the same pool.
#[derive(Debug, Clone)]
pub struct CoordinatorClient {
    addr: String,
    pool: Arc<ConnectionPool>,
    /// Unix ms of the last failed schema-v5 membership query (`0` = none);
    /// shared across clones so the fallback cooldown is per coordinator.
    membership_v2_failed_at_ms: Arc<AtomicU64>,
}

impl CoordinatorClient {
    /// Create a client that dials `addr` (`host:port`), with its own pool.
    pub fn new(addr: impl Into<String>) -> Self {
        Self::with_pool(addr, Arc::new(ConnectionPool::new()))
    }

    /// Create a client that reuses connections from the shared `pool`.
    pub fn with_pool(addr: impl Into<String>, pool: Arc<ConnectionPool>) -> Self {
        Self {
            addr: addr.into(),
            pool,
            membership_v2_failed_at_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The coordinator address this client talks to.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Ask the coordinator to locate `block`, requesting up to `k` owners.
    pub async fn placement_lookup(
        &self,
        block: &BlockId,
        k: u8,
    ) -> Result<Placement, CoordinatorError> {
        let req = ControlMessage::PlacementLookup {
            block: block.clone(),
            k,
        };
        match self.round_trip(req, "PlacementLookup").await? {
            ControlMessage::PlacementResponse { owners, epoch } => Ok(Placement { owners, epoch }),
            other => Err(CoordinatorError::Unexpected {
                expected: "PlacementLookup",
                got: Box::new(other),
            }),
        }
    }

    /// Fetch the current membership snapshot (node id + address).
    pub async fn membership(&self) -> Result<Vec<NodeInfo>, CoordinatorError> {
        let req = ControlMessage::MembershipQuery {};
        match self.round_trip(req, "MembershipQuery").await? {
            ControlMessage::MembershipList { nodes } => Ok(nodes),
            other => Err(CoordinatorError::Unexpected {
                expected: "MembershipQuery",
                got: Box::new(other),
            }),
        }
    }

    /// Fetch membership with per-node zones (ADR 0006).
    ///
    /// Sends the schema-v5 query first. A coordinator that predates schema v5
    /// drops the connection at the envelope, so any round-trip failure falls
    /// back to the v1 query (zones unknown) and starts a cooldown before v5
    /// is retried. A v5 coordinator's structured refusals (for example "not
    /// ready") surface as errors without a pointless v1 retry.
    ///
    /// `now_ms` drives the cooldown clock and is caller-supplied like every
    /// other timestamp in this crate, so tests control time.
    pub async fn membership_zoned(
        &self,
        now_ms: u64,
    ) -> Result<Vec<ZonedNodeInfo>, CoordinatorError> {
        let failed_at = self.membership_v2_failed_at_ms.load(Ordering::Relaxed);
        if failed_at == 0 || now_ms.saturating_sub(failed_at) > MEMBERSHIP_V2_RETRY_COOLDOWN_MS {
            let req = ControlMessage::MembershipQueryV2 {};
            match self.round_trip(req, "MembershipQueryV2").await {
                Ok(ControlMessage::MembershipListV2 { nodes }) => {
                    self.membership_v2_failed_at_ms.store(0, Ordering::Relaxed);
                    return Ok(nodes);
                }
                Ok(other) => {
                    return Err(CoordinatorError::Unexpected {
                        expected: "MembershipQueryV2",
                        got: Box::new(other),
                    })
                }
                Err(error) => {
                    self.membership_v2_failed_at_ms
                        .store(now_ms.max(1), Ordering::Relaxed);
                    tracing::debug!(
                        %error,
                        "zoned membership query failed; falling back to the v1 query"
                    );
                }
            }
        }
        Ok(self
            .membership()
            .await?
            .into_iter()
            .map(|info| ZonedNodeInfo { info, zone: None })
            .collect())
    }

    /// Fetch an object's size + version for `getattr` / block addressing.
    ///
    /// Forward-surface: the mount currently sizes files from the coordinator
    /// listing (`list_objects`) and addresses blocks under a canonical version
    /// (the worker owns freshness, #182), so this per-object `StatObject` RPC is
    /// not yet on the hot path. It is the natural entry point for real per-object
    /// version resolution if the mount ever becomes version-aware; kept for that
    /// and covered by its own test.
    pub async fn stat_object(&self, object: &ObjectId) -> Result<ObjectStat, CoordinatorError> {
        let req = ControlMessage::StatObject {
            object: object.clone(),
        };
        match self.round_trip(req, "StatObject").await? {
            ControlMessage::ObjectStat { size, version } => Ok(ObjectStat { size, version }),
            other => Err(CoordinatorError::Unexpected {
                expected: "StatObject",
                got: Box::new(other),
            }),
        }
    }

    /// List objects under `prefix` (mount-relative) for `readdir`.
    ///
    /// Returns `(path, size)` entries from which a protocol frontend can
    /// synthesize its namespace view.
    pub async fn list_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<talon_transport::ObjectEntry>, CoordinatorError> {
        let req = ControlMessage::ListObjects {
            prefix: prefix.to_string(),
        };
        match self.round_trip(req, "ListObjects").await? {
            ControlMessage::ObjectList { entries } => Ok(entries),
            other => Err(CoordinatorError::Unexpected {
                expected: "ListObjects",
                got: Box::new(other),
            }),
        }
    }

    /// Locate `block` and resolve the primary owner to a worker address.
    /// Combines [`placement_lookup`](Self::placement_lookup) with a
    /// [`membership`](Self::membership) resolution so a caller gets a directly
    /// dialable `host:port` for the primary owner. Returns `Ok(None)`
    /// when there are no owners (empty cluster). Returns the resolved address
    /// plus the full ordered owner list and epoch so the caller can cache the
    /// placement and fall back to other replicas.
    pub async fn locate_primary(
        &self,
        block: &BlockId,
        k: u8,
    ) -> Result<Option<ResolvedPlacement>, CoordinatorError> {
        let placement = self.placement_lookup(block, k).await?;
        if placement.owners.is_empty() {
            return Ok(None);
        }
        let members = self.membership().await?;
        let by_id: HashMap<&NodeId, &str> = members
            .iter()
            .map(|n| (&n.id, n.address.as_str()))
            .collect();
        let primary = &placement.owners[0];
        let address = by_id.get(primary).map(|s| s.to_string());
        Ok(Some(ResolvedPlacement {
            primary_address: address,
            owners: placement.owners,
            epoch: placement.epoch,
            addresses: members
                .iter()
                .map(|n| (n.id.clone(), n.address.clone()))
                .collect(),
        }))
    }

    /// Send one control message and read exactly one control reply.
    ///
    /// Uses a pooled connection when warm; a reused connection that fails with
    /// an I/O error (the peer may have closed it while idle) is retried once on
    /// a fresh dial, so a stale pooled socket never turns a healthy coordinator
    /// into a spurious failure. A non-I/O failure (a codec/protocol refusal)
    /// propagates immediately rather than re-asking the same coordinator. The
    /// connection is returned to the pool only after a fully successful
    /// exchange.
    async fn round_trip(
        &self,
        msg: ControlMessage,
        expected: &'static str,
    ) -> Result<ControlMessage, CoordinatorError> {
        let out = talon_transport::encode(0, &msg)?;
        match self.exchange(&out, expected).await {
            Ok(reply) => Ok(reply),
            Err((true, CoordinatorError::Io(_))) => {
                let mut stream = self.pool.fresh(&self.addr).await?;
                let reply = self
                    .pool
                    .with_request_deadline("coordinator round_trip retry", async {
                        stream.write_all(&out).await?;
                        stream.flush().await?;
                        read_control_frame(&mut stream, expected).await
                    })
                    .await?;
                self.pool.release(&self.addr, stream);
                Ok(reply)
            }
            Err((_, err)) => Err(err),
        }
    }

    /// One request/response over a pooled-or-fresh connection; returns
    /// `(was_reused, err)` on failure so the caller can retry a stale pooled one.
    async fn exchange(
        &self,
        out: &[u8],
        expected: &'static str,
    ) -> Result<ControlMessage, (bool, CoordinatorError)> {
        let (mut stream, reused) = self
            .pool
            .checkout(&self.addr)
            .await
            .map_err(|e| (false, CoordinatorError::from(e)))?;
        let result: Result<ControlMessage, CoordinatorError> = self
            .pool
            .with_request_deadline("coordinator round_trip", async {
                stream.write_all(out).await?;
                stream.flush().await?;
                read_control_frame(&mut stream, expected).await
            })
            .await;
        match result {
            Ok(reply) => {
                self.pool.release(&self.addr, stream);
                Ok(reply)
            }
            Err(err) => Err((reused, err)),
        }
    }
}

/// A placement resolved to worker addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPlacement {
    /// Address of the primary (first) owner, if it appears in membership.
    pub primary_address: Option<String>,
    /// Ordered owner ids (primary first).
    pub owners: Vec<NodeId>,
    /// Epoch the placement was computed at.
    pub epoch: u64,
    /// Full id→address map from the membership snapshot, for replica fallback.
    pub addresses: HashMap<NodeId, String>,
}

impl ResolvedPlacement {
    /// Resolve an owner id to its worker address, if known.
    pub fn address_of(&self, id: &NodeId) -> Option<&str> {
        self.addresses.get(id).map(String::as_str)
    }
}

/// Read one framed [`ControlMessage`] from `stream`.
///
/// Reads the 16-byte header, validates the payload against
/// [`MAX_CONTROL_PAYLOAD_LEN`] before allocation, then decodes the full frame
/// with the control codec. `expected` names the request for error context only.
async fn read_control_frame(
    stream: &mut TcpStream,
    expected: &'static str,
) -> Result<ControlMessage, CoordinatorError> {
    let mut header_buf = [0u8; HEADER_LEN];
    stream.read_exact(&mut header_buf).await?;
    let header = FrameHeader::decode(&header_buf)
        .map_err(|e| CoordinatorError::Codec(talon_transport::CodecError::Frame(e)))?;
    if header.msg_type != MsgType::Control {
        // Surface as a codec error to keep one error channel for framing.
        return Err(CoordinatorError::Codec(
            talon_transport::CodecError::NotControl(header.msg_type),
        ));
    }
    if header.length > MAX_CONTROL_PAYLOAD_LEN {
        return Err(CoordinatorError::PayloadTooLarge {
            length: header.length,
            cap: MAX_CONTROL_PAYLOAD_LEN,
        });
    }
    let mut payload = vec![0u8; header.length as usize];
    stream.read_exact(&mut payload).await?;
    // Reassemble header || payload for the codec's decode.
    let mut full = header_buf.to_vec();
    full.extend_from_slice(&payload);
    let (_hdr, msg) = talon_transport::decode(&full)?;
    // `expected` retained for future richer diagnostics.
    let _ = expected;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use talon_core::{Backend, NodeRole, ObjectId, Version};
    use talon_transport::frame::HEADER_LEN;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    fn block() -> BlockId {
        BlockId::new(
            ObjectId::new(Backend::S3, "b", "o/1"),
            0,
            256 << 20,
            Version::new("v1"),
        )
    }

    fn worker(id: &str, addr: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            address: addr.to_string(),
            role: NodeRole::Worker,
        }
    }

    /// Spawn a one-shot mock coordinator that reads a single control frame and
    /// replies with `reply`. Returns the bound address.
    async fn mock_coordinator(reply: ControlMessage) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Read one request frame (header + payload) and discard it.
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let out = talon_transport::encode(header.request_id, &reply).unwrap();
            sock.write_all(&out).await.unwrap();
            sock.flush().await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn placement_lookup_parses_response() {
        let addr = mock_coordinator(ControlMessage::PlacementResponse {
            owners: vec![NodeId::new("w1"), NodeId::new("w2")],
            epoch: 42,
        })
        .await;
        let client = CoordinatorClient::new(addr);
        let p = client.placement_lookup(&block(), 2).await.unwrap();
        assert_eq!(p.owners, vec![NodeId::new("w1"), NodeId::new("w2")]);
        assert_eq!(p.epoch, 42);
    }

    #[tokio::test]
    async fn membership_parses_nodes() {
        let addr = mock_coordinator(ControlMessage::MembershipList {
            nodes: vec![worker("w1", "10.0.0.1:7001"), worker("w2", "10.0.0.2:7001")],
        })
        .await;
        let client = CoordinatorClient::new(addr);
        let nodes = client.membership().await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].address, "10.0.0.1:7001");
    }

    #[tokio::test]
    async fn stat_object_parses_response() {
        let addr = mock_coordinator(ControlMessage::ObjectStat {
            size: 2_500_000_000,
            version: "etag-9".into(),
        })
        .await;
        let client = CoordinatorClient::new(addr);
        let stat = client
            .stat_object(&ObjectId::new(Backend::S3, "b", "o/1"))
            .await
            .unwrap();
        assert_eq!(stat.size, 2_500_000_000);
        assert_eq!(stat.version, "etag-9");
    }

    #[tokio::test]
    async fn list_objects_parses_entries() {
        let addr = mock_coordinator(ControlMessage::ObjectList {
            entries: vec![
                talon_transport::ObjectEntry {
                    path: "s3/b/dir/a.bin".into(),
                    size: 10,
                },
                talon_transport::ObjectEntry {
                    path: "s3/b/dir/c.bin".into(),
                    size: 30,
                },
            ],
        })
        .await;
        let client = CoordinatorClient::new(addr);
        let entries = client.list_objects("s3/b/dir").await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "s3/b/dir/a.bin");
        assert_eq!(entries[1].size, 30);
    }

    #[tokio::test]
    async fn unexpected_reply_is_error() {
        let addr = mock_coordinator(ControlMessage::Ack {
            ok: false,
            detail: Some("nope".into()),
        })
        .await;
        let client = CoordinatorClient::new(addr);
        let err = client.placement_lookup(&block(), 1).await.unwrap_err();
        assert!(matches!(err, CoordinatorError::Unexpected { .. }));
    }

    #[tokio::test]
    async fn connect_failure_is_io_error() {
        // Nothing listening on this port.
        let client = CoordinatorClient::new("127.0.0.1:1");
        let err = client.membership().await.unwrap_err();
        assert!(matches!(err, CoordinatorError::Io(_)));
    }

    #[tokio::test]
    async fn oversized_control_payload_is_rejected_before_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let request = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; request.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let reply = FrameHeader::new(
                MsgType::Control,
                request.request_id,
                MAX_CONTROL_PAYLOAD_LEN + 1,
            );
            sock.write_all(&reply.encode()).await.unwrap();
            sock.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            drop(sock);
        });
        let pool = Arc::new(ConnectionPool::new().with_timeouts(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(150),
        ));
        let client = CoordinatorClient::with_pool(addr, pool);

        let err = client.membership().await.unwrap_err();
        assert!(matches!(
            err,
            CoordinatorError::PayloadTooLarge {
                length,
                cap: MAX_CONTROL_PAYLOAD_LEN
            } if length == MAX_CONTROL_PAYLOAD_LEN + 1
        ));
    }

    #[tokio::test]
    async fn locate_primary_resolves_address() {
        // Two-step: this mock answers *both* the placement lookup and the
        // membership query on successive connections.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            // First connection: placement.
            let (mut s1, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            s1.read_exact(&mut hdr).await.unwrap();
            let h = FrameHeader::decode(&hdr).unwrap();
            let mut b = vec![0u8; h.length as usize];
            s1.read_exact(&mut b).await.unwrap();
            let reply = ControlMessage::PlacementResponse {
                owners: vec![NodeId::new("w2"), NodeId::new("w1")],
                epoch: 7,
            };
            s1.write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            s1.flush().await.unwrap();
            drop(s1);
            // Second connection: membership.
            let (mut s2, _) = listener.accept().await.unwrap();
            s2.read_exact(&mut hdr).await.unwrap();
            let h = FrameHeader::decode(&hdr).unwrap();
            let mut b = vec![0u8; h.length as usize];
            s2.read_exact(&mut b).await.unwrap();
            let reply = ControlMessage::MembershipList {
                nodes: vec![worker("w1", "10.0.0.1:7001"), worker("w2", "10.0.0.2:7001")],
            };
            s2.write_all(&talon_transport::encode(0, &reply).unwrap())
                .await
                .unwrap();
            s2.flush().await.unwrap();
        });
        let client = CoordinatorClient::new(addr);
        let resolved = client.locate_primary(&block(), 2).await.unwrap().unwrap();
        // Primary is w2 → its address.
        assert_eq!(resolved.primary_address.as_deref(), Some("10.0.0.2:7001"));
        assert_eq!(resolved.epoch, 7);
        assert_eq!(
            resolved.address_of(&NodeId::new("w1")),
            Some("10.0.0.1:7001")
        );
    }

    /// A codec-level refusal (e.g. a reply that isn't even a `Control` frame)
    /// on a reused connection must propagate immediately, not re-dial the
    /// same coordinator and re-ask.
    #[tokio::test]
    async fn not_control_reply_is_not_retried_on_the_coordinator() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let accepts = Arc::new(AtomicU32::new(0));
        let requests = Arc::new(AtomicU32::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepts_srv = Arc::clone(&accepts);
        let requests_srv = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                accepts_srv.fetch_add(1, Ordering::SeqCst);
                let requests = Arc::clone(&requests_srv);
                tokio::spawn(async move {
                    loop {
                        let mut hdr = [0u8; HEADER_LEN];
                        if sock.read_exact(&mut hdr).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&hdr).unwrap();
                        let mut body = vec![0u8; header.length as usize];
                        sock.read_exact(&mut body).await.unwrap();
                        let n = requests.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            let reply = ControlMessage::MembershipList {
                                nodes: vec![worker("w1", "10.0.0.1:7001")],
                            };
                            sock.write_all(
                                &talon_transport::encode(header.request_id, &reply).unwrap(),
                            )
                            .await
                            .unwrap();
                        } else {
                            let bad =
                                FrameHeader::new(MsgType::GetRange, header.request_id, 0).encode();
                            sock.write_all(&bad).await.unwrap();
                        }
                        sock.flush().await.unwrap();
                    }
                });
            }
        });
        let client = CoordinatorClient::new(addr);

        let members = client.membership().await.unwrap();
        assert_eq!(members.len(), 1);

        let err = client.membership().await.unwrap_err();
        assert!(matches!(
            err,
            CoordinatorError::Codec(talon_transport::CodecError::NotControl(MsgType::GetRange))
        ));

        assert_eq!(
            accepts.load(Ordering::SeqCst),
            1,
            "a codec-level refusal must not re-dial the same coordinator"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "the refused round trip must not be doubled"
        );
    }

    /// A coordinator that accepts and then goes silent used to hang the caller
    /// forever; on the read path that is an unkillable mount.
    #[tokio::test]
    async fn round_trip_times_out_against_a_stalling_coordinator() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let held: std::sync::Arc<std::sync::Mutex<Vec<tokio::net::TcpStream>>> =
            std::sync::Arc::new(std::sync::Mutex::new(vec![]));
        let keep = std::sync::Arc::clone(&held);
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                keep.lock().unwrap().push(sock);
            }
        });

        let pool = std::sync::Arc::new(crate::ConnectionPool::new().with_timeouts(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(150),
        ));
        let client = CoordinatorClient::with_pool(addr, pool);

        let started = std::time::Instant::now();
        let err = client
            .membership()
            .await
            .expect_err("a stalling coordinator must not hang the caller");
        match err {
            CoordinatorError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
            other => panic!("expected a TimedOut I/O error, got {other:?}"),
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "took {:?}",
            started.elapsed()
        );
    }
}

#[cfg(test)]
mod zoned_membership_tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use talon_core::NodeRole;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    fn worker(id: &str, addr: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId::new(id),
            address: addr.to_string(),
            role: NodeRole::Worker,
        }
    }

    /// A schema-v5 coordinator answers the zoned query directly.
    #[tokio::test]
    async fn zoned_membership_parses_zones() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut hdr = [0u8; HEADER_LEN];
            sock.read_exact(&mut hdr).await.unwrap();
            let header = FrameHeader::decode(&hdr).unwrap();
            let mut body = vec![0u8; header.length as usize];
            sock.read_exact(&mut body).await.unwrap();
            let reply = ControlMessage::MembershipListV2 {
                nodes: vec![ZonedNodeInfo {
                    info: worker("w1", "10.0.0.1:7001"),
                    zone: Some("az-a".into()),
                }],
            };
            sock.write_all(&talon_transport::encode(header.request_id, &reply).unwrap())
                .await
                .unwrap();
            sock.flush().await.unwrap();
        });
        let client = CoordinatorClient::new(addr);
        let members = client.membership_zoned(1_000).await.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].zone.as_deref(), Some("az-a"));
    }

    /// A pre-v5 coordinator cannot decode the zoned query and drops the
    /// connection; the client must fall back to the v1 query (zones unknown)
    /// and hold a cooldown so the next refresh goes straight to v1.
    #[tokio::test]
    async fn old_coordinator_triggers_v1_fallback_with_cooldown() {
        let v2_queries = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&v2_queries);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    loop {
                        let mut hdr = [0u8; HEADER_LEN];
                        if sock.read_exact(&mut hdr).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&hdr).unwrap();
                        let mut body = vec![0u8; header.length as usize];
                        sock.read_exact(&mut body).await.unwrap();
                        let mut full = hdr.to_vec();
                        full.extend_from_slice(&body);
                        // An old build rejects the unknown schema at decode and
                        // drops the connection; emulate by matching the message.
                        match talon_transport::decode(&full).unwrap().1 {
                            ControlMessage::MembershipQueryV2 {} => {
                                seen.fetch_add(1, Ordering::SeqCst);
                                return; // connection dropped, no reply
                            }
                            ControlMessage::MembershipQuery {} => {
                                let reply = ControlMessage::MembershipList {
                                    nodes: vec![worker("w1", "10.0.0.1:7001")],
                                };
                                sock.write_all(
                                    &talon_transport::encode(header.request_id, &reply).unwrap(),
                                )
                                .await
                                .unwrap();
                                sock.flush().await.unwrap();
                            }
                            other => panic!("unexpected request: {other:?}"),
                        }
                    }
                });
            }
        });

        let client = CoordinatorClient::new(addr);
        let members = client.membership_zoned(1_000).await.unwrap();
        assert_eq!(members.len(), 1);
        assert!(members[0].zone.is_none());
        assert_eq!(v2_queries.load(Ordering::SeqCst), 1);

        // Within the cooldown the client goes straight to v1: no new v2 probe.
        let again = client.membership_zoned(30_000).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(v2_queries.load(Ordering::SeqCst), 1);
    }

    /// Once the cooldown elapses the client probes v5 again, so a coordinator
    /// upgrade is picked up without restarting readers.
    #[tokio::test]
    async fn cooldown_expiry_reprobes_v2_and_recovers_zones() {
        let v2_queries = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&v2_queries);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    loop {
                        let mut hdr = [0u8; HEADER_LEN];
                        if sock.read_exact(&mut hdr).await.is_err() {
                            return;
                        }
                        let header = FrameHeader::decode(&hdr).unwrap();
                        let mut body = vec![0u8; header.length as usize];
                        sock.read_exact(&mut body).await.unwrap();
                        let mut full = hdr.to_vec();
                        full.extend_from_slice(&body);
                        match talon_transport::decode(&full).unwrap().1 {
                            // First probe: the coordinator still predates v5
                            // and drops the connection. After the upgrade
                            // (every later probe) it answers with zones.
                            ControlMessage::MembershipQueryV2 {} => {
                                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                                    return;
                                }
                                let reply = ControlMessage::MembershipListV2 {
                                    nodes: vec![ZonedNodeInfo {
                                        info: worker("w1", "10.0.0.1:7001"),
                                        zone: Some("az-a".into()),
                                    }],
                                };
                                sock.write_all(
                                    &talon_transport::encode(header.request_id, &reply).unwrap(),
                                )
                                .await
                                .unwrap();
                                sock.flush().await.unwrap();
                            }
                            ControlMessage::MembershipQuery {} => {
                                let reply = ControlMessage::MembershipList {
                                    nodes: vec![worker("w1", "10.0.0.1:7001")],
                                };
                                sock.write_all(
                                    &talon_transport::encode(header.request_id, &reply).unwrap(),
                                )
                                .await
                                .unwrap();
                                sock.flush().await.unwrap();
                            }
                            other => panic!("unexpected request: {other:?}"),
                        }
                    }
                });
            }
        });

        let client = CoordinatorClient::new(addr);
        // Probe fails at t=1s: fallback to v1, cooldown starts.
        let members = client.membership_zoned(1_000).await.unwrap();
        assert!(members[0].zone.is_none());
        assert_eq!(v2_queries.load(Ordering::SeqCst), 1);

        // Cooldown elapsed: the next refresh probes v5 again and gets zones.
        let after = 1_000 + MEMBERSHIP_V2_RETRY_COOLDOWN_MS + 1;
        let members = client.membership_zoned(after).await.unwrap();
        assert_eq!(members[0].zone.as_deref(), Some("az-a"));
        assert_eq!(v2_queries.load(Ordering::SeqCst), 2);
    }
}
