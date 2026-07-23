//! Control-plane client for talking to the coordinator.
//!
//! The FUSE client needs two answers from the coordinator on its read path:
//!
//! - **placement** — given a [`BlockId`], which worker(s) hold it, and at what
//!   [`epoch`](ControlMessage::PlacementResponse)? ([`CoordinatorClient::placement_lookup`])
//! - **membership** — a placement response names owners by [`NodeId`]; the
//!   client resolves those ids to concrete `host:port` worker addresses with a
//!   [`MembershipQuery`](ControlMessage::MembershipQuery). ([`CoordinatorClient::membership`])
//!
//! Both are single request/response round-trips over the control plane: write
//! one [`MsgType::Control`] frame carrying a bincode [`ControlMessage`], read
//! one framed [`ControlMessage`] back. A fresh TCP connection is opened per
//! call for simplicity; a pooled variant can wrap this later without changing
//! the surface. The transport framing/codec is reused verbatim
//! ([`talon_transport::encode`]/[`decode`](talon_transport::decode)), so this
//! module only owns the connect + read-a-frame glue and the response matching.

use std::collections::HashMap;

use talon_core::{BlockId, NodeId, NodeInfo};
use talon_transport::frame::{FrameHeader, MsgType, HEADER_LEN};
use talon_transport::ControlMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Placement answer for a block: ordered owners + the epoch they hold at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// Ordered replica node ids (highest HRW weight first). Empty if no nodes.
    pub owners: Vec<NodeId>,
    /// Placement epoch these owners were computed against.
    pub epoch: u64,
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
#[derive(Debug, Clone)]
pub struct CoordinatorClient {
    addr: String,
}

impl CoordinatorClient {
    /// Create a client that dials `addr` (`host:port`) per request.
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
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

    /// Locate `block` and resolve the primary owner to a worker address.
    ///
    /// Combines [`placement_lookup`](Self::placement_lookup) with a
    /// [`membership`](Self::membership) resolution so a caller gets a directly
    /// dialable `host:port` for the highest-weight owner. Returns `Ok(None)`
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
    async fn round_trip(
        &self,
        msg: ControlMessage,
        expected: &'static str,
    ) -> Result<ControlMessage, CoordinatorError> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        let out = talon_transport::encode(0, &msg)?;
        stream.write_all(&out).await?;
        stream.flush().await?;
        let reply = read_control_frame(&mut stream, expected).await?;
        Ok(reply)
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
/// Reads the 16-byte header, then exactly `length` payload bytes, then decodes
/// the full frame with the control codec. `expected` names the request for
/// error context only.
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
}
