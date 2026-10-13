//! Placement policy used by the shared stale-block scan.
use super::*;
use talon_core::{CachePlacementTable, NodeId, NodeRole, WorkerConfig};
use talon_transport::ZonedNodeInfo;

pub(super) struct Ownership {
    worker: NodeId,
    pub(super) valid_until: Instant,
    members: Vec<ZonedNodeInfo>,
    global: Option<CachePlacementTable>,
    local: Option<CachePlacementTable>,
    replicas: usize,
    coordinator: String,
    channel: Option<talon_transport::control_tls::ControlTlsChannel>,
}

impl Ownership {
    pub(super) fn new(
        worker: NodeId,
        config: &WorkerConfig,
        channel: Option<talon_transport::control_tls::ControlTlsChannel>,
    ) -> Self {
        Self {
            worker,
            valid_until: Instant::now(),
            members: Vec::new(),
            global: None,
            local: None,
            replicas: config.ownership_gc_replicas,
            coordinator: config.coordinator.clone(),
            channel,
        }
    }

    /// Refresh before every batch. False means placement changed and an active
    /// round must stop ownership reclamation until its next scan.
    pub(super) async fn refresh(&mut self) -> anyhow::Result<bool> {
        let started = Instant::now();
        let (nodes, valid_for) = tokio::time::timeout(Duration::from_secs(3), async {
            if let Some(channel) = &self.channel {
                ownership_membership(channel.connect(&self.coordinator).await?.stream).await
            } else {
                ownership_membership(tokio::net::TcpStream::connect(&self.coordinator).await?).await
            }
        })
        .await??;
        self.valid_until = started + valid_for.min(Duration::from_secs(1));
        anyhow::ensure!(
            Instant::now() < self.valid_until,
            "expired ownership observation"
        );
        self.observe(nodes)
    }

    fn observe(&mut self, mut members: Vec<ZonedNodeInfo>) -> anyhow::Result<bool> {
        members.retain(|n| n.info.role == NodeRole::Worker);
        members.sort_unstable_by(|a, b| a.info.id.0.cmp(&b.info.id.0));
        anyhow::ensure!(
            !members.windows(2).any(|w| w[0].info.id == w[1].info.id)
                && members.iter().any(|n| n.info.id == self.worker),
            "invalid ownership membership"
        );
        if self.global.is_some() && self.members == members {
            return Ok(true);
        }
        self.members = members;
        self.local = None;
        let nodes: Vec<_> = self.members.iter().map(|n| n.info.clone()).collect();
        if let Some(zone) = self
            .members
            .iter()
            .find(|n| n.info.id == self.worker)
            .and_then(|n| n.zone.as_ref())
        {
            let local: Vec<_> = self
                .members
                .iter()
                .filter(|n| n.zone.as_ref() == Some(zone))
                .map(|n| n.info.clone())
                .collect();
            if local.len() != nodes.len() {
                self.local = Some(CachePlacementTable::new(&local));
            }
        }
        self.global = Some(CachePlacementTable::new(&nodes));
        Ok(false)
    }

    pub(super) fn owns(&self, block: &BlockId) -> bool {
        self.global.iter().chain(self.local.iter()).any(|table| {
            if self.replicas == 1 {
                table.primary(block).is_some_and(|n| n.id == self.worker)
            } else {
                table
                    .rank(block, self.replicas)
                    .iter()
                    .any(|n| n.id == self.worker)
            }
        })
    }
}

async fn ownership_membership<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut stream: S,
) -> anyhow::Result<(Vec<ZonedNodeInfo>, Duration)> {
    use talon_transport::{codec, ControlMessage};
    use tokio::io::AsyncWriteExt;
    stream
        .write_all(&codec::encode(0, &ControlMessage::MembershipQuery {})?)
        .await?;
    stream.flush().await?;
    match crate::tokio_conn::read_control(&mut stream).await? {
        Some(ControlMessage::MembershipList { view }) => {
            anyhow::ensure!(view.valid_for_ms > 0, "expired ownership membership");
            let registry = talon_core::worker_membership::MemberRegistry {
                members: view.workers.into_iter().map(|w| w.member).collect(),
                ..Default::default()
            };
            registry.validate().map_err(anyhow::Error::msg)?;
            // Placement uses retained logical members, including offline/conflicted
            // instances. Addresses and process liveness must not move ownership.
            let nodes = registry
                .members
                .into_iter()
                .filter(|m| !m.retired)
                .map(|m| ZonedNodeInfo {
                    info: talon_core::NodeInfo {
                        id: NodeId::new(m.worker_id),
                        address: String::new(),
                        role: NodeRole::Worker,
                    },
                    zone: m.zone,
                })
                .collect();
            Ok((nodes, Duration::from_millis(view.valid_for_ms)))
        }
        other => anyhow::bail!("ownership GC requires retained membership: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokio_conn::read_control;
    use talon_core::NodeInfo;
    use talon_transport::{codec, ControlMessage};
    use tokio::io::AsyncWriteExt;

    fn members() -> Vec<ZonedNodeInfo> {
        ["a", "b", "c"]
            .into_iter()
            .map(|id| ZonedNodeInfo {
                info: NodeInfo {
                    id: NodeId::new(id),
                    address: String::new(),
                    role: NodeRole::Worker,
                },
                zone: None,
            })
            .collect()
    }
    fn discovery(nodes: &[ZonedNodeInfo]) -> talon_core::worker_membership::WorkerDiscovery {
        use talon_core::worker_membership::*;
        WorkerDiscovery::retained(
            &MemberRegistry {
                members: nodes
                    .iter()
                    .map(|n| WorkerMember {
                        worker_id: n.info.id.0.clone(),
                        zone: n.zone.clone(),
                        retired: false,
                    })
                    .collect(),
                ..Default::default()
            },
            &[],
        )
    }
    #[tokio::test]
    async fn ownership_membership_rejects_errors_and_unexpected_replies() {
        for reply in [
            ControlMessage::Ack {
                ok: false,
                detail: Some("not ready".into()),
            },
            ControlMessage::MembershipQuery {},
        ] {
            let (client, mut server) = tokio::io::duplex(4096);
            let responder = tokio::spawn(async move {
                assert_eq!(
                    read_control(&mut server).await.unwrap(),
                    Some(ControlMessage::MembershipQuery {})
                );
                server
                    .write_all(&codec::encode(0, &reply).unwrap())
                    .await
                    .unwrap();
            });
            assert!(ownership_membership(client).await.is_err());
            responder.await.unwrap();
        }
    }

    #[tokio::test]
    async fn membership_uses_logical_ids_and_zones_across_instance_changes() {
        use talon_core::worker_membership::*;
        let mut view = discovery(&members());
        let mut snapshots = Vec::new();
        for state in [
            InstanceState::Offline,
            InstanceState::Conflict,
            InstanceState::Serving {
                instance_id: "new-process".into(),
                address: "new-address:7001".into(),
            },
        ] {
            view.workers[0].state = state;
            let (client, mut server) = tokio::io::duplex(4096);
            let reply = ControlMessage::MembershipList { view: view.clone() };
            let responder = tokio::spawn(async move {
                read_control(&mut server).await.unwrap().unwrap();
                server
                    .write_all(&codec::encode(0, &reply).unwrap())
                    .await
                    .unwrap();
            });
            snapshots.push(ownership_membership(client).await.unwrap());
            responder.await.unwrap();
        }
        assert_eq!(
            snapshots[0].0.len(),
            3,
            "offline members still own placement"
        );
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[1], snapshots[2]);
    }

    #[test]
    fn validates_membership_and_retains_global_and_zone_candidates() {
        let mut policy = Ownership::new(NodeId::new("a"), &WorkerConfig::default(), None);
        assert!(!policy.observe(members()).unwrap());
        let mut reordered = members();
        reordered.reverse();
        assert!(policy.observe(reordered).unwrap());
        let mut duplicate = members();
        duplicate.push(duplicate[0].clone());
        for invalid in [Vec::new(), members()[1..].to_vec(), duplicate] {
            assert!(policy.observe(invalid).is_err());
        }
        let table =
            CachePlacementTable::new(&members().into_iter().map(|n| n.info).collect::<Vec<_>>());
        let id = (0..10000)
            .map(|n| {
                BlockId::new(
                    ObjectId::new(Backend::S3, "b", n.to_string()),
                    0,
                    256,
                    Version::new("v1"),
                )
            })
            .find(|id| table.primary(id).unwrap().id.0 == "b")
            .unwrap();
        assert!(!policy.owns(&id));
        policy.replicas = 3;
        assert!(policy.owns(&id));
        policy.replicas = 1;
        let mut zoned = members();
        zoned[0].zone = Some("zone-a".into());
        assert!(!policy.observe(zoned).unwrap());
        assert!(policy.owns(&id));
    }
}
