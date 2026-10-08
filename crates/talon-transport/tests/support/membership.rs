use talon_core::{
    worker_membership::{DiscoveredWorker, InstanceState, WorkerDiscovery, WorkerMember},
    NodeInfo,
};
use talon_transport::{ControlMessage, ZonedNodeInfo};

// This shared fixture is also included by suites that only use zoned members.
#[allow(dead_code)]
pub fn plain(nodes: Vec<NodeInfo>) -> ControlMessage {
    zoned(
        nodes
            .into_iter()
            .map(|info| ZonedNodeInfo { info, zone: None })
            .collect(),
    )
}

pub fn zoned(nodes: Vec<ZonedNodeInfo>) -> ControlMessage {
    ControlMessage::MembershipList {
        view: WorkerDiscovery {
            topology_token: 1,
            state_token: 1,
            valid_for_ms: 500,
            workers: nodes
                .into_iter()
                .map(|node| DiscoveredWorker {
                    member: WorkerMember {
                        worker_id: node.info.id.0,
                        zone: node.zone,
                        retired: false,
                    },
                    state: InstanceState::Serving {
                        instance_id: "test-instance".into(),
                        address: node.info.address,
                    },
                })
                .collect(),
        },
    }
}
