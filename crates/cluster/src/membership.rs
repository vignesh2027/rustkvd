use common::NodeId;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Alive,
    Suspected,
    Dead,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: String,
    pub state: NodeState,
    pub last_seen: Instant,
}

pub struct MembershipManager {
    nodes: Arc<DashMap<NodeId, NodeInfo>>,
    failure_timeout: Duration,
}

impl MembershipManager {
    pub fn new(failure_timeout: Duration) -> Self {
        MembershipManager {
            nodes: Arc::new(DashMap::new()),
            failure_timeout,
        }
    }

    pub fn add_node(&self, id: NodeId, addr: String) {
        self.nodes.insert(id.clone(), NodeInfo {
            id,
            addr,
            state: NodeState::Alive,
            last_seen: Instant::now(),
        });
    }

    pub fn remove_node(&self, id: &NodeId) {
        self.nodes.remove(id);
    }

    pub fn heartbeat(&self, id: &NodeId) {
        if let Some(mut node) = self.nodes.get_mut(id) {
            node.last_seen = Instant::now();
            node.state = NodeState::Alive;
        }
    }

    pub fn check_failures(&self) {
        let timeout = self.failure_timeout;
        for mut entry in self.nodes.iter_mut() {
            if entry.last_seen.elapsed() > timeout {
                entry.state = NodeState::Dead;
                tracing::warn!(node_id = %entry.id, "Node suspected dead");
            }
        }
    }

    pub fn alive_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|e| e.state == NodeState::Alive)
            .map(|e| e.clone())
            .collect()
    }

    pub fn get_addr(&self, id: &NodeId) -> Option<String> {
        self.nodes.get(id).map(|n| n.addr.clone())
    }

    pub fn all_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.iter().map(|e| e.clone()).collect()
    }
}
