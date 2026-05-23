use crate::hash_ring::HashRing;
use crate::membership::MembershipManager;
use common::NodeId;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Router {
    hash_ring: Arc<RwLock<HashRing>>,
    membership: Arc<MembershipManager>,
    local_id: NodeId,
}

impl Router {
    pub fn new(
        hash_ring: Arc<RwLock<HashRing>>,
        membership: Arc<MembershipManager>,
        local_id: NodeId,
    ) -> Self {
        Router { hash_ring, membership, local_id }
    }

    pub fn nodes_for_key(&self, key: &str, count: usize) -> Vec<NodeId> {
        self.hash_ring.read().get_nodes_for_key(key, count)
    }

    pub fn is_local_responsible(&self, key: &str, replication: usize) -> bool {
        self.nodes_for_key(key, replication).contains(&self.local_id)
    }

    pub fn primary_node(&self, key: &str) -> Option<NodeId> {
        self.nodes_for_key(key, 1).into_iter().next()
    }

    pub fn get_addr(&self, node_id: &NodeId) -> Option<String> {
        self.membership.get_addr(node_id)
    }
}
