use common::{KeyRange, NodeId};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const VIRTUAL_NODES: usize = 150;

pub struct HashRing {
    ring: BTreeMap<u32, NodeId>,
    nodes: Vec<NodeId>,
    replication_factor: usize,
}

impl HashRing {
    pub fn new(replication_factor: usize) -> Self {
        HashRing {
            ring: BTreeMap::new(),
            nodes: Vec::new(),
            replication_factor,
        }
    }

    fn hash_position(node_id: &NodeId, replica: usize) -> u32 {
        let input = format!("{}#{}", node_id.0, replica);
        let digest = Sha256::digest(input.as_bytes());
        u32::from_be_bytes(digest[0..4].try_into().unwrap())
    }

    fn key_position(key: &str) -> u32 {
        let digest = Sha256::digest(key.as_bytes());
        u32::from_be_bytes(digest[0..4].try_into().unwrap())
    }

    pub fn add_node(&mut self, node_id: NodeId) {
        if self.nodes.contains(&node_id) {
            return;
        }
        for i in 0..VIRTUAL_NODES {
            let pos = Self::hash_position(&node_id, i);
            self.ring.insert(pos, node_id.clone());
        }
        self.nodes.push(node_id);
    }

    pub fn remove_node(&mut self, node_id: &NodeId) {
        for i in 0..VIRTUAL_NODES {
            let pos = Self::hash_position(node_id, i);
            self.ring.remove(&pos);
        }
        self.nodes.retain(|n| n != node_id);
    }

    pub fn get_nodes_for_key(&self, key: &str, count: usize) -> Vec<NodeId> {
        if self.ring.is_empty() {
            return Vec::new();
        }
        let pos = Self::key_position(key);
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Clockwise from pos
        let after: Vec<_> = self.ring.range(pos..).chain(self.ring.range(..pos)).collect();
        for (_, node_id) in after {
            if seen.insert(node_id.clone()) {
                result.push(node_id.clone());
                if result.len() >= count {
                    break;
                }
            }
        }
        result
    }

    pub fn get_key_ranges_for_node(&self, node_id: &NodeId) -> Vec<KeyRange> {
        let mut ranges = Vec::new();
        let positions: Vec<u32> = self.ring
            .iter()
            .filter(|(_, n)| *n == node_id)
            .map(|(&pos, _)| pos)
            .collect();

        for pos in positions {
            let pred = self.ring
                .range(..pos)
                .next_back()
                .map(|(&p, _)| p)
                .unwrap_or_else(|| self.ring.keys().next_back().copied().unwrap_or(0));
            ranges.push(KeyRange {
                start: format!("{:010}", pred),
                end: format!("{:010}", pos),
            });
        }
        ranges
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }
}
