use crate::config::NodeConfig;
use bytes::Bytes;
use cluster::{HashRing, MembershipManager, Router};
use common::{KVError, NodeId};
use parking_lot::RwLock;
use raft::RaftNode;
use std::sync::Arc;
use std::time::Duration;
use storage::StorageEngine;
use tokio::sync::broadcast;

pub enum KVOp {
    Get { version: Option<u64> },
    Put,
    Delete,
}

pub enum KVResult {
    Get { value: Option<Bytes>, version: u64, found: bool },
    Put { version: u64 },
    Delete { success: bool },
}

pub struct NodeRunner {
    pub config: NodeConfig,
    pub storage: Arc<StorageEngine>,
    pub raft: Arc<RwLock<RaftNode>>,
    pub router: Arc<Router>,
    pub membership: Arc<MembershipManager>,
    pub watch_tx: broadcast::Sender<(String, Bytes, String)>,
}

impl NodeRunner {
    pub fn new(config: NodeConfig) -> Result<Self, KVError> {
        let storage = StorageEngine::open(config.data_dir.clone())
            .map_err(KVError::StorageError)?;

        let peer_ids: Vec<NodeId> = config.peers.iter().map(|p| p.node_id.clone()).collect();
        let raft = RaftNode::new(config.node_id.clone(), peer_ids);

        let membership = Arc::new(MembershipManager::new(Duration::from_secs(5)));
        membership.add_node(config.node_id.clone(), config.grpc_addr.clone());
        for peer in &config.peers {
            membership.add_node(peer.node_id.clone(), peer.addr.clone());
        }

        let mut ring = HashRing::new(config.replication_factor);
        ring.add_node(config.node_id.clone());
        for peer in &config.peers {
            ring.add_node(peer.node_id.clone());
        }
        let ring = Arc::new(RwLock::new(ring));

        let router = Arc::new(Router::new(ring, membership.clone(), config.node_id.clone()));

        let (watch_tx, _) = broadcast::channel(1024);

        Ok(NodeRunner {
            config,
            storage: Arc::new(storage),
            raft: Arc::new(RwLock::new(raft)),
            router,
            membership,
            watch_tx,
        })
    }

    pub fn get(&self, key: &str, version: Option<u64>) -> Result<KVResult, KVError> {
        match self.storage.get(key, version)? {
            Some(v) if !v.deleted => Ok(KVResult::Get {
                value: Some(Bytes::from(v.value)),
                version: v.version,
                found: true,
            }),
            Some(_) => Ok(KVResult::Get { value: None, version: 0, found: false }),
            None => Ok(KVResult::Get { value: None, version: 0, found: false }),
        }
    }

    pub fn put(&self, key: String, value: Bytes, ttl_secs: Option<u64>) -> Result<KVResult, KVError> {
        let version = self.storage.put(key.clone(), value.clone(), ttl_secs)?;
        let _ = self.watch_tx.send((key, value, "PUT".to_string()));
        Ok(KVResult::Put { version })
    }

    pub fn delete(&self, key: String) -> Result<KVResult, KVError> {
        let success = self.storage.delete(key.clone())?;
        if success {
            let _ = self.watch_tx.send((key, Bytes::new(), "DELETE".to_string()));
        }
        Ok(KVResult::Delete { success })
    }

    pub fn is_leader(&self) -> bool {
        self.raft.read().is_leader()
    }

    pub fn leader_hint(&self) -> Option<String> {
        let raft = self.raft.read();
        raft.leader_id.as_ref().and_then(|id| self.membership.get_addr(id))
    }
}
