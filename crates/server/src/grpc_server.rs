use crate::node_runner::{KVResult, NodeRunner};
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

pub mod proto {
    tonic::include_proto!("kvstore");
}

use proto::kv_store_server::{KvStore, KvStoreServer};
use proto::*;
use tonic::{Request, Response, Status};

pub struct KVStoreService {
    node: Arc<NodeRunner>,
}

impl KVStoreService {
    pub fn new(node: Arc<NodeRunner>) -> Self {
        KVStoreService { node }
    }
}

#[tonic::async_trait]
impl KvStore for KVStoreService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let version = if req.version == 0 { None } else { Some(req.version) };
        match self.node.get(&req.key, version) {
            Ok(KVResult::Get { value, version, found }) => {
                Ok(Response::new(GetResponse {
                    value: value.map(|v| v.to_vec()).unwrap_or_default(),
                    version,
                    found,
                }))
            }
            Err(e) => Err(Status::from(e)),
            _ => Err(Status::internal("unexpected result")),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        if !self.node.is_leader() {
            let hint = self.node.leader_hint().unwrap_or_default();
            return Err(Status::failed_precondition(format!("not_leader:{}", hint)));
        }
        let ttl = if req.ttl_secs == 0 { None } else { Some(req.ttl_secs) };
        match self.node.put(req.key, bytes::Bytes::from(req.value), ttl) {
            Ok(KVResult::Put { version }) => {
                Ok(Response::new(PutResponse {
                    version,
                    success: true,
                    leader_hint: String::new(),
                }))
            }
            Err(e) => Err(Status::from(e)),
            _ => Err(Status::internal("unexpected result")),
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        if !self.node.is_leader() {
            let hint = self.node.leader_hint().unwrap_or_default();
            return Err(Status::failed_precondition(format!("not_leader:{}", hint)));
        }
        match self.node.delete(req.key) {
            Ok(KVResult::Delete { success }) => {
                Ok(Response::new(DeleteResponse { success }))
            }
            Err(e) => Err(Status::from(e)),
            _ => Err(Status::internal("unexpected result")),
        }
    }

    type WatchStream = std::pin::Pin<Box<dyn futures::Stream<Item = Result<WatchEvent, Status>> + Send>>;

    async fn watch(&self, request: Request<WatchRequest>) -> Result<Response<Self::WatchStream>, Status> {
        let req = request.into_inner();
        let prefix = req.key_prefix;
        let rx = self.node.watch_tx.subscribe();
        let stream = BroadcastStream::new(rx)
            .filter_map(move |msg| {
                let prefix = prefix.clone();
                match msg {
                    Ok((key, value, event_type)) if key.starts_with(&prefix) => {
                        Some(Ok(WatchEvent {
                            key,
                            value: value.to_vec(),
                            event_type,
                        }))
                    }
                    _ => None,
                }
            });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let pairs = self.node.storage.scan(&req.start_key, &req.end_key, limit);
        Ok(Response::new(ScanResponse {
            pairs: pairs.into_iter().map(|p| KeyValue {
                key: p.key,
                value: p.value.to_vec(),
                version: p.version,
            }).collect(),
        }))
    }

    async fn node_status(&self, _request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        let raft = self.node.raft.read();
        Ok(Response::new(StatusResponse {
            node_id: self.node.config.node_id.0.clone(),
            role: raft.role_str(),
            term: raft.current_term,
            leader_id: raft.leader_id.as_ref().map(|id| id.0.clone()).unwrap_or_default(),
            committed_index: raft.commit_index,
            applied_index: raft.last_applied,
            peers: self.node.config.peers.iter().map(|p| p.addr.clone()).collect(),
        }))
    }
}

pub fn create_server(node: Arc<NodeRunner>) -> KvStoreServer<KVStoreService> {
    KvStoreServer::new(KVStoreService::new(node))
}
