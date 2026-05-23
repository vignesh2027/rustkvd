use bytes::Bytes;
use common::KVError;
use tonic::transport::Channel;

pub mod proto {
    tonic::include_proto!("kvstore");
}

use proto::kv_store_client::KvStoreClient;
use proto::*;

pub struct KVClient {
    inner: KvStoreClient<Channel>,
    peers: Vec<String>,
}

impl KVClient {
    pub async fn connect(peers: Vec<String>) -> Result<Self, KVError> {
        let addr = peers.first().ok_or_else(|| KVError::InvalidArgument("no peers".into()))?;
        let channel = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| KVError::InvalidArgument(e.to_string()))?
            .connect()
            .await
            .map_err(|e| KVError::NetworkError(tonic::Status::unavailable(e.to_string())))?;
        Ok(KVClient { inner: KvStoreClient::new(channel), peers })
    }

    pub async fn get(&mut self, key: &str, version: Option<u64>) -> Result<Option<Bytes>, KVError> {
        let resp = self.inner.get(GetRequest {
            key: key.to_string(),
            version: version.unwrap_or(0),
        }).await?.into_inner();
        if resp.found {
            Ok(Some(Bytes::from(resp.value)))
        } else {
            Ok(None)
        }
    }

    pub async fn put(&mut self, key: &str, value: Bytes, ttl_secs: Option<u64>) -> Result<u64, KVError> {
        let resp = self.inner.put(PutRequest {
            key: key.to_string(),
            value: value.to_vec(),
            ttl_secs: ttl_secs.unwrap_or(0),
        }).await?.into_inner();
        Ok(resp.version)
    }

    pub async fn delete(&mut self, key: &str) -> Result<bool, KVError> {
        let resp = self.inner.delete(DeleteRequest {
            key: key.to_string(),
        }).await?.into_inner();
        Ok(resp.success)
    }

    pub async fn scan(&mut self, start: &str, end: &str, limit: u32) -> Result<Vec<(String, Bytes)>, KVError> {
        let resp = self.inner.scan(ScanRequest {
            start_key: start.to_string(),
            end_key: end.to_string(),
            limit,
        }).await?.into_inner();
        Ok(resp.pairs.into_iter().map(|kv| (kv.key, Bytes::from(kv.value))).collect())
    }

    pub async fn status(&mut self) -> Result<proto::StatusResponse, KVError> {
        Ok(self.inner.node_status(proto::StatusRequest {}).await?.into_inner())
    }
}
