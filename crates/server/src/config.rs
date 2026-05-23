use clap::Parser;
use common::NodeId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub grpc_addr: String,
    pub peers: Vec<PeerConfig>,
    pub data_dir: PathBuf,
    pub replication_factor: usize,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub node_id: NodeId,
    pub addr: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            node_id: NodeId::new("node1"),
            grpc_addr: "0.0.0.0:7001".to_string(),
            peers: Vec::new(),
            data_dir: PathBuf::from("./data"),
            replication_factor: 3,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "rustkvd-server", about = "Distributed KV Store Server")]
pub struct ServerArgs {
    #[arg(long, default_value = "node1")]
    pub node_id: String,

    #[arg(long, default_value = "0.0.0.0:7001")]
    pub addr: String,

    #[arg(long, value_delimiter = ',')]
    pub peers: Vec<String>,

    #[arg(long, default_value = "./data")]
    pub data_dir: String,
}

impl From<ServerArgs> for NodeConfig {
    fn from(args: ServerArgs) -> Self {
        let peers = args.peers.iter().enumerate().map(|(i, addr)| {
            PeerConfig {
                node_id: NodeId::new(format!("peer{}", i)),
                addr: addr.clone(),
            }
        }).collect();
        NodeConfig {
            node_id: NodeId::new(args.node_id),
            grpc_addr: args.addr,
            peers,
            data_dir: PathBuf::from(args.data_dir),
            ..Default::default()
        }
    }
}
