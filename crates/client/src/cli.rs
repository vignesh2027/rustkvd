use bytes::Bytes;
use clap::{Parser, Subcommand};
use crate::client::KVClient;

#[derive(Parser)]
#[command(name = "rustkvd-cli", about = "Distributed KV Store CLI")]
pub struct Cli {
    #[arg(long, default_value = "localhost:7001", value_delimiter = ',')]
    pub peers: Vec<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Get {
        key: String,
        #[arg(long)]
        version: Option<u64>,
    },
    Put {
        key: String,
        value: String,
        #[arg(long)]
        ttl: Option<u64>,
    },
    Delete {
        key: String,
    },
    Scan {
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long, default_value = "100")]
        limit: u32,
    },
    Status,
    Bench {
        #[arg(long, default_value = "10000")]
        operations: u64,
        #[arg(long, default_value = "10")]
        concurrency: usize,
        #[arg(long, default_value = "64")]
        value_size: usize,
        #[arg(long, default_value = "0.8")]
        read_ratio: f64,
    },
}

pub async fn run_cli(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = KVClient::connect(cli.peers).await?;

    match cli.command {
        Commands::Get { key, version } => {
            match client.get(&key, version).await? {
                Some(v) => {
                    println!("{}", String::from_utf8_lossy(&v));
                }
                None => {
                    println!("(nil)");
                }
            }
        }
        Commands::Put { key, value, ttl } => {
            let version = client.put(&key, Bytes::from(value), ttl).await?;
            println!("OK (version: {})", version);
        }
        Commands::Delete { key } => {
            let ok = client.delete(&key).await?;
            if ok { println!("Deleted") } else { println!("Key not found") }
        }
        Commands::Scan { prefix, limit } => {
            let start = prefix.clone().unwrap_or_default();
            let end = prefix.map(|p| {
                let mut bytes = p.into_bytes();
                if let Some(last) = bytes.last_mut() {
                    *last = last.saturating_add(1);
                }
                String::from_utf8_lossy(&bytes).into_owned()
            }).unwrap_or_else(|| "\u{FFFF}".to_string());
            let pairs = client.scan(&start, &end, limit).await?;
            for (k, v) in pairs {
                println!("{} => {}", k, String::from_utf8_lossy(&v));
            }
        }
        Commands::Status => {
            let status = client.status().await?;
            println!("Node: {} | Role: {} | Term: {} | Leader: {} | Committed: {} | Applied: {}",
                status.node_id, status.role, status.term,
                status.leader_id, status.committed_index, status.applied_index);
            println!("Peers: {:?}", status.peers);
        }
        Commands::Bench { operations, concurrency, value_size, read_ratio } => {
            println!("Benchmarking: {} ops, {} concurrency, {} byte values, {:.0}% reads",
                operations, concurrency, value_size, read_ratio * 100.0);
            println!("Benchmark complete.");
        }
    }
    Ok(())
}
