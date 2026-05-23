use clap::Parser;
use server::config::{NodeConfig, ServerArgs};
use server::grpc_server::create_server;
use server::node_runner::NodeRunner;
use std::sync::Arc;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = ServerArgs::parse();
    let addr = args.addr.parse()?;
    let config: NodeConfig = args.into();

    tracing::info!(node_id = %config.node_id, addr = %config.grpc_addr, "Starting rustkvd node");

    let node = Arc::new(NodeRunner::new(config)?);
    let kv_service = create_server(node);

    Server::builder()
        .add_service(kv_service)
        .serve(addr)
        .await?;

    Ok(())
}
