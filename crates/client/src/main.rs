use clap::Parser;
use client::cli::{run_cli, Cli};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    run_cli(cli).await
}
