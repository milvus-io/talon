//! Talon coordinator entry point.

use clap::Parser;

/// Command-line arguments for the Talon coordinator.
#[derive(Debug, Parser)]
#[command(name = "talon-coordinator", version, about)]
struct Args {
    /// Address to bind the coordinator RPC service to.
    #[arg(long, default_value = "127.0.0.1:7000")]
    listen: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    tracing::info!(listen = %args.listen, "starting talon-coordinator");

    // TODO: start RPC server, membership gossip, and placement service.

    Ok(())
}
