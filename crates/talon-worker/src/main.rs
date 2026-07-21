//! Talon worker entry point.

use clap::Parser;

/// Command-line arguments for a Talon worker.
#[derive(Debug, Parser)]
#[command(name = "talon-worker", version, about)]
struct Args {
    /// Address to bind the worker RPC service to.
    #[arg(long, default_value = "127.0.0.1:7001")]
    listen: String,

    /// Address of the coordinator to register with.
    #[arg(long, default_value = "127.0.0.1:7000")]
    coordinator: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    tracing::info!(
        listen = %args.listen,
        coordinator = %args.coordinator,
        "starting talon-worker"
    );

    // TODO: register with coordinator and serve the object store over RPC.

    Ok(())
}
