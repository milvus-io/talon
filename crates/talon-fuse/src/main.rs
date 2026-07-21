//! Talon FUSE client entry point.

use clap::Parser;

/// Command-line arguments for the Talon FUSE mount.
#[derive(Debug, Parser)]
#[command(name = "talon-fuse", version, about)]
struct Args {
    /// Directory to mount the Talon filesystem at.
    #[arg(long)]
    mountpoint: String,

    /// Address of the coordinator to connect to.
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
        mountpoint = %args.mountpoint,
        coordinator = %args.coordinator,
        "starting talon-fuse"
    );

    // TODO: connect to the cluster and mount the FUSE filesystem.

    Ok(())
}
