//! Talon worker entry point.

use clap::Parser;
use std::path::PathBuf;
use talon_core::{WorkerConfig, WorkerConfigPatch};

/// Command-line arguments for a Talon worker.
///
/// Flags are the highest-precedence configuration layer (CLI > env > file >
/// default). Unset flags fall through to the lower layers.
#[derive(Debug, Parser)]
#[command(name = "talon-worker", version, about)]
struct Args {
    /// Path to a TOML config file.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Address to bind the worker RPC service to.
    #[arg(long)]
    listen: Option<String>,

    /// Address of the coordinator to register with.
    #[arg(long)]
    coordinator: Option<String>,

    /// Logical block size in bytes.
    #[arg(long)]
    block_size: Option<u32>,
}

impl Args {
    fn into_patch(self) -> WorkerConfigPatch {
        WorkerConfigPatch {
            listen: self.listen,
            coordinator: self.coordinator,
            block_size: self.block_size,
            cache_dirs: None,
            capacity_bytes: None,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let file = match &args.config {
        Some(path) => WorkerConfigPatch::from_file(path)?,
        None => WorkerConfigPatch::default(),
    };
    let env = WorkerConfigPatch::from_env()?;
    let cli = args.into_patch();
    let cfg = WorkerConfig::resolve(file, env, cli)?;

    tracing::info!(
        listen = %cfg.listen,
        coordinator = %cfg.coordinator,
        block_size = cfg.block_size,
        cache_dirs = ?cfg.cache_dirs,
        capacity_bytes = cfg.capacity_bytes,
        "starting talon-worker"
    );

    // TODO: register with coordinator and serve the object store over RPC.

    Ok(())
}
