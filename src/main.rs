use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use dnsbuffer::{config, dashboard};

#[derive(Parser, Debug)]
#[command(name = "dnsbuffer", about = "A DNS proxy with DoH/ECH upstreams")]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = config::load(&args.config)?;

    // The log level comes from the config file's log.level; the RUST_LOG environment variable takes precedence (convenient for ad-hoc debugging)
    let filter = match std::env::var("RUST_LOG") {
        Ok(env) => tracing_subscriber::EnvFilter::try_new(&env)
            .with_context(|| format!("invalid RUST_LOG {env:?}"))?,
        Err(_) => tracing_subscriber::EnvFilter::try_new(&cfg.log.level)
            .with_context(|| format!("invalid log.level {:?} in config", cfg.log.level))?,
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();

    dashboard::build_runtime(&cfg)
        .await?
        .run_until(async {
            tokio::signal::ctrl_c()
                .await
                .context("failed to listen for shutdown signal")
        })
        .await
}
