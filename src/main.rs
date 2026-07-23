use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use dnsbuffer::{build_pipeline, config, server};

#[derive(Parser, Debug)]
#[command(name = "dnsbuffer", about = "A DNS proxy with DoH/ECH upstreams")]
struct Args {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let cfg = config::load(&args.config)?;
    let pipeline = build_pipeline(&cfg)?;
    tracing::info!("dnsbuffer starting");
    server::run_udp(cfg.server.listen, pipeline).await
}
