use std::path::PathBuf;

use anyhow::{Context, Result};
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
    let args = Args::parse();
    let cfg = config::load(&args.config)?;

    // 日志等级来自配置文件 log.level；RUST_LOG 环境变量优先（便于临时调试）
    let filter = match std::env::var("RUST_LOG") {
        Ok(env) => tracing_subscriber::EnvFilter::try_new(&env)
            .with_context(|| format!("invalid RUST_LOG {env:?}"))?,
        Err(_) => tracing_subscriber::EnvFilter::try_new(&cfg.log.level)
            .with_context(|| format!("invalid log.level {:?} in config", cfg.log.level))?,
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let pipeline = build_pipeline(&cfg).await?;
    tracing::warn!("dnsbuffer starting");
    server::run_udp(cfg.server.listen, pipeline).await
}
