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

// A DNS proxy is I/O-bound: queries spend their lifetime awaiting upstream UDP
// or TLS responses, so allocating one worker thread per CPU core only inflates
// RSS (every worker carries its own 2 MB stack plus a task queue) without
// improving throughput. We default to 2 worker threads — plenty for a typical
// home/embedded workload — and let the operator override via the
// DNSBUFFER_WORKER_THREADS environment variable when more parallelism is needed.
// Worker stacks are also shrunk from the 2 MB default to 512 KB, which is
// ample headroom over the deepest await chain in the pipeline.
const DEFAULT_WORKER_THREADS: usize = 2;
const WORKER_STACK_SIZE: usize = 512 * 1024;

fn build_runtime() -> Result<tokio::runtime::Runtime> {
    let worker_threads = std::env::var("DNSBUFFER_WORKER_THREADS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_WORKER_THREADS);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .thread_stack_size(WORKER_STACK_SIZE)
        .enable_all()
        .build()
        .context("failed to build tokio runtime")
}

fn main() -> Result<()> {
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

    let runtime = build_runtime()?;
    runtime.block_on(async move {
        dashboard::build_runtime(&cfg)
            .await?
            .run_until(async {
                tokio::signal::ctrl_c()
                    .await
                    .context("failed to listen for shutdown signal")
            })
            .await
    })
}
