pub mod config;
pub mod pipeline;
pub mod resolver;
pub mod server;
pub mod stats;
pub mod upstream;

use std::sync::Arc;

use anyhow::{bail, Result};

use crate::config::{Config, UpstreamConfig};
use crate::pipeline::Pipeline;
use crate::resolver::Resolver;
use crate::upstream::plain::PlainResolver;

/// 依据配置构建本计划支持的上游（当前仅明文），返回 pipeline。
/// 后续计划在此扩展为构建上游组 + fallback。
pub fn build_pipeline(config: &Config) -> Result<Arc<Pipeline>> {
    let first_plain = config.upstream.iter().find_map(|u| match u {
        UpstreamConfig::Plain { addr } => Some(*addr),
        _ => None,
    });
    let addr = match first_plain {
        Some(addr) => addr,
        None => bail!("plan 1 requires at least one plain upstream (type = \"plain\")"),
    };
    let resolver: Arc<dyn Resolver> = Arc::new(PlainResolver::new(addr));
    Ok(Arc::new(Pipeline::new(resolver)))
}
