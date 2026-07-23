pub mod bootstrap;
pub mod cache;
pub mod config;
pub mod fetch;
pub mod filter;
pub mod hosts;
pub mod pipeline;
pub mod resolver;
pub mod server;
pub mod stats;
pub mod tls;
pub mod upstream;

use std::sync::Arc;

use anyhow::Result;

use crate::bootstrap::Bootstrap;
use crate::config::{Config, UpstreamConfig};
use crate::pipeline::Pipeline;
use crate::resolver::Resolver;
use crate::upstream::group::{FallbackResolver, UpstreamGroup};

/// 依据配置构建完整解析链：bootstrap → 上游组 →（可选）后备组。
pub async fn build_pipeline(config: &Config) -> Result<Arc<Pipeline>> {
    let bootstrap = Bootstrap::from_config(&config.bootstrap.servers)?;
    let primary = build_group(&config.upstream, config, &bootstrap).await?;
    let resolver: Arc<dyn Resolver> = if config.fallback.is_empty() {
        primary
    } else {
        let fb = build_group(&config.fallback, config, &bootstrap).await?;
        Arc::new(FallbackResolver::new(primary, fb))
    };
    Ok(Arc::new(Pipeline::new(resolver)))
}

async fn build_group(
    entries: &[UpstreamConfig],
    config: &Config,
    bootstrap: &Bootstrap,
) -> Result<Arc<dyn Resolver>> {
    let mut members: Vec<(String, Arc<dyn Resolver>)> = Vec::new();
    for u in entries {
        members.push(build_member(u, bootstrap).await?);
    }
    if members.is_empty() {
        anyhow::bail!("no upstreams configured");
    }
    Ok(Arc::new(UpstreamGroup::new(members, config.selector.window, config.selector.k)))
}

async fn build_member(
    u: &UpstreamConfig,
    bootstrap: &Bootstrap,
) -> Result<(String, Arc<dyn Resolver>)> {
    use crate::upstream::{doh::DohResolver, dot::DotResolver, plain::PlainResolver};
    match u {
        UpstreamConfig::Plain { addr } => {
            Ok((format!("plain:{addr}"), Arc::new(PlainResolver::new(*addr))))
        }
        UpstreamConfig::Dot { addr, domain, .. } => {
            let tls = Arc::new(crate::tls::client_config(&[], &[], None)?);
            Ok((format!("dot:{domain}"), Arc::new(DotResolver::new(*addr, domain, tls)?)))
        }
        UpstreamConfig::Doh { url, ech, http3, ips } => {
            let uri: http::Uri = url.parse()?;
            let host = uri.host().unwrap_or_default().to_string();
            let ips = if ips.is_empty() {
                if bootstrap.is_empty() {
                    anyhow::bail!("doh upstream {url} has no ips and no bootstrap configured");
                }
                bootstrap.resolve_ips(&host).await?
            } else {
                ips.clone()
            };
            let ech_bytes = if ech.is_empty() {
                if bootstrap.is_empty() {
                    None
                } else {
                    match bootstrap.fetch_ech(&host).await {
                        Ok(e) => e,
                        Err(err) => {
                            tracing::warn!("ECH fetch failed for {host}, using plain TLS: {err:#}");
                            None
                        }
                    }
                }
            } else {
                use base64::Engine as _;
                Some(
                    base64::engine::general_purpose::STANDARD
                        .decode(ech)
                        .map_err(|e| anyhow::anyhow!("invalid base64 ech for {url}: {e}"))?,
                )
            };
            if ech_bytes.is_none() {
                tracing::warn!("DoH upstream {host}: no ECH config available, SNI is visible");
            }
            Ok((format!("doh:{host}"), Arc::new(DohResolver::new(url, ips, ech_bytes, *http3)?)))
        }
    }
}
