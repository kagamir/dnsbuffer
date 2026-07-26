pub mod bootstrap;
pub mod cache;
pub mod config;
pub mod dashboard;
pub mod ecs;
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

/// 依据配置构建完整解析链：bootstrap → filter → hosts → cache → ECS → 上游组
/// →（可选）后备组 → Pipeline 装配。
pub async fn build_pipeline(config: &Config) -> Result<Arc<Pipeline>> {
    let bootstrap = Arc::new(Bootstrap::from_config(
        &config.bootstrap.servers,
        config.server.prefer_ipv6,
    )?);

    // 广告屏蔽：初次加载 + 定时热替换
    let filter = Arc::new(crate::filter::Filter::new(&config.adblock.allowlist));
    if !config.adblock.rule_sources.is_empty() {
        let rules = crate::filter::load_sources(&config.adblock.rule_sources, &bootstrap).await;
        filter.store(rules);
        crate::filter::spawn_updater(
            filter.clone(),
            config.adblock.rule_sources.clone(),
            bootstrap.clone(),
        );
    }

    let hosts = crate::hosts::HostsMap::from_entries(&config.hosts);
    let cache = Arc::new(crate::cache::Cache::new(config.cache.max_entries));
    let ecs = crate::ecs::subnet_from_config(&config.ecs);
    #[cfg(not(test))]
    let recorder = {
        let store = crate::dashboard::store::Store::open(&config.dashboard.database_path)?;
        crate::dashboard::store::StoreWorker::start(
            store,
            u64::from(config.dashboard.retention_days),
        )
        .detach()
    };

    let mut primary = build_group(&config.upstream, config, &bootstrap).await?;
    // 对冲式重试：主上游尝试超过 hedged_retry_ms 未返回即并行再发，0 禁用
    if config.server.hedged_retry_ms > 0 {
        primary = Arc::new(crate::upstream::hedged::HedgedResolver::new(
            primary,
            std::time::Duration::from_millis(config.server.hedged_retry_ms),
            std::time::Duration::from_millis(config.server.upstream_timeout_ms),
        ));
    }
    let resolver: Arc<dyn Resolver> = if config.fallback.is_empty() {
        primary
    } else {
        let fb = build_group(&config.fallback, config, &bootstrap).await?;
        Arc::new(FallbackResolver::new(
            primary,
            fb,
            std::time::Duration::from_millis(config.server.upstream_timeout_ms),
        ))
    };

    Ok(Arc::new(Pipeline::new(crate::pipeline::PipelineParts {
        hosts,
        filter,
        cache,
        upstream: resolver,
        ecs,
        query_timeout: std::time::Duration::from_millis(config.server.query_timeout_ms),
        #[cfg(not(test))]
        recorder,
    })))
}

async fn build_group(
    entries: &[UpstreamConfig],
    config: &Config,
    bootstrap: &Bootstrap,
) -> Result<Arc<dyn Resolver>> {
    let mut members: Vec<(String, Arc<dyn Resolver>)> = Vec::new();
    for u in entries {
        members.push(build_member(u, bootstrap, config.server.prefer_ipv6).await?);
    }
    if members.is_empty() {
        anyhow::bail!("no upstreams configured");
    }
    Ok(Arc::new(UpstreamGroup::new(members, config.selector.window, config.selector.k)))
}

async fn build_member(
    u: &UpstreamConfig,
    bootstrap: &Bootstrap,
    prefer_ipv6: bool,
) -> Result<(String, Arc<dyn Resolver>)> {
    use crate::upstream::{doh::DohResolver, dot::DotResolver, plain::PlainResolver};
    match u {
        UpstreamConfig::Plain { addr } => {
            Ok((format!("plain:{addr}"), Arc::new(PlainResolver::new(*addr))))
        }
        UpstreamConfig::Dot { ip, domain } => {
            let (host, port) = crate::config::split_domain_port(domain)?;
            let tls = Arc::new(crate::tls::client_config(&[], &[], None)?);
            Ok((
                format!("dot:{host}"),
                Arc::new(DotResolver::new(std::net::SocketAddr::new(*ip, port), &host, tls)?),
            ))
        }
        UpstreamConfig::Doh { url, ech, http3, ip } => {
            let uri: http::Uri = url.parse()?;
            let host = uri.host().unwrap_or_default().to_string();
            let ips = match ip {
                Some(ip) => vec![*ip],
                None => {
                    if bootstrap.is_empty() {
                        anyhow::bail!("doh upstream {url} has no ip and no bootstrap configured");
                    }
                    bootstrap.resolve_ips(&host).await?
                }
            };
            let ech_bytes = if ech.is_empty() {
                if bootstrap.is_empty() {
                    None
                } else {
                    match bootstrap.fetch_ech(&host).await {
                        Ok(e) => e,
                        Err(err) => {
                            tracing::info!("ECH fetch failed for {host}, using plain TLS: {err:#}");
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
            Ok((
                format!("doh:{host}"),
                Arc::new(DohResolver::new(url, ips, ech_bytes, *http3, prefer_ipv6)?),
            ))
        }
    }
}
