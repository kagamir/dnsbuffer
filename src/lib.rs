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
use crate::dashboard::upstreams::UpstreamMetricsBuilder;
use crate::dashboard::{Recorder, upstreams::UpstreamMetrics};
use crate::pipeline::Pipeline;
use crate::resolver::Resolver;
use crate::upstream::group::{FallbackResolver, UpstreamGroup};

pub struct BuiltPipeline {
    pub pipeline: Arc<Pipeline>,
    pub upstream_metrics: UpstreamMetrics,
    pub cache_sampler: Arc<crate::dashboard::sampler::CacheHitSampler>,
}

/// Builds the resolution chain from the config, and records queries using the caller-owned recorder.
pub async fn build_pipeline(config: &Config, recorder: Recorder) -> Result<BuiltPipeline> {
    let bootstrap = Arc::new(Bootstrap::from_config(
        &config.bootstrap.servers,
        config.server.prefer_ipv6,
    )?);

    // Ad blocking: initial load + periodic hot swap
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
    let mut metrics = UpstreamMetricsBuilder::default();
    let mut primary = build_group(
        &config.upstream,
        config,
        &bootstrap,
        &mut metrics,
        "primary",
    )
    .await?;
    // Hedged retry: if the primary upstream does not return within hedged_retry_ms, send a parallel retry; 0 disables it
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
        let fb = build_group(
            &config.fallback,
            config,
            &bootstrap,
            &mut metrics,
            "fallback",
        )
        .await?;
        Arc::new(FallbackResolver::new(
            primary,
            fb,
            std::time::Duration::from_millis(config.server.upstream_timeout_ms),
        ))
    };
    let metrics = metrics.build();

    let cache_sampler = Arc::new(crate::dashboard::sampler::CacheHitSampler::new(
        cache.clone(),
    ));
    let pipeline = Arc::new(Pipeline::new(crate::pipeline::PipelineParts {
        hosts,
        filter,
        cache,
        cache_sampler: cache_sampler.clone(),
        upstream: resolver,
        ecs,
        query_timeout: std::time::Duration::from_millis(config.server.query_timeout_ms),
        recorder,
    }));
    Ok(BuiltPipeline {
        pipeline,
        upstream_metrics: metrics,
        cache_sampler,
    })
}

async fn build_group(
    entries: &[UpstreamConfig],
    config: &Config,
    bootstrap: &Bootstrap,
    metrics: &mut UpstreamMetricsBuilder,
    group_kind: &'static str,
) -> Result<Arc<dyn Resolver>> {
    let mut members: Vec<(String, Arc<dyn Resolver>)> = Vec::new();
    for u in entries {
        members.push(build_member(u, bootstrap, config.server.prefer_ipv6).await?);
    }
    if members.is_empty() {
        anyhow::bail!("no upstreams configured");
    }
    Ok(Arc::new(UpstreamGroup::new(
        members,
        config.selector.window,
        config.selector.k,
        u64::from(config.dashboard.retention_days),
        metrics,
        group_kind,
    )))
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
                format!(
                    "dot:{host}:{port}@{}",
                    match ip {
                        std::net::IpAddr::V4(ip) => ip.to_string(),
                        std::net::IpAddr::V6(ip) => format!("[{ip}]"),
                    }
                ),
                Arc::new(DotResolver::new(
                    std::net::SocketAddr::new(*ip, port),
                    &host,
                    tls,
                )?),
            ))
        }
        UpstreamConfig::Doh {
            url,
            ech,
            http3,
            ip,
        } => {
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
                match ip {
                    Some(std::net::IpAddr::V4(ip)) => format!("doh:{url}@{ip}"),
                    Some(std::net::IpAddr::V6(ip)) => format!("doh:{url}@[{ip}]"),
                    None => format!("doh:{url}"),
                },
                Arc::new(DohResolver::new(url, ips, ech_bytes, *http3, prefer_ipv6)?),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pipeline_metrics_preserve_configured_groups_order_and_unique_identity() {
        let config: Config = toml::from_str(
            r#"
            [server]
            listen = "127.0.0.1:5300"
            hedged_retry_ms = 0

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"

            [[upstream]]
            type = "dot"
            ip = "2001:db8::1"
            domain = "dns.example:8853"

            [[upstream]]
            type = "doh"
            url = "https://dns.example:8443/first?profile=a"
            ip = "192.0.2.1"

            [[upstream]]
            type = "doh"
            url = "https://dns.example:8443/second"
            ip = "2001:db8::2"

            [[upstream]]
            type = "doh"
            url = "https://dns.example:8443/second"
            ip = "2001:db8::2"

            [[fallback]]
            type = "plain"
            addr = "8.8.8.8:53"
            "#,
        )
        .expect("config parses");

        let built = build_pipeline(&config, Recorder::disabled())
            .await
            .expect("builds");
        let snapshots = built.upstream_metrics.snapshot();

        assert_eq!(snapshots.len(), 6);
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot.id.as_str())
                .collect::<Vec<_>>(),
            [
                "primary-0",
                "primary-1",
                "primary-2",
                "primary-3",
                "primary-4",
                "fallback-0"
            ]
        );
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot.group)
                .collect::<Vec<_>>(),
            [
                "primary", "primary", "primary", "primary", "primary", "fallback"
            ]
        );
        assert_eq!(snapshots[0].name, "plain:1.1.1.1:53");
        assert_eq!(snapshots[1].name, "dot:dns.example:8853@[2001:db8::1]");
        assert_eq!(
            snapshots[2].name,
            "doh:https://dns.example:8443/first?profile=a@192.0.2.1"
        );
        assert_eq!(
            snapshots[3].name,
            "doh:https://dns.example:8443/second@[2001:db8::2]"
        );
        assert_eq!(snapshots[4].name, snapshots[3].name);
        assert_ne!(snapshots[4].id, snapshots[3].id);
    }
}
