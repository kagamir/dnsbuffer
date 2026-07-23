use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub ecs: EcsConfig,
    #[serde(default)]
    pub adblock: AdblockConfig,
    #[serde(default)]
    pub hosts: Vec<HostEntry>,
    #[serde(default)]
    pub upstream: Vec<UpstreamConfig>,
    #[serde(default)]
    pub bootstrap: BootstrapConfig,
    #[serde(default)]
    pub fallback: Vec<UpstreamConfig>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    #[serde(default = "default_true")]
    pub tcp: bool,
}

#[derive(Debug, Default, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
}

fn default_max_entries() -> usize {
    10_000
}

#[derive(Debug, Default, Deserialize)]
pub struct EcsConfig {
    #[serde(default)]
    pub mode: EcsMode,
    #[serde(default)]
    pub fixed_subnet: String,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EcsMode {
    #[default]
    Auto,
    Fixed,
    Disabled,
}

#[derive(Debug, Default, Deserialize)]
pub struct AdblockConfig {
    #[serde(default)]
    pub allowlist: Vec<String>,
    #[serde(default)]
    pub block_response: BlockResponse,
    #[serde(default, rename = "rule_source")]
    pub rule_sources: Vec<RuleSource>,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BlockResponse {
    #[default]
    Zero,
}

#[derive(Debug, Deserialize)]
pub struct RuleSource {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub update_interval: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HostEntry {
    pub name: String,
    pub addrs: Vec<IpAddr>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum UpstreamConfig {
    Plain {
        addr: SocketAddr,
    },
    Doh {
        url: String,
        #[serde(default)]
        ech: String,
        #[serde(default)]
        http3: bool,
    },
    Dot {
        addr: SocketAddr,
        domain: String,
        #[serde(default)]
        ips: Vec<IpAddr>,
    },
}

#[derive(Debug, Default, Deserialize)]
pub struct BootstrapConfig {
    #[serde(default, rename = "server")]
    pub servers: Vec<UpstreamConfig>,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.upstream.is_empty() {
            bail!("config must define at least one [[upstream]]");
        }
        Ok(())
    }
}

pub fn load(path: &Path) -> Result<Config> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let cfg: Config = toml::from_str(&text).context("parsing config TOML")?;
    cfg.validate()?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_plain_upstream() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert_eq!(cfg.server.listen.to_string(), "127.0.0.1:5300");
        assert!(cfg.server.tcp, "tcp defaults to true");
        assert_eq!(cfg.upstream.len(), 1);
        match &cfg.upstream[0] {
            UpstreamConfig::Plain { addr } => assert_eq!(addr.to_string(), "1.1.1.1:53"),
            _ => panic!("expected plain upstream"),
        }
    }

    #[test]
    fn rejects_config_without_upstream() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert!(cfg.validate().is_err(), "empty upstream must fail validation");
    }
}
