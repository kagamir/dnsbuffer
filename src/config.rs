use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub log: LogConfig,
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
    #[serde(default)]
    pub selector: SelectorConfig,
    #[serde(default)]
    pub dashboard: DashboardConfig,
}

fn default_query_timeout_ms() -> u64 {
    10_000
}

fn default_upstream_timeout_ms() -> u64 {
    5_000
}

fn default_hedged_retry_ms() -> u64 {
    1_000
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    /// 单查询总超时（毫秒），包裹「上游+后备」整链，最终安全网。
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    /// 主上游阶段预算（毫秒）：失败或超过此时限即切换 fallback 兜底。
    #[serde(default = "default_upstream_timeout_ms")]
    pub upstream_timeout_ms: u64,
    /// 拨号上游时的地址族偏好：true 则 IPv6 优先；默认 false（IPv4 优先）。
    #[serde(default)]
    pub prefer_ipv6: bool,
    /// 对冲式重试间隔（毫秒）：主上游尝试超过该时长未返回，即并行发起新尝试
    /// 而不取消在途的；任一返回即胜出，直到 upstream_timeout_ms 耗尽。0 表示禁用。
    #[serde(default = "default_hedged_retry_ms")]
    pub hedged_retry_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct LogConfig {
    /// 日志等级：error | warn | info | debug | trace（也接受 EnvFilter 指令语法，
    /// 如 "warn,dnsbuffer=debug"）；RUST_LOG 环境变量优先于此配置。
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self { level: default_log_level() }
    }
}

fn default_log_level() -> String {
    "info".into()
}

#[derive(Debug, Default, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
}

fn default_max_entries() -> usize {
    10_000
}

#[derive(Debug, Deserialize)]
pub struct SelectorConfig {
    #[serde(default = "default_window")]
    pub window: usize,
    #[serde(default = "default_k")]
    pub k: f64,
}

impl Default for SelectorConfig {
    fn default() -> Self {
        Self {
            window: default_window(),
            k: default_k(),
        }
    }
}

fn default_window() -> usize {
    32
}

fn default_k() -> f64 {
    5.0
}

#[derive(Debug, Deserialize)]
pub struct DashboardConfig {
    #[serde(default = "default_dashboard_listen")]
    pub listen: SocketAddr,
    #[serde(default = "default_database_path")]
    pub database_path: PathBuf,
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            listen: default_dashboard_listen(),
            database_path: default_database_path(),
            retention_days: default_retention_days(),
        }
    }
}

fn default_dashboard_listen() -> SocketAddr {
    "0.0.0.0:8080".parse().unwrap()
}

fn default_database_path() -> PathBuf {
    PathBuf::from("dnsbuffer.db")
}

fn default_retention_days() -> u32 {
    7
}

#[derive(Debug, Default, Deserialize)]
pub struct EcsConfig {
    /// 配置了子网（如 "203.0.113.0/24"）则注入 ECS，否则不使用 ECS。
    #[serde(default)]
    pub fixed_subnet: Option<String>,
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

#[derive(Debug, Clone, Deserialize)]
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
        /// 默认 HTTP/2；显式 http3 = true 则仅用 HTTP/3（严格按配置，不回退 H2）。
        #[serde(default)]
        http3: bool,
        /// 可选：该 DoH 域名的 IP（仅一个）；留空经 bootstrap 解析域名。
        #[serde(default)]
        ip: Option<IpAddr>,
    },
    Dot {
        /// 服务器 IP；端口写在 domain 里（`host` 或 `host:port`），默认 853。
        ip: IpAddr,
        domain: String,
    },
}

/// 拆分 DoT 的 `domain` 为（SNI 主机名, 端口）；未写端口默认 853。
pub fn split_domain_port(domain: &str) -> Result<(String, u16)> {
    match domain.rsplit_once(':') {
        Some((host, port)) => {
            let port: u16 = port
                .parse()
                .with_context(|| format!("invalid port in dot domain {domain}"))?;
            Ok((host.to_string(), port))
        }
        None => Ok((domain.to_string(), 853)),
    }
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
        if self.dashboard.database_path.as_os_str().is_empty() {
            bail!("dashboard database_path must not be empty");
        }
        for b in &self.bootstrap.servers {
            if let UpstreamConfig::Doh { ip: None, url, .. } = b {
                bail!("bootstrap doh {url} must specify ip (chicken-and-egg)");
            }
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

    #[test]
    fn doh_upstream_defaults() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "doh"
            url = "https://dns.example/dns-query"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        match &cfg.upstream[0] {
            UpstreamConfig::Doh { url, ech, http3, ip } => {
                assert_eq!(url, "https://dns.example/dns-query");
                assert!(ech.is_empty());
                assert!(!*http3, "http3 defaults to false (opt-in via http3 = true)");
                assert!(ip.is_none(), "ip defaults to unset (resolve via bootstrap)");
            }
            _ => panic!("expected doh upstream"),
        }
    }

    #[test]
    fn doh_ip_accepts_v4_and_v6_literal() {
        let base = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "doh"
            url = "https://dns.example/dns-query"
            ip = "{ip}"
        "#;
        for (literal, is_v6) in [("2606:4700::6810:f8f9", true), ("104.16.248.249", false)] {
            let cfg: Config = toml::from_str(&base.replace("{ip}", literal)).expect("parse");
            match &cfg.upstream[0] {
                UpstreamConfig::Doh { ip: Some(ip), .. } => assert_eq!(ip.is_ipv6(), is_v6),
                _ => panic!("expected doh upstream with ip"),
            }
        }
    }

    #[test]
    fn dot_uses_ip_and_domain_with_optional_port() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "dot"
            ip = "9.9.9.9"
            domain = "dns.quad9.net"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        match &cfg.upstream[0] {
            UpstreamConfig::Dot { ip, domain } => {
                assert_eq!(ip.to_string(), "9.9.9.9");
                assert_eq!(domain, "dns.quad9.net");
            }
            _ => panic!("expected dot upstream"),
        }
    }

    #[test]
    fn split_domain_port_defaults_to_853() {
        assert_eq!(split_domain_port("dns.quad9.net").unwrap(), ("dns.quad9.net".into(), 853));
        assert_eq!(split_domain_port("dns.quad9.net:8853").unwrap(), ("dns.quad9.net".into(), 8853));
        assert!(split_domain_port("dns.quad9.net:notaport").is_err());
        assert!(split_domain_port("dns.quad9.net:99999").is_err(), "port out of u16 range");
    }

    #[test]
    fn selector_defaults() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert_eq!(cfg.selector.window, 32);
        assert!((cfg.selector.k - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn ecs_defaults_to_no_subnet() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert!(cfg.ecs.fixed_subnet.is_none(), "no fixed_subnet means ECS off");
        let with = r#"
            [server]
            listen = "127.0.0.1:5300"

            [ecs]
            fixed_subnet = "203.0.113.0/24"

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(with).expect("parse");
        assert_eq!(cfg.ecs.fixed_subnet.as_deref(), Some("203.0.113.0/24"));
    }

    #[test]
    fn bootstrap_doh_without_ip_rejected() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"

            [[bootstrap.server]]
            type = "doh"
            url = "https://bootstrap.example/dns-query"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert!(cfg.validate().is_err(), "bootstrap doh without ip must fail");
    }

    #[test]
    fn query_timeout_defaults_to_ten_thousand_ms() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert_eq!(cfg.server.query_timeout_ms, 10_000);
        assert_eq!(cfg.server.upstream_timeout_ms, 5_000, "primary upstream budget defaults to 5s");
        assert_eq!(cfg.server.hedged_retry_ms, 1_000, "hedged retry interval defaults to 1s");
    }

    #[test]
    fn log_level_defaults_to_info_and_is_configurable() {
        let base = r#"
            [server]
            listen = "127.0.0.1:5300"
            {extra}
            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(&base.replace("{extra}", "")).expect("parse");
        assert_eq!(cfg.log.level, "info");
        let cfg: Config = toml::from_str(&base.replace("{extra}", "[log]\nlevel = \"warn\"\n"))
            .expect("parse");
        assert_eq!(cfg.log.level, "warn");
    }

    #[test]
    fn example_config_stays_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
        load(&path).expect("config.example.toml must parse and validate");
    }

    #[test]
    fn dashboard_defaults_and_overrides() {
        let base = r#"
            [server]
            listen = "127.0.0.1:5300"
            {dashboard}
            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(&base.replace("{dashboard}", "")).unwrap();
        assert_eq!(cfg.dashboard.listen, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(cfg.dashboard.database_path, PathBuf::from("dnsbuffer.db"));
        assert_eq!(cfg.dashboard.retention_days, 7);

        let custom = "[dashboard]\nlisten = \"127.0.0.1:9090\"\ndatabase_path = \"data/stats.db\"\nretention_days = 0";
        let cfg: Config = toml::from_str(&base.replace("{dashboard}", custom)).unwrap();
        assert_eq!(cfg.dashboard.listen, "127.0.0.1:9090".parse().unwrap());
        assert_eq!(cfg.dashboard.database_path, PathBuf::from("data/stats.db"));
        assert_eq!(cfg.dashboard.retention_days, 0);
    }

    #[test]
    fn dashboard_rejects_empty_database_path() {
        let text = r#"
            [server]
            listen = "127.0.0.1:5300"
            [dashboard]
            database_path = ""
            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(text).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn prefer_ipv6_defaults_false_and_is_opt_in() {
        let base = r#"
            [server]
            listen = "127.0.0.1:5300"
            {extra}

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(&base.replace("{extra}", "")).expect("parse");
        assert!(!cfg.server.prefer_ipv6, "defaults to IPv4-first dialing");
        let cfg: Config =
            toml::from_str(&base.replace("{extra}", "prefer_ipv6 = true")).expect("parse");
        assert!(cfg.server.prefer_ipv6);
    }
}
