use arc_swap::ArcSwap;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA};
use hickory_proto::rr::{RData, Record, RecordType};
use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use crate::bootstrap::Bootstrap;
use crate::config::RuleSource;
use std::time::Duration;

const BLOCK_TTL: u32 = 300;

fn normalize(name: &str) -> String {
    name.trim_end_matches('.').to_lowercase()
}

fn valid_domain(s: &str) -> bool {
    !s.is_empty()
        && !s.contains(['/', '*', '$', '|', '^', ' ', '\t'])
        && s.contains('.')
}

/// 编译后的规则集：屏蔽域集合 + 例外域集合（都按后缀匹配语义使用）。
#[derive(Default)]
pub struct RuleSet {
    blocked: HashSet<String>,
    exceptions: HashSet<String>,
}

impl RuleSet {
    pub fn merge(&mut self, other: RuleSet) {
        self.blocked.extend(other.blocked);
        self.exceptions.extend(other.exceptions);
    }

    pub fn len(&self) -> (usize, usize) {
        (self.blocked.len(), self.exceptions.len())
    }
}

/// 解析 adblock 子集 + hosts 语法 + 纯域名行；不认识的行跳过。
pub fn parse_rules(text: &str) -> RuleSet {
    let mut set = RuleSet::default();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('!') || line.starts_with('#') {
            continue;
        }
        // @@||domain^ 例外
        if let Some(rest) = line.strip_prefix("@@||") {
            let domain = rest.split(['^', '$']).next().unwrap_or("");
            let domain = normalize(domain);
            if valid_domain(&domain) {
                set.exceptions.insert(domain);
            }
            continue;
        }
        // ||domain^ 屏蔽
        if let Some(rest) = line.strip_prefix("||") {
            let domain = rest.split(['^', '$']).next().unwrap_or("");
            let domain = normalize(domain);
            if valid_domain(&domain) {
                set.blocked.insert(domain);
            }
            continue;
        }
        // hosts 语法：IP + 空白 + 域名（行内 # 注释截断）
        let line = line.split('#').next().unwrap_or("").trim();
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some(first), Some(second)) if first.parse::<std::net::IpAddr>().is_ok() => {
                let domain = normalize(second);
                if valid_domain(&domain) {
                    set.blocked.insert(domain);
                }
            }
            (Some(first), None) => {
                // 纯域名行
                let domain = normalize(first);
                if valid_domain(&domain) {
                    set.blocked.insert(domain);
                }
            }
            _ => {}
        }
    }
    set
}

/// 广告屏蔽器：ArcSwap 热替换规则集 + 配置豁免；读路径无锁。
pub struct Filter {
    rules: ArcSwap<RuleSet>,
    allowlist: HashSet<String>,
}

impl Filter {
    pub fn new(allowlist: &[String]) -> Self {
        Self {
            rules: ArcSwap::from_pointee(RuleSet::default()),
            allowlist: allowlist.iter().map(|s| normalize(s)).collect(),
        }
    }

    pub fn store(&self, rules: RuleSet) {
        self.rules.store(Arc::new(rules));
    }

    /// 后缀游走匹配；豁免（allowlist/例外）优先。
    pub fn is_blocked(&self, name: &str) -> bool {
        let name = normalize(name);
        let rules = self.rules.load();
        let mut candidate: &str = &name;
        loop {
            if self.allowlist.contains(candidate) || rules.exceptions.contains(candidate) {
                return false;
            }
            match candidate.find('.') {
                Some(pos) => candidate = &candidate[pos + 1..],
                None => break,
            }
        }
        let mut candidate: &str = &name;
        loop {
            if rules.blocked.contains(candidate) {
                return true;
            }
            match candidate.find('.') {
                Some(pos) => candidate = &candidate[pos + 1..],
                None => return false,
            }
        }
    }

    /// 屏蔽应答：A→0.0.0.0，AAAA→::，其他→NODATA。
    pub fn block_response(&self, query: &Message) -> Message {
        let mut resp = Message::new(query.metadata.id, MessageType::Response, query.metadata.op_code);
        resp.metadata.response_code = ResponseCode::NoError;
        resp.metadata.recursion_desired = query.metadata.recursion_desired;
        resp.metadata.recursion_available = true;
        if let Some(q) = query.queries.first() {
            resp.add_query(q.clone());
            let owner = q.name().clone();
            match q.query_type() {
                RecordType::A => {
                    resp.add_answer(Record::from_rdata(
                        owner,
                        BLOCK_TTL,
                        RData::A(A(Ipv4Addr::UNSPECIFIED)),
                    ));
                }
                RecordType::AAAA => {
                    resp.add_answer(Record::from_rdata(
                        owner,
                        BLOCK_TTL,
                        RData::AAAA(AAAA(Ipv6Addr::UNSPECIFIED)),
                    ));
                }
                _ => {}
            }
        }
        resp
    }
}

/// 加载全部规则源（本地 path / 远程 url），单源失败 warn 跳过。
pub async fn load_sources(sources: &[RuleSource], bootstrap: &Bootstrap) -> RuleSet {
    let mut merged = RuleSet::default();
    for s in sources {
        let text = if let Some(path) = &s.path {
            match std::fs::read_to_string(path) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("reading rule file {path} failed: {e}");
                    continue;
                }
            }
        } else if let Some(url) = &s.url {
            match crate::fetch::fetch_url(url, bootstrap).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(e) => {
                    tracing::warn!("fetching rules {url} failed: {e:#}");
                    continue;
                }
            }
        } else {
            tracing::warn!("rule source with neither path nor url, skipping");
            continue;
        };
        merged.merge(parse_rules(&text));
    }
    let (b, e) = merged.len();
    tracing::info!("loaded {b} blocked / {e} exception rules");
    merged
}

/// 有定时 url 源时启动后台刷新：按最小合法周期整体重拉并热替换。
pub fn spawn_updater(filter: Arc<Filter>, sources: Vec<RuleSource>, bootstrap: Arc<Bootstrap>) {
    let mut min_interval: Option<Duration> = None;
    for s in &sources {
        if s.url.is_some() {
            if let Some(iv) = &s.update_interval {
                match humantime::parse_duration(iv) {
                    Ok(d) if !d.is_zero() => {
                        min_interval = Some(min_interval.map_or(d, |m| m.min(d)));
                    }
                    Ok(_) => tracing::warn!("zero update_interval ignored"),
                    Err(e) => tracing::warn!("invalid update_interval {iv}: {e}"),
                }
            }
        }
    }
    let Some(period) = min_interval else { return };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // 首个 tick 立即完成，跳过（启动时已加载）
        loop {
            ticker.tick().await;
            let rules = load_sources(&sources, &bootstrap).await;
            let (b, _) = rules.len();
            if b == 0 {
                tracing::warn!("periodic rule refresh yielded 0 blocked entries, keeping old set");
                continue;
            }
            filter.store(rules);
            tracing::info!("rule set refreshed");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RData, RecordType};
    use std::str::FromStr;

    const RULES: &str = r#"
! adblock comment
# hosts comment
||ads.example.com^
||tracker.net^$third-party
@@||good.ads.example.com^
0.0.0.0 hosts-blocked.com
127.0.0.1 also-blocked.org # trailing comment
:: v6-blocked.io
plain-blocked.dev
/regex-rule/
*.wild.card.unsupported
"#;

    fn filter_with(allow: &[&str]) -> Filter {
        let f = Filter::new(&allow.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        f.store(parse_rules(RULES));
        f
    }

    #[test]
    fn adblock_and_hosts_and_plain_lines_block() {
        let f = filter_with(&[]);
        for d in [
            "ads.example.com",
            "sub.ads.example.com", // 后缀匹配
            "tracker.net",
            "hosts-blocked.com",
            "also-blocked.org",
            "v6-blocked.io",
            "plain-blocked.dev",
        ] {
            assert!(f.is_blocked(d), "{d} should be blocked");
        }
    }

    #[test]
    fn exceptions_and_allowlist_win() {
        let f = filter_with(&["whitelisted.tracker.net"]);
        assert!(!f.is_blocked("good.ads.example.com"), "@@ exception wins");
        assert!(!f.is_blocked("x.good.ads.example.com"), "exception suffix wins");
        assert!(!f.is_blocked("whitelisted.tracker.net"), "config allowlist wins");
        assert!(f.is_blocked("tracker.net"), "non-exempt name still blocked");
    }

    #[test]
    fn unparsable_lines_skipped_and_unblocked_pass() {
        let f = filter_with(&[]);
        assert!(!f.is_blocked("innocent.example"));
        assert!(!f.is_blocked("regex-rule"));
    }

    #[test]
    fn block_response_shapes() {
        let f = filter_with(&[]);
        let mk = |qtype| {
            let mut m = Message::new(0x77, MessageType::Query, OpCode::Query);
            let mut q = Query::new();
            q.set_name(Name::from_str("ads.example.com.").unwrap());
            q.set_query_type(qtype);
            m.add_query(q);
            m
        };
        let a = f.block_response(&mk(RecordType::A));
        assert_eq!(a.metadata.id, 0x77);
        assert_eq!(a.answers.len(), 1);
        assert!(matches!(a.answers[0].data, RData::A(v) if v.0.is_unspecified()));
        let aaaa = f.block_response(&mk(RecordType::AAAA));
        assert!(matches!(aaaa.answers[0].data, RData::AAAA(v) if v.0.is_unspecified()));
        let txt = f.block_response(&mk(RecordType::TXT));
        assert_eq!(txt.metadata.response_code, ResponseCode::NoError);
        assert!(txt.answers.is_empty(), "non-address qtype → NODATA");
    }

    #[test]
    fn hot_swap_replaces_rules() {
        let f = filter_with(&[]);
        assert!(f.is_blocked("plain-blocked.dev"));
        f.store(parse_rules("||only.new.rule^"));
        assert!(!f.is_blocked("plain-blocked.dev"), "old rules replaced");
        assert!(f.is_blocked("only.new.rule"));
    }

    #[tokio::test]
    async fn load_sources_merges_file_and_url() {
        let dir = std::env::temp_dir().join("dnsbuffer-filter-test");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("local.txt");
        std::fs::write(&file, "0.0.0.0 local-file-blocked.com\n").unwrap();

        let addr = crate::fetch::tests::spawn_http_server("||remote-blocked.example^\n").await;
        let sources = vec![
            crate::config::RuleSource {
                path: Some(file.to_string_lossy().into_owned()),
                url: None,
                update_interval: None,
            },
            crate::config::RuleSource {
                path: None,
                url: Some(format!("http://{addr}/rules.txt")),
                update_interval: None,
            },
            crate::config::RuleSource {
                path: None,
                url: Some(format!("http://{addr}/missing")), // 失败源仅 warn 跳过
                update_interval: None,
            },
        ];
        let bootstrap = crate::bootstrap::Bootstrap::from_config(&[]).unwrap();
        let rules = load_sources(&sources, &bootstrap).await;
        let f = Filter::new(&[]);
        f.store(rules);
        assert!(f.is_blocked("local-file-blocked.com"));
        assert!(f.is_blocked("remote-blocked.example"));
    }
}
