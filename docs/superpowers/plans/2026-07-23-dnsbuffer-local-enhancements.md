# dnsbuffer 计划三：本地增强 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 dnsbuffer 加入乐观缓存（纯内存链式哈希表）、自定义 hosts、广告屏蔽（adblock/hosts 语法，本地文件 + 远程 URL 定时更新，arc_swap 热替换）、EDNS 客户端子网注入，并把它们按 hosts → filter → cache → (ECS) → 上游 的顺序接入 Pipeline，同时加入单查询总超时预算。

**Architecture:** `Pipeline` 从单字段升级为编排器：持有 `HostsMap`、`Filter`（内部 `ArcSwap<RuleSet>`）、`Arc<Cache>`、`Arc<dyn Resolver>`、可选 ECS 子网与总超时。缓存乐观命中直接回（改写 id），过期时后台 spawn 刷新（克隆 `Arc<Cache>` + `Arc<dyn Resolver>`）。远程规则拉取复用 bootstrap 解析器解析规则源域名，经 hyper HTTP/1.1 + rustls 拉取，定时任务 `ArcSwap::store` 原子热替换。

**Tech Stack:** 计划二栈 + hashlink（LinkedHashMap）· arc-swap · humantime（时长解析）· hyper `http1` feature（规则拉取）。

## Global Constraints

- 继承计划一/二全部约束：anyhow::Result、运行时数据路径绝不 unwrap/panic（测试除外）、hickory-proto `Message` 编解码、模块注册 `src/lib.rs`、每 Task 提交、clippy 保持零告警。
- hickory-proto 0.26 适配模式：`msg.metadata.*` 直赋值、`Message::new(id, type, opcode)`、`msg.queries`/`msg.answers` 字段访问；EDNS/OPT API（`Edns`、`EdnsOption`、`ClientSubnet`）以安装版本为准适配（授权偏差，mirror 既有代码风格）。
- 缓存：**FIFO 非 LRU**——读取绝不改变队列顺序；乐观命中（过期也返回）；`remove`+`insert` 到队尾完成刷新；超限 `pop_front`；缓存命中返回前必须把存储报文的 id 改写为当前查询 id。
- 只缓存 `NoError` 响应；SERVFAIL/错误不入缓存。TTL 取 answers 最小 TTL（无 answers 用 60s）。
- 屏蔽命中返回：A → `0.0.0.0`，AAAA → `::`，其他 qtype → 空 answers 的 NoError（NODATA）；TTL 300。
- 豁免优先：规则内 `@@` 例外与配置 `allowlist` 都按后缀匹配，命中即放行。
- 域名匹配一律规范化：去尾点、转小写、后缀游走（`a.b.c` 检查 `a.b.c`、`b.c`、`c`）。
- 远程规则拉取失败：保留上一次成功规则集 + `tracing::warn!`；首次失败该源为空。拉取体积上限 20 MiB。
- ECS：`disabled` 不注入；`fixed` 用配置子网；`auto` 启动时探测出口 IP 取 /24（IPv4）、/56（IPv6），出口为私有地址时不注入（并 warn）。
- 单查询总超时（`server.query_timeout_secs`，默认 10s）包裹「上游+后备」整链，超时回 SERVFAIL。

---

## File Structure

- Modify: `Cargo.toml` — hashlink、arc-swap、humantime、hyper 加 `http1`。
- Modify: `src/config.rs` — `ServerConfig.query_timeout_secs`（默认 10）。
- Create: `src/cache.rs` — `CacheKey`、`Cache`（Mutex<LinkedHashMap>，FIFO+乐观）。
- Create: `src/hosts.rs` — `HostsMap`（精确+`*.`通配，构造 A/AAAA/NODATA 应答）。
- Create: `src/filter.rs` — 规则解析（hosts/adblock 子集）、`RuleSet`、`Filter`（ArcSwap + allowlist + 屏蔽应答构造）。
- Create: `src/fetch.rs` — HTTPS GET（h1，bootstrap 解析域名，≤3 跳转，20MiB 上限）。
- Modify: `src/filter.rs`（同 Task 内）— `spawn_updater` 定时拉取热替换。
- Create: `src/ecs.rs` — 子网解析/出口探测/掩码、OPT 注入。
- Modify: `src/pipeline.rs` — 编排器重写。
- Modify: `src/lib.rs` — `build_pipeline` 装配全链。
- Modify: `src/main.rs` — 无签名变化（装配在 lib）。
- Modify: `tests/forwarding.rs` — 适配 + hosts/屏蔽/缓存端到端测试。
- Modify: `config.example.toml` — 完整示例。

---

### Task 1: 依赖、config 扩展与 cache.rs

**Files:**
- Modify: `Cargo.toml`、`src/config.rs`
- Create: `src/cache.rs`
- Modify: `src/lib.rs`（`pub mod cache;`）

**Interfaces:**
- Consumes: `hickory_proto::op::Message`。
- Produces:
  - `ServerConfig` 增加 `#[serde(default = "default_query_timeout")] pub query_timeout_secs: u64`（默认 10）。
  - `#[derive(Clone, PartialEq, Eq, Hash)] pub struct CacheKey { pub name: String /*规范化小写无尾点*/, pub qtype: u16 }`，`CacheKey::from_query(&Message) -> Option<CacheKey>`（无 question 返回 None；qtype 用 `u16::from(q.query_type())`）。
  - `pub struct Cache`：`pub fn new(max_entries: usize) -> Self`；
    `pub fn get(&self, key: &CacheKey, query_id: u16) -> Option<(Message, bool /*expired*/)>`（命中克隆存储报文并把 `metadata.id` 改写为 `query_id`；**不改动队列顺序**）；
    `pub fn put(&self, key: CacheKey, message: Message)`（内部算 TTL：answers 最小 TTL，空则 60s；`remove` 旧条目再 `insert` 队尾；超限 `pop_front`）；
    `pub fn len(&self) -> usize`、`pub fn is_empty(&self) -> bool`。
  - 只在 `message.metadata.response_code == NoError` 时由调用方决定 put（Cache 本身不判断——pipeline 判断）。

- [ ] **Step 1: 添加依赖**

```bash
cargo add hashlink
cargo add arc-swap
cargo add humantime
```
并把 `Cargo.toml` 中 hyper 的 features 扩为 `["client", "http1", "http2", "server"]`（`cargo add hyper --features client,http1,http2,server`）。

- [ ] **Step 2: config 扩展（含测试）**

`src/config.rs` `ServerConfig` 增加：

```rust
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: u64,
```

```rust
fn default_query_timeout() -> u64 {
    10
}
```

tests 追加：

```rust
    #[test]
    fn query_timeout_defaults_to_ten() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:5300"

            [[upstream]]
            type = "plain"
            addr = "1.1.1.1:53"
        "#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert_eq!(cfg.server.query_timeout_secs, 10);
    }
```

先跑 `cargo test --lib config` 确认新测试编译失败→实现→通过。

- [ ] **Step 3: cache.rs 失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::rdata::A;
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::str::FromStr;

    fn response(id: u16, name: &str, ttl: u32) -> Message {
        let mut m = Message::new(id, MessageType::Response, OpCode::Query);
        m.metadata.response_code = ResponseCode::NoError;
        let n = Name::from_str(name).unwrap();
        let mut q = Query::new();
        q.set_name(n.clone());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m.add_answer(Record::from_rdata(n, ttl, RData::A(A::new(1, 2, 3, 4))));
        m
    }

    fn key(name: &str) -> CacheKey {
        CacheKey { name: name.trim_end_matches('.').to_lowercase(), qtype: 1 }
    }

    #[test]
    fn hit_rewrites_id_and_preserves_order() {
        let cache = Cache::new(10);
        cache.put(key("example.com."), response(0x1111, "example.com.", 300));
        let (msg, expired) = cache.get(&key("example.com."), 0x9999).expect("hit");
        assert_eq!(msg.metadata.id, 0x9999, "cached id must be rewritten");
        assert!(!expired);
    }

    #[test]
    fn expired_entry_still_returned_marked_expired() {
        let cache = Cache::new(10);
        cache.put(key("stale.com."), response(1, "stale.com.", 0)); // TTL 0 → 立即过期
        let (_, expired) = cache.get(&key("stale.com."), 2).expect("optimistic hit");
        assert!(expired, "ttl 0 entry must be flagged expired but still returned");
    }

    #[test]
    fn fifo_eviction_ignores_reads() {
        let cache = Cache::new(2);
        cache.put(key("a.com."), response(1, "a.com.", 300));
        cache.put(key("b.com."), response(2, "b.com.", 300));
        // 读 a 多次——FIFO 下不得改变淘汰顺序
        for _ in 0..5 {
            cache.get(&key("a.com."), 7);
        }
        cache.put(key("c.com."), response(3, "c.com.", 300));
        assert!(cache.get(&key("a.com."), 7).is_none(), "a 最先插入，必须最先被逐出");
        assert!(cache.get(&key("b.com."), 7).is_some());
        assert!(cache.get(&key("c.com."), 7).is_some());
    }

    #[test]
    fn put_replaces_and_moves_to_tail() {
        let cache = Cache::new(2);
        cache.put(key("a.com."), response(1, "a.com.", 300));
        cache.put(key("b.com."), response(2, "b.com.", 300));
        cache.put(key("a.com."), response(9, "a.com.", 300)); // 刷新 a → 移到队尾
        cache.put(key("c.com."), response(3, "c.com.", 300)); // 应逐出 b（现最旧）
        assert!(cache.get(&key("b.com."), 7).is_none());
        assert!(cache.get(&key("a.com."), 7).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn missing_question_key_is_none() {
        let m = Message::new(5, MessageType::Query, OpCode::Query);
        assert!(CacheKey::from_query(&m).is_none());
    }
}
```

- [ ] **Step 4: 运行确认失败** — `cargo test --lib cache`，编译失败。

- [ ] **Step 5: 实现 cache.rs**

```rust
use hashlink::LinkedHashMap;
use hickory_proto::op::Message;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// 缓存键：规范化域名（小写、无尾点）+ qtype 数值。
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub name: String,
    pub qtype: u16,
}

impl CacheKey {
    pub fn from_query(query: &Message) -> Option<CacheKey> {
        let q = query.queries.first()?;
        Some(CacheKey {
            name: q.name().to_string().trim_end_matches('.').to_lowercase(),
            qtype: u16::from(q.query_type()),
        })
    }
}

struct CacheEntry {
    message: Message,
    expires_at: Instant,
}

/// 纯内存 FIFO 乐观缓存：命中即返回（过期也返回并标记）；
/// 读取不改变队列顺序；put 删旧插队尾；超限逐出最旧。
pub struct Cache {
    map: Mutex<LinkedHashMap<CacheKey, CacheEntry>>,
    max_entries: usize,
}

impl Cache {
    pub fn new(max_entries: usize) -> Self {
        Self { map: Mutex::new(LinkedHashMap::new()), max_entries: max_entries.max(1) }
    }

    /// 命中时克隆报文并把 id 改写为当前查询 id。bool 表示已过期（需后台刷新）。
    pub fn get(&self, key: &CacheKey, query_id: u16) -> Option<(Message, bool)> {
        let map = self.map.lock().ok()?;
        let entry = map.get(key)?;
        let mut msg = entry.message.clone();
        msg.metadata.id = query_id;
        let expired = Instant::now() >= entry.expires_at;
        Some((msg, expired))
    }

    /// TTL = answers 最小 TTL（无 answers 用 60s）。
    pub fn put(&self, key: CacheKey, message: Message) {
        let ttl = message
            .answers
            .iter()
            .map(|r| r.ttl())
            .min()
            .unwrap_or(60);
        let entry = CacheEntry {
            message,
            expires_at: Instant::now() + Duration::from_secs(u64::from(ttl)),
        };
        if let Ok(mut map) = self.map.lock() {
            map.remove(&key); // 旧条目移除，保证重新入队尾
            map.insert(key, entry);
            while map.len() > self.max_entries {
                map.pop_front();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.lock().map(|m| m.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
```

（`msg.queries`/`msg.answers`/`r.ttl()` 访问形态以安装版本适配，mirror 既有代码。）

- [ ] **Step 6: 注册 `pub mod cache;`，测试通过**（5 个 PASS + config 新测试），全量绿，clippy 零告警。

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/config.rs src/cache.rs src/lib.rs
git commit -m "feat: add FIFO optimistic in-memory cache and query timeout config"
```

---

### Task 2: hosts.rs

**Files:**
- Create: `src/hosts.rs`
- Modify: `src/lib.rs`（`pub mod hosts;`）

**Interfaces:**
- Consumes: `config::HostEntry`、hickory-proto。
- Produces:
  - `pub struct HostsMap`：`pub fn from_entries(entries: &[HostEntry]) -> Self`；
    `pub fn lookup(&self, query: &Message) -> Option<Message>` —— 命中（精确或 `*.` 通配）时构造应答：qtype A → 该名下 IPv4 记录；AAAA → IPv6 记录；命中域名但无对应族/其他 qtype → 空 answers NoError（NODATA）。未命中返回 None。TTL 固定 300。应答 id=查询 id、回显 question、`recursion_available=true`。
  - 匹配规范化：小写、去尾点。通配条目 `*.example.com` 匹配任意子域（`a.example.com`、`a.b.example.com`）但不匹配 `example.com` 本身。

- [ ] **Step 1: 失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HostEntry;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;

    fn entries() -> Vec<HostEntry> {
        vec![
            HostEntry {
                name: "router.local".into(),
                addrs: vec!["192.168.1.1".parse().unwrap(), "fd00::1".parse().unwrap()],
            },
            HostEntry { name: "*.lab.example".into(), addrs: vec!["10.0.0.7".parse().unwrap()] },
        ]
    }

    fn query(name: &str, qtype: RecordType) -> Message {
        let mut m = Message::new(0x1234, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str(name).unwrap());
        q.set_query_type(qtype);
        m.add_query(q);
        m
    }

    #[test]
    fn exact_a_and_aaaa() {
        let h = HostsMap::from_entries(&entries());
        let resp = h.lookup(&query("Router.Local.", RecordType::A)).expect("hit");
        assert_eq!(resp.metadata.id, 0x1234);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert_eq!(resp.answers.len(), 1, "only the v4 addr for A query");
        let resp6 = h.lookup(&query("router.local.", RecordType::AAAA)).expect("hit");
        assert_eq!(resp6.answers.len(), 1, "only the v6 addr for AAAA query");
    }

    #[test]
    fn wildcard_matches_subdomains_only() {
        let h = HostsMap::from_entries(&entries());
        assert!(h.lookup(&query("box.lab.example.", RecordType::A)).is_some());
        assert!(h.lookup(&query("a.b.lab.example.", RecordType::A)).is_some());
        assert!(h.lookup(&query("lab.example.", RecordType::A)).is_none(), "wildcard 不匹配基域");
    }

    #[test]
    fn hit_without_family_is_nodata() {
        let h = HostsMap::from_entries(&entries());
        let resp = h.lookup(&query("box.lab.example.", RecordType::AAAA)).expect("name hit");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert!(resp.answers.is_empty(), "no v6 for wildcard entry → NODATA");
    }

    #[test]
    fn miss_returns_none() {
        let h = HostsMap::from_entries(&entries());
        assert!(h.lookup(&query("nope.example.", RecordType::A)).is_none());
    }
}
```

- [ ] **Step 2: 确认失败** — `cargo test --lib hosts`。

- [ ] **Step 3: 实现**

```rust
use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA};
use hickory_proto::rr::{RData, Record, RecordType};
use std::collections::HashMap;
use std::net::IpAddr;

const HOSTS_TTL: u32 = 300;

/// 自定义 hosts：精确名 + `*.` 通配后缀，直接构造应答。
pub struct HostsMap {
    exact: HashMap<String, Vec<IpAddr>>,
    wildcard: HashMap<String, Vec<IpAddr>>, // key 为去掉 "*." 的基域
}

fn normalize(name: &str) -> String {
    name.trim_end_matches('.').to_lowercase()
}

impl HostsMap {
    pub fn from_entries(entries: &[crate::config::HostEntry]) -> Self {
        let mut exact = HashMap::new();
        let mut wildcard = HashMap::new();
        for e in entries {
            let name = normalize(&e.name);
            if let Some(base) = name.strip_prefix("*.") {
                wildcard.insert(base.to_string(), e.addrs.clone());
            } else {
                exact.insert(name, e.addrs.clone());
            }
        }
        Self { exact, wildcard }
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.wildcard.is_empty()
    }

    fn find(&self, name: &str) -> Option<&Vec<IpAddr>> {
        if let Some(v) = self.exact.get(name) {
            return Some(v);
        }
        // 通配：逐级剥离最左标签，剩余部分匹配基域（不匹配基域本身）
        let mut rest = name;
        while let Some(pos) = rest.find('.') {
            rest = &rest[pos + 1..];
            if let Some(v) = self.wildcard.get(rest) {
                return Some(v);
            }
        }
        None
    }

    pub fn lookup(&self, query: &Message) -> Option<Message> {
        let q = query.queries.first()?;
        let name = normalize(&q.name().to_string());
        let addrs = self.find(&name)?;

        let mut resp = Message::new(query.metadata.id, MessageType::Response, query.metadata.op_code);
        resp.metadata.response_code = ResponseCode::NoError;
        resp.metadata.recursion_desired = query.metadata.recursion_desired;
        resp.metadata.recursion_available = true;
        resp.add_query(q.clone());

        let owner = q.name().clone();
        match q.query_type() {
            RecordType::A => {
                for ip in addrs {
                    if let IpAddr::V4(v4) = ip {
                        resp.add_answer(Record::from_rdata(owner.clone(), HOSTS_TTL, RData::A(A(*v4))));
                    }
                }
            }
            RecordType::AAAA => {
                for ip in addrs {
                    if let IpAddr::V6(v6) = ip {
                        resp.add_answer(Record::from_rdata(owner.clone(), HOSTS_TTL, RData::AAAA(AAAA(*v6))));
                    }
                }
            }
            _ => {} // 命中名但非地址类查询 → NODATA
        }
        Some(resp)
    }
}
```

（`A(*v4)`/`A::new` 构造形态、`metadata.op_code` 访问以安装版本适配。）

- [ ] **Step 4: 注册、测试通过、clippy 干净、全量绿。**

- [ ] **Step 5: Commit**

```bash
git add src/hosts.rs src/lib.rs
git commit -m "feat: add hosts map with exact and wildcard matching"
```

---

### Task 3: filter.rs 规则解析与匹配

**Files:**
- Create: `src/filter.rs`
- Modify: `src/lib.rs`（`pub mod filter;`）

**Interfaces:**
- Consumes: hickory-proto、arc-swap。
- Produces:
  - `pub struct RuleSet { blocked: HashSet<String>, exceptions: HashSet<String> }`
  - `pub fn parse_rules(text: &str) -> RuleSet` —— 支持：
    - 注释/空行：`!`、`#` 开头或空 → 跳过（hosts 行内 `#` 后为注释）。
    - hosts 语法：`0.0.0.0 domain`、`127.0.0.1 domain`、`::  domain` 等「IP + 空白 + 域名」行 → blocked。
    - adblock 子集：`||domain^`（可带 `$...` 修饰符——忽略修饰符只取域名）→ blocked；`@@||domain^` → exceptions。
    - 纯域名行（domain-list 格式）→ blocked。
    - 其余无法识别的行（含 `/`、`*`、正则等复杂 adblock 规则）跳过。
  - `impl RuleSet { pub fn merge(&mut self, other: RuleSet); pub fn len(&self) -> (usize, usize); }`
  - `pub struct Filter`：`pub fn new(allowlist: &[String]) -> Self`（内部 `ArcSwap<RuleSet>` 初始为空 + 规范化 allowlist HashSet）；
    `pub fn store(&self, rules: RuleSet)`（热替换）；
    `pub fn is_blocked(&self, name: &str) -> bool` —— 规范化后后缀游走：任一后缀命中 allowlist 或 exceptions → false；否则任一后缀命中 blocked → true。
    `pub fn block_response(&self, query: &Message) -> Message` —— 按 Global Constraints 构造 A→0.0.0.0 / AAAA→:: / 其他→NODATA，TTL 300。

- [ ] **Step 1: 失败测试**

```rust
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
        assert!(matches!(a.answers[0].data(), RData::A(v) if v.0.is_unspecified()));
        let aaaa = f.block_response(&mk(RecordType::AAAA));
        assert!(matches!(aaaa.answers[0].data(), RData::AAAA(v) if v.0.is_unspecified()));
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
}
```

（`answers[0].data()` 字段/方法形态按安装版本适配——Task 8 计划二时确认过 `Record.data` 为公有字段，mirror bootstrap.rs。）

- [ ] **Step 2: 确认失败**；**Step 3: 实现**

```rust
use arc_swap::ArcSwap;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA};
use hickory_proto::rr::{RData, Record, RecordType};
use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

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
```

- [ ] **Step 4: 注册、5 测试 PASS、全量绿、clippy 干净。**

- [ ] **Step 5: Commit**

```bash
git add src/filter.rs src/lib.rs
git commit -m "feat: add adblock/hosts rule parsing with hot-swappable filter"
```

---

### Task 4: fetch.rs 远程拉取与定时更新

**Files:**
- Create: `src/fetch.rs`
- Modify: `src/filter.rs`（追加 `load_sources` 与 `spawn_updater`）
- Modify: `src/lib.rs`（`pub mod fetch;`）

**Interfaces:**
- Consumes: `bootstrap::Bootstrap`、`tls::client_config`、hyper http1。
- Produces:
  - `pub async fn fetch_url(url: &str, bootstrap: &Bootstrap) -> anyhow::Result<Vec<u8>>` —— https GET：host 为 IP 字面量直接连，否则 `bootstrap.resolve_ips`（bootstrap 为空则 bail）；TLS ALPN `http/1.1`；hyper `http1` handshake；跟随 ≤3 次 3xx `Location` 跳转（相对路径基于当前 url 解析）；体积上限 20 MiB（超限 bail）；仅 2xx 成功。http:// 明文 URL 也支持（跳过 TLS）——规则源常见。
  - `filter.rs` 追加：
    - `pub async fn load_sources(sources: &[crate::config::RuleSource], bootstrap: &Bootstrap) -> RuleSet` —— 逐源：`path` 读文件 / `url` fetch_url；解析并 merge；单源失败 `warn` 跳过。
    - `pub fn spawn_updater(filter: Arc<Filter>, sources: Vec<crate::config::RuleSource>, bootstrap: Arc<Bootstrap>)` —— 对有 `update_interval` 的 url 源：`humantime::parse_duration` 解析周期（非法则 warn 跳过），spawn `tokio::time::interval` 循环重拉**全部源**并 `filter.store`（保持语义简单：任一周期到点即整体重建；无定时源则不 spawn）。取所有合法周期的最小值作为循环周期。
- 测试：本地 mock HTTP/1.1 明文 server（tokio TcpListener 手写响应）——覆盖 200 拉取、302 跳转、非 2xx 报错；`load_sources` 本地文件 + mock url 混合。HTTPS 路径经 rcgen mock（可选，authorized 简化为明文覆盖——TLS 栈已在 DoT/DoH 测过）。

- [ ] **Step 1: 失败测试**

`src/fetch.rs`：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::Bootstrap;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 极简 HTTP/1.1 明文 server：按路径返回 200 内容 / 302 跳转 / 404。
    async fn spawn_http_server(body: &'static str) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
                let resp = if path == "/rules.txt" {
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if path == "/redirect" {
                    "HTTP/1.1 302 Found\r\nlocation: /rules.txt\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string()
                } else {
                    "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string()
                };
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        addr
    }

    fn empty_bootstrap() -> Bootstrap {
        Bootstrap::from_config(&[]).unwrap()
    }

    #[tokio::test]
    async fn fetches_plain_http_by_ip() {
        let addr = spawn_http_server("||fetched.example^\n").await;
        let url = format!("http://{addr}/rules.txt");
        let body = fetch_url(&url, &empty_bootstrap()).await.expect("fetch");
        assert_eq!(String::from_utf8_lossy(&body), "||fetched.example^\n");
    }

    #[tokio::test]
    async fn follows_redirect() {
        let addr = spawn_http_server("redirected-content\n").await;
        let url = format!("http://{addr}/redirect");
        let body = fetch_url(&url, &empty_bootstrap()).await.expect("fetch via redirect");
        assert_eq!(String::from_utf8_lossy(&body), "redirected-content\n");
    }

    #[tokio::test]
    async fn non_2xx_is_error() {
        let addr = spawn_http_server("x").await;
        let url = format!("http://{addr}/missing");
        assert!(fetch_url(&url, &empty_bootstrap()).await.is_err());
    }
}
```

`src/filter.rs` tests 追加：

```rust
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
```

（需要把 fetch.rs 的 `mod tests` 设为 `pub(crate)` 并把 `spawn_http_server` 设为 `pub(crate)`，mirror doh3 的先例。）

- [ ] **Step 2: 确认失败**；**Step 3: 实现 fetch.rs**

```rust
use anyhow::{bail, Context, Result};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use rustls_pki_types::ServerName;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::bootstrap::Bootstrap;

const MAX_BODY: usize = 20 * 1024 * 1024;
const MAX_REDIRECTS: usize = 3;

/// 拉取规则文件：支持 http/https，域名经 bootstrap 解析，≤3 次跳转，20MiB 上限。
pub async fn fetch_url(url: &str, bootstrap: &Bootstrap) -> Result<Vec<u8>> {
    let mut current = url.to_string();
    for _ in 0..=MAX_REDIRECTS {
        let uri: http::Uri = current.parse().with_context(|| format!("invalid url {current}"))?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => bail!("unsupported scheme in {current}"),
        };
        let host = uri.host().context("url missing host")?.to_string();
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let ips: Vec<IpAddr> = match host.parse::<IpAddr>() {
            Ok(ip) => vec![ip],
            Err(_) => {
                if bootstrap.is_empty() {
                    bail!("cannot resolve {host}: no bootstrap configured");
                }
                bootstrap.resolve_ips(&host).await?
            }
        };

        let mut last_err: Option<anyhow::Error> = None;
        let mut tcp = None;
        for ip in &ips {
            match TcpStream::connect((*ip, port)).await {
                Ok(s) => {
                    tcp = Some(s);
                    break;
                }
                Err(e) => last_err = Some(e.into()),
            }
        }
        let tcp = tcp.ok_or_else(|| {
            last_err.unwrap_or_else(|| anyhow::anyhow!("no ips for {host}"))
        })?;

        let path = if uri.path().is_empty() { "/".to_string() } else {
            uri.path_and_query().map(|pq| pq.to_string()).unwrap_or_else(|| "/".into())
        };
        let req = http::Request::builder()
            .method(http::Method::GET)
            .uri(&path)
            .header(http::header::HOST, &host)
            .header(http::header::USER_AGENT, "dnsbuffer/0.1")
            .header(http::header::CONNECTION, "close")
            .body(Empty::<Bytes>::new())
            .context("building request")?;

        let resp = if https {
            let tls_cfg = Arc::new(crate::tls::client_config(&[b"http/1.1"], &[], None)?);
            let sn = ServerName::try_from(host.clone()).context("invalid server name")?;
            let tls = TlsConnector::from(tls_cfg).connect(sn, tcp).await.context("TLS handshake")?;
            let (mut sender, conn) =
                hyper::client::conn::http1::handshake(TokioIo::new(tls)).await.context("h1 handshake")?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            sender.send_request(req).await.context("sending request")?
        } else {
            let (mut sender, conn) =
                hyper::client::conn::http1::handshake(TokioIo::new(tcp)).await.context("h1 handshake")?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            sender.send_request(req).await.context("sending request")?
        };

        let status = resp.status();
        if status.is_redirection() {
            let loc = resp
                .headers()
                .get(http::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .context("redirect without location")?;
            current = if loc.starts_with("http://") || loc.starts_with("https://") {
                loc.to_string()
            } else {
                // 相对跳转：同 scheme/host/port
                let scheme = if https { "https" } else { "http" };
                format!("{scheme}://{host}:{port}{loc}")
            };
            continue;
        }
        if !status.is_success() {
            bail!("{current} returned {status}");
        }

        let mut body = Vec::new();
        let mut stream = resp.into_body();
        while let Some(frame) = stream.frame().await {
            let frame = frame.context("reading body")?;
            if let Some(chunk) = frame.data_ref() {
                if body.len() + chunk.len() > MAX_BODY {
                    bail!("{current} exceeds {MAX_BODY} byte limit");
                }
                body.extend_from_slice(chunk);
            }
        }
        return Ok(body);
    }
    bail!("too many redirects fetching {url}")
}
```

**Step 3b: filter.rs 追加 load_sources / spawn_updater**

```rust
use crate::bootstrap::Bootstrap;
use crate::config::RuleSource;
use std::time::Duration;

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
```

- [ ] **Step 4: 注册 `pub mod fetch;`、全部测试 PASS、clippy 干净。**

- [ ] **Step 5: Commit**

```bash
git add src/fetch.rs src/filter.rs src/lib.rs
git commit -m "feat: add rule fetching over http(s) with periodic hot-swap updates"
```

---

### Task 5: ecs.rs EDNS 客户端子网

**Files:**
- Create: `src/ecs.rs`
- Modify: `src/lib.rs`（`pub mod ecs;`）

**Interfaces:**
- Consumes: `config::{EcsConfig, EcsMode}`、hickory-proto EDNS API。
- Produces:
  - `#[derive(Clone, Copy, Debug, PartialEq)] pub struct EcsSubnet { pub addr: IpAddr, pub prefix: u8 }`
  - `pub fn mask_ip(ip: IpAddr, v4_prefix: u8, v6_prefix: u8) -> EcsSubnet` —— 把 IP 掩码到 /24（v4）、/56（v6）——纯函数可测。
  - `pub fn parse_subnet(s: &str) -> anyhow::Result<EcsSubnet>` —— 解析 `"1.2.3.0/24"` 形式；前缀超界 bail。
  - `pub async fn detect_egress() -> anyhow::Result<IpAddr>` —— UDP connect 到 `8.8.8.8:53` 取 `local_addr`（不发包）；失败尝试 v6 `[2001:4860:4860::8888]:53`。
  - `pub fn is_global(ip: &IpAddr) -> bool` —— 排除 loopback/私有（10/8、172.16/12、192.168/16）/链路本地/ULA(fc00::/7)。
  - `pub async fn subnet_from_config(cfg: &EcsConfig) -> Option<EcsSubnet>` —— `Disabled`→None；`Fixed`→parse_subnet（非法 warn→None）；`Auto`→detect_egress 掩码（失败或非公网 warn→None）。
  - `pub fn inject(query: &mut Message, subnet: &EcsSubnet)` —— 写入 EDNS OPT 的 ECS option（`EdnsOption::Subnet(ClientSubnet)`；scope_prefix=0）。已有 EDNS 则在其上加 option，没有则创建（udp payload size 1232）。hickory-proto 0.26 的 `Edns`/`extensions` 访问形态以安装版本为准适配（授权偏差）。

- [ ] **Step 1: 失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::str::FromStr;

    #[test]
    fn masks_v4_to_24_and_v6_to_56() {
        let v4 = mask_ip(IpAddr::from_str("203.0.113.77").unwrap(), 24, 56);
        assert_eq!(v4.addr, IpAddr::from_str("203.0.113.0").unwrap());
        assert_eq!(v4.prefix, 24);
        let v6 = mask_ip(IpAddr::from_str("2001:db8:aaaa:bbcc:1:2:3:4").unwrap(), 24, 56);
        assert_eq!(v6.addr, IpAddr::from_str("2001:db8:aaaa:bb00::").unwrap());
        assert_eq!(v6.prefix, 56);
    }

    #[test]
    fn parses_and_rejects_subnets() {
        let s = parse_subnet("198.51.100.0/24").unwrap();
        assert_eq!(s.prefix, 24);
        assert!(parse_subnet("198.51.100.0/33").is_err());
        assert!(parse_subnet("not-a-subnet").is_err());
        assert!(parse_subnet("2001:db8::/129").is_err());
    }

    #[test]
    fn global_detection() {
        assert!(is_global(&IpAddr::from_str("203.0.113.1").unwrap()));
        for private in ["10.0.0.1", "172.16.5.5", "192.168.1.1", "127.0.0.1", "fe80::1", "fd00::1"] {
            assert!(!is_global(&IpAddr::from_str(private).unwrap()), "{private} is not global");
        }
    }

    #[test]
    fn inject_adds_ecs_option() {
        use hickory_proto::op::{Message, MessageType, OpCode, Query};
        use hickory_proto::rr::{Name, RecordType};
        let mut m = Message::new(1, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        let subnet = parse_subnet("203.0.113.0/24").unwrap();
        inject(&mut m, &subnet);
        // 往返编解码后 ECS 选项仍在（证明 wire 层真实生效）
        let bytes = m.to_vec().unwrap();
        let decoded = hickory_proto::op::Message::from_vec(&bytes).unwrap();
        let edns = decoded.extensions().as_ref().expect("edns present");
        assert!(
            edns.option(hickory_proto::rr::rdata::opt::EdnsCode::Subnet).is_some(),
            "ECS option must survive encode/decode"
        );
    }
}
```

（`extensions()`/`option(EdnsCode::Subnet)` 访问形态按安装版本适配。）

- [ ] **Step 2: 确认失败**；**Step 3: 实现**

```rust
use anyhow::{bail, Context, Result};
use hickory_proto::op::Message;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::config::{EcsConfig, EcsMode};

/// ECS 子网：地址已按前缀掩码归零。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EcsSubnet {
    pub addr: IpAddr,
    pub prefix: u8,
}

pub fn mask_ip(ip: IpAddr, v4_prefix: u8, v6_prefix: u8) -> EcsSubnet {
    match ip {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let mask = if v4_prefix == 0 { 0 } else { u32::MAX << (32 - v4_prefix) };
            EcsSubnet { addr: IpAddr::V4(Ipv4Addr::from(bits & mask)), prefix: v4_prefix }
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let mask = if v6_prefix == 0 { 0 } else { u128::MAX << (128 - v6_prefix) };
            EcsSubnet { addr: IpAddr::V6(Ipv6Addr::from(bits & mask)), prefix: v6_prefix }
        }
    }
}

pub fn parse_subnet(s: &str) -> Result<EcsSubnet> {
    let (addr, prefix) = s.split_once('/').with_context(|| format!("invalid subnet {s}"))?;
    let addr: IpAddr = addr.parse().with_context(|| format!("invalid subnet addr {s}"))?;
    let prefix: u8 = prefix.parse().with_context(|| format!("invalid prefix {s}"))?;
    let max = if addr.is_ipv4() { 32 } else { 128 };
    if prefix > max {
        bail!("prefix /{prefix} out of range for {s}");
    }
    Ok(mask_ip(addr, if addr.is_ipv4() { prefix } else { 24 }, if addr.is_ipv4() { 56 } else { prefix }))
}

pub fn is_global(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified())
        }
        IpAddr::V6(v6) => {
            let seg0 = v6.segments()[0];
            !(v6.is_loopback()
                || v6.is_unspecified()
                || (seg0 & 0xffc0) == 0xfe80 // 链路本地
                || (seg0 & 0xfe00) == 0xfc00) // ULA fc00::/7
        }
    }
}

/// UDP connect 不发包即可取出口地址。
pub async fn detect_egress() -> Result<IpAddr> {
    for target in ["8.8.8.8:53", "[2001:4860:4860::8888]:53"] {
        let bind = if target.starts_with('[') { "[::]:0" } else { "0.0.0.0:0" };
        if let Ok(sock) = tokio::net::UdpSocket::bind(bind).await {
            if sock.connect(target).await.is_ok() {
                if let Ok(local) = sock.local_addr() {
                    return Ok(local.ip());
                }
            }
        }
    }
    bail!("cannot detect egress ip")
}

pub async fn subnet_from_config(cfg: &EcsConfig) -> Option<EcsSubnet> {
    match cfg.mode {
        EcsMode::Disabled => None,
        EcsMode::Fixed => match parse_subnet(&cfg.fixed_subnet) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!("invalid ecs.fixed_subnet, ECS disabled: {e:#}");
                None
            }
        },
        EcsMode::Auto => match detect_egress().await {
            Ok(ip) if is_global(&ip) => Some(mask_ip(ip, 24, 56)),
            Ok(ip) => {
                tracing::warn!("egress ip {ip} is not global, ECS disabled");
                None
            }
            Err(e) => {
                tracing::warn!("egress detection failed, ECS disabled: {e:#}");
                None
            }
        },
    }
}

/// 注入 ECS：已有 EDNS 则追加 option，否则创建。scope_prefix=0。
pub fn inject(query: &mut Message, subnet: &EcsSubnet) {
    use hickory_proto::rr::rdata::opt::{ClientSubnet, EdnsOption};
    let ecs = ClientSubnet::new(subnet.addr, subnet.prefix, 0);
    let edns = query.extensions_mut().get_or_insert_with(Default::default);
    edns.set_max_payload(1232);
    edns.options_mut().insert(EdnsOption::Subnet(ecs));
}
```

（`ClientSubnet::new` 参数序、`extensions_mut()`、`options_mut().insert` 形态以安装版本为准适配——`parse_subnet` 里 mask 参数的传法要保证 v4 用 `prefix`、v6 用 `prefix`，参考实现如上有意把非当前族的参数传默认值。若觉得绕，实现为按族分派更直白的版本也可，行为以测试为准。）

- [ ] **Step 4: 注册、4 测试 PASS、全量绿、clippy 干净。**

- [ ] **Step 5: Commit**

```bash
git add src/ecs.rs src/lib.rs
git commit -m "feat: add EDNS client subnet with fixed/auto modes"
```

---

### Task 6: Pipeline 编排重写与装配

**Files:**
- Modify: `src/pipeline.rs`（编排器重写）
- Modify: `src/lib.rs`（`build_pipeline` 装配全链）
- Modify: `tests/forwarding.rs`（新增 hosts/屏蔽/缓存端到端测试）
- Modify: `config.example.toml`

**Interfaces:**
- Consumes: 前 5 个 Task 的全部 Produces + 计划二的上游链。
- Produces:
  - `pub struct Pipeline`（新签名）：
    ```rust
    pub struct PipelineParts {
        pub hosts: crate::hosts::HostsMap,
        pub filter: std::sync::Arc<crate::filter::Filter>,
        pub cache: std::sync::Arc<crate::cache::Cache>,
        pub upstream: std::sync::Arc<dyn crate::resolver::Resolver>,
        pub ecs: Option<crate::ecs::EcsSubnet>,
        pub query_timeout: std::time::Duration,
    }
    impl Pipeline {
        pub fn new(parts: PipelineParts) -> Self;
        pub async fn handle(&self, query: &Message) -> Message;
    }
    ```
  - `handle` 顺序：
    1. 无 question → servfail。
    2. `hosts.lookup` 命中 → 返回。
    3. qname 被 `filter.is_blocked` → `filter.block_response`。
    4. `CacheKey::from_query` + `cache.get` 命中 → 若 expired：spawn 后台刷新（克隆 `Arc<Cache>`、`Arc<dyn Resolver>`、query，克隆内做 ECS 注入 + resolve + `NoError` 时 put）→ 返回缓存值。
    5. 未命中：克隆 query，ECS 注入（如有），`tokio::time::timeout(query_timeout, upstream.resolve(&q))`；成功且 `NoError` → `cache.put`；成功任意 rcode → 返回（id 对齐由上游 id 校验保证）；超时/错误 → warn + servfail。
  - `build_pipeline`：构建顺序——bootstrap → filter（`Filter::new(allowlist)` + 首次 `load_sources` + `spawn_updater`）→ hosts → cache（`config.cache.max_entries`）→ ECS（`subnet_from_config`）→ 上游链（沿用计划二 build_group/fallback）→ `Pipeline::new`。
- 集成测试新增（`tests/forwarding.rs`，复用既有 mock/query 辅助）：
  - `hosts_entry_served_locally`：配置 `[[hosts]]` → 查询直接返回配置地址（mock 上游不应收到请求——mock 收到即 panic 的变体或计数校验）。
  - `blocked_domain_returns_zero_address`：配置本地规则文件（tempdir 写入 `0.0.0.0 blocked.test`）→ A 查询返回 `0.0.0.0`。
  - `cache_serves_second_query`：mock 上游计数；同一查询发两次 → 第二次命中缓存，上游计数仍为 1。

- [ ] **Step 1: 先写集成测试（RED）**

`tests/forwarding.rs` 追加（示意——具体 mock 计数辅助按文件现状实现）：

```rust
#[tokio::test]
async fn hosts_entry_served_locally() {
    let upstream = spawn_counting_upstream().await; // 返回 (addr, Arc<AtomicUsize>)
    let listen = free_udp_addr().await;
    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[hosts]]
        name = "printer.home"
        addrs = ["192.168.9.9"]

        [[upstream]]
        type = "plain"
        addr = "{}"
        "#,
        upstream.0
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg).await.unwrap();
    tokio::spawn(async move { server::run_udp(listen, pipeline).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = udp_query(listen, "printer.home.", RecordType::A).await;
    assert_eq!(resp.answers.len(), 1);
    assert_eq!(upstream.1.load(std::sync::atomic::Ordering::SeqCst), 0, "hosts 命中不得走上游");
}

#[tokio::test]
async fn blocked_domain_returns_zero_address() {
    let upstream = spawn_counting_upstream().await;
    let listen = free_udp_addr().await;
    let dir = std::env::temp_dir().join("dnsbuffer-e2e-rules");
    std::fs::create_dir_all(&dir).unwrap();
    let rules = dir.join("rules.txt");
    std::fs::write(&rules, "0.0.0.0 blocked.test\n").unwrap();
    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[adblock.rule_source]]
        path = "{}"

        [[upstream]]
        type = "plain"
        addr = "{}"
        "#,
        rules.display(),
        upstream.0
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg).await.unwrap();
    tokio::spawn(async move { server::run_udp(listen, pipeline).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = udp_query(listen, "blocked.test.", RecordType::A).await;
    assert_eq!(resp.answers.len(), 1);
    assert!(matches!(resp.answers[0].data(), RData::A(a) if a.0.is_unspecified()));
}

#[tokio::test]
async fn cache_serves_second_query() {
    let upstream = spawn_counting_upstream().await;
    let listen = free_udp_addr().await;
    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[upstream]]
        type = "plain"
        addr = "{}"
        "#,
        upstream.0
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg).await.unwrap();
    tokio::spawn(async move { server::run_udp(listen, pipeline).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _ = udp_query(listen, "cached.example.", RecordType::A).await;
    let resp2 = udp_query(listen, "cached.example.", RecordType::A).await;
    assert_eq!(resp2.metadata.response_code, ResponseCode::NoError);
    assert_eq!(upstream.1.load(std::sync::atomic::Ordering::SeqCst), 1, "第二次必须走缓存");
}
```

辅助 `spawn_counting_upstream`（计数 + 回带 300s TTL A 记录的 NoError）、`free_udp_addr`、`udp_query` 按文件既有风格提取/新建。**mock 必须在响应里带一条 TTL 300 的 A 记录**（缓存 TTL 逻辑依赖）。

- [ ] **Step 2: 单元级 pipeline 测试改写**

`src/pipeline.rs` 原有 2 个测试适配新构造（`PipelineParts` 全默认空：`HostsMap::from_entries(&[])`、`Filter::new(&[])`、`Cache::new(16)`、timeout 5s），行为断言不变；再加一个乐观刷新单元测试：

```rust
    #[tokio::test]
    async fn expired_cache_hit_triggers_background_refresh() {
        // CountingOk 返回 TTL 0 → put 后立即过期；第二次 handle 命中过期缓存
        // 返回旧值，同时后台刷新应再次调用上游（计数最终为 2）
        let counter = Arc::new(AtomicUsize::new(0));
        let resolver = Arc::new(CountingTtlZero(counter.clone()));
        let pipeline = Pipeline::new(PipelineParts {
            hosts: crate::hosts::HostsMap::from_entries(&[]),
            filter: Arc::new(crate::filter::Filter::new(&[])),
            cache: Arc::new(crate::cache::Cache::new(16)),
            upstream: resolver,
            ecs: None,
            query_timeout: Duration::from_secs(5),
        });
        let q = sample_query();
        let _ = pipeline.handle(&q).await; // 首查 → 上游 1 次 + 入缓存(TTL0)
        let resp = pipeline.handle(&q).await; // 过期命中 → 立即返回 + 后台刷新
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        tokio::time::sleep(Duration::from_millis(200)).await; // 等后台任务
        assert_eq!(counter.load(Ordering::SeqCst), 2, "后台刷新必须调用上游");
    }
```

（`CountingTtlZero`：计数并返回带 TTL 0 A 记录的 NoError 响应的测试 Resolver。）

- [ ] **Step 3: 实现 pipeline.rs 重写**

```rust
use std::sync::Arc;
use std::time::Duration;

use hickory_proto::op::{Message, ResponseCode};

use crate::cache::{Cache, CacheKey};
use crate::ecs::EcsSubnet;
use crate::filter::Filter;
use crate::hosts::HostsMap;
use crate::resolver::{servfail, Resolver};

pub struct PipelineParts {
    pub hosts: HostsMap,
    pub filter: Arc<Filter>,
    pub cache: Arc<Cache>,
    pub upstream: Arc<dyn Resolver>,
    pub ecs: Option<EcsSubnet>,
    pub query_timeout: Duration,
}

/// 查询编排：hosts → filter → cache(乐观) → ECS 注入 → 上游链 → SERVFAIL。
pub struct Pipeline {
    hosts: HostsMap,
    filter: Arc<Filter>,
    cache: Arc<Cache>,
    upstream: Arc<dyn Resolver>,
    ecs: Option<EcsSubnet>,
    query_timeout: Duration,
}

impl Pipeline {
    pub fn new(parts: PipelineParts) -> Self {
        Self {
            hosts: parts.hosts,
            filter: parts.filter,
            cache: parts.cache,
            upstream: parts.upstream,
            ecs: parts.ecs,
            query_timeout: parts.query_timeout,
        }
    }

    fn prepared_query(&self, query: &Message) -> Message {
        let mut q = query.clone();
        if let Some(subnet) = &self.ecs {
            crate::ecs::inject(&mut q, subnet);
        }
        q
    }

    async fn resolve_upstream(&self, query: &Message) -> anyhow::Result<Message> {
        let q = self.prepared_query(query);
        tokio::time::timeout(self.query_timeout, self.upstream.resolve(&q))
            .await
            .map_err(|_| anyhow::anyhow!("query timed out after {:?}", self.query_timeout))?
    }

    pub async fn handle(&self, query: &Message) -> Message {
        let Some(q) = query.queries.first() else {
            return servfail(query);
        };
        let qname = q.name().to_string();

        // 1. hosts
        if let Some(resp) = self.hosts.lookup(query) {
            return resp;
        }
        // 2. 广告屏蔽
        if self.filter.is_blocked(&qname) {
            return self.filter.block_response(query);
        }
        // 3. 乐观缓存
        let key = CacheKey::from_query(query);
        if let Some(key) = &key {
            if let Some((cached, expired)) = self.cache.get(key, query.metadata.id) {
                if expired {
                    self.spawn_refresh(key.clone(), query.clone());
                }
                return cached;
            }
        }
        // 4. 上游
        match self.resolve_upstream(query).await {
            Ok(resp) => {
                if resp.metadata.response_code == ResponseCode::NoError {
                    if let Some(key) = key {
                        self.cache.put(key, resp.clone());
                    }
                }
                resp
            }
            Err(e) => {
                tracing::warn!("resolve failed: {e:#}");
                servfail(query)
            }
        }
    }

    /// 过期命中后的后台刷新：拿新结果替换缓存（删旧入队尾）。
    fn spawn_refresh(&self, key: CacheKey, query: Message) {
        let cache = self.cache.clone();
        let upstream = self.upstream.clone();
        let ecs = self.ecs;
        let timeout = self.query_timeout;
        tokio::spawn(async move {
            let mut q = query;
            if let Some(subnet) = &ecs {
                crate::ecs::inject(&mut q, subnet);
            }
            match tokio::time::timeout(timeout, upstream.resolve(&q)).await {
                Ok(Ok(resp)) if resp.metadata.response_code == ResponseCode::NoError => {
                    cache.put(key, resp);
                    tracing::debug!("cache refreshed");
                }
                Ok(Ok(resp)) => {
                    tracing::debug!("refresh got rcode {:?}, keeping stale entry", resp.metadata.response_code);
                }
                Ok(Err(e)) => tracing::warn!("cache refresh failed: {e:#}"),
                Err(_) => tracing::warn!("cache refresh timed out"),
            }
        });
    }
}
```

- [ ] **Step 4: lib.rs 装配**

`build_pipeline` 末段替换（上游链构建逻辑不动）：

```rust
pub async fn build_pipeline(config: &Config) -> Result<Arc<Pipeline>> {
    let bootstrap = Arc::new(Bootstrap::from_config(&config.bootstrap.servers)?);

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
    let ecs = crate::ecs::subnet_from_config(&config.ecs).await;

    let primary = build_group(&config.upstream, config, &bootstrap).await?;
    let resolver: Arc<dyn Resolver> = if config.fallback.is_empty() {
        primary
    } else {
        let fb = build_group(&config.fallback, config, &bootstrap).await?;
        Arc::new(FallbackResolver::new(primary, fb))
    };

    Ok(Arc::new(Pipeline::new(crate::pipeline::PipelineParts {
        hosts,
        filter,
        cache,
        upstream: resolver,
        ecs,
        query_timeout: std::time::Duration::from_secs(config.server.query_timeout_secs),
    })))
}
```

（`RuleSource` 需要 `#[derive(Clone)]`——在 config.rs 补上。`build_group` 签名如需从 `&Bootstrap` 调整为 `&Arc<Bootstrap>` 做最小适配。）

- [ ] **Step 5: config.example.toml 更新**

```toml
[server]
listen = "0.0.0.0:53"
tcp = true
query_timeout_secs = 10

[cache]
max_entries = 10000

[ecs]
mode = "auto"            # auto | fixed | disabled
# fixed_subnet = "203.0.113.0/24"

[selector]
window = 32
k = 5.0

[adblock]
allowlist = ["allowed.example.com"]
block_response = "zero"

[[adblock.rule_source]]
url = "https://example.com/easylist.txt"
update_interval = "24h"

# [[adblock.rule_source]]
# path = "/etc/dnsbuffer/extra-rules.txt"

[[hosts]]
name = "router.local"
addrs = ["192.168.1.1"]

[[upstream]]
type = "doh"
url = "https://cloudflare-dns.com/dns-query"
http3 = true

[[upstream]]
type = "dot"
addr = "9.9.9.9:853"
domain = "dns.quad9.net"

[[bootstrap.server]]
type = "plain"
addr = "1.1.1.1:53"

[[fallback]]
type = "plain"
addr = "8.8.8.8:53"
```

- [ ] **Step 6: 全量测试 + clippy** — 全绿零告警（pipeline 单元 3 个 + 集成新增 3 个 + 既有全部）。

- [ ] **Step 7: Commit**

```bash
git add src/pipeline.rs src/lib.rs src/config.rs tests/forwarding.rs config.example.toml
git commit -m "feat: orchestrate hosts/filter/cache/ecs in pipeline with query timeout"
```

---

## Self-Review

**Spec coverage（本计划范围）**：spec 第 5 点（hosts 直接返回）→ Task 2/6；第 6 点（adblock/hosts 语法文件地址 + 豁免；文件地址支持 URL + 更新周期）→ Task 3/4/6；第 7 点（乐观缓存、限条数、FIFO、命中即回+后台刷新+删旧入队；内存实现）→ Task 1/6；第 8 点（ECS）→ Task 5/6；spec §7.5（规则源加载/热替换/失败降级/不落盘）→ Task 3/4；计划二 carry-over（单查询总超时预算）→ Task 1/6。SERVFAIL 不入缓存（终审提出的交互问题）→ Task 6 明确只缓存 NoError、刷新失败保留旧条目。

**Placeholder scan**：无 TBD；EDNS/hosts/filter 的不确定 API 均给出完整意图代码并授权按安装版本适配（既定工作模式）。

**Type consistency**：`CacheKey::from_query` 与 `Cache::{get,put,len}`（Task 1 定义，Task 6 使用）；`HostsMap::{from_entries,lookup}`（Task 2 定义，Task 6 使用）；`Filter::{new,store,is_blocked,block_response}` + `parse_rules` + `RuleSet::merge`（Task 3 定义，Task 4/6 使用）；`fetch_url(url, &Bootstrap)`（Task 4 定义/使用）；`EcsSubnet`/`inject`/`subnet_from_config`（Task 5 定义，Task 6 使用）；`PipelineParts`/`Pipeline::new(parts)`（Task 6 定义，lib.rs 使用）。
