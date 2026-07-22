# dnsbuffer 计划一：核心 UDP 转发链路（MVP）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付一个能监听 UDP:53、把 DNS 查询转发到配置的明文上游、返回应答的最简可用 DNS 代理，并建立后续计划所需的骨架（完整配置结构、`Resolver` trait、server、pipeline）。

**Architecture:** tokio 异步单进程。核心抽象是 `Resolver` trait（`async fn resolve(&Message) -> Result<Message>`）；`server` 收 UDP 包解码成 `Message`，交给 `pipeline` 编排（本计划仅调用一个上游），编码后回包。配置一次性定义为覆盖整份 spec 的完整结构，本计划只消费其中 `server` 与 `upstream` 的明文部分。

**Tech Stack:** Rust 2021 · tokio · hickory-proto（DNS 报文编解码）· serde + toml · anyhow/thiserror · async-trait · clap · tracing。

## Global Constraints

- 语言/边界：Rust，跨平台前台进程，不做守护进程化。
- DNS 报文编解码统一走 `hickory-proto` 的 `Message`（`Message::from_vec` / `Message::to_vec`），不引入其高层 resolver/client。
- 所有 fallible 路径返回 `anyhow::Result`，绝不 `unwrap()`/`panic!` 于运行时数据路径（测试代码除外）。
- 上游解析统一实现 `Resolver` trait：`async fn resolve(&self, query: &Message) -> anyhow::Result<Message>`。
- 配置格式 TOML；配置结构一次定义为覆盖 spec 第 11 节的完整字段，本计划只消费 `server`、`upstream`（明文）。
- 缓存不落盘（本计划不涉及缓存）。
- 频繁提交：每个 Task 末尾提交一次。

---

## File Structure

- `Cargo.toml` — 依赖清单。
- `src/main.rs` — 入口：解析命令行、加载配置、构建上游与 pipeline、启动 server。
- `src/config.rs` — 完整配置结构 + TOML 加载 + 校验。
- `src/resolver.rs` — `Resolver` trait 定义 + 构造 SERVFAIL/响应骨架的辅助函数。
- `src/upstream/mod.rs` — upstream 子模块声明；本计划仅 `plain`。
- `src/upstream/plain.rs` — `PlainResolver`：UDP 明文上游转发。
- `src/server.rs` — UDP 监听、收包、spawn、回包。
- `src/pipeline.rs` — `Pipeline`：查询编排（本计划只转发到单一上游，失败回 SERVFAIL）。
- `tests/forwarding.rs` — 端到端集成测试：mock 上游 + 起本代理 + 验证转发。
- `config.example.toml` — 示例配置。

---

### Task 1: 项目脚手架与依赖

**Files:**
- Create: `Cargo.toml`（由 `cargo init` 生成后补依赖）
- Create: `src/main.rs`
- Create: `.gitignore`

**Interfaces:**
- Consumes: 无（首个任务）。
- Produces: 可编译运行的空壳二进制；`tracing` 已初始化。后续任务在此基础上增补模块。

- [ ] **Step 1: 初始化 crate 与依赖**

Run:
```bash
cargo init --name dnsbuffer .
cargo add tokio --features full
cargo add hickory-proto
cargo add serde --features derive
cargo add toml
cargo add anyhow
cargo add thiserror
cargo add async-trait
cargo add clap --features derive
cargo add tracing
cargo add tracing-subscriber --features env-filter
```

- [ ] **Step 2: 写入最小 `src/main.rs`**

```rust
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    tracing::info!("dnsbuffer starting");
    Ok(())
}
```

- [ ] **Step 3: 写入 `.gitignore`**

```gitignore
/target
```

- [ ] **Step 4: 编译验证**

Run: `cargo build`
Expected: 编译成功，无错误（可能有未使用依赖的 warning，可忽略）。

- [ ] **Step 5: 运行验证**

Run: `cargo run`
Expected: 打印一行 `dnsbuffer starting` 后正常退出（exit code 0）。

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/main.rs .gitignore
git commit -m "chore: scaffold dnsbuffer crate with core deps"
```

---

### Task 2: 完整配置结构与加载

**Files:**
- Create: `src/config.rs`
- Modify: `src/main.rs`（声明 `mod config;`）
- Create: `config.example.toml`
- Test: `src/config.rs`（`#[cfg(test)]` 内联单元测试）

**Interfaces:**
- Consumes: 无运行期依赖。
- Produces:
  - `pub struct Config { pub server: ServerConfig, pub cache: CacheConfig, pub ecs: EcsConfig, pub adblock: AdblockConfig, pub hosts: Vec<HostEntry>, pub upstream: Vec<UpstreamConfig>, pub bootstrap: BootstrapConfig, pub fallback: Vec<UpstreamConfig> }`
  - `pub struct ServerConfig { pub listen: SocketAddr, pub tcp: bool }`
  - `pub enum UpstreamConfig { Plain { addr: SocketAddr }, Doh { url: String, ech: String, http3: bool }, Dot { addr: SocketAddr, domain: String, ips: Vec<IpAddr> } }`（`#[serde(tag = "type", rename_all = "lowercase")]`）
  - `pub fn load(path: &std::path::Path) -> anyhow::Result<Config>`
  - 校验：至少一个 `upstream`，否则返回错误。

- [ ] **Step 1: 写失败测试**

在 `src/config.rs` 末尾：

```rust
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib config`
Expected: 编译失败（`Config` 等未定义）。

- [ ] **Step 3: 写入配置结构实现**

在 `src/config.rs` 顶部：

```rust
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
```

- [ ] **Step 4: 声明模块**

在 `src/main.rs` 顶部 `use` 之后加：

```rust
mod config;
```

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test --lib config`
Expected: 两个测试 PASS。

- [ ] **Step 6: 写示例配置 `config.example.toml`**

```toml
[server]
listen = "0.0.0.0:53"
tcp = true

[[upstream]]
type = "plain"
addr = "1.1.1.1:53"
```

- [ ] **Step 7: Commit**

```bash
git add src/config.rs src/main.rs config.example.toml
git commit -m "feat: full config schema with TOML loader and validation"
```

---

### Task 3: Resolver trait 与响应辅助

**Files:**
- Create: `src/resolver.rs`
- Modify: `src/main.rs`（声明 `mod resolver;`）
- Test: `src/resolver.rs`（`#[cfg(test)]` 内联）

**Interfaces:**
- Consumes: `hickory_proto::op::Message`。
- Produces:
  - `#[async_trait::async_trait] pub trait Resolver: Send + Sync { async fn resolve(&self, query: &Message) -> anyhow::Result<Message>; }`
  - `pub fn servfail(query: &Message) -> Message`（构造与 query 同 id、回显 question、response code = ServFail 的响应）。

- [ ] **Step 1: 写失败测试**

在 `src/resolver.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;

    fn sample_query() -> Message {
        let mut m = Message::new();
        m.set_id(0x1234);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[test]
    fn servfail_preserves_id_and_question() {
        let q = sample_query();
        let resp = servfail(&q);
        assert_eq!(resp.id(), 0x1234);
        assert_eq!(resp.message_type(), MessageType::Response);
        assert_eq!(resp.response_code(), ResponseCode::ServFail);
        assert_eq!(resp.queries().len(), 1);
        assert_eq!(resp.queries()[0].name().to_string(), "example.com.");
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib resolver`
Expected: 编译失败（`servfail`/`Resolver` 未定义）。

- [ ] **Step 3: 写入实现**

在 `src/resolver.rs` 顶部：

```rust
use anyhow::Result;
use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};

/// 所有上游解析器（明文/DoH/DoT）实现的统一抽象。
#[async_trait]
pub trait Resolver: Send + Sync {
    async fn resolve(&self, query: &Message) -> Result<Message>;
}

/// 构造一个与请求同 id、回显问题段、响应码为 SERVFAIL 的响应报文。
pub fn servfail(query: &Message) -> Message {
    let mut resp = Message::new();
    resp.set_id(query.id());
    resp.set_message_type(MessageType::Response);
    resp.set_op_code(query.op_code());
    resp.set_recursion_desired(query.recursion_desired());
    resp.set_recursion_available(true);
    resp.set_response_code(ResponseCode::ServFail);
    for q in query.queries() {
        resp.add_query(q.clone());
    }
    resp
}
```

- [ ] **Step 4: 声明模块**

在 `src/main.rs` 顶部加：

```rust
mod resolver;
```

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test --lib resolver`
Expected: `servfail_preserves_id_and_question` PASS。

- [ ] **Step 6: Commit**

```bash
git add src/resolver.rs src/main.rs
git commit -m "feat: add Resolver trait and servfail helper"
```

---

### Task 4: PlainResolver（UDP 明文上游）

**Files:**
- Create: `src/upstream/mod.rs`
- Create: `src/upstream/plain.rs`
- Modify: `src/main.rs`（声明 `mod upstream;`）
- Test: `src/upstream/plain.rs`（`#[cfg(test)]` 内联，含 mock UDP 上游）

**Interfaces:**
- Consumes: `resolver::Resolver`、`hickory_proto::op::Message`。
- Produces:
  - `pub struct PlainResolver`
  - `impl PlainResolver { pub fn new(addr: SocketAddr) -> Self; pub fn with_timeout(addr: SocketAddr, timeout: Duration) -> Self }`
  - `impl Resolver for PlainResolver`（转发 query 字节到 `addr`，收应答解码返回）。

- [ ] **Step 1: 写 upstream 模块声明**

`src/upstream/mod.rs`：

```rust
pub mod plain;
```

- [ ] **Step 2: 写失败测试**

`src/upstream/plain.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
    use tokio::net::UdpSocket;

    /// 起一个只回固定 NOERROR 响应的 mock UDP DNS 上游，返回其地址。
    async fn spawn_mock_upstream() -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            let mut resp = Message::new();
            resp.set_id(query.id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            for q in query.queries() {
                resp.add_query(q.clone());
            }
            let bytes = resp.to_vec().unwrap();
            sock.send_to(&bytes, peer).await.unwrap();
        });
        addr
    }

    fn sample_query() -> Message {
        let mut m = Message::new();
        m.set_id(0x4242);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn forwards_and_returns_response() {
        let upstream_addr = spawn_mock_upstream().await;
        let resolver = PlainResolver::new(upstream_addr);
        let resp = resolver.resolve(&sample_query()).await.expect("resolve");
        assert_eq!(resp.id(), 0x4242);
        assert_eq!(resp.message_type(), MessageType::Response);
    }
}
```

- [ ] **Step 3: 运行测试确认失败**

Run: `cargo test --lib upstream::plain`
Expected: 编译失败（`PlainResolver` 未定义）。

- [ ] **Step 4: 写入实现**

`src/upstream/plain.rs` 顶部：

```rust
use anyhow::{Context, Result};
use async_trait::async_trait;
use hickory_proto::op::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::resolver::Resolver;

/// 明文 UDP 上游解析器：把查询原样转发到目标地址并读回应答。
/// 用于 bootstrap 的 IP 上游、fallback 的 IP 上游，以及本计划的转发链路验证。
pub struct PlainResolver {
    addr: SocketAddr,
    timeout: Duration,
}

impl PlainResolver {
    pub fn new(addr: SocketAddr) -> Self {
        Self::with_timeout(addr, Duration::from_secs(5))
    }

    pub fn with_timeout(addr: SocketAddr, timeout: Duration) -> Self {
        Self { addr, timeout }
    }
}

#[async_trait]
impl Resolver for PlainResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        let bytes = query.to_vec().context("encoding query")?;
        let bind = if self.addr.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        };
        let sock = UdpSocket::bind(bind).await.context("binding local socket")?;
        sock.connect(self.addr)
            .await
            .with_context(|| format!("connecting to upstream {}", self.addr))?;
        sock.send(&bytes).await.context("sending query")?;

        let mut buf = vec![0u8; 4096];
        let n = timeout(self.timeout, sock.recv(&mut buf))
            .await
            .with_context(|| format!("upstream {} timed out", self.addr))?
            .context("receiving response")?;
        let resp = Message::from_vec(&buf[..n]).context("decoding response")?;
        Ok(resp)
    }
}
```

- [ ] **Step 5: 声明模块**

在 `src/main.rs` 顶部加：

```rust
mod upstream;
```

- [ ] **Step 6: 运行测试确认通过**

Run: `cargo test --lib upstream::plain`
Expected: `forwards_and_returns_response` PASS。

- [ ] **Step 7: Commit**

```bash
git add src/upstream/mod.rs src/upstream/plain.rs src/main.rs
git commit -m "feat: add PlainResolver for UDP upstream forwarding"
```

---

### Task 5: Pipeline 骨架

**Files:**
- Create: `src/pipeline.rs`
- Modify: `src/main.rs`（声明 `mod pipeline;`）
- Test: `src/pipeline.rs`（`#[cfg(test)]` 内联，含成功与失败两条路径）

**Interfaces:**
- Consumes: `resolver::Resolver`、`resolver::servfail`、`hickory_proto::op::Message`。
- Produces:
  - `pub struct Pipeline`
  - `impl Pipeline { pub fn new(upstream: std::sync::Arc<dyn Resolver>) -> Self; pub async fn handle(&self, query: &Message) -> Message }`
  - 行为：调用上游；成功返回其响应；失败记 warn 并返回 `servfail(query)`（`handle` 不返回 Result，始终产出一个可回给客户端的报文）。

- [ ] **Step 1: 写失败测试**

`src/pipeline.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
    use std::sync::Arc;

    struct OkResolver;
    #[async_trait]
    impl Resolver for OkResolver {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            let mut resp = Message::new();
            resp.set_id(query.id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            Ok(resp)
        }
    }

    struct ErrResolver;
    #[async_trait]
    impl Resolver for ErrResolver {
        async fn resolve(&self, _query: &Message) -> Result<Message> {
            Err(anyhow!("upstream down"))
        }
    }

    fn sample_query() -> Message {
        let mut m = Message::new();
        m.set_id(0x7);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn returns_upstream_response_on_success() {
        let pipeline = Pipeline::new(Arc::new(OkResolver));
        let resp = pipeline.handle(&sample_query()).await;
        assert_eq!(resp.response_code(), ResponseCode::NoError);
        assert_eq!(resp.id(), 0x7);
    }

    #[tokio::test]
    async fn returns_servfail_on_upstream_error() {
        let pipeline = Pipeline::new(Arc::new(ErrResolver));
        let resp = pipeline.handle(&sample_query()).await;
        assert_eq!(resp.response_code(), ResponseCode::ServFail);
        assert_eq!(resp.id(), 0x7);
        assert_eq!(resp.queries().len(), 1);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib pipeline`
Expected: 编译失败（`Pipeline` 未定义）。

- [ ] **Step 3: 写入实现**

`src/pipeline.rs` 顶部：

```rust
use std::sync::Arc;

use hickory_proto::op::Message;

use crate::resolver::{servfail, Resolver};

/// 查询编排。本计划仅转发到单一上游；后续计划在此插入
/// hosts → filter → cache → 上游组 → fallback 的完整链路。
pub struct Pipeline {
    upstream: Arc<dyn Resolver>,
}

impl Pipeline {
    pub fn new(upstream: Arc<dyn Resolver>) -> Self {
        Self { upstream }
    }

    /// 处理单个查询，始终返回一个可回给客户端的响应报文。
    pub async fn handle(&self, query: &Message) -> Message {
        match self.upstream.resolve(query).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("upstream resolve failed: {e:#}");
                servfail(query)
            }
        }
    }
}
```

- [ ] **Step 4: 声明模块**

在 `src/main.rs` 顶部加：

```rust
mod pipeline;
```

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test --lib pipeline`
Expected: 两个测试 PASS。

- [ ] **Step 6: Commit**

```bash
git add src/pipeline.rs src/main.rs
git commit -m "feat: add Pipeline skeleton with servfail fallback"
```

---

### Task 6: UDP Server

**Files:**
- Create: `src/server.rs`
- Modify: `src/main.rs`（声明 `mod server;`）
- Test: `src/server.rs`（`#[cfg(test)]` 内联，起 server + 客户端往返）

**Interfaces:**
- Consumes: `pipeline::Pipeline`、`hickory_proto::op::Message`。
- Produces:
  - `pub async fn run_udp(listen: SocketAddr, pipeline: Arc<Pipeline>) -> anyhow::Result<()>`（永久循环；每个包解码→`pipeline.handle`→编码→回包；单包错误只记 warn 不退出循环）。

- [ ] **Step 1: 写失败测试**

`src/server.rs` 末尾：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use anyhow::Result;
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::net::UdpSocket;

    struct EchoOk;
    #[async_trait]
    impl Resolver for EchoOk {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            let mut resp = Message::new();
            resp.set_id(query.id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            for q in query.queries() {
                resp.add_query(q.clone());
            }
            Ok(resp)
        }
    }

    fn sample_query(id: u16) -> Message {
        let mut m = Message::new();
        m.set_id(id);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn serves_udp_query_end_to_end() {
        let pipeline = Arc::new(Pipeline::new(Arc::new(EchoOk)));
        // 绑定随机端口获取地址，再交给 run_udp。
        let listen: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let bound = UdpSocket::bind(listen).await.unwrap();
        let addr = bound.local_addr().unwrap();
        drop(bound); // 释放端口给 run_udp 重新绑定
        tokio::spawn(async move {
            run_udp(addr, pipeline).await.unwrap();
        });
        // 给 server 一点启动时间
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(addr).await.unwrap();
        client.send(&sample_query(0xABCD).to_vec().unwrap()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
            .await
            .expect("no timeout")
            .unwrap();
        let resp = Message::from_vec(&buf[..n]).unwrap();
        assert_eq!(resp.id(), 0xABCD);
        assert_eq!(resp.response_code(), ResponseCode::NoError);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib server`
Expected: 编译失败（`run_udp` 未定义）。

- [ ] **Step 3: 写入实现**

`src/server.rs` 顶部：

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use hickory_proto::op::Message;
use tokio::net::UdpSocket;

use crate::pipeline::Pipeline;

/// 监听 UDP 并服务 DNS 查询，直到进程退出。
pub async fn run_udp(listen: SocketAddr, pipeline: Arc<Pipeline>) -> Result<()> {
    let sock = Arc::new(
        UdpSocket::bind(listen)
            .await
            .with_context(|| format!("binding UDP {listen}"))?,
    );
    tracing::info!("listening on udp {listen}");

    let mut buf = vec![0u8; 4096];
    loop {
        let (n, peer) = sock.recv_from(&mut buf).await.context("recv_from")?;
        let data = buf[..n].to_vec();
        let sock = sock.clone();
        let pipeline = pipeline.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_packet(&data, peer, &sock, &pipeline).await {
                tracing::warn!("error handling packet from {peer}: {e:#}");
            }
        });
    }
}

async fn handle_packet(
    data: &[u8],
    peer: SocketAddr,
    sock: &UdpSocket,
    pipeline: &Pipeline,
) -> Result<()> {
    let query = Message::from_vec(data).context("decoding client query")?;
    let response = pipeline.handle(&query).await;
    let bytes = response.to_vec().context("encoding response")?;
    sock.send_to(&bytes, peer)
        .await
        .with_context(|| format!("sending response to {peer}"))?;
    Ok(())
}
```

- [ ] **Step 4: 声明模块**

在 `src/main.rs` 顶部加：

```rust
mod server;
```

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test --lib server`
Expected: `serves_udp_query_end_to_end` PASS。

- [ ] **Step 6: Commit**

```bash
git add src/server.rs src/main.rs
git commit -m "feat: add UDP server loop"
```

---

### Task 7: main 整合与端到端集成测试

**Files:**
- Modify: `src/main.rs`（命令行参数、装配上游与 pipeline、启动 server）
- Test: `tests/forwarding.rs`（黑盒集成：mock 上游 + 二进制内 API 起代理 + 客户端验证）

**Interfaces:**
- Consumes: `config::{load, Config, UpstreamConfig}`、`upstream::plain::PlainResolver`、`pipeline::Pipeline`、`server::run_udp`。
- Produces:
  - `src/main.rs` 完整装配：解析 `--config <path>`，加载配置，取第一个 `UpstreamConfig::Plain` 构造 `PlainResolver`，包进 `Pipeline`，`run_udp`。
  - 为集成测试暴露一个库入口：把模块导出为 crate 库（`src/lib.rs` 或在 `main.rs` 用 `pub mod`）——本任务采用新增 `src/lib.rs` 重导出模块，`main.rs` 依赖该库。

- [ ] **Step 1: 建立库 crate 以便集成测试复用**

创建 `src/lib.rs`：

```rust
pub mod config;
pub mod pipeline;
pub mod resolver;
pub mod server;
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
```

- [ ] **Step 2: 改写 `src/main.rs` 使用库并装配启动**

```rust
use std::path::PathBuf;

use anyhow::Result;
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let cfg = config::load(&args.config)?;
    let pipeline = build_pipeline(&cfg)?;
    tracing::info!("dnsbuffer starting");
    server::run_udp(cfg.server.listen, pipeline).await
}
```

注意：删除 `main.rs` 里旧的 `mod config;` 等声明（模块现由 `lib.rs` 承载）。

- [ ] **Step 3: 写端到端集成测试**

创建 `tests/forwarding.rs`：

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dnsbuffer::config::Config;
use dnsbuffer::{build_pipeline, server};
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::{Name, RecordType};
use std::str::FromStr;
use tokio::net::UdpSocket;

async fn spawn_mock_upstream() -> SocketAddr {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            let mut resp = Message::new();
            resp.set_id(query.id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            for q in query.queries() {
                resp.add_query(q.clone());
            }
            sock.send_to(&resp.to_vec().unwrap(), peer).await.unwrap();
        }
    });
    addr
}

fn query(id: u16) -> Message {
    let mut m = Message::new();
    m.set_id(id);
    let mut q = Query::new();
    q.set_name(Name::from_str("example.com.").unwrap());
    q.set_query_type(RecordType::A);
    m.add_query(q);
    m
}

#[tokio::test]
async fn proxy_forwards_to_upstream_and_replies() {
    let upstream = spawn_mock_upstream().await;

    // 取一个空闲端口给代理监听
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let listen = probe.local_addr().unwrap();
    drop(probe);

    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[upstream]]
        type = "plain"
        addr = "{upstream}"
        "#
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg).unwrap();

    tokio::spawn(async move {
        server::run_udp(listen, pipeline).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(listen).await.unwrap();
    client.send(&query(0x1111).to_vec().unwrap()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .expect("no timeout")
        .unwrap();
    let resp = Message::from_vec(&buf[..n]).unwrap();
    assert_eq!(resp.id(), 0x1111);
    assert_eq!(resp.response_code(), ResponseCode::NoError);
    let _ = Arc::new(()); // keep imports tidy
}
```

- [ ] **Step 4: 集成测试需要 toml 作为 dev-dependency**

Run:
```bash
cargo add --dev toml
```
（`toml` 已是普通依赖时此步确保测试 crate 也可用；若 `cargo add --dev` 报重复可跳过。）

- [ ] **Step 5: 运行全部测试确认通过**

Run: `cargo test`
Expected: 所有单元测试 + `proxy_forwards_to_upstream_and_replies` PASS。

- [ ] **Step 6: 手动冒烟（可选，非 root 用高端口）**

Run:
```bash
printf '[server]\nlisten = "127.0.0.1:5300"\n\n[[upstream]]\ntype = "plain"\naddr = "1.1.1.1:53"\n' > /tmp/dnsbuffer-smoke.toml
cargo run -- --config /tmp/dnsbuffer-smoke.toml &
sleep 1
dig @127.0.0.1 -p 5300 example.com +short
kill %1
```
Expected: `dig` 返回 example.com 的 A 记录（说明经代理转发到 1.1.1.1 成功）。

- [ ] **Step 7: Commit**

```bash
git add src/lib.rs src/main.rs tests/forwarding.rs Cargo.toml
git commit -m "feat: wire config-driven UDP proxy with end-to-end test"
```

---

## 后续计划（路线图，非本计划任务）

以下在计划一落地后，用 writing-plans 技能各自展开为同样粒度的独立计划文档：

- **计划二 — 加密上游与智能调度**：`upstream/doh.rs`（H3 优先/H2 回退/ECH）、`upstream/dot.rs`、`bootstrap.rs`、`stats.rs`、`upstream/selector.rs`（加权随机）、`upstream/mod.rs` 上游组、`fallback` 回退链；pipeline 从「单一上游」升级为「上游组 → fallback → SERVFAIL」。
- **计划三 — 本地增强**：`cache.rs`（`hashlink` 乐观缓存）、`hosts.rs`、`filter.rs`（adblock/hosts 语法 + 远程 URL 定时更新 + `arc_swap` 热替换）、`ecs.rs`；pipeline 插入 hosts → filter → cache 前置层。

---

## Self-Review

**Spec coverage（本计划范围）**：spec 第 1 点（UDP:53 监听）→ Task 6/7；配置分层（spec 第 11 节）→ Task 2 一次性定义完整结构；`Resolver` 抽象（支撑 spec 4/9 的多形态上游复用）→ Task 3；明文上游（bootstrap/fallback 的 IP 形态基础，spec 第 4/9 点）→ Task 4；回退到 SERVFAIL 的健壮性（spec 第 10 节错误处理）→ Task 5。spec 其余点（DoH/ECH/加权/缓存/hosts/filter/ECS/fallback）明确归入计划二、三，已在路线图登记，无遗漏。

**Placeholder scan**：无 TBD/TODO；每个代码步骤给出完整可编译代码与确切命令、预期输出。

**Type consistency**：`Resolver::resolve(&self, &Message) -> Result<Message>` 在 Task 3 定义，Task 4/5/7 一致引用；`Pipeline::new(Arc<dyn Resolver>)` 与 `handle(&self, &Message) -> Message` 在 Task 5 定义、Task 6/7 一致；`build_pipeline(&Config) -> Result<Arc<Pipeline>>`、`server::run_udp(SocketAddr, Arc<Pipeline>)` 命名前后统一；`UpstreamConfig::Plain { addr }` 在 Task 2 定义、Task 7 匹配一致。
