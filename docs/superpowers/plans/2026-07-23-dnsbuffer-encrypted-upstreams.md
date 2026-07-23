# dnsbuffer 计划二：加密上游与智能调度 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 dnsbuffer 加入 DoH（HTTP/3 优先、HTTP/2 回退、ECH）、DoT 上游，bootstrap 域名解析与 ECH 动态获取，滑动窗口统计驱动的加权随机上游选择，以及后备 DNS 回退链。

**Architecture:** 所有新上游实现既有 `Resolver` trait。`UpstreamGroup`（含 stats + selector）与 `FallbackResolver` 也实现 `Resolver`，通过组合嵌套接入 `Pipeline`（Pipeline 不变）。TLS 层（rustls，可选 ECH）由 DoT/DoH/H3 共用。`build_pipeline` 变为 async：经 bootstrap 解析 DoH 域名 IP 与 ECH 配置后装配上游组。

**Tech Stack:** 计划一栈 + rustls 0.23（aws-lc-rs，ECH）· tokio-rustls · webpki-roots · hyper 1.x + hyper-util + http-body-util（H2 DoH）· quinn + h3 + h3-quinn（H3 DoH）· base64 · rand · rcgen（dev，测试证书）。

## Global Constraints

- 继承计划一全部约束：anyhow::Result、运行时数据路径绝不 unwrap/panic（测试除外）、hickory-proto `Message` 编解码、模块注册进 `src/lib.rs`、每 Task 提交。
- hickory-proto 0.26 适配模式已确立：`msg.metadata.field = value` 直赋值、`Message::new(id, message_type, op_code)` 构造、`msg.queries` 字段读取——新代码 mirror `src/resolver.rs`/`src/upstream/plain.rs` 现有模式。
- **授权偏差**：rustls ECH、h3、quinn、hyper 1.x、rcgen 的 API 若与本计划代码样例不符，以安装版本（docs.rs）为准做最小适配，并在报告中注明适配点。计划代码是意图基准，不是逐字节合同。
- DoH wire：RFC 8484，POST，`content-type: application/dns-message`，仅接受 HTTP 200。
- DoT wire：RFC 7858，TCP + 2 字节大端长度前缀，TLS SNI = 配置 domain。
- ALPN：H2 用 `b"h2"`，H3 用 `b"h3"`。
- 所有网络 resolver 默认超时 5s，提供 `with_timeout` 定制；超时包裹整次尝试。
- 所有解析结果须校验响应 id 与请求 id 一致，不一致 bail。
- ECH 优先级：配置静态 base64 > bootstrap HTTPS 记录动态获取 > 无（普通 TLS + `tracing::warn!`）。
- H3 失败必须运行时回退 H2（`http3=true` 时），回退记 warn。

---

## File Structure

- Modify: `Cargo.toml` — 新依赖。
- Modify: `src/config.rs` — Doh 变体加 `ips`、`http3` 默认 true；`[selector]` 配置；校验扩展。
- Modify: `src/upstream/plain.rs` — 响应 id 校验（计划一 carry-over）。
- Create: `src/stats.rs` — 滑动窗口统计（失败率/平均延迟/权重）。
- Create: `src/upstream/selector.rs` — 加权随机抽取（纯函数，可确定性测试）。
- Create: `src/tls.rs` — rustls ClientConfig 构建（roots + ALPN + 可选 ECH + 测试用附加根证书）。
- Create: `src/upstream/dot.rs` — DoT 解析器。
- Create: `src/upstream/doh.rs` — DoH 解析器（H2 + H3 编排与回退）。
- Create: `src/upstream/doh3.rs` — H3 连接封装（quinn/h3）。
- Create: `src/bootstrap.rs` — 域名→IP 与 HTTPS 记录 ECH 获取。
- Create: `src/upstream/group.rs` — `UpstreamGroup` + `FallbackResolver`。
- Modify: `src/lib.rs` — async `build_pipeline` 重写为装配上游组/回退链。
- Modify: `src/main.rs` — `.await` 适配。
- Modify: `tests/forwarding.rs` — async 适配 + 组回退集成测试。

---

### Task 1: 依赖、配置扩展与 PlainResolver id 校验

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/config.rs`
- Modify: `src/upstream/plain.rs`

**Interfaces:**
- Consumes: 计划一的 `UpstreamConfig`、`Config`、`PlainResolver`。
- Produces:
  - `UpstreamConfig::Doh { url: String, ech: String, http3: bool, ips: Vec<IpAddr> }`（`ips` 默认空 = 走 bootstrap；`http3` 默认 **true**）。
  - `pub struct SelectorConfig { pub window: usize /*默认32*/, pub k: f64 /*默认5.0*/ }`，`Config` 增加 `#[serde(default)] pub selector: SelectorConfig`。
  - `Config::validate` 扩展：`Dot` 的 `ips` 允许空（addr 即 IP）；bootstrap 中的 `Doh`/`Dot` 条目 `ips` 必须非空（否则鸡生蛋）。
  - `PlainResolver::resolve` 校验响应 id。

- [ ] **Step 1: 添加依赖**

```bash
cargo add rustls
cargo add tokio-rustls
cargo add rustls-pki-types
cargo add webpki-roots
cargo add hyper --features client,http2
cargo add hyper-util --features tokio
cargo add http
cargo add http-body-util
cargo add bytes
cargo add quinn
cargo add h3
cargo add h3-quinn
cargo add base64
cargo add rand
cargo add --dev rcgen
```

若 quinn 与 rustls 的 crypto provider 冲突（编译报 ring/aws-lc-rs 二选一错误），将 quinn 调整为 `default-features = false, features = ["runtime-tokio", "rustls-aws-lc-rs", "log"]`，rustls 保持默认（aws-lc-rs）。这是授权适配。

- [ ] **Step 2: 写失败测试（config 扩展）**

在 `src/config.rs` 的 `#[cfg(test)] mod tests` 中追加：

```rust
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
            UpstreamConfig::Doh { url, ech, http3, ips } => {
                assert_eq!(url, "https://dns.example/dns-query");
                assert!(ech.is_empty());
                assert!(*http3, "http3 defaults to true (H3-first design)");
                assert!(ips.is_empty());
            }
            _ => panic!("expected doh upstream"),
        }
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
    fn bootstrap_doh_without_ips_rejected() {
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
        assert!(cfg.validate().is_err(), "bootstrap doh without ips must fail");
    }
```

- [ ] **Step 3: 运行确认失败**

Run: `cargo test --lib config`
Expected: 编译失败（`Doh` 无 `ips` 字段 / 无 `selector` 字段）。

- [ ] **Step 4: 实现 config 扩展**

`src/config.rs` 变更：

```rust
// Doh 变体替换为：
    Doh {
        url: String,
        #[serde(default)]
        ech: String,
        #[serde(default = "default_true")]
        http3: bool,
        #[serde(default)]
        ips: Vec<IpAddr>,
    },

// 新增（放在 CacheConfig 附近）：
#[derive(Debug, Deserialize)]
pub struct SelectorConfig {
    #[serde(default = "default_window")]
    pub window: usize,
    #[serde(default = "default_k")]
    pub k: f64,
}

impl Default for SelectorConfig {
    fn default() -> Self {
        Self { window: default_window(), k: default_k() }
    }
}

fn default_window() -> usize { 32 }
fn default_k() -> f64 { 5.0 }

// Config 结构体增加字段：
    #[serde(default)]
    pub selector: SelectorConfig,

// validate() 追加：
        for b in &self.bootstrap.servers {
            match b {
                UpstreamConfig::Doh { ips, url, .. } if ips.is_empty() => {
                    bail!("bootstrap doh {url} must specify ips (chicken-and-egg)");
                }
                UpstreamConfig::Dot { ips, addr, domain } => {
                    let _ = (ips, addr, domain); // addr 即 IP，无需 ips
                }
                _ => {}
            }
        }
```

- [ ] **Step 5: 写失败测试（plain id 校验）**

`src/upstream/plain.rs` tests 追加（mirror 现有 mock 模式；mock 回响应时故意改 id）：

```rust
    async fn spawn_bad_id_upstream() -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            let mut resp = /* mirror 现有响应构造模式 */
                make_response(&query);
            resp.metadata.id = query.metadata.id.wrapping_add(1); // 故意错 id
            sock.send_to(&resp.to_vec().unwrap(), peer).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn rejects_mismatched_response_id() {
        let addr = spawn_bad_id_upstream().await;
        let resolver = PlainResolver::with_timeout(addr, std::time::Duration::from_secs(2));
        let err = resolver.resolve(&sample_query()).await;
        assert!(err.is_err(), "mismatched id must be rejected");
    }
```

（`make_response` 为提取的小测试辅助函数：从现有 mock 里把「构造 NoError 响应」抽出来复用；若现有 mock 内联构造，顺手提取。）

- [ ] **Step 6: 运行确认失败**

Run: `cargo test --lib upstream::plain`
Expected: `rejects_mismatched_response_id` FAIL（当前实现不校验 id，返回 Ok）。

- [ ] **Step 7: 实现 id 校验**

`src/upstream/plain.rs` `resolve` 中解码后追加：

```rust
        let resp = Message::from_vec(&buf[..n]).context("decoding response")?;
        if resp.metadata.id != query.metadata.id {
            anyhow::bail!(
                "upstream {} response id {} does not match query id {}",
                self.addr, resp.metadata.id, query.metadata.id
            );
        }
        Ok(resp)
```

- [ ] **Step 8: 全量测试与提交**

Run: `cargo test`
Expected: 全部通过（原 8 + 新 4 = 12 上下）。

```bash
git add Cargo.toml Cargo.lock src/config.rs src/upstream/plain.rs
git commit -m "feat: add crypto/http deps, extend doh/selector config, validate response ids"
```

---

### Task 2: stats.rs 滑动窗口统计

**Files:**
- Create: `src/stats.rs`
- Modify: `src/lib.rs`（`pub mod stats;`）

**Interfaces:**
- Consumes: 无。
- Produces:
  - `pub struct UpstreamStats`
  - `pub fn new(window: usize) -> Self`
  - `pub fn record_success(&mut self, latency: std::time::Duration)`
  - `pub fn record_failure(&mut self)`
  - `pub fn failure_rate(&self) -> f64`（窗口内失败占比；空窗口 = 0.0）
  - `pub fn avg_latency_ms(&self) -> f64`（窗口内成功样本平均；无成功样本 = 100.0 冷启动中值）
  - `pub fn weight(&self, k: f64) -> f64` = `1.0 / ((avg_latency_ms + 1.0) * (1.0 + k * failure_rate))`

- [ ] **Step 1: 写失败测试**

`src/stats.rs`：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cold_start_gives_neutral_weight() {
        let s = UpstreamStats::new(8);
        assert_eq!(s.failure_rate(), 0.0);
        assert_eq!(s.avg_latency_ms(), 100.0);
        let w = s.weight(5.0);
        assert!((w - 1.0 / 101.0).abs() < 1e-9);
    }

    #[test]
    fn failures_lower_weight() {
        let mut good = UpstreamStats::new(8);
        let mut bad = UpstreamStats::new(8);
        for _ in 0..4 {
            good.record_success(Duration::from_millis(50));
            bad.record_success(Duration::from_millis(50));
        }
        for _ in 0..4 {
            bad.record_failure();
        }
        assert!(bad.failure_rate() > 0.4);
        assert!(good.weight(5.0) > bad.weight(5.0));
    }

    #[test]
    fn lower_latency_wins() {
        let mut fast = UpstreamStats::new(8);
        let mut slow = UpstreamStats::new(8);
        for _ in 0..8 {
            fast.record_success(Duration::from_millis(10));
            slow.record_success(Duration::from_millis(500));
        }
        assert!(fast.weight(5.0) > slow.weight(5.0));
    }

    #[test]
    fn window_evicts_oldest() {
        let mut s = UpstreamStats::new(4);
        for _ in 0..4 {
            s.record_failure();
        }
        assert_eq!(s.failure_rate(), 1.0);
        for _ in 0..4 {
            s.record_success(Duration::from_millis(10));
        }
        assert_eq!(s.failure_rate(), 0.0, "old failures evicted from window");
    }
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test --lib stats`，编译失败。

- [ ] **Step 3: 实现**

```rust
use std::collections::VecDeque;
use std::time::Duration;

/// 单个上游的滑动窗口统计：近 N 次调用的失败率与平均延迟，驱动加权随机选择。
pub struct UpstreamStats {
    window: usize,
    samples: VecDeque<Sample>,
}

#[derive(Clone, Copy)]
enum Sample {
    Success { latency_ms: f64 },
    Failure,
}

impl UpstreamStats {
    pub fn new(window: usize) -> Self {
        Self { window: window.max(1), samples: VecDeque::new() }
    }

    fn push(&mut self, s: Sample) {
        if self.samples.len() == self.window {
            self.samples.pop_front();
        }
        self.samples.push_back(s);
    }

    pub fn record_success(&mut self, latency: Duration) {
        self.push(Sample::Success { latency_ms: latency.as_secs_f64() * 1000.0 });
    }

    pub fn record_failure(&mut self) {
        self.push(Sample::Failure);
    }

    pub fn failure_rate(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let failures = self.samples.iter().filter(|s| matches!(s, Sample::Failure)).count();
        failures as f64 / self.samples.len() as f64
    }

    pub fn avg_latency_ms(&self) -> f64 {
        let (sum, n) = self.samples.iter().fold((0.0, 0u32), |(sum, n), s| match s {
            Sample::Success { latency_ms } => (sum + latency_ms, n + 1),
            Sample::Failure => (sum, n),
        });
        if n == 0 {
            100.0 // 冷启动中值
        } else {
            sum / n as f64
        }
    }

    /// w = 1 / ((t_avg_ms + ε) × (1 + k·f))，ε=1.0
    pub fn weight(&self, k: f64) -> f64 {
        1.0 / ((self.avg_latency_ms() + 1.0) * (1.0 + k * self.failure_rate()))
    }
}
```

- [ ] **Step 4: 注册 `pub mod stats;` 到 `src/lib.rs`，测试通过** — `cargo test --lib stats`，4 个 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/stats.rs src/lib.rs
git commit -m "feat: add sliding-window upstream stats with weight formula"
```

---

### Task 3: selector.rs 加权随机抽取

**Files:**
- Create: `src/upstream/selector.rs`
- Modify: `src/upstream/mod.rs`（`pub mod selector;`）

**Interfaces:**
- Consumes: 无（纯函数）。
- Produces:
  - `pub fn pick_weighted(weights: &[f64], roll: f64) -> Option<usize>` — `roll ∈ [0,1)` 由调用方提供（生产用 `rand::random`，测试传定值）。全零/空权重时均匀退化（`(roll * len) as usize`）；空切片返回 `None`。

- [ ] **Step 1: 写失败测试**

`src/upstream/selector.rs`：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_returns_none() {
        assert_eq!(pick_weighted(&[], 0.5), None);
    }

    #[test]
    fn single_always_picked() {
        assert_eq!(pick_weighted(&[0.7], 0.0), Some(0));
        assert_eq!(pick_weighted(&[0.7], 0.999), Some(0));
    }

    #[test]
    fn roll_lands_proportionally() {
        // weights [1.0, 3.0] → 边界 0.25
        let w = [1.0, 3.0];
        assert_eq!(pick_weighted(&w, 0.10), Some(0));
        assert_eq!(pick_weighted(&w, 0.24), Some(0));
        assert_eq!(pick_weighted(&w, 0.26), Some(1));
        assert_eq!(pick_weighted(&w, 0.90), Some(1));
    }

    #[test]
    fn zero_weights_degrade_to_uniform() {
        let w = [0.0, 0.0, 0.0, 0.0];
        assert_eq!(pick_weighted(&w, 0.0), Some(0));
        assert_eq!(pick_weighted(&w, 0.30), Some(1));
        assert_eq!(pick_weighted(&w, 0.99), Some(3));
    }

    #[test]
    fn statistical_bias_holds() {
        // 用固定步长扫 roll，验证高权重上游被选中次数显著更多
        let w = [1.0, 9.0];
        let mut counts = [0usize; 2];
        for i in 0..1000 {
            let roll = i as f64 / 1000.0;
            counts[pick_weighted(&w, roll).unwrap()] += 1;
        }
        assert!(counts[1] > counts[0] * 5, "9:1 权重应显著偏向索引 1: {counts:?}");
    }
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test --lib upstream::selector`，编译失败。

- [ ] **Step 3: 实现**

```rust
/// 按权重随机抽取索引。roll ∈ [0,1) 由调用方提供以便确定性测试。
/// 权重总和为 0（或全部非有限）时退化为均匀抽取；空切片返回 None。
pub fn pick_weighted(weights: &[f64], roll: f64) -> Option<usize> {
    if weights.is_empty() {
        return None;
    }
    let total: f64 = weights.iter().filter(|w| w.is_finite() && **w > 0.0).sum();
    if total <= 0.0 {
        let idx = ((roll * weights.len() as f64) as usize).min(weights.len() - 1);
        return Some(idx);
    }
    let target = roll * total;
    let mut acc = 0.0;
    for (i, w) in weights.iter().enumerate() {
        if w.is_finite() && *w > 0.0 {
            acc += w;
            if target < acc {
                return Some(i);
            }
        }
    }
    // 浮点边界兜底：返回最后一个正权重索引
    weights.iter().rposition(|w| w.is_finite() && *w > 0.0)
}
```

- [ ] **Step 4: 注册 `pub mod selector;` 到 `src/upstream/mod.rs`，测试通过** — 5 个 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/upstream/selector.rs src/upstream/mod.rs
git commit -m "feat: add deterministic weighted random selector"
```

---

### Task 4: tls.rs TLS 客户端配置（含 ECH）

**Files:**
- Create: `src/tls.rs`
- Modify: `src/lib.rs`（`pub mod tls;`）

**Interfaces:**
- Consumes: rustls、webpki-roots、rustls-pki-types。
- Produces:
  - `pub fn client_config(alpn: &[&[u8]], extra_roots: &[rustls_pki_types::CertificateDer<'static>], ech_config_list: Option<&[u8]>) -> anyhow::Result<rustls::ClientConfig>`
  - 行为：webpki 根 + extra_roots（测试注入自签根）；设置 `alpn_protocols`；`ech_config_list = Some` 时启用 rustls ECH（TLS1.3），`None` 时普通 TLS。ECH 字节非法须返回 Err（不得 panic）。

- [ ] **Step 1: 写失败测试**

`src/tls.rs`：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_plain_config_with_alpn() {
        let cfg = client_config(&[b"h2"], &[], None).expect("plain config");
        assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn garbage_ech_is_error_not_panic() {
        let r = client_config(&[b"h2"], &[], Some(b"not an ech config list"));
        assert!(r.is_err(), "garbage ECHConfigList must be a clean error");
    }
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test --lib tls`，编译失败。

- [ ] **Step 3: 实现**

```rust
use anyhow::{Context, Result};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::CertificateDer;

/// 构建上游 TLS 客户端配置：webpki 根证书 + 可选附加根（测试用自签）+
/// ALPN + 可选 ECH。ECH 依赖 rustls aws-lc-rs provider 的 HPKE 套件。
pub fn client_config(
    alpn: &[&[u8]],
    extra_roots: &[CertificateDer<'static>],
    ech_config_list: Option<&[u8]>,
) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for cert in extra_roots {
        roots.add(cert.clone()).context("adding extra root cert")?;
    }

    let mut config = match ech_config_list {
        Some(bytes) => {
            // rustls 0.23 ECH：EchConfig::new(EchConfigListBytes, HPKE suites) + EchMode::Enable。
            // 具体 builder 链以安装版本 docs.rs 为准（授权适配）；参考 rustls 仓库
            // examples/src/bin/ech-client.rs。
            use rustls::client::{EchConfig, EchMode};
            use rustls_pki_types::EchConfigListBytes;
            let ech = EchConfig::new(
                EchConfigListBytes::from(bytes.to_vec()),
                rustls::crypto::aws_lc_rs::hpke::ALL_SUPPORTED_SUITES,
            )
            .context("parsing ECHConfigList")?;
            ClientConfig::builder_with_provider(
                rustls::crypto::aws_lc_rs::default_provider().into(),
            )
            .with_ech(EchMode::from(ech))
            .context("enabling ECH")?
            .with_root_certificates(roots)
            .with_no_client_auth()
        }
        None => ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    };

    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(config)
}
```

若安装的 rustls 需要 `ech` cargo feature 或 `EchMode` 路径不同，按 docs.rs 适配并在报告注明。

- [ ] **Step 4: 注册 `pub mod tls;`，测试通过** — 2 个 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/tls.rs src/lib.rs Cargo.toml Cargo.lock
git commit -m "feat: add TLS client config builder with optional ECH"
```

---

### Task 5: DoT 解析器

**Files:**
- Create: `src/upstream/dot.rs`
- Modify: `src/upstream/mod.rs`（`pub mod dot;`）

**Interfaces:**
- Consumes: `tls::client_config`、`resolver::Resolver`。
- Produces:
  - `pub struct DotResolver`
  - `pub fn new(addr: SocketAddr, domain: &str, tls: Arc<rustls::ClientConfig>) -> anyhow::Result<Self>`（内部 `ServerName::try_from(domain.to_string())`）
  - `pub fn with_timeout(..., timeout: Duration) -> anyhow::Result<Self>`
  - `impl Resolver`：TCP connect → TLS（SNI=domain）→ 2 字节长度前缀写 query → 读响应 → 解码 → id 校验。每查询新建连接（连接复用留待后续优化）。

- [ ] **Step 1: 写失败测试（rcgen 自签 + tokio-rustls mock DoT server）**

`src/upstream/dot.rs` tests：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 生成 localhost 自签证书，起一个单连接 mock DoT server，
    /// 返回 (addr, 根证书) 供客户端信任。
    async fn spawn_mock_dot_server() -> (SocketAddr, CertificateDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert);
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der.into())
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let mut len = [0u8; 2];
            tls.read_exact(&mut len).await.unwrap();
            let n = u16::from_be_bytes(len) as usize;
            let mut data = vec![0u8; n];
            tls.read_exact(&mut data).await.unwrap();
            let query = Message::from_vec(&data).unwrap();
            // mirror 仓库既有响应构造模式
            let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
            resp.metadata.response_code = ResponseCode::NoError;
            for q in &query.queries {
                resp.add_query(q.clone());
            }
            let bytes = resp.to_vec().unwrap();
            let mut out = Vec::with_capacity(2 + bytes.len());
            out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            out.extend_from_slice(&bytes);
            tls.write_all(&out).await.unwrap();
            tls.shutdown().await.ok();
        });
        (addr, cert_der)
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x5151, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn resolves_over_tls_with_length_prefix() {
        let (addr, root) = spawn_mock_dot_server().await;
        let tls = Arc::new(crate::tls::client_config(&[], &[root], None).unwrap());
        let resolver = DotResolver::new(addr, "localhost", tls).unwrap();
        let resp = resolver.resolve(&sample_query()).await.expect("dot resolve");
        assert_eq!(resp.metadata.id, 0x5151);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }
}
```

（`Message::new` 三参构造与 `metadata`/`queries` 访问 mirror 仓库既有模式；rcgen 字段名以安装版本为准适配。）

- [ ] **Step 2: 运行确认失败** — `cargo test --lib upstream::dot`，编译失败。

- [ ] **Step 3: 实现**

```rust
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use hickory_proto::op::Message;
use rustls_pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use crate::resolver::Resolver;

/// DNS-over-TLS 上游（RFC 7858）：TCP + TLS(SNI=domain) + 2 字节长度前缀。
/// 每查询新建连接；连接复用留待后续优化。
pub struct DotResolver {
    addr: SocketAddr,
    server_name: ServerName<'static>,
    connector: TlsConnector,
    timeout: Duration,
}

impl DotResolver {
    pub fn new(addr: SocketAddr, domain: &str, tls: Arc<rustls::ClientConfig>) -> Result<Self> {
        Self::with_timeout(addr, domain, tls, Duration::from_secs(5))
    }

    pub fn with_timeout(
        addr: SocketAddr,
        domain: &str,
        tls: Arc<rustls::ClientConfig>,
        timeout: Duration,
    ) -> Result<Self> {
        let server_name = ServerName::try_from(domain.to_string())
            .with_context(|| format!("invalid DoT server name {domain}"))?;
        Ok(Self { addr, server_name, connector: TlsConnector::from(tls), timeout })
    }

    async fn exchange(&self, query: &Message) -> Result<Message> {
        let tcp = TcpStream::connect(self.addr)
            .await
            .with_context(|| format!("connecting to DoT upstream {}", self.addr))?;
        let mut tls = self
            .connector
            .connect(self.server_name.clone(), tcp)
            .await
            .context("TLS handshake")?;

        let bytes = query.to_vec().context("encoding query")?;
        let mut out = Vec::with_capacity(2 + bytes.len());
        out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(&bytes);
        tls.write_all(&out).await.context("writing query")?;

        let mut len = [0u8; 2];
        tls.read_exact(&mut len).await.context("reading response length")?;
        let n = u16::from_be_bytes(len) as usize;
        let mut data = vec![0u8; n];
        tls.read_exact(&mut data).await.context("reading response body")?;

        let resp = Message::from_vec(&data).context("decoding response")?;
        if resp.metadata.id != query.metadata.id {
            bail!("DoT response id mismatch");
        }
        Ok(resp)
    }
}

#[async_trait]
impl Resolver for DotResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        timeout(self.timeout, self.exchange(query))
            .await
            .with_context(|| format!("DoT upstream {} timed out", self.addr))?
    }
}
```

- [ ] **Step 4: 注册 `pub mod dot;`，测试通过**，全量 `cargo test` 绿。

- [ ] **Step 5: Commit**

```bash
git add src/upstream/dot.rs src/upstream/mod.rs
git commit -m "feat: add DoT resolver with TLS and length-prefixed framing"
```

---

### Task 6: DoH H2 解析器

**Files:**
- Create: `src/upstream/doh.rs`
- Modify: `src/upstream/mod.rs`（`pub mod doh;`）

**Interfaces:**
- Consumes: `tls::client_config`、`resolver::Resolver`。
- Produces:
  - `pub struct DohResolver`
  - `pub fn new(url: &str, ips: Vec<IpAddr>, ech: Option<Vec<u8>>, http3: bool) -> anyhow::Result<Self>` — 解析 url 得 host/port/path；内部构建 h2 与 h3 两套 TLS 配置（ALPN 各异，共享 ech）。
  - `pub fn with_extra_roots(..., extra_roots: &[CertificateDer<'static>]) -> Result<Self>`（测试注入自签根；生产用 `new`）
  - `impl Resolver`：`http3=true` 先试 H3（Task 7 接入，本 Task 先留 stub 直接返回 Err("h3 not built")），失败 warn 后回退 H2。H2：TCP(遍历 ips 直到连上) → TLS → hyper http2 handshake → POST → 200 校验 → 解码 → id 校验。H2 连接经 `tokio::sync::Mutex<Option<SendRequest>>` 复用，发送失败清空重连一次。

- [ ] **Step 1: 写失败测试（rcgen + hyper H2 mock DoH server）**

`src/upstream/doh.rs` tests：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use bytes::Bytes;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    /// 起一个 HTTPS(H2) mock DoH server：对 POST /dns-query 回 NoError 响应。
    async fn spawn_mock_doh_server() -> (SocketAddr, CertificateDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert);
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let mut server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der.into())
            .unwrap();
        server_config.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(tcp).await.unwrap();
            let service = service_fn(|req: hyper::Request<hyper::body::Incoming>| async move {
                assert_eq!(req.method(), hyper::Method::POST);
                let body = req.into_body().collect().await.unwrap().to_bytes();
                let query = Message::from_vec(&body).unwrap();
                let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
                resp.metadata.response_code = ResponseCode::NoError;
                for q in &query.queries {
                    resp.add_query(q.clone());
                }
                Ok::<_, std::convert::Infallible>(
                    hyper::Response::builder()
                        .status(200)
                        .header("content-type", "application/dns-message")
                        .body(Full::new(Bytes::from(resp.to_vec().unwrap())))
                        .unwrap(),
                )
            });
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(tls), service)
                .await
                .ok();
        });
        (addr, cert_der)
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x6161, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn resolves_over_h2() {
        let (addr, root) = spawn_mock_doh_server().await;
        let url = format!("https://localhost:{}/dns-query", addr.port());
        let resolver = DohResolver::with_extra_roots(
            &url,
            vec![addr.ip()],
            None,
            false, // 本测试仅 H2
            &[root],
        )
        .unwrap();
        let resp = resolver.resolve(&sample_query()).await.expect("doh h2 resolve");
        assert_eq!(resp.metadata.id, 0x6161);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn h3_failure_falls_back_to_h2() {
        let (addr, root) = spawn_mock_doh_server().await;
        let url = format!("https://localhost:{}/dns-query", addr.port());
        let resolver = DohResolver::with_extra_roots(
            &url,
            vec![addr.ip()],
            None,
            true, // http3 开启但 mock 无 H3 端点（本 Task 阶段 stub 必 Err）→ 必须回退 H2 成功
            &[root],
        )
        .unwrap();
        let resp = resolver.resolve(&sample_query()).await.expect("h3->h2 fallback");
        assert_eq!(resp.metadata.id, 0x6161);
    }
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test --lib upstream::doh`，编译失败。

- [ ] **Step 3: 实现**

```rust
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use hickory_proto::op::Message;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls_pki_types::{CertificateDer, ServerName};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use crate::resolver::Resolver;

type H2Sender = hyper::client::conn::http2::SendRequest<Full<Bytes>>;

/// DNS-over-HTTPS 上游（RFC 8484，POST application/dns-message）。
/// http3=true 时先试 H3（doh3 模块），失败回退 H2；H2 连接复用，断线重连一次。
pub struct DohResolver {
    host: String,
    port: u16,
    path: String,
    ips: Vec<IpAddr>,
    tls_h2: Arc<rustls::ClientConfig>,
    http3: bool,
    timeout: Duration,
    h2: Mutex<Option<H2Sender>>,
    // Task 7 接入：h3 连接状态
    pub(crate) ech: Option<Vec<u8>>,
    pub(crate) extra_roots: Vec<CertificateDer<'static>>,
}

impl DohResolver {
    pub fn new(url: &str, ips: Vec<IpAddr>, ech: Option<Vec<u8>>, http3: bool) -> Result<Self> {
        Self::with_extra_roots(url, ips, ech, http3, &[])
    }

    pub fn with_extra_roots(
        url: &str,
        ips: Vec<IpAddr>,
        ech: Option<Vec<u8>>,
        http3: bool,
        extra_roots: &[CertificateDer<'static>],
    ) -> Result<Self> {
        let uri: http::Uri = url.parse().with_context(|| format!("invalid DoH url {url}"))?;
        if uri.scheme_str() != Some("https") {
            bail!("DoH url must be https: {url}");
        }
        let host = uri.host().context("DoH url missing host")?.to_string();
        let port = uri.port_u16().unwrap_or(443);
        let path = if uri.path().is_empty() { "/dns-query".into() } else { uri.path().to_string() };
        if ips.is_empty() {
            bail!("DoH resolver for {host} constructed without ips (bootstrap it first)");
        }
        let tls_h2 =
            Arc::new(crate::tls::client_config(&[b"h2"], extra_roots, ech.as_deref())?);
        Ok(Self {
            host,
            port,
            path,
            ips,
            tls_h2,
            http3,
            timeout: Duration::from_secs(5),
            h2: Mutex::new(None),
            ech,
            extra_roots: extra_roots.to_vec(),
        })
    }

    async fn connect_h2(&self) -> Result<H2Sender> {
        let mut last_err = None;
        for ip in &self.ips {
            match TcpStream::connect((*ip, self.port)).await {
                Ok(tcp) => {
                    let server_name = ServerName::try_from(self.host.clone())
                        .context("invalid DoH server name")?;
                    let tls = TlsConnector::from(self.tls_h2.clone())
                        .connect(server_name, tcp)
                        .await
                        .context("DoH TLS handshake")?;
                    let (sender, conn) =
                        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
                            .await
                            .context("h2 handshake")?;
                    tokio::spawn(async move {
                        if let Err(e) = conn.await {
                            tracing::debug!("h2 connection closed: {e}");
                        }
                    });
                    return Ok(sender);
                }
                Err(e) => last_err = Some(e),
            }
        }
        bail!("cannot connect to any DoH ip for {}: {:?}", self.host, last_err)
    }

    async fn resolve_h2(&self, query: &Message) -> Result<Message> {
        let body = query.to_vec().context("encoding query")?;
        // 复用连接；发送失败清空后重连一次
        for attempt in 0..2 {
            let mut guard = self.h2.lock().await;
            if guard.is_none() {
                *guard = Some(self.connect_h2().await?);
            }
            let sender = guard.as_mut().expect("just set");
            let req = http::Request::builder()
                .method(http::Method::POST)
                .uri(format!("https://{}:{}{}", self.host, self.port, self.path))
                .header(http::header::CONTENT_TYPE, "application/dns-message")
                .header(http::header::ACCEPT, "application/dns-message")
                .body(Full::new(Bytes::from(body.clone())))
                .context("building request")?;
            match sender.send_request(req).await {
                Ok(resp) => {
                    drop(guard);
                    if resp.status() != http::StatusCode::OK {
                        bail!("DoH upstream {} returned {}", self.host, resp.status());
                    }
                    let bytes =
                        resp.into_body().collect().await.context("reading body")?.to_bytes();
                    let msg = Message::from_vec(&bytes).context("decoding response")?;
                    if msg.metadata.id != query.metadata.id {
                        bail!("DoH response id mismatch");
                    }
                    return Ok(msg);
                }
                Err(e) => {
                    *guard = None; // 连接失效，下轮重连
                    if attempt == 1 {
                        return Err(e).context("h2 send_request failed after reconnect");
                    }
                    tracing::debug!("h2 send failed, reconnecting: {e}");
                }
            }
        }
        unreachable!("loop returns or errors")
    }

    async fn resolve_h3(&self, _query: &Message) -> Result<Message> {
        bail!("h3 support not built yet") // Task 7 替换为真实实现
    }
}

#[async_trait]
impl Resolver for DohResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        if self.http3 {
            match timeout(self.timeout, self.resolve_h3(query)).await {
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(e)) => tracing::warn!("DoH h3 failed for {}, falling back to h2: {e:#}", self.host),
                Err(_) => tracing::warn!("DoH h3 timed out for {}, falling back to h2", self.host),
            }
        }
        timeout(self.timeout, self.resolve_h2(query))
            .await
            .with_context(|| format!("DoH upstream {} timed out", self.host))?
    }
}
```

（`unreachable!` 在控制流上不可达——两轮循环内必 return/Err；若 clippy 抱怨可改为 `bail!`。）

- [ ] **Step 4: 注册 `pub mod doh;`，测试通过**（2 个 PASS：直连 H2 + stub H3 回退 H2），全量绿。

- [ ] **Step 5: Commit**

```bash
git add src/upstream/doh.rs src/upstream/mod.rs
git commit -m "feat: add DoH resolver with H2 client, connection reuse, H3 fallback scaffold"
```

---

### Task 7: DoH HTTP/3 支持

**Files:**
- Create: `src/upstream/doh3.rs`
- Modify: `src/upstream/mod.rs`（`pub mod doh3;`）
- Modify: `src/upstream/doh.rs`（`resolve_h3` 接入真实实现）

**Interfaces:**
- Consumes: `tls::client_config`（ALPN h3）。
- Produces:
  - `pub struct H3Conn`：`pub fn new(host: String, port: u16, ips: Vec<IpAddr>, tls: rustls::ClientConfig) -> anyhow::Result<Self>`（内部建 quinn Endpoint；连接惰性建立并复用，失败时重建）。
  - `pub async fn request(&self, uri: &str, body: Vec<u8>) -> anyhow::Result<Vec<u8>>` — H3 POST application/dns-message，200 校验，返回 body 字节。
  - `DohResolver` 增加字段 `h3: Option<doh3::H3Conn>`（构造时 `http3=true` 才建），`resolve_h3` 调用它 + id 校验。

- [ ] **Step 1: 写失败测试（quinn + h3 mock H3 DoH server）**

`src/upstream/doh3.rs` tests（构建真实 H3 端到端；rcgen 证书同前）：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, Bytes};
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    async fn spawn_mock_h3_server() -> (SocketAddr, CertificateDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert);
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der.into())
            .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];

        let server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = endpoint.local_addr().unwrap();

        tokio::spawn(async move {
            if let Some(incoming) = endpoint.accept().await {
                let conn = incoming.await.unwrap();
                let mut h3_conn: h3::server::Connection<_, Bytes> =
                    h3::server::Connection::new(h3_quinn::Connection::new(conn)).await.unwrap();
                while let Ok(Some((_req, mut stream))) = h3_conn.accept().await {
                    let mut body = Vec::new();
                    while let Some(mut chunk) = stream.recv_data().await.unwrap() {
                        while chunk.has_remaining() {
                            let c = chunk.chunk();
                            body.extend_from_slice(c);
                            chunk.advance(c.len());
                        }
                    }
                    let query = Message::from_vec(&body).unwrap();
                    let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
                    resp.metadata.response_code = ResponseCode::NoError;
                    for q in &query.queries {
                        resp.add_query(q.clone());
                    }
                    let http_resp = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/dns-message")
                        .body(())
                        .unwrap();
                    stream.send_response(http_resp).await.unwrap();
                    stream.send_data(Bytes::from(resp.to_vec().unwrap())).await.unwrap();
                    stream.finish().await.unwrap();
                }
            }
        });
        (addr, cert_der)
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x7171, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn h3_round_trip() {
        let (addr, root) = spawn_mock_h3_server().await;
        let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
        let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
        let uri = format!("https://localhost:{}/dns-query", addr.port());
        let body = conn.request(&uri, sample_query().to_vec().unwrap()).await.expect("h3 request");
        let resp = Message::from_vec(&body).unwrap();
        assert_eq!(resp.metadata.id, 0x7171);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test --lib upstream::doh3`，编译失败。

- [ ] **Step 3: 实现 H3Conn**

```rust
use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;

type H3Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// 惰性建立并复用的 HTTP/3 (QUIC) 连接。发送失败时重建一次由调用方驱动
/// （request 内部清空状态后返回错误，DohResolver 的 H2 回退接管）。
pub struct H3Conn {
    host: String,
    port: u16,
    ips: Vec<IpAddr>,
    endpoint: quinn::Endpoint,
    state: Mutex<Option<H3Sender>>,
}

impl H3Conn {
    pub fn new(host: String, port: u16, ips: Vec<IpAddr>, tls: rustls::ClientConfig) -> Result<Self> {
        let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .context("building QUIC client config (provider must support QUIC)")?;
        let client_config = quinn::ClientConfig::new(Arc::new(quic));
        let bind: SocketAddr = if ips.iter().all(|ip| ip.is_ipv6()) {
            "[::]:0".parse().expect("static addr")
        } else {
            "0.0.0.0:0".parse().expect("static addr")
        };
        let mut endpoint = quinn::Endpoint::client(bind).context("creating QUIC endpoint")?;
        endpoint.set_default_client_config(client_config);
        Ok(Self { host, port, ips, endpoint, state: Mutex::new(None) })
    }

    async fn connect(&self) -> Result<H3Sender> {
        let mut last: Option<anyhow::Error> = None;
        for ip in &self.ips {
            let addr = SocketAddr::new(*ip, self.port);
            match self.try_connect(addr).await {
                Ok(sender) => return Ok(sender),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no ips for {}", self.host)))
    }

    async fn try_connect(&self, addr: SocketAddr) -> Result<H3Sender> {
        let conn = self
            .endpoint
            .connect(addr, &self.host)
            .context("starting QUIC connection")?
            .await
            .context("QUIC handshake")?;
        let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(conn))
            .await
            .context("h3 client setup")?;
        tokio::spawn(async move {
            // 驱动连接直到关闭
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        Ok(sender)
    }

    pub async fn request(&self, uri: &str, body: Vec<u8>) -> Result<Vec<u8>> {
        let mut guard = self.state.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect().await?);
        }
        let sender = guard.as_mut().expect("just set");

        let req = http::Request::builder()
            .method(http::Method::POST)
            .uri(uri)
            .header(http::header::CONTENT_TYPE, "application/dns-message")
            .header(http::header::ACCEPT, "application/dns-message")
            .body(())
            .context("building h3 request")?;

        let result: Result<Vec<u8>> = async {
            let mut stream = sender.send_request(req).await.context("h3 send_request")?;
            stream.send_data(Bytes::from(body)).await.context("h3 send body")?;
            stream.finish().await.context("h3 finish")?;
            let resp = stream.recv_response().await.context("h3 recv response")?;
            if resp.status() != http::StatusCode::OK {
                bail!("DoH h3 upstream {} returned {}", self.host, resp.status());
            }
            let mut data = Vec::new();
            while let Some(mut chunk) = stream.recv_data().await.context("h3 recv data")? {
                while chunk.has_remaining() {
                    let c = chunk.chunk();
                    data.extend_from_slice(c);
                    chunk.advance(c.len());
                }
            }
            Ok(data)
        }
        .await;

        if result.is_err() {
            *guard = None; // 连接可能已坏，下次重建
        }
        result
    }
}
```

- [ ] **Step 4: 接入 DohResolver**

`src/upstream/doh.rs` 变更：
- 增加字段 `h3: Option<crate::upstream::doh3::H3Conn>`。
- `with_extra_roots` 中 `http3=true` 时：`tls::client_config(&[b"h3"], extra_roots, ech.as_deref())` 构建并 `H3Conn::new(host.clone(), port, ips.clone(), tls_h3)?`，存入 `h3`。
- `resolve_h3` 替换 stub：

```rust
    async fn resolve_h3(&self, query: &Message) -> Result<Message> {
        let conn = self.h3.as_ref().context("h3 not enabled")?;
        let uri = format!("https://{}:{}{}", self.host, self.port, self.path);
        let body = conn.request(&uri, query.to_vec().context("encoding query")?).await?;
        let msg = Message::from_vec(&body).context("decoding h3 response")?;
        if msg.metadata.id != query.metadata.id {
            bail!("DoH h3 response id mismatch");
        }
        Ok(msg)
    }
```

- 删除 Task 6 里临时的 `pub(crate) ech` / `pub(crate) extra_roots` 字段（若接入后不再需要）。

- [ ] **Step 5: 追加 doh.rs 端到端 H3 测试**

`src/upstream/doh.rs` tests 追加（复用 doh3 tests 的 mock —— 把 `spawn_mock_h3_server` 提为 `#[cfg(test)] pub(crate)`，或在 doh.rs 测试中复制；优先前者）：

```rust
    #[tokio::test]
    async fn resolves_over_h3_when_available() {
        let (addr, root) = crate::upstream::doh3::tests::spawn_mock_h3_server().await;
        let url = format!("https://localhost:{}/dns-query", addr.port());
        let resolver = DohResolver::with_extra_roots(
            &url,
            vec![addr.ip()],
            None,
            true,
            &[root],
        )
        .unwrap();
        let resp = resolver.resolve(&sample_query()).await.expect("doh h3 resolve");
        assert_eq!(resp.metadata.id, 0x6161);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }
```

（需要把 doh3 的 `mod tests` 改为 `pub(crate) mod tests` 并把 `spawn_mock_h3_server` 设为 `pub(crate)`。）

- [ ] **Step 6: 全部测试通过** — `cargo test`，含 h3 round trip、doh h3 端到端、h3→h2 回退（Task 6 的 stub 回退测试现在走真实 H3 连接失败路径——mock H2 server 无 H3 端点，H3 连接超时/拒绝后回退，仍应通过；若因 H3 连接等待拖慢测试，在该测试构造里用 `with_timeout` 缩短）。

- [ ] **Step 7: Commit**

```bash
git add src/upstream/doh3.rs src/upstream/doh.rs src/upstream/mod.rs
git commit -m "feat: add HTTP/3 DoH transport with runtime H2 fallback"
```

---

### Task 8: bootstrap.rs 域名解析与 ECH 获取

**Files:**
- Create: `src/bootstrap.rs`
- Modify: `src/lib.rs`（`pub mod bootstrap;`）

**Interfaces:**
- Consumes: `resolver::Resolver`、`config::UpstreamConfig`、`upstream::{plain, dot, doh}`。
- Produces:
  - `pub struct Bootstrap`
  - `pub fn from_config(servers: &[UpstreamConfig]) -> anyhow::Result<Self>` — Plain→PlainResolver；Dot→DotResolver（普通 TLS）；Doh→DohResolver（用其显式 ips，`http3=false` 简化，无 ECH）。
  - `pub fn is_empty(&self) -> bool`
  - `pub async fn resolve_ips(&self, domain: &str) -> anyhow::Result<Vec<IpAddr>>` — 依次问每个 bootstrap 解析器 A 与 AAAA，收集去重；全空则 Err。
  - `pub async fn fetch_ech(&self, domain: &str) -> anyhow::Result<Option<Vec<u8>>>` — 查 HTTPS(SVCB) 记录取 `ech` SvcParam；无记录/无参数 = Ok(None)。

- [ ] **Step 1: 写失败测试（mock plain 上游回 A/AAAA/HTTPS 记录）**

`src/bootstrap.rs` tests：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, ResponseCode};
    use hickory_proto::rr::rdata::svcb::{SvcParamKey, SvcParamValue, SVCB};
    use hickory_proto::rr::rdata::{A, AAAA, HTTPS};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::net::{IpAddr, SocketAddr};
    use std::str::FromStr;
    use tokio::net::UdpSocket;

    /// mock 上游：按 qtype 回 A/AAAA/HTTPS（带 ech 参数）记录。
    async fn spawn_mock_bootstrap_upstream(ech_bytes: Vec<u8>) -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
                let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
                let query = Message::from_vec(&buf[..n]).unwrap();
                let q = query.queries[0].clone();
                let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
                resp.metadata.response_code = ResponseCode::NoError;
                resp.add_query(q.clone());
                let name = q.name().clone();
                let rdata = match q.query_type() {
                    RecordType::A => Some(RData::A(A::new(93, 184, 216, 34))),
                    RecordType::AAAA => Some(RData::AAAA(AAAA::new(0x2606, 0x2800, 0, 0, 0, 0, 0, 1))),
                    RecordType::HTTPS => {
                        let mut svcb = SVCB::new(1, Name::root());
                        svcb.set_svc_param(
                            SvcParamKey::EchConfigList,
                            SvcParamValue::EchConfigList(ech_bytes.clone().into()),
                        );
                        Some(RData::HTTPS(HTTPS(svcb)))
                    }
                    _ => None,
                };
                if let Some(rdata) = rdata {
                    resp.add_answer(Record::from_rdata(name, 300, rdata));
                }
                sock.send_to(&resp.to_vec().unwrap(), peer).await.unwrap();
            }
        });
        addr
    }

    fn bootstrap_for(addr: SocketAddr) -> Bootstrap {
        Bootstrap::from_config(&[crate::config::UpstreamConfig::Plain { addr }]).unwrap()
    }

    #[tokio::test]
    async fn resolves_a_and_aaaa() {
        let addr = spawn_mock_bootstrap_upstream(vec![1, 2, 3]).await;
        let b = bootstrap_for(addr);
        let ips = b.resolve_ips("dns.example").await.expect("ips");
        assert!(ips.contains(&IpAddr::from_str("93.184.216.34").unwrap()));
        assert!(ips.iter().any(|ip| ip.is_ipv6()));
    }

    #[tokio::test]
    async fn fetches_ech_from_https_record() {
        let addr = spawn_mock_bootstrap_upstream(vec![0xAB, 0xCD]).await;
        let b = bootstrap_for(addr);
        let ech = b.fetch_ech("dns.example").await.expect("ech query");
        assert_eq!(ech, Some(vec![0xAB, 0xCD]));
    }
}
```

（hickory-proto svcb API——`SVCB::new`、`set_svc_param`、`SvcParamValue::EchConfigList` 的确切签名以安装版本为准适配；`Record::from_rdata`、`add_answer` 同理。）

- [ ] **Step 2: 运行确认失败** — `cargo test --lib bootstrap`，编译失败。

- [ ] **Step 3: 实现**

```rust
use anyhow::{bail, Context, Result};
use hickory_proto::op::{Message, MessageType, OpCode, Query};
use hickory_proto::rr::{Name, RData, RecordType};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use crate::config::UpstreamConfig;
use crate::resolver::Resolver;
use crate::upstream::{doh::DohResolver, dot::DotResolver, plain::PlainResolver};

/// Bootstrap 解析器组：为上游 DoH 域名解析 IP、为 ECH 拉取 HTTPS 记录。
/// 非 IP 形态的 bootstrap 服务器必须自带显式 ips（config.validate 已保证）。
pub struct Bootstrap {
    resolvers: Vec<Arc<dyn Resolver>>,
}

impl Bootstrap {
    pub fn from_config(servers: &[UpstreamConfig]) -> Result<Self> {
        let mut resolvers: Vec<Arc<dyn Resolver>> = Vec::new();
        for s in servers {
            let r: Arc<dyn Resolver> = match s {
                UpstreamConfig::Plain { addr } => Arc::new(PlainResolver::new(*addr)),
                UpstreamConfig::Dot { addr, domain, .. } => {
                    let tls = Arc::new(crate::tls::client_config(&[], &[], None)?);
                    Arc::new(DotResolver::new(*addr, domain, tls)?)
                }
                UpstreamConfig::Doh { url, ips, .. } => {
                    // bootstrap 场景简化：H2、无 ECH（validate 已保证 ips 非空）
                    Arc::new(DohResolver::new(url, ips.clone(), None, false)?)
                }
            };
            resolvers.push(r);
        }
        Ok(Self { resolvers })
    }

    pub fn is_empty(&self) -> bool {
        self.resolvers.is_empty()
    }

    fn make_query(domain: &str, rtype: RecordType) -> Result<Message> {
        let name = Name::from_str(&format!("{}.", domain.trim_end_matches('.')))
            .with_context(|| format!("invalid domain {domain}"))?;
        let mut m = Message::new(rand::random::<u16>(), MessageType::Query, OpCode::Query);
        m.metadata.recursion_desired = true;
        let mut q = Query::new();
        q.set_name(name);
        q.set_query_type(rtype);
        m.add_query(q);
        Ok(m)
    }

    async fn query(&self, domain: &str, rtype: RecordType) -> Result<Message> {
        let query = Self::make_query(domain, rtype)?;
        let mut last: Option<anyhow::Error> = None;
        for r in &self.resolvers {
            match r.resolve(&query).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    tracing::warn!("bootstrap resolver failed for {domain} {rtype}: {e:#}");
                    last = Some(e);
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no bootstrap resolvers configured")))
    }

    pub async fn resolve_ips(&self, domain: &str) -> Result<Vec<IpAddr>> {
        let mut ips = BTreeSet::new();
        for rtype in [RecordType::A, RecordType::AAAA] {
            match self.query(domain, rtype).await {
                Ok(resp) => {
                    for record in &resp.answers {
                        match record.data() {
                            RData::A(a) => {
                                ips.insert(IpAddr::V4(a.0));
                            }
                            RData::AAAA(aaaa) => {
                                ips.insert(IpAddr::V6(aaaa.0));
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => tracing::warn!("bootstrap {rtype} lookup failed for {domain}: {e:#}"),
            }
        }
        if ips.is_empty() {
            bail!("bootstrap could not resolve any ip for {domain}");
        }
        Ok(ips.into_iter().collect())
    }

    pub async fn fetch_ech(&self, domain: &str) -> Result<Option<Vec<u8>>> {
        let resp = self.query(domain, RecordType::HTTPS).await?;
        for record in &resp.answers {
            if let RData::HTTPS(https) = record.data() {
                for (key, value) in https.0.svc_params() {
                    use hickory_proto::rr::rdata::svcb::{SvcParamKey, SvcParamValue};
                    if matches!(key, SvcParamKey::EchConfigList) {
                        if let SvcParamValue::EchConfigList(list) = value {
                            return Ok(Some(list.clone().into()));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}
```

（`resp.answers` 字段 vs `resp.answers()` 方法、`record.data()`、`a.0`/`aaaa.0`、`https.0.svc_params()`、`EchConfigList` 内部类型转换——以安装版本适配，mirror 现有代码风格。）

- [ ] **Step 4: 注册 `pub mod bootstrap;`，测试通过**，全量绿。

- [ ] **Step 5: Commit**

```bash
git add src/bootstrap.rs src/lib.rs
git commit -m "feat: add bootstrap resolver for upstream ips and ECH via HTTPS records"
```

---

### Task 9: 上游组、回退链与装配

**Files:**
- Create: `src/upstream/group.rs`
- Modify: `src/upstream/mod.rs`（`pub mod group;`）
- Modify: `src/lib.rs`（async `build_pipeline` 重写）
- Modify: `src/main.rs`（`.await`）
- Modify: `tests/forwarding.rs`（async 适配 + 新集成测试）

**Interfaces:**
- Consumes: `stats::UpstreamStats`、`upstream::selector::pick_weighted`、全部 resolver 实现、`bootstrap::Bootstrap`。
- Produces:
  - `pub struct UpstreamGroup`：`pub fn new(members: Vec<(String, Arc<dyn Resolver>)>, window: usize, k: f64) -> Self`；`impl Resolver`——计算权重 → `pick_weighted`（roll 用 `rand::random::<f64>()`）→ 尝试；失败 `record_failure` 并在未试过的成员中重选，最多尝试 `members.len().min(2)` 次；成功 `record_success(耗时)`。全部失败 → Err。
  - `pub struct FallbackResolver`：`pub fn new(primary: Arc<dyn Resolver>, fallback: Arc<dyn Resolver>) -> Self`；`impl Resolver`——primary Err 时记 warn 走 fallback。
  - `src/lib.rs`：`pub async fn build_pipeline(config: &Config) -> Result<Arc<Pipeline>>`——bootstrap 构建 → 上游条目逐个构建成员（Doh 无 ips 时 `bootstrap.resolve_ips`；ech 为空时 `bootstrap.fetch_ech`（失败 warn→None）；ech 非空时 base64 解码）→ `UpstreamGroup` → fallback 组（如配置）→ `FallbackResolver` → `Pipeline`。

- [ ] **Step 1: 写失败测试（group 单元测试，mock Resolver）**

`src/upstream/group.rs` tests：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingOk(AtomicUsize);
    #[async_trait]
    impl Resolver for CountingOk {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
            resp.metadata.response_code = ResponseCode::NoError;
            Ok(resp)
        }
    }

    struct AlwaysErr(AtomicUsize);
    #[async_trait]
    impl Resolver for AlwaysErr {
        async fn resolve(&self, _q: &Message) -> Result<Message> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(anyhow!("dead upstream"))
        }
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x9, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn failing_member_retries_on_healthy_one() {
        let ok = Arc::new(CountingOk(AtomicUsize::new(0)));
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let group = UpstreamGroup::new(
            vec![
                ("bad".into(), bad.clone() as Arc<dyn Resolver>),
                ("ok".into(), ok.clone() as Arc<dyn Resolver>),
            ],
            8,
            5.0,
        );
        // 多次调用：每次都应最终成功（坏成员失败后重选好成员）
        for _ in 0..10 {
            let resp = group.resolve(&sample_query()).await.expect("group resolves");
            assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        }
        assert!(ok.0.load(Ordering::SeqCst) >= 10, "healthy member served all queries");
    }

    #[tokio::test]
    async fn weights_shift_away_from_failures() {
        let ok = Arc::new(CountingOk(AtomicUsize::new(0)));
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let group = UpstreamGroup::new(
            vec![
                ("bad".into(), bad.clone() as Arc<dyn Resolver>),
                ("ok".into(), ok.clone() as Arc<dyn Resolver>),
            ],
            8,
            5.0,
        );
        for _ in 0..50 {
            let _ = group.resolve(&sample_query()).await;
        }
        let bad_hits = bad.0.load(Ordering::SeqCst);
        let ok_hits = ok.0.load(Ordering::SeqCst);
        // 权重衰减后坏成员被选中的次数应显著低于好成员
        assert!(ok_hits > bad_hits, "ok {ok_hits} should exceed bad {bad_hits}");
    }

    #[tokio::test]
    async fn all_dead_is_error() {
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let group = UpstreamGroup::new(vec![("bad".into(), bad as Arc<dyn Resolver>)], 8, 5.0);
        assert!(group.resolve(&sample_query()).await.is_err());
    }

    #[tokio::test]
    async fn fallback_takes_over_when_primary_dies() {
        let ok = Arc::new(CountingOk(AtomicUsize::new(0)));
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let primary = Arc::new(UpstreamGroup::new(vec![("bad".into(), bad as Arc<dyn Resolver>)], 8, 5.0));
        let fallback = Arc::new(UpstreamGroup::new(vec![("ok".into(), ok as Arc<dyn Resolver>)], 8, 5.0));
        let chain = FallbackResolver::new(primary, fallback);
        let resp = chain.resolve(&sample_query()).await.expect("fallback serves");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }
}
```

- [ ] **Step 2: 运行确认失败** — `cargo test --lib upstream::group`，编译失败。

- [ ] **Step 3: 实现 group.rs**

```rust
use anyhow::{bail, Result};
use async_trait::async_trait;
use hickory_proto::op::Message;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::resolver::Resolver;
use crate::stats::UpstreamStats;
use crate::upstream::selector::pick_weighted;

/// 一次查询在组内最多尝试的成员数。
const MAX_ATTEMPTS: usize = 2;

struct Member {
    name: String,
    resolver: Arc<dyn Resolver>,
    stats: Mutex<UpstreamStats>,
}

/// 上游组：滑动窗口统计驱动加权随机选择，失败重选，统计反馈。
pub struct UpstreamGroup {
    members: Vec<Member>,
    k: f64,
}

impl UpstreamGroup {
    pub fn new(members: Vec<(String, Arc<dyn Resolver>)>, window: usize, k: f64) -> Self {
        let members = members
            .into_iter()
            .map(|(name, resolver)| Member {
                name,
                resolver,
                stats: Mutex::new(UpstreamStats::new(window)),
            })
            .collect();
        Self { members, k }
    }

    fn weights(&self, exclude: &[usize]) -> Vec<f64> {
        self.members
            .iter()
            .enumerate()
            .map(|(i, m)| {
                if exclude.contains(&i) {
                    0.0
                } else {
                    m.stats.lock().map(|s| s.weight(self.k)).unwrap_or(0.0)
                }
            })
            .collect()
    }
}

#[async_trait]
impl Resolver for UpstreamGroup {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        if self.members.is_empty() {
            bail!("upstream group is empty");
        }
        let attempts = self.members.len().min(MAX_ATTEMPTS);
        let mut tried: Vec<usize> = Vec::with_capacity(attempts);
        let mut last: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let weights = self.weights(&tried);
            let Some(idx) = pick_weighted(&weights, rand::random::<f64>()) else {
                break;
            };
            // 全零权重时 pick_weighted 均匀退化可能落在已试成员上；跳过
            if tried.contains(&idx) {
                if let Some(untried) = (0..self.members.len()).find(|i| !tried.contains(i)) {
                    tried.push(untried);
                    let m = &self.members[untried];
                    match try_member(m, query).await {
                        Ok(resp) => return Ok(resp),
                        Err(e) => last = Some(e),
                    }
                    continue;
                }
                break;
            }
            tried.push(idx);
            let m = &self.members[idx];
            match try_member(m, query).await {
                Ok(resp) => return Ok(resp),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("upstream group exhausted")))
    }
}

async fn try_member(m: &Member, query: &Message) -> Result<Message> {
    let start = Instant::now();
    match m.resolver.resolve(query).await {
        Ok(resp) => {
            if let Ok(mut s) = m.stats.lock() {
                s.record_success(start.elapsed());
            }
            Ok(resp)
        }
        Err(e) => {
            if let Ok(mut s) = m.stats.lock() {
                s.record_failure();
            }
            tracing::warn!("upstream {} failed: {e:#}", m.name);
            Err(e)
        }
    }
}

/// 主上游组全部失败时切换到后备组。
pub struct FallbackResolver {
    primary: Arc<dyn Resolver>,
    fallback: Arc<dyn Resolver>,
}

impl FallbackResolver {
    pub fn new(primary: Arc<dyn Resolver>, fallback: Arc<dyn Resolver>) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl Resolver for FallbackResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        match self.primary.resolve(query).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                tracing::warn!("primary upstreams exhausted, using fallback: {e:#}");
                self.fallback.resolve(query).await
            }
        }
    }
}
```

- [ ] **Step 4: group 测试通过** — `cargo test --lib upstream::group`，4 个 PASS。

- [ ] **Step 5: 重写 lib.rs 装配**

`src/lib.rs` 的 `build_pipeline` 及辅助替换为：

```rust
use crate::bootstrap::Bootstrap;
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
    use anyhow::Context;
    let mut members: Vec<(String, Arc<dyn Resolver>)> = Vec::new();
    for u in entries {
        members.push(build_member(u, bootstrap).await?);
    }
    if members.is_empty() {
        anyhow::bail!("no upstreams configured");
    }
    let _ = Context::context; // keep import used if needed
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
```

（`let _ = Context::context;` 若无必要直接删除——以编译为准。）

- [ ] **Step 6: main.rs 与 forwarding.rs async 适配 + 新集成测试**

`src/main.rs`：`let pipeline = build_pipeline(&cfg)?;` → `let pipeline = build_pipeline(&cfg).await?;`

`tests/forwarding.rs`：现有测试的 `build_pipeline(&cfg).unwrap()` → `build_pipeline(&cfg).await.unwrap()`；追加：

```rust
#[tokio::test]
async fn group_failover_and_fallback_serve() {
    // 主上游组：一个死端口 + 后备：活 mock → 查询仍应成功
    let alive = spawn_mock_upstream().await;
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dead = probe.local_addr().unwrap();
    drop(probe); // 无人监听 → ECONNREFUSED/超时

    let probe2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let listen = probe2.local_addr().unwrap();
    drop(probe2);

    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[upstream]]
        type = "plain"
        addr = "{dead}"

        [[fallback]]
        type = "plain"
        addr = "{alive}"
        "#
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg).await.unwrap();
    tokio::spawn(async move {
        server::run_udp(listen, pipeline).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(listen).await.unwrap();
    client.send(&query(0x2222).to_vec().unwrap()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(8), client.recv(&mut buf))
        .await
        .expect("no timeout")
        .unwrap();
    let resp = Message::from_vec(&buf[..n]).unwrap();
    assert_eq!(resp.metadata.id, 0x2222);
    assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
}
```

（`query()`/`spawn_mock_upstream()` 复用文件内既有辅助；`Message` 读取模式 mirror 现状。）

- [ ] **Step 7: 全量测试 + clippy** — `cargo test` 全绿；`cargo clippy --all-targets` 干净。

- [ ] **Step 8: 更新 config.example.toml**

```toml
[server]
listen = "0.0.0.0:53"
tcp = true

[selector]
window = 32
k = 5.0

[[upstream]]
type = "doh"
url = "https://cloudflare-dns.com/dns-query"
http3 = true
# ech = ""            # 可选：静态 base64 ECHConfigList；留空则经 bootstrap HTTPS 记录动态获取

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

- [ ] **Step 9: Commit**

```bash
git add src/upstream/group.rs src/upstream/mod.rs src/lib.rs src/main.rs tests/forwarding.rs config.example.toml
git commit -m "feat: wire upstream groups, weighted selection, and fallback chain"
```

---

## Self-Review

**Spec coverage（本计划范围）**：spec 第 2 点（DoH H2/H3 + ECH）→ Task 4/6/7；第 3 点（加权随机：最少失败 + 最低平均延迟）→ Task 2/3/9；第 4 点（bootstrap IP/DoH/DoT，非 IP 注明域名+IP）→ Task 1 校验 + Task 8；第 9 点（后备 DNS IP/DoH/DoT，主上游全失效时接管）→ Task 9 `FallbackResolver`；spec 7.3（ECH 静态优先→HTTPS 记录→普通 TLS+warn）→ Task 9 `build_member`；spec 7.1（k、窗口可配置）→ Task 1 `SelectorConfig`。计划一 carry-over：id 校验（Task 1）、Doh 配置补 ips（Task 1）、上游连接复用（Task 6 H2 / Task 7 H3 连接复用）。ECS/缓存/hosts/filter 归计划三。

**Placeholder scan**：无 TBD；不确定的第三方 API（rustls ECH、h3、quinn、rcgen、svcb）均给出完整意图代码并显式授权按安装版本适配——这是计划一验证过的工作模式，不是占位符。

**Type consistency**：`Resolver` trait 签名与计划一一致；`UpstreamStats::weight(k)`（Task 2 定义，Task 9 使用）；`pick_weighted(&[f64], f64) -> Option<usize>`（Task 3 定义，Task 9 使用）；`tls::client_config(alpn, extra_roots, ech)`（Task 4 定义，Task 5/6/7/8/9 一致使用）；`DohResolver::new/with_extra_roots`（Task 6 定义，Task 7 扩展、Task 8/9 使用）；`Bootstrap::{from_config,resolve_ips,fetch_ech,is_empty}`（Task 8 定义，Task 9 使用）；`UpstreamGroup::new(Vec<(String, Arc<dyn Resolver>)>, usize, f64)` 与 `FallbackResolver::new` 前后一致。
