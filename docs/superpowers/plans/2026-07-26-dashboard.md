# DNS 仪表板实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 dnsbuffer 单进程中增加持久化 SQLite 查询统计和嵌入式局域网 Web 仪表板，并支持按域名或响应 IP 搜索查询明细。

**Architecture:** 查询管线以非阻塞方式把 `QueryEvent` 投递给专用 SQLite 写线程，读取 API 通过 `spawn_blocking` 使用短连接查询 WAL 数据库；上游组提供共享的只读滑动窗口快照。Axum HTTP 服务与 UDP DNS 服务并行运行，原生 HTML/CSS/JavaScript 和仓库内图表模块通过 `include_str!` 嵌入二进制。

**Tech Stack:** Rust 1.85+、Tokio、Axum、rusqlite（bundled SQLite）、Serde/serde_json、Hickory Proto、原生 HTML/CSS/JavaScript。

## Global Constraints

- 默认 `[dashboard] listen = "0.0.0.0:8080"`、`database_path = "dnsbuffer.db"`、`retention_days = 7`，其中 `0` 表示永久保留。
- 不保存客户端 IP；只保存 DNS 最终响应 answer 区中的 A、AAAA 地址。
- HTTP 无认证且只读；文档必须明确仅适合受信任局域网。
- 页面和图表不能依赖 CDN、Node.js 运行时或外部静态文件。
- 页面每 5 秒轮询，明细默认每页 50 条且最大 200 条。
- 统计队列满或 SQLite 写入失败不得延迟、失败或改变 DNS 响应。
- 所有时间存为 UTC；前端仅在展示时转换为浏览器本地时区。
- 现有用户改动不得回退；每项提交只暂存该任务文件。

## 文件结构

- `src/dashboard/mod.rs`：公共类型、`Dashboard` 组合根和模块导出。
- `src/dashboard/store.rs`：SQLite schema、迁移、写线程、清理和只读查询。
- `src/dashboard/upstreams.rs`：跨主/后备组汇总上游滑动窗口快照。
- `src/dashboard/http.rs`：Axum 路由、参数验证、JSON 错误和嵌入资源响应。
- `src/dashboard/assets/index.html`：单页语义结构。
- `src/dashboard/assets/style.css`：桌面/移动端运维界面样式。
- `src/dashboard/assets/chart.js`：项目内固定版本的轻量 Canvas 三线图模块。
- `src/dashboard/assets/app.js`：API 轮询、搜索、分页和 DOM 渲染。
- `src/config.rs`、`config.example.toml`：仪表板配置。
- `src/pipeline.rs`：在所有查询出口生成查询事件。
- `src/stats.rs`、`src/upstream/group.rs`、`src/lib.rs`：上游快照和组合装配。
- `src/main.rs`：数据库、HTTP、DNS 服务生命周期。
- `README.md`、`Dockerfile`：部署、端口、持久卷和风险说明。
- `tests/dashboard.rs`：HTTP 与持久化集成测试。

---

### Task 1: 仪表板配置与依赖

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/config.rs`
- Modify: `config.example.toml`

**Interfaces:**
- Consumes: 现有 `Config` TOML 加载与 `example_config_stays_valid` 测试。
- Produces: `DashboardConfig { listen: SocketAddr, database_path: PathBuf, retention_days: u32 }`，后续组合根直接读取 `config.dashboard`。

- [ ] **Step 1: 写失败的配置测试**

在 `src/config.rs` 测试模块增加：

```rust
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test config::tests::dashboard -- --nocapture`

Expected: FAIL，提示 `Config` 没有 `dashboard` 字段或类型不存在。

- [ ] **Step 3: 添加依赖和最小配置实现**

在 `Cargo.toml` 添加：

```toml
axum = "0.8"
chrono = { version = "0.4", features = ["serde"] }
rusqlite = { version = "0.37", features = ["bundled"] }
serde_json = "1"
```

在 `src/config.rs` 导入 `PathBuf`，给 `Config` 增加 `#[serde(default)] pub dashboard: DashboardConfig`，并定义：

```rust
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

fn default_dashboard_listen() -> SocketAddr { "0.0.0.0:8080".parse().unwrap() }
fn default_database_path() -> PathBuf { PathBuf::from("dnsbuffer.db") }
fn default_retention_days() -> u32 { 7 }
```

在 `Config::validate` 中拒绝 `database_path.as_os_str().is_empty()`；在 `config.example.toml` 写入默认 `[dashboard]` 示例。

- [ ] **Step 4: 验证配置测试和示例配置**

Run: `cargo test config::tests -- --nocapture`

Expected: PASS，包含新测试和 `example_config_stays_valid`。

- [ ] **Step 5: 提交**

```bash
git add Cargo.toml Cargo.lock src/config.rs config.example.toml
git commit -m "feat: add dashboard configuration"
```

### Task 2: 查询事件与 SQLite schema

**Files:**
- Create: `src/dashboard/mod.rs`
- Create: `src/dashboard/store.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `DashboardConfig.database_path` 和 Hickory `Message` 响应数据。
- Produces: `QueryEvent`；`Store::open(&Path) -> Result<Store>`；`Store::insert_events(&[QueryEvent]) -> Result<()>`；schema v1 的三个统计表和 IP 关联表。

- [ ] **Step 1: 写 schema 和事务一致性的失败测试**

在 `src/dashboard/store.rs` 先加入测试模块，使用临时目录下唯一数据库路径（测试结束删除），测试：

```rust
#[test]
fn insert_persists_log_ips_and_both_aggregates() {
    let (_guard, store) = test_store("insert");
    let event = QueryEvent {
        timestamp_ms: 1_753_488_000_000,
        domain: "example.com".into(),
        query_type: "A".into(),
        response_code: "NOERROR".into(),
        duration_ms: 12,
        blocked: false,
        cache_hit: true,
        response_ips: vec!["1.1.1.1".into(), "1.1.1.1".into(), "2606:4700::1111".into()],
    };
    store.insert_events(&[event]).unwrap();
    let conn = store.connect().unwrap();
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_logs"), 1);
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM query_response_ips"), 2);
    assert_eq!(scalar(&conn, "SELECT total_queries FROM query_hourly_stats"), 1);
    assert_eq!(scalar(&conn, "SELECT cache_hits FROM query_daily_stats"), 1);
}

#[test]
fn rejects_database_with_future_schema_version() {
    let (guard, store) = test_store("future");
    store.connect().unwrap().pragma_update(None, "user_version", 999).unwrap();
    drop(store);
    assert!(Store::open(guard.path()).unwrap_err().to_string().contains("newer schema"));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test dashboard::store::tests -- --nocapture`

Expected: FAIL，`dashboard` 模块、`Store` 和 `QueryEvent` 尚不存在。

- [ ] **Step 3: 实现 schema v1 和原子写入**

在 `src/dashboard/mod.rs` 定义：

```rust
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryEvent {
    pub timestamp_ms: i64,
    pub domain: String,
    pub query_type: String,
    pub response_code: String,
    pub duration_ms: u64,
    pub blocked: bool,
    pub cache_hit: bool,
    pub response_ips: Vec<String>,
}

pub mod store;
```

`Store` 保存 `PathBuf`，每次 `connect()` 启用 `foreign_keys=ON`、`journal_mode=WAL`、`busy_timeout=2s`。迁移事务创建：

```sql
CREATE TABLE query_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp_ms INTEGER NOT NULL,
  domain TEXT NOT NULL,
  query_type TEXT NOT NULL,
  response_code TEXT NOT NULL,
  duration_ms INTEGER NOT NULL CHECK(duration_ms >= 0),
  blocked INTEGER NOT NULL CHECK(blocked IN (0,1)),
  cache_hit INTEGER NOT NULL CHECK(cache_hit IN (0,1))
);
CREATE INDEX query_logs_time_idx ON query_logs(timestamp_ms DESC, id DESC);
CREATE INDEX query_logs_domain_idx ON query_logs(domain);
CREATE TABLE query_response_ips (
  query_id INTEGER NOT NULL REFERENCES query_logs(id) ON DELETE CASCADE,
  ip TEXT NOT NULL,
  PRIMARY KEY(query_id, ip)
);
CREATE INDEX query_response_ips_ip_idx ON query_response_ips(ip);
CREATE TABLE query_hourly_stats (
  bucket_ms INTEGER PRIMARY KEY,
  total_queries INTEGER NOT NULL,
  blocked_queries INTEGER NOT NULL,
  cache_hits INTEGER NOT NULL
);
CREATE TABLE query_daily_stats (
  bucket_ms INTEGER PRIMARY KEY,
  total_queries INTEGER NOT NULL,
  blocked_queries INTEGER NOT NULL,
  cache_hits INTEGER NOT NULL
);
PRAGMA user_version = 1;
```

`insert_events` 在单个事务中插入明细，以 `INSERT OR IGNORE` 插入去重 IP，并对 UTC 小时/天桶执行 `INSERT ... ON CONFLICT(bucket_ms) DO UPDATE`。用 `timestamp_ms.div_euclid(3_600_000) * 3_600_000` 计算小时桶，使用 Chrono UTC 日期零点计算天桶。

- [ ] **Step 4: 运行 schema 测试**

Run: `cargo test dashboard::store::tests -- --nocapture`

Expected: PASS，重复 IP 仅保存一次，明细和两个聚合各增加一次。

- [ ] **Step 5: 提交**

```bash
git add src/dashboard/mod.rs src/dashboard/store.rs src/lib.rs
git commit -m "feat: persist dashboard query events"
```

### Task 3: 保留期清理与只读统计查询

**Files:**
- Modify: `src/dashboard/store.rs`

**Interfaces:**
- Consumes: Task 2 schema 和 `QueryEvent`。
- Produces: `Store::cleanup(retention_days, now_ms)`、`trend(retention_days, now_ms)`、`queries(page, page_size, search)`、`rankings()` 及其可序列化 DTO。

- [ ] **Step 1: 写失败的清理、趋势、搜索与排名测试**

覆盖以下精确行为：

```rust
#[test]
fn cleanup_removes_expired_logs_and_ips_but_zero_keeps_all() { /* 插入 8 天前和当前事件；cleanup(7, now) 后仅当前事件/IP，cleanup(0, now) 不再删除 */ }

#[test]
fn trend_uses_hours_through_15_days_days_after_and_30_days_for_forever() {
    assert_eq!(store.trend(7, now).unwrap().granularity, "hour");
    assert_eq!(store.trend(16, now).unwrap().granularity, "day");
    let forever = store.trend(0, now).unwrap();
    assert_eq!(forever.granularity, "day");
    assert_eq!(forever.start_ms, day_bucket(now) - 29 * DAY_MS);
}

#[test]
fn queries_search_domain_or_ip_without_duplicates_and_escape_wildcards() {
    /* example.com -> [1.1.1.1, 1.0.0.1]，literal_percent.com -> [2001:db8::1] */
    assert_eq!(store.queries(1, 50, Some("example")).unwrap().total, 1);
    assert_eq!(store.queries(1, 50, Some("1.1")).unwrap().total, 1);
    assert_eq!(store.queries(1, 50, Some("%")).unwrap().total, 1);
}

#[test]
fn rankings_return_top_20_with_stable_ties_and_counters() { /* 插入 21 个域名并验证长度、计数和 domain ASC 平局 */ }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test dashboard::store::tests -- --nocapture`

Expected: FAIL，缺少四个读取/清理方法和 DTO。

- [ ] **Step 3: 实现清理和查询 DTO**

定义 `TrendResponse { start_ms, end_ms, granularity, buckets }`、`TrendBucket`、`QueryPage { page, page_size, total, records }`、`QueryRecord`、`Ranking`。实现：

```sql
DELETE FROM query_logs WHERE timestamp_ms < ?1;
DELETE FROM query_hourly_stats WHERE bucket_ms + 3600000 <= ?1;
DELETE FROM query_daily_stats WHERE bucket_ms + 86400000 <= ?1;
```

趋势读取对应聚合表后在 Rust 中从首桶到末桶补零。查询明细使用参数绑定的 `LIKE ? ESCAPE '\'`，搜索文本依次把 `\`、`%`、`_` 转义，条件使用：

```sql
WHERE domain LIKE ? ESCAPE '\' COLLATE NOCASE
   OR EXISTS (
       SELECT 1 FROM query_response_ips ip
       WHERE ip.query_id = query_logs.id
         AND ip.ip LIKE ? ESCAPE '\' COLLATE NOCASE
   )
```

总数与列表共用同一条件；列表按 `timestamp_ms DESC, id DESC`，IP 通过第二条参数查询批量加载并按文本排序。排名执行 `GROUP BY domain ORDER BY COUNT(*) DESC, domain ASC LIMIT 20`，并以 `SUM(blocked)`、`SUM(cache_hit)` 返回计数。

- [ ] **Step 4: 验证存储行为**

Run: `cargo test dashboard::store::tests -- --nocapture`

Expected: PASS；趋势空桶连续，搜索 `%` 只匹配字面量，IP 多重命中不重复。

- [ ] **Step 5: 提交**

```bash
git add src/dashboard/store.rs
git commit -m "feat: query and retain dashboard history"
```

### Task 4: 非阻塞写线程与查询管线采集

**Files:**
- Modify: `src/dashboard/mod.rs`
- Modify: `src/dashboard/store.rs`
- Modify: `src/pipeline.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: Task 2 `QueryEvent`、`Store::insert_events`。
- Produces: `Recorder::try_record(QueryEvent)`；`StoreWorker::start(Store, retention_days)`；`PipelineParts.recorder: Recorder`；所有客户端查询出口的统一事件采集。

- [ ] **Step 1: 写失败的事件提取与故障隔离测试**

在 `pipeline.rs` 增加捕获 recorder 测试替身，验证 hosts、blocked、cache-hit、upstream 和 SERVFAIL。至少用以下响应验证 IP：

```rust
assert_eq!(event.domain, "example.com");
assert_eq!(event.query_type, "A");
assert_eq!(event.response_code, "NOERROR");
assert_eq!(event.response_ips, vec!["1.2.3.4"]);
assert!(event.blocked);
assert!(!event.cache_hit);
```

增加一个容量为 1 且不消费的 recorder，连续执行查询并断言第二次查询仍在测试超时内返回正确 DNS 响应。

- [ ] **Step 2: 运行管线测试确认失败**

Run: `cargo test pipeline::tests -- --nocapture`

Expected: FAIL，`PipelineParts` 无 recorder，事件未产生。

- [ ] **Step 3: 实现 recorder、批量工作线程和统一完成函数**

`Recorder` 内含 `tokio::sync::mpsc::Sender<QueryEvent>`，`try_record` 仅调用 `try_send`。`StoreWorker` 使用容量 4096 通道，在 `spawn_blocking` 内运行当前线程 Tokio blocking recv 或标准通道循环，每批最多 128 条或等待 100ms 后写入；写失败记录告警并继续。启动时清理一次，每 24 小时清理一次。

将 `Pipeline::handle` 重构为只有一次事件提交：记录 `Instant`，各分支返回 `(Message, blocked, cache_hit)`，最终调用：

```rust
fn record_query(&self, query: &Message, response: &Message, started: Instant, blocked: bool, cache_hit: bool) {
    let Some(q) = query.queries.first() else { return; };
    let mut ips: Vec<String> = response.answers.iter().filter_map(|record| match record.data() {
        RData::A(value) => Some(IpAddr::V4((*value).into()).to_string()),
        RData::AAAA(value) => Some(IpAddr::V6((*value).into()).to_string()),
        _ => None,
    }).collect();
    ips.sort();
    ips.dedup();
    self.recorder.try_record(QueryEvent { /* UTC timestamp、规范化 name、qtype、rcode、elapsed */ response_ips: ips });
}
```

`spawn_refresh` 不持有 recorder。测试默认 parts 使用 `Recorder::disabled()`，避免改变无关测试行为。

- [ ] **Step 4: 验证管线和存储测试**

Run: `cargo test pipeline::tests dashboard::store::tests -- --nocapture`

Expected: PASS；队列不可用不影响响应，A/AAAA 提取正确，后台刷新不写事件。

- [ ] **Step 5: 提交**

```bash
git add src/dashboard/mod.rs src/dashboard/store.rs src/pipeline.rs src/lib.rs
git commit -m "feat: record DNS query history"
```

### Task 5: 上游滑动窗口快照

**Files:**
- Modify: `src/stats.rs`
- Create: `src/dashboard/upstreams.rs`
- Modify: `src/dashboard/mod.rs`
- Modify: `src/upstream/group.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: 现有 `UpstreamStats` 和 `UpstreamGroup` 成员统计。
- Produces: `UpstreamStats::snapshot() -> StatsSnapshot`；`UpstreamMetrics::snapshot() -> Vec<UpstreamSnapshot>`；`UpstreamGroup::new(..., metrics, group_kind)`。

- [ ] **Step 1: 写失败的统计快照测试**

在 `stats.rs` 增加：

```rust
#[test]
fn snapshot_distinguishes_no_success_from_cold_start_value() {
    let mut stats = UpstreamStats::new(4);
    stats.record_failure();
    let snap = stats.snapshot();
    assert_eq!(snap.samples, 1);
    assert_eq!(snap.successes, 0);
    assert_eq!(snap.avg_latency_ms, None);
    stats.record_success(Duration::from_millis(20));
    assert_eq!(stats.snapshot().avg_latency_ms, Some(20.0));
}
```

在 `upstream/group.rs` 验证一次成功、一次失败后共享 metrics 包含正确 `primary`/`fallback`、名称、样本数和失败率。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test stats::tests::snapshot upstream::group::tests::metrics -- --nocapture`

Expected: FAIL，快照类型和共享 metrics 尚不存在。

- [ ] **Step 3: 实现轻量共享快照**

定义：

```rust
#[derive(Debug, Clone, serde::Serialize)]
pub struct StatsSnapshot {
    pub samples: usize,
    pub successes: usize,
    pub failure_rate: f64,
    pub avg_latency_ms: Option<f64>,
}

#[derive(Clone, Default)]
pub struct UpstreamMetrics(Arc<Vec<MetricMember>>);

#[derive(Debug, Clone, serde::Serialize)]
pub struct UpstreamSnapshot {
    pub name: String,
    pub group: &'static str,
    pub samples: usize,
    pub successes: usize,
    pub failure_rate: f64,
    pub avg_latency_ms: Option<f64>,
}
```

让 group 成员的 stats 变为 `Arc<Mutex<UpstreamStats>>`，构建成员时把名称、组别和同一 Arc 注册到 `UpstreamMetricsBuilder`。主组使用 `primary`，后备组使用 `fallback`。快照逐个短暂加锁并立即复制值；锁中毒时跳过该项。

- [ ] **Step 4: 运行相关测试**

Run: `cargo test stats::tests upstream::group::tests -- --nocapture`

Expected: PASS；无成功样本平均值为 `null`，调度的冷启动 `avg_latency_ms()` 仍为 100。

- [ ] **Step 5: 提交**

```bash
git add src/stats.rs src/dashboard/mod.rs src/dashboard/upstreams.rs src/upstream/group.rs src/lib.rs
git commit -m "feat: expose upstream performance snapshots"
```

### Task 6: 只读 HTTP API 与嵌入资源路由

**Files:**
- Create: `src/dashboard/http.rs`
- Create: `src/dashboard/assets/index.html`
- Create: `src/dashboard/assets/style.css`
- Create: `src/dashboard/assets/chart.js`
- Create: `src/dashboard/assets/app.js`
- Modify: `src/dashboard/mod.rs`
- Create: `tests/dashboard.rs`

**Interfaces:**
- Consumes: `Store` 读取方法、`UpstreamMetrics::snapshot()`、`DashboardConfig.retention_days`。
- Produces: `http::router(state) -> axum::Router`；`http::serve(listener, state)`；四个 API 和嵌入资源路由。

- [ ] **Step 1: 写失败的路由集成测试**

在 `tests/dashboard.rs` 用临时数据库和 `tower::ServiceExt::oneshot` 测试：

```rust
#[tokio::test]
async fn queries_validate_pagination_and_search() {
    let app = test_router_with_event(event("example.com", &["1.1.1.1"]));
    let ok = app.clone().oneshot(Request::get("/api/dashboard/queries?page=1&page_size=50&search=1.1").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
    let bad = app.oneshot(Request::get("/api/dashboard/queries?page=0").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn serves_embedded_assets_and_method_errors() { /* /、CSS、JS 为正确 content-type；POST API 为 405；未知路径为 404 */ }
```

为测试添加 dev dependency：

```toml
tower = { version = "0.5", features = ["util"] }
```

- [ ] **Step 2: 运行集成测试确认失败**

Run: `cargo test --test dashboard -- --nocapture`

Expected: FAIL，HTTP 模块和 router 不存在。

- [ ] **Step 3: 实现 API、错误映射与静态资源**

定义 `HttpState { store, upstreams, retention_days }`。handler 内使用 `tokio::task::spawn_blocking` 调用 Store，数据库内部错误记录完整 tracing 日志，对客户端仅返回：

```json
{"error":"dashboard database unavailable"}
```

`QueryParams` 使用字符串或 Option 反序列化后显式校验 `page >= 1`、`1 <= page_size <= 200`、trim 后 search UTF-8 字符数不超过 253。API DTO 将 `timestamp_ms` 转为 UTC RFC3339；静态资源通过 `include_str!("assets/index.html")` 等编译嵌入，设置 `text/html; charset=utf-8`、`text/css; charset=utf-8` 和 `text/javascript; charset=utf-8`。

先创建最小资源：HTML 包含四个区域和搜索/分页控件，CSS/JS 可为空但必须能从路由加载；`chart.js` 顶部声明 `/* dnsbuffer chart module v1.0.0 */`。

- [ ] **Step 4: 验证 API 契约**

Run: `cargo test --test dashboard -- --nocapture`

Expected: PASS，含 domain/IP 搜索、分页边界、JSON content-type、404/405 和资源 content-type。

- [ ] **Step 5: 提交**

```bash
git add Cargo.toml Cargo.lock src/dashboard tests/dashboard.rs
git commit -m "feat: serve dashboard API"
```

### Task 7: 仪表板前端行为和响应式视觉

**Files:**
- Modify: `src/dashboard/assets/index.html`
- Modify: `src/dashboard/assets/style.css`
- Modify: `src/dashboard/assets/chart.js`
- Modify: `src/dashboard/assets/app.js`
- Modify: `tests/dashboard.rs`

**Interfaces:**
- Consumes: Task 6 四个 JSON API。
- Produces: 每 5 秒更新、域名/IP 防抖搜索、分页、Canvas 三线图、桌面和移动端完整页面。

- [ ] **Step 1: 扩展嵌入资源契约测试**

读取 `/` 和 `/assets/app.js` 响应体，断言 HTML 包含 `trend-chart`、`upstream-list`、`ranking-body`、`query-search`、`query-body`、`previous-page`、`next-page` 和可访问 label；JS 包含 `5000` 刷新周期、`AbortController`、`encodeURIComponent(state.search)` 和搜索后 `state.page = 1`。

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --test dashboard embedded_frontend_contract -- --nocapture`

Expected: FAIL，最小资源缺少完整 DOM 和行为标记。

- [ ] **Step 3: 实现图表和页面脚本**

`chart.js` 导出 `window.DnsTrendChart = { render(canvas, buckets) }`，按 devicePixelRatio 调整 Canvas，从最大总查询数计算 Y 比例，绘制网格、时间标签和三条线；空数据绘制“暂无查询数据”。固定颜色：总体 `#5eead4`、屏蔽 `#fb7185`、缓存 `#fbbf24`。

`app.js` 使用：

```javascript
const state = { page: 1, pageSize: 50, search: "", controllers: new Map() };
async function loadRegion(name, url, render) {
  state.controllers.get(name)?.abort();
  const controller = new AbortController();
  state.controllers.set(name, controller);
  const response = await fetch(url, { signal: controller.signal });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  render(await response.json());
}
function loadQueries() {
  const query = `page=${state.page}&page_size=${state.pageSize}&search=${encodeURIComponent(state.search)}`;
  return loadRegion("queries", `/api/dashboard/queries?${query}`, renderQueries);
}
setInterval(refreshAll, 5000);
```

输入事件使用 300ms 防抖，trim 后更新搜索、回到第一页并加载；上一页/下一页保持 search。所有用户数据仅用 `textContent` 创建节点，不使用 `innerHTML` 拼接。响应 IP 逐项展示；blocked/cache_hit 使用文字徽标；API 局部失败只更新对应 `.panel-error`，保留现有数据。

- [ ] **Step 4: 完成响应式 CSS 并验证资源测试**

CSS 使用深色背景、清晰焦点环、CSS grid；`@media (max-width: 760px)` 切为单列，`.table-scroll { overflow-x: auto; }`。运行：

Run: `cargo test --test dashboard -- --nocapture`

Expected: PASS，嵌入页面包含全部关键区域且 API 契约未回归。

- [ ] **Step 5: 提交**

```bash
git add src/dashboard/assets tests/dashboard.rs
git commit -m "feat: build embedded dashboard UI"
```

### Task 8: 服务组合、生命周期和端到端行为

**Files:**
- Modify: `src/dashboard/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/main.rs`
- Modify: `tests/dashboard.rs`
- Modify: `tests/forwarding.rs`

**Interfaces:**
- Consumes: Store worker、Recorder、HTTP state、UpstreamMetrics、现有 `build_pipeline`。
- Produces: `build_runtime(&Config) -> Result<AppRuntime>` 或等价组合结构；HTTP 和 UDP 并行启动且任一异常退出会终止进程。

- [ ] **Step 1: 写失败的组合测试**

创建临时 SQLite，构建完整 pipeline，发送一条 DNS 查询后轮询 store 最多 2 秒，断言明细和聚合出现；另验证 `build_pipeline` 返回的 metrics 能列出配置上游。更新现有所有 `PipelineParts` 构造增加 `Recorder::disabled()`。

- [ ] **Step 2: 运行编译和组合测试确认失败**

Run: `cargo test --no-run`

Expected: FAIL，`build_pipeline` 不能接收 recorder/metrics 或 main 尚未启动 HTTP。

- [ ] **Step 3: 实现组合根与并行生命周期**

把构建结果改为明确结构：

```rust
pub struct BuiltPipeline {
    pub pipeline: Arc<Pipeline>,
    pub upstream_metrics: UpstreamMetrics,
}

pub async fn build_pipeline(config: &Config, recorder: Recorder) -> Result<BuiltPipeline>;
```

`main` 初始化 Store 和 worker，绑定 `TcpListener` 后创建 HTTP future，再创建 DNS future：

```rust
let store = Store::open(&cfg.dashboard.database_path)?;
store.cleanup(cfg.dashboard.retention_days, Utc::now().timestamp_millis())?;
let (recorder, worker) = StoreWorker::start(store.clone(), cfg.dashboard.retention_days);
let built = build_pipeline(&cfg, recorder).await?;
let listener = TcpListener::bind(cfg.dashboard.listen).await?;
let http = dashboard::http::serve(listener, HttpState::new(store, built.upstream_metrics, cfg.dashboard.retention_days));
let dns = server::run_udp(cfg.server.listen, built.pipeline);
tokio::select! {
    result = http => result.context("dashboard HTTP server stopped")?,
    result = dns => result.context("DNS UDP server stopped")?,
}
worker.shutdown(Duration::from_secs(2));
```

确保绑定 HTTP 失败发生在 UDP 无限循环之前；关闭句柄通知 writer 并有限等待。

- [ ] **Step 4: 运行完整 Rust 测试**

Run: `cargo test -- --nocapture`

Expected: PASS，现有 DNS 测试和新增仪表板测试全部通过。

- [ ] **Step 5: 提交**

```bash
git add src/dashboard src/lib.rs src/main.rs src/pipeline.rs src/server.rs tests/dashboard.rs tests/forwarding.rs
git commit -m "feat: run DNS dashboard service"
```

### Task 9: 文档、容器部署和最终验证

**Files:**
- Modify: `README.md`
- Modify: `Dockerfile`
- Modify: `config.example.toml`

**Interfaces:**
- Consumes: 完整用户可见配置、端口和数据库行为。
- Produces: 可执行的局域网与 Docker 部署说明，无代码侧新接口。

- [ ] **Step 1: 写文档验收清单并检查当前内容缺失**

Run: `rg -n "dashboard|8080|retention_days|database_path|dnsbuffer.db" README.md Dockerfile config.example.toml`

Expected: README/Dockerfile 尚未完整覆盖 8080 映射、持久卷、无认证风险和保留语义。

- [ ] **Step 2: 更新 README、Dockerfile 和示例**

README 增加：访问 `http://<主机IP>:8080/`、四块数据口径、域名/IP 搜索、默认 7 天/0 永久、数据库相对工作目录、无认证风险。Docker 示例改为包含：

```bash
-p 8080:8080/tcp \
-v /var/lib/dnsbuffer:/var/lib/dnsbuffer \
```

并示例配置 `database_path = "/var/lib/dnsbuffer/dnsbuffer.db"`。Dockerfile 添加 `EXPOSE 53/udp 8080/tcp`；确认 distroless 运行用户对挂载目录需要有写权限。

- [ ] **Step 3: 格式化并运行静态检查**

Run: `cargo fmt --all -- --check`

Expected: PASS；若失败先运行 `cargo fmt --all`，再重复 check。

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: PASS，无 warning。

- [ ] **Step 4: 运行完整测试和本地冒烟测试**

Run: `cargo test --all-targets --all-features`

Expected: PASS。

使用临时配置（DNS `127.0.0.1:15353`、仪表板 `127.0.0.1:18080`、临时数据库）启动编译产物，发送 A 查询后验证：

```powershell
Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:18080/"
Invoke-RestMethod "http://127.0.0.1:18080/api/dashboard/queries?page=1&page_size=50&search=1.1"
Invoke-RestMethod "http://127.0.0.1:18080/api/dashboard/trend"
Invoke-RestMethod "http://127.0.0.1:18080/api/dashboard/upstreams"
Invoke-RestMethod "http://127.0.0.1:18080/api/dashboard/rankings"
```

Expected: 页面为 200；查询 API 返回发送的记录和响应 IP；其他 API 返回有效 JSON。停止进程后重启并确认记录仍存在。

- [ ] **Step 5: 提交文档和格式化变更**

```bash
git add README.md Dockerfile config.example.toml src tests Cargo.toml Cargo.lock
git commit -m "docs: document dashboard deployment"
```

## 计划自审结果

- 规格覆盖：配置、SQLite 迁移、查询采集、响应 IP、清理、趋势、排名、上游窗口、API、嵌入前端、轮询、搜索、移动端、Docker 和故障隔离均有对应任务。
- 边界明确：统计写入与 DNS 服务隔离；主/后备指标共享同一统计对象；搜索总数与列表使用同一谓词。
- 类型一致：后续任务统一使用 `QueryEvent`、`Store`、`Recorder`、`StoreWorker`、`UpstreamMetrics`、`HttpState` 和 `BuiltPipeline`。
- 范围控制：不增加认证、客户端 IP、配置编辑、任意日期筛选、导出或排名汇总表。
