# dnsbuffer 设计方案

**日期**: 2026-07-22
**状态**: 已批准，待实现

## 1. 项目目标

用 Rust 实现一个 DNS 代理软件，监听本地 UDP 53 端口，将查询通过加密上游（DoH，支持 HTTP/2、HTTP/3 与 ECH）转发，并提供本地 hosts、广告屏蔽、乐观缓存、EDNS 客户端子网、后备 DNS 等能力。

## 2. 需求清单（来源）

1. 监听 UDP 53 端口的 DNS 代理。
2. 上游 DNS 支持 DoH（HTTP/2、HTTP/3），调用时支持 ECH。
3. 上游用加权随机算法选择「失败最少、平均查找时间最低」的服务器。
4. Bootstrap DNS 支持 IP、DoH、DoT；非 IP 时需注明域名及其对应 IP。
5. 支持自定义 hosts 直接返回解析结果。
6. 广告屏蔽支持 adblock 语法或 Hosts 语法的文件地址，也支持自定义豁免地址。
7. 乐观缓存：缓存解析过的地址，配置限制条数，FIFO 规则；命中直接返回，随后调用上游更新并重新入队，删除旧缓存。（原始需求提及 SQLite，经讨论为响应速度改用纯内存链式哈希表实现相同语义，见第 3、7.2 节。）
8. 支持 EDNS 客户端子网（ECS）。
9. 后备 DNS（支持 IP、DoH、DoT）：当上游全部失效或不响应时提供查询服务。

## 3. 关键决策（brainstorm 结论）

| 决策点 | 选择 |
|--------|------|
| DNS 协议栈 | 混合方案：`hickory-proto` 做报文编解码；上游连接层自建（rustls + hyper）以获得 ECH/HTTP3 |
| 配置格式 | TOML |
| HTTP/3 | HTTP/3 优先，失败回退 HTTP/2 |
| ECH 配置来源 | 静态配置优先；缺失时经 bootstrap 查 HTTPS/SVCB 记录动态获取；都没有则回退普通 TLS |
| 平台/形态 | 跨平台前台进程，不内置守护进程化 |
| ECS 策略 | 自动注入（可配置固定子网或自动探测出口子网），默认剥离客户端真实来源 |
| 广告屏蔽命中响应 | 返回空地址 `0.0.0.0` / `::` |
| 缓存存储 | **纯内存链式哈希表**（`hashlink::LinkedHashMap` / 等价 HashMap+侵入双向链表），不落盘，退出即丢失；O(1) 增删查，避免 SQL 与序列化开销 |

## 4. 总体架构与技术栈

tokio 异步、跨平台、前台运行。核心抽象是 `Resolver` trait：

```rust
#[async_trait]
trait Resolver {
    async fn resolve(&self, query: &Message) -> Result<Message>;
}
```

DoH / DoT / 明文 UDP 三种上游实现共用它，于是「上游组」「bootstrap」「后备 DNS」都是同一套解析器的不同配置组合，避免重复代码。

**依赖选型：**

- `tokio` — 运行时、UDP/TCP 监听
- `hickory-proto` — DNS 报文编解码（仅用 wire 编解码与 `Message`/`Record` 类型，不用其高层 resolver/client）
- `rustls`（启用 ECH 特性）+ `hyper` + `h3` / `quinn` — 自建上游连接层，获得 HTTP/3 与 ECH
- `hashlink`（`LinkedHashMap`）— 纯内存乐观缓存（保持插入顺序，O(1) 增删查/淘汰）
- `arc_swap` — 广告屏蔽规则集的无锁热替换
- `humantime`（或等价）— 解析 `update_interval` 等时长配置
- `serde` + `toml` — 配置
- `tracing` — 结构化日志
- `async-trait` — trait 中的 async 方法

## 5. 模块划分与职责

```
src/
  main.rs         入口：加载配置 → 构建各组件 → 启动 server
  config.rs       TOML 配置结构 + 启动时校验
  server.rs       UDP:53 监听（+ TCP:53 处理 truncation）；每查询 spawn task
  pipeline.rs     单次查询编排（hosts→filter→cache→upstream→fallback）
  resolver.rs     Resolver trait 定义
  upstream/
    mod.rs        上游组管理 + 加权随机选择 + 统计聚合
    doh.rs        DoH 客户端（H3 优先 / H2 回退 / ECH）
    dot.rs        DoT 客户端（rustls TLS over TCP）
    plain.rs      明文 UDP/IP 解析器（bootstrap / fallback 用）
    selector.rs   加权随机算法
  bootstrap.rs    解析上游域名 IP（IP/DoH/DoT）+ 查 HTTPS 记录取 ECHConfig
  cache.rs        纯内存链式哈希表乐观缓存（FIFO 限条数）
  hosts.rs        自定义 hosts 精确 / 通配匹配
  filter.rs       广告屏蔽（adblock/hosts 语法，本地+远程 URL 定时更新）+ 豁免列表
  ecs.rs          EDNS 客户端子网注入
  stats.rs        上游滑动窗口统计（失败数 / 平均延迟）
```

每个模块单一职责、可独立测试。`pipeline.rs` 只做编排，不含具体协议逻辑。

## 6. 查询处理管线（数据流）

UDP 包 →（hickory-proto 解码）→ pipeline，顺序：

1. **hosts 匹配** → 命中直接构造应答返回。
2. **广告屏蔽匹配**（且不在豁免列表）→ 命中返回空地址 `0.0.0.0` / `::`。
3. **缓存查询（乐观）** → 命中即返回旧值；若已过期，额外触发后台异步刷新任务。
4. **上游查询** → 加权随机选一个上游解析器，注入 ECS，成功则写缓存并返回。
5. **上游全部失败/超时** → 切到后备 DNS 组。
6. 后备也失败 → 返回 `SERVFAIL`。

→（编码）→ UDP 应答。

## 7. 关键算法

### 7.1 加权随机选择（selector.rs）

每个上游维护滑动窗口统计（近 N 次的失败率 `f` 与平均延迟 `t_avg`）。权重：

```
w = 1 / ((t_avg_ms + ε) × (1 + k·f))
```

延迟越低、失败越少权重越大；按权重随机抽取（而非总选最优，以保留探测与负载分散）。冷启动给默认中值权重。`k`、窗口大小 `N` 可配置。

### 7.2 乐观缓存（cache.rs）

纯内存链式哈希表（`hashlink::LinkedHashMap`，或语义等价的 HashMap + 侵入双向链表），保持插入顺序。条目结构约为：

```rust
struct CacheKey {   // 作为 map 的 key
    name: Name,     // qname
    qtype: RecordType,
}
struct CacheEntry {
    message: Message,   // 缓存的应答报文（无需序列化）
    expires_at: Instant, // TTL 到期时间
}
```

- **命中即返回**（不管 TTL 是否过期），保证低延迟；读取用 `get`，**不改动队列顺序**（保持 FIFO 而非 LRU）。
- **过期命中**额外触发后台刷新：上游拿到新结果后 `remove(key)` 再 `insert` 到队尾（O(1)）。
- **FIFO 限条数**：插入后若总数超过配置上限，`pop_front` 淘汰最旧条目（O(1)）。
- **并发访问**：用 `Mutex<LinkedHashMap>` 包裹即可——每次操作是纳秒级 map 操作、锁持有极短。若日后需要极致吞吐，可按 key 哈希分成 N 个桶各自加锁。无 SQL、无序列化开销、无 C 依赖。

### 7.3 ECH（doh.rs）

- 静态配置的 `ECHConfigList`（base64）优先。
- 缺失时经 bootstrap 查上游域名的 HTTPS/SVCB 记录，取 `ech` 参数。
- 都拿不到则回退普通 TLS，并记 `warn` 日志。

### 7.4 ECS 注入（ecs.rs）

默认剥离客户端真实来源；按配置的固定子网，或启动时自动探测出口 IP 得到的 `/24`（IPv4）、`/56`（IPv6），写入 OPT 记录的 ECS option。可配置完全禁用。

### 7.5 规则源加载与定时更新（filter.rs）

广告屏蔽规则源（`adblock.rule_source`）支持两种形态，可混用：

- **本地路径（`path`）**：启动时读取，随进程存活。
- **远程 URL（`url`）**：启动时异步拉取一次；若设了 `update_interval`（如 `"24h"`），后台定时任务按周期重新拉取。

要点：

- **拉取通道**：复用上游连接层的 `hyper` HTTPS 客户端下载规则文件。URL 域名的解析走 **bootstrap 解析器**（避免依赖尚未就绪的主管线，也防止「解析规则源要先加载规则」的环）。
- **热替换**：filter 内部编译后的规则集用 `arc_swap::ArcSwap<RuleSet>` 持有，后台拉取解析完成后原子替换指针；查询读路径无锁、零停顿。
- **失败降级**：远程拉取或解析失败时**保留上一次成功的规则集**并记 `warn`；首次拉取即失败则该源暂为空，等下个周期重试。
- **不落盘**：远程规则仅驻留内存，与缓存策略一致，不做磁盘持久化。
- **解析统一**：无论来源，加载后统一解析为 adblock 语法与 hosts 语法两类规则，合并进同一匹配结构（域名后缀匹配 + adblock 规则匹配），豁免列表（`allowlist`）优先短路。

## 8. Bootstrap（bootstrap.rs）

用于解析上游 DoH 域名的 IP，以及查询 HTTPS 记录获取 ECHConfig。支持三种形态：

- **IP**：直接使用，无需解析。
- **DoH / DoT**：配置里必须注明域名及其对应 IP 地址（否则无法建立首个加密连接，形成先有鸡还是先有蛋）。

Bootstrap 解析器同样是 `Resolver` trait 的实现，复用 `plain.rs` / `doh.rs` / `dot.rs`。

## 9. 后备 DNS（fallback）

独立的解析器组，配置形态与上游一致（IP / DoH / DoT）。仅当主上游组全部失败或超时才启用，复用上游组的 `Resolver` 抽象与调用逻辑。

## 10. 错误处理与可观测性

- 上游每次超时/协议错误计入 stats，驱动权重下降；绝不 panic。
- 分级回退链：主上游组 → 后备组 → `SERVFAIL`。
- `tracing` 结构化日志，可配 level；关键路径打点（命中来源、选中上游、延迟）。
- 配置校验在启动时完成，非法配置直接报错退出。

## 11. 配置结构（TOML 概览）

```toml
[server]
listen = "0.0.0.0:53"
tcp = true

[cache]
max_entries = 10000

[ecs]
mode = "auto"          # auto | fixed | disabled
fixed_subnet = ""      # mode=fixed 时使用，如 "1.2.3.0/24"

[adblock]
allowlist = ["example.com"]
block_response = "zero"          # zero(0.0.0.0/::)

# 规则源：可以是本地路径或远程 URL，二选一填写
[[adblock.rule_source]]
path = "/path/to/adblock.txt"    # 本地文件，随进程启动加载

[[adblock.rule_source]]
url = "https://example.com/easylist.txt"   # 远程规则
update_interval = "24h"          # 更新周期；省略则仅启动时拉取一次

[[hosts]]
name = "router.local"
addrs = ["192.168.1.1"]

[[upstream]]
type = "doh"
url = "https://dns.example/dns-query"
ech = ""               # 可选，base64 ECHConfigList
http3 = true

[bootstrap]
# 用于解析上游域名
[[bootstrap.server]]
type = "doh"
url = "https://bootstrap.example/dns-query"
domain = "bootstrap.example"
ips = ["1.1.1.1"]      # 非 IP 类型必须注明

[[fallback]]
type = "dot"
addr = "9.9.9.9:853"
domain = "dns.quad9.net"
ips = ["9.9.9.9"]
```

（最终字段以实现为准，此处示意分层结构。）

## 12. 测试策略

遵循 TDD，先写测试再实现。

- **单元测试**：配置解析、加权选择分布、缓存 FIFO 淘汰与乐观刷新、filter/hosts 匹配、DNS 编解码往返、ECS 注入。
- **集成测试**：起本地 mock 上游（可控延迟/失败），验证回退链、乐观缓存刷新、加权倾向。

## 13. 非目标（YAGNI）

- 不内置守护进程化 / 服务封装（交给 systemd 等外部工具）。
- 缓存不落盘、无跨重启持久化。
- 不实现 Web 管理界面。
