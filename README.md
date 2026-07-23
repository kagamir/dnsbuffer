# dnsbuffer

用 Rust 编写的本地 DNS 代理。监听 UDP 53 端口，把查询经加密上游（DoH / DoT）转发，内置乐观缓存、广告屏蔽、自定义 hosts、EDNS 客户端子网与后备 DNS。

## 特性

- **DoH 上游**：HTTP/3（QUIC）优先，失败运行时回退 HTTP/2；连接复用、多路复用；`ips` 支持 IPv4/IPv6 混填，拨号地址族偏好可配（`prefer_ipv6`，默认 IPv4 优先），bootstrap 解析结果同序
- **ECH（Encrypted Client Hello）**：静态配置 base64 优先，缺省时经 bootstrap 从 HTTPS/SVCB 记录动态获取；均不可用时回退普通 TLS 并告警
- **DoT 上游**：rustls TLS + RFC 7858 长度前缀帧
- **智能调度**：每个上游维护滑动窗口统计（失败率 × 平均延迟），按权重 `w = 1/((t_avg+ε)(1+k·f))` 加权随机选择，失败自动降权重选
- **Bootstrap DNS**：支持 IP / DoH / DoT 形态；非 IP 形态必须显式注明域名对应 IP
- **后备 DNS**：主上游组全部失败或超时后自动接管（IP / DoH / DoT）
- **乐观缓存**：纯内存 LRU（hashlink 链式哈希表）——命中即回（过期也返回），过期命中触发后台异步刷新，超限逐出最久未用；只缓存 NoError
- **自定义 hosts**：精确匹配 + `*.` 通配，直接本地应答
- **广告屏蔽**：adblock 语法子集（`||domain^`、`@@||domain^` 例外）+ hosts 语法 + 纯域名列表；本地文件与远程 URL 混用，远程源支持按周期热更新（ArcSwap 无锁替换）；命中返回 `0.0.0.0` / `::`；豁免列表优先
- **EDNS 客户端子网（ECS）**：`auto`（启动探测出口 IP，取 /24、/56，私网/CGNAT 自动禁用）、`fixed`（配置固定子网）、`disabled`；始终剥离客户端自带 ECS 保护隐私
- **健壮性**：响应 id 全链路校验、单查询总超时预算、任何上游故障均降级为 SERVFAIL 而非崩溃

## 构建

需要 Rust 1.85+（2024 edition）。

```bash
cargo build --release
# 产物：target/release/dnsbuffer
```

## 运行

```bash
# 监听 53 端口需要特权，或用 setcap 授权：
sudo setcap cap_net_bind_service=+ep target/release/dnsbuffer

dnsbuffer --config /etc/dnsbuffer/config.toml
```

- 配置路径默认 `config.toml`（`-c` / `--config` 指定）
- 日志级别用 `RUST_LOG` 控制（默认 `info`），如 `RUST_LOG=debug dnsbuffer ...`

## 配置

完整示例见 [`config.example.toml`](config.example.toml)。所有节除 `[server]` 与 `[[upstream]]` 外均可省略。

```toml
[server]
listen = "0.0.0.0:53"        # 监听地址（仅 UDP）
query_timeout_ms = 10000     # 单查询总超时（毫秒），包裹上游+后备整链
prefer_ipv6 = false          # 拨号上游的地址族偏好；true 则 IPv6 优先（默认 IPv4 优先）

[cache]
max_entries = 10000          # LRU 缓存最大条数

[ecs]
mode = "auto"                # auto | fixed | disabled
# fixed_subnet = "203.0.113.0/24"   # mode = "fixed" 时必填

[selector]                   # 上游加权随机参数
window = 32                  # 滑动窗口样本数
k = 5.0                      # 失败率惩罚系数

[adblock]
allowlist = ["allowed.example.com"]   # 豁免域（后缀匹配，优先于屏蔽）
block_response = "zero"               # 命中返回 0.0.0.0 / ::

[[adblock.rule_source]]      # 规则源可多个，path 与 url 二选一
url = "https://example.com/easylist.txt"
update_interval = "24h"      # 更新周期（humantime 格式）；省略则仅启动拉取一次

[[adblock.rule_source]]
path = "/etc/dnsbuffer/extra-rules.txt"

[[hosts]]                    # 自定义 hosts，可多条
name = "router.local"        # 支持 "*.lab.example" 通配
addrs = ["192.168.1.1", "fd00::1"]

# ---- 上游（可多个，同组内加权随机调度）----

[[upstream]]
type = "doh"
url = "https://cloudflare-dns.com/dns-query"
http3 = true                 # 默认 http/2；显式 true 才启用 H3（H3 优先、失败回退 H2）
# ech = "base64..."          # 可选：静态 ECHConfigList；留空自动经 HTTPS 记录获取
# ips = ["2606:4700::6810:f8f9", "104.16.248.249"]  # 可选：v4/v6 混填皆可，次序按 prefer_ipv6 整理；留空经 bootstrap 解析域名

[[upstream]]
type = "dot"
addr = "9.9.9.9:853"
domain = "dns.quad9.net"     # TLS SNI / 证书校验域名

[[upstream]]
type = "plain"               # 明文 UDP（不推荐作主上游）
addr = "1.1.1.1:53"

# ---- Bootstrap：为 DoH 域名解析 IP、拉取 ECH 配置、解析规则源域名 ----

[[bootstrap.server]]
type = "plain"
addr = "1.1.1.1:53"

# bootstrap 也可用 doh/dot，但必须显式给出 ips：
# [[bootstrap.server]]
# type = "doh"
# url = "https://bootstrap.example/dns-query"
# ips = ["203.0.113.10"]     # 非 IP 形态必填

# ---- 后备：主上游组全部失败时接管 ----

[[fallback]]
type = "plain"
addr = "8.8.8.8:53"
```

### 查询处理顺序

```
UDP 收包 → hosts 匹配 → 广告屏蔽 → LRU 缓存（乐观命中，过期后台刷新）
        → ECS 注入 → 上游组（加权随机 + 失败重选）→ 后备组 → SERVFAIL
```

## systemd 服务示例

`/etc/systemd/system/dnsbuffer.service`：

```ini
[Unit]
Description=dnsbuffer DNS proxy
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/dnsbuffer --config /etc/dnsbuffer/config.toml
Restart=on-failure
RestartSec=2
# 免 root 绑定 53 端口
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true
DynamicUser=true
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dnsbuffer
```

## 架构

```
src/
  main.rs         入口：命令行 + 配置加载 + 启动
  config.rs       TOML 配置结构与校验
  server.rs       UDP 监听循环（每包 spawn，单包错误不中断服务）
  pipeline.rs     查询编排（hosts→filter→cache→ECS→上游链）
  resolver.rs     Resolver trait（所有上游/组/回退链的统一抽象）
  upstream/
    plain.rs      明文 UDP 上游
    dot.rs        DoT 上游
    doh.rs        DoH 上游（H2 + H3 编排与回退）
    doh3.rs       HTTP/3 连接封装（quinn/h3）
    selector.rs   加权随机抽取（纯函数）
    group.rs      上游组（统计反馈 + 重选）+ FallbackResolver
  bootstrap.rs    上游域名 IP 解析 + HTTPS 记录 ECH 获取
  cache.rs        纯内存 LRU 乐观缓存
  hosts.rs        自定义 hosts
  filter.rs       广告屏蔽（解析/匹配/热更新）
  fetch.rs        规则文件 HTTP(S) 拉取
  ecs.rs          EDNS 客户端子网
  stats.rs        上游滑动窗口统计
  tls.rs          rustls 客户端配置（含 ECH）
```

测试：`cargo test`（70 项：单元 + 端到端集成，含 mock DoT/DoH/H3/HTTP 服务器）。

## 已知限制

- 缓存不落盘，重启即空
- 规则远程源拉取失败时保留上一次成功规则集；首次即失败则该源为空，等下个周期
- 过期热点键在上游持续故障时的后台刷新无去重（自愈型，上游恢复即停止）
