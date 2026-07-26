# dnsbuffer

用 Rust 编写的本地 DNS 代理。监听 UDP 53 端口，把查询经加密上游（DoH / DoT）转发，内置乐观缓存、广告屏蔽、自定义 hosts、EDNS 客户端子网与后备 DNS。

## 特性

- **DoH 上游**：严格按配置选协议——默认 HTTP/2，`http3 = true` 则仅用 HTTP/3（QUIC），不做协议降级；连接复用、多路复用；长连接保活（H2 ping / QUIC keep-alive 15s），复用前检查连接死活、失败自动重连再试，长时间放置后首个请求不再死等超时；可用 `ip` 指定单个服务器 IP（v4/v6 皆可），留空经 bootstrap 解析，解析结果按拨号地址族偏好排序（`prefer_ipv6`，默认 IPv4 优先）
- **ECH（Encrypted Client Hello）**：静态配置 base64 优先，缺省时经 bootstrap 从 HTTPS/SVCB 记录动态获取；均不可用时回退普通 TLS 并告警
- **DoT 上游**：rustls TLS + RFC 7858 长度前缀帧；`ip` 指定服务器 IP，端口写在 `domain` 中（默认 853）
- **智能调度**：每个上游维护滑动窗口统计（失败率 × 平均延迟），按权重 `w = 1/((t_avg+ε)(1+k·f))` 加权随机选择，失败自动降权重选
- **Bootstrap DNS**：支持 IP / DoH / DoT 形态；非 IP 形态必须显式注明域名对应 IP
- **对冲式重试**：主上游尝试超过 `hedged_retry_ms`（默认 1000ms）未返回，即并行发起新尝试且不取消在途的（重新加权选择，大概率换上游）；任一返回即胜出，直到 `upstream_timeout_ms` 预算耗尽
- **后备 DNS**：主上游组全部失败或超时后自动接管（IP / DoH / DoT）
- **乐观缓存**：纯内存 LRU（hashlink 链式哈希表）——命中即回（过期也返回），过期命中触发后台异步刷新，超限逐出最久未用；只缓存 NoError；小内存机器友好——操作系统拒绝内存申请时视作缓存已满，按 LRU 逐出换空间而不是崩溃
- **自定义 hosts**：精确匹配 + `*.` 通配，直接本地应答
- **广告屏蔽**：adblock 语法子集（`||domain^`、`@@||domain^` 例外）+ hosts 语法 + 纯域名列表；本地文件与远程 URL 混用，远程源支持按周期热更新（ArcSwap 无锁替换）；命中返回 `0.0.0.0` / `::`；豁免列表优先
- **EDNS 客户端子网（ECS）**：配置 `fixed_subnet` 则注入该子网，不配置则不使用 ECS；始终剥离客户端自带 ECS 保护隐私
- **健壮性**：响应 id 全链路校验、单查询总超时预算、任何上游故障均降级为 SERVFAIL 而非崩溃
- **查询仪表板**：内置 Web 页面展示查询趋势、查询明细、域名排名和上游滑动窗口状态，查询记录保存在 SQLite

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
- 日志级别在配置文件 `[log] level` 设置（默认 `info`）；`RUST_LOG` 环境变量优先，便于临时调试，如 `RUST_LOG=debug dnsbuffer ...`
- 级别约定：启动信息（"dnsbuffer starting" / "listening on udp"）为 WARN，配置问题为 WARN，请求失败与重试/切换 fallback 等运行期波动为 INFO——设 `level = "warn"` 可只保留启动与配置告警
- 仪表板默认访问地址为 `http://<主机IP>:8080/`。它没有认证；将 `[dashboard] listen` 设为 `0.0.0.0:8080` 会向所有可达网络暴露查询历史。应使用防火墙将 TCP 8080 限制到可信网段；如需额外访问控制，可选用带认证的反向代理
- SQLite 数据库会在启动时创建并迁移；路径不可写、目录不存在或数据库初始化失败会使整个程序启动失败，DNS 服务也不会启动

## Docker

多架构镜像（`linux/amd64`、`linux/arm64`）随每次 GitHub Release 自动构建并推送到 GHCR：

```bash
docker pull ghcr.io/kagamir/dnsbuffer:latest
```

镜像基于 distroless（无 shell，仅含运行所需的 glibc），内置一份示例配置在 `/etc/dnsbuffer/config.toml`。生产环境请挂载自己的配置覆盖它：

```bash
docker run -d --name dnsbuffer \
  --restart unless-stopped \
  -p 53:53/udp \
  -p 8080:8080/tcp \
  -v /etc/dnsbuffer/config.toml:/etc/dnsbuffer/config.toml:ro \
  -v /var/lib/dnsbuffer:/var/lib/dnsbuffer \
  ghcr.io/kagamir/dnsbuffer:latest
```

- 配置中 `listen` 需为 `0.0.0.0:53`（容器内监听全部网卡），宿主侧用 `-p` 映射端口
- 仅挂载 `/var/lib/dnsbuffer` 不会自动改变数据库位置；挂载的配置还必须显式设置 `[dashboard] database_path = "/var/lib/dnsbuffer/dnsbuffer.db"`，SQLite 才会持久化到该卷。内置配置使用相对路径 `dnsbuffer.db`，其位置取决于容器工作目录，不保证落在挂载卷中
- 本 Dockerfile 没有覆盖基础镜像的 `USER`；挂载的 `/var/lib/dnsbuffer` 必须对镜像的实际运行用户可写，否则 SQLite 初始化失败会阻止程序启动
- 临时调试可加 `-e RUST_LOG=debug`
- 若挂载了远程规则源的本地文件（`[[adblock.rule_source]] path = ...`），一并 `-v` 进容器
- 可用标签：`latest`、`vX.Y.Z`、`X.Y`（如 `v0.1.0`、`0.1`）

## 配置

完整示例见 [`config.example.toml`](config.example.toml)。所有节除 `[server]` 与 `[[upstream]]` 外均可省略。

```toml
[server]
listen = "0.0.0.0:53"        # 监听地址（仅 UDP）
query_timeout_ms = 10000     # 单查询总超时（毫秒），包裹上游+后备整链
prefer_ipv6 = false          # 拨号上游的地址族偏好；true 则 IPv6 优先（默认 IPv4 优先）
hedged_retry_ms = 1000       # 对冲式重试间隔（毫秒）；0 禁用

[log]
level = "info"               # error | warn | info | debug | trace；RUST_LOG 环境变量优先

[dashboard]
listen = "0.0.0.0:8080"      # 无认证；仅暴露到可信网络，或使用带认证的反向代理
database_path = "dnsbuffer.db" # 相对路径以进程工作目录为基准；初始化失败将阻止程序启动
retention_days = 7            # 默认保留 7 天；0 表示永久保留

[cache]
max_entries = 10000          # LRU 缓存最大条数

[ecs]
# fixed_subnet = "203.0.113.0/24"   # 配置则注入该子网作为 ECS；不配置则不使用 ECS

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
http3 = true                 # 默认 http/2；显式 true 则仅用 http/3（严格按配置，不回退 H2）
# ech = "base64..."          # 可选：静态 ECHConfigList；留空自动经 HTTPS 记录获取
# ip = "104.16.248.249"     # 可选：该域名的 IP（仅一个，v4/v6 皆可）；留空经 bootstrap 解析域名

[[upstream]]
type = "dot"
ip = "9.9.9.9"
domain = "dns.quad9.net"     # TLS SNI / 证书校验域名；端口写在这里（如 "dns.quad9.net:8853"），默认 853

[[upstream]]
type = "plain"               # 明文 UDP（不推荐作主上游）
addr = "1.1.1.1:53"

# ---- Bootstrap：为 DoH 域名解析 IP、拉取 ECH 配置、解析规则源域名 ----

[[bootstrap.server]]
type = "plain"
addr = "1.1.1.1:53"

# bootstrap 也可用 doh/dot，但必须显式给出 ip：
# [[bootstrap.server]]
# type = "doh"
# url = "https://bootstrap.example/dns-query"
# ip = "203.0.113.10"        # 非 IP 形态必填

# ---- 后备：主上游组全部失败时接管 ----

[[fallback]]
type = "plain"
addr = "8.8.8.8:53"
```

### 仪表板数据口径

- **查询趋势**：按保留期展示总查询数、广告屏蔽数和缓存命中数；`retention_days` 为 1 至 15 天时按小时，16 天及以上按天，0（永久保留）固定展示最近 30 个日桶
- **查询明细**：显示时间、查询域名、类型、响应码、耗时、屏蔽/缓存状态和响应 IP；搜索匹配查询域名或响应 IP，不搜索客户端 IP（dnsbuffer 不采集客户端 IP）
- **域名排名**：单个域名前 20 列表，按查询次数排序，并为每个域名附带总查询、屏蔽和缓存命中计数
- **上游状态**：来自进程内滑动窗口，只反映最近样本的成功率和平均延迟，不是 SQLite 保留期内的历史汇总，重启后重新统计

`retention_days` 默认是 7。设置为正数时，程序会定期清理早于该天数的查询明细及已结束的聚合桶；设置为 0 时不清理 SQLite 历史。`database_path` 的相对路径相对于 dnsbuffer 的进程工作目录，而不是配置文件所在目录。

### 查询处理顺序

```
UDP 收包 → hosts 匹配 → 广告屏蔽 → LRU 缓存（乐观命中，过期后台刷新）
        → ECS 注入 → 上游组（加权随机 + 失败重选，超 hedged_retry_ms 对冲并发重试）→ 后备组 → SERVFAIL
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
    doh.rs        DoH 上游（按配置严格走 H2 或 H3）
    doh3.rs       HTTP/3 连接封装（quinn/h3）
    selector.rs   加权随机抽取（纯函数）
    group.rs      上游组（统计反馈 + 重选）+ FallbackResolver
    hedged.rs     对冲式重试（hedged_retry_ms 并发再尝试）
  bootstrap.rs    上游域名 IP 解析 + HTTPS 记录 ECH 获取
  cache.rs        纯内存 LRU 乐观缓存
  hosts.rs        自定义 hosts
  filter.rs       广告屏蔽（解析/匹配/热更新）
  fetch.rs        规则文件 HTTP(S) 拉取
  ecs.rs          EDNS 客户端子网
  stats.rs        上游滑动窗口统计
  tls.rs          rustls 客户端配置（含 ECH）
```

测试：`cargo test --all-targets --all-features`（单元 + 端到端集成，含 mock DoT/DoH/H3/HTTP 服务器）。

## 已知限制

- 缓存不落盘，重启即空
- 规则远程源拉取失败时保留上一次成功规则集；首次即失败则该源为空，等下个周期
- 过期热点键在上游持续故障时的后台刷新无去重（自愈型，上游恢复即停止）
