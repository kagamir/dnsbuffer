# dnsbuffer

A local DNS proxy written in Rust. It listens on UDP port 53 and forwards queries over an encrypted upstream (DoH / DoT), with built-in optimistic caching, ad blocking, custom hosts, EDNS Client Subnet, and fallback DNS.

## Features

- **DoH upstream**: strictly picks the protocol per config — HTTP/2 by default, or HTTP/3 (QUIC) only when `http3 = true`, with no protocol downgrade; connection reuse and multiplexing; keep-alive for long-lived connections (H2 ping / QUIC keep-alive 15s), liveness checks before reuse, automatic reconnect-and-retry on failure, so the first request after a long idle period no longer waits out the full timeout; you can set `ip` to a single server IP (v4 or v6), or leave it blank to resolve via bootstrap, with resolved results ordered by the dialing address-family preference (`prefer_ipv6`, IPv4-first by default)
- **ECH (Encrypted Client Hello)**: a statically configured base64 value takes priority; when absent, it is fetched dynamically from HTTPS/SVCB records via bootstrap; if neither is available, it falls back to plain TLS with a warning
- **DoT upstream**: rustls TLS + RFC 7858 length-prefixed framing; `ip` specifies the server IP, and the port is written in `domain` (default 853)
- **Smart scheduling**: each upstream maintains sliding-window statistics (failure rate x average latency) and is chosen by weighted random selection with weight `w = 1/((t_avg+ε)(1+k·f))`, automatically down-weighting on failure
- **Bootstrap DNS**: supports IP / DoH / DoT forms; non-IP forms must explicitly note the domain's corresponding IP
- **Hedged retries**: if the primary upstream attempt does not return within `hedged_retry_ms` (default 1000ms), a new attempt is launched in parallel without cancelling the in-flight one (re-weighted selection, very likely switching upstreams); whichever returns first wins, until the `upstream_timeout_ms` budget is exhausted
- **Fallback DNS**: automatically takes over after the entire primary upstream group fails or times out (IP / DoH / DoT)
- **Optimistic cache**: pure in-memory LRU (hashlink chained hash table) — a hit returns immediately (even if expired), an expired hit triggers a background async refresh, and the least-recently-used entry is evicted when the limit is exceeded; only NoError responses are cached; friendly to low-memory machines — when the OS refuses a memory allocation it is treated as a full cache and evicts by LRU to make room instead of crashing
- **Custom hosts**: exact match + `*.` wildcard, answered directly and locally
- **Ad blocking**: a subset of adblock syntax (`||domain^`, `@@||domain^` exceptions) + hosts syntax + plain domain lists; local files and remote URLs can be mixed, and remote sources support periodic hot updates (lock-free swap via ArcSwap); a hit returns `0.0.0.0` / `::`; the allowlist takes priority
- **EDNS Client Subnet (ECS)**: setting `fixed_subnet` injects that subnet, otherwise ECS is not used; the client's own ECS is always stripped to protect privacy
- **Robustness**: response id validation across the whole chain, a total timeout budget per query, and any upstream failure degrades to SERVFAIL rather than crashing
- **Query dashboard**: a built-in web page shows query trends, query details, domain rankings, and upstream sliding-window status, with query records stored in SQLite

## Building

Requires Rust 1.85+ (2024 edition).

```bash
cargo build --release
# Artifact: target/release/dnsbuffer
```

## Running

```bash
# Listening on port 53 requires privileges, or grant it via setcap:
sudo setcap cap_net_bind_service=+ep target/release/dnsbuffer

dnsbuffer --config /etc/dnsbuffer/config.toml
```

- The config path defaults to `config.toml` (set with `-c` / `--config`)
- The log level is set in the config file via `[log] level` (default `info`); the `RUST_LOG` environment variable takes priority, which is handy for temporary debugging, e.g. `RUST_LOG=debug dnsbuffer ...`
- Level conventions: startup messages ("dnsbuffer starting" / "listening on udp") are WARN, configuration issues are WARN, and runtime fluctuations such as request failures and retries/fallback switches are INFO — setting `level = "warn"` keeps only startup and configuration warnings
- The dashboard is reachable by default at `http://<host IP>:8080/`. It has no authentication; setting `[dashboard] listen` to `0.0.0.0:8080` exposes the query history to every reachable network. You should use a firewall to restrict TCP 8080 to trusted subnets; if you need additional access control, consider an authenticating reverse proxy
- The SQLite database is created and migrated at startup; if the path is not writable, the directory does not exist, or database initialization fails, the whole program fails to start and the DNS service does not start either

## Docker

Multi-arch images (`linux/amd64`, `linux/arm64`) are built automatically on each GitHub Release and pushed to GHCR:

```bash
docker pull ghcr.io/kagamir/dnsbuffer:latest
```

```bash
docker run -d --name dnsbuffer \
  --restart unless-stopped \
  -p 53:53/udp \
  -p 8080:8080/tcp \
  -v $(pwd)/config.toml:/opt/dnsbuffer/config.toml:ro \
  -v $(pwd)/data/:/opt/dnsbuffer/data/ \
  ghcr.io/kagamir/dnsbuffer:latest
```
- If you mount local files for remote rule sources (`[[adblock.rule_source]] path = ...`), `-v` them into the container as well
- Available tags: `latest`, `vX.Y.Z`, `X.Y` (e.g. `v0.1.0`, `0.1`)

## Configuration

See [`config.example.toml`](config.example.toml) for a complete example. Every section except `[server]` and `[[upstream]]` can be omitted.

```toml
[server]
listen = "0.0.0.0:53"        # Listen address (UDP only)
query_timeout_ms = 10000     # Total per-query timeout (ms), wrapping the entire upstream + fallback chain
prefer_ipv6 = false          # Address-family preference when dialing upstreams; true means IPv6-first (IPv4-first by default)
hedged_retry_ms = 1000       # Hedged retry interval (ms); 0 disables it

[log]
level = "info"               # error | warn | info | debug | trace; the RUST_LOG environment variable takes priority

[dashboard]
listen = "0.0.0.0:8080"      # No authentication; expose only to trusted networks, or use an authenticating reverse proxy
database_path = "data/dnsbuffer.db" # A relative path is based on the process working directory; the directory must already exist, and an initialization failure prevents the program from starting
retention_days = 7            # Allows 0-9999; 0 means keep forever

[cache]
max_entries = 10000          # Maximum number of LRU cache entries

[ecs]
# fixed_subnet = "203.0.113.0/24"   # If set, inject this subnet as ECS; if unset, ECS is not used

[selector]                   # Upstream weighted-random parameters
window = 32                  # Number of sliding-window samples
k = 5.0                      # Failure-rate penalty coefficient

[adblock]
allowlist = ["allowed.example.com"]   # Exempt domains (suffix match, takes priority over blocking)
block_response = "zero"               # A hit returns 0.0.0.0 / ::

[[adblock.rule_source]]      # Multiple rule sources allowed; choose either path or url
url = "https://example.com/easylist.txt"
update_interval = "24h"      # Update period (humantime format); if omitted, fetch only once at startup

[[adblock.rule_source]]
path = "/etc/dnsbuffer/extra-rules.txt"

[[hosts]]                    # Custom hosts, multiple entries allowed
name = "router.local"        # Supports "*.lab.example" wildcards
addrs = ["192.168.1.1", "fd00::1"]

# ---- Upstreams (multiple allowed, weighted-random scheduling within a group) ----

[[upstream]]
type = "doh"
url = "https://cloudflare-dns.com/dns-query"
http3 = true                 # http/2 by default; an explicit true uses http/3 only (strictly per config, no H2 fallback)
# ech = "base64..."          # Optional: static ECHConfigList; leave blank to fetch automatically via HTTPS records
# ip = "104.16.248.249"     # Optional: the IP for this domain (a single one, v4 or v6); leave blank to resolve the domain via bootstrap

[[upstream]]
type = "dot"
ip = "9.9.9.9"
domain = "dns.quad9.net"     # TLS SNI / certificate validation domain; the port goes here (e.g. "dns.quad9.net:8853"), default 853

[[upstream]]
type = "plain"               # Plaintext UDP (not recommended as a primary upstream)
addr = "1.1.1.1:53"

# ---- Bootstrap: resolves IPs for DoH domains, fetches ECH config, and resolves rule-source domains ----

[[bootstrap.server]]
type = "plain"
addr = "1.1.1.1:53"

# bootstrap can also use doh/dot, but ip must be given explicitly:
# [[bootstrap.server]]
# type = "doh"
# url = "https://bootstrap.example/dns-query"
# ip = "203.0.113.10"        # Required for non-IP forms

# ---- Fallback: takes over when the entire primary upstream group fails ----

[[fallback]]
type = "plain"
addr = "8.8.8.8:53"
```

### Dashboard data semantics

- **Query trends**: shows total queries, ad-blocked count, and cache hits over the retention period; when `retention_days` is 1 to 15 days it buckets by hour (up to 361 buckets), 16 days and above by day, and 0 (keep forever) always shows the most recent 30 UTC calendar buckets. For a positive retention period, the start and end times are the RFC 3339 origins of the first and last buckets, and all buckets intersecting the exact retention window are returned, so a non-hourly / non-midnight request usually yields one more partial bucket than the day-count conversion would suggest
- **Query details**: shows time, queried domain, type, response code, latency, block/cache status, and response IP; search does a full-substring match on domain and response IP, leading-wildcard semantics require scanning existing data, and indexes mainly guarantee pagination, joins, and exact prefix structure — they do not falsely claim to speed up arbitrary substring search. The API returns 50 per page by default, up to 200; it does not search client IP (dnsbuffer does not collect client IPs)
- **Domain rankings**: a top-20 list of individual domains sorted by query count, each with total query, block, and cache-hit counts
- **Upstream status**: comes from the in-process sliding window and reflects only the success rate and average latency of recent samples — it is not a historical aggregate over the SQLite retention period, and it is recomputed from scratch after a restart

`retention_days` defaults to 7, with an allowed range of 0 to 9999. When set to a positive value, the program periodically prunes query details older than that many days, deleting only aggregate buckets that fall entirely before the cutoff; when set to 0, SQLite history is never pruned. A single DNS packet is at most 65535 bytes, so the number of response IPs has a natural protocol ceiling; the service does not truncate IPs, to avoid breaking reverse-lookup semantics, while the HTTP page limit of 200 caps the response size. The relative path in `database_path` is relative to dnsbuffer's process working directory, not the directory containing the config file.

### Query processing order

```
UDP packet in → hosts match → ad blocking → LRU cache (optimistic hit, background refresh after expiry)
        → ECS injection → upstream group (weighted random + reselect on failure, hedged concurrent retry past hedged_retry_ms) → fallback group → SERVFAIL
```

## systemd service example

`/etc/systemd/system/dnsbuffer.service`:

```ini
[Unit]
Description=dnsbuffer DNS proxy
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/dnsbuffer --config /etc/dnsbuffer/config.toml
Restart=on-failure
RestartSec=2
# Bind port 53 without root
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

## Architecture

```
src/
  main.rs         Entry point: command line + config loading + startup
  config.rs       TOML config structures and validation
  server.rs       UDP listen loop (spawn per packet, a single-packet error does not interrupt the service)
  pipeline.rs     Query orchestration (hosts→filter→cache→ECS→upstream chain)
  resolver.rs     Resolver trait (a unified abstraction for all upstreams/groups/fallback chains)
  upstream/
    plain.rs      Plaintext UDP upstream
    dot.rs        DoT upstream
    doh.rs        DoH upstream (strictly H2 or H3 per config)
    doh3.rs       HTTP/3 connection wrapper (quinn/h3)
    selector.rs   Weighted-random selection (pure function)
    group.rs      Upstream group (stats feedback + reselect) + FallbackResolver
    hedged.rs     Hedged retries (concurrent re-attempt after hedged_retry_ms)
  bootstrap.rs    Upstream domain IP resolution + ECH fetch from HTTPS records
  cache.rs        Pure in-memory LRU optimistic cache
  hosts.rs        Custom hosts
  filter.rs       Ad blocking (parse/match/hot update)
  fetch.rs        Rule-file HTTP(S) fetching
  ecs.rs          EDNS Client Subnet
  stats.rs        Upstream sliding-window statistics
  tls.rs          rustls client configuration (including ECH)
```

Tests: `cargo test --all-targets --all-features` (unit + end-to-end integration, including mock DoT/DoH/H3/HTTP servers).

## Known limitations

- The cache is not persisted to disk and is empty on restart
- When a remote rule source fails to fetch, the last successful ruleset is retained; if the first fetch fails, that source is empty until the next cycle
- Background refresh of expired hot keys is not deduplicated while an upstream keeps failing (it is self-healing and stops once the upstream recovers)
