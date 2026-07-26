# Task 9 Report

## Changes

- Updated `README.md` with dashboard access, four data scopes, domain/response-IP search semantics, SQLite startup failure behavior, retention and trend granularity rules, relative database paths, unauthenticated `0.0.0.0` exposure risk, and Docker persistence requirements.
- Updated `Dockerfile` to expose `53/udp` and `8080/tcp`; documentation accurately states the current distroless/root runtime model and writable-volume requirement.
- Updated `config.example.toml` with dashboard security, database-path, and retention semantics.
- Replaced the stale shutdown `2s` warning with the structured `timeout_ms` field derived from `SERVICE_SHUTDOWN_TIMEOUT`.
- Applied repository-wide rustfmt because the required format check reported existing drift across the Rust tree.
- Fixed the two existing clippy `collapsible_if` warnings in `src/bootstrap.rs` and `src/filter.rs` with minimal condition merges.

## Verification

- `rg -n "dashboard|8080|retention_days|database_path|dnsbuffer.db" README.md Dockerfile config.example.toml`
  - Initial result: only four matches, all in `config.example.toml`; README and Dockerfile lacked the required deployment and behavior documentation.
- `cargo fmt --all -- --check`
  - Initial result: FAIL with repository-wide formatting differences, including `bootstrap.rs`, `cache.rs`, `ecs.rs`, `fetch.rs`, `filter.rs`, `hosts.rs`, and upstream modules.
- `cargo fmt --all`
  - Result: PASS (exit 0); Cargo emitted only the environment warning that `C:\Users\user\.cargo\config` is deprecated in favor of `config.toml`.
- `cargo fmt --all -- --check`
  - Final result: PASS (exit 0); same Cargo configuration deprecation warning.
- `cargo clippy --all-targets --all-features -- -D warnings`
  - Initial result: FAIL on exactly two existing `clippy::collapsible_if` warnings in `src/bootstrap.rs:117` and `src/filter.rs:213`.
  - Final result: PASS; `Finished dev profile` with no code warnings. Cargo still emitted the external `C:\Users\user\.cargo\config` deprecation warning before compilation.
- `node --test tests/frontend.test.js`
  - Result: PASS, 15 tests, 0 failures, 0 skipped.
- `node --check src/dashboard/assets/app.js`
  - Result: PASS (exit 0, no output).
- `node --check src/dashboard/assets/chart.js`
  - Result: PASS (exit 0, no output).
- `cargo test --all-targets --all-features`
  - Result: PASS: 120 library tests, 0 binary tests, 15 dashboard integration tests, and 5 forwarding integration tests; 140 total passed, 0 failed. Cargo emitted only the external config deprecation warning.
- `cargo build --all-features`
  - Result: PASS; debug binary built successfully.
- Real process smoke test with temporary config (`127.0.0.1:15353` DNS, `127.0.0.1:18080` dashboard, temporary SQLite database), a raw PowerShell UDP A query for local host entry `smoke.test`, and live HTTP API requests.
  - First run result: page HTTP 200; DNS response 44 bytes; query API search `1.1` returned one `smoke.test` record with response IP `1.1.1.1`; trend returned `hour` with 168 buckets; upstream and rankings endpoints returned valid JSON.
  - Restart result: query API still returned the same one record and response IP, confirming SQLite persistence.
- `docker version`
  - Result: NOT EXECUTED successfully because `docker` is not installed or available on PATH (`CommandNotFoundException`). The Dockerfile could not be built locally in this Windows environment.

## Unexecuted Items

- No Docker image build or container runtime smoke test was possible because the Docker CLI is unavailable.
- No separate Node package-manager check exists: the repository has no `package.json`; direct `node --test` and `node --check` commands were run instead.

## Review Follow-up

- Corrected the ranking description to one top-20 domain list with per-domain total, blocked, and cache-hit counters; there is no separate blocked-domain ranking.
- Made the unauthenticated exposure guidance explicit: restrict TCP 8080 to trusted networks with a firewall, with an authenticated reverse proxy as an optional additional control.
- Clarified that mounting `/var/lib/dnsbuffer` alone does not persist the built-in relative database path. The mounted configuration must set `database_path = "/var/lib/dnsbuffer/dnsbuffer.db"`.
- Removed assumptions about the distroless base image user. The Dockerfile does not override `USER`, and the volume must be writable by the image's actual runtime user.
- Removed the task-generated untracked `dnsbuffer.db`, `dnsbuffer.db-shm`, and `dnsbuffer.db-wal` files from the worktree.
- `rg -n "屏蔽域名排名|默认以 root|因此容器默认|防火墙|database_path = \"/var/lib/dnsbuffer/dnsbuffer.db\"|实际运行用户|域名前 20" README.md Dockerfile`
  - Result: PASS. Required firewall, absolute database path, actual-user writability, and top-20 ranking text is present; the inaccurate blocked-ranking and root-user claims are absent.
- `git diff --check`
  - Result: PASS with no whitespace errors. Git emitted only Windows line-ending conversion notices.
- `git status --short`
  - Result before commit: only `README.md`, `Dockerfile`, and this report are modified; the three untracked SQLite smoke artifacts are absent.
