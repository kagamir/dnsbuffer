# Task 6 Report

## Implemented

- Added `dashboard::http::{HttpState, router, serve}` with read-only Axum routes for trend, queries, rankings, and upstream snapshots.
- Moved every Store operation used by HTTP handlers into `tokio::task::spawn_blocking`; `HttpState` owns `Arc<Store>` and cloneable upstream metrics.
- Added explicit `page`, `page_size`, and trimmed `search` validation, including malformed/duplicate/unknown query rejection and the 253-Unicode-character limit.
- Converted query and trend millisecond timestamps to RFC3339 UTC and removed raw millisecond fields from API output.
- Added consistent sanitized database failures (`500 {"error":"dashboard database unavailable"}`) while logging full internal errors.
- Embedded the placeholder dashboard HTML, CSS, and JavaScript with `include_str!`, complete asset routes, required content types, four regions, and search/pagination controls.
- Added `tower` integration-test support and coverage for API contracts, database failures, static resources, JSON content types, 404, and 405.

## TDD Evidence

1. Initial integration test failed to compile because `dashboard::http` did not exist.
2. Minimal implementation produced a failing RFC3339 assertion (`.000Z` instead of canonical `Z`); timestamp formatting was corrected.
3. Added malformed-query regression coverage; it failed first for unknown parameters, then for invalid UTF-8 percent encoding. Explicit extraction/error mapping was added until it passed.

## Verification

- `cargo test --test dashboard -- --nocapture`: 5 passed, 0 failed.
- `cargo test`: 117 unit tests, 10 integration tests, and doc tests passed; 0 failed.
- `rustfmt --edition 2024 --check src/dashboard/http.rs tests/dashboard.rs`: passed.
- `cargo clippy --all-targets -- -D warnings`: blocked by two pre-existing warnings in `src/bootstrap.rs:110` and `src/filter.rs:206`, outside Task 6 scope.

## Scope

- No pre-existing modifications in `src/config.rs`, `src/server.rs`, or `tests/forwarding.rs` were changed.
- Existing untracked dashboard database files were left untouched.

## Review Follow-up

- Added HTTP-layer checked pagination offset validation before `spawn_blocking`; multiplication overflow and offsets beyond `i64` now return JSON 400 rather than database 500. The `u64::MAX` regression test first reproduced the incorrect 500 response.
- Extended query integration coverage through Axum extraction and SQLite for a percent-encoded non-ASCII domain containing a literal `+`, plus IP search.
- Asserted the complete trend, upstream, rankings, and query response contracts, including parseable RFC3339 UTC timestamps, ordering/counters, nullable latency, and response IPs.
- Read embedded asset bodies and asserted the four regions, controls, script references, and chart module version declaration.
- Moved test database setup and event insertion into `spawn_blocking`; made the existing upstream metrics builder registration API public as the minimal production construction boundary needed by external consumers and integration tests.
- Deliberately did not add a production JoinError fault-injection interface. Panic-path injection would pollute the API solely for testing; the remaining risk is limited to static inspection of the shared `database_call` JoinError mapping.

### Follow-up Verification

- `cargo test --test dashboard -- --nocapture`: 6 passed, 0 failed.
- `cargo test`: 117 unit tests, 11 integration tests, and doc tests passed; 0 failed.
