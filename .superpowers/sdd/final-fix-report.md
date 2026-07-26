# Dashboard Final Fix Report

Date: 2026-07-26
Branch: `feature/dashboard`

## Commits

- `2b8c425 fix: harden dashboard data boundaries`
- `3583349 test: strengthen dashboard storage guarantees`

## RED/GREEN Evidence

1. Complete trend retention windows
   - RED: `finite_trend_includes_every_bucket_intersecting_the_retention_window` returned 24 buckets instead of 25. Existing finite trends started one bucket too late.
   - GREEN: non-aligned hourly and daily tests return 25 and 17 intersecting buckets; aligned finite boundaries include both endpoints; permanent retention remains exactly 30 daily buckets; cleanup overlap test passes.

2. Maximum trend/config consistency
   - RED: `dashboard_rejects_retention_above_9999_days` observed `Config::validate()` accepting 10,000.
   - GREEN: 10,000 is rejected with `dashboard retention_days must be between 0 and 9999`; 9,999 produces exactly 10,000 daily buckets and all legal configurations stay under the trend cap.

3. Schema v1 structure validation
   - RED: five malformed v1 cases (no tables, no response-IP table, missing key column, wrong FK action, missing index) all returned `Ok(Store)`; a correctly named index on wrong columns was also accepted.
   - GREEN: all malformed cases fail with the affected schema object in the error. Validation checks table presence, key column names/types/nullability/PK membership, response-IP `ON DELETE CASCADE`, required index names, and index columns. Normal reopen remains green.

4. Pipeline event start timestamp
   - RED: `event_timestamp_is_captured_before_resolver_completion` failed because event time equaled/followed delayed resolver completion.
   - GREEN: `Pipeline::handle` captures UTC milliseconds beside `Instant` on entry; delayed resolution proves event time precedes completion while duration remains monotonic.

5. Search/database resource control
   - RED: deadline API did not exist; the HTTP concurrency test did not compile without a bounded operation helper.
   - GREEN: all dashboard reads install a rusqlite progress handler against an absolute deadline. A recursive SQLite query is interrupted with `OperationInterrupted`; four operations consume all shared permits, the fifth receives sanitized 503, and permits remain held until blocking tasks actually finish. Search plan test confirms required indexes and explicitly proves leading-wildcard search remains a scan.

6. Static assets
   - RED: canonical asset test failed because HTML referenced `/chart.js` and `/app.js`.
   - GREEN: HTML and tested routes use `/assets/style.css`, `/assets/chart.js`, and `/assets/app.js`; root aliases remain compatible.

7. Response IP boundary
   - Accepted risk documented: a DNS packet is at most 65,535 bytes, naturally bounding addresses. IPs are not truncated so reverse-search data remains complete; HTTP pagination is capped at 200 records.

8. Repeated write errors
   - RED: `WarningRateLimit` did not exist.
   - GREEN: deterministic unit test proves repeated warnings are suppressed for 60 seconds. Write failure behavior remains best-effort and does not affect DNS.

## Verification

- `cargo fmt --check`: pass.
- `cargo clippy --all-targets --all-features -- -D warnings`: pass.
- `cargo test --all-targets --all-features`: pass, 137 library + 15 dashboard integration + 5 forwarding tests, 0 failures.
- `node --check src/dashboard/assets/chart.js`: pass.
- `node --check src/dashboard/assets/app.js`: pass.
- `node --test tests/frontend.test.js`: pass, 15 tests, 0 failures.

## Residual Concerns

- Arbitrary substring search intentionally remains a full scan. The four-read semaphore and two-second SQLite VM deadline bound service impact, but permanent retention can still yield frequent 503 responses on very large histories.
- SQLite busy waiting is configured for two seconds independently of the VM progress handler. The HTTP total deadline still bounds permit acquisition and reports timeout after synchronous work returns; SQLite progress interruption specifically bounds active VM execution.
