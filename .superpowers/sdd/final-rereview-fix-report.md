# Dashboard Final Re-review Fix Report

Date: 2026-07-26
Branch: `feature/dashboard`

## Commit

- `6431bd7 fix: enforce dashboard read deadlines`

## RED/GREEN Evidence

1. Exact schema primary keys and behavior
   - RED: `rejects_schema_v1_query_log_composite_primary_key` showed the bool PK check accepted a composite key. Equivalent tests covered hourly composite PK, reversed response-IP PK, and `INTEGER PRIMARY KEY DESC`.
   - GREEN: column metadata retains the exact PK ordinal. Every required table now has exactly the specified PK order and no extra PK columns. A rollback-only savepoint probe verifies production-style implicit `query_logs.id`, response-IP insertion, and both aggregate upserts; the DESC case fails because `id` is not the rowid alias. Normal reopen remains green and the probe leaves no rows.

2. Client deadline and resource lifetime
   - RED: `database_operation_returns_at_client_deadline_but_holds_permit_until_work_ends` initially had no JoinHandle deadline behavior. The locked-database integration test also demonstrated recovery requires releasing the exclusive connection.
   - GREEN: permit acquisition and JoinHandle waiting share one absolute deadline. The owned permit moves into the blocking closure, so a timed-out response detaches the worker without releasing its permit early. Store connections set `busy_timeout` from the remaining budget and install the progress handler; an already-expired deadline fails before querying. An exclusive SQLite lock returns sanitized 503 in under 2.5 seconds, permits recover after the worker ends, and a subsequent API request succeeds.

3. Explicit error classification
   - RED: no `DashboardReadError` or pure SQLite classifier existed; HTTP classified errors from wall-clock time after completion.
   - GREEN: `DashboardReadError::{Timeout, Database, Join}` drives response mapping. `OperationInterrupted`, `DatabaseBusy`, and `DatabaseLocked` become timeout only when the deadline is exhausted. Corruption remains database/500 regardless of deadline, JoinError always maps to 500, and permit/client timeout maps to fixed 503.

## Verification

- `cargo fmt --check`: pass.
- `cargo clippy --all-targets --all-features -- -D warnings`: pass.
- `cargo test --all-targets --all-features`: pass, 144 library + 16 dashboard integration + 5 forwarding tests, 0 failures.
- `node --check src/dashboard/assets/chart.js`: pass.
- `node --check src/dashboard/assets/app.js`: pass.
- `node --test tests/frontend.test.js`: pass, 15 tests, 0 failures.

## Residual Concerns

- SQLite busy handling and progress callbacks both use the same absolute deadline, but OS/filesystem calls inside SQLite are not preemptible. The HTTP client still receives 503 at the deadline while the detached blocking closure retains its semaphore permit until SQLite actually returns.
