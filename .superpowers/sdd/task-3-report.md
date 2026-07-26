# Task 3 Report: Retention Cleanup and Read-Only Statistics

## Status

Implemented the Task 3 requirements exclusively in `src/dashboard/store.rs`:

- Added serializable trend, query page/record, and ranking DTOs.
- Added retention cleanup with `retention_days == 0` as a no-op and transactional deletion of expired logs and aggregate buckets. Response IPs are removed by the existing foreign-key cascade.
- Added hourly trends for 1-15 days, daily trends above 15 days, and a 30-day daily window for retention 0. Missing buckets are returned with zero counters.
- Added paginated query history with shared search filtering for count and records, literal escaping of SQLite LIKE `\\`, `%`, and `_`, domain/IP substring matching, deduplication through `EXISTS`, deterministic record order, and separately loaded text-sorted IPs.
- Added stable top-20 domain rankings with total, blocked, and cache-hit counters.
- Added checked time/range arithmetic so extreme inputs return errors rather than panic.

## TDD Evidence

### RED

Command:

```text
cargo test dashboard::store::tests -- --nocapture
```

Observed expected compile failure after adding tests first: Rust reported missing methods `Store::cleanup`, `Store::trend`, `Store::queries`, and `Store::rankings` (18 `E0599` errors after correcting two test-local ambiguous integer literals). No production implementation existed at this point.

### GREEN

After the minimal implementation, the first run produced 10 passes and one meaningful failing search test. The test fixture used names such as `literal_percent.com` instead of containing literal wildcard characters. Correcting the fixture to `literal%.com` and `literal_.com` made the intended wildcard-escaping assertions valid.

Fresh specified command after implementation and formatting:

```text
cargo test dashboard::store::tests -- --nocapture
```

Result: 11 passed, 0 failed, 88 filtered out for the library tests; all other invoked test binaries also had 0 failures.

## Verification

- `rustfmt --edition 2024 src/dashboard/store.rs`: passed.
- `git diff --check`: passed; Git emitted only the existing line-ending conversion warning.
- `cargo test dashboard::store::tests -- --nocapture`: passed, 11/11 store tests.
- `cargo test`: passed, 99 unit tests plus 5 integration tests, 0 failures; doc tests had 0 failures.
- Cargo consistently warned that `C:\\Users\\user\\.cargo\\config` is deprecated in favor of `config.toml`; this is external to the task/worktree.

## Self-Review

- Scope: only `src/dashboard/store.rs` and this required report were changed.
- Cleanup: cutoff arithmetic is checked; all deletes share one transaction; retention 0 exits before arithmetic or database mutation.
- Trend: granularity boundaries are exact, retention 0 returns exactly 30 daily buckets, range and bucket arithmetic are checked, and every bucket from start through end is emitted.
- Queries: count and list interpolate the same fixed filter string; user input remains parameter-bound; backslash is escaped before wildcard characters; `EXISTS` prevents duplicate records from multiple matching IPs; record and IP ordering match the brief.
- Rankings: SQL uses `COUNT(*) DESC, domain ASC LIMIT 20` and sums both counters.
- DTOs: all produced DTOs derive `serde::Serialize` and expose the required data.

## Concerns

- Very large nonzero retention values are rejected instead of attempting an impractically large allocation; this is intentional boundary safety.
- IP attachment currently scans the page records for each returned IP. Page sizes are expected to be bounded by the caller, and the brief does not specify validation limits; no additional API policy was introduced.
- Workspace-wide `cargo fmt -- --check` reports pre-existing formatting differences in many out-of-scope files. The changed task file was formatted directly and passes its scoped formatting check.

## Commit

Pending at report creation; the final commit hash is reported in the final response and Git history.
