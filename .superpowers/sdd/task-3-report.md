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

## Review Fix: Bounded Read Queries

### Scope and Root Cause

- `Store::trend` checked whether bucket arithmetic fit integer types but did not apply a resource bound before `Vec::with_capacity`. An arithmetically valid large retention could therefore allocate or iterate an unsafe number of buckets.
- `Store::queries` converted any `u64` page size to a SQL limit without enforcing the specified 1-200 storage-layer range. A large returned page could also make the second IP query exceed SQLite's bind parameter limit.
- The prior panic-boundary test asserted only the outer `catch_unwind` result, so a non-panicking but incorrect successful inner result was not detected.

### RED Evidence

Tests were changed before production code to add:

- `queries_reject_zero_or_oversized_page_sizes`, covering 0, 201, and the valid boundary 200.
- `trend_rejects_huge_but_arithmetically_valid_ranges_without_panicking`, using 100,000 daily buckets and asserting outer `Ok` plus inner `Err`.
- Stronger `time_boundary_inputs_return_errors_instead_of_panicking` assertions for every outer and inner result.

Command:

```text
cargo test dashboard::store::tests -- --nocapture
```

Observed result: 10 passed, 3 failed. The failures showed that page size 0 was accepted, the huge valid trend returned `Ok`, and `trend(1, i64::MAX)` returned `Ok` rather than an error. These were the expected missing-validation failures.

### GREEN Evidence

Minimal production changes:

- Added `MAX_TREND_BUCKETS = 10_000`, allowing roughly 27 years of daily data while bounding response allocation and iteration. The limit is checked before time-range work, database access, or allocation.
- Added `MAX_QUERY_PAGE_SIZE = 200` and reject page sizes outside `1..=200` before offset calculation or database access.
- Validate `now_ms` with `DateTime::<Utc>::from_timestamp_millis` so unsupported extreme timestamps return errors consistently.
- The huge-range regression uses `now_ms = 0`, ensuring its error comes from the bucket limit rather than timestamp validation.

Fresh specified command:

```text
cargo test dashboard::store::tests -- --nocapture
```

Result: 13 passed, 0 failed, 88 filtered out; all invoked binaries had 0 failures.

Fresh full command:

```text
cargo test
```

Result: 101 unit tests and 5 integration tests passed, 0 failed; doc tests had 0 failures.

### Self-Review

- The trend bucket limit is applied to the final bucket count for every granularity and precedes `Vec::with_capacity`.
- Normal trend behavior remains unchanged: 1-15 days produce at most 360 hourly buckets, retention 0 produces 30 daily buckets, and ordinary multi-year daily retention remains accepted below 10,000 days.
- A maximum page of 200 records produces at most 200 bind parameters in the IP query; invalid sizes fail before SQL execution.
- Boundary tests now prove both no unwind and a returned application error.
- Only `src/dashboard/store.rs` and this required report were changed.

### Review Fix Commit

Pending at report update; the final commit hash is reported in the final response and Git history.

### Remaining Concerns

- The 10,000-bucket API bound is deliberately independent of product retention configuration: data may be retained longer, but one trend response cannot request more than 10,000 buckets.
- Cargo continues to emit the external user-level deprecation warning for `C:\\Users\\user\\.cargo\\config`; it is unrelated to this task.
