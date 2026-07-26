# Task 5 Report

## RED

- Added the `UpstreamStats::snapshot` test first; compilation failed because `StatsSnapshot` and `snapshot()` did not exist.
- Added the shared group metrics test first; compilation failed because the dashboard upstream metrics module, builder, and new `UpstreamGroup::new` arguments did not exist.
- The brief's combined Cargo command accepts only one test filter on this Cargo version, so the two filters were run separately.

## GREEN

- Added serializable `StatsSnapshot` and `UpstreamSnapshot` values.
- Kept scheduling's 100 ms cold-start value while snapshots report `None` until a success exists.
- Registered each primary/fallback member with `UpstreamMetricsBuilder` using the same `Arc<Mutex<UpstreamStats>>` used by scheduling.
- Snapshot locks are held only while copying one stats value; poisoned entries are skipped.
- Every completed group member attempt, including retries and independently completed hedged attempts, continues through `try_member` and records success or failure.
- Added `build_pipeline_with_metrics` while preserving the existing `build_pipeline` API.

## Tests

- `cargo test stats::tests -- --nocapture`: 6 passed.
- `cargo test upstream::group::tests -- --nocapture`: 8 passed.
- `cargo test dashboard::upstreams::tests -- --nocapture`: 1 passed.
- `cargo test`: 116 unit tests and 5 integration tests passed; doc tests passed.
- `git diff --check`: passed (line-ending conversion warnings only).
- `cargo clippy --all-targets -- -D warnings`: blocked by two pre-existing warnings in `src/bootstrap.rs` and `src/filter.rs`, outside this task's allowed files.

## Commit

- `feat: expose upstream performance snapshots`

## Concerns

- `std::sync::Mutex` remains the existing synchronization primitive. Locks cover only in-memory calculations/copies and no DNS or API I/O.
- Timed-out primary group futures can be cancelled before an underlying member attempt completes, so no final outcome exists to record for that cancelled attempt. Hedged attempts are spawned and naturally finish, and therefore do update group stats.
- Existing unrelated worktree changes in `src/config.rs`, `src/server.rs`, and `tests/forwarding.rs` were not staged or modified for this task.
