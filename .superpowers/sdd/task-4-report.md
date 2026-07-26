# Task 4 Report

## Status

Implemented non-blocking DNS query history recording and the SQLite store worker in the four files listed by the brief.

## RED

- Added pipeline tests for hosts, blocked, cache hit, upstream, SERVFAIL, one event per client query, final-answer-only normalized A/AAAA extraction, background refresh exclusion, and full/closed queue isolation.
- Added store worker tests for shutdown flushing and cleanup while idle.
- `cargo test pipeline::tests -- --nocapture` initially failed to compile because `Recorder`, `StoreWorker`, and `PipelineParts.recorder` did not exist.
- The worker test then exposed a panic from constructing a Tokio timer outside the worker's current-thread runtime. Moving the complete timed receive batch into `runtime.block_on` fixed the root cause.
- The first full `cargo test` exposed concurrent integration-test initialization failing with `database is locked`. `Store::connect` was executing `PRAGMA journal_mode=WAL` before setting `busy_timeout`; setting the timeout first fixed that initialization race.

## GREEN

- `cargo test pipeline::tests -- --nocapture`: 7 passed, 0 failed.
- `cargo test dashboard::store::tests -- --nocapture`: 15 passed, 0 failed.
- `cargo test --test forwarding`: 5 passed, 0 failed.
- `cargo test`: 106 library tests and 5 integration tests passed; 0 failed. Doc tests also passed.
- `git diff --cached --check`: passed with no whitespace errors.
- `cargo clippy --all-targets -- -D warnings`: blocked by two pre-existing `collapsible_if` findings in `src/bootstrap.rs:110` and `src/filter.rs:206`; no task-4 finding remained after fixing the pipeline findings.

## Implementation Notes

- `Recorder::try_record` calls only `tokio::sync::mpsc::Sender::try_send`; full and closed queues drop events without affecting DNS responses.
- Full/closed warnings are transition-limited to avoid per-query log flooding.
- Pipeline completion converges on one `record_query` call for each valid client query. Background refresh has no recorder.
- Event IPs come only from final `response.answers`, include only A/AAAA, use `IpAddr` canonical formatting, and are sorted/deduplicated.
- `StoreWorker` uses a dedicated standard thread and a current-thread Tokio runtime, so blocking receive and SQLite work never occupy an application async worker.
- Batches contain at most 128 events and wait at most 100 ms. Cleanup runs at startup and every 24 hours, including when no query arrives.
- The recorder owns the worker shutdown guard. Dropping the final recorder closes the channel, flushes pending events, and waits at most two seconds before detaching with a warning.

## Commit

- `121ec67 feat: record DNS query history`
- The report was created in that commit and updated immediately afterward with the resulting commit ID.

## Concerns

- Queue overflow intentionally loses history events to preserve DNS availability.
- A worker that cannot stop within two seconds is detached; the process can continue shutting down, but that final batch may not persist.
- Clippy remains globally red only because the two unrelated pre-existing findings are outside the brief's allowed files.
