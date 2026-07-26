### Task 8 Report

Implemented service composition and lifecycle ownership:

- Added `BuiltPipeline`; `build_pipeline` now receives the runtime-owned `Recorder` and returns the shared pipeline and upstream metrics.
- Removed hidden SQLite and worker initialization from pipeline construction and removed `cfg(test)` recorder behavior differences.
- Added `build_runtime` and `Runtime::run`: SQLite initialization and cleanup are fatal, HTTP binds before UDP starts, HTTP and DNS run under `tokio::select!`, and either service stopping returns a contextual error.
- Added HTTP and DNS startup logs.
- Explicit worker shutdown runs through `spawn_blocking`, including pipeline-build and HTTP-bind failure paths.
- Updated all pipeline builders and test `PipelineParts` values to provide a recorder explicitly.

TDD evidence:

- RED: `cargo test --no-run` failed because `PipelineParts` required the new recorder input.
- RED: runtime tests failed to compile because `dashboard::build_runtime` did not exist.
- GREEN: the complete pipeline test sends a real DNS query through a configured upstream and polls SQLite for both query details and aggregate data; it also checks configured upstream metrics.
- GREEN: lifecycle tests verify fatal SQLite initialization, fatal HTTP bind before DNS execution, and runtime failure when UDP exits.

Verification:

- `cargo test --no-run`: passed.
- `cargo test -- --nocapture`: passed, 133 tests total, 0 failed.
- `git diff --check`: passed.

### Review Fixes

- Runtime now pre-binds both TCP and UDP sockets and exposes their actual `port = 0` addresses through `http_addr()` and `dns_addr()`.
- Added graceful HTTP shutdown and cancellable UDP serving. `Runtime::run_until` signals the peer service when one service exits or external shutdown is requested, waits up to two seconds, then drains the writer without replacing the original service result.
- `StoreWorker::start` now propagates thread spawn failures through `Result` rather than panicking.
- `main` uses Ctrl-C as the external shutdown trigger.
- Added a real Runtime composition test starting from a nonexistent SQLite path, querying the pre-bound UDP address through a mock upstream, polling the real HTTP API until the event appears, and explicitly shutting down the runtime.

Review TDD evidence:

- RED: pre-bound address, `run_until`, cancellable UDP, and fallible worker tests failed to compile because the interfaces did not exist.
- RED: service completion test failed because normal service termination was accepted as success.
- GREEN: runtime, dashboard, forwarding, and full test suites passed after implementation.

Review verification:

- `cargo test --no-run`: passed.
- `cargo test --test dashboard -- --nocapture`: 13 passed, 0 failed.
- `cargo test --test forwarding -- --nocapture`: 5 passed, 0 failed.
- `cargo test -- --nocapture`: 137 passed, 0 failed.

### Second Review Fixes

- UDP query handlers are tracked in a `JoinSet`; shutdown and receive errors stop new reads, drain accepted handlers for up to two seconds, then abort and reap any remainder.
- Runtime selection is biased so ready HTTP/DNS results take precedence over external shutdown, with HTTP first for simultaneous service completion.
- `run_until` now accepts a shutdown future returning `Result<()>`, allowing Ctrl-C listener failures to propagate.
- Runtime E2E network activity is bounded by a two-second timeout.
- Added a writer-drain integration test that shuts down immediately after the DNS response, waits for Runtime to return, then reopens SQLite and verifies query details and aggregate persistence.
- Documented the Runtime consumption contract: coordinated cleanup requires `run` or `run_until`.

Second review TDD evidence:

- RED: the slow resolver test showed `run_udp_socket_until` returned while an accepted handler was still blocked.
- RED: shutdown tests failed to compile until `run_until` accepted a fallible shutdown future.
- GREEN: UDP drain and immediate writer flush tests pass.

### Final Review Fix

- External shutdown completion is stored as `First::Shutdown(Result<()>)`; both success and failure now trigger peer shutdown, bounded service drain, and writer shutdown before the original shutdown result is returned.
- Named the outer service grace deadline and set it to 2500ms so the UDP server's internal two-second handler abort/reap phase can complete first.
- Added a real shutdown-error regression test that queues a DNS event, returns an external shutdown error, verifies the original error, confirms the HTTP port closes, and checks the writer persisted the event after Runtime returns.

Final review TDD evidence:

- RED: the shutdown-error test hit its deadline because `?` returned from inside the `select!` branch before cleanup.
- GREEN: the same test passes after preserving the shutdown result through cleanup.
