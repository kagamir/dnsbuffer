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
