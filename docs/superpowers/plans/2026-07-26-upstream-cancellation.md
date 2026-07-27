# Upstream Cancellation and H3 State Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound every hedged primary attempt to its parent resolution lifetime and prevent stale H3 failures from evicting newer healthy connections.

**Architecture:** Replace detached hedge tasks with a resolver-owned Tokio `JoinSet`, aborting and draining losers on every terminal outcome while relying on `JoinSet` drop for caller cancellation. Add a monotonically wrapping H3 connection generation and conditionally invalidate cached state only when a failure belongs to the currently cached generation.

**Tech Stack:** Rust 2024, Tokio 1.53 `JoinSet`, async-trait, Quinn 0.11, h3 0.0.8, anyhow.

## Global Constraints

- Preserve the current hedge schedule, fallback behavior, timeout values, and one H3 reconnect retry.
- Cancel all losing attempts immediately after success, final failure, budget expiry, or caller cancellation.
- Canceled attempts must not emit late upstream failures or update failure metrics.
- Do not add dependencies or change H2 invalidation behavior.
- Do not disable hedging for a single primary, add H3 phase diagnostics, or implement Happy Eyeballs.

---

### Task 1: Scope Hedged Attempts to the Parent Resolution

**Files:**
- Modify: `src/upstream/hedged.rs:10-214`

**Interfaces:**
- Consumes: `Arc<dyn Resolver>`, `Message`, `Duration`, and Tokio task scheduling.
- Produces: unchanged `HedgedResolver::new(inner, interval, max_wait) -> Self` and `Resolver::resolve(&self, query) -> Result<Message>` behavior, with child-task ownership and cancellation guarantees.

- [ ] **Step 1: Add lifecycle instrumentation to the test module**

Add imports and an instrumented resolver whose guard exposes whether an attempt remains alive:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

struct AttemptGuard {
    active: Arc<AtomicUsize>,
    dropped: Arc<Notify>,
}

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.dropped.notify_waiters();
    }
}

struct ControlledResolver {
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    dropped: Arc<Notify>,
    first_succeeds: bool,
}

#[async_trait]
impl Resolver for ControlledResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _guard = AttemptGuard {
            active: self.active.clone(),
            dropped: self.dropped.clone(),
        };
        if self.first_succeeds && call == 1 {
            return Ok(ok_resp(query));
        }
        std::future::pending().await
    }
}

async fn wait_for_count(value: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while value.load(Ordering::SeqCst) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("count reached expected value");
}
```

Keep the existing `AtomicUsize` import only once when merging imports.

- [ ] **Step 2: Write failing cancellation tests**

Add three tests. The success test starts a hanging first attempt and lets the second attempt win; the budget test checks that both hanging attempts are gone when the error returns; the caller-cancellation test aborts the outer task and checks its children are gone.

```rust
#[tokio::test]
async fn success_cancels_losing_attempts_before_returning() {
    let active = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(ControlledResolver {
        calls: AtomicUsize::new(0),
        active: active.clone(),
        dropped: Arc::new(Notify::new()),
        first_succeeds: true,
    });
    let hedged = HedgedResolver::new(
        resolver,
        Duration::from_millis(20),
        Duration::from_secs(1),
    );

    hedged.resolve(&sample_query()).await.expect("hedge wins");

    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn budget_expiry_cancels_all_attempts_before_returning() {
    let active = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(ControlledResolver {
        calls: AtomicUsize::new(0),
        active: active.clone(),
        dropped: Arc::new(Notify::new()),
        first_succeeds: false,
    });
    let hedged = HedgedResolver::new(
        resolver,
        Duration::from_millis(20),
        Duration::from_millis(50),
    );

    assert!(hedged.resolve(&sample_query()).await.is_err());

    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_outer_resolution_cancels_all_attempts() {
    let active = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(ControlledResolver {
        calls: AtomicUsize::new(0),
        active: active.clone(),
        dropped: Arc::new(Notify::new()),
        first_succeeds: false,
    });
    let hedged = Arc::new(HedgedResolver::new(
        resolver,
        Duration::from_millis(20),
        Duration::from_secs(5),
    ));
    let query = sample_query();
    let task = tokio::spawn({
        let hedged = hedged.clone();
        async move { hedged.resolve(&query).await }
    });
    wait_for_count(&active, 2).await;

    task.abort();
    assert!(task.await.expect_err("outer task aborted").is_cancelled());
    wait_for_count(&active, 0).await;
}
```

Fix `ControlledResolver`'s success condition so the first call hangs and the second call succeeds: use `if self.first_succeeds && call == 1`. The name indicates the configured successful hedge, not call zero.

- [ ] **Step 3: Run the new tests and verify the current implementation fails**

Run:

```powershell
cargo test upstream::hedged::tests::success_cancels_losing_attempts_before_returning -- --nocapture
cargo test upstream::hedged::tests::budget_expiry_cancels_all_attempts_before_returning -- --nocapture
cargo test upstream::hedged::tests::dropping_outer_resolution_cancels_all_attempts -- --nocapture
```

Expected: each test fails because detached attempts remain active after the parent returns or is aborted.

- [ ] **Step 4: Replace the detached channel tasks with a `JoinSet`**

Remove `spawn_attempt`'s channel parameter and make it spawn directly into a task set:

```rust
fn spawn_attempt(
    &self,
    attempts: &mut tokio::task::JoinSet<Result<Message>>,
    query: &Message,
) {
    let inner = self.inner.clone();
    let query = query.clone();
    attempts.spawn(async move { inner.resolve(&query).await });
}

async fn stop_attempts(attempts: &mut tokio::task::JoinSet<Result<Message>>) {
    attempts.abort_all();
    while attempts.join_next().await.is_some() {}
}
```

Rewrite `resolve` so every explicit terminal branch calls `stop_attempts` before returning:

```rust
async fn resolve(&self, query: &Message) -> Result<Message> {
    let mut attempts = tokio::task::JoinSet::new();
    let deadline = Instant::now() + self.max_wait;
    let mut next_hedge = Instant::now() + self.interval;
    let mut in_flight = 1usize;
    self.spawn_attempt(&mut attempts, query);

    loop {
        tokio::select! {
            biased;
            joined = attempts.join_next() => {
                in_flight -= 1;
                let result = match joined.expect("an attempt is in flight") {
                    Ok(result) => result,
                    Err(error) => Err(anyhow::anyhow!("hedged attempt task failed: {error}")),
                };
                match result {
                    Ok(response) => {
                        stop_attempts(&mut attempts).await;
                        return Ok(response);
                    }
                    Err(error) if in_flight == 0 => {
                        stop_attempts(&mut attempts).await;
                        return Err(error);
                    }
                    Err(error) => {
                        tracing::debug!("hedged attempt failed, others in flight: {error:#}");
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                let error = anyhow::anyhow!(
                    "no upstream reply within {:?} ({} hedged attempts in flight)",
                    self.max_wait,
                    in_flight
                );
                stop_attempts(&mut attempts).await;
                return Err(error);
            }
            _ = tokio::time::sleep_until(next_hedge) => {
                next_hedge += self.interval;
                in_flight += 1;
                tracing::debug!("hedging: launching parallel attempt #{in_flight}");
                self.spawn_attempt(&mut attempts, query);
            }
        }
    }
}
```

`JoinSet` aborts contained tasks from its `Drop` implementation, covering cancellation of the outer resolver future even when no explicit branch executes.

- [ ] **Step 5: Run all hedging tests**

Run:

```powershell
cargo test upstream::hedged::tests -- --nocapture
```

Expected: all hedging tests pass, including the three lifecycle regressions and the existing timing/failure tests.

- [ ] **Step 6: Commit the hedge lifecycle fix**

```powershell
git add "src/upstream/hedged.rs"
git commit -m "fix: cancel losing hedged requests"
```

---

### Task 2: Make H3 Cache Invalidation Generation-Safe

**Files:**
- Modify: `src/upstream/doh3.rs:14-165`
- Test: `src/upstream/doh3.rs:168-332`

**Interfaces:**
- Consumes: existing `H3State`, `H3Conn::connect`, and `H3Conn::request` internals.
- Produces: private `H3Conn::invalidate_generation(generation: u64)` and generation-tagged cached H3 state; public `H3Conn` behavior remains unchanged.

- [ ] **Step 1: Write focused failing tests for conditional invalidation**

Add a test-only state constructor and two tests inside `doh3::tests`. Reuse the mock H3 server to obtain valid connection/sender pairs, install known generations, and call the private invalidation method:

```rust
async fn connected_state(conn: &H3Conn, generation: u64) -> H3State {
    let (quinn, sender) = conn.connect().await.expect("connect mock H3 server");
    H3State {
        generation,
        conn: quinn,
        sender,
    }
}

#[tokio::test]
async fn stale_generation_does_not_clear_newer_connection() {
    let (addr, root) = spawn_mock_h3_server().await;
    let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
    let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
    let state = connected_state(&conn, 2).await;
    *conn.state.lock().await = Some(state);

    conn.invalidate_generation(1).await;

    assert_eq!(
        conn.state.lock().await.as_ref().map(|state| state.generation),
        Some(2)
    );
}

#[tokio::test]
async fn current_generation_is_cleared_after_failure() {
    let (addr, root) = spawn_mock_h3_server().await;
    let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
    let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
    let state = connected_state(&conn, 7).await;
    *conn.state.lock().await = Some(state);

    conn.invalidate_generation(7).await;

    assert!(conn.state.lock().await.is_none());
}
```

- [ ] **Step 2: Run the tests and verify they fail to compile**

Run:

```powershell
cargo test upstream::doh3::tests::stale_generation_does_not_clear_newer_connection -- --nocapture
cargo test upstream::doh3::tests::current_generation_is_cleared_after_failure -- --nocapture
```

Expected: compilation fails because `H3State::generation` and `H3Conn::invalidate_generation` do not exist.

- [ ] **Step 3: Add generation state and conditional invalidation**

Extend the private state and connection structs:

```rust
struct H3State {
    generation: u64,
    conn: quinn::Connection,
    sender: H3Sender,
}

pub struct H3Conn {
    host: String,
    port: u16,
    ips: Vec<IpAddr>,
    endpoint: quinn::Endpoint,
    state: Mutex<Option<H3State>>,
    next_generation: std::sync::atomic::AtomicU64,
}
```

Initialize the counter in `H3Conn::new`:

```rust
next_generation: std::sync::atomic::AtomicU64::new(0),
```

Add generation allocation and conditional invalidation methods:

```rust
fn allocate_generation(&self) -> u64 {
    self.next_generation
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

async fn invalidate_generation(&self, generation: u64) {
    let mut state = self.state.lock().await;
    if state
        .as_ref()
        .is_some_and(|current| current.generation == generation)
    {
        *state = None;
    }
}
```

When installing a newly connected state, allocate and store its generation. Return both sender and generation from the state lock:

```rust
if guard.is_none() {
    let (conn, sender) = self.connect().await?;
    *guard = Some(H3State {
        generation: self.allocate_generation(),
        conn,
        sender,
    });
}
let state = guard.as_ref().expect("just set");
(state.sender.clone(), state.generation)
```

Change the failure branch from unconditional clearing to generation-aware invalidation:

```rust
Err(error) => {
    self.invalidate_generation(generation).await;
    if attempt == 1 {
        return Err(error);
    }
    tracing::debug!("h3 request failed, reconnecting: {error:#}");
}
```

- [ ] **Step 4: Run all H3 and DoH tests**

Run:

```powershell
cargo test upstream::doh3::tests -- --nocapture
cargo test upstream::doh::tests -- --nocapture
```

Expected: all H3 generation, reconnect, IPv4/IPv6, round-trip, H2, and strict-H3 tests pass.

- [ ] **Step 5: Commit the H3 generation fix**

```powershell
git add "src/upstream/doh3.rs"
git commit -m "fix: preserve newer H3 connections"
```

---

### Task 3: Run Complete Quality Gates

**Files:**
- Modify only if formatting or Clippy requires a correction: `src/upstream/hedged.rs`, `src/upstream/doh3.rs`

**Interfaces:**
- Consumes: completed hedge lifecycle and H3 generation fixes.
- Produces: verified repository state with no formatting, lint, or test regressions.

- [ ] **Step 1: Format the edited Rust code**

Run:

```powershell
cargo fmt --all
```

Expected: command exits successfully and changes only formatting in intended Rust files.

- [ ] **Step 2: Check formatting**

Run:

```powershell
cargo fmt --all -- --check
```

Expected: exit code 0 with no diff.

- [ ] **Step 3: Run Clippy with warnings denied**

Run:

```powershell
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: exit code 0 and no Clippy diagnostics. The existing Cargo config deprecation warning is external to the repository and does not fail this gate.

- [ ] **Step 4: Run the complete Rust test suite**

Run:

```powershell
cargo test --all-targets --all-features
```

Expected: every unit and integration test passes with zero failures.

- [ ] **Step 5: Inspect the final diff and repository status**

Run:

```powershell
git status --short --branch
git diff --check
git diff HEAD~2 -- "src/upstream/hedged.rs" "src/upstream/doh3.rs"
```

Expected: no unstaged implementation changes after commits, no whitespace errors, and the diff is limited to scoped hedge cancellation and H3 generation safety.

- [ ] **Step 6: Commit formatting corrections only if Step 1 changed committed files**

If `git status --short` shows formatting-only changes in the two intended files:

```powershell
git add "src/upstream/hedged.rs" "src/upstream/doh3.rs"
git commit -m "style: format upstream fixes"
```

If the worktree is clean, do not create an empty commit.
