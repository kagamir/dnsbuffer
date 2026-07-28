use anyhow::Result;
use async_trait::async_trait;
use hickory_proto::op::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use crate::resolver::{QUERY_SUCCEEDED, Resolver};

/// Hard cap on parallel hedged attempts, so a black-hole upstream group does not
/// accumulate an unbounded pile of in-flight sockets within one budget window.
const MAX_IN_FLIGHT: usize = 8;
/// Throttle floor for relaunching after an active error: a fast-failing upstream
/// (e.g. instant RST) is retried at most every RETRY_FLOOR instead of busy-looping.
const RETRY_FLOOR: Duration = Duration::from_millis(50);

/// Budget-scoped retry engine: keeps trying the inner resolver until one attempt
/// succeeds or the `max_wait` budget expires — the only two possible outcomes.
///
/// An active error (transport failure) is not a final verdict: the failed attempt
/// is relaunched (throttled by [`RETRY_FLOOR`]) for as long as budget remains.
/// With `interval > 0`, slowness also triggers hedging: a parallel attempt is
/// launched every `interval` (each re-runs the inner weighted selection, very
/// likely picking a different member) and whichever succeeds first wins; with
/// `interval == 0`, retries stay serial (one attempt in flight at a time).
/// Any remaining in-flight attempts are cancelled when resolution ends.
pub struct HedgedResolver {
    inner: Arc<dyn Resolver>,
    interval: Duration,
    max_wait: Duration,
}

impl HedgedResolver {
    pub fn new(inner: Arc<dyn Resolver>, interval: Duration, max_wait: Duration) -> Self {
        Self {
            inner,
            interval,
            max_wait,
        }
    }

    fn spawn_attempt(
        &self,
        attempts: &mut tokio::task::JoinSet<Result<(Message, Option<String>)>>,
        query: &Message,
        succeeded: &Arc<AtomicBool>,
    ) {
        let inner = self.inner.clone();
        let query = query.clone();
        // Scope the success flag around the attempt so per-member accounting can
        // distinguish "cancelled because a sibling won" from a budget timeout.
        attempts.spawn(QUERY_SUCCEEDED.scope(succeeded.clone(), async move {
            inner.resolve_attributed(&query).await
        }));
    }
}

async fn stop_attempts(attempts: &mut tokio::task::JoinSet<Result<(Message, Option<String>)>>) {
    attempts.abort_all();
    while attempts.join_next().await.is_some() {}
}

#[async_trait]
impl Resolver for HedgedResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        Ok(self.resolve_attributed(query).await?.0)
    }

    async fn resolve_attributed(&self, query: &Message) -> Result<(Message, Option<String>)> {
        let mut attempts = tokio::task::JoinSet::new();
        let succeeded = Arc::new(AtomicBool::new(false));
        let max_in_flight = if self.interval.is_zero() {
            1
        } else {
            MAX_IN_FLIGHT
        };
        let deadline = Instant::now() + self.max_wait;
        let mut next_hedge = Instant::now() + self.interval;
        let mut last_spawn = Instant::now();
        // Only polled while nothing is in flight (all attempts errored out).
        let mut retry_at = last_spawn;
        let mut in_flight = 1usize;
        let mut last_error: Option<anyhow::Error> = None;
        self.spawn_attempt(&mut attempts, query, &succeeded);

        loop {
            tokio::select! {
                biased;
                joined = attempts.join_next(), if in_flight > 0 => {
                    in_flight -= 1;
                    let result = match joined.expect("an attempt is in flight") {
                        Ok(result) => result,
                        Err(error) => Err(anyhow::anyhow!("hedged attempt task failed: {error}")),
                    };
                    match result {
                        Ok(response) => {
                            succeeded.store(true, Ordering::SeqCst);
                            stop_attempts(&mut attempts).await;
                            return Ok(response);
                        }
                        Err(error) => {
                            // An active error is not a final verdict: keep retrying
                            // for as long as budget remains.
                            tracing::debug!("upstream attempt failed, retrying within budget: {error:#}");
                            last_error = Some(error);
                            if in_flight == 0 {
                                retry_at = Instant::now().max(last_spawn + RETRY_FLOOR);
                            }
                        }
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let error = match last_error {
                        Some(e) => e.context(format!(
                            "no upstream success within {:?} budget",
                            self.max_wait
                        )),
                        None => anyhow::anyhow!(
                            "no upstream reply within {:?} budget ({in_flight} attempts in flight)",
                            self.max_wait
                        ),
                    };
                    stop_attempts(&mut attempts).await;
                    return Err(error);
                }
                _ = tokio::time::sleep_until(next_hedge), if !self.interval.is_zero() => {
                    next_hedge += self.interval;
                    if in_flight < max_in_flight {
                        in_flight += 1;
                        last_spawn = Instant::now();
                        tracing::debug!("hedging: launching parallel attempt #{in_flight}");
                        self.spawn_attempt(&mut attempts, query, &succeeded);
                    }
                }
                _ = tokio::time::sleep_until(retry_at), if in_flight == 0 => {
                    in_flight += 1;
                    last_spawn = Instant::now();
                    self.spawn_attempt(&mut attempts, query, &succeeded);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use hickory_proto::op::{MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    fn sample_query() -> Message {
        let mut m = Message::new(0x42, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    fn ok_resp(query: &Message) -> Message {
        let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
        resp
    }

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

    async fn wait_for_at_least(value: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while value.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("count reached expected minimum");
    }

    /// The first call hangs for 60s, later calls succeed immediately -- simulating "the first connection drops packets, only a retry gets through".
    struct SlowThenFast(AtomicUsize);
    #[async_trait]
    impl Resolver for SlowThenFast {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Err(anyhow!("unreachable"))
            } else {
                Ok(ok_resp(query))
            }
        }
    }

    struct InstantOk(AtomicUsize);
    #[async_trait]
    impl Resolver for InstantOk {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ok_resp(query))
        }
    }

    struct InstantErr(AtomicUsize);
    #[async_trait]
    impl Resolver for InstantErr {
        async fn resolve(&self, _q: &Message) -> Result<Message> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(anyhow!("dead"))
        }
    }

    struct Hang;
    #[async_trait]
    impl Resolver for Hang {
        async fn resolve(&self, _q: &Message) -> Result<Message> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Err(anyhow!("unreachable"))
        }
    }

    #[tokio::test]
    async fn second_attempt_wins_after_interval() {
        let inner = Arc::new(SlowThenFast(AtomicUsize::new(0)));
        let hedged = HedgedResolver::new(
            inner.clone(),
            Duration::from_millis(100),
            Duration::from_secs(5),
        );
        let start = std::time::Instant::now();
        let resp = hedged.resolve(&sample_query()).await.expect("hedge wins");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(100) && elapsed < Duration::from_secs(1),
            "second attempt should win right after the 100ms hedge interval, took {elapsed:?}"
        );
        assert!(
            inner.0.load(Ordering::SeqCst) >= 2,
            "a parallel attempt was launched"
        );
    }

    #[tokio::test]
    async fn fast_success_needs_no_hedge() {
        let inner = Arc::new(InstantOk(AtomicUsize::new(0)));
        let hedged = HedgedResolver::new(
            inner.clone(),
            Duration::from_millis(100),
            Duration::from_secs(5),
        );
        let start = std::time::Instant::now();
        hedged.resolve(&sample_query()).await.expect("resolves");
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "no hedge wait on the fast path"
        );
        assert_eq!(inner.0.load(Ordering::SeqCst), 1, "only one attempt fired");
    }

    #[tokio::test]
    async fn active_errors_retry_until_budget_expires() {
        let inner = Arc::new(InstantErr(AtomicUsize::new(0)));
        let hedged = HedgedResolver::new(
            inner.clone(),
            Duration::ZERO, // serial mode: retries alone must fill the budget
            Duration::from_millis(300),
        );
        let start = std::time::Instant::now();
        assert!(hedged.resolve(&sample_query()).await.is_err());
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(300),
            "an active error is retried, not surrendered: only the budget ends the query, took {elapsed:?}"
        );
        let calls = inner.0.load(Ordering::SeqCst);
        assert!(calls >= 2, "failed attempts must be relaunched, got {calls}");
        assert!(
            calls <= 10,
            "retries are throttled by the floor, got {calls}"
        );
    }

    #[tokio::test]
    async fn error_then_recovery_within_budget_succeeds() {
        /// Fails the first two calls with an active error, then answers.
        struct FlakyThenOk(AtomicUsize);
        #[async_trait]
        impl Resolver for FlakyThenOk {
            async fn resolve(&self, query: &Message) -> Result<Message> {
                if self.0.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err(anyhow!("transient RST"));
                }
                Ok(ok_resp(query))
            }
        }

        let inner = Arc::new(FlakyThenOk(AtomicUsize::new(0)));
        let hedged = HedgedResolver::new(
            inner.clone(),
            Duration::ZERO,
            Duration::from_secs(5),
        );
        let resp = hedged
            .resolve(&sample_query())
            .await
            .expect("retry within budget recovers");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert_eq!(inner.0.load(Ordering::SeqCst), 3, "two errors, then success");
    }

    #[tokio::test]
    async fn serial_mode_keeps_a_single_attempt_in_flight() {
        let resolver = Arc::new(ControlledResolver {
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(Notify::new()),
            first_succeeds: false,
        });
        let hedged = HedgedResolver::new(
            resolver.clone(),
            Duration::ZERO,
            Duration::from_millis(150),
        );
        assert!(hedged.resolve(&sample_query()).await.is_err());
        assert_eq!(
            resolver.calls.load(Ordering::SeqCst),
            1,
            "interval 0 must not launch parallel hedges against a hanging attempt"
        );
    }

    #[tokio::test]
    async fn gives_up_at_budget_with_attempts_still_hanging() {
        let hedged = HedgedResolver::new(
            Arc::new(Hang),
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
        let start = std::time::Instant::now();
        assert!(hedged.resolve(&sample_query()).await.is_err());
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(200) && elapsed < Duration::from_secs(1),
            "must stop at the 200ms budget, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn success_cancels_losing_attempts_before_returning() {
        let active = Arc::new(AtomicUsize::new(0));
        let resolver = Arc::new(ControlledResolver {
            calls: AtomicUsize::new(0),
            active: active.clone(),
            dropped: Arc::new(Notify::new()),
            first_succeeds: true,
        });
        let hedged =
            HedgedResolver::new(resolver, Duration::from_millis(20), Duration::from_secs(1));

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
        wait_for_at_least(&active, 2).await;

        task.abort();
        assert!(task.await.expect_err("outer task aborted").is_cancelled());
        wait_for_count(&active, 0).await;
    }
}
