use anyhow::Result;
use async_trait::async_trait;
use hickory_proto::op::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use crate::resolver::Resolver;

/// 对冲式快速重试：一次尝试超过 interval 未返回，就并行发起新尝试
/// （每次尝试重新走内层的加权选择，大概率换成员）；任一成功即胜出。
/// 全部在飞尝试都失败时立即报错（失败不是慢，交给上层 fallback）；
/// 超过 max_wait 预算仍无结果则放弃。解析结束时取消其余在途尝试。
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
        attempts: &mut tokio::task::JoinSet<Result<Message>>,
        query: &Message,
    ) {
        let inner = self.inner.clone();
        let query = query.clone();
        attempts.spawn(async move { inner.resolve(&query).await });
    }
}

async fn stop_attempts(attempts: &mut tokio::task::JoinSet<Result<Message>>) {
    attempts.abort_all();
    while attempts.join_next().await.is_some() {}
}

#[async_trait]
impl Resolver for HedgedResolver {
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

    /// 第一次调用挂 60s，后续调用立即成功——模拟「首连丢包，重试才通」。
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

    struct InstantErr;
    #[async_trait]
    impl Resolver for InstantErr {
        async fn resolve(&self, _q: &Message) -> Result<Message> {
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
    async fn total_failure_bubbles_up_without_waiting_for_ticks() {
        let hedged = HedgedResolver::new(
            Arc::new(InstantErr),
            Duration::from_millis(500),
            Duration::from_secs(5),
        );
        let start = std::time::Instant::now();
        assert!(hedged.resolve(&sample_query()).await.is_err());
        assert!(
            start.elapsed() < Duration::from_millis(400),
            "failure (not slowness) must return immediately so fallback can take over"
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
        wait_for_at_least(&active, 2).await;

        task.abort();
        assert!(task.await.expect_err("outer task aborted").is_cancelled());
        wait_for_count(&active, 0).await;
    }
}
