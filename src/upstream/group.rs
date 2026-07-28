use anyhow::{Result, bail};
use async_trait::async_trait;
use hickory_proto::op::Message;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::dashboard::upstreams::UpstreamMetricsBuilder;
use crate::resolver::{QUERY_SUCCEEDED, Resolver};
use crate::stats::UpstreamStats;
use crate::upstream::selector::pick_weighted;

/// Maximum number of members a single query will try within the group.
const MAX_ATTEMPTS: usize = 2;

struct Member {
    name: String,
    resolver: Arc<dyn Resolver>,
    stats: Arc<Mutex<UpstreamStats>>,
}

/// Upstream group: sliding-window statistics drive weighted random selection, reselect on failure, with statistics feedback.
pub struct UpstreamGroup {
    members: Vec<Member>,
    k: f64,
}

impl UpstreamGroup {
    pub fn new(
        members: Vec<(String, Arc<dyn Resolver>)>,
        window: usize,
        k: f64,
        retention_days: u64,
        metrics: &mut UpstreamMetricsBuilder,
        group_kind: &'static str,
    ) -> Self {
        let members = members
            .into_iter()
            .map(|(name, resolver)| {
                let stats = Arc::new(Mutex::new(UpstreamStats::with_retention(
                    window,
                    retention_days,
                )));
                metrics.register(name.clone(), group_kind, stats.clone());
                Member {
                    name,
                    resolver,
                    stats,
                }
            })
            .collect();
        Self { members, k }
    }

    fn weights(&self, exclude: &[usize]) -> Vec<f64> {
        self.members
            .iter()
            .enumerate()
            .map(|(i, m)| {
                if exclude.contains(&i) {
                    0.0
                } else {
                    m.stats.lock().map(|s| s.weight(self.k)).unwrap_or(0.0)
                }
            })
            .collect()
    }

    /// Weighted-select a member, reselecting on failure, and report the name of
    /// the member that produced the answer.
    async fn resolve_member(&self, query: &Message) -> Result<(Message, String)> {
        if self.members.is_empty() {
            bail!("upstream group is empty");
        }
        let attempts = self.members.len().min(MAX_ATTEMPTS);
        let mut tried: Vec<usize> = Vec::with_capacity(attempts);
        let mut last: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let weights = self.weights(&tried);
            let Some(idx) = pick_weighted(&weights, rand::random::<f64>()) else {
                break;
            };
            // With all-zero weights, pick_weighted's uniform degradation may land on an already-tried member; skip it
            if tried.contains(&idx) {
                if let Some(untried) = (0..self.members.len()).find(|i| !tried.contains(i)) {
                    tried.push(untried);
                    let m = &self.members[untried];
                    match try_member(m, query).await {
                        Ok(resp) => return Ok((resp, m.name.clone())),
                        Err(e) => last = Some(e),
                    }
                    continue;
                }
                break;
            }
            tried.push(idx);
            let m = &self.members[idx];
            match try_member(m, query).await {
                Ok(resp) => return Ok((resp, m.name.clone())),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("upstream group exhausted")))
    }
}

#[async_trait]
impl Resolver for UpstreamGroup {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        Ok(self.resolve_member(query).await?.0)
    }

    async fn resolve_attributed(&self, query: &Message) -> Result<(Message, Option<String>)> {
        let (response, name) = self.resolve_member(query).await?;
        Ok((response, Some(name)))
    }
}

/// Records a timeout against the member if the in-flight attempt is dropped
/// before it resolves. When the retry engine's budget expires it cancels the
/// still-pending attempts: the future is dropped mid-await and neither arm in
/// [`try_member`] runs — without this guard a black-hole upstream would keep
/// looking perfectly healthy. A cancellation because a hedged sibling already
/// won (detected via [`QUERY_SUCCEEDED`]) is not a failure and records nothing.
/// Disarmed once the attempt resolves so the normal Ok/Err paths own accounting.
struct AttemptGuard<'a> {
    stats: &'a Mutex<UpstreamStats>,
    /// Captured at creation while the engine's task-local scope is active;
    /// `None` when the member is resolved outside the retry engine.
    succeeded: Option<Arc<AtomicBool>>,
    armed: bool,
}

impl<'a> AttemptGuard<'a> {
    fn new(stats: &'a Mutex<UpstreamStats>) -> Self {
        let succeeded = QUERY_SUCCEEDED.try_with(|flag| flag.clone()).ok();
        Self {
            stats,
            succeeded,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AttemptGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Cancelled mid-flight. A hedged sibling delivering the answer means the
        // query succeeded — this member was merely slower, not failing.
        if self
            .succeeded
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
        {
            return;
        }
        if let Ok(mut s) = self.stats.lock() {
            s.record_timeout();
        }
    }
}

async fn try_member(m: &Member, query: &Message) -> Result<Message> {
    let start = Instant::now();
    let mut guard = AttemptGuard::new(&m.stats);
    match m.resolver.resolve(query).await {
        Ok(resp) => {
            guard.disarm();
            if let Ok(mut s) = m.stats.lock() {
                s.record_success(start.elapsed());
            }
            Ok(resp)
        }
        Err(e) => {
            guard.disarm();
            // An active transport error is soft: it lowers the member's selection
            // weight but is not the query's final verdict — the retry engine keeps
            // retrying within the budget, so it stays out of the dashboard failures.
            if let Ok(mut s) = m.stats.lock() {
                s.record_soft_fail();
            }
            tracing::info!("upstream {} failed: {e:#}", m.name);
            Err(e)
        }
    }
}

/// Switches to the fallback chain when the primary chain gives up.
///
/// The primary side is wrapped in the budget-scoped retry engine
/// (`HedgedResolver`), which retries active errors internally and only returns
/// `Err` once its time budget is exhausted — so "primary budget exhausted" is
/// the single condition that triggers the fallback here.
pub struct FallbackResolver {
    primary: Arc<dyn Resolver>,
    fallback: Arc<dyn Resolver>,
}

impl FallbackResolver {
    pub fn new(primary: Arc<dyn Resolver>, fallback: Arc<dyn Resolver>) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl Resolver for FallbackResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        Ok(self.resolve_attributed(query).await?.0)
    }

    async fn resolve_attributed(&self, query: &Message) -> Result<(Message, Option<String>)> {
        match self.primary.resolve_attributed(query).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                tracing::info!("primary budget exhausted, using fallback: {e:#}");
                self.fallback.resolve_attributed(query).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::upstreams::UpstreamMetricsBuilder;
    use crate::resolver::Resolver;
    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingOk(AtomicUsize);
    #[async_trait]
    impl Resolver for CountingOk {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
            resp.metadata.response_code = ResponseCode::NoError;
            Ok(resp)
        }
    }

    struct AlwaysErr(AtomicUsize);
    #[async_trait]
    impl Resolver for AlwaysErr {
        async fn resolve(&self, _q: &Message) -> Result<Message> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(anyhow!("dead upstream"))
        }
    }

    struct BlackHole;
    #[async_trait]
    impl Resolver for BlackHole {
        async fn resolve(&self, _q: &Message) -> Result<Message> {
            std::future::pending().await
        }
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x9, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn metrics_share_group_stats_and_distinguish_primary_from_fallback() {
        let mut metrics = UpstreamMetricsBuilder::default();
        let primary = UpstreamGroup::new(
            vec![(
                "primary-ok".into(),
                Arc::new(CountingOk(AtomicUsize::new(0))) as Arc<dyn Resolver>,
            )],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        );
        let fallback = UpstreamGroup::new(
            vec![(
                "fallback-dead".into(),
                Arc::new(AlwaysErr(AtomicUsize::new(0))) as Arc<dyn Resolver>,
            )],
            8,
            5.0,
            0,
            &mut metrics,
            "fallback",
        );
        let metrics = metrics.build();

        primary
            .resolve(&sample_query())
            .await
            .expect("primary succeeds");
        assert!(fallback.resolve(&sample_query()).await.is_err());

        let snapshots = metrics.snapshot();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].id, "primary-0");
        assert_eq!(snapshots[0].name, "primary-ok");
        assert_eq!(snapshots[0].group, "primary");
        assert_eq!(snapshots[0].samples, 1);
        assert_eq!(snapshots[0].successes, 1);
        assert_eq!(snapshots[0].failure_rate, 0.0);
        assert_eq!(snapshots[1].id, "fallback-0");
        assert_eq!(snapshots[1].name, "fallback-dead");
        assert_eq!(snapshots[1].group, "fallback");
        // An active error is a soft fail: it stays out of the dashboard history
        // (only budget timeouts count as dashboard failures).
        assert_eq!(snapshots[1].samples, 0);
        assert_eq!(snapshots[1].successes, 0);
        assert_eq!(snapshots[1].failure_rate, 0.0);
        assert_eq!(snapshots[1].avg_latency_ms, None);
    }

    #[tokio::test]
    async fn active_errors_stay_out_of_dashboard_metrics() {
        let mut metrics = UpstreamMetricsBuilder::default();
        let group = UpstreamGroup::new(
            vec![(
                "dead".into(),
                Arc::new(AlwaysErr(AtomicUsize::new(0))) as Arc<dyn Resolver>,
            )],
            64,
            5.0,
            0,
            &mut metrics,
            "primary",
        );
        let metrics = metrics.build();

        assert!(group.resolve(&sample_query()).await.is_err());

        // The soft fail lowers the selection weight but is not a dashboard
        // failure: only exhausting the budget (a timeout) is.
        let snapshots = metrics.snapshot();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, "dead");
        assert_eq!(snapshots[0].samples, 0);
        assert_eq!(snapshots[0].successes, 0);
        assert_eq!(snapshots[0].failure_rate, 0.0);
    }

    #[tokio::test]
    async fn cancelled_in_flight_attempt_counts_as_upstream_timeout_failure() {
        let mut metrics = UpstreamMetricsBuilder::default();
        let group = UpstreamGroup::new(
            vec![(
                "black-hole".into(),
                Arc::new(BlackHole) as Arc<dyn Resolver>,
            )],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        );
        let metrics = metrics.build();

        // An upper layer (fallback budget / hedged deadline) gives up and drops
        // the still-pending attempt; the black-hole upstream must still be
        // charged a failure rather than looking healthy.
        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            group.resolve(&sample_query()),
        )
        .await;
        assert!(outcome.is_err(), "black-hole attempt never returns on its own");

        let snapshots = metrics.snapshot();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, "black-hole");
        assert_eq!(snapshots[0].samples, 1);
        assert_eq!(snapshots[0].successes, 0);
        assert_eq!(snapshots[0].failure_rate, 1.0);
    }

    #[tokio::test]
    async fn poisoned_stats_do_not_block_dns_resolution() {
        let mut metrics = UpstreamMetricsBuilder::default();
        let group = UpstreamGroup::new(
            vec![(
                "ok".into(),
                Arc::new(CountingOk(AtomicUsize::new(0))) as Arc<dyn Resolver>,
            )],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        );
        let stats = group.members[0].stats.clone();
        let _ = std::thread::spawn(move || {
            let _guard = stats.lock().unwrap();
            panic!("poison group stats lock");
        })
        .join();

        group
            .resolve(&sample_query())
            .await
            .expect("stats poison must not affect DNS");
    }

    #[tokio::test]
    async fn failing_member_retries_on_healthy_one() {
        let ok = Arc::new(CountingOk(AtomicUsize::new(0)));
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let mut metrics = UpstreamMetricsBuilder::default();
        let group = UpstreamGroup::new(
            vec![
                ("bad".into(), bad.clone() as Arc<dyn Resolver>),
                ("ok".into(), ok.clone() as Arc<dyn Resolver>),
            ],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        );
        // Multiple calls: each should ultimately succeed (after the bad member fails, the good member is reselected)
        for _ in 0..10 {
            let resp = group
                .resolve(&sample_query())
                .await
                .expect("group resolves");
            assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        }
        assert!(
            ok.0.load(Ordering::SeqCst) >= 10,
            "healthy member served all queries"
        );
    }

    #[tokio::test]
    async fn weights_shift_away_from_failures() {
        let ok = Arc::new(CountingOk(AtomicUsize::new(0)));
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let mut metrics = UpstreamMetricsBuilder::default();
        let group = UpstreamGroup::new(
            vec![
                ("bad".into(), bad.clone() as Arc<dyn Resolver>),
                ("ok".into(), ok.clone() as Arc<dyn Resolver>),
            ],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        );
        for _ in 0..50 {
            let _ = group.resolve(&sample_query()).await;
        }
        let bad_hits = bad.0.load(Ordering::SeqCst);
        let ok_hits = ok.0.load(Ordering::SeqCst);
        // After weight decay, the bad member should be chosen far less often than the good member
        assert!(
            ok_hits > bad_hits,
            "ok {ok_hits} should exceed bad {bad_hits}"
        );
    }

    #[tokio::test]
    async fn all_dead_is_error() {
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let mut metrics = UpstreamMetricsBuilder::default();
        let group = UpstreamGroup::new(
            vec![("bad".into(), bad as Arc<dyn Resolver>)],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        );
        assert!(group.resolve(&sample_query()).await.is_err());
    }

    #[tokio::test]
    async fn fallback_takes_over_when_primary_dies() {
        let ok = Arc::new(CountingOk(AtomicUsize::new(0)));
        let bad = Arc::new(AlwaysErr(AtomicUsize::new(0)));
        let mut metrics = UpstreamMetricsBuilder::default();
        let primary = Arc::new(UpstreamGroup::new(
            vec![("bad".into(), bad as Arc<dyn Resolver>)],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        ));
        let fallback = Arc::new(UpstreamGroup::new(
            vec![("ok".into(), ok as Arc<dyn Resolver>)],
            8,
            5.0,
            0,
            &mut metrics,
            "fallback",
        ));
        let chain = FallbackResolver::new(primary, fallback);
        let resp = chain
            .resolve(&sample_query())
            .await
            .expect("fallback serves");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }

    /// A resolver that hangs without returning: simulates an unresponsive upstream (black hole).
    struct Hang;
    #[async_trait]
    impl Resolver for Hang {
        async fn resolve(&self, _q: &Message) -> Result<Message> {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            Err(anyhow!("unreachable"))
        }
    }

    #[tokio::test]
    async fn fallback_takes_over_when_primary_hangs() {
        use crate::upstream::hedged::HedgedResolver;

        let ok = Arc::new(CountingOk(AtomicUsize::new(0)));
        let mut metrics = UpstreamMetricsBuilder::default();
        let primary = Arc::new(UpstreamGroup::new(
            vec![("hang".into(), Arc::new(Hang) as Arc<dyn Resolver>)],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        ));
        let fallback = Arc::new(UpstreamGroup::new(
            vec![("ok".into(), ok as Arc<dyn Resolver>)],
            8,
            5.0,
            0,
            &mut metrics,
            "fallback",
        ));
        let metrics = metrics.build();
        // Primary upstream black hole: the retry engine gives up once its 200ms
        // budget elapses, and only then does the query switch to the fallback.
        let engine = Arc::new(HedgedResolver::new(
            primary,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_millis(200),
        ));
        let chain = FallbackResolver::new(engine, fallback);
        let start = std::time::Instant::now();
        let resp = chain
            .resolve(&sample_query())
            .await
            .expect("fallback serves after timeout");
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(200)
                && elapsed < std::time::Duration::from_secs(2),
            "should cut over right after the 200ms primary budget, took {elapsed:?}"
        );

        // Exhausting the budget is the one failure kind shown on the dashboard:
        // every hedged attempt still in flight at the deadline charges the
        // black-hole member with a timeout.
        let snapshots = metrics.snapshot();
        let hang = snapshots.iter().find(|s| s.name == "hang").expect("hang member");
        assert!(hang.samples >= 1, "timeout must reach the dashboard history");
        assert_eq!(hang.successes, 0);
        assert_eq!(hang.failure_rate, 1.0);
    }

    /// First call hangs, second call answers instantly: the classic case where a
    /// hedged sibling wins while the first attempt is still in flight.
    struct HangThenOk(AtomicUsize);
    #[async_trait]
    impl Resolver for HangThenOk {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                return Err(anyhow!("unreachable"));
            }
            let mut resp = Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
            resp.metadata.response_code = ResponseCode::NoError;
            Ok(resp)
        }
    }

    #[tokio::test]
    async fn hedge_loser_cancelled_after_sibling_win_is_not_a_failure() {
        use crate::upstream::hedged::HedgedResolver;

        let mut metrics = UpstreamMetricsBuilder::default();
        let group = Arc::new(UpstreamGroup::new(
            vec![(
                "slow-then-fast".into(),
                Arc::new(HangThenOk(AtomicUsize::new(0))) as Arc<dyn Resolver>,
            )],
            8,
            5.0,
            0,
            &mut metrics,
            "primary",
        ));
        let metrics = metrics.build();
        let engine = HedgedResolver::new(
            group,
            std::time::Duration::from_millis(30),
            std::time::Duration::from_secs(5),
        );

        engine
            .resolve(&sample_query())
            .await
            .expect("hedged sibling wins");

        // The first attempt was cancelled because its sibling won — the query
        // succeeded, so the member must not be charged a timeout for it.
        let snapshots = metrics.snapshot();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].samples, 1, "only the winning attempt is recorded");
        assert_eq!(snapshots[0].successes, 1);
        assert_eq!(snapshots[0].failure_rate, 0.0);
    }
}
