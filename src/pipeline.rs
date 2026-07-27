use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_proto::op::{Message, ResponseCode};
use hickory_proto::rr::RData;

use crate::cache::{Cache, CacheKey};
use crate::dashboard::sampler::CacheHitSampler;
use crate::dashboard::{QueryEvent, Recorder};
use crate::ecs::EcsSubnet;
use crate::filter::Filter;
use crate::hosts::HostsMap;
use crate::resolver::{Resolver, servfail};

pub struct PipelineParts {
    pub hosts: HostsMap,
    pub filter: Arc<Filter>,
    pub cache: Arc<Cache>,
    pub cache_sampler: Arc<CacheHitSampler>,
    pub upstream: Arc<dyn Resolver>,
    pub ecs: Option<EcsSubnet>,
    pub query_timeout: Duration,
    pub recorder: Recorder,
}

/// Query orchestration: hosts → filter → cache (optimistic) → ECS injection → upstream chain → SERVFAIL.
pub struct Pipeline {
    hosts: HostsMap,
    filter: Arc<Filter>,
    cache: Arc<Cache>,
    cache_sampler: Arc<CacheHitSampler>,
    upstream: Arc<dyn Resolver>,
    ecs: Option<EcsSubnet>,
    query_timeout: Duration,
    recorder: Recorder,
}

impl Pipeline {
    pub fn new(parts: PipelineParts) -> Self {
        Self {
            hosts: parts.hosts,
            filter: parts.filter,
            cache: parts.cache,
            cache_sampler: parts.cache_sampler,
            upstream: parts.upstream,
            ecs: parts.ecs,
            query_timeout: parts.query_timeout,
            recorder: parts.recorder,
        }
    }

    fn prepared_query(&self, query: &Message) -> Message {
        let mut q = query.clone();
        // By default strip the client-supplied ECS to avoid leaking the client's real origin
        if let Some(edns) = q.edns.as_mut() {
            edns.options_mut()
                .remove(hickory_proto::rr::rdata::opt::EdnsCode::Subnet);
        }
        if let Some(subnet) = &self.ecs {
            crate::ecs::inject(&mut q, subnet);
        }
        q
    }

    async fn resolve_upstream(&self, query: &Message) -> anyhow::Result<Message> {
        let q = self.prepared_query(query);
        tokio::time::timeout(self.query_timeout, self.upstream.resolve(&q))
            .await
            .map_err(|_| anyhow::anyhow!("query timed out after {:?}", self.query_timeout))?
    }

    /// Handle a single query, always returning a response message that can be sent back to the client.
    pub async fn handle(&self, query: &Message) -> Message {
        let started = Instant::now();
        let started_at_ms = chrono::Utc::now().timestamp_millis();
        let Some(q) = query.queries.first() else {
            return servfail(query);
        };
        let qname = q.name().to_string();

        // 1. hosts
        let (response, blocked, cache_hit) = if let Some(resp) = self.hosts.lookup(query) {
            (resp, false, false)
        } else if self.filter.is_blocked(&qname) {
            (self.filter.block_response(query), true, false)
        } else {
            // 3. Optimistic cache
            let key = CacheKey::from_query(query);
            let outcome = if let Some((key, cached, expired)) = key.as_ref().and_then(|key| {
                self.cache
                    .get(key, query.metadata.id)
                    .map(|(cached, expired)| (key, cached, expired))
            }) {
                if expired {
                    self.spawn_refresh(key.clone(), query.clone());
                }
                // Cached block-type responses (0.0.0.0/:: returned by a filtering upstream) also count as blocked
                let blocked = is_blocked_response(&cached);
                (cached, blocked, true)
            } else {
                // 4. Upstream
                let response = match self.resolve_upstream(query).await {
                    Ok(resp) => {
                        if resp.metadata.response_code == ResponseCode::NoError
                            && let Some(key) = key
                        {
                            self.cache.put(key, resp.clone());
                        }
                        resp
                    }
                    Err(e) => {
                        tracing::info!("resolve failed: {e:#}");
                        servfail(query)
                    }
                };
                let blocked = is_blocked_response(&response);
                (response, blocked, false)
            };
            // Take an observation point after each cache-processed request completes (including any write)
            self.cache_sampler.observe();
            outcome
        };
        self.record_query(query, &response, started, started_at_ms, blocked, cache_hit);
        response
    }

    fn record_query(
        &self,
        query: &Message,
        response: &Message,
        started: Instant,
        started_at_ms: i64,
        blocked: bool,
        cache_hit: bool,
    ) {
        let Some(q) = query.queries.first() else {
            return;
        };
        let mut ips: Vec<String> = response
            .answers
            .iter()
            .filter_map(|record| match &record.data {
                RData::A(value) => Some(IpAddr::V4((*value).into()).to_string()),
                RData::AAAA(value) => Some(IpAddr::V6((*value).into()).to_string()),
                _ => None,
            })
            .collect();
        ips.sort();
        ips.dedup();
        self.recorder.try_record(QueryEvent {
            timestamp_ms: started_at_ms,
            domain: q.name().to_string().trim_end_matches('.').to_lowercase(),
            query_type: q.query_type().to_string(),
            response_code: response_code_name(response.metadata.response_code),
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            blocked,
            cache_hit,
            response_ips: ips,
        });
    }

    /// Background refresh after an expired hit: replace the cache with the fresh result (remove old, enqueue at tail).
    fn spawn_refresh(&self, key: CacheKey, query: Message) {
        let cache = self.cache.clone();
        let upstream = self.upstream.clone();
        let ecs = self.ecs;
        let timeout = self.query_timeout;
        tokio::spawn(async move {
            let mut q = query;
            if let Some(subnet) = &ecs {
                crate::ecs::inject(&mut q, subnet);
            }
            match tokio::time::timeout(timeout, upstream.resolve(&q)).await {
                Ok(Ok(resp)) if resp.metadata.response_code == ResponseCode::NoError => {
                    cache.put(key, resp);
                    tracing::debug!("cache refreshed");
                }
                Ok(Ok(resp)) => {
                    tracing::debug!(
                        "refresh got rcode {:?}, keeping stale entry",
                        resp.metadata.response_code
                    );
                }
                Ok(Err(e)) => tracing::info!("cache refresh failed: {e:#}"),
                Err(_) => tracing::info!("cache refresh timed out"),
            }
        });
    }
}

/// Identify a block-type response by its content: NoError, A/AAAA records present, and all
/// address records point to the unspecified address (0.0.0.0 / ::). Both the local filter and
/// filtering upstreams (NextDNS, AdGuard DNS, etc.) use this shape to signal a block, so block
/// responses returned by the upstream or served from cache can also be counted as blocked.
fn is_blocked_response(message: &Message) -> bool {
    if message.metadata.response_code != ResponseCode::NoError {
        return false;
    }
    let mut addresses = 0usize;
    for record in &message.answers {
        match &record.data {
            RData::A(value) => {
                if !value.0.is_unspecified() {
                    return false;
                }
                addresses += 1;
            }
            RData::AAAA(value) => {
                if !value.0.is_unspecified() {
                    return false;
                }
                addresses += 1;
            }
            _ => {}
        }
    }
    addresses > 0
}

fn response_code_name(code: ResponseCode) -> String {
    match code {
        ResponseCode::NoError => "NOERROR".into(),
        ResponseCode::FormErr => "FORMERR".into(),
        ResponseCode::ServFail => "SERVFAIL".into(),
        ResponseCode::NXDomain => "NXDOMAIN".into(),
        ResponseCode::NotImp => "NOTIMP".into(),
        ResponseCode::Refused => "REFUSED".into(),
        ResponseCode::YXDomain => "YXDOMAIN".into(),
        ResponseCode::YXRRSet => "YXRRSET".into(),
        ResponseCode::NXRRSet => "NXRRSET".into(),
        ResponseCode::NotAuth => "NOTAUTH".into(),
        ResponseCode::NotZone => "NOTZONE".into(),
        ResponseCode::BADVERS => "BADVERS".into(),
        ResponseCode::BADSIG => "BADSIG".into(),
        ResponseCode::BADKEY => "BADKEY".into(),
        ResponseCode::BADTIME => "BADTIME".into(),
        ResponseCode::BADMODE => "BADMODE".into(),
        ResponseCode::BADNAME => "BADNAME".into(),
        ResponseCode::BADALG => "BADALG".into(),
        ResponseCode::BADTRUNC => "BADTRUNC".into(),
        ResponseCode::BADCOOKIE => "BADCOOKIE".into(),
        ResponseCode::Unknown(value) => format!("RCODE{value}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::Recorder;
    use crate::resolver::Resolver;
    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::rdata::{A, AAAA};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct OkResolver;
    #[async_trait]
    impl Resolver for OkResolver {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            let mut resp = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            resp.metadata.response_code = ResponseCode::NoError;
            Ok(resp)
        }
    }

    struct ErrResolver;
    #[async_trait]
    impl Resolver for ErrResolver {
        async fn resolve(&self, _query: &Message) -> Result<Message> {
            Err(anyhow!("upstream down"))
        }
    }

    struct DelayedResolver {
        completed_at_ms: Arc<std::sync::atomic::AtomicI64>,
    }

    #[async_trait]
    impl Resolver for DelayedResolver {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            tokio::time::sleep(Duration::from_millis(30)).await;
            self.completed_at_ms
                .store(chrono::Utc::now().timestamp_millis(), Ordering::SeqCst);
            OkResolver.resolve(query).await
        }
    }

    /// Counts and returns a single TTL 0 A record (NoError) — expires immediately once cached.
    struct CountingTtlZero(Arc<AtomicUsize>);
    #[async_trait]
    impl Resolver for CountingTtlZero {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let mut resp = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            resp.metadata.response_code = ResponseCode::NoError;
            if let Some(q) = query.queries.first() {
                resp.add_query(q.clone());
                resp.add_answer(Record::from_rdata(
                    q.name().clone(),
                    0,
                    RData::A(A::new(1, 2, 3, 4)),
                ));
            }
            Ok(resp)
        }
    }

    fn sample_query() -> Message {
        let mut m = Message::query();
        m.metadata.id = 0x7;
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    /// Filtering upstream (e.g. NextDNS): signals a block by responding with 0.0.0.0.
    struct FilteringUpstream;
    #[async_trait]
    impl Resolver for FilteringUpstream {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            let mut resp = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            resp.metadata.response_code = ResponseCode::NoError;
            if let Some(q) = query.queries.first() {
                resp.add_query(q.clone());
                resp.add_answer(Record::from_rdata(
                    q.name().clone(),
                    300,
                    RData::A(A(std::net::Ipv4Addr::UNSPECIFIED)),
                ));
            }
            Ok(resp)
        }
    }

    #[test]
    fn blocked_response_detection_covers_zero_addresses_only() {
        let build = |records: Vec<RData>, code: ResponseCode| {
            let mut resp = Message::new(1, MessageType::Response, hickory_proto::op::OpCode::Query);
            resp.metadata.response_code = code;
            for data in records {
                resp.add_answer(Record::from_rdata(
                    Name::from_str("example.com.").unwrap(),
                    300,
                    data,
                ));
            }
            resp
        };
        let zero4 = RData::A(A(std::net::Ipv4Addr::UNSPECIFIED));
        let zero6 = RData::AAAA(AAAA(std::net::Ipv6Addr::UNSPECIFIED));
        let real = RData::A(A::new(1, 2, 3, 4));

        assert!(is_blocked_response(&build(
            vec![zero4.clone()],
            ResponseCode::NoError
        )));
        assert!(is_blocked_response(&build(
            vec![zero4.clone(), zero6.clone()],
            ResponseCode::NoError
        )));
        assert!(
            !is_blocked_response(&build(vec![real.clone()], ResponseCode::NoError)),
            "a real address is not a block"
        );
        assert!(
            !is_blocked_response(&build(vec![zero4.clone(), real], ResponseCode::NoError)),
            "a mix containing a real address does not count as a block"
        );
        assert!(
            !is_blocked_response(&build(vec![], ResponseCode::NoError)),
            "no address records does not count as a block"
        );
        assert!(!is_blocked_response(&build(
            vec![zero4],
            ResponseCode::NXDomain
        )));
    }

    #[tokio::test]
    async fn upstream_filtered_answers_count_as_blocked_also_when_cached() {
        let (pipeline, mut events) = recording_parts(Arc::new(FilteringUpstream));

        pipeline.handle(&sample_query()).await;
        let first = events.recv().await.unwrap();
        assert!(first.blocked, "upstream returning 0.0.0.0 counts as blocked");
        assert!(!first.cache_hit);

        pipeline.handle(&sample_query()).await;
        let second = events.recv().await.unwrap();
        assert!(second.blocked, "a cache-hit block response counts as blocked too");
        assert!(second.cache_hit);
    }

    fn default_parts(upstream: Arc<dyn Resolver>) -> PipelineParts {
        let cache = Arc::new(crate::cache::Cache::new(16));
        PipelineParts {
            hosts: crate::hosts::HostsMap::from_entries(&[]),
            filter: Arc::new(crate::filter::Filter::new(&[])),
            cache: cache.clone(),
            cache_sampler: Arc::new(CacheHitSampler::new(cache)),
            upstream,
            ecs: None,
            query_timeout: Duration::from_secs(5),
            recorder: Recorder::disabled(),
        }
    }

    fn recording_parts(
        upstream: Arc<dyn Resolver>,
    ) -> (
        Pipeline,
        tokio::sync::mpsc::Receiver<crate::dashboard::QueryEvent>,
    ) {
        let (recorder, receiver) = Recorder::channel(16);
        let mut pipeline = Pipeline::new(default_parts(upstream));
        pipeline.recorder = recorder;
        (pipeline, receiver)
    }

    #[tokio::test]
    async fn returns_upstream_response_on_success() {
        let pipeline = Pipeline::new(default_parts(Arc::new(OkResolver)));
        let resp = pipeline.handle(&sample_query()).await;
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert_eq!(resp.metadata.id, 0x7);
    }

    #[tokio::test]
    async fn returns_servfail_on_upstream_error() {
        let (pipeline, mut events) = recording_parts(Arc::new(ErrResolver));
        let resp = pipeline.handle(&sample_query()).await;
        assert_eq!(resp.metadata.response_code, ResponseCode::ServFail);
        assert_eq!(resp.metadata.id, 0x7);
        assert_eq!(resp.queries.len(), 1);
        let event = events.recv().await.unwrap();
        assert_eq!(event.response_code, "SERVFAIL");
        assert!(!event.blocked);
        assert!(!event.cache_hit);
    }

    #[tokio::test]
    async fn event_timestamp_is_captured_before_resolver_completion() {
        let completed_at_ms = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let before_handle_ms = chrono::Utc::now().timestamp_millis();
        let (pipeline, mut events) = recording_parts(Arc::new(DelayedResolver {
            completed_at_ms: completed_at_ms.clone(),
        }));

        pipeline.handle(&sample_query()).await;
        let event = events.recv().await.unwrap();
        let completed_at_ms = completed_at_ms.load(Ordering::SeqCst);

        assert!(event.timestamp_ms >= before_handle_ms);
        assert!(event.timestamp_ms < completed_at_ms);
        assert!(event.duration_ms >= 20);
    }

    #[tokio::test]
    async fn records_hosts_and_blocked_queries_once_with_final_answer_ips() {
        let (recorder, mut events) = Recorder::channel(4);
        let mut parts = default_parts(Arc::new(OkResolver));
        parts.hosts = crate::hosts::HostsMap::from_entries(&[crate::config::HostEntry {
            name: "example.com".into(),
            addrs: vec!["1.2.3.4".parse().unwrap()],
        }]);
        let mut pipeline = Pipeline::new(parts);
        pipeline.recorder = recorder;
        pipeline.handle(&sample_query()).await;

        let event = events.recv().await.unwrap();
        assert_eq!(event.domain, "example.com");
        assert_eq!(event.query_type, "A");
        assert_eq!(event.response_code, "NOERROR");
        assert_eq!(event.response_ips, vec!["1.2.3.4"]);
        assert!(!event.blocked);
        assert!(!event.cache_hit);
        assert!(events.try_recv().is_err());

        let (recorder, mut events) = Recorder::channel(4);
        let parts = default_parts(Arc::new(OkResolver));
        parts
            .filter
            .store(crate::filter::parse_rules("example.com"));
        let mut pipeline = Pipeline::new(parts);
        pipeline.recorder = recorder;
        pipeline.handle(&sample_query()).await;

        let event = events.recv().await.unwrap();
        assert_eq!(event.response_ips, vec!["0.0.0.0"]);
        assert!(event.blocked);
        assert!(!event.cache_hit);
        assert!(events.try_recv().is_err());
    }

    struct MixedAnswerResolver;

    #[async_trait]
    impl Resolver for MixedAnswerResolver {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            let mut response = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            response.metadata.response_code = ResponseCode::NoError;
            let name = query.queries[0].name().clone();
            response.add_answer(Record::from_rdata(
                name.clone(),
                60,
                RData::AAAA(AAAA("2001:0db8:0:0:0:0:0:1".parse().unwrap())),
            ));
            response.add_answer(Record::from_rdata(
                name.clone(),
                60,
                RData::A(A::new(2, 2, 2, 2)),
            ));
            response.add_answer(Record::from_rdata(
                name.clone(),
                60,
                RData::A(A::new(1, 1, 1, 1)),
            ));
            response.add_answer(Record::from_rdata(
                name.clone(),
                60,
                RData::A(A::new(1, 1, 1, 1)),
            ));
            response.add_authority(Record::from_rdata(name, 60, RData::A(A::new(9, 9, 9, 9))));
            Ok(response)
        }
    }

    #[tokio::test]
    async fn records_only_normalized_unique_sorted_ips_from_final_answers() {
        let (pipeline, mut events) = recording_parts(Arc::new(MixedAnswerResolver));

        pipeline.handle(&sample_query()).await;

        let event = events.recv().await.unwrap();
        assert_eq!(event.response_ips, ["1.1.1.1", "2.2.2.2", "2001:db8::1"]);
    }

    #[tokio::test]
    async fn background_refresh_records_no_extra_client_event() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (pipeline, mut events) = recording_parts(Arc::new(CountingTtlZero(counter.clone())));
        let query = sample_query();
        pipeline.handle(&query).await;
        pipeline.handle(&query).await;

        let upstream = events.recv().await.unwrap();
        let cached = events.recv().await.unwrap();
        assert_eq!(upstream.response_ips, ["1.2.3.4"]);
        assert!(!upstream.cache_hit);
        assert!(cached.cache_hit);
        assert_eq!(cached.response_ips, upstream.response_ips);
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background refresh must complete its upstream call");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert!(
            events.try_recv().is_err(),
            "refresh must not record a third event"
        );
    }

    #[tokio::test]
    async fn full_or_closed_recorder_never_delays_dns_response() {
        let (recorder, receiver) = Recorder::channel(1);
        let mut pipeline = Pipeline::new(default_parts(Arc::new(OkResolver)));
        pipeline.recorder = recorder;
        pipeline.handle(&sample_query()).await;

        let response =
            tokio::time::timeout(Duration::from_millis(100), pipeline.handle(&sample_query()))
                .await
                .expect("full recorder must not block DNS");
        assert_eq!(response.metadata.response_code, ResponseCode::NoError);
        drop(receiver);
        let response =
            tokio::time::timeout(Duration::from_millis(100), pipeline.handle(&sample_query()))
                .await
                .expect("closed recorder must not block DNS");
        assert_eq!(response.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn expired_cache_hit_triggers_background_refresh() {
        // CountingTtlZero returns TTL 0 → expires immediately after put; the second handle hits the
        // expired cache and returns the stale value, while the background refresh should call the
        // upstream again (final count is 2)
        let counter = Arc::new(AtomicUsize::new(0));
        let resolver = Arc::new(CountingTtlZero(counter.clone()));
        let pipeline = Pipeline::new(default_parts(resolver));
        let q = sample_query();
        let _ = pipeline.handle(&q).await; // first query → 1 upstream call + cache insert (TTL0)
        let resp = pipeline.handle(&q).await; // expired hit → return immediately + background refresh
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        tokio::time::sleep(Duration::from_millis(200)).await; // wait for the background task
        assert_eq!(counter.load(Ordering::SeqCst), 2, "background refresh must call the upstream");
    }

    #[test]
    fn prepared_query_strips_client_supplied_ecs_when_disabled() {
        // The client supplies an ECS option and the pipeline has no ecs configured (None) — it must be stripped, not forwarded upstream
        let pipeline = Pipeline::new(default_parts(Arc::new(OkResolver)));
        let mut q = sample_query();
        let client_subnet = crate::ecs::parse_subnet("198.51.100.0/24").unwrap();
        crate::ecs::inject(&mut q, &client_subnet);
        assert!(
            q.edns
                .as_ref()
                .unwrap()
                .option(hickory_proto::rr::rdata::opt::EdnsCode::Subnet)
                .is_some(),
            "sanity: client ECS option present before prepare"
        );

        let prepared = pipeline.prepared_query(&q);

        // wire round-trip to ensure ECS is not carried at the encoding/decoding level either
        let bytes = prepared.to_vec().unwrap();
        let decoded = Message::from_vec(&bytes).unwrap();
        let has_subnet = decoded
            .edns
            .as_ref()
            .and_then(|e| e.option(hickory_proto::rr::rdata::opt::EdnsCode::Subnet))
            .is_some();
        assert!(
            !has_subnet,
            "client-supplied ECS must be stripped when ecs is disabled"
        );
    }
}
