use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_proto::op::{Message, ResponseCode};
use hickory_proto::rr::RData;

use crate::cache::{Cache, CacheKey};
use crate::dashboard::{QueryEvent, Recorder};
use crate::ecs::EcsSubnet;
use crate::filter::Filter;
use crate::hosts::HostsMap;
use crate::resolver::{Resolver, servfail};

pub struct PipelineParts {
    pub hosts: HostsMap,
    pub filter: Arc<Filter>,
    pub cache: Arc<Cache>,
    pub upstream: Arc<dyn Resolver>,
    pub ecs: Option<EcsSubnet>,
    pub query_timeout: Duration,
    pub recorder: Recorder,
}

/// 查询编排：hosts → filter → cache(乐观) → ECS 注入 → 上游链 → SERVFAIL。
pub struct Pipeline {
    hosts: HostsMap,
    filter: Arc<Filter>,
    cache: Arc<Cache>,
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
            upstream: parts.upstream,
            ecs: parts.ecs,
            query_timeout: parts.query_timeout,
            recorder: parts.recorder,
        }
    }

    fn prepared_query(&self, query: &Message) -> Message {
        let mut q = query.clone();
        // 默认剥离客户端自带的 ECS，避免泄露客户端真实来源
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

    /// 处理单个查询，始终返回一个可回给客户端的响应报文。
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
            // 3. 乐观缓存
            let key = CacheKey::from_query(query);
            if let Some((key, cached, expired)) = key.as_ref().and_then(|key| {
                self.cache
                    .get(key, query.metadata.id)
                    .map(|(cached, expired)| (key, cached, expired))
            }) {
                if expired {
                    self.spawn_refresh(key.clone(), query.clone());
                }
                (cached, false, true)
            } else {
                // 4. 上游
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
                (response, false, false)
            }
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

    /// 过期命中后的后台刷新：拿新结果替换缓存（删旧入队尾）。
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

    /// 计数并返回一条 TTL 0 的 A 记录（NoError）——放入缓存后立即过期。
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

    fn default_parts(upstream: Arc<dyn Resolver>) -> PipelineParts {
        PipelineParts {
            hosts: crate::hosts::HostsMap::from_entries(&[]),
            filter: Arc::new(crate::filter::Filter::new(&[])),
            cache: Arc::new(crate::cache::Cache::new(16)),
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
        // CountingTtlZero 返回 TTL 0 → put 后立即过期；第二次 handle 命中过期缓存
        // 返回旧值，同时后台刷新应再次调用上游（计数最终为 2）
        let counter = Arc::new(AtomicUsize::new(0));
        let resolver = Arc::new(CountingTtlZero(counter.clone()));
        let pipeline = Pipeline::new(PipelineParts {
            hosts: crate::hosts::HostsMap::from_entries(&[]),
            filter: Arc::new(crate::filter::Filter::new(&[])),
            cache: Arc::new(crate::cache::Cache::new(16)),
            upstream: resolver,
            ecs: None,
            query_timeout: Duration::from_secs(5),
            recorder: Recorder::disabled(),
        });
        let q = sample_query();
        let _ = pipeline.handle(&q).await; // 首查 → 上游 1 次 + 入缓存(TTL0)
        let resp = pipeline.handle(&q).await; // 过期命中 → 立即返回 + 后台刷新
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        tokio::time::sleep(Duration::from_millis(200)).await; // 等后台任务
        assert_eq!(counter.load(Ordering::SeqCst), 2, "后台刷新必须调用上游");
    }

    #[test]
    fn prepared_query_strips_client_supplied_ecs_when_disabled() {
        // 客户端自带 ECS 选项，pipeline 未配置 ecs（None）——必须剥离，不得转发上游
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

        // wire 往返，确保编解码层面也不携带 ECS
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
