use std::sync::Arc;
use std::time::Duration;

use hickory_proto::op::{Message, ResponseCode};

use crate::cache::{Cache, CacheKey};
use crate::ecs::EcsSubnet;
use crate::filter::Filter;
use crate::hosts::HostsMap;
use crate::resolver::{servfail, Resolver};

pub struct PipelineParts {
    pub hosts: HostsMap,
    pub filter: Arc<Filter>,
    pub cache: Arc<Cache>,
    pub upstream: Arc<dyn Resolver>,
    pub ecs: Option<EcsSubnet>,
    pub query_timeout: Duration,
}

/// 查询编排：hosts → filter → cache(乐观) → ECS 注入 → 上游链 → SERVFAIL。
pub struct Pipeline {
    hosts: HostsMap,
    filter: Arc<Filter>,
    cache: Arc<Cache>,
    upstream: Arc<dyn Resolver>,
    ecs: Option<EcsSubnet>,
    query_timeout: Duration,
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
        }
    }

    fn prepared_query(&self, query: &Message) -> Message {
        let mut q = query.clone();
        // 默认剥离客户端自带的 ECS，避免泄露客户端真实来源
        if let Some(edns) = q.edns.as_mut() {
            edns.options_mut().remove(hickory_proto::rr::rdata::opt::EdnsCode::Subnet);
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
        let Some(q) = query.queries.first() else {
            return servfail(query);
        };
        let qname = q.name().to_string();

        // 1. hosts
        if let Some(resp) = self.hosts.lookup(query) {
            return resp;
        }
        // 2. 广告屏蔽
        if self.filter.is_blocked(&qname) {
            return self.filter.block_response(query);
        }
        // 3. 乐观缓存
        let key = CacheKey::from_query(query);
        if let Some(key) = &key {
            if let Some((cached, expired)) = self.cache.get(key, query.metadata.id) {
                if expired {
                    self.spawn_refresh(key.clone(), query.clone());
                }
                return cached;
            }
        }
        // 4. 上游
        match self.resolve_upstream(query).await {
            Ok(resp) => {
                if resp.metadata.response_code == ResponseCode::NoError {
                    if let Some(key) = key {
                        self.cache.put(key, resp.clone());
                    }
                }
                resp
            }
            Err(e) => {
                tracing::info!("resolve failed: {e:#}");
                servfail(query)
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::rdata::A;
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
        }
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
        let pipeline = Pipeline::new(default_parts(Arc::new(ErrResolver)));
        let resp = pipeline.handle(&sample_query()).await;
        assert_eq!(resp.metadata.response_code, ResponseCode::ServFail);
        assert_eq!(resp.metadata.id, 0x7);
        assert_eq!(resp.queries.len(), 1);
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
        assert!(!has_subnet, "client-supplied ECS must be stripped when ecs is disabled");
    }
}
