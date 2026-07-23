use std::sync::Arc;

use hickory_proto::op::Message;

use crate::resolver::{servfail, Resolver};

/// 查询编排。本计划仅转发到单一上游；后续计划在此插入
/// hosts → filter → cache → 上游组 → fallback 的完整链路。
pub struct Pipeline {
    upstream: Arc<dyn Resolver>,
}

impl Pipeline {
    pub fn new(upstream: Arc<dyn Resolver>) -> Self {
        Self { upstream }
    }

    /// 处理单个查询，始终返回一个可回给客户端的响应报文。
    pub async fn handle(&self, query: &Message) -> Message {
        match self.upstream.resolve(query).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("upstream resolve failed: {e:#}");
                servfail(query)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
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

    fn sample_query() -> Message {
        let mut m = Message::query();
        m.metadata.id = 0x7;
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn returns_upstream_response_on_success() {
        let pipeline = Pipeline::new(Arc::new(OkResolver));
        let resp = pipeline.handle(&sample_query()).await;
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        assert_eq!(resp.metadata.id, 0x7);
    }

    #[tokio::test]
    async fn returns_servfail_on_upstream_error() {
        let pipeline = Pipeline::new(Arc::new(ErrResolver));
        let resp = pipeline.handle(&sample_query()).await;
        assert_eq!(resp.metadata.response_code, ResponseCode::ServFail);
        assert_eq!(resp.metadata.id, 0x7);
        assert_eq!(resp.queries.len(), 1);
    }
}
