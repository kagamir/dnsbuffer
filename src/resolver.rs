use anyhow::Result;
use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};

/// 所有上游解析器（明文/DoH/DoT）实现的统一抽象。
#[async_trait]
pub trait Resolver: Send + Sync {
    async fn resolve(&self, query: &Message) -> Result<Message>;
}

/// 构造一个与请求同 id、回显问题段、响应码为 SERVFAIL 的响应报文。
pub fn servfail(query: &Message) -> Message {
    let mut resp = Message::new(
        query.metadata.id,
        MessageType::Response,
        query.metadata.op_code,
    );
    resp.metadata.recursion_desired = query.metadata.recursion_desired;
    resp.metadata.recursion_available = true;
    resp.metadata.response_code = ResponseCode::ServFail;
    for q in &query.queries {
        resp.add_query(q.clone());
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::Query;
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;

    fn sample_query() -> Message {
        let mut m = Message::query();
        m.metadata.id = 0x1234;
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[test]
    fn servfail_preserves_id_and_question() {
        let q = sample_query();
        let resp = servfail(&q);
        assert_eq!(resp.metadata.id, 0x1234);
        assert_eq!(resp.metadata.message_type, MessageType::Response);
        assert_eq!(resp.metadata.response_code, ResponseCode::ServFail);
        assert_eq!(resp.queries.len(), 1);
        assert_eq!(resp.queries[0].name().to_string(), "example.com.");
    }
}
