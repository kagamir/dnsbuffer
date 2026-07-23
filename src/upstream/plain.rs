use anyhow::{Context, Result};
use async_trait::async_trait;
use hickory_proto::op::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::resolver::Resolver;

/// 明文 UDP 上游解析器：把查询原样转发到目标地址并读回应答。
/// 用于 bootstrap 的 IP 上游、fallback 的 IP 上游，以及本计划的转发链路验证。
pub struct PlainResolver {
    addr: SocketAddr,
    timeout: Duration,
}

impl PlainResolver {
    pub fn new(addr: SocketAddr) -> Self {
        Self::with_timeout(addr, Duration::from_secs(5))
    }

    pub fn with_timeout(addr: SocketAddr, timeout: Duration) -> Self {
        Self { addr, timeout }
    }
}

#[async_trait]
impl Resolver for PlainResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        let bytes = query.to_vec().context("encoding query")?;
        let bind = if self.addr.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        };
        let sock = UdpSocket::bind(bind).await.context("binding local socket")?;
        sock.connect(self.addr)
            .await
            .with_context(|| format!("connecting to upstream {}", self.addr))?;
        sock.send(&bytes).await.context("sending query")?;

        let mut buf = vec![0u8; 65535];
        let n = timeout(self.timeout, sock.recv(&mut buf))
            .await
            .with_context(|| format!("upstream {} timed out", self.addr))?
            .context("receiving response")?;
        let resp = Message::from_vec(&buf[..n]).context("decoding response")?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use crate::resolver::Resolver;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use tokio::net::UdpSocket;

    /// 起一个只回固定 NOERROR 响应的 mock UDP DNS 上游，返回其地址。
    async fn spawn_mock_upstream() -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            let mut resp = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            resp.metadata.response_code = ResponseCode::NoError;
            for q in &query.queries {
                resp.add_query(q.clone());
            }
            let bytes = resp.to_vec().unwrap();
            sock.send_to(&bytes, peer).await.unwrap();
        });
        addr
    }

    fn sample_query() -> Message {
        let mut m = Message::query();
        m.metadata.id = 0x4242;
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn forwards_and_returns_response() {
        let upstream_addr = spawn_mock_upstream().await;
        let resolver = super::PlainResolver::new(upstream_addr);
        let resp = resolver.resolve(&sample_query()).await.expect("resolve");
        assert_eq!(resp.metadata.id, 0x4242);
        assert_eq!(resp.metadata.message_type, MessageType::Response);
    }
}
