use anyhow::{Context, Result};
use async_trait::async_trait;
use hickory_proto::op::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::resolver::Resolver;

/// Plaintext UDP upstream resolver: forwards the query verbatim to the target address and reads back the reply.
/// Used for the bootstrap IP upstream, the fallback IP upstream, and this plan's forwarding-path verification.
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
        let sock = UdpSocket::bind(bind)
            .await
            .context("binding local socket")?;
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
        if resp.metadata.id != query.metadata.id {
            anyhow::bail!(
                "upstream {} response id {} does not match query id {}",
                self.addr,
                resp.metadata.id,
                query.metadata.id
            );
        }
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

    /// Builds a NOERROR response based on the query.
    fn make_response(query: &Message) -> Message {
        let mut resp = Message::new(
            query.metadata.id,
            MessageType::Response,
            query.metadata.op_code,
        );
        resp.metadata.response_code = ResponseCode::NoError;
        for q in &query.queries {
            resp.add_query(q.clone());
        }
        resp
    }

    /// Starts a mock UDP DNS upstream that only replies with a fixed NOERROR response, returning its address.
    async fn spawn_mock_upstream() -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            let resp = make_response(&query);
            let bytes = resp.to_vec().unwrap();
            sock.send_to(&bytes, peer).await.unwrap();
        });
        addr
    }

    /// Starts a mock UDP DNS upstream that replies with a wrong-id response.
    async fn spawn_bad_id_upstream() -> SocketAddr {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            let mut resp = make_response(&query);
            resp.metadata.id = query.metadata.id.wrapping_add(1);
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

    #[tokio::test]
    async fn rejects_mismatched_response_id() {
        let addr = spawn_bad_id_upstream().await;
        let resolver = super::PlainResolver::with_timeout(addr, std::time::Duration::from_secs(2));
        let err = resolver.resolve(&sample_query()).await;
        assert!(err.is_err(), "mismatched id must be rejected");
    }
}
