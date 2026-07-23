use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use hickory_proto::op::Message;
use rustls_pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use crate::resolver::Resolver;

/// DNS-over-TLS 上游（RFC 7858）：TCP + TLS(SNI=domain) + 2 字节长度前缀。
/// 每查询新建连接；连接复用留待后续优化。
pub struct DotResolver {
    addr: SocketAddr,
    server_name: ServerName<'static>,
    connector: TlsConnector,
    timeout: Duration,
}

impl DotResolver {
    pub fn new(addr: SocketAddr, domain: &str, tls: Arc<rustls::ClientConfig>) -> Result<Self> {
        Self::with_timeout(addr, domain, tls, Duration::from_secs(5))
    }

    pub fn with_timeout(
        addr: SocketAddr,
        domain: &str,
        tls: Arc<rustls::ClientConfig>,
        timeout: Duration,
    ) -> Result<Self> {
        let server_name = ServerName::try_from(domain.to_string())
            .with_context(|| format!("invalid DoT server name {domain}"))?;
        Ok(Self {
            addr,
            server_name,
            connector: TlsConnector::from(tls),
            timeout,
        })
    }

    async fn exchange(&self, query: &Message) -> Result<Message> {
        let tcp = TcpStream::connect(self.addr)
            .await
            .with_context(|| format!("connecting to DoT upstream {}", self.addr))?;
        let mut tls = self
            .connector
            .connect(self.server_name.clone(), tcp)
            .await
            .context("TLS handshake")?;

        let bytes = query.to_vec().context("encoding query")?;
        if bytes.len() > u16::MAX as usize {
            bail!("query too large for DoT framing: {} bytes", bytes.len());
        }
        let mut out = Vec::with_capacity(2 + bytes.len());
        out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(&bytes);
        tls.write_all(&out).await.context("writing query")?;

        let mut len = [0u8; 2];
        tls.read_exact(&mut len)
            .await
            .context("reading response length")?;
        let n = u16::from_be_bytes(len) as usize;
        let mut data = vec![0u8; n];
        tls.read_exact(&mut data)
            .await
            .context("reading response body")?;

        let resp = Message::from_vec(&data).context("decoding response")?;
        if resp.metadata.id != query.metadata.id {
            bail!("DoT response id mismatch");
        }
        Ok(resp)
    }
}

#[async_trait]
impl Resolver for DotResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        timeout(self.timeout, self.exchange(query))
            .await
            .with_context(|| format!("DoT upstream {} timed out", self.addr))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::Resolver;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 生成 localhost 自签证书，起一个单连接 mock DoT server，
    /// 返回 (addr, 根证书) 供客户端信任。
    /// 若 bad_id 为 true，响应 ID 会被改为 query.id + 1。
    async fn spawn_mock_dot_server(bad_id: bool) -> (SocketAddr, CertificateDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert);
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let server_config = rustls::ServerConfig::builder_with_provider(
            rustls::crypto::aws_lc_rs::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der.into())
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let mut len = [0u8; 2];
            tls.read_exact(&mut len).await.unwrap();
            let n = u16::from_be_bytes(len) as usize;
            let mut data = vec![0u8; n];
            tls.read_exact(&mut data).await.unwrap();
            let query = Message::from_vec(&data).unwrap();
            // mirror 仓库既有响应构造模式
            let resp_id = if bad_id {
                query.metadata.id.wrapping_add(1)
            } else {
                query.metadata.id
            };
            let mut resp = Message::new(resp_id, MessageType::Response, OpCode::Query);
            resp.metadata.response_code = ResponseCode::NoError;
            for q in &query.queries {
                resp.add_query(q.clone());
            }
            let bytes = resp.to_vec().unwrap();
            let mut out = Vec::with_capacity(2 + bytes.len());
            out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            out.extend_from_slice(&bytes);
            tls.write_all(&out).await.unwrap();
            tls.shutdown().await.ok();
        });
        (addr, cert_der)
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x5151, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn resolves_over_tls_with_length_prefix() {
        let (addr, root) = spawn_mock_dot_server(false).await;
        let tls = Arc::new(crate::tls::client_config(&[], &[root], None).unwrap());
        let resolver = DotResolver::new(addr, "localhost", tls).unwrap();
        let resp = resolver.resolve(&sample_query()).await.expect("dot resolve");
        assert_eq!(resp.metadata.id, 0x5151);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn rejects_mismatched_response_id() {
        let (addr, root) = spawn_mock_dot_server(true).await;
        let tls = Arc::new(crate::tls::client_config(&[], &[root], None).unwrap());
        let resolver = DotResolver::new(addr, "localhost", tls).unwrap();
        let result = resolver.resolve(&sample_query()).await;
        assert!(result.is_err());
    }
}
