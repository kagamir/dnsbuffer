use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use hickory_proto::op::Message;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls_pki_types::{CertificateDer, ServerName};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use crate::resolver::Resolver;

type H2Sender = hyper::client::conn::http2::SendRequest<Full<Bytes>>;

/// DNS-over-HTTPS 上游（RFC 8484，POST application/dns-message）。
/// http3=true 时先试 H3（doh3 模块），失败回退 H2；H2 连接复用，断线重连一次。
pub struct DohResolver {
    host: String,
    port: u16,
    path: String,
    ips: Vec<IpAddr>,
    tls_h2: Arc<rustls::ClientConfig>,
    http3: bool,
    timeout: Duration,
    h2: Mutex<Option<H2Sender>>,
    // Task 7 接入：h3 连接状态
    #[allow(dead_code)]
    pub(crate) ech: Option<Vec<u8>>,
    #[allow(dead_code)]
    pub(crate) extra_roots: Vec<CertificateDer<'static>>,
}

impl DohResolver {
    pub fn new(url: &str, ips: Vec<IpAddr>, ech: Option<Vec<u8>>, http3: bool) -> Result<Self> {
        Self::with_extra_roots(url, ips, ech, http3, &[])
    }

    pub fn with_extra_roots(
        url: &str,
        ips: Vec<IpAddr>,
        ech: Option<Vec<u8>>,
        http3: bool,
        extra_roots: &[CertificateDer<'static>],
    ) -> Result<Self> {
        let uri: http::Uri = url
            .parse()
            .with_context(|| format!("invalid DoH url {url}"))?;
        if uri.scheme_str() != Some("https") {
            bail!("DoH url must be https: {url}");
        }
        let host = uri.host().context("DoH url missing host")?.to_string();
        let port = uri.port_u16().unwrap_or(443);
        let p = uri.path();
        let path = if p.is_empty() || p == "/" {
            "/dns-query".to_string()
        } else {
            p.to_string()
        };
        if ips.is_empty() {
            bail!("DoH resolver for {host} constructed without ips (bootstrap it first)");
        }
        let tls_h2 = Arc::new(crate::tls::client_config(&[b"h2"], extra_roots, ech.as_deref())?);
        Ok(Self {
            host,
            port,
            path,
            ips,
            tls_h2,
            http3,
            timeout: Duration::from_secs(5),
            h2: Mutex::new(None),
            ech,
            extra_roots: extra_roots.to_vec(),
        })
    }

    async fn connect_h2(&self) -> Result<H2Sender> {
        let mut last_err = None;
        for ip in &self.ips {
            match TcpStream::connect((*ip, self.port)).await {
                Ok(tcp) => {
                    let server_name = ServerName::try_from(self.host.clone())
                        .context("invalid DoH server name")?;
                    let tls = TlsConnector::from(self.tls_h2.clone())
                        .connect(server_name, tcp)
                        .await
                        .context("DoH TLS handshake")?;
                    let (sender, conn) = hyper::client::conn::http2::handshake(
                        TokioExecutor::new(),
                        TokioIo::new(tls),
                    )
                    .await
                    .context("h2 handshake")?;
                    tokio::spawn(async move {
                        if let Err(e) = conn.await {
                            tracing::debug!("h2 connection closed: {e}");
                        }
                    });
                    return Ok(sender);
                }
                Err(e) => last_err = Some(e),
            }
        }
        bail!(
            "cannot connect to any DoH ip for {}: {:?}",
            self.host,
            last_err
        )
    }

    async fn resolve_h2(&self, query: &Message) -> Result<Message> {
        let body = query.to_vec().context("encoding query")?;
        for attempt in 0..2 {
            // 锁内只做取用/建连并克隆 sender；请求发送在锁外，保住 H2 多路复用
            let mut sender = {
                let mut guard = self.h2.lock().await;
                if guard.is_none() {
                    *guard = Some(self.connect_h2().await?);
                }
                guard.as_ref().expect("just set").clone()
            };
            let req = http::Request::builder()
                .method(http::Method::POST)
                .uri(format!("https://{}:{}{}", self.host, self.port, self.path))
                .header(http::header::CONTENT_TYPE, "application/dns-message")
                .header(http::header::ACCEPT, "application/dns-message")
                .body(Full::new(Bytes::from(body.clone())))
                .context("building request")?;
            match sender.send_request(req).await {
                Ok(resp) => {
                    if resp.status() != http::StatusCode::OK {
                        bail!("DoH upstream {} returned {}", self.host, resp.status());
                    }
                    let bytes = resp
                        .into_body()
                        .collect()
                        .await
                        .context("reading body")?
                        .to_bytes();
                    let msg = Message::from_vec(&bytes).context("decoding response")?;
                    if msg.metadata.id != query.metadata.id {
                        bail!("DoH response id mismatch");
                    }
                    return Ok(msg);
                }
                Err(e) => {
                    *self.h2.lock().await = None; // 连接失效，下轮重连
                    if attempt == 1 {
                        return Err(e).context("h2 send_request failed after reconnect");
                    }
                    tracing::debug!("h2 send failed, reconnecting: {e}");
                }
            }
        }
        bail!("h2 retry loop exhausted")
    }

    async fn resolve_h3(&self, _query: &Message) -> Result<Message> {
        bail!("h3 support not built yet") // Task 7 替换为真实实现
    }
}

#[async_trait]
impl Resolver for DohResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        if self.http3 {
            match timeout(self.timeout, self.resolve_h3(query)).await {
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(e)) => {
                    tracing::warn!("DoH h3 failed for {}, falling back to h2: {e:#}", self.host)
                }
                Err(_) => tracing::warn!("DoH h3 timed out for {}, falling back to h2", self.host),
            }
        }
        timeout(self.timeout, self.resolve_h2(query))
            .await
            .with_context(|| format!("DoH upstream {} timed out", self.host))?
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    use super::*;
    use crate::resolver::Resolver;

    /// 起一个 HTTPS(H2) mock DoH server：对 POST /dns-query 回 NoError 响应。
    async fn spawn_mock_doh_server() -> (SocketAddr, CertificateDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert);
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let mut server_config = rustls::ServerConfig::builder_with_provider(
            rustls::crypto::aws_lc_rs::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der.into())
        .unwrap();
        server_config.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(tcp).await.unwrap();
            let service = service_fn(|req: hyper::Request<hyper::body::Incoming>| async move {
                if req.method() != hyper::Method::POST || req.uri().path() != "/dns-query" {
                    return Ok::<_, std::convert::Infallible>(
                        hyper::Response::builder()
                            .status(400)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    );
                }
                let body = req.into_body().collect().await.unwrap().to_bytes();
                let query = Message::from_vec(&body).unwrap();
                let mut resp =
                    Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
                resp.metadata.response_code = ResponseCode::NoError;
                for q in &query.queries {
                    resp.add_query(q.clone());
                }
                Ok::<_, std::convert::Infallible>(
                    hyper::Response::builder()
                        .status(200)
                        .header("content-type", "application/dns-message")
                        .body(Full::new(Bytes::from(resp.to_vec().unwrap())))
                        .unwrap(),
                )
            });
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(tls), service)
                .await
                .ok();
        });
        (addr, cert_der)
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x6161, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn resolves_over_h2() {
        let (addr, root) = spawn_mock_doh_server().await;
        let url = format!("https://localhost:{}/dns-query", addr.port());
        let resolver = DohResolver::with_extra_roots(
            &url,
            vec![addr.ip()],
            None,
            false, // 本测试仅 H2
            &[root],
        )
        .unwrap();
        let resp = resolver
            .resolve(&sample_query())
            .await
            .expect("doh h2 resolve");
        assert_eq!(resp.metadata.id, 0x6161);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn h3_failure_falls_back_to_h2() {
        let (addr, root) = spawn_mock_doh_server().await;
        let url = format!("https://localhost:{}/dns-query", addr.port());
        let resolver = DohResolver::with_extra_roots(
            &url,
            vec![addr.ip()],
            None,
            true, // http3 开启但 mock 无 H3 端点（本 Task 阶段 stub 必 Err）→ 必须回退 H2 成功
            &[root],
        )
        .unwrap();
        let resp = resolver
            .resolve(&sample_query())
            .await
            .expect("h3->h2 fallback");
        assert_eq!(resp.metadata.id, 0x6161);
    }

    #[tokio::test]
    async fn bare_url_defaults_to_dns_query_path() {
        let (addr, root) = spawn_mock_doh_server().await;
        let url = format!("https://localhost:{}", addr.port()); // 无 path
        let resolver =
            DohResolver::with_extra_roots(&url, vec![addr.ip()], None, false, &[root]).unwrap();
        let resp = resolver
            .resolve(&sample_query())
            .await
            .expect("bare url hits /dns-query");
        assert_eq!(resp.metadata.id, 0x6161);
    }
}
