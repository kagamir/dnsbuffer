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
/// 严格按配置选协议：http3=true 仅用 H3（doh3 模块），否则仅用 H2（连接复用，断线重连一次）。
/// H3 失败不降级 H2，由上层（组内重选/对冲/fallback）兜底。
pub struct DohResolver {
    host: String,
    port: u16,
    path: String,
    ips: Vec<IpAddr>,
    tls_h2: Arc<rustls::ClientConfig>,
    http3: bool,
    timeout: Duration,
    h2: Mutex<Option<H2Sender>>,
    h3: Option<crate::upstream::doh3::H3Conn>,
}

impl DohResolver {
    pub fn new(
        url: &str,
        ips: Vec<IpAddr>,
        ech: Option<Vec<u8>>,
        http3: bool,
        prefer_ipv6: bool,
    ) -> Result<Self> {
        Self::with_extra_roots(url, ips, ech, http3, prefer_ipv6, &[])
    }

    pub fn with_extra_roots(
        url: &str,
        ips: Vec<IpAddr>,
        ech: Option<Vec<u8>>,
        http3: bool,
        prefer_ipv6: bool,
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
        let mut ips = ips;
        crate::upstream::sort_by_family(&mut ips, prefer_ipv6);
        let tls_h2 = Arc::new(crate::tls::client_config(&[b"h2"], extra_roots, ech.as_deref())?);
        let h3 = if http3 {
            let tls_h3 = crate::tls::client_config(&[b"h3"], extra_roots, ech.as_deref())?;
            Some(crate::upstream::doh3::H3Conn::new(
                host.clone(),
                port,
                ips.clone(),
                tls_h3,
            )?)
        } else {
            None
        };
        Ok(Self {
            host,
            port,
            path,
            ips,
            tls_h2,
            http3,
            timeout: Duration::from_secs(5),
            h2: Mutex::new(None),
            h3,
        })
    }

    /// 测试专用：缩短超时以避免 QUIC 连接到无人监听端口时的长等待拖慢测试。
    #[cfg(test)]
    fn set_timeout(&mut self, d: Duration) {
        self.timeout = d;
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

    async fn resolve_h3(&self, query: &Message) -> Result<Message> {
        let conn = self.h3.as_ref().context("h3 not enabled")?;
        let uri = format!("https://{}:{}{}", self.host, self.port, self.path);
        let body = conn
            .request(&uri, query.to_vec().context("encoding query")?)
            .await?;
        let msg = Message::from_vec(&body).context("decoding h3 response")?;
        if msg.metadata.id != query.metadata.id {
            bail!("DoH h3 response id mismatch");
        }
        Ok(msg)
    }
}

#[async_trait]
impl Resolver for DohResolver {
    async fn resolve(&self, query: &Message) -> Result<Message> {
        if self.http3 {
            timeout(self.timeout, self.resolve_h3(query))
                .await
                .with_context(|| format!("DoH upstream {} (h3) timed out", self.host))?
        } else {
            timeout(self.timeout, self.resolve_h2(query))
                .await
                .with_context(|| format!("DoH upstream {} timed out", self.host))?
        }
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
        spawn_mock_doh_server_at("127.0.0.1:0", None).await
    }

    /// 同上，但可指定绑定地址与应答记录（用于区分「哪台服务器接到了请求」）。
    async fn spawn_mock_doh_server_at(
        bind: &str,
        answer_ip: Option<std::net::IpAddr>,
    ) -> (SocketAddr, CertificateDer<'static>) {
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

        let listener = TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(tcp).await.unwrap();
            let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| async move {
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
                if let Some(ip) = answer_ip {
                    use hickory_proto::rr::rdata::{A, AAAA};
                    use hickory_proto::rr::{RData, Record};
                    let rdata = match ip {
                        std::net::IpAddr::V4(v4) => RData::A(A(v4)),
                        std::net::IpAddr::V6(v6) => RData::AAAA(AAAA(v6)),
                    };
                    let name = query.queries[0].name().clone();
                    resp.add_answer(Record::from_rdata(name, 300, rdata));
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
            false,
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

    /// v4 与 v6 两台 mock 用同一端口号（先绑 v4 拿端口，再绑 [::1] 同端口），
    /// 各自应答带本机族标记记录，据此判断实际连到了哪台。
    async fn spawn_dual_family_servers() -> (u16, Vec<CertificateDer<'static>>) {
        let (addr4, root4) =
            spawn_mock_doh_server_at("127.0.0.1:0", Some("1.2.3.4".parse().unwrap())).await;
        let bind6 = format!("[::1]:{}", addr4.port());
        let (_addr6, root6) =
            spawn_mock_doh_server_at(&bind6, Some("2001:db8::42".parse().unwrap())).await;
        (addr4.port(), vec![root4, root6])
    }

    #[tokio::test]
    async fn prefers_ipv6_when_configured() {
        use hickory_proto::rr::rdata::AAAA;
        use hickory_proto::rr::RData;
        let (port, roots) = spawn_dual_family_servers().await;
        let url = format!("https://localhost:{port}/dns-query");
        // 配置顺序故意 v4 在前：prefer_ipv6 = true 时仍必须先连 IPv6
        let resolver = DohResolver::with_extra_roots(
            &url,
            vec!["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()],
            None,
            false,
            true,
            &roots,
        )
        .unwrap();
        let resp = resolver.resolve(&sample_query()).await.expect("resolve");
        assert_eq!(
            resp.answers[0].data,
            RData::AAAA(AAAA("2001:db8::42".parse().unwrap())),
            "must have connected to the IPv6 server first"
        );
    }

    #[tokio::test]
    async fn prefers_ipv4_by_default() {
        use hickory_proto::rr::rdata::A;
        use hickory_proto::rr::RData;
        let (port, roots) = spawn_dual_family_servers().await;
        let url = format!("https://localhost:{port}/dns-query");
        // 配置顺序故意 v6 在前：默认（prefer_ipv6 = false）必须先连 IPv4
        let resolver = DohResolver::with_extra_roots(
            &url,
            vec!["::1".parse().unwrap(), "127.0.0.1".parse().unwrap()],
            None,
            false,
            false,
            &roots,
        )
        .unwrap();
        let resp = resolver.resolve(&sample_query()).await.expect("resolve");
        assert_eq!(
            resp.answers[0].data,
            RData::A(A("1.2.3.4".parse().unwrap())),
            "must have connected to the IPv4 server first"
        );
    }

    #[tokio::test]
    async fn h3_strict_errors_when_no_h3_endpoint() {
        let (addr, root) = spawn_mock_doh_server().await;
        let url = format!("https://localhost:{}/dns-query", addr.port());
        let mut resolver = DohResolver::with_extra_roots(
            &url,
            vec![addr.ip()],
            None,
            true, // http3 = true 严格走 H3；mock 只有 TCP(H2) 端点 → 必须失败而非回退 H2
            false,
            &[root],
        )
        .unwrap();
        // mock 服务器无 UDP 端点：QUIC 会等到 idle timeout。缩短超时避免测试变慢。
        resolver.set_timeout(Duration::from_millis(300));
        assert!(
            resolver.resolve(&sample_query()).await.is_err(),
            "strict h3 must not silently fall back to h2"
        );
    }

    #[tokio::test]
    async fn resolves_over_h3_when_available() {
        let (addr, root) = crate::upstream::doh3::tests::spawn_mock_h3_server().await;
        let url = format!("https://localhost:{}/dns-query", addr.port());
        let resolver =
            DohResolver::with_extra_roots(&url, vec![addr.ip()], None, true, false, &[root])
                .unwrap();
        let resp = resolver
            .resolve(&sample_query())
            .await
            .expect("doh h3 resolve");
        assert_eq!(resp.metadata.id, 0x6161);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn bare_url_defaults_to_dns_query_path() {
        let (addr, root) = spawn_mock_doh_server().await;
        let url = format!("https://localhost:{}", addr.port()); // 无 path
        let resolver =
            DohResolver::with_extra_roots(&url, vec![addr.ip()], None, false, false, &[root])
                .unwrap();
        let resp = resolver
            .resolve(&sample_query())
            .await
            .expect("bare url hits /dns-query");
        assert_eq!(resp.metadata.id, 0x6161);
    }
}
