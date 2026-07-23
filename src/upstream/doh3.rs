use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;

type H3Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// QUIC 保活间隔：防止 NAT 映射过期把放置的连接悄悄弄死。
const KEEP_ALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
/// QUIC 空闲超时：超时后连接明确进入关闭态，复用前可检出并立即重建。
const MAX_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);

struct H3State {
    /// 保留 quinn 连接句柄，复用前用 close_reason() 检查死活。
    conn: quinn::Connection,
    sender: H3Sender,
}

/// 惰性建立并复用的 HTTP/3 (QUIC) 连接。复用前检查连接死活（死则重建），
/// 发送失败重连再试一次；仍失败由上层（组内重选/对冲/fallback）兜底。
pub struct H3Conn {
    host: String,
    port: u16,
    ips: Vec<IpAddr>,
    endpoint: quinn::Endpoint,
    state: Mutex<Option<H3State>>,
}

impl H3Conn {
    pub fn new(host: String, port: u16, ips: Vec<IpAddr>, tls: rustls::ClientConfig) -> Result<Self> {
        if ips.is_empty() {
            bail!("H3Conn requires at least one ip for {host}");
        }
        let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .context("building QUIC client config (provider must support QUIC)")?;
        let mut client_config = quinn::ClientConfig::new(Arc::new(quic));
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(KEEP_ALIVE_INTERVAL));
        transport.max_idle_timeout(Some(
            MAX_IDLE_TIMEOUT.try_into().context("QUIC idle timeout out of range")?,
        ));
        client_config.transport_config(Arc::new(transport));
        // 列表含任意 v6 就绑 [::]（quinn 会把 v4 目标映射为 v6-mapped 一并可达）；
        // 纯 v4 或本机无 v6 栈时退回 0.0.0.0（此时 v6 目标逐个失败，由重选/fallback 兜底）。
        let v4_bind: SocketAddr = "0.0.0.0:0".parse().expect("static addr");
        let mut endpoint = if ips.iter().any(|ip| ip.is_ipv6()) {
            quinn::Endpoint::client("[::]:0".parse().expect("static addr")).or_else(|e| {
                tracing::warn!("binding [::] for QUIC failed ({e}), falling back to IPv4-only");
                quinn::Endpoint::client(v4_bind)
            })
        } else {
            quinn::Endpoint::client(v4_bind)
        }
        .context("creating QUIC endpoint")?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            host,
            port,
            ips,
            endpoint,
            state: Mutex::new(None),
        })
    }

    async fn connect(&self) -> Result<(quinn::Connection, H3Sender)> {
        let mut last: Option<anyhow::Error> = None;
        for ip in &self.ips {
            let addr = SocketAddr::new(*ip, self.port);
            match self.try_connect(addr).await {
                Ok(pair) => return Ok(pair),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no ips for {}", self.host)))
    }

    async fn try_connect(&self, addr: SocketAddr) -> Result<(quinn::Connection, H3Sender)> {
        let conn = self
            .endpoint
            .connect(addr, &self.host)
            .context("starting QUIC connection")?
            .await
            .context("QUIC handshake")?;
        let handle = conn.clone();
        let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(conn))
            .await
            .context("h3 client setup")?;
        tokio::spawn(async move {
            // 驱动连接直到关闭
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        Ok((handle, sender))
    }

    pub async fn request(&self, uri: &str, body: Vec<u8>) -> Result<Vec<u8>> {
        for attempt in 0..2 {
            // 锁内只做检活/取用/建连并克隆 sender；请求发送在锁外，保住 H3 多路复用
            let mut sender = {
                let mut guard = self.state.lock().await;
                // 已死连接（idle 超时/对端关闭）直接丢弃，避免拿着它死等
                if guard.as_ref().is_some_and(|s| s.conn.close_reason().is_some()) {
                    *guard = None;
                }
                if guard.is_none() {
                    let (conn, sender) = self.connect().await?;
                    *guard = Some(H3State { conn, sender });
                }
                guard.as_ref().expect("just set").sender.clone()
            };

            let req = http::Request::builder()
                .method(http::Method::POST)
                .uri(uri)
                .header(http::header::CONTENT_TYPE, "application/dns-message")
                .header(http::header::ACCEPT, "application/dns-message")
                .body(())
                .context("building h3 request")?;

            let result: Result<Vec<u8>> = async {
                let mut stream = sender.send_request(req).await.context("h3 send_request")?;
                stream
                    .send_data(Bytes::from(body.clone()))
                    .await
                    .context("h3 send body")?;
                stream.finish().await.context("h3 finish")?;
                let resp = stream.recv_response().await.context("h3 recv response")?;
                if resp.status() != http::StatusCode::OK {
                    bail!("DoH h3 upstream {} returned {}", self.host, resp.status());
                }
                let mut data = Vec::new();
                while let Some(mut chunk) = stream.recv_data().await.context("h3 recv data")? {
                    while chunk.has_remaining() {
                        let c = chunk.chunk();
                        data.extend_from_slice(c);
                        chunk.advance(c.len());
                    }
                }
                Ok(data)
            }
            .await;

            match result {
                Ok(data) => return Ok(data),
                Err(e) => {
                    *self.state.lock().await = None; // 连接可能已坏，重建
                    if attempt == 1 {
                        return Err(e);
                    }
                    tracing::debug!("h3 request failed, reconnecting: {e:#}");
                }
            }
        }
        unreachable!("loop returns on success or second failure")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::{Buf, Bytes};
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    pub(crate) async fn spawn_mock_h3_server() -> (SocketAddr, CertificateDer<'static>) {
        spawn_mock_h3_server_at("127.0.0.1:0").await
    }

    pub(crate) async fn spawn_mock_h3_server_at(bind: &str) -> (SocketAddr, CertificateDer<'static>) {
        spawn_mock_h3_server_with_cap(bind, usize::MAX).await
    }

    /// 每个连接最多服务 `max_requests_per_conn` 个请求后服务端主动关连接
    /// （模拟放置后连接被断），可继续接受新连接。
    pub(crate) async fn spawn_mock_h3_server_with_cap(
        bind: &str,
        max_requests_per_conn: usize,
    ) -> (SocketAddr, CertificateDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert);
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let mut tls = rustls::ServerConfig::builder_with_provider(
            rustls::crypto::aws_lc_rs::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der.into())
        .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];

        let server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        let endpoint =
            quinn::Endpoint::server(server_config, bind.parse().unwrap()).unwrap();
        let addr = endpoint.local_addr().unwrap();

        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                tokio::spawn(async move {
                    let conn = incoming.await.unwrap();
                    let mut h3_conn: h3::server::Connection<_, Bytes> =
                        h3::server::Connection::new(h3_quinn::Connection::new(conn))
                            .await
                            .unwrap();
                    let mut served = 0usize;
                    while served < max_requests_per_conn {
                        let Ok(Some(resolver)) = h3_conn.accept().await else { break };
                        let (_req, mut stream) = resolver.resolve_request().await.unwrap();
                        let mut body = Vec::new();
                        while let Some(mut chunk) = stream.recv_data().await.unwrap() {
                            while chunk.has_remaining() {
                                let c = chunk.chunk();
                                body.extend_from_slice(c);
                                chunk.advance(c.len());
                            }
                        }
                        let query = Message::from_vec(&body).unwrap();
                        let mut resp =
                            Message::new(query.metadata.id, MessageType::Response, OpCode::Query);
                        resp.metadata.response_code = ResponseCode::NoError;
                        for q in &query.queries {
                            resp.add_query(q.clone());
                        }
                        let http_resp = http::Response::builder()
                            .status(200)
                            .header("content-type", "application/dns-message")
                            .body(())
                            .unwrap();
                        stream.send_response(http_resp).await.unwrap();
                        stream
                            .send_data(Bytes::from(resp.to_vec().unwrap()))
                            .await
                            .unwrap();
                        stream.finish().await.unwrap();
                        served += 1;
                    }
                    // 等最后一个响应送达，避免 CONNECTION_CLOSE 竞争掉在途数据；
                    // 随后退出作用域即关闭该连接
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                });
            }
        });
        (addr, cert_der)
    }

    fn sample_query() -> Message {
        let mut m = Message::new(0x7171, MessageType::Query, OpCode::Query);
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn h3_mixed_family_ips_reach_ipv6_server() {
        // 服务器只在 [::1] 上监听；ips 混合 v6+v4。
        // endpoint 必须绑 [::]（而非 0.0.0.0），否则拨 v6 直接 InvalidRemoteAddress。
        let (addr, root) = spawn_mock_h3_server_at("[::1]:0").await;
        let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
        let conn = H3Conn::new(
            "localhost".into(),
            addr.port(),
            vec!["::1".parse().unwrap(), "127.0.0.1".parse().unwrap()],
            tls,
        )
        .unwrap();
        let uri = format!("https://localhost:{}/dns-query", addr.port());
        let body = conn
            .request(&uri, sample_query().to_vec().unwrap())
            .await
            .expect("h3 request over ipv6 with mixed ip list");
        let resp = Message::from_vec(&body).unwrap();
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn h3_reconnects_when_server_closed_idle_connection() {
        // 服务端每个连接只服务 1 个请求就关闭——模拟长时间放置后连接已死。
        // 第二个请求必须检出死连接并立即重建，而不是拿着死 sender 报错/死等。
        let (addr, root) = spawn_mock_h3_server_with_cap("127.0.0.1:0", 1).await;
        let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
        let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
        let uri = format!("https://localhost:{}/dns-query", addr.port());
        conn.request(&uri, sample_query().to_vec().unwrap())
            .await
            .expect("first request on fresh connection");
        // 留出时间让服务端的 CONNECTION_CLOSE 抵达客户端
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let body = conn
            .request(&uri, sample_query().to_vec().unwrap())
            .await
            .expect("second request must reconnect after server closed the connection");
        let resp = Message::from_vec(&body).unwrap();
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }

    #[tokio::test]
    async fn h3_round_trip() {
        let (addr, root) = spawn_mock_h3_server().await;
        let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
        let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
        let uri = format!("https://localhost:{}/dns-query", addr.port());
        let body = conn
            .request(&uri, sample_query().to_vec().unwrap())
            .await
            .expect("h3 request");
        let resp = Message::from_vec(&body).unwrap();
        assert_eq!(resp.metadata.id, 0x7171);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }
}
