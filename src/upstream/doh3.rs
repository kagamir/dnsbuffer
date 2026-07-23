use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;

type H3Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// 惰性建立并复用的 HTTP/3 (QUIC) 连接。发送失败时重建一次由调用方驱动
/// （request 内部清空状态后返回错误，DohResolver 的 H2 回退接管）。
pub struct H3Conn {
    host: String,
    port: u16,
    ips: Vec<IpAddr>,
    endpoint: quinn::Endpoint,
    state: Mutex<Option<H3Sender>>,
}

impl H3Conn {
    pub fn new(host: String, port: u16, ips: Vec<IpAddr>, tls: rustls::ClientConfig) -> Result<Self> {
        let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .context("building QUIC client config (provider must support QUIC)")?;
        let client_config = quinn::ClientConfig::new(Arc::new(quic));
        let bind: SocketAddr = if ips.iter().all(|ip| ip.is_ipv6()) {
            "[::]:0".parse().expect("static addr")
        } else {
            "0.0.0.0:0".parse().expect("static addr")
        };
        let mut endpoint = quinn::Endpoint::client(bind).context("creating QUIC endpoint")?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            host,
            port,
            ips,
            endpoint,
            state: Mutex::new(None),
        })
    }

    async fn connect(&self) -> Result<H3Sender> {
        let mut last: Option<anyhow::Error> = None;
        for ip in &self.ips {
            let addr = SocketAddr::new(*ip, self.port);
            match self.try_connect(addr).await {
                Ok(sender) => return Ok(sender),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no ips for {}", self.host)))
    }

    async fn try_connect(&self, addr: SocketAddr) -> Result<H3Sender> {
        let conn = self
            .endpoint
            .connect(addr, &self.host)
            .context("starting QUIC connection")?
            .await
            .context("QUIC handshake")?;
        let (mut driver, sender) = h3::client::new(h3_quinn::Connection::new(conn))
            .await
            .context("h3 client setup")?;
        tokio::spawn(async move {
            // 驱动连接直到关闭
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        Ok(sender)
    }

    pub async fn request(&self, uri: &str, body: Vec<u8>) -> Result<Vec<u8>> {
        let mut guard = self.state.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect().await?);
        }
        let sender = guard.as_mut().expect("just set");

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
                .send_data(Bytes::from(body))
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

        if result.is_err() {
            *guard = None; // 连接可能已坏，下次重建
        }
        result
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
            quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = endpoint.local_addr().unwrap();

        tokio::spawn(async move {
            if let Some(incoming) = endpoint.accept().await {
                let conn = incoming.await.unwrap();
                let mut h3_conn: h3::server::Connection<_, Bytes> =
                    h3::server::Connection::new(h3_quinn::Connection::new(conn))
                        .await
                        .unwrap();
                while let Ok(Some(resolver)) = h3_conn.accept().await {
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
                }
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
