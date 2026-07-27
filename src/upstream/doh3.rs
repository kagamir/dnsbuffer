use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;

type H3Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// QUIC keep-alive interval: prevents NAT mapping expiry from silently killing an idle connection.
const KEEP_ALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
/// QUIC idle timeout: after it expires the connection clearly enters the closed state, so it can be detected before reuse and rebuilt immediately.
const MAX_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);

struct H3State {
    generation: u64,
    /// Keeps the quinn connection handle so close_reason() can check liveness before reuse.
    conn: quinn::Connection,
    sender: H3Sender,
}

/// A lazily established and reused HTTP/3 (QUIC) connection. Checks liveness before reuse (rebuilds if dead),
/// reconnects and retries once on a send failure; if it still fails, the upper layer (in-group reselection / hedging / fallback) handles it.
pub struct H3Conn {
    host: String,
    port: u16,
    ips: Vec<IpAddr>,
    endpoint: quinn::Endpoint,
    state: Mutex<Option<H3State>>,
    next_generation: std::sync::atomic::AtomicU64,
}

impl H3Conn {
    pub fn new(
        host: String,
        port: u16,
        ips: Vec<IpAddr>,
        tls: rustls::ClientConfig,
    ) -> Result<Self> {
        if ips.is_empty() {
            bail!("H3Conn requires at least one ip for {host}");
        }
        let quic = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .context("building QUIC client config (provider must support QUIC)")?;
        let mut client_config = quinn::ClientConfig::new(Arc::new(quic));
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(KEEP_ALIVE_INTERVAL));
        transport.max_idle_timeout(Some(
            MAX_IDLE_TIMEOUT
                .try_into()
                .context("QUIC idle timeout out of range")?,
        ));
        client_config.transport_config(Arc::new(transport));
        // If the list contains any v6, bind [::] (quinn maps v4 targets to v6-mapped so both are reachable);
        // for pure v4, or when the host has no v6 stack, fall back to 0.0.0.0 (v6 targets then fail one by one, handled by reselection/fallback).
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
            next_generation: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn allocate_generation(&self) -> u64 {
        self.next_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    async fn invalidate_generation(&self, generation: u64) {
        let mut state = self.state.lock().await;
        if state
            .as_ref()
            .is_some_and(|current| current.generation == generation)
        {
            *state = None;
        }
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
            // Drive the connection until it closes
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        Ok((handle, sender))
    }

    pub async fn request(&self, uri: &str, body: Vec<u8>) -> Result<Vec<u8>> {
        for attempt in 0..2 {
            // Inside the lock only check liveness / fetch / establish the connection and clone the sender; the request is sent outside the lock to preserve H3 multiplexing
            let (mut sender, generation) = {
                let mut guard = self.state.lock().await;
                // Discard an already-dead connection (idle timeout / peer closed) so we don't hold it and wait forever
                if guard
                    .as_ref()
                    .is_some_and(|s| s.conn.close_reason().is_some())
                {
                    *guard = None;
                }
                if guard.is_none() {
                    let (conn, sender) = self.connect().await?;
                    *guard = Some(H3State {
                        generation: self.allocate_generation(),
                        conn,
                        sender,
                    });
                }
                let state = guard.as_ref().expect("just set");
                (state.sender.clone(), state.generation)
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
                    self.invalidate_generation(generation).await;
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

    pub(crate) async fn spawn_mock_h3_server_at(
        bind: &str,
    ) -> (SocketAddr, CertificateDer<'static>) {
        spawn_mock_h3_server_with_cap(bind, usize::MAX).await
    }

    /// After serving at most `max_requests_per_conn` requests per connection, the server actively closes it
    /// (simulating a connection being dropped after being idle), and can keep accepting new connections.
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
        let endpoint = quinn::Endpoint::server(server_config, bind.parse().unwrap()).unwrap();
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
                        let Ok(Some(resolver)) = h3_conn.accept().await else {
                            break;
                        };
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
                    // Wait for the last response to be delivered so CONNECTION_CLOSE doesn't race away in-flight data;
                    // then leaving scope closes the connection
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

    async fn connected_state(conn: &H3Conn, generation: u64) -> H3State {
        let (quinn, sender) = conn.connect().await.expect("connect mock H3 server");
        H3State {
            generation,
            conn: quinn,
            sender,
        }
    }

    #[tokio::test]
    async fn stale_generation_does_not_clear_newer_connection() {
        let (addr, root) = spawn_mock_h3_server().await;
        let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
        let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
        let state = connected_state(&conn, 2).await;
        *conn.state.lock().await = Some(state);

        conn.invalidate_generation(1).await;

        assert_eq!(
            conn.state
                .lock()
                .await
                .as_ref()
                .map(|state| state.generation),
            Some(2)
        );
    }

    #[tokio::test]
    async fn current_generation_is_cleared_after_failure() {
        let (addr, root) = spawn_mock_h3_server().await;
        let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
        let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
        let state = connected_state(&conn, 7).await;
        *conn.state.lock().await = Some(state);

        conn.invalidate_generation(7).await;

        assert!(conn.state.lock().await.is_none());
    }

    #[tokio::test]
    async fn h3_mixed_family_ips_reach_ipv6_server() {
        // The server listens only on [::1]; ips mix v6+v4.
        // The endpoint must bind [::] (not 0.0.0.0), otherwise dialing v6 fails outright with InvalidRemoteAddress.
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
        // The server serves only 1 request per connection then closes it -- simulating a connection that has died after being idle a long time.
        // The second request must detect the dead connection and rebuild immediately, rather than erroring / hanging on a dead sender.
        let (addr, root) = spawn_mock_h3_server_with_cap("127.0.0.1:0", 1).await;
        let tls = crate::tls::client_config(&[b"h3"], &[root], None).unwrap();
        let conn = H3Conn::new("localhost".into(), addr.port(), vec![addr.ip()], tls).unwrap();
        let uri = format!("https://localhost:{}/dns-query", addr.port());
        conn.request(&uri, sample_query().to_vec().unwrap())
            .await
            .expect("first request on fresh connection");
        // Allow time for the server's CONNECTION_CLOSE to reach the client
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
