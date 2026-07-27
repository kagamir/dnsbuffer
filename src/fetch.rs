use anyhow::{Context, Result, bail};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use rustls_pki_types::ServerName;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::bootstrap::Bootstrap;

const MAX_BODY: usize = 20 * 1024 * 1024;
const MAX_REDIRECTS: usize = 3;

/// 拉取规则文件：支持 http/https，域名经 bootstrap 解析，≤3 次跳转，20MiB 上限。
pub async fn fetch_url(url: &str, bootstrap: &Bootstrap) -> Result<Vec<u8>> {
    let mut current = url.to_string();
    for _ in 0..=MAX_REDIRECTS {
        let uri: http::Uri = current
            .parse()
            .with_context(|| format!("invalid url {current}"))?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => bail!("unsupported scheme in {current}"),
        };
        let host = uri.host().context("url missing host")?.to_string();
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let ips: Vec<IpAddr> = match host.parse::<IpAddr>() {
            Ok(ip) => vec![ip],
            Err(_) => {
                if bootstrap.is_empty() {
                    bail!("cannot resolve {host}: no bootstrap configured");
                }
                bootstrap.resolve_ips(&host).await?
            }
        };

        let mut last_err: Option<anyhow::Error> = None;
        let mut tcp = None;
        for ip in &ips {
            match TcpStream::connect((*ip, port)).await {
                Ok(s) => {
                    tcp = Some(s);
                    break;
                }
                Err(e) => last_err = Some(e.into()),
            }
        }
        let tcp =
            tcp.ok_or_else(|| last_err.unwrap_or_else(|| anyhow::anyhow!("no ips for {host}")))?;

        let path = if uri.path().is_empty() {
            "/".to_string()
        } else {
            uri.path_and_query()
                .map(|pq| pq.to_string())
                .unwrap_or_else(|| "/".into())
        };
        let req = http::Request::builder()
            .method(http::Method::GET)
            .uri(&path)
            .header(http::header::HOST, &host)
            .header(http::header::USER_AGENT, "dnsbuffer/0.1")
            .header(http::header::CONNECTION, "close")
            .body(Empty::<Bytes>::new())
            .context("building request")?;

        let resp = if https {
            let tls_cfg = Arc::new(crate::tls::client_config(&[b"http/1.1"], &[], None)?);
            let sn = ServerName::try_from(host.clone()).context("invalid server name")?;
            let tls = TlsConnector::from(tls_cfg)
                .connect(sn, tcp)
                .await
                .context("TLS handshake")?;
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
                .await
                .context("h1 handshake")?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            sender.send_request(req).await.context("sending request")?
        } else {
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
                .await
                .context("h1 handshake")?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            sender.send_request(req).await.context("sending request")?
        };

        let status = resp.status();
        if status.is_redirection() {
            let loc = resp
                .headers()
                .get(http::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .context("redirect without location")?;
            current = if loc.starts_with("http://") || loc.starts_with("https://") {
                loc.to_string()
            } else {
                // 相对跳转：同 scheme/host/port
                let scheme = if https { "https" } else { "http" };
                format!("{scheme}://{host}:{port}{loc}")
            };
            continue;
        }
        if !status.is_success() {
            bail!("{current} returned {status}");
        }

        let mut body = Vec::new();
        let mut stream = resp.into_body();
        while let Some(frame) = stream.frame().await {
            let frame = frame.context("reading body")?;
            if let Some(chunk) = frame.data_ref() {
                if body.len() + chunk.len() > MAX_BODY {
                    bail!("{current} exceeds {MAX_BODY} byte limit");
                }
                body.extend_from_slice(chunk);
            }
        }
        return Ok(body);
    }
    bail!("too many redirects fetching {url}")
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::bootstrap::Bootstrap;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 极简 HTTP/1.1 明文 server：按路径返回 200 内容 / 302 跳转 / 404。
    pub(crate) async fn spawn_http_server(body: &'static str) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
                let resp = if path == "/rules.txt" {
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if path == "/redirect" {
                    "HTTP/1.1 302 Found\r\nlocation: /rules.txt\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string()
                } else {
                    "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        .to_string()
                };
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        addr
    }

    fn empty_bootstrap() -> Bootstrap {
        Bootstrap::from_config(&[], false).unwrap()
    }

    #[tokio::test]
    async fn fetches_plain_http_by_ip() {
        let addr = spawn_http_server("||fetched.example^\n").await;
        let url = format!("http://{addr}/rules.txt");
        let body = fetch_url(&url, &empty_bootstrap()).await.expect("fetch");
        assert_eq!(String::from_utf8_lossy(&body), "||fetched.example^\n");
    }

    #[tokio::test]
    async fn follows_redirect() {
        let addr = spawn_http_server("redirected-content\n").await;
        let url = format!("http://{addr}/redirect");
        let body = fetch_url(&url, &empty_bootstrap())
            .await
            .expect("fetch via redirect");
        assert_eq!(String::from_utf8_lossy(&body), "redirected-content\n");
    }

    #[tokio::test]
    async fn non_2xx_is_error() {
        let addr = spawn_http_server("x").await;
        let url = format!("http://{addr}/missing");
        assert!(fetch_url(&url, &empty_bootstrap()).await.is_err());
    }
}
