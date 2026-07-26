use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use hickory_proto::op::Message;
use tokio::net::UdpSocket;

use crate::pipeline::Pipeline;

/// 监听 UDP 并服务 DNS 查询，直到进程退出。
pub async fn run_udp(listen: SocketAddr, pipeline: Arc<Pipeline>) -> Result<()> {
    let socket = Arc::new(
        UdpSocket::bind(listen)
            .await
            .with_context(|| format!("binding UDP {listen}"))?,
    );
    tracing::warn!(listen = %socket.local_addr()?, "DNS UDP server starting");
    run_udp_socket(socket, pipeline).await
}

pub async fn run_udp_socket(socket: Arc<UdpSocket>, pipeline: Arc<Pipeline>) -> Result<()> {
    run_udp_socket_until(socket, pipeline, std::future::pending()).await
}

pub async fn run_udp_socket_until<F>(
    socket: Arc<UdpSocket>,
    pipeline: Arc<Pipeline>,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()>,
{
    tokio::pin!(shutdown);

    let mut buf = vec![0u8; 65535];
    loop {
        let received = tokio::select! {
            result = socket.recv_from(&mut buf) => result,
            () = &mut shutdown => return Ok(()),
        };
        let (n, peer) = received.context("recv_from")?;
        let data = buf[..n].to_vec();
        let socket = socket.clone();
        let pipeline = pipeline.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_packet(&data, peer, &socket, &pipeline).await {
                tracing::info!("error handling packet from {peer}: {e:#}");
            }
        });
    }
}

async fn handle_packet(
    data: &[u8],
    peer: SocketAddr,
    sock: &UdpSocket,
    pipeline: &Pipeline,
) -> Result<()> {
    let query = Message::from_vec(data).context("decoding client query")?;
    let response = pipeline.handle(&query).await;
    let bytes = response.to_vec().context("encoding response")?;
    sock.send_to(&bytes, peer)
        .await
        .with_context(|| format!("sending response to {peer}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::run_udp_socket_until;
    use crate::resolver::Resolver;
    use anyhow::Result;
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::net::UdpSocket;

    struct EchoOk;
    #[async_trait]
    impl Resolver for EchoOk {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            let mut resp = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            resp.metadata.response_code = ResponseCode::NoError;
            for q in &query.queries {
                resp.add_query(q.clone());
            }
            Ok(resp)
        }
    }

    fn sample_query(id: u16) -> Message {
        let mut m = Message::query();
        m.metadata.id = id;
        let mut q = Query::new();
        q.set_name(Name::from_str("example.com.").unwrap());
        q.set_query_type(RecordType::A);
        m.add_query(q);
        m
    }

    #[tokio::test]
    async fn serves_udp_query_end_to_end() {
        let pipeline = crate::pipeline::Pipeline::new(crate::pipeline::PipelineParts {
            hosts: crate::hosts::HostsMap::from_entries(&[]),
            filter: std::sync::Arc::new(crate::filter::Filter::new(&[])),
            cache: std::sync::Arc::new(crate::cache::Cache::new(16)),
            upstream: std::sync::Arc::new(EchoOk),
            ecs: None,
            query_timeout: Duration::from_secs(5),
            recorder: crate::dashboard::Recorder::disabled(),
        });
        let socket = std::sync::Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = socket.local_addr().unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(run_udp_socket_until(
            socket,
            std::sync::Arc::new(pipeline),
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(addr).await.unwrap();
        client
            .send(&sample_query(0xABCD).to_vec().unwrap())
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
            .await
            .expect("no timeout")
            .unwrap();
        let resp = Message::from_vec(&buf[..n]).unwrap();
        assert_eq!(resp.metadata.id, 0xABCD);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}
