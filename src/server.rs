use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use hickory_proto::op::Message;
use tokio::net::UdpSocket;

use crate::pipeline::Pipeline;

/// 监听 UDP 并服务 DNS 查询，直到进程退出。
pub async fn run_udp(listen: SocketAddr, pipeline: Arc<Pipeline>) -> Result<()> {
    let sock = Arc::new(
        UdpSocket::bind(listen)
            .await
            .with_context(|| format!("binding UDP {listen}"))?,
    );
    tracing::warn!("listening on udp {listen}");

    let mut buf = vec![0u8; 65535];
    loop {
        let (n, peer) = sock.recv_from(&mut buf).await.context("recv_from")?;
        let data = buf[..n].to_vec();
        let sock = sock.clone();
        let pipeline = pipeline.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_packet(&data, peer, &sock, &pipeline).await {
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
    use super::run_udp;
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
        });
        // 绑定随机端口获取地址，再交给 run_udp。
        let listen: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let bound = UdpSocket::bind(listen).await.unwrap();
        let addr = bound.local_addr().unwrap();
        drop(bound); // 释放端口给 run_udp 重新绑定
        tokio::spawn(async move {
            run_udp(addr, std::sync::Arc::new(pipeline)).await.unwrap();
        });
        // 给 server 一点启动时间
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.connect(addr).await.unwrap();
        client.send(&sample_query(0xABCD).to_vec().unwrap()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
            .await
            .expect("no timeout")
            .unwrap();
        let resp = Message::from_vec(&buf[..n]).unwrap();
        assert_eq!(resp.metadata.id, 0xABCD);
        assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    }
}
