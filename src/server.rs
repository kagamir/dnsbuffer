use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use hickory_proto::op::Message;
use tokio::net::UdpSocket;

use crate::pipeline::Pipeline;

/// Listens on UDP and serves DNS queries until the process exits.
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
    let mut handlers = tokio::task::JoinSet::new();
    let receive_result = loop {
        let received = tokio::select! {
            result = socket.recv_from(&mut buf) => result,
            () = &mut shutdown => break Ok(()),
        };
        let (n, peer) = match received.context("recv_from") {
            Ok(received) => received,
            Err(error) => break Err(error),
        };
        let data = buf[..n].to_vec();
        let socket = socket.clone();
        let pipeline = pipeline.clone();
        handlers.spawn(async move {
            if let Err(e) = handle_packet(&data, peer, &socket, &pipeline).await {
                tracing::info!("error handling packet from {peer}: {e:#}");
            }
        });
    };
    let drained = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(result) = handlers.join_next().await {
            if let Err(error) = result {
                tracing::warn!("DNS query handler failed: {error}");
            }
        }
    })
    .await;
    if drained.is_err() {
        handlers.abort_all();
        while handlers.join_next().await.is_some() {}
        tracing::warn!("DNS query handlers did not drain within 2s");
    }
    receive_result
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
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

    struct WaitResolver {
        started: Arc<AtomicBool>,
        release: tokio::sync::Notify,
    }

    #[async_trait]
    impl Resolver for WaitResolver {
        async fn resolve(&self, query: &Message) -> Result<Message> {
            self.started.store(true, Ordering::SeqCst);
            self.release.notified().await;
            EchoOk.resolve(query).await
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
        let cache = std::sync::Arc::new(crate::cache::Cache::new(16));
        let pipeline = crate::pipeline::Pipeline::new(crate::pipeline::PipelineParts {
            hosts: crate::hosts::HostsMap::from_entries(&[]),
            filter: std::sync::Arc::new(crate::filter::Filter::new(&[])),
            cache: cache.clone(),
            cache_sampler: std::sync::Arc::new(crate::dashboard::sampler::CacheHitSampler::new(
                cache,
            )),
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

    #[tokio::test]
    async fn shutdown_drains_an_already_received_query() {
        let started = Arc::new(AtomicBool::new(false));
        let resolver = Arc::new(WaitResolver {
            started: started.clone(),
            release: tokio::sync::Notify::new(),
        });
        let release = resolver.clone();
        let cache = Arc::new(crate::cache::Cache::new(16));
        let pipeline = crate::pipeline::Pipeline::new(crate::pipeline::PipelineParts {
            hosts: crate::hosts::HostsMap::from_entries(&[]),
            filter: Arc::new(crate::filter::Filter::new(&[])),
            cache: cache.clone(),
            cache_sampler: Arc::new(crate::dashboard::sampler::CacheHitSampler::new(cache)),
            upstream: resolver,
            ecs: None,
            query_timeout: Duration::from_secs(5),
            recorder: crate::dashboard::Recorder::disabled(),
        });
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = socket.local_addr().unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(run_udp_socket_until(
            socket,
            Arc::new(pipeline),
            async move {
                let _ = shutdown_rx.await;
            },
        ));
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client
            .send_to(&sample_query(1).to_vec().unwrap(), addr)
            .await
            .unwrap();
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        shutdown.send(()).unwrap();
        tokio::task::yield_now().await;
        assert!(
            !server.is_finished(),
            "server returned before its handler drained"
        );
        release.release.notify_one();
        server.await.unwrap().unwrap();
    }
}
