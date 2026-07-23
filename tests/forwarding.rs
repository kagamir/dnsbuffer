use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use dnsbuffer::config::Config;
use dnsbuffer::{build_pipeline, server};
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::{Name, RecordType};
use tokio::net::UdpSocket;

async fn spawn_mock_upstream() -> SocketAddr {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            let mut resp = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            resp.metadata.response_code = ResponseCode::NoError;
            for q in &query.queries {
                resp.add_query(q.clone());
            }
            sock.send_to(&resp.to_vec().unwrap(), peer).await.unwrap();
        }
    });
    addr
}

fn query(id: u16) -> Message {
    let mut m = Message::query();
    m.metadata.id = id;
    let mut q = Query::new();
    q.set_name(Name::from_str("example.com.").unwrap());
    q.set_query_type(RecordType::A);
    m.add_query(q);
    m
}

#[tokio::test]
async fn proxy_forwards_to_upstream_and_replies() {
    let upstream = spawn_mock_upstream().await;

    // 取一个空闲端口给代理监听
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let listen = probe.local_addr().unwrap();
    drop(probe);

    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[upstream]]
        type = "plain"
        addr = "{upstream}"
        "#
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg).await.unwrap();

    tokio::spawn(async move {
        server::run_udp(listen, pipeline).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(listen).await.unwrap();
    client.send(&query(0x1111).to_vec().unwrap()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .expect("no timeout")
        .unwrap();
    let resp = Message::from_vec(&buf[..n]).unwrap();
    assert_eq!(resp.metadata.id, 0x1111);
    assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
}

#[tokio::test]
async fn group_failover_and_fallback_serve() {
    // 主上游组：一个死端口 + 后备：活 mock → 查询仍应成功
    let alive = spawn_mock_upstream().await;
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dead = probe.local_addr().unwrap();
    drop(probe); // 无人监听 → ECONNREFUSED/超时

    let probe2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let listen = probe2.local_addr().unwrap();
    drop(probe2);

    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[upstream]]
        type = "plain"
        addr = "{dead}"

        [[fallback]]
        type = "plain"
        addr = "{alive}"
        "#
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg).await.unwrap();
    tokio::spawn(async move {
        server::run_udp(listen, pipeline).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(listen).await.unwrap();
    client.send(&query(0x2222).to_vec().unwrap()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(8), client.recv(&mut buf))
        .await
        .expect("no timeout")
        .unwrap();
    let resp = Message::from_vec(&buf[..n]).unwrap();
    assert_eq!(resp.metadata.id, 0x2222);
    assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
}
