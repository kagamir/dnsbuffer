use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dnsbuffer::config::Config;
use dnsbuffer::dashboard::Recorder;
use dnsbuffer::{build_pipeline, server};
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::rdata::A;
use hickory_proto::rr::{Name, RData, Record, RecordType};
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

/// Mock upstream: counts the queries it receives and replies NoError with a
/// single TTL 300 A record (the cache's TTL>0 logic depends on this record).
async fn spawn_counting_upstream() -> (SocketAddr, Arc<AtomicUsize>) {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let counted = counter.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        loop {
            let (n, peer) = sock.recv_from(&mut buf).await.unwrap();
            let query = Message::from_vec(&buf[..n]).unwrap();
            counted.fetch_add(1, Ordering::SeqCst);
            let mut resp = Message::new(
                query.metadata.id,
                MessageType::Response,
                query.metadata.op_code,
            );
            resp.metadata.response_code = ResponseCode::NoError;
            for q in &query.queries {
                let name = q.name().clone();
                resp.add_query(q.clone());
                resp.add_answer(Record::from_rdata(
                    name,
                    300,
                    RData::A(A::new(93, 184, 216, 34)),
                ));
            }
            sock.send_to(&resp.to_vec().unwrap(), peer).await.unwrap();
        }
    });
    (addr, counter)
}

/// Grab a free UDP port address (bind, then release immediately so the server under test can rebind it).
async fn free_udp_addr() -> SocketAddr {
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    addr
}

/// Send a single query to `listen` and wait for the decoded response message.
async fn udp_query(listen: SocketAddr, name: &str, rtype: RecordType) -> Message {
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(listen).await.unwrap();
    let mut m = Message::query();
    m.metadata.id = 0xBEEF;
    let mut q = Query::new();
    q.set_name(Name::from_str(name).unwrap());
    q.set_query_type(rtype);
    m.add_query(q);
    client.send(&m.to_vec().unwrap()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .expect("no timeout")
        .unwrap();
    Message::from_vec(&buf[..n]).unwrap()
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

    // Grab a free port for the proxy to listen on
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
    let pipeline = build_pipeline(&cfg, Recorder::disabled())
        .await
        .unwrap()
        .pipeline;

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
    // Primary upstream group: one dead port + fallback: a live mock -> the query should still succeed
    let alive = spawn_mock_upstream().await;
    let probe = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dead = probe.local_addr().unwrap();
    drop(probe); // nobody listening -> ECONNREFUSED/timeout

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
    let pipeline = build_pipeline(&cfg, Recorder::disabled())
        .await
        .unwrap()
        .pipeline;
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

#[tokio::test]
async fn hosts_entry_served_locally() {
    let upstream = spawn_counting_upstream().await; // (addr, Arc<AtomicUsize>)
    let listen = free_udp_addr().await;
    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[hosts]]
        name = "printer.home"
        addrs = ["192.168.9.9"]

        [[upstream]]
        type = "plain"
        addr = "{}"
        "#,
        upstream.0
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg, Recorder::disabled())
        .await
        .unwrap()
        .pipeline;
    tokio::spawn(async move { server::run_udp(listen, pipeline).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = udp_query(listen, "printer.home.", RecordType::A).await;
    assert_eq!(resp.answers.len(), 1);
    assert_eq!(upstream.1.load(Ordering::SeqCst), 0, "a hosts hit must not reach the upstream");
}

#[tokio::test]
async fn blocked_domain_returns_zero_address() {
    let upstream = spawn_counting_upstream().await;
    let listen = free_udp_addr().await;
    let dir = std::env::temp_dir().join("dnsbuffer-e2e-rules");
    std::fs::create_dir_all(&dir).unwrap();
    let rules = dir.join("rules.txt");
    std::fs::write(&rules, "0.0.0.0 blocked.test\n").unwrap();
    let rules_path = toml::Value::String(rules.to_string_lossy().into_owned()).to_string();
    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[adblock.rule_source]]
        path = {}

        [[upstream]]
        type = "plain"
        addr = "{}"
        "#,
        rules_path, upstream.0
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg, Recorder::disabled())
        .await
        .unwrap()
        .pipeline;
    tokio::spawn(async move { server::run_udp(listen, pipeline).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = udp_query(listen, "blocked.test.", RecordType::A).await;
    assert_eq!(resp.answers.len(), 1);
    assert!(matches!(resp.answers[0].data, RData::A(a) if a.0.is_unspecified()));
}

#[tokio::test]
async fn cache_serves_second_query() {
    let upstream = spawn_counting_upstream().await;
    let listen = free_udp_addr().await;
    let toml = format!(
        r#"
        [server]
        listen = "{listen}"

        [[upstream]]
        type = "plain"
        addr = "{}"
        "#,
        upstream.0
    );
    let cfg: Config = toml::from_str(&toml).unwrap();
    let pipeline = build_pipeline(&cfg, Recorder::disabled())
        .await
        .unwrap()
        .pipeline;
    tokio::spawn(async move { server::run_udp(listen, pipeline).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _ = udp_query(listen, "cached.example.", RecordType::A).await;
    let resp2 = udp_query(listen, "cached.example.", RecordType::A).await;
    assert_eq!(resp2.metadata.response_code, ResponseCode::NoError);
    assert_eq!(upstream.1.load(Ordering::SeqCst), 1, "the second query must be served from cache");
}
