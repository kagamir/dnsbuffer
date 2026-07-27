use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use dnsbuffer::build_pipeline;
use dnsbuffer::config::Config;
use dnsbuffer::dashboard::build_runtime;
use dnsbuffer::dashboard::http::{HttpState, router};
use dnsbuffer::dashboard::sampler::CacheHitSampler;
use dnsbuffer::dashboard::store::{Store, StoreWorker};
use dnsbuffer::dashboard::upstreams::UpstreamMetrics;
use dnsbuffer::dashboard::{QueryEvent, Recorder};
use http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.0);
        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

async fn test_store(name: &str) -> (TestDatabase, Store) {
    let path = std::env::temp_dir().join(format!(
        "dnsbuffer-http-{name}-{}-{}.db",
        std::process::id(),
        rand::random::<u64>()
    ));
    let guard = TestDatabase(path.clone());
    let store = tokio::task::spawn_blocking(move || Store::open(&path).unwrap())
        .await
        .unwrap();
    (guard, store)
}

async fn insert_events(store: Store, events: Vec<QueryEvent>) -> Store {
    tokio::task::spawn_blocking(move || {
        store.insert_events(&events).unwrap();
        store
    })
    .await
    .unwrap()
}

async fn configured_metrics(
    database_path: &Path,
) -> (Arc<dnsbuffer::pipeline::Pipeline>, UpstreamMetrics) {
    let mut config: Config = toml::from_str(
        r#"
        [server]
        listen = "127.0.0.1:5300"
        hedged_retry_ms = 0

        [[upstream]]
        type = "plain"
        addr = "1.1.1.1:53"
        "#,
    )
    .unwrap();
    config.dashboard.database_path = database_path.to_owned();
    config.dashboard.retention_days = 0;
    let built = build_pipeline(&config, Recorder::disabled()).await.unwrap();
    (built.pipeline, built.upstream_metrics)
}

fn runtime_config(database_path: &Path) -> Config {
    let mut config: Config = toml::from_str(
        r#"
        [server]
        listen = "127.0.0.1:0"
        hedged_retry_ms = 0

        [dashboard]
        listen = "127.0.0.1:0"

        [[upstream]]
        type = "plain"
        addr = "1.1.1.1:53"
        "#,
    )
    .unwrap();
    config.dashboard.database_path = database_path.to_owned();
    config
}

#[tokio::test]
async fn runtime_build_fails_when_sqlite_cannot_be_initialized() {
    let directory = std::env::temp_dir().join(format!(
        "dnsbuffer-invalid-runtime-db-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&directory).unwrap();
    let config = runtime_config(&directory);

    let error = build_runtime(&config).await.unwrap_err();

    assert!(error.to_string().contains("dashboard database"));
    std::fs::remove_dir(directory).unwrap();
}

#[tokio::test]
async fn runtime_build_fails_before_dns_when_http_address_is_occupied() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (guard, _store) = test_store("runtime-http-bind").await;
    let mut config = runtime_config(guard.path());
    config.dashboard.listen = listener.local_addr().unwrap();

    let error = build_runtime(&config).await.unwrap_err();

    assert!(error.to_string().contains("dashboard HTTP server"));
}

#[tokio::test]
async fn runtime_build_fails_when_dns_address_is_occupied() {
    let occupied = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (guard, _store) = test_store("runtime-dns-exit").await;
    let mut config = runtime_config(guard.path());
    config.server.listen = occupied.local_addr().unwrap();

    let error = build_runtime(&config).await.unwrap_err();

    assert!(error.to_string().contains("failed to bind DNS UDP server"));
}

#[tokio::test]
async fn runtime_exposes_prebound_ephemeral_addresses_and_shuts_down_cleanly() {
    let (guard, _store) = test_store("runtime-addresses").await;
    let runtime = build_runtime(&runtime_config(guard.path())).await.unwrap();

    assert_ne!(runtime.http_addr().port(), 0);
    assert_ne!(runtime.dns_addr().port(), 0);
    let http_addr = runtime.http_addr();
    let dns_addr = runtime.dns_addr();
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let running = tokio::spawn(runtime.run_until(async move {
        let _ = shutdown_rx.await;
        Ok(())
    }));

    tokio::net::TcpStream::connect(http_addr).await.unwrap();
    tokio::net::UdpSocket::bind(dns_addr).await.unwrap_err();
    shutdown.send(()).unwrap();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn complete_pipeline_persists_query_details_aggregates_and_exposes_metrics() {
    let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buffer = [0; 4096];
        let (length, peer) = upstream.recv_from(&mut buffer).await.unwrap();
        let query = hickory_proto::op::Message::from_vec(&buffer[..length]).unwrap();
        let mut response = hickory_proto::op::Message::new(
            query.metadata.id,
            hickory_proto::op::MessageType::Response,
            query.metadata.op_code,
        );
        response.metadata.response_code = hickory_proto::op::ResponseCode::NoError;
        response.queries = query.queries;
        upstream
            .send_to(&response.to_vec().unwrap(), peer)
            .await
            .unwrap();
    });
    let (guard, store) = test_store("complete-pipeline").await;
    let worker = StoreWorker::start(store, 7).unwrap();
    let config: Config = toml::from_str(&format!(
        r#"
        [server]
        listen = "127.0.0.1:0"
        hedged_retry_ms = 0

        [[upstream]]
        type = "plain"
        addr = "{upstream_addr}"
        "#
    ))
    .unwrap();
    let built = build_pipeline(&config, worker.recorder()).await.unwrap();
    assert_eq!(
        built.upstream_metrics.snapshot()[0].name,
        format!("plain:{upstream_addr}")
    );

    let mut query = hickory_proto::op::Message::query();
    let mut question = hickory_proto::op::Query::new();
    question.set_name("persisted.example.".parse().unwrap());
    question.set_query_type(hickory_proto::rr::RecordType::A);
    query.add_query(question);
    built.pipeline.handle(&query).await;

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let path = guard.path().to_owned();
        let persisted = tokio::task::spawn_blocking(move || {
            let store = Store::open(&path).unwrap();
            (
                store.queries(1, 10, None).unwrap(),
                store
                    .trend(7, chrono::Utc::now().timestamp_millis())
                    .unwrap(),
            )
        })
        .await
        .unwrap();
        if persisted.0.total == 1 {
            assert_eq!(persisted.0.records[0].domain, "persisted.example");
            assert_eq!(
                persisted
                    .1
                    .buckets
                    .iter()
                    .map(|bucket| bucket.total_queries)
                    .sum::<i64>(),
                1
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "query was not persisted"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    tokio::task::spawn_blocking(move || worker.shutdown())
        .await
        .unwrap();
}

async fn http_get_json(addr: std::net::SocketAddr, path: &str) -> Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
}

#[tokio::test]
async fn runtime_serves_dns_persists_to_http_and_drains_writer_on_shutdown() {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = [0; 4096];
            let (length, peer) = upstream.recv_from(&mut buffer).await.unwrap();
            let query = hickory_proto::op::Message::from_vec(&buffer[..length]).unwrap();
            let mut response = hickory_proto::op::Message::new(
                query.metadata.id,
                hickory_proto::op::MessageType::Response,
                query.metadata.op_code,
            );
            response.metadata.response_code = hickory_proto::op::ResponseCode::NoError;
            response.queries = query.queries;
            upstream
                .send_to(&response.to_vec().unwrap(), peer)
                .await
                .unwrap();
        });
        let database = TestDatabase(std::env::temp_dir().join(format!(
            "dnsbuffer-runtime-e2e-{}-{}.db",
            std::process::id(),
            rand::random::<u64>()
        )));
        assert!(!database.path().exists());
        let mut config = runtime_config(database.path());
        config.upstream = vec![dnsbuffer::config::UpstreamConfig::Plain {
            addr: upstream_addr,
        }];
        let runtime = build_runtime(&config).await.unwrap();
        let http_addr = runtime.http_addr();
        let dns_addr = runtime.dns_addr();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let running = tokio::spawn(runtime.run_until(async move {
            let _ = shutdown_rx.await;
            Ok(())
        }));

        let response = super_udp_query(dns_addr, "runtime.example.").await;
        assert_eq!(
            response.metadata.response_code,
            hickory_proto::op::ResponseCode::NoError
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let queries = http_get_json(http_addr, "/api/dashboard/queries").await;
            if queries["total"] == 1 {
                assert_eq!(queries["records"][0]["domain"], "runtime.example");
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "query was not exposed by HTTP"
            );
            tokio::task::yield_now().await;
        }

        shutdown.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), running)
            .await
            .expect("runtime shutdown timed out")
            .unwrap()
            .unwrap();
    })
    .await
    .expect("runtime end-to-end test timed out");
}

#[tokio::test]
async fn runtime_shutdown_flushes_a_responded_query_before_returning() {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = [0; 4096];
            let (length, peer) = upstream.recv_from(&mut buffer).await.unwrap();
            let query = hickory_proto::op::Message::from_vec(&buffer[..length]).unwrap();
            let mut response = hickory_proto::op::Message::new(
                query.metadata.id,
                hickory_proto::op::MessageType::Response,
                query.metadata.op_code,
            );
            response.metadata.response_code = hickory_proto::op::ResponseCode::NoError;
            response.queries = query.queries;
            upstream
                .send_to(&response.to_vec().unwrap(), peer)
                .await
                .unwrap();
        });
        let database = TestDatabase(std::env::temp_dir().join(format!(
            "dnsbuffer-runtime-flush-{}-{}.db",
            std::process::id(),
            rand::random::<u64>()
        )));
        let mut config = runtime_config(database.path());
        config.upstream = vec![dnsbuffer::config::UpstreamConfig::Plain {
            addr: upstream_addr,
        }];
        let runtime = build_runtime(&config).await.unwrap();
        let dns_addr = runtime.dns_addr();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let running = tokio::spawn(runtime.run_until(async move {
            let _ = shutdown_rx.await;
            Ok(())
        }));

        super_udp_query(dns_addr, "flush.example.").await;
        shutdown.send(()).unwrap();
        running.await.unwrap().unwrap();

        let path = database.path().to_owned();
        let (queries, total) = tokio::task::spawn_blocking(move || {
            let store = Store::open(&path).unwrap();
            let queries = store.queries(1, 10, None).unwrap();
            let total = store
                .trend(7, chrono::Utc::now().timestamp_millis())
                .unwrap()
                .buckets
                .iter()
                .map(|bucket| bucket.total_queries)
                .sum::<i64>();
            (queries, total)
        })
        .await
        .unwrap();
        assert_eq!(queries.total, 1);
        assert_eq!(queries.records[0].domain, "flush.example");
        assert_eq!(total, 1);
    })
    .await
    .expect("runtime writer drain test timed out");
}

#[tokio::test]
async fn failed_shutdown_still_closes_services_and_flushes_the_writer() {
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = [0; 4096];
            let (length, peer) = upstream.recv_from(&mut buffer).await.unwrap();
            let query = hickory_proto::op::Message::from_vec(&buffer[..length]).unwrap();
            let mut response = hickory_proto::op::Message::new(
                query.metadata.id,
                hickory_proto::op::MessageType::Response,
                query.metadata.op_code,
            );
            response.metadata.response_code = hickory_proto::op::ResponseCode::NoError;
            response.queries = query.queries;
            upstream
                .send_to(&response.to_vec().unwrap(), peer)
                .await
                .unwrap();
        });
        let database = TestDatabase(std::env::temp_dir().join(format!(
            "dnsbuffer-runtime-shutdown-error-{}-{}.db",
            std::process::id(),
            rand::random::<u64>()
        )));
        let mut config = runtime_config(database.path());
        config.upstream = vec![dnsbuffer::config::UpstreamConfig::Plain {
            addr: upstream_addr,
        }];
        let runtime = build_runtime(&config).await.unwrap();
        let http_addr = runtime.http_addr();
        let dns_addr = runtime.dns_addr();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let running = tokio::spawn(runtime.run_until(async move {
            let _ = shutdown_rx.await;
            anyhow::bail!("shutdown listener failed")
        }));

        super_udp_query(dns_addr, "shutdown-error.example.").await;
        shutdown.send(()).unwrap();
        let error = running.await.unwrap().unwrap_err();

        assert_eq!(error.to_string(), "shutdown listener failed");
        assert!(tokio::net::TcpStream::connect(http_addr).await.is_err());
        let path = database.path().to_owned();
        let total = tokio::task::spawn_blocking(move || {
            Store::open(&path)
                .unwrap()
                .queries(1, 10, None)
                .unwrap()
                .total
        })
        .await
        .unwrap();
        assert_eq!(total, 1);
    })
    .await
    .expect("failed shutdown cleanup test timed out");
}

async fn super_udp_query(addr: std::net::SocketAddr, domain: &str) -> hickory_proto::op::Message {
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut query = hickory_proto::op::Message::query();
    let mut question = hickory_proto::op::Query::new();
    question.set_name(domain.parse().unwrap());
    question.set_query_type(hickory_proto::rr::RecordType::A);
    query.add_query(question);
    client
        .send_to(&query.to_vec().unwrap(), addr)
        .await
        .unwrap();
    let mut buffer = [0; 4096];
    let (length, _) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.recv_from(&mut buffer),
    )
    .await
    .unwrap()
    .unwrap();
    hickory_proto::op::Message::from_vec(&buffer[..length]).unwrap()
}

fn event(domain: &str, ips: &[&str]) -> QueryEvent {
    QueryEvent {
        timestamp_ms: 1_753_488_000_000,
        domain: domain.into(),
        query_type: "A".into(),
        response_code: "NOERROR".into(),
        duration_ms: 12,
        blocked: false,
        cache_hit: true,
        response_ips: ips.iter().map(|ip| (*ip).into()).collect(),
    }
}

fn test_router(store: Store) -> axum::Router {
    test_router_with_metrics(store, UpstreamMetrics::default())
}

fn test_router_with_metrics(store: Store, upstreams: UpstreamMetrics) -> axum::Router {
    test_router_with_sampler(
        store,
        upstreams,
        Arc::new(CacheHitSampler::new(Arc::new(
            dnsbuffer::cache::Cache::new(10_000),
        ))),
    )
}

fn test_router_with_sampler(
    store: Store,
    upstreams: UpstreamMetrics,
    cache_sampler: Arc<CacheHitSampler>,
) -> axum::Router {
    router(HttpState {
        store: Arc::new(store),
        upstreams,
        retention_days: 7,
        database_reads: Arc::new(tokio::sync::Semaphore::new(4)),
        cache_sampler,
        cache_max_entries: 10_000,
    })
}

async fn request(app: axum::Router, uri: &str) -> axum::response::Response {
    app.oneshot(
        Request::get(uri)
            .body(Body::empty())
            .expect("valid test request"),
    )
    .await
    .unwrap()
}

async fn json(response: axum::response::Response) -> Value {
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn text(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn queries_validate_pagination_search_and_return_utc_timestamps() {
    let (_guard, store) = test_store("queries").await;
    let store = insert_events(
        store,
        vec![
            event("café+dns.example", &["1.1.1.1"]),
            event("other.test", &["192.0.2.1"]),
        ],
    )
    .await;
    let app = test_router(store);

    let domain = request(app.clone(), "/api/dashboard/queries?search=caf%C3%A9%2Bdns").await;
    assert_eq!(domain.status(), StatusCode::OK);
    let domain = json(domain).await;
    assert_eq!(domain["total"], 1);
    assert_eq!(domain["records"][0]["domain"], "café+dns.example");

    let response = request(
        app.clone(),
        "/api/dashboard/queries?page=1&page_size=50&search=%20%201.1%20%20",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json(response).await;
    assert_eq!(body["total"], 1);
    assert_eq!(body["records"][0]["domain"], "café+dns.example");
    assert_eq!(body["records"][0]["timestamp"], "2025-07-26T00:00:00Z");
    assert!(body["records"][0].get("timestamp_ms").is_none());

    for uri in [
        "/api/dashboard/queries?page=0",
        "/api/dashboard/queries?page=x",
        "/api/dashboard/queries?page_size=0",
        "/api/dashboard/queries?page_size=201",
        "/api/dashboard/queries?page_size=x",
        &format!("/api/dashboard/queries?search={}", "a".repeat(254)),
        &format!("/api/dashboard/queries?search={}", "界".repeat(254)),
    ] {
        assert_eq!(
            request(app.clone(), uri).await.status(),
            StatusCode::BAD_REQUEST
        );
    }

    let unicode_limit = format!("/api/dashboard/queries?search={}", "界".repeat(253));
    assert_eq!(request(app, &unicode_limit).await.status(), StatusCode::OK);
}

#[tokio::test]
async fn all_api_routes_are_json_read_only_and_use_utc_times() {
    let (guard, store) = test_store("api-routes").await;
    let mut first = event("popular.example", &["1.1.1.1", "2606:4700::1111"]);
    first.blocked = true;
    first.cache_hit = false;
    let mut second = event("popular.example", &[]);
    second.duration_ms = 25;
    let store = insert_events(store, vec![first, second, event("other.example", &[])]).await;
    let (_pipeline, metrics) = configured_metrics(guard.path()).await;
    let app = test_router_with_metrics(store, metrics);

    let trend = request(app.clone(), "/api/dashboard/trend").await;
    assert_eq!(trend.status(), StatusCode::OK);
    let trend = json(trend).await;
    for field in ["start", "end"] {
        assert!(
            chrono::DateTime::parse_from_rfc3339(trend[field].as_str().unwrap())
                .unwrap()
                .offset()
                .local_minus_utc()
                == 0
        );
    }
    assert_eq!(trend["granularity"], "hour");
    let bucket = &trend["buckets"][0];
    chrono::DateTime::parse_from_rfc3339(bucket["timestamp"].as_str().unwrap()).unwrap();
    assert!(bucket["total_queries"].is_i64());
    assert!(bucket["blocked_queries"].is_i64());
    assert!(bucket["cache_hits"].is_i64());

    let upstreams = json(request(app.clone(), "/api/dashboard/upstreams").await).await;
    assert_eq!(upstreams[0]["id"], "primary-0");
    assert_eq!(upstreams[0]["name"], "plain:1.1.1.1:53");
    assert_eq!(upstreams[0]["group"], "primary");
    assert_eq!(upstreams[0]["samples"], 0);
    assert_eq!(upstreams[0]["successes"], 0);
    assert_eq!(upstreams[0]["failure_rate"], 0.0);
    assert!(upstreams[0]["avg_latency_ms"].is_null());

    let response_time = json(request(app.clone(), "/api/dashboard/response-time").await).await;
    assert_eq!(response_time["samples"], 3);
    // durations: 12 + 25 + 12
    assert!((response_time["avg_ms"].as_f64().unwrap() - 49.0 / 3.0).abs() < 1e-9);

    let rankings = json(request(app.clone(), "/api/dashboard/rankings").await).await;
    assert_eq!(rankings[0]["domain"], "popular.example");
    assert_eq!(rankings[0]["total_queries"], 2);
    assert_eq!(rankings[0]["blocked_queries"], 1);
    assert_eq!(rankings[0]["cache_hits"], 1);
    assert_eq!(rankings[1]["domain"], "other.example");

    let queries = json(request(app, "/api/dashboard/queries").await).await;
    assert_eq!(queries["page"], 1);
    assert_eq!(queries["page_size"], 50);
    assert_eq!(queries["total"], 3);
    let record = queries["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["blocked"] == true)
        .unwrap();
    assert!(record["id"].is_i64());
    chrono::DateTime::parse_from_rfc3339(record["timestamp"].as_str().unwrap()).unwrap();
    assert_eq!(record["domain"], "popular.example");
    assert_eq!(record["query_type"], "A");
    assert_eq!(record["response_code"], "NOERROR");
    assert_eq!(record["duration_ms"], 12);
    assert_eq!(record["blocked"], true);
    assert_eq!(record["cache_hit"], false);
    assert_eq!(
        record["response_ips"],
        serde_json::json!(["1.1.1.1", "2606:4700::1111"])
    );
}

#[tokio::test]
async fn malformed_query_strings_return_json_bad_requests() {
    let (_guard, store) = test_store("malformed-parameters").await;
    let app = test_router(store);

    for uri in [
        "/api/dashboard/queries?unknown=value",
        "/api/dashboard/queries?page=1&page=2",
        "/api/dashboard/queries?search=%FF",
    ] {
        let response = request(app.clone(), uri).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert!(json(response).await["error"].is_string());
    }
}

#[tokio::test]
async fn queries_reject_page_offsets_that_sqlite_cannot_represent() {
    let (_guard, store) = test_store("page-offset").await;
    let app = test_router(store);

    let response = request(
        app,
        "/api/dashboard/queries?page=18446744073709551615&page_size=200",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json(response).await,
        serde_json::json!({"error": "page offset is out of range"})
    );
}

#[tokio::test]
async fn database_failures_return_sanitized_consistent_errors() {
    let (guard, store) = test_store("db-error").await;
    std::fs::remove_file(guard.path()).unwrap();
    std::fs::create_dir(guard.path()).unwrap();
    let app = test_router(store);

    for uri in [
        "/api/dashboard/trend",
        "/api/dashboard/queries",
        "/api/dashboard/rankings",
        "/api/dashboard/response-time",
    ] {
        let response = request(app.clone(), uri).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            json(response).await,
            serde_json::json!({"error": "dashboard database unavailable"})
        );
    }
}

#[tokio::test]
async fn cache_curve_endpoint_reports_observed_size_and_hit_rate_points() {
    let (_guard, store) = test_store("cache-curve-endpoint").await;
    let cache = Arc::new(dnsbuffer::cache::Cache::new(10_000));
    let sampler = Arc::new(CacheHitSampler::new(cache.clone()));
    let key = |name: &str| dnsbuffer::cache::CacheKey {
        name: name.into(),
        qtype: 1,
    };
    let cached_response = |name: &str| {
        let mut message = hickory_proto::op::Message::new(
            1,
            hickory_proto::op::MessageType::Response,
            hickory_proto::op::OpCode::Query,
        );
        message.metadata.response_code = hickory_proto::op::ResponseCode::NoError;
        let parsed: hickory_proto::rr::Name = format!("{name}.").parse().unwrap();
        let mut question = hickory_proto::op::Query::new();
        question.set_name(parsed.clone());
        question.set_query_type(hickory_proto::rr::RecordType::A);
        message.add_query(question);
        message.add_answer(hickory_proto::rr::Record::from_rdata(
            parsed,
            300,
            hickory_proto::rr::RData::A(hickory_proto::rr::rdata::A::new(192, 0, 2, 1)),
        ));
        message
    };
    // 观测 1：1 条缓存，累计 2 次查询 1 次命中
    cache.get(&key("hot.example"), 1);
    cache.put(key("hot.example"), cached_response("hot.example"));
    cache.get(&key("hot.example"), 2);
    sampler.observe();
    // 观测 2：2 条缓存，累计 3 次查询 2 次命中
    cache.put(key("second.example"), cached_response("second.example"));
    cache.get(&key("second.example"), 3);
    sampler.observe();

    let app = test_router_with_sampler(store, UpstreamMetrics::default(), sampler);
    let response = request(app, "/api/dashboard/cache-curve").await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = json(response).await;
    assert_eq!(body["max_entries"], 10_000);
    assert_eq!(body["current_entries"], 2);
    assert_eq!(body["current"]["size"], 2);
    let expected = 2.0 / 3.0;
    assert!((body["current"]["hit_rate"].as_f64().unwrap() - expected).abs() < 1e-9);
    let points = body["points"].as_array().unwrap();
    assert_eq!(points.len(), 3, "含初始 (0, 0) 观测点");
    assert_eq!(
        (points[0]["size"].as_u64(), points[0]["hit_rate"].as_f64()),
        (Some(0), Some(0.0))
    );
    assert_eq!(
        (points[1]["size"].as_u64(), points[1]["hit_rate"].as_f64()),
        (Some(1), Some(0.5))
    );
    assert_eq!(points[2]["size"], 2);
    assert!((points[2]["hit_rate"].as_f64().unwrap() - expected).abs() < 1e-9);
}

#[tokio::test]
async fn locked_database_returns_timeout_within_client_deadline_and_recovers() {
    let (_guard, store) = test_store("db-lock-timeout").await;
    let lock = store.connect().unwrap();
    lock.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;")
        .unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(4));
    let app = router(HttpState {
        store: Arc::new(store),
        upstreams: UpstreamMetrics::default(),
        retention_days: 7,
        database_reads: permits.clone(),
        cache_sampler: Arc::new(CacheHitSampler::new(Arc::new(
            dnsbuffer::cache::Cache::new(1),
        ))),
        cache_max_entries: 10_000,
    });
    let started = std::time::Instant::now();

    let response = request(app.clone(), "/api/dashboard/queries").await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json(response).await,
        serde_json::json!({"error": "dashboard database request timed out"})
    );
    assert!(started.elapsed() < std::time::Duration::from_millis(2_500));
    lock.execute_batch("ROLLBACK").unwrap();
    drop(lock);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while permits.available_permits() < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        request(app, "/api/dashboard/queries").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn serves_embedded_assets_and_method_errors() {
    let (_guard, store) = test_store("assets").await;
    let app = test_router(store);

    let index = request(app.clone(), "/").await;
    assert_eq!(index.status(), StatusCode::OK);
    assert_eq!(
        index.headers()[header::CONTENT_TYPE],
        "text/html; charset=utf-8"
    );
    let index = text(index).await;
    for marker in [
        "id=\"trend\"",
        "id=\"cache\"",
        "id=\"cache-chart\"",
        "id=\"rankings\"",
        "id=\"upstreams\"",
        "id=\"queries\"",
        "id=\"query-search\"",
        "id=\"previous-page\"",
        "id=\"next-page\"",
        "src=\"/assets/chart.js\"",
        "src=\"/assets/app.js\"",
    ] {
        assert!(index.contains(marker), "index missing {marker}");
    }

    for (uri, content_type) in [
        ("/assets/style.css", "text/css; charset=utf-8"),
        ("/assets/app.js", "text/javascript; charset=utf-8"),
    ] {
        let response = request(app.clone(), uri).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
        let _body = text(response).await;
    }

    let chart = request(app.clone(), "/assets/chart.js").await;
    assert_eq!(
        chart.headers()[header::CONTENT_TYPE],
        "text/javascript; charset=utf-8"
    );
    assert!(
        text(chart)
            .await
            .starts_with("/* dnsbuffer chart module v1.1.0 */")
    );

    let post = app
        .clone()
        .oneshot(
            Request::post("/api/dashboard/queries")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        request(app, "/api/dashboard/not-found").await.status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn embedded_frontend_contract() {
    let (_guard, store) = test_store("frontend-contract").await;
    let app = test_router(store);

    let index = text(request(app.clone(), "/").await).await;
    for marker in [
        "id=\"trend-chart\"",
        "id=\"upstream-list\"",
        "id=\"ranking-body\"",
        "id=\"query-search\"",
        "id=\"query-body\"",
        "id=\"previous-page\"",
        "id=\"next-page\"",
        "id=\"refresh-status\"",
        "id=\"refresh-button\"",
        "id=\"avg-response-value\"",
        "id=\"trend-range\"",
        "id=\"trend-summary\"",
        "aria-label=",
    ] {
        assert!(index.contains(marker), "index missing {marker}");
    }

    let app_js = text(request(app.clone(), "/assets/app.js").await).await;
    for marker in [
        "refresh-button",
        "response-time",
        "AbortController",
        "encodeURIComponent(state.search)",
        "page: 1",
        "root.DnsDashboard",
        "DOMContentLoaded",
        "createRoundTracker",
        "queryResponseDecision",
    ] {
        assert!(app_js.contains(marker), "app.js missing {marker}");
    }

    let chart_js = text(request(app, "/assets/chart.js").await).await;
    for marker in ["granularity", "setLineDash", "normalizeValue"] {
        assert!(chart_js.contains(marker), "chart.js missing {marker}");
    }
}
