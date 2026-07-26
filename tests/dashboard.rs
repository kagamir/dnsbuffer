use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use dnsbuffer::dashboard::QueryEvent;
use dnsbuffer::dashboard::http::{HttpState, router};
use dnsbuffer::dashboard::store::Store;
use dnsbuffer::dashboard::upstreams::{UpstreamMetrics, UpstreamMetricsBuilder};
use dnsbuffer::stats::UpstreamStats;
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
    router(HttpState {
        store: Arc::new(store),
        upstreams,
        retention_days: 7,
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
    let (_guard, store) = test_store("api-routes").await;
    let mut first = event("popular.example", &["1.1.1.1", "2606:4700::1111"]);
    first.blocked = true;
    first.cache_hit = false;
    let mut second = event("popular.example", &[]);
    second.duration_ms = 25;
    let store = insert_events(store, vec![first, second, event("other.example", &[])]).await;
    let stats = Arc::new(Mutex::new(UpstreamStats::new(4)));
    stats.lock().unwrap().record_failure();
    let mut metrics = UpstreamMetricsBuilder::default();
    metrics.register("plain:1.1.1.1:53".into(), "primary", stats);
    let app = test_router_with_metrics(store, metrics.build());

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
    assert_eq!(upstreams[0]["samples"], 1);
    assert_eq!(upstreams[0]["successes"], 0);
    assert_eq!(upstreams[0]["failure_rate"], 1.0);
    assert!(upstreams[0]["avg_latency_ms"].is_null());

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
        "id=\"rankings\"",
        "id=\"upstreams\"",
        "id=\"queries\"",
        "id=\"search\"",
        "id=\"previous\"",
        "id=\"next\"",
        "src=\"/chart.js\"",
        "src=\"/app.js\"",
    ] {
        assert!(index.contains(marker), "index missing {marker}");
    }

    for (uri, content_type) in [
        ("/style.css", "text/css; charset=utf-8"),
        ("/app.js", "text/javascript; charset=utf-8"),
    ] {
        let response = request(app.clone(), uri).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
        let _body = text(response).await;
    }

    let chart = request(app.clone(), "/chart.js").await;
    assert_eq!(
        chart.headers()[header::CONTENT_TYPE],
        "text/javascript; charset=utf-8"
    );
    assert!(
        text(chart)
            .await
            .starts_with("/* dnsbuffer chart module v1.0.0 */")
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
