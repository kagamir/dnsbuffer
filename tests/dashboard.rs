use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use dnsbuffer::dashboard::QueryEvent;
use dnsbuffer::dashboard::http::{HttpState, router};
use dnsbuffer::dashboard::store::Store;
use dnsbuffer::dashboard::upstreams::UpstreamMetrics;
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

fn test_store(name: &str) -> (TestDatabase, Store) {
    let path = std::env::temp_dir().join(format!(
        "dnsbuffer-http-{name}-{}-{}.db",
        std::process::id(),
        rand::random::<u64>()
    ));
    let guard = TestDatabase(path);
    let store = Store::open(guard.path()).unwrap();
    (guard, store)
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
    router(HttpState {
        store: Arc::new(store),
        upstreams: UpstreamMetrics::default(),
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

#[tokio::test]
async fn queries_validate_pagination_search_and_return_utc_timestamps() {
    let (_guard, store) = test_store("queries");
    store
        .insert_events(&[
            event("example.com", &["1.1.1.1"]),
            event("other.test", &["192.0.2.1"]),
        ])
        .unwrap();
    let app = test_router(store);

    let response = request(
        app.clone(),
        "/api/dashboard/queries?page=1&page_size=50&search=%20%201.1%20%20",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json(response).await;
    assert_eq!(body["total"], 1);
    assert_eq!(body["records"][0]["domain"], "example.com");
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
    let (_guard, store) = test_store("api-routes");
    store
        .insert_events(&[event("example.com", &["1.1.1.1"])])
        .unwrap();
    let app = test_router(store);

    let trend = request(app.clone(), "/api/dashboard/trend").await;
    assert_eq!(trend.status(), StatusCode::OK);
    let trend = json(trend).await;
    assert!(trend["start"].as_str().unwrap().ends_with('Z'));
    assert!(trend["end"].as_str().unwrap().ends_with('Z'));
    assert!(
        trend["buckets"][0]["timestamp"]
            .as_str()
            .unwrap()
            .ends_with('Z')
    );

    for uri in [
        "/api/dashboard/rankings",
        "/api/dashboard/upstreams",
        "/api/dashboard/queries",
    ] {
        let response = request(app.clone(), uri).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let _ = json(response).await;
    }

    let queries = json(request(app, "/api/dashboard/queries").await).await;
    assert_eq!(queries["total"], 1);
}

#[tokio::test]
async fn malformed_query_strings_return_json_bad_requests() {
    let (_guard, store) = test_store("malformed-parameters");
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
async fn database_failures_return_sanitized_consistent_errors() {
    let (guard, store) = test_store("db-error");
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
    let (_guard, store) = test_store("assets");
    let app = test_router(store);

    for (uri, content_type) in [
        ("/", "text/html; charset=utf-8"),
        ("/style.css", "text/css; charset=utf-8"),
        ("/chart.js", "text/javascript; charset=utf-8"),
        ("/app.js", "text/javascript; charset=utf-8"),
    ] {
        let response = request(app.clone(), uri).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
    }

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
