use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use weir::budget::BudgetRegistry;
use weir::config::{BudgetLimit, TenantLimits};
use weir::gateway::{router, AppState};
use weir::provider::Tokenizer;

#[tokio::test]
async fn healthz_returns_ok() {
    let app = weir::gateway::health_router();
    let response = app
        .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

fn state_pointed_at(mock_base: &str, tenant: &str, max_tokens: u64) -> AppState {
    let mut limits: TenantLimits = HashMap::new();
    limits.insert(
        tenant.to_string(),
        BudgetLimit { max_tokens, window: Duration::from_secs(60) },
    );
    // Real config loading (see `config::parse`) always populates a policy
    // entry for every tenant that has a limit entry, even if that policy is
    // empty — `BudgetRegistry::policy_for` treats a tenant absent from the
    // policies map as entirely unknown (401), not "known but unrestricted".
    let mut policies = HashMap::new();
    policies.insert(tenant.to_string(), weir::config::PolicyConfig::default());
    let parsed = weir::config::ParsedConfig { limits, policies };
    AppState {
        budget: Arc::new(BudgetRegistry::new(Arc::new(arc_swap::ArcSwap::from_pointee(parsed)))),
        tokenizer: Arc::new(Tokenizer::load()),
        http: reqwest::Client::new(),
        openai_base: mock_base.to_string(),
        anthropic_base: mock_base.to_string(),
        events: Arc::new(weir::telemetry::EventLog::new(1000)),
    }
}

#[tokio::test]
async fn happy_path_forwards_full_stream_within_budget() {
    let mock = MockServer::start().await;
    let sse_body = "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n\
data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(sse_body, "text/event-stream"),
        )
        .mount(&mock)
        .await;

    let app = router(state_pointed_at(&mock.uri(), "acct_1", 1000));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/openai/v1/chat/completions")
                .method("POST")
                .header("x-weir-tenant", "acct_1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("Hi"));
    assert!(!body.contains("budget_exceeded"));
}

#[tokio::test]
async fn mid_stream_trip_never_forwards_over_budget_chunk() {
    let mock = MockServer::start().await;
    // Each content chunk costs a handful of tokens; ceiling of 1 token
    // guarantees the very first content chunk already trips.
    let sse_body = "data: {\"choices\":[{\"delta\":{\"content\":\"This is more than one token\"}}]}\n\n\
data: {\"choices\":[{\"delta\":{\"content\":\"should never be forwarded\"}}]}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(sse_body, "text/event-stream"),
        )
        .mount(&mock)
        .await;

    let app = router(state_pointed_at(&mock.uri(), "acct_1", 1));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/openai/v1/chat/completions")
                .method("POST")
                .header("x-weir-tenant", "acct_1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK); // headers already committed before the trip
    assert_eq!(response.headers().get("connection").unwrap(), "close");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("budget_exceeded"));
    assert!(!body.contains("This is more than one token"));
    assert!(!body.contains("should never be forwarded"));
}

#[tokio::test]
async fn streaming_response_with_blocked_tool_trips_and_is_never_forwarded() {
    let mock = MockServer::start().await;
    let sse_body = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"send_email\",\"arguments\":\"{}\"}}]}}]}\n\n\
data: {\"choices\":[{\"delta\":{\"content\":\"should never be forwarded\"}}]}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse_body, "text/event-stream"))
        .mount(&mock)
        .await;

    let mut limits = HashMap::new();
    limits.insert(
        "acct_1".to_string(),
        BudgetLimit { max_tokens: 1000, window: Duration::from_secs(60) },
    );
    let mut policies = HashMap::new();
    policies.insert(
        "acct_1".to_string(),
        weir::config::PolicyConfig {
            blocked_models: Vec::new(),
            blocked_tools: vec!["send_email".to_string()],
        },
    );
    let parsed = weir::config::ParsedConfig { limits, policies };

    let state = AppState {
        budget: Arc::new(BudgetRegistry::new(Arc::new(arc_swap::ArcSwap::from_pointee(parsed)))),
        tokenizer: Arc::new(Tokenizer::load()),
        http: reqwest::Client::new(),
        openai_base: mock.uri(),
        anthropic_base: mock.uri(),
        events: Arc::new(weir::telemetry::EventLog::new(100)),
    };
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/openai/v1/chat/completions")
                .method("POST")
                .header("x-weir-tenant", "acct_1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK); // headers already committed before the trip
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("policy_violation"));
    assert!(!body.contains("should never be forwarded"));
}

#[tokio::test]
async fn events_endpoint_reflects_a_completed_request() {
    let mock = MockServer::start().await;
    let sse_body = "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n\
data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse_body, "text/event-stream"))
        .mount(&mock)
        .await;

    let mut limits = HashMap::new();
    limits.insert(
        "acct_1".to_string(),
        BudgetLimit { max_tokens: 1000, window: Duration::from_secs(60) },
    );
    let mut policies = HashMap::new();
    policies.insert("acct_1".to_string(), weir::config::PolicyConfig::default());
    let parsed = weir::config::ParsedConfig { limits, policies };

    let state = AppState {
        budget: Arc::new(BudgetRegistry::new(Arc::new(arc_swap::ArcSwap::from_pointee(parsed)))),
        tokenizer: Arc::new(Tokenizer::load()),
        http: reqwest::Client::new(),
        openai_base: mock.uri(),
        anthropic_base: mock.uri(),
        events: Arc::new(weir::telemetry::EventLog::new(100)),
    };
    let app = router(state);

    let first_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/openai/v1/chat/completions")
                .method("POST")
                .header("x-weir-tenant", "acct_1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // The streaming response body is a lazy stream that drives policy/budget
    // enforcement (and the resulting event push) as it is polled — it must
    // be fully consumed here for the event to have landed before /events is
    // queried below, same as the other streaming tests in this file.
    let _ = to_bytes(first_response.into_body(), usize::MAX).await.unwrap();

    let events_response = app
        .oneshot(Request::builder().uri("/events").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(events_response.status(), StatusCode::OK);
    let body = to_bytes(events_response.into_body(), usize::MAX).await.unwrap();
    let events: Vec<weir::telemetry::UsageEvent> = serde_json::from_slice(&body).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].tenant, "acct_1");
    assert_eq!(events[0].outcome, weir::telemetry::UsageOutcome::Completed);
}
