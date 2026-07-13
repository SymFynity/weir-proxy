use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::budget::BudgetRegistry;
use crate::enforcer;
use crate::error::WeirError;
use crate::provider::{Provider, Tokenizer};
use crate::telemetry::{EventLog, UsageEvent};

const TENANT_HEADER: &str = "x-weir-tenant";

#[derive(Clone)]
pub struct AppState {
    pub budget: Arc<BudgetRegistry>,
    pub tokenizer: Arc<Tokenizer>,
    pub http: reqwest::Client,
    pub openai_base: String,
    pub anthropic_base: String,
    pub events: Arc<EventLog>,
}

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { "ok" }))
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/openai/*rest",
            any(|state, headers, method, path, body| {
                proxy(state, headers, method, path, body, Provider::OpenAi)
            }),
        )
        .route(
            "/anthropic/*rest",
            any(|state, headers, method, path, body| {
                proxy(state, headers, method, path, body, Provider::Anthropic)
            }),
        )
        .route("/events", get(events_handler))
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024)) // 100MiB: generous for vision/long-context payloads, but bounded
        .with_state(state)
}

/// Headers that must never be blindly forwarded between Weir and an
/// upstream provider: hop-by-hop headers (meaningful only for one specific
/// connection, not the end-to-end request/response) plus `content-length`
/// (inaccurate once the body is re-streamed).
fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

/// True if this client request header should be forwarded upstream
/// unmodified. Excludes Weir's own routing header, `host` (must be derived
/// from the actual upstream URL, not copied from the client-facing
/// request), `accept-encoding` (Weir's HTTP client doesn't decompress
/// responses, so honoring a client's compression request would make the
/// adapter parse compressed bytes as SSE text — a silent enforcement
/// bypass, since it would estimate near-zero tokens for content it can't
/// read), and general hop-by-hop headers.
fn should_forward_request_header(name: &HeaderName) -> bool {
    let n = name.as_str();
    n != TENANT_HEADER && n != "host" && n != "accept-encoding" && !is_hop_by_hop(name)
}

#[derive(serde::Deserialize)]
struct EventsQuery {
    since: Option<u64>,
    limit: Option<usize>,
}

async fn events_handler(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> axum::Json<Vec<UsageEvent>> {
    let since = query.since.unwrap_or(0);
    let limit = query.limit.unwrap_or(100).min(1000);
    axum::Json(state.events.since(since, limit))
}

fn extract_model_name(body: &Bytes) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct ModelField {
        model: Option<String>,
    }
    serde_json::from_slice::<ModelField>(body).ok()?.model
}

async fn proxy(
    State(state): State<AppState>,
    headers: HeaderMap,
    method: Method,
    Path(rest): Path<String>,
    body: Bytes,
    provider: Provider,
) -> Response {
    let tenant = match headers.get(TENANT_HEADER).and_then(|v| v.to_str().ok()) {
        Some(t) => t.to_string(),
        None => return WeirError::UnknownTenant.into_response(),
    };

    let now = now_ms();
    match state.budget.is_within_budget(&tenant, now) {
        Ok(true) => {}
        Ok(false) => {
            return with_connection_close(WeirError::BudgetExceeded(tenant).into_response())
        }
        Err(e) => return with_connection_close(e.into_response()),
    }

    let policy = match state.budget.policy_for(&tenant) {
        Ok(p) => p,
        Err(e) => return with_connection_close(e.into_response()),
    };

    let model = extract_model_name(&body);
    if let Some(model_name) = &model {
        if policy.blocked_models.contains(model_name) {
            state.events.push(UsageEvent {
                id: 0,
                tenant: tenant.clone(),
                provider,
                model: model.clone(),
                tools_called: Vec::new(),
                tokens: 0,
                blocked: true,
                block_reason: Some(format!("blocked_model:{model_name}")),
                timestamp_ms: now_ms(),
            });
            // The client-facing error names the specific blocked model: the
            // caller already knows its own model names, and revealing the
            // NAME (never the upstream response content or tool arguments) is
            // better DX for a governance product. This matches the specific
            // format used by the internal `UsageEvent.block_reason` above.
            return with_connection_close(
                WeirError::PolicyViolation { tenant, reason: format!("blocked_model:{model_name}") }
                    .into_response(),
            );
        }
    }

    let base = match provider {
        Provider::OpenAi => &state.openai_base,
        Provider::Anthropic => &state.anthropic_base,
    };
    let url = format!("{base}/{rest}");

    let mut upstream_req = state.http.request(method, &url).body(body);
    for (name, value) in headers.iter() {
        if should_forward_request_header(name) {
            upstream_req = upstream_req.header(name, value);
        }
    }

    let upstream_res = match upstream_req.send().await {
        Ok(res) => res,
        Err(e) => return with_connection_close(WeirError::Upstream(e).into_response()),
    };

    let status = upstream_res.status();
    let upstream_headers = upstream_res.headers().clone();
    // Real OpenAI/Anthropic responses always set Content-Type, so this
    // check is reliable in practice. A response with no Content-Type at
    // all falls to the non-streaming path below; if it were actually a
    // stream, that response is buffered whole and forwarded correctly but
    // without incremental delivery or enforcement for that one response,
    // since non_streaming_cost can't parse SSE text as JSON. This is a
    // known, low-likelihood edge against compliant providers, not a case
    // worth adding stream-sniffing complexity for.
    let is_streaming = upstream_headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.starts_with("text/event-stream"))
        .unwrap_or(false);

    if is_streaming {
        let adapter = state.tokenizer.new_adapter(provider);
        let stream = enforcer::enforce(
            tenant,
            provider,
            model,
            upstream_res.bytes_stream(),
            adapter,
            state.budget.clone(),
            policy.blocked_tools,
            state.events.clone(),
            now_ms,
        );

        let mut response_builder = Response::builder().status(status);
        for (name, value) in upstream_headers.iter() {
            if !is_hop_by_hop(name) {
                response_builder = response_builder.header(name, value);
            }
        }

        let mut response = response_builder.body(Body::from_stream(stream)).unwrap();
        response.headers_mut().insert(
            HeaderName::from_static("connection"),
            HeaderValue::from_static("close"),
        );
        return response;
    }

    // Non-streaming: the whole response is one atomic unit, and it always
    // carries its own authoritative usage — no bytes have reached the
    // client yet, so we buffer the full body, check its tool calls against
    // policy and record its token usage, and only forward it if both
    // checks pass. A response that violates policy or exceeds budget is
    // rejected outright (a real error status, not a mid-stream trip)
    // rather than delivered to the client.
    let body_bytes = match upstream_res.bytes().await {
        Ok(b) => b,
        Err(e) => return with_connection_close(WeirError::Upstream(e).into_response()),
    };

    let adapter = state.tokenizer.new_adapter(provider);
    let cost = adapter.non_streaming_cost(&body_bytes);

    for tool in &cost.tool_calls {
        if policy.blocked_tools.contains(tool) {
            state.events.push(UsageEvent {
                id: 0,
                tenant: tenant.clone(),
                provider,
                model: model.clone(),
                tools_called: cost.tool_calls.clone(),
                tokens: cost.total_tokens.unwrap_or(0),
                blocked: true,
                block_reason: Some(format!("blocked_tool:{tool}")),
                timestamp_ms: now_ms(),
            });
            // The client-facing error names the specific blocked tool,
            // consistent with the streaming path's `policy_violation` terminal
            // event. Only the tool NAME is revealed — the upstream response
            // content and tool call ARGUMENTS must never reach the client.
            // This matches the specific format used by the internal
            // `UsageEvent.block_reason` above.
            return with_connection_close(
                WeirError::PolicyViolation { tenant, reason: format!("blocked_tool:{tool}") }
                    .into_response(),
            );
        }
    }

    if let Some(total) = cost.total_tokens {
        match state.budget.record(&tenant, total, now_ms()) {
            Ok(true) => {}
            Ok(false) => {
                state.events.push(UsageEvent {
                    id: 0,
                    tenant: tenant.clone(),
                    provider,
                    model: model.clone(),
                    tools_called: cost.tool_calls.clone(),
                    tokens: total,
                    blocked: true,
                    block_reason: Some("budget_exceeded".to_string()),
                    timestamp_ms: now_ms(),
                });
                return with_connection_close(WeirError::BudgetExceeded(tenant).into_response());
            }
            Err(e) => return with_connection_close(e.into_response()),
        }
    }

    state.events.push(UsageEvent {
        id: 0,
        tenant,
        provider,
        model,
        tools_called: cost.tool_calls,
        tokens: cost.total_tokens.unwrap_or(0),
        blocked: false,
        block_reason: None,
        timestamp_ms: now_ms(),
    });

    let mut response_builder = Response::builder().status(status);
    for (name, value) in upstream_headers.iter() {
        if !is_hop_by_hop(name) {
            response_builder = response_builder.header(name, value);
        }
    }
    let mut response = response_builder.body(Body::from(body_bytes)).unwrap();
    response.headers_mut().insert(
        HeaderName::from_static("connection"),
        HeaderValue::from_static("close"),
    );
    response
}

fn with_connection_close(mut response: Response) -> Response {
    response.headers_mut().insert(
        HeaderName::from_static("connection"),
        HeaderValue::from_static("close"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BudgetLimit, ParsedConfig, TenantLimits};
    use crate::telemetry::EventLog;
    use arc_swap::ArcSwap;
    use axum::body::Body as AxumBody;
    use axum::http::Request;
    use std::collections::HashMap;
    use std::time::Duration;
    use tower::ServiceExt;

    fn state_with_tenant(tenant: &str, max_tokens: u64) -> AppState {
        let mut limits: TenantLimits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(60) },
        );
        // Real config loading (see `config::parse`) always populates a
        // policy entry for every tenant that has a limit entry, even if
        // that policy is empty — keep that invariant here too, since
        // `BudgetRegistry::policy_for` treats a tenant absent from the
        // policies map as entirely unknown (401), not "known but
        // unrestricted".
        let mut policies = HashMap::new();
        policies.insert(tenant.to_string(), crate::config::PolicyConfig::default());
        let parsed = ParsedConfig { limits, policies };
        AppState {
            budget: Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed)))),
            tokenizer: Arc::new(Tokenizer::load()),
            http: reqwest::Client::new(),
            openai_base: "http://127.0.0.1:1".into(), // unreachable on purpose for this test
            anthropic_base: "http://127.0.0.1:1".into(),
            events: Arc::new(EventLog::new(1000)),
        }
    }

    #[tokio::test]
    async fn missing_tenant_header_is_rejected() {
        let app = router(state_with_tenant("acct_1", 1000));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn tenant_already_over_budget_gets_429_at_admission() {
        let app = router(state_with_tenant("acct_1", 0)); // zero budget: always over
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get("connection").unwrap(), "close");
    }

    #[tokio::test]
    async fn successful_proxy_strips_tenant_header_forwards_auth_and_sets_connection_close() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw("data: {\"choices\":[{\"delta\":{}}]}\n\n", "text/event-stream"),
            )
            .mount(&mock)
            .await;

        let mut state = state_with_tenant("acct_1", 1000);
        state.openai_base = mock.uri();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .header("authorization", "Bearer real-secret")
                    .header("host", "weir.internal.example")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("connection").unwrap(), "close");

        let received = mock.received_requests().await.unwrap();
        assert_eq!(received.len(), 1);
        let upstream_req = &received[0];
        assert_eq!(
            upstream_req.headers.get("authorization").unwrap(),
            "Bearer real-secret",
            "client's real credentials must reach the upstream provider unmodified"
        );
        assert!(
            !upstream_req.headers.contains_key("x-weir-tenant"),
            "Weir's own routing header must never be forwarded upstream"
        );
        assert_ne!(
            upstream_req.headers.get("host").map(|v| v.to_str().unwrap()),
            Some("weir.internal.example"),
            "the client-facing Host header must not override the upstream provider's own host"
        );
    }

    #[tokio::test]
    async fn large_request_body_is_not_rejected_by_body_limit() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw("data: {}\n\n", "text/event-stream"),
            )
            .mount(&mock)
            .await;

        let mut state = state_with_tenant("acct_1", 1000);
        state.openai_base = mock.uri();
        let app = router(state);

        // Larger than Axum's default 2MB request-body limit, but comfortably
        // under Weir's raised 100MiB cap (see `router()`).
        let large_body = vec![b'x'; 3 * 1024 * 1024];
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::from(large_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a request body larger than axum's default 2MB limit, but under Weir's 100MiB cap, must not be rejected"
        );
    }

    #[tokio::test]
    async fn body_exceeding_raised_cap_is_still_rejected() {
        // Proves the raised limit is a bound, not a full disable: a body
        // past the 100MiB cap must still be rejected with 413, before ever
        // reaching a mock upstream.
        let state = state_with_tenant("acct_1", 1000);
        let app = router(state);

        let oversized_body = vec![b'x'; 100 * 1024 * 1024 + 1];
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::from(oversized_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "a request body larger than Weir's 100MiB cap must be rejected with 413"
        );
    }

    #[tokio::test]
    async fn non_streaming_response_usage_is_recorded_against_budget() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(
                    "{\"choices\":[{\"message\":{\"content\":\"Hi\"}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}",
                    "application/json",
                ),
            )
            .mount(&mock)
            .await;

        // Budget of exactly 7 tokens: the first non-streaming response
        // lands exactly at the ceiling (allowed, matching BudgetRegistry's
        // record() <= semantics), and a second request should then be
        // rejected at admission.
        let mut state = state_with_tenant("acct_1", 7);
        state.openai_base = mock.uri();
        let app = router(state);

        let make_request = || {
            Request::builder()
                .uri("/openai/v1/chat/completions")
                .method("POST")
                .header(TENANT_HEADER, "acct_1")
                .body(AxumBody::empty())
                .unwrap()
        };

        let first = app.clone().oneshot(make_request()).await.unwrap();
        assert_eq!(
            first.status(),
            StatusCode::OK,
            "first non-streaming response should be forwarded and its usage recorded"
        );
        let body = axum::body::to_bytes(first.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("Hi"));

        let second = app.oneshot(make_request()).await.unwrap();
        assert_eq!(
            second.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "the 7-token usage from the first non-streaming response must have been recorded, tripping admission for the second request"
        );
    }

    #[tokio::test]
    async fn non_streaming_response_over_budget_is_rejected() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(
                    "{\"choices\":[{\"message\":{\"content\":\"Hi\"}}],\"usage\":{\"prompt_tokens\":50,\"completion_tokens\":50,\"total_tokens\":100}}",
                    "application/json",
                ),
            )
            .mount(&mock)
            .await;

        let mut state = state_with_tenant("acct_1", 10); // ceiling far below the 100-token response
        state.openai_base = mock.uri();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a non-streaming response whose usage alone exceeds the ceiling must be rejected, not forwarded to the client"
        );
    }

    #[tokio::test]
    async fn blocked_model_is_rejected_before_any_upstream_call() {
        let mut state = state_with_tenant("acct_1", 1000);
        // Point at an address nothing is listening on — if the request
        // ever reached this point, the test would hang/error on connect,
        // proving the block happened before any upstream call.
        state.openai_base = "http://127.0.0.1:1".into();

        let mut limits = HashMap::new();
        limits.insert(
            "acct_1".to_string(),
            BudgetLimit { max_tokens: 1000, window: Duration::from_secs(60) },
        );
        let mut policies = HashMap::new();
        policies.insert(
            "acct_1".to_string(),
            crate::config::PolicyConfig {
                blocked_models: vec!["gpt-3.5-turbo".to_string()],
                blocked_tools: Vec::new(),
            },
        );
        let parsed = crate::config::ParsedConfig { limits, policies };
        state.budget = Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed))));

        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::from(r#"{"model":"gpt-3.5-turbo"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn events_endpoint_returns_pushed_events() {
        let state = state_with_tenant("acct_1", 1000);
        state.events.push(UsageEvent {
            id: 0,
            tenant: "acct_1".to_string(),
            provider: Provider::OpenAi,
            model: Some("gpt-4o-mini".to_string()),
            tools_called: Vec::new(),
            tokens: 10,
            blocked: false,
            block_reason: None,
            timestamp_ms: 0,
        });
        let app = router(state);

        let response = app
            .oneshot(Request::builder().uri("/events").body(AxumBody::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let events: Vec<UsageEvent> = serde_json::from_slice(&body).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant, "acct_1");
    }

    #[tokio::test]
    async fn non_streaming_blocked_tool_is_rejected_not_forwarded() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "{\"choices\":[{\"message\":{\"content\":\"CONFIDENTIAL_UPSTREAM_TEXT\",\"tool_calls\":[{\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"send_email\",\"arguments\":\"{\\\"secret\\\":\\\"ARGUMENT_SECRET\\\"}\"}}]}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}",
                "application/json",
            ))
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
            crate::config::PolicyConfig {
                blocked_models: Vec::new(),
                blocked_tools: vec!["send_email".to_string()],
            },
        );
        let parsed = crate::config::ParsedConfig { limits, policies };

        let mut state = state_with_tenant("acct_1", 1000);
        state.budget = Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed))));
        state.openai_base = mock.uri();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains("send_email"),
            "the blocked tool name is intentionally revealed to the client in the error reason"
        );
        assert!(
            !body.contains("CONFIDENTIAL_UPSTREAM_TEXT"),
            "the upstream response content must never be forwarded to the client on a policy reject"
        );
        assert!(
            !body.contains("ARGUMENT_SECRET"),
            "tool call arguments must never be forwarded to the client"
        );
    }
}
