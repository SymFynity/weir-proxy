use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
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

const TENANT_HEADER: &str = "x-weir-tenant";

#[derive(Clone)]
pub struct AppState {
    pub budget: Arc<BudgetRegistry>,
    pub tokenizer: Arc<Tokenizer>,
    pub http: reqwest::Client,
    pub openai_base: String,
    pub anthropic_base: String,
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
        .with_state(state)
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

    let base = match provider {
        Provider::OpenAi => &state.openai_base,
        Provider::Anthropic => &state.anthropic_base,
    };
    let url = format!("{base}/{rest}");

    let mut upstream_req = state.http.request(method, &url).body(body);
    for (name, value) in headers.iter() {
        if name.as_str() != TENANT_HEADER {
            upstream_req = upstream_req.header(name, value);
        }
    }

    let upstream_res = match upstream_req.send().await {
        Ok(res) => res,
        Err(e) => return with_connection_close(WeirError::Upstream(e).into_response()),
    };

    let status = upstream_res.status();
    let adapter = state.tokenizer.new_adapter(provider);
    let stream = enforcer::enforce(
        tenant,
        upstream_res.bytes_stream(),
        adapter,
        state.budget.clone(),
        now_ms,
    );

    let mut response = Response::builder()
        .status(status)
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(stream))
        .unwrap();
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
    use crate::config::{BudgetLimit, TenantLimits};
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
        AppState {
            budget: Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(limits)))),
            tokenizer: Arc::new(Tokenizer::load()),
            http: reqwest::Client::new(),
            openai_base: "http://127.0.0.1:1".into(), // unreachable on purpose for this test
            anthropic_base: "http://127.0.0.1:1".into(),
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
}
