use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum WeirError {
    #[error("tenant '{0}' has exceeded its token budget")]
    BudgetExceeded(String),
    #[error("unknown tenant or missing X-Weir-Tenant header")]
    UnknownTenant,
    #[error("upstream provider request failed: {0}")]
    Upstream(#[from] reqwest::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}

impl IntoResponse for WeirError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            WeirError::BudgetExceeded(_) => (StatusCode::TOO_MANY_REQUESTS, "budget_exceeded"),
            WeirError::UnknownTenant => (StatusCode::UNAUTHORIZED, "unknown_tenant"),
            WeirError::Upstream(_) => (StatusCode::BAD_GATEWAY, "upstream_error"),
            WeirError::Config(_) => (StatusCode::INTERNAL_SERVER_ERROR, "config_error"),
        };
        let body = ErrorBody { error: code, message: self.to_string() };
        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_exceeded_maps_to_429() {
        let response = WeirError::BudgetExceeded("acct_1".into()).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn unknown_tenant_maps_to_401() {
        let response = WeirError::UnknownTenant.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
