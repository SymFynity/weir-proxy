use axum::routing::get;
use axum::Router;

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { "ok" }))
}
