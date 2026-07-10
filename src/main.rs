#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = weir::gateway::health_router();
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("weir listening on 0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}
