use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use weir::budget::BudgetRegistry;
use weir::config;
use weir::gateway::{router, AppState};
use weir::provider::Tokenizer;
use weir::telemetry::EventLog;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config_path = env::var("WEIR_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("weir.toml"));

    let event_log_capacity: usize = env::var("WEIR_EVENT_LOG_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let shared_config = config::load_shared(&config_path)
        .unwrap_or_else(|e| panic!("failed to load config at {}: {e}", config_path.display()));

    let _watcher = config::watch(config_path.clone(), shared_config.clone())
        .unwrap_or_else(|e| panic!("failed to watch config at {}: {e}", config_path.display()));

    let generation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_string());

    let state = AppState {
        budget: Arc::new(BudgetRegistry::new(shared_config)),
        tokenizer: Arc::new(Tokenizer::load()),
        http: reqwest::Client::new(),
        openai_base: env::var("WEIR_OPENAI_BASE")
            .unwrap_or_else(|_| "https://api.openai.com".to_string()),
        anthropic_base: env::var("WEIR_ANTHROPIC_BASE")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string()),
        events: Arc::new(EventLog::new(event_log_capacity)),
        generation,
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("weir listening on 0.0.0.0:8080");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

/// Waits for Ctrl+C or SIGTERM. Without this, Weir (running as PID 1 in a
/// container with no init process) never installs a signal handler, so the
/// kernel doesn't apply the default terminate action for either signal to
/// PID 1 — Ctrl+C and `docker stop` are both silently ignored until the
/// stop grace period expires and Docker escalates to SIGKILL.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl+C, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
