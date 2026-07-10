use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use weir::budget::BudgetRegistry;
use weir::config;
use weir::gateway::{router, AppState};
use weir::provider::Tokenizer;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config_path = env::var("WEIR_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("weir.toml"));

    let shared_config = config::load_shared(&config_path)
        .unwrap_or_else(|e| panic!("failed to load config at {}: {e}", config_path.display()));

    let _watcher = config::watch(config_path.clone(), shared_config.clone())
        .unwrap_or_else(|e| panic!("failed to watch config at {}: {e}", config_path.display()));

    let state = AppState {
        budget: Arc::new(BudgetRegistry::new(shared_config)),
        tokenizer: Arc::new(Tokenizer::load()),
        http: reqwest::Client::new(),
        openai_base: env::var("WEIR_OPENAI_BASE")
            .unwrap_or_else(|_| "https://api.openai.com".to_string()),
        anthropic_base: env::var("WEIR_ANTHROPIC_BASE")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string()),
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("weir listening on 0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}
