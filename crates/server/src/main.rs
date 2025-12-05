use axum::{routing::get, Router};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod routes;
mod state;

use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let app_state = AppState::new();

    let app = Router::new()
        .route("/health", get(routes::health))
        .with_state(app_state);

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("🚀 openvdb-server listening on http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "openvdb-server=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}
