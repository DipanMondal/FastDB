use axum::{
    routing::{get, post, delete},
    Router,
};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod routes;
mod state;
mod index;
mod models;

use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let app_state = AppState::new();

    let app = Router::new()
        .route("/health", get(routes::health))
        .route(
            "/collections",
            post(routes::create_collection).get(routes::list_collections),
        )
        .route(
            "/collections/:name",
            get(routes::get_collection).delete(routes::delete_collection),
        )
        .route(
            "/collections/:name/vectors/upsert",
            post(routes::upsert_vectors),
        )
        .route(
            "/collections/:name/vectors/:id",
            delete(routes::delete_vector),
        )
        .route("/collections/:name/query", post(routes::query_vectors))
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
