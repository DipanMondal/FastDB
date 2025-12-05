use axum::{extract::State, Json};
use serde::Serialize;

use crate::state::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
}

pub async fn health(State(_state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
