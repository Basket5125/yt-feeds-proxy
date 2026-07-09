use axum::{body::Bytes, response::IntoResponse};
use tracing::info;

pub async fn handle_browse(body: Bytes) -> impl IntoResponse {
    info!("Browse request received: {} bytes", body.len());
    // Placeholder for protobuf browse handling
    axum::http::StatusCode::OK
}