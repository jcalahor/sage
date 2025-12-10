use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::post};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StartResponse {
    status: bool,
    id: Uuid,
}

async fn start() -> Result<impl IntoResponse, StatusCode> {
    let response = StartResponse {
        status: true,
        id: Uuid::new_v4(),
    };

    Ok(Json(response))
}

pub fn create_routes() -> Router {
    Router::new().route("/tasks/v1/start", post(start))
}

pub async fn build_server() -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any) // Allow all origins for testing; replace with specific domain in production
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new().merge(create_routes()).layer(cors)
}
