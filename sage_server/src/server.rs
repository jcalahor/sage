use axum::Router;
use rdkafka::producer::FutureProducer;
use sqlx::PgPool;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use crate::task_handler;
use crate::types::AppState;

pub async fn build_server(producer: Arc<FutureProducer>, db_pool: PgPool) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app_state = Arc::new(AppState {
        producer,
        db_pool: Arc::new(db_pool),
    });

    Router::new()
        .merge(task_handler::create_routes())
        .layer(cors)
        .with_state(app_state)
}
