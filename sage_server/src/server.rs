use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use task::SageMessage;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

use crate::db::{self, TaskCreate};

#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<FutureProducer>,
    pub db_pool: Arc<PgPool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StartResponse {
    status: bool,
    id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StartRequest {
    requestor_id: i64,
    task_name: String,
    task_context: String,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    max_retries: Option<i32>,
}

async fn start(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<StartRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    let task_id = Uuid::new_v4();
    let key = payload.requestor_id.to_string();

    // Save task to database
    let task_create = TaskCreate {
        id: task_id,
        requestor_id: payload.requestor_id,
        task_name: payload.task_name.clone(),
        task_context: payload.task_context.clone(),
        priority: payload.priority,
        max_retries: payload.max_retries,
    };

    match db::create_task(&state.db_pool, task_create).await {
        Ok(task) => {
            println!("Task created in database: {:?}", task.id);
        }
        Err(e) => {
            eprintln!("Failed to create task in database: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    let sage_message = SageMessage {
        task_id,
        task_name: payload.task_name,
        task_context: payload.task_context,
    };

    let payload_string =
        serde_json::to_string(&sage_message).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let record = FutureRecord::to("input-readings") // Kafka topic
        .key(&key)
        .payload(&payload_string);

    match state
        .producer
        .send(
            record, 0, // Optional timeout
        )
        .await
    {
        Ok(_) => {
            println!(
                "Message |{}| sent successfully to topic '{}'",
                &payload_string, "input-readings"
            );
        }
        Err(e) => {
            println!("Failed to send message: {}", e);
        }
    }

    let response: StartResponse = StartResponse {
        status: true,
        id: task_id,
    };

    Ok(Json(response))
}

pub fn create_routes() -> Router<Arc<AppState>> {
    Router::new().route("/tasks/v1/start", post(start))
}

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
        .merge(create_routes())
        .layer(cors)
        .with_state(app_state)
}
