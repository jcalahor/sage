use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use log::{error, info};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use task::SageMessage;
use uuid::Uuid;

use crate::db::{self, Task, TaskCreate};
use crate::types::AppState;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StartResponse {
    status: bool,
    id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StartRequest {
    requestor_id: i64,
    task_name: String,
    task_envelope: String,
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
        task_context: payload.task_envelope.clone(),
        priority: payload.priority,
        max_retries: payload.max_retries,
    };

    match db::create_task(&state.db_pool, task_create).await {
        Ok(task) => {
            info!("Task created in database: {:?}", task.id);
        }
        Err(e) => {
            error!("Failed to create task in database: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    let sage_message = SageMessage {
        task_id,
        task_name: payload.task_name,
        task_envelope: payload.task_envelope,
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
            info!(
                "Message |{}| sent successfully to topic '{}'",
                &payload_string, "input-readings"
            );
        }
        Err(e) => {
            info!("Failed to send message: {}", e);
        }
    }

    let response: StartResponse = StartResponse {
        status: true,
        id: task_id,
    };

    Ok(Json(response))
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ListQueryParams {
    requestor_id: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ListResponse {
    status: bool,
    tasks: Vec<Task>,
    count: usize,
}

async fn list_tasks(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListQueryParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let tasks = match params.requestor_id {
        Some(requestor_id) => {
            // Get tasks for specific requestor
            match db::get_tasks_by_requestor(&state.db_pool, requestor_id).await {
                Ok(tasks) => {
                    info!(
                        "Retrieved {} tasks for requestor_id: {}",
                        tasks.len(),
                        requestor_id
                    );
                    tasks
                }
                Err(e) => {
                    error!("Failed to retrieve tasks for requestor: {}", e);
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
        None => {
            // Get all tasks
            match db::get_all_tasks(&state.db_pool).await {
                Ok(tasks) => {
                    info!("Retrieved {} total tasks", tasks.len());
                    tasks
                }
                Err(e) => {
                    error!("Failed to retrieve all tasks: {}", e);
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
        }
    };

    let count = tasks.len();
    let response = ListResponse {
        status: true,
        tasks,
        count,
    };

    Ok(Json(response))
}

pub fn create_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/tasks/v1/start", post(start))
        .route("/tasks/v1/list", get(list_tasks))
}
