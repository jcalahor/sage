use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use chrono::{DateTime, Utc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use std::sync::Arc;
use uuid::Uuid;

use crate::db::{JobCreate, JobUpdate};
use crate::db::{create_job, get_all_jobs, get_job_by_id, get_jobs_by_requestor, update_job};
use crate::schedule_utils::{calculate_next_run, validate_cron_expression};
use crate::types::AppState;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateScheduleRequest {
    pub requestor_id: i64,
    pub schedule_name: String,
    pub task_name: String,
    pub task_context: String,
    pub cron_expression: String,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub max_retries: Option<i32>,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub metadata: Option<JsonValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateScheduleResponse {
    pub status: bool,
    pub id: Uuid,
    pub next_run_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EditScheduleRequest {
    pub id: Uuid,
    #[serde(default)]
    pub task_context: Option<String>,
    #[serde(default)]
    pub cron_expression: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub max_retries: Option<i32>,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub metadata: Option<JsonValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EditScheduleResponse {
    pub status: bool,
    pub id: Uuid,
    pub next_run_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScheduleStatusRequest {
    pub id: Uuid,
    pub enabled: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScheduleStatusResponse {
    pub status: bool,
    pub id: Uuid,
    pub enabled: bool,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListSchedulesRequest {
    #[serde(default)]
    pub requestor_id: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobResponse {
    pub id: Uuid,
    pub requestor_id: i64,
    pub schedule_name: String,
    pub task_name: String,
    pub task_context: String,
    pub cron_expression: String,
    pub timezone: String,
    pub enabled: bool,
    pub priority: i32,
    pub max_retries: i32,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub metadata: Option<JsonValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListSchedulesResponse {
    pub status: bool,
    pub count: usize,
    pub jobs: Vec<JobResponse>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ErrorResponse {
    pub status: bool,
    pub error: String,
}

async fn create_schedule(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateScheduleRequest>,
) -> impl IntoResponse {
    // Validate cron expression
    if let Err(e) = validate_cron_expression(&payload.cron_expression) {
        let error_response = ErrorResponse {
            status: false,
            error: format!("Invalid cron expression: {}", e),
        };
        return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
    }

    // Calculate next run time
    let timezone = payload.timezone.as_deref().unwrap_or("UTC");
    let next_run_at = match calculate_next_run(&payload.cron_expression, timezone) {
        Ok(dt) => dt,
        Err(e) => {
            let error_response = ErrorResponse {
                status: false,
                error: format!("Failed to calculate next run time: {}", e),
            };
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    // Create scheduled task in database
    let schedule_create = JobCreate {
        requestor_id: payload.requestor_id,
        schedule_name: payload.schedule_name.clone(),
        task_name: payload.task_name,
        task_context: payload.task_context,
        cron_expression: payload.cron_expression,
        timezone: payload.timezone,
        enabled: payload.enabled,
        priority: payload.priority,
        max_retries: payload.max_retries,
        next_run_at,
        created_by: payload.created_by,
        metadata: payload.metadata,
    };

    match create_job(&state.db_pool, schedule_create).await {
        Ok(job) => {
            info!(
                "Scheduled task created: {} - Next run at: {}",
                job.id, job.next_run_at
            );

            let response = CreateScheduleResponse {
                status: true,
                id: job.id,
                next_run_at: job.next_run_at,
            };

            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to create scheduled task in database: {}", e);
            let error_response = ErrorResponse {
                status: false,
                error: format!("Database error: {}", e),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

async fn edit_schedule(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EditScheduleRequest>,
) -> impl IntoResponse {
    // Check if scheduled task exists
    let existing_task = match get_job_by_id(&state.db_pool, payload.id).await {
        Ok(Some(task)) => task,
        Ok(None) => {
            let error_response = ErrorResponse {
                status: false,
                error: format!("Scheduled task with id {} not found", payload.id),
            };
            return (StatusCode::NOT_FOUND, Json(error_response)).into_response();
        }
        Err(e) => {
            error!("Failed to fetch scheduled task: {}", e);
            let error_response = ErrorResponse {
                status: false,
                error: format!("Database error: {}", e),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response();
        }
    };

    // Validate cron expression if provided
    let cron_expression = payload
        .cron_expression
        .as_ref()
        .unwrap_or(&existing_task.cron_expression);
    if let Err(e) = validate_cron_expression(cron_expression) {
        let error_response = ErrorResponse {
            status: false,
            error: format!("Invalid cron expression: {}", e),
        };
        return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
    }

    // Calculate new next_run_at if cron expression or timezone changed
    let next_run_at = if payload.cron_expression.is_some() || payload.timezone.is_some() {
        let timezone = payload
            .timezone
            .as_deref()
            .unwrap_or(&existing_task.timezone);
        match calculate_next_run(cron_expression, timezone) {
            Ok(dt) => Some(dt),
            Err(e) => {
                let error_response = ErrorResponse {
                    status: false,
                    error: format!("Failed to calculate next run time: {}", e),
                };
                return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
            }
        }
    } else {
        None
    };

    // Update scheduled task in database
    // Note: schedule_name, task_name, and task_context are not editable
    let schedule_update = JobUpdate {
        id: payload.id,
        task_context: payload.task_context,
        cron_expression: payload.cron_expression,
        timezone: payload.timezone,
        enabled: payload.enabled,
        priority: payload.priority,
        max_retries: payload.max_retries,
        last_run_at: None, // Don't update last_run_at in edit operation
        next_run_at,
        created_by: payload.created_by,
        metadata: payload.metadata,
    };

    match update_job(&state.db_pool, schedule_update).await {
        Ok(updated_task) => {
            info!(
                "Scheduled task updated: {} - Next run at: {}",
                updated_task.id, updated_task.next_run_at
            );

            let response = EditScheduleResponse {
                status: true,
                id: updated_task.id,
                next_run_at: updated_task.next_run_at,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to update scheduled task in database: {}", e);
            let error_response = ErrorResponse {
                status: false,
                error: format!("Database error: {}", e),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

async fn list_jobs(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ListSchedulesRequest>,
) -> impl IntoResponse {
    let jobs_result = if let Some(requestor_id) = payload.requestor_id {
        // Get jobs for specific requestor
        get_jobs_by_requestor(&state.db_pool, requestor_id).await
    } else {
        // Get all jobs
        get_all_jobs(&state.db_pool).await
    };

    match jobs_result {
        Ok(jobs) => {
            let schedule_responses: Vec<JobResponse> = jobs
                .into_iter()
                .map(|s| JobResponse {
                    id: s.id,
                    requestor_id: s.requestor_id,
                    schedule_name: s.schedule_name,
                    task_name: s.task_name,
                    task_context: s.task_context,
                    cron_expression: s.cron_expression,
                    timezone: s.timezone,
                    enabled: s.enabled,
                    priority: s.priority,
                    max_retries: s.max_retries,
                    last_run_at: s.last_run_at,
                    next_run_at: s.next_run_at,
                    created_at: s.created_at,
                    updated_at: s.updated_at,
                    created_by: s.created_by,
                    metadata: s.metadata,
                })
                .collect();

            let count = schedule_responses.len();
            info!("Retrieved {} scheduled task(s)", count);

            let response = ListSchedulesResponse {
                status: true,
                count,
                jobs: schedule_responses,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to retrieve scheduled tasks from database: {}", e);
            let error_response = ErrorResponse {
                status: false,
                error: format!("Database error: {}", e),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

async fn toggle_schedule_status(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ScheduleStatusRequest>,
) -> impl IntoResponse {
    // Check if scheduled task exists
    let existing_task = match get_job_by_id(&state.db_pool, payload.id).await {
        Ok(Some(task)) => task,
        Ok(None) => {
            let error_response = ErrorResponse {
                status: false,
                error: format!("Scheduled task with id {} not found", payload.id),
            };
            return (StatusCode::NOT_FOUND, Json(error_response)).into_response();
        }
        Err(e) => {
            error!("Failed to fetch scheduled task: {}", e);
            let error_response = ErrorResponse {
                status: false,
                error: format!("Database error: {}", e),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response();
        }
    };

    // Check if status is already set to the desired value
    if existing_task.enabled == payload.enabled {
        let status_text = if payload.enabled {
            "activated"
        } else {
            "deactivated"
        };
        let response = ScheduleStatusResponse {
            status: true,
            id: payload.id,
            enabled: payload.enabled,
            message: format!("Scheduled task is already {}", status_text),
        };
        return (StatusCode::OK, Json(response)).into_response();
    }

    // Update the scheduled task status
    let schedule_update = JobUpdate {
        id: payload.id,
        task_context: None,
        cron_expression: None,
        timezone: None,
        enabled: Some(payload.enabled),
        priority: None,
        max_retries: None,
        last_run_at: None,
        next_run_at: None,
        created_by: None,
        metadata: None,
    };

    match update_job(&state.db_pool, schedule_update).await {
        Ok(updated_task) => {
            let status_text = if payload.enabled {
                "activated"
            } else {
                "deactivated"
            };
            info!("Scheduled task {}: {}", status_text, updated_task.id);

            let response = ScheduleStatusResponse {
                status: true,
                id: updated_task.id,
                enabled: updated_task.enabled,
                message: format!("Scheduled task {} successfully", status_text),
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let status_text = if payload.enabled {
                "activate"
            } else {
                "deactivate"
            };
            error!(
                "Failed to {} scheduled task in database: {}",
                status_text, e
            );
            let error_response = ErrorResponse {
                status: false,
                error: format!("Database error: {}", e),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

pub fn create_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/jobs/v1/create", post(create_schedule))
        .route("/jobs/v1/edit", post(edit_schedule))
        .route("/jobs/v1/toggle-status", post(toggle_schedule_status))
        .route("/jobs/v1/list", post(list_jobs))
}
