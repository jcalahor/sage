use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

use crate::db::{self, ScheduledTaskCreate};
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
pub struct ErrorResponse {
    pub status: bool,
    pub error: String,
}

/// Calculate the next run time based on cron expression
/// For now, this is a placeholder that returns current time + 1 minute
/// TODO: Implement proper cron parsing with the `cron` crate
fn calculate_next_run(cron_expression: &str, _timezone: &str) -> Result<DateTime<Utc>, String> {
    // Validate that the cron expression is not empty
    if cron_expression.trim().is_empty() {
        return Err("Cron expression cannot be empty".to_string());
    }

    // Basic validation: check if it has at least 5 fields
    let fields: Vec<&str> = cron_expression.split_whitespace().collect();
    if fields.len() < 5 {
        return Err(format!(
            "Invalid cron expression: expected at least 5 fields, got {}",
            fields.len()
        ));
    }

    // TODO: Implement proper cron parsing using the `cron` crate
    // For now, return current time + 1 minute as a placeholder
    Ok(Utc::now() + chrono::Duration::minutes(1))
}

/// Validate the cron expression format
fn validate_cron_expression(cron_expression: &str) -> Result<(), String> {
    let fields: Vec<&str> = cron_expression.split_whitespace().collect();

    if fields.len() < 5 || fields.len() > 6 {
        return Err(format!(
            "Invalid cron expression: must have 5 or 6 fields, got {}",
            fields.len()
        ));
    }

    // TODO: Add more detailed validation with the `cron` crate
    Ok(())
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
    let schedule_create = ScheduledTaskCreate {
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

    match db::create_scheduled_task(&state.db_pool, schedule_create).await {
        Ok(scheduled_task) => {
            println!(
                "Scheduled task created: {} - Next run at: {}",
                scheduled_task.id, scheduled_task.next_run_at
            );

            let response = CreateScheduleResponse {
                status: true,
                id: scheduled_task.id,
                next_run_at: scheduled_task.next_run_at,
            };

            (StatusCode::CREATED, Json(response)).into_response()
        }
        Err(e) => {
            eprintln!("Failed to create scheduled task in database: {}", e);
            let error_response = ErrorResponse {
                status: false,
                error: format!("Database error: {}", e),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

pub fn create_routes() -> Router<Arc<AppState>> {
    Router::new().route("/schedules/v1/create", post(create_schedule))
}
