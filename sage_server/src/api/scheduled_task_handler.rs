use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::str::FromStr;
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
/// Uses the cron crate to properly parse cron expressions
fn calculate_next_run(cron_expression: &str, _timezone: &str) -> Result<DateTime<Utc>, String> {
    // Validate that the cron expression is not empty
    if cron_expression.trim().is_empty() {
        return Err("Cron expression cannot be empty".to_string());
    }

    // Convert 5-field cron expression to 6-field format (add seconds field)
    let cron_expr = normalize_cron_expression(cron_expression)?;

    // Parse the cron expression
    let schedule = Schedule::from_str(&cron_expr)
        .map_err(|e| format!("Failed to parse cron expression: {}", e))?;

    // Get the next scheduled time after now
    let now = Utc::now();
    let next = schedule
        .after(&now)
        .next()
        .ok_or_else(|| "No upcoming execution time found".to_string())?;

    Ok(next)
}

/// Validate the cron expression format using the cron crate
fn validate_cron_expression(cron_expression: &str) -> Result<(), String> {
    if cron_expression.trim().is_empty() {
        return Err("Cron expression cannot be empty".to_string());
    }

    let fields: Vec<&str> = cron_expression.split_whitespace().collect();

    // Support both 5-field (standard cron) and 6-field (with seconds) formats
    if fields.len() < 5 || fields.len() > 7 {
        return Err(format!(
            "Invalid cron expression: must have 5-7 fields, got {}",
            fields.len()
        ));
    }

    // Normalize and try to parse to validate
    let cron_expr = normalize_cron_expression(cron_expression)?;
    Schedule::from_str(&cron_expr).map_err(|e| format!("Invalid cron expression: {}", e))?;

    Ok(())
}

/// Normalize cron expression to 6-field format expected by the cron crate
/// Converts 5-field (min hour day month dow) to 6-field (sec min hour day month dow)
fn normalize_cron_expression(cron_expression: &str) -> Result<String, String> {
    let fields: Vec<&str> = cron_expression.split_whitespace().collect();

    match fields.len() {
        5 => {
            // Standard 5-field cron: add "0" for seconds at the beginning
            Ok(format!("0 {}", cron_expression))
        }
        6 | 7 => {
            // Already in 6 or 7 field format (with optional year)
            Ok(cron_expression.to_string())
        }
        _ => Err(format!(
            "Invalid cron expression: expected 5-7 fields, got {}",
            fields.len()
        )),
    }
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
