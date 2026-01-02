use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;
/// Calculate the next run time based on cron expression
/// Uses the cron crate to properly parse cron expressions
pub fn calculate_next_run(cron_expression: &str, _timezone: &str) -> Result<DateTime<Utc>, String> {
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
pub fn validate_cron_expression(cron_expression: &str) -> Result<(), String> {
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
