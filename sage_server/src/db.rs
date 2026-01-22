use chrono::{DateTime, Utc};
use log::info;
use serde_json::Value as JsonValue;
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow)]
pub struct Task {
    pub id: Uuid,
    pub requestor_id: i64,
    pub task_name: String,
    pub task_context: String,
    pub status: String,
    pub priority: i32,
    pub retry_count: i32,
    pub max_retries: i32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<JsonValue>,
    pub error: Option<String>,
    pub worker_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TaskCreate {
    pub id: Uuid,
    pub requestor_id: i64,
    pub task_name: String,
    pub task_context: String,
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct TaskUpdate {
    pub id: Uuid,
    pub status: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<JsonValue>,
    pub error: Option<String>,
    pub worker_id: Option<String>,
    pub retry_count: Option<i32>,
}

#[derive(Debug, Clone, FromRow)]
pub struct ScheduledTask {
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

#[derive(Debug, Clone)]
pub struct ScheduledTaskCreate {
    pub requestor_id: i64,
    pub schedule_name: String,
    pub task_name: String,
    pub task_context: String,
    pub cron_expression: String,
    pub timezone: Option<String>,
    pub enabled: Option<bool>,
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
    pub next_run_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone)]
pub struct ScheduledTaskUpdate {
    pub id: Uuid,
    pub task_context: Option<String>,
    pub cron_expression: Option<String>,
    pub timezone: Option<String>,
    pub enabled: Option<bool>,
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub created_by: Option<String>,
    pub metadata: Option<JsonValue>,
}

pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
}

pub async fn init_db(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS tasks (
            id UUID PRIMARY KEY,
            requestor_id BIGINT NOT NULL,
            task_name VARCHAR(255) NOT NULL,
            task_context TEXT NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 0,
            retry_count INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            result JSONB,
            error TEXT,
            worker_id VARCHAR(255)
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create index on requestor_id for faster queries
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_tasks_requestor_id ON tasks(requestor_id)
        "#,
    )
    .execute(pool)
    .await?;

    // Create index on status for faster queries
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)
        "#,
    )
    .execute(pool)
    .await?;

    // Create index on worker_id for faster queries
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_tasks_worker_id ON tasks(worker_id)
        "#,
    )
    .execute(pool)
    .await?;

    // Add CHECK constraint to status column to restrict values to pending, completed, error
    sqlx::query(
        r#"
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'check_status_values'
            ) THEN
                ALTER TABLE tasks 
                ADD CONSTRAINT check_status_values 
                CHECK (status IN ('pending', 'completed', 'error'));
            END IF;
        END $$;
        "#,
    )
    .execute(pool)
    .await?;

    // Create scheduled_tasks table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS scheduled_tasks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            requestor_id BIGINT NOT NULL,
            schedule_name VARCHAR(255) NOT NULL,
            task_name VARCHAR(255) NOT NULL,
            task_context TEXT NOT NULL,
            cron_expression VARCHAR(100) NOT NULL,
            timezone VARCHAR(50) DEFAULT 'UTC',
            enabled BOOLEAN NOT NULL DEFAULT true,
            priority INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            last_run_at TIMESTAMPTZ,
            next_run_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(255),
            metadata JSONB,
            CONSTRAINT unique_schedule_name_per_requestor 
                UNIQUE (requestor_id, schedule_name)
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create index on next_run_at for efficient scheduling queries
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_next_run 
            ON scheduled_tasks(next_run_at) 
            WHERE enabled = true
        "#,
    )
    .execute(pool)
    .await?;

    // Create index on requestor_id for scheduled tasks
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_requestor 
            ON scheduled_tasks(requestor_id)
        "#,
    )
    .execute(pool)
    .await?;

    // Create index on enabled for scheduled tasks
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_enabled 
            ON scheduled_tasks(enabled)
        "#,
    )
    .execute(pool)
    .await?;

    // Create scheduled_task_history table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS scheduled_task_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            scheduled_task_id UUID NOT NULL,
            task_id UUID,
            executed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50) NOT NULL,
            error_message TEXT,
            CONSTRAINT check_history_status_values 
                CHECK (status IN ('submitted', 'skipped', 'error'))
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Add foreign key constraint for scheduled_task_history
    sqlx::query(
        r#"
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'fk_scheduled_task_history_scheduled_task'
            ) THEN
                ALTER TABLE scheduled_task_history 
                ADD CONSTRAINT fk_scheduled_task_history_scheduled_task 
                FOREIGN KEY (scheduled_task_id) REFERENCES scheduled_tasks(id) ON DELETE CASCADE;
            END IF;
        END $$;
        "#,
    )
    .execute(pool)
    .await?;

    // Add foreign key constraint for scheduled_task_history to tasks
    sqlx::query(
        r#"
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'fk_scheduled_task_history_task'
            ) THEN
                ALTER TABLE scheduled_task_history 
                ADD CONSTRAINT fk_scheduled_task_history_task 
                FOREIGN KEY (task_id) REFERENCES tasks(id);
            END IF;
        END $$;
        "#,
    )
    .execute(pool)
    .await?;

    // Create index on scheduled_task_id for history queries
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_scheduled_task_history_scheduled_task 
            ON scheduled_task_history(scheduled_task_id, executed_at DESC)
        "#,
    )
    .execute(pool)
    .await?;

    info!("Database initialized successfully");
    Ok(())
}

pub async fn create_task(pool: &PgPool, task: TaskCreate) -> Result<Task, sqlx::Error> {
    let row = sqlx::query_as::<_, Task>(
        r#"
        INSERT INTO tasks (id, requestor_id, task_name, task_context, status, priority, max_retries)
        VALUES ($1, $2, $3, $4, 'pending', $5, $6)
        RETURNING id, requestor_id, task_name, task_context, status, priority, retry_count, max_retries,
                  created_at, started_at, completed_at, result, error, worker_id
        "#,
    )
    .bind(task.id)
    .bind(task.requestor_id)
    .bind(task.task_name)
    .bind(task.task_context)
    .bind(task.priority.unwrap_or(0))
    .bind(task.max_retries.unwrap_or(3))
    .fetch_one(pool)
    .await?;

    Ok(row)
}

pub async fn get_task_by_id(pool: &PgPool, task_id: Uuid) -> Result<Option<Task>, sqlx::Error> {
    let task = sqlx::query_as::<_, Task>(
        r#"
        SELECT id, requestor_id, task_name, task_context, status, priority, retry_count, max_retries,
               created_at, started_at, completed_at, result, error, worker_id
        FROM tasks
        WHERE id = $1
        "#,
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;

    Ok(task)
}

pub async fn get_tasks_by_requestor(
    pool: &PgPool,
    requestor_id: i64,
) -> Result<Vec<Task>, sqlx::Error> {
    let tasks = sqlx::query_as::<_, Task>(
        r#"
        SELECT id, requestor_id, task_name, task_context, status, priority, retry_count, max_retries,
               created_at, started_at, completed_at, result, error, worker_id
        FROM tasks
        WHERE requestor_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(requestor_id)
    .fetch_all(pool)
    .await?;

    Ok(tasks)
}

pub async fn update_task(pool: &PgPool, task_update: TaskUpdate) -> Result<Task, sqlx::Error> {
    let mut query = String::from("UPDATE tasks SET ");
    let mut updates = Vec::new();
    let mut param_count = 1;

    if task_update.status.is_some() {
        updates.push(format!("status = ${}", param_count));
        param_count += 1;
    }
    if task_update.started_at.is_some() {
        updates.push(format!("started_at = ${}", param_count));
        param_count += 1;
    }
    if task_update.completed_at.is_some() {
        updates.push(format!("completed_at = ${}", param_count));
        param_count += 1;
    }
    if task_update.result.is_some() {
        updates.push(format!("result = ${}", param_count));
        param_count += 1;
    }
    if task_update.error.is_some() {
        updates.push(format!("error = ${}", param_count));
        param_count += 1;
    }
    if task_update.worker_id.is_some() {
        updates.push(format!("worker_id = ${}", param_count));
        param_count += 1;
    }
    if task_update.retry_count.is_some() {
        updates.push(format!("retry_count = ${}", param_count));
        param_count += 1;
    }

    query.push_str(&updates.join(", "));
    query.push_str(&format!(" WHERE id = ${} RETURNING id, requestor_id, task_name, task_context, status, priority, retry_count, max_retries, created_at, started_at, completed_at, result, error, worker_id", param_count));

    let mut sqlx_query = sqlx::query_as::<_, Task>(&query);

    if let Some(status) = task_update.status {
        sqlx_query = sqlx_query.bind(status);
    }
    if let Some(started_at) = task_update.started_at {
        sqlx_query = sqlx_query.bind(started_at);
    }
    if let Some(completed_at) = task_update.completed_at {
        sqlx_query = sqlx_query.bind(completed_at);
    }
    if let Some(result) = task_update.result {
        sqlx_query = sqlx_query.bind(result);
    }
    if let Some(error) = task_update.error {
        sqlx_query = sqlx_query.bind(error);
    }
    if let Some(worker_id) = task_update.worker_id {
        sqlx_query = sqlx_query.bind(worker_id);
    }
    if let Some(retry_count) = task_update.retry_count {
        sqlx_query = sqlx_query.bind(retry_count);
    }

    sqlx_query = sqlx_query.bind(task_update.id);

    let row = sqlx_query.fetch_one(pool).await?;

    Ok(row)
}

pub async fn get_all_tasks(pool: &PgPool) -> Result<Vec<Task>, sqlx::Error> {
    let tasks = sqlx::query_as::<_, Task>(
        r#"
        SELECT id, requestor_id, task_name, task_context, status, priority, retry_count, max_retries,
               created_at, started_at, completed_at, result, error, worker_id
        FROM tasks
        ORDER BY priority DESC, created_at ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(tasks)
}

pub async fn delete_task(pool: &PgPool, task_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        DELETE FROM tasks
        WHERE id = $1
        "#,
    )
    .bind(task_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

// Scheduled Task CRUD operations

pub async fn create_scheduled_task(
    pool: &PgPool,
    schedule: ScheduledTaskCreate,
) -> Result<ScheduledTask, sqlx::Error> {
    let row = sqlx::query_as::<_, ScheduledTask>(
        r#"
        INSERT INTO scheduled_tasks (
            requestor_id, schedule_name, task_name, task_context, cron_expression,
            timezone, enabled, priority, max_retries, next_run_at, created_by, metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING id, requestor_id, schedule_name, task_name, task_context, cron_expression,
                  timezone, enabled, priority, max_retries, last_run_at, next_run_at,
                  created_at, updated_at, created_by, metadata
        "#,
    )
    .bind(schedule.requestor_id)
    .bind(schedule.schedule_name)
    .bind(schedule.task_name)
    .bind(schedule.task_context)
    .bind(schedule.cron_expression)
    .bind(schedule.timezone.unwrap_or_else(|| "UTC".to_string()))
    .bind(schedule.enabled.unwrap_or(true))
    .bind(schedule.priority.unwrap_or(0))
    .bind(schedule.max_retries.unwrap_or(3))
    .bind(schedule.next_run_at)
    .bind(schedule.created_by)
    .bind(schedule.metadata)
    .fetch_one(pool)
    .await?;

    Ok(row)
}

pub async fn get_scheduled_task_by_id(
    pool: &PgPool,
    schedule_id: Uuid,
) -> Result<Option<ScheduledTask>, sqlx::Error> {
    let schedule = sqlx::query_as::<_, ScheduledTask>(
        r#"
        SELECT id, requestor_id, schedule_name, task_name, task_context, cron_expression,
               timezone, enabled, priority, max_retries, last_run_at, next_run_at,
               created_at, updated_at, created_by, metadata
        FROM scheduled_tasks
        WHERE id = $1
        "#,
    )
    .bind(schedule_id)
    .fetch_optional(pool)
    .await?;

    Ok(schedule)
}

pub async fn get_scheduled_tasks_by_requestor(
    pool: &PgPool,
    requestor_id: i64,
) -> Result<Vec<ScheduledTask>, sqlx::Error> {
    let schedules = sqlx::query_as::<_, ScheduledTask>(
        r#"
        SELECT id, requestor_id, schedule_name, task_name, task_context, cron_expression,
               timezone, enabled, priority, max_retries, last_run_at, next_run_at,
               created_at, updated_at, created_by, metadata
        FROM scheduled_tasks
        WHERE requestor_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(requestor_id)
    .fetch_all(pool)
    .await?;

    Ok(schedules)
}

pub async fn get_all_scheduled_tasks(pool: &PgPool) -> Result<Vec<ScheduledTask>, sqlx::Error> {
    let schedules = sqlx::query_as::<_, ScheduledTask>(
        r#"
        SELECT id, requestor_id, schedule_name, task_name, task_context, cron_expression,
               timezone, enabled, priority, max_retries, last_run_at, next_run_at,
               created_at, updated_at, created_by, metadata
        FROM scheduled_tasks
        ORDER BY next_run_at ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(schedules)
}

pub async fn get_due_scheduled_tasks(pool: &PgPool) -> Result<Vec<ScheduledTask>, sqlx::Error> {
    let schedules = sqlx::query_as::<_, ScheduledTask>(
        r#"
        SELECT id, requestor_id, schedule_name, task_name, task_context, cron_expression,
               timezone, enabled, priority, max_retries, last_run_at, next_run_at,
               created_at, updated_at, created_by, metadata
        FROM scheduled_tasks
        WHERE enabled = true AND next_run_at <= CURRENT_TIMESTAMP
        ORDER BY priority DESC, next_run_at ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(schedules)
}

pub async fn update_scheduled_task(
    pool: &PgPool,
    schedule_update: ScheduledTaskUpdate,
) -> Result<ScheduledTask, sqlx::Error> {
    let mut query = String::from("UPDATE scheduled_tasks SET updated_at = CURRENT_TIMESTAMP");
    let mut updates = Vec::new();
    let mut param_count = 1;

    if schedule_update.task_context.is_some() {
        updates.push(format!("task_context = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.cron_expression.is_some() {
        updates.push(format!("cron_expression = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.timezone.is_some() {
        updates.push(format!("timezone = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.enabled.is_some() {
        updates.push(format!("enabled = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.priority.is_some() {
        updates.push(format!("priority = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.max_retries.is_some() {
        updates.push(format!("max_retries = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.last_run_at.is_some() {
        updates.push(format!("last_run_at = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.next_run_at.is_some() {
        updates.push(format!("next_run_at = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.created_by.is_some() {
        updates.push(format!("created_by = ${}", param_count));
        param_count += 1;
    }
    if schedule_update.metadata.is_some() {
        updates.push(format!("metadata = ${}", param_count));
        param_count += 1;
    }

    if !updates.is_empty() {
        query.push_str(", ");
        query.push_str(&updates.join(", "));
    }

    query.push_str(&format!(
        " WHERE id = ${} RETURNING id, requestor_id, schedule_name, task_name, task_context, cron_expression, timezone, enabled, priority, max_retries, last_run_at, next_run_at, created_at, updated_at, created_by, metadata",
        param_count
    ));

    let mut sqlx_query = sqlx::query_as::<_, ScheduledTask>(&query);

    if let Some(task_context) = schedule_update.task_context {
        sqlx_query = sqlx_query.bind(task_context);
    }
    if let Some(cron_expression) = schedule_update.cron_expression {
        sqlx_query = sqlx_query.bind(cron_expression);
    }
    if let Some(timezone) = schedule_update.timezone {
        sqlx_query = sqlx_query.bind(timezone);
    }
    if let Some(enabled) = schedule_update.enabled {
        sqlx_query = sqlx_query.bind(enabled);
    }
    if let Some(priority) = schedule_update.priority {
        sqlx_query = sqlx_query.bind(priority);
    }
    if let Some(max_retries) = schedule_update.max_retries {
        sqlx_query = sqlx_query.bind(max_retries);
    }
    if let Some(last_run_at) = schedule_update.last_run_at {
        sqlx_query = sqlx_query.bind(last_run_at);
    }
    if let Some(next_run_at) = schedule_update.next_run_at {
        sqlx_query = sqlx_query.bind(next_run_at);
    }
    if let Some(created_by) = schedule_update.created_by {
        sqlx_query = sqlx_query.bind(created_by);
    }
    if let Some(metadata) = schedule_update.metadata {
        sqlx_query = sqlx_query.bind(metadata);
    }

    sqlx_query = sqlx_query.bind(schedule_update.id);

    let row = sqlx_query.fetch_one(pool).await?;

    Ok(row)
}

pub async fn delete_scheduled_task(pool: &PgPool, schedule_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        DELETE FROM scheduled_tasks
        WHERE id = $1
        "#,
    )
    .bind(schedule_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}
