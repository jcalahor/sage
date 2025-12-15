use chrono::{DateTime, Utc};
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

    println!("Database initialized successfully");
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
