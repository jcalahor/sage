# Task Scheduling Design - Database Approach

## Overview

Implementation of cron-like task scheduling stored in PostgreSQL, integrated with Sage's existing task queue system.

## Database Schema

### scheduled_tasks Table

```sql
CREATE TABLE scheduled_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requestor_id BIGINT NOT NULL,
    schedule_name VARCHAR(255) NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    task_context TEXT NOT NULL,              -- JSON string with task parameters
    cron_expression VARCHAR(100) NOT NULL,   -- Standard cron: "*/5 * * * *"
    timezone VARCHAR(50) DEFAULT 'UTC',      -- IANA timezone: "America/Bogota"
    enabled BOOLEAN NOT NULL DEFAULT true,
    priority INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ NOT NULL,        -- Precomputed for efficient queries
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    metadata JSONB,                          -- Additional data (tags, description, etc.)
    
    CONSTRAINT unique_schedule_name_per_requestor 
        UNIQUE (requestor_id, schedule_name)
);

-- Indexes for efficient scheduling queries
CREATE INDEX idx_scheduled_tasks_next_run 
    ON scheduled_tasks(next_run_at) 
    WHERE enabled = true;

CREATE INDEX idx_scheduled_tasks_requestor 
    ON scheduled_tasks(requestor_id);

CREATE INDEX idx_scheduled_tasks_enabled 
    ON scheduled_tasks(enabled);
```

### scheduled_task_history Table (Optional - for audit trail)

```sql
CREATE TABLE scheduled_task_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scheduled_task_id UUID NOT NULL REFERENCES scheduled_tasks(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id),       -- Links to executed task
    executed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL,             -- 'submitted', 'skipped', 'error'
    error_message TEXT,
    
    CONSTRAINT check_history_status_values 
        CHECK (status IN ('submitted', 'skipped', 'error'))
);

CREATE INDEX idx_scheduled_task_history_scheduled_task 
    ON scheduled_task_history(scheduled_task_id, executed_at DESC);
```

## Cron Expression Format

Standard cron syntax with optional seconds field:

```
┌───────────── second (optional, 0-59)
│ ┌───────────── minute (0-59)
│ │ ┌───────────── hour (0-23)
│ │ │ ┌───────────── day of month (1-31)
│ │ │ │ ┌───────────── month (1-12)
│ │ │ │ │ ┌───────────── day of week (0-6) (Sunday=0)
│ │ │ │ │ │
* * * * * *

Examples:
- "*/5 * * * *"        → Every 5 minutes
- "0 */2 * * *"        → Every 2 hours
- "0 9 * * MON-FRI"    → Weekdays at 9am
- "0 0 1 * *"          → First day of month at midnight
- "*/30 * * * * *"     → Every 30 seconds (with seconds field)
```

## Architecture Components

### 1. Scheduler Service (New)

A background task in `sage_server` that:
- Runs every N seconds (configurable, e.g., every 30 seconds)
- Queries database for due scheduled tasks (`next_run_at <= NOW() AND enabled = true`)
- Creates task entries in `tasks` table
- Publishes tasks to Kafka (reusing existing infrastructure)
- Updates `last_run_at` and calculates `next_run_at`
- Logs to history table

```
┌─────────────────────────────────────┐
│      sage_server                    │
│  ┌───────────────────────────────┐  │
│  │  Scheduler Loop (Tokio task)  │  │
│  │  • Every 30s query DB         │  │
│  │  • Find due schedules         │  │
│  │  • Create tasks               │  │
│  │  • Publish to Kafka           │  │
│  │  • Update next_run_at         │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │  Schedule API Endpoints       │  │
│  │  • POST /schedules/create     │  │
│  │  • GET /schedules             │  │
│  │  • GET /schedules/:id         │  │
│  │  • PUT /schedules/:id         │  │
│  │  • DELETE /schedules/:id      │  │
│  │  • POST /schedules/:id/run    │  │ ← Manual trigger
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

### 2. Database Module Updates

Add to `sage_server/src/db.rs`:

```rust
// Models
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

// CRUD operations
pub async fn create_scheduled_task(pool: &PgPool, schedule: ScheduledTaskCreate) -> Result<ScheduledTask, sqlx::Error>
pub async fn get_scheduled_task(pool: &PgPool, id: Uuid) -> Result<Option<ScheduledTask>, sqlx::Error>
pub async fn get_scheduled_tasks_by_requestor(pool: &PgPool, requestor_id: i64) -> Result<Vec<ScheduledTask>, sqlx::Error>
pub async fn update_scheduled_task(pool: &PgPool, update: ScheduledTaskUpdate) -> Result<ScheduledTask, sqlx::Error>
pub async fn delete_scheduled_task(pool: &PgPool, id: Uuid) -> Result<bool, sqlx::Error>
pub async fn get_due_scheduled_tasks(pool: &PgPool) -> Result<Vec<ScheduledTask>, sqlx::Error>
```

### 3. Cron Parser (Dependency)

Add to `sage_server/Cargo.toml`:

```toml
[dependencies]
cron = "0.12"                    # Cron expression parsing
chrono = "0.4"                   # Already in use
chrono-tz = "0.8"                # Timezone support
```

### 4. API Endpoints

```
POST   /api/v1/schedules          - Create new schedule
GET    /api/v1/schedules          - List all schedules (with filters)
GET    /api/v1/schedules/:id      - Get schedule details
PUT    /api/v1/schedules/:id      - Update schedule
DELETE /api/v1/schedules/:id      - Delete schedule
PATCH  /api/v1/schedules/:id/enable  - Enable schedule
PATCH  /api/v1/schedules/:id/disable - Disable schedule
POST   /api/v1/schedules/:id/run     - Manual trigger (run now)
GET    /api/v1/schedules/:id/history - Get execution history
```

## Implementation Flow

### Creating a Schedule

1. **API Request**:
```json
POST /api/v1/schedules
{
  "requestor_id": 12345,
  "schedule_name": "hourly_data_sync",
  "task_name": "DataSyncTask",
  "task_context": "{\"source\": \"db1\", \"target\": \"db2\"}",
  "cron_expression": "0 * * * *",
  "timezone": "America/Bogota",
  "enabled": true,
  "priority": 5,
  "max_retries": 3,
  "metadata": {
    "description": "Sync data every hour",
    "tags": ["sync", "hourly"]
  }
}
```

2. **Server Processing**:
   - Validate cron expression
   - Parse and calculate `next_run_at` based on cron + timezone
   - Insert into `scheduled_tasks` table
   - Return schedule ID

3. **Response**:
```json
{
  "status": true,
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "next_run_at": "2025-12-19T13:00:00Z"
}
```

### Executing Scheduled Tasks

1. **Scheduler Loop** (runs every 30 seconds):
```rust
loop {
    // Query for due tasks
    let due_tasks = get_due_scheduled_tasks(&pool).await?;
    
    for scheduled_task in due_tasks {
        // Create task entry in tasks table
        let task_id = Uuid::new_v4();
        let task = create_task(&pool, TaskCreate {
            id: task_id,
            requestor_id: scheduled_task.requestor_id,
            task_name: scheduled_task.task_name.clone(),
            task_context: scheduled_task.task_context.clone(),
            priority: Some(scheduled_task.priority),
            max_retries: Some(scheduled_task.max_retries),
        }).await?;
        
        // Publish to Kafka
        publish_task_to_kafka(&producer, &task).await?;
        
        // Calculate next run time
        let next_run = calculate_next_run(
            &scheduled_task.cron_expression,
            &scheduled_task.timezone,
        )?;
        
        // Update scheduled task
        update_scheduled_task(&pool, ScheduledTaskUpdate {
            id: scheduled_task.id,
            last_run_at: Some(Utc::now()),
            next_run_at: Some(next_run),
            ..Default::default()
        }).await?;
        
        // Log to history (optional)
        log_schedule_execution(&pool, scheduled_task.id, task_id, "submitted").await?;
    }
    
    tokio::time::sleep(Duration::from_secs(30)).await;
}
```

2. **Workers process tasks** normally via Kafka (no changes needed)

3. **Server updates task status** when results come back (existing flow)

## Configuration

Add to environment variables or config file:

```bash
# Scheduler settings
SCHEDULER_ENABLED=true
SCHEDULER_INTERVAL_SECS=30
SCHEDULER_TIMEZONE=UTC
SCHEDULER_MAX_TASKS_PER_RUN=100

# Database URL (already exists)
DATABASE_URL=postgres://user:pass@localhost/sage
```

## Error Handling

### Misfire Handling

If scheduler was down or task was missed:

**Strategy 1: Skip missed runs**
- Only schedule next occurrence
- Safe default

**Strategy 2: Execute missed runs**
- Check if `next_run_at` is in the past
- Execute immediately
- Then calculate next run

**Strategy 3: Configurable per schedule**
- Add `misfire_policy` field to table
- Options: `skip`, `run_once`, `run_all`

## Migration Path

### Phase 1: Core Implementation
1. Create database tables
2. Add CRUD operations
3. Implement scheduler loop
4. Basic API endpoints

### Phase 2: Enhanced Features
1. Add execution history tracking
2. Implement misfire handling
3. Add timezone support
4. Manual trigger endpoint

### Phase 3: Monitoring
1. Metrics (schedules count, executions per hour)
2. Health checks (scheduler alive?)
3. Admin dashboard integration

## Example Use Cases

### 1. Hourly Report Generation
```json
{
  "schedule_name": "hourly_reports",
  "cron_expression": "0 * * * *",
  "task_name": "GenerateReportTask",
  "task_context": "{\"report_type\": \"sales\"}"
}
```

### 2. Daily Backup at 2 AM
```json
{
  "schedule_name": "daily_backup",
  "cron_expression": "0 2 * * *",
  "timezone": "America/Bogota",
  "task_name": "BackupTask",
  "task_context": "{\"target\": \"s3://backups/\"}"
}
```

### 3. Every 5 Minutes Health Check
```json
{
  "schedule_name": "health_check",
  "cron_expression": "*/5 * * * *",
  "task_name": "HealthCheckTask",
  "task_context": "{\"endpoints\": [\"https://api.example.com\"]}"
}
```

### 4. Monthly Cleanup
```json
{
  "schedule_name": "monthly_cleanup",
  "cron_expression": "0 0 1 * *",
  "task_name": "CleanupTask",
  "task_context": "{\"older_than_days\": 90}"
}
```

## Benefits of Database Approach

✅ **Dynamic** - Create/modify schedules via API without redeployment
✅ **Multi-tenant** - Each requestor manages their own schedules
✅ **Scalable** - Query-based, indexed lookups
✅ **Integrated** - Works with existing task/Kafka infrastructure
✅ **Auditable** - History table tracks all executions
✅ **Flexible** - Per-schedule configuration (priority, retries, timezone)
✅ **Distributed** - Multiple servers can read same schedules (with proper locking)
✅ **Dashboard-ready** - Easy to build UI on top of database tables

## Next Steps

1. **Review this design** - Any changes needed?
2. **Implement database schema** - Create tables and indexes
3. **Add cron parsing logic** - Calculate next run times
4. **Build scheduler loop** - Core scheduling engine
5. **Create API endpoints** - REST API for schedule management
6. **Test with examples** - Verify end-to-end flow
7. **Add monitoring** - Metrics and health checks

Would you like me to start implementing this design?
