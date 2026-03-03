# Sage Server

A Rust-based web server that manages task processing and scheduling with Kafka and PostgreSQL integration.

## Features

- **REST API**: HTTP endpoints for task and scheduled job management
- **Kafka Integration**: Produces and consumes messages from Kafka topics
- **PostgreSQL Database**: Persistent storage for tasks, jobs, and job history
- **Task Scheduling**: Cron-based job scheduling with timezone support
- **Retry Mechanism**: Automatic retry logic for failed tasks
- **Job History Tracking**: Track execution history for scheduled tasks
- **Async Runtime**: Built with Tokio for high-performance async I/O
- **CORS Enabled**: Cross-origin requests supported for web UI

## Database Schema

The server automatically creates the following database schema on startup:

### Tasks Table
```sql
CREATE TABLE tasks (
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
    worker_id VARCHAR(255),
    CONSTRAINT check_status_values 
        CHECK (status IN ('pending', 'completed', 'error'))
);

-- Indexes for performance
CREATE INDEX idx_tasks_requestor_id ON tasks(requestor_id);
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_worker_id ON tasks(worker_id);
```

### Jobs Table (Scheduled Tasks)
```sql
CREATE TABLE jobs (
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
);

-- Indexes for efficient scheduling queries
CREATE INDEX idx_jobs_next_run ON jobs(next_run_at) WHERE enabled = true;
CREATE INDEX idx_jobs_requestor ON jobs(requestor_id);
CREATE INDEX idx_jobs_enabled ON jobs(enabled);
```

### Job History Table
```sql
CREATE TABLE job_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    task_id UUID,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    CONSTRAINT check_history_status_values 
        CHECK (status IN ('submitted', 'completed', 'skipped', 'error')),
    CONSTRAINT fk_job_history_job 
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    CONSTRAINT fk_job_history_task 
        FOREIGN KEY (task_id) REFERENCES tasks(id)
);

-- Index for history queries
CREATE INDEX idx_job_history_job ON job_history(job_id, executed_at DESC);
```

## Environment Variables

- `DATABASE_URL`: PostgreSQL connection string (default: `postgres://sage:sage_password@localhost:5432/sage_db`)

## API Endpoints

### Task Endpoints

#### POST /tasks/v1/start
Start a new task. The task is saved to the database and sent to Kafka.

**Request Body:**
```json
{
  "requestor_id": 123,
  "task_name": "PrimeTask",
  "task_envelope": "{\"limit\": 10000}",
  "priority": 0,
  "max_retries": 3
}
```

**Response:**
```json
{
  "status": true,
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### GET /tasks/v1/list
List all tasks or filter by requestor.

**Query Parameters:**
- `requestor_id` (optional): Filter tasks by requestor ID

**Response:**
```json
{
  "status": true,
  "count": 10,
  "tasks": [...]
}
```

### Job Endpoints (Scheduled Tasks)

#### POST /jobs/v1/create
Create a new scheduled task with cron expression.

**Request Body:**
```json
{
  "requestor_id": 123,
  "schedule_name": "daily_task",
  "task_name": "PrimeTask",
  "task_context": "{\"limit\": 50000}",
  "cron_expression": "0 2 * * *",
  "timezone": "America/New_York",
  "enabled": true,
  "priority": 0,
  "max_retries": 3
}
```

#### POST /jobs/v1/list
List all scheduled tasks or filter by requestor.

#### POST /jobs/v1/edit
Update an existing scheduled task.

#### POST /jobs/v1/toggle-status
Enable or disable a scheduled task.

#### POST /jobs/v1/history
Get execution history for a scheduled task.

## Running the Server

1. **Start PostgreSQL and Kafka** (using Docker Compose):
   ```bash
   cd environment
   docker-compose up -d postgres kafka zookeeper
   ```

2. **Run the server**:
   ```bash
   cargo run --package sage_server
   ```

The server will:
- Connect to PostgreSQL at `localhost:5432`
- Initialize the database schema automatically
- Connect to Kafka at `localhost:9092`
- Start listening on `0.0.0.0:4000`

## Database Functions

The `db` module provides the following functions:

### Task Functions
- `create_pool()`: Create a connection pool to PostgreSQL
- `init_db()`: Initialize database schema (creates tables and indexes)
- `create_task()`: Insert a new task record
- `get_task_by_id()`: Retrieve a task by UUID
- `get_tasks_by_requestor()`: Get all tasks for a requestor
- `update_task()`: Update task fields (status, result, error, retry_count, etc.)
- `get_all_tasks()`: Get all tasks
- `delete_task()`: Delete a task by UUID

### Job Functions
- `create_job()`: Create a new scheduled task
- `get_job_by_id()`: Retrieve a job by UUID
- `get_jobs_by_requestor()`: Get all jobs for a requestor
- `get_all_jobs()`: Get all scheduled tasks
- `get_due_jobs()`: Get jobs that are due to run
- `update_job()`: Update job fields
- `delete_job()`: Delete a job by UUID

### Job History Functions
- `add_job_history_record()`: Add a history record for job execution
- `get_job_history()`: Get execution history for a job

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────┐     ┌───────────┐
│   Client    │────▶│       Sage Server (Axum)             │────▶│   Kafka   │
│   /UI       │     │  • Task API (/tasks/v1/*)            │     │  Topics:  │
└─────────────┘     │  • Job API (/jobs/v1/*)              │     │  - input  │
                    │  • Response Consumer (responses)     │     │  - resp.  │
                    │  • Error Consumer (task-errors)      │     │  - errors │
                    │  • Scheduler (checks every 30s)      │     └───────────┘
                    └──────────────┬───────────────────────┘
                                   │
                                   ▼
                           ┌──────────────┐
                           │  PostgreSQL  │
                           │  • tasks     │
                           │  • jobs      │
                           │  • history   │
                           └──────────────┘
```

### Request Flow

**Task Submission:**
1. Client sends POST request to `/tasks/v1/start`
2. Server saves task to PostgreSQL with status "pending"
3. Server publishes task message to Kafka topic `input-readings`
4. Server returns task ID to client

**Response Processing:**
1. Worker completes task and publishes result to `responses` topic
2. Server's response consumer receives result
3. Server updates task in database (status="completed", result stored)
4. If task was created by a job, updates job_history

**Error Handling:**
1. Worker encounters error and publishes to `task-errors` topic
2. Server's error consumer receives error
3. Server checks retry eligibility (retry_count < max_retries)
4. If retryable: increments retry_count and republishes to `input-readings`
5. If not retryable or max retries exceeded: marks task as "error"

**Scheduling:**
1. Scheduler runs every 30 seconds
2. Queries for jobs where `enabled=true` AND `next_run_at <= NOW()`
3. For each due job:
   - Creates a new task in tasks table
   - Publishes task to Kafka
   - Records submission in job_history
   - Calculates and updates next_run_at based on cron expression

## Development

Build the project:
```bash
cargo build --package sage_server
```

Run tests:
```bash
cargo test --package sage_server
```

Check for issues:
```bash
cargo clippy --package sage_server
```
