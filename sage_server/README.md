# Sage Server

A Rust-based web server that manages task processing with Kafka and PostgreSQL integration.

## Features

- **REST API**: HTTP endpoints for task management
- **Kafka Integration**: Produces and consumes messages from Kafka topics
- **PostgreSQL Database**: Persistent storage for task information
- **Async Runtime**: Built with Tokio for high-performance async I/O

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_tasks_requestor_id ON tasks(requestor_id);
CREATE INDEX idx_tasks_status ON tasks(status);
```

## Environment Variables

- `DATABASE_URL`: PostgreSQL connection string (default: `postgres://sage:sage_password@localhost:5432/sage_db`)

## API Endpoints

### POST /tasks/v1/start
Start a new task. The task is saved to the database and sent to Kafka.

**Request Body:**
```json
{
  "requestor_id": 123,
  "task_name": "example_task",
  "task_context": "Task context data"
}
```

**Response:**
```json
{
  "status": true,
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

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

- `create_pool()`: Create a connection pool to PostgreSQL
- `init_db()`: Initialize database schema (creates tables and indexes)
- `create_task()`: Insert a new task record
- `get_task_by_id()`: Retrieve a task by UUID
- `get_tasks_by_requestor()`: Get all tasks for a requestor
- `update_task_status()`: Update the status of a task
- `get_all_tasks()`: Get all tasks
- `delete_task()`: Delete a task by UUID

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────┐
│   Client    │────▶│  Sage Server │────▶│   Kafka   │
└─────────────┘     └──────────────┘     └───────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │  PostgreSQL  │
                    └──────────────┘
```

When a task is started:
1. Client sends POST request to `/tasks/v1/start`
2. Server saves task to PostgreSQL database
3. Server publishes task message to Kafka topic `input-readings`
4. Server returns task ID to client

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
