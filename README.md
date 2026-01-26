# Sage

A high-performance distributed task queue system built in Rust with Kafka, featuring automatic request/response ID tracking and type-safe task execution.

## Overview

Sage is a distributed task processing system that allows you to:
- Execute async tasks across multiple worker nodes
- Distribute workload via Kafka message broker
- Automatic UUID-based request/response correlation
- Type-safe task requests and responses with generic wrappers
- Scale horizontally by adding more worker instances
- JSON-transparent API with flattened serialization

## Architecture

```
┌──────────────────────────────────────────────┐
│           sage_server (Axum Web API)          │
│  ┌─────────────────────────────────────────┐ │
│  │  POST /tasks/v1/start                   │ │ ← HTTP REST API
│  │  • Receives task requests               │ │
│  │  • Generates task UUID                  │ │
│  │  • Stores task in PostgreSQL            │ │
│  │  • Publishes to Kafka                   │ │
│  └─────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────┐ │
│  │  Response Consumer                      │ │ ← Listens for results
│  │  • Consumes from 'responses' topic     │ │
│  │  • Updates task status in DB            │ │
│  │  • Stores result in 'result' column     │ │
│  └─────────────────────────────────────────┘ │
└──────────────────┬──────────────┬────────────┘
                   │              │
                   ▼              ▼
        ┌─────────────────────┐  ┌─────────────────┐
        │      Kafka          │  │   PostgreSQL    │
        │  input-readings     │  │   Task Queue    │
        │     responses       │  │   • tasks table │
        └──────────┬──────────┘  │   • Status:     │
                   │              │     - pending   │
    ┌──────────────┴──────────┐  │     - completed │
    │                          │  │     - error     │
┌───▼──────────┐   ┌──────▼───┐  └─────────────────┘
│ sage_worker 1│   │sage_worker│  ← Distributed Workers
│              │   │     2     │
│ • Consumes   │   │• Consumes │
│   tasks      │   │  tasks    │
│ • Executes   │   │• Executes │
│   async or   │   │  async or │
│   CPU tasks  │   │  CPU tasks│
│ • Publishes  │   │• Publishes│
│   results    │   │  results  │
└──────────────┘   └───────────┘
```

## Key Features

- ✅ **Automatic ID Tracking** - UUID correlation between requests and responses
- ✅ **Type-Safe Tasks** - Generic `TaskRequest<T>` and `TaskResponse<R>` wrappers
- ✅ **No Trait Boilerplate** - Clean data structs, automatic trait implementations
- ✅ **Async/Await** - Built on Tokio for efficient concurrency
- ✅ **CPU-Intensive Support** - spawn_blocking for compute-heavy tasks
- ✅ **Kafka Integration** - Production-ready distributed messaging
- ✅ **JSON Transparent** - Flattened serialization with `#[serde(flatten)]`
- ✅ **Extensible** - Enum-based dispatch for multiple task types
- ✅ **Memory Safe** - Rust's ownership system prevents data races

## Project Structure

```
sage/
├── task/              # Core traits and generic wrappers
│   ├── SageMessage      # Message format for Kafka
│   ├── TaskRequest<T>   # Request wrapper with auto UUID
│   ├── TaskResponse<R>  # Response wrapper with ID correlation
│   └── SageTask<T, R>   # Task trait definition
├── tasks_impl/        # Task implementations (business logic)
│   ├── PrimeTask        # Example: Prime number calculation
│   └── SampleTask       # Example: Simple task
├── sage_server/       # HTTP API Server (Axum)
│   ├── REST API         # POST /tasks/v1/start endpoint
│   ├── Kafka Producer   # Publishes tasks to input-readings
│   └── Kafka Consumer   # Consumes results from responses
├── sage_worker/       # Worker orchestration & dispatch
│   ├── TaskRequestType  # Enum for routing
│   ├── TaskResponseType # Enum for responses
│   ├── Kafka Consumer   # Consumes from input-readings
│   └── Kafka Producer   # Publishes to responses
├── samples/
│   ├── producer/      # Python example producer (legacy)
│   └── consumer/      # Python example consumer (legacy)
└── environment/       # Docker compose for Kafka
```

## Quick Start

### Automated (Recommended)

Use the provided scripts to start/stop all services with a single command:

```bash
# Start all services (Docker Compose + Server + Worker + UI)
./bin/start_all_services.sh

# Stop all services
./bin/stop_all_services.sh
```

**Features:**
- ✅ Automatic Kafka health checks with retry mechanism
- ✅ Auto-restart Kafka if it fails during startup
- ✅ Cleans up old containers to prevent conflicts
- ✅ Displays all service PIDs and URLs
- ✅ Comprehensive logging to `/log` directory
- ✅ Graceful shutdown on Ctrl+C

See [Service Management Scripts](#service-management-scripts) for details.

### Manual Setup

If you prefer manual control:

#### 1. Start Kafka

```bash
cd environment
docker-compose up -d
```

#### 2. Run the Server (HTTP API)

```bash
cargo run --bin sage_server
```

The server will start at `http://0.0.0.0:4000`

#### 3. Run Workers (Scale as needed)

```bash
# Terminal 2 - Worker 1
cargo run --bin sage_worker

# Terminal 3 - Worker 2 (optional)
cargo run --bin sage_worker

# Terminal 4 - Worker 3 (optional)
cargo run --bin sage_worker
```

#### 4. Submit Tasks via HTTP API

```bash
# Using curl
curl -X POST http://localhost:4000/tasks/v1/start \
  -H "Content-Type: application/json" \
  -d '{
    "requestor_id": 12345,
    "task_name": "PrimeTask",
    "task_context": "{\"limit\": 10000}"
  }'

# Response:
# {"status": true, "id": "550e8400-e29b-41d4-a716-446655440000"}
```

### 5. Legacy: Python Producer/Consumer (Optional)

You can still use Python clients if needed:

```bash
# Producer
cd samples/producer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python produce.py

# Consumer
cd samples/consumer
source venv/bin/activate
python consume.py
```

## Defining a Task

### 1. Create Your Data Types

```rust
// In tasks_impl/src/lib.rs

#[derive(Debug, Serialize, Deserialize)]
pub struct EmailTaskData {
    pub recipient: String,
    pub subject: String,
    pub body: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmailTaskResponseData {
    pub sent: bool,
    pub message_id: String,
}
```

### 2. Implement the Task

```rust
pub struct EmailTask {}

#[async_trait]
impl SageTask<EmailTaskData, EmailTaskResponseData> for EmailTask {
    async fn run(
        &self,
        request: &TaskRequest<EmailTaskData>,
    ) -> Result<TaskResponse<EmailTaskResponseData>, Box<dyn std::error::Error + Send>> {
        // Send email logic here
        let sent = send_email(&request.data).await?;
        
        // Return response with same ID as request
        Ok(TaskResponse::new(
            request.id,  // ← Automatic correlation!
            EmailTaskResponseData {
                sent: true,
                message_id: "msg-123".to_string(),
            }
        ))
    }
}
```

### 3. Register in sage_worker (sage_worker/src/main.rs)

```rust
// Add to TaskRequestType enum
pub enum TaskRequestType {
    Prime(TaskRequest<PrimeTaskData>),
    Email(TaskRequest<EmailTaskData>),  // ← Add here
}

// Add to TaskResponseType enum
pub enum TaskResponseType {
    Prime(TaskResponse<PrimeTaskResponseData>),
    Email(TaskResponse<EmailTaskResponseData>),  // ← Add here
}

// Add to run_task() match
TaskRequestType::Email(email_request) => {
    let task = EmailTask {};
    Ok(TaskResponseType::Email(task.run(email_request).await?))
}
```

## Message Format

### Request (Auto-generated ID):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "limit": 45000
}
```

### Response (Same ID):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "prime_founds": 4669
}
```

The `id` field automatically correlates requests with responses!

## Architecture Highlights

### Generic Wrappers
- `TaskRequest<T>` - Adds UUID to any request type
- `TaskResponse<R>` - Correlates response with request ID
- `#[serde(flatten)]` - Keeps JSON flat and clean

### No Redundant Traits
- Removed `SageTaskRequest` and `SageTaskResponse` traits
- Direct use of generic types - simpler, cleaner code
- Compile-time type safety without boilerplate

### Enum Dispatch Pattern
- Worker defines supported task types via enums
- Type-safe routing at compile time
- Easy to add new task types

## Database Schema

### PostgreSQL Tasks Table

Sage uses PostgreSQL to persist task state and results:

```sql
CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    requestor_id BIGINT NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    task_context TEXT NOT NULL,           -- Input parameters (JSON string)
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    result JSONB,                         -- Response data from worker
    error TEXT,
    worker_id VARCHAR(255),
    
    CONSTRAINT check_status_values 
        CHECK (status IN ('pending', 'completed', 'error'))
);
```

### Task Status Flow

1. **pending** - Task created and queued (initial state)
2. **completed** - Task successfully processed, result stored
3. **error** - Task failed, error message stored

### SageMessage Format

The `SageMessage` struct is used for Kafka communication:

```rust
pub struct SageMessage {
    pub task_id: Uuid,
    pub task_name: String,
    pub task_envelope: String,  // JSON string containing request/response data
}
```

- **Request**: `task_envelope` contains the serialized task input parameters
- **Response**: `task_envelope` contains the serialized task result data

### Response Processing Flow

1. **Worker completes task** → Creates `SageMessage` with result in `task_envelope`
2. **Worker publishes** → Sends to Kafka `responses` topic
3. **Server consumes** → Receives message from Kafka
4. **Server parses** → Deserializes `task_envelope` as JSON
5. **Server updates DB** → Creates `TaskUpdate` with:
   - `status` = "completed"
   - `completed_at` = current timestamp
   - `result` = parsed JSON from `task_envelope`
6. **Database persists** → Task result permanently stored

## Task Retry Mechanism

Sage implements automatic retry logic for failed tasks with configurable maximum attempts.

### How Retry Works

When a task fails during execution:

1. **Worker detects failure** → Creates `SageErrorResponse` with error details
2. **Worker publishes error** → Sends to Kafka `task-errors` topic
3. **Server error consumer** → Receives error message
4. **Server checks retry eligibility**:
   - Fetches task from database
   - Calculates `new_retry_count = retry_count + 1`
   - If `is_retryable` AND `new_retry_count <= max_retries`:
     - Updates retry count in database
     - Republishes task to `input-readings` for retry
   - Else:
     - Marks task as permanently failed (status = "error")

### Configuration

Tasks can specify custom retry limits:

```bash
curl -X POST http://localhost:4000/tasks/v1/start \
  -H "Content-Type: application/json" \
  -d '{
    "requestor_id": 12345,
    "task_name": "PrimeTask",
    "task_context": "{\"limit\": 10000}",
    "max_retries": 5
  }'
```

**Default:** `max_retries = 3`

### Retry Flow Diagram

```
Task Fails
    │
    ▼
Worker sends error to 'task-errors' topic
    │
    ▼
Server Error Consumer
    │
    ├─→ retry_count <= max_retries?
    │       │
    │       ├─→ YES: Increment retry_count
    │       │        Republish to 'input-readings'
    │       │        Worker retries task
    │       │
    │       └─→ NO:  Mark as 'error' status
    │                Store final error message
    │                Task permanently failed
```

### Example Scenarios

**Scenario 1: Task succeeds after 2 retries**
- Attempt 1: Fails (network timeout) → retry_count = 1
- Attempt 2: Fails (network timeout) → retry_count = 2
- Attempt 3: Succeeds → status = "completed", retry_count = 2

**Scenario 2: Task exhausts all retries**
- Attempt 1-4: All fail
- Final: status = "error", retry_count = 4 (exceeds max_retries = 3)

**Scenario 3: Non-retryable error**
- Worker sets `is_retryable = false`
- Server immediately marks as "error" without retry

### Kafka Topics for Retry

| Topic | Purpose |
|-------|---------|
| `input-readings` | Task queue (initial + retries) |
| `responses` | Successful task results |
| `task-errors` | Failed task errors (triggers retry logic) |

### Monitoring Retries

```sql
-- View tasks with retries
SELECT id, task_name, retry_count, max_retries, status, error
FROM tasks
WHERE retry_count > 0
ORDER BY retry_count DESC;

-- Tasks that failed permanently
SELECT id, task_name, retry_count, error
FROM tasks
WHERE status = 'error';
```

## Service Management Scripts

Sage provides convenient shell scripts in the `bin/` directory for managing all services.

### bin/start_all_services.sh

Launches all Sage components in the correct order with health checks and auto-recovery.

**Usage:**
```bash
./bin/start_all_services.sh
```

**What it does:**
1. **Cleans up old containers** - Runs `docker-compose down` to prevent conflicts
2. **Starts Docker Compose** - Launches Kafka, Zookeeper, PostgreSQL, Kafka UI
3. **Kafka health monitoring** - Checks Kafka every 5 seconds (up to 60 seconds)
   - If Kafka fails, automatically restarts ONLY the Kafka container
   - Uses `docker ps` to verify container status
4. **PostgreSQL health check** - Verifies database is ready
5. **Starts Sage Server** - HTTP API at `http://localhost:4000`
6. **Starts Sage Worker** - Task processor
7. **Starts Sage UI** - Web interface at `http://localhost:5173`
8. **Reports status** - Displays all PIDs, URLs, and log file locations

**Features:**
- ✅ **Kafka auto-restart** - Automatically restarts Kafka if it fails during startup
- ✅ **Intelligent retry** - Retries Kafka health check every 5 seconds (configurable)
- ✅ **Process tracking** - Displays PIDs for all spawned processes
- ✅ **Comprehensive logging** - Logs saved to `/log` directory
- ✅ **Graceful cleanup** - Ctrl+C stops all services gracefully
- ✅ **Color-coded output** - Easy to read status messages

**Configuration (edit script to customize):**
```bash
RETRY_INTERVAL=5        # Seconds between Kafka retry attempts
MAX_KAFKA_RETRIES=12    # Maximum retry attempts (60 seconds total)
```

**Example output:**
```
╔═══════════════════════════════════════════════════════════╗
║                   ALL SERVICES ARE UP!                    ║
╚═══════════════════════════════════════════════════════════╝

✓ Service Status Summary:

  ✓ Docker Compose:  Running
    - Kafka:           localhost:9092
    - Kafka UI:        http://localhost:8080
    - PostgreSQL:      localhost:5432
    - Zookeeper:       localhost:2181

  ✓ Sage Server:     http://localhost:4000 (PID: 12345)
  ✓ Sage Worker:     Running (PID: 12346)
  ✓ Sage UI:         http://localhost:5173 (PID: 12347)
```

### bin/stop_all_services.sh

Stops all Sage services and Docker containers with verification.

**Usage:**
```bash
./bin/stop_all_services.sh
```

**What it does:**
1. **Stops Sage processes** - Kills sage_server, sage_worker, sage_ui, and cargo processes
   - Attempts graceful shutdown with SIGTERM
   - Force kills with SIGKILL after 1 second if needed
2. **Stops Docker containers** - Runs `docker-compose down`
   - Falls back to manual container stopping if docker-compose fails
3. **Verifies cleanup** - Checks that all services stopped successfully

**Features:**
- ✅ **Graceful shutdown** - Tries SIGTERM first, then SIGKILL if necessary
- ✅ **Complete cleanup** - Stops and removes all Docker containers
- ✅ **Verification** - Confirms all services are stopped
- ✅ **Fallback mode** - Manual container stopping if docker-compose fails
- ✅ **Color-coded output** - Clear feedback on what was stopped

**Example output:**
```
╔═══════════════════════════════════════════════════════════╗
║          SAGE - Stopping All Services                     ║
╚═══════════════════════════════════════════════════════════╝

▶ Stopping Sage processes (Server, Worker, UI)...
ℹ Found sage_server processes: 12345
✓ Killed sage_server (PID: 12345)
ℹ Found sage_worker processes: 12346
✓ Killed sage_worker (PID: 12346)
ℹ Found sage_ui processes: 12347
✓ Killed sage_ui (PID: 12347)
✓ All Sage processes stopped

▶ Stopping Docker Compose containers...
✓ Docker Compose containers stopped and removed

▶ Verifying cleanup...
✓ All services successfully stopped
```

### Log Files

All services log to the `/log` directory:
- `log/docker-compose.log` - Docker startup logs
- `log/sage_server.log` - Server output
- `log/sage_worker.log` - Worker output
- `log/sage_ui.log` - UI development server output

## Components

### sage_server

An Axum-based HTTP REST API server that:
- **Accepts HTTP POST requests** at `/tasks/v1/start`
- **Stores tasks in PostgreSQL** with status "pending"
- **Publishes tasks** to Kafka topic `input-readings`
- **Consumes responses** from Kafka topic `responses`
- **Updates task status** in database with results
- **Auto-generates UUIDs** for request/response correlation
- **Graceful shutdown** with `tokio::select!` for Ctrl+C handling
- **CORS enabled** for cross-origin requests

### sage_worker

A background worker that:
- **Consumes tasks** from Kafka topic `input-readings`
- **Executes tasks** asynchronously with Tokio
- **spawn_blocking** for CPU-intensive tasks (e.g., PrimeTask)
- **Creates SageMessage** with result in `task_envelope` field
- **Publishes results** to Kafka topic `responses`
- **Parallel processing** - run multiple workers simultaneously
- **Type-safe dispatch** via enum pattern matching

## Performance

- **Native async/await** with Tokio runtime
- **spawn_blocking** for CPU-intensive tasks (Prime calculation)
- **Zero-copy** message passing where possible
- **Parallel execution** using Rayon for compute tasks
- **Horizontal scaling** - run multiple workers
- **No GIL** - True parallelism

## Roadmap

- [x] Kafka integration
- [x] Type-safe task definitions
- [x] Request/Response ID correlation
- [x] CPU-intensive task support
- [x] HTTP REST API server (sage_server)
- [x] Worker orchestration (sage_worker)
- [x] Graceful shutdown handling
- [x] Python producer/consumer examples (legacy)
- [x] PostgreSQL result backend with task persistence
- [x] Task status constraints (pending, completed, error)
- [x] Response processing and database updates
- [x] **Task retry mechanism with configurable max attempts**
- [ ] Priority queues
- [ ] Task scheduling (cron-like)
- [ ] Monitoring and metrics
- [ ] Web dashboard
- [ ] Task chains and workflows
- [ ] WebSocket support for real-time updates

## Comparison

| Feature | Sage (Rust) | Celery (Python) |
|---------|-------------|-----------------|
| **Performance** | ⭐⭐⭐⭐⭐ Native | ⭐⭐ Interpreted |
| **Memory Safety** | ✅ Compile-time | ❌ Runtime |
| **ID Tracking** | ✅ Automatic | ⚠️ Manual |
| **Type Safety** | ✅ Strong generics | ⚠️ Dynamic |
| **Concurrency** | ✅ True parallelism | ❌ GIL limited |
| **CPU Tasks** | ✅ spawn_blocking | ⚠️ multiprocessing |

## Contributing

Contributions welcome! This project demonstrates:
- Modern Rust async patterns
- Generic wrapper types with serde
- Kafka integration with rdkafka
- Enum dispatch patterns
- Cross-language RPC (Rust ↔ Python)

## License

MIT / Apache-2.0 (choose your preference)

## API Documentation

### POST /tasks/v1/start

Submit a new task for processing.

**Request Body:**
```json
{
  "requestor_id": 12345,
  "task_name": "PrimeTask",
  "task_context": "{\"limit\": 10000}"
}
```

**Response:**
```json
{
  "status": true,
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Available Tasks:**
- `PrimeTask` - Calculate prime numbers up to a limit
- `SampleTask` - Simple echo task for testing

---

**Status**: 🚀 **Production-Ready** - HTTP API server + distributed workers operational
