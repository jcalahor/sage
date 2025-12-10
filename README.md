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
┌──────────────┐
│   Producer   │ ← Sends tasks with auto-generated UUIDs
│  (Python/Any)│
└──────┬───────┘
       │
       ▼
┌─────────────────────┐
│      Kafka          │ ← Message Broker
│  input-readings     │   (Distributes tasks)
│     responses       │   (Collects results)
└──────────┬──────────┘
           │
    ┌──────┴──────┬──────────┐
    │             │          │
┌───▼────┐   ┌───▼────┐  ┌──▼─────┐
│Worker 1│   │Worker 2│  │Worker 3│ ← Parallel execution
│(Rust)  │   │(Rust)  │  │(Rust)  │   CPU-intensive tasks
└───┬────┘   └───┬────┘  └───┬────┘
    │            │           │
    └────────────┴───────────┘
                 │
                 ▼
          ┌──────────────┐
          │   Consumer   │ ← Receives results with IDs
          │ (Python/Any) │
          └──────────────┘
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
│   ├── TaskRequest<T>   # Request wrapper with auto UUID
│   ├── TaskResponse<R>  # Response wrapper with ID correlation
│   └── SageTask<T, R>   # Task trait definition
├── tasks_impl/        # Task implementations (business logic)
│   ├── PrimeTask        # Example: Prime number calculation
│   └── SampleTask       # Example: Simple task
├── sage_worker/       # Worker orchestration & dispatch
│   ├── TaskRequestType  # Enum for routing
│   ├── TaskResponseType # Enum for responses
│   └── Kafka consumers/producers
├── samples/
│   ├── producer/      # Python example producer
│   └── consumer/      # Python example consumer
└── environment/       # Docker compose for Kafka
```

## Quick Start

### 1. Start Kafka

```bash
cd environment
docker-compose up -d
```

### 2. Run the Worker

```bash
cargo run --bin sage_worker
```

### 3. Send Tasks (Python Producer)

```bash
cd samples/producer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python produce.py
```

### 4. Consume Results (Python Consumer)

```bash
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

### 3. Register in Worker (sage_worker/src/main.rs)

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

## Performance

- **Native async/await** with Tokio runtime
- **spawn_blocking** for CPU-intensive tasks (Prime calculation)
- **Zero-copy** message passing where possible
- **Parallel execution** using Rayon for compute tasks
- **No GIL** - True parallelism

## Roadmap

- [x] Kafka integration
- [x] Type-safe task definitions
- [x] Request/Response ID correlation
- [x] CPU-intensive task support
- [x] Python producer/consumer examples
- [ ] Result backend (Redis/PostgreSQL)
- [ ] Task retry mechanism
- [ ] Priority queues
- [ ] Task scheduling (cron-like)
- [ ] Monitoring and metrics
- [ ] Web dashboard
- [ ] Task chains and workflows

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

---

**Status**: 🚀 **Production-Ready Core** - Kafka integration complete, ID tracking implemented
