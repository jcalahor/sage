# Sage

A high-performance distributed task queue system built in Rust, similar to Celery but with Rust's performance and safety guarantees.

## Overview

Sage is a distributed task processing system that allows you to:
- Execute async tasks across multiple worker nodes
- Distribute workload via Kafka message broker
- Define type-safe task requests and implementations
- Scale horizontally by adding more worker instances

## Architecture

```
                    ┌─────────────┐
                    │    Kafka    │ ← Message Broker
                    │   (Broker)  │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
    ┌───▼───┐          ┌───▼───┐         ┌───▼───┐
    │Worker │          │Worker │         │Worker │
    │  #1   │          │  #2   │         │  #3   │
    └───────┘          └───────┘         └───────┘
```

## Features

- ✅ **Type-Safe Tasks** - Compile-time guarantees for task definitions
- ✅ **Async/Await** - Built on Tokio for efficient concurrency
- ✅ **Clean API** - Using `async-trait` for ergonomic async traits
- ✅ **Distributed** - Kafka integration for horizontal scaling (planned)
- ✅ **Fast** - Rust performance without garbage collection overhead
- ✅ **Memory Safe** - No data races or memory leaks

## Project Structure

```
sage/
├── task/           # Core task trait definitions
├── tasks_impl/     # Task implementations
└── sage_server/    # Main worker server application
```

## Current Implementation

### Defining a Task

```rust
use task::{SageTask, SageTaskRequest};
use async_trait::async_trait;

// Define your request type
pub struct MyRequest {
    pub data: String,
}
impl SageTaskRequest for MyRequest {}

// Define your task
pub struct MyTask {}

// Implement the task
#[async_trait]
impl SageTask<MyRequest> for MyTask {
    async fn run(&self, request: &MyRequest) -> Result<(), Box<dyn std::error::Error + Send>> {
        println!("Processing: {}", request.data);
        // Your task logic here
        Ok(())
    }
}
```

### Running Tasks

```rust
let request = MyRequest { data: "Hello".to_string() };
let task = MyTask {};
task.run(&request).await?;
```

## Getting Started

### Prerequisites

- Rust 1.70+ (edition 2024)
- Cargo

### Building

```bash
cargo build --release
```

### Running

```bash
cargo run
```

## Roadmap

- [ ] Kafka integration for distributed task queue
- [ ] Result backend for task result storage
- [ ] Task retry mechanism
- [ ] Priority queues
- [ ] Task scheduling (cron-like)
- [ ] Monitoring and metrics
- [ ] Web dashboard (like Celery Flower)
- [ ] Task chains and workflows
- [ ] Rate limiting

## Comparison with Other Systems

| Feature | Sage (Rust) | Celery (Python) | Sidekiq (Ruby) |
|---------|-------------|-----------------|----------------|
| **Performance** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| **Memory Safety** | ✅ Compile-time | ❌ Runtime | ❌ Runtime |
| **Async** | Native (Tokio) | Asyncio | N/A |
| **Type Safety** | ✅ Strong | ⚠️ Dynamic | ⚠️ Dynamic |
| **Concurrency** | True parallelism | GIL limited | GIL limited |

## Use Cases

- Background job processing for web applications
- Data pipeline orchestration
- Scheduled task execution
- Webhook processing
- Email/notification systems
- Image/video processing
- API request offloading
- Batch data processing

## Contributing

Contributions are welcome! This is an early-stage project.

## License

[Your chosen license]

## Acknowledgments

- Inspired by Celery (Python) and Sidekiq (Ruby)
- Built with Rust's async ecosystem (Tokio, async-trait)
- Uses Kafka for distributed messaging (planned)

---

**Status**: 🚧 Early Development - Not production ready
