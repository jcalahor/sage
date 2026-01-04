mod api;
mod db;
mod schedule_utils;
mod server;
mod types;

use crate::db::{ScheduledTaskUpdate, TaskCreate, create_task, get_due_scheduled_tasks};
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{
    Message as RdMesssage,
    consumer::{BaseConsumer, Consumer},
};
use sqlx::PgPool;
use std::sync::Arc;
use task::SageMessage;
use tokio::sync::broadcast;
use uuid::Uuid;

use schedule_utils::calculate_next_run;

/// Publishes a task to Kafka for processing
async fn publish_task_to_kafka(
    producer: &Arc<FutureProducer>,
    task: &db::Task,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = task.requestor_id.to_string();

    let sage_message = SageMessage {
        task_id: task.id,
        task_name: task.task_name.clone(),
        task_envelope: task.task_context.clone(),
    };

    let payload_string = serde_json::to_string(&sage_message)?;

    let record = FutureRecord::to("input-readings")
        .key(&key)
        .payload(&payload_string);

    match producer.send(record, 0).await {
        Ok(Ok(_)) => {
            println!(
                "Scheduled task |{}| sent successfully to topic 'input-readings'",
                task.task_name
            );
            Ok(())
        }
        Ok(Err((kafka_error, _))) => {
            let error_msg = format!("Failed to send scheduled task to Kafka: {}", kafka_error);
            eprintln!("{}", error_msg);
            Err(error_msg.into())
        }
        Err(e) => {
            let error_msg = format!("Kafka producer send cancelled: {}", e);
            eprintln!("{}", error_msg);
            Err(error_msg.into())
        }
    }
}

/// Runs the scheduler loop, checking for due tasks and publishing them to Kafka
async fn run_scheduler(
    producer: Arc<FutureProducer>,
    db_pool: PgPool,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    println!("Scheduler task started");

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                println!("Scheduler task shutting down...");
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                // Query for due tasks
                println!("Checking if any tasks needs to run");
                match get_due_scheduled_tasks(&db_pool).await {
                    Ok(due_tasks) => {
                        if !due_tasks.is_empty() {
                            println!("Found {} due scheduled tasks", due_tasks.len());
                        }

                        for scheduled_task in due_tasks {
                            println!(
                                "Processing scheduled task: {} (ID: {})",
                                scheduled_task.schedule_name, scheduled_task.id
                            );

                            // Create task entry in tasks table
                            let task_id = Uuid::new_v4();
                            match create_task(
                                &db_pool,
                                TaskCreate {
                                    id: task_id,
                                    requestor_id: scheduled_task.requestor_id,
                                    task_name: scheduled_task.task_name.clone(),
                                    task_context: scheduled_task.task_context.clone(),
                                    priority: Some(scheduled_task.priority),
                                    max_retries: Some(scheduled_task.max_retries),
                                },
                            )
                            .await {
                                Ok(task) => {
                                    println!("Created task {} for scheduled task {}", task.id, scheduled_task.schedule_name);

                                    // Publish to Kafka
                                    if let Err(e) = publish_task_to_kafka(&producer, &task).await {
                                        eprintln!("Failed to publish task {} to Kafka: {}", task.id, e);
                                        continue;
                                    }

                                    // Calculate next run time
                                    match calculate_next_run(&scheduled_task.cron_expression, &scheduled_task.timezone) {
                                        Ok(next_run) => {
                                            // Update scheduled task
                                            match db::update_scheduled_task(
                                                &db_pool,
                                                ScheduledTaskUpdate {
                                                    id: scheduled_task.id,
                                                    schedule_name: None,
                                                    task_name: None,
                                                    task_context: None,
                                                    cron_expression: None,
                                                    timezone: None,
                                                    enabled: None,
                                                    priority: None,
                                                    max_retries: None,
                                                    last_run_at: Some(Utc::now()),
                                                    next_run_at: Some(next_run),
                                                    created_by: None,
                                                    metadata: None,
                                                },
                                            )
                                            .await {
                                                Ok(_) => {
                                                    println!(
                                                        "Updated scheduled task {} - next run at {}",
                                                        scheduled_task.schedule_name, next_run
                                                    );
                                                }
                                                Err(e) => {
                                                    eprintln!(
                                                        "Failed to update scheduled task {}: {}",
                                                        scheduled_task.schedule_name, e
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Failed to calculate next run for scheduled task '{}': {}",
                                                scheduled_task.schedule_name, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Failed to create task for scheduled task '{}': {}",
                                        scheduled_task.schedule_name, e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to query due scheduled tasks: {}", e);
                    }
                }
            }
        }
    }

    println!("Scheduler task terminated");
}

async fn refresh_tasks_schedules(db_pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut scheduled_tasks = db::get_all_scheduled_tasks(&db_pool)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    // TODO: Process scheduled_tasks and update their next run times
    println!("Retrieved {} scheduled tasks", scheduled_tasks.len());

    for scheduled_task in &mut scheduled_tasks {
        // TODO: Calculate next_run_at and update the scheduled task
        println!(
            "Processing scheduled task: {}",
            scheduled_task.schedule_name
        );

        match calculate_next_run(&scheduled_task.cron_expression, &scheduled_task.timezone) {
            Ok(next_run_time) => {
                scheduled_task.next_run_at = next_run_time;

                db::update_scheduled_task(
                    &db_pool,
                    ScheduledTaskUpdate {
                        id: scheduled_task.id,
                        schedule_name: None,
                        task_name: None,
                        task_context: None,
                        cron_expression: None,
                        timezone: None,
                        enabled: None,
                        priority: None,
                        max_retries: None,
                        last_run_at: None,
                        next_run_at: Some(next_run_time),
                        created_by: None,
                        metadata: None,
                    },
                )
                .await?;
            }
            Err(e) => {
                eprintln!(
                    "Failed to calculate next run for scheduled task '{}': {}",
                    scheduled_task.schedule_name, e
                );
            }
        }
    }

    Ok(())
}

fn create_kafka_consumer() -> BaseConsumer {
    match ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092") // Replace with actual config
        .set("group.id", "sage_server") // Replace with actual group ID
        .create::<BaseConsumer>()
    {
        Ok(consumer) => {
            println!("Kafka consumer successfully created!");
            consumer
        }
        Err(err) => {
            println!("Failed to create Kafka consumer: {}", err);
            panic!("Kafka consumer creation failed");
        }
    }
}

fn create_kafka_producer() -> FutureProducer {
    match ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<FutureProducer>()
    {
        Ok(producer) => {
            println!("Kafka producer successfully created!");
            producer
        }
        Err(err) => {
            println!("Failed to create Kafka producer: {}", err);
            panic!("Kafka producer creation failed");
        }
    }
}

async fn process_responses(
    consumer: BaseConsumer,
    db_pool: PgPool,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                println!("Consumer task shutting down...");
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                match consumer.poll(std::time::Duration::from_millis(100)) {
                    Some(Ok(msg)) => {
                        if let Some(payload) = msg.payload() {
                            match serde_json::from_slice::<SageMessage>(payload) {
                                Ok(sage_msg) => {
                                    println!(
                                        "Response received - task_id: {}, task_name: '{}'",
                                        sage_msg.task_id, sage_msg.task_name
                                    );

                                    // Parse envelope as JSON to store in result column
                                    let result_json: serde_json::Value = match serde_json::from_str(&sage_msg.task_envelope) {
                                        Ok(json) => json,
                                        Err(e) => {
                                            eprintln!("Failed to parse task_envelope as JSON: {}", e);
                                            continue;
                                        }
                                    };

                                    // Create TaskUpdate with the response data
                                    let task_update = db::TaskUpdate {
                                        id: sage_msg.task_id,
                                        status: Some("completed".to_string()),
                                        started_at: None,
                                        completed_at: Some(Utc::now()),
                                        result: Some(result_json),
                                        error: None,
                                        worker_id: None,
                                        retry_count: None,
                                    };

                                    // Update the task in the database
                                    match db::update_task(&db_pool, task_update).await {
                                        Ok(task) => {
                                            println!("Task {} updated successfully with status: {}", task.id, task.status);
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to update task {}: {}", sage_msg.task_id, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to deserialize SageMessage: {}", e);
                                }
                            }
                        } else {
                            println!("Received empty Kafka message payload");
                        }
                    }
                    Some(Err(e)) => {
                        println!("Kafka error: {}", e);
                    }
                    None => {}
                }
            }
        }
    }
    println!("Consumer task terminated");
}

#[tokio::main]
async fn main() {
    // Initialize database connection
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sage:sage_password@localhost:5432/sage_db".to_string());

    println!("Connecting to database...");
    let db_pool = match db::create_pool(&database_url).await {
        Ok(pool) => {
            println!("Database connection established!");
            pool
        }
        Err(err) => {
            eprintln!("Failed to connect to database: {}", err);
            panic!("Database connection failed");
        }
    };

    // Initialize database schema
    if let Err(err) = db::init_db(&db_pool).await {
        eprintln!("Failed to initialize database: {}", err);
        panic!("Database initialization failed");
    }

    let address = format!("{}:{}", "0.0.0.0", 4000);
    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();

    // set next run for all tasks in case server has been down for a while
    if let Err(err) = refresh_tasks_schedules(db_pool.clone()).await {
        eprintln!("Failed to refresh task schedules: {}", err);
    }

    println!("Server started at {}", &address);
    let producer: Arc<FutureProducer> = Arc::new(create_kafka_producer());
    let consumer = create_kafka_consumer();
    consumer
        .subscribe(&["responses"])
        .expect("topic subscribe failed");

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = broadcast::channel(3);

    // Spawn consumer task
    let shutdown_rx_consumer = shutdown_tx.subscribe();
    let consumer_handle = tokio::spawn(process_responses(
        consumer,
        db_pool.clone(),
        shutdown_rx_consumer,
    ));

    // Spawn scheduler task
    let shutdown_rx_scheduler = shutdown_tx.subscribe();
    let scheduler_handle = tokio::spawn(run_scheduler(
        producer.clone(),
        db_pool.clone(),
        shutdown_rx_scheduler,
    ));

    // Setup Ctrl+C handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        println!("\nReceived Ctrl+C, initiating graceful shutdown...");
        let _ = shutdown_tx_clone.send(());
    });

    // Run server with graceful shutdown
    let server = axum::serve(
        listener,
        server::build_server(producer.clone(), db_pool.clone())
            .await
            .into_make_service(),
    );

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                eprintln!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nReceived Ctrl+C, shutting down server...");
        }
    }

    // Signal shutdown to all tasks
    let _ = shutdown_tx.send(());

    // Wait for all tasks to finish gracefully
    println!("Waiting for background tasks to finish...");

    if let Err(e) = consumer_handle.await {
        eprintln!("Consumer task error: {}", e);
    }

    if let Err(e) = scheduler_handle.await {
        eprintln!("Scheduler task error: {}", e);
    }

    println!("Shutdown complete");
}
