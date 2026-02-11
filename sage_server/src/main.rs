mod api;
mod db;
mod schedule_utils;
mod server;
mod types;

use crate::db::{JobUpdate, TaskCreate, add_job_history_record, create_task, get_due_jobs};
use chrono::Utc;
use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{
    Message as RdMesssage,
    consumer::{BaseConsumer, Consumer},
};
use simplelog::*;
use sqlx::PgPool;
use std::fs::File;
use std::sync::Arc;
use task::{SageErrorResponse, SageMessage};
use tokio::sync::broadcast;
use uuid::Uuid;

use schedule_utils::calculate_next_run;

/// Publishes a task to Kafka for processing
async fn publish_task_to_kafka(
    producer: &Arc<FutureProducer>,
    task: &db::Task,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = task.requestor_id.to_string();
    let topic = std::env::var("KAFKA_TOPIC_INPUT")
        .expect("KAFKA_TOPIC_INPUT must be set in environment or .env file");

    let sage_message = SageMessage {
        task_id: task.id,
        task_name: task.task_name.clone(),
        task_envelope: task.task_context.clone(),
    };

    let payload_string = serde_json::to_string(&sage_message)?;

    let record = FutureRecord::to(&topic).key(&key).payload(&payload_string);

    match producer.send(record, 0).await {
        Ok(Ok(_)) => {
            info!(
                "Scheduled task |{}| sent successfully to topic '{}'",
                task.task_name, topic
            );
            Ok(())
        }
        Ok(Err((kafka_error, _))) => {
            let error_msg = format!("Failed to send scheduled task to Kafka: {}", kafka_error);
            error!("{}", error_msg);
            Err(error_msg.into())
        }
        Err(e) => {
            let error_msg = format!("Kafka producer send cancelled: {}", e);
            error!("{}", error_msg);
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
    info!("Scheduler task started");

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Scheduler task shutting down...");
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                // Query for due tasks
                info!("Checking if any tasks needs to run");
                match get_due_jobs(&db_pool).await {
                    Ok(due_tasks) => {
                        if !due_tasks.is_empty() {
                            info!("Found {} due scheduled tasks", due_tasks.len());
                        }

                        for job in due_tasks {
                            info!(
                                "Processing scheduled task: {} (ID: {})",
                                job.schedule_name, job.id
                            );

                            // Create task entry in tasks table
                            let task_id = Uuid::new_v4();
                            match create_task(
                                &db_pool,
                                TaskCreate {
                                    id: task_id,
                                    requestor_id: job.requestor_id,
                                    task_name: job.task_name.clone(),
                                    task_context: job.task_context.clone(),
                                    priority: Some(job.priority),
                                    max_retries: Some(job.max_retries),
                                },
                            )
                            .await {
                                Ok(task) => {
                                    info!("Created task {} for scheduled task {}", task.id, job.schedule_name);

                                    // Publish to Kafka and immediately extract result
                                    let task_id_for_history = task.id;
                                    let job_id_for_history = job.id;

                                    let publish_succeeded;
                                    let publish_error_msg;
                                    {
                                        let result = publish_task_to_kafka(&producer, &task).await;
                                        publish_succeeded = result.is_ok();
                                        publish_error_msg = result.err().map(|e| format!("Failed to publish to Kafka: {}", e));
                                    }

                                    if publish_succeeded {
                                        // Record successful submission in job history
                                        if let Err(e) = add_job_history_record(
                                            &db_pool,
                                            job_id_for_history,
                                            Some(task_id_for_history),
                                            "submitted".to_string(),
                                            None,
                                        ).await {
                                            error!("Failed to record job history for job {}: {}", job_id_for_history, e);
                                        }
                                    } else {
                                        let error_msg = publish_error_msg.unwrap();
                                        error!("Failed to publish task {} to Kafka: {}", task_id_for_history, error_msg);

                                        // Record error in job history
                                        if let Err(hist_err) = add_job_history_record(
                                            &db_pool,
                                            job_id_for_history,
                                            Some(task_id_for_history),
                                            "error".to_string(),
                                            Some(error_msg),
                                        ).await {
                                            error!("Failed to record job history error for job {}: {}", job_id_for_history, hist_err);
                                        }
                                        continue;
                                    }

                                    // Calculate next run time
                                    match calculate_next_run(&job.cron_expression, &job.timezone) {
                                        Ok(next_run) => {
                                            // Update scheduled task
                                            match db::update_job(
                                                &db_pool,
                                                JobUpdate {
                                                    id: job.id,
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
                                                    info!(
                                                        "Updated scheduled task {} - next run at {}",
                                                        job.schedule_name, next_run
                                                    );
                                                }
                                                Err(e) => {
                                                    error!(
                                                        "Failed to update scheduled task {}: {}",
                                                        job.schedule_name, e
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to calculate next run for scheduled task '{}': {}",
                                                job.schedule_name, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    let error_msg = format!("Failed to create task: {}", e);
                                    let schedule_name = job.schedule_name.clone();
                                    drop(e); // Drop error before await
                                    error!(
                                        "Failed to create task for scheduled task '{}': {}",
                                        schedule_name, error_msg
                                    );

                                    // Record error in job history
                                    if let Err(hist_err) = add_job_history_record(
                                        &db_pool,
                                        job.id,
                                        None,
                                        "error".to_string(),
                                        Some(error_msg),
                                    ).await {
                                        error!("Failed to record job history error for job {}: {}", job.id, hist_err);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to query due scheduled tasks: {}", e);
                    }
                }
            }
        }
    }

    info!("Scheduler task terminated");
}

async fn refresh_tasks_schedules(db_pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut jobs = db::get_all_jobs(&db_pool)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    // TODO: Process jobs and update their next run times
    info!("Retrieved {} scheduled tasks", jobs.len());

    for job in &mut jobs {
        // TODO: Calculate next_run_at and update the scheduled task
        info!("Processing scheduled task: {}", job.schedule_name);

        match calculate_next_run(&job.cron_expression, &job.timezone) {
            Ok(next_run_time) => {
                job.next_run_at = next_run_time;

                db::update_job(
                    &db_pool,
                    JobUpdate {
                        id: job.id,
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
                error!(
                    "Failed to calculate next run for scheduled task '{}': {}",
                    job.schedule_name, e
                );
            }
        }
    }

    Ok(())
}

fn create_kafka_consumer() -> BaseConsumer {
    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .expect("KAFKA_BOOTSTRAP_SERVERS must be set in environment or .env file");
    let group_id = std::env::var("KAFKA_SERVER_GROUP_ID")
        .expect("KAFKA_SERVER_GROUP_ID must be set in environment or .env file");

    match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", &group_id)
        .create::<BaseConsumer>()
    {
        Ok(consumer) => {
            info!("Kafka consumer successfully created!");
            consumer
        }
        Err(err) => {
            info!("Failed to create Kafka consumer: {}", err);
            panic!("Kafka consumer creation failed");
        }
    }
}

fn create_kafka_producer() -> FutureProducer {
    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .expect("KAFKA_BOOTSTRAP_SERVERS must be set in environment or .env file");

    match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create::<FutureProducer>()
    {
        Ok(producer) => {
            info!("Kafka producer successfully created!");
            producer
        }
        Err(err) => {
            info!("Failed to create Kafka producer: {}", err);
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
                info!("Consumer task shutting down...");
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                match consumer.poll(std::time::Duration::from_millis(100)) {
                    Some(Ok(msg)) => {
                        if let Some(payload) = msg.payload() {
                            match serde_json::from_slice::<SageMessage>(payload) {
                                Ok(sage_msg) => {
                                    info!(
                                        "Response received - task_id: {}, task_name: '{}', response: {}",
                                        sage_msg.task_id, sage_msg.task_name, sage_msg.task_envelope
                                    );

                                    // Parse envelope as JSON to store in result column
                                    let result_json: serde_json::Value = match serde_json::from_str(&sage_msg.task_envelope) {
                                        Ok(json) => json,
                                        Err(e) => {
                                            error!("Failed to parse task_envelope as JSON: {}", e);
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
                                            info!("Task {} updated successfully with status: {}", task.id, task.status);

                                            // Find the job that created this task and record completion in job history
                                            match sqlx::query_scalar::<_, uuid::Uuid>(
                                                "SELECT job_id FROM job_history WHERE task_id = $1 AND status = 'submitted' LIMIT 1"
                                            )
                                            .bind(task.id)
                                            .fetch_optional(&db_pool)
                                            .await {
                                                Ok(Some(job_id)) => {
                                                    // Record task completion in job history
                                                    if let Err(e) = add_job_history_record(
                                                        &db_pool,
                                                        job_id,
                                                        Some(task.id),
                                                        "completed".to_string(),
                                                        None,
                                                    ).await {
                                                        error!("Failed to record job completion history for job {}: {}", job_id, e);
                                                    }
                                                }
                                                Ok(None) => {
                                                    // Task not created by a job (manually submitted)
                                                }
                                                Err(e) => {
                                                    error!("Failed to query job_history for task {}: {}", task.id, e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to update task {}: {}", sage_msg.task_id, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to deserialize SageMessage: {}", e);
                                }
                            }
                        } else {
                            info!("Received empty Kafka message payload");
                        }
                    }
                    Some(Err(e)) => {
                        info!("Kafka error: {}", e);
                    }
                    None => {}
                }
            }
        }
    }
    info!("Consumer task terminated");
}

async fn process_errors(
    consumer: BaseConsumer,
    producer: Arc<FutureProducer>,
    db_pool: PgPool,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    info!("Error consumer task started");

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Error consumer task shutting down...");
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                match consumer.poll(std::time::Duration::from_millis(100)) {
                    Some(Ok(msg)) => {
                        if let Some(payload) = msg.payload() {
                            match serde_json::from_slice::<SageErrorResponse>(payload) {
                                Ok(error_response) => {
                                    info!(
                                        "Error received - task_id: {}, task_name: '{}', error: '{}'",
                                        error_response.task_id, error_response.task_name, error_response.error_message
                                    );

                                    // Fetch the task from database to check retry count
                                    match db::get_task_by_id(&db_pool, error_response.task_id).await {
                                        Ok(Some(task)) => {
                                            let new_retry_count = task.retry_count + 1;

                                            // Check if we should retry
                                            if error_response.is_retryable && new_retry_count <= task.max_retries {
                                                info!(
                                                    "Task {} failed (attempt {}/{}), retrying...",
                                                    task.id, new_retry_count, task.max_retries
                                                );

                                                // Update retry count in database
                                                let task_update = db::TaskUpdate {
                                                    id: task.id,
                                                    status: None, // Keep as pending
                                                    started_at: None,
                                                    completed_at: None,
                                                    result: None,
                                                    error: Some(format!(
                                                        "Attempt {}/{}: {}",
                                                        new_retry_count, task.max_retries, error_response.error_message
                                                    )),
                                                    worker_id: None,
                                                    retry_count: Some(new_retry_count),
                                                };

                                                match db::update_task(&db_pool, task_update).await {
                                                    Ok(updated_task) => {
                                                        info!("Task {} retry count updated to {}", updated_task.id, updated_task.retry_count);

                                                        // Republish task to Kafka for retry
                                                        if let Err(e) = publish_task_to_kafka(&producer, &updated_task).await {
                                                            error!("Failed to republish task {} for retry: {}", updated_task.id, e);
                                                        } else {
                                                            info!("Task {} republished for retry", updated_task.id);
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to update retry count for task {}: {}", task.id, e);
                                                    }
                                                }
                                            } else {
                                                // Max retries exceeded or not retryable
                                                let reason = if error_response.is_retryable {
                                                    format!(
                                                        "Max retries ({}) exceeded. Last error: {}",
                                                        task.max_retries, error_response.error_message
                                                    )
                                                } else {
                                                    format!("Non-retryable error: {}", error_response.error_message)
                                                };

                                                info!(
                                                    "Task {} permanently failed: {}",
                                                    task.id, reason
                                                );

                                                let task_update = db::TaskUpdate {
                                                    id: task.id,
                                                    status: Some("error".to_string()),
                                                    started_at: None,
                                                    completed_at: Some(Utc::now()),
                                                    result: None,
                                                    error: Some(reason),
                                                    worker_id: None,
                                                    retry_count: Some(new_retry_count),
                                                };

                                                match db::update_task(&db_pool, task_update).await {
                                                    Ok(_) => {
                                                        info!("Task {} marked as permanently failed", task.id);
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to update task {} to error status: {}", task.id, e);
                                                    }
                                                }
                                            }
                                        }
                                        Ok(None) => {
                                            error!("Task {} not found in database", error_response.task_id);
                                        }
                                        Err(e) => {
                                            error!("Failed to fetch task {} from database: {}", error_response.task_id, e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to deserialize SageErrorResponse: {}", e);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!("Kafka error in error consumer: {}", e);
                    }
                    None => {}
                }
            }
        }
    }
    info!("Error consumer task terminated");
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Initialize logging
    let pid = std::process::id();
    std::fs::create_dir_all("log").expect("Failed to create log directory");
    let log_file =
        File::create(format!("log/sage_server_{}.log", pid)).expect("Failed to create log file");

    CombinedLogger::init(vec![WriteLogger::new(
        LevelFilter::Info,
        Config::default(),
        log_file,
    )])
    .expect("Failed to initialize logger");

    info!("Sage Server starting (PID: {})", pid);

    // Initialize database connection
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in environment or .env file");

    info!("Connecting to database...");
    let db_pool = match db::create_pool(&database_url).await {
        Ok(pool) => {
            info!("Database connection established!");
            pool
        }
        Err(err) => {
            error!("Failed to connect to database: {}", err);
            panic!("Database connection failed");
        }
    };

    // Initialize database schema
    if let Err(err) = db::init_db(&db_pool).await {
        error!("Failed to initialize database: {}", err);
        panic!("Database initialization failed");
    }

    let server_host =
        std::env::var("SERVER_HOST").expect("SERVER_HOST must be set in environment or .env file");
    let server_port =
        std::env::var("SERVER_PORT").expect("SERVER_PORT must be set in environment or .env file");
    let address = format!("{}:{}", server_host, server_port);
    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();

    // set next run for all tasks in case server has been down for a while
    if let Err(err) = refresh_tasks_schedules(db_pool.clone()).await {
        error!("Failed to refresh task schedules: {}", err);
    }

    info!("Server started at {}", &address);
    let producer: Arc<FutureProducer> = Arc::new(create_kafka_producer());

    // Get Kafka topic names from environment
    let responses_topic = std::env::var("KAFKA_TOPIC_RESPONSES")
        .expect("KAFKA_TOPIC_RESPONSES must be set in environment or .env file");
    let errors_topic = std::env::var("KAFKA_TOPIC_ERRORS")
        .expect("KAFKA_TOPIC_ERRORS must be set in environment or .env file");

    // Create response consumer
    let consumer = create_kafka_consumer();
    consumer
        .subscribe(&[&responses_topic])
        .expect("topic subscribe failed");

    // Create error consumer
    let error_consumer = create_kafka_consumer();
    error_consumer
        .subscribe(&[&errors_topic])
        .expect("topic subscribe failed");

    // Create shutdown channel (4 tasks: response consumer, error consumer, scheduler, server)
    let (shutdown_tx, shutdown_rx) = broadcast::channel(4);

    // Spawn response consumer task
    let shutdown_rx_consumer = shutdown_tx.subscribe();
    let consumer_handle = tokio::spawn(process_responses(
        consumer,
        db_pool.clone(),
        shutdown_rx_consumer,
    ));

    // Spawn error consumer task
    let shutdown_rx_error_consumer = shutdown_tx.subscribe();
    let error_consumer_handle = tokio::spawn(process_errors(
        error_consumer,
        producer.clone(),
        db_pool.clone(),
        shutdown_rx_error_consumer,
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
        info!("\nReceived Ctrl+C, initiating graceful shutdown...");
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
                error!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("\nReceived Ctrl+C, shutting down server...");
        }
    }

    // Signal shutdown to all tasks
    let _ = shutdown_tx.send(());

    // Wait for all tasks to finish gracefully
    info!("Waiting for background tasks to finish...");

    if let Err(e) = consumer_handle.await {
        error!("Consumer task error: {}", e);
    }

    if let Err(e) = error_consumer_handle.await {
        error!("Error consumer task error: {}", e);
    }

    if let Err(e) = scheduler_handle.await {
        error!("Scheduler task error: {}", e);
    }

    info!("Shutdown complete");
}
