use log::{error, info};
use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
};
use serde::{Deserialize, Serialize};
use simplelog::*;
use std::error::Error;
use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use task::{SageErrorResponse, SageMessage, SageTask, TaskRequest, TaskResponse};
use tasks_impl::{PrimeTask, PrimeTaskData, PrimeTaskResponseData, SampleTask};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

// Enum to support multiple task request types
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TaskRequestType {
    Prime(TaskRequest<PrimeTaskData>),
    // Add more task types here as needed
}

// Enum to support multiple task response types
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TaskResponseType {
    Prime(TaskResponse<PrimeTaskResponseData>),
    // Add more task types here as needed
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

async fn run_task(
    task_name: &str,
    request: &TaskRequestType,
) -> Result<TaskResponseType, Box<dyn std::error::Error + Send>> {
    match request {
        TaskRequestType::Prime(prime_request) => {
            let response = match task_name {
                "SampleTask" => {
                    let task = SampleTask {};
                    task.run(prime_request).await?
                }
                "PrimeTask" => {
                    let task = PrimeTask {};
                    task.run(prime_request).await?
                }
                _ => {
                    return Err(Box::new(io::Error::new(
                        io::ErrorKind::NotFound,
                        "Task not found",
                    )));
                }
            };
            Ok(TaskResponseType::Prime(response))
        }
    }
}

fn get_context(
    message: &SageMessage,
) -> Result<TaskRequestType, Box<dyn std::error::Error + Send>> {
    match message.task_name.as_str() {
        "SampleTask" | "PrimeTask" => {
            // Deserialize just the data from task_envelope
            let data: PrimeTaskData = serde_json::from_str(&message.task_envelope)
                .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;

            // Create TaskRequest with the task_id from SageMessage
            let request = TaskRequest::with_id(message.task_id, data);
            Ok(TaskRequestType::Prime(request))
        }
        _ => Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Unknown task name: {}", message.task_name),
        ))),
    }
}

async fn submit_response(
    producer: Arc<FutureProducer>,
    message: &SageMessage,
    response: TaskResponseType,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let responses_topic = std::env::var("KAFKA_TOPIC_RESPONSES")
        .expect("KAFKA_TOPIC_RESPONSES must be set in environment or .env file");

    // Pattern match on response type to extract inner data
    let response_payload = match &response {
        TaskResponseType::Prime(prime_response) => {
            info!(
                "{} Response (task_id: {}): {:?}",
                message.task_name.clone(),
                prime_response.task_id,
                prime_response.data
            );
            let envelope = serde_json::to_string(&prime_response.data)
                .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;
            (message.task_name.clone(), prime_response.task_id, envelope)
        }
    };

    let sage_message = SageMessage {
        task_id: response_payload.1,
        task_name: response_payload.0,
        task_envelope: response_payload.2,
    };

    let payload = serde_json::to_string(&sage_message)
        .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;

    let record = FutureRecord::to(&responses_topic)
        .payload(&payload)
        .key(&message.task_name);

    match producer.send(record, 0).await {
        Ok(Ok(_)) => {
            info!(
                "Response sent successfully to topic 'responses': {}",
                &payload
            );
            Ok(())
        }
        Ok(Err((e, _))) => {
            error!("Failed to send response: {:?}", e);
            Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to send to Kafka: {:?}", e),
            )))
        }
        Err(e) => {
            error!("Kafka send future canceled: {:?}", e);
            Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                format!("Kafka send canceled: {:?}", e),
            )))
        }
    }
}

async fn submit_error_response(
    producer: Arc<FutureProducer>,
    message: &SageMessage,
    error: Box<dyn std::error::Error + Send>,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let errors_topic = std::env::var("KAFKA_TOPIC_ERRORS")
        .expect("KAFKA_TOPIC_ERRORS must be set in environment or .env file");

    let error_response = SageErrorResponse {
        task_id: message.task_id,
        task_name: message.task_name.clone(),
        error_message: error.to_string(),
        is_retryable: true, // Consider all errors retryable for now
    };

    let payload = serde_json::to_string(&error_response)
        .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;

    let record = FutureRecord::to(&errors_topic)
        .payload(&payload)
        .key(&message.task_name);

    match producer.send(record, 0).await {
        Ok(Ok(_)) => {
            info!(
                "Error response sent successfully to topic 'task-errors' for task_id: {}",
                message.task_id
            );
            Ok(())
        }
        Ok(Err((e, _))) => {
            error!("Failed to send error response: {:?}", e);
            Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to send error to Kafka: {:?}", e),
            )))
        }
        Err(e) => {
            error!("Kafka error send future canceled: {:?}", e);
            Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                format!("Kafka error send canceled: {:?}", e),
            )))
        }
    }
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Initialize logging
    let pid = std::process::id();
    std::fs::create_dir_all("log").expect("Failed to create log directory");
    let log_file =
        File::create(format!("log/sage_worker_{}.log", pid)).expect("Failed to create log file");

    CombinedLogger::init(vec![WriteLogger::new(
        LevelFilter::Info,
        Config::default(),
        log_file,
    )])
    .expect("Failed to initialize logger");

    info!("Sage Worker starting (PID: {})", pid);

    let shutdown = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = mpsc::channel(100);

    // Attempt to start the consumer, exit if it fails
    let consumer_handle = match start_consumer(tx, shutdown.clone()) {
        Ok(handle) => handle,
        Err(e) => {
            error!("Failed to start consumer: {}", e);
            error!("Shutting down...");
            std::process::exit(1);
        }
    };

    tokio::task::spawn(async move {
        let producer = Arc::new(create_kafka_producer());

        while let Some(res) = rx.recv().await {
            // Determine if the task is CPU-intensive and needs spawn_blocking
            let is_cpu_intensive = matches!(res.task_name.as_str(), "PrimeTask");

            if is_cpu_intensive {
                // Spawn blocking for CPU-intensive tasks
                let producer_clone = producer.clone();
                tokio::task::spawn(async move {
                    let handle = tokio::task::spawn_blocking(move || {
                        // Use a new runtime to run the async task in blocking context
                        let runtime = tokio::runtime::Handle::current();
                        runtime.block_on(async move {
                            match get_context(&res) {
                                Ok(request) => {
                                    // Use the task_name from the message to run the appropriate task
                                    match run_task(&res.task_name, &request).await {
                                        Ok(response) => Ok((res, response)),
                                        Err(e) => Err((res, e)),
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to unpack context: {}", e);
                                    Err((res, e))
                                }
                            }
                        })
                    });

                    match handle.await {
                        Ok(Ok((message, response))) => {
                            info!(
                                "Task '{}' completed successfully (spawn_blocking)",
                                message.task_name
                            );
                            if let Err(e) =
                                submit_response(producer_clone, &message, response).await
                            {
                                error!("Failed to submit response: {}", e);
                            }
                        }
                        Ok(Err((message, error))) => {
                            error!("Task '{}' failed: {}", message.task_name, error);
                            if let Err(e) =
                                submit_error_response(producer_clone, &message, error).await
                            {
                                error!("Failed to submit error response: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("Blocking task panicked: {}", e);
                        }
                    }
                });
            } else {
                // Regular spawn for async tasks
                let producer_clone = producer.clone();
                let handle = tokio::task::spawn(async move {
                    match get_context(&res) {
                        Ok(request) => {
                            // Use the task_name from the message to run the appropriate task
                            match run_task(&res.task_name, &request).await {
                                Ok(response) => Ok((res, response)),
                                Err(e) => Err((res, e)),
                            }
                        }
                        Err(e) => {
                            error!("Failed to unpack context: {}", e);
                            Err((res, e))
                        }
                    }
                });

                match handle.await {
                    Ok(Ok((message, response))) => {
                        info!(
                            "Task '{}' completed successfully (spawn)",
                            message.task_name
                        );
                        if let Err(e) = submit_response(producer_clone, &message, response).await {
                            error!("Failed to submit response: {}", e);
                        }
                    }
                    Ok(Err((message, error))) => {
                        error!("Task '{}' failed: {}", message.task_name, error);
                        if let Err(e) = submit_error_response(producer_clone, &message, error).await
                        {
                            error!("Failed to submit error response: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Async task panicked: {}", e);
                    }
                }
            }
        }
    });

    // Run sample tasks once
    // Keep the application running to continue consuming messages
    info!("Worker running, press Ctrl+C to exit...");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");

    // Signal shutdown
    info!("Shutting down...");
    shutdown.store(true, Ordering::Relaxed);

    // Wait for consumer to finish
    let _ = consumer_handle.await;
    info!("Consumer stopped. Goodbye!");
}

fn start_consumer(
    tx: Sender<SageMessage>,
    shutdown: Arc<AtomicBool>,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error>> {
    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .expect("KAFKA_BOOTSTRAP_SERVERS must be set in environment or .env file");
    let group_id = std::env::var("KAFKA_WORKER_GROUP_ID")
        .expect("KAFKA_WORKER_GROUP_ID must be set in environment or .env file");
    let input_topic = std::env::var("KAFKA_TOPIC_INPUT")
        .expect("KAFKA_TOPIC_INPUT must be set in environment or .env file");

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(|e| format!("Failed to create consumer: {}", e))?;

    consumer
        .subscribe(&[&input_topic])
        .map_err(|e| format!("Failed to subscribe to topic: {}", e))?;

    info!("Kafka consumer started successfully, listening for messages...");

    let handle = tokio::task::spawn_blocking(move || {
        loop {
            // Check shutdown signal
            if shutdown.load(Ordering::Relaxed) {
                info!("Consumer received shutdown signal");
                break;
            }

            // Poll with timeout so we can check shutdown signal regularly
            match consumer.poll(std::time::Duration::from_millis(100)) {
                Some(Ok(msg)) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<SageMessage>(payload) {
                            Ok(sage_msg) => {
                                info!(
                                    "Received SageMessage: task_name='{}', task_envelope='{}'",
                                    sage_msg.task_name, sage_msg.task_envelope
                                );
                                // Use try_send to avoid blocking the Kafka consumer
                                if let Err(e) = tx.try_send(sage_msg) {
                                    error!("Failed to send message to channel: {:?}", e);
                                }
                            }
                            Err(e) => error!("Failed to deserialize SageMessage: {}", e),
                        }
                    }
                }
                Some(Err(e)) => {
                    error!("Error receiving message: {}", e);
                }
                None => {
                    // Timeout, continue to check shutdown signal
                }
            }
        }
    });

    Ok(handle)
}
