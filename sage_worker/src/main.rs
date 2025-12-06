use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
};
use std::any::Any;
use std::error::Error;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use task::{SageMessage, SageTask, SageTaskRequest, SageTaskResponse};
use tasks_impl::PrimeTask;
use tasks_impl::PrimeTaskRequest;
use tasks_impl::PrimeTaskResponse;
use tasks_impl::SampleTask;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

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

async fn run_task(
    task_name: &str,
    request: &dyn SageTaskRequest,
) -> Result<Box<dyn SageTaskResponse>, Box<dyn std::error::Error + Send>> {
    // Downcast the trait object to concrete type

    match task_name {
        "SampleTask" => {
            let concrete_request = (request as &dyn Any)
                .downcast_ref::<PrimeTaskRequest>()
                .ok_or_else(|| -> Box<dyn std::error::Error + Send> {
                    Box::new(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Failed to downcast request",
                    ))
                })?;
            let task = SampleTask {};
            let response: Box<dyn SageTaskResponse> = task.run(concrete_request).await?;
            return Ok(response);
        }
        "PrimeTask" => {
            let concrete_request = (request as &dyn Any)
                .downcast_ref::<PrimeTaskRequest>()
                .ok_or_else(|| -> Box<dyn std::error::Error + Send> {
                    Box::new(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Failed to downcast request",
                    ))
                })?;
            let task = PrimeTask {};
            let response: Box<dyn SageTaskResponse> = task.run(concrete_request).await?;
            return Ok(response);
        }
        _ => {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::NotFound,
                "Task not found",
            )));
        }
    };
}

fn get_context(
    message: &SageMessage,
) -> Result<Box<dyn SageTaskRequest>, Box<dyn std::error::Error + Send>> {
    match message.task_name.as_str() {
        "SampleTask" => {
            let request: PrimeTaskRequest = serde_json::from_str(&message.task_context)
                .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;
            Ok(Box::new(request))
        }
        "PrimeTask" => {
            let request: PrimeTaskRequest = serde_json::from_str(&message.task_context)
                .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;
            Ok(Box::new(request))
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
    response: Box<dyn SageTaskResponse>,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let payload: String = match message.task_name.as_str() {
        "SampleTask" => {
            let concrete_response = (response.as_ref() as &dyn Any)
                .downcast_ref::<PrimeTaskResponse>()
                .ok_or_else(|| -> Box<dyn std::error::Error + Send> {
                    Box::new(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Failed to downcast response",
                    ))
                })?;
            println!("SampleTask Response: {:?}", concrete_response);
            serde_json::to_string(concrete_response)
                .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?
        }
        "PrimeTask" => {
            let concrete_response = (response.as_ref() as &dyn Any)
                .downcast_ref::<PrimeTaskResponse>()
                .ok_or_else(|| -> Box<dyn std::error::Error + Send> {
                    Box::new(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Failed to downcast response",
                    ))
                })?;
            println!("PrimeTask Response: {:?}", concrete_response);
            serde_json::to_string(concrete_response)
                .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?
        }
        _ => {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unknown task name: {}", message.task_name),
            )));
        }
    };

    let record = FutureRecord::to("responses")
        .payload(&payload)
        .key(&message.task_name);

    match producer.send(record, 0).await {
        Ok(Ok(_)) => {
            println!(
                "Response sent successfully to topic 'responses': {}",
                &payload
            );
            Ok(())
        }
        Ok(Err((e, _))) => {
            eprintln!("Failed to send response: {:?}", e);
            Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to send to Kafka: {:?}", e),
            )))
        }
        Err(e) => {
            eprintln!("Kafka send future canceled: {:?}", e);
            Err(Box::new(io::Error::new(
                io::ErrorKind::Other,
                format!("Kafka send canceled: {:?}", e),
            )))
        }
    }
}

#[tokio::main]
async fn main() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = mpsc::channel(100);

    // Attempt to start the consumer, exit if it fails
    let consumer_handle = match start_consumer(tx, shutdown.clone()) {
        Ok(handle) => handle,
        Err(e) => {
            eprintln!("Failed to start consumer: {}", e);
            eprintln!("Shutting down...");
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
                                    match run_task(&res.task_name, request.as_ref()).await {
                                        Ok(response) => Ok((res, response)),
                                        Err(e) => Err((res, e)),
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to unpack context: {}", e);
                                    Err((res, e))
                                }
                            }
                        })
                    });

                    match handle.await {
                        Ok(Ok((message, response))) => {
                            println!(
                                "Task '{}' completed successfully (spawn_blocking)",
                                message.task_name
                            );
                            if let Err(e) =
                                submit_response(producer_clone, &message, response).await
                            {
                                eprintln!("Failed to submit response: {}", e);
                            }
                        }
                        Ok(Err((message, error))) => {
                            eprintln!("Task '{}' failed: {}", message.task_name, error);
                        }
                        Err(e) => {
                            eprintln!("Blocking task panicked: {}", e);
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
                            match run_task(&res.task_name, request.as_ref()).await {
                                Ok(response) => Ok((res, response)),
                                Err(e) => Err((res, e)),
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to unpack context: {}", e);
                            Err((res, e))
                        }
                    }
                });

                match handle.await {
                    Ok(Ok((message, response))) => {
                        println!(
                            "Task '{}' completed successfully (spawn)",
                            message.task_name
                        );
                        if let Err(e) = submit_response(producer_clone, &message, response).await {
                            eprintln!("Failed to submit response: {}", e);
                        }
                    }
                    Ok(Err((message, error))) => {
                        eprintln!("Task '{}' failed: {}", message.task_name, error);
                    }
                    Err(e) => {
                        eprintln!("Async task panicked: {}", e);
                    }
                }
            }
        }
    });

    // Run sample tasks once
    // Keep the application running to continue consuming messages
    println!("Worker running, press Ctrl+C to exit...");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");

    // Signal shutdown
    println!("Shutting down...");
    shutdown.store(true, Ordering::Relaxed);

    // Wait for consumer to finish
    let _ = consumer_handle.await;
    println!("Consumer stopped. Goodbye!");
}

fn start_consumer(
    tx: Sender<SageMessage>,
    shutdown: Arc<AtomicBool>,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "andean-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(|e| format!("Failed to create consumer: {}", e))?;

    consumer
        .subscribe(&["input-readings"])
        .map_err(|e| format!("Failed to subscribe to topic: {}", e))?;

    println!("Kafka consumer started successfully, listening for messages...");

    let handle = tokio::task::spawn_blocking(move || {
        loop {
            // Check shutdown signal
            if shutdown.load(Ordering::Relaxed) {
                println!("Consumer received shutdown signal");
                break;
            }

            // Poll with timeout so we can check shutdown signal regularly
            match consumer.poll(std::time::Duration::from_millis(100)) {
                Some(Ok(msg)) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<SageMessage>(payload) {
                            Ok(sage_msg) => {
                                println!(
                                    "Received SageMessage: task_name='{}', task_context='{}'",
                                    sage_msg.task_name, sage_msg.task_context
                                );
                                // Use try_send to avoid blocking the Kafka consumer
                                if let Err(e) = tx.try_send(sage_msg) {
                                    eprintln!("Failed to send message to channel: {:?}", e);
                                }
                            }
                            Err(e) => eprintln!("Failed to deserialize SageMessage: {}", e),
                        }
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Error receiving message: {}", e);
                }
                None => {
                    // Timeout, continue to check shutdown signal
                }
            }
        }
    });

    Ok(handle)
}
