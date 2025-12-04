use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
};
use std::any::Any;
use std::error::Error;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use task::{SageMessage, SageTask, SageTaskRequest};
use tasks_impl::PrimeTask;
use tasks_impl::PrimeTaskRequest;
use tasks_impl::SampleTask;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

async fn run_task(
    task_name: &str,
    request: &dyn SageTaskRequest,
) -> Result<(), Box<dyn std::error::Error + Send>> {
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
            task.run(concrete_request).await?;
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
            task.run(concrete_request).await?;
        }
        _ => {}
    };

    Ok(())
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
        while let Some(res) = rx.recv().await {
            // Spawn a separate task for each message to allow concurrent processing
            tokio::task::spawn(async move {
                match get_context(&res) {
                    Ok(request) => {
                        // Use the task_name from the message to run the appropriate task
                        match run_task(&res.task_name, request.as_ref()).await {
                            Ok(()) => {
                                println!("Task '{}' completed successfully", res.task_name);
                            }
                            Err(error) => {
                                eprintln!("Task '{}' failed: {}", res.task_name, error);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to unpack context: {}", e);
                    }
                }
            });
        }
    });

    // Run sample tasks once
    let request: PrimeTaskRequest = PrimeTaskRequest { limit: 20 };

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
