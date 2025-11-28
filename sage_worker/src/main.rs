use rdkafka::{
    ClientConfig, Message,
    consumer::{BaseConsumer, Consumer},
};
use std::any::Any;
use std::error::Error;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use task::{SageTask, SageTaskRequest};
use tasks_impl::SampleRequest;
use tasks_impl::SampleTask;
use tasks_impl::SampleTask2;

async fn run_task(
    task_name: &str,
    request: &dyn SageTaskRequest,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    // Downcast the trait object to concrete type
    let concrete_request = (request as &dyn Any)
        .downcast_ref::<SampleRequest>()
        .ok_or_else(|| -> Box<dyn std::error::Error + Send> {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Failed to downcast request",
            ))
        })?;

    match task_name {
        "SampleTask" => {
            let task = SampleTask {};
            task.run(concrete_request).await?;
        }
        "SampleTask2" => {
            let task = SampleTask2 {};
            task.run(concrete_request).await?;
        }
        _ => {}
    };

    Ok(())
}

#[tokio::main]
async fn main() {
    let shutdown = Arc::new(AtomicBool::new(false));

    // Attempt to start the consumer, exit if it fails
    let consumer_handle = match start_consumer(shutdown.clone()) {
        Ok(handle) => handle,
        Err(e) => {
            eprintln!("Failed to start consumer: {}", e);
            eprintln!("Shutting down...");
            std::process::exit(1);
        }
    };

    // Run sample tasks once
    let request: SampleRequest = SampleRequest { i: 20 };

    match run_task("SampleTask", &request).await {
        Ok(()) => {
            println!("Task completed successfully");
        }
        Err(error) => {
            eprintln!("Task failed: {}", error);
        }
    }

    match run_task("SampleTask2", &request).await {
        Ok(()) => {
            println!("Task completed successfully");
        }
        Err(error) => {
            eprintln!("Task failed: {}", error);
        }
    }

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
                        match std::str::from_utf8(payload) {
                            Ok(text) => println!("Received: {}", text),
                            Err(e) => eprintln!("Failed to decode message payload: {}", e),
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
