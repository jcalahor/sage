mod db;
mod server;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{
    Message as RdMesssage,
    consumer::{BaseConsumer, Consumer},
};
use std::sync::Arc;
use tokio::sync::broadcast;

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

async fn process_responses(consumer: BaseConsumer, mut shutdown_rx: broadcast::Receiver<()>) {
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
                    if let Ok(buff) = std::str::from_utf8(payload) {
                        println!(
                            "Kafka message received: {:?} at offset {:?} from partition {}",
                            buff,
                            msg.offset(),
                            msg.partition()
                        );

                        let msg: String = buff.to_string();
                        println!("{}", msg);
                    } else {
                        println!("Failed to parse Kafka message payload");
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
    println!("Server started at {}", &address);
    let producer: Arc<FutureProducer> = Arc::new(create_kafka_producer());
    let consumer = create_kafka_consumer();
    consumer
        .subscribe(&["responses"])
        .expect("topic subscribe failed");

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let consumer_handle = tokio::spawn(process_responses(consumer, shutdown_rx));

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

    // Signal shutdown to consumer
    let _ = shutdown_tx.send(());

    // Wait for consumer task to finish
    if let Err(e) = consumer_handle.await {
        eprintln!("Consumer task error: {}", e);
    }

    println!("Shutdown complete");
}
