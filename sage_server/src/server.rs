use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use task::SageMessage;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StartResponse {
    status: bool,
    id: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StartRequest {
    requestor_id: i64,
    task_name: String,
    task_context: String,
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

async fn start(
    State(producer): State<Arc<FutureProducer>>,
    Json(payload): Json<StartRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    let task_id = Uuid::new_v4();
    let key = payload.requestor_id.to_string();

    let sage_message = SageMessage {
        task_id,
        task_name: payload.task_name,
        task_context: payload.task_context,
    };

    let payload_string =
        serde_json::to_string(&sage_message).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let record = FutureRecord::to("input-readings") // Kafka topic
        .key(&key)
        .payload(&payload_string);

    match producer
        .send(
            record, 0, // Optional timeout
        )
        .await
    {
        Ok(_) => {
            println!(
                "Message |{}| sent successfully to topic '{}'",
                &payload_string, "input-readings"
            );
        }
        Err(e) => {
            println!("Failed to send message: {}", e);
        }
    }

    let response: StartResponse = StartResponse {
        status: true,
        id: task_id,
    };

    Ok(Json(response))
}

pub fn create_routes() -> Router<Arc<FutureProducer>> {
    Router::new().route("/tasks/v1/start", post(start))
}

pub async fn build_server() -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let producer: Arc<FutureProducer> = Arc::new(create_kafka_producer());
    Router::new()
        .merge(create_routes())
        .layer(cors)
        .with_state(producer)
}
