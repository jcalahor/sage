use rdkafka::producer::FutureProducer;
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<FutureProducer>,
    pub db_pool: Arc<PgPool>,
}
