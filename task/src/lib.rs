use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Generic wrapper that automatically provides an id field for any request type
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskRequest<T> {
    pub id: Uuid,
    #[serde(flatten)]
    pub data: T,
}

impl<T> TaskRequest<T> {
    pub fn new(data: T) -> Self {
        Self {
            id: Uuid::new_v4(),
            data,
        }
    }

    pub fn with_id(id: Uuid, data: T) -> Self {
        Self { id, data }
    }
}

/// Generic wrapper that automatically provides an id field for any response type
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskResponse<T> {
    pub id: Uuid,
    #[serde(flatten)]
    pub data: T,
}

impl<T> TaskResponse<T> {
    pub fn new(request_id: Uuid, data: T) -> Self {
        Self {
            id: request_id,
            data,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SageMessage {
    pub task_name: String,
    pub task_context: String,
}

#[async_trait]
pub trait SageTask<T, R> {
    async fn run(
        &self,
        request: &TaskRequest<T>,
    ) -> Result<TaskResponse<R>, Box<dyn std::error::Error + Send>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        //let result = add(2, 2);
        //sassert_eq!(result, 4);
    }
}
