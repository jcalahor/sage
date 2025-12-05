use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;

pub trait SageTaskRequest: Any + Send + Sync {}
pub trait SageTaskResponse: Any + Send + Sync {}

#[derive(Debug, Serialize, Deserialize)]
pub struct SageMessage {
    pub task_name: String,
    pub task_context: String,
}

#[async_trait]
pub trait SageTask<T: SageTaskRequest> {
    async fn run(
        &self,
        request: &T,
    ) -> Result<Box<dyn SageTaskResponse>, Box<dyn std::error::Error + Send>>;
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
