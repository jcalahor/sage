use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;

pub trait SageTaskRequest: Any + Send + Sync {}

#[derive(Debug, Serialize, Deserialize)]
pub struct SageMessage {
    pub task_name: String,
    pub task_context: String,
}

#[async_trait]
pub trait SageTask<T: SageTaskRequest> {
    async fn run(&self, request: &T) -> Result<(), Box<dyn std::error::Error + Send>>;
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
