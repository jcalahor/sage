use task::{SageTask, SageTaskRequest};
use async_trait::async_trait;

pub struct SampleRequest {
    pub i: i64
}
impl SageTaskRequest for SampleRequest {}

pub struct SampleTask {}
pub struct SampleTask2 {}

#[async_trait]
impl SageTask<SampleRequest> for SampleTask {
    async fn run(&self, request: &SampleRequest) -> Result<(), Box<dyn std::error::Error + Send>> {
        println!("Running task with request value: {}", request.i);
        Ok(())
    }
}

#[async_trait]
impl SageTask<SampleRequest> for SampleTask2 {
    async fn run(&self, request: &SampleRequest) -> Result<(), Box<dyn std::error::Error + Send>> {
        println!("Running task2 with request value: {}", request.i);
        Ok(())
    }
}
