use async_trait::async_trait;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use task::{SageTask, TaskResponse};

#[derive(Debug, Serialize, Deserialize)]
pub struct PrimeTaskData {
    pub limit: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrimeTaskResponseData {
    pub prime_founds: u64,
}

pub struct SampleTask {}
pub struct PrimeTask {}

#[async_trait]
impl SageTask<PrimeTaskData, PrimeTaskResponseData> for SampleTask {
    async fn run(
        &self,
        request: &task::TaskRequest<PrimeTaskData>,
    ) -> Result<TaskResponse<PrimeTaskResponseData>, Box<dyn std::error::Error + Send>> {
        println!("Running task with request value: {}", request.data.limit);
        Ok(TaskResponse::new(
            request.id,
            PrimeTaskResponseData { prime_founds: 0 },
        ))
    }
}

fn is_prime(n: u64) -> bool {
    if n < 2 {
        return false;
    }
    if n == 2 {
        return true;
    }
    if n % 2 == 0 {
        return false;
    }

    let limit = (n as f64).sqrt() as u64;
    for i in (3..=limit).step_by(2) {
        if n % i == 0 {
            return false;
        }
    }
    true
}

#[async_trait]
impl SageTask<PrimeTaskData, PrimeTaskResponseData> for PrimeTask {
    async fn run(
        &self,
        request: &task::TaskRequest<PrimeTaskData>,
    ) -> Result<TaskResponse<PrimeTaskResponseData>, Box<dyn std::error::Error + Send>> {
        let start = Instant::now();
        let primes_par: Vec<u64> = (2..=request.data.limit)
            .into_par_iter()
            .filter(|&n| is_prime(n))
            .collect();
        let duration_par = start.elapsed();
        println!(
            "Rayon parallel: Found {} primes in {:?} for array of {:?} items",
            primes_par.len(),
            duration_par,
            request.data.limit
        );
        Ok(TaskResponse::new(
            request.id,
            PrimeTaskResponseData {
                prime_founds: primes_par.len() as u64,
            },
        ))
    }
}
