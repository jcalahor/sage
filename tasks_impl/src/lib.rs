use async_trait::async_trait;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use task::{SageTask, SageTaskRequest, SageTaskResponse};

#[derive(Debug, Serialize, Deserialize)]
pub struct PrimeTaskRequest {
    pub limit: u64,
}
impl SageTaskRequest for PrimeTaskRequest {}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrimeTaskResponse {
    pub prime_founds: u64,
}
impl SageTaskResponse for PrimeTaskResponse {}

pub struct SampleTask {}
pub struct PrimeTask {}

#[async_trait]
impl SageTask<PrimeTaskRequest> for SampleTask {
    async fn run(
        &self,
        request: &PrimeTaskRequest,
    ) -> Result<Box<dyn SageTaskResponse>, Box<dyn std::error::Error + Send>> {
        println!("Running task with request value: {}", request.limit);
        Ok(Box::new(PrimeTaskResponse { prime_founds: 0 }))
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
impl SageTask<PrimeTaskRequest> for PrimeTask {
    async fn run(
        &self,
        request: &PrimeTaskRequest,
    ) -> Result<Box<dyn SageTaskResponse>, Box<dyn std::error::Error + Send>> {
        let start = Instant::now();
        let primes_par: Vec<u64> = (2..=request.limit)
            .into_par_iter()
            .filter(|&n| is_prime(n))
            .collect();
        let duration_par = start.elapsed();
        println!(
            "Rayon parallel: Found {} primes in {:?} for array of {:?} items",
            primes_par.len(),
            duration_par,
            request.limit
        );
        Ok(Box::new(PrimeTaskResponse {
            prime_founds: primes_par.len() as u64,
        }))
    }
}
