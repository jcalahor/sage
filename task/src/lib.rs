use async_trait::async_trait;

pub trait SageTaskRequest {}

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
