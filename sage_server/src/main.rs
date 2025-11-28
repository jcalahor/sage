
use task::{SageTask};
use tasks_impl::SampleTask;
use tasks_impl::SampleTask2;
use tasks_impl::SampleRequest;

async fn run_task(task_name: &str)
    -> Result<(), Box<dyn std::error::Error + Send>> {
    let request = SampleRequest {i: 20};
    
    match task_name {
        "SampleTask" => {
            let task = SampleTask{};
            task.run(&request).await?;
        },
        "SampleTask2" => {
            let task = SampleTask2{};
            task.run(&request).await?;
        },
        _ => {}
    };

    Ok(())
}


#[tokio::main]
async fn main() {
    
    match run_task("SampleTask").await {
        Ok(()) => {
            println!("Task completed successfully");
        },
        Err(error) => {
            eprintln!("Task failed: {}", error);
        }
    }
    
    match run_task("SampleTask2").await {
        Ok(()) => {
            println!("Task completed successfully");
        },
        Err(error) => {
            eprintln!("Task failed: {}", error);
        }
    }
}
