use celery::{TaskResult, task::Task, error::TaskError};
use dotenv::dotenv;
use std::env;

#[celery::task(
    on_failure = failure_callback,
    on_success = print_response_consumer,
)]
fn send_text(text: String) -> TaskResult<String> {
    println!("text_message: {}", text);
    let response = "Hello Provider".to_string();
    Ok(response)
}

async fn failure_callback<T: Task>(task: &T, err: &TaskError) {
    match err {
        TaskError::TimeoutError => println!("Oops! Task {} task file!", task.name()),
        _ => println!("Hmm task {} failed with {:?}", task.name(), err),
    };
}

async fn print_response_consumer<T:Task>(task: &T, ret: &T::Returns)
    where T::Returns: ToString {
    let seld_url = env::var("SLED_URL").expect("SLED_URL must be set");
    let tree = sled::open(&seld_url).unwrap();
    let text = ret.to_string();
    let id = task.request().id.clone();
    
    tree.insert(id, text.as_bytes()).unwrap();
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let my_app = celery::app!(
        broker = AMQP { std::env::var("AMPQ_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672".into())},
        tasks = [
            send_text,
        ],
        task_routes = [
            "send_text" => "text_queue",
        ]
    );

    my_app.consume_from(&["text_queue"]).await.unwrap();
}
