mod server;

#[tokio::main]
async fn main() {
    let address = format!("{}:{}", "0.0.0.0", 8080);
    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
    println!("Server started at {}", &address);
    axum::serve(listener, server::build_server().await.into_make_service())
        .await
        .unwrap();
}
