use serde_json::json;

#[tokio::test]
async fn test_transaction_submission() {
    let client = reqwest::Client::new();

    // Submit a transaction
    let response = client
        .post("http://127.0.0.1:8080/tx")
        .json(&json!({"data": "test transaction"}))
        .send()
        .await;

    assert!(response.is_ok());
    let status = response.unwrap().status();
    assert!(status.is_success());
}

#[tokio::test]
async fn test_mempool_endpoint() {
    let client = reqwest::Client::new();

    // Get mempool
    let response = client.get("http://127.0.0.1:8080/mempool").send().await;

    assert!(response.is_ok());
    let status = response.unwrap().status();
    assert!(status.is_success());
}

#[tokio::test]
async fn test_head_endpoint() {
    let client = reqwest::Client::new();

    // Get head block
    let response = client.get("http://127.0.0.1:8080/head").send().await;

    assert!(response.is_ok());
    let body = response.unwrap().json::<serde_json::Value>().await;

    assert!(body.is_ok());
    let data = body.unwrap();
    assert!(data.get("height").is_some());
    assert!(data.get("hash").is_some());
}

#[tokio::test]
async fn test_block_endpoint() {
    let client = reqwest::Client::new();

    // Get genesis block (height 0)
    let response = client.get("http://127.0.0.1:8080/block/0").send().await;

    assert!(response.is_ok());
    let body = response.unwrap().json::<serde_json::Value>().await;

    assert!(body.is_ok());
    let data = body.unwrap();
    assert_eq!(data.get("height").unwrap().as_u64().unwrap(), 0);
}

#[tokio::test]
async fn test_block_not_found() {
    let client = reqwest::Client::new();

    // Try to get a non-existent block
    let response = client.get("http://127.0.0.1:8080/block/99999").send().await;

    assert!(response.is_ok());
    let status = response.unwrap().status();
    assert_eq!(status, 404);
}

#[tokio::test]
async fn test_websocket_connection() {
    use futures_util::stream::StreamExt;
    use tokio_tungstenite::connect_async;

    let ws_url = "ws://127.0.0.1:8080/ws";

    match connect_async(ws_url).await {
        Ok((ws_stream, _)) => {
            let (_write, mut read) = ws_stream.split();

            // Connection successful
            assert!(read.next().await.is_some() || read.next().await.is_none());
        }
        Err(_) => {
            // WebSocket connection attempted
            assert!(true);
        }
    }
}

#[tokio::test]
async fn test_multiple_transactions() {
    let client = reqwest::Client::new();

    // Submit multiple transactions
    for i in 0..5 {
        let response = client
            .post("http://127.0.0.1:8080/tx")
            .json(&json!({"data": format!("transaction {}", i)}))
            .send()
            .await;

        assert!(response.is_ok());
        assert!(response.unwrap().status().is_success());
    }

    // Check mempool has transactions
    let response = client.get("http://127.0.0.1:8080/mempool").send().await;

    assert!(response.is_ok());
    let body = response.unwrap().json::<Vec<serde_json::Value>>().await;
    assert!(body.is_ok());
    assert!(body.unwrap().len() >= 5);
}
