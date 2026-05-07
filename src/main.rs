use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{
        Extension,
        Path,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tx {
    id: String,
    data: String,
}

#[derive(Debug, Deserialize)]
struct NewTx {
    data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Block {
    height: u64,
    hash: String,
    prev_hash: String,
    txs: Vec<Tx>,
}

#[derive(Debug)]
struct AppState {
    mempool: RwLock<Vec<Tx>>,
    chain: RwLock<Vec<Block>>,
    tx_broadcast: broadcast::Sender<Tx>,
}

impl Block {
    fn new(height: u64, prev_hash: String, txs: Vec<Tx>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(height.to_be_bytes());
        hasher.update(prev_hash.as_bytes());
        for tx in &txs {
            hasher.update(tx.id.as_bytes());
            hasher.update(tx.data.as_bytes());
        }
        let hash = hex::encode(hasher.finalize());

        Block {
            height,
            hash,
            prev_hash,
            txs,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (tx_sender, _) = broadcast::channel(32);

    let genesis = Block::new(0, "0".repeat(64), vec![]);

    let state = Arc::new(AppState {
        mempool: RwLock::new(Vec::new()),
        chain: RwLock::new(vec![genesis]),
        tx_broadcast: tx_sender,
    });

    let app = Router::new()
        .route("/tx", post(handle_tx))
        .route("/mempool", get(handle_mempool))
        .route("/block/:height", get(handle_block))
        .route("/head", get(handle_head))
        .route("/ws", get(ws_handler))
        .layer(Extension(state));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("🚀 Axum sequencer running at http://127.0.0.1:8080");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn handle_tx(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<NewTx>,
) -> impl IntoResponse {
    let data = payload.data.trim();
    if data.is_empty() || data.len() > 4096 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid data"})),
        );
    }

    let tx = Tx {
        id: Uuid::new_v4().to_string(),
        data: data.to_string(),
    };

    {
        let mut guard = state.mempool.write().await;
        guard.push(tx.clone());
    }

    let _ = state.tx_broadcast.send(tx);

    (StatusCode::CREATED, Json(serde_json::json!({"status": "ok"})))
}

async fn handle_mempool(
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    let guard = state.mempool.read().await;
    Json(guard.clone())
}

async fn handle_block(
    Path(height): Path<u64>,
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    let guard = state.chain.read().await;

    let index = match usize::try_from(height) {
        Ok(index) => index,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid block height"})),
            )
                .into_response();
        }
    };

    if let Some(b) = guard.get(index) {
        Json(b.clone()).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"not found"})),
        )
            .into_response()
    }
}

async fn handle_head(
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    let guard = state.chain.read().await;

    if let Some(b) = guard.last() {
        Json(b.clone()).into_response()
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({"head": null})),
        )
            .into_response()
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        ws_connection(socket, state).await;
    })
}

async fn ws_connection(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.tx_broadcast.subscribe();

    loop {
        tokio::select! {
            Ok(tx) = rx.recv() => {
                if let Ok(msg) = serde_json::to_string(&tx) {
                    if socket.send(Message::Text(msg)).await.is_err() {
                        break;
                    }
                } else {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if serde_json::from_str::<serde_json::Value>(&text).is_err() {
                            let _ = socket.send(Message::Close(None)).await;
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        let _ = socket.send(Message::Close(None)).await;
                        break;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let _ = socket.send(Message::Close(frame)).await;
                        break;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Err(_)) | None => break,
                }
            }
        }
    }
}
