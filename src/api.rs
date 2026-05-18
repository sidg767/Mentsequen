use axum::{
    extract::{Extension, Path, ws::{Message, WebSocket, WebSocketUpgrade}},
    http::StatusCode,
  response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::{sequencer::SequencerState, tx::Transaction};

#[derive(Deserialize)]
struct NewTx {
    data: String,
}

pub fn router(state: Arc<SequencerState>) -> Router {
    Router::new()
        .route("/tx", post(handle_tx))
       .route("/mempool", get(handle_mempool))
        .route("/block/:height", get(handle_block))
        .route("/head", get(handle_head))
        .route("/ws", get(ws_handler))
       .layer(Extension(state))
}
async fn handle_tx(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<NewTx>,
) -> impl IntoResponse {
    let data = payload.data.trim();
    // less than 4096 for DoS  protection
    if data.is_empty() || data.len() > 4096 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid data"})),
        );
    }
    // sequencers hv deterministic ids, simple version here
    let tx = Tx {
        id: Uuid::new_v4().to_string(),
        data: data.to_string(),
    };
    // requesting write access to shared mempool, scope is needed cuz Lock is released right after insertion. Holding locks
    // too long causes contention, latency, throughput collapse. Real sequencers carefully minimize lock duration.
    {
        let mut guard = state.mempool.write().await;
        guard.push(tx.clone());
    }

    //publish tx to all subscribers. let _ means ignore possible errors
    let _ = state.tx_broadcast.send(tx);

    //At this point a new mempool transaction has been created, stored in the mempool, and broadcast to all websocket
    // clients. The API response is a simple JSON object indicating success. Real sequencers may return more info,
    //like the tx id, or a receipt with execution result.
    (
        StatusCode::CREATED,
        Json(serde_json::json!({"status": "ok"})),
    )
}

// returns all pending transactions in the mempool. Extension(state) gives access to shared AppState, state.mempool.read()
//acquires a read lock on the mempool, allowing concurrent reads but blocking writes. The handler clones the mempool
// transactions and returns them as a JSON response. Real sequencers may paginate this response or return only transaction
// summaries to avoid large payloads.
async fn handle_mempool(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    let guard = state.mempool.read().await;
    Json(guard.clone())
}
// get block by height, block 0 gets genesis, block 1 gets 1st block after genesis, etc.
async fn handle_block(
    Path(height): Path<u64>,
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    let guard = state.chain.read().await;

    let index = match usize::try_from(height) {
        Ok(index) => index,
        Err(_) => {
            //into.response converts the tuple into an HTTP response, with status code and JSON body. This handles the case
            //where the block height is too large to fit in a usize, which would cause an error when trying to access the
            //chain vector. In that case, we return a 400 Bad Request with an error message.
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

//returns latest block in the chain
async fn handle_head(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    let guard = state.chain.read().await;
    // last returns last element of vec, the latest block
    if let Some(b) = guard.last() {
        Json(b.clone()).into_response()
    }
    // head null never happens, but still it's good to have
    else {
        (StatusCode::OK, Json(serde_json::json!({"head": null}))).into_response()
    }
}

// sequencer has 2 networking models, rest api(pull-based, get me mempool, block etc), websocket api(pushing tx to clients),
// this upgrades http to ws, blockchain data changes continuously, polling is ineffecient
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
        //Wait simultaneously on multiple async operations, basically wait for whichever happens first.
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
            //incoming websocket frames trigger this branch
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