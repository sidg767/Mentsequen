///SocketAddr gives Ip + Port
use axum::{
    Json,
    Router, // Json: an HTTP extractor/response wrapper built on top of Serde
    extract::{
        //extension injects shared application state into handlers, path extracts variables from url,
        //ws handles websocket support and upgarde  from HTTP to Websocket
        Extension,
        Path,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode, //returns HTTP status codes, needed for api responses, like statuscode:::ok, bad_request, not_found, etc.
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize}; //rust structs to json, yaml, toml, binary and vice versa, needed for api payloads, responses, websocket messages, etc.
use sha2::{Digest, Sha256}; //digest is the trait proving the hashing fn
///Logic
/// A sequencer is a network service, it must: listen for transactions, expose APIs,
/// accept websocket connections, communicate with validators/nodes.
/// It needs Arc(shared ownership, for heap data, across threads, immutable by default but mut with mutex/rwlock)
/// Sequencers need arc cuz highly concurrent, websocket handlers, mempool updates, block prod, stae readers,
/// rpc handlers all need access to shared state. Axum is the HTTP/websocket framework, router defines api routes,
/// needed cuz sequencer exposes endpoints for submitting transacs, etc.
/// Json extractor : for incoming requests reads: HTTP body, Content-Type: application/json
/// Then uses Serde internally to deserialize into T. Outgoing response: Converts Rust data into JSON HTTP response.
/// System has State Layer: {mempool, chain} Event Layer: {broadcast channel}, Network Layer: HTTP + WebSocket.
/// Currently websocket broadcasts only new txs, Real sequencers also broadcast: new blocks, finalized blocks, reorgs
/// state updates, receipts, logs/events
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{RwLock, broadcast}; //broadcast is a publish-subscribe channel, many clients subscribe at once, then all recieve the updates, needed for websocket notifications of new transactions
use uuid::Uuid; //generates unique request ids, session ids, transaction ids, etc. 

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
/// Shared global state of sequencer, this is stored in axum::extract::State<AppState> and shared across request handlers, background tasks, websocket connections, miners, and networking logic.
#[derive(Debug)]
struct AppState {
    // mempool stores pending transactions that are received from users but not yet included in a block
    mempool: RwLock<Vec<Tx>>,
    // This is the blockchain, stores blocks in order
    chain: RwLock<Vec<Block>>,
    // for real-time pub/sub communication, broadcasts all new tx to subscribers.
    tx_broadcast: broadcast::Sender<Tx>,
}
/// impl- define methods associated with Block
impl Block {
    // Constructor creates a new block, computes its hash based on height(block no.), prev_hash, and
    //transactions. Real blockchain sequencers also hash: timestamp, proposer/sequencer address,
    //state root, transaction Merkle root, receipts root, gas usage, signature, nonce. This implementation
    // is NOT canonical-safe for production because concatenation can collide logically. Eg tx1.id = "ab"
    //tx1.data = "cd", tx1.id = "abc" tx1.data = "d" both give "abcd" as input, real sequencer would also
    //include a block header struct with  height: u64, prev_hash: Hash, state_root: Hash, tx_root: Hash,
    //timestamp: u64, sequencer: Address, then hash(header), instead of hashing raw txs directly.
    //Sequential hashing gives integrity of entire block, but not efficient membership proofs.
    //Modern blockchains need proofs, so they use merkle trees to hash transactions, then include the
    //merkle root in the block header, so you can verify a tx is in a block with a short proof.
    //Sequential takes O(n) to verify a tx is in a block, merkle takes O(log n).
    fn new(height: u64, prev_hash: String, txs: Vec<Tx>) -> Self {
        // Creates a Sha256  hashing object, Sha256 is incremental, we can feed bytes piece by piece,
        //then call finalize(consumes hasher) to get the final hash. We feed height, prev_hash, and all txs into the hasher to compute the block hash.
        let mut hasher = Sha256::new();
        // as bytes cuz hash fns need bytes, order of tx also changes hash
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
///main initializes: async runtime, networking, blockchain state, mempool, websocket broadcasting
///HTTP API, shared concurrent state, the genesis block, the Axum server. Basically, node startup
/// + networking layer + state manager
/// Send + sync means the error can be sent across threads and shared across threads, needed for
/// async fn main which may spawn tasks that return errors
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // broadcast tx to: websocket clients, indexers, explorers, monitoring systems. capacity=32  means channel stores only 32
    // recent msgs, if receivers lag behind they may miss old msgs.
    let (tx_sender, _) = broadcast::channel(32);
    // Genesis block has no parent(height 0, prev_hash all zeros), and no transactions. Real sequencers may have a more
    //complex genesis block with initial state, pre-allocated accounts, etc.
    let genesis = Block::new(0, "0".repeat(64), vec![]);

    /*
    AppState is the shared global state of the sequencer, it includes the mempool, the blockchain, and the tx broadcast
    channel. need arc cuz at the same time many processes need to see state. Mempool stores txs b4 they enter blocks,
    chain stores the blocks, tx_broadcast is for real-time pub/sub of new transactions to websocket clients.
    */
    let state = Arc::new(AppState {
        mempool: RwLock::new(Vec::new()),
        chain: RwLock::new(vec![genesis]),
        tx_broadcast: tx_sender,
    });

    /*Creates Axum router,this maps HTTP paths to handlers. Tx submits transactions, mempool returns pending transactions,
     block returns a block by height, head returns latest block, ws upgrades to websocket connection. Extension(state)
     injects the shared AppState into each handler so they can access mempool, chain, and tx_broadcast. it's the middle layer
     that provides shared state to handlers. Each route specifies the HTTP method (get, post) and the handler function that
     processes requests to that route. */
    let app = Router::new()
        .route("/tx", post(handle_tx))
        .route("/mempool", get(handle_mempool))
        .route("/block/:height", get(handle_block))
        .route("/head", get(handle_head))
        .route("/ws", get(ws_handler))
        .layer(Extension(state));

    /*Bind the Axum server to localhost:8080 and start listening for incoming HTTP requests. The server will run
     indefinitely until it is stopped. Each incoming request will be routed to the appropriate handler based on the
    defined routes. The handlers will have access to the shared AppState through the Extension layer, allowing them to
      read/write the mempool, chain, and broadcast new transactions to websocket clients. */
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("Axum sequencer running at http://127.0.0.1:8080");

    /*bind creates tcp listener on the specified address, serve turns the router into HTTP service and starts accepting
     incoming connections. After startup: Tokio Runtime, TCP Listener, Accept Connections, Route Requests, Async Handlers,
     Shared Blockchain State */
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

/* client sends data, seq verifies the tx, assigns an id, adds it to mempool, broadcasts tx event, return success.
 Extension extracts shared application state from Axum( that was entered in main), Json(payload): Json<NewTx>, here Axum
  automatically reads request body, parses JSON n deserializes into Rust struct */
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
