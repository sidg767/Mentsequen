use mentsequen::{api, da::DALayer, sequencer::SequencerState};
///Logic
/// A sequencer is a network service, it must: listen for transactions, expose APIs,
/// accept websocket connections, communicate with validators/nodes.
/// It needs Arc(shared ownership, for heap data, across threads, immutable by default but mut with mutex/rwlock)
/// Sequencers need arc cuz highly concurrent, websocket handlers, mempool updates, block prod, state readers,
/// rpc handlers all need access to shared state. Axum is the HTTP/websocket framework, router defines api routes,
/// needed cuz sequencer exposes endpoints for submitting transacs, etc.
/// Json extractor : for incoming requests reads: HTTP body, Content-Type: application/json
/// Then uses Serde internally to deserialize into T. Outgoing response: Converts Rust data into JSON HTTP response.
/// System has State Layer: {mempool, chain} Event Layer: {broadcast channel}, Network Layer: HTTP + WebSocket.
/// Currently websocket broadcasts only new txs, Real sequencers also broadcast: new blocks, finalized blocks, reorgs
/// state updates, receipts, logs/events. Json: an HTTP extractor/response wrapper built on top of Serde,
/// SocketAddr gives Ip + Port, extension injects shared application state into handlers, path extracts variables from url,
/// ws handles websocket support and upgarde  from HTTP to Websocket
/// HTTP status codes needed for api responses, like statuscode:::ok, bad_request, not_found, etc.        
/// //broadcast is a publish-subscribe channel, many clients subscribe at once, then all recieve the updates, needed for websocket notifications of new transactions
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;
#[tokio::main]
///main initializes: async runtime, networking, blockchain state, mempool, websocket broadcasting
///HTTP API, shared concurrent state, the genesis block, the Axum server. Basically, node startup
/// + networking layer + state manager
/// Send + sync means the error can be sent across threads and shared across threads, needed for
/// async fn main which may spawn tasks that return errors
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // broadcast tx to: websocket clients, indexers, explorers, monitoring systems. capacity=32  means channel stores only 32
    // recent msgs, if receivers lag behind they may miss old msgs.
    let (tx_sender, _) = broadcast::channel(32);
    let dal = DALayer::new("./data");
    /*
    State is the shared global state of the sequencer, it includes the mempool, the blockchain, and the tx broadcast
    channel. need arc cuz at the same time many processes need to see state. Mempool stores txs b4 they enter blocks,
    chain stores the blocks, tx_broadcast is for real-time pub/sub of new transactions to websocket clients.
    */
    let state = Arc::new(SequencerState::new(dal, tx_sender));
    let app = api::router(state);
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
