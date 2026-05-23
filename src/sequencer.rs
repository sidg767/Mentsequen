use anyhow::Result;
use tokio::sync::{RwLock, broadcast};

use crate::{block::Block, da::DALayer, mempool::Mempool, tx::Transaction, verify::verify_ed25519};

/// Shared global state of sequencer, this is stored in axum::extract::State<AppState> and shared across request handlers, background tasks, websocket connections, miners, and networking logic.
pub struct SequencerState {
    // This is the blockchain, stores blocks in order
    pub chain: RwLock<Vec<Block>>,
    // mempool stores pending transactions that are received from users but not yet included in a block
    pub mempool: Mempool,
    // for real-time pub/sub communication, broadcasts all new tx to subscribers.
    pub tx_broadcast: broadcast::Sender<Transaction>,
    pub dal: DALayer,
}

impl SequencerState {
    pub fn new(dal: DALayer, tx_sender: broadcast::Sender<Transaction>) -> Self {
        let genesis = Block::new(0, "0".repeat(64), vec![]);
        Self {
            chain: RwLock::new(vec![genesis]),
            mempool: Mempool::new(),
            tx_broadcast: tx_sender,
            dal,
        }
    }

    pub async fn submit_transaction(&self, tx: Transaction) -> Result<String> {
        self.verify_transaction(&tx)?;
        self.mempool.add_tx(tx.clone()).await;
        let _ = self.tx_broadcast.send(tx.clone());
        Ok(tx.id)
    }

    fn verify_transaction(&self, tx: &Transaction) -> Result<()> {
        if let (Some(pubkey), Some(signature)) = (&tx.pubkey, &tx.signature) {
            verify_ed25519(pubkey, signature, tx.data.as_bytes())
        } else {
            Ok(())
        }
    }

    pub async fn produce_block(&self, max_txs: usize) -> Result<Block> {
        let txs = self.mempool.drain(max_txs).await;
        let mut chain = self.chain.write().await;
        let prev_hash = chain.last().unwrap().hash.clone();
        let height = chain.len() as u64;
        let block = Block::new(height, prev_hash, txs);
        self.dal.persist_block(&block)?;
        chain.push(block.clone());
        Ok(block)
    }

    pub async fn head(&self) -> Option<Block> {
        self.chain.read().await.last().cloned()
    }

    pub async fn get_block(&self, height: usize) -> Option<Block> {
        self.chain.read().await.get(height).cloned()
    }
}
