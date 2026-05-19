use crate::tx::Transaction;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Mempool {
    inner: Arc<RwLock<Vec<Transaction>>>,
}

impl Mempool {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add_tx(&self, tx: Transaction) {
        let mut guard = self.inner.write().await;
        guard.push(tx);
    }

    pub async fn drain(&self, count: usize) -> Vec<Transaction> {
        let mut guard = self.inner.write().await;
        let take = count.min(guard.len());
        guard.drain(0..take).collect()
    }

    pub async fn list(&self) -> Vec<Transaction> {
        self.inner.read().await.clone()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }
}
