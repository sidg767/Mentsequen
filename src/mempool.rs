use parking_lot::Mutex;
use once_cell::sync::Lazy;
use crate::tx::Transaction;

pub static MEMPOOL: Lazy<Mutex<Vec<Transaction>>> = Lazy::new(|| Mutex::new(Vec::new()));

pub fn add_tx(tx: Transaction) {
    MEMPOOL.lock().push(tx);
}

pub fn drain_mempool(count: usize) -> Vec<Transaction> {
    let mut guard = MEMPOOL.lock();
    let take = count.min(guard.len());
    guard.drain(0..take).collect()
}

pub fn list_mempool() -> Vec<Transaction> {
    MEMPOOL.lock().clone()
}

pub fn len() -> usize {
    MEMPOOL.lock().len()
}

