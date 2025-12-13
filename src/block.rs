use serde::{Serialize, Deserialize};
use sha2::{Digest, Sha256};
use crate::tx::Transaction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub height: u64,
    pub prev_hash: String,
    pub txs: Vec<Transaction>,
    pub merkle_root: String,
    pub hash: String,
}

impl Block {
    pub fn new(height: u64, prev_hash: String, txs: Vec<Transaction>) -> Self {
        let merkle = Self::merkle_root(&txs);
        let hash = Self::calculate_hash(height, &prev_hash, &merkle);
        Self { height, prev_hash, txs, merkle_root: merkle, hash }
    }

    fn calculate_hash(height: u64, prev: &str, merkle: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(height.to_be_bytes());
        hasher.update(prev.as_bytes());
        hasher.update(merkle.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn merkle_root(txs: &Vec<Transaction>) -> String {
        if txs.is_empty() {
            return hex::encode(Sha256::digest(b""));
        }
        let mut leaves: Vec<Vec<u8>> = txs.iter().map(|tx| {
            let mut h = Sha256::new();
            h.update(tx.id.as_bytes());
            h.update(tx.timestamp.to_be_bytes());
            h.finalize().to_vec()
        }).collect();

        while leaves.len() > 1 {
            if leaves.len() % 2 == 1 {

                let last = leaves.last().unwrap().clone();
                leaves.push(last);
            }
            let mut next = Vec::with_capacity(leaves.len()/2);
            for chunk in leaves.chunks(2) {
                let mut h = Sha256::new();
                h.update(&chunk[0]);
                h.update(&chunk[1]);
                next.push(h.finalize().to_vec());
            }
            leaves = next;
        }
        hex::encode(&leaves[0])
    }
}
