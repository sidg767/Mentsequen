use crate::tx::Transaction;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub height: u64,
    pub prev_hash: String,
    pub txs: Vec<Transaction>,
    pub merkle_root: String,
    pub hash: String,
}
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
impl Block {
    pub fn new(height: u64, prev_hash: String, txs: Vec<Transaction>) -> Self {
        let merkle = Self::merkle_root(&txs);
        let hash = Self::calculate_hash(height, &prev_hash, &merkle);
        Self {
            height,
            prev_hash,
            txs,
            merkle_root: merkle,
            hash,
        }
    }

    fn calculate_hash(height: u64, prev: &str, merkle: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(height.to_be_bytes());
        hasher.update(prev.as_bytes());
        hasher.update(merkle.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn merkle_root(txs: &[Transaction]) -> String {
        if txs.is_empty() {
            return hex::encode(Sha256::digest(b""));
        }
        let mut leaves: Vec<Vec<u8>> = txs
            .iter()
            .map(|tx| {
                let mut h = Sha256::new();
                h.update(tx.id.as_bytes());
                h.update(tx.timestamp.to_be_bytes());
                h.finalize().to_vec()
            })
            .collect();

        while leaves.len() > 1 {
            if leaves.len() % 2 == 1 {
                let last = leaves.last().unwrap().clone();
                leaves.push(last);
            }
            let mut next = Vec::with_capacity(leaves.len() / 2);
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
