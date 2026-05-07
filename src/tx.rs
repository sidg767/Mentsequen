use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub data: String,
    pub timestamp: u64,
    pub pubkey: Option<String>,
    pub signature: Option<String>,
}

impl Transaction {
    pub fn new(data: String) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Transaction {
            id: Uuid::new_v4().to_string(),
            data,
            timestamp: ts,
            pubkey: None,
            signature: None,
        }
    }
}
