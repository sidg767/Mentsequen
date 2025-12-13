use crate::block::Block;
use std::path::PathBuf;
use std::fs;
use serde_json::to_string_pretty;
use anyhow::Result;

#[derive(Clone)]
pub struct DALayer {
    base: PathBuf,
}

impl DALayer {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let base = dir.into();
        let _ = fs::create_dir_all(&base);
        Self { base }
    }

    pub fn persist_block(&self, block: &Block) -> Result<()> {
        let name = format!("block_{:06}.json", block.height);
        let path = self.base.join(name);
        let s = to_string_pretty(block)?;
        std::fs::write(path, s)?;
        Ok(())
    }
}
