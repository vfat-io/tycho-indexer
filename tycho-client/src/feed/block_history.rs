use std::{collections::VecDeque, num::NonZeroUsize};

use lru::LruCache;

use tracing::error;
use tycho_types::{dto::Block, Bytes};

pub struct BlockHistory {
    history: VecDeque<Block>,
    reverts: LruCache<Bytes, Block>,
    size: usize,
}

pub enum BlockPosition {
    // The next expected block
    NextExpected,
    // The latest processed block
    Latest,
    // A previously seen block
    Delayed,
    // A unkown block with a height above latest
    Advanced,
}

impl BlockHistory {
    pub fn new(data: Vec<Block>, size: usize) -> Self {
        Self {
            history: VecDeque::from(data),
            size,
            reverts: LruCache::new(NonZeroUsize::new(size * 10).expect("cache size is 0")),
        }
    }

    pub fn push(&mut self, block: Block) -> anyhow::Result<()> {
        match self.determine_block_position(&block) {
            Ok(BlockPosition::NextExpected) => {
                // if the block is NextExpected, but does not fit on the latest block (via parent
                // hash), we are dealing with a revert.
                if let Some(true) = self
                    .latest()
                    .map(|b| b.hash != block.parent_hash)
                {
                    // keep removing the head until the new block fits
                    loop {
                        let head = self.history.back().ok_or_else(|| {
                            anyhow::format_err!(
                                "Revert block insert position not found! History exceeded."
                            )
                        })?;

                        if head.hash == block.parent_hash {
                            break;
                        } else {
                            let reverted_block = self.history.pop_back().ok_or_else(|| {
                                anyhow::format_err!(
                                    "Revert block insert position not found! History exceeded."
                                )
                            })?;
                            self.reverts
                                .push(reverted_block.hash.clone(), reverted_block);
                        }
                    }
                }
                self.history.push_back(block);
                if self.history.len() > self.size {
                    self.history.pop_front();
                }
                Ok(())
            }
            Err(e) => return Err(e),
            _ => return Ok(()),
        }
    }

    pub fn determine_block_position(&self, block: &Block) -> anyhow::Result<BlockPosition> {
        let latest = self
            .latest()
            .ok_or_else(|| anyhow::format_err!("history empty"))?;
        Ok(if block.parent_hash == latest.hash {
            BlockPosition::NextExpected
        } else if block.hash == latest.hash {
            BlockPosition::Latest
        } else if block.number > latest.number {
            BlockPosition::Advanced
        } else {
            if self
                .history
                .iter()
                .find(|b| b.hash == block.hash)
                .is_some()
            {
                // if this block is delayed we have seen it's hash before
                BlockPosition::Delayed
            } else if self.reverts.contains(&block.hash) {
                // the block is still on an already reverted branch.
                BlockPosition::Delayed
            } else if self
                .history
                .iter()
                .find(|b| b.hash == block.parent_hash)
                .is_some()
            {
                // if this is a revert, we will find the
                // parent hash within out history
                BlockPosition::NextExpected
            } else {
                let n_historical_blocks = self.history.len();
                error!(?n_historical_blocks, ?block, "Could not determine history");
                anyhow::bail!("Could not determine the blocks position!")
            }
        })
    }

    pub fn latest(&self) -> Option<&Block> {
        self.history.back()
    }
}

#[cfg(test)]
mod test {

    use chrono::Local;
    use rand::Rng;
    use tycho_types::{
        dto::{Block, Chain},
        Bytes,
    };

    fn random_hash() -> Bytes {
        let mut rng = rand::thread_rng();

        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes[..]);

        return Bytes::from(bytes);
    }

    fn generate_blocks(n: usize) {
        let mut blocks = Vec::with_capacity(n);
        let mut parent_hash = random_hash();
        for i in 0..n {
            let hash = random_hash();
            blocks.push(Block {
                number: n as u64,
                hash,
                parent_hash,
                chain: Chain::Ethereum,
                ts: Local::now().naive_utc(),
            });
            parent_hash = random_hash();
        }
    }
}
