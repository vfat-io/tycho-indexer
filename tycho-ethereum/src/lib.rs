pub mod account_extractor;
pub mod token_analyzer;
pub mod token_pre_processor;

use ethers::providers::ProviderError;
use thiserror::Error;
use tycho_core::models::{blockchain::BlockTag, token::TokenQuality};
use web3::types::BlockNumber;

#[derive(Error, Debug)]
pub enum RPCError {
    #[error("RPC setup error: {0}")]
    SetupError(String),
    #[error("RPC error: {0}")]
    RequestError(#[from] ProviderError),
}

pub struct BlockTagWrapper(BlockTag);

impl From<BlockTagWrapper> for BlockNumber {
    fn from(value: BlockTagWrapper) -> Self {
        match value.0 {
            BlockTag::Finalized => BlockNumber::Finalized,
            BlockTag::Safe => BlockNumber::Safe,
            BlockTag::Latest => BlockNumber::Latest,
            BlockTag::Earliest => BlockNumber::Earliest,
            BlockTag::Pending => BlockNumber::Pending,
            BlockTag::Number(n) => BlockNumber::Number(n.into()),
        }
    }
}
