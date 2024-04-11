pub mod ethrpc;
pub mod http_client;
pub mod trace_call;
pub mod trace_many;

use std::collections::HashMap;

use anyhow::Result;
use ethers::types::{H160, U256};
use trace_call::TokenOwnerFinding;
use web3::types::BlockNumber;
/// How well behaved a token is.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TokenQuality {
    Good,
    Bad { reason: String },
}

impl TokenQuality {
    pub fn is_good(&self) -> bool {
        matches!(self, Self::Good { .. })
    }

    pub fn bad(reason: impl ToString) -> Self {
        Self::Bad { reason: reason.to_string() }
    }
}

#[derive(Debug)]
pub struct TokenFinder {
    values: HashMap<H160, (H160, U256)>,
}

impl TokenFinder {
    pub fn new(values: HashMap<H160, (H160, U256)>) -> Self {
        TokenFinder { values }
    }
}

#[async_trait::async_trait]
impl TokenOwnerFinding for TokenFinder {
    async fn find_owner(&self, token: H160, _min_balance: U256) -> Result<Option<(H160, U256)>> {
        Ok(self.values.get(&token).copied())
    }
}

/// Detect how well behaved a token is.
#[mockall::automock]
#[async_trait::async_trait]
pub trait BadTokenDetecting: Send + Sync {
    async fn detect(
        &self,
        token: H160,
        block: BlockNumber,
    ) -> Result<(TokenQuality, Option<U256>, Option<U256>)>;
}
