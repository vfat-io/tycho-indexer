pub mod ambient;

use crate::models::{Chain, ExtractorIdentity, NormalisedMessage};
use std::collections::HashMap;

use ethers::types::{H256, U256};

pub struct SwapPool {}

pub struct ERC20Token {}

pub struct Block {}

pub struct Transaction {}

pub struct Account {}

pub struct AccountUpdate {
    extractor: String,
    pub slots: HashMap<U256, U256>,
    pub balance: Option<U256>,
    pub code: Option<Vec<u8>>,
    pub code_hash: Option<H256>,
}

impl NormalisedMessage for AccountUpdate {
    fn source(&self) -> ExtractorIdentity {
        return ExtractorIdentity {
            chain: Chain::Ethereum,
            name: self.extractor.clone(),
        };
    }
}
