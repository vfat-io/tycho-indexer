use std::collections::HashMap;

use ethers::types::{H256, U256};

use crate::models::{Chain, ExtractorIdentity, NormalisedMessage};

pub mod ambient;

pub struct SwapPool {}

pub struct ERC20Token {}

pub struct Block {}

pub struct Transaction {}

pub struct Account {}

pub struct AccountUpdate {
    extractor: String,
    slots: HashMap<U256, U256>,
    balance: Option<U256>,
    code: Option<Vec<u8>>,
    code_hash: Option<H256>,
}

impl NormalisedMessage for AccountUpdate {
    fn source(&self) -> ExtractorIdentity {
        return ExtractorIdentity {
            chain: Chain::Ethereum,
            name: self.extractor.clone(),
        };
    }
}
