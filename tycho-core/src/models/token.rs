use std::collections::HashMap;

use crate::{models::Chain, traits::TokenOwnerFinding, Bytes};
use serde::{Deserialize, Serialize};

use super::{Address, Balance};

/// Cost related to a token transfer, for example amount of gas in evm chains.
pub type TransferCost = u64;

/// Tax related to a token transfer. Should be given in Basis Points (1/100th of a percent)
pub type TransferTax = u64;

#[derive(PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct CurrencyToken {
    pub address: Bytes,
    pub symbol: String,
    pub decimals: u32,
    pub tax: TransferTax,
    pub gas: Vec<Option<TransferCost>>,
    pub chain: Chain,
    /// Quality is between 0-100, where:
    ///  - 100: Normal token
    ///  - 75: Rebase token
    ///  - 50: Fee token
    ///  - 10: Token analysis failed at creation
    ///  - 5: Token analysis failed on cronjob (after creation).
    ///  - 0: Failed to extract decimals onchain
    pub quality: u32,
}

impl CurrencyToken {
    pub fn new(
        address: &Bytes,
        symbol: &str,
        decimals: u32,
        tax: u64,
        gas: &[Option<u64>],
        chain: Chain,
        quality: u32,
    ) -> Self {
        Self {
            address: address.clone(),
            symbol: symbol.to_string(),
            decimals,
            tax,
            gas: gas.to_owned(),
            chain,
            quality,
        }
    }
}

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
pub struct TokenFinderStore {
    values: HashMap<Address, (Address, Balance)>,
}

impl TokenFinderStore {
    pub fn new(values: HashMap<Address, (Address, Balance)>) -> Self {
        TokenFinderStore { values }
    }
}

#[async_trait::async_trait]
impl TokenOwnerFinding for TokenFinderStore {
    async fn find_owner(
        &self,
        token: Address,
        _min_balance: Balance,
    ) -> Result<Option<(Address, Balance)>, String> {
        Ok(self.values.get(&token).cloned())
    }
}
