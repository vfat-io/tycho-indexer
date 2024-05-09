use crate::{models::Chain, Bytes};
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct CurrencyToken {
    pub address: Bytes,
    pub symbol: String,
    pub decimals: u32,
    pub tax: u64,
    pub gas: Vec<Option<u64>>,
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
