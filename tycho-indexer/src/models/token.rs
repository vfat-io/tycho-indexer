use crate::{
    extractor::{evm, evm::ERC20Token},
    models::Chain,
};
use tycho_types::Bytes;

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
    ///  - 0: Scam token that we shouldn't use
    pub quality: u32,
}

impl From<evm::ERC20Token> for CurrencyToken {
    fn from(_value: ERC20Token) -> Self {
        todo!()
    }
}
