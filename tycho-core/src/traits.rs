use core::fmt::Debug;
use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use crate::{
    models::{
        blockchain::{Block, BlockTag},
        contract::AccountDelta,
        token::{CurrencyToken, TokenQuality, TransferCost, TransferTax},
        Address, Balance,
    },
    Bytes,
};

#[async_trait]
pub trait AccountExtractor {
    type Error;

    async fn get_accounts(
        &self,
        block: Block,
        account_addresses: Vec<Address>,
    ) -> Result<HashMap<Bytes, AccountDelta>, Self::Error>; //TODO: do not return `AccountUpdate` but `Account`
}

/// Analyze how well behaved a token is.
#[async_trait]
pub trait TokenQualityAnalyzer: Send + Sync {
    type Error;

    /// Given a token address and a block tag, this function analyzes the quality of a token.
    /// It return its `TokenQuality`, the average cost per transfer and any transfer tax.
    async fn analyze(
        &self,
        token: Bytes,
        block: BlockTag,
    ) -> Result<(TokenQuality, Option<TransferCost>, Option<TransferTax>), Self::Error>;
}

/// To detect bad tokens we need to find some address on the network that owns
/// the token so that we can use it in our simulations.
#[async_trait]
pub trait TokenOwnerFinding: Send + Sync + Debug {
    /// Find an address with at least `min_balance` of tokens and return it,
    /// along with its actual balance.
    /// If no such address is found, returns None.
    async fn find_owner(
        &self,
        token: Address,
        min_balance: Balance,
    ) -> Result<Option<(Address, Balance)>, String>; //TODO: introduce custom error
}

/// Get additionnal tokens informations
#[async_trait]
pub trait TokenPreProcessorTrait: Send + Sync {
    /// Given a list of token addresses, iterate over each token address to get additionnal
    /// information such as the number of decimals and the symbol to construct a `CurrencyToken`.
    async fn get_tokens(
        &self,
        addresses: Vec<Bytes>,
        token_finder: Arc<dyn TokenOwnerFinding>,
        block: BlockTag,
    ) -> Vec<CurrencyToken>;
}
