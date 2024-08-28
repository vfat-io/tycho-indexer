use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    models::{blockchain::Block, contract::AccountUpdate, Address},
    Bytes,
};

#[async_trait]
pub trait AccountExtractor {
    type Error;

    async fn get_accounts(
        &self,
        block: Block,
        account_addresses: Vec<Address>,
    ) -> Result<HashMap<Bytes, AccountUpdate>, Self::Error>;
}

// #[async_trait]
// pub trait TokenPreProcessor: Send + Sync {
//     async fn get_tokens(
//         &self,
//         addresses: Vec<Bytes>,
//         token_finder: Arc<dyn TokenOwnerFinding>,
//         block: BlockNumber,
//     ) -> Vec<ERC20Token>;
// }
