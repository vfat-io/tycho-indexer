use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    models::{blockchain::Block, contract::AccountDelta, Address},
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
