use crate::{
    extractor::{
        evm,
        evm::{Account, AccountUpdate},
    },
    models::Chain,
    storage,
    storage::ChangeType,
};
use std::collections::HashMap;
use tycho_types::Bytes;

#[derive(Clone)]
pub struct Contract {
    pub chain: Chain,
    pub address: Bytes,
    pub title: String,
    pub slots: HashMap<Bytes, Bytes>,
    pub balance: Bytes,
    pub code: Bytes,
    pub code_hash: Bytes,
    pub balance_modify_tx: Bytes,
    pub code_modify_tx: Bytes,
    pub creation_tx: Option<Bytes>,
}

impl Contract {
    pub fn new() -> Self {
        todo!();
    }
}

impl From<evm::Account> for Contract {
    fn from(_value: Account) -> Self {
        todo!()
    }
}

#[derive(Clone)]
pub struct ContractDelta {
    pub chain: Chain,
    pub address: Bytes,
    pub slots: HashMap<Bytes, Bytes>,
    pub balance: Option<Bytes>,
    pub code: Option<Bytes>,
    pub change: ChangeType,
}

impl ContractDelta {
    pub fn new() -> Self {
        todo!();
    }
    pub fn contract_id(&self) -> storage::ContractId {
        storage::ContractId::new(self.chain, self.address.into())
    }
}

impl From<evm::AccountUpdate> for ContractDelta {
    fn from(_value: AccountUpdate) -> Self {
        todo!()
    }
}

impl From<Contract> for ContractDelta {
    fn from(value: Contract) -> Self {
        todo!()
    }
}
