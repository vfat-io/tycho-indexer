use crate::{
    extractor::{
        evm,
        evm::{Account, AccountUpdate},
    },
    models::Chain,
    storage::ChangeType,
};
use std::collections::HashMap;
use tycho_types::Bytes;

pub struct Contract {
    chain: Chain,
    address: Bytes,
    title: String,
    slots: HashMap<Bytes, Bytes>,
    balance: Bytes,
    code_hash: Bytes,
    balance_modify_tx: Bytes,
    code_modify_tx: Bytes,
    creation_tx: Option<Bytes>,
}

impl From<evm::Account> for Contract {
    fn from(_value: Account) -> Self {
        todo!()
    }
}

pub struct ContractDelta {
    chain: Chain,
    address: Bytes,
    slots: HashMap<Bytes, Bytes>,
    balance: Option<Bytes>,
    code: Option<Bytes>,
    change: ChangeType,
}

impl From<evm::AccountUpdate> for ContractDelta {
    fn from(_value: AccountUpdate) -> Self {
        todo!()
    }
}
